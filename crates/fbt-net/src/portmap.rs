//! Port mapping on the gateway: UPnP IGD and PCP/NAT-PMP side by side.
//!
//! [`PortMapper`] runs one worker per method. Each keeps the wanted
//! mappings, makes them, renews them at half their lifetime (never more often
//! than every [`MIN_RENEW`]) and, after a failure, retries from
//! [`PortMapConfig::retry_base`], doubling up to [`PortMapConfig::retry_max`].
//! Results arrive as [`MapEvent`]s. [`PortMapper::shutdown`] removes every
//! mapping made, within [`SHUTDOWN_BUDGET`].

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tracing::{debug, info};

use crate::natpmp::{self, Mapping, NATPMP_PORT};
use crate::upnp::{self, Igd, SSDP_V4};
use crate::{MapMethod, MapProtocol, gateway};

/// Renewals never come more often than this.
pub const MIN_RENEW: Duration = Duration::from_secs(60);
/// Total time [`PortMapper::shutdown`] may spend removing mappings.
pub const SHUTDOWN_BUDGET: Duration = Duration::from_secs(2);
/// Distinct mappings one [`PortMapper`] keeps; further requests are ignored.
pub const MAX_MAPPINGS: usize = 64;
/// Bound on one UPnP discovery (SSDP rounds and the description fetch).
const DISCOVER_TIMEOUT: Duration = Duration::from_secs(5);
/// Queued `map` requests per worker.
const CMD_QUEUE: usize = MAX_MAPPINGS;

/// What a [`PortMapper`] does.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PortMapConfig {
    /// Map through UPnP IGD.
    pub upnp: bool,
    /// Map through PCP, falling back to NAT-PMP.
    pub natpmp: bool,
    /// The PCP/NAT-PMP gateway; discovered (the default route) when `None`.
    pub gateway: Option<Ipv4Addr>,
    /// Where SSDP searches go ([`SSDP_V4`]).
    pub ssdp: SocketAddr,
    /// The gateway's PCP/NAT-PMP port ([`NATPMP_PORT`]).
    pub natpmp_port: u16,
    /// Lifetime asked for, in seconds.
    pub lease: u32,
    /// First retry delay after a failure.
    pub retry_base: Duration,
    /// Longest retry delay.
    pub retry_max: Duration,
}

impl Default for PortMapConfig {
    fn default() -> Self {
        PortMapConfig {
            upnp: true,
            natpmp: true,
            gateway: None,
            ssdp: SSDP_V4,
            natpmp_port: NATPMP_PORT,
            lease: 3600,
            retry_base: Duration::from_secs(60),
            retry_max: Duration::from_secs(30 * 60),
        }
    }
}

/// What happened to a mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MapEvent {
    /// A mapping was made (or its external address changed).
    Mapped {
        /// Transport protocol.
        protocol: MapProtocol,
        /// The method that made it.
        method: MapMethod,
        /// The local port.
        internal: u16,
        /// The public address and port (`0.0.0.0` when UPnP could not tell
        /// the address).
        external: SocketAddr,
    },
    /// An attempt to make or renew a mapping failed; it is retried later.
    Failed {
        /// Transport protocol.
        protocol: MapProtocol,
        /// The method that failed.
        method: MapMethod,
        /// Why.
        message: String,
    },
    /// The gateway reported its public address (first time or changed).
    ExternalIp {
        /// The address.
        ip: Ipv4Addr,
        /// Who reported it.
        method: MapMethod,
    },
}

#[derive(Debug, Clone, Copy)]
struct Wanted {
    protocol: MapProtocol,
    internal: u16,
}

#[derive(Debug)]
struct Shared {
    upnp: Option<mpsc::Sender<Wanted>>,
    natpmp: Option<mpsc::Sender<Wanted>>,
    stop: watch::Sender<bool>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

/// Keeps ports mapped on the gateway. Cheap to clone; every clone is the
/// same mapper. Dropping the last clone stops it without unmapping.
#[derive(Debug, Clone)]
pub struct PortMapper {
    shared: Arc<Shared>,
}

impl PortMapper {
    /// Starts the workers `cfg` enables; results go to `events` (dropped
    /// when it is full).
    pub fn spawn(cfg: PortMapConfig, events: mpsc::Sender<MapEvent>) -> PortMapper {
        let (stop, _) = watch::channel(false);
        let mut workers = Vec::new();
        let mut start = |method: Method| {
            let (tx, rx) = mpsc::channel(CMD_QUEUE);
            let w = Worker {
                method,
                cfg: cfg.clone(),
                events: events.clone(),
                entries: Vec::new(),
                gateway: cfg.gateway,
                igd: None,
                last_igd: None,
                external: None,
                reported_ip: None,
                epoch: None,
            };
            workers.push(tokio::spawn(w.run(rx, stop.subscribe())));
            tx
        };
        let upnp = cfg.upnp.then(|| start(Method::Upnp));
        let natpmp = cfg.natpmp.then(|| start(Method::NatPmp));
        PortMapper {
            shared: Arc::new(Shared {
                upnp,
                natpmp,
                stop,
                workers: Mutex::new(workers),
            }),
        }
    }

    /// Maps `internal` for `p` with every enabled method, and keeps it
    /// mapped. A repeat is a no-op.
    pub fn map(&self, p: MapProtocol, internal: u16) {
        let want = Wanted {
            protocol: p,
            internal,
        };
        for tx in [&self.shared.upnp, &self.shared.natpmp]
            .into_iter()
            .flatten()
        {
            if tx.try_send(want).is_err() {
                debug!(?p, internal, "port mapping request dropped");
            }
        }
    }

    /// Stops the workers and removes every mapping they made, giving up
    /// after [`SHUTDOWN_BUDGET`] in total. Idempotent.
    pub async fn shutdown(&self) {
        self.shared.stop.send_replace(true);
        let workers = std::mem::take(
            &mut *self
                .shared
                .workers
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
        );
        let aborts: Vec<_> = workers.iter().map(JoinHandle::abort_handle).collect();
        let all = async {
            for w in workers {
                let _ = w.await;
            }
        };
        if tokio::time::timeout(SHUTDOWN_BUDGET, all).await.is_err() {
            debug!("port unmapping cut short");
            for a in aborts {
                a.abort();
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Method {
    Upnp,
    NatPmp,
}

/// What a method holds for one mapping.
#[derive(Debug, Clone, Copy)]
enum Held {
    Upnp { external: u16 },
    NatPmp(Mapping),
}

#[derive(Debug)]
struct Entry {
    want: Wanted,
    held: Option<Held>,
    /// When the next attempt (map, renewal or retry) is due.
    due: Instant,
    /// Delay before the next retry after a failure.
    backoff: Duration,
    /// When the held mapping expires.
    expires: Option<Instant>,
    /// The external address last reported in `Mapped`.
    reported: Option<SocketAddr>,
}

struct Worker {
    method: Method,
    cfg: PortMapConfig,
    events: mpsc::Sender<MapEvent>,
    entries: Vec<Entry>,
    /// The PCP/NAT-PMP gateway, once known.
    gateway: Option<Ipv4Addr>,
    /// The discovered IGD; dropped after a failure, so it is found again.
    igd: Option<Igd>,
    /// The IGD of the last successful UPnP mapping: it removes the held
    /// mappings at shutdown even while `igd` is being looked up again.
    last_igd: Option<Igd>,
    /// The gateway's external address as last learned.
    external: Option<Ipv4Addr>,
    /// The external address an `ExternalIp` event last went out for.
    reported_ip: Option<Ipv4Addr>,
    /// The PCP/NAT-PMP gateway's last epoch.
    epoch: Option<u32>,
}

impl Worker {
    async fn run(mut self, mut cmds: mpsc::Receiver<Wanted>, mut stop: watch::Receiver<bool>) {
        loop {
            let next = self.entries.iter().map(|e| e.due).min();
            tokio::select! {
                biased;
                unmap = stop_signal(&mut stop) => if unmap { break } else { return },
                cmd = cmds.recv() => {
                    let Some(w) = cmd else { return };
                    self.add(w);
                    // Requests made together go out in one round.
                    while let Ok(w) = cmds.try_recv() {
                        self.add(w);
                    }
                }
                () = sleep_until_opt(next) => {
                    tokio::select! {
                        biased;
                        unmap = stop_signal(&mut stop) => if unmap { break } else { return },
                        () = self.round() => {}
                    }
                }
            }
        }
        self.unmap_all().await;
    }

    fn add(&mut self, w: Wanted) {
        let dup = self
            .entries
            .iter()
            .any(|e| e.want.protocol == w.protocol && e.want.internal == w.internal);
        if dup || self.entries.len() >= MAX_MAPPINGS {
            return;
        }
        self.entries.push(Entry {
            want: w,
            held: None,
            due: Instant::now(),
            backoff: self.cfg.retry_base,
            expires: None,
            reported: None,
        });
    }

    /// Sends `ev`; false when the channel was full (or closed), so the
    /// caller reports it again later.
    fn emit(&self, ev: MapEvent) -> bool {
        let sent = self.events.try_send(ev).is_ok();
        if !sent {
            debug!("port mapping event dropped");
        }
        sent
    }

    /// One pass over the due entries. Delays count from the round's start,
    /// so the schedule does not drift with the time attempts take.
    async fn round(&mut self) {
        let started = Instant::now();
        let due: Vec<usize> = (0..self.entries.len())
            .filter(|&i| self.entries.get(i).is_some_and(|e| e.due <= started))
            .collect();
        if due.is_empty() {
            return;
        }
        let ready = match self.method {
            Method::Upnp => self.ensure_igd().await,
            Method::NatPmp => self.ensure_gateway().await,
        };
        let mut failed = false;
        for i in due {
            let result = match &ready {
                Err(e) => Err(e.clone()),
                Ok(()) => self.attempt(i).await,
            };
            failed |= result.is_err();
            self.settle(i, started, result);
        }
        // After a failure the IGD is looked up again next round (it may
        // have restarted elsewhere); the rest of this round still used it.
        if failed && self.method == Method::Upnp {
            self.igd = None;
        }
    }

    /// Finds the IGD (and asks its external address) unless known.
    async fn ensure_igd(&mut self) -> Result<(), String> {
        if self.igd.is_some() {
            return Ok(());
        }
        let igd = upnp::discover(self.cfg.ssdp, DISCOVER_TIMEOUT)
            .await
            .map_err(|e| format!("discovery: {e}"))?;
        match igd.external_ip().await {
            Ok(ip) => self.note_external(ip, MapMethod::Upnp),
            Err(e) => debug!(error = %e, "UPnP: no external address"),
        }
        self.igd = Some(igd);
        Ok(())
    }

    /// Finds the default gateway unless configured or known.
    async fn ensure_gateway(&mut self) -> Result<(), String> {
        if self.gateway.is_none() {
            self.gateway = gateway::default_gateway_v4().await;
        }
        match self.gateway {
            Some(_) => Ok(()),
            None => Err("no default IPv4 gateway".to_owned()),
        }
    }

    /// Records the gateway's external address and reports it unless the
    /// last report already carried it. A report the channel dropped is
    /// retried at the next answer.
    fn note_external(&mut self, ip: Ipv4Addr, method: MapMethod) {
        if self.external != Some(ip) {
            info!(%ip, ?method, "gateway external address");
        }
        self.external = Some(ip);
        if self.reported_ip != Some(ip) && self.emit(MapEvent::ExternalIp { ip, method }) {
            self.reported_ip = Some(ip);
        }
    }

    /// Makes or renews entry `i`; the mapping and its lifetime in seconds.
    async fn attempt(&mut self, i: usize) -> Result<(Held, u32), String> {
        let Some(e) = self.entries.get(i) else {
            return Err("no such mapping".to_owned());
        };
        let (want, held) = (e.want, e.held);
        match self.method {
            Method::Upnp => {
                let igd = self.igd.clone().ok_or_else(|| "no IGD".to_owned())?;
                // A renewal asks for the port it holds: after a conflict
                // (718) that is a random one, and asking for the internal
                // port again would get another, leaking the first.
                let prev = match held {
                    Some(Held::Upnp { external }) => Some(external),
                    _ => None,
                };
                let first = prev.unwrap_or(want.internal);
                let external = igd
                    .add_mapping_from(want.protocol, want.internal, first, self.cfg.lease)
                    .await
                    .map_err(|e| e.to_string())?;
                // Moved anyway (the held port was taken meanwhile): the old
                // entry, if still ours, must not outlive us.
                if let Some(old) = prev.filter(|&old| old != external)
                    && let Err(e) = igd.delete_mapping(want.protocol, old).await
                {
                    debug!(port = old, error = %e, "UPnP: old mapping not removed");
                }
                self.last_igd = Some(igd);
                // Retries an `ExternalIp` report the channel dropped.
                if let Some(ip) = self.external {
                    self.note_external(ip, MapMethod::Upnp);
                }
                Ok((Held::Upnp { external }, self.cfg.lease))
            }
            Method::NatPmp => {
                let gw = self.gateway.ok_or_else(|| "no gateway".to_owned())?;
                let port = self.cfg.natpmp_port;
                let r = match held {
                    Some(Held::NatPmp(prev)) => {
                        natpmp::renew(
                            gw,
                            port,
                            &prev,
                            want.protocol,
                            want.internal,
                            self.cfg.lease,
                        )
                        .await
                    }
                    _ => natpmp::map(gw, port, want.protocol, want.internal, self.cfg.lease).await,
                };
                let m = r.map_err(|e| e.to_string())?;
                if let IpAddr::V4(ip) = m.external.ip() {
                    self.note_external(ip, m.method);
                }
                // The epoch going backwards means the gateway lost its
                // table: everything else is remapped now.
                if self.epoch.is_some_and(|prev| m.epoch < prev) {
                    info!("PCP/NAT-PMP gateway restarted; remapping");
                    let now = Instant::now();
                    for (j, other) in self.entries.iter_mut().enumerate() {
                        if j != i && other.held.is_some() {
                            other.due = other.due.min(now);
                        }
                    }
                }
                self.epoch = Some(m.epoch);
                Ok((Held::NatPmp(m), m.lifetime))
            }
        }
    }

    /// The method a result of entry `i` is reported under.
    fn method_of(&self, held: Option<&Held>) -> MapMethod {
        match (self.method, held) {
            (Method::Upnp, _) => MapMethod::Upnp,
            (Method::NatPmp, Some(Held::NatPmp(m))) => m.method,
            // A first attempt starts with PCP.
            (Method::NatPmp, _) => MapMethod::Pcp,
        }
    }

    fn settle(&mut self, i: usize, started: Instant, result: Result<(Held, u32), String>) {
        let method_before = self.method_of(self.entries.get(i).and_then(|e| e.held.as_ref()));
        let external_ip = self.external;
        let base = self.cfg.retry_base;
        let max = self.cfg.retry_max;
        let Some(e) = self.entries.get_mut(i) else {
            return;
        };
        match result {
            Ok((held, lifetime)) => {
                let life = Duration::from_secs(u64::from(lifetime));
                e.held = Some(held);
                e.expires = Some(started + life);
                e.due = started + (life / 2).max(MIN_RENEW);
                e.backoff = base;
                let (method, external) = match held {
                    Held::Upnp { external } => (
                        MapMethod::Upnp,
                        SocketAddr::new(
                            IpAddr::V4(external_ip.unwrap_or(Ipv4Addr::UNSPECIFIED)),
                            external,
                        ),
                    ),
                    Held::NatPmp(m) => (m.method, m.external),
                };
                if e.reported != Some(external) {
                    let ev = MapEvent::Mapped {
                        protocol: e.want.protocol,
                        method,
                        internal: e.want.internal,
                        external,
                    };
                    info!(?ev, "port mapped");
                    // A dropped report goes out again at the next renewal.
                    if self.emit(ev)
                        && let Some(e) = self.entries.get_mut(i)
                    {
                        e.reported = Some(external);
                    }
                }
            }
            Err(message) => {
                e.due = started + e.backoff;
                e.backoff = e.backoff.saturating_mul(2).min(max);
                // An expired mapping is gone: the next attempt starts over.
                if e.expires.is_some_and(|x| x <= Instant::now()) {
                    e.held = None;
                    e.expires = None;
                    e.reported = None;
                }
                let protocol = e.want.protocol;
                debug!(?protocol, %message, "port mapping failed");
                self.emit(MapEvent::Failed {
                    protocol,
                    method: method_before,
                    message,
                });
            }
        }
    }

    /// Removes every held mapping, all at once (the caller bounds the time).
    async fn unmap_all(&mut self) {
        let mut jobs = JoinSet::new();
        for e in &self.entries {
            let (p, internal) = (e.want.protocol, e.want.internal);
            match e.held {
                Some(Held::Upnp { external }) => {
                    if let Some(igd) = self.igd.clone().or_else(|| self.last_igd.clone()) {
                        jobs.spawn(async move {
                            if let Err(e) = igd.delete_mapping(p, external).await {
                                debug!(error = %e, "UPnP unmap failed");
                            }
                        });
                    }
                }
                Some(Held::NatPmp(m)) => {
                    if let Some(gw) = self.gateway {
                        let port = self.cfg.natpmp_port;
                        jobs.spawn(async move {
                            if let Err(e) = natpmp::unmap(gw, port, &m, p, internal).await {
                                debug!(error = %e, "PCP/NAT-PMP unmap failed");
                            }
                        });
                    }
                }
                None => {}
            }
        }
        // Dropping the set (when the caller gives up) aborts what is left.
        while jobs.join_next().await.is_some() {}
    }
}

/// True once shutdown is asked for; false when the mapper is gone (then
/// nothing is unmapped).
async fn stop_signal(stop: &mut watch::Receiver<bool>) -> bool {
    loop {
        if *stop.borrow_and_update() {
            return true;
        }
        if stop.changed().await.is_err() {
            return false;
        }
    }
}

async fn sleep_until_opt(t: Option<Instant>) {
    match t {
        Some(t) => tokio::time::sleep_until(t).await,
        None => std::future::pending().await,
    }
}
