//! Binding the listen addresses: one TCP peer listener per address and, on
//! the same port, one UDP socket shared by uTP, the DHT and UDP trackers.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use fbt_core::udp::DatagramSender;
use fbt_dht::ExternalIpVotes;
use fbt_net::demux::{PacketKind, SharedUdpSocket};
use fbt_net::lsd::{LSD_HASHES_PER_MESSAGE, Lsd, LsdConfig, LsdPeer};
use fbt_net::{MapEvent, MapProtocol, PortMapConfig, PortMapper};
use fbt_utp::{UtpConfig, UtpSocket};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::error::SessionError;
use crate::event::{Event, ExternalIpSource};
use crate::settings::Settings;

/// Tries per port-0 address to find a port free for both TCP and UDP.
const BIND_ATTEMPTS: usize = 8;

/// Backlog of each TCP listener (tokio's default for `TcpListener::bind`).
const LISTEN_BACKLOG: i32 = 1024;

/// Most destinations NAT emulation remembers per uTP socket; the oldest is
/// forgotten beyond it.
const MAX_RECENT_SENDS: usize = 1024;

/// How long an emulated NAT mapping stays open after a send.
const NAT_MAPPING_LIFETIME: Duration = Duration::from_secs(30);

/// A listen address that could not be bound (fully, or only its UDP side).
/// See [`Session::listen_errors`](crate::Session::listen_errors).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ListenError {
    /// The address as given in [`Settings::listen_addrs`].
    pub addr: SocketAddr,
    /// TCP bound and only the UDP socket failed: the address accepts peer
    /// connections, but the DHT and UDP trackers do not use it.
    pub udp_only: bool,
    /// The OS error.
    pub message: String,
}

/// One bound listen address.
#[derive(Debug)]
pub(crate) struct Endpoint {
    /// The bound address (the real port when the setting asked for 0).
    pub addr: SocketAddr,
    /// `None` with `enable_incoming_tcp` off: the TCP socket only picked
    /// the port and was closed again, so TCP dials to it are refused at
    /// once. (A socket left bound but not listening would not do: macOS
    /// drops SYNs to one, so a dialler waits out its connect timeout.)
    pub tcp: Option<TcpListener>,
    /// `None` when only the UDP side failed to bind.
    pub udp: Option<SharedUdpSocket>,
}

/// Binds every address of `settings.listen_addrs`, in order. An address
/// whose TCP bind fails is skipped; one whose UDP bind fails keeps TCP only.
/// Both are reported as [`ListenError`]s. Without `udp` (a SOCKS5 relay
/// carries all UDP) only TCP is bound. Fails with the first error when
/// nothing binds.
pub(crate) async fn bind_all(
    settings: &Settings,
    udp: bool,
) -> Result<(Vec<Endpoint>, Vec<ListenError>), SessionError> {
    let mut endpoints = Vec::new();
    let mut errors = Vec::new();
    let mut first: Option<io::Error> = None;
    for &addr in &settings.listen_addrs {
        // NAT emulation refuses incoming TCP as `enable_incoming_tcp` off does.
        let listen = settings.enable_incoming_tcp && !settings.nat_emulation;
        let bound = if udp {
            bind_one(addr, listen)
        } else {
            bind_tcp_only(addr, listen)
        };
        match bound {
            Ok((ep, udp_err)) => {
                if let Some(e) = udp_err {
                    warn!(%addr, error = %e, "cannot bind the UDP socket; TCP only on this address");
                    errors.push(ListenError {
                        addr,
                        udp_only: true,
                        message: e.to_string(),
                    });
                }
                endpoints.push(ep);
            }
            Err(e) => {
                warn!(%addr, error = %e, "cannot bind the listen address; skipping it");
                errors.push(ListenError {
                    addr,
                    udp_only: false,
                    message: e.to_string(),
                });
                first.get_or_insert(e);
            }
        }
    }
    if endpoints.is_empty() {
        let e = first.unwrap_or_else(|| io::Error::other("no listen address"));
        return Err(SessionError::Bind(e));
    }
    Ok((endpoints, errors))
}

/// Binds TCP on `addr`, then UDP on the TCP's port. With port 0 a UDP
/// failure retries both on a fresh port, up to [`BIND_ATTEMPTS`] times;
/// otherwise the endpoint keeps TCP only and the UDP error comes back.
/// Without `listen` the TCP socket is closed once UDP is bound (or failed).
fn bind_one(addr: SocketAddr, listen: bool) -> io::Result<(Endpoint, Option<io::Error>)> {
    let mut attempt = 1;
    loop {
        let tcp = bind_tcp(addr, listen)?;
        let local = tcp.local_addr()?;
        let tcp = listen.then_some(tcp);
        match SharedUdpSocket::bind(local) {
            Ok(udp) => {
                let ep = Endpoint {
                    addr: local,
                    tcp,
                    udp: Some(udp),
                };
                return Ok((ep, None));
            }
            Err(_) if addr.port() == 0 && attempt < BIND_ATTEMPTS => attempt += 1,
            Err(e) => {
                let ep = Endpoint {
                    addr: local,
                    tcp,
                    udp: None,
                };
                return Ok((ep, Some(e)));
            }
        }
    }
}

/// Binds TCP on `addr` alone: the endpoint has no UDP socket.
fn bind_tcp_only(addr: SocketAddr, listen: bool) -> io::Result<(Endpoint, Option<io::Error>)> {
    let tcp = bind_tcp(addr, listen)?;
    let ep = Endpoint {
        addr: tcp.local_addr()?,
        tcp: listen.then_some(tcp),
        udp: None,
    };
    Ok((ep, None))
}

/// A TCP listener on `addr`; IPv6 binds IPv6-only, so `[::]:p` does not
/// collide with `0.0.0.0:p` on dual-stack hosts. Without `listen` the
/// socket is only bound (to pick a port). Must run inside a tokio runtime.
fn bind_tcp(addr: SocketAddr, listen: bool) -> io::Result<TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    let domain = if addr.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };
    let s = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    if addr.is_ipv6() {
        s.set_only_v6(true)?;
    }
    // As std and tokio do on Unix: a restarted session can rebind its port
    // while old connections linger in TIME_WAIT. (On Windows the option
    // would let another socket steal the port, so it stays off.)
    #[cfg(not(windows))]
    s.set_reuse_address(true)?;
    s.set_nonblocking(true)?;
    s.bind(&addr.into())?;
    if listen {
        s.listen(LISTEN_BACKLOG)?;
    }
    TcpListener::from_std(s.into())
}

/// Capacity of the channel LSD hands found peers over.
const LSD_FOUND_QUEUE: usize = 256;

/// How often the session sends the announces the torrents asked for,
/// batching them across torrents.
const LSD_FLUSH: Duration = Duration::from_millis(250);

/// Messages the receiving side lets through per source in a burst
/// (`fbt-net`'s inbound limiter), then one a second. The session paces its
/// sends to match, or the hashes beyond the burst would be dropped every
/// cycle.
const LSD_SEND_BURST: u64 = 5;
/// Cost of one message in the pacing bucket, in milliseconds of refill (one
/// message a second).
const LSD_MSG_COST_MS: u64 = 1000;

#[derive(Debug)]
struct LsdState {
    /// When each wire hash was last queued for announcing.
    last: HashMap<[u8; 20], Instant>,
    /// Hashes waiting to be sent, oldest first; `queued` is their set.
    pending: VecDeque<[u8; 20]>,
    queued: HashSet<[u8; 20]>,
    /// Pacing bucket in milliseconds of credit (a message costs
    /// [`LSD_MSG_COST_MS`]), and when it was last refilled.
    credit_ms: u64,
    refilled: Option<Instant>,
}

impl Default for LsdState {
    fn default() -> Self {
        LsdState {
            last: HashMap::new(),
            pending: VecDeque::new(),
            queued: HashSet::new(),
            credit_ms: LSD_SEND_BURST * LSD_MSG_COST_MS,
            refilled: None,
        }
    }
}

/// The session's local service discovery: the [`Lsd`] instance (none when
/// disabled or when no group could be joined) and the per-hash announce
/// bookkeeping that keeps every torrent to one announce per interval,
/// however often it pauses and resumes.
#[derive(Debug, Default)]
pub(crate) struct LsdHub {
    lsd: Option<Lsd>,
    interval: Duration,
    state: Mutex<LsdState>,
}

impl LsdHub {
    /// No LSD.
    pub(crate) fn disabled() -> Self {
        LsdHub::default()
    }

    /// Starts LSD as the settings say; the found peers arrive on the
    /// returned receiver. Must run inside a tokio runtime. A failure is
    /// logged and leaves LSD off.
    pub(crate) fn start(settings: &Settings) -> (Self, Option<mpsc::Receiver<LsdPeer>>) {
        if !settings.enable_lsd {
            return (LsdHub::disabled(), None);
        }
        let (tx, rx) = mpsc::channel(LSD_FOUND_QUEUE);
        let mut cfg = LsdConfig::default();
        let started = match settings.lsd_bind {
            Some(bind) => Lsd::spawn_unicast(cfg, bind, settings.lsd_groups.clone(), tx),
            None => {
                cfg.group_v4 = settings
                    .lsd_groups
                    .iter()
                    .copied()
                    .find(SocketAddr::is_ipv4);
                cfg.group_v6 = settings
                    .lsd_groups
                    .iter()
                    .copied()
                    .find(SocketAddr::is_ipv6);
                Lsd::spawn(cfg, tx)
            }
        };
        match started {
            Ok(lsd) => (
                LsdHub {
                    lsd: Some(lsd),
                    interval: settings.lsd_announce_interval,
                    state: Mutex::default(),
                },
                Some(rx),
            ),
            Err(e) => {
                warn!(error = %e, "cannot start local service discovery; running without it");
                (LsdHub::disabled(), None)
            }
        }
    }

    /// A public torrent is running: queues the announce of each of its
    /// `wire_hashes` (both hashes of a hybrid, in order, so they share a
    /// message) unless it was queued within the interval.
    pub(crate) fn want(&self, wire_hashes: &[[u8; 20]], now: Instant) {
        if self.lsd.is_none() {
            return;
        }
        let mut st = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        for &wire_hash in wire_hashes {
            if st
                .last
                .get(&wire_hash)
                .is_some_and(|t| now.saturating_duration_since(*t) < self.interval)
            {
                continue;
            }
            st.last.insert(wire_hash, now);
            if st.queued.insert(wire_hash) {
                st.pending.push_back(wire_hash);
            }
        }
    }

    /// The hashes to send now: as many as the pacing allows (at most
    /// [`LSD_SEND_BURST`] messages' worth, then about one message a second),
    /// oldest first; the rest stay pending.
    fn take_batch(&self, now: Instant) -> Vec<[u8; 20]> {
        let mut st = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let interval = self.interval;
        st.last
            .retain(|_, t| now.saturating_duration_since(*t) < interval);
        let cap = LSD_SEND_BURST * LSD_MSG_COST_MS;
        if let Some(prev) = st.refilled {
            let ms =
                u64::try_from(now.saturating_duration_since(prev).as_millis()).unwrap_or(u64::MAX);
            st.credit_ms = st.credit_ms.saturating_add(ms).min(cap);
        }
        st.refilled = Some(now);
        let messages = usize::try_from(st.credit_ms / LSD_MSG_COST_MS).unwrap_or(0);
        let n = st
            .pending
            .len()
            .min(messages.saturating_mul(LSD_HASHES_PER_MESSAGE));
        if n == 0 {
            return Vec::new();
        }
        let used = u64::try_from(n.div_ceil(LSD_HASHES_PER_MESSAGE)).unwrap_or(u64::MAX);
        st.credit_ms = st
            .credit_ms
            .saturating_sub(used.saturating_mul(LSD_MSG_COST_MS));
        let batch: Vec<[u8; 20]> = st.pending.drain(..n).collect();
        for h in &batch {
            st.queued.remove(h);
        }
        batch
    }

    /// Announces the queued hashes that the pacing allows, with our listen
    /// `port`.
    pub(crate) fn flush(&self, port: u16, now: Instant) {
        let Some(lsd) = &self.lsd else {
            return;
        };
        let hashes = self.take_batch(now);
        if !hashes.is_empty() {
            lsd.announce(port, &hashes);
        }
    }

    pub(crate) async fn shutdown(&self) {
        if let Some(lsd) = &self.lsd {
            lsd.shutdown().await;
        }
    }
}

/// The task that sends the queued announces every [`LSD_FLUSH`] and routes
/// found peers to the torrent with their wire hash. Ends with the session.
pub(crate) fn spawn_lsd_task(
    mut found: mpsc::Receiver<LsdPeer>,
    session: std::sync::Weak<crate::session::SessionInner>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(LSD_FLUSH);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                peer = found.recv() => {
                    let Some(peer) = peer else { return };
                    // One batch per torrent for everything already waiting.
                    let mut by_hash: HashMap<[u8; 20], Vec<SocketAddr>> = HashMap::new();
                    by_hash.entry(peer.info_hash).or_default().push(peer.addr);
                    for _ in 0..LSD_FOUND_QUEUE {
                        let Ok(p) = found.try_recv() else { break };
                        let addrs = by_hash.entry(p.info_hash).or_default();
                        if !addrs.contains(&p.addr) {
                            addrs.push(p.addr);
                        }
                    }
                    let Some(inner) = session.upgrade() else { return };
                    for (hash, addrs) in by_hash {
                        let handle = {
                            let reg = inner.registry();
                            reg.by_wire
                                .get(&hash)
                                .and_then(|ih| reg.torrents.get(ih))
                                .map(|e| e.handle.clone())
                        };
                        if let Some(h) = handle {
                            h.offer_found_peers(crate::event::PeerSource::Lsd, addrs);
                        }
                    }
                }
                _ = tick.tick() => {
                    let Some(inner) = session.upgrade() else { return };
                    let shared = &inner.shared;
                    shared.lsd.flush(shared.listen_port, Instant::now());
                }
            }
        }
    })
}

/// Capacity of the channel the port mapper reports on.
const MAP_EVENT_QUEUE: usize = 64;

/// The session's external address as the router and peers report it (the
/// DHT keeps its own votes).
#[derive(Debug)]
pub(crate) struct ExternalIp {
    events: broadcast::Sender<Event>,
    state: Mutex<ExternalState>,
}

#[derive(Debug, Default)]
struct ExternalState {
    /// The router's answer (UPnP, PCP or NAT-PMP).
    mapped: Option<IpAddr>,
    /// Peers' `yourip` (BEP 10), one vote per peer IP.
    votes: ExternalIpVotes,
}

impl ExternalIp {
    pub(crate) fn new(events: broadcast::Sender<Event>) -> Self {
        ExternalIp {
            events,
            state: Mutex::new(ExternalState::default()),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, ExternalState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// What the router reported last.
    pub(crate) fn mapped(&self) -> Option<IpAddr> {
        self.state().mapped
    }

    /// What a majority of peers reported, once three agree.
    pub(crate) fn peers_estimate(&self) -> Option<IpAddr> {
        self.state().votes.current()
    }

    /// The router reported `ip`.
    pub(crate) fn set_mapped(&self, ip: IpAddr) {
        let changed = self.state().mapped.replace(ip) != Some(ip);
        if changed {
            self.emit(ip, ExternalIpSource::PortMapping);
        }
    }

    /// The peer at `reporter` says we are `ip` (its `yourip`). Only a peer
    /// of the same address family can see that address.
    pub(crate) fn peer_vote(&self, reporter: IpAddr, ip: IpAddr) {
        if reporter.to_canonical().is_ipv4() != ip.to_canonical().is_ipv4() {
            return;
        }
        let new = self.state().votes.vote(reporter, ip);
        if let Some(ip) = new {
            self.emit(ip, ExternalIpSource::Peers);
        }
    }

    fn emit(&self, ip: IpAddr, source: ExternalIpSource) {
        let _ = self.events.send(Event::ExternalIpChanged { ip, source });
    }
}

/// Starts port mapping as the settings say, for the IPv4 listen addresses
/// in `local_addrs` (TCP and UDP share each port). Loopback addresses are
/// mapped only when [`Settings::nat_gateway`] is a loopback address (the
/// tests' fake gateway). `None` with UPnP and NAT-PMP both off.
pub(crate) fn start_port_mapping(
    settings: &Settings,
    local_addrs: &[SocketAddr],
) -> Option<(PortMapper, mpsc::Receiver<MapEvent>)> {
    if !(settings.enable_upnp || settings.enable_natpmp) {
        return None;
    }
    let mut cfg = PortMapConfig::default();
    cfg.upnp = settings.enable_upnp;
    cfg.natpmp = settings.enable_natpmp;
    cfg.gateway = settings.nat_gateway;
    cfg.ssdp = settings.upnp_ssdp_addr;
    cfg.natpmp_port = settings.natpmp_port;
    cfg.retry_base = settings.nat_retry_base;
    cfg.retry_max = cfg.retry_max.max(cfg.retry_base);
    let loopback_gateway = settings.nat_gateway.is_some_and(|g| g.is_loopback());
    let (tx, rx) = mpsc::channel(MAP_EVENT_QUEUE);
    let mapper = PortMapper::spawn(cfg, tx);
    let mut ports = HashSet::new();
    for a in local_addrs {
        let IpAddr::V4(ip) = a.ip() else { continue };
        if ip.is_loopback() && !loopback_gateway {
            continue;
        }
        if ports.insert(a.port()) {
            mapper.map(MapProtocol::Tcp, a.port());
            mapper.map(MapProtocol::Udp, a.port());
        }
    }
    Some((mapper, rx))
}

/// Turns the port mapper's reports into session events and external
/// address updates. Ends with the mapper or the session.
pub(crate) fn spawn_nat_task(
    mut rx: mpsc::Receiver<MapEvent>,
    shared: std::sync::Weak<crate::session::Shared>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            let Some(shared) = shared.upgrade() else {
                return;
            };
            match ev {
                MapEvent::Mapped {
                    protocol,
                    method,
                    external,
                    ..
                } => shared.emit(Event::PortMapped {
                    protocol,
                    method,
                    external,
                }),
                MapEvent::Failed {
                    protocol,
                    method,
                    message,
                } => shared.emit(Event::PortMappingFailed {
                    protocol,
                    method,
                    message,
                }),
                MapEvent::ExternalIp { ip, .. } => shared.external_ip.set_mapped(IpAddr::V4(ip)),
                _ => {}
            }
        }
    })
}

/// Decides which sources' uTP SYNs a socket answers.
pub(crate) type AcceptFilter = Arc<dyn Fn(SocketAddr) -> bool + Send + Sync>;

/// NAT emulation's mappings: when each address last got a uTP datagram
/// from one socket. At most [`MAX_RECENT_SENDS`] addresses.
#[derive(Debug, Default)]
pub(crate) struct RecentSends {
    inner: Mutex<RecentInner>,
}

#[derive(Debug, Default)]
struct RecentInner {
    last: HashMap<SocketAddr, Instant>,
    /// Addresses with the send time they were queued at, oldest first. A
    /// refreshed address keeps its old slot; eviction re-queues it at its
    /// newer time instead of forgetting it.
    order: VecDeque<(SocketAddr, Instant)>,
}

impl RecentSends {
    /// A datagram went to `to` at `now`.
    fn record(&self, to: SocketAddr, now: Instant) {
        let to = canonical(to);
        let mut g = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if g.last.insert(to, now).is_some() {
            return;
        }
        g.order.push_back((to, now));
        while g.last.len() > MAX_RECENT_SENDS {
            let Some((a, queued)) = g.order.pop_front() else {
                break;
            };
            match g.last.get(&a).copied() {
                Some(t) if t != queued => g.order.push_back((a, t)),
                _ => {
                    g.last.remove(&a);
                }
            }
        }
    }

    /// The emulated NAT lets a SYN from `from` in at `now`.
    fn allows(&self, from: SocketAddr, now: Instant) -> bool {
        let g = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        g.last
            .get(&canonical(from))
            .is_some_and(|t| now.saturating_duration_since(*t) < NAT_MAPPING_LIFETIME)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .last
            .len()
    }
}

/// `::ffff:a.b.c.d` and `a.b.c.d` are one endpoint.
fn canonical(a: SocketAddr) -> SocketAddr {
    SocketAddr::new(a.ip().to_canonical(), a.port())
}

/// A uTP socket's sender that records every destination for NAT emulation.
#[derive(Debug)]
struct RecordingSender {
    inner: Arc<dyn DatagramSender>,
    recent: Arc<RecentSends>,
}

impl DatagramSender for RecordingSender {
    fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr()
    }

    fn send_to(&self, data: &[u8], to: SocketAddr) -> io::Result<usize> {
        self.recent.record(to, Instant::now());
        self.inner.send_to(data, to)
    }
}

/// The accept filter of one uTP socket: `None` (everyone) with
/// `enable_incoming_utp` and no NAT emulation. Otherwise one closure: with
/// incoming uTP off it rejects every SYN, so it is never acked and the
/// dialler's connect times out (and falls back to TCP) instead of reaching
/// a socket nobody accepts on; with NAT emulation (`recent`) it accepts only
/// sources the socket sent to within [`NAT_MAPPING_LIFETIME`].
pub(crate) fn utp_accept_filter(
    settings: &Settings,
    recent: Option<Arc<RecentSends>>,
) -> Option<AcceptFilter> {
    let incoming = settings.enable_incoming_utp;
    if incoming && recent.is_none() {
        return None;
    }
    Some(Arc::new(move |from: SocketAddr| {
        incoming
            && recent
                .as_ref()
                .is_none_or(|r| r.allows(from, Instant::now()))
    }))
}

/// One uTP socket per shared UDP socket, in the same order, registered as
/// its uTP handler and given the session's accept filter (with NAT
/// emulation, each socket its own mappings, fed by its sends). Must run
/// inside a tokio runtime.
pub(crate) fn utp_sockets(settings: &Settings, udp: &[SharedUdpSocket]) -> Vec<UtpSocket> {
    let mut cfg = UtpConfig::default();
    cfg.connect_timeout = settings.utp_connect_timeout;
    udp.iter()
        .map(|u| {
            let recent = settings
                .nat_emulation
                .then(|| Arc::new(RecentSends::default()));
            let sender: Arc<dyn DatagramSender> = match &recent {
                Some(r) => Arc::new(RecordingSender {
                    inner: u.sender(),
                    recent: Arc::clone(r),
                }),
                None => u.sender(),
            };
            let socket = UtpSocket::new(sender, cfg.clone());
            socket.set_accept_filter(utp_accept_filter(settings, recent));
            u.set_handler(PacketKind::Utp, socket.handler());
            socket
        })
        .collect()
}

/// The uTP socket to dial `peer` from: the first one of the peer's address
/// family (an IPv4-mapped IPv6 peer counts as IPv4), as for the DHT and
/// UDP trackers; `None` without one.
pub(crate) fn utp_for<'a>(sockets: &'a [UtpSocket], peer: &SocketAddr) -> Option<&'a UtpSocket> {
    let v4 = peer.ip().to_canonical().is_ipv4();
    sockets.iter().find(|s| s.local_addr().is_ipv4() == v4)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sa(s: &str) -> SocketAddr {
        s.parse().expect("address")
    }

    fn addr(i: u32) -> SocketAddr {
        SocketAddr::from((std::net::Ipv4Addr::from(0x0a00_0000 + i), 6881))
    }

    #[test]
    fn nat_emulation_lets_in_recent_destinations_only() {
        let r = RecentSends::default();
        let t0 = Instant::now();
        r.record(sa("10.0.0.1:6881"), t0);
        assert!(r.allows(sa("10.0.0.1:6881"), t0));
        // The same endpoint written as IPv4-mapped IPv6.
        assert!(r.allows(sa("[::ffff:10.0.0.1]:6881"), t0));
        // Endpoint-dependent: another port of the same host stays out.
        assert!(!r.allows(sa("10.0.0.1:6882"), t0));
        assert!(!r.allows(sa("10.0.0.1:6881"), t0 + NAT_MAPPING_LIFETIME));
    }

    #[test]
    fn recent_sends_are_bounded_and_keep_refreshed_ones() {
        let r = RecentSends::default();
        let t0 = Instant::now();
        for i in 0..MAX_RECENT_SENDS as u32 {
            r.record(addr(i), t0);
        }
        // The oldest is refreshed: the next overflow forgets addr(1).
        let t1 = t0 + Duration::from_secs(1);
        r.record(addr(0), t1);
        r.record(addr(5000), t1);
        assert_eq!(r.len(), MAX_RECENT_SENDS);
        assert!(r.allows(addr(0), t1));
        assert!(!r.allows(addr(1), t1));
        assert!(r.allows(addr(5000), t1));
        for i in 0..(3 * MAX_RECENT_SENDS as u32) {
            r.record(addr(10_000 + i), t1);
        }
        assert_eq!(r.len(), MAX_RECENT_SENDS);
        let g = r.inner.lock().expect("lock");
        assert_eq!(g.order.len(), g.last.len());
    }

    #[test]
    fn accept_filter_composes_both_rules() {
        let mut s = Settings::default();
        assert!(utp_accept_filter(&s, None).is_none());
        let recent = Arc::new(RecentSends::default());
        recent.record(sa("10.0.0.1:1"), Instant::now());
        let f = utp_accept_filter(&s, Some(Arc::clone(&recent))).expect("filter");
        assert!(f(sa("10.0.0.1:1")) && !f(sa("10.0.0.2:1")));
        s.enable_incoming_utp = false;
        let f = utp_accept_filter(&s, Some(recent)).expect("filter");
        assert!(!f(sa("10.0.0.1:1")), "incoming uTP off wins");
        let f = utp_accept_filter(&s, None).expect("filter");
        assert!(!f(sa("10.0.0.1:1")));
    }

    fn hub(interval: Duration) -> LsdHub {
        let (tx, _rx) = mpsc::channel(1);
        let (lsd, _handler) = Lsd::spawn_on(LsdConfig::default(), Vec::new(), tx);
        LsdHub {
            lsd: Some(lsd),
            interval,
            state: Mutex::default(),
        }
    }

    fn h(i: u32) -> [u8; 20] {
        let mut x = [0u8; 20];
        x[..4].copy_from_slice(&i.to_be_bytes());
        x
    }

    #[test]
    fn lsd_sends_are_paced_to_the_receivers_burst() {
        let hub = hub(Duration::from_secs(300));
        let t0 = Instant::now();
        for i in 0..200 {
            hub.want(&[h(i)], t0);
        }
        // A burst of 5 messages (80 hashes), then nothing until credit refills.
        assert_eq!(hub.take_batch(t0).len(), 5 * LSD_HASHES_PER_MESSAGE);
        assert!(hub.take_batch(t0 + Duration::from_millis(250)).is_empty());
        let one = hub.take_batch(t0 + Duration::from_secs(1));
        assert_eq!(one.len(), LSD_HASHES_PER_MESSAGE);
        assert_eq!(one.first(), Some(&h(80)), "oldest first");
        // Credit is capped at the burst however long we wait; everything
        // arrives eventually, each hash once.
        let mut seen = 96;
        let mut now = t0 + Duration::from_secs(100);
        while seen < 200 {
            let b = hub.take_batch(now);
            assert!(b.len() <= 5 * LSD_HASHES_PER_MESSAGE);
            assert!(!b.is_empty());
            seen += b.len();
            now += Duration::from_secs(100);
        }
        assert_eq!(seen, 200);
        assert!(hub.take_batch(now).is_empty());
    }

    #[test]
    fn every_hash_of_a_torrent_goes_out_together_in_order() {
        let hub = hub(Duration::from_secs(300));
        let t0 = Instant::now();
        hub.want(&[h(1), h(2)], t0);
        assert_eq!(hub.take_batch(t0), vec![h(1), h(2)]);
        hub.want(&[h(1), h(2)], t0 + Duration::from_secs(1));
        assert!(hub.take_batch(t0 + Duration::from_secs(1)).is_empty());
    }

    #[test]
    fn a_torrent_is_announced_again_after_the_interval_only() {
        let interval = Duration::from_secs(300);
        let hub = hub(interval);
        let t0 = Instant::now();
        hub.want(&[h(1)], t0);
        assert_eq!(hub.take_batch(t0), vec![h(1)]);
        let mid = t0 + interval / 2;
        hub.want(&[h(1)], mid);
        assert!(hub.take_batch(mid).is_empty(), "within the interval");
        let later = t0 + interval + Duration::from_secs(1);
        hub.want(&[h(1)], later);
        assert_eq!(hub.take_batch(later), vec![h(1)], "after the interval");
        hub.want(&[h(1)], later + Duration::from_secs(1));
        assert!(hub.take_batch(later + Duration::from_secs(1)).is_empty());
    }
}
