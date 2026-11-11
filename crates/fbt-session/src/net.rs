//! Binding the listen addresses: one TCP peer listener per address and, on
//! the same port, one UDP socket shared by uTP, the DHT and UDP trackers.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use fbt_core::udp::DatagramSender;
use fbt_net::demux::{PacketKind, SharedUdpSocket};
use fbt_utp::{UtpConfig, UtpSocket};
use tokio::net::TcpListener;
use tracing::warn;

use crate::error::SessionError;
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
/// Both are reported as [`ListenError`]s. Fails with the first error when
/// nothing binds.
pub(crate) async fn bind_all(
    settings: &Settings,
) -> Result<(Vec<Endpoint>, Vec<ListenError>), SessionError> {
    let mut endpoints = Vec::new();
    let mut errors = Vec::new();
    let mut first: Option<io::Error> = None;
    for &addr in &settings.listen_addrs {
        // NAT emulation refuses incoming TCP as `enable_incoming_tcp` off does.
        let listen = settings.enable_incoming_tcp && !settings.nat_emulation;
        match bind_one(addr, listen) {
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
}
