//! Binding the listen addresses: one TCP peer listener per address and, on
//! the same port, one UDP socket shared by uTP, the DHT and UDP trackers.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

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
    /// Listening only with `enable_incoming_tcp`; otherwise the socket is
    /// bound (holding the port) but not listening, so TCP dials to it are
    /// refused at once.
    pub tcp: TcpListener,
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
        match bind_one(addr, settings.enable_incoming_tcp) {
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

/// Binds TCP on `addr` (listening when `listen`), then UDP on the TCP's
/// port. With port 0 a UDP failure retries both on a fresh port, up to
/// [`BIND_ATTEMPTS`] times; otherwise the endpoint keeps TCP only and the
/// UDP error comes back.
fn bind_one(addr: SocketAddr, listen: bool) -> io::Result<(Endpoint, Option<io::Error>)> {
    let mut attempt = 1;
    loop {
        let tcp = bind_tcp(addr, listen)?;
        let local = tcp.local_addr()?;
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
/// socket is only bound. Must run inside a tokio runtime.
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

/// The accept filter every uTP socket of the session gets: `None` (everyone)
/// with `enable_incoming_utp`, otherwise one that rejects every SYN, so it
/// is never acked and the dialler's connect times out (and falls back to
/// TCP) instead of reaching a socket nobody accepts on. Further checks join
/// this one closure.
pub(crate) fn utp_accept_filter(settings: &Settings) -> Option<AcceptFilter> {
    if settings.enable_incoming_utp {
        return None;
    }
    Some(Arc::new(|_from: SocketAddr| false))
}

/// One uTP socket per shared UDP socket, in the same order, registered as
/// its uTP handler and given the session's accept filter. Must run inside a
/// tokio runtime.
pub(crate) fn utp_sockets(settings: &Settings, udp: &[SharedUdpSocket]) -> Vec<UtpSocket> {
    let mut cfg = UtpConfig::default();
    cfg.connect_timeout = settings.utp_connect_timeout;
    let filter = utp_accept_filter(settings);
    udp.iter()
        .map(|u| {
            let socket = UtpSocket::new(u.sender(), cfg.clone());
            socket.set_accept_filter(filter.clone());
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
