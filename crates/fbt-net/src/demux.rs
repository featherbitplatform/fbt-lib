//! One UDP socket shared by uTP, the DHT and UDP trackers.
//!
//! Incoming datagrams are routed to a handler by packet shape (see
//! [`classify`]); the three shapes are disjoint. Datagrams of an unknown
//! shape, or of a kind with no handler, are dropped and counted.

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use fbt_core::udp::{DatagramHandler, DatagramSender};
use tokio::net::UdpSocket;
use tokio::sync::Notify;

/// The receive buffer: the largest possible UDP payload.
pub const MAX_DATAGRAM: usize = 65_536;

/// Consecutive `recv_from` errors after which the loop sleeps [`ERROR_PAUSE`].
const ERROR_BURST: u32 = 16;
const ERROR_PAUSE: Duration = Duration::from_millis(50);

/// The protocol a datagram belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PacketKind {
    /// A uTP packet (BEP 29).
    Utp,
    /// A KRPC (DHT) message, a bencoded dictionary.
    Dht,
    /// A UDP tracker response (BEP 15).
    Tracker,
}

/// Classifies a datagram by shape; `None` means it matches no protocol.
pub fn classify(data: &[u8]) -> Option<PacketKind> {
    let first = *data.first()?;
    if data.len() >= 8 && data.get(..3) == Some(&[0, 0, 0]) && data.get(3).is_some_and(|a| *a <= 3)
    {
        return Some(PacketKind::Tracker);
    }
    if first == b'd' {
        return Some(PacketKind::Dht);
    }
    if data.len() >= 20 && first & 0x0f == 1 && first >> 4 <= 4 {
        return Some(PacketKind::Utp);
    }
    None
}

/// Counts of datagrams dispatched to a handler, per kind, since the socket
/// was bound.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct DemuxStats {
    /// Datagrams dispatched to the uTP handler.
    pub utp: u64,
    /// Datagrams dispatched to the DHT handler.
    pub dht: u64,
    /// Datagrams dispatched to the tracker handler.
    pub tracker: u64,
    /// Datagrams of unknown shape or with no handler for their kind.
    pub dropped: u64,
}

#[derive(Debug, Default)]
struct Handlers {
    utp: Option<Arc<dyn DatagramHandler>>,
    dht: Option<Arc<dyn DatagramHandler>>,
    tracker: Option<Arc<dyn DatagramHandler>>,
}

#[derive(Debug, Default)]
struct Counters {
    utp: AtomicU64,
    dht: AtomicU64,
    tracker: AtomicU64,
    dropped: AtomicU64,
}

/// The non-blocking send clone; `None` once the socket is closed.
type SendHalf = Arc<Mutex<Option<std::net::UdpSocket>>>;

#[derive(Debug)]
struct Inner {
    send: SendHalf,
    local: SocketAddr,
    handlers: Mutex<Handlers>,
    counters: Counters,
    closed: AtomicBool,
    stop: Arc<Notify>,
}

impl Inner {
    fn stop(&self) {
        self.closed.store(true, Ordering::SeqCst);
        // drop the send clone so the port is released once the receive task
        // has dropped its own handle, even while sender() Arcs live on
        self.send
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        // notify_one stores a permit, so a task not yet waiting still sees it
        self.stop.notify_one();
    }

    fn dispatch(&self, from: SocketAddr, data: &[u8]) {
        let (handler, counter) = match classify(data) {
            Some(PacketKind::Utp) => (self.handler(PacketKind::Utp), &self.counters.utp),
            Some(PacketKind::Dht) => (self.handler(PacketKind::Dht), &self.counters.dht),
            Some(PacketKind::Tracker) => {
                (self.handler(PacketKind::Tracker), &self.counters.tracker)
            }
            None => (None, &self.counters.dropped),
        };
        match handler {
            Some(h) => {
                counter.fetch_add(1, Ordering::Relaxed);
                h.handle(from, data);
            }
            None => {
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn handler(&self, kind: PacketKind) -> Option<Arc<dyn DatagramHandler>> {
        let h = self.handlers.lock().unwrap_or_else(PoisonError::into_inner);
        match kind {
            PacketKind::Utp => h.utp.clone(),
            PacketKind::Dht => h.dht.clone(),
            PacketKind::Tracker => h.tracker.clone(),
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.stop();
    }
}

/// A UDP socket shared by uTP, the DHT and UDP trackers. Clones share it.
#[derive(Debug, Clone)]
pub struct SharedUdpSocket {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Sender {
    socket: SendHalf,
    local: SocketAddr,
}

impl DatagramSender for Sender {
    fn local_addr(&self) -> SocketAddr {
        self.local
    }

    fn send_to(&self, data: &[u8], to: SocketAddr) -> io::Result<usize> {
        let guard = self.socket.lock().unwrap_or_else(PoisonError::into_inner);
        match guard.as_ref() {
            Some(s) => s.send_to(data, to),
            None => Err(io::ErrorKind::NotConnected.into()),
        }
    }
}

impl SharedUdpSocket {
    /// Binds `addr` and starts the receive task. Must run inside a tokio
    /// runtime. An IPv6 address binds IPv6-only, so an IPv4 socket can share
    /// its port.
    pub fn bind(addr: SocketAddr) -> io::Result<SharedUdpSocket> {
        use socket2::{Domain, Protocol, Socket, Type};
        let domain = if addr.is_ipv6() {
            Domain::IPV6
        } else {
            Domain::IPV4
        };
        let s = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
        if addr.is_ipv6() {
            s.set_only_v6(true)?;
        }
        s.set_nonblocking(true)?;
        s.bind(&addr.into())?;
        let std_sock: std::net::UdpSocket = s.into();
        // sends go straight to a non-blocking clone: tokio's try_send_to
        // fails until the first readiness event, which a fresh socket lacks
        let send = std_sock.try_clone()?;
        send.set_nonblocking(true)?;
        let socket = Arc::new(UdpSocket::from_std(std_sock)?);
        let local = socket.local_addr()?;
        let inner = Arc::new(Inner {
            send: Arc::new(Mutex::new(Some(send))),
            local,
            handlers: Mutex::new(Handlers::default()),
            counters: Counters::default(),
            closed: AtomicBool::new(false),
            stop: Arc::new(Notify::new()),
        });
        tokio::spawn(receive_loop(
            socket,
            Arc::downgrade(&inner),
            Arc::clone(&inner.stop),
        ));
        Ok(SharedUdpSocket { inner })
    }

    /// The bound address (with the real port when bound to port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local
    }

    /// A sender on this socket, so replies leave from the shared port.
    pub fn sender(&self) -> Arc<dyn DatagramSender> {
        Arc::new(Sender {
            socket: Arc::clone(&self.inner.send),
            local: self.inner.local,
        })
    }

    /// Routes datagrams of `kind` to `h`, replacing any previous handler.
    pub fn set_handler(&self, kind: PacketKind, h: Arc<dyn DatagramHandler>) {
        let mut hs = self
            .inner
            .handlers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match kind {
            PacketKind::Utp => hs.utp = Some(h),
            PacketKind::Dht => hs.dht = Some(h),
            PacketKind::Tracker => hs.tracker = Some(h),
        }
    }

    /// Datagram counts so far.
    pub fn stats(&self) -> DemuxStats {
        let c = &self.inner.counters;
        DemuxStats {
            utp: c.utp.load(Ordering::Relaxed),
            dht: c.dht.load(Ordering::Relaxed),
            tracker: c.tracker.load(Ordering::Relaxed),
            dropped: c.dropped.load(Ordering::Relaxed),
        }
    }

    /// Closes the socket; idempotent. Later sends fail with `NotConnected`
    /// and the port is released once the receive task has exited. The task also ends when the last
    /// clone drops.
    pub fn close(&self) {
        self.inner.stop();
    }
}

async fn receive_loop(socket: Arc<UdpSocket>, inner: Weak<Inner>, stop: Arc<Notify>) {
    let mut buf = vec![0u8; MAX_DATAGRAM];
    let mut errors = 0u32;
    loop {
        let res = tokio::select! {
            () = stop.notified() => break,
            r = socket.recv_from(&mut buf) => r,
        };
        match res {
            Ok((n, from)) => {
                errors = 0;
                let Some(inner) = inner.upgrade() else { break };
                if inner.closed.load(Ordering::SeqCst) {
                    break;
                }
                if let Some(data) = buf.get(..n) {
                    inner.dispatch(from, data);
                }
            }
            Err(e) => {
                tracing::debug!("udp recv_from: {e}");
                // Windows reports an ICMP unreachable for an earlier send as
                // an error on the next receive. That says nothing about this
                // socket, and a burst of them (replies to spoofed or dead
                // peers) must not stall the loop, so only other errors count
                // toward the pause.
                if !is_icmp_echo(&e) {
                    errors = errors.saturating_add(1);
                }
                if errors >= ERROR_BURST {
                    tokio::time::sleep(ERROR_PAUSE).await;
                }
            }
        }
    }
}

/// An error that reports an ICMP message about an earlier send (Windows
/// surfaces these on `recv_from`), not a fault of the socket.
fn is_icmp_echo(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::HostUnreachable
            | std::io::ErrorKind::NetworkUnreachable
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn icmp_echoes_do_not_count_as_socket_errors() {
        use std::io::{Error, ErrorKind};
        assert!(is_icmp_echo(&Error::from(ErrorKind::ConnectionReset)));
        assert!(is_icmp_echo(&Error::from(ErrorKind::HostUnreachable)));
        assert!(!is_icmp_echo(&Error::from(ErrorKind::PermissionDenied)));
        assert!(!is_icmp_echo(&Error::from(ErrorKind::Other)));
    }

    #[test]
    fn classify_by_shape() {
        let mut syn = [0u8; 20];
        syn[0] = 0x41;
        assert_eq!(classify(&syn), Some(PacketKind::Utp));
        assert_eq!(
            classify(b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe"),
            Some(PacketKind::Dht)
        );
        assert_eq!(
            classify(&[0, 0, 0, 1, 0, 0, 0, 7]),
            Some(PacketKind::Tracker)
        );
        assert_eq!(classify(&[0, 0, 0, 9, 0, 0, 0, 0, 0, 0]), None);
        assert_eq!(classify(b"x"), None);
        assert_eq!(classify(&[]), None);
        assert_eq!(classify(&syn[..19]), None);
    }

    proptest! {
        #[test]
        fn classify_never_panics(data in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _ = classify(&data);
        }
    }
}
