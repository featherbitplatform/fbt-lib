//! BEP 14 local service discovery: announcing our torrents on the LAN's
//! multicast groups and learning the peers that announce theirs.
//!
//! Every datagram comes from an untrusted host: the codec is bounded
//! ([`MAX_LSD_MESSAGE`], [`MAX_LSD_DECODED_HASHES`], 64-byte values) and the
//! inbound path is rate-limited per source IP.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use fbt_core::rate::IpRateLimiter;
use fbt_core::udp::{DatagramHandler, DatagramSender};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, trace};

/// The IPv4 multicast group and port of BEP 14.
pub const LSD_V4: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(239, 192, 152, 143)), 6771);
/// The IPv6 multicast group and port of BEP 14 (`ff15::efc0:988f`).
pub const LSD_V6: SocketAddr = SocketAddr::new(
    IpAddr::V6(Ipv6Addr::new(0xff15, 0, 0, 0, 0, 0, 0xefc0, 0x988f)),
    6771,
);
/// The largest message decoded or sent, in bytes.
pub const MAX_LSD_MESSAGE: usize = 1400;
/// Info hashes announced in one message.
pub const LSD_HASHES_PER_MESSAGE: usize = 16;
/// Info hashes kept from one decoded message; the rest are dropped.
pub const MAX_LSD_DECODED_HASHES: usize = 64;
/// Longest header value kept; longer ones are ignored.
const MAX_VALUE: usize = 64;
/// IPs the inbound limiter tracks.
const LSD_MAX_IPS: usize = 1024;
/// Inbound messages per IP allowed in a burst.
const LSD_BURST: u32 = 5;
/// Receive buffer: more than a message may hold, so oversize ones are seen
/// as such rather than truncated into validity.
const RECV_BUF: usize = 2048;
const REQUEST_LINE: &str = "BT-SEARCH * HTTP/1.1";

/// Why a datagram is not a valid LSD message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LsdError {
    /// Longer than [`MAX_LSD_MESSAGE`].
    #[error("message over {MAX_LSD_MESSAGE} bytes")]
    TooLong,
    /// Not text, or not a `BT-SEARCH * HTTP/1.1` request.
    #[error("not a BT-SEARCH request")]
    BadRequest,
    /// The `Port` header is missing, not a number or outside `1..=65535`.
    #[error("missing or invalid port")]
    BadPort,
    /// No valid `Infohash` header.
    #[error("no valid info hash")]
    NoInfoHash,
}

/// A BEP 14 `BT-SEARCH` message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LsdMessage {
    /// The sender's peer listen port.
    pub port: u16,
    /// The torrents announced.
    pub info_hashes: Vec<[u8; 20]>,
    /// The sender's cookie, to recognise its own looped-back messages.
    pub cookie: Option<String>,
}

impl LsdMessage {
    /// The wire form, addressed to `host` (the group, in the `Host` header).
    pub fn encode(&self, host: SocketAddr) -> Vec<u8> {
        use std::fmt::Write;
        let mut s = format!("{REQUEST_LINE}\r\nHost: {host}\r\nPort: {}\r\n", self.port);
        for h in &self.info_hashes {
            s.push_str("Infohash: ");
            for b in h {
                let _ = write!(s, "{b:02x}");
            }
            s.push_str("\r\n");
        }
        if let Some(c) = &self.cookie {
            let _ = write!(s, "cookie: {c}\r\n");
        }
        s.push_str("\r\n\r\n");
        s.into_bytes()
    }

    /// Parses a datagram.
    pub fn decode(data: &[u8]) -> Result<LsdMessage, LsdError> {
        Self::decode_limited(data, MAX_LSD_MESSAGE)
    }

    /// [`decode`](Self::decode) with the size limit as a parameter, so the
    /// hash cap can be tested: 64 hashes do not fit in 1400 bytes.
    fn decode_limited(data: &[u8], max_len: usize) -> Result<LsdMessage, LsdError> {
        if data.len() > max_len {
            return Err(LsdError::TooLong);
        }
        let text = std::str::from_utf8(data).map_err(|_| LsdError::BadRequest)?;
        let mut lines = text.split("\r\n");
        if lines.next() != Some(REQUEST_LINE) {
            return Err(LsdError::BadRequest);
        }
        let mut port = None;
        let mut info_hashes = Vec::new();
        let mut cookie = None;
        for line in lines {
            if line.is_empty() {
                break;
            }
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            let value = value.trim();
            if value.len() > MAX_VALUE {
                continue;
            }
            let name = name.trim();
            if name.eq_ignore_ascii_case("port") {
                port = value.parse::<u16>().ok().filter(|p| *p != 0);
            } else if name.eq_ignore_ascii_case("infohash") {
                if info_hashes.len() < MAX_LSD_DECODED_HASHES
                    && let Some(h) = parse_hash(value)
                    && !info_hashes.contains(&h)
                {
                    info_hashes.push(h);
                }
            } else if name.eq_ignore_ascii_case("cookie") {
                cookie = Some(value.to_owned());
            }
        }
        let port = port.ok_or(LsdError::BadPort)?;
        if info_hashes.is_empty() {
            return Err(LsdError::NoInfoHash);
        }
        Ok(LsdMessage {
            port,
            info_hashes,
            cookie,
        })
    }
}

fn parse_hash(s: &str) -> Option<[u8; 20]> {
    let b = s.as_bytes();
    if b.len() != 40 {
        return None;
    }
    let mut out = [0u8; 20];
    let (pairs, _) = b.as_chunks::<2>();
    for (o, [h, l]) in out.iter_mut().zip(pairs) {
        let hi = hex_val(*h)?;
        let lo = hex_val(*l)?;
        *o = (hi << 4) | lo;
    }
    Some(out)
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Settings of an [`Lsd`] instance.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct LsdConfig {
    /// The IPv4 group to announce on and listen to; `None` skips IPv4.
    pub group_v4: Option<SocketAddr>,
    /// The IPv6 group; `None` skips IPv6.
    pub group_v6: Option<SocketAddr>,
    /// Identifies our own messages when the group loops them back (random
    /// 8 hex characters by default).
    pub cookie: String,
    /// Inbound messages per second and source IP (bursts of 5).
    pub rate_per_ip: u32,
    /// IPv4 interface to join and send on; unspecified (the default) lets
    /// the OS choose. Internal tunable (tests pin loopback); not stable API.
    #[doc(hidden)]
    pub interface_v4: Ipv4Addr,
    /// Multicast TTL of our sends; `None` keeps the OS default (1, the
    /// local link). Internal tunable (tests use 0); not stable API.
    #[doc(hidden)]
    pub multicast_ttl: Option<u32>,
}

impl Default for LsdConfig {
    fn default() -> Self {
        LsdConfig {
            group_v4: Some(LSD_V4),
            group_v6: Some(LSD_V6),
            cookie: format!("{:08x}", rand::random::<u32>()),
            rate_per_ip: 1,
            interface_v4: Ipv4Addr::UNSPECIFIED,
            multicast_ttl: None,
        }
    }
}

/// A peer learned from an announce: the sender's IP with the port it named.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LsdPeer {
    /// The torrent announced.
    pub info_hash: [u8; 20],
    /// Where the peer listens.
    pub addr: SocketAddr,
}

type Destinations = Vec<(Arc<dyn DatagramSender>, SocketAddr)>;

struct Core {
    cookie: String,
    /// Emptied at shutdown, which drops the send halves and frees the ports.
    destinations: Mutex<Destinations>,
    found: mpsc::Sender<LsdPeer>,
    limiter: Mutex<IpRateLimiter>,
}

impl std::fmt::Debug for Core {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Core").finish_non_exhaustive()
    }
}

impl Core {
    fn new(cfg: &LsdConfig, destinations: Destinations, found: mpsc::Sender<LsdPeer>) -> Arc<Core> {
        Arc::new(Core {
            cookie: cfg.cookie.clone(),
            destinations: Mutex::new(destinations),
            found,
            limiter: Mutex::new(IpRateLimiter::new(cfg.rate_per_ip, LSD_BURST, LSD_MAX_IPS)),
        })
    }
}

impl DatagramHandler for Core {
    fn handle(&self, from: SocketAddr, data: &[u8]) {
        let allowed = self
            .limiter
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .allow(from.ip().to_canonical(), Instant::now());
        if !allowed {
            trace!(%from, "LSD message over the per-IP rate");
            return;
        }
        let Ok(msg) = LsdMessage::decode(data) else {
            return;
        };
        if msg.cookie.as_deref() == Some(self.cookie.as_str()) {
            return;
        }
        let addr = peer_addr(from, msg.port);
        for info_hash in msg.info_hashes {
            if self.found.try_send(LsdPeer { info_hash, addr }).is_err() {
                break;
            }
        }
    }
}

/// The address a peer announcing `port` from `from` listens on. An IPv4-mapped
/// source becomes plain IPv4; any other address keeps its IPv6 scope id,
/// which a link-local peer cannot be dialled without.
fn peer_addr(from: SocketAddr, port: u16) -> SocketAddr {
    match from.ip().to_canonical() {
        IpAddr::V4(v4) => SocketAddr::new(IpAddr::V4(v4), port),
        IpAddr::V6(_) => {
            let mut a = from;
            a.set_port(port);
            a
        }
    }
}

/// Aborts the receive tasks when the last [`Lsd`] clone goes.
#[derive(Debug, Default)]
struct Tasks(Mutex<Vec<JoinHandle<()>>>);

impl Tasks {
    fn abort(&self) -> Vec<JoinHandle<()>> {
        let tasks = std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner));
        for t in &tasks {
            t.abort();
        }
        tasks
    }
}

impl Drop for Tasks {
    fn drop(&mut self) {
        self.abort();
    }
}

/// BEP 14 local service discovery. Cheap to clone; the receive tasks stop
/// at [`shutdown`](Self::shutdown) or when the last clone is dropped.
#[derive(Debug, Clone)]
pub struct Lsd {
    core: Arc<Core>,
    tasks: Arc<Tasks>,
}

/// A real socket's non-blocking send half.
#[derive(Debug)]
struct UdpSender {
    socket: std::net::UdpSocket,
    local: SocketAddr,
}

impl DatagramSender for UdpSender {
    fn local_addr(&self) -> SocketAddr {
        self.local
    }

    fn send_to(&self, data: &[u8], to: SocketAddr) -> io::Result<usize> {
        self.socket.send_to(data, to)
    }
}

impl Lsd {
    /// Joins the configured multicast groups on the real network (IPv4 on
    /// `0.0.0.0:<group port>`, IPv6 when it can; that is skipped with a
    /// debug log) and starts listening. Must run inside a tokio runtime.
    ///
    /// IPv4 joins and sends on the OS default interface (or
    /// [`LsdConfig::interface_v4`]) and IPv6 uses interface 0, so a
    /// multi-homed host reaches the peers of one interface only.
    ///
    /// Fails when no configured group could be joined.
    pub fn spawn(cfg: LsdConfig, found: mpsc::Sender<LsdPeer>) -> io::Result<Lsd> {
        let mut sockets: Vec<(UdpSocket, SocketAddr)> = Vec::new();
        let mut first_err: Option<io::Error> = None;
        if let Some(g) = cfg.group_v4 {
            match join_v4(&cfg, g) {
                Ok(s) => sockets.push((s, g)),
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }
        if let Some(g) = cfg.group_v6 {
            match join_v6(g) {
                Ok(s) => sockets.push((s, g)),
                Err(e) => debug!(error = %e, "LSD: cannot join the IPv6 group; IPv4 only"),
            }
        }
        if sockets.is_empty() {
            return Err(first_err.unwrap_or_else(|| io::Error::other("no LSD group configured")));
        }
        Lsd::from_sockets(cfg, sockets, found)
    }

    /// Runs on one plain unicast socket bound to `bind`, announcing to each
    /// of `destinations`. For tests and embedders without multicast.
    pub fn spawn_unicast(
        cfg: LsdConfig,
        bind: SocketAddr,
        destinations: Vec<SocketAddr>,
        found: mpsc::Sender<LsdPeer>,
    ) -> io::Result<Lsd> {
        let std_sock = std::net::UdpSocket::bind(bind)?;
        std_sock.set_nonblocking(true)?;
        let send = Arc::new(UdpSender {
            local: std_sock.local_addr()?,
            socket: std_sock.try_clone()?,
        });
        let recv = UdpSocket::from_std(std_sock)?;
        let dests: Destinations = destinations
            .into_iter()
            .map(|d| (Arc::clone(&send) as Arc<dyn DatagramSender>, d))
            .collect();
        let (lsd, handler) = Lsd::spawn_on(cfg, dests, found);
        lsd.add_task(tokio::spawn(recv_loop(recv, handler)));
        Ok(lsd)
    }

    /// Runs over caller-provided senders: announces go to each destination
    /// (any address, unicast included) and the returned handler must be fed
    /// the datagrams that reach the receiving socket.
    pub fn spawn_on(
        cfg: LsdConfig,
        sockets: Destinations,
        found: mpsc::Sender<LsdPeer>,
    ) -> (Lsd, Arc<dyn DatagramHandler>) {
        let core = Core::new(&cfg, sockets, found);
        let handler: Arc<dyn DatagramHandler> = Arc::clone(&core) as Arc<dyn DatagramHandler>;
        let lsd = Lsd {
            core,
            tasks: Arc::new(Tasks::default()),
        };
        (lsd, handler)
    }

    fn from_sockets(
        cfg: LsdConfig,
        sockets: Vec<(UdpSocket, SocketAddr)>,
        found: mpsc::Sender<LsdPeer>,
    ) -> io::Result<Lsd> {
        let mut dests: Destinations = Vec::new();
        let mut recvs = Vec::new();
        for (sock, group) in sockets {
            let std_sock = sock.into_std()?;
            let send = Arc::new(UdpSender {
                local: std_sock.local_addr()?,
                socket: std_sock.try_clone()?,
            });
            recvs.push(UdpSocket::from_std(std_sock)?);
            dests.push((send as Arc<dyn DatagramSender>, group));
        }
        let (lsd, handler) = Lsd::spawn_on(cfg, dests, found);
        for r in recvs {
            lsd.add_task(tokio::spawn(recv_loop(r, Arc::clone(&handler))));
        }
        Ok(lsd)
    }

    fn add_task(&self, t: JoinHandle<()>) {
        self.tasks
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(t);
    }

    /// Announces `info_hashes` on every destination, [`LSD_HASHES_PER_MESSAGE`]
    /// to a message. Never blocks; a failed send is dropped. The caller keeps
    /// to the 5-minute cadence per torrent.
    pub fn announce(&self, port: u16, info_hashes: &[[u8; 20]]) {
        for chunk in info_hashes.chunks(LSD_HASHES_PER_MESSAGE) {
            let msg = LsdMessage {
                port,
                info_hashes: chunk.to_vec(),
                cookie: Some(self.core.cookie.clone()),
            };
            let dests = self
                .core
                .destinations
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            for (sender, dest) in dests.iter() {
                if let Err(e) = sender.send_to(&msg.encode(*dest), *dest) {
                    debug!(%dest, error = %e, "LSD announce failed");
                }
            }
        }
    }

    /// Stops the receive tasks and drops the send halves, releasing the
    /// sockets (for sockets this instance opened itself). Later announces do
    /// nothing.
    pub async fn shutdown(&self) {
        for t in self.tasks.abort() {
            let _ = t.await;
        }
        self.core
            .destinations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

async fn recv_loop(socket: UdpSocket, handler: Arc<dyn DatagramHandler>) {
    let mut buf = vec![0u8; RECV_BUF];
    let mut errors = 0u32;
    loop {
        match socket.recv_from(&mut buf).await {
            Ok((n, from)) => {
                errors = 0;
                if let Some(data) = buf.get(..n) {
                    handler.handle(from, data);
                }
            }
            Err(e) => {
                debug!(error = %e, "LSD receive error");
                errors = errors.saturating_add(1);
                if errors >= 16 {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
    }
}

fn new_socket(domain: socket2::Domain) -> io::Result<socket2::Socket> {
    let s = socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))?;
    s.set_reuse_address(true)?;
    #[cfg(unix)]
    s.set_reuse_port(true)?;
    s.set_nonblocking(true)?;
    Ok(s)
}

fn join_v4(cfg: &LsdConfig, group: SocketAddr) -> io::Result<UdpSocket> {
    let SocketAddr::V4(g) = group else {
        return Err(io::Error::other("IPv4 group expected"));
    };
    let s = new_socket(socket2::Domain::IPV4)?;
    s.bind(&SocketAddr::from((Ipv4Addr::UNSPECIFIED, g.port())).into())?;
    s.join_multicast_v4(g.ip(), &cfg.interface_v4)?;
    s.set_multicast_loop_v4(true)?;
    if !cfg.interface_v4.is_unspecified() {
        s.set_multicast_if_v4(&cfg.interface_v4)?;
    }
    if let Some(ttl) = cfg.multicast_ttl {
        s.set_multicast_ttl_v4(ttl)?;
    }
    UdpSocket::from_std(s.into())
}

fn join_v6(group: SocketAddr) -> io::Result<UdpSocket> {
    let SocketAddr::V6(g) = group else {
        return Err(io::Error::other("IPv6 group expected"));
    };
    let s = new_socket(socket2::Domain::IPV6)?;
    s.set_only_v6(true)?;
    s.bind(&SocketAddr::from((Ipv6Addr::UNSPECIFIED, g.port())).into())?;
    s.join_multicast_v6(g.ip(), 0)?;
    s.set_multicast_loop_v6(true)?;
    UdpSocket::from_std(s.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn hash(b: u8) -> [u8; 20] {
        [b; 20]
    }

    #[test]
    fn bep14_message_roundtrip() {
        let m = LsdMessage {
            port: 6881,
            info_hashes: vec![hash(0xab), hash(1)],
            cookie: Some("deadbeef".to_owned()),
        };
        let bytes = m.encode(LSD_V4);
        assert!(
            bytes.starts_with(
                b"BT-SEARCH * HTTP/1.1\r\nHost: 239.192.152.143:6771\r\nPort: 6881\r\n"
            )
        );
        assert!(bytes.ends_with(b"\r\n\r\n\r\n"));
        assert_eq!(LsdMessage::decode(&bytes), Ok(m.clone()));
        let v6 = m.encode(LSD_V6);
        assert!(v6.starts_with(b"BT-SEARCH * HTTP/1.1\r\nHost: [ff15::efc0:988f]:6771\r\n"));
        assert_eq!(LsdMessage::decode(&v6), Ok(m));
    }

    #[test]
    fn decode_is_case_insensitive_and_bounded() {
        let mut s = String::from("BT-SEARCH * HTTP/1.1\r\nhOsT: x\r\nPORT: 7000\r\nX-Other: y\r\n");
        for i in 0..100u8 {
            s.push_str(&format!("INFOHASH: {}\r\n", format!("{i:02X}").repeat(20)));
        }
        s.push_str(&format!("Cookie: {}\r\n\r\n\r\n", "c".repeat(65)));
        assert_eq!(
            LsdMessage::decode(s.as_bytes()),
            Err(LsdError::TooLong),
            "over the message size"
        );
        let m = LsdMessage::decode_limited(s.as_bytes(), usize::MAX).unwrap();
        assert_eq!(m.port, 7000);
        assert_eq!(m.info_hashes.len(), MAX_LSD_DECODED_HASHES);
        assert_eq!(m.info_hashes.first(), Some(&hash(0)));
        assert_eq!(m.cookie, None, "an over-long cookie is dropped");
    }

    #[test]
    fn rejects_bad_messages() {
        let h = "ab".repeat(20);
        let ok = format!("BT-SEARCH * HTTP/1.1\r\nPort: 1\r\nInfohash: {h}\r\n\r\n\r\n");
        assert!(LsdMessage::decode(ok.as_bytes()).is_ok());
        let bad_method = ok.replace("BT-SEARCH", "GET");
        assert_eq!(
            LsdMessage::decode(bad_method.as_bytes()),
            Err(LsdError::BadRequest)
        );
        for port in ["0", "65536", "-1", "x"] {
            let m = ok.replace("Port: 1", &format!("Port: {port}"));
            assert_eq!(LsdMessage::decode(m.as_bytes()), Err(LsdError::BadPort));
        }
        let no_port = ok.replace("Port: 1\r\n", "");
        assert_eq!(
            LsdMessage::decode(no_port.as_bytes()),
            Err(LsdError::BadPort)
        );
        let big = format!("{ok}{}", "x".repeat(MAX_LSD_MESSAGE));
        assert_eq!(LsdMessage::decode(big.as_bytes()), Err(LsdError::TooLong));
        let no_hash = "BT-SEARCH * HTTP/1.1\r\nPort: 1\r\nInfohash: zz\r\n\r\n\r\n";
        assert_eq!(
            LsdMessage::decode(no_hash.as_bytes()),
            Err(LsdError::NoInfoHash)
        );
        assert!(LsdMessage::decode(&[0xff, 0xfe]).is_err());
    }

    #[test]
    fn own_cookie_ignored() {
        let (tx, mut rx) = mpsc::channel(8);
        let cfg = LsdConfig {
            cookie: "mine".to_owned(),
            ..LsdConfig::default()
        };
        let (_lsd, handler) = Lsd::spawn_on(cfg, Vec::new(), tx);
        let from: SocketAddr = "10.1.2.3:5555".parse().unwrap();
        let msg = |cookie: &str| {
            LsdMessage {
                port: 6881,
                info_hashes: vec![hash(7)],
                cookie: Some(cookie.to_owned()),
            }
            .encode(LSD_V4)
        };
        handler.handle(from, &msg("mine"));
        assert!(rx.try_recv().is_err(), "own message ignored");
        handler.handle(from, &msg("theirs"));
        assert_eq!(
            rx.try_recv().unwrap(),
            LsdPeer {
                info_hash: hash(7),
                addr: "10.1.2.3:6881".parse().unwrap()
            }
        );
    }

    #[test]
    fn decode_drops_repeated_hashes() {
        let h = "ab".repeat(20);
        let mut s = String::from("BT-SEARCH * HTTP/1.1\r\nPort: 1\r\n");
        for _ in 0..20 {
            s.push_str(&format!("Infohash: {h}\r\n"));
        }
        s.push_str("\r\n\r\n");
        assert_eq!(
            LsdMessage::decode(s.as_bytes()).unwrap().info_hashes.len(),
            1
        );
    }

    #[test]
    fn link_local_source_keeps_its_scope_and_mapped_v4_is_canonical() {
        use std::net::SocketAddrV6;
        let (tx, mut rx) = mpsc::channel(8);
        let (_lsd, handler) = Lsd::spawn_on(LsdConfig::default(), Vec::new(), tx);
        let msg = LsdMessage {
            port: 6881,
            info_hashes: vec![hash(7)],
            cookie: None,
        }
        .encode(LSD_V6);
        let ll = SocketAddrV6::new("fe80::1".parse().unwrap(), 5555, 0, 3);
        handler.handle(SocketAddr::V6(ll), &msg);
        let SocketAddr::V6(got) = rx.try_recv().unwrap().addr else {
            panic!("IPv6 expected");
        };
        assert_eq!((got.port(), got.scope_id()), (6881, 3));
        assert_eq!(got.ip(), ll.ip());
        handler.handle("[::ffff:10.1.2.3]:5555".parse().unwrap(), &msg);
        assert_eq!(
            rx.try_recv().unwrap().addr,
            "10.1.2.3:6881".parse::<SocketAddr>().unwrap()
        );
    }

    proptest! {
        #[test]
        fn lsd_decode_never_panics(data in proptest::collection::vec(any::<u8>(), 0..2000)) {
            let _ = LsdMessage::decode(&data);
        }

        #[test]
        fn lsd_decode_never_panics_on_near_valid(tail in "\\PC{0,300}") {
            let s = format!("BT-SEARCH * HTTP/1.1\r\n{tail}");
            let _ = LsdMessage::decode(s.as_bytes());
        }
    }
}
