//! UDP tracker client (BEP 15). Responses are untrusted and spoofable.
//!
//! One socket per address family is bound lazily and owned by the client; a
//! receive task routes datagrams to waiters by transaction id and drops
//! anything short, unknown or coming from an address other than the one the
//! request was sent to. The task is aborted when the client is dropped.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use fbt_core::udp::{DatagramHandler, DatagramSender};
use fbt_net::proxy::{ProxyConfig, ProxyKind};
use tokio::net::UdpSocket;
use tokio::sync::{OnceCell, oneshot};
use tokio::task::JoinHandle;

use crate::scrape::{build_udp_scrape, parse_udp_scrape};
use crate::types::{MIN_INTERVAL, bounded_text};
use crate::{
    AnnounceEvent, AnnounceRequest, AnnounceResponse, ScrapeInfo, TrackerConfig, TrackerError,
};

const PROTOCOL_ID: u64 = 0x0417_2710_1980;
const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;
const ACTION_ERROR: u32 = 3;
const CONNECTION_TTL: Duration = Duration::from_secs(60);
/// Largest backoff exponent, so `base * 2^n` cannot overflow.
const MAX_BACKOFF_SHIFT: u32 = 16;
/// Resolved addresses considered per tracker host.
const MAX_RESOLVED: usize = 16;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn be_u32(buf: &[u8], off: usize) -> Option<u32> {
    let b = buf.get(off..off.checked_add(4)?)?;
    Some(u32::from_be_bytes(<[u8; 4]>::try_from(b).ok()?))
}

fn be_u64(buf: &[u8], off: usize) -> Option<u64> {
    let b = buf.get(off..off.checked_add(8)?)?;
    Some(u64::from_be_bytes(<[u8; 8]>::try_from(b).ok()?))
}

/// Parses a UDP announce response. Input is untrusted.
#[doc(hidden)]
pub fn parse_udp_announce(
    buf: &[u8],
    transaction_id: u32,
    ipv6: bool,
) -> Result<AnnounceResponse, TrackerError> {
    let (Some(action), Some(tid)) = (be_u32(buf, 0), be_u32(buf, 4)) else {
        return Err(TrackerError::InvalidResponse("short datagram"));
    };
    if tid != transaction_id {
        return Err(TrackerError::InvalidResponse("transaction id mismatch"));
    }
    if action == ACTION_ERROR {
        let msg = buf.get(8..).unwrap_or_default();
        return Err(TrackerError::Failure(bounded_text(msg)));
    }
    if action != ACTION_ANNOUNCE {
        return Err(TrackerError::InvalidResponse("unexpected action"));
    }
    let (Some(interval), Some(leechers), Some(seeders)) =
        (be_u32(buf, 8), be_u32(buf, 12), be_u32(buf, 16))
    else {
        return Err(TrackerError::InvalidResponse("short announce response"));
    };
    let body = buf.get(20..).unwrap_or_default();
    let mut peers = Vec::new();
    if ipv6 {
        let (chunks, rest) = body.as_chunks::<18>();
        if !rest.is_empty() {
            return Err(TrackerError::InvalidResponse("misaligned peer list"));
        }
        for c in chunks {
            let (ip, port) = c.split_at(16);
            if let (Ok(ip), &[p0, p1]) = (<[u8; 16]>::try_from(ip), port) {
                peers.push(SocketAddr::new(
                    IpAddr::V6(Ipv6Addr::from(ip)),
                    u16::from_be_bytes([p0, p1]),
                ));
            }
        }
    } else {
        let (chunks, rest) = body.as_chunks::<6>();
        if !rest.is_empty() {
            return Err(TrackerError::InvalidResponse("misaligned peer list"));
        }
        for &[a, b, c, d, p0, p1] in chunks {
            peers.push(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(a, b, c, d)),
                u16::from_be_bytes([p0, p1]),
            ));
        }
    }
    Ok(AnnounceResponse {
        interval: Duration::from_secs(u64::from(interval)).max(MIN_INTERVAL),
        min_interval: None,
        peers,
        complete: Some(seeders),
        incomplete: Some(leechers),
        tracker_id: None,
        warning: None,
        external_ip: None,
    })
}

/// Connection ids per tracker address, valid for 60 s. Time is injected.
pub(crate) struct ConnCache {
    map: HashMap<SocketAddr, (u64, Instant)>,
}

impl ConnCache {
    pub(crate) fn new() -> Self {
        ConnCache {
            map: HashMap::new(),
        }
    }

    pub(crate) fn get(&self, addr: SocketAddr, now: Instant) -> Option<u64> {
        let (id, at) = self.map.get(&addr)?;
        (now.saturating_duration_since(*at) < CONNECTION_TTL).then_some(*id)
    }

    pub(crate) fn put(&mut self, addr: SocketAddr, id: u64, now: Instant) {
        self.map
            .retain(|_, (_, at)| now.saturating_duration_since(*at) < CONNECTION_TTL);
        self.map.insert(addr, (id, now));
    }

    pub(crate) fn clear(&mut self, addr: SocketAddr) {
        self.map.remove(&addr);
    }
}

type Pending = Arc<Mutex<HashMap<u32, (SocketAddr, oneshot::Sender<Vec<u8>>)>>>;

/// Builds a request datagram from the connection id, the transaction id
/// and the address family (`true` for IPv6).
type BuildFn<'a> = &'a (dyn Fn(u64, u32, bool) -> Vec<u8> + Sync);
/// Parses a reply datagram, given the transaction id and the family.
type ParseFn<'a, T> = &'a (dyn Fn(&[u8], u32, bool) -> Result<T, TrackerError> + Sync);

/// A bound socket plus its receive task; the task dies with this value.
struct Sock {
    socket: Arc<UdpSocket>,
    pending: Pending,
    task: JoinHandle<()>,
}

impl Drop for Sock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Sock {
    fn bind(v6: bool) -> std::io::Result<Sock> {
        let std_sock = if v6 {
            std::net::UdpSocket::bind((Ipv6Addr::UNSPECIFIED, 0))?
        } else {
            std::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?
        };
        std_sock.set_nonblocking(true)?;
        let socket = Arc::new(UdpSocket::from_std(std_sock)?);
        let pending: Pending = Arc::default();
        let task = tokio::spawn(recv_loop(socket.clone(), pending.clone()));
        Ok(Sock {
            socket,
            pending,
            task,
        })
    }
}

async fn recv_loop(socket: Arc<UdpSocket>, pending: Pending) {
    let mut buf = vec![0u8; 65536];
    loop {
        let Ok((n, from)) = socket.recv_from(&mut buf).await else {
            // Transient errors (e.g. ICMP unreachable on some platforms) must
            // not kill the task; yield to avoid spinning on a dead socket.
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        let Some(dgram) = buf.get(..n) else { continue };
        deliver(&pending, from, dgram);
    }
}

/// Routes one datagram to its waiter: the transaction id must be pending and
/// `from` must be the address the request went to; anything else is dropped.
fn deliver(pending: &Pending, from: SocketAddr, dgram: &[u8]) {
    let Some(tid) = be_u32(dgram, 4) else {
        return;
    };
    let mut map = lock(pending);
    if map.get(&tid).is_some_and(|(addr, _)| *addr == from)
        && let Some((_, tx)) = map.remove(&tid)
    {
        let _ = tx.send(dgram.to_vec());
    }
}

/// Receives tracker replies from the session's shared socket.
#[derive(Debug)]
struct SharedHandler {
    pending: Pending,
}

impl DatagramHandler for SharedHandler {
    fn handle(&self, from: SocketAddr, data: &[u8]) {
        deliver(&self.pending, from, data);
    }
}

/// Where a request is sent from and where its reply is matched.
enum Endpoint<'a> {
    Own(&'a Sock),
    Shared(&'a Arc<dyn DatagramSender>, &'a Pending),
}

impl Endpoint<'_> {
    fn pending(&self) -> &Pending {
        match self {
            Endpoint::Own(s) => &s.pending,
            Endpoint::Shared(_, p) => p,
        }
    }

    /// Sends one datagram. A full shared socket (`WouldBlock`) counts as a
    /// lost packet: the retransmit timer covers it.
    async fn send(&self, dgram: &[u8], addr: SocketAddr) -> std::io::Result<()> {
        match self {
            Endpoint::Own(s) => s.socket.send_to(dgram, addr).await.map(|_| ()),
            Endpoint::Shared(s, _) => match s.send_to(dgram, addr) {
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(()),
                r => r.map(|_| ()),
            },
        }
    }
}

/// Removes a waiter on every exit path.
struct Waiter<'a> {
    pending: &'a Pending,
    tid: u32,
}

impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        lock(self.pending).remove(&self.tid);
    }
}

pub(crate) struct UdpClient {
    /// Bound on the DNS lookup of the tracker host.
    dns_timeout: Duration,
    base_timeout: Duration,
    max_retries: u32,
    v4: OnceCell<Sock>,
    v6: OnceCell<Sock>,
    shared_v4: Option<Arc<dyn DatagramSender>>,
    shared_v6: Option<Arc<dyn DatagramSender>>,
    shared_pending: Pending,
    conns: Mutex<ConnCache>,
    route: Route,
}

/// How announces leave, as the proxy settings decide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    /// Shared sockets per family, else the client's own.
    Direct,
    /// Every announce, of either family, through the one shared (relayed)
    /// socket; the client never binds its own.
    Relayed,
    /// Trackers go through an HTTP proxy, which cannot carry UDP.
    Unsupported,
}

impl Route {
    fn of(proxy: Option<&ProxyConfig>) -> Route {
        match proxy {
            // One socket cannot be half proxied: SOCKS5 relays all UDP.
            Some(p) if p.kind == ProxyKind::Socks5 && (p.proxy_peers || p.proxy_trackers) => {
                Route::Relayed
            }
            Some(p) if p.kind == ProxyKind::Http && p.proxy_trackers => Route::Unsupported,
            _ => Route::Direct,
        }
    }
}

impl std::fmt::Debug for UdpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpClient").finish_non_exhaustive()
    }
}

/// Extracts `(host, port)` from a `udp://host:port[/path]` URL.
fn parse_udp_url(url: &str) -> Result<(String, u16), TrackerError> {
    let rest = url
        .split_once("://")
        .map(|(_, r)| r)
        .ok_or_else(|| TrackerError::InvalidUrl("missing scheme".to_owned()))?;
    let auth = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let (host, port) = if let Some(stripped) = auth.strip_prefix('[') {
        let (h, tail) = stripped
            .split_once(']')
            .ok_or_else(|| TrackerError::InvalidUrl("bad ipv6 host".to_owned()))?;
        (h, tail.strip_prefix(':'))
    } else {
        match auth.rsplit_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (auth, None),
        }
    };
    if host.is_empty() {
        return Err(TrackerError::InvalidUrl("missing host".to_owned()));
    }
    let port = port
        .ok_or_else(|| TrackerError::InvalidUrl("missing port".to_owned()))?
        .parse::<u16>()
        .map_err(|_| TrackerError::InvalidUrl("bad port".to_owned()))?;
    Ok((host.to_owned(), port))
}

fn event_code(e: AnnounceEvent) -> u32 {
    match e {
        AnnounceEvent::None => 0,
        AnnounceEvent::Completed => 1,
        AnnounceEvent::Started => 2,
        AnnounceEvent::Stopped => 3,
    }
}

/// An announce over IPv6 (`ipv6`) or IPv4, carrying that family's port.
fn build_announce(cid: u64, tid: u32, req: &AnnounceRequest, ipv6: bool) -> Vec<u8> {
    let num_want = req
        .num_want
        .and_then(|n| i32::try_from(n).ok())
        .unwrap_or(-1);
    let mut b = Vec::with_capacity(98);
    b.extend_from_slice(&cid.to_be_bytes());
    b.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
    b.extend_from_slice(&tid.to_be_bytes());
    b.extend_from_slice(&req.info_hash);
    b.extend_from_slice(&req.peer_id.0);
    b.extend_from_slice(&req.downloaded.to_be_bytes());
    b.extend_from_slice(&req.left.to_be_bytes());
    b.extend_from_slice(&req.uploaded.to_be_bytes());
    b.extend_from_slice(&event_code(req.event).to_be_bytes());
    b.extend_from_slice(&0u32.to_be_bytes());
    b.extend_from_slice(&req.key.to_be_bytes());
    b.extend_from_slice(&num_want.to_be_bytes());
    b.extend_from_slice(&req.port_for(ipv6).to_be_bytes());
    b
}

fn build_connect(tid: u32) -> Vec<u8> {
    let mut b = Vec::with_capacity(16);
    b.extend_from_slice(&PROTOCOL_ID.to_be_bytes());
    b.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
    b.extend_from_slice(&tid.to_be_bytes());
    b
}

/// Returns the connection id from a connect response.
fn parse_connect(buf: &[u8], tid: u32) -> Result<u64, TrackerError> {
    let (Some(action), Some(got)) = (be_u32(buf, 0), be_u32(buf, 4)) else {
        return Err(TrackerError::InvalidResponse("short datagram"));
    };
    if got != tid {
        return Err(TrackerError::InvalidResponse("transaction id mismatch"));
    }
    if action == ACTION_ERROR {
        let msg = buf.get(8..).unwrap_or_default();
        return Err(TrackerError::Failure(bounded_text(msg)));
    }
    if action != ACTION_CONNECT {
        return Err(TrackerError::InvalidResponse("unexpected action"));
    }
    be_u64(buf, 8).ok_or(TrackerError::InvalidResponse("short connect response"))
}

impl UdpClient {
    pub(crate) fn new(cfg: &TrackerConfig) -> UdpClient {
        UdpClient::with_sockets(cfg, Vec::new(), None)
    }

    /// Sends through the shared socket of each family; a family without one
    /// binds its own socket lazily. Through a SOCKS5 `proxy` every announce
    /// takes the first shared socket (the relayed one) and none is bound;
    /// through an HTTP proxy with `proxy_trackers` announces fail with
    /// [`TrackerError::ProxyUnsupported`].
    pub(crate) fn with_sockets(
        cfg: &TrackerConfig,
        sockets: Vec<Arc<dyn DatagramSender>>,
        proxy: Option<&ProxyConfig>,
    ) -> UdpClient {
        let mut shared_v4 = None;
        let mut shared_v6 = None;
        for s in sockets {
            let slot = if s.local_addr().is_ipv6() {
                &mut shared_v6
            } else {
                &mut shared_v4
            };
            slot.get_or_insert(s);
        }
        UdpClient {
            shared_v4,
            shared_v6,
            shared_pending: Pending::default(),
            dns_timeout: cfg.http_timeout,
            base_timeout: cfg.udp_base_timeout,
            max_retries: cfg.udp_max_retries,
            v4: OnceCell::new(),
            v6: OnceCell::new(),
            conns: Mutex::new(ConnCache::new()),
            route: Route::of(proxy),
        }
    }

    pub(crate) fn handler(&self) -> Arc<dyn DatagramHandler> {
        Arc::new(SharedHandler {
            pending: self.shared_pending.clone(),
        })
    }

    pub(crate) async fn announce(
        &self,
        url: &str,
        req: &AnnounceRequest,
    ) -> Result<AnnounceResponse, TrackerError> {
        self.request(
            url,
            &|cid, tid, ipv6| build_announce(cid, tid, req, ipv6),
            &parse_udp_announce,
        )
        .await
    }

    /// Scrapes `url` for `hashes` (at most
    /// [`MAX_SCRAPE_HASHES`](crate::MAX_SCRAPE_HASHES); the caller checks).
    pub(crate) async fn scrape(
        &self,
        url: &str,
        hashes: &[[u8; 20]],
    ) -> Result<Vec<ScrapeInfo>, TrackerError> {
        let counts = self
            .request(
                url,
                &|cid, tid, _| build_udp_scrape(cid, tid, hashes),
                &|buf, tid, _| parse_udp_scrape(buf, tid, hashes.len()),
            )
            .await?;
        Ok(hashes
            .iter()
            .zip(counts)
            .map(|(h, (complete, downloaded, incomplete))| ScrapeInfo {
                info_hash: *h,
                complete,
                downloaded,
                incomplete,
            })
            .collect())
    }

    /// One request after a connect (or a cached connection id): `build`
    /// makes the datagram from the connection id, the transaction id and
    /// the address family (`true` for IPv6), `parse` reads the reply.
    async fn request<T>(
        &self,
        url: &str,
        build: BuildFn<'_>,
        parse: ParseFn<'_, T>,
    ) -> Result<T, TrackerError> {
        if self.route == Route::Unsupported {
            return Err(TrackerError::ProxyUnsupported);
        }
        let (host, port) = parse_udp_url(url)?;
        // The host comes from a hostile torrent: bound the lookup.
        let addrs: Vec<SocketAddr> = match tokio::time::timeout(
            self.dns_timeout,
            tokio::net::lookup_host((host.as_str(), port)),
        )
        .await
        {
            Ok(found) => found?.take(MAX_RESOLVED).collect(),
            Err(_) => return Err(TrackerError::Timeout),
        };
        let first = *addrs
            .first()
            .ok_or_else(|| TrackerError::InvalidUrl("host did not resolve".to_owned()))?;
        match self.request_to(first, build, parse).await {
            // Unreachable over the first family (no IPv6 route, say): one
            // address of the other family gets a chance.
            Err(e) if unreachable(&e) => match other_family(&addrs, first) {
                Some(second) => self.request_to(second, build, parse).await,
                None => Err(e),
            },
            r => r,
        }
    }

    /// One request to one resolved tracker address.
    async fn request_to<T>(
        &self,
        addr: SocketAddr,
        build: BuildFn<'_>,
        parse: ParseFn<'_, T>,
    ) -> Result<T, TrackerError> {
        let ipv6 = addr.is_ipv6();
        let shared = if self.route == Route::Relayed {
            // The relay carries either family; never a direct socket.
            let relayed = self.shared_v4.as_ref().or(self.shared_v6.as_ref());
            if relayed.is_none() {
                return Err(TrackerError::Proxy("no relayed UDP socket".to_owned()));
            }
            relayed
        } else if ipv6 {
            self.shared_v6.as_ref()
        } else {
            self.shared_v4.as_ref()
        };
        let sock = match shared {
            Some(s) => Endpoint::Shared(s, &self.shared_pending),
            None if ipv6 => Endpoint::Own(
                self.v6
                    .get_or_try_init(|| async { Sock::bind(true) })
                    .await?,
            ),
            None => Endpoint::Own(
                self.v4
                    .get_or_try_init(|| async { Sock::bind(false) })
                    .await?,
            ),
        };
        let sock = &sock;

        let mut cached = lock(&self.conns).get(addr, Instant::now());
        loop {
            let (cid, from_cache) = match cached.take() {
                Some(id) => (id, true),
                None => (self.connect(sock, addr).await?, false),
            };
            let tid = new_tid(sock.pending());
            let dgram = build(cid, tid, ipv6);
            let resp = self.exchange(sock, addr, tid, &dgram).await?;
            match parse(&resp, tid, ipv6) {
                // A cached connection id the tracker no longer knows:
                // reconnect once.
                Err(TrackerError::Failure(_)) if from_cache => {
                    lock(&self.conns).clear(addr);
                }
                other => return other,
            }
        }
    }

    async fn connect(&self, sock: &Endpoint<'_>, addr: SocketAddr) -> Result<u64, TrackerError> {
        let tid = new_tid(sock.pending());
        let resp = self.exchange(sock, addr, tid, &build_connect(tid)).await?;
        let id = parse_connect(&resp, tid)?;
        lock(&self.conns).put(addr, id, Instant::now());
        Ok(id)
    }

    /// Sends `dgram` and waits for the matching reply, retransmitting with a
    /// doubling timeout.
    async fn exchange(
        &self,
        sock: &Endpoint<'_>,
        addr: SocketAddr,
        tid: u32,
        dgram: &[u8],
    ) -> Result<Vec<u8>, TrackerError> {
        let (tx, mut rx) = oneshot::channel();
        lock(sock.pending()).insert(tid, (addr, tx));
        let _guard = Waiter {
            pending: sock.pending(),
            tid,
        };
        for n in 0..=self.max_retries {
            sock.send(dgram, addr).await?;
            let wait = self
                .base_timeout
                .saturating_mul(1u32 << n.min(MAX_BACKOFF_SHIFT));
            if let Ok(reply) = tokio::time::timeout(wait, &mut rx).await {
                return reply.map_err(|_| TrackerError::Timeout);
            }
        }
        Err(TrackerError::Timeout)
    }
}

/// The tracker did not answer at this address (as opposed to answering with
/// an error).
fn unreachable(e: &TrackerError) -> bool {
    matches!(e, TrackerError::Timeout | TrackerError::Io(_))
}

/// The first address in `addrs` of the other family than `first`.
fn other_family(addrs: &[SocketAddr], first: SocketAddr) -> Option<SocketAddr> {
    addrs
        .iter()
        .copied()
        .find(|a| a.is_ipv6() != first.is_ipv6())
}

/// A transaction id not currently in flight.
fn new_tid(pending: &Pending) -> u32 {
    loop {
        let tid: u32 = rand::random();
        if !lock(pending).contains_key(&tid) {
            return tid;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use fbt_core::PeerId;
    use tokio::net::UdpSocket;

    use super::*;
    use crate::{AnnounceEvent, AnnounceRequest, TrackerClient, TrackerConfig};

    const CID: u64 = 0x1122_3344_5566_7788;

    fn request() -> AnnounceRequest {
        AnnounceRequest {
            info_hash: [0xaa; 20],
            peer_id: PeerId([b'x'; 20]),
            port: 6881,
            port_v4: None,
            port_v6: None,
            uploaded: 1,
            downloaded: 2,
            left: 3,
            event: AnnounceEvent::Started,
            num_want: Some(50),
            key: 0xbeef,
            tracker_id: None,
        }
    }

    #[test]
    fn announce_carries_the_port_of_its_family() {
        let port = |b: &[u8]| u16::from_be_bytes([b[96], b[97]]);
        let mut req = request();
        assert_eq!(port(&build_announce(1, 2, &req, false)), 6881);
        assert_eq!(port(&build_announce(1, 2, &req, true)), 6881);
        req.port_v4 = Some(7000);
        req.port_v6 = Some(7001);
        assert_eq!(port(&build_announce(1, 2, &req, false)), 7000);
        assert_eq!(port(&build_announce(1, 2, &req, true)), 7001);
    }

    fn client() -> TrackerClient {
        TrackerClient::new(TrackerConfig {
            udp_base_timeout: Duration::from_millis(100),
            ..TrackerConfig::default()
        })
        .unwrap()
    }

    fn connect_reply(tid: &[u8], cid: u64) -> Vec<u8> {
        let mut r = vec![0, 0, 0, 0];
        r.extend_from_slice(tid);
        r.extend_from_slice(&cid.to_be_bytes());
        r
    }

    fn announce_reply(tid: &[u8], peers: &[u8]) -> Vec<u8> {
        let mut r = vec![0, 0, 0, 1];
        r.extend_from_slice(tid);
        r.extend_from_slice(&900u32.to_be_bytes());
        r.extend_from_slice(&7u32.to_be_bytes());
        r.extend_from_slice(&5u32.to_be_bytes());
        r.extend_from_slice(peers);
        r
    }

    fn error_reply(tid: &[u8], msg: &str) -> Vec<u8> {
        let mut r = vec![0, 0, 0, 3];
        r.extend_from_slice(tid);
        r.extend_from_slice(msg.as_bytes());
        r
    }

    const PEERS: [u8; 12] = [1, 2, 3, 4, 0x1a, 0xe1, 5, 6, 7, 8, 0x1a, 0xe2];

    /// Spawns a fake tracker; `f(datagram, spoof_socket)` returns the replies
    /// to send from the tracker's own socket.
    async fn fake<F>(f: F) -> String
    where
        F: Fn(&[u8], &UdpSocket, SocketAddr) -> Vec<Vec<u8>> + Send + 'static,
    {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let spoof = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let url = format!("udp://{}/announce", sock.local_addr().unwrap());
        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            loop {
                let Ok((n, from)) = sock.recv_from(&mut buf).await else {
                    return;
                };
                for r in f(&buf[..n], &spoof, from) {
                    let _ = sock.send_to(&r, from).await;
                }
            }
        });
        url
    }

    fn is_connect(b: &[u8]) -> bool {
        b.len() == 16 && b[8..12] == [0, 0, 0, 0]
    }

    #[tokio::test]
    async fn udp_connect_then_announce() {
        let seen = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let s = seen.clone();
        let url = fake(move |b, _, _| {
            if is_connect(b) {
                assert_eq!(&b[..8], &0x0417_2710_1980u64.to_be_bytes());
                vec![connect_reply(&b[12..16], CID)]
            } else {
                s.lock().unwrap().push(b.to_vec());
                vec![announce_reply(&b[12..16], &PEERS)]
            }
        })
        .await;
        let r = client().announce(&url, &request()).await.unwrap();
        assert_eq!(r.interval, Duration::from_secs(900));
        assert_eq!(r.incomplete, Some(7));
        assert_eq!(r.complete, Some(5));
        assert_eq!(
            r.peers,
            vec![
                "1.2.3.4:6881".parse::<SocketAddr>().unwrap(),
                "5.6.7.8:6882".parse().unwrap()
            ]
        );
        let seen = seen.lock().unwrap();
        let b = &seen[0];
        assert_eq!(b.len(), 98);
        assert_eq!(&b[0..8], &CID.to_be_bytes());
        assert_eq!(&b[8..12], &1u32.to_be_bytes());
        assert_eq!(&b[16..36], &[0xaa; 20]);
        assert_eq!(&b[36..56], &[b'x'; 20]);
        assert_eq!(&b[56..64], &2u64.to_be_bytes());
        assert_eq!(&b[64..72], &3u64.to_be_bytes());
        assert_eq!(&b[72..80], &1u64.to_be_bytes());
        assert_eq!(&b[80..84], &2u32.to_be_bytes()); // started
        assert_eq!(&b[84..88], &0u32.to_be_bytes());
        assert_eq!(&b[88..92], &0xbeefu32.to_be_bytes());
        assert_eq!(&b[92..96], &50i32.to_be_bytes());
        assert_eq!(&b[96..98], &6881u16.to_be_bytes());
    }

    #[tokio::test]
    async fn udp_connection_id_reused_within_60s() {
        let connects = Arc::new(AtomicUsize::new(0));
        let c = connects.clone();
        let url = fake(move |b, _, _| {
            if is_connect(b) {
                c.fetch_add(1, Ordering::SeqCst);
                vec![connect_reply(&b[12..16], CID)]
            } else {
                assert_eq!(&b[0..8], &CID.to_be_bytes());
                vec![announce_reply(&b[12..16], &PEERS)]
            }
        })
        .await;
        let cl = client();
        cl.announce(&url, &request()).await.unwrap();
        cl.announce(&url, &request()).await.unwrap();
        assert_eq!(connects.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn udp_connection_cache_expires_after_60s() {
        let addr: SocketAddr = "127.0.0.1:6969".parse().unwrap();
        let t0 = Instant::now();
        let mut cache = ConnCache::new();
        assert_eq!(cache.get(addr, t0), None);
        cache.put(addr, 42, t0);
        assert_eq!(cache.get(addr, t0 + Duration::from_secs(59)), Some(42));
        assert_eq!(cache.get(addr, t0 + Duration::from_secs(60)), None);
        cache.put(addr, 43, t0);
        cache.clear(addr);
        assert_eq!(cache.get(addr, t0), None);
    }

    #[tokio::test]
    async fn udp_retransmits_then_times_out() {
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let url = fake(move |_, _, _| {
            c.fetch_add(1, Ordering::SeqCst);
            vec![]
        })
        .await;
        let start = Instant::now();
        let err = client().announce(&url, &request()).await.unwrap_err();
        assert!(matches!(err, TrackerError::Timeout), "{err:?}");
        assert!(start.elapsed() >= Duration::from_millis(700));
        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn udp_error_action_maps_to_failure() {
        let url = fake(|b, _, _| {
            if is_connect(b) {
                vec![connect_reply(&b[12..16], CID)]
            } else {
                vec![error_reply(&b[12..16], "torrent not registered")]
            }
        })
        .await;
        let err = client().announce(&url, &request()).await.unwrap_err();
        assert!(
            matches!(&err, TrackerError::Failure(m) if m == "torrent not registered"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn udp_rejected_connection_id_reconnects_once() {
        let connects = Arc::new(AtomicUsize::new(0));
        let announces = Arc::new(AtomicUsize::new(0));
        let (c, a) = (connects.clone(), announces.clone());
        let url = fake(move |b, _, _| {
            if is_connect(b) {
                c.fetch_add(1, Ordering::SeqCst);
                vec![connect_reply(&b[12..16], CID)]
            } else if a.fetch_add(1, Ordering::SeqCst) == 1 {
                vec![error_reply(&b[12..16], "bad connection id")]
            } else {
                vec![announce_reply(&b[12..16], &PEERS)]
            }
        })
        .await;
        let cl = client();
        cl.announce(&url, &request()).await.unwrap();
        cl.announce(&url, &request()).await.unwrap();
        assert_eq!(connects.load(Ordering::SeqCst), 2);
        assert_eq!(announces.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn udp_wrong_transaction_id_ignored() {
        let url = fake(|b, spoof, from| {
            let tid = &b[12..16];
            let mut wrong = tid.to_vec();
            wrong[0] ^= 0xff;
            let real = if is_connect(b) {
                connect_reply(tid, CID)
            } else {
                assert_eq!(&b[0..8], &CID.to_be_bytes());
                announce_reply(tid, &PEERS)
            };
            // Same body with a wrong tid, plus a correct-tid reply from a
            // different source address carrying poisoned data.
            let mut bad = real.clone();
            bad[4..8].copy_from_slice(&wrong);
            let poisoned = if is_connect(b) {
                connect_reply(tid, 0xdead)
            } else {
                announce_reply(tid, &[9, 9, 9, 9, 0, 1])
            };
            let _ = spoof.try_send_to(&poisoned, from);
            vec![bad, vec![1, 2, 3], real]
        })
        .await;
        let r = client().announce(&url, &request()).await.unwrap();
        assert_eq!(r.peers.len(), 2);
        assert_eq!(r.peers[0], "1.2.3.4:6881".parse::<SocketAddr>().unwrap());
    }

    /// A shared socket: sends on a real UDP socket, and a task feeds what it
    /// receives to the client's handler.
    #[derive(Debug)]
    struct Shared(std::net::UdpSocket);

    impl DatagramSender for Shared {
        fn local_addr(&self) -> SocketAddr {
            self.0.local_addr().unwrap()
        }
        fn send_to(&self, data: &[u8], to: SocketAddr) -> std::io::Result<usize> {
            self.0.send_to(data, to)
        }
    }

    fn shared_socket(bind: &str) -> Option<(Arc<Shared>, std::net::UdpSocket)> {
        let s = std::net::UdpSocket::bind(bind).ok()?;
        let rx = s.try_clone().ok()?;
        Some((Arc::new(Shared(s)), rx))
    }

    fn pump(rx: std::net::UdpSocket, h: Arc<dyn DatagramHandler>) {
        std::thread::spawn(move || {
            let mut buf = [0u8; 2048];
            while let Ok((n, from)) = rx.recv_from(&mut buf) {
                h.handle(from, &buf[..n]);
            }
        });
    }

    fn shared_client(socks: Vec<Arc<dyn DatagramSender>>) -> TrackerClient {
        TrackerClient::new_with(
            TrackerConfig {
                udp_base_timeout: Duration::from_millis(100),
                ..TrackerConfig::default()
            },
            socks,
            None,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn udp_announce_through_shared_socket() {
        let (sock, rx) = shared_socket("127.0.0.1:0").unwrap();
        let port = sock.local_addr().port();
        let seen = Arc::new(Mutex::new(Vec::<u16>::new()));
        let s = seen.clone();
        let url = fake(move |b, _, from| {
            s.lock().unwrap().push(from.port());
            if is_connect(b) {
                vec![connect_reply(&b[12..16], CID)]
            } else {
                vec![announce_reply(&b[12..16], &PEERS)]
            }
        })
        .await;
        let cl = shared_client(vec![sock]);
        pump(rx, cl.udp_handler());
        let r = cl.announce(&url, &request()).await.unwrap();
        assert_eq!(r.peers.len(), 2);
        let seen = seen.lock().unwrap();
        assert!(!seen.is_empty() && seen.iter().all(|p| *p == port));
    }

    #[tokio::test]
    async fn shared_handler_ignores_wrong_source_and_unknown_tid() {
        let cl = client();
        let h = cl.udp_handler();
        let pending = &cl.inner.udp.shared_pending;
        let (tx, mut rx) = oneshot::channel();
        let good: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let other: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        lock(pending).insert(5, (good, tx));
        let tid = 5u32.to_be_bytes();
        h.handle(other, &connect_reply(&tid, CID));
        h.handle(good, &connect_reply(&6u32.to_be_bytes(), CID));
        h.handle(good, &[1, 2, 3]);
        assert!(rx.try_recv().is_err());
        assert!(lock(pending).contains_key(&5));
        h.handle(good, &connect_reply(&tid, CID));
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn family_without_shared_socket_uses_its_own() {
        let Ok(sock6) = UdpSocket::bind("[::1]:0").await else {
            return;
        };
        let url = format!("udp://{}/announce", sock6.local_addr().unwrap());
        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            while let Ok((n, from)) = sock6.recv_from(&mut buf).await {
                let b = &buf[..n];
                let reply = if is_connect(b) {
                    connect_reply(&b[12..16], CID)
                } else {
                    announce_reply(&b[12..16], &[0u8; 18])
                };
                let _ = sock6.send_to(&reply, from).await;
            }
        });
        let (sock, rx) = shared_socket("127.0.0.1:0").unwrap();
        let cl = shared_client(vec![sock]);
        pump(rx, cl.udp_handler());
        let r = cl.announce(&url, &request()).await.unwrap();
        assert_eq!(r.peers.len(), 1);
    }

    #[test]
    fn fallback_picks_the_other_family() {
        let v4: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let v4b: SocketAddr = "127.0.0.2:1".parse().unwrap();
        let v6: SocketAddr = "[::1]:1".parse().unwrap();
        assert_eq!(other_family(&[v6, v4, v4b], v6), Some(v4));
        assert_eq!(other_family(&[v4, v4b, v6], v4), Some(v6));
        assert_eq!(other_family(&[v4, v4b], v4), None);
        assert!(unreachable(&TrackerError::Timeout));
        assert!(!unreachable(&TrackerError::Failure(String::new())));
    }

    /// A host resolving to both families, with the tracker listening only
    /// on the second one: the announce falls back after the first times
    /// out. Skipped where `localhost` does not resolve to both families.
    #[tokio::test]
    async fn udp_falls_back_to_the_other_family() {
        let addrs: Vec<SocketAddr> = tokio::net::lookup_host(("localhost", 0))
            .await
            .map(Iterator::collect)
            .unwrap_or_default();
        let Some(first) = addrs.first().copied() else {
            return;
        };
        let Some(second) = other_family(&addrs, first) else {
            return;
        };
        let Ok(sock) = UdpSocket::bind(SocketAddr::new(second.ip(), 0)).await else {
            return;
        };
        let port = sock.local_addr().unwrap().port();
        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            while let Ok((n, from)) = sock.recv_from(&mut buf).await {
                let b = &buf[..n];
                let reply = if is_connect(b) {
                    connect_reply(&b[12..16], CID)
                } else if second.is_ipv6() {
                    announce_reply(&b[12..16], &[0u8; 18])
                } else {
                    announce_reply(&b[12..16], &PEERS)
                };
                let _ = sock.send_to(&reply, from).await;
            }
        });
        let url = format!("udp://localhost:{port}/announce");
        let r = client().announce(&url, &request()).await.unwrap();
        assert!(!r.peers.is_empty());
    }

    #[test]
    fn parse_rejects_short_and_misaligned() {
        let tid = 7u32.to_be_bytes();
        let ok = announce_reply(&tid, &PEERS);
        assert_eq!(parse_udp_announce(&ok, 7, false).unwrap().peers.len(), 2);
        assert!(parse_udp_announce(&ok[..19], 7, false).is_err());
        assert!(parse_udp_announce(&[], 7, false).is_err());
        assert!(parse_udp_announce(&ok, 8, false).is_err());
        let mut odd = ok.clone();
        odd.push(0);
        assert!(matches!(
            parse_udp_announce(&odd, 7, false),
            Err(TrackerError::InvalidResponse(_))
        ));
        // 12 bytes of v4 peers are not a whole number of 18-byte v6 entries.
        assert!(parse_udp_announce(&ok, 7, true).is_err());
        let v6 = announce_reply(&tid, &[0u8; 18]);
        assert_eq!(parse_udp_announce(&v6, 7, true).unwrap().peers.len(), 1);
        // Interval is clamped to the 60 s floor.
        let mut low = announce_reply(&tid, &[]);
        low[8..12].copy_from_slice(&5u32.to_be_bytes());
        assert_eq!(
            parse_udp_announce(&low, 7, false).unwrap().interval,
            Duration::from_secs(60)
        );
    }

    fn scrape_reply(tid: &[u8], counts: &[[u32; 3]]) -> Vec<u8> {
        let mut r = vec![0, 0, 0, 2];
        r.extend_from_slice(tid);
        for c in counts {
            for n in c {
                r.extend_from_slice(&n.to_be_bytes());
            }
        }
        r
    }

    #[tokio::test]
    async fn udp_scrape_round_trip() {
        let connects = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let (c, s) = (connects.clone(), seen.clone());
        let url = fake(move |b, _, _| {
            if is_connect(b) {
                c.fetch_add(1, Ordering::SeqCst);
                vec![connect_reply(&b[12..16], CID)]
            } else if b[8..12] == 2u32.to_be_bytes() {
                s.lock().unwrap().push(b.to_vec());
                // seeders, completed, leechers per hash, in order.
                vec![scrape_reply(&b[12..16], &[[5, 9, 7], [1, 2, 3]])]
            } else {
                vec![announce_reply(&b[12..16], &PEERS)]
            }
        })
        .await;
        let cl = client();
        let got = cl.scrape(&url, &[[0xaa; 20], [0xbb; 20]]).await.unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(
            (
                got[0].info_hash,
                got[0].complete,
                got[0].downloaded,
                got[0].incomplete
            ),
            ([0xaa; 20], 5, 9, 7)
        );
        assert_eq!(
            (
                got[1].info_hash,
                got[1].complete,
                got[1].downloaded,
                got[1].incomplete
            ),
            ([0xbb; 20], 1, 2, 3)
        );
        {
            let seen = seen.lock().unwrap();
            let b = &seen[0];
            assert_eq!(b.len(), 16 + 40);
            assert_eq!(&b[0..8], &CID.to_be_bytes());
            assert_eq!(&b[16..36], &[0xaa; 20]);
            assert_eq!(&b[36..56], &[0xbb; 20]);
        }
        // Announce and scrape share the connection id cache.
        cl.announce(&url, &request()).await.unwrap();
        assert_eq!(connects.load(Ordering::SeqCst), 1);

        // The parser: fewer entries than asked keep what is there; a
        // ragged tail, a wrong tid or an error action are errors.
        let tid = 7u32.to_be_bytes();
        let one = scrape_reply(&tid, &[[1, 2, 3]]);
        assert_eq!(parse_udp_scrape(&one, 7, 2).unwrap(), vec![(1, 2, 3)]);
        assert_eq!(parse_udp_scrape(&one, 7, 0).unwrap(), vec![]);
        let mut ragged = one.clone();
        ragged.push(0);
        assert!(parse_udp_scrape(&ragged, 7, 2).is_err());
        assert!(parse_udp_scrape(&one, 8, 2).is_err());
        assert!(matches!(
            parse_udp_scrape(&error_reply(&tid, "nope"), 7, 1),
            Err(TrackerError::Failure(m)) if m == "nope"
        ));
        assert!(parse_udp_scrape(&announce_reply(&tid, &[]), 7, 1).is_err());
    }
}
