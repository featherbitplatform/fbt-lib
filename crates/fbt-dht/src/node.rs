//! The DHT node runtime: one actor task owns the UDP sockets (one per
//! address family), the routing tables, tokens, peer store and rate
//! limiters. [`Dht`] is a cheap handle that talks to it over a channel.
//!
//! Every datagram is hostile and its source may be spoofed: undecodable or
//! oversized datagrams and malformed queries are dropped without a reply,
//! inbound queries pass a per-IP and a global rate limit, a response is
//! accepted only from the address its query went to, and only nodes that
//! answered a query of ours enter a table: an unknown node that queries us
//! is pinged first ([`MAX_VERIFY_PINGS`] at once) and enters when it answers.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fbt_core::udp::{DatagramHandler, DatagramSender};
use rand::Rng;
use rand::rngs::StdRng;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::id::{NodeId, bep42_id, canonical, canonical_addr, is_local};
use crate::krpc::{Body, MAX_DATAGRAM, Message, NodeInfo, Query, QueryMsg, Response, Want};
use crate::lookup::{self, Goal, PeerLookup};
use crate::rate::{MAX_IPS, RateLimiter};
use crate::server::{Server, version};
use crate::state::{DhtState, MAX_STATE_NODES};
use crate::store::PeerStore;
use crate::table::{Family, Insert, K, RoutingTable};
use crate::token::Tokens;

/// External-address votes kept (one per reporter IP).
pub(crate) const MAX_EXTERNAL_VOTES: usize = 64;
/// Distinct reporters that must agree on our external address.
const EXTERNAL_IP_QUORUM: usize = 3;
/// Inbound queries per second over all sources, on top of the per-IP
/// limit: spoofed sources cycling through the per-IP table cannot exceed it.
const GLOBAL_QUERY_RATE: u32 = 1000;
/// Commands buffered between the handles and the actor.
const CMD_QUEUE: usize = 256;
/// Transaction ids are 16 bits: never more queries in flight than half of them.
const MAX_TIDS: usize = 1 << 15;
/// Stats are published at most this often.
const STATS_INTERVAL: Duration = Duration::from_secs(1);
/// Peer-store expiry and bucket refresh.
const HOUSEKEEPING: Duration = Duration::from_secs(60);
/// Token secret rotation.
const TOKEN_ROTATION: Duration = Duration::from_secs(5 * 60);
/// A bucket is refreshed at most this often, answered or not.
const REFRESH_INTERVAL: Duration = Duration::from_secs(15 * 60);
/// Refresh lookups started per housekeeping tick and family.
const MAX_REFRESHES_PER_TICK: usize = 8;
/// Time allowed to resolve one bootstrap entry.
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(5);
/// Addresses kept from one resolved bootstrap entry.
const MAX_RESOLVED: usize = 8;
/// Bootstrap addresses contacted per attempt and family.
const MAX_BOOTSTRAP_SEEDS: usize = MAX_STATE_NODES + 64;
/// Bootstrap addresses queried at once.
const BOOTSTRAP_CONCURRENCY: usize = 16;
/// Pings in flight to unknown nodes that queried us (their address may be
/// spoofed until they answer); further ones are not pinged.
const MAX_VERIFY_PINGS: usize = 64;
/// Random draws for a free transaction id before a scan; at most half the
/// ids are in use ([`MAX_TIDS`]), so a draw fails at most half the time.
const TID_DRAWS: usize = 16;

/// Node configuration.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DhtConfig {
    /// IPv4 socket address; `None` disables IPv4.
    pub bind_v4: Option<SocketAddr>,
    /// IPv6 socket address (IPv6-only); `None` disables IPv6. A failed bind
    /// logs a warning and the node runs IPv4-only.
    pub bind_v6: Option<SocketAddr>,
    /// Bootstrap nodes as `host:port`; resolved at each attempt, malformed
    /// entries are skipped. Empty by default: nothing is contacted unless
    /// configured.
    pub bootstrap: Vec<String>,
    /// BEP 43: answer no queries and ask others to keep us out of tables.
    pub read_only: bool,
    /// A saved state: ids and known nodes for a fast rejoin.
    pub state: Option<DhtState>,
    /// How long a query waits for its answer.
    pub query_timeout: Duration,
    /// Inbound queries per second and source IP (burst: twice that).
    pub query_rate_per_ip: u32,
    /// BEP 42: keep nodes whose id does not match their IP out of tables.
    pub enforce_node_id: bool,
    /// Info hashes kept in the peer store.
    pub max_stored_torrents: usize,
    /// Peers kept per info hash.
    pub max_peers_per_torrent: usize,
    /// Our queries in flight at once; more give `Busy`.
    pub max_pending_queries: usize,
    /// First bootstrap retry delay (doubles per attempt).
    #[doc(hidden)]
    pub bootstrap_retry_base: Duration,
    /// Longest bootstrap retry delay.
    #[doc(hidden)]
    pub bootstrap_retry_max: Duration,
}

impl Default for DhtConfig {
    fn default() -> Self {
        DhtConfig {
            bind_v4: Some(SocketAddr::from(([0, 0, 0, 0], 6881))),
            bind_v6: Some(SocketAddr::from(([0u16; 8], 6881))),
            bootstrap: Vec::new(),
            read_only: false,
            state: None,
            query_timeout: Duration::from_secs(3),
            query_rate_per_ip: 5,
            enforce_node_id: false,
            max_stored_torrents: 2000,
            max_peers_per_torrent: 500,
            max_pending_queries: 1024,
            bootstrap_retry_base: Duration::from_secs(30),
            bootstrap_retry_max: Duration::from_secs(15 * 60),
        }
    }
}

/// A snapshot of the node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct DhtStats {
    /// Nodes in the IPv4 routing table.
    pub nodes_v4: usize,
    /// Nodes in the IPv6 routing table.
    pub nodes_v6: usize,
    /// Info hashes in the peer store.
    pub stored_info_hashes: usize,
    /// A routing table holds at least `K` (8) nodes.
    pub bootstrapped: bool,
    /// The bound socket addresses.
    pub local_addrs: Vec<SocketAddr>,
}

/// DHT failures.
#[derive(Debug, thiserror::Error)]
pub enum DhtError {
    /// The IPv4 socket could not be bound.
    #[error("cannot bind the DHT socket: {0}")]
    Bind(io::Error),
    /// No socket is configured or none could be bound.
    #[error("no DHT socket could be bound")]
    NoSocket,
    /// [`Dht::spawn_on`] got two sockets of one address family.
    #[error("two DHT sockets of the same address family")]
    DuplicateFamily,
    /// The node has shut down.
    #[error("the DHT node has shut down")]
    Shutdown,
    /// A saved state could not be read; names the offending part.
    #[error("invalid DHT state: {0}")]
    InvalidState(&'static str),
}

/// Why a query got no answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueryError {
    /// No answer within `query_timeout`.
    Timeout,
    /// The node answered with a KRPC error.
    Remote { code: i64 },
    /// `max_pending_queries` queries are already in flight.
    Busy,
    /// The node has shut down.
    Shutdown,
}

/// One family's routing view, handed to lookups.
#[derive(Debug, Clone)]
pub(crate) struct FamilyView {
    pub(crate) family: Family,
    pub(crate) own: NodeId,
    pub(crate) closest: Vec<NodeInfo>,
}

#[derive(Debug)]
pub(crate) enum Cmd {
    Query {
        addr: SocketAddr,
        query: Query,
        reply: oneshot::Sender<Result<Response, QueryError>>,
    },
    AddNode(SocketAddr),
    Closest {
        target: Option<NodeId>,
        reply: oneshot::Sender<Vec<FamilyView>>,
    },
    State(oneshot::Sender<DhtState>),
    /// A bootstrap attempt ended: the next is due a backoff from now.
    BootstrapDone,
    Shutdown(oneshot::Sender<()>),
}

/// The way lookups reach the actor: user lookups keep it alive, internal
/// ones (bootstrap, refresh) do not.
#[derive(Debug, Clone)]
pub(crate) enum Link {
    Strong(Dht),
    Weak(mpsc::WeakSender<Cmd>),
}

impl Link {
    async fn send(&self, cmd: Cmd) -> Result<(), QueryError> {
        let tx = match self {
            Link::Strong(d) => d.tx.clone(),
            Link::Weak(w) => w.upgrade().ok_or(QueryError::Shutdown)?,
        };
        tx.send(cmd).await.map_err(|_| QueryError::Shutdown)
    }

    pub(crate) async fn query(
        &self,
        addr: SocketAddr,
        query: Query,
    ) -> Result<Response, QueryError> {
        match self {
            Link::Strong(d) => d.query(addr, query).await,
            Link::Weak(_) => {
                let (reply, rx) = oneshot::channel();
                self.send(Cmd::Query { addr, query, reply }).await?;
                rx.await.unwrap_or(Err(QueryError::Shutdown))
            }
        }
    }

    pub(crate) async fn closest(
        &self,
        target: Option<NodeId>,
    ) -> Result<Vec<FamilyView>, QueryError> {
        let (reply, rx) = oneshot::channel();
        self.send(Cmd::Closest { target, reply }).await?;
        rx.await.map_err(|_| QueryError::Shutdown)
    }
}

/// A running DHT node. Clones share the node; it stops at
/// [`shutdown`](Dht::shutdown) or when the last handle is dropped.
#[derive(Debug, Clone)]
pub struct Dht {
    tx: mpsc::Sender<Cmd>,
    stats: watch::Receiver<DhtStats>,
    local: Arc<[SocketAddr]>,
}

/// Votes on our external address from the `ip` field of answers (BEP 42).
#[derive(Debug, Default)]
pub struct ExternalIpVotes {
    /// (reporter, reported address), oldest first; one entry per reporter.
    votes: VecDeque<(IpAddr, IpAddr)>,
    current: Option<IpAddr>,
}

impl ExternalIpVotes {
    /// An empty vote set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `reporter`'s view of our address. Returns a new, non-local
    /// address once it leads the held votes: at least
    /// [`EXTERNAL_IP_QUORUM`] distinct reporters agree on it and strictly
    /// more than on any other address. A minority, however many Sybils it
    /// has, never overrides the majority; a tie keeps the current address.
    pub fn vote(&mut self, reporter: IpAddr, ip: IpAddr) -> Option<IpAddr> {
        let ip = canonical(ip);
        let reporter = canonical(reporter);
        if is_local(ip) || ip.is_unspecified() {
            return None;
        }
        if let Some(v) = self.votes.iter_mut().find(|(r, _)| *r == reporter) {
            v.1 = ip;
        } else {
            while self.votes.len() >= MAX_EXTERNAL_VOTES {
                self.votes.pop_front();
            }
            self.votes.push_back((reporter, ip));
        }
        let leader = self.leader()?;
        if self.current == Some(leader) {
            return None;
        }
        self.current = Some(leader);
        Some(leader)
    }

    /// The address with at least [`EXTERNAL_IP_QUORUM`] votes and strictly
    /// more than the runner-up.
    fn leader(&self) -> Option<IpAddr> {
        let mut counts: HashMap<IpAddr, usize> = HashMap::new();
        for (_, v) in &self.votes {
            *counts.entry(*v).or_default() += 1;
        }
        let (best, n) = counts.iter().max_by_key(|(_, n)| **n)?;
        let runner_up = counts
            .iter()
            .filter(|(ip, _)| *ip != best)
            .map(|(_, n)| *n)
            .max()
            .unwrap_or(0);
        (*n >= EXTERNAL_IP_QUORUM && *n > runner_up).then_some(*best)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.votes.len()
    }
}

/// A new BEP 42 id for `ip` when `own` is not valid for it.
pub(crate) fn regenerate_for(own: NodeId, ip: IpAddr, rng: &mut impl Rng) -> Option<NodeId> {
    if own.is_valid_for(ip) {
        return None;
    }
    let mut r = [0u8; 1];
    rng.fill_bytes(&mut r);
    let [r] = r;
    Some(bep42_id(ip, r, rng))
}

/// The bootstrap retry delay after `attempt` failed attempts: `base`
/// doubling per attempt, capped at `max`.
pub(crate) fn backoff(base: Duration, max: Duration, attempt: u32) -> Duration {
    base.checked_mul(1u32.checked_shl(attempt).unwrap_or(u32::MAX))
        .unwrap_or(max)
        .min(max)
}

/// A non-blocking IPv4 socket.
fn bind_v4(addr: SocketAddr) -> io::Result<std::net::UdpSocket> {
    if !addr.is_ipv4() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "bind_v4 is not IPv4",
        ));
    }
    let s = std::net::UdpSocket::bind(addr)?;
    s.set_nonblocking(true)?;
    Ok(s)
}

/// A non-blocking IPv6-only socket, so an IPv4 socket can share its port.
fn bind_v6(addr: SocketAddr) -> io::Result<std::net::UdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};
    if !addr.is_ipv6() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "bind_v6 is not IPv6",
        ));
    }
    let s = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
    s.set_only_v6(true)?;
    s.set_nonblocking(true)?;
    s.bind(&addr.into())?;
    Ok(s.into())
}

/// The receiving half (registered with tokio) and a clone for sending.
/// Sends go straight to the non-blocking socket, so the actor never waits
/// on write readiness; a full send buffer drops the datagram, as UDP may.
fn split(s: std::net::UdpSocket) -> io::Result<(UdpSocket, std::net::UdpSocket)> {
    let send = s.try_clone()?;
    // the clone shares the file status on Unix but not necessarily on
    // Windows: never let a send block the actor
    send.set_nonblocking(true)?;
    Ok((UdpSocket::from_std(s)?, send))
}

/// A [`DatagramSender`] over a standalone non-blocking socket.
#[derive(Debug)]
struct StdSender {
    sock: std::net::UdpSocket,
    local: SocketAddr,
}

impl DatagramSender for StdSender {
    fn local_addr(&self) -> SocketAddr {
        self.local
    }

    fn send_to(&self, data: &[u8], to: SocketAddr) -> io::Result<usize> {
        self.sock.send_to(data, to)
    }
}

/// A queued datagram: its source and a copy of its bytes.
type InputDatagram = (SocketAddr, Box<[u8]>);

/// Datagrams [`DhtInput`] queues for the node; more are dropped.
pub const DHT_INPUT_QUEUE: usize = 1024;

/// Where a node on shared sockets receives its datagrams: register it as the
/// handler of the DHT packet kind. It copies each datagram into a bounded
/// queue and never blocks; a full queue drops the datagram.
#[derive(Debug, Clone)]
pub struct DhtInput {
    tx: mpsc::Sender<InputDatagram>,
}

impl DatagramHandler for DhtInput {
    fn handle(&self, from: SocketAddr, data: &[u8]) {
        // one byte past the maximum, so an oversized datagram is still
        // detected (and dropped) by the node
        let data = data.get(..=MAX_DATAGRAM).unwrap_or(data);
        let _ = self.tx.try_send((from, data.into()));
    }
}

/// Where the actor's datagrams come from: its own sockets (standalone) or
/// the [`DhtInput`] queue (shared sockets).
struct Inbound {
    v4: Option<UdpSocket>,
    v6: Option<UdpSocket>,
    input: Option<mpsc::Receiver<InputDatagram>>,
}

impl Dht {
    /// Binds the sockets and starts the node. A restored `state` provides
    /// the ids and the first nodes to contact; with neither `state` nor
    /// `bootstrap`, the node only waits to be contacted.
    pub async fn spawn(cfg: DhtConfig) -> Result<Dht, DhtError> {
        let sock_v4 = match cfg.bind_v4 {
            Some(a) => Some(bind_v4(a).and_then(split).map_err(DhtError::Bind)?),
            None => None,
        };
        let sock_v6 = match cfg.bind_v6.map(|a| bind_v6(a).and_then(split)) {
            Some(Ok(s)) => Some(s),
            Some(Err(e)) => {
                warn!(error = %e, "DHT: cannot bind the IPv6 socket, running IPv4-only");
                None
            }
            None => None,
        };
        if sock_v4.is_none() && sock_v6.is_none() {
            return Err(DhtError::NoSocket);
        }
        let (recv_v4, send_v4) = sock_v4.unzip();
        let (recv_v6, send_v6) = sock_v6.unzip();
        let wrap = |s: std::net::UdpSocket| -> Option<Arc<dyn DatagramSender>> {
            let local = s.local_addr().ok()?;
            Some(Arc::new(StdSender { sock: s, local }))
        };
        let send_v4 = send_v4.and_then(wrap);
        let send_v6 = send_v6.and_then(wrap);
        Ok(Self::start(
            cfg,
            send_v4,
            send_v6,
            Inbound {
                v4: recv_v4,
                v6: recv_v6,
                input: None,
            },
        ))
    }

    /// Starts a node on sockets owned by someone else, such as the shared
    /// UDP socket. It ignores `bind_v4` and `bind_v6` and takes one sender
    /// per address family, by its `local_addr()`. Received datagrams reach
    /// the node through the returned [`DhtInput`]. Fails with
    /// [`DhtError::NoSocket`] without senders and with
    /// [`DhtError::DuplicateFamily`] for two of one family.
    pub async fn spawn_on(
        cfg: DhtConfig,
        sockets: Vec<Arc<dyn DatagramSender>>,
    ) -> Result<(Dht, DhtInput), DhtError> {
        let (mut v4, mut v6) = (None, None);
        for s in sockets {
            let slot = if s.local_addr().is_ipv4() {
                &mut v4
            } else {
                &mut v6
            };
            if slot.replace(s).is_some() {
                return Err(DhtError::DuplicateFamily);
            }
        }
        if v4.is_none() && v6.is_none() {
            return Err(DhtError::NoSocket);
        }
        let (in_tx, in_rx) = mpsc::channel(DHT_INPUT_QUEUE);
        let dht = Self::start(
            cfg,
            v4,
            v6,
            Inbound {
                v4: None,
                v6: None,
                input: Some(in_rx),
            },
        );
        Ok((dht, DhtInput { tx: in_tx }))
    }

    /// Everything past the sockets, shared by both entry points.
    fn start(
        cfg: DhtConfig,
        send_v4: Option<Arc<dyn DatagramSender>>,
        send_v6: Option<Arc<dyn DatagramSender>>,
        inbound: Inbound,
    ) -> Dht {
        let local: Vec<SocketAddr> = [&send_v4, &send_v6]
            .into_iter()
            .flatten()
            .map(|s| s.local_addr())
            .collect();
        let (tx, cmd_rx) = mpsc::channel(CMD_QUEUE);
        let stats = DhtStats {
            local_addrs: local.clone(),
            ..DhtStats::default()
        };
        let (stats_tx, stats_rx) = watch::channel(stats);
        let actor = Actor::new(
            cfg,
            send_v4,
            send_v6,
            tx.downgrade(),
            stats_tx,
            Instant::now(),
        );
        tokio::spawn(actor.run(cmd_rx, inbound));
        Dht {
            tx,
            stats: stats_rx,
            local: local.into(),
        }
    }

    /// The bound socket addresses (IPv4 first).
    pub fn local_addrs(&self) -> Vec<SocketAddr> {
        self.local.to_vec()
    }

    /// Pings `addr` and adds it to the routing table if it answers.
    pub fn add_node(&self, addr: SocketAddr) {
        let _ = self.tx.try_send(Cmd::AddNode(addr));
    }

    /// The latest published stats. They are published when they change, at
    /// most once a second.
    pub fn stats(&self) -> DhtStats {
        self.stats.borrow().clone()
    }

    /// A receiver of every published [`DhtStats`].
    pub fn subscribe_stats(&self) -> watch::Receiver<DhtStats> {
        self.stats.clone()
    }

    /// Stops the node and closes its sockets. Idempotent.
    pub async fn shutdown(&self) {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(Cmd::Shutdown(reply)).await.is_ok() {
            let _ = rx.await;
        }
    }

    /// Sends `q` to `addr` and waits for the answer.
    pub(crate) async fn query(&self, addr: SocketAddr, q: Query) -> Result<Response, QueryError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::Query {
                addr,
                query: q,
                reply,
            })
            .await
            .map_err(|_| QueryError::Shutdown)?;
        rx.await.unwrap_or(Err(QueryError::Shutdown))
    }

    /// Looks up peers for `info_hash`, one lookup per bound family.
    pub fn get_peers(&self, info_hash: [u8; 20]) -> PeerLookup {
        PeerLookup::start(Link::Strong(self.clone()), Goal::GetPeers, info_hash)
    }

    /// Looks up peers for `info_hash` like [`get_peers`](Dht::get_peers),
    /// then announces us on `port` (`None`: the DHT socket's port, BEP 5
    /// `implied_port`) to the `K` closest nodes that gave a token.
    pub fn announce(&self, info_hash: [u8; 20], port: Option<u16>) -> PeerLookup {
        PeerLookup::start(Link::Strong(self.clone()), Goal::Announce(port), info_hash)
    }

    /// Our ids and the nodes in our tables, to restore with
    /// [`DhtConfig::state`].
    pub async fn state(&self) -> Result<DhtState, DhtError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::State(reply))
            .await
            .map_err(|_| DhtError::Shutdown)?;
        rx.await.map_err(|_| DhtError::Shutdown)
    }
}

#[derive(Debug)]
struct Pending {
    addr: SocketAddr,
    deadline: Instant,
    reply: Option<oneshot::Sender<Result<Response, QueryError>>>,
    /// The ping this query is, if any (de-duplicated by node id).
    pinging: Option<Ping>,
}

/// Why a node is pinged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ping {
    /// A questionable table entry, before a newcomer may replace it.
    Questionable(NodeId),
    /// An unknown node that queried us: it enters the table by answering.
    Verify(NodeId),
}

impl Ping {
    fn id(self) -> NodeId {
        match self {
            Ping::Questionable(id) | Ping::Verify(id) => id,
        }
    }
}

struct Actor {
    read_only: bool,
    enforce_node_id: bool,
    query_timeout: Duration,
    max_pending: usize,
    bootstrap: Vec<String>,
    restored: Vec<SocketAddr>,
    retry_base: Duration,
    retry_max: Duration,

    sock_v4: Option<Arc<dyn DatagramSender>>,
    sock_v6: Option<Arc<dyn DatagramSender>>,
    table_v4: Option<RoutingTable>,
    table_v6: Option<RoutingTable>,
    tokens: Tokens,
    store: PeerStore,
    rate: RateLimiter,
    global_rate: RateLimiter,
    ext_v4: ExternalIpVotes,
    ext_v6: ExternalIpVotes,
    rng: StdRng,

    pending: HashMap<u16, Pending>,
    /// (deadline, tid) in send order; all queries share one timeout, so
    /// this is sorted.
    timeouts: VecDeque<(Instant, u16)>,
    pinging: HashSet<NodeId>,
    /// [`Ping::Verify`] pings in flight.
    verifying: usize,
    refreshed: HashMap<(u8, usize), Instant>,

    link: mpsc::WeakSender<Cmd>,
    stats_tx: watch::Sender<DhtStats>,
    local: Vec<SocketAddr>,
    last_publish: Option<Instant>,
    stats_due: Option<Instant>,

    boot_started: bool,
    boot_attempt: u32,
    boot_due: Option<Instant>,
    /// The running bootstrap attempt; a new one waits until it is done.
    boot_task: Option<tokio::task::JoinHandle<()>>,
    next_housekeeping: Instant,
    next_rotation: Instant,
}

/// Longest delay the actor schedules; absurd configured durations are cut
/// to it instead of overflowing `Instant`.
const MAX_DELAY: Duration = Duration::from_secs(365 * 24 * 3600);

fn after(now: Instant, d: Duration) -> Instant {
    now.checked_add(d.min(MAX_DELAY)).unwrap_or(now)
}

async fn recv(sock: &Option<UdpSocket>, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
    match sock {
        Some(s) => s.recv_from(buf).await,
        None => std::future::pending().await,
    }
}

async fn recv_input(input: &mut Option<mpsc::Receiver<InputDatagram>>) -> Option<InputDatagram> {
    match input {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

enum Event {
    Datagram(io::Result<(usize, SocketAddr)>, Family),
    Input(Option<InputDatagram>),
    Cmd(Option<Cmd>),
    Timer,
}

fn tid_of(tid: &[u8]) -> Option<u16> {
    <[u8; 2]>::try_from(tid).ok().map(u16::from_be_bytes)
}

fn family_key(f: Family) -> u8 {
    match f {
        Family::V4 => 4,
        Family::V6 => 6,
    }
}

impl Actor {
    fn new(
        cfg: DhtConfig,
        sock_v4: Option<Arc<dyn DatagramSender>>,
        sock_v6: Option<Arc<dyn DatagramSender>>,
        link: mpsc::WeakSender<Cmd>,
        stats_tx: watch::Sender<DhtStats>,
        now: Instant,
    ) -> Actor {
        let mut rng: StdRng = rand::make_rng();
        let state = cfg.state.unwrap_or(DhtState {
            id_v4: None,
            id_v6: None,
            nodes_v4: Vec::new(),
            nodes_v6: Vec::new(),
        });
        let table_v4 = sock_v4.as_ref().map(|_| {
            let id = state.id_v4.unwrap_or_else(|| NodeId::random(&mut rng));
            RoutingTable::new(id, Family::V4)
        });
        let table_v6 = sock_v6.as_ref().map(|_| {
            let id = state.id_v6.unwrap_or_else(|| NodeId::random(&mut rng));
            RoutingTable::new(id, Family::V6)
        });
        let mut restored: Vec<SocketAddr> = Vec::new();
        if sock_v4.is_some() {
            restored.extend(state.nodes_v4.iter().take(MAX_STATE_NODES).copied());
        }
        if sock_v6.is_some() {
            restored.extend(state.nodes_v6.iter().take(MAX_STATE_NODES).copied());
        }
        let has_seeds = !cfg.bootstrap.is_empty() || !restored.is_empty();
        let local = stats_tx.borrow().local_addrs.clone();
        Actor {
            read_only: cfg.read_only,
            enforce_node_id: cfg.enforce_node_id,
            query_timeout: cfg.query_timeout,
            max_pending: cfg.max_pending_queries.min(MAX_TIDS),
            bootstrap: cfg.bootstrap,
            restored,
            retry_base: cfg.bootstrap_retry_base,
            retry_max: cfg.bootstrap_retry_max,
            sock_v4,
            sock_v6,
            table_v4,
            table_v6,
            tokens: Tokens::new(&mut rng),
            store: PeerStore::new(cfg.max_stored_torrents, cfg.max_peers_per_torrent),
            rate: RateLimiter::new(cfg.query_rate_per_ip, MAX_IPS),
            global_rate: RateLimiter::new(GLOBAL_QUERY_RATE.max(cfg.query_rate_per_ip), 1),
            ext_v4: ExternalIpVotes::new(),
            ext_v6: ExternalIpVotes::new(),
            rng,
            pending: HashMap::new(),
            timeouts: VecDeque::new(),
            pinging: HashSet::new(),
            verifying: 0,
            refreshed: HashMap::new(),
            link,
            stats_tx,
            local,
            last_publish: None,
            stats_due: None,
            boot_started: false,
            boot_attempt: 0,
            // the first attempt runs as soon as the actor starts
            boot_due: has_seeds.then_some(now),
            boot_task: None,
            next_housekeeping: after(now, HOUSEKEEPING),
            next_rotation: after(now, TOKEN_ROTATION),
        }
    }

    async fn run(mut self, mut cmd_rx: mpsc::Receiver<Cmd>, inbound: Inbound) {
        let Inbound {
            v4: sock_v4,
            v6: sock_v6,
            input: mut input_rx,
        } = inbound;
        let mut buf_v4 = vec![0u8; MAX_DATAGRAM + 1];
        let mut buf_v6 = vec![0u8; MAX_DATAGRAM + 1];
        let mut stop = None;
        loop {
            let wake = tokio::time::Instant::from_std(self.next_wake());
            let event = tokio::select! {
                r = recv(&sock_v4, &mut buf_v4) => Event::Datagram(r, Family::V4),
                r = recv(&sock_v6, &mut buf_v6) => Event::Datagram(r, Family::V6),
                d = recv_input(&mut input_rx) => Event::Input(d),
                c = cmd_rx.recv() => Event::Cmd(c),
                () = tokio::time::sleep_until(wake) => Event::Timer,
            };
            // the one place the actor reads the clock
            let now = Instant::now();
            match event {
                Event::Datagram(Ok((n, from)), f) => {
                    let buf = match f {
                        Family::V4 => &buf_v4,
                        Family::V6 => &buf_v6,
                    };
                    if n <= MAX_DATAGRAM
                        && let Some(bytes) = buf.get(..n)
                    {
                        self.on_datagram(bytes, from, now);
                    }
                }
                // e.g. ICMP port unreachable surfacing as a reset on Windows
                Event::Datagram(Err(e), _) => debug!(error = %e, "DHT: recv failed"),
                Event::Input(Some((from, data))) => {
                    if data.len() <= MAX_DATAGRAM {
                        self.on_datagram(&data, from, now);
                    }
                }
                // every DhtInput is gone: nothing more will arrive
                Event::Input(None) => input_rx = None,
                Event::Cmd(None) => break,
                Event::Cmd(Some(Cmd::Shutdown(reply))) => {
                    stop = Some(reply);
                    break;
                }
                Event::Cmd(Some(cmd)) => self.on_cmd(cmd, now),
                Event::Timer => self.on_timer(now),
            }
            self.publish_stats(now);
        }
        for (_, p) in self.pending.drain() {
            if let Some(reply) = p.reply {
                let _ = reply.send(Err(QueryError::Shutdown));
            }
        }
        drop(cmd_rx);
        drop(self);
        drop((sock_v4, sock_v6, input_rx));
        if let Some(reply) = stop {
            let _ = reply.send(());
        }
    }

    fn next_wake(&self) -> Instant {
        [
            self.timeouts.front().map(|t| t.0),
            Some(self.next_housekeeping),
            Some(self.next_rotation),
            self.stats_due,
            self.boot_due,
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(self.next_housekeeping)
    }

    fn table(&self, f: Family) -> Option<&RoutingTable> {
        match f {
            Family::V4 => self.table_v4.as_ref(),
            Family::V6 => self.table_v6.as_ref(),
        }
    }

    fn table_mut(&mut self, f: Family) -> Option<&mut RoutingTable> {
        match f {
            Family::V4 => self.table_v4.as_mut(),
            Family::V6 => self.table_v6.as_mut(),
        }
    }

    fn tables(&self) -> impl Iterator<Item = &RoutingTable> {
        self.table_v4.iter().chain(self.table_v6.iter())
    }

    /// Sends one datagram; `false` when it did not leave this host.
    fn send(&self, to: SocketAddr, bytes: &[u8]) -> bool {
        let sock = if to.is_ipv4() {
            &self.sock_v4
        } else {
            &self.sock_v6
        };
        let Some(s) = sock else {
            return false;
        };
        match s.send_to(bytes, to) {
            Ok(_) => true,
            Err(e) => {
                debug!(error = %e, %to, "DHT: send failed");
                false
            }
        }
    }

    fn on_datagram(&mut self, bytes: &[u8], raw_from: SocketAddr, now: Instant) {
        let Ok(msg) = Message::decode(bytes) else {
            return;
        };
        let from = canonical_addr(raw_from);
        match msg.body {
            Body::Query(q) => self.on_query(raw_from, from, &msg.tid, &q, now),
            Body::Response(r) => {
                if let Some(p) = self.take_pending(&msg.tid, from) {
                    self.learn(
                        NodeInfo {
                            id: r.id,
                            addr: from,
                        },
                        now,
                    );
                    if let Some(ip) = msg.ip {
                        self.vote(from.ip(), ip.ip());
                    }
                    if let Some(reply) = p.reply {
                        let _ = reply.send(Ok(r));
                    }
                }
            }
            Body::Error { code, .. } => {
                if let Some(p) = self.take_pending(&msg.tid, from)
                    && let Some(reply) = p.reply
                {
                    let _ = reply.send(Err(QueryError::Remote { code }));
                }
            }
        }
    }

    /// The pending query `tid` if it went to `from`.
    fn take_pending(&mut self, tid: &[u8], from: SocketAddr) -> Option<Pending> {
        let tid = tid_of(tid)?;
        if self.pending.get(&tid)?.addr != from {
            return None;
        }
        let p = self.pending.remove(&tid)?;
        self.ping_done(p.pinging);
        Some(p)
    }

    /// A ping got its answer or timed out: its de-duplication slot is free.
    fn ping_done(&mut self, ping: Option<Ping>) {
        let Some(ping) = ping else {
            return;
        };
        self.pinging.remove(&ping.id());
        if let Ping::Verify(_) = ping {
            self.verifying = self.verifying.saturating_sub(1);
        }
    }

    fn on_query(
        &mut self,
        raw_from: SocketAddr,
        from: SocketAddr,
        tid: &[u8],
        q: &QueryMsg,
        now: Instant,
    ) {
        if self.read_only {
            return;
        }
        // per IP first: a flood from one source must not drain the global
        // budget everyone else shares
        if !self.rate.allow(from.ip(), now)
            || !self
                .global_rate
                .allow(IpAddr::V4(Ipv4Addr::UNSPECIFIED), now)
        {
            return;
        }
        let mut server = Server {
            table_v4: self.table_v4.as_ref(),
            table_v6: self.table_v6.as_ref(),
            tokens: &self.tokens,
            store: &mut self.store,
        };
        let Some(out) = server.answer(from, tid, q, now, &mut self.rng) else {
            return;
        };
        let _ = self.send(raw_from, &out);
        // BEP 43: read-only nodes are answered but never stored
        if !q.read_only {
            self.heard_from(
                NodeInfo {
                    id: q.id,
                    addr: from,
                },
                now,
            );
        }
    }

    /// `n` queried us. Its source address may be spoofed: a node already in
    /// the table at that address is refreshed, any other is pinged and
    /// enters the table only if it answers (see [`RoutingTable::insert`]).
    fn heard_from(&mut self, n: NodeInfo, now: Instant) {
        let Some(table) = self.table(Family::of(&n.addr)) else {
            return;
        };
        if table.contains(&n) {
            self.learn(n, now);
            return;
        }
        if self.enforce_node_id && !n.id.is_valid_for(n.addr.ip()) {
            return;
        }
        if self.verifying >= MAX_VERIFY_PINGS || !self.pinging.insert(n.id) {
            return;
        }
        if self
            .send_query(n.addr, Query::Ping, None, Some(Ping::Verify(n.id)), now)
            .is_ok()
        {
            self.verifying = self.verifying.saturating_add(1);
        } else {
            self.pinging.remove(&n.id);
        }
    }

    /// `n` answered a query of ours at its address (or is a table entry
    /// that queried us again from it): try it in its table.
    fn learn(&mut self, n: NodeInfo, now: Instant) {
        if self.enforce_node_id && !n.id.is_valid_for(n.addr.ip()) {
            return;
        }
        let Some(table) = self.table_mut(Family::of(&n.addr)) else {
            return;
        };
        if let Insert::Pending { ping } = table.insert(n, now)
            && self.pinging.insert(ping.id)
            && self
                .send_query(
                    ping.addr,
                    Query::Ping,
                    None,
                    Some(Ping::Questionable(ping.id)),
                    now,
                )
                .is_err()
        {
            self.pinging.remove(&ping.id);
        }
    }

    fn vote(&mut self, reporter: IpAddr, ip: IpAddr) {
        let ip = canonical(ip);
        // an IPv4 reporter can only see our IPv4 address, and vice versa
        if ip.is_ipv4() != canonical(reporter).is_ipv4() {
            return;
        }
        let family = if ip.is_ipv4() { Family::V4 } else { Family::V6 };
        let ext = match family {
            Family::V4 => &mut self.ext_v4,
            Family::V6 => &mut self.ext_v6,
        };
        let Some(external) = ext.vote(reporter, ip) else {
            return;
        };
        let Some(own) = self.table(family).map(RoutingTable::own_id) else {
            return;
        };
        info!(%external, "DHT: external address");
        if let Some(id) = regenerate_for(own, external, &mut self.rng)
            && let Some(t) = self.table_mut(family)
        {
            info!(%id, "DHT: new node id for the external address (BEP 42)");
            t.reset_own(id);
            // bucket indices are relative to our id: the pacing restarts
            let key = family_key(family);
            self.refreshed.retain(|(f, _), _| *f != key);
        }
    }

    /// Whether a query to `addr` can be sent now.
    fn can_send(&self, addr: SocketAddr) -> Result<(), QueryError> {
        if self.table(Family::of(&canonical_addr(addr))).is_none() {
            // no socket of that family: it can never answer
            return Err(QueryError::Timeout);
        }
        if self.pending.len() >= self.max_pending {
            return Err(QueryError::Busy);
        }
        Ok(())
    }

    /// Sends a query; on `Err` nothing is pending (and `reply`, if any, has
    /// been answered for a local send failure).
    fn send_query(
        &mut self,
        addr: SocketAddr,
        query: Query,
        reply: Option<oneshot::Sender<Result<Response, QueryError>>>,
        pinging: Option<Ping>,
        now: Instant,
    ) -> Result<(), QueryError> {
        self.can_send(addr)?;
        let addr = canonical_addr(addr);
        let Some(own) = self.table(Family::of(&addr)).map(RoutingTable::own_id) else {
            return Err(QueryError::Timeout);
        };
        let Some(tid) = self.free_tid() else {
            return Err(QueryError::Busy);
        };
        let msg = Message {
            tid: tid.to_be_bytes().to_vec(),
            ip: None,
            version: Some(version()),
            body: Body::Query(QueryMsg {
                id: own,
                query,
                want: Want::default(),
                read_only: self.read_only,
            }),
        };
        if !self.send(addr, &msg.encode()) {
            // a local failure says nothing about the node: no pending
            // entry, so its timeout cannot mark a healthy node failed
            if let Some(reply) = reply {
                let _ = reply.send(Err(QueryError::Timeout));
            }
            return Err(QueryError::Timeout);
        }
        let deadline = after(now, self.query_timeout);
        self.pending.insert(
            tid,
            Pending {
                addr,
                deadline,
                reply,
                pinging,
            },
        );
        self.timeouts.push_back((deadline, tid));
        Ok(())
    }

    /// A random transaction id not in use, so ids are not guessable
    /// off-path: a few random draws, then a scan from a random start.
    fn free_tid(&mut self) -> Option<u16> {
        let mut draw = || {
            let mut b = [0u8; 2];
            self.rng.fill_bytes(&mut b);
            u16::from_be_bytes(b)
        };
        for _ in 0..TID_DRAWS {
            let tid = draw();
            if !self.pending.contains_key(&tid) {
                return Some(tid);
            }
        }
        let start = draw();
        (0..=u16::MAX)
            .map(|i| start.wrapping_add(i))
            .find(|t| !self.pending.contains_key(t))
    }

    fn on_cmd(&mut self, cmd: Cmd, now: Instant) {
        match cmd {
            Cmd::Query { addr, query, reply } => match self.can_send(addr) {
                Ok(()) => {
                    let _ = self.send_query(addr, query, Some(reply), None, now);
                }
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
            },
            Cmd::AddNode(addr) => {
                let _ = self.send_query(addr, Query::Ping, None, None, now);
            }
            Cmd::Closest { target, reply } => {
                let views = self
                    .tables()
                    .map(|t| FamilyView {
                        family: t.family(),
                        own: t.own_id(),
                        closest: t.closest(&target.unwrap_or(t.own_id()), K),
                    })
                    .collect();
                let _ = reply.send(views);
            }
            Cmd::State(reply) => {
                let nodes = |t: Option<&RoutingTable>| -> Vec<SocketAddr> {
                    t.map(|t| t.nodes().map(|n| n.addr).take(MAX_STATE_NODES).collect())
                        .unwrap_or_default()
                };
                let _ = reply.send(DhtState {
                    id_v4: self.table_v4.as_ref().map(RoutingTable::own_id),
                    id_v6: self.table_v6.as_ref().map(RoutingTable::own_id),
                    nodes_v4: nodes(self.table_v4.as_ref()),
                    nodes_v6: nodes(self.table_v6.as_ref()),
                });
            }
            Cmd::BootstrapDone => {
                self.boot_due = Some(after(
                    now,
                    backoff(self.retry_base, self.retry_max, self.boot_attempt),
                ));
            }
            Cmd::Shutdown(_) => {}
        }
    }

    fn on_timer(&mut self, now: Instant) {
        self.expire_queries(now);
        if now >= self.next_housekeeping {
            self.next_housekeeping = after(now, HOUSEKEEPING);
            self.store.expire(now);
            self.refresh(now);
        }
        if now >= self.next_rotation {
            self.next_rotation = after(now, TOKEN_ROTATION);
            self.tokens.rotate(&mut self.rng);
        }
        if let Some(due) = self.boot_due
            && now >= due
        {
            if self.boot_task.as_ref().is_some_and(|t| !t.is_finished()) {
                // the last attempt is still running: no overlap; look again
                // soon, so the next starts shortly after it ends
                self.boot_due = Some(after(now, self.retry_base.min(self.query_timeout)));
            } else if !self.boot_started {
                self.boot_started = true;
                self.start_bootstrap();
            } else if self.tables().all(|t| t.len() == 0) {
                self.boot_attempt = self.boot_attempt.saturating_add(1);
                self.start_bootstrap();
            } else {
                self.boot_due = None;
            }
        }
    }

    fn expire_queries(&mut self, now: Instant) {
        while let Some(&(deadline, tid)) = self.timeouts.front() {
            if deadline > now {
                break;
            }
            self.timeouts.pop_front();
            if self
                .pending
                .get(&tid)
                .is_none_or(|p| p.deadline != deadline)
            {
                continue;
            }
            let Some(p) = self.pending.remove(&tid) else {
                continue;
            };
            self.ping_done(p.pinging);
            if let Some(t) = self.table_mut(Family::of(&p.addr))
                && let Some(id) = t.id_at(&p.addr)
            {
                t.failed(&id);
            }
            if let Some(reply) = p.reply {
                let _ = reply.send(Err(QueryError::Timeout));
            }
        }
    }

    /// Starts a bootstrap attempt and schedules the next one.
    fn start_bootstrap(&mut self) {
        // the next attempt is due a backoff after this one ends
        // ([`Cmd::BootstrapDone`]), not after it started
        self.boot_due = None;
        let views: Vec<(Family, NodeId)> =
            self.tables().map(|t| (t.family(), t.own_id())).collect();
        let link = Link::Weak(self.link.clone());
        let (bootstrap_link, entries, restored) =
            (link.clone(), self.bootstrap.clone(), self.restored.clone());
        self.boot_task = Some(tokio::spawn(async move {
            bootstrap(bootstrap_link, entries, restored, views).await;
            let _ = link.send(Cmd::BootstrapDone).await;
        }));
    }

    /// One `find_node` lookup per stale or empty bucket, each bucket at most
    /// once per [`REFRESH_INTERVAL`].
    fn refresh(&mut self, now: Instant) {
        for f in [Family::V4, Family::V6] {
            let Some(t) = self.table(f) else { continue };
            let own = t.own_id();
            let due: Vec<usize> = t
                .refresh_candidates(now)
                .into_iter()
                .filter(|i| {
                    self.refreshed
                        .get(&(family_key(f), *i))
                        .is_none_or(|last| now.saturating_duration_since(*last) >= REFRESH_INTERVAL)
                })
                .take(MAX_REFRESHES_PER_TICK)
                .collect();
            for i in due {
                self.refreshed.insert((family_key(f), i), now);
                let t = match f {
                    Family::V4 => self.table_v4.as_ref(),
                    Family::V6 => self.table_v6.as_ref(),
                };
                let Some(t) = t else { continue };
                let target = t.random_id_in_bucket(i, &mut self.rng);
                let seeds = t.closest(&target, K);
                tokio::spawn(lookup::run(
                    Link::Weak(self.link.clone()),
                    f,
                    own,
                    target,
                    seeds,
                    Goal::FindNode,
                    None,
                ));
            }
        }
    }

    fn current_stats(&self) -> DhtStats {
        let len = |t: &Option<RoutingTable>| t.as_ref().map_or(0, RoutingTable::len);
        DhtStats {
            nodes_v4: len(&self.table_v4),
            nodes_v6: len(&self.table_v6),
            stored_info_hashes: self.store.torrents(),
            bootstrapped: self.tables().any(|t| t.len() >= K),
            local_addrs: self.local.clone(),
        }
    }

    /// Publishes changed stats, at most once per [`STATS_INTERVAL`].
    fn publish_stats(&mut self, now: Instant) {
        let cur = self.current_stats();
        if *self.stats_tx.borrow() == cur {
            self.stats_due = None;
            return;
        }
        match self.last_publish {
            Some(last) if now < after(last, STATS_INTERVAL) => {
                self.stats_due = Some(after(last, STATS_INTERVAL));
            }
            _ => {
                self.stats_tx.send_replace(cur);
                self.last_publish = Some(now);
                self.stats_due = None;
            }
        }
    }
}
/// One bootstrap attempt: resolve the entries, ask every seed (and every
/// restored node) for nodes near our id, then run a `find_node` lookup for
/// our id from what they returned. Nodes enter the tables only by answering.
async fn bootstrap(
    link: Link,
    entries: Vec<String>,
    restored: Vec<SocketAddr>,
    views: Vec<(Family, NodeId)>,
) {
    let mut routers = Vec::new();
    for e in &entries {
        match tokio::time::timeout(RESOLVE_TIMEOUT, tokio::net::lookup_host(e.as_str())).await {
            Ok(Ok(found)) => routers.extend(found.take(MAX_RESOLVED)),
            Ok(Err(err)) => debug!(entry = %e, error = %err, "DHT: bootstrap entry skipped"),
            Err(_) => debug!(entry = %e, "DHT: bootstrap entry did not resolve in time"),
        }
    }
    let addrs = seed_order(routers, restored);
    let mut set = JoinSet::new();
    for (family, own) in views {
        let mut seen = HashSet::new();
        let seeds: Vec<SocketAddr> = addrs
            .iter()
            .map(|a| canonical_addr(*a))
            .filter(|a| Family::of(a) == family && seen.insert(*a))
            .take(MAX_BOOTSTRAP_SEEDS)
            .collect();
        if seeds.is_empty() {
            continue;
        }
        set.spawn(bootstrap_family(link.clone(), family, own, seeds));
    }
    while set.join_next().await.is_some() {}
}

/// The configured routers first, then the restored nodes: a saved state
/// full of dead nodes must not delay the first router query by whole
/// query timeouts.
fn seed_order(mut routers: Vec<SocketAddr>, restored: Vec<SocketAddr>) -> Vec<SocketAddr> {
    routers.extend(restored);
    routers
}

async fn bootstrap_family(link: Link, family: Family, own: NodeId, seeds: Vec<SocketAddr>) {
    let mut found: Vec<NodeInfo> = Vec::new();
    for chunk in seeds.chunks(BOOTSTRAP_CONCURRENCY) {
        let mut set = JoinSet::new();
        for a in chunk.iter().copied() {
            let link = link.clone();
            set.spawn(async move { (a, link.query(a, Query::FindNode { target: own }).await) });
        }
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((a, Ok(r))) => {
                    let nodes = match family {
                        Family::V4 => &r.nodes,
                        Family::V6 => &r.nodes6,
                    };
                    found.extend(lookup::usable(a, family, own, nodes));
                }
                Ok((_, Err(QueryError::Shutdown))) => return,
                _ => {}
            }
        }
    }
    let mut seeds = found;
    if let Ok(views) = link.closest(Some(own)).await {
        for v in views.into_iter().filter(|v| v.family == family) {
            seeds.extend(v.closest);
        }
    }
    lookup::run(link, family, own, own, seeds, Goal::FindNode, None).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::krpc::{Body, Message, QueryMsg, Want};
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use std::net::{IpAddr, SocketAddr};
    use std::time::Duration;
    use tokio::net::UdpSocket;
    use tokio::time::timeout;

    fn cfg() -> DhtConfig {
        DhtConfig {
            bind_v4: Some("127.0.0.1:0".parse().unwrap()),
            bind_v6: None,
            query_timeout: Duration::from_millis(500),
            ..DhtConfig::default()
        }
    }

    async fn spawn(c: DhtConfig) -> (Dht, SocketAddr) {
        let d = Dht::spawn(c).await.unwrap();
        let a = d.local_addrs()[0];
        (d, a)
    }

    async fn raw() -> UdpSocket {
        UdpSocket::bind("127.0.0.1:0").await.unwrap()
    }

    fn id(b: u8) -> NodeId {
        NodeId([b; 20])
    }

    fn query_bytes(tid: &[u8], from: NodeId, query: Query, read_only: bool) -> Vec<u8> {
        Message {
            tid: tid.to_vec(),
            ip: None,
            version: None,
            body: Body::Query(QueryMsg {
                id: from,
                query,
                want: Want::default(),
                read_only,
            }),
        }
        .encode()
    }

    /// The next decodable datagram within `ms`.
    async fn recv(sock: &UdpSocket, ms: u64) -> Option<(Message, SocketAddr)> {
        let mut buf = [0u8; 2048];
        timeout(Duration::from_millis(ms), async {
            loop {
                let (n, from) = sock.recv_from(&mut buf).await.unwrap();
                if let Ok(m) = Message::decode(&buf[..n]) {
                    return (m, from);
                }
            }
        })
        .await
        .ok()
    }

    /// Sends `query` and returns the answer, skipping the node's own
    /// queries (its verification ping of us).
    async fn ask(sock: &UdpSocket, to: SocketAddr, query: Query) -> Option<Message> {
        sock.send_to(&query_bytes(b"aa", id(1), query, false), to)
            .await
            .unwrap();
        recv_answer(sock, 1000).await
    }

    /// The next response or error within `ms`, skipping queries.
    async fn recv_answer(sock: &UdpSocket, ms: u64) -> Option<Message> {
        let end = tokio::time::Instant::now() + Duration::from_millis(ms);
        loop {
            let left = end.saturating_duration_since(tokio::time::Instant::now());
            let (m, _) = recv(sock, left.as_millis() as u64).await?;
            if !matches!(m.body, Body::Query(_)) {
                return Some(m);
            }
        }
    }

    fn response(m: Message) -> Response {
        match m.body {
            Body::Response(r) => r,
            other => panic!("not a response: {other:?}"),
        }
    }

    /// Waits for the node's next query on `sock` and answers it as `own`.
    async fn answer_ping(sock: &UdpSocket, own: NodeId) {
        let (m, from) = recv(sock, 2000).await.expect("a ping from the node");
        assert!(matches!(m.body, Body::Query(_)), "{m:?}");
        let r = Message {
            tid: m.tid,
            ip: None,
            version: None,
            body: Body::Response(Response {
                id: own,
                nodes: vec![],
                nodes6: vec![],
                values: vec![],
                token: None,
            }),
        };
        sock.send_to(&r.encode(), from).await.unwrap();
    }

    /// A raw node that answers every ping with `own`.
    async fn responder(own: NodeId) -> SocketAddr {
        let sock = raw().await;
        let addr = sock.local_addr().unwrap();
        tokio::spawn(async move {
            while let Some((m, from)) = recv(&sock, 60_000).await {
                if let Body::Query(_) = m.body {
                    let r = Message {
                        tid: m.tid,
                        ip: None,
                        version: None,
                        body: Body::Response(Response {
                            id: own,
                            nodes: vec![],
                            nodes6: vec![],
                            values: vec![],
                            token: None,
                        }),
                    };
                    let _ = sock.send_to(&r.encode(), from).await;
                }
            }
        });
        addr
    }

    #[tokio::test]
    async fn answers_ping_with_id_and_requester_ip() {
        let (dht, addr) = spawn(cfg()).await;
        let sock = raw().await;
        let m = ask(&sock, addr, Query::Ping).await.expect("pong");
        assert_eq!(m.tid, b"aa");
        assert_eq!(m.ip, Some(sock.local_addr().unwrap()));
        let v = m.version.clone().unwrap();
        assert_eq!(&v[..2], b"FB");
        assert_eq!(v.len(), 4);
        let r = response(m);
        assert_ne!(r.id, id(1));
        dht.shutdown().await;
        dht.shutdown().await;
    }

    #[tokio::test]
    async fn unknown_method_gets_error_204() {
        let (_dht, addr) = spawn(cfg()).await;
        let sock = raw().await;
        let m = ask(&sock, addr, Query::Unknown("vote".into()))
            .await
            .unwrap();
        assert!(matches!(m.body, Body::Error { code: 204, .. }), "{m:?}");
    }

    async fn token_for(sock: &UdpSocket, addr: SocketAddr, ih: [u8; 20]) -> Vec<u8> {
        let r = response(
            ask(sock, addr, Query::GetPeers { info_hash: ih })
                .await
                .unwrap(),
        );
        assert!(r.values.is_empty());
        r.token.expect("token")
    }

    #[tokio::test]
    async fn get_peers_then_announce_then_values_returned() {
        let (_dht, addr) = spawn(cfg()).await;
        let sock = raw().await;
        let ih = [7; 20];
        let token = token_for(&sock, addr, ih).await;
        let m = ask(
            &sock,
            addr,
            Query::AnnouncePeer {
                info_hash: ih,
                port: 4000,
                implied_port: false,
                token,
            },
        )
        .await
        .unwrap();
        response(m);
        let r = response(
            ask(&sock, addr, Query::GetPeers { info_hash: ih })
                .await
                .unwrap(),
        );
        assert_eq!(r.values, vec!["127.0.0.1:4000".parse().unwrap()]);
        assert!(r.token.is_some());
    }

    #[tokio::test]
    async fn implied_port_uses_source_port() {
        let (dht, addr) = spawn(cfg()).await;
        let sock = raw().await;
        let ih = [8; 20];
        let token = token_for(&sock, addr, ih).await;
        response(
            ask(
                &sock,
                addr,
                Query::AnnouncePeer {
                    info_hash: ih,
                    port: 4000,
                    implied_port: true,
                    token,
                },
            )
            .await
            .unwrap(),
        );
        let r = response(
            ask(&sock, addr, Query::GetPeers { info_hash: ih })
                .await
                .unwrap(),
        );
        assert_eq!(r.values, vec![sock.local_addr().unwrap()]);
        // the store size reaches the stats
        let mut rx = dht.subscribe_stats();
        timeout(
            Duration::from_secs(3),
            rx.wait_for(|s| s.stored_info_hashes == 1),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    async fn announce_with_bad_token_gets_error_203() {
        let (_dht, addr) = spawn(cfg()).await;
        let sock = raw().await;
        let ih = [9; 20];
        let mut token = token_for(&sock, addr, ih).await;
        token[0] ^= 1;
        let m = ask(
            &sock,
            addr,
            Query::AnnouncePeer {
                info_hash: ih,
                port: 4000,
                implied_port: false,
                token,
            },
        )
        .await
        .unwrap();
        assert!(matches!(m.body, Body::Error { code: 203, .. }), "{m:?}");
        let r = response(
            ask(&sock, addr, Query::GetPeers { info_hash: ih })
                .await
                .unwrap(),
        );
        assert!(r.values.is_empty());
    }

    #[tokio::test]
    async fn read_only_node_ignores_queries_and_flags_its_own() {
        let mut c = cfg();
        c.read_only = true;
        let (dht, addr) = spawn(c).await;
        let sock = raw().await;
        sock.send_to(&query_bytes(b"aa", id(1), Query::Ping, false), addr)
            .await
            .unwrap();
        assert!(
            recv(&sock, 500).await.is_none(),
            "read-only nodes stay silent"
        );
        dht.add_node(sock.local_addr().unwrap());
        let (m, from) = recv(&sock, 2000).await.expect("a ping from the node");
        assert_eq!(from, addr);
        assert!(matches!(
            m.body,
            Body::Query(QueryMsg {
                query: Query::Ping,
                read_only: true,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn queries_from_read_only_nodes_do_not_enter_table() {
        let (dht, addr) = spawn(cfg()).await;
        let sock = raw().await;
        for k in 1..=5u8 {
            let mut i = [k; 20];
            i[0] = k.wrapping_mul(37);
            sock.send_to(&query_bytes(b"aa", NodeId(i), Query::Ping, true), addr)
                .await
                .unwrap();
            assert!(recv(&sock, 1000).await.is_some(), "still answered");
        }
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert_eq!(dht.stats().nodes_v4, 0);
        // the same pings without `ro` do enter the table, once we answer
        // the node's verification ping
        sock.send_to(&query_bytes(b"aa", id(5), Query::Ping, false), addr)
            .await
            .unwrap();
        recv(&sock, 1000).await.unwrap();
        answer_ping(&sock, id(5)).await;
        let mut rx = dht.subscribe_stats();
        timeout(Duration::from_secs(3), rx.wait_for(|s| s.nodes_v4 == 1))
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn query_flood_is_rate_limited() {
        let mut c = cfg();
        c.query_rate_per_ip = 5;
        let (_dht, addr) = spawn(c).await;
        let sock = raw().await;
        let ping = query_bytes(b"aa", id(1), Query::Ping, false);
        for _ in 0..100 {
            sock.send_to(&ping, addr).await.unwrap();
        }
        let mut answers = 0;
        while recv_answer(&sock, 300).await.is_some() {
            answers += 1;
        }
        assert!((1..=10).contains(&answers), "{answers} answers");
    }

    #[tokio::test]
    async fn garbage_and_oversized_datagrams_are_ignored() {
        let (_dht, addr) = spawn(cfg()).await;
        let sock = raw().await;
        sock.send_to(&[0xde; 2000], addr).await.unwrap();
        sock.send_to(&[], addr).await.unwrap();
        sock.send_to(b"d1:t2:aa1:y1:qe", addr).await.unwrap();
        sock.send_to(b"de", addr).await.unwrap();
        // an oversized but otherwise valid ping is dropped
        let mut big = query_bytes(b"aa", id(1), Query::Ping, false);
        big.truncate(big.len() - 1);
        big.extend_from_slice(b"1:z1600:");
        big.extend_from_slice(&[b'x'; 1600]);
        big.push(b'e');
        sock.send_to(&big, addr).await.unwrap();
        assert!(recv(&sock, 300).await.is_none());
        assert!(ask(&sock, addr, Query::Ping).await.is_some());
    }

    #[tokio::test]
    async fn response_from_wrong_address_is_ignored() {
        let (dht, addr) = spawn(cfg()).await;
        let a = raw().await;
        let b = raw().await;
        let a_addr = a.local_addr().unwrap();
        let q = {
            let dht = dht.clone();
            tokio::spawn(async move { dht.query(a_addr, Query::Ping).await })
        };
        let (m, _) = recv(&a, 2000).await.unwrap();
        let fake = Message {
            tid: m.tid,
            ip: None,
            version: None,
            body: Body::Response(Response {
                id: id(3),
                nodes: vec![],
                nodes6: vec![],
                values: vec![],
                token: None,
            }),
        };
        b.send_to(&fake.encode(), addr).await.unwrap();
        assert!(matches!(q.await.unwrap(), Err(QueryError::Timeout)));
        assert_eq!(dht.stats().nodes_v4, 0);
    }

    #[tokio::test]
    async fn query_gets_response_and_remote_error() {
        let (dht, _) = spawn(cfg()).await;
        let other = responder(id(4)).await;
        let r = dht.query(other, Query::Ping).await.unwrap();
        assert_eq!(r.id, id(4));
        let (_dht2, addr2) = spawn(cfg()).await;
        let e = dht.query(addr2, Query::Unknown("vote".into())).await;
        assert!(matches!(e, Err(QueryError::Remote { code: 204 })), "{e:?}");
        dht.shutdown().await;
        assert!(matches!(
            dht.query(other, Query::Ping).await,
            Err(QueryError::Shutdown)
        ));
    }

    #[tokio::test]
    async fn pending_queries_are_bounded() {
        let mut c = cfg();
        c.max_pending_queries = 4;
        let (dht, _) = spawn(c).await;
        let silent = raw().await;
        let to = silent.local_addr().unwrap();
        let tasks: Vec<_> = (0..10)
            .map(|_| {
                let dht = dht.clone();
                tokio::spawn(async move { dht.query(to, Query::Ping).await })
            })
            .collect();
        let mut results = Vec::new();
        for t in tasks {
            results.push(t.await.unwrap());
        }
        for (i, r) in results.iter().enumerate() {
            if i < 4 {
                assert!(matches!(r, Err(QueryError::Timeout)), "{i}: {r:?}");
            } else {
                assert!(matches!(r, Err(QueryError::Busy)), "{i}: {r:?}");
            }
        }
    }

    #[tokio::test]
    async fn ipv6_bind_failure_keeps_ipv4_running() {
        let Ok(taken) = std::net::UdpSocket::bind("[::1]:0") else {
            eprintln!("no IPv6 on this host: skipped");
            return;
        };
        let mut c = cfg();
        c.bind_v6 = Some(taken.local_addr().unwrap());
        let dht = Dht::spawn(c).await.unwrap();
        let addrs = dht.local_addrs();
        assert_eq!(addrs.len(), 1);
        assert!(addrs[0].is_ipv4());
        assert_eq!(dht.stats().local_addrs, addrs);
        let sock = raw().await;
        assert!(ask(&sock, addrs[0], Query::Ping).await.is_some());
    }

    /// A node on an externally owned socket: a pump task plays the demux.
    async fn spawn_shared(c: DhtConfig) -> (Dht, DhtInput, SocketAddr) {
        let std_sock = bind_v4("127.0.0.1:0".parse().unwrap()).unwrap();
        let local = std_sock.local_addr().unwrap();
        let send = std_sock.try_clone().unwrap();
        send.set_nonblocking(true).unwrap();
        let sender: Arc<dyn DatagramSender> = Arc::new(StdSender { sock: send, local });
        let (dht, input) = Dht::spawn_on(c, vec![sender]).await.unwrap();
        let recv = UdpSocket::from_std(std_sock).unwrap();
        let pump = input.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            while let Ok((n, from)) = recv.recv_from(&mut buf).await {
                pump.handle(from, &buf[..n]);
            }
        });
        (dht, input, local)
    }

    #[tokio::test]
    async fn spawn_on_answers_ping_through_the_shared_sender() {
        let (dht, _input, addr) = spawn_shared(cfg()).await;
        assert_eq!(dht.local_addrs(), vec![addr]);
        let sock = raw().await;
        let m = ask(&sock, addr, Query::Ping).await.expect("pong");
        assert!(matches!(m.body, Body::Response(_)), "{m:?}");
        dht.shutdown().await;
    }

    #[tokio::test]
    async fn full_input_queue_drops_instead_of_blocking() {
        let (dht, input, addr) = spawn_shared(cfg()).await;
        let from: SocketAddr = "127.0.0.1:9".parse().unwrap();
        let junk = vec![0xffu8; 100];
        let started = std::time::Instant::now();
        for _ in 0..5000 {
            input.handle(from, &junk);
        }
        // oversize datagrams are cut and dropped, never an error
        input.handle(from, &vec![b'x'; 70_000]);
        assert!(started.elapsed() < Duration::from_secs(1), "handle blocked");
        let sock = raw().await;
        let m = ask(&sock, addr, Query::Ping)
            .await
            .expect("still answering");
        assert!(matches!(m.body, Body::Response(_)), "{m:?}");
        dht.shutdown().await;
    }

    #[tokio::test]
    async fn spawn_on_rejects_no_socket_and_duplicate_families() {
        assert!(matches!(
            Dht::spawn_on(cfg(), Vec::new()).await,
            Err(DhtError::NoSocket)
        ));
        let mk = || -> Arc<dyn DatagramSender> {
            let s = bind_v4("127.0.0.1:0".parse().unwrap()).unwrap();
            let local = s.local_addr().unwrap();
            Arc::new(StdSender { sock: s, local })
        };
        assert!(matches!(
            Dht::spawn_on(cfg(), vec![mk(), mk()]).await,
            Err(DhtError::DuplicateFamily)
        ));
    }

    #[tokio::test]
    async fn no_socket_at_all_is_an_error() {
        let mut c = cfg();
        c.bind_v4 = None;
        assert!(matches!(Dht::spawn(c).await, Err(DhtError::NoSocket)));
    }

    #[test]
    fn external_ip_votes_regenerate_id() {
        let mut e = ExternalIpVotes::new();
        let ext: IpAddr = "203.0.113.7".parse().unwrap();
        let r = |k: u8| IpAddr::from([198, 51, 100, k]);
        assert_eq!(e.vote(r(1), ext), None);
        assert_eq!(e.vote(r(1), ext), None, "one reporter counts once");
        assert_eq!(e.vote(r(2), ext), None);
        assert_eq!(e.vote(r(3), ext), Some(ext));
        assert_eq!(e.vote(r(4), ext), None, "already our external IP");
        // local addresses never win
        let local: IpAddr = "192.168.1.2".parse().unwrap();
        let mut e = ExternalIpVotes::new();
        for k in 1..=10 {
            assert_eq!(e.vote(r(k), local), None);
        }
        // memory is bounded
        let mut e = ExternalIpVotes::new();
        for k in 0..1000u32 {
            e.vote(
                IpAddr::V4((0x0100_0000 + k).into()),
                IpAddr::V4((0x0200_0000 + k).into()),
            );
            assert!(e.len() <= MAX_EXTERNAL_VOTES);
        }
    }

    #[test]
    fn external_ip_minority_never_overrides_the_majority() {
        let a: IpAddr = "203.0.113.7".parse().unwrap();
        let b: IpAddr = "198.18.0.1".parse().unwrap();
        let c: IpAddr = "198.18.0.2".parse().unwrap();
        let r = |k: u32| IpAddr::V4((0x0500_0000 + k).into());
        let mut e = ExternalIpVotes::new();
        let mut switches = Vec::new();
        for k in 0..20 {
            switches.extend(e.vote(r(k), a));
        }
        assert_eq!(switches, [a]);
        // 3 Sybil reporters dissent: no switch
        for k in 100..103 {
            assert_eq!(e.vote(r(k), b), None);
        }
        // alternating minorities, each far below the majority, do not flap
        for k in 200..230 {
            let ip = if k % 2 == 0 { b } else { c };
            assert_eq!(e.vote(r(k), ip), None, "vote {k}");
        }
        // a tie keeps the current address
        let mut t = ExternalIpVotes::new();
        for k in 0..3 {
            t.vote(r(k), a);
        }
        for k in 10..13 {
            assert_eq!(t.vote(r(k), b), None);
        }
    }

    #[test]
    fn external_ip_genuine_move_switches() {
        let old: IpAddr = "203.0.113.7".parse().unwrap();
        let new: IpAddr = "198.51.100.200".parse().unwrap();
        let r = |k: u32| IpAddr::V4((0x0500_0000 + k).into());
        let mut e = ExternalIpVotes::new();
        for k in 0..10 {
            e.vote(r(k), old);
        }
        // the same reporters see us move, one by one
        let mut switched_at = None;
        for k in 0..10 {
            if e.vote(r(k), new) == Some(new) {
                switched_at = Some(k);
            }
        }
        assert_eq!(switched_at, Some(5), "once new holds 6 of 10");
        // and old votes ageing out of the window move us back
        let mut e = ExternalIpVotes::new();
        for k in 0..MAX_EXTERNAL_VOTES as u32 {
            e.vote(r(k), old);
        }
        let mut switched = false;
        for k in 1000..1000 + MAX_EXTERNAL_VOTES as u32 {
            switched |= e.vote(r(k), new) == Some(new);
        }
        assert!(switched);
        assert_eq!(e.len(), MAX_EXTERNAL_VOTES);
    }

    #[test]
    fn external_ip_changes_the_id_when_invalid() {
        let mut rng = StdRng::seed_from_u64(1);
        let ext: IpAddr = "203.0.113.7".parse().unwrap();
        let own = NodeId([0; 20]);
        let new = regenerate_for(own, ext, &mut rng).expect("new id");
        assert!(new.is_valid_for(ext));
        assert_eq!(regenerate_for(new, ext, &mut rng), None, "already valid");
    }

    #[test]
    fn backoff_doubles_to_the_cap() {
        let base = Duration::from_secs(30);
        let max = Duration::from_secs(15 * 60);
        let got: Vec<u64> = (0..8).map(|n| backoff(base, max, n).as_secs()).collect();
        assert_eq!(got, [30, 60, 120, 240, 480, 900, 900, 900]);
        assert_eq!(backoff(base, max, u32::MAX), max);
    }

    #[tokio::test]
    async fn stats_published_on_change_rate_limited() {
        let (dht, _) = spawn(cfg()).await;
        let mut rx = dht.subscribe_stats();
        assert_eq!(dht.stats().nodes_v4, 0);
        let first = responder(id(0x80)).await;
        dht.add_node(first);
        timeout(Duration::from_secs(2), rx.wait_for(|s| s.nodes_v4 == 1))
            .await
            .expect("published within 2 s")
            .unwrap();
        let mut rng = StdRng::seed_from_u64(9);
        let mut nodes = Vec::new();
        for _ in 0..20 {
            nodes.push(responder(NodeId::random(&mut rng)).await);
        }
        rx.mark_unchanged();
        let start = tokio::time::Instant::now();
        let feeder = {
            let dht = dht.clone();
            tokio::spawn(async move {
                for n in nodes {
                    dht.add_node(n);
                    tokio::time::sleep(Duration::from_millis(45)).await;
                }
            })
        };
        let mut published = 0;
        let window = start + Duration::from_secs(1);
        while let Ok(Ok(())) = tokio::time::timeout_at(window, rx.changed()).await {
            published += 1;
        }
        assert!(published <= 3, "{published} values in 1 s");
        feeder.await.unwrap();
        timeout(Duration::from_secs(3), rx.wait_for(|s| s.nodes_v4 >= 8))
            .await
            .expect("the table grew")
            .unwrap();
    }

    /// An actor on loopback sockets, driven by direct calls.
    fn actor(c: DhtConfig) -> (Actor, mpsc::Sender<Cmd>) {
        let s = bind_v4(c.bind_v4.unwrap()).unwrap();
        let (tx, _rx) = mpsc::channel(1);
        let stats = watch::channel(DhtStats::default()).0;
        let local = s.local_addr().unwrap();
        let s: Arc<dyn DatagramSender> = Arc::new(StdSender { sock: s, local });
        let a = Actor::new(c, Some(s), None, tx.downgrade(), stats, Instant::now());
        (a, tx)
    }

    /// A node in bucket 0 of the all-zero id, on loopback port `20000 + k`
    /// (so pings to it leave this host's stack without error).
    fn far(k: u8) -> NodeInfo {
        let mut i = [0u8; 20];
        i[0] = 0x80;
        i[19] = k;
        NodeInfo {
            id: NodeId(i),
            addr: SocketAddr::from(([127, 0, 0, 1], 20000 + u16::from(k))),
        }
    }

    #[tokio::test]
    async fn enforce_node_id_keeps_invalid_ids_out() {
        let (mut a, _tx) = actor(DhtConfig {
            enforce_node_id: true,
            ..cfg()
        });
        let now = Instant::now();
        let public: SocketAddr = "203.0.113.9:6881".parse().unwrap();
        let mut rng = StdRng::seed_from_u64(5);
        let mut bad = bep42_id(public.ip(), 1, &mut rng);
        bad.0[0] ^= 0xff;
        a.learn(
            NodeInfo {
                id: bad,
                addr: public,
            },
            now,
        );
        assert_eq!(a.table_v4.as_ref().unwrap().len(), 0, "invalid id refused");
        let good = bep42_id(public.ip(), 2, &mut rng);
        a.learn(
            NodeInfo {
                id: good,
                addr: public,
            },
            now,
        );
        assert_eq!(a.table_v4.as_ref().unwrap().id_at(&public), Some(good));
        // local addresses are exempt
        let mut l = far(3);
        l.id = bad;
        a.learn(l, now);
        assert_eq!(a.table_v4.as_ref().unwrap().id_at(&l.addr), Some(bad));
        // without enforcement any id goes in
        let (mut a, _tx) = actor(cfg());
        a.learn(
            NodeInfo {
                id: bad,
                addr: public,
            },
            now,
        );
        assert_eq!(a.table_v4.as_ref().unwrap().id_at(&public), Some(bad));
    }

    #[tokio::test]
    async fn local_send_failure_registers_no_pending_query() {
        let (mut a, _tx) = actor(cfg());
        let now = Instant::now();
        // broadcast without SO_BROADCAST is refused by the local stack
        let nowhere: SocketAddr = "255.255.255.255:6881".parse().unwrap();
        let (tx, mut rx) = oneshot::channel();
        assert_eq!(
            a.send_query(nowhere, Query::Ping, Some(tx), None, now),
            Err(QueryError::Timeout)
        );
        assert_eq!(rx.try_recv(), Ok(Err(QueryError::Timeout)));
        assert!(a.pending.is_empty() && a.timeouts.is_empty());
        // a reachable address is registered
        let ok = far(1).addr;
        assert_eq!(a.send_query(ok, Query::Ping, None, None, now), Ok(()));
        assert_eq!(a.pending.len(), 1);
    }

    #[tokio::test]
    async fn tids_are_random_and_never_reused_while_pending() {
        let (mut a, _tx) = actor(cfg());
        let now = Instant::now();
        let mut tids = Vec::new();
        for k in 0..64u8 {
            a.send_query(far(k).addr, Query::Ping, None, None, now)
                .unwrap();
        }
        tids.extend(a.timeouts.iter().map(|(_, t)| *t));
        let distinct: HashSet<u16> = tids.iter().copied().collect();
        assert_eq!(distinct.len(), 64, "no tid in use twice");
        let sequential = tids
            .windows(2)
            .filter(|w| w[1] == w[0].wrapping_add(1))
            .count();
        assert!(sequential < 4, "not a counter: {tids:?}");
        // with every id but one in use, the free one is found
        let (mut a, _tx) = actor(cfg());
        for t in 0..=u16::MAX {
            if t != 4242 {
                a.pending.insert(
                    t,
                    Pending {
                        addr: far(1).addr,
                        deadline: now,
                        reply: None,
                        pinging: None,
                    },
                );
            }
        }
        assert_eq!(a.free_tid(), Some(4242));
        a.pending.insert(
            4242,
            Pending {
                addr: far(1).addr,
                deadline: now,
                reply: None,
                pinging: None,
            },
        );
        assert_eq!(a.free_tid(), None);
    }

    #[tokio::test]
    async fn votes_from_the_other_family_are_ignored() {
        let (mut a, _tx) = actor(cfg());
        let ext: IpAddr = "203.0.113.7".parse().unwrap();
        for k in 1..=5u16 {
            let v6_reporter = IpAddr::from([0x2001, 0xdb8, 0, 0, 0, 0, 0, k]);
            a.vote(v6_reporter, ext);
        }
        assert_eq!(a.ext_v4.len(), 0);
        let own = a.table_v4.as_ref().unwrap().own_id();
        for k in 1..=3 {
            a.vote(IpAddr::from([198, 51, 100, k]), ext);
        }
        let new = a.table_v4.as_ref().unwrap().own_id();
        assert_ne!(new, own, "same-family quorum regenerates the id");
        assert!(new.is_valid_for(ext));
    }

    #[tokio::test]
    async fn id_change_restarts_refresh_pacing() {
        let (mut a, _tx) = actor(cfg());
        a.table_v4 = Some(RoutingTable::new(NodeId([0; 20]), Family::V4));
        let t0 = Instant::now();
        a.learn(far(1), t0);
        a.refresh(t0 + Duration::from_secs(16 * 60));
        assert!(!a.refreshed.is_empty());
        for k in 1..=3 {
            a.vote(
                IpAddr::from([198, 51, 100, k]),
                "203.0.113.7".parse().unwrap(),
            );
        }
        assert!(a.refreshed.is_empty());
    }

    #[tokio::test]
    async fn questionable_node_is_pinged_once_for_many_newcomers() {
        let (mut a, _tx) = actor(cfg());
        a.table_v4 = Some(RoutingTable::new(NodeId([0; 20]), Family::V4));
        let t0 = Instant::now();
        for k in 0..8 {
            a.learn(far(k), t0);
        }
        let later = t0 + Duration::from_secs(16 * 60);
        for k in 8..20 {
            a.learn(far(k), later);
        }
        assert_eq!(
            a.pending.len(),
            1,
            "one ping for the oldest questionable node"
        );
        assert_eq!(a.pinging.len(), 1);
        // its timeout counts as a failure and frees the de-duplication slot
        a.expire_queries(later + Duration::from_secs(1));
        assert!(a.pending.is_empty() && a.pinging.is_empty());
        a.learn(far(20), later + Duration::from_secs(2));
        assert_eq!(a.pending.len(), 1, "pinged again after the failure");
    }

    #[tokio::test]
    async fn refresh_is_paced_per_bucket() {
        let (mut a, _tx) = actor(cfg());
        a.table_v4 = Some(RoutingTable::new(NodeId([0; 20]), Family::V4));
        let t0 = Instant::now();
        a.learn(far(1), t0);
        let stale = t0 + Duration::from_secs(16 * 60);
        a.refresh(stale);
        let first = a.refreshed.clone();
        assert!(first.contains_key(&(4, 0)), "the stale bucket 0");
        assert!(first.contains_key(&(4, 1)), "the empty bucket after it");
        // nothing answered: the next ticks do not retrigger the same buckets
        a.refresh(stale + HOUSEKEEPING);
        a.refresh(stale + 2 * HOUSEKEEPING);
        assert_eq!(a.refreshed, first);
        a.refresh(stale + REFRESH_INTERVAL);
        assert!(a.refreshed.values().all(|t| *t == stale + REFRESH_INTERVAL));
    }

    fn ping_from(k: u8) -> QueryMsg {
        QueryMsg {
            id: id(k),
            query: Query::Ping,
            want: Want::default(),
            read_only: false,
        }
    }

    /// A query from an unknown source is answered but its node enters the
    /// table only once it answers our ping; a spoofed source never does.
    #[tokio::test]
    async fn queries_from_unknown_nodes_are_verified_before_insert() {
        let (mut a, _tx) = actor(cfg());
        let now = Instant::now();
        let sock = raw().await;
        let me = sock.local_addr().unwrap();
        a.on_query(me, me, b"aa", &ping_from(7), now);
        let (pong, _) = recv(&sock, 1000).await.expect("answered");
        assert!(matches!(pong.body, Body::Response(_)));
        assert_eq!(a.table_v4.as_ref().unwrap().len(), 0, "not before the ping");
        let (verify, _) = recv(&sock, 1000).await.expect("verification ping");
        assert!(matches!(
            verify.body,
            Body::Query(QueryMsg {
                query: Query::Ping,
                ..
            })
        ));
        // a second query meanwhile is not pinged again
        a.on_query(me, me, b"ab", &ping_from(7), now);
        recv(&sock, 1000).await.expect("answered");
        assert_eq!(a.pending.len(), 1);
        let answer = Message {
            tid: verify.tid,
            ip: None,
            version: None,
            body: Body::Response(Response {
                id: id(7),
                nodes: vec![],
                nodes6: vec![],
                values: vec![],
                token: None,
            }),
        };
        a.on_datagram(&answer.encode(), me, now);
        assert_eq!(a.table_v4.as_ref().unwrap().id_at(&me), Some(id(7)));
        assert!(a.pinging.is_empty() && a.verifying == 0);
        // known now: its next query refreshes it without a ping
        a.on_query(me, me, b"ac", &ping_from(7), now);
        recv(&sock, 1000).await.expect("answered");
        assert!(a.pending.is_empty());

        // spoofed sources (nobody answers there) never enter, and at most
        // MAX_VERIFY_PINGS pings are out at once
        let (mut a, _tx) = actor(DhtConfig {
            query_rate_per_ip: 1000,
            ..cfg()
        });
        for k in 0..200u8 {
            let from = far(k).addr;
            a.on_query(from, from, b"aa", &ping_from(k), now);
        }
        assert_eq!(a.verifying, MAX_VERIFY_PINGS);
        assert_eq!(a.pending.len(), MAX_VERIFY_PINGS);
        a.expire_queries(now + Duration::from_secs(1));
        assert_eq!(a.table_v4.as_ref().unwrap().len(), 0);
        assert!(a.pending.is_empty() && a.pinging.is_empty() && a.verifying == 0);
    }

    /// One IP flooding past its own limit does not use up the global budget.
    #[tokio::test]
    async fn per_ip_limit_is_checked_before_the_global_one() {
        let (mut a, _tx) = actor(DhtConfig {
            query_rate_per_ip: 5,
            ..cfg()
        });
        let now = Instant::now();
        // another IP (unreachable from this loopback socket: nothing leaves)
        let flooder: SocketAddr = "10.0.0.1:6881".parse().unwrap();
        for _ in 0..3000 {
            a.on_query(flooder, flooder, b"aa", &ping_from(1), now);
        }
        let sock = raw().await;
        let me = sock.local_addr().unwrap();
        a.on_query(me, me, b"aa", &ping_from(2), now);
        assert!(recv(&sock, 1000).await.is_some(), "another IP is answered");
    }

    /// A new attempt never starts while the last one still runs.
    #[tokio::test]
    async fn bootstrap_attempts_do_not_overlap() {
        let (mut a, _tx) = actor(DhtConfig {
            bootstrap: vec!["127.0.0.1:9".into()],
            ..cfg()
        });
        let t0 = Instant::now();
        let running = tokio::spawn(std::future::pending::<()>());
        a.boot_started = true;
        a.boot_task = Some(running);
        a.on_timer(t0);
        assert_eq!(a.boot_attempt, 0, "no new attempt");
        assert_eq!(a.boot_due, Some(t0 + a.query_timeout), "polled again soon");
        // once it is done, the next one starts
        if let Some(t) = a.boot_task.take() {
            t.abort();
            let _ = t.await;
        }
        a.boot_task = Some(tokio::spawn(async {}));
        tokio::task::yield_now().await;
        while !a.boot_task.as_ref().unwrap().is_finished() {
            tokio::task::yield_now().await;
        }
        a.on_timer(t0 + a.retry_base);
        assert_eq!(a.boot_attempt, 1);
        assert!(a.boot_task.is_some());
    }

    #[test]
    fn routers_are_tried_before_restored_nodes() {
        let r: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let s: Vec<SocketAddr> = (2..5)
            .map(|p| SocketAddr::from(([127, 0, 0, 1], p)))
            .collect();
        let order = seed_order(vec![r], s.clone());
        assert_eq!(order[0], r);
        assert_eq!(order[1..], s[..]);
    }

    #[tokio::test]
    async fn global_query_cap_holds_against_spoofed_sources() {
        let (mut a, _tx) = actor(DhtConfig {
            query_rate_per_ip: 5,
            ..cfg()
        });
        let now = Instant::now();
        let ping = QueryMsg {
            id: id(1),
            query: Query::Ping,
            want: Want::default(),
            read_only: false,
        };
        // every spoofed source is new to the per-IP limiter
        for k in 0..3000u32 {
            let from = SocketAddr::from(((0x0a00_0000 + k).to_be_bytes(), 6881));
            a.on_query(from, from, b"aa", &ping, now);
        }
        let sock = raw().await;
        let me = sock.local_addr().unwrap();
        a.on_query(me, me, b"aa", &ping, now);
        assert!(recv(&sock, 300).await.is_none(), "over the global cap");
        a.on_query(me, me, b"aa", &ping, now + Duration::from_secs(1));
        assert!(recv(&sock, 1000).await.is_some(), "refilled");
    }

    #[tokio::test]
    async fn mapped_sources_are_canonical_and_local_nodes_from_remote_dropped() {
        let (mut a, _tx) = actor(cfg());
        let now = Instant::now();
        let sock = raw().await;
        let v4 = sock.local_addr().unwrap();
        let mapped: SocketAddr = format!("[::ffff:127.0.0.1]:{}", v4.port()).parse().unwrap();
        let ping = Message {
            tid: b"aa".to_vec(),
            ip: None,
            version: None,
            body: Body::Query(QueryMsg {
                id: id(7),
                query: Query::Ping,
                want: Want::default(),
                read_only: false,
            }),
        };
        a.on_datagram(&ping.encode(), mapped, now);
        // the answer goes back to the mapped source (no IPv6 socket here),
        // the verification ping to the canonical IPv4 address
        let (verify, _) = recv(&sock, 1000).await.expect("verification ping");
        assert!(matches!(verify.body, Body::Query(_)), "{verify:?}");
        assert!(a.pending.values().all(|p| p.addr == v4));
        let answer = Message {
            tid: verify.tid,
            ip: None,
            version: None,
            body: Body::Response(Response {
                id: id(7),
                nodes: vec![],
                nodes6: vec![],
                values: vec![],
                token: None,
            }),
        };
        a.on_datagram(&answer.encode(), mapped, now);
        let t = a.table_v4.as_ref().unwrap();
        assert_eq!(t.id_at(&v4), Some(id(7)));
        // a remote responder cannot hand out local addresses
        let nodes = [
            NodeInfo {
                id: id(8),
                addr: "192.168.1.1:1".parse().unwrap(),
            },
            NodeInfo {
                id: id(9),
                addr: "5.6.7.8:1".parse().unwrap(),
            },
        ];
        let kept = lookup::usable("1.2.3.4:1".parse().unwrap(), Family::V4, id(1), &nodes);
        assert_eq!(kept, nodes[1..]);
        let kept = lookup::usable("10.0.0.1:1".parse().unwrap(), Family::V4, id(1), &nodes);
        assert_eq!(kept, nodes);
    }
}
