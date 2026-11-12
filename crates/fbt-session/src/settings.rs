//! Session settings.

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use fbt_net::lsd::{LSD_V4, LSD_V6};
use fbt_net::natpmp::NATPMP_PORT;
use fbt_net::proxy::ProxyConfig;
use fbt_net::upnp::SSDP_V4;
use fbt_tracker::TrackerConfig;

use crate::bandwidth::{MixedMode, PeerClassLimits, RateLimit};
use crate::choker::{ChokingAlgorithm, SeedChokingAlgorithm};
use crate::stream::ReadAhead;

/// Largest accepted [`Settings::event_queue`]; the queue is allocated up
/// front.
pub const MAX_EVENT_QUEUE: usize = 1 << 20;
/// Largest accepted [`Settings::max_incoming_requests`]; each peer's command
/// queue is sized from it.
pub const MAX_INCOMING_REQUESTS: usize = 1 << 16;
/// Largest accepted [`Settings::max_metadata_size`].
pub const MAX_METADATA_SIZE: usize = 64 * 1024 * 1024;
/// Largest accepted [`Settings::disk_threads`].
pub const MAX_DISK_THREADS: usize = 64;
/// Largest accepted [`Settings::max_active_checks`].
const MAX_ACTIVE_CHECKS: usize = 64;
/// Smallest accepted [`Settings::max_metadata_size`]: one metadata piece.
const MIN_METADATA_SIZE: usize = 16_384;
/// The public DHT routers [`Settings::default`] bootstraps from. Nothing
/// else in the library names a public node: `fbt_dht::DhtConfig` bootstraps
/// from nothing by default.
pub const DEFAULT_DHT_BOOTSTRAP: [&str; 3] = [
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "router.utorrent.com:6881",
];

/// Whether peer connections use MSE/PE (message stream encryption).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum EncryptionPolicy {
    /// Encrypted (RC4) connections only, in both directions.
    Forced,
    /// Outgoing connections try MSE (offering RC4 or plaintext) and fall
    /// back to a plain BitTorrent handshake once per address when the MSE
    /// handshake fails; incoming connections may be either.
    #[default]
    Preferred,
    /// Plain BitTorrent handshakes only; incoming MSE connections are
    /// refused.
    Disabled,
}

/// Tunables for a session and its torrents.
///
/// Build with [`Settings::default`] and assign the fields to change.
/// [`Session::new`](crate::Session::new) refuses values that cannot work
/// (zero intervals and timeouts that drive timers, queues of zero or beyond
/// [`MAX_EVENT_QUEUE`] / [`MAX_INCOMING_REQUESTS`], a metadata cap outside
/// 16 KiB..=[`MAX_METADATA_SIZE`], a DHT bootstrap entry that is not
/// `host:port`, an empty or duplicated `listen_addrs`, every transport
/// switched off, `disk_threads` or `max_active_checks` outside 1..=64, a zero
/// `max_peers_per_ip`, a zero `read_ahead.time` or `read_ahead.bitrate`) with
/// [`SessionError::InvalidSettings`](crate::SessionError::InvalidSettings).
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
#[non_exhaustive]
pub struct Settings {
    /// Addresses the session listens on (non-empty, no duplicates). Each
    /// gets a TCP peer listener and, on the same port, a UDP socket shared
    /// by the DHT and UDP tracker announces. The first address of each
    /// family carries that family's DHT and UDP tracker traffic, so put the
    /// routable address first. An IPv6 address binds
    /// IPv6-only, so `0.0.0.0:p` and `[::]:p` can share a port. An address
    /// that cannot be bound is skipped and reported by
    /// [`Session::listen_errors`](crate::Session::listen_errors).
    pub listen_addrs: Vec<SocketAddr>,
    /// Connected peers per torrent.
    pub max_peers_per_torrent: usize,
    /// Connected peers across all torrents.
    pub max_peers_global: usize,
    /// Connections a torrent accepts from one IP address (non-zero; an
    /// IPv4-mapped address counts as its IPv4 address). Further ones are
    /// closed with [`BlockReason::PerIpLimit`](crate::BlockReason::PerIpLimit).
    /// 1 by default, as libtorrent's `allow_multiple_connections_per_ip`
    /// off.
    pub max_peers_per_ip: usize,
    /// Loopback peers are exempt from `max_peers_per_ip`, so clients on the
    /// same host (and test suites) can connect to each other.
    #[doc(hidden)]
    pub per_ip_exempt_loopback: bool,
    /// Bans name one endpoint (address and port) instead of the whole IP,
    /// so loopback tests can ban one of several peers on `127.0.0.1`.
    #[doc(hidden)]
    pub ban_endpoints: bool,
    /// Outgoing connections still connecting or handshaking.
    pub max_half_open: usize,
    /// Upload slots per torrent (regular plus optimistic). With
    /// [`ChokingAlgorithm::RateBased`] it bounds only the slots given
    /// between rechokes; the rechoke opens regular slots by rate.
    pub unchoke_slots: usize,
    /// How regular upload slots are opened while downloading.
    pub choking_algorithm: ChokingAlgorithm,
    /// How regular upload slots are given while seeding.
    pub seed_choking_algorithm: SeedChokingAlgorithm,
    /// Optimistic upload slots per torrent, part of
    /// [`unchoke_slots`](Self::unchoke_slots) (as with libtorrent, which
    /// reserves them out of its upload slots); 0 means
    /// `max(1, unchoke_slots / 5)`, but always leaving one regular slot (so
    /// a single upload slot has no optimistic one).
    pub optimistic_unchoke_slots: usize,
    /// How often the optimistic slots move to other peers (30 s). At least
    /// [`rechoke_interval`](Self::rechoke_interval): they rotate every
    /// `optimistic_unchoke_interval / rechoke_interval` rechokes.
    pub optimistic_unchoke_interval: Duration,
    /// With [`SeedChokingAlgorithm::RoundRobin`], a peer keeps its seeding
    /// slot until it has received this many pieces' worth of bytes (20) or
    /// three rechokes have passed.
    pub seeding_piece_quota: u32,
    /// A peer that sends none of the blocks we requested for this long
    /// (60 s) is snubbed: one request in flight at a time, and only
    /// optimistic upload slots, until a block arrives. Non-zero. The timer
    /// pauses while a download limit holds its reads.
    pub snub_timeout: Duration,
    /// Each peer gets enough requests in flight to keep it busy this long
    /// at its current rate (3 s): `rate * request_queue_time / 16 KiB`,
    /// at least 2 and at most
    /// [`max_outstanding_requests`](Self::max_outstanding_requests), and
    /// no more than the `reqq` the peer advertised.
    pub request_queue_time: Duration,
    /// Most block requests in flight to one peer (500, libtorrent's
    /// `max_out_request_queue`). The cache budget still bounds how many
    /// pieces are in progress.
    pub max_outstanding_requests: usize,
    /// Requests from one peer queued before further ones are dropped
    /// (1 to [`MAX_INCOMING_REQUESTS`]). Under an upload limit, the blocks
    /// answering them wait in the connection as copies outside
    /// [`cache_size`](Self::cache_size): up to this many 16 KiB blocks per
    /// connection (about 4 MiB with the default 250). Copies, because a
    /// queued block sharing its cached piece would keep the whole piece
    /// alive, which costs more.
    pub max_incoming_requests: usize,
    /// TCP connect timeout.
    pub connect_timeout: Duration,
    /// Time allowed for the handshake exchange, the MSE handshake included.
    pub handshake_timeout: Duration,
    /// MSE/PE policy for outgoing and incoming connections.
    pub encryption: EncryptionPolicy,
    /// A peer that sends nothing for this long is disconnected (non-zero).
    pub peer_idle_timeout: Duration,
    /// A keepalive goes out after this much send silence (non-zero).
    pub keepalive_interval: Duration,
    /// A request unanswered for this long is cancelled and re-picked
    /// (non-zero).
    pub request_timeout: Duration,
    /// Wait before a failed outgoing connection is tried again; the first
    /// retry after a success (or ever) waits a twelfth of it. Internal
    /// tunable (tests shorten it); not part of the stable API.
    #[doc(hidden)]
    pub peer_retry_delay: Duration,
    /// Time between choker runs (non-zero).
    pub rechoke_interval: Duration,
    /// Time between status updates (non-zero).
    pub status_interval: Duration,
    /// Capacity of the event queues (1 to [`MAX_EVENT_QUEUE`]).
    pub event_queue: usize,
    /// Peers asked from each tracker announce.
    pub num_want: u32,
    /// Tracker client settings.
    pub tracker: TrackerConfig,
    /// First wait after every tracker failed; doubles with each further
    /// failed round, up to 30 minutes. Internal tunable (tests shorten it);
    /// not part of the stable API. Non-zero.
    #[doc(hidden)]
    pub tracker_retry_base: Duration,
    /// While a magnet's metadata is pending and its peer slots are full, a
    /// peer connected this long without offering `ut_metadata` is closed
    /// to make room. Internal tunable (tests shorten it); not part of the
    /// stable API.
    #[doc(hidden)]
    pub metadata_peer_grace: Duration,
    /// Largest info dictionary a magnet torrent fetches from peers (BEP 9),
    /// in bytes; peers advertising more are disconnected. 16 KiB to
    /// [`MAX_METADATA_SIZE`].
    pub max_metadata_size: usize,
    /// Run a session-wide Mainline DHT node (BEP 5) on the listen
    /// addresses' shared UDP sockets, on the first one of each address
    /// family (on a free port of that IP when no UDP socket could be bound).
    /// Private torrents never use it.
    pub dht_enabled: bool,
    /// Nodes the DHT bootstraps from, as `host:port`; by default the public
    /// routers in [`DEFAULT_DHT_BOOTSTRAP`].
    pub dht_bootstrap_nodes: Vec<String>,
    /// BEP 43 read-only node: query others, answer nobody.
    pub dht_read_only: bool,
    /// A DHT state saved earlier with
    /// [`Session::dht_state`](crate::Session::dht_state), for a fast
    /// rejoin. One that does not parse is ignored with a warning. The
    /// library never reads or writes files: storing it is the caller's job.
    pub dht_state: Option<Vec<u8>>,
    /// Time between DHT announces of a running torrent (non-zero). A
    /// magnet still fetching its metadata announces every 60 s instead.
    pub dht_announce_interval: Duration,
    /// How long a DHT query waits for its answer. Internal tunable (tests
    /// shorten it); not part of the stable API.
    #[doc(hidden)]
    pub dht_query_timeout: Duration,
    /// Inbound DHT queries per second and source IP. Internal tunable
    /// (tests raise it); not part of the stable API.
    #[doc(hidden)]
    pub dht_query_rate_per_ip: u32,
    /// Exchange peers with connected peers (BEP 11 `ut_pex`). Private
    /// torrents never do, whatever this says.
    pub pex_enabled: bool,
    /// Time between PEX messages to one peer (non-zero); a peer's own
    /// messages are accepted at most once per three quarters of it.
    /// Internal tunable (tests shorten it); not part of the stable API.
    #[doc(hidden)]
    pub pex_interval: Duration,
    /// Dial peers over TCP: after a failed uTP connect, or first when uTP
    /// cannot be used for the peer. At least one of the four transport
    /// switches must be on.
    pub enable_outgoing_tcp: bool,
    /// Accept peer connections on the TCP listeners. Off, no TCP listener
    /// runs: each listen address's TCP socket only picks the port (the UDP
    /// socket binds the same number, which announces and extension
    /// handshakes advertise) and is closed, so TCP dials to it are refused
    /// at once and a dialler falls back to uTP without waiting. The TCP
    /// port is then not held: another program may take it. (A socket left
    /// bound but not listening would hold it, but macOS drops SYNs to one,
    /// so diallers would wait out their connect timeout instead.)
    pub enable_incoming_tcp: bool,
    /// Dial peers over uTP (BEP 29) first, from the shared UDP socket of
    /// the peer's address family; a failed uTP connect is retried once at
    /// once over TCP (when `enable_outgoing_tcp`), and a peer reached that
    /// way is dialled over TCP first from then on.
    pub enable_outgoing_utp: bool,
    /// Accept uTP connections on the shared UDP sockets. Off, their SYNs
    /// are never answered, so a dialler's uTP connect times out (and falls
    /// back to TCP).
    pub enable_incoming_utp: bool,
    /// How long an outgoing uTP connect waits for the remote's answer
    /// before it fails (and falls back to TCP). 3 s, libtorrent's
    /// `utp_connect_timeout`: a peer that does not speak uTP costs that much
    /// before its TCP dial. Internal tunable (tests shorten it); not part of
    /// the stable API. Non-zero.
    #[doc(hidden)]
    pub utp_connect_timeout: Duration,
    /// BEP 55 holepunch over uTP: we advertise `ut_holepunch`, relay
    /// rendezvous requests between peers we are connected to, and when a
    /// connection to an address learned through PEX fails, ask the peer we
    /// learned it from to introduce us. Takes effect only with both
    /// `enable_outgoing_utp` and `enable_incoming_utp`, and never for
    /// private torrents.
    pub enable_holepunch: bool,
    /// BEP 14 local service discovery: announce each public torrent on the
    /// LAN's multicast groups (at most every 5 minutes) and connect to the
    /// peers that announce the same torrent. Private torrents never use it.
    /// Caveat (BEP 27): a magnet whose metadata has not arrived yet is
    /// announced, because whether it is private is unknown until then. On a
    /// multi-homed host only one interface is used (IPv4 the OS default,
    /// IPv6 interface 0).
    pub enable_lsd: bool,
    /// The multicast groups LSD announces to. Internal tunable (tests point
    /// it at unicast loopback); not part of the stable API.
    #[doc(hidden)]
    pub lsd_groups: Vec<SocketAddr>,
    /// Time between LSD announces of one torrent (a torrent is never
    /// announced more often). Internal tunable; not part of the stable API.
    #[doc(hidden)]
    pub lsd_announce_interval: Duration,
    /// Binds LSD to this plain UDP socket instead of joining the real
    /// multicast groups, for tests. Internal tunable; not part of the
    /// stable API.
    #[doc(hidden)]
    pub lsd_bind: Option<SocketAddr>,
    /// Map the listen port (TCP and UDP) on the router with UPnP IGD, and
    /// renew the mapping while the session runs. Shutdown removes it.
    pub enable_upnp: bool,
    /// Map the listen port with PCP, falling back to NAT-PMP, likewise.
    pub enable_natpmp: bool,
    /// The PCP/NAT-PMP gateway; `None` finds the default IPv4 gateway (from
    /// `/proc/net/route` on Linux, the `route` command elsewhere). Listen
    /// addresses on loopback are mapped only when this is a loopback
    /// address.
    pub nat_gateway: Option<Ipv4Addr>,
    /// Where UPnP's SSDP searches go. Internal tunable (tests point it at a
    /// loopback fake); not part of the stable API.
    #[doc(hidden)]
    pub upnp_ssdp_addr: SocketAddr,
    /// The gateway's PCP/NAT-PMP port. Internal tunable; not part of the
    /// stable API.
    #[doc(hidden)]
    pub natpmp_port: u16,
    /// First retry delay after a failed port mapping, doubling up to 30
    /// minutes. Internal tunable; not part of the stable API. Non-zero.
    #[doc(hidden)]
    pub nat_retry_base: Duration,
    /// Emulates an endpoint-dependent NAT in front of the session, for
    /// tests: incoming TCP is refused, and a uTP SYN is accepted only from
    /// an address the session sent a uTP datagram to in the last 30 s.
    /// Internal tunable; not part of the stable API.
    #[doc(hidden)]
    pub nat_emulation: bool,
    /// Storage jobs (piece reads, writes and flushes) running at once
    /// across the session, each on a blocking thread (1 to
    /// [`MAX_DISK_THREADS`]). Piece checks do not count against it: they
    /// add at most [`max_active_checks`](Self::max_active_checks) threads
    /// on top.
    pub disk_threads: usize,
    /// Torrents hash-checking their data at once across the session (1 to
    /// 64; libtorrent's `active_checking`). The others wait as
    /// [`TorrentState::QueuedForChecking`](crate::TorrentState::QueuedForChecking).
    pub max_active_checks: usize,
    /// Bytes of piece data the session keeps in memory across all its
    /// torrents (64 MiB): verified pieces not yet written (*dirty*, served
    /// to peers at once and written later in coalesced runs), written
    /// pieces kept to serve peers (*clean*, least recently used go first)
    /// and the buffers of pieces being downloaded (*partial*). Dirty pieces
    /// are written once they pass half of this, once they are older than
    /// [`cache_expiry`](Self::cache_expiry), and always when a torrent
    /// finishes, pauses, is removed, rechecks or the session shuts down.
    ///
    /// The one exception to the bound is a progress floor: each
    /// downloading torrent may hold up to 4 pieces beyond it (counted from
    /// the buffer until the piece is written, and written at once), so it
    /// never stalls behind other torrents' cached data. Memory stays within
    /// `cache_size + 4 * piece_length` per downloading torrent. The floor
    /// grants a buffer only while the session's dirty bytes (writes in
    /// flight included) are within `cache_size`: above that no new partial
    /// buffer is created for any torrent until a write lands, so a slow
    /// disk cannot grow memory without bound.
    ///
    /// 0 means write-through: every verified piece is written at once and
    /// there is no read cache. A download then runs on the floor alone (4
    /// pieces in progress per torrent, and none while a write is in
    /// flight), so its speed is bounded by the disk's write latency; keep
    /// it for testing or memory-starved hosts.
    pub cache_size: u64,
    /// How long a verified piece may stay unwritten (30 s). Non-zero.
    pub cache_expiry: Duration,
    /// The session-wide limit, in bytes per second each way (0:
    /// unlimited). It applies to peers on the local network only with
    /// [`rate_limit_local_peers`](Self::rate_limit_local_peers). Changed
    /// at runtime with [`Session::set_rate_limit`](crate::Session::set_rate_limit).
    pub rate_limit: RateLimit,
    /// Limits per peer class: TCP, uTP and local-network peers. They apply
    /// to local peers too. Changed at runtime with
    /// [`Session::set_peer_class_limits`](crate::Session::set_peer_class_limits).
    pub peer_class_limits: PeerClassLimits,
    /// Apply the global and per-torrent limits to peers on the local
    /// network (loopback, RFC 1918, link-local, CGNAT, IPv6 unique-local)
    /// as well (off: they are exempt, as with libtorrent's
    /// `ignore_limits_on_local_network`). Their class limits apply anyway.
    pub rate_limit_local_peers: bool,
    /// How a global limit is shared between TCP and uTP peers.
    pub mixed_mode: MixedMode,
    /// How far ahead of a file reader its torrent fetches (16 MiB, or 10 s
    /// at the stream's bitrate when that is more). Its `time` and a known
    /// `bitrate` must be non-zero.
    pub read_ahead: ReadAhead,
    /// The proxy peer connections, web seeds and trackers go through (none
    /// by default). See [`ProxyConfig`] for what each kind can carry:
    ///
    /// - SOCKS5 with `proxy_peers` or `proxy_trackers` relays all UDP
    ///   (outgoing uTP, the DHT, UDP trackers) through one UDP ASSOCIATE.
    ///   The session then binds TCP listeners only, plus one relayed UDP
    ///   socket on a free port; the DHT runs over the relay's address
    ///   family only (IPv4 in practice). If the
    ///   association cannot be made, [`Session::new`](crate::Session::new)
    ///   fails with [`SessionError::Proxy`](crate::SessionError::Proxy).
    /// - An HTTP proxy with `proxy_peers` dials peers over TCP only
    ///   (`enable_outgoing_utp` is treated as off); the DHT still runs
    ///   directly unless `dht_enabled` is off. With `enable_outgoing_tcp`
    ///   off as well, the session cannot dial peers at all;
    ///   [`Session::new`](crate::Session::new) logs a warning.
    /// - UDP tracker and DHT bootstrap host names are resolved locally
    ///   whatever `proxy_hostnames` says, and LSD, UPnP and NAT-PMP stay
    ///   direct on the local network.
    ///
    /// Listening is unchanged: incoming connections never use the proxy.
    pub proxy: ProxyConfig,
}

impl Settings {
    /// Checks the values `Session::new` cannot work with; the error names
    /// the field.
    pub(crate) fn validate(&self) -> Result<(), String> {
        let non_zero = [
            ("peer_idle_timeout", self.peer_idle_timeout),
            ("keepalive_interval", self.keepalive_interval),
            ("request_timeout", self.request_timeout),
            ("rechoke_interval", self.rechoke_interval),
            ("status_interval", self.status_interval),
            ("tracker_retry_base", self.tracker_retry_base),
            ("dht_announce_interval", self.dht_announce_interval),
            ("dht_query_timeout", self.dht_query_timeout),
            ("pex_interval", self.pex_interval),
            ("utp_connect_timeout", self.utp_connect_timeout),
            ("cache_expiry", self.cache_expiry),
            ("snub_timeout", self.snub_timeout),
            ("nat_retry_base", self.nat_retry_base),
        ];
        for (name, d) in non_zero {
            if d.is_zero() {
                return Err(format!("{name} must be greater than zero"));
            }
        }
        if self.optimistic_unchoke_interval < self.rechoke_interval {
            return Err("optimistic_unchoke_interval must be at least rechoke_interval".to_owned());
        }
        if !(self.enable_outgoing_tcp
            || self.enable_incoming_tcp
            || self.enable_outgoing_utp
            || self.enable_incoming_utp)
        {
            return Err(
                "enable_outgoing_tcp, enable_incoming_tcp, enable_outgoing_utp and \
                 enable_incoming_utp must not all be off"
                    .to_owned(),
            );
        }
        if !(1..=MAX_EVENT_QUEUE).contains(&self.event_queue) {
            return Err(format!(
                "event_queue must be between 1 and {MAX_EVENT_QUEUE}, got {}",
                self.event_queue
            ));
        }
        if !(1..=MAX_INCOMING_REQUESTS).contains(&self.max_incoming_requests) {
            return Err(format!(
                "max_incoming_requests must be between 1 and {MAX_INCOMING_REQUESTS}, got {}",
                self.max_incoming_requests
            ));
        }
        if !(MIN_METADATA_SIZE..=MAX_METADATA_SIZE).contains(&self.max_metadata_size) {
            return Err(format!(
                "max_metadata_size must be between {MIN_METADATA_SIZE} and {MAX_METADATA_SIZE}, got {}",
                self.max_metadata_size
            ));
        }
        if !(1..=MAX_DISK_THREADS).contains(&self.disk_threads) {
            return Err(format!(
                "disk_threads must be between 1 and {MAX_DISK_THREADS}, got {}",
                self.disk_threads
            ));
        }
        if !(1..=MAX_ACTIVE_CHECKS).contains(&self.max_active_checks) {
            return Err(format!(
                "max_active_checks must be between 1 and {MAX_ACTIVE_CHECKS}, got {}",
                self.max_active_checks
            ));
        }
        if self.listen_addrs.is_empty() {
            return Err("listen_addrs must not be empty".to_owned());
        }
        let addrs = &self.listen_addrs;
        if let Some(dup) = addrs
            .iter()
            .enumerate()
            .find_map(|(i, a)| addrs.iter().take(i).any(|b| b == a).then_some(a))
        {
            return Err(format!("listen_addrs has {dup} twice"));
        }
        if self.max_peers_per_ip == 0 {
            return Err("max_peers_per_ip must be greater than zero".to_owned());
        }
        if self.dht_query_rate_per_ip == 0 {
            return Err("dht_query_rate_per_ip must be greater than zero".to_owned());
        }
        if let Some(bad) = self.dht_bootstrap_nodes.iter().find(|e| !is_host_port(e)) {
            return Err(format!(
                "dht_bootstrap_nodes entries must be host:port, got {bad:?}"
            ));
        }
        self.proxy.validate().map_err(|e| format!("proxy: {e}"))?;
        self.read_ahead.validate()
    }
}

/// `host:port` with a non-empty host and a `u16` port, no whitespace; a
/// host containing `:` (IPv6) must be bracketed: `[::1]:6881`.
fn is_host_port(entry: &str) -> bool {
    if entry.chars().any(char::is_whitespace) {
        return false;
    }
    let Some((host, port)) = entry.rsplit_once(':') else {
        return false;
    };
    let digits = !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit());
    if !digits || port.parse::<u16>().is_err() || host.is_empty() {
        return false;
    }
    match host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        Some(inner) => !inner.is_empty() && !inner.contains(['[', ']']),
        None => !host.contains([':', '[', ']']),
    }
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            listen_addrs: vec![
                SocketAddr::from((Ipv4Addr::UNSPECIFIED, 6881)),
                SocketAddr::from((Ipv6Addr::UNSPECIFIED, 6881)),
            ],
            max_peers_per_torrent: 50,
            max_peers_global: 200,
            max_peers_per_ip: 1,
            per_ip_exempt_loopback: true,
            ban_endpoints: false,
            max_half_open: 10,
            unchoke_slots: 4,
            choking_algorithm: ChokingAlgorithm::default(),
            seed_choking_algorithm: SeedChokingAlgorithm::default(),
            optimistic_unchoke_slots: 0,
            optimistic_unchoke_interval: Duration::from_secs(30),
            seeding_piece_quota: 20,
            snub_timeout: Duration::from_secs(60),
            request_queue_time: Duration::from_secs(3),
            max_outstanding_requests: 500,
            max_incoming_requests: 250,
            connect_timeout: Duration::from_secs(10),
            handshake_timeout: Duration::from_secs(10),
            encryption: EncryptionPolicy::default(),
            peer_idle_timeout: Duration::from_secs(120),
            keepalive_interval: Duration::from_secs(60),
            request_timeout: Duration::from_secs(60),
            peer_retry_delay: Duration::from_secs(60),
            rechoke_interval: Duration::from_secs(10),
            status_interval: Duration::from_millis(500),
            event_queue: 4096,
            num_want: 50,
            tracker: TrackerConfig::default(),
            tracker_retry_base: Duration::from_secs(60),
            metadata_peer_grace: Duration::from_secs(30),
            max_metadata_size: 16 * 1024 * 1024,
            dht_enabled: true,
            dht_bootstrap_nodes: DEFAULT_DHT_BOOTSTRAP.map(str::to_owned).to_vec(),
            dht_read_only: false,
            dht_state: None,
            dht_announce_interval: Duration::from_secs(15 * 60),
            dht_query_timeout: Duration::from_secs(3),
            dht_query_rate_per_ip: 5,
            pex_enabled: true,
            pex_interval: Duration::from_secs(60),
            enable_outgoing_tcp: true,
            enable_incoming_tcp: true,
            enable_outgoing_utp: true,
            enable_incoming_utp: true,
            utp_connect_timeout: Duration::from_secs(3),
            enable_holepunch: true,
            enable_lsd: true,
            lsd_groups: vec![LSD_V4, LSD_V6],
            lsd_announce_interval: Duration::from_secs(5 * 60),
            lsd_bind: None,
            enable_upnp: true,
            enable_natpmp: true,
            nat_gateway: None,
            upnp_ssdp_addr: SSDP_V4,
            natpmp_port: NATPMP_PORT,
            nat_retry_base: Duration::from_secs(60),
            nat_emulation: false,
            disk_threads: 8,
            max_active_checks: 1,
            cache_size: 64 * 1024 * 1024,
            cache_expiry: Duration::from_secs(30),
            rate_limit: RateLimit::default(),
            peer_class_limits: PeerClassLimits::default(),
            rate_limit_local_peers: false,
            mixed_mode: MixedMode::default(),
            read_ahead: ReadAhead::default(),
            proxy: ProxyConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_read_ahead_time_is_invalid() {
        let mut s = Settings::default();
        assert_eq!(s.read_ahead, ReadAhead::default());
        assert!(s.validate().is_ok());
        s.read_ahead = ReadAhead::new(16 << 20, Duration::ZERO, None);
        assert_eq!(
            s.validate().unwrap_err(),
            "read_ahead.time must be greater than zero"
        );
        s.read_ahead = ReadAhead::new(16 << 20, Duration::from_secs(10), Some(0));
        assert_eq!(
            s.validate().unwrap_err(),
            "read_ahead.bitrate must be greater than zero"
        );
        s.read_ahead = ReadAhead::new(16 << 20, Duration::from_secs(10), Some(1));
        assert!(s.validate().is_ok());
    }
}
