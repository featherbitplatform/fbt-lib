//! The session: torrent registry, peer listener, events and limits.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use fbt_core::udp::{DatagramHandler, DatagramSender};
use fbt_core::{InfoHashes, PeerId};
use fbt_dht::{Dht, DhtConfig, DhtState, DhtStats};
use fbt_metainfo::{Magnet, Metainfo};
use fbt_net::demux::{DemuxStats, PacketKind, SharedUdpSocket};
use fbt_tracker::TrackerClient;
use fbt_utp::UtpSocket;
use fbt_wire::req2_hash;
use tokio::sync::{broadcast, oneshot};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::bandwidth::{Bandwidth, PeerClassLimits, RateLimit};
use crate::disk::DiskPool;
use crate::error::SessionError;
use crate::event::{Event, EventMask, EventStream};
use crate::listener::ListenerState;
use crate::net::{Endpoint, ListenError, bind_all, utp_for, utp_sockets};
use crate::settings::Settings;
use crate::status::TorrentStatus;
use crate::storage::cache::{CacheBudget, CacheStats};
use crate::storage::{FsStorage, Storage};
use crate::torrent::{self, TorrentHandle};

/// Download priority of a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum Priority {
    /// Not downloaded.
    Skip,
    /// Downloaded after everything else.
    Low,
    /// The default.
    #[default]
    Normal,
    /// Downloaded first.
    High,
}

impl Priority {
    /// The piece priority this maps to (0, 1, 4 or 7).
    pub(crate) fn piece_priority(self) -> u8 {
        match self {
            Priority::Skip => 0,
            Priority::Low => 1,
            Priority::Normal => 4,
            Priority::High => 7,
        }
    }
}

/// What to do with a removed torrent's files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RemoveMode {
    /// Leave the files on disk.
    KeepData,
    /// Delete the torrent's files.
    DeleteData,
}

/// What a torrent is added from.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TorrentSource {
    /// A parsed `.torrent`.
    Metainfo(Box<Metainfo>),
    /// A magnet link: the metadata is fetched from peers (BEP 9) and
    /// verified against its info hash.
    Magnet(Magnet),
}

impl TorrentSource {
    /// The info hashes the torrent is known by.
    pub(crate) fn info_hashes(&self) -> InfoHashes {
        match self {
            TorrentSource::Metainfo(m) => m.info_hashes,
            TorrentSource::Magnet(m) => m.info_hashes,
        }
    }
}

/// Parameters for [`Session::add_torrent`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct AddTorrent {
    /// The torrent.
    pub source: TorrentSource,
    /// Directory the torrent's files live below.
    pub save_path: PathBuf,
    /// Start paused (the existing data is still checked).
    pub paused: bool,
    /// Download pieces in index order instead of rarest first.
    pub sequential: bool,
    /// One per file (pad files included); empty means all `Normal`. Must be
    /// empty for a magnet (its `so` selection applies instead).
    pub file_priorities: Vec<Priority>,
    /// Custom storage; `None` uses the filesystem below `save_path`.
    pub storage: Option<Arc<dyn Storage>>,
    /// The torrent's own limit (0: unlimited), within the session's. Like
    /// the global limit it applies to local-network peers only with
    /// [`Settings::rate_limit_local_peers`]. Changed at runtime with
    /// [`TorrentHandle::set_rate_limit`].
    pub rate_limit: RateLimit,
}

impl AddTorrent {
    /// Adds `metainfo` with its files below `save_path`, every other field
    /// at its default.
    pub fn new(metainfo: Metainfo, save_path: PathBuf) -> Self {
        Self::from_source(TorrentSource::Metainfo(Box::new(metainfo)), save_path)
    }

    /// Adds a magnet link with its files below `save_path`, every other
    /// field at its default.
    pub fn magnet(magnet: Magnet, save_path: PathBuf) -> Self {
        Self::from_source(TorrentSource::Magnet(magnet), save_path)
    }

    fn from_source(source: TorrentSource, save_path: PathBuf) -> Self {
        AddTorrent {
            source,
            save_path,
            paused: false,
            sequential: false,
            file_priorities: Vec::new(),
            storage: None,
            rate_limit: RateLimit::default(),
        }
    }
}

/// State shared by the session, its listener and every torrent actor.
#[derive(Debug)]
pub(crate) struct Shared {
    pub settings: Arc<Settings>,
    pub peer_id: PeerId,
    /// Port of the first peer listener: announced over HTTP and to the DHT.
    pub listen_port: u16,
    /// Port of the first IPv4 listener, `None` without one: UDP announces
    /// over IPv4 and IPv4 peers' extension handshakes carry it.
    pub listen_port_v4: Option<u16>,
    /// Port of the first IPv6 listener, likewise for IPv6.
    pub listen_port_v6: Option<u16>,
    /// Shared by every torrent's announcer.
    pub tracker: TrackerClient,
    pub events: broadcast::Sender<Event>,
    pub shutdown: AtomicBool,
    /// Peer tasks alive across the session (connecting or connected).
    pub global_peers: AtomicUsize,
    /// Outgoing connections still connecting or handshaking.
    pub half_open: AtomicUsize,
    /// The session-wide DHT node; `None` with `dht_enabled` off.
    pub dht: Option<Dht>,
    /// The DHT's UDP sockets; `Port` messages carry the port of the one
    /// matching the peer's address family ([`dht_port_for`]).
    pub dht_addrs: Vec<SocketAddr>,
    /// One uTP socket per shared UDP socket, in the same order (the
    /// endpoints', then the per-family fallbacks).
    pub utp: Vec<UtpSocket>,
    /// Runs every storage job and bounds the concurrent checks.
    pub disk: DiskPool,
    /// The byte budget every torrent's write-back cache draws from.
    pub cache: Arc<CacheBudget>,
    /// The global and class rate limits.
    pub bandwidth: Arc<Bandwidth>,
    /// BEP 55 holepunch activity, for [`Session::holepunch_stats`].
    pub holepunch: HolepunchCounters,
}

/// BEP 55 holepunch messages handled, over every torrent of the session.
#[derive(Debug, Default)]
pub(crate) struct HolepunchCounters {
    /// `Rendezvous` messages we sent to a relay.
    pub rendezvous_sent: AtomicU64,
    /// Rendezvous we relayed (`Connect` sent to both peers).
    pub relayed: AtomicU64,
    /// `Connect` messages we accepted and acted on.
    pub connects_received: AtomicU64,
}

impl HolepunchCounters {
    pub(crate) fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// The DHT port to send a peer at `peer` in `Port`: that of our DHT socket
/// of the same address family (an IPv4-mapped IPv6 peer counts as IPv4),
/// `None` without one, since the peer could not reach the other socket.
pub(crate) fn dht_port_for(dht_addrs: &[SocketAddr], peer: &SocketAddr) -> Option<u16> {
    let v4 = peer.ip().to_canonical().is_ipv4();
    dht_addrs
        .iter()
        .find(|a| a.is_ipv4() == v4)
        .map(SocketAddr::port)
}

impl Shared {
    /// The listen port of `peer`'s address family (an IPv4-mapped IPv6
    /// peer counts as IPv4): the port our listener of the family the
    /// connection runs over has, else the first listener's.
    pub(crate) fn listen_port_for(&self, peer: &SocketAddr) -> u16 {
        let family = if peer.ip().to_canonical().is_ipv4() {
            self.listen_port_v4
        } else {
            self.listen_port_v6
        };
        family.unwrap_or(self.listen_port)
    }

    /// The uTP socket to dial `peer` from: the first of its address family,
    /// as for the DHT and UDP trackers. `None` without one.
    pub(crate) fn utp_for(&self, peer: &SocketAddr) -> Option<&UtpSocket> {
        utp_for(&self.utp, peer)
    }

    pub(crate) fn emit(&self, ev: Event) {
        // No subscribers is fine.
        let _ = self.events.send(ev);
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
}

/// Takes one unit of `counter` if it is below `max`.
pub(crate) fn try_reserve(counter: &AtomicUsize, max: usize) -> bool {
    counter
        .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
            (n < max).then_some(n + 1)
        })
        .is_ok()
}

/// Returns one unit of `counter`.
pub(crate) fn release(counter: &AtomicUsize) {
    let _ = counter.try_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
}

/// One registered torrent.
#[derive(Debug)]
pub(crate) struct TorrentEntry {
    pub handle: TorrentHandle,
    pub wire_hash: [u8; 20],
}

#[derive(Debug, Default)]
pub(crate) struct Registry {
    pub torrents: HashMap<InfoHashes, TorrentEntry>,
    pub by_wire: HashMap<[u8; 20], InfoHashes>,
    /// `HASH('req2', wire hash)` to the wire hash (the MSE `SKEY`), kept
    /// with `by_wire`, so an MSE responder finds its torrent in one lookup.
    pub by_req2: HashMap<[u8; 20], [u8; 20]>,
}

#[derive(Debug)]
pub(crate) struct SessionInner {
    pub shared: Arc<Shared>,
    /// The TCP listeners' addresses, in settings order.
    local_addrs: Vec<SocketAddr>,
    listen_errors: Vec<ListenError>,
    /// Every shared UDP socket (the endpoints', then the per-family
    /// fallbacks), closed at shutdown.
    udp: Vec<SharedUdpSocket>,
    /// Shared by every accept loop of the session.
    listener_state: Arc<ListenerState>,
    registry: Mutex<Registry>,
    /// One TCP accept loop per listener (with `enable_incoming_tcp`) and
    /// one uTP accept loop per uTP socket (with `enable_incoming_utp`).
    listeners: Mutex<Vec<JoinHandle<()>>>,
}

impl SessionInner {
    pub(crate) fn registry(&self) -> MutexGuard<'_, Registry> {
        self.registry.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Unregisters a torrent; true if it was registered.
    pub(crate) fn unregister(&self, ih: &InfoHashes) -> bool {
        let mut reg = self.registry();
        match reg.torrents.remove(ih) {
            Some(e) => {
                reg.by_wire.remove(&e.wire_hash);
                reg.by_req2.remove(&req2_hash(&e.wire_hash));
                true
            }
            None => false,
        }
    }

    /// Stops every accept loop (dropping its listener).
    fn stop_listeners(&self) {
        let tasks = std::mem::take(&mut *self.listeners.lock().unwrap_or_else(|e| e.into_inner()));
        for t in tasks {
            t.abort();
        }
    }

    /// Resets every uTP connection and stops the uTP sockets' tasks.
    fn shutdown_utp(&self) {
        for u in &self.shared.utp {
            u.shutdown_now();
        }
    }

    /// Closes every shared UDP socket, freeing its port.
    fn close_udp(&self) {
        for u in &self.udp {
            u.close();
        }
    }
}

impl Drop for SessionInner {
    fn drop(&mut self) {
        self.stop_listeners();
        self.shutdown_utp();
        self.close_udp();
    }
}

/// A BitTorrent session: a set of torrents sharing the listeners, one peer
/// id, connection limits and an event stream.
///
/// Cheap to clone; every clone is the same session.
#[derive(Debug, Clone)]
pub struct Session {
    inner: Arc<SessionInner>,
}

impl Session {
    /// Starts a session and binds every address of
    /// [`Settings::listen_addrs`]: a TCP peer listener and, on the same
    /// port, a UDP socket that the DHT and UDP tracker announces share.
    /// Addresses that cannot be bound are skipped and reported by
    /// [`listen_errors`](Self::listen_errors).
    ///
    /// Fails with [`SessionError::InvalidSettings`] for settings it cannot
    /// work with (see [`Settings`]) and with [`SessionError::Bind`] (the
    /// first error) when no address binds.
    pub async fn new(settings: Settings) -> Result<Session, SessionError> {
        settings.validate().map_err(SessionError::InvalidSettings)?;
        let (endpoints, listen_errors) = bind_all(&settings).await?;
        let local_addrs: Vec<SocketAddr> = endpoints.iter().map(|e| e.addr).collect();
        let mut udp: Vec<SharedUdpSocket> =
            endpoints.iter().filter_map(|e| e.udp.clone()).collect();
        udp.extend(fallback_udp(&endpoints));
        let utp = utp_sockets(&settings, &udp);
        let senders: Vec<Arc<dyn DatagramSender>> =
            udp.iter().map(SharedUdpSocket::sender).collect();
        let tracker = match TrackerClient::with_udp_sockets(settings.tracker.clone(), senders) {
            Ok(t) => t,
            Err(e) => {
                for u in &utp {
                    u.shutdown_now();
                }
                udp.iter().for_each(SharedUdpSocket::close);
                return Err(SessionError::Tracker(e));
            }
        };
        let tracker_input = tracker.udp_handler();
        for u in &udp {
            u.set_handler(PacketKind::Tracker, Arc::clone(&tracker_input));
        }
        let (events, _) = broadcast::channel(settings.event_queue);
        let dht = if settings.dht_enabled {
            start_dht(&settings, &udp).await
        } else {
            None
        };
        let dht_addrs = dht.as_ref().map(Dht::local_addrs).unwrap_or_default();
        let listen_port = local_addrs.first().map_or(0, SocketAddr::port);
        let first_port = |v4: bool| {
            local_addrs
                .iter()
                .find(|a| a.is_ipv4() == v4)
                .map(SocketAddr::port)
        };
        let (listen_port_v4, listen_port_v6) = (first_port(true), first_port(false));
        // One for every accept loop, so all of them together allow only
        // `max_peers_global` pending handshakes.
        let listener_state = Arc::new(ListenerState::new(settings.max_peers_global));
        let disk = DiskPool::new(settings.disk_threads, settings.max_active_checks);
        let cache = Arc::new(CacheBudget::new(settings.cache_size));
        let bandwidth = Arc::new(Bandwidth::new(&settings));
        bandwidth.spawn_rebalance();
        let shared = Arc::new(Shared {
            settings: Arc::new(settings),
            peer_id: PeerId::generate(),
            listen_port,
            listen_port_v4,
            listen_port_v6,
            tracker,
            events,
            shutdown: AtomicBool::new(false),
            global_peers: AtomicUsize::new(0),
            half_open: AtomicUsize::new(0),
            dht,
            dht_addrs,
            utp,
            disk,
            cache,
            bandwidth,
            holepunch: HolepunchCounters::default(),
        });
        if let Some(dht) = &shared.dht {
            watch_bootstrap(dht, Arc::downgrade(&shared));
        }
        let inner = Arc::new(SessionInner {
            shared: Arc::clone(&shared),
            local_addrs,
            listen_errors,
            udp,
            listener_state,
            registry: Mutex::new(Registry::default()),
            listeners: Mutex::new(Vec::new()),
        });
        // No TCP listeners with `enable_incoming_tcp` off.
        let mut tasks: Vec<JoinHandle<()>> = endpoints
            .into_iter()
            .filter_map(|e| e.tcp)
            .map(|tcp| {
                crate::listener::spawn(
                    tcp,
                    Arc::clone(&inner.listener_state),
                    Arc::downgrade(&inner),
                    Arc::clone(&shared),
                )
            })
            .collect();
        if shared.settings.enable_incoming_utp {
            tasks.extend(shared.utp.iter().map(|u| {
                crate::listener::spawn_utp(
                    u.clone(),
                    Arc::clone(&inner.listener_state),
                    Arc::downgrade(&inner),
                    Arc::clone(&shared),
                )
            }));
        }
        *inner.listeners.lock().unwrap_or_else(|e| e.into_inner()) = tasks;
        Ok(Session { inner })
    }

    /// BEP 55 holepunch activity so far, over every torrent: `Rendezvous`
    /// messages sent, rendezvous relayed and `Connect` messages acted on.
    /// Internal (tests observe the exchange with it); not part of the
    /// stable API.
    #[doc(hidden)]
    pub fn holepunch_stats(&self) -> (u64, u64, u64) {
        let h = &self.inner.shared.holepunch;
        (
            h.rendezvous_sent.load(Ordering::Relaxed),
            h.relayed.load(Ordering::Relaxed),
            h.connects_received.load(Ordering::Relaxed),
        )
    }

    /// The session's DHT node, `None` with [`Settings::dht_enabled`] off.
    pub fn dht_stats(&self) -> Option<DhtStats> {
        self.inner.shared.dht.as_ref().map(Dht::stats)
    }

    /// The DHT's state (node ids and known nodes) for
    /// [`Settings::dht_state`] in a later session, `None` with the DHT off.
    /// The library never writes it anywhere: storing it is the caller's
    /// job, typically just before [`shutdown`](Self::shutdown).
    ///
    /// Fails with [`SessionError::Shutdown`] once the session (and its
    /// DHT) has shut down.
    pub async fn dht_state(&self) -> Result<Option<Vec<u8>>, SessionError> {
        let Some(dht) = &self.inner.shared.dht else {
            return Ok(None);
        };
        let state = dht.state().await.map_err(|_| SessionError::Shutdown)?;
        Ok(Some(state.to_bytes()))
    }

    /// Bytes held by the write-back cache of every torrent, against
    /// [`Settings::cache_size`].
    pub fn cache_stats(&self) -> CacheStats {
        self.inner.shared.cache.stats()
    }

    /// Sets the session-wide rate limit ([`Settings::rate_limit`]); it
    /// applies at once, and to waiting connections within a second.
    pub fn set_rate_limit(&self, limit: RateLimit) {
        self.inner.shared.bandwidth.set_global(limit);
    }

    /// Sets the peer class limits ([`Settings::peer_class_limits`]); they
    /// apply at once, and to waiting connections within a second.
    pub fn set_peer_class_limits(&self, limits: PeerClassLimits) {
        self.inner.shared.bandwidth.set_classes(limits);
    }

    /// The address of the first peer listener (the first of
    /// [`local_addrs`](Self::local_addrs)).
    pub fn local_addr(&self) -> SocketAddr {
        // `Session::new` fails unless at least one listener bound.
        self.inner
            .local_addrs
            .first()
            .copied()
            .unwrap_or_else(|| SocketAddr::from((std::net::Ipv4Addr::UNSPECIFIED, 0)))
    }

    /// The addresses the peer listeners are bound to, in
    /// [`Settings::listen_addrs`] order, without the ones that failed.
    pub fn local_addrs(&self) -> Vec<SocketAddr> {
        self.inner.local_addrs.clone()
    }

    /// The listen addresses that could not be bound, fully or only on
    /// their UDP side, when the session started. They are not events:
    /// binding happens in [`Session::new`], before anyone can subscribe.
    pub fn listen_errors(&self) -> Vec<ListenError> {
        self.inner.listen_errors.clone()
    }

    /// Datagram counts of each shared UDP socket, by its address. For
    /// tests; not part of the stable API.
    #[doc(hidden)]
    pub fn demux_stats(&self) -> Vec<(SocketAddr, DemuxStats)> {
        self.inner
            .udp
            .iter()
            .map(|u| (u.local_addr(), u.stats()))
            .collect()
    }

    /// Adds a torrent and starts checking its existing data. A magnet
    /// starts in [`TorrentState::DownloadingMetadata`](crate::TorrentState)
    /// and is registered under its own info hashes, which stay its identity
    /// after the metadata arrives.
    ///
    /// Fails with [`SessionError::V2OnlyUnsupported`] for a magnet without a
    /// v1 info hash and with [`SessionError::InvalidPriorities`] (expecting
    /// 0) for a magnet with file priorities.
    pub async fn add_torrent(&self, params: AddTorrent) -> Result<TorrentHandle, SessionError> {
        let shared = &self.inner.shared;
        if shared.is_shutdown() {
            return Err(SessionError::Shutdown);
        }
        let num_files = match &params.source {
            TorrentSource::Metainfo(m) => m.info.files.len(),
            TorrentSource::Magnet(m) => {
                if m.info_hashes.v1.is_none() {
                    return Err(SessionError::V2OnlyUnsupported);
                }
                // Unknown until the metadata arrives.
                0
            }
        };
        if !params.file_priorities.is_empty() && params.file_priorities.len() != num_files {
            return Err(SessionError::InvalidPriorities {
                expected: num_files,
                got: params.file_priorities.len(),
            });
        }
        let ih = params.source.info_hashes();
        let wire_hash = ih.wire_hash();
        let mut reg = self.inner.registry();
        if shared.is_shutdown() {
            return Err(SessionError::Shutdown);
        }
        if reg.torrents.contains_key(&ih) || reg.by_wire.contains_key(&wire_hash) {
            return Err(SessionError::DuplicateTorrent);
        }
        // A magnet's storage is built once its metadata is known.
        let storage: Option<Arc<dyn Storage>> = match (&params.source, params.storage.clone()) {
            (TorrentSource::Magnet(_), _) => None,
            (TorrentSource::Metainfo(_), Some(s)) => Some(s),
            (TorrentSource::Metainfo(m), None) => {
                Some(Arc::new(FsStorage::new(params.save_path.clone(), m)))
            }
        };
        shared.emit(Event::TorrentAdded { torrent: ih });
        let handle = torrent::spawn(
            params,
            storage,
            Arc::clone(shared),
            Arc::downgrade(&self.inner),
        );
        reg.by_wire.insert(wire_hash, ih);
        reg.by_req2.insert(req2_hash(&wire_hash), wire_hash);
        reg.torrents.insert(
            ih,
            TorrentEntry {
                handle: handle.clone(),
                wire_hash,
            },
        );
        Ok(handle)
    }

    /// The torrent with these info hashes.
    pub fn torrent(&self, ih: &InfoHashes) -> Option<TorrentHandle> {
        self.inner
            .registry()
            .torrents
            .get(ih)
            .map(|e| e.handle.clone())
    }

    /// Every torrent in the session.
    pub fn torrents(&self) -> Vec<TorrentHandle> {
        self.inner
            .registry()
            .torrents
            .values()
            .map(|e| e.handle.clone())
            .collect()
    }

    /// The status of every torrent.
    pub fn torrents_status(&self) -> Vec<TorrentStatus> {
        self.torrents().iter().map(TorrentHandle::status).collect()
    }

    /// Subscribes to the events in `mask` (plus [`Event::Lagged`]).
    pub fn subscribe(&self, mask: EventMask) -> EventStream {
        EventStream::new(self.inner.shared.events.subscribe(), mask)
    }

    /// Stops the listeners and every torrent, waiting for pending writes
    /// and for up to 5 s for the `stopped` announces (sent in parallel),
    /// then the DHT and the uTP sockets, and closes the shared UDP sockets,
    /// freeing every port.
    /// Idempotent; afterwards every command returns
    /// [`SessionError::Shutdown`].
    pub async fn shutdown(&self) {
        if self.inner.shared.shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        self.inner.stop_listeners();
        let handles: Vec<TorrentHandle> = {
            let mut reg = self.inner.registry();
            reg.by_wire.clear();
            reg.by_req2.clear();
            reg.torrents.drain().map(|(_, e)| e.handle).collect()
        };
        let mut waits: Vec<oneshot::Receiver<()>> = Vec::new();
        for h in &handles {
            if let Some(rx) = h.send_shutdown().await {
                waits.push(rx);
            }
        }
        for rx in waits {
            let _ = rx.await;
        }
        if let Some(dht) = &self.inner.shared.dht {
            dht.shutdown().await;
        }
        // The peers are gone; the uTP sockets stop before the UDP sockets
        // under them close.
        for u in &self.inner.shared.utp {
            u.shutdown().await;
        }
        // Last: the `stopped` announces and the DHT used these sockets.
        self.inner.close_udp();
    }
}

/// A UDP socket on a free port for each address family that has a
/// listener but no shared UDP socket (its port was taken), on the IP of the
/// family's first listener, so the DHT and UDP trackers still run over that
/// family. One that cannot be bound is logged and left out.
fn fallback_udp(endpoints: &[Endpoint]) -> Vec<SharedUdpSocket> {
    let mut out = Vec::new();
    for v4 in [true, false] {
        let mut family = endpoints.iter().filter(|e| e.addr.is_ipv4() == v4);
        let Some(first) = family.clone().next() else {
            continue;
        };
        if family.any(|e| e.udp.is_some()) {
            continue;
        }
        let addr = SocketAddr::new(first.addr.ip(), 0);
        match SharedUdpSocket::bind(addr) {
            Ok(u) => {
                warn!(
                    listener = %first.addr,
                    udp = %u.local_addr(),
                    "UDP port taken; the DHT and UDP trackers use a free port"
                );
                out.push(u);
            }
            Err(e) => warn!(%addr, error = %e, "cannot bind a UDP socket for this family"),
        }
    }
    out
}

/// Starts the session's DHT on the shared UDP sockets, the first one of
/// each address family, and registers it as their DHT handler (so replies
/// leave from the address a query reached). A DHT that cannot start at all
/// (no socket, say) is logged and left out.
async fn start_dht(settings: &Settings, udp: &[SharedUdpSocket]) -> Option<Dht> {
    let state = settings
        .dht_state
        .as_deref()
        .and_then(|b| match DhtState::from_bytes(b) {
            Ok(s) => Some(s),
            Err(e) => {
                warn!(error = %e, "ignoring the saved DHT state; starting fresh");
                None
            }
        });
    let mut c = DhtConfig::default();
    c.bootstrap = settings.dht_bootstrap_nodes.clone();
    c.read_only = settings.dht_read_only;
    c.state = state;
    c.query_timeout = settings.dht_query_timeout;
    c.query_rate_per_ip = settings.dht_query_rate_per_ip;
    // At most one socket per family: the first one wins.
    let mut sockets: Vec<&SharedUdpSocket> = Vec::new();
    for u in udp {
        if !sockets
            .iter()
            .any(|s| s.local_addr().is_ipv4() == u.local_addr().is_ipv4())
        {
            sockets.push(u);
        }
    }
    let senders = sockets.iter().map(|s| s.sender()).collect();
    let r = Dht::spawn_on(c, senders).await.map(|(dht, input)| {
        let input: Arc<dyn DatagramHandler> = Arc::new(input);
        for s in &sockets {
            s.set_handler(PacketKind::Dht, Arc::clone(&input));
        }
        dht
    });
    match r {
        Ok(d) => Some(d),
        Err(e) => {
            warn!(error = %e, "cannot start the DHT; running without it");
            None
        }
    }
}

/// Emits [`Event::DhtBootstrapped`] the first time the DHT's routing table
/// fills, then ends; also ends with the DHT or the session.
fn watch_bootstrap(dht: &Dht, shared: std::sync::Weak<Shared>) {
    let mut stats = dht.subscribe_stats();
    tokio::spawn(async move {
        let Ok(s) = stats.wait_for(|s| s.bootstrapped).await else {
            return;
        };
        let nodes = s.nodes_v4.saturating_add(s.nodes_v6);
        drop(s);
        if let Some(shared) = shared.upgrade() {
            shared.emit(Event::DhtBootstrapped { nodes });
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listen_port_follows_the_peers_family() {
        let shared = |v4: Option<u16>, v6: Option<u16>| Shared {
            settings: Arc::new(Settings::default()),
            peer_id: PeerId::generate(),
            listen_port: 7000,
            listen_port_v4: v4,
            listen_port_v6: v6,
            tracker: TrackerClient::new(Default::default()).unwrap(),
            events: broadcast::channel(1).0,
            shutdown: AtomicBool::new(false),
            global_peers: AtomicUsize::new(0),
            half_open: AtomicUsize::new(0),
            dht: None,
            dht_addrs: Vec::new(),
            utp: Vec::new(),
            disk: crate::disk::DiskPool::new(1, 1),
            cache: Arc::new(crate::storage::cache::CacheBudget::new(0)),
            bandwidth: Arc::new(crate::bandwidth::Bandwidth::new(&Settings::default())),
            holepunch: HolepunchCounters::default(),
        };
        let peer4: SocketAddr = "10.0.0.1:6881".parse().unwrap();
        let mapped: SocketAddr = "[::ffff:10.0.0.1]:6881".parse().unwrap();
        let peer6: SocketAddr = "[2001:db8::1]:6881".parse().unwrap();
        let both = shared(Some(7000), Some(7001));
        assert_eq!(both.listen_port_for(&peer4), 7000);
        assert_eq!(both.listen_port_for(&mapped), 7000);
        assert_eq!(both.listen_port_for(&peer6), 7001);
        // No listener of the family: the first listener's port.
        let v6_only = shared(None, Some(7000));
        assert_eq!(v6_only.listen_port_for(&peer4), 7000);
    }

    #[test]
    fn port_message_carries_the_dht_port_of_the_peers_family() {
        let v4: SocketAddr = "0.0.0.0:7001".parse().unwrap();
        let v6: SocketAddr = "[::]:7002".parse().unwrap();
        let peer4: SocketAddr = "10.0.0.1:6881".parse().unwrap();
        let peer6: SocketAddr = "[2001:db8::1]:6881".parse().unwrap();
        let mapped: SocketAddr = "[::ffff:10.0.0.1]:6881".parse().unwrap();
        let both = [v4, v6];
        assert_eq!(dht_port_for(&both, &peer4), Some(7001));
        assert_eq!(dht_port_for(&both, &peer6), Some(7002));
        assert_eq!(dht_port_for(&both, &mapped), Some(7001));
        // Only one family bound: the other gets no `Port`.
        assert_eq!(dht_port_for(&[v6], &peer4), None);
        assert_eq!(dht_port_for(&[v4], &peer6), None);
        assert_eq!(dht_port_for(&[], &peer4), None);
    }
}
