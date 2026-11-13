//! The session: torrent registry, peer listener, events and limits.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};

use fbt_core::udp::{DatagramHandler, DatagramSender};
use fbt_core::{InfoHashes, PeerId};
use fbt_dht::{
    Dht, DhtConfig, DhtError, DhtSamples, DhtScrape, DhtState, DhtStats, MutableItem, NodeId,
};
use fbt_metainfo::{Magnet, Metainfo};
use fbt_net::PortMapper;
use fbt_net::demux::{DemuxStats, PacketKind, SharedUdpSocket};
use fbt_net::ipfilter::IpFilter;
use fbt_net::proxy::{ProxyError, ProxyKind, udp_associate};
use fbt_tracker::TrackerClient;
use fbt_utp::UtpSocket;
use fbt_wire::req2_hash;
use tokio::sync::{Notify, broadcast, oneshot};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::bandwidth::{Bandwidth, PeerClassLimits, RateLimit};
use crate::bans::{BanList, BlockReason};
use crate::disk::DiskPool;
use crate::error::SessionError;
use crate::event::{Event, EventMask, EventStream, ExternalIpSource};
use crate::listener::ListenerState;
use crate::net::{
    Endpoint, ExternalIp, ListenError, LsdHub, bind_all, spawn_lsd_task, spawn_nat_task,
    start_port_mapping, utp_for, utp_sockets,
};
use crate::queue::{QueueEntry, QueueSettings, is_slow, plan, running_since};
use crate::resume::{ResumeError, ResumeState, SessionState, decode_resume};
use crate::settings::Settings;
use crate::status::TorrentStatus;
use crate::storage::cache::{CacheBudget, CacheStats};
use crate::storage::{FsStorage, Storage};
use crate::torrent::{self, TorrentHandle};

/// Download priority of a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
    /// Under the session's queue ([`Settings::queue`]; true): it may be
    /// added [`Queued`](crate::TorrentState::Queued) and is started and
    /// stopped as slots free and fill. Changed with
    /// [`TorrentHandle::set_auto_managed`].
    pub auto_managed: bool,
    /// What [`from_resume`](Self::from_resume) read beyond the public
    /// fields.
    pub(crate) resume: Option<Box<ResumeState>>,
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
            auto_managed: true,
            resume: None,
        }
    }

    /// Adds a torrent from its resume data
    /// ([`TorrentHandle::save_resume_data`]): the torrent (its `info`
    /// dictionary, or the magnet while the metadata was missing), save
    /// path, flags, file priorities, rate limit and `auto_managed` fill the
    /// public fields; the verified pieces, renamed files, web seeds, queue
    /// position, counters, times and peers come along. `storage` may still
    /// be set; otherwise the files are found below the save path at their
    /// renamed paths.
    ///
    /// At the add the saved pieces are trusted without a check when every
    /// file holding one (and the part file, for pieces of skipped files
    /// kept there) has the length and modification time it had at the save
    /// ([`Storage::stamps`]); otherwise, or without stamps from an
    /// [`FsStorage`], the data is checked in full.
    /// [`TorrentStatus::resume_trusted`] tells which happened. The torrent
    /// takes its saved queue position when the queue has that many
    /// torrents, else the bottom.
    ///
    /// Resume data is hostile input: anything longer than
    /// [`MAX_RESUME_BYTES`](crate::MAX_RESUME_BYTES), malformed, of another
    /// format version, whose `info` does not hash to its info hash, with a
    /// field of the wrong type or length (`pieces` for another piece count,
    /// say), or with a renamed path that is unsafe or collides, fails with
    /// a [`ResumeError`] naming what was wrong. At most
    /// [`MAX_RESUME_PEERS`](crate::MAX_RESUME_PEERS) peers per address
    /// family are kept.
    pub fn from_resume(bytes: &[u8]) -> Result<AddTorrent, ResumeError> {
        let d = decode_resume(bytes)?;
        Ok(AddTorrent {
            source: d.source,
            save_path: d.save_path,
            paused: d.paused,
            sequential: d.sequential,
            file_priorities: d.file_priorities,
            storage: None,
            rate_limit: d.rate_limit,
            auto_managed: d.auto_managed,
            resume: Some(Box::new(d.state)),
        })
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
    /// Local service discovery (BEP 14): the instance and its announce
    /// bookkeeping.
    pub lsd: LsdHub,
    /// UPnP and PCP/NAT-PMP port mapping; `None` with both off.
    pub portmap: Option<PortMapper>,
    /// The external address as the router and peers report it.
    pub external_ip: Arc<ExternalIp>,
    /// The session's IP filter ([`Session::set_ip_filter`]); swapped whole,
    /// so a reader holds the lock only for the lookup.
    pub ip_filter: RwLock<Arc<IpFilter>>,
    /// Peers banned for the rest of the session (smart ban).
    pub bans: Mutex<BanList>,
    /// The queue's limits ([`Session::set_queue_settings`]).
    pub queue: Mutex<QueueSettings>,
    /// Wakes the queue scheduler for a pass now.
    pub queue_dirty: Notify,
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

    /// Why a connection to or from `addr` is refused: the IP filter blocks
    /// its IP, or it is banned (its IP, or with `ban_endpoints` that
    /// endpoint). `None` lets it through. The per-IP limit is the
    /// torrent's to check.
    pub(crate) fn blocked(&self, addr: SocketAddr) -> Option<BlockReason> {
        let filtered = self
            .ip_filter
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .is_blocked(addr.ip());
        if filtered {
            return Some(BlockReason::IpFilter);
        }
        self.bans
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_banned(addr)
            .then_some(BlockReason::Banned)
    }

    /// Reports a refused peer ([`Event::PeerBlocked`]).
    pub(crate) fn emit_blocked(
        &self,
        torrent: Option<InfoHashes>,
        addr: SocketAddr,
        reason: BlockReason,
    ) {
        tracing::debug!(%addr, ?reason, "peer blocked");
        self.emit(Event::PeerBlocked {
            torrent,
            addr,
            reason,
        });
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// The queue's current limits.
    pub(crate) fn queue_settings(&self) -> QueueSettings {
        self.queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
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
    /// Under the queue's control.
    pub auto_managed: bool,
    /// What the scheduler last told the actor: stay queued.
    pub queued: bool,
    /// The position the actor was last told.
    pub position_sent: Option<usize>,
    /// When the queue last saw it start running past its check; `None`
    /// while it is not.
    pub running_since: Option<std::time::Instant>,
    /// Paused by the user: the actor's own flag, recorded by the actor (a
    /// paused torrent still reports its check states, not `Paused`).
    pub user_paused: bool,
    /// The last pass found it slow (hysteresis, see `queue::is_slow`).
    pub slow: bool,
    /// A `SetQueued` the last pass could not send (the actor's queue was
    /// full); retried by the next pass, or sent with a wait by the handle
    /// methods that promise the change is applied.
    pub pending_queued: Option<bool>,
}

#[derive(Debug, Default)]
pub(crate) struct Registry {
    pub torrents: HashMap<InfoHashes, TorrentEntry>,
    /// Every torrent in queue order: an index is a queue position.
    pub order: Vec<InfoHashes>,
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
                reg.order.retain(|o| o != ih);
                // Positions behind it moved up, and a slot may be free.
                self.shared.queue_dirty.notify_one();
                true
            }
            None => false,
        }
    }

    /// One scheduler pass: every torrent's queue entry from its status, the
    /// plan, and `SetQueued` / `SetQueuePosition` to the torrents whose
    /// answer or position changed. Never waits: a torrent whose command
    /// queue is full (or that is moving its storage, which holds the change
    /// until the move is done) keeps its old record and is retried on the
    /// next pass.
    pub(crate) fn queue_pass(&self) {
        let cfg = self.shared.queue_settings();
        let mut reg = self.registry();
        let reg = &mut *reg;
        let now = std::time::Instant::now();
        let mut entries = Vec::with_capacity(reg.order.len());
        for (position, ih) in reg.order.iter().enumerate() {
            let Some(e) = reg.torrents.get(ih) else {
                continue;
            };
            let (q, active) = queue_entry(e, position);
            entries.push((*ih, q, active));
        }
        let run = if cfg.unlimited() {
            // Nothing is queued: skip the sort.
            entries
                .iter()
                .map(|(_, q, _)| q.auto_managed && !q.user_paused)
                .collect()
        } else {
            let only: Vec<QueueEntry> = entries.iter().map(|(_, q, _)| q.clone()).collect();
            plan(&only, &cfg, now)
        };
        for ((ih, q, active), run) in entries.into_iter().zip(run) {
            let Some(e) = reg.torrents.get_mut(&ih) else {
                continue;
            };
            e.slow = is_slow(&q, &cfg, now);
            if e.position_sent != Some(q.position)
                && e.handle.try_cmd(torrent::Cmd::SetQueuePosition(q.position))
            {
                e.position_sent = Some(q.position);
            }
            // The user's pause and errors are not the queue's to change.
            if !e.auto_managed || q.user_paused {
                e.running_since = None;
                e.pending_queued = None;
                continue;
            }
            let queued = !run;
            e.pending_queued = None;
            if e.queued != queued {
                if e.handle.try_cmd(torrent::Cmd::SetQueued(queued)) {
                    e.queued = queued;
                } else {
                    e.pending_queued = Some(queued);
                }
            }
            e.running_since = running_since(e.running_since, e.queued, active, now);
        }
    }

    /// The queue commands of `ih` the last pass could not send, marked as
    /// sent: the caller sends them with a wait. Its position is always
    /// included when it differs from what the actor was told.
    pub(crate) fn take_unsent_queue_cmds(&self, ih: &InfoHashes) -> Vec<torrent::Cmd> {
        let mut reg = self.registry();
        let Some(position) = reg.order.iter().position(|o| o == ih) else {
            return Vec::new();
        };
        let Some(e) = reg.torrents.get_mut(ih) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        if e.position_sent != Some(position) {
            e.position_sent = Some(position);
            out.push(torrent::Cmd::SetQueuePosition(position));
        }
        if let Some(queued) = e.pending_queued.take() {
            e.queued = queued;
            out.push(torrent::Cmd::SetQueued(queued));
        }
        out
    }

    /// Records the actor's user-pause flag.
    pub(crate) fn set_user_paused(&self, ih: &InfoHashes, paused: bool) {
        if let Some(e) = self.registry().torrents.get_mut(ih) {
            e.user_paused = paused;
        }
    }

    /// Before a user resume of `ih`: whether the queue would hold it back,
    /// with its pause lifted. If so it is marked queued and true returned,
    /// so the caller queues it before resuming (no `started`/`stopped`
    /// pair); the next pass decides from there.
    pub(crate) fn queue_on_resume(&self, ih: &InfoHashes) -> bool {
        let cfg = self.shared.queue_settings();
        if cfg.unlimited() {
            return false;
        }
        let mut reg = self.registry();
        let reg = &mut *reg;
        let Some(me) = reg.order.iter().position(|o| o == ih) else {
            return false;
        };
        if reg
            .torrents
            .get(ih)
            .is_none_or(|e| !e.auto_managed || e.queued)
        {
            // Not the queue's, or already held: resume decides nothing.
            return false;
        }
        let entries: Vec<QueueEntry> = reg
            .order
            .iter()
            .enumerate()
            .filter_map(|(i, o)| {
                let e = reg.torrents.get(o)?;
                let (mut q, _) = queue_entry(e, i);
                if i == me {
                    q.user_paused = false;
                }
                Some(q)
            })
            .collect();
        let run = plan(&entries, &cfg, std::time::Instant::now());
        let held = !run.get(me).copied().unwrap_or(true);
        if held && let Some(e) = reg.torrents.get_mut(ih) {
            e.queued = true;
            e.pending_queued = None;
        }
        held
    }

    /// Moves `ih` to `position` in the queue (clamped to the last place)
    /// and runs a pass. Fails with [`SessionError::TorrentNotFound`].
    pub(crate) fn move_in_queue(
        &self,
        ih: &InfoHashes,
        position: usize,
    ) -> Result<(), SessionError> {
        {
            let mut reg = self.registry();
            let at = reg
                .order
                .iter()
                .position(|o| o == ih)
                .ok_or(SessionError::TorrentNotFound)?;
            let ih = reg.order.remove(at);
            let to = position.min(reg.order.len());
            reg.order.insert(to, ih);
        }
        self.queue_pass();
        Ok(())
    }

    /// Records whether `ih` is auto-managed. Clearing it also clears the
    /// queue's record: the actor unqueues itself.
    pub(crate) fn set_auto_managed(&self, ih: &InfoHashes, on: bool) {
        if let Some(e) = self.registry().torrents.get_mut(ih) {
            e.auto_managed = on;
            if !on {
                e.queued = false;
                e.running_since = None;
            }
        }
        self.shared.queue_dirty.notify_one();
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
    /// work with (see [`Settings`]), with [`SessionError::Bind`] (the
    /// first error) when no address binds, and with [`SessionError::Proxy`]
    /// when a SOCKS5 [`Settings::proxy`] cannot open its UDP association
    /// (the session then binds TCP listeners only and one relayed UDP
    /// socket; see [`Settings::proxy`]).
    pub async fn new(mut settings: Settings) -> Result<Session, SessionError> {
        settings.validate().map_err(SessionError::InvalidSettings)?;
        let proxy = &settings.proxy;
        // SOCKS5 relays all UDP through one association: one socket cannot
        // be half proxied.
        let relay_udp =
            proxy.kind == ProxyKind::Socks5 && (proxy.proxy_peers || proxy.proxy_trackers);
        // An HTTP proxy cannot carry uTP.
        if proxy.kind == ProxyKind::Http && proxy.proxy_peers {
            if !settings.enable_outgoing_tcp {
                warn!(
                    "an HTTP proxy carries peer connections over TCP only, and \
                     enable_outgoing_tcp is off: this session cannot dial peers"
                );
            }
            settings.enable_outgoing_utp = false;
        }
        for feature in settings.proxy_bypasses() {
            warn!(
                feature,
                "a proxy is configured, but {feature} does not go through it: \
                 it sends from this host's own address (turn it off to keep \
                 all traffic on the proxy)"
            );
        }
        let (endpoints, listen_errors) = bind_all(&settings, !relay_udp).await?;
        let local_addrs: Vec<SocketAddr> = endpoints.iter().map(|e| e.addr).collect();
        let udp: Vec<SharedUdpSocket> = if relay_udp {
            vec![bind_relay(&settings, &local_addrs).await?]
        } else {
            let mut udp: Vec<SharedUdpSocket> =
                endpoints.iter().filter_map(|e| e.udp.clone()).collect();
            udp.extend(fallback_udp(&endpoints));
            udp
        };
        let utp = utp_sockets(&settings, &udp);
        let senders: Vec<Arc<dyn DatagramSender>> =
            udp.iter().map(SharedUdpSocket::sender).collect();
        let tracker = match TrackerClient::new_with(
            settings.tracker.clone(),
            senders,
            Some(settings.proxy.clone()),
        ) {
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
        let (lsd, lsd_found) = LsdHub::start(&settings);
        let (portmap, map_events) = start_port_mapping(&settings, &local_addrs).unzip();
        let external_ip = Arc::new(ExternalIp::new(events.clone()));
        let queue = Mutex::new(settings.queue.clone());
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
            lsd,
            portmap,
            external_ip,
            ip_filter: RwLock::new(Arc::new(IpFilter::default())),
            bans: Mutex::new(BanList::default()),
            queue,
            queue_dirty: Notify::new(),
        });
        if let Some(dht) = &shared.dht {
            watch_bootstrap(dht, Arc::downgrade(&shared));
            watch_dht_external_ip(dht, Arc::downgrade(&shared));
        }
        if relay_udp {
            for u in &udp {
                watch_relay_lost(u, Arc::downgrade(&shared));
            }
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
        if let Some(found) = lsd_found {
            tasks.push(spawn_lsd_task(found, Arc::downgrade(&inner)));
        }
        if let Some(rx) = map_events {
            tasks.push(spawn_nat_task(rx, Arc::downgrade(&shared)));
        }
        tasks.push(tokio::spawn(run_queue(
            Arc::downgrade(&inner),
            Arc::clone(&shared),
        )));
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

    /// The best estimate of our external address: what the router reported
    /// through port mapping, else what the DHT nodes' votes agree on, else
    /// what three or more peers' `yourip` agree on. `None` until one of
    /// them knows. [`Event::ExternalIpChanged`] reports each source's news.
    pub fn external_ip(&self) -> Option<std::net::IpAddr> {
        let shared = &self.inner.shared;
        shared
            .external_ip
            .mapped()
            .or_else(|| shared.dht.as_ref().and_then(|d| d.stats().external_ip))
            .or_else(|| shared.external_ip.peers_estimate())
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

    /// BEP 33 scrape through the DHT: the estimated number of seeds and of
    /// other peers announced for `info_hash` to the nodes closest to it
    /// (bloom filters: estimates count distinct IPs, not ports).
    ///
    /// Fails with [`SessionError::DhtDisabled`] with the DHT off,
    /// [`SessionError::Dht`] while the DHT knows no node yet (nobody to
    /// ask: not the same as zero seeds and peers) and
    /// [`SessionError::Shutdown`] once the session has shut down.
    pub async fn dht_scrape(&self, info_hash: [u8; 20]) -> Result<DhtScrape, SessionError> {
        let dht = self.dht()?;
        dht.scrape(info_hash).await.map_err(dht_error)
    }

    /// BEP 51: asks the DHT node at `node` for a sample of the info hashes
    /// it stores and for the nodes it knows closest to `target`. One query:
    /// crawling (following [`DhtSamples::nodes`], honouring
    /// [`DhtSamples::interval`]) is the caller's job.
    ///
    /// Fails with [`SessionError::DhtDisabled`] with the DHT off,
    /// [`SessionError::Dht`] when the node does not answer or answers with
    /// an error (`204` if it does not support BEP 51), and
    /// [`SessionError::Shutdown`] once the session has shut down.
    pub async fn dht_sample_infohashes(
        &self,
        node: SocketAddr,
        target: [u8; 20],
    ) -> Result<DhtSamples, SessionError> {
        let dht = self.dht()?;
        dht.sample_infohashes(node, NodeId(target))
            .await
            .map_err(dht_error)
    }

    /// BEP 44: the immutable item stored in the DHT under `target` (the
    /// SHA-1 of its bencoded value), or `None` when no node close to it has
    /// it.
    ///
    /// Fails with [`SessionError::DhtDisabled`] with the DHT off,
    /// [`SessionError::Dht`] while the DHT knows no node yet, and
    /// [`SessionError::Shutdown`] once the session has shut down.
    pub async fn dht_get_immutable(
        &self,
        target: [u8; 20],
    ) -> Result<Option<Vec<u8>>, SessionError> {
        let dht = self.dht()?;
        dht.get_immutable(target).await.map_err(dht_error)
    }

    /// BEP 44: stores `value` (one canonical bencoded value of at most 1000
    /// bytes) in the DHT as an immutable item, on the nodes closest to its
    /// target. Returns the target (the SHA-1 of `value`) and the number of
    /// nodes that stored it. Items live about 2 hours on a node: put them
    /// again to keep them.
    ///
    /// Fails like [`dht_get_immutable`](Self::dht_get_immutable), and with
    /// [`SessionError::Dht`] for an invalid value.
    pub async fn dht_put_immutable(
        &self,
        value: Vec<u8>,
    ) -> Result<([u8; 20], usize), SessionError> {
        let dht = self.dht()?;
        dht.put_immutable(value).await.map_err(dht_error)
    }

    /// BEP 44: the mutable item of `public_key` and `salt` (empty for none)
    /// with the highest sequence number whose signature verifies, or `None`
    /// when no node close to it has one.
    ///
    /// Fails like [`dht_get_immutable`](Self::dht_get_immutable), and with
    /// [`SessionError::Dht`] for a salt over 64 bytes.
    pub async fn dht_get_mutable(
        &self,
        public_key: [u8; 32],
        salt: &[u8],
    ) -> Result<Option<MutableItem>, SessionError> {
        let dht = self.dht()?;
        dht.get_mutable(public_key, salt).await.map_err(dht_error)
    }

    /// BEP 44: signs `value` with the ed25519 key of `secret_seed` (never
    /// sent) under `salt` and `seq`, and stores it in the DHT as a mutable
    /// item. With `cas`, nodes replace only an item of exactly that `seq`.
    /// Returns the number of nodes that stored it.
    ///
    /// Fails like [`dht_put_immutable`](Self::dht_put_immutable); also with
    /// [`SessionError::Dht`] for a salt over 64 bytes or a negative `seq`,
    /// and when every node refused it (`seq` not above theirs, or `cas`
    /// mismatch).
    pub async fn dht_put_mutable(
        &self,
        secret_seed: [u8; 32],
        salt: Vec<u8>,
        value: Vec<u8>,
        seq: i64,
        cas: Option<i64>,
    ) -> Result<usize, SessionError> {
        let dht = self.dht()?;
        dht.put_mutable(secret_seed, salt, value, seq, cas)
            .await
            .map_err(dht_error)
    }

    fn dht(&self) -> Result<&Dht, SessionError> {
        self.inner
            .shared
            .dht
            .as_ref()
            .ok_or(SessionError::DhtDisabled)
    }

    /// Bytes held by the write-back cache of every torrent, against
    /// [`Settings::cache_size`].
    pub fn cache_stats(&self) -> CacheStats {
        self.inner.shared.cache.stats()
    }

    /// Replaces the queue's limits ([`Settings::queue`]) and runs a
    /// scheduler pass at once: torrents start or stop within it. Fails with
    /// [`SessionError::InvalidSettings`] for a zero `interval`.
    pub fn set_queue_settings(&self, queue: QueueSettings) -> Result<(), SessionError> {
        queue.validate().map_err(SessionError::InvalidSettings)?;
        let shared = &self.inner.shared;
        *shared.queue.lock().unwrap_or_else(PoisonError::into_inner) = queue;
        // The scheduler also picks up the new interval.
        shared.queue_dirty.notify_one();
        self.inner.queue_pass();
        Ok(())
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

    /// Replaces the session's IP filter. Every torrent closes its peers the
    /// new filter blocks (with
    /// [`DisconnectReason::Blocked`](crate::DisconnectReason::Blocked) and
    /// [`Event::PeerBlocked`]) before this returns, and from then on no
    /// blocked address is dialled or accepted. The filter applies to peers
    /// from every source (trackers, the DHT, PEX, LSD, `add_peer`,
    /// holepunch introductions, incoming connections) and to the peer's own
    /// address when dialling through a proxy, never to the proxy, trackers
    /// or DHT nodes. Bans are kept apart: a new filter lifts none.
    pub async fn set_ip_filter(&self, filter: IpFilter) {
        *self
            .inner
            .shared
            .ip_filter
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Arc::new(filter);
        for h in self.torrents() {
            // A torrent that is gone has no peers to close.
            let _ = h.filter_changed().await;
        }
    }

    /// A copy of the session's IP filter (allow everything by default).
    /// The copy is deep: a filter near
    /// [`MAX_IP_FILTER_RANGES`](crate::MAX_IP_FILTER_RANGES) costs tens of MB, so
    /// read it when needed rather than polling it.
    pub fn ip_filter(&self) -> IpFilter {
        let f = self
            .inner
            .shared
            .ip_filter
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        IpFilter::clone(&f)
    }

    /// The peers banned for the rest of the session, oldest first: an
    /// address with port 0 bans the whole IP.
    pub fn banned_peers(&self) -> Vec<SocketAddr> {
        self.inner
            .shared
            .bans
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .list()
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
        // The queue's first pass for it runs before its actor starts, so an
        // auto-managed torrent beyond the limits starts queued: no
        // `started`/`stopped` pair. Its data is unknown yet: it counts as a
        // download until its check says otherwise (the check's end wakes
        // the scheduler). Resume data's position is taken when it fits.
        let position = params
            .resume
            .as_ref()
            .and_then(|r| r.queue_position)
            .filter(|&p| p <= reg.order.len())
            .unwrap_or(reg.order.len());
        let queued = params.auto_managed && !params.paused && {
            let cfg = shared.queue_settings();
            !cfg.unlimited() && {
                let mut entries: Vec<QueueEntry> = reg
                    .order
                    .iter()
                    .filter_map(|o| reg.torrents.get(o))
                    .enumerate()
                    .map(|(i, e)| queue_entry(e, if i < position { i } else { i + 1 }).0)
                    .collect();
                let at = position.min(entries.len());
                entries.insert(
                    at,
                    QueueEntry {
                        position,
                        seeding: false,
                        running_since: None,
                        down_rate: 0,
                        up_rate: 0,
                        user_paused: false,
                        auto_managed: true,
                        slow: false,
                    },
                );
                !plan(&entries, &cfg, std::time::Instant::now())
                    .get(at)
                    .copied()
                    .unwrap_or(true)
            }
        };
        let auto_managed = params.auto_managed;
        let user_paused = params.paused;
        // A magnet's storage is built once its metadata is known.
        let storage: Option<Arc<dyn Storage>> = match (&params.source, params.storage.clone()) {
            (TorrentSource::Magnet(_), _) => None,
            (TorrentSource::Metainfo(_), Some(s)) => Some(s),
            (TorrentSource::Metainfo(m), None) => {
                let renamed = params.resume.as_ref().map_or(&[][..], |r| &r.renamed[..]);
                Some(Arc::new(FsStorage::with_paths(
                    params.save_path.clone(),
                    m,
                    renamed,
                )))
            }
        };
        shared.emit(Event::TorrentAdded { torrent: ih });
        let handle = torrent::spawn(
            params,
            storage,
            torrent::QueueStart { queued, position },
            Arc::clone(shared),
            Arc::downgrade(&self.inner),
        );
        reg.by_wire.insert(wire_hash, ih);
        reg.by_req2.insert(req2_hash(&wire_hash), wire_hash);
        reg.order.insert(position, ih);
        reg.torrents.insert(
            ih,
            TorrentEntry {
                handle: handle.clone(),
                wire_hash,
                auto_managed,
                queued,
                position_sent: Some(position),
                running_since: None,
                user_paused,
                slow: false,
                pending_queued: None,
            },
        );
        drop(reg);
        shared.queue_dirty.notify_one();
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

    /// Every torrent in the session, in queue order.
    pub fn torrents(&self) -> Vec<TorrentHandle> {
        let reg = self.inner.registry();
        reg.order
            .iter()
            .filter_map(|ih| reg.torrents.get(ih))
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
        self.shutdown_inner(false).await;
    }

    /// [`shutdown`](Self::shutdown), returning the session's state for
    /// [`restore`](Self::restore): each torrent's resume data taken after
    /// its final flush (so nothing is written after it and its pieces are
    /// trusted on restart), in queue order, and the DHT's state. A torrent
    /// whose resume data could not be taken (its task already gone, or data
    /// that would not load back, see
    /// [`TorrentHandle::save_resume_data`]) is left out of the state, logged,
    /// and returned with its error. Empty if the session was already shut
    /// down.
    pub async fn shutdown_and_save(&self) -> (SessionState, Vec<(InfoHashes, SessionError)>) {
        self.shutdown_inner(true).await.unwrap_or_default()
    }

    /// Every torrent's resume data
    /// ([`TorrentHandle::save_resume_data`]) in queue order, and the DHT's
    /// state ([`dht_state`](Self::dht_state)). The torrents keep running:
    /// what they write afterwards makes their resume data untrusted on
    /// restart (a full check), unless they were paused first; see
    /// [`shutdown_and_save`](Self::shutdown_and_save). A torrent removed
    /// meanwhile is left out silently; one whose save fails is left out,
    /// logged, and returned with its error, the others still saved.
    ///
    /// The torrents are asked one after the other, and a torrent whose
    /// storage is moving ([`TorrentHandle::move_storage`]) answers only
    /// once the move is done: one long move (a copy across filesystems)
    /// delays the whole save. It fails with [`SessionError::Busy`] only
    /// when 64 other commands already wait for that move.
    ///
    /// Fails with [`SessionError::Shutdown`] once the session has shut
    /// down.
    pub async fn save_state(
        &self,
    ) -> Result<(SessionState, Vec<(InfoHashes, SessionError)>), SessionError> {
        if self.inner.shared.is_shutdown() {
            return Err(SessionError::Shutdown);
        }
        let mut torrents = Vec::new();
        let mut failed = Vec::new();
        for h in self.torrents() {
            match h.save_resume_data().await {
                Ok(b) => torrents.push(b),
                Err(SessionError::TorrentNotFound) => {}
                Err(SessionError::Shutdown) => return Err(SessionError::Shutdown),
                Err(e) => {
                    warn!(torrent = %h.info_hashes(), error = %e, "left out of the saved state");
                    failed.push((h.info_hashes(), e));
                }
            }
        }
        let state = SessionState {
            dht: self.dht_state().await?,
            torrents,
        };
        Ok((state, failed))
    }

    /// Starts a session from `state` ([`save_state`](Self::save_state) or
    /// [`shutdown_and_save`](Self::shutdown_and_save)): its DHT state
    /// replaces [`Settings::dht_state`] (when it has one), and each torrent
    /// is added from its resume data ([`AddTorrent::from_resume`]) in the
    /// saved order, which is the queue's (each torrent's own saved position
    /// is ignored here: the order of `state.torrents` decides). A torrent
    /// that cannot be added
    /// does not stop the others: it is returned with its index in
    /// `state.torrents` and the error ([`SessionError::Resume`] for resume
    /// data that does not decode).
    ///
    /// Fails as [`Session::new`] does.
    pub async fn restore(
        mut settings: Settings,
        state: &SessionState,
    ) -> Result<(Session, Vec<(usize, SessionError)>), SessionError> {
        if let Some(dht) = &state.dht {
            settings.dht_state = Some(dht.clone());
        }
        let session = Session::new(settings).await?;
        let mut failed = Vec::new();
        for (i, bytes) in state.torrents.iter().enumerate() {
            let r = match AddTorrent::from_resume(bytes) {
                Ok(mut p) => {
                    // At the bottom, so the queue follows the state's order.
                    if let Some(r) = p.resume.as_mut() {
                        r.queue_position = None;
                    }
                    session.add_torrent(p).await.map(|_| ())
                }
                Err(e) => Err(SessionError::Resume(e)),
            };
            if let Err(e) = r {
                failed.push((i, e));
            }
        }
        Ok((session, failed))
    }

    /// The shutdown; with `save`, each torrent's resume data after its
    /// final flush and the DHT state before the DHT stops. `None` if the
    /// session was already shut down.
    async fn shutdown_inner(
        &self,
        save: bool,
    ) -> Option<(SessionState, Vec<(InfoHashes, SessionError)>)> {
        if self.inner.shared.shutdown.swap(true, Ordering::AcqRel) {
            return None;
        }
        self.inner.stop_listeners();
        let handles: Vec<TorrentHandle> = {
            let mut reg = self.inner.registry();
            let reg = &mut *reg;
            reg.by_wire.clear();
            reg.by_req2.clear();
            let order = std::mem::take(&mut reg.order);
            let handles = order
                .iter()
                .filter_map(|ih| reg.torrents.remove(ih))
                .map(|e| e.handle)
                .collect();
            reg.torrents.clear();
            handles
        };
        let mut state = SessionState::default();
        let mut failed = Vec::new();
        if save {
            let mut waits = Vec::new();
            for h in &handles {
                waits.push((h.info_hashes(), h.send_shutdown_save().await));
            }
            for (ih, rx) in waits {
                // A task already gone (it stopped on its own) cannot save.
                let r = match rx {
                    Some(rx) => rx.await.unwrap_or(Err(SessionError::TorrentNotFound)),
                    None => Err(SessionError::TorrentNotFound),
                };
                match r {
                    Ok(bytes) => state.torrents.push(bytes),
                    Err(e) => {
                        warn!(torrent = %ih, error = %e, "left out of the saved state");
                        failed.push((ih, e));
                    }
                }
            }
        } else {
            let mut waits: Vec<oneshot::Receiver<()>> = Vec::new();
            for h in &handles {
                if let Some(rx) = h.send_shutdown().await {
                    waits.push(rx);
                }
            }
            for rx in waits {
                let _ = rx.await;
            }
        }
        self.inner.shared.lsd.shutdown().await;
        // Bounded (2 s in all): an unreachable router never holds us up.
        if let Some(pm) = &self.inner.shared.portmap {
            pm.shutdown().await;
        }
        if let Some(dht) = &self.inner.shared.dht {
            if save {
                state.dht = dht.state().await.ok().map(|s| s.to_bytes());
            }
            dht.shutdown().await;
        }
        // The peers are gone; the uTP sockets stop before the UDP sockets
        // under them close.
        for u in &self.inner.shared.utp {
            u.shutdown().await;
        }
        // Last: the `stopped` announces and the DHT used these sockets.
        self.inner.close_udp();
        Some((state, failed))
    }
}

/// A torrent as the queue sees it, at `position`, from its latest status
/// and the user-pause flag its actor recorded; plus whether it is past its
/// check and running (`Downloading`, `Seeding`, `DownloadingMetadata`), when
/// its startup grace may start.
fn queue_entry(e: &TorrentEntry, position: usize) -> (QueueEntry, bool) {
    use crate::status::TorrentState as S;
    e.handle.with_status(|st| {
        let q = QueueEntry {
            position,
            seeding: st.has_metadata && st.progress >= 1.0,
            running_since: e.running_since,
            down_rate: st.download_rate,
            up_rate: st.upload_rate,
            user_paused: e.user_paused || st.state == S::Error,
            auto_managed: e.auto_managed,
            slow: e.slow,
        };
        let active = matches!(
            st.state,
            S::Downloading | S::Seeding | S::DownloadingMetadata
        );
        (q, active)
    })
}

/// The queue scheduler: a pass every [`QueueSettings::interval`] and
/// whenever `queue_dirty` is notified, until the session is gone.
async fn run_queue(inner: std::sync::Weak<SessionInner>, shared: Arc<Shared>) {
    loop {
        let interval = shared.queue_settings().interval;
        tokio::select! {
            () = shared.queue_dirty.notified() => {}
            () = tokio::time::sleep(interval) => {}
        }
        if shared.is_shutdown() {
            return;
        }
        let Some(inner) = inner.upgrade() else {
            return;
        };
        inner.queue_pass();
    }
}

/// The one UDP socket of a session behind a SOCKS5 proxy: a UDP association
/// (bounded by `connect_timeout`) and a socket relayed through it, on a free
/// port of the first listen IP of the relay's family (IPv4 in practice),
/// else the unspecified address. Fails with [`SessionError::Proxy`].
async fn bind_relay(
    settings: &Settings,
    local_addrs: &[SocketAddr],
) -> Result<SharedUdpSocket, SessionError> {
    let assoc = udp_associate(&settings.proxy, settings.connect_timeout)
        .await
        .map_err(SessionError::Proxy)?;
    let v4 = assoc.relay().is_ipv4();
    let ip = local_addrs.iter().find(|a| a.is_ipv4() == v4).map_or(
        if v4 {
            IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        } else {
            IpAddr::V6(Ipv6Addr::UNSPECIFIED)
        },
        SocketAddr::ip,
    );
    SharedUdpSocket::bind_relayed(SocketAddr::new(ip, 0), assoc)
        .map_err(|e| SessionError::Proxy(ProxyError::Io(e)))
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

/// A DHT failure as a session error: a DHT shut down with the session is
/// [`SessionError::Shutdown`], as for [`Session::dht_state`].
fn dht_error(e: DhtError) -> SessionError {
    match e {
        DhtError::Shutdown => SessionError::Shutdown,
        e => SessionError::Dht(e),
    }
}

/// Warns and emits [`Event::ProxyError`] once when the relayed UDP socket
/// `u` loses its SOCKS5 association (the proxy closed the control
/// connection): from then on the DHT, uTP and UDP trackers fail until the
/// session restarts. Ends with the socket or the session.
fn watch_relay_lost(u: &SharedUdpSocket, shared: std::sync::Weak<Shared>) {
    let mut lost = u.relay_lost();
    tokio::spawn(async move {
        if lost.wait_for(|l| *l).await.is_err() {
            return;
        }
        let Some(shared) = shared.upgrade() else {
            return;
        };
        if shared.shutdown.load(Ordering::SeqCst) {
            return;
        }
        let message = "the SOCKS5 proxy closed the UDP association: the DHT, uTP and \
                       UDP trackers stop working until the session restarts"
            .to_owned();
        warn!("{message}");
        shared.emit(Event::ProxyError { message });
    });
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

/// Emits [`Event::ExternalIpChanged`] with [`ExternalIpSource::Dht`] each
/// time the DHT's votes settle on a new address; ends with the DHT or the
/// session.
fn watch_dht_external_ip(dht: &Dht, shared: std::sync::Weak<Shared>) {
    let mut stats = dht.subscribe_stats();
    tokio::spawn(async move {
        let mut last = None;
        while stats.changed().await.is_ok() {
            let ip = stats.borrow_and_update().external_ip;
            if ip.is_none() || ip == last {
                continue;
            }
            last = ip;
            let (Some(shared), Some(ip)) = (shared.upgrade(), ip) else {
                return;
            };
            shared.emit(Event::ExternalIpChanged {
                ip,
                source: ExternalIpSource::Dht,
            });
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
            lsd: LsdHub::disabled(),
            portmap: None,
            external_ip: Arc::new(ExternalIp::new(broadcast::channel(1).0)),
            ip_filter: RwLock::new(Arc::new(IpFilter::default())),
            bans: Mutex::new(BanList::default()),
            queue: Mutex::new(QueueSettings::default()),
            queue_dirty: Notify::new(),
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
