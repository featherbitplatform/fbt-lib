//! The session: torrent registry, peer listener, events and limits.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};

use fbt_core::{InfoHashes, PeerId};
use fbt_dht::{Dht, DhtError, DhtSamples, DhtScrape, DhtStats, MutableItem, NodeId};
use fbt_metainfo::{Magnet, Metainfo};
use fbt_net::demux::{DemuxStats, SharedUdpSocket};
use fbt_net::ipfilter::IpFilter;
use fbt_net::proxy::{ProxyKind, Socks5Udp, udp_associate};
use fbt_utp::UtpSocket;
use fbt_wire::req2_hash;
use tokio::sync::{Notify, broadcast, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::bandwidth::{Bandwidth, PeerClassLimits, RateLimit};
use crate::bans::{BanList, BlockReason};
use crate::disk::DiskPool;
use crate::error::SessionError;
use crate::event::{Event, EventMask, EventStream, ExternalIpSource};
use crate::listener::ListenerState;
use crate::net::{ExternalIp, ListenError, spawn_lsd_task, spawn_nat_task};
use crate::netstack::NetStack;
use crate::queue::{QueueEntry, QueueSettings, is_slow, plan, running_since};
use crate::resume::{ResumeError, ResumeState, SessionState, decode_resume};
use crate::settings::{NetworkSettings, Settings};
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
    /// The current network stack: every socket the session sends from (the
    /// shared UDP sockets, uTP, the DHT, the tracker client), LSD, port
    /// mapping, the listeners and their accept loops, stamped with a
    /// generation. [`Session::set_network_settings`] replaces it whole; read
    /// it once per use through [`net`](Self::net) and keep the `Arc` for the
    /// duration of what reads it.
    pub net: RwLock<Arc<NetStack>>,
    /// The generation of [`net`](Self::net), carried on a `watch` channel so
    /// a dial or a handshake can notice a change without taking the lock.
    net_gen: watch::Sender<u64>,
    pub events: broadcast::Sender<Event>,
    pub shutdown: AtomicBool,
    /// Peer tasks alive across the session (connecting or connected).
    pub global_peers: AtomicUsize,
    /// Outgoing connections still connecting or handshaking.
    pub half_open: AtomicUsize,
    /// Runs every storage job and bounds the concurrent checks.
    pub disk: DiskPool,
    /// The byte budget every torrent's write-back cache draws from.
    pub cache: Arc<CacheBudget>,
    /// The global and class rate limits.
    pub bandwidth: Arc<Bandwidth>,
    /// BEP 55 holepunch activity, for [`Session::holepunch_stats`].
    pub holepunch: HolepunchCounters,
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
    /// The current network stack.
    pub(crate) fn net(&self) -> Arc<NetStack> {
        Arc::clone(&self.net.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// A receiver that fires on every stack change, carrying the new
    /// generation.
    pub(crate) fn net_changed(&self) -> watch::Receiver<u64> {
        self.net_gen.subscribe()
    }

    /// Installs `stack` as the current one and announces its generation.
    /// The caller serialises changes; the stack's own generation must be
    /// newer than the current one.
    pub(crate) fn set_net(&self, stack: Arc<NetStack>) {
        *self.net.write().unwrap_or_else(PoisonError::into_inner) = Arc::clone(&stack);
        self.net_gen.send_replace(stack.generation);
    }

    /// The uTP socket to dial `peer` from: the first of its address family,
    /// as for the DHT and UDP trackers. `None` without one.
    pub(crate) fn utp_for(&self, peer: &SocketAddr) -> Option<UtpSocket> {
        self.net().utp_for(peer)
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

/// A `Shared` for unit tests of the torrent actors and the session helpers:
/// `settings`, the given `stack` (usually [`NetStack::offline`]) and a cache
/// budget of `cache_size`; nothing here opens a socket.
#[cfg(test)]
impl Shared {
    pub(crate) fn test(settings: Settings, stack: Arc<NetStack>, cache_size: u64) -> Arc<Shared> {
        let (events, _) = broadcast::channel(64);
        let (net_gen, _) = watch::channel(stack.generation);
        Arc::new(Shared {
            settings: Arc::new(settings),
            peer_id: PeerId::generate(),
            net: RwLock::new(stack),
            net_gen,
            events,
            shutdown: AtomicBool::new(false),
            global_peers: AtomicUsize::new(0),
            half_open: AtomicUsize::new(0),
            disk: DiskPool::new(1, 1),
            cache: Arc::new(CacheBudget::new(cache_size)),
            bandwidth: Arc::new(Bandwidth::new(&Settings::default())),
            holepunch: HolepunchCounters::default(),
            external_ip: Arc::new(ExternalIp::new(broadcast::channel(1).0)),
            ip_filter: RwLock::new(Arc::new(IpFilter::default())),
            bans: Mutex::new(BanList::default()),
            queue: Mutex::new(QueueSettings::default()),
            queue_dirty: Notify::new(),
        })
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

/// The hashes a peer may name a torrent by in its handshake: the v1 hash,
/// then the truncated v2 hash, either one when only it exists (the all-zero
/// [`InfoHashes::wire_hash`] when neither does).
pub(crate) fn wire_hashes(ih: &InfoHashes) -> Vec<[u8; 20]> {
    let mut out = Vec::with_capacity(2);
    if let Some(v1) = ih.v1 {
        out.push(v1.0);
    }
    if let Some(v2) = ih.v2 {
        out.push(InfoHashes::from_v2(v2).wire_hash());
    }
    if out.is_empty() {
        out.push(ih.wire_hash());
    }
    out
}

/// One registered torrent.
#[derive(Debug)]
pub(crate) struct TorrentEntry {
    pub handle: TorrentHandle,
    /// Every hash a peer may name it by (see [`wire_hashes`]), plus any
    /// alias its metadata revealed: one or two.
    pub wire_hashes: Vec<[u8; 20]>,
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
    /// Every wire hash of every torrent (v1 and truncated v2) to its key.
    pub by_wire: HashMap<[u8; 20], InfoHashes>,
    /// `HASH('req2', wire hash)` to the wire hash (the MSE `SKEY`), kept
    /// with `by_wire`, so an MSE responder finds its torrent in one lookup.
    pub by_req2: HashMap<[u8; 20], [u8; 20]>,
}

#[derive(Debug)]
pub(crate) struct SessionInner {
    pub shared: Arc<Shared>,
    /// The network settings as the caller set them ([`Session::network_settings`]),
    /// which the applied stack may differ from (an HTTP proxy carrying peers
    /// turns outgoing uTP off in the stack only).
    network: RwLock<NetworkSettings>,
    /// Shared by every accept loop of the session.
    listener_state: Arc<ListenerState>,
    registry: Mutex<Registry>,
    /// The session-level tasks (the queue scheduler); the network stack
    /// holds its own.
    listeners: Mutex<Vec<JoinHandle<()>>>,
    /// Serialises [`rebuild`](Self::rebuild) and the teardown, so two
    /// runtime changes apply one after the other and one never races the
    /// shutdown.
    change: tokio::sync::Mutex<()>,
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
                for w in &e.wire_hashes {
                    reg.by_wire.remove(w);
                    reg.by_req2.remove(&req2_hash(w));
                }
                reg.order.retain(|o| o != ih);
                // Positions behind it moved up, and a slot may be free.
                self.shared.queue_dirty.notify_one();
                true
            }
            None => false,
        }
    }

    /// Registers `wire` as one more wire hash of the torrent keyed `key`
    /// (a hash its metadata revealed). False, with nothing changed, when
    /// the torrent is gone, already has it, or another torrent owns it.
    pub(crate) fn add_alias(&self, key: &InfoHashes, wire: [u8; 20]) -> bool {
        let mut reg = self.registry();
        let reg = &mut *reg;
        let Some(e) = reg.torrents.get_mut(key) else {
            return false;
        };
        if let Some(owner) = reg.by_wire.get(&wire) {
            if owner != key {
                warn!(torrent = %key, other = %owner, "revealed hash belongs to another torrent; alias skipped");
            }
            return false;
        }
        e.wire_hashes.push(wire);
        reg.by_wire.insert(wire, *key);
        reg.by_req2.insert(req2_hash(&wire), wire);
        true
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

    /// Stops the session-level tasks (the queue scheduler).
    fn stop_listeners(&self) {
        let tasks = std::mem::take(&mut *self.listeners.lock().unwrap_or_else(|e| e.into_inner()));
        for t in tasks {
            t.abort();
        }
    }

    /// Spawns `stack`'s accept loops and routers, so its listeners accept
    /// and its LSD and port-mapping reports reach torrents. Each task stops
    /// when the stack (or the session) is shut down.
    fn wire(self: &Arc<Self>, stack: &Arc<NetStack>) {
        let shared = &self.shared;
        let stop = stack.stop_receiver();
        let mut tasks: Vec<JoinHandle<()>> = stack
            .take_tcp()
            .into_iter()
            .map(|tcp| {
                crate::listener::spawn(
                    tcp,
                    Arc::clone(&self.listener_state),
                    Arc::downgrade(self),
                    Arc::clone(shared),
                    stop.clone(),
                )
            })
            .collect();
        if stack.net.enable_incoming_utp {
            tasks.extend(stack.utp.iter().map(|u| {
                crate::listener::spawn_utp(
                    u.clone(),
                    Arc::clone(&self.listener_state),
                    Arc::downgrade(self),
                    Arc::clone(shared),
                    stop.clone(),
                )
            }));
        }
        if let Some(found) = stack.take_lsd_found() {
            tasks.push(spawn_lsd_task(
                found,
                Arc::downgrade(self),
                Arc::downgrade(stack),
                stop.clone(),
            ));
        }
        if let Some(rx) = stack.take_map_events() {
            tasks.push(spawn_nat_task(rx, Arc::downgrade(shared), stop.clone()));
        }
        if let Some(dht) = &stack.dht {
            tasks.push(watch_bootstrap(dht, Arc::downgrade(shared), stop.clone()));
            tasks.push(watch_dht_external_ip(
                dht,
                Arc::downgrade(shared),
                stop.clone(),
            ));
        }
        // The relayed socket's association monitor; a direct socket's is
        // never signalled.
        tasks.extend(
            stack
                .udp
                .iter()
                .map(|u| watch_relay_lost(u, Arc::downgrade(shared), stop.clone())),
        );
        stack.add_tasks(tasks);
    }

    /// The network settings as the caller set them.
    fn network(&self) -> NetworkSettings {
        self.network
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Applies `net` as the session's network settings: the steps that make
    /// the change safe (the order is the guarantee). Under the change lock
    /// (so two changes apply one after the other, and the shutdown never
    /// races one):
    ///
    /// 1. Install a closed stack: from here no new dial, announce, web seed
    ///    request or datagram read uses the old one.
    /// 2. Shut the old stack down (its listeners, LSD, port mapping, DHT,
    ///    uTP and UDP sockets; the DHT's state is kept), dropping its
    ///    tracker client and connection pool.
    /// 3. With `close_tcp` (the proxy changed), every torrent closes its
    ///    peers, web seed and tracker requests with
    ///    [`DisconnectReason::NetworkChanged`](crate::DisconnectReason::NetworkChanged),
    ///    awaited as
    ///    [`set_ip_filter`](Session::set_ip_filter) awaits `filter_changed`.
    /// 4. Build the new stack for the next generation on the addresses the
    ///    old one had bound (so the listen ports stay) and install it.
    /// 5. Every torrent re-announces to its trackers and the DHT at once.
    async fn rebuild(
        self: &Arc<Self>,
        net: NetworkSettings,
        close_tcp: bool,
    ) -> Result<(), SessionError> {
        // The proxy's own rules, and every transport off, are checked here
        // as `Session::new` checks them, on the settings the session would
        // have.
        let mut probe = (*self.shared.settings).clone();
        probe.set_network(net.clone());
        probe.validate().map_err(SessionError::InvalidSettings)?;
        let _guard = self.change.lock().await;
        if self.shared.is_shutdown() {
            return Err(SessionError::Shutdown);
        }
        let old = self.shared.net();
        let generation = old.generation + 1;
        // The addresses the old stack had bound, so a rebuild keeps the
        // listen ports (a closed stack has none, so fall back to the
        // settings: the retry after a failed change rebinds them).
        let mut addrs = old.local_addrs.clone();
        if addrs.is_empty() {
            addrs = self.shared.settings.listen_addrs.clone();
        }
        let dht_state = self.shared.settings.dht_state.clone();
        // 1. A closed stack first: nothing more uses the old one.
        self.shared.set_net(Arc::new(NetStack::closed(generation)));
        // 2. Tear the old one down, keeping the DHT's state for the new one.
        let kept_dht = old.shutdown().await.or(dht_state);
        // 3. With the proxy gone or changed, the connections of older
        // generations are closed at once (handshaking and dialling ones
        // included, through the generation they recorded).
        self.close_generation(generation, close_tcp).await;
        // 4. Build the new stack and install it (unless the session is
        // shutting down: then nothing must stay open). The proxy's UDP
        // association is taken here, so `build` knows to bind UDP.
        let relay = match relay_socket(&probe).await {
            Ok(r) => r,
            Err(e) => {
                // A change that fails while the session is going away is a
                // shutdown, not a proxy error.
                return Err(if self.shared.is_shutdown() {
                    SessionError::Shutdown
                } else {
                    e
                });
            }
        };
        let stack = match NetStack::build(
            &self.shared.settings,
            &net,
            &addrs,
            kept_dht,
            generation,
            relay,
        )
        .await
        {
            Ok(s) => Arc::new(s),
            Err(e) => {
                if self.shared.is_shutdown() {
                    return Err(SessionError::Shutdown);
                }
                return Err(e);
            }
        };
        if self.shared.is_shutdown() {
            stack.abort();
            return Err(SessionError::Shutdown);
        }
        *self.network.write().unwrap_or_else(PoisonError::into_inner) = net.clone();
        self.shared.set_net(Arc::clone(&stack));
        self.wire(&stack);
        // 5. Re-announce at once.
        for h in self.handles() {
            h.network_ready(generation);
        }
        Ok(())
    }

    /// Closes every torrent's connections of an older generation, awaited as
    /// [`set_ip_filter`](Session::set_ip_filter) awaits `filter_changed`.
    /// With `close_tcp` (the proxy changed) all of them (every connection
    /// must be redialled through it); otherwise only the connections still
    /// being established (a dial or handshake the new stack must not
    /// finish).
    async fn close_generation(&self, generation: u64, close_tcp: bool) {
        for h in self.handles() {
            // A torrent that is gone has nothing to close.
            let _ = h.network_changed(generation, close_tcp).await;
        }
    }

    /// Every torrent handle, in queue order.
    fn handles(&self) -> Vec<TorrentHandle> {
        let reg = self.registry();
        reg.order
            .iter()
            .filter_map(|ih| reg.torrents.get(ih))
            .map(|e| e.handle.clone())
            .collect()
    }
}

impl Drop for SessionInner {
    fn drop(&mut self) {
        self.stop_listeners();
        self.shared.net().abort();
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
        let (events, _) = broadcast::channel(settings.event_queue);
        // One for every accept loop, so all of them together allow only
        // `max_peers_global` pending handshakes.
        let listener_state = Arc::new(ListenerState::new(settings.max_peers_global));
        let disk = DiskPool::new(settings.disk_threads, settings.max_active_checks);
        let cache = Arc::new(CacheBudget::new(settings.cache_size));
        let bandwidth = Arc::new(Bandwidth::new(&settings));
        bandwidth.spawn_rebalance();
        let external_ip = Arc::new(ExternalIp::new(events.clone()));
        let queue = Mutex::new(settings.queue.clone());
        // Everything a session sends with lives in one stack, built here as
        // generation 1. The proxy's UDP association (a failure still fails
        // `new` in this task) is taken first, so `build` knows to bind UDP.
        let net = settings.network();
        let addrs = settings.listen_addrs.clone();
        let dht_state = settings.dht_state.clone();
        let relay = relay_socket(&settings).await?;
        let stack = Arc::new(NetStack::build(&settings, &net, &addrs, dht_state, 1, relay).await?);
        let (net_gen, _) = watch::channel(1u64);
        let shared = Arc::new(Shared {
            settings: Arc::new(settings),
            peer_id: PeerId::generate(),
            net: RwLock::new(Arc::clone(&stack)),
            net_gen,
            events,
            shutdown: AtomicBool::new(false),
            global_peers: AtomicUsize::new(0),
            half_open: AtomicUsize::new(0),
            disk,
            cache,
            bandwidth,
            holepunch: HolepunchCounters::default(),
            external_ip,
            ip_filter: RwLock::new(Arc::new(IpFilter::default())),
            bans: Mutex::new(BanList::default()),
            queue,
            queue_dirty: Notify::new(),
        });
        let inner = Arc::new(SessionInner {
            shared: Arc::clone(&shared),
            network: RwLock::new(net),
            listener_state,
            registry: Mutex::new(Registry::default()),
            listeners: Mutex::new(Vec::new()),
            change: tokio::sync::Mutex::new(()),
        });
        // The accept loops, LSD and port-mapping routers and the watchers,
        // all stopped with the stack (or the session).
        inner.wire(&stack);
        let queue_task = tokio::spawn(run_queue(Arc::downgrade(&inner), Arc::clone(&shared)));
        inner
            .listeners
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(queue_task);
        Ok(Session { inner })
    }

    /// The network settings as the caller set them
    /// ([`network_settings`](Self::network_settings)): the stored switches,
    /// which the applied stack may differ from (an HTTP proxy carrying peers
    /// turns outgoing uTP off in the stack only).
    pub fn network_settings(&self) -> NetworkSettings {
        self.inner.network()
    }

    /// Replaces the session's network settings (the proxy and the network
    /// switches) at runtime, without a restart and without any direct
    /// connection in between: the change installs a closed stack first, tears
    /// the old one down, closes the connections of older generations (all of
    /// them with [`DisconnectReason::NetworkChanged`](crate::DisconnectReason::NetworkChanged)
    /// when the proxy changed),
    /// then builds and installs the new stack on the addresses the old one
    /// had bound (so the listen ports stay) and re-announces every torrent.
    ///
    /// Fails with [`SessionError::InvalidSettings`] (the proxy's own
    /// [`validate`](fbt_net::proxy::ProxyConfig::validate), or a switch
    /// combination the session cannot work with) and with
    /// [`SessionError::Shutdown`] once the session has shut down. Setting a
    /// value equal to the current one is a no-op.
    pub async fn set_network_settings(&self, net: NetworkSettings) -> Result<(), SessionError> {
        if self.inner.shared.is_shutdown() {
            return Err(SessionError::Shutdown);
        }
        if self.inner.network() == net {
            return Ok(());
        }
        // With the proxy changed, every connection must be redialled through
        // the new one; without it, only the connections still being
        // established are closed (a connected TCP peer keeps running).
        let close_tcp = self.inner.network().proxy != net.proxy;
        self.inner.rebuild(net, close_tcp).await
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
        self.inner.shared.net().dht.as_ref().map(Dht::stats)
    }

    /// The best estimate of our external address: what the router reported
    /// through port mapping, else what the DHT nodes' votes agree on, else
    /// what three or more peers' `yourip` agree on. `None` until one of
    /// them knows. [`Event::ExternalIpChanged`] reports each source's news.
    pub fn external_ip(&self) -> Option<std::net::IpAddr> {
        let shared = &self.inner.shared;
        let stack = shared.net();
        shared
            .external_ip
            .mapped()
            .or_else(|| stack.dht.as_ref().and_then(|d| d.stats().external_ip))
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
        let stack = self.inner.shared.net();
        let Some(dht) = &stack.dht else {
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
        let stack = self.inner.shared.net();
        let dht = stack.dht.as_ref().ok_or(SessionError::DhtDisabled)?;
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
        let stack = self.inner.shared.net();
        let dht = stack.dht.as_ref().ok_or(SessionError::DhtDisabled)?;
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
        let stack = self.inner.shared.net();
        let dht = stack.dht.as_ref().ok_or(SessionError::DhtDisabled)?;
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
        let stack = self.inner.shared.net();
        let dht = stack.dht.as_ref().ok_or(SessionError::DhtDisabled)?;
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
        let stack = self.inner.shared.net();
        let dht = stack.dht.as_ref().ok_or(SessionError::DhtDisabled)?;
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
        let stack = self.inner.shared.net();
        let dht = stack.dht.as_ref().ok_or(SessionError::DhtDisabled)?;
        dht.put_mutable(secret_seed, salt, value, seq, cas)
            .await
            .map_err(dht_error)
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

    /// The queue's limits: [`Settings::queue`], or the last
    /// [`set_queue_settings`](Self::set_queue_settings).
    pub fn queue_settings(&self) -> QueueSettings {
        self.inner.shared.queue_settings()
    }

    /// The session-wide rate limit: [`Settings::rate_limit`], or the last
    /// [`set_rate_limit`](Self::set_rate_limit).
    pub fn rate_limit(&self) -> RateLimit {
        self.inner.shared.bandwidth.global_limit()
    }

    /// The peer class limits: [`Settings::peer_class_limits`], or the last
    /// [`set_peer_class_limits`](Self::set_peer_class_limits).
    pub fn peer_class_limits(&self) -> PeerClassLimits {
        self.inner.shared.bandwidth.class_limits()
    }

    /// True once [`shutdown`](Self::shutdown) or
    /// [`shutdown_and_save`](Self::shutdown_and_save) has begun on this
    /// session or any clone of it; every command then returns
    /// [`SessionError::Shutdown`]. For a long-lived consumer holding a
    /// clone (an event stream adapter, say) to notice the end.
    pub fn is_shut_down(&self) -> bool {
        self.inner.shared.is_shutdown()
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
        self.local_addrs()
            .first()
            .copied()
            .unwrap_or_else(|| SocketAddr::from((std::net::Ipv4Addr::UNSPECIFIED, 0)))
    }

    /// The addresses the peer listeners are bound to, in
    /// [`Settings::listen_addrs`] order, without the ones that failed. A
    /// runtime change rebinds the same addresses, so the ports stay.
    pub fn local_addrs(&self) -> Vec<SocketAddr> {
        self.inner.shared.net().local_addrs.clone()
    }

    /// The listen addresses that could not be bound, fully or only on
    /// their UDP side, by the current stack. They are not events: binding
    /// happens in [`Session::new`] (and in a runtime change), before anyone
    /// can subscribe.
    pub fn listen_errors(&self) -> Vec<ListenError> {
        self.inner.shared.net().listen_errors.clone()
    }

    /// Datagram counts of each shared UDP socket, by its address. For
    /// tests; not part of the stable API.
    #[doc(hidden)]
    pub fn demux_stats(&self) -> Vec<(SocketAddr, DemuxStats)> {
        self.inner
            .shared
            .net()
            .udp
            .iter()
            .map(|u| (u.local_addr(), u.stats()))
            .collect()
    }

    /// Adds a torrent and starts checking its existing data. A magnet
    /// starts in [`TorrentState::DownloadingMetadata`](crate::TorrentState)
    /// and is registered under its own info hashes, which stay its identity
    /// after the metadata arrives. A torrent is found under each of its wire
    /// hashes (v1 and truncated v2): adding one that shares any of them with
    /// a torrent already added fails with [`SessionError::DuplicateTorrent`].
    ///
    /// Fails with [`SessionError::InvalidPriorities`] (expecting 0) for a
    /// magnet with file priorities.
    pub async fn add_torrent(&self, params: AddTorrent) -> Result<TorrentHandle, SessionError> {
        let shared = &self.inner.shared;
        if shared.is_shutdown() {
            return Err(SessionError::Shutdown);
        }
        let num_files = match &params.source {
            TorrentSource::Metainfo(m) => m.info.files.len(),
            // Unknown until the metadata arrives.
            TorrentSource::Magnet(_) => 0,
        };
        if !params.file_priorities.is_empty() && params.file_priorities.len() != num_files {
            return Err(SessionError::InvalidPriorities {
                expected: num_files,
                got: params.file_priorities.len(),
            });
        }
        let ih = params.source.info_hashes();
        let wires = wire_hashes(&ih);
        let mut reg = self.inner.registry();
        if shared.is_shutdown() {
            return Err(SessionError::Shutdown);
        }
        if reg.torrents.contains_key(&ih) || wires.iter().any(|w| reg.by_wire.contains_key(w)) {
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
        for w in &wires {
            reg.by_wire.insert(*w, ih);
            reg.by_req2.insert(req2_hash(w), *w);
        }
        reg.order.insert(position, ih);
        reg.torrents.insert(
            ih,
            TorrentEntry {
                handle: handle.clone(),
                wire_hashes: wires,
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

    /// The torrent with these info hashes, else the one known by any of
    /// their wire hashes (a hybrid is found by its v1 or its v2 hash alone).
    pub fn torrent(&self, ih: &InfoHashes) -> Option<TorrentHandle> {
        let reg = self.inner.registry();
        reg.torrents
            .get(ih)
            .or_else(|| {
                wire_hashes(ih)
                    .iter()
                    .find_map(|w| reg.by_wire.get(w).and_then(|k| reg.torrents.get(k)))
            })
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
    /// session was already shut down. Takes the change lock, so it never
    /// races a runtime change under way: it waits for the change to finish
    /// (bounded by `connect_timeout`) and then shuts the installed stack
    /// down.
    async fn shutdown_inner(
        &self,
        save: bool,
    ) -> Option<(SessionState, Vec<(InfoHashes, SessionError)>)> {
        if self.inner.shared.shutdown.swap(true, Ordering::AcqRel) {
            return None;
        }
        self.inner.stop_listeners();
        // A change in flight sees the shutdown flag, aborts the stack it
        // built and returns; the lock makes this wait for that.
        let _guard = self.inner.change.lock().await;
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
        // Every socket the session sends from lives in the stack: shutting
        // it down stops the accept loops (freeing the ports), LSD, port
        // mapping, the DHT (returning its state for `save`) and uTP, and
        // closes the shared UDP sockets.
        let dht_state = self.inner.shared.net().shutdown().await;
        if save {
            state.dht = dht_state;
        }
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

/// The session's one UDP socket behind a SOCKS5 proxy: the UDP association
/// (bounded by `connect_timeout`) the caller classifies, or `None` when the
/// proxy does not relay UDP. Built before the stack, so `NetStack::build`
/// knows to bind only TCP and relay all UDP through it. Fails with
/// [`SessionError::Proxy`].
async fn relay_socket(settings: &Settings) -> Result<Option<Socks5Udp>, SessionError> {
    let proxy = &settings.proxy;
    // SOCKS5 relays all UDP through one association: one socket cannot be
    // half proxied.
    let relay_udp = proxy.kind == ProxyKind::Socks5 && (proxy.proxy_peers || proxy.proxy_trackers);
    if !relay_udp {
        return Ok(None);
    }
    let assoc = udp_associate(proxy, settings.connect_timeout)
        .await
        .map_err(SessionError::Proxy)?;
    Ok(Some(assoc))
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
fn watch_relay_lost(
    u: &SharedUdpSocket,
    shared: std::sync::Weak<Shared>,
    mut stop: watch::Receiver<bool>,
) -> JoinHandle<()> {
    let mut lost = u.relay_lost();
    tokio::spawn(async move {
        let lost = tokio::select! {
            r = lost.wait_for(|l| *l) => r.is_ok(),
            _ = stop.changed() => return,
        };
        if !lost {
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
    })
}

/// Emits [`Event::DhtBootstrapped`] the first time the DHT's routing table
/// fills, then ends; also ends with the DHT or the session.
fn watch_bootstrap(
    dht: &Dht,
    shared: std::sync::Weak<Shared>,
    mut stop: watch::Receiver<bool>,
) -> JoinHandle<()> {
    let mut stats = dht.subscribe_stats();
    tokio::spawn(async move {
        let bootstrapped = tokio::select! {
            r = stats.wait_for(|s| s.bootstrapped) => match r {
                Ok(s) => s.nodes_v4.saturating_add(s.nodes_v6),
                Err(_) => return,
            },
            _ = stop.changed() => return,
        };
        if let Some(shared) = shared.upgrade() {
            shared.emit(Event::DhtBootstrapped {
                nodes: bootstrapped,
            });
        }
    })
}

/// Emits [`Event::ExternalIpChanged`] with [`ExternalIpSource::Dht`] each
/// time the DHT's votes settle on a new address; ends with the DHT or the
/// session.
fn watch_dht_external_ip(
    dht: &Dht,
    shared: std::sync::Weak<Shared>,
    mut stop: watch::Receiver<bool>,
) -> JoinHandle<()> {
    let mut stats = dht.subscribe_stats();
    tokio::spawn(async move {
        let mut last = None;
        loop {
            tokio::select! {
                r = stats.changed() => if r.is_err() {
                    return;
                },
                _ = stop.changed() => return,
            }
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listen_port_follows_the_peers_family() {
        let stack = |v4: Option<u16>, v6: Option<u16>| {
            Arc::new(NetStack::offline(&Settings::default(), 7000, v4, v6))
        };
        let shared =
            |v4: Option<u16>, v6: Option<u16>| Shared::test(Settings::default(), stack(v4, v6), 0);
        let peer4: SocketAddr = "10.0.0.1:6881".parse().unwrap();
        let mapped: SocketAddr = "[::ffff:10.0.0.1]:6881".parse().unwrap();
        let peer6: SocketAddr = "[2001:db8::1]:6881".parse().unwrap();
        let both = shared(Some(7000), Some(7001));
        let net = both.net();
        assert_eq!(net.listen_port_for(&peer4), 7000);
        assert_eq!(net.listen_port_for(&mapped), 7000);
        assert_eq!(net.listen_port_for(&peer6), 7001);
        // No listener of the family: the first listener's port.
        let v6_only = shared(None, Some(7000));
        assert_eq!(v6_only.net().listen_port_for(&peer4), 7000);
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
