//! The torrent actor: one task owning a torrent's picker, peers, buffers and status. It never
//! blocks: storage jobs run through the session's disk pool and hashing in `spawn_blocking`, both
//! coming back as [`Job`]s, and commands to peers use `try_send` (a peer whose queue is full is
//! closed as stalled).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use fbt_core::{Bitfield, InfoHashes, PieceLayout};
use fbt_metainfo::{Metainfo, RelPath};
use fbt_wire::PexFlags;
use rand::rngs::StdRng;
use tokio::sync::{mpsc, watch};
use tokio::task::{AbortHandle, JoinError, JoinSet};
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, warn};

use super::announcer::{self, Announcer, Signals};
use super::data::Job;
use super::deadline::Deadlines;
use super::dht::{self, DhtAnnouncer, DhtParams};
use super::known::{Known, dialable};
use super::layers::{HashServing, LayerBuilder, LayerFetch, PendingLayers};
use super::magnet::MagnetState;
use super::metadata::MetadataFetch;
use super::peer::Peer;
use super::readers::Readers;
use super::retries::Retries;
use super::serve::PieceReads;
use super::smartban::SmartBan;
use super::storage_ops::StorageOp;
use super::webseed::WebSeeds;
use super::{Cmd, piece_priorities, same_state};
use crate::bandwidth::{BucketPair, Throttle};
use crate::choker::{Choker, ChokerConfig};
use crate::error::StreamError;
use crate::event::{Event, PeerSource};
use crate::peer::messages::PeerEvent;
use crate::picker::{PeerKey, PiecePicker};
use crate::rate::RateMeter;
use crate::resume::ResumeState;
use crate::session::{AddTorrent, Priority, SessionInner, Shared, TorrentSource, wire_hashes};
use crate::status::{TorrentState, TorrentStatus};
use crate::storage::cache::{FlushReason, PieceCache};
use crate::storage::{Storage, error_path};

/// Capacity of the channel carrying every peer's events into the actor.
const PEER_EVENT_QUEUE: usize = 1024;
/// How long remove and shutdown wait for peer tasks to report back.
pub(super) const PEER_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

#[cfg(test)]
thread_local! {
    /// Unit tests: every event emitted on this thread, with the status
    /// handles could read at that moment.
    pub(super) static EMITTED: std::cell::RefCell<Vec<(Event, TorrentStatus)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Where the torrent is in its life.
#[derive(Debug)]
pub(super) enum Phase {
    /// A magnet without metadata, or a v2-only torrent without its piece
    /// layers: peers connect and what is missing is fetched from them.
    Metadata,
    /// Waiting for one of the session's `max_active_checks` slots; the
    /// waiting job is aborted through `cancel`.
    QueuedForCheck { cancel: AbortHandle },
    /// `check_pieces` is running; it counts checked pieces into `done` and
    /// stops at the next piece once `cancel` is set.
    Checking {
        done: Arc<AtomicU32>,
        cancel: Arc<AtomicBool>,
    },
    /// Resume data's stamps are compared with the files' on the disk pool
    /// (`Job::ResumeChecked`): its pieces are trusted, or a full check
    /// follows. Shown as `QueuedForChecking`.
    Resuming,
    /// Checked; downloading or seeding (unless paused).
    Ready,
    /// Stopped by a storage error or invalid metadata.
    Error(String),
}

/// Byte counters and rates.
#[derive(Debug, Default)]
pub(super) struct Counters {
    pub downloaded: u64,
    pub uploaded: u64,
    pub wasted: u64,
    pub dl: RateMeter,
    pub ul: RateMeter,
}

/// What the main loop does after a command.
pub(super) enum Flow {
    Continue,
    Exit,
}

/// Everything that exists only once the torrent's metadata is known.
#[derive(Debug)]
pub(super) struct Loaded {
    pub meta: Arc<Metainfo>,
    pub layout: PieceLayout,
    /// `None` while checking.
    pub picker: Option<PiecePicker>,
    pub piece_prio: Vec<u8>,
    /// Bytes of pieces with a non-zero priority; follows `piece_prio`.
    pub total_wanted: u64,
    pub file_prio: Vec<Priority>,
    pub sequential: bool,
    /// Partially downloaded pieces, by index.
    pub bufs: HashMap<u32, Vec<u8>>,
    /// Which files the storage is told are wanted; follows `file_prio`.
    pub wanted: Arc<WantedFiles>,
    /// Files given a new path by `rename_file`, relative to the save path.
    pub renamed: BTreeMap<usize, RelPath>,
    /// Builds the piece layers a hybrid lacks from its verified pieces.
    pub layer_builder: Option<LayerBuilder>,
    /// Layers built and not yet merged into `meta` (see
    /// [`Actor::swap_layers`]); bounded by the builder's cap.
    pub built_layers: BTreeMap<[u8; 32], Vec<[u8; 32]>>,
    /// When `meta` last took built layers.
    pub layers_swapped: Option<Instant>,
    /// Cached Merkle trees and serving jobs for peers' `hash request`s.
    pub hash_serving: HashServing,
}

/// The wanted flag of each file, for
/// [`Storage::set_file_priorities`]. The check and the priority jobs both
/// apply it on their disk thread; the last one to run reads the latest
/// flags, so an older job never undoes a newer change.
#[derive(Debug)]
pub(super) struct WantedFiles {
    flags: Mutex<Vec<bool>>,
    /// Held by a job from reading `flags` until the storage has them.
    turn: Mutex<()>,
}

impl WantedFiles {
    fn new(file_prio: &[Priority]) -> Self {
        WantedFiles {
            flags: Mutex::new(wanted_flags(file_prio)),
            turn: Mutex::new(()),
        }
    }

    /// Records new priorities; the next `apply` passes them on.
    pub(super) fn set(&self, file_prio: &[Priority]) {
        *self.flags.lock().unwrap_or_else(|e| e.into_inner()) = wanted_flags(file_prio);
    }

    /// Tells `storage` the latest flags. Blocking: disk threads only.
    pub(super) fn apply(&self, storage: &dyn Storage) -> io::Result<()> {
        let _turn = self.turn.lock().unwrap_or_else(|e| e.into_inner());
        let flags = self.flags.lock().unwrap_or_else(|e| e.into_inner()).clone();
        storage.set_file_priorities(&flags)
    }
}

fn wanted_flags(file_prio: &[Priority]) -> Vec<bool> {
    file_prio.iter().map(|p| *p != Priority::Skip).collect()
}

impl Loaded {
    pub(super) fn new(meta: Arc<Metainfo>, file_prio: Vec<Priority>, sequential: bool) -> Self {
        let layout = meta.piece_layout();
        let piece_prio = piece_priorities(&meta, &file_prio);
        let total_wanted = wanted_bytes(&layout, &piece_prio);
        let wanted = Arc::new(WantedFiles::new(&file_prio));
        let layer_builder = LayerBuilder::new(&meta);
        Loaded {
            meta,
            layout,
            picker: None,
            piece_prio,
            total_wanted,
            file_prio,
            sequential,
            bufs: HashMap::new(),
            wanted,
            renamed: BTreeMap::new(),
            layer_builder,
            built_layers: BTreeMap::new(),
            layers_swapped: None,
            hash_serving: HashServing::default(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Actor {
    pub(super) ih: InfoHashes,
    /// The metadata and everything built from it; `None` for a magnet
    /// until its metadata arrives. Set only by [`Actor::install`], together
    /// with `storage`.
    pub(super) loaded: Option<Loaded>,
    /// `None` until the metadata is known.
    pub(super) storage: Option<Arc<dyn Storage>>,
    /// `Some` while a magnet waits for its metadata.
    pub(super) magnet: Option<Box<MagnetState>>,
    /// `Some` while a v2-only torrent, its metadata known (from any
    /// source), fetches the piece layers it lacks; `loaded` is `None` and
    /// the phase `Metadata` meanwhile.
    pub(super) pending_layers: Option<Box<PendingLayers>>,
    /// The handle's view of the metadata, filled once it is known.
    pub(super) metainfo_cell: Arc<OnceLock<Arc<Metainfo>>>,
    /// The check that follows a magnet's metadata is running: the announcer
    /// keeps running through it (no `stopped`/`started` pair).
    pub(super) metadata_check: bool,
    /// Bumped by every `start_check`: a check slot that arrives for an
    /// older one is given back.
    pub(super) check_seq: u64,
    pub(super) shared: Arc<Shared>,
    /// The session's registry, which the actor leaves when removed.
    pub(super) session: Weak<SessionInner>,
    pub(super) cmds: mpsc::Receiver<Cmd>,
    pub(super) peer_tx: mpsc::Sender<PeerEvent>,
    pub(super) peer_rx: mpsc::Receiver<PeerEvent>,
    pub(super) status_tx: watch::Sender<TorrentStatus>,
    pub(super) jobs: JoinSet<Job>,
    /// Bumped on recheck; jobs and peers of older generations are stale.
    pub(super) generation: u64,
    pub(super) phase: Phase,
    /// Paused by the user.
    pub(super) paused: bool,
    /// Held back by the session's queue (see [`Actor::stopped`]).
    pub(super) queued: bool,
    /// A `SetQueued` that arrived while the storage moved: applied once it
    /// is done, the last one winning.
    pub(super) queue_pending: Option<bool>,
    /// The torrent's place in the session's queue, as the scheduler last
    /// said.
    pub(super) queue_position: usize,
    /// Under the queue's control; seed limits apply only then.
    pub(super) auto_managed: bool,
    /// Time spent running, and seeding, up to `time_mark`.
    pub(super) active_time: Duration,
    pub(super) seeding_time: Duration,
    /// When `active_time` and `seeding_time` were last brought up to date.
    pub(super) time_mark: Instant,
    pub(super) peers: HashMap<PeerKey, Peer>,
    pub(super) next_key: u64,
    /// Addresses waiting for a connection slot.
    pub(super) candidates: VecDeque<(SocketAddr, PeerSource)>,
    /// Every address given to `add_peer`, reconnected on resume.
    pub(super) known: Known,
    /// Retry schedule and never-retry set of outgoing connections.
    pub(super) retries: Retries,
    pub(super) choker: Choker,
    pub(super) counters: Counters,
    pub(super) last_state: TorrentState,
    /// `TorrentFinished` was emitted for the current set of wanted pieces.
    pub(super) finished_emitted: bool,
    /// We told our peers we are upload-only (BEP 21): the torrent is
    /// seeding, with every piece on disk.
    pub(super) upload_only_sent: bool,
    /// Addresses of seeds and upload-only peers we closed (or that closed)
    /// while `upload_only_sent`: not dialled again until we stop being
    /// finished. At most `MAX_KNOWN`.
    pub(super) seed_addrs: std::collections::HashSet<SocketAddr>,
    /// Files whose loss `FileError` reported since the torrent last was
    /// Seeding or rechecked.
    pub(super) lost_files: std::collections::HashSet<std::path::PathBuf>,
    /// Pieces lost on disk and not downloaded again yet, for peers whose
    /// extension handshake comes late.
    pub(super) lost_pieces: std::collections::BTreeSet<u32>,
    /// Addresses we asked a relay to introduce us to (BEP 55).
    pub(super) hp_pending: super::holepunch::Pending,
    /// Rendezvous work accepted per peer (BEP 55).
    pub(super) hp_limiter: super::holepunch::RendezvousLimiter,
    pub(super) rng: StdRng,
    /// What the tracker and DHT announcers are told.
    pub(super) signals: watch::Sender<Signals>,
    /// The announcer task; `None` without trackers.
    pub(super) announcer: Option<Announcer>,
    /// Peers from tracker replies; `None` once the announcer is gone.
    pub(super) tracker_rx: Option<mpsc::Receiver<Vec<SocketAddr>>>,
    /// The DHT announcer; `None` without a session DHT, for a private
    /// torrent, and once a magnet's metadata says private.
    pub(super) dht_announcer: Option<DhtAnnouncer>,
    /// Peers from DHT lookups; `None` with no DHT announcer.
    pub(super) dht_rx: Option<mpsc::Receiver<Vec<SocketAddr>>>,
    /// The connectable peers PEX lists, built for the `peers_epoch` it is
    /// keyed by; rebuilt by the first tick with a due peer after the epoch
    /// moved.
    pub(super) pex_live: Option<(u64, Vec<(SocketAddr, PexFlags)>)>,
    /// Bumped whenever the set of connections or their PEX addresses
    /// changes (see [`Actor::on_peer_set_changed`]).
    pub(super) peers_epoch: u64,
    /// The earliest `last_sent + pex_interval` of the PEX peers; `None`
    /// when a peer was never sent to or the peer set changed (scan).
    pub(super) next_pex_due: Option<Instant>,
    /// Unit tests: how often the live list was built.
    /// Verified pieces not yet written, and pieces kept to serve peers;
    /// also the budget reservations of `Loaded.bufs`.
    pub(super) cache: PieceCache,
    /// Hash jobs running; pause waits for them, as for writes.
    pub(super) hashing: usize,
    /// Storage reads running (`Job::Read`, `ReadPiece`, `ReadChunk`): a
    /// storage operation waits for them.
    pub(super) reads_in_flight: usize,
    /// A storage operation holds the storage: no write is issued and reads
    /// that miss the cache wait.
    pub(super) storage_busy: bool,
    /// The storage operation under way.
    pub(super) storage_op: Option<StorageOp>,
    /// Commands that wait for the storage operation, in arrival order; at
    /// most `MAX_DEFERRED_CMDS` (plus the session's one `Shutdown`).
    pub(super) deferred: VecDeque<Cmd>,
    /// A `KeepExisting` move is done: its recheck runs, then this answers.
    pub(super) recheck_after_move:
        Option<tokio::sync::oneshot::Sender<Result<(), crate::error::SessionError>>>,
    /// Where the files are (the save path given at add, changed by moves).
    pub(super) save_path: PathBuf,
    /// The torrent finished: `TorrentFinished` follows once every piece is
    /// written and the storage flushed.
    pub(super) finish_pending: bool,
    /// The flush before `TorrentFinished` is running.
    pub(super) finish_flush: bool,
    /// The first write error since the last `stop` began: `stop` reports
    /// it, since its data was lost.
    pub(super) write_error: Option<io::Error>,
    /// Whole-piece reads in flight for peers' requests, with the requests
    /// waiting on each.
    /// Keyed by piece, with the generation the read was started in.
    pub(super) piece_reads: PieceReads,
    /// The torrent's own rate limit buckets, chained by its peers.
    pub(super) rate: BucketPair,
    /// Open file readers and their waiting reads. After `cmds`, so a
    /// reader whose answer is dropped when the actor exits sees the
    /// command channel closed.
    pub(super) readers: Readers,
    /// The readers' windows: the missing pieces they wait for, and when.
    pub(super) deadlines: Deadlines,
    /// Who sent the blocks of pieces in progress and of failed pieces,
    /// and each sender's trust.
    pub(super) smartban: SmartBan,
    /// The torrent's web seeds (BEP 19).
    pub(super) web_seeds: WebSeeds,
    /// Resume data not yet applied: its pieces wait for the stamp
    /// comparison (taken by the first check).
    pub(super) resume: Option<Box<ResumeState>>,
    /// The pieces verified before the running check started, for resume
    /// data saved meanwhile.
    pub(super) last_have: Option<Bitfield>,
    /// See [`TorrentStatus::need_save_resume`].
    pub(super) need_save_resume: bool,
    /// See [`TorrentStatus::resume_trusted`].
    pub(super) resume_trusted: Option<bool>,
    /// The torrent's own rate limit, as last set.
    pub(super) rate_limit: crate::bandwidth::RateLimit,
    /// When the torrent was first added, seconds since the Unix epoch.
    pub(super) added_time: i64,
    /// When it first finished, likewise.
    pub(super) completed_time: Option<i64>,
    /// What web seed downloads are charged to: the torrent's and the
    /// session's download limits.
    pub(super) web_seed_throttle: Throttle,
    /// How often the status tick ran; read by
    /// [`TorrentHandle::status_ticks`](crate::TorrentHandle::status_ticks),
    /// which tests use to watch the tick rate.
    pub(super) status_ticks: u64,
    #[cfg(test)]
    pub(super) pex_live_builds: u32,
    /// Unit tests: how often `pex_tick` got past its early return.
    #[cfg(test)]
    pub(super) pex_scans: u32,
}

impl Actor {
    pub(super) fn new(
        params: AddTorrent,
        storage: Option<Arc<dyn Storage>>,
        metainfo_cell: Arc<OnceLock<Arc<Metainfo>>>,
        shared: Arc<Shared>,
        session: Weak<SessionInner>,
        cmds: mpsc::Receiver<Cmd>,
        status_tx: watch::Sender<TorrentStatus>,
    ) -> Self {
        let ih = params.source.info_hashes();
        let rate = BucketPair::new(params.rate_limit);
        let rate_limit = params.rate_limit;
        let resume = params.resume;
        let save_path = params.save_path.clone();
        let web_seed_throttle = shared.bandwidth.web_seed(&rate);
        let mut storage = storage;
        let (loaded, magnet, pending_layers, trackers, web_seed_urls) = match params.source {
            TorrentSource::Metainfo(m) => {
                let trackers = m.trackers.clone();
                let urls = m.web_seeds.clone();
                if let Some(fetch) = LayerFetch::new(&m) {
                    // Like a magnet's metadata: the storage is built once
                    // the layers are in (only a custom one is kept).
                    storage = None;
                    let pending = PendingLayers {
                        meta: Arc::new(*m),
                        fetch,
                        storage: params.storage,
                        file_prio: params.file_priorities,
                        sequential: params.sequential,
                    };
                    (None, None, Some(Box::new(pending)), trackers, urls)
                } else {
                    let meta = Arc::new(*m);
                    let _ = metainfo_cell.set(Arc::clone(&meta));
                    let mut loaded = Loaded::new(meta, params.file_priorities, params.sequential);
                    if let Some(r) = &resume {
                        loaded.renamed = r.renamed.iter().cloned().collect();
                    }
                    (Some(loaded), None, None, trackers, urls)
                }
            }
            TorrentSource::Magnet(magnet) => {
                // One tier per `tr`, in order.
                let trackers = magnet.trackers.iter().map(|t| vec![t.clone()]).collect();
                let urls = magnet.web_seeds.clone();
                let fetch =
                    MetadataFetch::new(magnet.info_hashes, shared.settings.max_metadata_size)
                        .deferred();
                let state = MagnetState {
                    fetch,
                    magnet,
                    save_path: params.save_path,
                    storage: params.storage,
                    sequential: params.sequential,
                };
                (None, Some(Box::new(state)), None, trackers, urls)
            }
        };
        let phase = if loaded.is_some() {
            Phase::Checking {
                done: Arc::new(AtomicU32::new(0)),
                cancel: Arc::new(AtomicBool::new(false)),
            }
        } else {
            Phase::Metadata
        };
        let (peer_tx, peer_rx) = mpsc::channel(PEER_EVENT_QUEUE);
        let piece_len = loaded
            .as_ref()
            .map_or(0, |l| u64::from(l.layout.piece_len()));
        let choker = Choker::new(ChokerConfig::from_settings(&shared.settings, piece_len));
        let retries = Retries::new(shared.settings.peer_retry_delay);
        let (signals, _) = watch::channel(Signals::default());
        let (announcer, tracker_rx) = announcer::spawn(announcer::Params {
            ih,
            trackers,
            shared: Arc::clone(&shared),
            status: status_tx.subscribe(),
            signals: signals.subscribe(),
        })
        .unzip();
        // A magnet's privacy is unknown yet: it runs one until it is known.
        let private = loaded.as_ref().is_some_and(|l| l.meta.info.private)
            || pending_layers.as_ref().is_some_and(|p| p.meta.info.private);
        let (dht_announcer, dht_rx) = shared
            .dht
            .clone()
            .filter(|_| !private)
            .map(|dht| {
                dht::spawn(DhtParams {
                    dht,
                    info_hashes: wire_hashes(&ih),
                    port: shared.listen_port,
                    interval: shared.settings.dht_announce_interval,
                    signals: signals.subscribe(),
                    status: status_tx.subscribe(),
                })
            })
            .unzip();
        let cache = PieceCache::new(Arc::clone(&shared.cache), shared.settings.cache_expiry);
        let piece_reads = PieceReads::new(Arc::clone(&shared.cache));
        let mut actor = Actor {
            ih,
            loaded,
            storage,
            magnet,
            pending_layers,
            metainfo_cell,
            metadata_check: false,
            check_seq: 0,
            shared,
            session,
            cmds,
            peer_tx,
            peer_rx,
            status_tx,
            jobs: JoinSet::new(),
            generation: 0,
            phase,
            paused: params.paused,
            queued: false,
            queue_pending: None,
            queue_position: 0,
            auto_managed: params.auto_managed,
            active_time: Duration::ZERO,
            seeding_time: Duration::ZERO,
            time_mark: Instant::now(),
            peers: HashMap::new(),
            next_key: 0,
            candidates: VecDeque::new(),
            known: Known::default(),
            retries,
            choker,
            counters: Counters::default(),
            last_state: TorrentState::CheckingFiles { progress: 0.0 },
            finished_emitted: false,
            upload_only_sent: false,
            seed_addrs: Default::default(),
            lost_files: Default::default(),
            lost_pieces: Default::default(),
            hp_pending: Default::default(),
            hp_limiter: Default::default(),
            rng: rand::make_rng(),
            signals,
            announcer,
            tracker_rx,
            dht_announcer,
            dht_rx,
            pex_live: None,
            peers_epoch: 0,
            next_pex_due: None,
            cache,
            hashing: 0,
            reads_in_flight: 0,
            storage_busy: false,
            storage_op: None,
            deferred: VecDeque::new(),
            recheck_after_move: None,
            save_path,
            finish_pending: false,
            finish_flush: false,
            piece_reads,
            write_error: None,
            rate,
            readers: Readers::default(),
            deadlines: Deadlines::default(),
            smartban: SmartBan::default(),
            web_seeds: WebSeeds::default(),
            web_seed_throttle,
            status_ticks: 0,
            need_save_resume: resume.is_none(),
            resume_trusted: None,
            last_have: None,
            rate_limit,
            added_time: resume.as_ref().map_or_else(unix_now, |r| r.added),
            completed_time: resume.as_ref().and_then(|r| r.completed),
            resume: None,
            #[cfg(test)]
            pex_live_builds: 0,
            #[cfg(test)]
            pex_scans: 0,
        };
        actor.init_web_seeds(web_seed_urls);
        if let Some(r) = resume {
            actor.counters.uploaded = r.uploaded;
            actor.counters.downloaded = r.downloaded;
            actor.active_time = r.active_time;
            actor.seeding_time = r.seeding_time;
            // The current set: seeds added by hand included.
            actor.init_web_seeds(r.web_seeds.clone());
            actor.resume = Some(r);
        }
        if actor.loaded.is_none() {
            actor.last_state = actor.state();
        }
        actor
    }

    pub(super) async fn run(mut self) {
        // Resume data's peers are known addresses, dialled once the torrent
        // runs.
        let saved = self
            .resume
            .as_ref()
            .map(|r| r.peers.clone())
            .unwrap_or_default();
        for addr in saved {
            if dialable(&addr) && self.shared.blocked(addr).is_none() {
                self.remember(addr, PeerSource::Resume);
            }
        }
        self.start_check();
        // Checking, queued for it, or fetching the metadata.
        self.last_state = self.state();
        self.publish();
        self.emit(Event::StateChanged {
            torrent: self.ih,
            state: self.last_state,
        });
        // A magnet connects to its `x.pe` peers and announces right away.
        let hints = self
            .magnet
            .as_ref()
            .map(|m| m.magnet.peers.clone())
            .unwrap_or_default();
        let mut count = 0usize;
        for addr in hints {
            if dialable(&addr) && !self.refuse_blocked(addr) {
                count = count.saturating_add(1);
                if self.remember(addr, PeerSource::Magnet) {
                    let source = self.known_source(&addr);
                    self.queue_addr(addr, source);
                }
            }
        }
        self.peers_found(PeerSource::Magnet, count);
        self.sync_running();
        self.try_connect();
        let status_interval = self.shared.settings.status_interval;
        let idle_interval = self.shared.settings.idle_status_interval;
        let mut tick = tokio::time::interval(status_interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut rechoke = tokio::time::interval(self.shared.settings.rechoke_interval);
        rechoke.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // Whether `tick` currently runs at the idle period; the check at the
        // end of every iteration keeps it in step with [`Actor::is_idle`].
        let mut tick_idle = false;
        loop {
            if !self.storage_busy {
                if let Some(queued) = self.queue_pending.take() {
                    self.set_queued(queued).await;
                    continue;
                }
                if self.recheck_after_move.is_some() {
                    self.finish_keep_existing().await;
                    continue;
                }
                if let Some(cmd) = self.deferred.pop_front() {
                    if let Flow::Exit = self.on_cmd(cmd).await {
                        return;
                    }
                    continue;
                }
            }
            tokio::select! {
                cmd = self.cmds.recv() => {
                    let Some(cmd) = cmd else {
                        // Every handle is gone, the session included.
                        let _ = self.stop(None).await;
                        return;
                    };
                    if let Flow::Exit = self.on_cmd(cmd).await {
                        return;
                    }
                }
                Some(ev) = self.peer_rx.recv() => self.on_peer_event(ev),
                peers = recv_opt(&mut self.tracker_rx) => match peers {
                    Some(peers) => self.on_found_peers(PeerSource::Tracker, peers),
                    None => self.tracker_rx = None,
                },
                peers = recv_opt(&mut self.dht_rx) => match peers {
                    Some(peers) => self.on_found_peers(PeerSource::Dht, peers),
                    None => self.dht_rx = None,
                },
                Some(r) = self.jobs.join_next(), if !self.jobs.is_empty() => self.on_job_result(r),
                _ = tick.tick() => {
                    self.status_ticks += 1;
                    self.flush(FlushReason::Expired);
                    self.release_stale_partials();
                    self.expire_parole_pieces();
                    self.readers_tick();
                    self.top_up_all(None);
                    self.web_seed_tick();
                    self.metadata_tick();
                    self.layer_tick();
                    self.pex_tick();
                    self.lsd_tick();
                    self.queue_retries();
                    self.try_connect();
                    self.account_time();
                    self.check_seed_limits();
                    self.swap_layers(false);
                    self.publish();
                }
                _ = rechoke.tick() => self.rechoke(),
            }
            // End of every iteration: an idle torrent does not need a
            // sub-second tick, and any event that ends idleness is noticed
            // here, so leaving idle waits for no more than one
            // `status_interval`. Re-evaluating is a few field reads;
            // re-creating the interval restarts its countdown from now,
            // which is right in both directions (the iteration that flips
            // idleness has already published whatever it changed).
            let idle = self.is_idle();
            if idle != tick_idle {
                tick_idle = idle;
                let period = if idle { idle_interval } else { status_interval };
                tick = tokio::time::interval_at(Instant::now() + period, period);
                tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            }
        }
    }

    // ----- status and state -------------------------------------------------

    /// The torrent's name; until the metadata is known, the magnet's `dn`
    /// or empty.
    pub(super) fn name(&self) -> &str {
        match (self.known_meta(), &self.magnet) {
            (Some(m), _) => m.info.name.as_str(),
            (None, Some(m)) => m.magnet.display_name.as_deref().unwrap_or(""),
            (None, None) => "",
        }
    }

    /// The metadata, installed or waiting for its layers.
    pub(super) fn known_meta(&self) -> Option<&Arc<Metainfo>> {
        self.loaded
            .as_ref()
            .map(|l| &l.meta)
            .or_else(|| self.pending_layers.as_ref().map(|p| &p.meta))
    }

    /// Sets the metadata and the storage built for it: the one place either
    /// is set, so neither exists without the other.
    pub(super) fn install(&mut self, loaded: Loaded, storage: Arc<dyn Storage>) {
        self.loaded = Some(loaded);
        self.storage = Some(storage);
    }

    /// Drops every partially downloaded piece and its budget reservation,
    /// and what smart ban recorded of pieces (trust stays).
    pub(super) fn clear_bufs(&mut self) {
        if let Some(l) = self.loaded.as_mut() {
            l.bufs.clear();
        }
        self.smartban.forget_all();
        self.cache.release_partials();
        self.drop_piece_reads();
    }

    /// Pieces that left the picker without their buffer (every request
    /// came back unanswered) give their reservation back; on the status
    /// tick, so `fill` stays cheap.
    fn release_stale_partials(&mut self) {
        let Some(picker) = self.loaded.as_ref().and_then(|l| l.picker.as_ref()) else {
            return;
        };
        self.cache.retain_partials(|i| picker.is_in_progress(i));
    }

    pub(super) fn emit(&self, ev: Event) {
        #[cfg(test)]
        EMITTED.with(|log| {
            log.borrow_mut()
                .push((ev.clone(), self.status_tx.borrow().clone()));
        });
        self.shared.emit(ev);
    }

    /// Paused by the user or held back by the queue: both stop the torrent
    /// the same way (no peers, `stopped` announced).
    pub(super) fn stopped(&self) -> bool {
        self.paused || self.queued
    }

    /// Ready and not stopped: data may flow.
    pub(super) fn active(&self) -> bool {
        matches!(self.phase, Phase::Ready) && !self.stopped()
    }

    /// Peers may connect: ready, or a magnet fetching its metadata, and not
    /// stopped.
    pub(super) fn connectable(&self) -> bool {
        matches!(self.phase, Phase::Ready | Phase::Metadata) && !self.stopped()
    }

    /// What the announcer is told: announcing while peers may connect, and
    /// through the check that follows a magnet's metadata (only while that
    /// check is still queued or running).
    pub(super) fn running(&self) -> bool {
        self.connectable()
            || (self.metadata_check
                && matches!(
                    self.phase,
                    Phase::Checking { .. } | Phase::QueuedForCheck { .. } | Phase::Resuming
                )
                && !self.stopped())
    }

    /// Whether the status tick may run at `idle_status_interval`: the torrent
    /// is seeding or paused, and nothing the tick drives has work — no peers,
    /// open readers, deadline pieces, web seeds in use, background jobs,
    /// layers to fetch, or built layers waiting for a swap. Commands, peer
    /// events and job results wake the actor whatever the tick does; only
    /// tick-driven timers slow down, and [`Actor::run`] switches back at the
    /// end of the first iteration anything above appears in.
    pub(super) fn is_idle(&self) -> bool {
        (self.paused || self.state() == TorrentState::Seeding)
            && self.peers.is_empty()
            && self.readers.is_idle()
            && self.deadlines.is_empty()
            && self.web_seeds.num_active() == 0
            && self.jobs.is_empty()
            && self.pending_layers.is_none()
            && self
                .loaded
                .as_ref()
                .is_none_or(|l| l.built_layers.is_empty())
    }

    pub(super) fn state(&self) -> TorrentState {
        match &self.phase {
            Phase::Error(_) => TorrentState::Error,
            Phase::Metadata | Phase::Ready if self.paused => TorrentState::Paused,
            Phase::Metadata | Phase::Ready if self.queued => TorrentState::Queued,
            Phase::Metadata => TorrentState::DownloadingMetadata,
            Phase::QueuedForCheck { .. } | Phase::Resuming => TorrentState::QueuedForChecking,
            Phase::Checking { done, .. } => {
                let n = self.loaded.as_ref().map_or(0, |l| l.layout.num_pieces());
                let progress = if n == 0 {
                    1.0
                } else {
                    f64::from(done.load(Ordering::Relaxed).min(n)) / f64::from(n)
                };
                TorrentState::CheckingFiles { progress }
            }
            Phase::Ready => {
                if self
                    .loaded
                    .as_ref()
                    .and_then(|l| l.picker.as_ref())
                    .is_some_and(PiecePicker::is_finished)
                    && (self.cache.is_idle() || self.finished_emitted)
                    && !self.finish_pending
                    && !self.finish_flush
                {
                    // Complete on disk, not only in the cache. Once the
                    // finish's flush is back (`finished_emitted` and no
                    // finish under way), whatever is dirty passed after it:
                    // skipped pieces (a reader's window), which do not
                    // make the torrent downloading again.
                    TorrentState::Seeding
                } else {
                    TorrentState::Downloading
                }
            }
        }
    }

    pub(super) fn snapshot(&self) -> TorrentStatus {
        let total_wanted = self.loaded.as_ref().map_or(0, |l| l.total_wanted);
        let wanted_done = self
            .loaded
            .as_ref()
            .and_then(|l| l.picker.as_ref())
            .map_or(0, |p| total_wanted.saturating_sub(p.wanted_left_bytes()));
        let progress = if total_wanted == 0 {
            if self.loaded.as_ref().is_some_and(|l| l.picker.is_some()) {
                1.0
            } else {
                0.0
            }
        } else {
            wanted_done as f64 / total_wanted as f64
        };
        let now = Instant::now();
        let connected = self
            .peers
            .values()
            .filter(|p| p.connected && p.tx.is_some());
        let (num_peers, num_seeds) = connected.fold((0u32, 0u32), |(n, s), p| {
            (
                n.saturating_add(1),
                s.saturating_add(u32::from(p.is_seed())),
            )
        });
        TorrentStatus {
            info_hashes: self.ih,
            name: self.name().to_owned(),
            has_metadata: self.loaded.is_some(),
            state: self.state(),
            progress,
            total_wanted,
            wanted_done,
            total_downloaded: self.counters.downloaded,
            total_uploaded: self.counters.uploaded,
            total_wasted: self.counters.wasted,
            download_rate: self.counters.dl.rate(now),
            upload_rate: self.counters.ul.rate(now),
            num_peers,
            num_seeds,
            num_web_seeds: self.web_seeds.num_active(),
            error: match &self.phase {
                Phase::Error(e) => Some(e.clone()),
                _ => None,
            },
            save_path: self.save_path.clone(),
            queue_position: self.queue_position,
            auto_managed: self.auto_managed,
            active_time: self.active_time,
            seeding_time: self.seeding_time,
            need_save_resume: self.need_save_resume,
            resume_trusted: self.resume_trusted,
            is_seed: self
                .loaded
                .as_ref()
                .and_then(|l| l.picker.as_ref())
                .is_some_and(|p| p.have().is_full()),
        }
    }

    /// Adds the time since the last call to `active_time` (while running)
    /// and `seeding_time` (while seeding).
    pub(super) fn account_time(&mut self) {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.time_mark);
        self.time_mark = now;
        if self.running() {
            self.active_time = self.active_time.saturating_add(elapsed);
        }
        if self.state() == TorrentState::Seeding {
            self.seeding_time = self.seeding_time.saturating_add(elapsed);
        }
    }

    /// An auto-managed seed past [`Settings::seed_ratio_limit`] or
    /// [`Settings::seed_time_limit`] pauses, leaves the queue's control
    /// (so resuming it seeds on) and emits `SeedLimitReached`. Ratio:
    /// uploaded over the larger of downloaded and wanted bytes.
    ///
    /// [`Settings::seed_ratio_limit`]: crate::Settings::seed_ratio_limit
    /// [`Settings::seed_time_limit`]: crate::Settings::seed_time_limit
    fn check_seed_limits(&mut self) {
        // During a storage move the pause could not flush: the check
        // waits for the tick after the move, when the verdict is the same.
        if !self.auto_managed
            || self.stopped()
            || self.storage_busy
            || self.state() != TorrentState::Seeding
        {
            return;
        }
        let s = &self.shared.settings;
        let base = self
            .counters
            .downloaded
            .max(self.loaded.as_ref().map_or(0, |l| l.total_wanted));
        let ratio_hit = s
            .seed_ratio_limit
            .is_some_and(|limit| base > 0 && self.counters.uploaded as f64 / base as f64 >= limit);
        let time_hit = s
            .seed_time_limit
            .is_some_and(|limit| self.seeding_time >= limit);
        if !(ratio_hit || time_hit) {
            return;
        }
        debug!(torrent = %self.name(), ratio_hit, time_hit, "seed limit reached");
        self.auto_managed = false;
        // Paused and out of the queue's control: both are in resume data.
        self.need_save_resume = true;
        if let Some(session) = self.session.upgrade() {
            session.set_auto_managed(&self.ih, false);
        }
        // The pause path; nobody waits for the flush.
        self.paused = true;
        self.record_user_pause();
        self.metadata_check = false;
        self.close_all();
        self.flush(FlushReason::All);
        self.emit(Event::SeedLimitReached { torrent: self.ih });
        self.refresh_state();
        self.publish();
    }

    /// Publishes the status if it changed.
    pub(super) fn publish(&self) {
        let new = self.snapshot();
        self.status_tx.send_if_modified(|cur| {
            if *cur == new {
                false
            } else {
                *cur = new;
                true
            }
        });
    }

    /// Publishes the status and emits `StateChanged` only if the state
    /// changed; other changes wait for the next status tick. The announcer
    /// learns whether to run.
    pub(super) fn refresh_state(&mut self) {
        self.account_time();
        let state = self.state();
        if !same_state(&state, &self.last_state) {
            self.publish();
            self.last_state = state;
            // Pauses, resumes, finishes and errors change what the queue
            // counts.
            self.shared.queue_dirty.notify_one();
            self.emit(Event::StateChanged {
                torrent: self.ih,
                state,
            });
        }
        self.sync_upload_only();
        self.sync_running();
    }

    /// Tells the announcer whether to run; a new run is a new `started`.
    fn sync_running(&self) {
        let running = self.running();
        self.signal(|s| {
            if s.running == running {
                return false;
            }
            s.running = running;
            if running {
                s.starts = s.starts.wrapping_add(1);
            }
            true
        });
    }

    // ----- announcing ---------------------------------------------------------

    /// Updates the announcers' signals; `f` returns whether it changed
    /// them. Never waits.
    pub(super) fn signal(&self, f: impl FnOnce(&mut Signals) -> bool) {
        self.signals.send_if_modified(f);
    }

    /// Peers from a tracker reply, a DHT lookup or PEX join the known addresses,
    /// as `add_peer` does (bounded, oldest unused evicted); addresses nobody
    /// can dial (port 0, unspecified, multicast) are dropped, and blocked
    /// ones are reported and dropped.
    pub(super) fn on_found_peers(&mut self, source: PeerSource, peers: Vec<SocketAddr>) {
        let mut count = 0usize;
        for addr in peers {
            if !dialable(&addr) || self.refuse_blocked(addr) {
                continue;
            }
            count = count.saturating_add(1);
            if !self.retries.is_never(&addr) && self.remember(addr, source) && self.connectable() {
                let source = self.known_source(&addr);
                self.queue_addr(addr, source);
            }
        }
        self.peers_found(source, count);
        self.try_connect();
    }

    /// Asks the session to announce this torrent on the LAN (BEP 14) while
    /// it is running, public (a magnet counts as public until its metadata
    /// says otherwise) and not paused. The session keeps it to one announce
    /// per interval.
    fn lsd_tick(&self) {
        if self.connectable() && !self.is_private() {
            self.shared
                .lsd
                .want(&wire_hashes(&self.ih), std::time::Instant::now());
        }
    }

    /// `PeersFound` for a non-empty batch.
    fn peers_found(&self, source: PeerSource, count: usize) {
        if count > 0 {
            self.emit(Event::PeersFound {
                torrent: self.ih,
                source,
                count,
            });
        }
    }

    /// The torrent is known to be private (BEP 27): no DHT, PEX or `Port`.
    /// A magnet is not, until its metadata arrives.
    pub(super) fn is_private(&self) -> bool {
        self.known_meta().is_some_and(|m| m.info.private)
    }

    // ----- checking -----------------------------------------------------------

    // ----- errors -------------------------------------------------------------

    /// A storage error: the torrent stops with `TorrentState::Error`.
    pub(super) fn fail(&mut self, e: &io::Error, during: &str) {
        if matches!(self.phase, Phase::Error(_)) {
            return;
        }
        warn!(torrent = %self.name(), error = %e, "storage error while {during}");
        // What the files hold is unknown now: no check is skipped.
        self.resume = None;
        let message = format!("{during}: {e}");
        let reader_error = StreamError::TorrentError(message.clone());
        self.cancel_check();
        self.phase = Phase::Error(message.clone());
        // A failed metadata check ends the announce like any other error.
        self.metadata_check = false;
        self.emit(Event::FileError {
            path: error_path(e).map(Path::to_path_buf),
            torrent: self.ih,
            message,
        });
        self.close_all();
        self.clear_bufs();
        // Waiting reads (those of the dropped whole-piece reads included)
        // fail; the readers stay open.
        self.fail_readers(reader_error);
        // The pieces not written yet are lost; the next resume rechecks.
        self.cache.discard_all();
        self.finish_pending = false;
        self.refresh_state();
    }

    /// The metadata is unusable (it matches the info hash but does not
    /// parse): the torrent stops with `TorrentState::Error`. A resume
    /// fetches it again.
    pub(super) fn metadata_failed(&mut self, message: String) {
        warn!(torrent = %self.name(), %message, "metadata failed");
        self.phase = Phase::Error(message);
        self.metadata_check = false;
        self.close_all();
        self.refresh_state();
    }

    pub(super) fn on_job_result(&mut self, r: Result<Job, JoinError>) {
        match r {
            Ok(job) => self.on_job(job),
            Err(e) if e.is_cancelled() => debug!(error = %e, "queued check left the queue"),
            Err(e) => warn!(error = %e, "torrent background job failed"),
        }
    }

    // ----- metadata -------------------------------------------------------------

    // ----- commands -----------------------------------------------------------
}

/// The next message of `rx`; pending forever without one.
async fn recv_opt<T>(rx: &mut Option<mpsc::Receiver<T>>) -> Option<T> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Now, in seconds since the Unix epoch.
pub(super) fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// Bytes of the pieces with a non-zero priority.
pub(super) fn wanted_bytes(layout: &PieceLayout, prio: &[u8]) -> u64 {
    (0..layout.num_pieces())
        .filter(|&i| prio.get(i as usize).is_some_and(|&p| p > 0))
        .map(|i| u64::from(layout.piece_size(i)))
        .sum()
}

#[cfg(test)]
mod tests {
    use fbt_core::Bitfield;

    use super::super::check::add_availability;
    use super::super::commands::selected_priorities;
    use super::*;

    /// A peer closed while the check ran (no command sender, still waiting
    /// for its `Disconnected`) counts toward availability, since its
    /// disconnect takes its pieces off again; older generations do not.
    #[test]
    fn availability_counts_closing_peers_of_the_generation() {
        let layout = PieceLayout::new(4 * 16_384, 16_384).expect("layout");
        let mut picker = PiecePicker::new(layout, &Bitfield::new(4));
        let (tx, _rx) = mpsc::channel(1);
        let addr = SocketAddr::from(([10, 0, 0, 1], 6881));
        let mut closing = Peer::new(
            addr,
            tx.clone(),
            3,
            4,
            crate::peer::transport::Transport::Tcp,
            crate::event::PeerSource::Tracker,
        );
        closing.bits.set(0);
        closing.tx = None;
        let mut stale = Peer::new(
            addr,
            tx,
            2,
            4,
            crate::peer::transport::Transport::Tcp,
            crate::event::PeerSource::Tracker,
        );
        stale.bits.set(1);
        let peers = HashMap::from([(PeerKey(1), closing), (PeerKey(2), stale)]);
        add_availability(&mut picker, &peers, 3);
        assert_eq!(picker.availability(0), 1);
        assert_eq!(picker.availability(1), 0);
        // The disconnect removes exactly what was added.
        picker.remove_peer_pieces(peers[&PeerKey(1)].pieces());
        assert_eq!(picker.availability(0), 0);
    }

    /// A ready actor whose every piece passed: seeding, with nothing else
    /// going on.
    fn seeding_actor() -> Actor {
        let mut a = super::super::swarm::tests::actor_with(&[3 * 16_384], 16_384, 0);
        let l = a.loaded.as_mut().expect("loaded");
        let n = l.layout.num_pieces();
        let picker = l.picker.as_mut().expect("picker");
        for piece in 0..n {
            picker.piece_passed(piece);
        }
        assert_eq!(a.state(), TorrentState::Seeding);
        a
    }

    /// Seeding with no peers and nothing running is idle; a peer ends that
    /// at once, even one still dialling.
    #[test]
    fn seeding_without_peers_or_jobs_is_idle() {
        use super::super::swarm::tests::addr;
        use crate::event::PeerSource;
        use crate::peer::transport::Transport;

        let mut a = seeding_actor();
        assert!(a.is_idle(), "seeding with nothing going on");
        let (tx, _rx) = mpsc::channel(1);
        let generation = a.generation;
        let key = a.new_key();
        a.peers.insert(
            key,
            Peer::new(
                addr(1),
                tx,
                generation,
                1,
                Transport::Tcp,
                PeerSource::Tracker,
            ),
        );
        assert!(!a.is_idle(), "a peer is connected or dialling");
        a.peers.remove(&key).expect("peer");
        assert!(a.is_idle(), "the peer is gone");
    }

    /// Everything Review Focus 1 lists as ending idleness keeps the fast
    /// tick: a download, a job in flight, an open reader and a web seed.
    /// A pause alone does not: only commands can end it.
    #[tokio::test]
    async fn anything_pending_keeps_the_fast_tick() {
        use tokio::sync::oneshot;

        use super::super::swarm::tests::actor_with;
        use crate::stream::ReadAhead;
        use crate::stream::reader::ReaderLink;

        let mut a = actor_with(&[3 * 16_384], 16_384, 0);
        assert!(!a.is_idle(), "downloading");
        a.paused = true;
        assert!(a.is_idle(), "paused with nothing running");
        a.paused = false;

        let mut a = seeding_actor();
        a.jobs.spawn(std::future::pending::<Job>());
        assert!(!a.is_idle(), "a job is running");

        let mut b = seeding_actor();
        let (tx, mut rx) = oneshot::channel();
        let link = std::sync::Arc::new(ReaderLink::new(0, ReadAhead::default()));
        b.on_open_reader(0, link, tx);
        assert!(rx.try_recv().expect("answered").is_ok(), "reader opened");
        assert!(!b.is_idle(), "an open reader is tick-driven work");

        let mut c = seeding_actor();
        c.init_web_seeds(vec!["http://127.0.0.1:8080/torrent/".to_owned()]);
        assert!(!c.is_idle(), "a web seed in use");
    }

    /// A paused torrent that starts a recheck is not idle while its check
    /// job runs, and goes idle again only once the result has been handled.
    #[tokio::test]
    async fn paused_during_a_check_is_not_idle_until_it_returns() {
        let mut a = seeding_actor();
        a.paused = true;
        assert!(a.is_idle(), "paused with nothing running");

        a.recheck();
        assert!(
            matches!(a.phase, Phase::Checking { .. }),
            "a free check slot starts the check at once"
        );
        assert!(!a.is_idle(), "the recheck is in flight");

        let r = a.jobs.join_next().await.expect("the check result");
        a.on_job_result(r);
        assert_eq!(a.jobs.len(), 0, "the handled check spawned nothing");
        assert!(a.is_idle(), "paused with the check back");
    }

    /// A paused torrent whose storage is being moved is not idle while the
    /// move's job runs, and goes idle again only once it has returned and
    /// the caller has been answered.
    #[tokio::test]
    async fn paused_during_a_storage_move_is_not_idle_until_it_finishes() {
        use std::path::PathBuf;

        use tokio::sync::oneshot;

        use crate::storage::MoveMode;

        let mut a = seeding_actor();
        a.paused = true;
        assert!(a.is_idle(), "paused with nothing running");

        let (tx, mut rx) = oneshot::channel();
        a.on_move_storage(PathBuf::from("elsewhere"), MoveMode::FailIfExists, tx)
            .await;
        assert!(a.storage_busy);
        assert_eq!(a.jobs.len(), 1, "the move job started");
        assert!(!a.is_idle(), "the move is in flight");

        let r = a.jobs.join_next().await.expect("the move result");
        a.on_job_result(r);
        assert!(rx.try_recv().is_ok(), "the caller was answered");
        assert!(!a.storage_busy, "the storage is free again");
        assert_eq!(a.jobs.len(), 0, "the handled move spawned nothing");
        assert!(a.is_idle(), "paused with the move back");
    }

    #[test]
    fn so_selection_is_merged_in_one_pass() {
        use Priority::{Normal as N, Skip as S};
        assert!(selected_priorities(3, None).is_empty());
        // Unsorted, overlapping, nested, empty and out-of-range ranges.
        #[allow(
            clippy::reversed_empty_ranges,
            reason = "an empty range is valid input"
        )]
        let sel = [7..=20, 0..=0, 2..=4, 3..=3, 5..=4, 3..=5, 100..=200];
        assert_eq!(
            selected_priorities(10, Some(&sel)),
            [N, S, N, N, N, N, S, N, N, N]
        );
        assert_eq!(selected_priorities(0, Some(&sel)), []);
        // The worst case the magnet parser allows stays cheap.
        let many: Vec<_> = (0..1000).rev().map(|i| i * 1000..=i * 1000 + 999).collect();
        let all = selected_priorities(1_000_000, Some(&many));
        assert!(all.iter().all(|p| *p == N));
    }
}
