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

use fbt_core::{InfoHashes, PieceLayout};
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
use super::magnet::MagnetState;
use super::metadata::MetadataFetch;
use super::peer::Peer;
use super::readers::Readers;
use super::retries::Retries;
use super::serve::PieceReads;
use super::smartban::SmartBan;
use super::storage_ops::StorageOp;
use super::{Cmd, piece_priorities, same_state};
use crate::bandwidth::BucketPair;
use crate::choker::{Choker, ChokerConfig};
use crate::error::StreamError;
use crate::event::{Event, PeerSource};
use crate::peer::messages::PeerEvent;
use crate::picker::{PeerKey, PiecePicker};
use crate::rate::RateMeter;
use crate::session::{AddTorrent, Priority, SessionInner, Shared, TorrentSource};
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
    /// A magnet without metadata: peers connect and the metadata is
    /// fetched from them.
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
    pub(super) paused: bool,
    pub(super) peers: HashMap<PeerKey, Peer>,
    pub(super) next_key: u64,
    /// Addresses waiting for a connection slot.
    pub(super) candidates: VecDeque<SocketAddr>,
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
        let save_path = params.save_path.clone();
        let (loaded, magnet, trackers) = match params.source {
            TorrentSource::Metainfo(m) => {
                let trackers = m.trackers.clone();
                let meta = Arc::new(*m);
                let _ = metainfo_cell.set(Arc::clone(&meta));
                let loaded = Loaded::new(meta, params.file_priorities, params.sequential);
                (Some(loaded), None, trackers)
            }
            TorrentSource::Magnet(magnet) => {
                // One tier per `tr`, in order.
                let trackers = magnet.trackers.iter().map(|t| vec![t.clone()]).collect();
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
                (None, Some(Box::new(state)), trackers)
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
        let private = loaded.as_ref().is_some_and(|l| l.meta.info.private);
        let (dht_announcer, dht_rx) = shared
            .dht
            .clone()
            .filter(|_| !private)
            .map(|dht| {
                dht::spawn(DhtParams {
                    dht,
                    info_hash: ih.wire_hash(),
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
            #[cfg(test)]
            pex_live_builds: 0,
            #[cfg(test)]
            pex_scans: 0,
        };
        if actor.magnet.is_some() {
            actor.last_state = actor.state();
        }
        actor
    }

    pub(super) async fn run(mut self) {
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
                if self.remember(addr) {
                    self.queue_addr(addr);
                }
            }
        }
        self.peers_found(PeerSource::Magnet, count);
        self.sync_running();
        self.try_connect();
        let mut tick = tokio::time::interval(self.shared.settings.status_interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut rechoke = tokio::time::interval(self.shared.settings.rechoke_interval);
        rechoke.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            if !self.storage_busy {
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
                    self.flush(FlushReason::Expired);
                    self.release_stale_partials();
                    self.expire_parole_pieces();
                    self.readers_tick();
                    self.top_up_all(None);
                    self.metadata_tick();
                    self.pex_tick();
                    self.lsd_tick();
                    self.queue_retries();
                    self.try_connect();
                    self.publish();
                }
                _ = rechoke.tick() => self.rechoke(),
            }
        }
    }

    // ----- status and state -------------------------------------------------

    /// The torrent's name; until the metadata is known, the magnet's `dn`
    /// or empty.
    pub(super) fn name(&self) -> &str {
        match (&self.loaded, &self.magnet) {
            (Some(l), _) => l.meta.info.name.as_str(),
            (None, Some(m)) => m.magnet.display_name.as_deref().unwrap_or(""),
            (None, None) => "",
        }
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

    /// Ready and not paused: data may flow.
    pub(super) fn active(&self) -> bool {
        matches!(self.phase, Phase::Ready) && !self.paused
    }

    /// Peers may connect: ready, or a magnet fetching its metadata, and not
    /// paused.
    pub(super) fn connectable(&self) -> bool {
        matches!(self.phase, Phase::Ready | Phase::Metadata) && !self.paused
    }

    /// What the announcer is told: announcing while peers may connect, and
    /// through the check that follows a magnet's metadata (only while that
    /// check is still queued or running).
    pub(super) fn running(&self) -> bool {
        self.connectable()
            || (self.metadata_check
                && matches!(
                    self.phase,
                    Phase::Checking { .. } | Phase::QueuedForCheck { .. }
                )
                && !self.paused)
    }

    pub(super) fn state(&self) -> TorrentState {
        match &self.phase {
            Phase::Error(_) => TorrentState::Error,
            Phase::Metadata if self.paused => TorrentState::Paused,
            Phase::Metadata => TorrentState::DownloadingMetadata,
            Phase::QueuedForCheck { .. } => TorrentState::QueuedForChecking,
            Phase::Checking { done, .. } => {
                let n = self.loaded.as_ref().map_or(0, |l| l.layout.num_pieces());
                let progress = if n == 0 {
                    1.0
                } else {
                    f64::from(done.load(Ordering::Relaxed).min(n)) / f64::from(n)
                };
                TorrentState::CheckingFiles { progress }
            }
            Phase::Ready if self.paused => TorrentState::Paused,
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
            error: match &self.phase {
                Phase::Error(e) => Some(e.clone()),
                _ => None,
            },
            save_path: self.save_path.clone(),
        }
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
        let state = self.state();
        if !same_state(&state, &self.last_state) {
            self.publish();
            self.last_state = state;
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
            if !self.retries.is_never(&addr) && self.remember(addr) && self.connectable() {
                self.queue_addr(addr);
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
                .want(self.ih.wire_hash(), std::time::Instant::now());
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
        self.loaded.as_ref().is_some_and(|l| l.meta.info.private)
    }

    // ----- checking -----------------------------------------------------------

    // ----- errors -------------------------------------------------------------

    /// A storage error: the torrent stops with `TorrentState::Error`.
    pub(super) fn fail(&mut self, e: &io::Error, during: &str) {
        if matches!(self.phase, Phase::Error(_)) {
            return;
        }
        warn!(torrent = %self.name(), error = %e, "storage error while {during}");
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
        );
        closing.bits.set(0);
        closing.tx = None;
        let mut stale = Peer::new(addr, tx, 2, 4, crate::peer::transport::Transport::Tcp);
        stale.bits.set(1);
        let peers = HashMap::from([(PeerKey(1), closing), (PeerKey(2), stale)]);
        add_availability(&mut picker, &peers, 3);
        assert_eq!(picker.availability(0), 1);
        assert_eq!(picker.availability(1), 0);
        // The disconnect removes exactly what was added.
        picker.remove_peer_pieces(peers[&PeerKey(1)].pieces());
        assert_eq!(picker.availability(0), 0);
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
