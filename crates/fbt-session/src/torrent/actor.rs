//! The torrent actor: one task owning a torrent's picker, peers, buffers and
//! status. It never blocks: disk and hashing run in `spawn_blocking` and
//! come back as [`Job`]s, and commands to peers use `try_send` (a peer whose
//! queue is full is closed as stalled).

use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use fbt_core::{Bitfield, InfoHashes, PieceLayout};
use fbt_metainfo::{Magnet, Metainfo, MetainfoError};
use rand::rngs::StdRng;
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinError, JoinSet};
use tokio::time::{Instant, MissedTickBehavior};
use tracing::warn;

use super::announcer::{self, Announcer, STOPPED_TIMEOUT, Signals};
use super::data::Job;
use super::dht::{self, DhtAnnouncer, DhtParams};
use super::metadata::MetadataFetch;
use super::swarm::{BanTarget, Known, Peer, Retries, dialable};
use super::{Cmd, piece_priorities, same_state};
use crate::choker::Choker;
use crate::error::SessionError;
use crate::event::{Event, PeerSource};
use crate::peer::messages::{PeerCommand, PeerEvent};
use crate::picker::{PeerKey, PiecePicker};
use crate::rate::RateMeter;
use crate::session::{
    AddTorrent, Priority, RemoveMode, SessionInner, Shared, TorrentSource, release,
};
use crate::status::{TorrentState, TorrentStatus};
use crate::storage::{FsStorage, Storage, check_pieces};

/// Capacity of the channel carrying every peer's events into the actor.
const PEER_EVENT_QUEUE: usize = 1024;
/// How long remove and shutdown wait for peer tasks to report back.
const PEER_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Where the torrent is in its life.
#[derive(Debug)]
pub(super) enum Phase {
    /// A magnet without metadata: peers connect and the metadata is
    /// fetched from them.
    Metadata,
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
enum Flow {
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
}

impl Loaded {
    fn new(meta: Arc<Metainfo>, file_prio: Vec<Priority>, sequential: bool) -> Self {
        let layout = meta.piece_layout();
        let piece_prio = piece_priorities(&meta, &file_prio);
        let total_wanted = wanted_bytes(&layout, &piece_prio);
        Loaded {
            meta,
            layout,
            picker: None,
            piece_prio,
            total_wanted,
            file_prio,
            sequential,
            bufs: HashMap::new(),
        }
    }
}

/// A magnet torrent until its metadata arrives.
#[derive(Debug)]
pub(super) struct MagnetState {
    pub fetch: MetadataFetch,
    pub magnet: Magnet,
    pub save_path: PathBuf,
    /// The custom storage from `AddTorrent`, used once the metadata is in.
    pub storage: Option<Arc<dyn Storage>>,
    pub sequential: bool,
}

impl MagnetState {
    /// A fresh fetch; completed buffers are verified in `spawn_blocking`.
    pub(super) fn new_fetch(&self, max_size: usize) -> MetadataFetch {
        MetadataFetch::new(self.magnet.info_hashes, max_size).deferred()
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
        let choker = Choker::new(shared.settings.unchoke_slots);
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
        let mut actor = Actor {
            ih,
            loaded,
            storage,
            magnet,
            metainfo_cell,
            metadata_check: false,
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
            rng: rand::make_rng(),
            signals,
            announcer,
            tracker_rx,
            dht_announcer,
            dht_rx,
        };
        if actor.magnet.is_some() {
            actor.last_state = actor.state();
        }
        actor
    }

    pub(super) async fn run(mut self) {
        self.start_check();
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
            if dialable(&addr) {
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
                    self.metadata_tick();
                    self.pex_tick();
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

    /// Drops every partially downloaded piece.
    pub(super) fn clear_bufs(&mut self) {
        if let Some(l) = self.loaded.as_mut() {
            l.bufs.clear();
        }
    }

    pub(super) fn emit(&self, ev: Event) {
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
    /// through the check that follows a magnet's metadata.
    pub(super) fn running(&self) -> bool {
        self.connectable() || (self.metadata_check && !self.paused)
    }

    pub(super) fn state(&self) -> TorrentState {
        match &self.phase {
            Phase::Error(_) => TorrentState::Error,
            Phase::Metadata if self.paused => TorrentState::Paused,
            Phase::Metadata => TorrentState::DownloadingMetadata,
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
                {
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
    /// can dial (port 0, unspecified, multicast) are dropped.
    pub(super) fn on_found_peers(&mut self, source: PeerSource, peers: Vec<SocketAddr>) {
        let mut count = 0usize;
        for addr in peers {
            if !dialable(&addr) {
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

    pub(super) fn start_check(&mut self) {
        // A check still running belongs to an older generation: stop it
        // instead of letting checks pile up.
        self.cancel_check();
        let (Some(storage), Some(l)) = (self.storage.as_ref(), self.loaded.as_mut()) else {
            return;
        };
        let storage = Arc::clone(storage);
        l.picker = None;
        let meta = Arc::clone(&l.meta);
        let layout = l.layout;
        let done = Arc::new(AtomicU32::new(0));
        let cancel = Arc::new(AtomicBool::new(false));
        self.phase = Phase::Checking {
            done: Arc::clone(&done),
            cancel: Arc::clone(&cancel),
        };
        let generation = self.generation;
        self.jobs.spawn_blocking(move || {
            let result = check_pieces(&*storage, &layout, &meta.info.pieces, |i| {
                done.store(i.saturating_add(1), Ordering::Relaxed);
                if cancel.load(Ordering::Relaxed) {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            });
            Job::Checked { generation, result }
        });
    }

    /// Stops the running check, if any, at its next piece.
    fn cancel_check(&self) {
        if let Phase::Checking { cancel, .. } = &self.phase {
            cancel.store(true, Ordering::Relaxed);
        }
    }

    pub(super) fn on_checked(&mut self, generation: u64, result: io::Result<Bitfield>) {
        if generation != self.generation {
            return;
        }
        match result {
            Ok(have) => {
                let Some(l) = self.loaded.as_mut() else {
                    return;
                };
                let mut picker = PiecePicker::new(l.layout, &have);
                picker.set_piece_priorities(&l.piece_prio);
                picker.set_sequential(l.sequential);
                // Peers still connected (a magnet keeps those it fetched
                // the metadata through) count toward availability.
                let generation = self.generation;
                for p in self.peers.values() {
                    if p.tx.is_some() && p.generation == generation {
                        picker.add_peer_pieces(p.pieces());
                    }
                }
                // Finishing during this session is what `TorrentFinished` reports.
                self.finished_emitted = picker.is_finished();
                l.picker = Some(picker);
                self.phase = Phase::Ready;
                self.metadata_check = false;
                self.refresh_state();
                self.catch_up_peers();
                self.requeue_known();
                self.try_connect();
            }
            Err(e) => self.fail(&e, "checking files"),
        }
    }

    // ----- errors -------------------------------------------------------------

    /// A storage error: the torrent stops with `TorrentState::Error`.
    pub(super) fn fail(&mut self, e: &io::Error, during: &str) {
        if matches!(self.phase, Phase::Error(_)) {
            return;
        }
        warn!(torrent = %self.name(), error = %e, "storage error while {during}");
        let message = format!("{during}: {e}");
        self.phase = Phase::Error(message.clone());
        self.emit(Event::FileError {
            path: None,
            torrent: self.ih,
            message,
        });
        self.close_all();
        self.clear_bufs();
        self.refresh_state();
    }

    /// The metadata is unusable (it matches the info hash but does not
    /// parse): the torrent stops with `TorrentState::Error`. A resume
    /// fetches it again.
    pub(super) fn metadata_failed(&mut self, message: String) {
        warn!(torrent = %self.name(), %message, "metadata failed");
        self.phase = Phase::Error(message);
        self.close_all();
        self.refresh_state();
    }

    pub(super) fn on_job_result(&mut self, r: Result<Job, JoinError>) {
        match r {
            Ok(job) => self.on_job(job),
            Err(e) => warn!(error = %e, "torrent background job failed"),
        }
    }

    // ----- metadata -------------------------------------------------------------

    /// A complete metadata buffer was hashed and parsed off the actor.
    pub(super) fn on_metadata_job(
        &mut self,
        key: PeerKey,
        ban: Option<BanTarget>,
        result: Result<Box<Metainfo>, MetainfoError>,
    ) {
        if self.loaded.is_some() || !matches!(self.phase, Phase::Metadata) {
            return;
        }
        match result {
            Ok(meta) => self.install_metadata(*meta),
            Err(MetainfoError::InfoHashMismatch) => {
                let sends = self
                    .magnet
                    .as_mut()
                    .map(|m| m.fetch.on_verify_failed(key))
                    .unwrap_or_default();
                if self.peers.contains_key(&key) {
                    self.ban(key);
                } else if let Some(t) = ban {
                    self.apply_ban(t);
                }
                self.send_metadata(sends);
            }
            Err(e) => self.metadata_failed(format!("invalid metadata: {e}")),
        }
    }

    /// The verified metadata: build `Loaded` and the storage, tell the
    /// handle and every peer, then check the existing data as for a
    /// `.torrent`.
    fn install_metadata(&mut self, meta: Metainfo) {
        let Some(m) = self.magnet.take() else {
            return;
        };
        let m = *m;
        let file_prio = selected_priorities(&meta, m.magnet.select_only.as_deref());
        let storage: Arc<dyn Storage> = match m.storage {
            Some(s) => s,
            None => Arc::new(FsStorage::new(m.save_path, &meta)),
        };
        let meta = Arc::new(meta);
        let loaded = Loaded::new(Arc::clone(&meta), file_prio, m.sequential);
        let layout = loaded.layout;
        let total_wanted = loaded.total_wanted;
        self.install(loaded, storage);
        let _ = self.metainfo_cell.set(meta);
        // Whoever reacts to the event sees `has_metadata` already.
        self.publish();
        self.emit(Event::MetadataReceived { torrent: self.ih });
        // Peers were set up with no pieces; they get the layout now.
        let n = layout.num_pieces();
        if self.is_private() {
            // The magnet exception ends here: off the DHT and PEX at once.
            self.dht_announcer = None;
            self.dht_rx = None;
            self.drop_pex_peers();
        }
        let ext = self.ext_config();
        let keys: Vec<PeerKey> = self.peers.keys().copied().collect();
        for key in keys {
            if let Some(p) = self.peers.get_mut(&key) {
                p.bits = Bitfield::new(n);
                p.fast_has = None;
                if p.have_sent.is_some() {
                    p.have_sent = Some(Bitfield::new(n));
                }
            }
            self.send(key, PeerCommand::MetadataReady { layout, ext });
        }
        // Until the check is done, `stopped` (if any) reports everything
        // as left.
        self.signal(|s| {
            s.left = total_wanted;
            true
        });
        self.metadata_check = true;
        self.start_check();
        self.refresh_state();
    }

    /// Advances the metadata fetch's timers.
    fn metadata_tick(&mut self) {
        let now = Instant::now();
        let sends = match self.magnet.as_mut() {
            Some(m) if matches!(self.phase, Phase::Metadata) => m.fetch.on_tick(now),
            _ => return,
        };
        self.send_metadata(sends);
        self.close_useless_metadata_peer(now);
    }

    // ----- commands -----------------------------------------------------------

    async fn on_cmd(&mut self, cmd: Cmd) -> Flow {
        match cmd {
            Cmd::Pause(tx) => {
                if !self.paused {
                    self.paused = true;
                    self.metadata_check = false;
                    self.close_all();
                    self.refresh_state();
                }
                let _ = tx.send(());
            }
            Cmd::Resume(tx) => {
                self.resume();
                let _ = tx.send(());
            }
            Cmd::Recheck(tx) => {
                self.recheck();
                let _ = tx.send(());
            }
            Cmd::Reannounce(tx) => {
                self.signal(|s| {
                    s.reannounces = s.reannounces.wrapping_add(1);
                    true
                });
                let _ = tx.send(());
            }
            Cmd::SetPriorities(p, tx) => {
                self.set_priorities(p);
                let _ = tx.send(());
            }
            Cmd::AddPeer(addr, tx) => {
                if !self.retries.is_never(&addr) && self.remember(addr) {
                    self.queue_addr(addr);
                    self.try_connect();
                }
                let _ = tx.send(());
            }
            Cmd::Pieces(tx) => {
                let bf = self.loaded.as_ref().map_or_else(
                    || Bitfield::new(0),
                    |l| {
                        l.picker.as_ref().map_or_else(
                            || Bitfield::new(l.layout.num_pieces()),
                            |p| p.have().clone(),
                        )
                    },
                );
                let _ = tx.send(bf);
            }
            Cmd::Incoming {
                stream,
                handshake,
                addr,
            } => self.on_incoming(*stream, handshake, addr),
            Cmd::Remove(mode, tx) => {
                let r = self.stop(Some(mode)).await;
                // Done here rather than by the caller, whose future may be
                // dropped before this point.
                if let Some(session) = self.session.upgrade()
                    && session.unregister(&self.ih)
                {
                    self.emit(Event::TorrentRemoved { torrent: self.ih });
                }
                let _ = tx.send(r);
                return Flow::Exit;
            }
            Cmd::Shutdown(tx) => {
                let _ = self.stop(None).await;
                let _ = tx.send(());
                return Flow::Exit;
            }
        }
        Flow::Continue
    }

    pub(super) fn resume(&mut self) {
        let was_error = matches!(self.phase, Phase::Error(_));
        self.paused = false;
        if was_error && self.loaded.is_none() {
            // Bad metadata: fetch it again from scratch.
            let max = self.shared.settings.max_metadata_size;
            if let Some(m) = self.magnet.as_mut() {
                m.fetch = m.new_fetch(max);
            }
            self.phase = Phase::Metadata;
        } else if was_error {
            // Whatever was in flight when the error hit is unknown: forget
            // the picker and partial pieces and trust only the disk.
            self.remember_left();
            self.generation = self.generation.wrapping_add(1);
            self.clear_bufs();
            self.start_check();
        }
        self.refresh_state();
        self.requeue_known();
        self.try_connect();
    }

    /// Tells the announcer how much is left, before a check drops the
    /// picker: it reports this on `stopped`. Without a picker the figure is
    /// meaningless and the earlier one stays.
    fn remember_left(&self) {
        if self.loaded.as_ref().is_none_or(|l| l.picker.is_none()) {
            return;
        }
        let st = self.snapshot();
        let left = st.total_wanted.saturating_sub(st.wanted_done);
        self.signal(|s| {
            s.left = left;
            true
        });
    }

    pub(super) fn recheck(&mut self) {
        if self.loaded.is_none() {
            return;
        }
        self.remember_left();
        self.generation = self.generation.wrapping_add(1);
        self.close_all();
        self.clear_bufs();
        self.start_check();
        self.refresh_state();
    }

    pub(super) fn set_priorities(&mut self, p: Vec<Priority>) {
        let Some(l) = self.loaded.as_mut() else {
            return;
        };
        l.file_prio = p;
        l.piece_prio = piece_priorities(&l.meta, &l.file_prio);
        l.total_wanted = wanted_bytes(&l.layout, &l.piece_prio);
        if let Some(picker) = l.picker.as_mut() {
            picker.set_piece_priorities(&l.piece_prio);
            // Partial pieces no longer wanted give back their buffers.
            let skipped: Vec<u32> = picker
                .partial_pieces()
                .filter(|&i| l.piece_prio.get(i as usize).is_some_and(|&p| p == 0))
                .collect();
            for piece in skipped {
                picker.piece_failed(piece);
                l.bufs.remove(&piece);
            }
            if !picker.is_finished() {
                self.finished_emitted = false;
            }
        }
        let keys: Vec<PeerKey> = self.peers.keys().copied().collect();
        for key in keys {
            self.update_interest(key);
        }
        self.top_up_all(None);
        self.refresh_state();
    }

    /// Closes every peer, waits for running jobs, flushes and optionally
    /// deletes the data. The actor exits afterwards.
    /// On shutdown (`remove` is `None`) it also waits up to
    /// [`STOPPED_TIMEOUT`] for the `stopped` announce; on remove that
    /// announce finishes on its own.
    pub(super) async fn stop(&mut self, remove: Option<RemoveMode>) -> Result<(), SessionError> {
        let announce_deadline = Instant::now() + STOPPED_TIMEOUT;
        self.signal(|s| {
            s.exit = true;
            true
        });
        self.dht_announcer = None;
        self.dht_rx = None;
        self.close_all();
        // Its result would be thrown away; do not wait for a whole check.
        self.cancel_check();
        let deadline = Instant::now() + PEER_DRAIN_TIMEOUT;
        // Only connected peers read their commands; one still connecting
        // or handshaking ends on its own timeout, its slots released below.
        while self.peers.values().any(|p| p.connected) {
            match tokio::time::timeout_at(deadline, self.peer_rx.recv()).await {
                Ok(Some(ev @ PeerEvent::Disconnected { .. })) => self.on_peer_event(ev),
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => break,
            }
        }
        // Peers that did not report back still hold session slots.
        for (_, p) in self.peers.drain() {
            if p.connecting {
                release(&self.shared.half_open);
            }
            release(&self.shared.global_peers);
        }
        // Pending writes finish before files are flushed or deleted.
        while self.jobs.join_next().await.is_some() {}
        let delete = remove == Some(RemoveMode::DeleteData);
        let r = match self.storage.clone() {
            Some(storage) => tokio::task::spawn_blocking(move || {
                if delete {
                    storage.delete_files()
                } else {
                    storage.flush()
                }
            })
            .await
            .unwrap_or_else(|e| Err(io::Error::other(e))),
            None => Ok(()),
        };
        if let Some(a) = self.announcer.take()
            && remove.is_none()
        {
            let mut task = a.task;
            if tokio::time::timeout_at(announce_deadline, &mut task)
                .await
                .is_err()
            {
                task.abort();
            }
        }
        match r {
            Ok(()) => Ok(()),
            Err(e) => {
                warn!(torrent = %self.name(), error = %e, "flush or delete failed");
                if delete {
                    Err(SessionError::Storage(e))
                } else {
                    Ok(())
                }
            }
        }
    }
}

/// The next message of `rx`; pending forever without one.
async fn recv_opt<T>(rx: &mut Option<mpsc::Receiver<T>>) -> Option<T> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// File priorities from a magnet's `so` selection: unselected files are
/// skipped; no selection means every file at `Normal`.
fn selected_priorities(
    meta: &Metainfo,
    select: Option<&[std::ops::RangeInclusive<usize>]>,
) -> Vec<Priority> {
    let Some(select) = select else {
        return Vec::new();
    };
    (0..meta.info.files.len())
        .map(|i| {
            if select.iter().any(|r| r.contains(&i)) {
                Priority::Normal
            } else {
                Priority::Skip
            }
        })
        .collect()
}

/// Bytes of the pieces with a non-zero priority.
fn wanted_bytes(layout: &PieceLayout, prio: &[u8]) -> u64 {
    (0..layout.num_pieces())
        .filter(|&i| prio.get(i as usize).is_some_and(|&p| p > 0))
        .map(|i| u64::from(layout.piece_size(i)))
        .sum()
}
