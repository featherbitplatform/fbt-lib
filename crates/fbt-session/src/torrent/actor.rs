//! The torrent actor: one task owning a torrent's picker, peers, buffers and
//! status. It never blocks: disk and hashing run in `spawn_blocking` and
//! come back as [`Job`]s, and commands to peers use `try_send` (a peer whose
//! queue is full is closed as stalled).

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use fbt_core::{Bitfield, InfoHashes, PieceLayout};
use fbt_metainfo::Metainfo;
use rand::rngs::StdRng;
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinError, JoinSet};
use tokio::time::{Instant, MissedTickBehavior};
use tracing::warn;

use super::data::Job;
use super::peers::Peer;
use super::{Cmd, piece_priorities, same_state};
use crate::error::SessionError;
use crate::event::Event;
use crate::peer::messages::PeerEvent;
use crate::picker::{PeerKey, PiecePicker};
use crate::rate::RateMeter;
use crate::session::{AddTorrent, Priority, RemoveMode, Shared, release};
use crate::status::{TorrentState, TorrentStatus};
use crate::storage::{Storage, check_pieces};

/// Capacity of the channel carrying every peer's events into the actor.
const PEER_EVENT_QUEUE: usize = 1024;
/// How long remove and shutdown wait for peer tasks to report back.
const PEER_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Where the torrent is in its life.
#[derive(Debug)]
pub(super) enum Phase {
    /// `check_pieces` is running; it counts checked pieces into `done`.
    Checking { done: Arc<AtomicU32> },
    /// Checked; downloading or seeding (unless paused).
    Ready,
    /// Stopped by a storage error.
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

#[derive(Debug)]
pub(crate) struct Actor {
    pub(super) ih: InfoHashes,
    pub(super) meta: Arc<Metainfo>,
    pub(super) layout: PieceLayout,
    pub(super) storage: Arc<dyn Storage>,
    pub(super) shared: Arc<Shared>,
    pub(super) cmds: mpsc::Receiver<Cmd>,
    pub(super) peer_tx: mpsc::Sender<PeerEvent>,
    pub(super) peer_rx: mpsc::Receiver<PeerEvent>,
    pub(super) status_tx: watch::Sender<TorrentStatus>,
    pub(super) jobs: JoinSet<Job>,
    /// Bumped on recheck; jobs and peers of older generations are stale.
    pub(super) generation: u64,
    pub(super) phase: Phase,
    pub(super) paused: bool,
    pub(super) picker: Option<PiecePicker>,
    pub(super) piece_prio: Vec<u8>,
    /// Bytes of pieces with a non-zero priority; follows `piece_prio`.
    pub(super) total_wanted: u64,
    pub(super) file_prio: Vec<Priority>,
    pub(super) sequential: bool,
    pub(super) peers: HashMap<PeerKey, Peer>,
    pub(super) next_key: u64,
    /// Addresses waiting for a connection slot.
    pub(super) candidates: VecDeque<SocketAddr>,
    /// Every address given to `add_peer`, reconnected on resume.
    pub(super) known: HashSet<SocketAddr>,
    /// Partially downloaded pieces, by index.
    pub(super) bufs: HashMap<u32, Vec<u8>>,
    pub(super) counters: Counters,
    pub(super) last_state: TorrentState,
    /// `TorrentFinished` was emitted for the current set of wanted pieces.
    pub(super) finished_emitted: bool,
    pub(super) rng: StdRng,
}

impl Actor {
    pub(super) fn new(
        params: AddTorrent,
        storage: Arc<dyn Storage>,
        shared: Arc<Shared>,
        cmds: mpsc::Receiver<Cmd>,
        status_tx: watch::Sender<TorrentStatus>,
    ) -> Self {
        let meta = Arc::new(params.metainfo);
        let (peer_tx, peer_rx) = mpsc::channel(PEER_EVENT_QUEUE);
        let piece_prio = piece_priorities(&meta, &params.file_priorities);
        let total_wanted = wanted_bytes(&meta.piece_layout(), &piece_prio);
        Actor {
            ih: meta.info_hashes,
            layout: meta.piece_layout(),
            meta,
            storage,
            shared,
            cmds,
            peer_tx,
            peer_rx,
            status_tx,
            jobs: JoinSet::new(),
            generation: 0,
            phase: Phase::Checking {
                done: Arc::new(AtomicU32::new(0)),
            },
            paused: params.paused,
            picker: None,
            total_wanted,
            piece_prio,
            file_prio: params.file_priorities,
            sequential: params.sequential,
            peers: HashMap::new(),
            next_key: 0,
            candidates: VecDeque::new(),
            known: HashSet::new(),
            bufs: HashMap::new(),
            counters: Counters::default(),
            last_state: TorrentState::CheckingFiles { progress: 0.0 },
            finished_emitted: false,
            rng: rand::make_rng(),
        }
    }

    pub(super) async fn run(mut self) {
        self.start_check();
        self.emit(Event::StateChanged {
            torrent: self.ih,
            state: self.last_state,
        });
        let mut tick = tokio::time::interval(self.shared.settings.status_interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
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
                Some(r) = self.jobs.join_next(), if !self.jobs.is_empty() => self.on_job_result(r),
                _ = tick.tick() => {
                    self.try_connect();
                    self.publish();
                }
            }
        }
    }

    // ----- status and state -------------------------------------------------

    pub(super) fn emit(&self, ev: Event) {
        self.shared.emit(ev);
    }

    /// Ready and not paused: peers may connect and data may flow.
    pub(super) fn active(&self) -> bool {
        matches!(self.phase, Phase::Ready) && !self.paused
    }

    pub(super) fn state(&self) -> TorrentState {
        match &self.phase {
            Phase::Error(_) => TorrentState::Error,
            Phase::Checking { done } => {
                let n = self.layout.num_pieces();
                let progress = if n == 0 {
                    1.0
                } else {
                    f64::from(done.load(Ordering::Relaxed).min(n)) / f64::from(n)
                };
                TorrentState::CheckingFiles { progress }
            }
            Phase::Ready if self.paused => TorrentState::Paused,
            Phase::Ready => {
                if self.picker.as_ref().is_some_and(PiecePicker::is_finished) {
                    TorrentState::Seeding
                } else {
                    TorrentState::Downloading
                }
            }
        }
    }

    pub(super) fn snapshot(&self) -> TorrentStatus {
        let total_wanted = self.total_wanted;
        let wanted_done = self
            .picker
            .as_ref()
            .map_or(0, |p| total_wanted.saturating_sub(p.wanted_left_bytes()));
        let progress = if total_wanted == 0 {
            if self.picker.is_some() { 1.0 } else { 0.0 }
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
            name: self.meta.info.name.clone(),
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

    /// Publishes the status and emits `StateChanged` if the state changed.
    /// Publishes the status and emits `StateChanged` only if the state
    /// changed; other changes wait for the next status tick.
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
    }

    // ----- checking -----------------------------------------------------------

    pub(super) fn start_check(&mut self) {
        let done = Arc::new(AtomicU32::new(0));
        self.phase = Phase::Checking {
            done: Arc::clone(&done),
        };
        self.picker = None;
        let storage = Arc::clone(&self.storage);
        let meta = Arc::clone(&self.meta);
        let layout = self.layout;
        let generation = self.generation;
        self.jobs.spawn_blocking(move || {
            let result = check_pieces(&*storage, &layout, &meta.info.pieces, |i| {
                done.store(i.saturating_add(1), Ordering::Relaxed);
            });
            Job::Checked { generation, result }
        });
    }

    pub(super) fn on_checked(&mut self, generation: u64, result: io::Result<Bitfield>) {
        if generation != self.generation {
            return;
        }
        match result {
            Ok(have) => {
                let mut picker = PiecePicker::new(self.layout, &have);
                picker.set_piece_priorities(&self.piece_prio);
                picker.set_sequential(self.sequential);
                // Finishing during this session is what `TorrentFinished` reports.
                self.finished_emitted = picker.is_finished();
                self.picker = Some(picker);
                self.phase = Phase::Ready;
                self.refresh_state();
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
        warn!(torrent = %self.meta.info.name, error = %e, "storage error while {during}");
        let message = format!("{during}: {e}");
        self.phase = Phase::Error(message.clone());
        self.emit(Event::FileError {
            path: None,
            torrent: self.ih,
            message,
        });
        self.close_all();
        self.bufs.clear();
        self.refresh_state();
    }

    pub(super) fn on_job_result(&mut self, r: Result<Job, JoinError>) {
        match r {
            Ok(job) => self.on_job(job),
            Err(e) => warn!(error = %e, "torrent background job failed"),
        }
    }

    // ----- commands -----------------------------------------------------------

    async fn on_cmd(&mut self, cmd: Cmd) -> Flow {
        match cmd {
            Cmd::Pause(tx) => {
                if !self.paused {
                    self.paused = true;
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
            // Task 15: announcing.
            Cmd::Reannounce(tx) => {
                let _ = tx.send(());
            }
            Cmd::SetPriorities(p, tx) => {
                self.set_priorities(p);
                let _ = tx.send(());
            }
            Cmd::AddPeer(addr, tx) => {
                self.known.insert(addr);
                let busy = self
                    .peers
                    .values()
                    .any(|p| p.addr == addr && p.tx.is_some());
                if !busy && !self.candidates.contains(&addr) {
                    self.candidates.push_back(addr);
                }
                self.try_connect();
                let _ = tx.send(());
            }
            Cmd::Pieces(tx) => {
                let bf = self.picker.as_ref().map_or_else(
                    || Bitfield::new(self.layout.num_pieces()),
                    |p| p.have().clone(),
                );
                let _ = tx.send(bf);
            }
            Cmd::Incoming {
                stream,
                handshake,
                addr,
            } => self.on_incoming(stream, handshake, addr),
            Cmd::Remove(mode, tx) => {
                let r = self.stop(Some(mode)).await;
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
        if was_error {
            // Whatever was in flight when the error hit is unknown: forget
            // the picker and partial pieces and trust only the disk.
            self.generation = self.generation.wrapping_add(1);
            self.bufs.clear();
            self.start_check();
        }
        self.refresh_state();
        self.requeue_known();
        self.try_connect();
    }

    pub(super) fn recheck(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.close_all();
        self.bufs.clear();
        self.start_check();
        self.refresh_state();
    }

    pub(super) fn set_priorities(&mut self, p: Vec<Priority>) {
        self.file_prio = p;
        self.piece_prio = piece_priorities(&self.meta, &self.file_prio);
        self.total_wanted = wanted_bytes(&self.layout, &self.piece_prio);
        if let Some(picker) = self.picker.as_mut() {
            picker.set_piece_priorities(&self.piece_prio);
            // Partial pieces no longer wanted give back their buffers.
            let skipped: Vec<u32> = picker
                .partial_pieces()
                .filter(|&i| self.piece_prio.get(i as usize).is_some_and(|&p| p == 0))
                .collect();
            for piece in skipped {
                picker.piece_failed(piece);
                self.bufs.remove(&piece);
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
    pub(super) async fn stop(&mut self, remove: Option<RemoveMode>) -> Result<(), SessionError> {
        self.close_all();
        let deadline = Instant::now() + PEER_DRAIN_TIMEOUT;
        while !self.peers.is_empty() {
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
        let storage = Arc::clone(&self.storage);
        let delete = remove == Some(RemoveMode::DeleteData);
        let r = tokio::task::spawn_blocking(move || {
            if delete {
                storage.delete_files()
            } else {
                storage.flush()
            }
        })
        .await
        .unwrap_or_else(|e| Err(io::Error::other(e)));
        match r {
            Ok(()) => Ok(()),
            Err(e) => {
                warn!(torrent = %self.meta.info.name, error = %e, "flush or delete failed");
                if delete {
                    Err(SessionError::Storage(e))
                } else {
                    Ok(())
                }
            }
        }
    }
}

/// Bytes of the pieces with a non-zero priority.
fn wanted_bytes(layout: &PieceLayout, prio: &[u8]) -> u64 {
    (0..layout.num_pieces())
        .filter(|&i| prio.get(i as usize).is_some_and(|&p| p > 0))
        .map(|i| u64::from(layout.piece_size(i)))
        .sum()
}
