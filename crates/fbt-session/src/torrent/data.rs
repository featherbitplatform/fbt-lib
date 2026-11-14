//! Downloaded data: blocks are buffered per piece in memory, a complete
//! piece is hashed in `spawn_blocking`, and a verified piece counts as ours
//! at once: it is held dirty in the write-back cache (served from there)
//! and written later through the session's disk pool, in contiguous runs.

use std::io;
use std::sync::Arc;

use bytes::Bytes;
use fbt_core::merkle::MerkleTree;
use fbt_core::{BLOCK_SIZE, Bitfield, BlockRef};
use fbt_metainfo::{Metainfo, MetainfoError};
use fbt_wire::HashRequest;
use tokio::time::Instant;
use tracing::debug;

use super::ReaderId;
use super::actor::{Actor, Phase};
use super::layers::ServeError;
use super::readers::Answer;
use super::smartban::BlockSource;
use super::swarm::BanTarget;
use super::webseed::FetchError;
use crate::event::Event;
use crate::peer::messages::PeerCommand;
use crate::picker::PeerKey;
use crate::storage::cache::FlushReason;
use crate::storage::{PieceCheck, sha1_piece, verify_piece};

/// Result of a blocking job, fed back into the actor loop.
#[derive(Debug)]
pub(crate) enum Job {
    /// A check slot is free for the check `seq` (`None` only if the
    /// semaphore was closed, which it never is).
    CheckSlot {
        seq: u64,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    },
    /// `check_pieces` finished: the pieces found, and the v2 subtree root
    /// of each passing piece (empty for v1 torrents).
    Checked {
        generation: u64,
        result: io::Result<(Bitfield, Vec<Option<[u8; 32]>>)>,
    },
    /// Resume data's stamps were compared: its pieces when trusted.
    ResumeChecked {
        generation: u64,
        trusted: Option<Bitfield>,
    },
    /// A downloaded piece was hashed; `data` comes back for the write.
    Hashed {
        generation: u64,
        piece: u32,
        ok: bool,
        /// The piece's v2 subtree root (v2 and hybrid torrents).
        v2_root: Option<[u8; 32]>,
        data: Vec<u8>,
        /// The SHA-1 of each block, for smart ban: computed when the piece
        /// fails, or passes while an earlier failure of it is on record.
        block_hashes: Option<Vec<[u8; 20]>>,
    },
    /// A run of `count` verified pieces from `first` was written.
    WroteRun {
        generation: u64,
        first: u32,
        count: u32,
        result: io::Result<()>,
    },
    /// Storage was flushed after a finished torrent's last write.
    Flushed {
        generation: u64,
        result: io::Result<()>,
    },
    /// A complete metadata buffer from `key` was verified and parsed.
    Metadata {
        key: PeerKey,
        /// Banned if the buffer fails the hash, even if the peer is gone.
        ban: Option<BanTarget>,
        result: Result<Box<Metainfo>, MetainfoError>,
    },
    /// A whole piece was read to serve the requests waiting on it (and to
    /// keep in the read cache).
    ReadPiece {
        generation: u64,
        piece: u32,
        result: io::Result<Vec<u8>>,
    },
    /// A block requested by a peer was read.
    Read {
        generation: u64,
        key: PeerKey,
        block: BlockRef,
        result: io::Result<Vec<u8>>,
    },
    /// A reader's chunk was read on its own (its piece is too large to
    /// read whole, or too many whole-piece reads run).
    ReadChunk {
        generation: u64,
        reader: ReaderId,
        offset: u64,
        len: u32,
        tx: Answer,
        result: io::Result<Vec<u8>>,
    },
    /// A peer's `hash request` was served from the layers.
    HashesServed {
        key: PeerKey,
        req: HashRequest,
        result: Result<Vec<u8>, ServeError>,
        /// The file's tree, when this job built it (for the cache).
        built: Option<Arc<MerkleTree>>,
    },
    /// The storage operation (move or rename) returned.
    StorageOp { result: io::Result<()> },
    /// A web seed's request for `blocks`, issued under `generation`,
    /// returned their data (one buffer per block) or failed.
    WebSeed {
        key: PeerKey,
        /// The seed's request id, to tell a superseded request.
        request: u64,
        generation: u64,
        blocks: Vec<BlockRef>,
        /// Whether the seed's redirects may land on internal addresses, as the run decided
        /// before it fetched (kept for its next run: see `WebSeed::allow_internal`).
        allow_internal: bool,
        result: Result<Vec<Bytes>, FetchError>,
    },
}

impl Actor {
    pub(super) fn on_job(&mut self, job: Job) {
        match job {
            Job::CheckSlot { seq, permit } => self.on_check_slot(seq, permit),
            Job::Checked { generation, result } => {
                if generation != self.generation {
                    return;
                }
                // What the check found may differ from any saved data.
                self.need_save_resume = true;
                let result = result.map(|(have, roots)| {
                    for (i, root) in (0u32..).zip(roots) {
                        self.on_piece_root(i, root);
                    }
                    have
                });
                self.on_checked(generation, result);
                let finished = self
                    .loaded
                    .as_ref()
                    .and_then(|l| l.picker.as_ref())
                    .is_some_and(crate::picker::PiecePicker::is_finished);
                self.swap_layers(finished);
            }
            Job::ResumeChecked {
                generation,
                trusted,
            } => self.on_resume_checked(generation, trusted),
            Job::HashesServed {
                key,
                req,
                result,
                built,
            } => self.on_hashes_served(key, req, result, built),
            Job::Hashed {
                generation,
                piece,
                ok,
                v2_root,
                data,
                block_hashes,
            } => {
                self.hashing = self.hashing.saturating_sub(1);
                let check = PieceCheck { ok, v2_root };
                self.on_hashed(generation, piece, check, data, block_hashes);
            }
            Job::WroteRun {
                generation,
                first,
                count,
                result,
            } => self.on_wrote_run(generation, first, count, result),
            Job::ReadPiece {
                generation,
                piece,
                result,
            } => {
                self.reads_in_flight = self.reads_in_flight.saturating_sub(1);
                self.on_read_piece(generation, piece, result);
            }
            Job::Read {
                generation,
                key,
                block,
                result,
            } => {
                self.reads_in_flight = self.reads_in_flight.saturating_sub(1);
                self.on_read_block(generation, key, block, result);
            }
            Job::ReadChunk {
                generation,
                reader,
                offset,
                len,
                tx,
                result,
            } => {
                self.reads_in_flight = self.reads_in_flight.saturating_sub(1);
                self.on_read_chunk(generation, (reader, offset, len, tx), result);
            }
            Job::StorageOp { result } => self.on_storage_done(result),
            Job::Flushed { generation, result } => {
                // A check since then starts its own finish.
                if generation == self.generation {
                    self.on_flushed(result);
                }
            }
            Job::Metadata { key, ban, result } => self.on_metadata_job(key, ban, result),
            Job::WebSeed {
                key,
                request,
                generation,
                blocks,
                allow_internal,
                result,
            } => self.on_web_seed_job(key, request, generation, blocks, allow_internal, result),
        }
        // Whatever returned may have been the last thing a storage
        // operation waited for.
        self.try_start_storage_job();
    }

    /// A block we requested arrived from peer `key`: accepted (see
    /// [`accept_block`](Self::accept_block)) only from a connection of the
    /// current generation that is still open.
    pub(super) fn on_block(&mut self, key: PeerKey, block: BlockRef, data: Bytes) {
        let len = data.len() as u64;
        let source = self.block_source(key);
        let generation = self.generation;
        let current = match self.peers.get_mut(&key) {
            Some(p) => {
                p.requested.remove(&block);
                p.generation == generation && p.tx.is_some()
            }
            None => false,
        };
        match source {
            Some(source) if current => self.accept_block(key, block, data, source, generation),
            _ => self.counters.wasted = self.counters.wasted.saturating_add(len),
        }
    }

    /// `block`, requested from `key` under `generation`, arrived from
    /// `source` (a peer or a web seed): kept if the picker accepts it
    /// (outstanding from `key`, not received before) while the torrent is
    /// ready and still in that generation, else counted as wasted. The last
    /// block of a piece sends it to its hash check.
    pub(super) fn accept_block(
        &mut self,
        key: PeerKey,
        block: BlockRef,
        data: Bytes,
        source: BlockSource,
        generation: u64,
    ) {
        let len = data.len() as u64;
        let current = generation == self.generation && matches!(self.phase, Phase::Ready);
        let Some(l) = self.loaded.as_mut() else {
            self.counters.wasted = self.counters.wasted.saturating_add(len);
            return;
        };
        let picker = match l.picker.as_mut() {
            Some(pk) if current => pk,
            _ => {
                self.counters.wasted = self.counters.wasted.saturating_add(len);
                return;
            }
        };
        let res = picker.on_block(key, block);
        if !res.accepted {
            self.counters.wasted = self.counters.wasted.saturating_add(len);
            return;
        }
        let size = l.layout.piece_size(block.piece) as usize;
        let buf = l.bufs.entry(block.piece).or_insert_with(|| vec![0; size]);
        let start = block.offset as usize;
        let Some(dst) = start
            .checked_add(data.len())
            .and_then(|end| buf.get_mut(start..end))
        else {
            // The picker only accepts exact layout blocks; unreachable.
            self.counters.wasted = self.counters.wasted.saturating_add(len);
            return;
        };
        dst.copy_from_slice(&data);
        self.smartban
            .record_block(block.piece, block.offset / BLOCK_SIZE, source);
        self.counters.downloaded = self.counters.downloaded.saturating_add(len);
        self.counters.dl.record(len);
        if let Some(p) = self.peers.get_mut(&key) {
            p.from_rate.record(len);
        }
        // End-game: the other holders of this block no longer need it.
        for owner in res.cancel {
            if let Some(o) = self.peers.get_mut(&owner) {
                o.requested.remove(&block);
            }
            self.send(owner, PeerCommand::Cancel(block));
        }
        if res.piece_complete
            && let Some(data) = self
                .loaded
                .as_mut()
                .and_then(|l| l.bufs.remove(&block.piece))
        {
            self.spawn_hash(block.piece, data);
        }
    }

    fn spawn_hash(&mut self, piece: u32, data: Vec<u8>) {
        let Some(l) = self.loaded.as_ref() else {
            return;
        };
        let meta = Arc::clone(&l.meta);
        let generation = self.generation;
        let wants_blocks = self.smartban.wants_block_hashes(piece);
        self.hashing = self.hashing.saturating_add(1);
        self.jobs.spawn_blocking(move || {
            let PieceCheck { ok, v2_root } = verify_piece(&meta, piece, &data);
            let block_hashes = (!ok || wants_blocks).then(|| {
                data.chunks(BLOCK_SIZE as usize)
                    .map(sha1_piece)
                    .collect::<Vec<_>>()
            });
            Job::Hashed {
                generation,
                piece,
                ok,
                v2_root,
                data,
                block_hashes,
            }
        });
    }

    fn on_hashed(
        &mut self,
        generation: u64,
        piece: u32,
        check: PieceCheck,
        data: Vec<u8>,
        block_hashes: Option<Vec<[u8; 20]>>,
    ) {
        if generation != self.generation || !matches!(self.phase, Phase::Ready) {
            return;
        }
        let Some(l) = self.loaded.as_mut() else {
            return;
        };
        if !check.ok {
            debug!(torrent = %l.meta.info.name, piece, "piece failed its hash check");
            if let Some(picker) = l.picker.as_mut() {
                picker.piece_failed(piece);
            }
            self.cache.release_partial(piece);
            let size = u64::from(l.layout.piece_size(piece));
            self.counters.wasted = self.counters.wasted.saturating_add(size);
            self.emit(Event::HashFailed {
                torrent: self.ih,
                piece,
            });
            let distrusted = self
                .smartban
                .piece_failed(piece, block_hashes.unwrap_or_default());
            for s in distrusted {
                self.smart_ban(s);
            }
            self.top_up_all(None);
            return;
        }
        if l.picker.is_none() {
            return;
        }
        // Verified: ours from now on, served from memory until written.
        // Waiting readers get it first, before any write.
        let data: Arc<[u8]> = data.into();
        self.answer_piece(piece, &data);
        self.deadlines.piece_done(piece);
        let on_floor = self.cache.holds_floor(piece);
        self.cache
            .insert_dirty(piece, data, Instant::now().into_std());
        let Some(picker) = self.loaded.as_mut().and_then(|l| l.picker.as_mut()) else {
            return;
        };
        picker.piece_passed(piece);
        self.lost_pieces.remove(&piece);
        let finished = picker.is_finished();
        self.on_piece_root(piece, check.v2_root);
        self.swap_layers(finished);
        self.emit(Event::PieceFinished {
            torrent: self.ih,
            piece,
        });
        // Whoever sent a block that differs from the good one when this
        // piece failed earlier is banned.
        for s in self.smartban.piece_passed(piece, block_hashes) {
            self.smart_ban(s);
        }
        let keys: Vec<PeerKey> = self.peers.keys().copied().collect();
        for key in keys {
            let Some(p) = self.peers.get(&key) else {
                continue;
            };
            if !p.connected || p.tx.is_none() {
                continue;
            }
            let recheck_interest = p.am_interested && p.has(piece);
            self.send(key, PeerCommand::Have(piece));
            if recheck_interest {
                self.update_interest(key);
            }
        }
        let just_finished = finished && !self.finished_emitted;
        if just_finished {
            self.finished_emitted = true;
            self.finish_pending = true;
        }
        if finished || !self.active() {
            // Finished (now, or earlier and this is a piece of a file
            // skipped while it was hashed), or paused while the hash ran:
            // nothing more is coming soon, so nothing waits in memory.
            self.flush(FlushReason::All);
        } else if on_floor || self.cache.over_budget() {
            // Running on the floor: written at once, so the floor comes
            // back after one write rather than at `cache_expiry`.
            self.flush(FlushReason::Floor);
        } else {
            self.flush(FlushReason::Pressure);
        }
        self.maybe_finish();
        self.refresh_state();
    }

    /// Hands the dirty pieces `reason` selects to writes on the disk pool.
    pub(super) fn flush(&mut self, reason: FlushReason) {
        // The files are moving: verified pieces stay dirty until they are
        // in place.
        if self.storage_busy {
            return;
        }
        let (Some(storage), Some(l)) = (self.storage.as_ref(), self.loaded.as_ref()) else {
            return;
        };
        let runs = self.cache.take_flush(reason, Instant::now().into_std());
        for run in runs {
            let storage = Arc::clone(storage);
            let offset = l.layout.piece_offset(run.first);
            let generation = self.generation;
            let first = run.first;
            let count = u32::try_from(run.pieces.len()).unwrap_or(u32::MAX);
            self.shared.disk.spawn(&mut self.jobs, move || {
                let bufs: Vec<&[u8]> = run.pieces.iter().map(|(_, d)| &d[..]).collect();
                let result = storage.write_vectored(offset, &bufs);
                Job::WroteRun {
                    generation,
                    first,
                    count,
                    result,
                }
            });
        }
    }

    fn on_wrote_run(&mut self, generation: u64, first: u32, count: u32, result: io::Result<()>) {
        // Discarded pieces the write held are given back whatever happened.
        self.cache.landed(first, count);
        if generation != self.generation {
            return;
        }
        if let Err(e) = result {
            self.cache.write_failed(first, count);
            // Drops every unwritten piece; the next resume rechecks.
            self.fail(&e, "writing pieces");
            self.write_error.get_or_insert(e);
            return;
        }
        self.cache.written(first, count);
        self.need_save_resume = true;
        self.maybe_finish();
        self.refresh_state();
        // A landed write may make room for new pieces.
        self.top_up_all(None);
    }

    /// Once a finished torrent's pieces are all written, the storage is
    /// flushed; `TorrentFinished` follows the flush.
    pub(super) fn maybe_finish(&mut self) {
        if !self.finish_pending || !self.cache.is_idle() || self.storage_busy {
            return;
        }
        self.finish_pending = false;
        self.finish_flush = true;
        let generation = self.generation;
        match self.storage.clone() {
            Some(storage) => self
                .shared
                .disk
                .spawn(&mut self.jobs, move || Job::Flushed {
                    generation,
                    result: storage.flush(),
                }),
            None => self.on_flushed(Ok(())),
        }
    }

    /// The flush after the last write returned: the torrent is complete on
    /// disk, so it is seeding and finished. A failing flush stops it.
    fn on_flushed(&mut self, result: io::Result<()>) {
        if !std::mem::take(&mut self.finish_flush) {
            return;
        }
        if let Err(e) = result {
            self.fail(&e, "flushing the finished torrent");
            return;
        }
        let finished = matches!(self.phase, Phase::Ready)
            && self
                .loaded
                .as_ref()
                .and_then(|l| l.picker.as_ref())
                .is_some_and(crate::picker::PiecePicker::is_finished);
        if finished && !self.cache.is_idle() {
            // A piece passed after this flush started (one of a file
            // skipped while it was hashed): the finish waits for its write
            // and a flush of its own, so `Seeding` still comes first.
            self.finish_pending = true;
            self.maybe_finish();
            self.refresh_state();
            return;
        }
        // Seeding now (unless paused).
        self.refresh_state();
        if !finished {
            return;
        }
        self.completed_time
            .get_or_insert_with(super::actor::unix_now);
        // Whoever reacts to the event reads the final counters and state.
        self.publish();
        self.emit(Event::TorrentFinished { torrent: self.ih });
        // After `refresh_state`, so the announce reads `left` = 0.
        self.signal(|s| !std::mem::replace(&mut s.completed, true));
    }

    /// Flushes every dirty piece and waits until the writes land, or until
    /// a failure drops them, together with any hash still running (its
    /// piece is then flushed too). Other job results are handled
    /// meanwhile, so nothing the writes depend on can stall.
    pub(super) async fn flush_and_wait(&mut self) {
        loop {
            self.flush(FlushReason::All);
            if self.hashing == 0 && !self.cache.writes_in_flight() {
                return;
            }
            match self.jobs.join_next().await {
                Some(r) => self.on_job_result(r),
                None => return,
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{OnceLock, Weak};

    use fbt_core::PeerId;
    use fbt_tracker::TrackerClient;
    use tokio::sync::{broadcast, mpsc, watch};

    use super::*;
    use crate::session::{AddTorrent, Priority, Shared};
    use crate::settings::Settings;
    use crate::status::{TorrentState, TorrentStatus};
    use crate::storage::{MemStorage, Storage};
    use crate::torrent::actor::EMITTED;
    use crate::torrent::peer::Peer;

    /// An actor for `meta` over `storage`, checked with nothing on disk,
    /// with a session cache of `cache_size` bytes whose dirty pieces expire
    /// after ten minutes.
    pub(in crate::torrent) fn test_actor(
        meta: Metainfo,
        storage: Arc<dyn Storage>,
        cache_size: u64,
    ) -> Actor {
        let pieces = meta.info.pieces.len();
        let settings = Settings {
            dht_enabled: false,
            dht_bootstrap_nodes: Vec::new(),
            cache_size,
            cache_expiry: std::time::Duration::from_secs(600),
            ..Settings::default()
        };
        let tracker = TrackerClient::new(settings.tracker.clone()).expect("tracker client");
        let shared = Arc::new(Shared {
            settings: Arc::new(settings),
            peer_id: PeerId::generate(),
            listen_port: 6881,
            listen_port_v4: Some(6881),
            listen_port_v6: None,
            tracker,
            events: broadcast::channel(64).0,
            shutdown: AtomicBool::new(false),
            global_peers: AtomicUsize::new(0),
            half_open: AtomicUsize::new(0),
            dht: None,
            dht_addrs: Vec::new(),
            utp: Vec::new(),
            disk: crate::disk::DiskPool::new(1, 1),
            cache: Arc::new(crate::storage::cache::CacheBudget::new(cache_size)),
            bandwidth: Arc::new(crate::bandwidth::Bandwidth::new(&Settings::default())),
            holepunch: Default::default(),
            lsd: crate::net::LsdHub::disabled(),
            portmap: None,
            external_ip: Arc::new(crate::net::ExternalIp::new(broadcast::channel(1).0)),
            ip_filter: Default::default(),
            bans: Default::default(),
            queue: Default::default(),
            queue_dirty: Default::default(),
        });
        let mut add = AddTorrent::new(meta, PathBuf::from("unused"));
        add.storage = Some(Arc::clone(&storage));
        let ih = add.source.info_hashes();
        let (status_tx, _status_rx) = watch::channel(TorrentStatus::initial(ih, "a".into(), true));
        let (_cmd_tx, cmd_rx) = mpsc::channel(1);
        let mut actor = Actor::new(
            add,
            Some(storage),
            Arc::new(OnceLock::new()),
            shared,
            Weak::new(),
            cmd_rx,
            status_tx,
        );
        let n = u32::try_from(pieces).expect("piece count");
        actor.on_checked(actor.generation, Ok(Bitfield::new(n)));
        actor
    }

    /// A connected seed of a torrent of `num_pieces`, and the receiver of
    /// the commands sent to it.
    fn seed_peer(actor: &Actor, num_pieces: u32) -> (Peer, mpsc::Receiver<PeerCommand>) {
        let (tx, rx) = mpsc::channel(64);
        let mut peer = Peer::new(
            SocketAddr::from(([10, 0, 0, 1], 6881)),
            tx,
            actor.generation,
            num_pieces,
            crate::peer::transport::Transport::Tcp,
            crate::event::PeerSource::Tracker,
        );
        peer.connected = true;
        peer.seed = true;
        (peer, rx)
    }

    /// Whoever reacts to `TorrentFinished` reads the final status: every
    /// downloaded byte counted and the torrent seeding, not the last tick's
    /// snapshot.
    #[tokio::test]
    async fn torrent_finished_sees_the_final_status() {
        const LEN: usize = 32_768;
        let payload: Vec<u8> = (0..LEN).map(|i| (i % 251) as u8).collect();
        let mut bytes =
            format!("d4:infod6:lengthi{LEN}e4:name1:a12:piece lengthi{LEN}e6:pieces20:")
                .into_bytes();
        bytes.extend_from_slice(&sha1_piece(&payload));
        bytes.extend_from_slice(b"ee");
        let meta = Metainfo::from_bytes(&bytes).expect("metainfo");
        let storage: Arc<dyn Storage> = Arc::new(MemStorage::new(LEN as u64));
        let mut actor = test_actor(meta, storage, 0);

        let key = PeerKey(0);
        let (mut peer, _peer_rx) = seed_peer(&actor, 1);
        let blocks = actor
            .loaded
            .as_mut()
            .and_then(|l| l.picker.as_mut())
            .expect("picker")
            .pick(key, peer.pieces(), 16, &mut actor.rng);
        peer.requested.extend(blocks.iter().copied());
        actor.peers.insert(key, peer);
        assert_eq!(blocks.len(), 2);
        for b in blocks {
            let start = b.offset as usize;
            let end = start + b.len as usize;
            actor.on_block(key, b, Bytes::copy_from_slice(&payload[start..end]));
        }
        // Hashed (which finishes), then WroteRun, then Flushed.
        while let Some(r) = actor.jobs.join_next().await {
            actor.on_job_result(r);
        }

        let log = EMITTED.take();
        let (_, seen) = log
            .iter()
            .find(|(e, _)| matches!(e, Event::TorrentFinished { .. }))
            .expect("TorrentFinished");
        assert_eq!(seen.total_downloaded, LEN as u64);
        assert_eq!(seen.total_wasted, 0);
        assert_eq!(seen.state, TorrentState::Seeding);
    }

    /// When the hash of a skipped file's piece returns, relative to the
    /// finish that skipping the file started.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum HashLands {
        /// Before the other piece's write lands.
        BeforeTheWrite,
        /// While the storage flush of the finish runs.
        DuringTheFlush,
    }

    /// Skipping the rest finishes the torrent; a piece of a skipped file
    /// that was being hashed then passes afterwards. It is written at once
    /// like every other piece of the finished torrent, not held in the
    /// cache until it expires, which would hold `Seeding` back as long;
    /// and `TorrentFinished` waits for it, still after `Seeding`.
    #[tokio::test]
    async fn piece_passing_after_skip_finished_before_the_write() {
        skip_while_hashing(HashLands::BeforeTheWrite).await;
    }

    #[tokio::test]
    async fn piece_passing_after_skip_finished_during_the_flush() {
        skip_while_hashing(HashLands::DuringTheFlush).await;
    }

    async fn skip_while_hashing(lands: HashLands) {
        const PIECE: usize = 16_384;
        let payload: Vec<u8> = (0..2 * PIECE).map(|i| (i % 251) as u8).collect();
        let mut bytes = format!(
            "d4:infod5:filesld6:lengthi{PIECE}e4:pathl1:aeed6:lengthi{PIECE}e4:pathl1:beee\
             4:name1:t12:piece lengthi{PIECE}e6:pieces40:"
        )
        .into_bytes();
        bytes.extend_from_slice(&sha1_piece(&payload[..PIECE]));
        bytes.extend_from_slice(&sha1_piece(&payload[PIECE..]));
        bytes.extend_from_slice(b"ee");
        let meta = Metainfo::from_bytes(&bytes).expect("metainfo");
        let storage: Arc<dyn Storage> = Arc::new(MemStorage::new(2 * PIECE as u64));
        // Room for both pieces: neither is written for cache pressure.
        let mut actor = test_actor(meta, storage, 64 << 20);
        let key = PeerKey(0);
        let (mut peer, _peer_rx) = seed_peer(&actor, 2);
        let mut blocks = actor
            .loaded
            .as_mut()
            .and_then(|l| l.picker.as_mut())
            .expect("picker")
            .pick(key, peer.pieces(), 16, &mut actor.rng);
        blocks.sort_unstable_by_key(|b| b.piece);
        assert_eq!(blocks.len(), 2);
        peer.requested.extend(blocks.iter().copied());
        actor.peers.insert(key, peer);
        let block = |b: BlockRef| {
            let start = b.piece as usize * PIECE + b.offset as usize;
            Bytes::copy_from_slice(&payload[start..start + b.len as usize])
        };
        // File a's piece passes and is held dirty.
        actor.on_block(key, blocks[0], block(blocks[0]));
        while let Some(r) = actor.jobs.join_next().await {
            actor.on_job_result(r);
        }
        assert!(!actor.cache.is_idle());
        // File b's piece arrives and goes to its hash; meanwhile b is
        // skipped, which finishes the torrent.
        actor.on_block(key, blocks[1], block(blocks[1]));
        actor.set_priorities(vec![Priority::Normal, Priority::Skip]);
        // The job results, reordered so the hash lands when `lands` says.
        // A held job is handled as soon as its condition clears. For
        // `DuringTheFlush` the flush's own result is also held until the
        // hash has been handled, so the hash always lands while the flush
        // runs, however the two jobs finish.
        let mut held: Vec<Job> = Vec::new();
        let mut hashed = false;
        let hold = |actor: &Actor, job: &Job, hashed: bool| match (lands, job) {
            (HashLands::BeforeTheWrite, Job::WroteRun { .. }) => actor.hashing > 0,
            (HashLands::DuringTheFlush, Job::Hashed { .. }) => !actor.finish_flush,
            (HashLands::DuringTheFlush, Job::Flushed { .. }) => !hashed,
            _ => false,
        };
        while let Some(r) = actor.jobs.join_next().await {
            let job = r.expect("job");
            if hold(&actor, &job, hashed) {
                held.push(job);
                continue;
            }
            hashed |= matches!(job, Job::Hashed { .. });
            actor.on_job(job);
            while let Some(i) = held.iter().position(|j| !hold(&actor, j, hashed)) {
                let job = held.remove(i);
                hashed |= matches!(job, Job::Hashed { .. });
                actor.on_job(job);
            }
        }
        assert!(held.is_empty(), "{held:?}");

        assert!(actor.cache.is_idle(), "a passed piece is still held dirty");
        let log = EMITTED.take();
        let (_, seen) = log
            .iter()
            .find(|(e, _)| matches!(e, Event::TorrentFinished { .. }))
            .expect("TorrentFinished");
        assert_eq!(seen.state, TorrentState::Seeding);
        assert_eq!(actor.state(), TorrentState::Seeding);
    }

    /// The liar of the parole tests.
    const LIAR: PeerKey = PeerKey(1);
    /// The honest seed of the parole tests.
    const HONEST: PeerKey = PeerKey(2);
    const LIAR_ADDR: ([u8; 4], u16) = ([10, 0, 0, 1], 6881);
    const HONEST_ADDR: ([u8; 4], u16) = ([10, 0, 0, 2], 6881);

    /// Two unchoking seeds of a single-file torrent of `pieces` pieces of
    /// `blocks` blocks: [`LIAR`] and [`HONEST`]. Returns the actor, the
    /// payload and the seeds' command receivers.
    fn liar_and_honest(
        pieces: usize,
        blocks: usize,
    ) -> (Actor, Vec<u8>, Vec<mpsc::Receiver<PeerCommand>>) {
        let piece_len = blocks * BLOCK_SIZE as usize;
        let len = pieces * piece_len;
        let payload: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let mut bytes = format!(
            "d4:infod6:lengthi{len}e4:name1:a12:piece lengthi{piece_len}e6:pieces{}:",
            20 * pieces
        )
        .into_bytes();
        for chunk in payload.chunks(piece_len) {
            bytes.extend_from_slice(&sha1_piece(chunk));
        }
        bytes.extend_from_slice(b"ee");
        let meta = Metainfo::from_bytes(&bytes).expect("metainfo");
        let storage: Arc<dyn Storage> = Arc::new(MemStorage::new(len as u64));
        let mut actor = test_actor(meta, storage, 64 << 20);
        let n = u32::try_from(pieces).expect("piece count");
        let mut rxs = Vec::new();
        for (key, addr) in [(LIAR, LIAR_ADDR), (HONEST, HONEST_ADDR)] {
            let (mut peer, rx) = seed_peer(&actor, n);
            peer.addr = SocketAddr::from(addr);
            peer.choked_us = false;
            rxs.push(rx);
            actor
                .loaded
                .as_mut()
                .and_then(|l| l.picker.as_mut())
                .expect("picker")
                .add_peer_pieces(peer.pieces());
            actor.peers.insert(key, peer);
        }
        EMITTED.take();
        (actor, payload, rxs)
    }

    /// Gives `key` (if still connected) a demand of `n` blocks.
    fn want_blocks(actor: &mut Actor, key: PeerKey, n: usize) {
        if let Some(p) = actor.peers.get_mut(&key) {
            p.demand = n;
        }
        actor.fill(key);
    }

    /// Both seeds answer every block requested from them, the liar with
    /// every byte flipped; then every hash job lands.
    async fn answer_all(
        actor: &mut Actor,
        payload: &[u8],
        rxs: &mut [mpsc::Receiver<PeerCommand>],
    ) {
        let piece_len = actor
            .loaded
            .as_ref()
            .map_or(0, |l| l.layout.piece_len() as usize);
        for key in [LIAR, HONEST] {
            let Some(p) = actor.peers.get(&key) else {
                continue;
            };
            let mut asked: Vec<BlockRef> = p.requested.iter().copied().collect();
            asked.sort_unstable();
            for blk in asked {
                let start = blk.piece as usize * piece_len + blk.offset as usize;
                let mut data = payload[start..start + blk.len as usize].to_vec();
                if key == LIAR {
                    data.iter_mut().for_each(|x| *x ^= 0xff);
                }
                actor.on_block(key, blk, Bytes::from(data));
            }
        }
        while let Some(r) = actor.jobs.join_next().await {
            actor.on_job_result(r);
        }
        for rx in rxs {
            while rx.try_recv().is_ok() {}
        }
    }

    fn all_passed(actor: &Actor) -> bool {
        actor
            .loaded
            .as_ref()
            .and_then(|l| l.picker.as_ref())
            .is_some_and(crate::picker::PiecePicker::is_finished)
    }

    /// Rounds of: the liar, then the honest seed, ask for `n` blocks each,
    /// and both answer; until every piece passed (at most 64 rounds).
    async fn rounds_until_done(
        actor: &mut Actor,
        payload: &[u8],
        rxs: &mut [mpsc::Receiver<PeerCommand>],
        n: usize,
    ) {
        for _ in 0..64 {
            if all_passed(actor) {
                return;
            }
            want_blocks(actor, LIAR, n);
            want_blocks(actor, HONEST, n);
            answer_all(actor, payload, rxs).await;
        }
    }

    /// The emitted events so far: the addresses banned and the number of
    /// hash failures.
    fn bans_and_failures() -> (Vec<SocketAddr>, usize) {
        let log = EMITTED.take();
        let banned = log
            .iter()
            .filter_map(|(e, _)| match e {
                Event::PeerBlocked { addr, .. } => Some(*addr),
                _ => None,
            })
            .collect();
        let failures = log
            .iter()
            .filter(|(e, _)| matches!(e, Event::HashFailed { .. }))
            .count();
        (banned, failures)
    }

    /// A liar (every block corrupt) and an honest seed share a piece:
    /// each round both ask for two of its four blocks, the liar first,
    /// and every requested block is answered at once. The first attempt
    /// mixes their blocks and fails, which puts both on parole; from then
    /// on each attempt is one peer's alone, so only the liar loses trust
    /// for its failures. The piece passes from the honest seed, the liar
    /// is banned and the honest seed never is. Without parole every
    /// attempt mixes their blocks, and the honest seed's trust falls with
    /// the liar's until both are banned together.
    #[tokio::test]
    async fn honest_co_contributor_of_a_failed_piece_is_not_banned() {
        let (mut actor, payload, mut rxs) = liar_and_honest(1, 4);
        rounds_until_done(&mut actor, &payload, &mut rxs, 2).await;
        let (banned, failures) = bans_and_failures();
        let honest = SocketAddr::from(HONEST_ADDR);
        assert!(!banned.contains(&honest), "B was banned: {banned:?}");
        assert_eq!(banned, vec![SocketAddr::from(LIAR_ADDR)], "A is banned");
        assert!(all_passed(&actor), "the piece passes ({failures} failures)");
        // B lost 2 for the mixed failure, got them back when the piece
        // passed, and 1 for the pass.
        let b_trust = actor.smartban.trust_of(actor.peer_block_source(honest));
        assert_eq!(b_trust, 1);
        // The first, mixed attempt and at least one of A's alone.
        assert!(failures >= 2, "{failures} failures");
    }

    /// Four pieces are in flight, each shared by the liar and the honest
    /// seed, when the first of them fails: the other three fail mixed too,
    /// after both are on parole. Only the first charges them, so the
    /// honest seed is never banned, the liar is, and every piece passes.
    /// Charging every mixed failure would take both to -8 together.
    #[tokio::test]
    async fn mixed_pieces_in_flight_do_not_ban_the_honest_seed() {
        let (mut actor, payload, mut rxs) = liar_and_honest(6, 2);
        // One block each, in turn: each new piece is shared.
        for _ in 0..4 {
            want_blocks(&mut actor, LIAR, 1);
            want_blocks(&mut actor, HONEST, 1);
        }
        let shared: std::collections::HashSet<u32> = [LIAR, HONEST]
            .iter()
            .flat_map(|k| actor.peers[k].requested.iter().map(|b| b.piece))
            .collect();
        assert_eq!(shared.len(), 4, "four pieces in flight: {shared:?}");
        answer_all(&mut actor, &payload, &mut rxs).await;
        let honest = SocketAddr::from(HONEST_ADDR);
        let b_trust = actor.smartban.trust_of(actor.peer_block_source(honest));
        assert_eq!(b_trust, -2, "only the first mixed failure charges B");
        let failed = EMITTED.with(|log| {
            log.borrow()
                .iter()
                .filter(|(e, _)| matches!(e, Event::HashFailed { .. }))
                .count()
        });
        assert_eq!(failed, 4);
        rounds_until_done(&mut actor, &payload, &mut rxs, 2).await;
        let (banned, failures) = bans_and_failures();
        assert!(!banned.contains(&honest), "B was banned: {banned:?}");
        assert_eq!(banned, vec![SocketAddr::from(LIAR_ADDR)], "A is banned");
        assert!(
            all_passed(&actor),
            "every piece passes ({failures} failures)"
        );
    }

    /// The honest seed, on parole, downloads the whole piece alone and
    /// leaves while it is being hashed. The piece is not reopened (the
    /// liar gets nothing of it), the hash result is applied as usual, and
    /// no buffer or budget reservation is left behind.
    #[tokio::test]
    async fn owner_leaving_while_its_piece_is_hashed_does_not_reopen_it() {
        let (mut actor, payload, mut rxs) = liar_and_honest(1, 4);
        // A mixed attempt fails: both are on parole.
        want_blocks(&mut actor, LIAR, 2);
        want_blocks(&mut actor, HONEST, 2);
        answer_all(&mut actor, &payload, &mut rxs).await;
        let (_, failures) = bans_and_failures();
        assert_eq!(failures, 1);
        // B takes the whole piece and sends it; the hash job is in flight.
        want_blocks(&mut actor, HONEST, 4);
        let mut asked: Vec<BlockRef> = actor.peers[&HONEST].requested.iter().copied().collect();
        asked.sort_unstable();
        assert_eq!(asked.len(), 4, "B alone has the piece");
        for blk in asked {
            let start = blk.offset as usize;
            let data = Bytes::copy_from_slice(&payload[start..start + blk.len as usize]);
            actor.on_block(HONEST, blk, data);
        }
        assert_eq!(actor.hashing, 1);
        actor.close_peer(HONEST);
        let picker = |actor: &Actor| {
            actor
                .loaded
                .as_ref()
                .and_then(|l| l.picker.as_ref())
                .map(|p| p.is_in_progress(0))
        };
        assert_eq!(picker(&actor), Some(true), "still pending its hash");
        want_blocks(&mut actor, LIAR, 4);
        assert!(
            actor.peers[&LIAR].requested.is_empty(),
            "the piece was reopened"
        );
        while let Some(r) = actor.jobs.join_next().await {
            actor.on_job_result(r);
        }
        let log = EMITTED.take();
        assert!(
            log.iter()
                .any(|(e, _)| matches!(e, Event::PieceFinished { piece: 0, .. })),
            "the hash result is applied"
        );
        assert!(all_passed(&actor));
        assert!(actor.loaded.as_ref().is_some_and(|l| l.bufs.is_empty()));
        assert!(!actor.cache.has_partial(0), "a reservation leaked");
    }
}
