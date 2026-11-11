//! Downloaded data: blocks are buffered per piece in memory, a complete
//! piece is hashed in `spawn_blocking`, and a verified piece counts as ours
//! at once: it is held dirty in the write-back cache (served from there)
//! and written later through the session's disk pool, in contiguous runs.

use std::io;
use std::sync::Arc;

use bytes::Bytes;
use fbt_core::{Bitfield, BlockRef};
use fbt_metainfo::{Metainfo, MetainfoError};
use tokio::time::Instant;
use tracing::debug;

use super::actor::{Actor, Phase};
use super::swarm::BanTarget;
use crate::event::Event;
use crate::peer::messages::PeerCommand;
use crate::picker::PeerKey;
use crate::storage::cache::FlushReason;
use crate::storage::sha1_piece;

/// Result of a blocking job, fed back into the actor loop.
#[derive(Debug)]
pub(crate) enum Job {
    /// A check slot is free for the check `seq` (`None` only if the
    /// semaphore was closed, which it never is).
    CheckSlot {
        seq: u64,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    },
    /// `check_pieces` finished.
    Checked {
        generation: u64,
        result: io::Result<Bitfield>,
    },
    /// A downloaded piece was hashed; `data` comes back for the write.
    Hashed {
        generation: u64,
        piece: u32,
        ok: bool,
        data: Vec<u8>,
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
}

impl Actor {
    pub(super) fn on_job(&mut self, job: Job) {
        match job {
            Job::CheckSlot { seq, permit } => self.on_check_slot(seq, permit),
            Job::Checked { generation, result } => self.on_checked(generation, result),
            Job::Hashed {
                generation,
                piece,
                ok,
                data,
            } => {
                self.hashing = self.hashing.saturating_sub(1);
                self.on_hashed(generation, piece, ok, data);
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
            } => self.on_read_piece(generation, piece, result),
            Job::Read {
                generation,
                key,
                block,
                result,
            } => self.on_read(generation, key, block, result),
            Job::Flushed { generation, result } => {
                // A check since then starts its own finish.
                if generation == self.generation {
                    self.on_flushed(result);
                }
            }
            Job::Metadata { key, ban, result } => self.on_metadata_job(key, ban, result),
        }
    }

    /// A block we requested arrived from `key`.
    pub(super) fn on_block(&mut self, key: PeerKey, block: BlockRef, data: Bytes) {
        let len = data.len() as u64;
        let generation = self.generation;
        let Some(p) = self.peers.get_mut(&key) else {
            self.counters.wasted = self.counters.wasted.saturating_add(len);
            return;
        };
        p.requested.remove(&block);
        let current = p.generation == generation && p.tx.is_some();
        let ready = matches!(self.phase, Phase::Ready);
        let Some(l) = self.loaded.as_mut() else {
            self.counters.wasted = self.counters.wasted.saturating_add(len);
            return;
        };
        let picker = match l.picker.as_mut() {
            Some(pk) if current && ready => pk,
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
        self.hashing = self.hashing.saturating_add(1);
        self.jobs.spawn_blocking(move || {
            let ok = meta.info.pieces.get(piece as usize) == Some(&sha1_piece(&data));
            Job::Hashed {
                generation,
                piece,
                ok,
                data,
            }
        });
    }

    fn on_hashed(&mut self, generation: u64, piece: u32, ok: bool, data: Vec<u8>) {
        if generation != self.generation || !matches!(self.phase, Phase::Ready) {
            return;
        }
        let Some(l) = self.loaded.as_mut() else {
            return;
        };
        if !ok {
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
            self.top_up_all(None);
            return;
        }
        let Some(picker) = l.picker.as_mut() else {
            return;
        };
        // Verified: ours from now on, served from memory until written.
        let on_floor = self.cache.holds_floor(piece);
        self.cache
            .insert_dirty(piece, data.into(), Instant::now().into_std());
        picker.piece_passed(piece);
        self.lost_pieces.remove(&piece);
        let finished = picker.is_finished();
        self.emit(Event::PieceFinished {
            torrent: self.ih,
            piece,
        });
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
        if just_finished || !self.active() {
            // Finished, or paused while the hash ran: nothing more is coming
            // soon, so nothing waits in memory.
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
        self.maybe_finish();
        self.refresh_state();
        // A landed write may make room for new pieces.
        self.top_up_all(None);
    }

    /// Once a finished torrent's pieces are all written, the storage is
    /// flushed; `TorrentFinished` follows the flush.
    pub(super) fn maybe_finish(&mut self) {
        if !self.finish_pending || !self.cache.is_idle() {
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
        // Seeding now (unless paused).
        self.refresh_state();
        if !finished {
            return;
        }
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
mod tests {
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{OnceLock, Weak};

    use fbt_core::PeerId;
    use fbt_tracker::TrackerClient;
    use tokio::sync::{broadcast, mpsc, watch};

    use super::*;
    use crate::session::{AddTorrent, Shared};
    use crate::settings::Settings;
    use crate::status::{TorrentState, TorrentStatus};
    use crate::storage::{MemStorage, Storage};
    use crate::torrent::actor::EMITTED;
    use crate::torrent::swarm::Peer;

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
        let settings = Settings {
            dht_enabled: false,
            dht_bootstrap_nodes: Vec::new(),
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
            cache: Arc::new(crate::storage::cache::CacheBudget::new(0)),
            bandwidth: Arc::new(crate::bandwidth::Bandwidth::new(&Settings::default())),
        });
        let storage: Arc<dyn Storage> = Arc::new(MemStorage::new(LEN as u64));
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
        actor.on_checked(actor.generation, Ok(Bitfield::new(1)));

        let key = PeerKey(0);
        let (tx, _peer_rx) = mpsc::channel(16);
        let mut peer = Peer::new(
            SocketAddr::from(([10, 0, 0, 1], 6881)),
            tx,
            actor.generation,
            1,
            crate::peer::transport::Transport::Tcp,
        );
        peer.connected = true;
        peer.seed = true;
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
}
