//! Downloaded data: blocks are buffered per piece in memory, a complete
//! piece is hashed and then written in `spawn_blocking`, and only a written
//! piece counts as ours.

use std::io;
use std::sync::Arc;

use bytes::Bytes;
use fbt_core::{Bitfield, BlockRef};
use tracing::debug;

use super::actor::{Actor, Phase};
use crate::event::Event;
use crate::peer::messages::PeerCommand;
use crate::picker::PeerKey;
use crate::storage::sha1_piece;

/// Result of a blocking job, fed back into the actor loop.
#[derive(Debug)]
pub(crate) enum Job {
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
    /// A verified piece was written.
    Written {
        generation: u64,
        piece: u32,
        result: io::Result<()>,
    },
    /// Storage was flushed.
    Flushed(io::Result<()>),
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
            Job::Checked { generation, result } => self.on_checked(generation, result),
            Job::Hashed {
                generation,
                piece,
                ok,
                data,
            } => self.on_hashed(generation, piece, ok, data),
            Job::Written {
                generation,
                piece,
                result,
            } => self.on_written(generation, piece, result),
            Job::Read {
                generation,
                key,
                block,
                result,
            } => self.on_read(generation, key, block, result),
            Job::Flushed(Err(e)) => {
                tracing::warn!(torrent = %self.meta.info.name, error = %e, "flush failed");
            }
            Job::Flushed(Ok(())) => {}
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
        let picker = match self.picker.as_mut() {
            Some(pk) if current && matches!(self.phase, Phase::Ready) => pk,
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
        let size = self.layout.piece_size(block.piece) as usize;
        let buf = self
            .bufs
            .entry(block.piece)
            .or_insert_with(|| vec![0; size]);
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
            && let Some(data) = self.bufs.remove(&block.piece)
        {
            self.spawn_hash(block.piece, data);
        }
    }

    fn spawn_hash(&mut self, piece: u32, data: Vec<u8>) {
        let meta = Arc::clone(&self.meta);
        let generation = self.generation;
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
        if ok {
            let storage = Arc::clone(&self.storage);
            let offset = self.layout.piece_offset(piece);
            self.jobs.spawn_blocking(move || Job::Written {
                generation,
                piece,
                result: storage.write(offset, &data),
            });
            return;
        }
        debug!(torrent = %self.meta.info.name, piece, "piece failed its hash check");
        if let Some(picker) = self.picker.as_mut() {
            picker.piece_failed(piece);
        }
        self.counters.wasted = self
            .counters
            .wasted
            .saturating_add(u64::from(self.layout.piece_size(piece)));
        self.emit(Event::HashFailed {
            torrent: self.ih,
            piece,
        });
        self.top_up_all(None);
    }

    fn on_written(&mut self, generation: u64, piece: u32, result: io::Result<()>) {
        if generation != self.generation || !matches!(self.phase, Phase::Ready) {
            return;
        }
        if let Err(e) = result {
            self.fail(&e, "writing a piece");
            return;
        }
        let Some(picker) = self.picker.as_mut() else {
            return;
        };
        picker.piece_passed(piece);
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
        if finished && !self.finished_emitted {
            self.finished_emitted = true;
            self.emit(Event::TorrentFinished { torrent: self.ih });
            let storage = Arc::clone(&self.storage);
            // Durability point; a failure here is logged, the next write
            // would surface a real storage problem.
            self.jobs
                .spawn_blocking(move || Job::Flushed(storage.flush()));
        }
        self.refresh_state();
    }
}
