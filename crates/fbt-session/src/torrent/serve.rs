//! Serving blocks to peers: each request is validated, then served from
//! the write-back cache (a verified piece not written yet, or a clean
//! entry) or read through the session's disk pool, and sent back with
//! `SendBlock`. A miss reads the whole piece and keeps it clean when the
//! budget allows.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use bytes::Bytes;
use fbt_core::{BLOCK_SIZE, BlockRef};
use tracing::debug;

use super::actor::{Actor, Phase};
use super::data::Job;
use crate::peer::messages::PeerCommand;
use crate::picker::PeerKey;
use crate::storage::cache::{CacheBudget, CacheKind};

impl Actor {
    /// A peer requests `block`: served only if we have the piece, the block
    /// is a valid layout block of at most 16 KiB and we do not choke it.
    pub(super) fn on_request(&mut self, key: PeerKey, block: BlockRef) {
        let Some(p) = self.peers.get(&key) else {
            return;
        };
        let valid = self.active()
            && p.tx.is_some()
            && !p.am_choking
            && self.storage.is_some()
            && block.len <= BLOCK_SIZE
            && self.loaded.as_ref().is_some_and(|l| {
                l.layout.is_valid_block(&block)
                    && l.picker
                        .as_ref()
                        .is_some_and(|pk| pk.have().get(block.piece))
            });
        if !valid {
            debug!(
                key = key.0,
                ?block,
                "rejecting invalid or unservable request"
            );
            self.send(key, PeerCommand::Reject(block));
            return;
        }
        if let Some(data) = self.cache.get(block.piece) {
            if let Some(bytes) = block_of(&data, block) {
                self.deliver(key, block, bytes);
            }
            return;
        }
        if let Some(read) = self.piece_reads.get_mut(&block.piece)
            && read.waiting.len() < MAX_PIECE_READ_WAITERS
        {
            read.waiting.push((key, block));
            return;
        }
        let (Some(storage), Some(l)) = (self.storage.as_ref(), self.loaded.as_ref()) else {
            return;
        };
        let storage = Arc::clone(storage);
        let generation = self.generation;
        let size = l.layout.piece_size(block.piece);
        // The whole piece is read when its bytes fit the budget now: they
        // are reserved before the read, so the buffer counts from the start.
        if size <= WHOLE_PIECE_READ_MAX
            && self.piece_reads.len() < MAX_PIECE_READS
            && !self.piece_reads.contains_key(&block.piece)
            && self.cache.reserve_clean(u64::from(size))
        {
            let offset = l.layout.piece_offset(block.piece);
            let piece = block.piece;
            self.piece_reads.insert(
                piece,
                PieceRead {
                    generation,
                    reserved: u64::from(size),
                    waiting: vec![(key, block)],
                },
            );
            self.shared.disk.spawn(&mut self.jobs, move || {
                let mut buf = vec![0u8; size as usize];
                let result = storage.read(offset, &mut buf).map(|()| buf);
                Job::ReadPiece {
                    generation,
                    piece,
                    result,
                }
            });
            return;
        }
        let offset = l.layout.piece_offset(block.piece) + u64::from(block.offset);
        self.shared.disk.spawn(&mut self.jobs, move || {
            let mut buf = vec![0u8; block.len as usize];
            let result = storage.read(offset, &mut buf).map(|()| buf);
            Job::Read {
                generation,
                key,
                block,
                result,
            }
        });
    }

    /// A whole piece was read: kept clean if the budget allows, and every
    /// request waiting on it is served.
    pub(super) fn on_read_piece(
        &mut self,
        generation: u64,
        piece: u32,
        result: io::Result<Vec<u8>>,
    ) {
        // Only the read the entry waits on answers it (the entry goes with
        // every generation change, its reservation given back).
        if self
            .piece_reads
            .get(&piece)
            .is_none_or(|r| r.generation != generation)
        {
            return;
        }
        let Some(read) = self.piece_reads.remove(&piece) else {
            return;
        };
        let data: Arc<[u8]> = match result {
            Ok(buf) if buf.len() as u64 == read.reserved => buf.into(),
            Ok(_) => {
                self.cache.release_clean(read.reserved);
                return;
            }
            Err(e) => {
                self.cache.release_clean(read.reserved);
                self.fail(&e, "reading a block");
                return;
            }
        };
        if generation != self.generation || !matches!(self.phase, Phase::Ready) {
            self.cache.release_clean(read.reserved);
            return;
        }
        self.cache.insert_reserved_clean(piece, Arc::clone(&data));
        let waiting = read.waiting;
        for (key, block) in waiting {
            if let Some(bytes) = block_of(&data, block) {
                self.deliver(key, block, bytes);
            }
        }
    }

    /// Sends a block to a peer that still wants it, counting the upload.
    fn deliver(&mut self, key: PeerKey, block: BlockRef, data: Bytes) {
        let len = data.len() as u64;
        // Choked since the request: the peer task dropped it already.
        if self
            .peers
            .get(&key)
            .is_none_or(|p| p.am_choking || p.tx.is_none())
        {
            return;
        }
        if self.send(key, PeerCommand::SendBlock { block, data }) {
            self.counters.uploaded = self.counters.uploaded.saturating_add(len);
            self.counters.ul.record(len);
            if let Some(p) = self.peers.get_mut(&key) {
                p.to_rate.record(len);
            }
        }
    }

    pub(super) fn on_read(
        &mut self,
        generation: u64,
        key: PeerKey,
        block: BlockRef,
        result: io::Result<Vec<u8>>,
    ) {
        if generation != self.generation || !matches!(self.phase, Phase::Ready) {
            return;
        }
        match result {
            Ok(buf) => self.deliver(key, block, Bytes::from(buf)),
            Err(e) => self.fail(&e, "reading a block"),
        }
    }
}

/// Pieces up to this size are read whole on a cache miss; larger ones only
/// the requested block.
const WHOLE_PIECE_READ_MAX: u32 = 4 * 1024 * 1024;
/// Whole-piece reads one torrent runs at once; further misses read only
/// their block.
pub(super) const MAX_PIECE_READS: usize = 4;
/// Requests waiting on one whole-piece read; further ones read their block.
pub(super) const MAX_PIECE_READ_WAITERS: usize = 256;

/// A whole-piece read in flight and the requests waiting on it.
#[derive(Debug)]
pub(super) struct PieceRead {
    /// The generation it was started in.
    pub generation: u64,
    /// Clean bytes reserved for the piece before the read.
    pub reserved: u64,
    pub waiting: Vec<(PeerKey, BlockRef)>,
}

/// The whole-piece reads of one torrent, by piece. Each holds a clean
/// reservation in the session budget, given back when the entry goes:
/// answered, cleared, or at the latest when the actor is dropped.
#[derive(Debug)]
pub(super) struct PieceReads {
    reads: HashMap<u32, PieceRead>,
    budget: Arc<CacheBudget>,
}

impl PieceReads {
    pub(super) fn new(budget: Arc<CacheBudget>) -> Self {
        PieceReads {
            reads: HashMap::new(),
            budget,
        }
    }

    pub(super) fn get(&self, piece: &u32) -> Option<&PieceRead> {
        self.reads.get(piece)
    }

    pub(super) fn get_mut(&mut self, piece: &u32) -> Option<&mut PieceRead> {
        self.reads.get_mut(piece)
    }

    pub(super) fn contains_key(&self, piece: &u32) -> bool {
        self.reads.contains_key(piece)
    }

    pub(super) fn len(&self) -> usize {
        self.reads.len()
    }

    pub(super) fn insert(&mut self, piece: u32, read: PieceRead) {
        if let Some(old) = self.reads.insert(piece, read) {
            self.budget.release(CacheKind::Clean, old.reserved);
        }
    }

    /// Takes the entry out; its reservation is the caller's now.
    pub(super) fn remove(&mut self, piece: &u32) -> Option<PieceRead> {
        self.reads.remove(piece)
    }

    /// Forgets every read, giving their reservations back; their results
    /// are then ignored.
    pub(super) fn clear(&mut self) {
        for (_, r) in self.reads.drain() {
            self.budget.release(CacheKind::Clean, r.reserved);
        }
    }
}

impl Drop for PieceReads {
    fn drop(&mut self) {
        self.clear();
    }
}

impl Actor {
    /// Forgets every whole-piece read in flight, giving their reservations
    /// back; their results are then ignored.
    pub(super) fn drop_piece_reads(&mut self) {
        self.piece_reads.clear();
    }
}

/// The bytes of `block` within its piece's `data`.
fn block_of(data: &[u8], block: BlockRef) -> Option<Bytes> {
    let start = block.offset as usize;
    let end = start.checked_add(block.len as usize)?;
    data.get(start..end).map(Bytes::copy_from_slice)
}
