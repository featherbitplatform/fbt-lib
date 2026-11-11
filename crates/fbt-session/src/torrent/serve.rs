//! Serving blocks to peers: each request is validated, then served from
//! the write-back cache (a verified piece not written yet, or a clean
//! entry) or read through the session's disk pool, and sent back with
//! `SendBlock`. A miss reads the whole piece and keeps it clean when the
//! budget allows.

use std::io;
use std::sync::Arc;

use bytes::Bytes;
use fbt_core::{BLOCK_SIZE, BlockRef};
use tracing::debug;

use super::actor::{Actor, Phase};
use super::data::Job;
use crate::peer::messages::PeerCommand;
use crate::picker::PeerKey;

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
        if let Some((_, waiting)) = self.piece_reads.get_mut(&block.piece) {
            waiting.push((key, block));
            return;
        }
        let (Some(storage), Some(l)) = (self.storage.as_ref(), self.loaded.as_ref()) else {
            return;
        };
        let storage = Arc::clone(storage);
        let generation = self.generation;
        let size = l.layout.piece_size(block.piece);
        if size <= WHOLE_PIECE_READ_MAX && self.cache.can_hold_clean(u64::from(size)) {
            let offset = l.layout.piece_offset(block.piece);
            let piece = block.piece;
            self.piece_reads
                .insert(piece, (generation, vec![(key, block)]));
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
        // every generation change).
        if self
            .piece_reads
            .get(&piece)
            .is_none_or(|(g, _)| *g != generation)
        {
            return;
        }
        let (_, waiting) = self.piece_reads.remove(&piece).unwrap_or_default();
        if generation != self.generation || !matches!(self.phase, Phase::Ready) {
            return;
        }
        let data: Arc<[u8]> = match result {
            Ok(buf) => buf.into(),
            Err(e) => {
                self.fail(&e, "reading a block");
                return;
            }
        };
        self.cache.insert_clean(
            piece,
            Arc::clone(&data),
            tokio::time::Instant::now().into_std(),
        );
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

/// The bytes of `block` within its piece's `data`.
fn block_of(data: &[u8], block: BlockRef) -> Option<Bytes> {
    let start = block.offset as usize;
    let end = start.checked_add(block.len as usize)?;
    data.get(start..end).map(Bytes::copy_from_slice)
}
