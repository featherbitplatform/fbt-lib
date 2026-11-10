//! Serving blocks to peers: each request is validated, read in
//! `spawn_blocking` and sent back with `SendBlock`.

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
        let (Some(storage), Some(l)) = (self.storage.as_ref(), self.loaded.as_ref()) else {
            return;
        };
        let storage = Arc::clone(storage);
        let offset = l.layout.piece_offset(block.piece) + u64::from(block.offset);
        let generation = self.generation;
        self.jobs.spawn_blocking(move || {
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
            Ok(buf) => {
                let len = buf.len() as u64;
                let data = Bytes::from(buf);
                // Choked since the request: the peer task dropped it already.
                if self.peers.get(&key).is_none_or(|p| p.am_choking) {
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
            Err(e) => self.fail(&e, "reading a block"),
        }
    }
}
