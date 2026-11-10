//! The initial piece check.

use std::collections::HashMap;
use std::io;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use fbt_core::Bitfield;

use super::actor::{Actor, Phase};
use super::data::Job;
use super::swarm::Peer;
use crate::picker::{PeerKey, PiecePicker};
use crate::storage::check_pieces;

impl Actor {
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
    pub(super) fn cancel_check(&self) {
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
                add_availability(&mut picker, &self.peers, self.generation);
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
}

/// Adds the pieces of every peer of `generation` to `picker`'s
/// availability: the same peers whose pieces `Disconnected` removes again
/// (closing ones included), so the counts never drift.
pub(super) fn add_availability(
    picker: &mut PiecePicker,
    peers: &HashMap<PeerKey, Peer>,
    generation: u64,
) {
    for p in peers.values().filter(|p| p.generation == generation) {
        picker.add_peer_pieces(p.pieces());
    }
}
