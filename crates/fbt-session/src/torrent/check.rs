//! The piece check, one of the session's `max_active_checks` at a time.

use std::collections::HashMap;
use std::io;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use fbt_core::Bitfield;

use super::actor::{Actor, Phase};
use super::data::Job;
use super::peer::Peer;
use crate::picker::{PeerKey, PiecePicker};
use crate::resume::stamps_match;
use crate::storage::check_pieces;
use tokio::sync::OwnedSemaphorePermit;
use tracing::debug;

impl Actor {
    /// Starts checking the data: at once when one of the session's
    /// `max_active_checks` slots is free, else as
    /// [`Phase::QueuedForCheck`] until one is.
    pub(super) fn start_check(&mut self) {
        // A check still running or waiting belongs to an older generation:
        // stop it instead of letting checks pile up.
        self.cancel_check();
        let Some(l) = self.loaded.as_mut() else {
            return;
        };
        if self.storage.is_none() {
            return;
        }
        if let Some(picker) = l.picker.take() {
            // Resume data saved during the check reports these.
            self.last_have = Some(picker.have().clone());
        }
        let num_pieces = l.layout.num_pieces();
        self.lost_files.clear();
        self.lost_pieces.clear();
        self.check_seq = self.check_seq.wrapping_add(1);
        if matches!(self.phase, Phase::Resuming) {
            // A check asked for while the stamps were compared: it runs in
            // full, and the comparison's answer is stale.
            self.drop_resume();
        }
        if self
            .resume
            .as_ref()
            .is_some_and(|r| r.have.len() == num_pieces)
        {
            self.compare_stamps();
            return;
        }
        // A magnet's resume data saved before its metadata has no pieces:
        // nothing was trusted or doubted, so `resume_trusted` stays `None`.
        self.resume = None;
        let seq = self.check_seq;
        let checks = self.shared.disk.check_permit();
        // A free slot goes to the queue's head first: this only succeeds
        // with nobody waiting.
        match Arc::clone(&checks).try_acquire_owned() {
            Ok(permit) => self.run_check(Some(permit)),
            Err(_) => {
                let cancel = self.jobs.spawn(async move {
                    // The semaphore is never closed.
                    let permit = checks.acquire_owned().await.ok();
                    Job::CheckSlot { seq, permit }
                });
                self.phase = Phase::QueuedForCheck { cancel };
            }
        }
    }

    /// Resume data a full check replaces: what the check finds is not
    /// what was saved.
    fn drop_resume(&mut self) {
        if self.resume.take().is_some() {
            self.resume_trusted = Some(false);
        }
    }

    /// The fast path of a torrent added from resume data: one disk-pool
    /// job reads the storage's stamps and compares them with the saved
    /// ones. Trusted, the storage learns the file priorities (the part
    /// file) in the same job, before anything reads the data, and the
    /// saved pieces stand as the check's result; otherwise a full check
    /// follows. No check slot is taken: no piece is read.
    fn compare_stamps(&mut self) {
        let (Some(storage), Some(l), Some(r)) = (
            self.storage.as_ref(),
            self.loaded.as_ref(),
            self.resume.as_ref(),
        ) else {
            return;
        };
        let storage = Arc::clone(storage);
        let meta = Arc::clone(&l.meta);
        let wanted = Arc::clone(&l.wanted);
        let have = r.have.clone();
        let saved = r.stamps.clone();
        let generation = self.generation;
        self.phase = Phase::Resuming;
        self.shared.disk.spawn(&mut self.jobs, move || {
            let trusted = match storage.stamps() {
                Ok(None) => true,
                Ok(Some(now)) => stamps_match(&meta, &have, &saved, &now),
                Err(e) => {
                    debug!(error = %e, "cannot read the files' stamps; checking");
                    false
                }
            };
            // A failure here is the full check's to report.
            let trusted = trusted && wanted.apply(&*storage).is_ok();
            Job::ResumeChecked {
                generation,
                trusted: trusted.then_some(have),
            }
        });
    }

    /// The stamp comparison returned: the saved pieces are the check's
    /// result (`Some`), or a full check runs.
    pub(super) fn on_resume_checked(&mut self, generation: u64, trusted: Option<Bitfield>) {
        if generation != self.generation || !matches!(self.phase, Phase::Resuming) {
            return;
        }
        self.resume = None;
        self.resume_trusted = Some(trusted.is_some());
        match trusted {
            Some(have) => {
                debug!(torrent = %self.name(), "resume data trusted; no check");
                self.on_checked(generation, Ok(have));
            }
            None => {
                debug!(torrent = %self.name(), "files changed since the resume data; checking");
                self.need_save_resume = true;
                self.start_check();
                self.refresh_state();
            }
        }
    }

    /// A check slot came free for the check `seq`: it runs now, unless it
    /// was cancelled or replaced meanwhile (the slot is then given back).
    pub(super) fn on_check_slot(&mut self, seq: u64, permit: Option<OwnedSemaphorePermit>) {
        if seq != self.check_seq || !matches!(self.phase, Phase::QueuedForCheck { .. }) {
            return;
        }
        self.run_check(permit);
        self.refresh_state();
    }

    /// Runs `check_pieces` on its own blocking thread (outside the disk
    /// threads, so it never starves reads and writes), holding `permit`
    /// (the check slot) until it returns.
    fn run_check(&mut self, permit: Option<OwnedSemaphorePermit>) {
        let (Some(storage), Some(l)) = (self.storage.as_ref(), self.loaded.as_ref()) else {
            return;
        };
        let storage = Arc::clone(storage);
        let meta = Arc::clone(&l.meta);
        let wanted = Arc::clone(&l.wanted);
        let layout = l.layout;
        let done = Arc::new(AtomicU32::new(0));
        let cancel = Arc::new(AtomicBool::new(false));
        self.phase = Phase::Checking {
            done: Arc::clone(&done),
            cancel: Arc::clone(&cancel),
        };
        let generation = self.generation;
        self.shared.disk.spawn_check(&mut self.jobs, move || {
            let _slot = permit;
            // The storage learns which files are skipped before it is read
            // (the part file).
            if let Err(e) = wanted.apply(&*storage) {
                return Job::Checked {
                    generation,
                    result: Err(e),
                };
            }
            let result = check_pieces(&*storage, &layout, &meta, |i| {
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

    /// Stops the running check, if any, at its next piece; a check still
    /// waiting for its slot leaves the queue at once.
    pub(super) fn cancel_check(&self) {
        match &self.phase {
            Phase::Checking { cancel, .. } => cancel.store(true, Ordering::Relaxed),
            Phase::QueuedForCheck { cancel } => cancel.abort(),
            _ => {}
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
                self.last_have = None;
                let mut picker = PiecePicker::new(l.layout, &have);
                picker.set_piece_priorities(&l.piece_prio);
                picker.set_sequential(l.sequential);
                // Peers still connected (a magnet keeps those it fetched
                // the metadata through) count toward availability.
                add_availability(&mut picker, &self.peers, self.generation);
                // Finishing during this session is what `TorrentFinished`
                // reports; a finish under way before the check (its final
                // flush not back yet) completes after it.
                let finishing = std::mem::take(&mut self.finish_pending)
                    | std::mem::take(&mut self.finish_flush);
                self.finished_emitted = picker.is_finished();
                self.finish_pending = finishing && picker.is_finished();
                l.picker = Some(picker);
                // Web seeds not retired are `HaveAll` peers of it.
                self.count_web_seeds();
                self.phase = Phase::Ready;
                self.metadata_check = false;
                self.maybe_finish();
                // Windows hold missing pieces only now; reads of pieces
                // the check found are answered.
                self.sync_all_readers();
                self.serve_waiting_reads();
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
