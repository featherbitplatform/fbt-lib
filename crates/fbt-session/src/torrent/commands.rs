//! Commands the torrent actor accepts, and the actions behind them.

use std::io;

use fbt_core::Bitfield;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::warn;

use super::actor::{Actor, Flow, PEER_DRAIN_TIMEOUT, Phase, wanted_bytes};
use super::announcer::STOPPED_TIMEOUT;
use super::data::Job;
use super::{Cmd, piece_priorities};
use crate::error::SessionError;
use crate::event::{Event, PeerSource};
use crate::peer::messages::PeerEvent;
use crate::picker::PeerKey;
use crate::session::{Priority, RemoveMode, release};
use crate::storage::cache::FlushReason;

impl Actor {
    pub(super) async fn on_cmd(&mut self, cmd: Cmd) -> Flow {
        match cmd {
            Cmd::Pause(tx) => {
                if !self.paused {
                    self.paused = true;
                    self.metadata_check = false;
                    self.close_all();
                    self.refresh_state();
                }
                // Every verified piece is on disk before the reply.
                self.flush_and_wait().await;
                let _ = tx.send(());
            }
            Cmd::Resume(tx) => {
                self.resume();
                let _ = tx.send(());
            }
            Cmd::Recheck(tx) => {
                // The check reads the disk: everything verified is there first.
                self.flush_and_wait().await;
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
            Cmd::Scrape(tx) => {
                self.signal(|s| {
                    s.scrapes = s.scrapes.wrapping_add(1);
                    true
                });
                let _ = tx.send(());
            }
            Cmd::SetPriorities(p, tx) => {
                self.set_priorities(p);
                let _ = tx.send(());
            }
            Cmd::AddPeer(addr, tx) => {
                if !self.refuse_blocked(addr)
                    && !self.retries.is_never(&addr)
                    && self.remember(addr)
                {
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
            Cmd::SetRateLimit(limit, tx) => {
                self.rate.set(limit);
                let _ = tx.send(());
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
            Cmd::FoundPeers(source, peers) => {
                // LSD must never feed a private torrent (a magnet is public
                // until its metadata says otherwise).
                if !(source == PeerSource::Lsd && self.is_private()) {
                    self.on_found_peers(source, peers);
                }
            }
            Cmd::FilterChanged(tx) => {
                self.on_filter_changed();
                let _ = tx.send(());
            }
            Cmd::BansChanged => {
                self.close_blocked();
            }
            Cmd::PeerTrust(addr, tx) => {
                let s = self.peer_block_source(addr);
                let _ = tx.send(self.smartban.trust_of(s));
            }
            Cmd::Shutdown(tx) => {
                let _ = self.stop(None).await;
                let _ = tx.send(());
                return Flow::Exit;
            }
            Cmd::OpenReader { file, link, tx } => self.on_open_reader(file, link, tx),
            Cmd::ReaderMoved(id) => self.on_reader_moved(id),
            Cmd::CloseReader(id) => self.on_close_reader(id),
            Cmd::Read {
                reader,
                offset,
                len,
                tx,
            } => self.on_read(reader, offset, len, tx),
            Cmd::DebugDeadlines(tx) => {
                let _ = tx.send(self.debug_deadlines());
            }
        }
        Flow::Continue
    }

    pub(super) fn resume(&mut self) {
        let was_error = matches!(self.phase, Phase::Error(_));
        self.paused = false;
        if matches!(self.phase, Phase::Ready) {
            // Kept while paused; due relative to now again.
            self.sync_all_readers();
        }
        if was_error && self.loaded.is_none() {
            // Bad metadata: fetch it again from scratch.
            let max = self.shared.settings.max_metadata_size;
            if let Some(m) = self.magnet.as_mut() {
                m.fetch = m.new_fetch(max);
            }
            self.phase = Phase::Metadata;
        } else if was_error {
            // Whatever was in flight when the error hit is unknown: forget
            // the picker and partial pieces and trust only the disk. Peers
            // of the old generation go too.
            self.remember_left();
            self.generation = self.generation.wrapping_add(1);
            self.close_all();
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
    pub(super) fn remember_left(&self) {
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
            // Partial pieces no longer wanted give back their buffers,
            // unless a reader's window holds them.
            let skipped: Vec<u32> = picker
                .partial_pieces()
                .filter(|&i| {
                    l.piece_prio.get(i as usize).is_some_and(|&p| p == 0)
                        && !self.deadlines.contains(i)
                })
                .collect();
            for piece in skipped {
                picker.piece_failed(piece);
                l.bufs.remove(&piece);
                self.cache.release_partial(piece);
                self.smartban.forget(piece);
            }
            if !picker.is_finished() {
                self.finished_emitted = false;
            } else if !self.finished_emitted && matches!(self.phase, Phase::Ready) {
                // Skipping the rest finished it with no piece passing: the
                // same path as the last piece, so it seeds and reports
                // `TorrentFinished` once everything is on disk.
                self.finished_emitted = true;
                self.finish_pending = true;
            }
        }
        if self.finish_pending {
            self.flush(FlushReason::All);
            self.maybe_finish();
        } else if self.finished_emitted && !self.cache.is_idle() {
            // Still finished, and a dirty skipped piece may be wanted now:
            // it goes to disk at once (`state` counts a finished torrent's
            // dirty pieces as skipped ones).
            self.flush(FlushReason::All);
        }
        let keys: Vec<PeerKey> = self.peers.keys().copied().collect();
        for key in keys {
            self.update_interest(key);
        }
        self.top_up_all(None);
        self.refresh_state();
    }

    /// Closes every peer, writes every verified piece (or, deleting,
    /// discards them), waits for running jobs, flushes and optionally
    /// deletes the data. The actor exits afterwards.
    /// On shutdown (`remove` is `None`) it also waits up to
    /// [`STOPPED_TIMEOUT`] for the `stopped` announce; on remove that
    /// announce finishes on its own. The timeout never cuts the writes
    /// short: data comes first.
    pub(super) async fn stop(&mut self, remove: Option<RemoveMode>) -> Result<(), SessionError> {
        let announce_deadline = Instant::now() + STOPPED_TIMEOUT;
        // Only a write that fails from here on is this stop's error.
        self.write_error = None;
        self.signal(|s| {
            s.exit = true;
            true
        });
        self.dht_announcer = None;
        self.dht_rx = None;
        self.close_all();
        // Its result would be thrown away; do not wait for a whole check
        // (one still queued leaves the queue).
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
        let delete = remove == Some(RemoveMode::DeleteData);
        if delete {
            // Nothing more reaches the files; hashes and reads still running
            // are stale.
            self.cache.discard_all();
            self.drop_piece_reads();
            self.generation = self.generation.wrapping_add(1);
        } else {
            self.flush(FlushReason::All);
        }
        // Pending jobs finish before files are flushed or deleted. A hash
        // that passes meanwhile is written too; reads and checks are moot.
        while let Some(r) = self.jobs.join_next().await {
            if let Ok(job @ (Job::Hashed { .. } | Job::WroteRun { .. })) = r {
                self.on_job(job);
            }
            if !delete {
                self.flush(FlushReason::All);
            }
        }
        // Whole-piece reads answered no one: their reservations go back.
        self.drop_piece_reads();
        let r = match self.storage.clone() {
            Some(storage) => {
                let mut job = JoinSet::new();
                self.shared.disk.spawn(&mut job, move || {
                    if delete {
                        storage.delete_files()
                    } else {
                        storage.flush()
                    }
                });
                match job.join_next().await {
                    Some(r) => r.unwrap_or_else(|e| Err(io::Error::other(e))),
                    None => Ok(()),
                }
            }
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
        if let Some(e) = self.write_error.take() {
            // Verified pieces were lost (`FileError` was emitted).
            return Err(SessionError::Storage(e));
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

/// File priorities for `files` files from a magnet's `so` selection:
/// unselected files are skipped; no selection means every file at `Normal`.
/// The ranges are sorted first, so this is O(files + ranges log ranges).
pub(super) fn selected_priorities(
    files: usize,
    select: Option<&[std::ops::RangeInclusive<usize>]>,
) -> Vec<Priority> {
    let Some(select) = select else {
        return Vec::new();
    };
    let mut ranges: Vec<(usize, usize)> = select.iter().map(|r| (*r.start(), *r.end())).collect();
    ranges.sort_unstable();
    let mut out = vec![Priority::Skip; files];
    // Every file below `next` is already settled.
    let mut next = 0usize;
    for (start, end) in ranges {
        let start = start.max(next);
        let end = end.min(files.saturating_sub(1));
        if start >= files {
            break;
        }
        if start > end {
            continue;
        }
        if let Some(slice) = out.get_mut(start..=end) {
            slice.fill(Priority::Normal);
        }
        next = end.saturating_add(1);
    }
    out
}
