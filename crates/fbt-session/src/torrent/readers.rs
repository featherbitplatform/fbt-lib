//! Open file readers, as the actor sees them: each reader's file and link,
//! the window of missing pieces it sets in the actor's `Deadlines`, and the
//! reads waiting for a piece.
//!
//! A read is served from the cache, read from disk (sharing the
//! whole-piece reads peers use), or waits for the download: the hash pass
//! answers it before the piece is written, and a check answers it when the
//! piece turns out to be on disk. While the torrent lives no answer is
//! dropped: reads fail only with the torrent's error, and the actor
//! exiting (remove, shutdown) closes them all.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::ops::{Range, RangeInclusive};
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use fbt_core::{BLOCK_SIZE, BlockRef};
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::ReaderId;
use super::actor::{Actor, Phase};
use super::data::Job;
use super::peers::partial_cap;
use super::serve::{MAX_PIECE_READS, WHOLE_PIECE_READ_MAX, Waiter, read_guarded};
use crate::error::{SessionError, StreamError};
use crate::peer::messages::PeerCommand;
use crate::picker::PeerKey;
use crate::stream::reader::{MAX_READ_CHUNK, MAX_READERS, READ_PREFETCH_PIECES, ReaderLink};
use crate::stream::window::window;

/// The download rate [`Actor::arrival`] assumes of a peer that measures
/// slower (a new peer measures 0).
pub(super) const MIN_DEADLINE_RATE: u64 = 16 * 1024;

/// When the next block asked of a peer with `requested` blocks outstanding
/// and a download rate of `rate` would arrive.
fn arrival_after(requested: usize, rate: u64) -> Duration {
    let bytes = (requested as u128 + 1) * u128::from(BLOCK_SIZE);
    let nanos = bytes * 1_000_000_000 / u128::from(rate.max(MIN_DEADLINE_RATE));
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

/// How many blocks a peer at `rate` delivers within `left`, counting those
/// it already has outstanding.
fn blocks_within(left: Duration, rate: u64) -> usize {
    let bytes = left.as_nanos() * u128::from(rate.max(MIN_DEADLINE_RATE)) / 1_000_000_000;
    usize::try_from(bytes / u128::from(BLOCK_SIZE)).unwrap_or(usize::MAX)
}

/// Where a read's answer goes.
pub(super) type Answer = oneshot::Sender<Result<Bytes, StreamError>>;

/// A reader's read: the reader, the torrent-global offset, the length
/// (within one piece) and the answer.
pub(super) type ReadReq = (ReaderId, u64, u32, Answer);

/// One open reader.
#[derive(Debug)]
pub(super) struct Reader {
    /// The file's torrent-global range.
    file: Range<u64>,
    /// Dead once the `FileReader` is dropped.
    link: Weak<ReaderLink>,
    /// The piece under its position; `None` at or past the file's end.
    piece: Option<u32>,
    /// The pieces its window covered when last set, missing or not.
    span: Option<RangeInclusive<u32>>,
}

/// The readers of one torrent and the reads waiting for a piece.
#[derive(Debug, Default)]
pub(super) struct Readers {
    next_id: u64,
    open: BTreeMap<ReaderId, Reader>,
    /// Reads waiting for a piece, by piece; at most one per reader.
    pub(super) waiting: HashMap<u32, Vec<ReadReq>>,
}

impl Readers {
    fn remove_waiter(&mut self, id: ReaderId) {
        self.waiting.retain(|_, v| {
            v.retain(|w| w.0 != id);
            !v.is_empty()
        });
    }
}

impl Actor {
    pub(super) fn on_open_reader(
        &mut self,
        file: usize,
        link: Arc<ReaderLink>,
        tx: oneshot::Sender<Result<ReaderId, SessionError>>,
    ) {
        let r = self.open_reader(file, &link);
        let _ = tx.send(r);
    }

    fn open_reader(
        &mut self,
        file: usize,
        link: &Arc<ReaderLink>,
    ) -> Result<ReaderId, SessionError> {
        self.drop_dead_readers();
        let Some(l) = self.loaded.as_ref() else {
            return Err(SessionError::NoMetadata);
        };
        let files = l.meta.info.files.len();
        let range = l
            .meta
            .info
            .files
            .get(file)
            .filter(|f| !f.pad)
            .and_then(|_| l.meta.file_layout().file_range(file))
            .ok_or(SessionError::InvalidFileIndex { index: file, files })?;
        if self.readers.open.len() >= MAX_READERS {
            return Err(SessionError::TooManyReaders);
        }
        let id = ReaderId(self.readers.next_id);
        self.readers.next_id = self.readers.next_id.wrapping_add(1);
        self.readers.open.insert(
            id,
            Reader {
                file: range,
                link: Arc::downgrade(link),
                piece: None,
                span: None,
            },
        );
        self.sync_reader(id, Instant::now(), true);
        // Its file may be incomplete although its window is not.
        self.sync_upload_only();
        Ok(id)
    }

    pub(super) fn on_reader_moved(&mut self, id: ReaderId) {
        self.sync_reader(id, Instant::now(), true);
    }

    pub(super) fn on_close_reader(&mut self, id: ReaderId) {
        self.forget_reader(id);
    }

    /// A reader asks for `len` bytes at `offset`; its earlier read, if one
    /// still waits (a seek abandoned it), goes.
    pub(super) fn on_read(&mut self, reader: ReaderId, offset: u64, len: u32, tx: Answer) {
        self.remove_reader_waiters(reader);
        self.serve_read((reader, offset, len, tx));
    }

    /// On the status tick: readers whose `FileReader` is gone (its
    /// `CloseReader` may not have fit the queue) and reads nobody waits for
    /// any more are dropped, and every window is set again relative to now.
    pub(super) fn readers_tick(&mut self) {
        if self.readers.open.is_empty() && self.readers.waiting.is_empty() {
            return;
        }
        self.drop_dead_readers();
        // A seek drops its pending read on purpose: only the read goes.
        self.readers.waiting.retain(|_, v| {
            v.retain(|w| !w.3.is_closed());
            !v.is_empty()
        });
        self.piece_reads
            .retain_waiters(|w| !matches!(w, Waiter::Reader(r) if r.3.is_closed()));
        self.sync_all_readers();
    }

    /// Sets every reader's window again, relative to now.
    pub(super) fn sync_all_readers(&mut self) {
        let now = Instant::now();
        let ids: Vec<ReaderId> = self.readers.open.keys().copied().collect();
        for id in ids {
            self.sync_reader(id, now, false);
        }
    }

    /// Answers every waiting read with `err` (the torrent stopped).
    pub(super) fn fail_readers(&mut self, err: StreamError) {
        for (_, v) in self.readers.waiting.drain() {
            for w in v {
                let _ = w.3.send(Err(err.clone()));
            }
        }
    }

    /// Answers the reads waiting for `piece` from its verified `data`.
    pub(super) fn answer_piece(&mut self, piece: u32, data: &Arc<[u8]>) {
        for req in self.readers.waiting.remove(&piece).unwrap_or_default() {
            self.answer_read(req, data);
        }
    }

    /// Serves the waiting reads of pieces the picker has now (after a
    /// check).
    pub(super) fn serve_waiting_reads(&mut self) {
        let Some(picker) = self.loaded.as_ref().and_then(|l| l.picker.as_ref()) else {
            return;
        };
        let ready: Vec<u32> = self
            .readers
            .waiting
            .keys()
            .copied()
            .filter(|&p| picker.have().get(p))
            .collect();
        for piece in ready {
            for req in self.readers.waiting.remove(&piece).unwrap_or_default() {
                self.serve_read(req);
            }
        }
    }

    /// Answers `req` from its piece's verified `data`.
    pub(super) fn answer_read(&self, req: ReadReq, data: &Arc<[u8]>) {
        let (_, offset, len, tx) = req;
        let start = self.loaded.as_ref().and_then(|l| {
            let piece = u32::try_from(offset / u64::from(l.layout.piece_len())).ok()?;
            usize::try_from(offset.checked_sub(l.layout.piece_offset(piece))?).ok()
        });
        let range = start.and_then(|s| Some(s..s.checked_add(len as usize)?));
        let answer = match range {
            Some(r) if r.end <= data.len() => Ok(Bytes::from_owner(Arc::clone(data)).slice(r)),
            _ => Err(StreamError::TorrentError(
                "read outside its piece".to_owned(),
            )),
        };
        let _ = tx.send(answer);
    }

    /// Serves `req`: an error in `Phase::Error`, the bytes when the picker
    /// has its piece (from the cache, else from disk), otherwise it waits.
    pub(super) fn serve_read(&mut self, req: ReadReq) {
        if let Phase::Error(m) = &self.phase {
            let _ = req.3.send(Err(StreamError::TorrentError(m.clone())));
            return;
        }
        let Some(l) = self.loaded.as_ref() else {
            let _ = req.3.send(Err(StreamError::TorrentError(
                "the torrent's metadata is not known".to_owned(),
            )));
            return;
        };
        let (_, offset, len, _) = req;
        let layout = l.layout;
        let piece = u32::try_from(offset / u64::from(layout.piece_len()))
            .ok()
            .filter(|&p| p < layout.num_pieces());
        let valid = piece.filter(|&p| {
            let end = offset.saturating_add(u64::from(len));
            len > 0
                && len <= MAX_READ_CHUNK
                && end <= layout.piece_offset(p) + u64::from(layout.piece_size(p))
        });
        let Some(piece) = valid else {
            let _ = req.3.send(Err(StreamError::TorrentError(
                "read outside the torrent".to_owned(),
            )));
            return;
        };
        let have = l.picker.as_ref().is_some_and(|pk| pk.have().get(piece));
        if !have {
            self.readers.waiting.entry(piece).or_default().push(req);
            return;
        }
        if let Some(data) = self.cache.get(piece) {
            self.answer_read(req, &data);
            return;
        }
        if self.storage_busy {
            // The files are moving: served once they are in place.
            self.readers.waiting.entry(piece).or_default().push(req);
            return;
        }
        let size = layout.piece_size(piece);
        if size <= WHOLE_PIECE_READ_MAX {
            if let Some(read) = self.piece_reads.get_mut(&piece) {
                read.waiting.push(Waiter::Reader(req));
                return;
            }
            if self.piece_reads.len() < MAX_PIECE_READS {
                let reserved = if self.cache.reserve_clean(u64::from(size)) {
                    u64::from(size)
                } else {
                    0
                };
                self.start_piece_read(piece, reserved, vec![Waiter::Reader(req)]);
                return;
            }
        }
        self.read_chunk(req);
    }

    /// Reads a reader's chunk alone.
    fn read_chunk(&mut self, req: ReadReq) {
        let Some(storage) = self.storage.clone() else {
            // Unreachable (storage comes with the metadata); see
            // `start_piece_read`.
            let _ = req.3.send(Err(StreamError::TorrentError(
                "the torrent has no storage".to_owned(),
            )));
            return;
        };
        let generation = self.generation;
        let (reader, offset, len, tx) = req;
        self.reads_in_flight = self.reads_in_flight.saturating_add(1);
        self.shared.disk.spawn(&mut self.jobs, move || {
            // A seek dropped the answer before the read started: skip it.
            let result = if tx.is_closed() {
                Err(io::ErrorKind::Interrupted.into())
            } else {
                read_guarded(&*storage, offset, len as usize)
            };
            Job::ReadChunk {
                generation,
                reader,
                offset,
                len,
                tx,
                result,
            }
        });
    }

    pub(super) fn on_read_chunk(
        &mut self,
        generation: u64,
        req: ReadReq,
        result: io::Result<Vec<u8>>,
    ) {
        // Nobody waits for it (a seek dropped it): nothing to answer, and a
        // skipped read says nothing about the storage.
        if req.3.is_closed() {
            return;
        }
        if generation != self.generation || !matches!(self.phase, Phase::Ready) {
            // Stale: served again as things are now.
            self.serve_read(req);
            return;
        }
        match result {
            Ok(buf) if buf.len() == req.2 as usize => {
                let _ = req.3.send(Ok(Bytes::from(buf)));
            }
            // Unreachable (the buffer is sized before the read): served
            // again rather than parked in `waiting`, which a piece we have
            // never leaves.
            Ok(_) => self.serve_read(req),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let piece = self
                    .loaded
                    .as_ref()
                    .and_then(|l| u32::try_from(req.1 / u64::from(l.layout.piece_len())).ok());
                match piece {
                    Some(piece) => self.piece_lost(piece, &e, vec![Waiter::Reader(req)]),
                    None => self.wait_read(req),
                }
            }
            Err(e) => {
                self.fail(&e, "reading a block");
                self.serve_read(req);
            }
        }
    }

    /// Readers among `waiting` are served again as things are now; peers'
    /// requests are dropped.
    pub(super) fn serve_reads(&mut self, waiting: Vec<Waiter>) {
        for w in waiting {
            if let Waiter::Reader(req) = w {
                self.serve_read(req);
            }
        }
    }

    /// Readers among `waiting` fail with `message`; peers' requests are
    /// dropped.
    pub(super) fn fail_reads(&mut self, waiting: Vec<Waiter>, message: &str) {
        for w in waiting {
            if let Waiter::Reader(req) = w {
                let _ = req
                    .3
                    .send(Err(StreamError::TorrentError(message.to_owned())));
            }
        }
    }

    /// Readers among `waiting` wait again; peers' requests are dropped.
    pub(super) fn wait_reads(&mut self, waiting: Vec<Waiter>) {
        for w in waiting {
            if let Waiter::Reader(req) = w {
                self.wait_read(req);
            }
        }
    }

    /// `req` waits for its piece (verified by the download or found by a
    /// check).
    pub(super) fn wait_read(&mut self, req: ReadReq) {
        let piece = self
            .loaded
            .as_ref()
            .and_then(|l| u32::try_from(req.1 / u64::from(l.layout.piece_len())).ok());
        match piece {
            Some(piece) => self.readers.waiting.entry(piece).or_default().push(req),
            None => {
                let _ = req.3.send(Err(StreamError::TorrentError(
                    "the torrent's metadata is not known".to_owned(),
                )));
            }
        }
    }

    /// Takes `id`'s pending read out, wherever it waits.
    fn remove_reader_waiters(&mut self, id: ReaderId) {
        self.readers.remove_waiter(id);
        self.piece_reads
            .retain_waiters(|w| !matches!(w, Waiter::Reader(r) if r.0 == id));
    }

    fn drop_dead_readers(&mut self) {
        let dead: Vec<ReaderId> = self
            .readers
            .open
            .iter()
            .filter(|(_, r)| r.link.strong_count() == 0)
            .map(|(&id, _)| id)
            .collect();
        for id in dead {
            self.forget_reader(id);
        }
    }

    fn forget_reader(&mut self, id: ReaderId) {
        self.readers.open.remove(&id);
        let orphans = self.deadlines.remove(id);
        self.remove_reader_waiters(id);
        self.deadlines_changed(&[], &orphans);
        // Its file no longer keeps the seeds (as interesting, and from an
        // upload-only close), whatever its window held.
        let interested: Vec<PeerKey> = self
            .peers
            .iter()
            .filter(|(_, p)| p.live() && p.am_interested)
            .map(|(k, _)| *k)
            .collect();
        for key in interested {
            self.update_interest(key);
        }
        self.sync_upload_only();
    }

    /// Reads the reader's link and sets its window (missing pieces only)
    /// relative to `now`; `take` also takes its queued nudge. When its
    /// current piece changes, the previous one is demoted in the cache and
    /// the next ones we have are read ahead.
    fn sync_reader(&mut self, id: ReaderId, now: Instant, take: bool) {
        let Some(r) = self.readers.open.get(&id) else {
            return;
        };
        let Some(link) = r.link.upgrade() else {
            self.forget_reader(id);
            return;
        };
        let (pos, _) = if take { link.take() } else { link.peek() };
        let ra = link.read_ahead();
        let file = r.file.clone();
        let Some(l) = self.loaded.as_ref() else {
            return;
        };
        let layout = l.layout;
        let piece = (file.contains(&pos))
            .then(|| u32::try_from(pos / u64::from(layout.piece_len())).ok())
            .flatten();
        let full = window(file.clone(), pos, &layout, &ra, now);
        let span = full.first().zip(full.last()).map(|(a, b)| a.0..=b.0);
        // No picker while checking: unfiltered until `on_checked`.
        let missing: Vec<(u32, Instant)> = match l.picker.as_ref() {
            Some(pk) => full
                .into_iter()
                .filter(|&(p, _)| !pk.have().get(p))
                .collect(),
            None => full,
        };
        let Some(r) = self.readers.open.get_mut(&id) else {
            return;
        };
        r.span = span;
        let old = std::mem::replace(&mut r.piece, piece);
        let own: Vec<u32> = missing.iter().map(|&(p, _)| p).collect();
        let added: Vec<u32> = own
            .iter()
            .copied()
            .filter(|&p| !self.deadlines.contains(p))
            .collect();
        let orphans = self.deadlines.set(id, missing);
        self.deadlines_changed(&added, &orphans);
        if old == piece {
            return;
        }
        if let Some(p) = old
            && !self.held_by_other(id, p, own.contains(&p))
        {
            self.cache.demote(p);
        }
        if let Some(q) = piece {
            self.prefetch_after(q, &file);
        }
    }

    /// Another reader than `id` still wants `p`: its window or its current
    /// piece holds it. The windows hold missing pieces only, so the
    /// readers' unfiltered spans count too. `own` says whether `id`'s own
    /// window holds `p`.
    fn held_by_other(&self, id: ReaderId, p: u32, own: bool) -> bool {
        self.deadlines.holders(p) > usize::from(own)
            || self.readers.open.iter().any(|(&o, r)| {
                o != id && (r.piece == Some(p) || r.span.as_ref().is_some_and(|s| s.contains(&p)))
            })
    }

    /// Reads up to [`READ_PREFETCH_PIECES`] pieces we have after `q` in
    /// `file` into the clean cache, when the budget allows.
    fn prefetch_after(&mut self, q: u32, file: &Range<u64>) {
        if !matches!(self.phase, Phase::Ready) || self.storage_busy {
            return;
        }
        let Some(l) = self.loaded.as_ref() else {
            return;
        };
        let Some(picker) = l.picker.as_ref() else {
            return;
        };
        let layout = l.layout;
        let Some(last) = file
            .end
            .checked_sub(1)
            .and_then(|e| u32::try_from(e / u64::from(layout.piece_len())).ok())
        else {
            return;
        };
        let end = q.saturating_add(READ_PREFETCH_PIECES).min(last);
        let ahead: Vec<u32> = (q.saturating_add(1)..=end)
            .filter(|&p| {
                picker.have().get(p)
                    && layout.piece_size(p) <= WHOLE_PIECE_READ_MAX
                    && !self.cache.is_cached(p)
                    && !self.piece_reads.contains_key(&p)
            })
            .collect();
        for p in ahead {
            let size = u64::from(layout.piece_size(p));
            if self.piece_reads.len() >= MAX_PIECE_READS || !self.cache.reserve_clean(size) {
                return;
            }
            self.start_piece_read(p, size, Vec::new());
        }
    }

    // ----- deadline picking -------------------------------------------------

    /// After the merged deadlines changed: `added` pieces are new to them,
    /// `orphans` left every window. An orphan of priority 0 has no other
    /// reason to finish: its outstanding blocks are cancelled and its
    /// buffer freed (one pending its hash check keeps going). Peers that
    /// have an added piece may become interesting; interested peers may
    /// stop being so once pieces left the windows.
    pub(super) fn deadlines_changed(&mut self, added: &[u32], orphans: &[u32]) {
        if added.is_empty() && orphans.is_empty() {
            return;
        }
        for &piece in orphans {
            self.abandon_skipped(piece);
        }
        let keys: Vec<PeerKey> = self
            .peers
            .iter()
            .filter(|(_, p)| {
                p.live()
                    && if p.am_interested {
                        !orphans.is_empty()
                    } else {
                        added.iter().any(|&i| p.has(i))
                    }
            })
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            self.update_interest(key);
        }
        self.sync_upload_only();
        if !added.is_empty() {
            self.top_up_all(None);
        }
    }

    /// Drops the download of `piece` if it has priority 0: its blocks are
    /// cancelled, its buffer and budget reservation given back.
    fn abandon_skipped(&mut self, piece: u32) {
        let generation = self.generation;
        let Some(l) = self.loaded.as_mut() else {
            return;
        };
        if l.piece_prio.get(piece as usize).is_none_or(|&p| p > 0) {
            return;
        }
        let Some(picker) = l.picker.as_mut() else {
            return;
        };
        let cancel = picker.abandon(piece);
        // Passed, or pending its hash check: the result stands.
        if picker.have().get(piece) || picker.is_in_progress(piece) {
            return;
        }
        l.bufs.remove(&piece);
        self.cache.release_partial(piece);
        self.smartban.forget(piece);
        for (key, block) in cancel {
            let Some(p) = self.peers.get_mut(&key) else {
                continue;
            };
            if p.generation == generation && p.requested.remove(&block) {
                self.send(key, PeerCommand::Cancel(block));
            }
        }
    }

    /// Some open reader's file still has a missing piece. A finished
    /// torrent is not upload-only (BEP 21) and keeps its interest in seeds
    /// while this holds: a reader slower than the download completes its
    /// window again and again, and turning upload-only on at each of those
    /// moments would close and redial the seeds once per window.
    pub(super) fn reader_needs_pieces(&self) -> bool {
        let Some(l) = self.loaded.as_ref() else {
            return false;
        };
        let Some(pk) = l.picker.as_ref() else {
            return false;
        };
        let pl = u64::from(l.layout.piece_len());
        self.readers.open.values().any(|r| {
            let Some(last) = r.file.end.checked_sub(1) else {
                return false;
            };
            if r.file.start > last {
                return false;
            }
            let (Ok(first), Ok(last)) =
                (u32::try_from(r.file.start / pl), u32::try_from(last / pl))
            else {
                return false;
            };
            (first..=last).any(|i| !pk.have().get(i))
        })
    }

    /// Some piece with a deadline is missing.
    pub(super) fn deadline_missing(&self) -> bool {
        self.loaded
            .as_ref()
            .and_then(|l| l.picker.as_ref())
            .is_some_and(|pk| {
                self.deadlines
                    .ordered()
                    .iter()
                    .any(|&(p, _)| !pk.have().get(p))
            })
    }

    /// When one more block asked of `key` would arrive: its outstanding
    /// blocks plus that one, at its download rate (at least
    /// [`MIN_DEADLINE_RATE`]).
    pub(super) fn arrival(&self, key: PeerKey, now: Instant) -> Duration {
        self.peers.get(&key).map_or(Duration::MAX, |p| {
            arrival_after(p.requested.len(), p.from_rate.rate(now))
        })
    }

    /// Blocks of deadline pieces for `key`, at most `room`, earliest
    /// deadline first (spec 5.5). Free blocks of a piece go to a peer whose
    /// [`arrival`](Self::arrival) is within the piece's time left, or to
    /// the best-ranked peer that has it; blocks held by a late owner (one
    /// that would deliver after the deadline, chokes us or is snubbed) are
    /// duplicated to a peer that would deliver them sooner, never to more
    /// than two holders. A piece starts only within the partial-piece cap
    /// and with its buffer reserved; one that cannot reserve waits.
    pub(super) fn fill_deadlines(
        &mut self,
        key: PeerKey,
        room: usize,
        now: Instant,
    ) -> Vec<BlockRef> {
        let mut out = Vec::new();
        if room == 0 || self.deadlines.is_empty() {
            return out;
        }
        let generation = self.generation;
        let Some(p) = self.peers.get(&key) else {
            return out;
        };
        let choked = p.choked_us;
        if choked && !p.can_request_fast() {
            return out;
        }
        let base = p.requested.len();
        let rate = p.from_rate.rate(now);
        // Live peers that neither choke us nor are snubbed, by arrival,
        // once per call (a snubbed peer is never on time, so it must not
        // stand in the way of the best-ranked one).
        let mut ranked: Vec<(Duration, PeerKey)> = self
            .peers
            .iter()
            .filter(|(_, o)| o.live() && !o.choked_us && !o.snubbed && o.generation == generation)
            .map(|(&k, o)| (arrival_after(o.requested.len(), o.from_rate.rate(now)), k))
            .collect();
        ranked.sort_unstable();
        // The peers ranked before `key` (all of them if it is not ranked).
        let ahead: Vec<PeerKey> = ranked
            .iter()
            .map(|&(_, k)| k)
            .take_while(|&k| k != key)
            .collect();
        let owner_arrival = |o: PeerKey| -> Duration {
            match self.peers.get(&o) {
                Some(q) if q.live() && !q.choked_us && !q.snubbed => ranked
                    .iter()
                    .find(|&&(_, k)| k == o)
                    .map_or(Duration::MAX, |&(t, _)| t),
                // Choking, snubbed or gone: never on time.
                _ => Duration::MAX,
            }
        };
        let Some(l) = self.loaded.as_mut() else {
            return out;
        };
        let layout = l.layout;
        let Some(picker) = l.picker.as_mut() else {
            return out;
        };
        let cap = partial_cap(
            picker.num_in_progress(),
            self.cache.partial_room(u64::from(layout.piece_len())),
        );
        for &(piece, deadline) in self.deadlines.ordered() {
            let left_room = room.saturating_sub(out.len());
            if left_room == 0 {
                break;
            }
            let has = if choked {
                p.fast_has.as_ref().is_some_and(|f| f.get(piece))
            } else {
                p.has(piece)
            };
            if !has {
                continue;
            }
            let Some(progress) = picker.progress(piece) else {
                continue;
            };
            let left = deadline.saturating_duration_since(now);
            let mine = arrival_after(base.saturating_add(out.len()), rate);
            // Free blocks: as many as arrive in time, or all for the best
            // (worked out only when the piece has free blocks).
            let free = if progress.free == 0 {
                0
            } else {
                let in_time = blocks_within(left, rate)
                    .saturating_sub(base.saturating_add(out.len()))
                    .min(left_room);
                let best = || {
                    !choked
                        && !ahead
                            .iter()
                            .any(|k| self.peers.get(k).is_some_and(|o| o.has(piece)))
                };
                if in_time < left_room && best() {
                    left_room
                } else {
                    in_time
                }
            };
            let take_free = free > 0;
            let late: Vec<PeerKey> = if progress.owners.contains(&key) {
                Vec::new()
            } else {
                progress
                    .owners
                    .iter()
                    .copied()
                    .filter(|&o| {
                        let t = owner_arrival(o);
                        t > left && t > mine
                    })
                    .collect()
            };
            if !take_free && late.is_empty() {
                continue;
            }
            // Its buffer first; a new piece also needs room under the cap.
            if !self.cache.has_partial(piece) {
                if !picker.is_in_progress(piece) && picker.num_in_progress() >= cap {
                    continue;
                }
                if !self
                    .cache
                    .reserve_partial(piece, u64::from(layout.piece_size(piece)))
                {
                    continue;
                }
            }
            if take_free {
                out.extend(picker.pick_piece(key, piece, free, &[]));
            }
            // Copies only once no block is free (`pick_piece` takes free
            // blocks first, and those were offered above).
            let n = room.saturating_sub(out.len());
            let none_free = picker.progress(piece).is_some_and(|pr| pr.free == 0);
            if !late.is_empty() && n > 0 && none_free {
                out.extend(picker.pick_piece(key, piece, n, &late));
            }
            if !picker.is_in_progress(piece) {
                // Nothing taken after all.
                self.cache.release_partial(piece);
            }
        }
        out
    }

    /// Tests: the merged deadlines as time left.
    pub(super) fn debug_deadlines(&self) -> Vec<(u32, std::time::Duration)> {
        let now = Instant::now();
        self.deadlines
            .ordered()
            .iter()
            .map(|&(p, t)| (p, t.saturating_duration_since(now)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::super::serve::PieceRead;
    use super::super::swarm::tests::actor_with;
    use super::*;
    use crate::stream::ReadAhead;
    use tokio::sync::oneshot::error::TryRecvError;

    const PL: u32 = 16_384;

    /// Opens a reader of `file` at the torrent-global `pos`.
    fn open(a: &mut Actor, file: usize, pos: u64) -> (ReaderId, Arc<ReaderLink>) {
        let link = Arc::new(ReaderLink::new(pos, ReadAhead::default()));
        let (tx, mut rx) = oneshot::channel();
        a.on_open_reader(file, Arc::clone(&link), tx);
        let id = rx.try_recv().expect("answered").expect("opened");
        (id, link)
    }

    #[tokio::test]
    async fn dropped_reader_is_forgotten_within_a_tick() {
        let mut a = actor_with(&[8 * u64::from(PL)], PL, 0);
        let (id, link) = open(&mut a, 0, 0);
        let (tx, rx) = oneshot::channel();
        a.on_read(id, 3 * u64::from(PL), 100, tx);
        assert_eq!(a.readers.waiting[&3].len(), 1);
        assert!(!a.deadlines.is_empty());
        // Dropped with no `CloseReader` (a full command queue).
        drop(link);
        drop(rx);
        a.readers_tick();
        assert!(a.deadlines.is_empty());
        assert!(a.readers.waiting.is_empty());
        assert!(a.readers.open.is_empty());
        let mut links = Vec::new();
        for _ in 0..MAX_READERS {
            links.push(open(&mut a, 0, 0).1);
        }
        let (tx, mut rx) = oneshot::channel();
        a.on_open_reader(0, Arc::new(ReaderLink::new(0, ReadAhead::default())), tx);
        assert!(matches!(
            rx.try_recv(),
            Ok(Err(SessionError::TooManyReaders))
        ));
    }

    /// A storage that panics while reading is an I/O error, so the job
    /// keeps its identity and its waiters are answered.
    #[test]
    fn a_panicking_read_is_an_io_error() {
        #[derive(Debug)]
        struct Panics;
        impl crate::storage::Storage for Panics {
            fn read(&self, _: u64, _: &mut [u8]) -> io::Result<()> {
                panic!("broken storage");
            }
            fn write(&self, _: u64, _: &[u8]) -> io::Result<()> {
                Ok(())
            }
            fn flush(&self) -> io::Result<()> {
                Ok(())
            }
            fn delete_files(&self) -> io::Result<()> {
                Ok(())
            }
        }
        let e = read_guarded(&Panics, 0, 16).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::Other);
    }

    /// A seek drops the reader's pending read: the tick drops that read
    /// and keeps the reader, its window and its slot.
    #[tokio::test]
    async fn tick_drops_a_closed_waiter_and_keeps_its_reader() {
        let mut a = actor_with(&[8 * u64::from(PL)], PL, 0);
        let (id, _link) = open(&mut a, 0, 0);
        let (tx, rx) = oneshot::channel();
        a.on_read(id, 3 * u64::from(PL), 100, tx);
        assert_eq!(a.readers.waiting[&3].len(), 1);
        let deadlines = a.debug_deadlines().len();
        assert!(deadlines > 0);
        // Abandoned by a seek; the link (the `FileReader`) lives on.
        drop(rx);
        a.readers_tick();
        assert!(a.readers.waiting.is_empty());
        assert!(a.readers.open.contains_key(&id));
        assert_eq!(a.debug_deadlines().len(), deadlines);
        // Its next read waits as usual.
        let (tx, mut rx) = oneshot::channel();
        a.on_read(id, 4 * u64::from(PL), 100, tx);
        assert_eq!(a.readers.waiting[&4].len(), 1);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn moving_on_demotes_the_previous_piece() {
        let plen = u64::from(PL);
        let (p, q) = (2u32, 3u32);
        let data = || Arc::<[u8]>::from(vec![7u8; PL as usize]);
        let setup = || {
            let mut a = actor_with(&[8 * plen], PL, 64 * plen);
            let now = std::time::Instant::now();
            a.cache.insert_clean(p, data(), now);
            a.cache.insert_clean(q, data(), now);
            // `q` is now the least recently used.
            assert!(a.cache.get(p).is_some());
            a
        };
        let victim = |a: &Actor| a.shared.cache.next_clean_victim().map(|(_, piece)| piece);

        let mut a = setup();
        assert_eq!(victim(&a), Some(q));
        let (id, link) = open(&mut a, 0, u64::from(p) * plen);
        link.moved(u64::from(q) * plen, || true);
        a.on_reader_moved(id);
        assert_eq!(victim(&a), Some(p), "left behind, so evicted first");

        // A second reader's window holds `p`: it stays.
        let mut a = setup();
        let (_b, _b_link) = open(&mut a, 0, u64::from(p) * plen);
        let (id, link) = open(&mut a, 0, u64::from(p) * plen);
        link.moved(u64::from(q) * plen, || true);
        a.on_reader_moved(id);
        assert_eq!(victim(&a), Some(q));
    }

    #[tokio::test]
    async fn abandoned_whole_piece_read_requeues_its_reader() {
        let mut a = actor_with(&[8 * u64::from(PL)], PL, 0);
        let (id, _link) = open(&mut a, 0, 0);
        let (tx, mut rx) = oneshot::channel();
        a.piece_reads.insert(
            5,
            PieceRead {
                generation: a.generation,
                reserved: 0,
                waiting: vec![Waiter::Reader((id, 5 * u64::from(PL), 100, tx))],
                started: true,
            },
        );
        a.drop_piece_reads();
        assert!(!a.piece_reads.contains_key(&5));
        let waiting = &a.readers.waiting[&5];
        assert_eq!(waiting.len(), 1);
        assert_eq!(waiting[0].0, id);
        assert!(!waiting[0].3.is_closed());
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    // ----- deadline picking -------------------------------------------------

    use std::collections::HashMap;
    use std::time::Duration;

    use fbt_core::{Bitfield, BlockRef};
    use tokio::sync::mpsc;

    use super::super::peer::Peer;
    use super::super::swarm::tests::addr;
    use crate::peer::messages::PeerCommand;
    use crate::peer::transport::Transport;
    use crate::picker::PeerKey;
    use crate::session::Priority;
    use crate::storage::cache::PROGRESS_FLOOR_PIECES;

    /// Pieces of four blocks.
    const P4: u32 = 65_536;
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * 1024;

    /// A connected peer that unchokes us, downloading to us at `rate`
    /// bytes per second (recorded now: the test runs on paused time), with
    /// `bits` (`None`: a seed) and no demand yet. It counts in the
    /// picker's availability.
    fn add_peer(
        a: &mut Actor,
        i: u32,
        rate: u64,
        bits: Option<Bitfield>,
    ) -> (PeerKey, mpsc::Receiver<PeerCommand>) {
        let n = a.loaded.as_ref().expect("loaded").layout.num_pieces();
        let (tx, rx) = mpsc::channel(1 << 16);
        let mut p = Peer::new(addr(i), tx, a.generation, n, Transport::Tcp);
        p.outgoing = true;
        p.connected = true;
        p.choked_us = false;
        match bits {
            None => p.seed = true,
            Some(b) => p.bits = b,
        }
        p.from_rate.record(rate);
        let key = a.new_key();
        a.loaded
            .as_mut()
            .and_then(|l| l.picker.as_mut())
            .expect("picker")
            .add_peer_pieces(p.pieces());
        a.peers.insert(key, p);
        (key, rx)
    }

    /// Opens a reader of `file` at `pos` with `ra`.
    fn open_with(
        a: &mut Actor,
        file: usize,
        pos: u64,
        ra: ReadAhead,
    ) -> (ReaderId, Arc<ReaderLink>) {
        let link = Arc::new(ReaderLink::new(pos, ra));
        let (tx, mut rx) = oneshot::channel();
        a.on_open_reader(file, Arc::clone(&link), tx);
        let id = rx.try_recv().expect("answered").expect("opened");
        (id, link)
    }

    /// Asks `key` for `demand` blocks, as its `WantBlocks` does.
    fn want(a: &mut Actor, key: PeerKey, demand: usize) {
        a.peers.get_mut(&key).expect("peer").demand = demand;
        a.fill(key);
    }

    fn requested(a: &Actor, key: PeerKey) -> Vec<BlockRef> {
        let mut v: Vec<BlockRef> = a.peers[&key].requested.iter().copied().collect();
        v.sort_by_key(|b| (b.piece, b.offset));
        v
    }

    fn of_piece(a: &Actor, key: PeerKey, piece: u32) -> Vec<u32> {
        requested(a, key)
            .into_iter()
            .filter(|b| b.piece == piece)
            .map(|b| b.offset)
            .collect()
    }

    fn picker(a: &mut Actor) -> &mut crate::picker::PiecePicker {
        a.loaded
            .as_mut()
            .and_then(|l| l.picker.as_mut())
            .expect("picker")
    }

    /// `key` holds `n` free blocks of `piece`, as an earlier `fill` left
    /// them.
    fn hold(a: &mut Actor, key: PeerKey, piece: u32, n: usize) {
        assert!(a.cache.reserve_partial(piece, u64::from(P4)) || a.cache.has_partial(piece));
        let got = picker(a).pick_piece(key, piece, n, &[]);
        assert_eq!(got.len(), n);
        a.peers.get_mut(&key).expect("peer").requested.extend(got);
    }

    #[tokio::test(start_paused = true)]
    async fn fastest_peer_gets_the_first_deadline_piece() {
        let mut a = actor_with(&[64 * u64::from(P4)], P4, 64 * MIB);
        let (fast, _rf) = add_peer(&mut a, 1, MIB, None);
        let (slow, _rs) = add_peer(&mut a, 2, 20 * KIB, None);
        // The default window covers the whole 4 MiB file, at 1.6 MiB/s.
        let (_id, _link) = open(&mut a, 0, 0);
        assert_eq!(a.deadlines.ordered().len(), 64);
        // The slow peer asks first.
        want(&mut a, slow, 32);
        want(&mut a, fast, 32);
        assert_eq!(of_piece(&a, fast, 0), vec![0, 16_384, 32_768, 49_152]);
        assert!(of_piece(&a, slow, 0).is_empty());
        // One block from the slow peer takes 0.8 s at least.
        let now = Instant::now();
        let slow_arrival = Duration::from_millis(800);
        for b in requested(&a, slow) {
            let left = a
                .deadlines
                .deadline(b.piece)
                .expect("a deadline piece")
                .saturating_duration_since(now);
            assert!(left >= slow_arrival, "piece {} left {left:?}", b.piece);
        }
    }

    /// Pieces 0 to 9 are there: piece 10 is the window's first, due in
    /// about 390 ms.
    fn late_setup() -> Actor {
        let mut a = actor_with(&[64 * u64::from(P4)], P4, 64 * MIB);
        for i in 0..10 {
            picker(&mut a).piece_passed(i);
        }
        a
    }

    #[tokio::test(start_paused = true)]
    async fn late_block_is_duplicated_to_a_faster_peer() {
        let mut a = late_setup();
        let (slow, _r1) = add_peer(&mut a, 1, 20 * KIB, None);
        let (fast_owner, _r2) = add_peer(&mut a, 2, MIB, None);
        let (c, _r3) = add_peer(&mut a, 3, MIB, None);
        let (d, _r4) = add_peer(&mut a, 4, MIB, None);
        let (_id, _link) = open(&mut a, 0, 0);
        assert_eq!(a.deadlines.ordered().first().map(|e| e.0), Some(10));
        // The slow peer holds blocks 0 and 1 (2.4 s away), the fast owner
        // blocks 2 and 3 (47 ms away).
        hold(&mut a, slow, 10, 2);
        hold(&mut a, fast_owner, 10, 2);
        want(&mut a, c, 4);
        assert_eq!(of_piece(&a, c, 10), vec![0, 16_384], "the slow peer's");
        // Every late block has two owners now; the fast owner's are on time.
        want(&mut a, d, 4);
        assert!(of_piece(&a, d, 10).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn duplicate_never_goes_to_a_slower_peer() {
        let mut a = late_setup();
        // Blocks 0 and 1 at 100 KiB/s: 480 ms away, late for piece 10.
        let (owner, _r1) = add_peer(&mut a, 1, 100 * KIB, None);
        let (slower, _r2) = add_peer(&mut a, 2, 20 * KIB, None);
        let (faster, _r3) = add_peer(&mut a, 3, MIB, None);
        let (_id, _link) = open(&mut a, 0, 0);
        hold(&mut a, owner, 10, 2);
        // 800 ms away: no copy, and the free blocks wait for a peer in time.
        want(&mut a, slower, 4);
        assert!(of_piece(&a, slower, 10).is_empty());
        want(&mut a, faster, 4);
        assert_eq!(of_piece(&a, faster, 10), vec![0, 16_384, 32_768, 49_152]);
    }

    /// A snubbed peer never counts as ahead in the ranking: the piece
    /// under the position (due now, so nobody is in time) goes to the
    /// best healthy peer even when the snubbed one measures faster.
    #[tokio::test(start_paused = true)]
    async fn snubbed_peer_never_outranks_a_healthy_one() {
        for snubbed in [true, false] {
            let mut a = actor_with(&[64 * u64::from(P4)], P4, 64 * MIB);
            let (fast, _r1) = add_peer(&mut a, 1, MIB, None);
            let (healthy, _r2) = add_peer(&mut a, 2, 100 * KIB, None);
            a.peers.get_mut(&fast).expect("fast").snubbed = snubbed;
            let (_id, _link) = open(&mut a, 0, 0);
            want(&mut a, healthy, 4);
            let got = of_piece(&a, healthy, 0);
            if snubbed {
                assert_eq!(
                    got,
                    vec![0, 16_384, 32_768, 49_152],
                    "the best healthy peer"
                );
            } else {
                assert!(got.is_empty(), "the faster peer ranks first");
            }
        }
    }

    /// An owner that chokes us (fast extension: its requests stay
    /// outstanding) is late whatever its rate says; unchoking, the same
    /// owner is on time and keeps its blocks to itself.
    #[tokio::test(start_paused = true)]
    async fn a_choking_holder_on_time_by_rate_is_still_late() {
        for choking in [true, false] {
            let mut a = late_setup();
            // 1 MiB/s holding two blocks: 47 ms away, piece 10 is due in 390 ms.
            let (owner, _r1) = add_peer(&mut a, 1, MIB, None);
            let (other, _r2) = add_peer(&mut a, 2, MIB, None);
            let (_id, _link) = open(&mut a, 0, 0);
            hold(&mut a, owner, 10, 2);
            hold(&mut a, other, 10, 2);
            a.peers.get_mut(&owner).expect("owner").choked_us = choking;
            let (c, _r3) = add_peer(&mut a, 3, MIB, None);
            want(&mut a, c, 2);
            if choking {
                assert_eq!(of_piece(&a, c, 10), vec![0, 16_384], "the choking owner's");
            } else {
                assert!(of_piece(&a, c, 10).is_empty());
            }
        }
    }

    /// Interest that only a reader's window gave goes with the reader.
    #[tokio::test(start_paused = true)]
    async fn closing_the_reader_drops_interest_it_gave() {
        let plen = u64::from(P4);
        let mut a = actor_with(&[8 * plen, 8 * plen], P4, 64 * MIB);
        a.set_priorities(vec![Priority::Normal, Priority::Skip]);
        let mut only_12 = Bitfield::new(16);
        only_12.set(12);
        let (p, mut rp) = add_peer(&mut a, 1, MIB, Some(only_12));
        let (id, _link) = open(&mut a, 1, 12 * plen);
        assert!(a.peers[&p].am_interested);
        a.on_close_reader(id);
        assert!(!a.peers[&p].am_interested);
        let mut said = Vec::new();
        while let Ok(cmd) = rp.try_recv() {
            if let PeerCommand::Interested(on) = cmd {
                said.push(on);
            }
        }
        assert_eq!(said, vec![true, false]);
    }

    #[tokio::test(start_paused = true)]
    async fn endgame_and_deadline_never_exceed_two_owners() {
        let mut a = actor_with(&[16 * u64::from(P4)], P4, 64 * MIB);
        let rates = [MIB, 500 * KIB, 100 * KIB, 20 * KIB];
        let mut keys = Vec::new();
        let mut rxs = Vec::new();
        for (i, &r) in (1u32..).zip(&rates) {
            let (k, rx) = add_peer(&mut a, i, r, None);
            keys.push(k);
            rxs.push(rx);
        }
        // A window of four pieces; the rest goes to the normal pick.
        let (_id, _link) = open_with(
            &mut a,
            0,
            0,
            ReadAhead::new(4 * u64::from(P4), Duration::from_secs(1), None),
        );
        assert_eq!(a.deadlines.ordered().len(), 4);
        for _ in 0..4 {
            for &k in &keys {
                want(&mut a, k, 500);
            }
        }
        assert!(picker(&mut a).is_endgame());
        let mut count: HashMap<BlockRef, usize> = HashMap::new();
        for &k in &keys {
            for b in &a.peers[&k].requested {
                *count.entry(*b).or_default() += 1;
            }
        }
        assert_eq!(count.len(), 16 * 4, "every block requested");
        assert!(count.values().all(|&n| n <= 2), "{count:?}");
        assert!(count.values().any(|&n| n == 2), "some duplicates");
    }

    #[tokio::test(start_paused = true)]
    async fn skipped_piece_with_deadline_is_requested_and_wanted() {
        let plen = u64::from(P4);
        let mut a = actor_with(&[8 * plen, 8 * plen], P4, 64 * MIB);
        a.set_priorities(vec![Priority::Normal, Priority::Skip]);
        let mut only_12 = Bitfield::new(16);
        only_12.set(12);
        let (p, _rp) = add_peer(&mut a, 1, MIB, Some(only_12));
        let (seed, _rs) = add_peer(&mut a, 2, MIB, None);
        // Every wanted piece is there.
        for i in 0..8 {
            picker(&mut a).piece_passed(i);
        }
        a.update_interest(p);
        a.update_interest(seed);
        assert!(!a.wants_piece(12));
        assert!(!a.peers[&p].am_interested);
        assert!(!a.peers[&seed].am_interested);
        // A reader on the skipped file, at piece 12.
        let (_id, _link) = open(&mut a, 1, 12 * plen);
        assert!(a.wants_piece(12));
        assert!(!a.wants_piece(11), "behind the reader: not in its window");
        assert!(a.peers[&p].am_interested);
        assert!(a.peers[&seed].am_interested);
        want(&mut a, p, 8);
        assert_eq!(of_piece(&a, p, 12), vec![0, 16_384, 32_768, 49_152]);
    }

    #[tokio::test(start_paused = true)]
    async fn seek_storm_leaves_only_the_last_window() {
        let plen = u64::from(P4);
        let cache_pieces = 8u64;
        let mut a = actor_with(&[32 * plen], P4, cache_pieces * plen);
        // Odd pieces are skipped.
        {
            let l = a.loaded.as_mut().expect("loaded");
            for (i, p) in l.piece_prio.iter_mut().enumerate() {
                *p = if i % 2 == 1 { 0 } else { 4 };
            }
            let prio = l.piece_prio.clone();
            l.picker
                .as_mut()
                .expect("picker")
                .set_piece_priorities(&prio);
        }
        let (k1, mut r1) = add_peer(&mut a, 1, MIB, None);
        let (k2, mut r2) = add_peer(&mut a, 2, 200 * KIB, None);
        let ra = ReadAhead::new(3 * plen, Duration::from_secs(1), None);
        let (id, link) = open_with(&mut a, 0, 0, ra);
        let file = 0..32 * plen;
        let mut pos = 0;
        for i in 0..1000u64 {
            pos = (i * 7_919 * 1_013) % (32 * plen);
            link.moved(pos, || true);
            a.on_reader_moved(id);
            want(&mut a, k1, 64);
            want(&mut a, k2, 64);
            while r1.try_recv().is_ok() {}
            while r2.try_recv().is_ok() {}
            let in_progress = picker(&mut a).num_in_progress();
            assert!(
                in_progress <= (cache_pieces as usize) + PROGRESS_FLOOR_PIECES,
                "step {i}: {in_progress} in progress"
            );
            assert_eq!(
                a.shared.cache.stats().partial,
                in_progress as u64 * plen,
                "step {i}: one reservation per piece in progress"
            );
        }
        let layout = a.loaded.as_ref().expect("loaded").layout;
        let last: Vec<u32> = crate::stream::window::window(file, pos, &layout, &ra, Instant::now())
            .into_iter()
            .map(|(p, _)| p)
            .collect();
        let mut got: Vec<u32> = a.deadlines.ordered().iter().map(|e| e.0).collect();
        got.sort_unstable();
        assert_eq!(got, last);
        assert!(!last.is_empty());
        // Skipped pieces outside the last window were abandoned; wanted
        // ones of earlier windows are still in progress.
        let pk = picker(&mut a);
        let stale_skipped: Vec<u32> = (0..32)
            .filter(|&p| p % 2 == 1 && !last.contains(&p) && pk.is_in_progress(p))
            .collect();
        assert!(stale_skipped.is_empty(), "{stale_skipped:?}");
        assert!(
            (0..32).any(|p| p % 2 == 0 && !last.contains(&p) && pk.is_in_progress(p)),
            "old windows' wanted pieces continue"
        );
    }
}
