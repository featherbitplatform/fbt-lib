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

use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::ReaderId;
use super::actor::{Actor, Phase};
use super::data::Job;
use super::serve::{MAX_PIECE_READS, WHOLE_PIECE_READ_MAX, Waiter};
use crate::error::{SessionError, StreamError};
use crate::stream::reader::{MAX_READ_CHUNK, MAX_READERS, READ_PREFETCH_PIECES, ReaderLink};
use crate::stream::window::window;

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
            self.wait_read(req);
            return;
        };
        let generation = self.generation;
        let (reader, offset, len, tx) = req;
        self.shared.disk.spawn(&mut self.jobs, move || {
            let mut buf = vec![0u8; len as usize];
            let result = storage.read(offset, &mut buf).map(|()| buf);
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
        if generation != self.generation || !matches!(self.phase, Phase::Ready) {
            // Stale: served again as things are now.
            self.serve_read(req);
            return;
        }
        match result {
            Ok(buf) if buf.len() == req.2 as usize => {
                let _ = req.3.send(Ok(Bytes::from(buf)));
            }
            Ok(_) => self.wait_read(req),
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
        self.deadlines.remove(id);
        self.remove_reader_waiters(id);
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
        self.deadlines.set(id, missing);
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
        if !matches!(self.phase, Phase::Ready) {
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
}
