//! [`FileReader`]: one file of a torrent, read while it downloads.
//!
//! The reader keeps its position locally and shares it with the torrent's
//! actor through a [`ReaderLink`]; reads go to the actor one chunk at a
//! time and wait there until the piece is verified.

use std::fmt;
use std::future::Future;
use std::io::{self, SeekFrom};
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};
use tokio::sync::oneshot;

use super::ReadAhead;
use crate::error::{SessionError, StreamError};
use crate::torrent::{Cmd, ReaderId, TorrentHandle};

/// Most open readers per torrent.
pub(crate) const MAX_READERS: usize = 16;
/// Most bytes one read asks the torrent for; a chunk never crosses a piece
/// boundary either.
pub(crate) const MAX_READ_CHUNK: u32 = 1 << 20;
/// Pieces we have that the torrent reads into its cache ahead of a reader
/// entering a new piece.
pub(crate) const READ_PREFETCH_PIECES: u32 = 2;

/// What a reader shares with its torrent's actor: its position and
/// read-ahead, and whether a [`Cmd::ReaderMoved`] is already queued, so a
/// seek storm queues at most one.
#[derive(Debug)]
pub(crate) struct ReaderLink {
    /// Torrent-global offset.
    pos: AtomicU64,
    /// Bumped on every move.
    seq: AtomicU64,
    /// A `ReaderMoved` is queued and the actor has not taken it yet.
    nudged: AtomicBool,
    read_ahead: Mutex<ReadAhead>,
}

impl ReaderLink {
    pub(crate) fn new(pos: u64, read_ahead: ReadAhead) -> Self {
        ReaderLink {
            pos: AtomicU64::new(pos),
            seq: AtomicU64::new(0),
            nudged: AtomicBool::new(false),
            read_ahead: Mutex::new(read_ahead),
        }
    }

    /// The reader moved to `pos`; `send` queues a `ReaderMoved` (false if it
    /// could not) and runs only when none is queued yet.
    pub(crate) fn moved(&self, pos: u64, send: impl FnOnce() -> bool) {
        self.pos.store(pos, Ordering::SeqCst);
        self.seq.fetch_add(1, Ordering::SeqCst);
        if !self.nudged.swap(true, Ordering::SeqCst) && !send() {
            self.nudged.store(false, Ordering::SeqCst);
        }
    }

    /// The actor takes the nudge: the next move queues a new one. Returns
    /// the position and the move count.
    pub(crate) fn take(&self) -> (u64, u64) {
        self.nudged.store(false, Ordering::SeqCst);
        self.peek()
    }

    /// The position and the move count, leaving a queued nudge queued.
    pub(crate) fn peek(&self) -> (u64, u64) {
        (
            self.pos.load(Ordering::SeqCst),
            self.seq.load(Ordering::SeqCst),
        )
    }

    pub(crate) fn read_ahead(&self) -> ReadAhead {
        *self.read_ahead.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn set_read_ahead(&self, ra: ReadAhead) {
        *self.read_ahead.lock().unwrap_or_else(|e| e.into_inner()) = ra;
    }
}

type Fetch = Pin<Box<dyn Future<Output = io::Result<Bytes>> + Send>>;

/// One file of a torrent, readable while the torrent downloads: an
/// [`AsyncRead`] and [`AsyncSeek`] over the file's bytes, opened with
/// [`TorrentHandle::open_file`] or [`TorrentHandle::open_file_with`].
///
/// A read returns only bytes whose piece passed its hash check. A read of a
/// piece the torrent does not have yet waits until the download brings it,
/// however long that takes: there is no read timeout, so wrap reads in
/// [`tokio::time::timeout`] when that matters. Seeking is free (nothing is
/// read until the next read), and a read at or past the end returns 0.
///
/// The reader's position and its [`ReadAhead`] tell the torrent which
/// pieces it waits for and when. Streaming changes neither whom the
/// torrent uploads to (the choker) nor the rate limits: deadline blocks
/// count against every limit, so a limit below the stream's bitrate makes
/// reads wait.
///
/// A skipped file can be read too: its pieces are downloaded for the
/// reader all the same. With [`FsStorage`](crate::storage::FsStorage) a
/// skipped file that does not exist on disk stays uncreated, and the bytes
/// streamed fill the torrent's part file instead
/// ([`FsStorage::part_path`](crate::storage::FsStorage::part_path)).
///
/// A read fails only when the torrent is removed, the session shuts down or
/// the torrent stops with an error; the [`io::Error`] carries a
/// [`StreamError`]. The reader is not poisoned: after
/// [`resume`](TorrentHandle::resume) the next read works. Dropping the
/// reader frees its slot (a torrent has at most 16).
pub struct FileReader {
    handle: TorrentHandle,
    id: ReaderId,
    link: Arc<ReaderLink>,
    index: usize,
    /// The file's torrent-global range.
    file: Range<u64>,
    piece_len: u64,
    /// Position within the file.
    pos: u64,
    /// The last chunk read and its torrent-global offset.
    chunk: Bytes,
    chunk_at: u64,
    /// The chunk being fetched, by torrent-global offset.
    fetch: Option<(u64, Fetch)>,
}

impl FileReader {
    pub(crate) fn new(
        handle: TorrentHandle,
        id: ReaderId,
        link: Arc<ReaderLink>,
        index: usize,
        file: Range<u64>,
        piece_len: u32,
    ) -> Self {
        FileReader {
            handle,
            id,
            link,
            index,
            chunk_at: file.start,
            file,
            piece_len: u64::from(piece_len).max(1),
            pos: 0,
            chunk: Bytes::new(),
            fetch: None,
        }
    }

    /// The file's length in bytes.
    pub fn len(&self) -> u64 {
        self.file.end - self.file.start
    }

    /// The file is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The position within the file.
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// The file's index in the torrent.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Changes how far ahead of this reader the torrent fetches; applies at
    /// once. Fails with [`SessionError::InvalidSettings`] for a zero `time`
    /// or a zero `bitrate`.
    pub fn set_read_ahead(&mut self, read_ahead: ReadAhead) -> Result<(), SessionError> {
        read_ahead
            .validate()
            .map_err(SessionError::InvalidSettings)?;
        self.link.set_read_ahead(read_ahead);
        self.nudge();
        Ok(())
    }

    /// The torrent-global offset of the position.
    fn global(&self) -> u64 {
        self.file.start.saturating_add(self.pos)
    }

    /// Tells the actor the position moved.
    fn nudge(&self) {
        let (cmds, id) = (self.handle.cmd_sender(), self.id);
        self.link.moved(self.global(), || {
            cmds.try_send(Cmd::ReaderMoved(id)).is_ok()
        });
    }

    /// The bytes of the current chunk from the torrent-global `at` on.
    fn buffered(&self, at: u64) -> Option<&[u8]> {
        let start = usize::try_from(at.checked_sub(self.chunk_at)?).ok()?;
        self.chunk.get(start..).filter(|b| !b.is_empty())
    }

    /// The chunk to fetch at the torrent-global `at` (inside the file):
    /// up to [`MAX_READ_CHUNK`], the piece's end or the file's end.
    fn chunk_len(&self, at: u64) -> u32 {
        let piece_end = (at / self.piece_len)
            .saturating_add(1)
            .saturating_mul(self.piece_len);
        let end = at
            .saturating_add(u64::from(MAX_READ_CHUNK))
            .min(piece_end)
            .min(self.file.end);
        u32::try_from(end.saturating_sub(at)).unwrap_or(MAX_READ_CHUNK)
    }

    /// Moves the position on by `n` read bytes, telling the actor when it
    /// enters a new piece.
    fn advance(&mut self, n: u64) {
        let before = self.global() / self.piece_len;
        self.pos = self.pos.saturating_add(n);
        if self.global() / self.piece_len != before {
            self.nudge();
        }
    }
}

impl AsyncRead for FileReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            if this.pos >= this.len() || buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }
            let at = this.global();
            if let Some(src) = this.buffered(at) {
                let n = src.len().min(buf.remaining());
                buf.put_slice(src.get(..n).unwrap_or_default());
                this.advance(n as u64);
                return Poll::Ready(Ok(()));
            }
            if this.fetch.as_ref().is_none_or(|(offset, _)| *offset != at) {
                let len = this.chunk_len(at);
                let fut = fetch(this.handle.clone(), this.id, at, len);
                this.fetch = Some((at, Box::pin(fut)));
            }
            let Some((_, fut)) = this.fetch.as_mut() else {
                continue;
            };
            let result = std::task::ready!(fut.as_mut().poll(cx));
            this.fetch = None;
            match result {
                Ok(bytes) if !bytes.is_empty() => {
                    this.chunk = bytes;
                    this.chunk_at = at;
                }
                Ok(_) => return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into())),
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }
}

impl AsyncSeek for FileReader {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let this = self.get_mut();
        let new = match position {
            SeekFrom::Start(n) => Some(n),
            SeekFrom::End(d) => offset(this.len(), d),
            SeekFrom::Current(d) => offset(this.pos, d),
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "seek before the file start"))?;
        this.pos = new;
        let at = this.global();
        if this.fetch.as_ref().is_some_and(|(o, _)| *o != at) {
            this.fetch = None;
        }
        this.nudge();
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.pos))
    }
}

impl Drop for FileReader {
    fn drop(&mut self) {
        // A full queue leaves it to the actor's next status tick.
        let _ = self.handle.cmd_sender().try_send(Cmd::CloseReader(self.id));
    }
}

impl fmt::Debug for FileReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileReader")
            .field("index", &self.index)
            .field("len", &self.len())
            .field("position", &self.pos)
            .field("reading", &self.fetch.is_some())
            .finish_non_exhaustive()
    }
}

/// `base + d`, saturating above; `None` below 0.
fn offset(base: u64, d: i64) -> Option<u64> {
    if d >= 0 {
        Some(base.saturating_add(d.unsigned_abs()))
    } else {
        base.checked_sub(d.unsigned_abs())
    }
}

/// Asks the actor for `len` bytes at the torrent-global `offset` and waits
/// for them. An answer the actor dropped while the torrent still lives is
/// asked for again; a closed command channel means the torrent is gone.
async fn fetch(
    handle: TorrentHandle,
    reader: ReaderId,
    offset: u64,
    len: u32,
) -> io::Result<Bytes> {
    let cmds = handle.cmd_sender();
    loop {
        let (tx, rx) = oneshot::channel();
        let read = Cmd::Read {
            reader,
            offset,
            len,
            tx,
        };
        if cmds.send(read).await.is_err() {
            return Err(gone(&handle));
        }
        match rx.await {
            Ok(answer) => return answer.map_err(io::Error::from),
            Err(_) if cmds.is_closed() => return Err(gone(&handle)),
            Err(_) => {}
        }
    }
}

fn gone(handle: &TorrentHandle) -> io::Error {
    match handle.gone() {
        SessionError::Shutdown => StreamError::Shutdown,
        _ => StreamError::TorrentRemoved,
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seek_storm_queues_one_nudge() {
        let link = ReaderLink::new(0, ReadAhead::default());
        let mut sent = 0;
        for i in 1..=10_000u64 {
            link.moved(i * 7, || {
                sent += 1;
                true
            });
        }
        assert_eq!(sent, 1);
        assert_eq!(link.take(), (70_000, 10_000));
        // Taken: the next move queues a new one.
        link.moved(5, || {
            sent += 1;
            true
        });
        assert_eq!(sent, 2);
        assert_eq!(link.peek(), (5, 10_001));

        // A send that fails (a full queue) is retried by every move.
        let link = ReaderLink::new(0, ReadAhead::default());
        let mut tries = 0;
        for i in 0..100u64 {
            link.moved(i, || {
                tries += 1;
                false
            });
        }
        assert_eq!(tries, 100);
    }

    #[test]
    fn offsets_saturate_and_refuse_negatives() {
        assert_eq!(offset(10, -10), Some(0));
        assert_eq!(offset(10, -11), None);
        assert_eq!(offset(u64::MAX - 1, 5), Some(u64::MAX));
        assert_eq!(offset(0, i64::MIN), None);
    }
}
