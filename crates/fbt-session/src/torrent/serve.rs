//! Serving blocks to peers: each request is validated, then served from
//! the write-back cache (a verified piece not written yet, or a clean
//! entry) or read through the session's disk pool, and sent back with
//! `SendBlock`. A miss reads the whole piece and keeps it clean when the
//! budget allows. File readers share those whole-piece reads.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use fbt_core::{BLOCK_SIZE, BlockRef};
use tracing::{debug, warn};

use super::actor::{Actor, Phase};
use super::data::Job;
use super::readers::ReadReq;
use crate::event::Event;
use crate::peer::messages::PeerCommand;
use crate::picker::PeerKey;
use crate::storage::cache::{CacheBudget, CacheKind};
use crate::storage::{Storage, error_path};

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
            && read.peer_waiters() < MAX_PIECE_READ_WAITERS
        {
            read.waiting.push(Waiter::Peer(key, block));
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
            self.start_piece_read(block.piece, u64::from(size), vec![Waiter::Peer(key, block)]);
            return;
        }
        if self.storage_busy {
            // The files are moving: the request waits in a whole-piece
            // read that starts once they are in place, or is rejected (the
            // peer asks again).
            if size <= WHOLE_PIECE_READ_MAX
                && self.piece_reads.len() < MAX_PIECE_READS
                && !self.piece_reads.contains_key(&block.piece)
            {
                self.start_piece_read(block.piece, 0, vec![Waiter::Peer(key, block)]);
            } else {
                self.send(key, PeerCommand::Reject(block));
            }
            return;
        }
        let offset = l.layout.piece_offset(block.piece) + u64::from(block.offset);
        self.reads_in_flight = self.reads_in_flight.saturating_add(1);
        self.shared.disk.spawn(&mut self.jobs, move || {
            let result = read_guarded(&*storage, offset, block.len as usize);
            Job::Read {
                generation,
                key,
                block,
                result,
            }
        });
    }

    /// Reads `piece` whole for `waiting`, under a clean reservation of
    /// `reserved` bytes (its size, or 0: not kept). While the storage is
    /// busy the read is held, and starts once the operation is done.
    pub(super) fn start_piece_read(&mut self, piece: u32, reserved: u64, waiting: Vec<Waiter>) {
        if self.storage.is_none() || self.loaded.is_none() {
            // Unreachable (storage comes with the metadata). Serving the
            // readers again would come straight back here, and waiting
            // would hang them: they get an error.
            self.cache.release_clean(reserved);
            self.fail_reads(waiting, "the torrent has no storage");
            return;
        }
        let started = !self.storage_busy;
        self.piece_reads.insert(
            piece,
            PieceRead {
                generation: self.generation,
                reserved,
                waiting,
                started,
            },
        );
        if started {
            self.spawn_piece_read(piece);
        }
    }

    /// Starts the disk read of the whole-piece read entry of `piece`.
    pub(super) fn spawn_piece_read(&mut self, piece: u32) {
        let (Some(storage), Some(l)) = (self.storage.as_ref(), self.loaded.as_ref()) else {
            return;
        };
        let storage = Arc::clone(storage);
        let generation = self.generation;
        let size = l.layout.piece_size(piece);
        let offset = l.layout.piece_offset(piece);
        self.reads_in_flight = self.reads_in_flight.saturating_add(1);
        self.shared.disk.spawn(&mut self.jobs, move || {
            let result = read_guarded(&*storage, offset, size as usize);
            Job::ReadPiece {
                generation,
                piece,
                result,
            }
        });
    }

    /// A whole piece was read: kept clean if it was reserved, and every
    /// request waiting on it is served. Whatever stops that sends the
    /// readers among them back to wait or to be served again; no reader's
    /// answer is dropped.
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
        let size = self
            .loaded
            .as_ref()
            .map_or(0, |l| u64::from(l.layout.piece_size(piece)));
        let data: Arc<[u8]> = match result {
            Ok(buf) if buf.len() as u64 == size => buf.into(),
            Ok(_) => {
                self.cache.release_clean(read.reserved);
                self.wait_reads(read.waiting);
                return;
            }
            Err(e) => {
                self.cache.release_clean(read.reserved);
                let current = generation == self.generation && matches!(self.phase, Phase::Ready);
                if e.kind() != io::ErrorKind::NotFound {
                    self.fail(&e, "reading a block");
                    // Failed with the torrent's error.
                    self.serve_reads(read.waiting);
                } else if current {
                    self.piece_lost(piece, &e, read.waiting);
                } else {
                    // A stale read (the torrent was rechecked or stopped
                    // since) says nothing about the piece now.
                    self.serve_reads(read.waiting);
                }
                return;
            }
        };
        if generation != self.generation || !matches!(self.phase, Phase::Ready) {
            self.cache.release_clean(read.reserved);
            self.serve_reads(read.waiting);
            return;
        }
        if read.reserved > 0 {
            self.cache.insert_reserved_clean(piece, Arc::clone(&data));
        }
        for w in read.waiting {
            match w {
                Waiter::Peer(key, block) => {
                    if let Some(bytes) = block_of(&data, block) {
                        self.deliver(key, block, bytes);
                    }
                }
                Waiter::Reader(req) => self.answer_read(req, &data),
            }
        }
    }

    /// Sends a block to a peer that still wants it; the upload counts once
    /// the peer task reports it written ([`Actor::on_block_sent`]).
    fn deliver(&mut self, key: PeerKey, block: BlockRef, data: Bytes) {
        // Choked since the request: the peer task dropped it already.
        if self
            .peers
            .get(&key)
            .is_none_or(|p| p.am_choking || p.tx.is_none())
        {
            return;
        }
        self.send(key, PeerCommand::SendBlock { block, data });
    }

    /// A peer task wrote and flushed `len` bytes of block data: only
    /// these count as uploaded, never blocks cancelled or choked away
    /// while queued.
    pub(super) fn on_block_sent(&mut self, key: PeerKey, len: u64) {
        self.counters.uploaded = self.counters.uploaded.saturating_add(len);
        self.counters.ul.record(len);
        if let Some(p) = self.peers.get_mut(&key) {
            p.to_rate.record(len);
            p.uploaded_since_unchoke = p.uploaded_since_unchoke.saturating_add(len);
        }
    }

    pub(super) fn on_read_block(
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
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                self.piece_lost(block.piece, &e, vec![Waiter::Peer(key, block)]);
            }
            Err(e) => self.fail(&e, "reading a block"),
        }
    }

    /// A piece we have is gone from disk (BEP 54): it is wanted again, our
    /// peers are told, the peer requests waiting on it are rejected (the
    /// readers wait for its download) and
    /// `FileError` reports the file; the torrent keeps running. Every piece
    /// of the failing file we have is given up in the same pass (one
    /// `FileError` per file, one batch of `DontHave`s, one refresh), not
    /// one request at a time. A piece already given up (a second read
    /// failing) is only rejected.
    pub(super) fn piece_lost(&mut self, piece: u32, e: &io::Error, waiting: Vec<Waiter>) {
        let path = error_path(e).map(Path::to_path_buf);
        let range = self.file_pieces(path.as_deref(), piece);
        let mut lost = Vec::new();
        if let Some(pk) = self.loaded.as_mut().and_then(|l| l.picker.as_mut()) {
            for i in range {
                if pk.have().get(i) {
                    pk.piece_lost(i);
                    lost.push(i);
                }
            }
        }
        // Rejected after the `DontHave`s below, so the requester clears its
        // bit before it asks again.
        let keys: Vec<PeerKey> = self
            .peers
            .iter()
            .filter(|(_, p)| p.connected && p.tx.is_some())
            .map(|(k, _)| *k)
            .collect();
        for &i in &lost {
            self.lost_pieces.insert(i);
            for &key in &keys {
                // Optional: a peer whose queue is filling up keeps its
                // stale belief, and its requests are rejected.
                self.send_lossy(key, PeerCommand::DontHave(i));
            }
        }
        for w in waiting {
            match w {
                Waiter::Peer(key, block) => {
                    self.send(key, PeerCommand::Reject(block));
                }
                Waiter::Reader(req) => self.wait_read(req),
            }
        }
        if lost.is_empty() {
            return;
        }
        warn!(torrent = %self.name(), piece, count = lost.len(), error = %e, "pieces lost on disk");
        if path
            .as_ref()
            .is_none_or(|p| self.lost_files.insert(p.clone()))
        {
            self.emit(Event::FileError {
                path,
                torrent: self.ih,
                message: format!("reading a block: {e}"),
            });
        }
        // Wanted again, so the torrent is no longer finished, unless no
        // wanted file overlaps what was lost (a skipped file's piece that a
        // reader fetched): getting it back must not finish it twice.
        if self
            .loaded
            .as_ref()
            .and_then(|l| l.picker.as_ref())
            .is_none_or(|p| !p.is_finished())
        {
            self.finished_emitted = false;
        }
        self.refresh_state();
        for key in keys {
            self.update_interest(key);
        }
        self.top_up_all(None);
    }

    /// The pieces overlapping the torrent file that `path` names (the
    /// longest matching entry, renamed files at their new path), or just
    /// `piece` when it names none.
    fn file_pieces(&self, path: Option<&Path>, piece: u32) -> std::ops::RangeInclusive<u32> {
        let Some(l) = self.loaded.as_ref() else {
            return piece..=piece;
        };
        let plen = u64::from(l.layout.piece_len());
        let mut off = 0u64;
        let mut best: Option<(usize, u64, u64)> = None;
        for (i, f) in l.meta.info.files.iter().enumerate() {
            // Where `rename_file` put it, if it did.
            let rel = l.renamed.get(&i).unwrap_or(&f.path).to_path_buf();
            if !f.pad
                && f.len > 0
                && path.is_some_and(|p| p.ends_with(&rel))
                && best.is_none_or(|(n, _, _)| rel.components().count() > n)
            {
                best = Some((rel.components().count(), off, f.len));
            }
            off = off.saturating_add(f.len);
        }
        match best {
            Some((_, start, len)) if plen > 0 => {
                let first = u32::try_from(start / plen).unwrap_or(piece);
                let last = u32::try_from((start + len - 1) / plen).unwrap_or(piece);
                // Never smaller than the piece that failed.
                first.min(piece)..=last.max(piece)
            }
            _ => piece..=piece,
        }
    }
}

/// Pieces up to this size are read whole on a cache miss; larger ones only
/// the requested block (or a reader's chunk).
pub(super) const WHOLE_PIECE_READ_MAX: u32 = 4 * 1024 * 1024;
/// Whole-piece reads one torrent runs at once; further misses read only
/// their block.
pub(super) const MAX_PIECE_READS: usize = 4;
/// Peer requests waiting on one whole-piece read; further ones read their
/// block. Readers' reads (at most one per reader) do not count.
pub(super) const MAX_PIECE_READ_WAITERS: usize = 256;

/// Who waits on a whole-piece read.
#[derive(Debug)]
pub(super) enum Waiter {
    /// A peer's block request.
    Peer(PeerKey, BlockRef),
    /// A file reader's read.
    Reader(ReadReq),
}

/// A whole-piece read in flight and the requests waiting on it.
#[derive(Debug)]
pub(super) struct PieceRead {
    /// The generation it was started in.
    pub generation: u64,
    /// Clean bytes reserved for the piece before the read; 0 when it is
    /// read without a reservation and not kept.
    pub reserved: u64,
    pub waiting: Vec<Waiter>,
    /// Its disk read was started; a read asked for while the storage is
    /// busy is held until the operation is done.
    pub started: bool,
}

impl PieceRead {
    fn peer_waiters(&self) -> usize {
        self.waiting
            .iter()
            .filter(|w| matches!(w, Waiter::Peer(..)))
            .count()
    }
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

    /// Adds a read; one it replaces gives back its reservation and hands
    /// its waiters to the new one.
    pub(super) fn insert(&mut self, piece: u32, mut read: PieceRead) {
        if let Some(old) = self.reads.remove(&piece) {
            self.budget.release(CacheKind::Clean, old.reserved);
            read.waiting.extend(old.waiting);
        }
        self.reads.insert(piece, read);
    }

    /// The pieces whose reads are held, marked started: the caller starts
    /// them.
    pub(super) fn take_unstarted(&mut self) -> Vec<u32> {
        let mut out = Vec::new();
        for (&piece, r) in &mut self.reads {
            if !r.started {
                r.started = true;
                out.push(piece);
            }
        }
        out
    }

    /// Takes the entry out; its reservation is the caller's now.
    pub(super) fn remove(&mut self, piece: &u32) -> Option<PieceRead> {
        self.reads.remove(piece)
    }

    /// Keeps only the waiters `keep` accepts; the reads go on.
    pub(super) fn retain_waiters(&mut self, mut keep: impl FnMut(&Waiter) -> bool) {
        for r in self.reads.values_mut() {
            r.waiting.retain(&mut keep);
        }
    }

    /// Forgets every read, giving their reservations back, and returns
    /// their waiters; the reads' results are then ignored.
    pub(super) fn take_all(&mut self) -> Vec<Waiter> {
        let mut waiting = Vec::new();
        for (_, r) in self.reads.drain() {
            self.budget.release(CacheKind::Clean, r.reserved);
            waiting.extend(r.waiting);
        }
        waiting
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
    /// back; their results are then ignored. Their peer requests are
    /// dropped; their readers wait again (a check or the download answers
    /// them, a storage error fails them).
    pub(super) fn drop_piece_reads(&mut self) {
        let waiting = self.piece_reads.take_all();
        self.wait_reads(waiting);
    }
}

/// Reads `len` bytes at `offset` on a disk thread. A panicking storage is
/// an I/O error like any other, so the job still comes back with its
/// identity (its waiters, its reader's answer) instead of a bare
/// `JoinError` that leaves them waiting.
pub(super) fn read_guarded(storage: &dyn Storage, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut buf = vec![0u8; len];
        storage.read(offset, &mut buf).map(|()| buf)
    }))
    .unwrap_or_else(|_| Err(io::Error::other("the storage panicked while reading")))
}

/// The bytes of `block` within its piece's `data`.
fn block_of(data: &[u8], block: BlockRef) -> Option<Bytes> {
    let start = block.offset as usize;
    let end = start.checked_add(block.len as usize)?;
    data.get(start..end).map(Bytes::copy_from_slice)
}
