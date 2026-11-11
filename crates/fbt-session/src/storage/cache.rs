//! The write-back cache: a session-wide byte budget ([`CacheBudget`]) and
//! one [`PieceCache`] per torrent.
//!
//! Only verified pieces enter the cache. A piece that passed its hash check
//! is held *dirty* (served to peers from memory, not yet on disk) and later
//! written in contiguous runs ([`WriteRun`]); once written it stays as a
//! *clean* read-cache entry until the budget needs the room. Pieces being
//! downloaded hold a *partial* reservation for their buffer. The budget
//! covers all three kinds together; the only bytes beyond it are each
//! torrent's [`PROGRESS_FLOOR_PIECES`] floor (see
//! [`Settings::cache_size`](crate::Settings::cache_size)).
//!
//! [`PieceCache`] is pure bookkeeping: it plans the writes and the torrent
//! actor runs them through the disk pool.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Partial-piece buffers a downloading torrent may hold beyond the budget,
/// so it keeps making progress while other torrents fill the cache; granted
/// only while the session's dirty bytes (in-flight writes included) are
/// within `cache_size`.
pub(crate) const PROGRESS_FLOOR_PIECES: usize = 4;

/// Longest coalesced write: a run stops growing at this many bytes (it
/// always holds at least one piece), which bounds the buffer the write job
/// copies the run into.
const MAX_RUN_BYTES: u64 = 4 * 1024 * 1024;

/// What a cached byte is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CacheKind {
    /// A buffer of a piece being downloaded (never readable by peers).
    Partial,
    /// A verified piece not yet on disk (being written included).
    Dirty,
    /// A verified piece that is on disk too.
    Clean,
}

/// Bytes the session's cache holds, from
/// [`Session::cache_stats`](crate::Session::cache_stats).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CacheStats {
    /// Verified pieces not yet on disk, writes in flight included.
    pub dirty: u64,
    /// Written pieces kept to serve peers.
    pub clean: u64,
    /// Buffers of pieces being downloaded.
    pub partial: u64,
    /// [`Settings::cache_size`](crate::Settings::cache_size).
    pub limit: u64,
}

/// The session-wide byte budget of every torrent's cache. Lock-free: each
/// counter is atomic and `total` (the sum of the three kinds) is what
/// [`try_reserve`](Self::try_reserve) checks against the limit.
#[derive(Debug)]
pub(crate) struct CacheBudget {
    limit: u64,
    total: AtomicU64,
    partial: AtomicU64,
    dirty: AtomicU64,
    clean: AtomicU64,
    /// Dirty bytes whose write is in flight (part of `dirty`).
    writing: AtomicU64,
}

impl CacheBudget {
    pub(crate) fn new(limit: u64) -> Self {
        CacheBudget {
            limit,
            total: AtomicU64::new(0),
            partial: AtomicU64::new(0),
            dirty: AtomicU64::new(0),
            clean: AtomicU64::new(0),
            writing: AtomicU64::new(0),
        }
    }

    fn counter(&self, kind: CacheKind) -> &AtomicU64 {
        match kind {
            CacheKind::Partial => &self.partial,
            CacheKind::Dirty => &self.dirty,
            CacheKind::Clean => &self.clean,
        }
    }

    pub(crate) fn limit(&self) -> u64 {
        self.limit
    }

    /// Takes `n` bytes of `kind` if the total stays within the limit.
    pub(crate) fn try_reserve(&self, kind: CacheKind, n: u64) -> bool {
        self.try_reserve_within(kind, n, self.limit)
    }

    /// Takes `n` bytes of `kind` if the total stays within `max`.
    fn try_reserve_within(&self, kind: CacheKind, n: u64, max: u64) -> bool {
        let ok = self
            .total
            .try_update(Ordering::AcqRel, Ordering::Acquire, |t| {
                t.checked_add(n).filter(|&new| new <= max)
            })
            .is_ok();
        if ok {
            self.counter(kind).fetch_add(n, Ordering::AcqRel);
        }
        ok
    }

    /// Takes `n` bytes of `kind` whatever the limit.
    fn force(&self, kind: CacheKind, n: u64) {
        self.total.fetch_add(n, Ordering::AcqRel);
        self.counter(kind).fetch_add(n, Ordering::AcqRel);
    }

    /// Gives back `n` bytes of `kind`.
    pub(crate) fn release(&self, kind: CacheKind, n: u64) {
        sub(&self.total, n);
        sub(self.counter(kind), n);
    }

    /// `n` bytes change kind; the total stays.
    fn convert(&self, from: CacheKind, to: CacheKind, n: u64) {
        sub(self.counter(from), n);
        self.counter(to).fetch_add(n, Ordering::AcqRel);
    }

    fn total(&self) -> u64 {
        self.total.load(Ordering::Acquire)
    }

    fn dirty(&self) -> u64 {
        self.dirty.load(Ordering::Acquire)
    }

    /// Dirty bytes no write has been started for yet.
    fn pending_dirty(&self) -> u64 {
        self.dirty()
            .saturating_sub(self.writing.load(Ordering::Acquire))
    }

    pub(crate) fn stats(&self) -> CacheStats {
        CacheStats {
            dirty: self.dirty(),
            clean: self.clean.load(Ordering::Acquire),
            partial: self.partial.load(Ordering::Acquire),
            limit: self.limit,
        }
    }
}

/// Subtracts without wrapping; a release of more than was reserved is a
/// bookkeeping bug.
fn sub(c: &AtomicU64, n: u64) {
    let r = c.try_update(Ordering::AcqRel, Ordering::Acquire, |v| {
        Some(v.saturating_sub(n))
    });
    debug_assert!(
        r.is_ok_and(|v| v >= n),
        "cache budget released more than it held"
    );
}

/// Why dirty pieces are written now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlushReason {
    /// Every dirty piece: finished, paused, stopping, before a recheck.
    All,
    /// The session's dirty bytes passed half the budget: the oldest runs,
    /// until a quarter is left.
    Pressure,
    /// Pieces dirty for longer than `cache_expiry`.
    Expired,
}

/// Contiguous dirty pieces written by one `Storage::write`.
#[derive(Debug)]
pub(crate) struct WriteRun {
    pub first: u32,
    pub pieces: Vec<(u32, Arc<[u8]>)>,
}

impl WriteRun {
    pub(crate) fn len_bytes(&self) -> u64 {
        self.pieces.iter().map(|(_, d)| d.len() as u64).sum()
    }
}

#[derive(Debug)]
struct Dirty {
    data: Arc<[u8]>,
    since: Instant,
}

#[derive(Debug)]
struct Clean {
    data: Arc<[u8]>,
    /// Key into `PieceCache::lru`.
    used: u64,
}

#[derive(Debug, Clone, Copy)]
struct Reservation {
    bytes: u64,
    /// Granted from the progress floor, beyond the budget.
    floor: bool,
}

/// One torrent's share of the cache. Every byte it holds is reserved in the
/// shared [`CacheBudget`] and given back when it leaves, or at the latest
/// when the cache is dropped.
#[derive(Debug)]
pub(crate) struct PieceCache {
    budget: Arc<CacheBudget>,
    expiry: Duration,
    /// Verified, not yet handed to a write.
    dirty: BTreeMap<u32, Dirty>,
    /// Verified, its write in flight: still served from here.
    writing: BTreeMap<u32, Arc<[u8]>>,
    clean: HashMap<u32, Clean>,
    /// Clean pieces by last use, oldest first.
    lru: BTreeMap<u64, u32>,
    next_use: u64,
    partial: HashMap<u32, Reservation>,
}

impl PieceCache {
    pub(crate) fn new(budget: Arc<CacheBudget>, expiry: Duration) -> Self {
        PieceCache {
            budget,
            expiry,
            dirty: BTreeMap::new(),
            writing: BTreeMap::new(),
            clean: HashMap::new(),
            lru: BTreeMap::new(),
            next_use: 0,
            partial: HashMap::new(),
        }
    }

    /// A verified piece: held dirty until written. Its partial reservation,
    /// if any, becomes the dirty bytes; clean entries make room if the
    /// budget is over.
    pub(crate) fn insert_dirty(&mut self, piece: u32, data: Arc<[u8]>, now: Instant) {
        self.release_partial(piece);
        if self.dirty.contains_key(&piece) || self.writing.contains_key(&piece) {
            return;
        }
        self.remove_clean(piece);
        self.budget.force(CacheKind::Dirty, data.len() as u64);
        self.dirty.insert(piece, Dirty { data, since: now });
        self.trim_clean(self.budget.limit());
    }

    /// The piece's verified bytes, dirty or clean.
    pub(crate) fn get(&mut self, piece: u32) -> Option<Arc<[u8]>> {
        if let Some(d) = self.dirty.get(&piece) {
            return Some(Arc::clone(&d.data));
        }
        if let Some(d) = self.writing.get(&piece) {
            return Some(Arc::clone(d));
        }
        let used = self.next_use;
        let c = self.clean.get_mut(&piece)?;
        self.lru.remove(&c.used);
        c.used = used;
        self.lru.insert(used, piece);
        self.next_use = self.next_use.wrapping_add(1);
        Some(Arc::clone(&c.data))
    }

    /// Whether a piece read from disk would be kept: the budget has room
    /// for `n` bytes, counting this torrent's clean entries as room.
    pub(crate) fn can_hold_clean(&self, n: u64) -> bool {
        let own: u64 = self.clean.values().map(|c| c.data.len() as u64).sum();
        self.budget.limit() > 0
            && self.budget.total().saturating_sub(own).saturating_add(n) <= self.budget.limit()
    }

    /// A piece read from disk, kept to serve peers: least recently used
    /// clean entries of this torrent make room; nothing is kept if the
    /// budget still has none.
    pub(crate) fn insert_clean(&mut self, piece: u32, data: Arc<[u8]>, _now: Instant) {
        if self.dirty.contains_key(&piece)
            || self.writing.contains_key(&piece)
            || self.clean.contains_key(&piece)
        {
            return;
        }
        let n = data.len() as u64;
        while !self.budget.try_reserve(CacheKind::Clean, n) {
            if !self.evict_lru() {
                return;
            }
        }
        self.push_clean(piece, data);
    }

    /// Hands dirty pieces to writes, in contiguous runs, oldest run first.
    /// They stay readable until [`written`](Self::written).
    pub(crate) fn take_flush(&mut self, reason: FlushReason, now: Instant) -> Vec<WriteRun> {
        let pieces: Vec<u32> = match reason {
            FlushReason::All | FlushReason::Pressure => self.dirty.keys().copied().collect(),
            FlushReason::Expired => self
                .dirty
                .iter()
                .filter(|(_, d)| now.saturating_duration_since(d.since) >= self.expiry)
                .map(|(&i, _)| i)
                .collect(),
        };
        let limit = self.budget.limit();
        if reason == FlushReason::Pressure && self.budget.pending_dirty() <= limit / 2 {
            return Vec::new();
        }
        let mut runs = self.runs_of(&pieces);
        let mut out = Vec::new();
        for (first, members, _) in runs.drain(..) {
            if reason == FlushReason::Pressure && self.budget.pending_dirty() <= limit / 4 {
                break;
            }
            let mut run = WriteRun {
                first,
                pieces: Vec::with_capacity(members.len()),
            };
            for i in members {
                if let Some(d) = self.dirty.remove(&i) {
                    self.budget
                        .writing
                        .fetch_add(d.data.len() as u64, Ordering::AcqRel);
                    self.writing.insert(i, Arc::clone(&d.data));
                    run.pieces.push((i, d.data));
                }
            }
            out.push(run);
        }
        out
    }

    /// `pieces` (ascending) cut into contiguous runs of at most
    /// [`MAX_RUN_BYTES`], oldest (by its oldest piece) first.
    fn runs_of(&self, pieces: &[u32]) -> Vec<(u32, Vec<u32>, Instant)> {
        let mut runs: Vec<(u32, Vec<u32>, Instant)> = Vec::new();
        let mut bytes = 0u64;
        for &i in pieces {
            let Some(d) = self.dirty.get(&i) else {
                continue;
            };
            let n = d.data.len() as u64;
            match runs.last_mut() {
                Some((_, members, oldest))
                    if members.last().is_some_and(|&l| l.checked_add(1) == Some(i))
                        && bytes.saturating_add(n) <= MAX_RUN_BYTES =>
                {
                    members.push(i);
                    *oldest = (*oldest).min(d.since);
                    bytes = bytes.saturating_add(n);
                }
                _ => {
                    runs.push((i, vec![i], d.since));
                    bytes = n;
                }
            }
        }
        runs.sort_by_key(|r| (r.2, r.0));
        runs
    }

    /// The write of `count` pieces from `first` landed: they become clean
    /// (and are trimmed if the budget is over). Pieces no longer tracked
    /// (discarded meanwhile) are skipped.
    pub(crate) fn written(&mut self, first: u32, count: u32) {
        for i in (0..count).filter_map(|k| first.checked_add(k)) {
            let Some(data) = self.writing.remove(&i) else {
                continue;
            };
            let n = data.len() as u64;
            sub(&self.budget.writing, n);
            self.budget.convert(CacheKind::Dirty, CacheKind::Clean, n);
            self.push_clean(i, data);
        }
        self.trim_clean(self.budget.limit());
    }

    /// Drops every piece and reservation without writing anything, giving
    /// all of its bytes back to the budget. Writes in flight still land;
    /// [`written`](Self::written) then ignores them.
    pub(crate) fn discard_all(&mut self) {
        for (_, d) in std::mem::take(&mut self.dirty) {
            self.budget.release(CacheKind::Dirty, d.data.len() as u64);
        }
        for (_, d) in std::mem::take(&mut self.writing) {
            let n = d.len() as u64;
            sub(&self.budget.writing, n);
            self.budget.release(CacheKind::Dirty, n);
        }
        while self.evict_lru() {}
        self.release_partials();
    }

    /// Nothing dirty and no write in flight.
    pub(crate) fn is_idle(&self) -> bool {
        self.dirty.is_empty() && self.writing.is_empty()
    }

    /// A write handed out by [`take_flush`](Self::take_flush) has not
    /// landed yet.
    pub(crate) fn writes_in_flight(&self) -> bool {
        !self.writing.is_empty()
    }

    // ----- partial buffers ------------------------------------------------

    /// Reserves `n` bytes for the buffer of `piece`, about to be
    /// downloaded: from the budget (this torrent's clean entries make room
    /// first), else from the progress floor. The floor grants at most
    /// [`PROGRESS_FLOOR_PIECES`] buffers, never more than that many pieces
    /// beyond the budget, and only while the session's dirty bytes (writes
    /// in flight included) are within the budget: a slow disk stops it.
    pub(crate) fn reserve_partial(&mut self, piece: u32, n: u64) -> bool {
        if self.partial.contains_key(&piece) {
            return true;
        }
        loop {
            if self.budget.try_reserve(CacheKind::Partial, n) {
                self.partial.insert(
                    piece,
                    Reservation {
                        bytes: n,
                        floor: false,
                    },
                );
                return true;
            }
            if !self.evict_lru() {
                break;
            }
        }
        let floor_used = self.partial.values().filter(|r| r.floor).count();
        let limit = self.budget.limit();
        let floor_max = limit.saturating_add(n.saturating_mul(PROGRESS_FLOOR_PIECES as u64));
        if floor_used < PROGRESS_FLOOR_PIECES
            && self.budget.dirty() <= limit
            && self
                .budget
                .try_reserve_within(CacheKind::Partial, n, floor_max)
        {
            self.partial.insert(
                piece,
                Reservation {
                    bytes: n,
                    floor: true,
                },
            );
            return true;
        }
        false
    }

    /// The buffer of `piece` is gone (verified, failed or dropped).
    pub(crate) fn release_partial(&mut self, piece: u32) {
        if let Some(r) = self.partial.remove(&piece) {
            self.budget.release(CacheKind::Partial, r.bytes);
        }
    }

    pub(crate) fn has_partial(&self, piece: u32) -> bool {
        self.partial.contains_key(&piece)
    }

    /// Keeps the reservations of pieces still in progress only.
    pub(crate) fn retain_partials(&mut self, mut keep: impl FnMut(u32) -> bool) {
        let gone: Vec<u32> = self.partial.keys().copied().filter(|&p| !keep(p)).collect();
        for p in gone {
            self.release_partial(p);
        }
    }

    /// Drops every partial reservation.
    pub(crate) fn release_partials(&mut self) {
        for (_, r) in self.partial.drain() {
            self.budget.release(CacheKind::Partial, r.bytes);
        }
    }

    /// About how many more partial buffers of `n` bytes this torrent could
    /// reserve now (budget room, its own clean entries and the floor).
    pub(crate) fn partial_room(&self, n: u64) -> usize {
        let n = n.max(1);
        let limit = self.budget.limit();
        let own: u64 = self.clean.values().map(|c| c.data.len() as u64).sum();
        let free = limit
            .saturating_sub(self.budget.total())
            .saturating_add(own);
        let floor_used = self.partial.values().filter(|r| r.floor).count();
        let floor = if self.budget.dirty() <= limit {
            PROGRESS_FLOOR_PIECES.saturating_sub(floor_used)
        } else {
            0
        };
        usize::try_from(free / n)
            .unwrap_or(usize::MAX)
            .saturating_add(floor)
    }

    // ----- clean entries --------------------------------------------------

    fn push_clean(&mut self, piece: u32, data: Arc<[u8]>) {
        let used = self.next_use;
        self.next_use = self.next_use.wrapping_add(1);
        self.lru.insert(used, piece);
        self.clean.insert(piece, Clean { data, used });
    }

    fn remove_clean(&mut self, piece: u32) {
        if let Some(c) = self.clean.remove(&piece) {
            self.lru.remove(&c.used);
            self.budget.release(CacheKind::Clean, c.data.len() as u64);
        }
    }

    /// Evicts the least recently used clean entry; false with none.
    fn evict_lru(&mut self) -> bool {
        let Some((_, piece)) = self.lru.pop_first() else {
            return false;
        };
        if let Some(c) = self.clean.remove(&piece) {
            self.budget.release(CacheKind::Clean, c.data.len() as u64);
        }
        true
    }

    /// Evicts clean entries while the budget's total is above `max`.
    fn trim_clean(&mut self, max: u64) {
        while self.budget.total() > max && self.evict_lru() {}
    }
}

impl Drop for PieceCache {
    /// Whatever the torrent still holds goes back to the session's budget.
    fn drop(&mut self) {
        self.discard_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const P: u64 = 16_384;

    fn piece(i: u32) -> Arc<[u8]> {
        vec![i as u8; P as usize].into()
    }

    fn cache(limit: u64) -> (Arc<CacheBudget>, PieceCache) {
        let budget = Arc::new(CacheBudget::new(limit));
        let c = PieceCache::new(Arc::clone(&budget), Duration::from_secs(30));
        (budget, c)
    }

    fn shape(runs: &[WriteRun]) -> Vec<Vec<u32>> {
        runs.iter()
            .map(|r| r.pieces.iter().map(|(i, _)| *i).collect())
            .collect()
    }

    #[test]
    fn pressure_flushes_oldest_contiguous_runs() {
        let (budget, mut c) = cache(3 * P);
        let t0 = Instant::now();
        // Piece 2 is the oldest, so its run goes first.
        c.insert_dirty(2, piece(2), t0);
        c.insert_dirty(7, piece(7), t0 + Duration::from_secs(1));
        c.insert_dirty(1, piece(1), t0 + Duration::from_secs(2));
        c.insert_dirty(3, piece(3), t0 + Duration::from_secs(3));
        assert_eq!(budget.stats().dirty, 4 * P);
        let runs = c.take_flush(FlushReason::Pressure, t0 + Duration::from_secs(4));
        assert_eq!(shape(&runs), [vec![1, 2, 3], vec![7]]);
        assert_eq!(runs[0].first, 1);
        assert_eq!(runs[1].first, 7);
        // Still readable while the writes run, and still dirty.
        assert!(c.get(2).is_some());
        assert_eq!(budget.stats().dirty, 4 * P);
        assert!(c.writes_in_flight());
        // Below the threshold now: nothing more to write.
        assert!(c.take_flush(FlushReason::Pressure, t0).is_empty());

        // With room left, pressure does nothing.
        let (_b, mut roomy) = cache(100 * P);
        roomy.insert_dirty(1, piece(1), t0);
        assert!(roomy.take_flush(FlushReason::Pressure, t0).is_empty());
    }

    #[test]
    fn expiry_flushes_only_old_pieces() {
        let (_b, mut c) = cache(100 * P);
        let t0 = Instant::now();
        c.insert_dirty(1, piece(1), t0);
        c.insert_dirty(2, piece(2), t0 + Duration::from_secs(20));
        c.insert_dirty(5, piece(5), t0 + Duration::from_secs(1));
        assert!(
            c.take_flush(FlushReason::Expired, t0 + Duration::from_secs(29))
                .is_empty()
        );
        let runs = c.take_flush(FlushReason::Expired, t0 + Duration::from_secs(31));
        // Piece 2 is adjacent to 1 but not old: it stays.
        assert_eq!(shape(&runs), [vec![1], vec![5]]);
        let rest = c.take_flush(FlushReason::All, t0 + Duration::from_secs(31));
        assert_eq!(shape(&rest), [vec![2]]);
        assert!(c.take_flush(FlushReason::All, t0).is_empty());
    }

    #[test]
    fn clean_entries_evicted_before_dirty_are_flushed() {
        let (budget, mut c) = cache(4 * P);
        let t0 = Instant::now();
        for i in 10..13 {
            c.insert_clean(i, piece(i), t0);
        }
        assert_eq!(budget.stats().clean, 3 * P);
        // Piece 11 was used last: 10 is the oldest.
        assert!(c.get(10).is_some());
        assert!(c.get(11).is_some());
        c.insert_dirty(1, piece(1), t0);
        c.insert_dirty(2, piece(2), t0);
        // Over the budget by one piece: the least recently used clean
        // entry went, no dirty piece was written.
        assert!(c.get(12).is_none());
        assert!(c.get(10).is_some() && c.get(11).is_some());
        assert_eq!(budget.stats().clean, 2 * P);
        assert_eq!(budget.stats().dirty, 2 * P);
        assert!(c.take_flush(FlushReason::Pressure, t0).is_empty());
        assert!(c.get(1).is_some() && c.get(2).is_some());
        // A clean entry only goes in within the budget.
        c.insert_clean(20, piece(20), t0);
        assert_eq!(budget.stats().clean + budget.stats().dirty, 4 * P);
    }

    #[test]
    fn zero_size_is_write_through() {
        let (budget, mut c) = cache(0);
        let t0 = Instant::now();
        c.insert_dirty(3, piece(3), t0);
        // Served from memory until written, but written at once.
        assert!(c.get(3).is_some());
        let runs = c.take_flush(FlushReason::Pressure, t0);
        assert_eq!(shape(&runs), [vec![3]]);
        c.written(3, 1);
        // No read cache.
        assert!(c.get(3).is_none());
        c.insert_clean(4, piece(4), t0);
        assert!(c.get(4).is_none());
        assert!(!c.can_hold_clean(P));
        assert_eq!(budget.stats(), CacheStats::default());
        // Downloads still progress on the floor.
        assert!(c.reserve_partial(5, P));
        assert_eq!(budget.stats().partial, P);
    }

    #[test]
    fn budget_counts_all_three_kinds() {
        let (budget, mut c) = cache(4 * P);
        let t0 = Instant::now();
        assert!(c.reserve_partial(1, P));
        c.insert_dirty(2, piece(2), t0);
        c.insert_clean(3, piece(3), t0);
        assert_eq!(
            budget.stats(),
            CacheStats {
                dirty: P,
                clean: P,
                partial: P,
                limit: 4 * P,
            }
        );
        // One piece of room left.
        assert!(budget.try_reserve(CacheKind::Partial, P));
        assert!(!budget.try_reserve(CacheKind::Partial, 1));
        budget.release(CacheKind::Partial, P);
        // The partial buffer becomes the dirty piece.
        c.insert_dirty(1, piece(1), t0);
        assert_eq!(budget.stats().partial, 0);
        assert_eq!(budget.stats().dirty, 2 * P);
        let runs = c.take_flush(FlushReason::All, t0);
        assert_eq!(shape(&runs), [vec![1, 2]]);
        c.written(1, 2);
        assert_eq!(budget.stats().dirty, 0);
        assert_eq!(budget.stats().clean, 3 * P);
        // Everything goes back, by discard or by drop.
        assert!(c.reserve_partial(9, P));
        c.discard_all();
        assert_eq!(budget.stats().dirty + budget.stats().clean, 0);
        assert_eq!(budget.stats().partial, 0);
        assert!(c.reserve_partial(9, P));
        c.insert_dirty(8, piece(8), t0);
        drop(c);
        assert_eq!(
            budget.stats(),
            CacheStats {
                limit: 4 * P,
                ..CacheStats::default()
            }
        );
    }

    #[test]
    fn progress_floor_stops_while_writes_lag() {
        let (budget, mut c) = cache(4 * P);
        let t0 = Instant::now();
        for i in 0..5 {
            c.insert_dirty(i, piece(i), t0);
        }
        // Dirty plus in flight above the budget: no floor buffer.
        let runs = c.take_flush(FlushReason::All, t0);
        assert_eq!(shape(&runs), [vec![0, 1, 2, 3, 4]]);
        assert!(budget.stats().dirty > budget.limit());
        assert!(!c.reserve_partial(10, P));
        assert_eq!(c.partial_room(P), 0);
        // The write lands: the room is back.
        c.written(0, 5);
        assert!(c.reserve_partial(10, P));
        assert!(c.has_partial(10));

        // The floor: four buffers beyond a full budget, no more.
        let (budget, mut c) = cache(2 * P);
        let mut other = PieceCache::new(Arc::clone(&budget), Duration::from_secs(30));
        assert!(other.reserve_partial(0, P) && other.reserve_partial(1, P));
        for i in 0..4 {
            assert!(c.reserve_partial(i, P), "floor buffer {i}");
        }
        assert!(!c.reserve_partial(4, P));
        let s = budget.stats();
        assert_eq!(s.partial, 2 * P + 4 * P);
        c.release_partial(0);
        assert!(c.reserve_partial(4, P));
        c.retain_partials(|_| false);
        other.release_partials();
        assert_eq!(budget.stats().partial, 0);
    }
}
