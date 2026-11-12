//! The write-back cache: a session-wide byte budget ([`CacheBudget`], which
//! also holds the session's clean read cache) and one [`PieceCache`] per
//! torrent.
//!
//! Only verified pieces enter the cache. A piece that passed its hash check
//! is held *dirty* (served to peers from memory, not yet on disk) and later
//! written in contiguous runs ([`WriteRun`]); once written it becomes a
//! *clean* read-cache entry until the budget needs the room. Clean entries
//! of every torrent share one least-recently-used order, so whichever
//! torrent needs room evicts the session's oldest entry. Pieces being
//! downloaded hold a *partial* reservation for their buffer. The budget
//! covers all three kinds together; the only bytes beyond it are each
//! downloading torrent's [`PROGRESS_FLOOR_PIECES`] floor (see
//! [`Settings::cache_size`](crate::Settings::cache_size)).
//!
//! [`PieceCache`] is pure bookkeeping: it plans the writes and the torrent
//! actor runs them through the disk pool.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// Partial-piece buffers a downloading torrent may hold beyond the budget,
/// so it keeps making progress while the cache is full; granted only while
/// the session's dirty bytes (in-flight writes included) are within
/// `cache_size`. A floor piece counts against its torrent's floor until it
/// is written.
pub(crate) const PROGRESS_FLOOR_PIECES: usize = 4;

/// Longest coalesced write: a run stops growing at this many bytes (it
/// always holds at least one piece), so one write never holds a disk thread
/// for long.
const MAX_RUN_BYTES: u64 = 4 * 1024 * 1024;

/// What a cached byte is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CacheKind {
    /// A buffer of a piece being downloaded (never readable by peers).
    Partial,
    /// A verified piece not yet on disk (being written included).
    Dirty,
    /// A verified piece that is on disk too, or a whole-piece read in
    /// flight that will become one.
    Clean,
}

/// Bytes the session's cache holds, from
/// [`Session::cache_stats`](crate::Session::cache_stats).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CacheStats {
    /// Verified pieces not yet on disk, writes in flight included.
    pub dirty: u64,
    /// Written pieces kept to serve peers (and whole-piece reads in flight).
    pub clean: u64,
    /// Buffers of pieces being downloaded.
    pub partial: u64,
    /// [`Settings::cache_size`](crate::Settings::cache_size).
    pub limit: u64,
}

/// A clean entry's key: the torrent's cache id and the piece.
type CleanKey = (u64, u32);

/// Where use stamps start: every demoted entry's stamp (counting up from
/// 0) sorts before every used one's.
const FIRST_USE: u64 = 1 << 62;

/// The session's clean entries, least recently used first.
#[derive(Debug)]
struct CleanLru {
    entries: HashMap<CleanKey, (Arc<[u8]>, u64)>,
    /// Last use -> key.
    order: BTreeMap<u64, CleanKey>,
    by_torrent: HashMap<u64, HashSet<u32>>,
    /// Stamp of the next use, from [`FIRST_USE`].
    next_use: u64,
    /// Stamp of the next demotion, from 0: demoted entries go first, the
    /// earliest demoted first.
    next_cold: u64,
}

impl Default for CleanLru {
    fn default() -> Self {
        CleanLru {
            entries: HashMap::new(),
            order: BTreeMap::new(),
            by_torrent: HashMap::new(),
            next_use: FIRST_USE,
            next_cold: 0,
        }
    }
}

impl CleanLru {
    fn tick(&mut self) -> u64 {
        let t = self.next_use;
        self.next_use = self.next_use.wrapping_add(1);
        t
    }

    /// `evictable` (the budget's mirror of the bytes in here) changes
    /// under the same lock as the entries.
    fn insert(&mut self, key: CleanKey, data: Arc<[u8]>, evictable: &AtomicU64) -> bool {
        if self.entries.contains_key(&key) {
            return false;
        }
        evictable.fetch_add(data.len() as u64, Ordering::AcqRel);
        let used = self.tick();
        self.order.insert(used, key);
        self.by_torrent.entry(key.0).or_default().insert(key.1);
        self.entries.insert(key, (data, used));
        true
    }

    /// Makes `key` the next eviction after the entries demoted before it.
    fn demote(&mut self, key: CleanKey) {
        let cold = self.next_cold;
        let Some((_, at)) = self.entries.get_mut(&key) else {
            return;
        };
        let old = std::mem::replace(at, cold);
        self.next_cold = self.next_cold.wrapping_add(1);
        self.order.remove(&old);
        self.order.insert(cold, key);
    }

    fn remove(&mut self, key: CleanKey, evictable: &AtomicU64) -> Option<Arc<[u8]>> {
        let (data, used) = self.entries.remove(&key)?;
        sub(evictable, data.len() as u64);
        self.order.remove(&used);
        if let Some(set) = self.by_torrent.get_mut(&key.0) {
            set.remove(&key.1);
            if set.is_empty() {
                self.by_torrent.remove(&key.0);
            }
        }
        Some(data)
    }

    fn pop_oldest(&mut self, evictable: &AtomicU64) -> Option<Arc<[u8]>> {
        let (_, key) = self.order.first_key_value()?;
        let key = *key;
        self.remove(key, evictable)
    }
}

/// The session-wide byte budget of every torrent's cache, and the session's
/// clean read cache. The counters are atomic and `total` (the sum of the
/// three kinds) is what [`try_reserve`](Self::try_reserve) checks against
/// the limit. The clean entries sit behind one mutex, held only for map
/// updates: never across I/O or another lock.
#[derive(Debug)]
pub(crate) struct CacheBudget {
    limit: u64,
    total: AtomicU64,
    partial: AtomicU64,
    dirty: AtomicU64,
    clean: AtomicU64,
    /// Dirty bytes whose write is in flight (part of `dirty`).
    writing: AtomicU64,
    /// Bytes of the entries in `lru` (part of `clean`): what eviction can
    /// free.
    evictable: AtomicU64,
    lru: Mutex<CleanLru>,
    next_id: AtomicU64,
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
            evictable: AtomicU64::new(0),
            lru: Mutex::new(CleanLru::default()),
            next_id: AtomicU64::new(0),
        }
    }

    fn counter(&self, kind: CacheKind) -> &AtomicU64 {
        match kind {
            CacheKind::Partial => &self.partial,
            CacheKind::Dirty => &self.dirty,
            CacheKind::Clean => &self.clean,
        }
    }

    fn lru(&self) -> MutexGuard<'_, CleanLru> {
        self.lru.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn limit(&self) -> u64 {
        self.limit
    }

    /// Takes `n` bytes of `kind` if the total stays within the limit,
    /// evicting the session's least recently used clean entries for room;
    /// none is evicted when even all of them would not make enough.
    pub(crate) fn try_reserve(&self, kind: CacheKind, n: u64) -> bool {
        loop {
            let ok = self
                .total
                .try_update(Ordering::AcqRel, Ordering::Acquire, |t| {
                    t.checked_add(n).filter(|&new| new <= self.limit)
                })
                .is_ok();
            if ok {
                self.counter(kind).fetch_add(n, Ordering::AcqRel);
                return true;
            }
            let fixed = self
                .total()
                .saturating_sub(self.evictable.load(Ordering::Acquire));
            if self.limit.saturating_sub(fixed) < n || !self.evict_oldest() {
                return false;
            }
        }
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

    /// Bytes a new reservation could get now, evicting clean entries.
    fn room(&self) -> u64 {
        let fixed = self
            .total()
            .saturating_sub(self.evictable.load(Ordering::Acquire));
        self.limit.saturating_sub(fixed)
    }

    pub(crate) fn stats(&self) -> CacheStats {
        CacheStats {
            dirty: self.dirty(),
            clean: self.clean.load(Ordering::Acquire),
            partial: self.partial.load(Ordering::Acquire),
            limit: self.limit,
        }
    }

    // ----- clean entries --------------------------------------------------

    /// Evicts the session's least recently used clean entry; false with
    /// none.
    fn evict_oldest(&self) -> bool {
        let popped = self.lru().pop_oldest(&self.evictable);
        let Some(data) = popped else {
            return false;
        };
        self.forget_clean(data.len() as u64);
        true
    }

    /// An entry of `n` bytes left the LRU (`evictable` already went down
    /// under the lock).
    fn forget_clean(&self, n: u64) {
        self.release(CacheKind::Clean, n);
    }

    fn clean_get(&self, key: CleanKey) -> Option<Arc<[u8]>> {
        let mut lru = self.lru();
        let used = lru.tick();
        let (data, at) = lru.entries.get_mut(&key)?;
        let old = std::mem::replace(at, used);
        let data = Arc::clone(data);
        lru.order.remove(&old);
        lru.order.insert(used, key);
        Some(data)
    }

    fn clean_demote(&self, key: CleanKey) {
        self.lru().demote(key);
    }

    fn clean_contains(&self, key: CleanKey) -> bool {
        self.lru().entries.contains_key(&key)
    }

    /// Adds an entry whose bytes are already reserved as `Clean`; a
    /// duplicate gives them back.
    fn clean_insert_reserved(&self, key: CleanKey, data: Arc<[u8]>) {
        let n = data.len() as u64;
        let inserted = self.lru().insert(key, data, &self.evictable);
        if !inserted {
            self.release(CacheKind::Clean, n);
        }
        self.trim();
    }

    fn clean_remove(&self, key: CleanKey) {
        let removed = self.lru().remove(key, &self.evictable);
        if let Some(data) = removed {
            self.forget_clean(data.len() as u64);
        }
    }

    /// Drops every clean entry of torrent `id`.
    fn purge(&self, id: u64) {
        let removed: Vec<Arc<[u8]>> = {
            let mut lru = self.lru();
            let pieces = lru.by_torrent.remove(&id).unwrap_or_default();
            pieces
                .into_iter()
                .filter_map(|p| lru.remove((id, p), &self.evictable))
                .collect()
        };
        for data in removed {
            self.forget_clean(data.len() as u64);
        }
    }

    /// Evicts clean entries while the total is above the limit.
    fn trim(&self) {
        while self.total() > self.limit && self.evict_oldest() {}
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
    /// The torrent runs on its progress floor (or the budget is over):
    /// every dirty piece, so the floor comes back after one write.
    Floor,
}

/// Contiguous dirty pieces written by one `Storage::write_vectored`.
#[derive(Debug)]
pub(crate) struct WriteRun {
    pub first: u32,
    pub pieces: Vec<(u32, Arc<[u8]>)>,
}

#[derive(Debug)]
struct Dirty {
    data: Arc<[u8]>,
    since: Instant,
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
    /// Key of this torrent's clean entries in the session LRU.
    id: u64,
    budget: Arc<CacheBudget>,
    expiry: Duration,
    /// Verified, not yet handed to a write.
    dirty: BTreeMap<u32, Dirty>,
    /// Verified, its write in flight: still served from here.
    writing: BTreeMap<u32, Arc<[u8]>>,
    /// Bytes of discarded pieces whose write is still in flight: counted
    /// as dirty until it lands, since the write job still holds them.
    orphaned: HashMap<u32, u64>,
    partial: HashMap<u32, Reservation>,
    /// Pieces granted from the floor: partial, dirty or being written.
    floor: HashSet<u32>,
}

impl PieceCache {
    pub(crate) fn new(budget: Arc<CacheBudget>, expiry: Duration) -> Self {
        let id = budget.next_id.fetch_add(1, Ordering::Relaxed);
        PieceCache {
            id,
            budget,
            expiry,
            dirty: BTreeMap::new(),
            writing: BTreeMap::new(),
            orphaned: HashMap::new(),
            partial: HashMap::new(),
            floor: HashSet::new(),
        }
    }

    /// A verified piece: held dirty until written. Its partial reservation,
    /// if any, becomes the dirty bytes (a floor piece stays on the floor
    /// until written); clean entries make room if the budget is over.
    pub(crate) fn insert_dirty(&mut self, piece: u32, data: Arc<[u8]>, now: Instant) {
        if let Some(r) = self.partial.remove(&piece) {
            self.budget.release(CacheKind::Partial, r.bytes);
        }
        if self.dirty.contains_key(&piece) || self.writing.contains_key(&piece) {
            return;
        }
        self.budget.clean_remove((self.id, piece));
        self.budget.force(CacheKind::Dirty, data.len() as u64);
        self.dirty.insert(piece, Dirty { data, since: now });
        self.budget.trim();
    }

    /// `piece` was granted from the progress floor and is not written yet.
    pub(crate) fn holds_floor(&self, piece: u32) -> bool {
        self.floor.contains(&piece)
    }

    /// The session's cache is over its budget (floors in use).
    pub(crate) fn over_budget(&self) -> bool {
        self.budget.total() > self.budget.limit()
    }

    /// The piece's verified bytes, dirty or clean.
    pub(crate) fn get(&mut self, piece: u32) -> Option<Arc<[u8]>> {
        if let Some(d) = self.dirty.get(&piece) {
            return Some(Arc::clone(&d.data));
        }
        if let Some(d) = self.writing.get(&piece) {
            return Some(Arc::clone(d));
        }
        self.budget.clean_get((self.id, piece))
    }

    /// A clean entry of `piece` becomes the session's next eviction (after
    /// the entries demoted before it): a sequential reader left it behind.
    /// Dirty pieces and pieces being written are unaffected.
    #[cfg_attr(not(test), expect(dead_code, reason = "wired up in Task 5/6"))]
    pub(crate) fn demote(&mut self, piece: u32) {
        if self.dirty.contains_key(&piece) || self.writing.contains_key(&piece) {
            return;
        }
        self.budget.clean_demote((self.id, piece));
    }

    /// The piece's verified bytes are here (dirty, being written or clean),
    /// without counting as a use of a clean entry.
    #[cfg_attr(not(test), expect(dead_code, reason = "wired up in Task 5/6"))]
    pub(crate) fn is_cached(&self, piece: u32) -> bool {
        self.dirty.contains_key(&piece)
            || self.writing.contains_key(&piece)
            || self.budget.clean_contains((self.id, piece))
    }

    /// A piece read from disk, kept to serve peers if the budget has room
    /// (the session's least recently used clean entries make it).
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the actor reserves before its reads (reserve_clean)"
        )
    )]
    pub(crate) fn insert_clean(&mut self, piece: u32, data: Arc<[u8]>, _now: Instant) {
        if self.dirty.contains_key(&piece)
            || self.writing.contains_key(&piece)
            || !self.reserve_clean(data.len() as u64)
        {
            return;
        }
        self.budget.clean_insert_reserved((self.id, piece), data);
    }

    /// Reserves `n` clean bytes for a whole-piece read; nothing with a
    /// zero `cache_size`.
    pub(crate) fn reserve_clean(&self, n: u64) -> bool {
        self.budget.limit() > 0 && self.budget.try_reserve(CacheKind::Clean, n)
    }

    /// Gives back a [`reserve_clean`](Self::reserve_clean) reservation.
    pub(crate) fn release_clean(&self, n: u64) {
        self.budget.release(CacheKind::Clean, n);
    }

    /// Keeps a piece read under a [`reserve_clean`](Self::reserve_clean)
    /// reservation of exactly its size.
    pub(crate) fn insert_reserved_clean(&mut self, piece: u32, data: Arc<[u8]>) {
        if self.dirty.contains_key(&piece) || self.writing.contains_key(&piece) {
            self.release_clean(data.len() as u64);
            return;
        }
        self.budget.clean_insert_reserved((self.id, piece), data);
    }

    /// Hands dirty pieces to writes, in contiguous runs, oldest run first.
    /// They stay readable until [`written`](Self::written).
    pub(crate) fn take_flush(&mut self, reason: FlushReason, now: Instant) -> Vec<WriteRun> {
        let pieces: Vec<u32> = match reason {
            FlushReason::All | FlushReason::Pressure | FlushReason::Floor => {
                self.dirty.keys().copied().collect()
            }
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
        let runs = self.runs_of(&pieces);
        let mut out = Vec::new();
        for (first, members, _) in runs {
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

    /// A write of `count` pieces from `first` returned, whatever its
    /// result or generation: discarded pieces it held are given back.
    pub(crate) fn landed(&mut self, first: u32, count: u32) {
        if self.orphaned.is_empty() {
            return;
        }
        for i in (0..count).filter_map(|k| first.checked_add(k)) {
            if let Some(n) = self.orphaned.remove(&i) {
                sub(&self.budget.writing, n);
                self.budget.release(CacheKind::Dirty, n);
            }
        }
    }

    /// The write of `count` pieces from `first` failed: its pieces are
    /// dropped (the write job no longer holds them).
    pub(crate) fn write_failed(&mut self, first: u32, count: u32) {
        for i in (0..count).filter_map(|k| first.checked_add(k)) {
            if let Some(data) = self.writing.remove(&i) {
                self.floor.remove(&i);
                let n = data.len() as u64;
                sub(&self.budget.writing, n);
                self.budget.release(CacheKind::Dirty, n);
            }
        }
    }

    /// The write of `count` pieces from `first` succeeded: they become
    /// clean (trimmed if the budget is over) and leave the floor. Pieces no
    /// longer tracked (discarded meanwhile) are skipped.
    pub(crate) fn written(&mut self, first: u32, count: u32) {
        for i in (0..count).filter_map(|k| first.checked_add(k)) {
            let Some(data) = self.writing.remove(&i) else {
                continue;
            };
            self.floor.remove(&i);
            let n = data.len() as u64;
            sub(&self.budget.writing, n);
            self.budget.convert(CacheKind::Dirty, CacheKind::Clean, n);
            self.budget.clean_insert_reserved((self.id, i), data);
        }
    }

    /// Drops every piece and reservation without writing anything. Writes
    /// in flight still land: their bytes stay counted (the jobs hold them)
    /// until [`landed`](Self::landed) reports them, and
    /// [`written`](Self::written) then ignores them.
    pub(crate) fn discard_all(&mut self) {
        for (_, d) in std::mem::take(&mut self.dirty) {
            self.budget.release(CacheKind::Dirty, d.data.len() as u64);
        }
        for (i, d) in std::mem::take(&mut self.writing) {
            let n = d.len() as u64;
            *self.orphaned.entry(i).or_default() += n;
        }
        self.budget.purge(self.id);
        self.release_partials();
        self.floor.clear();
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
    /// downloaded: from the budget (the session's oldest clean entries make
    /// room), else from this torrent's progress floor. The floor holds at
    /// most [`PROGRESS_FLOOR_PIECES`] pieces until they are written, and
    /// grants one only while the session's dirty bytes (writes in flight
    /// included) are within the budget: a slow disk stops it.
    pub(crate) fn reserve_partial(&mut self, piece: u32, n: u64) -> bool {
        if self.partial.contains_key(&piece) {
            return true;
        }
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
        if self.floor_open() {
            self.budget.force(CacheKind::Partial, n);
            self.partial.insert(
                piece,
                Reservation {
                    bytes: n,
                    floor: true,
                },
            );
            self.floor.insert(piece);
            return true;
        }
        false
    }

    /// Floor buffers this torrent may still be granted now.
    fn floor_left(&self) -> usize {
        if self.budget.dirty() <= self.budget.limit() {
            PROGRESS_FLOOR_PIECES.saturating_sub(self.floor.len())
        } else {
            0
        }
    }

    fn floor_open(&self) -> bool {
        self.floor_left() > 0
    }

    /// The buffer of `piece` is gone (failed or dropped).
    pub(crate) fn release_partial(&mut self, piece: u32) {
        if let Some(r) = self.partial.remove(&piece) {
            self.budget.release(CacheKind::Partial, r.bytes);
            if r.floor {
                self.floor.remove(&piece);
            }
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
        for (piece, r) in self.partial.drain() {
            self.budget.release(CacheKind::Partial, r.bytes);
            if r.floor {
                self.floor.remove(&piece);
            }
        }
    }

    /// How many more partial buffers of `n` bytes this torrent could
    /// reserve now, by the rule of [`reserve_partial`](Self::reserve_partial):
    /// budget room (clean entries count as room) plus what is left of the
    /// floor.
    pub(crate) fn partial_room(&self, n: u64) -> usize {
        let by_budget = self.budget.room() / n.max(1);
        usize::try_from(by_budget)
            .unwrap_or(usize::MAX)
            .saturating_add(self.floor_left())
    }
}

impl Drop for PieceCache {
    /// Whatever the torrent still holds goes back to the session's budget,
    /// writes in flight included: nobody will report them any more.
    fn drop(&mut self) {
        self.discard_all();
        for (_, n) in self.orphaned.drain() {
            sub(&self.budget.writing, n);
            self.budget.release(CacheKind::Dirty, n);
        }
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

    fn other(budget: &Arc<CacheBudget>) -> PieceCache {
        PieceCache::new(Arc::clone(budget), Duration::from_secs(30))
    }

    fn shape(runs: &[WriteRun]) -> Vec<Vec<u32>> {
        runs.iter()
            .map(|r| r.pieces.iter().map(|(i, _)| *i).collect())
            .collect()
    }

    fn empty(limit: u64) -> CacheStats {
        CacheStats {
            limit,
            ..CacheStats::default()
        }
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
        // Piece 11 was used last: 12 is the oldest.
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

    /// One torrent's clean entries make room for another's downloads,
    /// oldest first across the session.
    #[test]
    fn clean_lru_is_session_wide() {
        let (budget, mut seeding) = cache(4 * P);
        let mut downloading = other(&budget);
        let t0 = Instant::now();
        for i in 0..4 {
            seeding.insert_clean(i, piece(i), t0);
        }
        assert!(seeding.get(0).is_some());
        assert_eq!(downloading.partial_room(P), 4 + PROGRESS_FLOOR_PIECES);
        // Piece 1 is the session's oldest entry.
        assert!(downloading.reserve_partial(100, P));
        assert!(!downloading.holds_floor(100));
        assert!(seeding.get(1).is_none());
        assert!(seeding.get(0).is_some());
        // A clean entry that cannot fit even after evicting everything
        // evicts nothing.
        let big: Arc<[u8]> = vec![0u8; (5 * P) as usize].into();
        seeding.insert_clean(50, big, t0);
        assert_eq!(budget.stats().clean, 3 * P);
        // Dropping a torrent purges its entries.
        drop(seeding);
        assert_eq!(budget.stats().clean, 0);
        downloading.release_partials();
        assert_eq!(budget.stats(), empty(4 * P));
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
        assert!(!c.reserve_clean(P));
        assert_eq!(budget.stats(), CacheStats::default());
        // Downloads still progress on the floor.
        assert!(c.reserve_partial(5, P));
        assert!(c.holds_floor(5));
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
        // One piece of room left, or two by evicting the clean entry.
        assert!(budget.try_reserve(CacheKind::Partial, P));
        assert!(budget.try_reserve(CacheKind::Partial, P));
        assert!(c.get(3).is_none());
        assert!(!budget.try_reserve(CacheKind::Partial, 1));
        budget.release(CacheKind::Partial, 2 * P);
        // The partial buffer becomes the dirty piece.
        c.insert_dirty(1, piece(1), t0);
        assert_eq!(budget.stats().partial, 0);
        assert_eq!(budget.stats().dirty, 2 * P);
        let runs = c.take_flush(FlushReason::All, t0);
        assert_eq!(shape(&runs), [vec![1, 2]]);
        c.written(1, 2);
        assert_eq!(budget.stats().dirty, 0);
        assert_eq!(budget.stats().clean, 2 * P);
        // A whole-piece read reserves first, then keeps its piece.
        assert!(c.reserve_clean(P));
        c.insert_reserved_clean(7, piece(7));
        assert_eq!(budget.stats().clean, 3 * P);
        // Everything goes back, by discard or by drop.
        assert!(c.reserve_partial(9, P));
        c.discard_all();
        assert_eq!(budget.stats(), empty(4 * P));
        assert!(c.reserve_partial(9, P));
        c.insert_dirty(8, piece(8), t0);
        let _ = c.take_flush(FlushReason::All, t0);
        drop(c);
        assert_eq!(budget.stats(), empty(4 * P));
    }

    /// A discarded piece whose write is in flight stays counted until the
    /// write returns.
    #[test]
    fn discarded_writes_count_until_they_land() {
        let (budget, mut c) = cache(4 * P);
        let t0 = Instant::now();
        c.insert_dirty(1, piece(1), t0);
        c.insert_dirty(2, piece(2), t0);
        let runs = c.take_flush(FlushReason::All, t0);
        assert_eq!(shape(&runs), [vec![1, 2]]);
        c.insert_dirty(5, piece(5), t0);
        c.discard_all();
        assert!(c.get(1).is_none() && c.get(5).is_none());
        assert!(c.is_idle());
        assert_eq!(budget.stats().dirty, 2 * P);
        // A stale success is not a written piece.
        c.landed(1, 2);
        c.written(1, 2);
        assert_eq!(budget.stats(), empty(4 * P));
        assert!(c.get(1).is_none());
        // A failed write's pieces go at once, before the discard that
        // follows the failure.
        c.insert_dirty(3, piece(3), t0);
        let _ = c.take_flush(FlushReason::All, t0);
        c.landed(3, 1);
        c.write_failed(3, 1);
        c.discard_all();
        assert_eq!(budget.stats(), empty(4 * P));
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

        // The floor: four pieces per torrent beyond a full budget, counted
        // until written.
        let (budget, mut c) = cache(2 * P);
        let mut other = other(&budget);
        assert!(other.reserve_partial(0, P) && other.reserve_partial(1, P));
        assert_eq!(c.partial_room(P), PROGRESS_FLOOR_PIECES);
        for i in 0..4 {
            assert!(c.reserve_partial(i, P), "floor buffer {i}");
            assert!(c.holds_floor(i));
        }
        assert!(!c.reserve_partial(4, P));
        assert_eq!(c.partial_room(P), 0);
        assert_eq!(budget.stats().partial, 6 * P);
        assert!(c.over_budget());
        // A verified floor piece still holds its floor slot.
        c.insert_dirty(0, piece(0), t0);
        assert!(c.holds_floor(0));
        assert!(!c.reserve_partial(4, P));
        let runs = c.take_flush(FlushReason::Floor, t0);
        assert_eq!(shape(&runs), [vec![0]]);
        assert!(!c.reserve_partial(4, P));
        c.written(0, 1);
        assert!(!c.holds_floor(0));
        assert!(c.reserve_partial(4, P));
        // A dropped floor buffer frees its slot too.
        c.release_partial(1);
        assert!(c.reserve_partial(5, P));
        c.retain_partials(|_| false);
        other.release_partials();
        drop(c);
        assert_eq!(budget.stats(), empty(2 * P));
    }

    #[test]
    fn demoted_entry_is_evicted_first() {
        let (budget, mut c) = cache(4 * P);
        let t0 = Instant::now();
        for i in 0..4 {
            c.insert_clean(i, piece(i), t0);
        }
        // 2 then 1 are demoted: they go first, 2 before 1, then 0.
        c.demote(2);
        c.demote(1);
        c.insert_dirty(10, piece(10), t0);
        assert!(!c.is_cached(2));
        assert!(c.is_cached(1) && c.is_cached(0) && c.is_cached(3));
        c.insert_dirty(11, piece(11), t0);
        assert!(!c.is_cached(1));
        assert!(c.is_cached(0) && c.is_cached(3));
        c.insert_dirty(12, piece(12), t0);
        assert!(!c.is_cached(0));
        assert!(c.is_cached(3));
        assert_eq!(budget.stats().clean, P);
    }

    #[test]
    fn demote_leaves_dirty_pieces_alone() {
        let (budget, mut c) = cache(3 * P);
        let mut d = other(&budget);
        let t0 = Instant::now();
        c.insert_clean(1, piece(1), t0);
        d.insert_clean(5, piece(5), t0);
        c.insert_dirty(5, piece(5), t0);
        // A dirty piece, then one being written: nothing to demote.
        c.demote(5);
        assert!(c.is_cached(5));
        let _ = c.take_flush(FlushReason::All, t0);
        c.demote(5);
        assert!(c.is_cached(5));
        // Another torrent's piece 5 was not demoted either: 1 is the oldest.
        assert!(c.reserve_clean(P));
        assert!(!c.is_cached(1));
        assert!(d.is_cached(5));
        c.release_clean(P);
        // Pieces nobody holds are ignored.
        c.demote(99);
        d.demote(99);
        c.written(5, 1);
        assert!(c.is_cached(5));
        c.demote(5);
        assert!(c.reserve_clean(P) && c.reserve_clean(P));
        assert!(!c.is_cached(5));
        assert!(d.is_cached(5));
        c.release_clean(2 * P);
    }

    #[test]
    fn is_cached_does_not_touch_the_lru() {
        let (_budget, mut c) = cache(2 * P);
        let t0 = Instant::now();
        c.insert_clean(0, piece(0), t0);
        c.insert_clean(1, piece(1), t0);
        assert!(c.is_cached(0));
        // 0 is still the oldest.
        c.insert_dirty(2, piece(2), t0);
        assert!(!c.is_cached(0));
        assert!(c.is_cached(1) && c.is_cached(2));
        // A partial buffer is not cached data.
        assert!(c.reserve_partial(3, 1));
        assert!(!c.is_cached(3));
        c.release_partials();
    }
}
