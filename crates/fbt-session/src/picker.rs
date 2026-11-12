//! Piece picker: rarest-first with priorities, partial-piece preference,
//! sequential mode, end-game and parole.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::{Duration, Instant};

use fbt_core::{BLOCK_SIZE, Bitfield, BlockRef, PieceLayout};
use rand::RngExt;

/// Identifies a peer connection inside one torrent.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug)]
pub struct PeerKey(pub u64);

/// The pieces a peer has.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum PeerPieces<'a> {
    /// A seed (`HaveAll` or a full bitfield).
    All,
    /// The peer's bitfield.
    Some(&'a Bitfield),
}

/// Outcome of [`PiecePicker::on_block`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BlockResult {
    /// False for blocks never requested from this peer, or duplicates.
    pub accepted: bool,
    /// True exactly once per piece, when its last block arrives.
    pub piece_complete: bool,
    /// Other peers that also had this block outstanding (end-game).
    pub cancel: Vec<PeerKey>,
}

/// Download progress of one piece, from [`PiecePicker::progress`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PieceProgress {
    /// Blocks neither received nor outstanding from any peer.
    pub free: usize,
    /// Peers holding an unreceived block of the piece, sorted, each once.
    pub owners: Vec<PeerKey>,
}

/// Default piece priority.
const DEFAULT_PRIORITY: u8 = 4;
/// Highest piece priority.
const MAX_PRIORITY: u8 = 7;
/// End-game: at most this many peers hold the same block.
const MAX_OWNERS: usize = 2;
/// `pos` marker for a piece that is not in any bucket.
const NOT_QUEUED: u32 = u32::MAX;

/// Lifecycle of one piece inside the picker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PieceState {
    /// No block requested or received.
    Unrequested,
    /// Some blocks requested or received, not all received.
    Partial,
    /// Every block received; the hash check is in flight.
    PendingHash,
    /// Hash check passed.
    Have,
}

/// One block of a piece being downloaded.
#[derive(Debug, Clone, Default)]
struct BlockSlot {
    received: bool,
    /// Peers this block is outstanding from (at most [`MAX_OWNERS`]).
    owners: Vec<PeerKey>,
}

/// A piece with at least one block requested or received.
#[derive(Debug)]
struct Downloading {
    blocks: Vec<BlockSlot>,
    /// Blocks neither received nor outstanding from anyone.
    free: usize,
    received: usize,
    /// Started by a peer on parole: only that peer downloads it.
    exclusive: Option<PeerKey>,
    /// For an exclusive piece: `received` when last looked at by
    /// [`PiecePicker::expire_exclusive`], and since when it has been that.
    progress_seen: Option<(usize, Instant)>,
}

/// Ordering key of a queued piece: priority descending, then availability.
type BucketKey = (Reverse<u8>, u32);

/// Chooses which blocks to request from which peer.
///
/// Order: free blocks of partial pieces (priority descending), then new
/// pieces by priority descending and rarest first (or lowest index when
/// sequential, random tie-break otherwise), then end-game duplicates once
/// every block of every wanted piece is requested. Single-threaded: owned by
/// one torrent actor.
///
/// Parole (as libtorrent's): a peer on parole ([`set_parole`]) downloads
/// only whole pieces, each its alone, so a piece that fails afterwards
/// has a single contributor. It never joins another peer's piece or takes
/// end-game duplicates, and no other peer joins the pieces it starts. Such
/// a piece stays reserved for its owner while the owner's requests come
/// and go (choke, reject, timeout): its received blocks are kept and the
/// owner continues it. It is forfeited (its progress dropped and listed by
/// [`take_forfeited`], so its blocks are never completed by someone else)
/// when the owner leaves ([`release_peer`]) or has received none of its
/// blocks for a grace period ([`expire_exclusive`]). The reservation ends
/// when the owner leaves parole. Pieces picked by
/// [`pick_piece`](Self::pick_piece) (reader deadlines) are never reserved.
///
/// [`set_parole`]: Self::set_parole
/// [`take_forfeited`]: Self::take_forfeited
/// [`release_peer`]: Self::release_peer
/// [`expire_exclusive`]: Self::expire_exclusive
#[derive(Debug)]
pub struct PiecePicker {
    layout: PieceLayout,
    have: Bitfield,
    prio: Vec<u8>,
    /// Per-piece count of peers with a bitfield/`Have` for it.
    avail: Vec<u32>,
    /// Peers that have every piece; they add the same to every piece, so
    /// they never change the rarest-first order and are kept apart.
    seeds: u32,
    state: Vec<PieceState>,
    sequential: bool,
    /// Wanted `Unrequested` pieces, bucketed by (priority, availability).
    buckets: BTreeMap<BucketKey, Vec<u32>>,
    /// Index of each piece inside its bucket, or [`NOT_QUEUED`].
    pos: Vec<u32>,
    /// The same pieces as `buckets`, by (priority, index), for sequential mode.
    seq: BTreeSet<(Reverse<u8>, u32)>,
    /// Wanted `Partial` pieces that still have free blocks.
    partial: BTreeSet<(Reverse<u8>, u32)>,
    downloading: BTreeMap<u32, Downloading>,
    /// Blocks outstanding per peer, as (piece, block index).
    outstanding: HashMap<PeerKey, HashSet<(u32, u32)>>,
    wanted_left_pieces: u32,
    wanted_left_bytes: u64,
    /// Peers on parole (connected ones only: a departed peer is dropped).
    parole: HashSet<PeerKey>,
    /// Pieces whose parole download was given up, for the owner of their
    /// buffers; each at most once.
    forfeited: Vec<u32>,
}

fn peer_has_piece(has: PeerPieces, piece: u32) -> bool {
    match has {
        PeerPieces::All => true,
        PeerPieces::Some(bf) => bf.get(piece),
    }
}

impl PiecePicker {
    /// Creates a picker for `layout`; pieces set in `have` are already
    /// verified. Every piece starts at the default priority (4).
    pub fn new(layout: PieceLayout, have: &Bitfield) -> Self {
        let n = layout.num_pieces();
        let len = n as usize;
        let mut own = Bitfield::new(n);
        for i in have.iter_ones().take_while(|&i| i < n) {
            own.set(i);
        }
        let state = (0..n)
            .map(|i| {
                if own.get(i) {
                    PieceState::Have
                } else {
                    PieceState::Unrequested
                }
            })
            .collect();
        let mut p = Self {
            layout,
            have: own,
            prio: vec![DEFAULT_PRIORITY; len],
            avail: vec![0; len],
            seeds: 0,
            state,
            sequential: false,
            buckets: BTreeMap::new(),
            pos: vec![NOT_QUEUED; len],
            seq: BTreeSet::new(),
            partial: BTreeSet::new(),
            downloading: BTreeMap::new(),
            outstanding: HashMap::new(),
            wanted_left_pieces: 0,
            wanted_left_bytes: 0,
            parole: HashSet::new(),
            forfeited: Vec::new(),
        };
        for i in 0..n {
            p.enqueue(i);
        }
        p.recount_wanted();
        p
    }

    /// Sets per-piece priorities: 0 skips the piece, 1..=7 (higher first).
    /// Missing entries get the default (4); values above 7 count as 7.
    pub fn set_piece_priorities(&mut self, prio: &[u8]) {
        for i in 0..self.layout.num_pieces() {
            self.dequeue(i);
            self.partial_remove(i);
            let new = prio
                .get(i as usize)
                .copied()
                .unwrap_or(DEFAULT_PRIORITY)
                .min(MAX_PRIORITY);
            if let Some(p) = self.prio.get_mut(i as usize) {
                *p = new;
            }
            self.enqueue(i);
            self.partial_sync(i);
        }
        self.recount_wanted();
    }

    /// Sequential mode: new pieces in index order instead of rarest first.
    pub fn set_sequential(&mut self, on: bool) {
        self.sequential = on;
    }

    /// Puts `peer` on parole or takes it off. On parole it gets only whole
    /// pieces of its own (see the type's docs). Off parole, the pieces it
    /// kept to itself are open to every peer again.
    pub fn set_parole(&mut self, peer: PeerKey, on: bool) {
        if on {
            self.parole.insert(peer);
        } else if self.parole.remove(&peer) {
            let mine: Vec<u32> = self.exclusive_to(peer).collect();
            for piece in mine {
                if let Some(d) = self.downloading.get_mut(&piece) {
                    d.exclusive = None;
                    d.progress_seen = None;
                }
            }
        }
    }

    /// Forfeits every piece reserved for a peer on parole that has
    /// received none of its blocks for `grace` (a reserved piece's
    /// progress is first seen at the first call after it changes, so
    /// calls should come at least every few seconds). A piece with no
    /// block received is opened to every peer instead: nothing of it is
    /// lost.
    pub fn expire_exclusive(&mut self, now: Instant, grace: Duration) {
        let mut stale = Vec::new();
        for (&piece, d) in &mut self.downloading {
            if d.exclusive.is_none() {
                continue;
            }
            match d.progress_seen {
                Some((received, since)) if received == d.received => {
                    if now.saturating_duration_since(since) >= grace {
                        stale.push(piece);
                    }
                }
                _ => d.progress_seen = Some((d.received, now)),
            }
        }
        for piece in stale {
            self.forfeit(piece);
        }
    }

    /// The pieces forfeited since the last call (see the type's docs).
    /// Their progress is already dropped (they are wanted again); the
    /// caller frees their buffers.
    pub fn take_forfeited(&mut self) -> Vec<u32> {
        std::mem::take(&mut self.forfeited)
    }

    /// Counts a peer's pieces into availability (bitfield or `HaveAll`).
    /// Bits past the torrent's piece count are ignored.
    pub fn add_peer_pieces(&mut self, p: PeerPieces) {
        match p {
            PeerPieces::All => self.seeds = self.seeds.saturating_add(1),
            PeerPieces::Some(bf) => {
                let n = self.layout.num_pieces();
                for i in bf.iter_ones().take_while(|&i| i < n) {
                    self.change_avail(i, true);
                }
            }
        }
    }

    /// Removes a departed peer's pieces from availability.
    pub fn remove_peer_pieces(&mut self, p: PeerPieces) {
        match p {
            PeerPieces::All => self.seeds = self.seeds.saturating_sub(1),
            PeerPieces::Some(bf) => {
                let n = self.layout.num_pieces();
                for i in bf.iter_ones().take_while(|&i| i < n) {
                    self.change_avail(i, false);
                }
            }
        }
    }

    /// A peer announced `piece` (`Have`); out of range is ignored.
    pub fn peer_has(&mut self, piece: u32) {
        if piece < self.layout.num_pieces() {
            self.change_avail(piece, true);
        }
    }

    /// A peer retracted `piece` (BEP 54 `lt_donthave`); mirrors
    /// [`peer_has`](Self::peer_has). Out of range is ignored.
    pub fn peer_lost(&mut self, piece: u32) {
        if piece < self.layout.num_pieces() {
            self.change_avail(piece, false);
        }
    }

    /// Number of known peers that have `piece` (seeds included).
    pub fn availability(&self, piece: u32) -> u32 {
        self.avail
            .get(piece as usize)
            .map_or(0, |a| a.saturating_add(self.seeds))
    }

    /// Picks up to `max_blocks` blocks to request from `peer`, whose pieces
    /// are `has`, and records them as outstanding from it.
    pub fn pick(
        &mut self,
        peer: PeerKey,
        has: PeerPieces,
        max_blocks: usize,
        rng: &mut impl rand::Rng,
    ) -> Vec<BlockRef> {
        let mut out = Vec::new();
        if max_blocks == 0 {
            return out;
        }
        let partial: Vec<u32> = self
            .partial
            .iter()
            .map(|&(_, i)| i)
            .filter(|&i| peer_has_piece(has, i) && self.may_join(peer, i))
            .collect();
        for i in partial {
            if out.len() >= max_blocks {
                return out;
            }
            self.take_free(peer, i, max_blocks, &mut out);
        }
        while out.len() < max_blocks {
            let Some(i) = self.next_new(has, rng) else {
                break;
            };
            self.start_piece(i);
            self.claim(peer, i);
            self.take_free(peer, i, max_blocks, &mut out);
        }
        if out.len() < max_blocks && self.is_endgame() && !self.parole.contains(&peer) {
            self.take_endgame(peer, has, max_blocks, &mut out);
        }
        out
    }

    /// Picks up to `max_blocks` blocks of `piece` for `peer`, whatever the
    /// piece's priority (0 included), and records them as outstanding from
    /// it: first its free blocks (starting the piece if it is unrequested),
    /// then blocks whose single owner is in `late` and is not `peer`. An
    /// empty `late` takes no duplicate, and no block gets more than two
    /// owners (the end-game cap). Nothing for a piece we have, one pending
    /// its hash check, or out of range.
    ///
    /// A priority-0 piece started here never enters the partial set or the
    /// buckets, so [`pick`](Self::pick) never continues it: only
    /// `pick_piece` does.
    pub fn pick_piece(
        &mut self,
        peer: PeerKey,
        piece: u32,
        max_blocks: usize,
        late: &[PeerKey],
    ) -> Vec<BlockRef> {
        let mut out = Vec::new();
        if max_blocks == 0 || piece >= self.layout.num_pieces() {
            return out;
        }
        match self.state_of(piece) {
            // A reader's deadline piece is never reserved: whoever is in
            // time takes it, parole or not.
            PieceState::Unrequested => self.start_piece(piece),
            PieceState::Partial => {
                if let Some(d) = self.downloading.get_mut(&piece) {
                    d.exclusive = None;
                    d.progress_seen = None;
                }
            }
            PieceState::PendingHash | PieceState::Have => return out,
        }
        self.take_free(peer, piece, max_blocks, &mut out);
        if out.len() >= max_blocks || late.is_empty() {
            return out;
        }
        let Some(d) = self.downloading.get_mut(&piece) else {
            return out;
        };
        let mut taken = Vec::new();
        for (bi, slot) in (0u32..).zip(d.blocks.iter_mut()) {
            if out.len() + taken.len() >= max_blocks {
                break;
            }
            let late_owner = match slot.owners.as_slice() {
                [owner] => *owner != peer && late.contains(owner),
                _ => false,
            };
            if !slot.received && late_owner {
                slot.owners.push(peer);
                taken.push(bi);
            }
        }
        self.assign(peer, piece, &taken, &mut out);
        out
    }

    /// Download progress of `piece`: its free blocks and the peers holding
    /// the rest. `None` for a piece we have, one pending its hash check, or
    /// out of range.
    pub fn progress(&self, piece: u32) -> Option<PieceProgress> {
        if piece >= self.layout.num_pieces() {
            return None;
        }
        match self.state_of(piece) {
            PieceState::Unrequested => Some(PieceProgress {
                free: self.layout.blocks_in_piece(piece) as usize,
                owners: Vec::new(),
            }),
            PieceState::Partial => {
                let d = self.downloading.get(&piece)?;
                let mut owners: Vec<PeerKey> = d
                    .blocks
                    .iter()
                    .filter(|b| !b.received)
                    .flat_map(|b| b.owners.iter().copied())
                    .collect();
                owners.sort_unstable();
                owners.dedup();
                Some(PieceProgress {
                    free: d.free,
                    owners,
                })
            }
            PieceState::PendingHash | PieceState::Have => None,
        }
    }

    /// Drops a partial piece's download progress, as
    /// [`piece_failed`](Self::piece_failed) does, and returns the blocks
    /// that were outstanding (by peer, then block), for the caller to
    /// cancel. A wanted piece goes back to the new-piece candidates; a
    /// priority-0 one is left alone. Nothing for a piece not partial
    /// (unrequested, pending its hash check, passed or out of range).
    pub fn abandon(&mut self, piece: u32) -> Vec<(PeerKey, BlockRef)> {
        if self.state_of(piece) != PieceState::Partial {
            return Vec::new();
        }
        let layout = self.layout;
        let mut cancel: Vec<(PeerKey, BlockRef)> = self
            .downloading
            .get(&piece)
            .map(|d| {
                (0u32..)
                    .zip(&d.blocks)
                    .flat_map(|(bi, slot)| {
                        slot.owners
                            .iter()
                            .map(move |&o| (o, layout.block(piece, bi)))
                    })
                    .collect()
            })
            .unwrap_or_default();
        cancel.sort_unstable();
        self.piece_failed(piece);
        cancel
    }

    /// A block arrived from `peer`. Accepted only if it was outstanding from
    /// that peer and not received before.
    pub fn on_block(&mut self, peer: PeerKey, block: BlockRef) -> BlockResult {
        let mut res = BlockResult::default();
        let Some(bi) = self.block_index(&block) else {
            return res;
        };
        let piece = block.piece;
        let Some(d) = self.downloading.get_mut(&piece) else {
            return res;
        };
        let Some(slot) = d.blocks.get_mut(bi as usize) else {
            return res;
        };
        if slot.received || !slot.owners.contains(&peer) {
            return res;
        }
        slot.received = true;
        let owners = std::mem::take(&mut slot.owners);
        d.received += 1;
        let complete = d.received == d.blocks.len();
        for owner in owners {
            self.forget_outstanding(owner, piece, bi);
            if owner != peer {
                res.cancel.push(owner);
            }
        }
        if complete {
            self.set_state(piece, PieceState::PendingHash);
            self.partial_remove(piece);
        }
        res.accepted = true;
        res.piece_complete = complete;
        res
    }

    /// Returns `block` from `peer` to the pool (rejected, timed out, choked),
    /// unless another peer still holds it.
    pub fn release(&mut self, peer: PeerKey, block: BlockRef) {
        if let Some(bi) = self.block_index(&block) {
            self.release_inner(peer, block.piece, bi);
        }
    }

    /// Returns every block outstanding from `peer` (disconnect), takes it
    /// off parole and forfeits every piece reserved for it, whether or not
    /// it still had a block of it outstanding.
    pub fn release_peer(&mut self, peer: PeerKey) {
        self.parole.remove(&peer);
        if let Some(blocks) = self.outstanding.remove(&peer) {
            let mut blocks: Vec<(u32, u32)> = blocks.into_iter().collect();
            blocks.sort_unstable();
            for (piece, bi) in blocks {
                self.release_inner(peer, piece, bi);
            }
        }
        let mine: Vec<u32> = self.exclusive_to(peer).collect();
        for piece in mine {
            self.forfeit(piece);
        }
    }

    /// The piece's hash check passed. Out of range is ignored.
    pub fn piece_passed(&mut self, piece: u32) {
        if piece >= self.layout.num_pieces() || self.have.get(piece) {
            return;
        }
        self.drop_downloading(piece);
        self.dequeue(piece);
        self.partial_remove(piece);
        self.have.set(piece);
        self.set_state(piece, PieceState::Have);
        if self.prio_of(piece) > 0 {
            self.wanted_left_pieces = self.wanted_left_pieces.saturating_sub(1);
            self.wanted_left_bytes = self
                .wanted_left_bytes
                .saturating_sub(u64::from(self.layout.piece_size(piece)));
        }
    }

    /// The piece's hash check failed: every block goes back to unrequested.
    /// No effect on a piece already passed or out of range.
    pub fn piece_failed(&mut self, piece: u32) {
        if piece >= self.layout.num_pieces() || self.have.get(piece) {
            return;
        }
        self.drop_downloading(piece);
        self.partial_remove(piece);
        self.set_state(piece, PieceState::Unrequested);
        self.enqueue(piece);
    }

    /// A passed piece is gone from disk: it is wanted again (queued like a
    /// failed one). No effect on a piece we do not have or out of range.
    pub fn piece_lost(&mut self, piece: u32) {
        if piece >= self.layout.num_pieces() || !self.have.get(piece) {
            return;
        }
        self.have.clear(piece);
        self.set_state(piece, PieceState::Unrequested);
        if self.prio_of(piece) > 0 {
            self.wanted_left_pieces = self.wanted_left_pieces.saturating_add(1);
            self.wanted_left_bytes = self
                .wanted_left_bytes
                .saturating_add(u64::from(self.layout.piece_size(piece)));
        }
        self.enqueue(piece);
    }

    /// Pieces whose hash check passed.
    pub fn have(&self) -> &Bitfield {
        &self.have
    }

    /// Every block of every wanted, unfinished piece is requested or received.
    pub fn is_endgame(&self) -> bool {
        self.wanted_left_pieces > 0 && self.buckets.is_empty() && self.partial.is_empty()
    }

    /// Bytes of wanted pieces not yet passed.
    pub fn wanted_left_bytes(&self) -> u64 {
        self.wanted_left_bytes
    }

    /// Every wanted (priority > 0) piece passed.
    pub fn is_finished(&self) -> bool {
        self.wanted_left_pieces == 0
    }

    /// Pieces with blocks requested or received but not all received.
    pub fn partial_pieces(&self) -> impl Iterator<Item = u32> + '_ {
        self.downloading
            .keys()
            .copied()
            .filter(|&i| self.state_of(i) == PieceState::Partial)
    }

    /// Pieces started and not yet passed or failed (partial or awaiting
    /// their hash check).
    pub fn num_in_progress(&self) -> usize {
        self.downloading.len()
    }

    /// `piece` is started and not yet passed or failed.
    pub fn is_in_progress(&self, piece: u32) -> bool {
        self.downloading.contains_key(&piece)
    }

    fn prio_of(&self, piece: u32) -> u8 {
        self.prio.get(piece as usize).copied().unwrap_or(0)
    }

    fn state_of(&self, piece: u32) -> PieceState {
        self.state
            .get(piece as usize)
            .copied()
            .unwrap_or(PieceState::Have)
    }

    fn set_state(&mut self, piece: u32, st: PieceState) {
        if let Some(s) = self.state.get_mut(piece as usize) {
            *s = st;
        }
    }

    fn recount_wanted(&mut self) {
        self.wanted_left_pieces = 0;
        self.wanted_left_bytes = 0;
        for i in 0..self.layout.num_pieces() {
            if self.prio_of(i) > 0 && !self.have.get(i) {
                self.wanted_left_pieces += 1;
                self.wanted_left_bytes += u64::from(self.layout.piece_size(i));
            }
        }
    }

    /// Index of `block` in its piece if it is exactly one of the layout's
    /// blocks (right offset and length).
    fn block_index(&self, block: &BlockRef) -> Option<u32> {
        if !self.layout.is_valid_block(block) {
            return None;
        }
        let bi = block.offset / BLOCK_SIZE;
        (self.layout.block(block.piece, bi) == *block).then_some(bi)
    }

    fn bucket_key(&self, piece: u32) -> BucketKey {
        let avail = self.avail.get(piece as usize).copied().unwrap_or(0);
        (Reverse(self.prio_of(piece)), avail)
    }

    /// Queues a wanted `Unrequested` piece as a new-piece candidate.
    fn enqueue(&mut self, piece: u32) {
        let queued = self
            .pos
            .get(piece as usize)
            .is_none_or(|&p| p != NOT_QUEUED);
        if queued || self.prio_of(piece) == 0 || self.state_of(piece) != PieceState::Unrequested {
            return;
        }
        let key = self.bucket_key(piece);
        let bucket = self.buckets.entry(key).or_default();
        let at = u32::try_from(bucket.len()).unwrap_or(NOT_QUEUED);
        bucket.push(piece);
        if let Some(p) = self.pos.get_mut(piece as usize) {
            *p = at;
        }
        self.seq.insert((key.0, piece));
    }

    /// Removes a piece from the candidates; call before changing its
    /// priority or availability.
    fn dequeue(&mut self, piece: u32) {
        let Some(&at) = self.pos.get(piece as usize) else {
            return;
        };
        if at == NOT_QUEUED {
            return;
        }
        let key = self.bucket_key(piece);
        if let Some(bucket) = self.buckets.get_mut(&key) {
            if (at as usize) < bucket.len() {
                bucket.swap_remove(at as usize);
                if let Some(&moved) = bucket.get(at as usize)
                    && let Some(p) = self.pos.get_mut(moved as usize)
                {
                    *p = at;
                }
            }
            if bucket.is_empty() {
                self.buckets.remove(&key);
            }
        }
        if let Some(p) = self.pos.get_mut(piece as usize) {
            *p = NOT_QUEUED;
        }
        self.seq.remove(&(key.0, piece));
    }

    fn change_avail(&mut self, piece: u32, up: bool) {
        let queued = self
            .pos
            .get(piece as usize)
            .is_some_and(|&p| p != NOT_QUEUED);
        if queued {
            self.dequeue(piece);
        }
        if let Some(a) = self.avail.get_mut(piece as usize) {
            *a = if up {
                a.saturating_add(1)
            } else {
                a.saturating_sub(1)
            };
        }
        if queued {
            self.enqueue(piece);
        }
    }

    fn partial_remove(&mut self, piece: u32) {
        self.partial.remove(&(Reverse(self.prio_of(piece)), piece));
    }

    /// Puts a piece in `partial` exactly when it is wanted, `Partial` and has
    /// free blocks.
    fn partial_sync(&mut self, piece: u32) {
        let key = (Reverse(self.prio_of(piece)), piece);
        let free = self.downloading.get(&piece).is_some_and(|d| d.free > 0);
        if free && key.0.0 > 0 && self.state_of(piece) == PieceState::Partial {
            self.partial.insert(key);
        } else {
            self.partial.remove(&key);
        }
    }

    /// The next new piece the peer has: lowest index when sequential, else
    /// rarest with a random starting point inside the bucket.
    fn next_new(&self, has: PeerPieces, rng: &mut impl rand::Rng) -> Option<u32> {
        if self.sequential {
            return self
                .seq
                .iter()
                .map(|&(_, i)| i)
                .find(|&i| peer_has_piece(has, i));
        }
        for bucket in self.buckets.values() {
            let len = bucket.len();
            if len == 0 {
                continue;
            }
            let start = rng.random_range(0..len);
            let found = bucket
                .iter()
                .skip(start)
                .chain(bucket.iter().take(start))
                .copied()
                .find(|&i| peer_has_piece(has, i));
            if found.is_some() {
                return found;
            }
        }
        None
    }

    fn start_piece(&mut self, piece: u32) {
        self.dequeue(piece);
        let n = self.layout.blocks_in_piece(piece) as usize;
        self.downloading.insert(
            piece,
            Downloading {
                blocks: vec![BlockSlot::default(); n],
                free: n,
                received: 0,
                exclusive: None,
                progress_seen: None,
            },
        );
        self.set_state(piece, PieceState::Partial);
    }

    /// The pieces reserved for `peer`.
    fn exclusive_to(&self, peer: PeerKey) -> impl Iterator<Item = u32> + '_ {
        self.downloading
            .iter()
            .filter(move |(_, d)| d.exclusive == Some(peer))
            .map(|(&i, _)| i)
    }

    /// Gives up the reserved `piece`: with blocks received, its progress
    /// is dropped and it is listed for [`take_forfeited`](Self::take_forfeited);
    /// with none, it is just opened to every peer.
    fn forfeit(&mut self, piece: u32) {
        let Some(d) = self.downloading.get_mut(&piece) else {
            return;
        };
        if d.received == 0 {
            d.exclusive = None;
            d.progress_seen = None;
            return;
        }
        self.piece_failed(piece);
        if !self.forfeited.contains(&piece) {
            self.forfeited.push(piece);
        }
    }

    /// `peer`, just starting `piece`, keeps it to itself if on parole.
    fn claim(&mut self, peer: PeerKey, piece: u32) {
        if self.parole.contains(&peer)
            && let Some(d) = self.downloading.get_mut(&piece)
        {
            d.exclusive = Some(peer);
        }
    }

    /// Whether `peer` may take blocks of the partial `piece`: a piece
    /// another peer keeps to itself is never shared, and a peer on parole
    /// joins only its own.
    fn may_join(&self, peer: PeerKey, piece: u32) -> bool {
        match self.downloading.get(&piece).and_then(|d| d.exclusive) {
            Some(owner) => owner == peer,
            None => !self.parole.contains(&peer),
        }
    }

    /// Assigns free blocks of `piece` to `peer`.
    fn take_free(&mut self, peer: PeerKey, piece: u32, max: usize, out: &mut Vec<BlockRef>) {
        let Some(d) = self.downloading.get_mut(&piece) else {
            return;
        };
        let mut taken = Vec::new();
        for (bi, slot) in (0u32..).zip(d.blocks.iter_mut()) {
            if out.len() + taken.len() >= max {
                break;
            }
            if !slot.received && slot.owners.is_empty() {
                slot.owners.push(peer);
                d.free -= 1;
                taken.push(bi);
            }
        }
        self.assign(peer, piece, &taken, out);
        self.partial_sync(piece);
    }

    /// End-game: blocks outstanding from others (fewer than [`MAX_OWNERS`])
    /// but not from `peer`, in pieces it has, priority descending.
    fn take_endgame(
        &mut self,
        peer: PeerKey,
        has: PeerPieces,
        max: usize,
        out: &mut Vec<BlockRef>,
    ) {
        let mut pieces: Vec<(Reverse<u8>, u32)> = self
            .downloading
            .keys()
            .copied()
            .filter(|&i| {
                self.prio_of(i) > 0
                    && self.state_of(i) == PieceState::Partial
                    && peer_has_piece(has, i)
                    && self.may_join(peer, i)
            })
            .map(|i| (Reverse(self.prio_of(i)), i))
            .collect();
        pieces.sort_unstable();
        for (_, piece) in pieces {
            let Some(d) = self.downloading.get_mut(&piece) else {
                continue;
            };
            let mut taken = Vec::new();
            for (bi, slot) in (0u32..).zip(d.blocks.iter_mut()) {
                if out.len() + taken.len() >= max {
                    break;
                }
                if !slot.received && slot.owners.len() < MAX_OWNERS && !slot.owners.contains(&peer)
                {
                    slot.owners.push(peer);
                    taken.push(bi);
                }
            }
            self.assign(peer, piece, &taken, out);
            if out.len() >= max {
                return;
            }
        }
    }

    fn assign(&mut self, peer: PeerKey, piece: u32, taken: &[u32], out: &mut Vec<BlockRef>) {
        if taken.is_empty() {
            return;
        }
        let set = self.outstanding.entry(peer).or_default();
        for &bi in taken {
            set.insert((piece, bi));
            out.push(self.layout.block(piece, bi));
        }
    }

    fn forget_outstanding(&mut self, peer: PeerKey, piece: u32, bi: u32) {
        if let Some(set) = self.outstanding.get_mut(&peer) {
            set.remove(&(piece, bi));
            if set.is_empty() {
                self.outstanding.remove(&peer);
            }
        }
    }

    fn release_inner(&mut self, peer: PeerKey, piece: u32, bi: u32) {
        self.forget_outstanding(peer, piece, bi);
        let Some(d) = self.downloading.get_mut(&piece) else {
            return;
        };
        let Some(slot) = d.blocks.get_mut(bi as usize) else {
            return;
        };
        let Some(at) = slot.owners.iter().position(|&o| o == peer) else {
            return;
        };
        slot.owners.swap_remove(at);
        if slot.owners.is_empty() && !slot.received {
            d.free += 1;
        }
        // A reserved piece stays its owner's (received blocks included)
        // while its requests come and go: see the type's docs.
        if d.received == 0 && d.free == d.blocks.len() {
            // Nothing left of this piece: back to the new-piece candidates.
            self.downloading.remove(&piece);
            self.partial_remove(piece);
            self.set_state(piece, PieceState::Unrequested);
            self.enqueue(piece);
        } else {
            self.partial_sync(piece);
        }
    }

    /// Forgets a piece's download progress and every peer's claim on it.
    fn drop_downloading(&mut self, piece: u32) {
        let Some(d) = self.downloading.remove(&piece) else {
            return;
        };
        for (bi, slot) in (0u32..).zip(d.blocks) {
            for owner in slot.owners {
                self.forget_outstanding(owner, piece, bi);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    const B: u32 = BLOCK_SIZE;

    fn rng() -> StdRng {
        StdRng::seed_from_u64(7)
    }

    fn bf(bits: &[u8]) -> Bitfield {
        let mut b = Bitfield::new(bits.len() as u32);
        for (i, &v) in bits.iter().enumerate() {
            if v != 0 {
                b.set(i as u32);
            }
        }
        b
    }

    fn blk(piece: u32, idx: u32) -> BlockRef {
        BlockRef {
            piece,
            offset: idx * B,
            len: B,
        }
    }

    /// `pieces` pieces of `blocks` full blocks each, nothing owned.
    fn picker(pieces: u32, blocks: u32) -> PiecePicker {
        let l = PieceLayout::new(u64::from(pieces * blocks * B), blocks * B).unwrap();
        PiecePicker::new(l, &Bitfield::new(pieces))
    }

    #[test]
    fn peer_lost_decrements_availability() {
        let mut p = picker(3, 1);
        p.add_peer_pieces(PeerPieces::Some(&bf(&[1, 1, 0])));
        p.peer_has(2);
        check_invariants(&p, &[1, 1, 1], 0);
        p.peer_lost(1);
        assert_eq!(p.availability(1), 0);
        check_invariants(&p, &[1, 0, 1], 0);
        // Never below zero; out of range ignored.
        p.peer_lost(1);
        p.peer_lost(99);
        check_invariants(&p, &[1, 0, 1], 0);
        // A seed is materialised the way the actor does it.
        p.add_peer_pieces(PeerPieces::All);
        p.remove_peer_pieces(PeerPieces::All);
        p.add_peer_pieces(PeerPieces::Some(&bf(&[1, 1, 1])));
        p.peer_lost(0);
        check_invariants(&p, &[1, 1, 2], 0);
    }

    #[test]
    fn piece_lost_is_wanted_again() {
        let l = PieceLayout::new(3 * u64::from(B), B).unwrap();
        let mut have = Bitfield::new(3);
        have.set(0);
        have.set(1);
        let mut p = PiecePicker::new(l, &have);
        p.add_peer_pieces(PeerPieces::All);
        check_invariants(&p, &[0, 0, 0], 1);
        // A piece we lack or out of range: no effect.
        p.piece_lost(2);
        p.piece_lost(99);
        check_invariants(&p, &[0, 0, 0], 1);
        assert_eq!(p.wanted_left_bytes(), u64::from(B));
        p.piece_lost(1);
        check_invariants(&p, &[0, 0, 0], 1);
        assert!(!p.have().get(1));
        assert!(!p.is_finished());
        assert_eq!(p.wanted_left_bytes(), 2 * u64::from(B));
        let got = p.pick(PeerKey(1), PeerPieces::All, 2, &mut rng());
        assert!(got.contains(&blk(1, 0)));
        for b in got {
            assert!(p.on_block(PeerKey(1), b).accepted);
        }
        p.piece_passed(1);
        p.piece_passed(2);
        assert!(p.is_finished());
        check_invariants(&p, &[0, 0, 0], 1);
    }

    #[test]
    fn picks_rarest_first() {
        let l = PieceLayout::new(3 * 16_384, 16_384).unwrap();
        let mut p = PiecePicker::new(l, &Bitfield::new(3));
        for bits in [[1, 1, 1], [1, 0, 1], [1, 0, 0]] {
            p.add_peer_pieces(PeerPieces::Some(&bf(&bits)));
        }
        let got = p.pick(PeerKey(9), PeerPieces::All, 1, &mut rng());
        assert_eq!(
            got,
            vec![BlockRef {
                piece: 1,
                offset: 0,
                len: 16_384
            }]
        );
        // Then piece 2 (availability 2), then piece 0 (availability 3).
        let got = p.pick(PeerKey(9), PeerPieces::All, 2, &mut rng());
        assert_eq!(got, vec![blk(2, 0), blk(0, 0)]);
        // A seed raises every piece equally and changes nothing.
        p.add_peer_pieces(PeerPieces::All);
        assert!(p.pick(PeerKey(1), PeerPieces::All, 1, &mut rng()).len() == 1);
    }

    #[test]
    fn finishes_partial_pieces_first() {
        let mut p = picker(3, 2);
        p.add_peer_pieces(PeerPieces::Some(&bf(&[1, 1, 1])));
        p.add_peer_pieces(PeerPieces::Some(&bf(&[0, 1, 1])));
        p.add_peer_pieces(PeerPieces::Some(&bf(&[0, 0, 1])));
        // Piece 0 is rarest: A takes its first block.
        assert_eq!(
            p.pick(PeerKey(1), PeerPieces::All, 1, &mut rng()),
            vec![blk(0, 0)]
        );
        // Piece 0 becomes the most common piece, yet B finishes it first.
        for _ in 0..5 {
            p.peer_has(0);
        }
        assert_eq!(
            p.pick(PeerKey(2), PeerPieces::All, 2, &mut rng()),
            vec![blk(0, 1), blk(1, 0)]
        );
    }

    #[test]
    fn respects_priorities_and_skip() {
        let mut p = picker(4, 1);
        p.add_peer_pieces(PeerPieces::Some(&bf(&[1, 1, 1, 1])));
        p.add_peer_pieces(PeerPieces::Some(&bf(&[0, 0, 1, 1])));
        p.set_piece_priorities(&[0, 1, 7, 4]);
        let got = p.pick(PeerKey(1), PeerPieces::All, 10, &mut rng());
        assert_eq!(got, vec![blk(2, 0), blk(3, 0), blk(1, 0)]);
        assert_eq!(p.wanted_left_bytes(), 3 * u64::from(B));
        for i in 1..4 {
            assert!(p.on_block(PeerKey(1), blk(i, 0)).piece_complete);
            p.piece_passed(i);
        }
        assert_eq!(p.wanted_left_bytes(), 0);
        assert!(p.is_finished());
        assert!(
            p.pick(PeerKey(2), PeerPieces::All, 10, &mut rng())
                .is_empty()
        );
    }

    #[test]
    fn sequential_picks_lowest_index() {
        let mut p = picker(4, 1);
        p.add_peer_pieces(PeerPieces::Some(&bf(&[1, 1, 1, 0])));
        p.add_peer_pieces(PeerPieces::Some(&bf(&[1, 1, 0, 0])));
        p.set_sequential(true);
        assert_eq!(
            p.pick(PeerKey(1), PeerPieces::All, 2, &mut rng()),
            vec![blk(0, 0), blk(1, 0)]
        );
        // Priority still wins over index.
        p.set_piece_priorities(&[4, 4, 4, 7]);
        assert_eq!(
            p.pick(PeerKey(1), PeerPieces::All, 2, &mut rng()),
            vec![blk(3, 0), blk(2, 0)]
        );
    }

    #[test]
    fn never_picks_what_peer_lacks() {
        let mut p = picker(3, 2);
        let mine = bf(&[0, 0, 1]);
        p.add_peer_pieces(PeerPieces::Some(&mine));
        p.add_peer_pieces(PeerPieces::All);
        let got = p.pick(PeerKey(1), PeerPieces::Some(&mine), 10, &mut rng());
        assert_eq!(got, vec![blk(2, 0), blk(2, 1)]);
        // Nothing left that this peer has, even in end-game terms.
        assert!(
            p.pick(PeerKey(1), PeerPieces::Some(&mine), 10, &mut rng())
                .is_empty()
        );
        // A mis-sized bitfield: bits past the torrent are absent, no panic.
        let mut long = Bitfield::new(64);
        long.set(40);
        p.add_peer_pieces(PeerPieces::Some(&long));
        p.remove_peer_pieces(PeerPieces::Some(&long));
        p.peer_has(1_000_000);
        assert!(
            p.pick(PeerKey(2), PeerPieces::Some(&long), 10, &mut rng())
                .is_empty()
        );
        assert!(
            p.pick(
                PeerKey(2),
                PeerPieces::Some(&Bitfield::new(0)),
                10,
                &mut rng()
            )
            .is_empty()
        );
    }

    #[test]
    fn release_peer_returns_blocks() {
        let mut p = picker(2, 2);
        p.add_peer_pieces(PeerPieces::All);
        let only0 = bf(&[1, 0]);
        // A seeder takes piece 0, delivers one block and leaves.
        let a = p.pick(PeerKey(1), PeerPieces::Some(&only0), 4, &mut rng());
        assert_eq!(a, vec![blk(0, 0), blk(0, 1)]);
        assert!(p.on_block(PeerKey(1), blk(0, 0)).accepted);
        p.release_peer(PeerKey(1));
        assert!(!p.on_block(PeerKey(1), blk(0, 1)).accepted);
        // Another peer finishes the piece: the returned block comes first.
        let b = p.pick(PeerKey(2), PeerPieces::All, 1, &mut rng());
        assert_eq!(b, vec![blk(0, 1)]);
        assert!(p.on_block(PeerKey(2), blk(0, 1)).piece_complete);
        // A single released block goes back too.
        let c = p.pick(PeerKey(2), PeerPieces::All, 1, &mut rng());
        assert_eq!(c, vec![blk(1, 0)]);
        p.release(PeerKey(2), blk(1, 0));
        assert_eq!(
            p.pick(PeerKey(3), PeerPieces::All, 1, &mut rng()),
            vec![blk(1, 0)]
        );
    }

    #[test]
    fn failed_piece_is_repicked() {
        let mut p = picker(1, 2);
        p.add_peer_pieces(PeerPieces::All);
        let got = p.pick(PeerKey(1), PeerPieces::All, 2, &mut rng());
        assert_eq!(got.len(), 2);
        assert!(!p.on_block(PeerKey(1), got[0]).piece_complete);
        assert!(p.on_block(PeerKey(1), got[1]).piece_complete);
        // Waiting for the hash check: nothing to pick, not even in end-game.
        assert!(
            p.pick(PeerKey(2), PeerPieces::All, 2, &mut rng())
                .is_empty()
        );
        p.piece_failed(0);
        assert!(!p.is_finished());
        assert_eq!(p.wanted_left_bytes(), 2 * u64::from(B));
        let again = p.pick(PeerKey(2), PeerPieces::All, 2, &mut rng());
        assert_eq!(again, vec![blk(0, 0), blk(0, 1)]);
        assert!(!p.on_block(PeerKey(2), blk(0, 0)).piece_complete);
        assert!(p.on_block(PeerKey(2), blk(0, 1)).piece_complete);
        p.piece_passed(0);
        assert!(p.have().get(0));
        assert!(p.is_finished());
    }

    #[test]
    fn endgame_duplicates_and_cancels() {
        let mut p = picker(2, 1);
        p.add_peer_pieces(PeerPieces::All);
        let a = p.pick(PeerKey(1), PeerPieces::All, 2, &mut rng());
        assert_eq!(a.len(), 2);
        assert!(p.is_endgame());
        assert!(p.on_block(PeerKey(1), a[0]).piece_complete);
        let last = a[1];
        // B gets the last block too; A never gets it twice.
        assert_eq!(
            p.pick(PeerKey(2), PeerPieces::All, 4, &mut rng()),
            vec![last]
        );
        assert!(
            p.pick(PeerKey(1), PeerPieces::All, 4, &mut rng())
                .is_empty()
        );
        // At most two owners per block.
        assert!(
            p.pick(PeerKey(3), PeerPieces::All, 4, &mut rng())
                .is_empty()
        );
        let r = p.on_block(PeerKey(1), last);
        assert!(r.accepted && r.piece_complete);
        assert_eq!(r.cancel, vec![PeerKey(2)]);
        // B's copy is now a duplicate.
        let dup = p.on_block(PeerKey(2), last);
        assert!(!dup.accepted && !dup.piece_complete);
    }

    #[test]
    fn unrequested_block_not_accepted() {
        let mut p = picker(2, 2);
        p.add_peer_pieces(PeerPieces::All);
        assert!(!p.on_block(PeerKey(1), blk(0, 0)).accepted);
        let got = p.pick(PeerKey(1), PeerPieces::All, 1, &mut rng());
        // Another peer, a bogus block and an out-of-range piece are refused.
        assert!(!p.on_block(PeerKey(2), got[0]).accepted);
        let bogus = BlockRef {
            piece: got[0].piece,
            offset: 7,
            len: 3,
        };
        assert!(!p.on_block(PeerKey(1), bogus).accepted);
        assert!(!p.on_block(PeerKey(1), blk(99, 0)).accepted);
        p.release(PeerKey(1), blk(99, 0));
        p.piece_passed(99);
        p.piece_failed(99);
        // The real one is accepted once.
        assert!(p.on_block(PeerKey(1), got[0]).accepted);
        assert!(!p.on_block(PeerKey(1), got[0]).accepted);
    }

    #[test]
    fn pick_piece_starts_a_skipped_piece() {
        let mut p = picker(4, 2);
        p.add_peer_pieces(PeerPieces::All);
        p.set_piece_priorities(&[4, 4, 4, 0]);
        assert_eq!(p.pick_piece(PeerKey(1), 3, 1, &[]), vec![blk(3, 0)]);
        assert!(p.is_in_progress(3));
        check_invariants(&p, &[0, 0, 0, 0], 1);
        // Neither the normal pick nor end-game continues it.
        for peer in 2..5 {
            let got = p.pick(PeerKey(peer), PeerPieces::All, 100, &mut rng());
            assert!(got.iter().all(|b| b.piece != 3), "{got:?}");
        }
        assert!(p.is_endgame());
        check_invariants(&p, &[0, 0, 0, 0], 1);
        // Only `pick_piece` does.
        assert_eq!(p.pick_piece(PeerKey(1), 3, 10, &[]), vec![blk(3, 1)]);
        // Nothing for a piece out of range, one pending its hash, or one we have.
        assert!(p.pick_piece(PeerKey(1), 99, 10, &[]).is_empty());
        assert!(p.on_block(PeerKey(1), blk(3, 0)).accepted);
        assert!(p.on_block(PeerKey(1), blk(3, 1)).piece_complete);
        assert!(p.pick_piece(PeerKey(2), 3, 10, &[PeerKey(1)]).is_empty());
        p.piece_passed(3);
        assert!(p.pick_piece(PeerKey(2), 3, 10, &[]).is_empty());
        check_invariants(&p, &[0, 0, 0, 0], 1);
    }

    #[test]
    fn pick_piece_duplicates_once() {
        let (a, b, c) = (PeerKey(1), PeerKey(2), PeerKey(3));
        let mut p = picker(1, 2);
        p.add_peer_pieces(PeerPieces::All);
        let all = vec![blk(0, 0), blk(0, 1)];
        assert_eq!(p.pick(a, PeerPieces::All, 2, &mut rng()), all);
        // A late owner never duplicates to itself.
        assert!(p.pick_piece(a, 0, 10, &[a]).is_empty());
        assert_eq!(p.pick_piece(b, 0, 10, &[a]), all);
        // Two owners: no third copy, late or not.
        assert!(p.pick_piece(c, 0, 10, &[a]).is_empty());
        assert!(p.pick_piece(c, 0, 10, &[a, b]).is_empty());
        assert!(p.pick_piece(c, 0, 10, &[]).is_empty());
        check_invariants(&p, &[0], 1);
        let r = p.on_block(b, blk(0, 0));
        assert!(r.accepted);
        assert_eq!(r.cancel, vec![a]);
        check_invariants(&p, &[0], 1);
    }

    #[test]
    fn pick_piece_duplicates_only_late_owners() {
        let (a, b, d) = (PeerKey(1), PeerKey(2), PeerKey(4));
        let mut p = picker(1, 4);
        p.add_peer_pieces(PeerPieces::All);
        assert_eq!(p.pick_piece(a, 0, 2, &[]), vec![blk(0, 0), blk(0, 1)]);
        assert_eq!(p.pick_piece(d, 0, 2, &[]), vec![blk(0, 2), blk(0, 3)]);
        assert_eq!(p.pick_piece(b, 0, 10, &[a]), vec![blk(0, 0), blk(0, 1)]);
        // `max_blocks` bounds the duplicates too.
        assert_eq!(p.pick_piece(b, 0, 1, &[d]), vec![blk(0, 2)]);
        check_invariants(&p, &[0], 1);
    }

    #[test]
    fn progress_lists_owners_and_free() {
        let (a, b) = (PeerKey(1), PeerKey(2));
        let mut p = picker(2, 4);
        p.add_peer_pieces(PeerPieces::All);
        assert_eq!(
            p.progress(0),
            Some(PieceProgress {
                free: 4,
                owners: vec![]
            })
        );
        assert_eq!(p.pick_piece(b, 0, 1, &[]), vec![blk(0, 0)]);
        assert_eq!(p.pick_piece(a, 0, 2, &[]), vec![blk(0, 1), blk(0, 2)]);
        // Sorted, and A once although it holds two blocks.
        assert_eq!(
            p.progress(0),
            Some(PieceProgress {
                free: 1,
                owners: vec![a, b]
            })
        );
        // A received block has no owner any more.
        assert!(p.on_block(b, blk(0, 0)).accepted);
        assert_eq!(
            p.progress(0),
            Some(PieceProgress {
                free: 1,
                owners: vec![a]
            })
        );
        assert_eq!(p.progress(99), None);
        // Pending its hash, then passed: no progress to report.
        assert_eq!(p.pick_piece(b, 1, 4, &[]).len(), 4);
        for i in 0..4 {
            assert!(p.on_block(b, blk(1, i)).accepted);
        }
        assert_eq!(p.progress(1), None);
        p.piece_passed(1);
        assert_eq!(p.progress(1), None);
    }

    #[test]
    fn abandon_returns_outstanding_and_requeues() {
        let (a, b, c) = (PeerKey(1), PeerKey(2), PeerKey(3));
        let mut p = picker(3, 2);
        p.add_peer_pieces(PeerPieces::All);
        p.set_piece_priorities(&[4, 0, 4]);
        assert_eq!(p.pick_piece(a, 0, 1, &[]), vec![blk(0, 0)]);
        assert_eq!(p.pick_piece(b, 0, 1, &[]), vec![blk(0, 1)]);
        assert!(p.on_block(b, blk(0, 1)).accepted);
        assert_eq!(p.abandon(0), vec![(a, blk(0, 0))]);
        assert!(!p.is_in_progress(0));
        check_invariants(&p, &[0, 0, 0], 1);
        // The cancelled block is refused; the piece is picked afresh.
        assert!(!p.on_block(a, blk(0, 0)).accepted);
        p.set_sequential(true);
        let got = p.pick(c, PeerPieces::All, 2, &mut rng());
        assert_eq!(got, vec![blk(0, 0), blk(0, 1)]);
        // A skipped piece goes back to nothing, not to the buckets.
        assert_eq!(p.pick_piece(a, 1, 2, &[]), vec![blk(1, 0), blk(1, 1)]);
        assert_eq!(p.abandon(1), vec![(a, blk(1, 0)), (a, blk(1, 1))]);
        check_invariants(&p, &[0, 0, 0], 1);
        assert!(
            p.pick(c, PeerPieces::All, 10, &mut rng())
                .iter()
                .all(|b| b.piece != 1)
        );
        // Nothing to abandon: unstarted, out of range.
        assert!(p.abandon(1).is_empty());
        assert!(p.abandon(99).is_empty());
        check_invariants(&p, &[0, 0, 0], 1);
    }

    /// Checks every internal invariant of the picker against the expected
    /// availability (`avail` from bitfields, `seeds` from `HaveAll`).
    fn check_invariants(p: &PiecePicker, avail: &[u32], seeds: u32) {
        let n = p.layout.num_pieces();
        assert_eq!(p.seeds, seeds, "seed count");
        assert_eq!(p.avail, avail, "availability");
        let mut left_pieces = 0u32;
        let mut left_bytes = 0u64;
        for i in 0..n {
            let st = p.state_of(i);
            let prio = p.prio_of(i);
            assert_eq!(
                p.have.get(i),
                st == PieceState::Have,
                "have <-> state, piece {i}"
            );
            if prio > 0 && st != PieceState::Have {
                left_pieces += 1;
                left_bytes += u64::from(p.layout.piece_size(i));
            }
            // Queued exactly when wanted and unrequested: no wanted piece is
            // lost, nothing else is offered.
            let queued = p.pos[i as usize] != NOT_QUEUED;
            assert_eq!(
                queued,
                prio > 0 && st == PieceState::Unrequested,
                "queued piece {i} ({st:?}, prio {prio})"
            );
            if queued {
                let key = p.bucket_key(i);
                let bucket = p.buckets.get(&key).expect("piece's bucket");
                assert_eq!(
                    bucket.get(p.pos[i as usize] as usize),
                    Some(&i),
                    "pos of {i}"
                );
            }
            assert_eq!(p.seq.contains(&(Reverse(prio), i)), queued, "seq of {i}");
            let downloading = p.downloading.get(&i);
            assert_eq!(
                downloading.is_some(),
                matches!(st, PieceState::Partial | PieceState::PendingHash),
                "downloading <-> state, piece {i} ({st:?})"
            );
            let mut partial_expected = false;
            if let Some(d) = downloading {
                assert_eq!(d.blocks.len(), p.layout.blocks_in_piece(i) as usize);
                let received = d.blocks.iter().filter(|b| b.received).count();
                let free = d
                    .blocks
                    .iter()
                    .filter(|b| !b.received && b.owners.is_empty())
                    .count();
                assert_eq!(d.received, received, "received count of {i}");
                assert_eq!(d.free, free, "free count of {i}");
                assert_eq!(
                    st == PieceState::PendingHash,
                    received == d.blocks.len(),
                    "pending hash <-> all received, piece {i}"
                );
                assert!(
                    !(received == 0 && free == d.blocks.len()),
                    "untouched piece {i} still downloading"
                );
                for (bi, slot) in (0u32..).zip(&d.blocks) {
                    assert!(slot.owners.len() <= MAX_OWNERS, "owners of {i}/{bi}");
                    let distinct: HashSet<_> = slot.owners.iter().collect();
                    assert_eq!(distinct.len(), slot.owners.len(), "repeated owner");
                    assert!(!(slot.received && !slot.owners.is_empty()));
                    for o in &slot.owners {
                        assert!(
                            p.outstanding.get(o).is_some_and(|s| s.contains(&(i, bi))),
                            "owner {o:?} of {i}/{bi} missing from outstanding"
                        );
                    }
                }
                partial_expected = st == PieceState::Partial && prio > 0 && free > 0;
            }
            assert_eq!(
                p.partial.contains(&(Reverse(prio), i)),
                partial_expected,
                "partial set, piece {i}"
            );
        }
        // No stale key: a started priority-0 piece (from `pick_piece`) or a
        // re-prioritised one is never left in `partial` under another priority.
        for &(Reverse(pr), i) in &p.partial {
            assert_eq!(pr, p.prio_of(i), "partial key of {i}");
            assert!(pr > 0, "skipped piece {i} in partial");
        }
        assert!(p.buckets.values().all(|b| !b.is_empty()), "empty bucket");
        let queued: usize = p.buckets.values().map(Vec::len).sum();
        assert_eq!(queued, p.seq.len());
        assert_eq!(
            p.partial.len(),
            p.partial.iter().collect::<HashSet<_>>().len()
        );
        // Outstanding mirrors the owners exactly.
        for (peer, set) in &p.outstanding {
            assert!(!set.is_empty(), "empty outstanding set of {peer:?}");
            for &(i, bi) in set {
                let slot = p
                    .downloading
                    .get(&i)
                    .and_then(|d| d.blocks.get(bi as usize))
                    .expect("outstanding block exists");
                assert!(slot.owners.contains(peer), "{peer:?} not owner of {i}/{bi}");
            }
        }
        assert_eq!(p.wanted_left_pieces, left_pieces, "wanted_left_pieces");
        assert_eq!(p.wanted_left_bytes, left_bytes, "wanted_left_bytes");
    }

    /// A simulated remote peer: its pieces and whether they are counted.
    struct SimPeer {
        bits: Bitfield,
        seed: bool,
        counted: bool,
    }

    impl SimPeer {
        fn pieces(&self) -> PeerPieces<'_> {
            if self.seed {
                PeerPieces::All
            } else {
                PeerPieces::Some(&self.bits)
            }
        }
    }

    fn random_blocks(p: &PiecePicker, peer: PeerKey, r: &mut StdRng) -> Option<BlockRef> {
        let set = p.outstanding.get(&peer)?;
        let mut v: Vec<(u32, u32)> = set.iter().copied().collect();
        v.sort_unstable();
        let (i, bi) = v[r.random_range(0..v.len())];
        Some(p.layout.block(i, bi))
    }

    /// Random sequences of every picker operation keep every invariant, and
    /// the picker can always be driven to the end afterwards.
    #[test]
    fn random_operations_keep_invariants() {
        for seed in 0..300u64 {
            let mut r = StdRng::seed_from_u64(seed);
            let blocks_per_piece = r.random_range(1..=4u32);
            let piece_len = blocks_per_piece * B;
            let n = r.random_range(1..=12u32);
            // The last piece may be short, down to one byte.
            let total =
                u64::from(n - 1) * u64::from(piece_len) + r.random_range(1..=u64::from(piece_len));
            let layout = PieceLayout::new(total, piece_len).unwrap();
            let mut have = Bitfield::new(n);
            for i in 0..n {
                if r.random_bool(0.2) {
                    have.set(i);
                }
            }
            let mut p = PiecePicker::new(layout, &have);
            let mut peers: Vec<SimPeer> = (0..4)
                .map(|_| SimPeer {
                    bits: Bitfield::new(n),
                    seed: false,
                    counted: false,
                })
                .collect();
            let mut avail = vec![0u32; n as usize];
            let mut seeds = 0u32;
            let mut pending: Vec<u32> = Vec::new();
            check_invariants(&p, &avail, seeds);
            for _ in 0..80 {
                let k = r.random_range(0..peers.len());
                let key = PeerKey(k as u64);
                match r.random_range(0..14u32) {
                    0 | 1 => {
                        let max = r.random_range(1..=8);
                        let got = p.pick(key, peers[k].pieces(), max, &mut r);
                        assert!(got.len() <= max);
                        for b in &got {
                            assert!(peer_has_piece(peers[k].pieces(), b.piece));
                            // `pick` never continues a skipped piece, even in end-game.
                            assert!(p.prio_of(b.piece) > 0, "{b:?} of a skipped piece");
                        }
                    }
                    2 | 3 => {
                        if let Some(b) = random_blocks(&p, key, &mut r) {
                            let res = p.on_block(key, b);
                            assert!(res.accepted);
                            if res.piece_complete {
                                pending.push(b.piece);
                            }
                        } else {
                            // Unrequested data is refused.
                            let i = r.random_range(0..n);
                            assert!(!p.on_block(key, p.layout.block(i, 0)).accepted);
                        }
                    }
                    4 => {
                        if let Some(b) = random_blocks(&p, key, &mut r) {
                            p.release(key, b);
                        }
                    }
                    5 => p.release_peer(key),
                    6 => {
                        if !pending.is_empty() {
                            let i = pending.swap_remove(r.random_range(0..pending.len()));
                            if r.random_bool(0.7) {
                                p.piece_passed(i);
                            } else {
                                p.piece_failed(i);
                            }
                        } else {
                            // Hash results for pieces not pending are harmless.
                            p.piece_failed(r.random_range(0..n));
                        }
                    }
                    7 => {
                        let prio: Vec<u8> = (0..n).map(|_| r.random_range(0..=8)).collect();
                        p.set_piece_priorities(&prio);
                    }
                    8 => p.set_sequential(r.random_bool(0.5)),
                    12 => {
                        let i = r.random_range(0..n + 1);
                        let max = r.random_range(0..=8);
                        let late: Vec<PeerKey> = (0..peers.len() as u64)
                            .filter(|_| r.random_bool(0.4))
                            .map(PeerKey)
                            .collect();
                        let before: Vec<(bool, Vec<PeerKey>)> = (0..blocks_per_piece)
                            .map(|bi| {
                                p.downloading
                                    .get(&i)
                                    .and_then(|d| d.blocks.get(bi as usize))
                                    .map_or_else(
                                        || (false, Vec::new()),
                                        |s| (s.received, s.owners.clone()),
                                    )
                            })
                            .collect();
                        let got = p.pick_piece(key, i, max, &late);
                        assert!(got.len() <= max);
                        for b in &got {
                            assert_eq!(b.piece, i);
                            // Not received, and free before or held by one late owner only.
                            let (received, owners) = &before[(b.offset / B) as usize];
                            assert!(!received, "{b:?} was received");
                            assert!(
                                owners.is_empty()
                                    || (owners.len() == 1
                                        && owners[0] != key
                                        && late.contains(&owners[0])),
                                "{b:?} owned by {owners:?}"
                            );
                        }
                    }
                    13 => {
                        let i = r.random_range(0..n + 1);
                        let held: Vec<(PeerKey, BlockRef)> = p
                            .outstanding
                            .iter()
                            .flat_map(|(&k, set)| {
                                set.iter()
                                    .filter(|&&(pi, _)| pi == i)
                                    .map(move |&(pi, bi)| (k, pi, bi))
                            })
                            .map(|(k, pi, bi)| (k, p.layout.block(pi, bi)))
                            .collect();
                        let mut held = held;
                        held.sort_unstable();
                        let state = p.state_of(i);
                        let got = p.abandon(i);
                        if state == PieceState::Partial {
                            assert_eq!(got, held);
                            assert!(!p.is_in_progress(i));
                        } else {
                            assert!(got.is_empty());
                        }
                    }
                    9 => {
                        let sp = &mut peers[k];
                        if sp.counted {
                            p.remove_peer_pieces(sp.pieces());
                            if sp.seed {
                                seeds -= 1;
                            } else {
                                for i in sp.bits.iter_ones() {
                                    avail[i as usize] -= 1;
                                }
                            }
                            p.release_peer(key);
                            sp.counted = false;
                            sp.seed = false;
                            sp.bits = Bitfield::new(n);
                        } else {
                            sp.seed = r.random_bool(0.3);
                            for i in 0..n {
                                if r.random_bool(0.5) {
                                    sp.bits.set(i);
                                }
                            }
                            p.add_peer_pieces(sp.pieces());
                            if sp.seed {
                                seeds += 1;
                            } else {
                                for i in sp.bits.iter_ones() {
                                    avail[i as usize] += 1;
                                }
                            }
                            sp.counted = true;
                        }
                    }
                    _ => {
                        let sp = &mut peers[k];
                        let i = r.random_range(0..n);
                        if sp.counted && !sp.seed && !sp.bits.get(i) {
                            sp.bits.set(i);
                            p.peer_has(i);
                            avail[i as usize] += 1;
                        }
                    }
                }
                check_invariants(&p, &avail, seeds);
            }

            // Drain: every simulated peer leaves, a seed finishes the rest.
            for (k, sp) in peers.iter().enumerate() {
                p.release_peer(PeerKey(k as u64));
                if sp.counted {
                    p.remove_peer_pieces(sp.pieces());
                }
            }
            avail.iter_mut().for_each(|a| *a = 0);
            seeds = 0;
            for i in pending.drain(..) {
                p.piece_passed(i);
            }
            let seed_key = PeerKey(99);
            p.add_peer_pieces(PeerPieces::All);
            seeds += 1;
            check_invariants(&p, &avail, seeds);
            for _ in 0..(n * blocks_per_piece + 2) {
                if p.is_finished() {
                    break;
                }
                for b in p.pick(seed_key, PeerPieces::All, 1000, &mut r) {
                    let res = p.on_block(seed_key, b);
                    assert!(res.accepted, "seed {seed}: {b:?}");
                    if res.piece_complete {
                        p.piece_passed(b.piece);
                    }
                }
                check_invariants(&p, &avail, seeds);
            }
            assert!(p.is_finished(), "seed {seed}: picker never finished");
            assert_eq!(p.wanted_left_bytes(), 0, "seed {seed}");
        }
    }

    #[test]
    fn parole_peer_downloads_whole_pieces_alone() {
        let (a, b) = (PeerKey(1), PeerKey(2));
        let mut p = picker(2, 4);
        p.add_peer_pieces(PeerPieces::All);
        p.set_parole(a, true);
        // A starts piece 0 (or 1) and keeps it: B gets the other one.
        let got_a = p.pick(a, PeerPieces::All, 2, &mut rng());
        assert_eq!(got_a.len(), 2);
        let mine = got_a[0].piece;
        assert!(got_a.iter().all(|b| b.piece == mine));
        let got_b = p.pick(b, PeerPieces::All, 8, &mut rng());
        assert!(got_b.iter().all(|x| x.piece != mine), "{got_b:?}");
        // Every block is requested now: B's end-game skips A's piece, and
        // A (on parole) takes no duplicates, only its own free blocks.
        assert!(p.pick(b, PeerPieces::All, 8, &mut rng()).is_empty());
        let rest = p.pick(a, PeerPieces::All, 8, &mut rng());
        assert_eq!(rest, vec![blk(mine, 2), blk(mine, 3)]);
        assert!(p.take_forfeited().is_empty());
    }

    #[test]
    fn deadline_pieces_are_never_reserved() {
        let (a, b) = (PeerKey(1), PeerKey(2));
        let mut p = picker(2, 4);
        p.add_peer_pieces(PeerPieces::All);
        p.set_parole(a, true);
        // A deadline piece A starts is not A's alone: B joins it.
        assert_eq!(p.pick_piece(a, 0, 2, &[]), vec![blk(0, 0), blk(0, 1)]);
        let got_b = p.pick(b, PeerPieces::All, 2, &mut rng());
        assert_eq!(got_b, vec![blk(0, 2), blk(0, 3)]);
        // A piece reserved by a normal pick loses its reservation once a
        // reader's deadline reaches it.
        assert_eq!(p.pick(a, PeerPieces::All, 2, &mut rng()).len(), 2);
        assert!(p.pick(b, PeerPieces::All, 2, &mut rng()).is_empty());
        assert_eq!(p.pick_piece(b, 1, 2, &[]), vec![blk(1, 2), blk(1, 3)]);
    }

    #[test]
    fn leaving_parole_opens_reserved_pieces() {
        let (a, b) = (PeerKey(1), PeerKey(2));
        let mut p = picker(1, 4);
        p.add_peer_pieces(PeerPieces::All);
        p.set_parole(a, true);
        assert_eq!(p.pick(a, PeerPieces::All, 2, &mut rng()).len(), 2);
        assert!(p.pick(b, PeerPieces::All, 2, &mut rng()).is_empty());
        p.set_parole(a, false);
        let got_b = p.pick(b, PeerPieces::All, 2, &mut rng());
        assert_eq!(got_b, vec![blk(0, 2), blk(0, 3)]);
    }

    /// The owner of a reserved piece got blocks 0 and 1, asks for nothing
    /// more (no demand, nothing outstanding) and leaves: the piece is
    /// forfeited and another peer downloads all of it.
    #[test]
    fn departed_owner_forfeits_its_piece_with_nothing_outstanding() {
        let (a, b) = (PeerKey(1), PeerKey(2));
        let mut p = picker(1, 4);
        p.add_peer_pieces(PeerPieces::All);
        p.set_parole(a, true);
        let got = p.pick(a, PeerPieces::All, 2, &mut rng());
        assert_eq!(got, vec![blk(0, 0), blk(0, 1)]);
        for x in got {
            assert!(p.on_block(a, x).accepted);
        }
        assert!(p.pick(b, PeerPieces::All, 4, &mut rng()).is_empty());
        p.release_peer(a);
        assert_eq!(p.take_forfeited(), vec![0]);
        let got_b = p.pick(b, PeerPieces::All, 4, &mut rng());
        assert_eq!(got_b.len(), 4);
        let done: Vec<bool> = got_b
            .into_iter()
            .map(|x| p.on_block(b, x).piece_complete)
            .collect();
        assert_eq!(done, vec![false, false, false, true]);
        // An owner leaving a reserved piece with nothing received loses
        // nothing: the piece is just open again.
        let mut p = picker(1, 4);
        p.add_peer_pieces(PeerPieces::All);
        p.set_parole(a, true);
        assert_eq!(p.pick(a, PeerPieces::All, 2, &mut rng()).len(), 2);
        p.release_peer(a);
        assert!(p.take_forfeited().is_empty());
        assert_eq!(p.pick(b, PeerPieces::All, 8, &mut rng()).len(), 4);
    }

    /// A choke drops the owner's requests: the piece stays reserved with
    /// its received blocks, and after the unchoke the owner completes it
    /// without starting over.
    #[test]
    fn choke_keeps_the_reserved_piece_for_its_owner() {
        let (a, b) = (PeerKey(1), PeerKey(2));
        let mut p = picker(1, 4);
        p.add_peer_pieces(PeerPieces::All);
        p.set_parole(a, true);
        let got = p.pick(a, PeerPieces::All, 4, &mut rng());
        assert_eq!(got.len(), 4);
        assert!(p.on_block(a, got[0]).accepted);
        assert!(p.on_block(a, got[1]).accepted);
        // Choked: the rest are rejected.
        p.release(a, got[2]);
        p.release(a, got[3]);
        assert!(p.take_forfeited().is_empty());
        assert!(p.pick(b, PeerPieces::All, 4, &mut rng()).is_empty());
        // Unchoked: A asks again for exactly what is missing.
        let again = p.pick(a, PeerPieces::All, 4, &mut rng());
        assert_eq!(again, vec![blk(0, 2), blk(0, 3)]);
        assert!(!p.on_block(a, again[0]).piece_complete);
        assert!(p.on_block(a, again[1]).piece_complete);
        assert!(p.take_forfeited().is_empty());
    }

    #[test]
    fn stalled_owner_forfeits_after_the_grace_period() {
        let (a, b) = (PeerKey(1), PeerKey(2));
        let grace = Duration::from_secs(120);
        let t0 = Instant::now();
        let mut p = picker(1, 4);
        p.add_peer_pieces(PeerPieces::All);
        p.set_parole(a, true);
        let got = p.pick(a, PeerPieces::All, 4, &mut rng());
        assert!(p.on_block(a, got[0]).accepted);
        p.expire_exclusive(t0, grace);
        p.expire_exclusive(t0 + grace - Duration::from_millis(1), grace);
        assert!(p.take_forfeited().is_empty());
        // A block arrives: the grace period starts over.
        assert!(p.on_block(a, got[1]).accepted);
        p.expire_exclusive(t0 + grace, grace);
        p.expire_exclusive(t0 + 2 * grace - Duration::from_millis(1), grace);
        assert!(p.take_forfeited().is_empty());
        p.expire_exclusive(t0 + 2 * grace, grace);
        assert_eq!(p.take_forfeited(), vec![0]);
        assert_eq!(p.pick(b, PeerPieces::All, 8, &mut rng()).len(), 4);
        // Nothing received in the grace period: opened, not forfeited.
        let mut p = picker(1, 4);
        p.add_peer_pieces(PeerPieces::All);
        p.set_parole(a, true);
        assert_eq!(p.pick(a, PeerPieces::All, 2, &mut rng()).len(), 2);
        p.expire_exclusive(t0, grace);
        p.expire_exclusive(t0 + grace, grace);
        assert!(p.take_forfeited().is_empty());
        assert_eq!(p.pick(b, PeerPieces::All, 2, &mut rng()).len(), 2);
    }

    #[test]
    #[ignore = "perf: run with --release -- --ignored"]
    fn perf_100k_pieces_10k_picks() {
        let n = 100_000;
        let mut p = picker(n, 1);
        let mut r = rng();
        for k in 0..20u32 {
            let mut b = Bitfield::new(n);
            for i in (k % 7..n).step_by(3 + k as usize % 5) {
                b.set(i);
            }
            p.add_peer_pieces(PeerPieces::Some(&b));
        }
        p.add_peer_pieces(PeerPieces::All);
        let start = std::time::Instant::now();
        let mut picked = 0;
        for i in 0..10_000u64 {
            picked += p.pick(PeerKey(i % 50), PeerPieces::All, 1, &mut r).len();
        }
        let took = start.elapsed();
        eprintln!("10k picks over 100k pieces: {took:?}");
        assert_eq!(picked, 10_000);
        assert!(took < std::time::Duration::from_secs(2));
    }
}
