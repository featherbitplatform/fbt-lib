//! Smart ban: who sent the blocks of a piece that failed its hash check.
//!
//! As libtorrent's `smart_ban` plugin, plus its trust points. Every block
//! the picker accepts remembers its sender. When a piece fails, the SHA-1 of
//! each of its blocks is kept with that sender (at most
//! [`MAX_SMART_BAN_PIECES`] pieces); when the piece later passes, every
//! sender whose block differs from the good one is banned. Independently,
//! each sender's trust drops by 2 per failed piece it contributed to and
//! rises by 1 per passed piece (up to 8): at −7 it is banned, so a peer
//! whose data never passes goes after its fourth failed piece.
//!
//! This is separate from the per-torrent ban of peers that sent bad
//! metadata (`BanTarget`).

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;

use crate::picker::PeerKey;

/// Most failed pieces whose block hashes are kept; the oldest goes first.
pub(super) const MAX_SMART_BAN_PIECES: usize = 64;
/// Most senders whose trust is tracked; beyond it the most trusted entry
/// is dropped.
pub(super) const MAX_TRUST_ENTRIES: usize = 4096;

/// Trust lost per failed piece a sender contributed to.
const FAIL_POINTS: i32 = -2;
/// Trust gained per passed piece a sender contributed to.
const PASS_POINTS: i32 = 1;
/// Highest trust a sender can hold.
const MAX_TRUST: i32 = 8;
/// A sender at or below this trust is banned.
const BAN_TRUST: i32 = -7;

/// Who sent a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum BlockSource {
    /// A peer, by its ban key: port 0 (its whole IP) unless `ban_endpoints`.
    Peer(SocketAddr),
    /// A web seed.
    #[allow(dead_code, reason = "web seeds (Task 11) are its first producer")]
    WebSeed(PeerKey),
}

/// A failed piece's blocks: each block's sender and SHA-1, by block index
/// (`None` where no sender was recorded).
#[derive(Debug)]
struct Failed {
    blocks: Vec<Option<(BlockSource, [u8; 20])>>,
}

/// One torrent's smart-ban state.
#[derive(Debug, Default)]
pub(super) struct SmartBan {
    /// Senders of the accepted blocks of pieces in progress (or hashing), by
    /// block index. Holds only pieces with a buffer in `Loaded.bufs` or a
    /// hash job in flight, so it is bounded as those are.
    senders: HashMap<u32, Vec<Option<BlockSource>>>,
    /// Failed pieces' block records, at most [`MAX_SMART_BAN_PIECES`].
    failed: HashMap<u32, Failed>,
    /// `failed`'s keys, oldest first.
    failed_order: VecDeque<u32>,
    /// Trust points per sender, at most [`MAX_TRUST_ENTRIES`].
    trust: HashMap<BlockSource, i32>,
}

impl SmartBan {
    /// Block `block_index` of `piece`, just accepted by the picker, came
    /// from `source`.
    pub(super) fn record_block(&mut self, piece: u32, block_index: u32, source: BlockSource) {
        let blocks = self.senders.entry(piece).or_default();
        let i = block_index as usize;
        if blocks.len() <= i {
            blocks.resize(i + 1, None);
        }
        if let Some(slot) = blocks.get_mut(i) {
            *slot = Some(source);
        }
    }

    /// Whether the hash job of `piece` should return its block hashes even
    /// if it passes: an earlier failure of it is on record.
    pub(super) fn wants_block_hashes(&self, piece: u32) -> bool {
        self.failed.contains_key(&piece)
    }

    /// `piece` failed its hash check with these block hashes. Records each
    /// block's sender and hash (replacing an earlier record of the piece),
    /// takes trust from every contributor and returns those whose trust
    /// fell to [`BAN_TRUST`] or below.
    pub(super) fn piece_failed(
        &mut self,
        piece: u32,
        block_hashes: Vec<[u8; 20]>,
    ) -> Vec<BlockSource> {
        let senders = self.senders.remove(&piece).unwrap_or_default();
        let blocks: Vec<Option<(BlockSource, [u8; 20])>> = block_hashes
            .into_iter()
            .enumerate()
            .map(|(i, h)| senders.get(i).copied().flatten().map(|s| (s, h)))
            .collect();
        let contributors = distinct(senders.iter().copied().flatten());
        if self.failed.insert(piece, Failed { blocks }).is_none() {
            self.failed_order.push_back(piece);
        }
        while self.failed_order.len() > MAX_SMART_BAN_PIECES {
            if let Some(old) = self.failed_order.pop_front() {
                self.failed.remove(&old);
            }
        }
        let mut banned = Vec::new();
        for s in contributors {
            if self.adjust(s, FAIL_POINTS) <= BAN_TRUST {
                self.trust.remove(&s);
                banned.push(s);
            }
        }
        banned
    }

    /// `piece` passed. With an earlier failure on record and the good
    /// block hashes, returns every sender of a block that differs from the
    /// good one (and drops the record). Every contributor of the passed
    /// piece gains trust.
    pub(super) fn piece_passed(
        &mut self,
        piece: u32,
        block_hashes: Option<Vec<[u8; 20]>>,
    ) -> Vec<BlockSource> {
        let senders = self.senders.remove(&piece).unwrap_or_default();
        let mut banned = Vec::new();
        if let Some(failed) = self.failed.remove(&piece) {
            self.failed_order.retain(|&p| p != piece);
            if let Some(good) = block_hashes {
                let liars = failed
                    .blocks
                    .iter()
                    .zip(&good)
                    .filter_map(|(rec, g)| rec.filter(|(_, h)| h != g).map(|(s, _)| s));
                banned = distinct(liars);
            }
        }
        for s in &banned {
            self.trust.remove(s);
        }
        for s in distinct(senders.iter().copied().flatten()) {
            if !banned.contains(&s) {
                self.adjust(s, PASS_POINTS);
            }
        }
        banned
    }

    /// Drops what is recorded of `piece`: its download was abandoned or it
    /// is no longer wanted.
    pub(super) fn forget(&mut self, piece: u32) {
        self.senders.remove(&piece);
        if self.failed.remove(&piece).is_some() {
            self.failed_order.retain(|&p| p != piece);
        }
    }

    /// Drops every piece's records (a recheck, an error); trust stays.
    pub(super) fn forget_all(&mut self) {
        self.senders.clear();
        self.failed.clear();
        self.failed_order.clear();
    }

    /// Adds `delta` to `s`'s trust (capped at [`MAX_TRUST`]) and returns
    /// the new value.
    fn adjust(&mut self, s: BlockSource, delta: i32) -> i32 {
        if !self.trust.contains_key(&s) && self.trust.len() >= MAX_TRUST_ENTRIES {
            // Forget the most trusted sender: it is the least likely to be
            // banned.
            if let Some(top) = self.trust.iter().max_by_key(|&(_, &t)| t).map(|(&k, _)| k) {
                self.trust.remove(&top);
            }
        }
        let t = self.trust.entry(s).or_insert(0);
        *t = t.saturating_add(delta).min(MAX_TRUST);
        *t
    }
}

/// The items of `it` without repeats, in first-seen order.
fn distinct(it: impl Iterator<Item = BlockSource>) -> Vec<BlockSource> {
    let mut out: Vec<BlockSource> = Vec::new();
    for s in it {
        if !out.contains(&s) {
            out.push(s);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(n: u8) -> BlockSource {
        BlockSource::Peer(SocketAddr::from(([10, 0, 0, n], 0)))
    }

    fn h(n: u8) -> [u8; 20] {
        [n; 20]
    }

    /// `s` sends both blocks of `piece`, which then fails.
    fn fail_alone(sb: &mut SmartBan, piece: u32, s: BlockSource) -> Vec<BlockSource> {
        sb.record_block(piece, 0, s);
        sb.record_block(piece, 1, s);
        sb.piece_failed(piece, vec![h(1), h(2)])
    }

    /// `s` sends both blocks of `piece`, which then passes.
    fn pass_alone(sb: &mut SmartBan, piece: u32, s: BlockSource) -> Vec<BlockSource> {
        sb.record_block(piece, 0, s);
        sb.record_block(piece, 1, s);
        let hashes = sb.wants_block_hashes(piece).then(|| vec![h(1), h(2)]);
        sb.piece_passed(piece, hashes)
    }

    #[test]
    fn trust_points_ban_after_four_failures() {
        let mut sb = SmartBan::default();
        let a = peer(1);
        for piece in 0..3 {
            assert!(fail_alone(&mut sb, piece, a).is_empty(), "piece {piece}");
        }
        assert_eq!(sb.trust.get(&a), Some(&-6));
        assert_eq!(fail_alone(&mut sb, 3, a), vec![a]);
        assert!(!sb.trust.contains_key(&a), "a banned source is forgotten");
    }

    #[test]
    fn a_passed_piece_restores_trust() {
        let mut sb = SmartBan::default();
        let a = peer(1);
        for piece in 0..3 {
            assert!(fail_alone(&mut sb, piece, a).is_empty());
        }
        // Two good pieces (the same data as recorded: nobody is blamed).
        assert!(pass_alone(&mut sb, 0, a).is_empty());
        assert!(pass_alone(&mut sb, 1, a).is_empty());
        assert_eq!(sb.trust.get(&a), Some(&-4));
        // A fourth failure no longer bans; a fifth does.
        assert!(fail_alone(&mut sb, 10, a).is_empty());
        assert_eq!(fail_alone(&mut sb, 11, a), vec![a]);
        // Trust is capped at 8.
        let b = peer(2);
        for piece in 20..40 {
            assert!(pass_alone(&mut sb, piece, b).is_empty());
        }
        assert_eq!(sb.trust.get(&b), Some(&MAX_TRUST));
    }

    #[test]
    fn records_are_bounded() {
        let mut sb = SmartBan::default();
        let total = MAX_SMART_BAN_PIECES as u32 + 1;
        for piece in 0..total {
            // A different sender each time: nobody reaches the ban.
            let s = BlockSource::WebSeed(PeerKey(u64::from(piece)));
            assert!(fail_alone(&mut sb, piece, s).is_empty());
        }
        assert_eq!(sb.failed.len(), MAX_SMART_BAN_PIECES);
        assert_eq!(sb.failed_order.len(), MAX_SMART_BAN_PIECES);
        assert!(!sb.wants_block_hashes(0), "the oldest record is dropped");
        assert!(sb.wants_block_hashes(total - 1));
        // Failing a recorded piece again replaces its record.
        assert!(fail_alone(&mut sb, total - 1, peer(9)).is_empty());
        assert_eq!(sb.failed.len(), MAX_SMART_BAN_PIECES);

        for i in 0..=MAX_TRUST_ENTRIES as u64 {
            sb.adjust(BlockSource::WebSeed(PeerKey(1_000 + i)), -1);
        }
        assert_eq!(sb.trust.len(), MAX_TRUST_ENTRIES);

        sb.forget(total - 1);
        assert!(!sb.wants_block_hashes(total - 1));
        sb.record_block(500, 0, peer(1));
        sb.forget_all();
        assert!(sb.failed.is_empty() && sb.failed_order.is_empty() && sb.senders.is_empty());
    }

    #[test]
    fn only_differing_blocks_are_blamed() {
        let mut sb = SmartBan::default();
        let (a, b, c) = (peer(1), peer(2), peer(3));
        // A sent block 0 (bad), B blocks 1 and 2 (good).
        sb.record_block(7, 0, a);
        sb.record_block(7, 1, b);
        sb.record_block(7, 2, b);
        assert!(sb.piece_failed(7, vec![h(9), h(2), h(3)]).is_empty());
        assert!(sb.wants_block_hashes(7));
        // The retry: C sends every block; block 0 now hashes differently.
        sb.record_block(7, 0, c);
        sb.record_block(7, 1, c);
        sb.record_block(7, 2, c);
        assert_eq!(sb.piece_passed(7, Some(vec![h(1), h(2), h(3)])), vec![a]);
        assert!(!sb.wants_block_hashes(7), "the record is used up");
        assert_eq!(sb.trust.get(&b), Some(&-2), "B keeps its trust");
        assert_eq!(sb.trust.get(&c), Some(&1));
        assert!(!sb.trust.contains_key(&a));
    }
}
