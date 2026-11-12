//! The pieces readers are waiting for, and when each is due.
//!
//! Every open reader holds a window of `(piece, deadline)` pairs (from
//! `stream::window`); a piece's deadline is the earliest over the readers
//! that hold it.

use std::collections::{BTreeMap, HashMap};

use tokio::time::Instant;

use crate::stream::window::MAX_DEADLINE_PIECES;

/// Identifies one reader of a torrent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ReaderId(pub u64);

/// Every reader's window, merged.
#[derive(Debug, Default)]
pub(super) struct Deadlines {
    /// Per reader: its window, one entry per piece, at most
    /// [`MAX_DEADLINE_PIECES`].
    windows: BTreeMap<ReaderId, Vec<(u32, Instant)>>,
    /// Per piece: its earliest deadline and how many readers hold it.
    merged: HashMap<u32, (Instant, usize)>,
    /// `merged` by deadline, then index.
    ordered: Vec<(u32, Instant)>,
}

impl Deadlines {
    /// Replaces `reader`'s window; returns the pieces no reader holds any more.
    pub fn set(&mut self, reader: ReaderId, mut window: Vec<(u32, Instant)>) -> Vec<u32> {
        // One entry per piece (its earliest), and never more than the cap.
        window.sort_unstable();
        window.dedup_by_key(|e| e.0);
        window.truncate(MAX_DEADLINE_PIECES);
        let unchanged = match self.windows.get(&reader) {
            Some(old) => *old == window,
            None => window.is_empty(),
        };
        if unchanged {
            return Vec::new();
        }
        let old = if window.is_empty() {
            self.windows.remove(&reader)
        } else {
            self.windows.insert(reader, window)
        };
        self.rebuild();
        self.orphans(old)
    }

    /// Forgets a reader; returns the pieces no reader holds any more.
    pub fn remove(&mut self, reader: ReaderId) -> Vec<u32> {
        let old = self.windows.remove(&reader);
        if old.is_none() {
            return Vec::new();
        }
        self.rebuild();
        self.orphans(old)
    }

    /// The earliest deadline of `piece` over the readers holding it.
    pub fn deadline(&self, piece: u32) -> Option<Instant> {
        self.merged.get(&piece).map(|&(t, _)| t)
    }

    /// Some reader's window holds `piece`.
    pub fn contains(&self, piece: u32) -> bool {
        self.deadline(piece).is_some()
    }

    /// Every piece with a deadline, earliest first (ties by index). Kept
    /// sorted and rebuilt only when `set`, `remove` or `piece_done`
    /// change something, so `fill` never sorts.
    pub fn ordered(&self) -> &[(u32, Instant)] {
        &self.ordered
    }

    /// The piece passed: dropped from every window until the next `set`.
    pub fn piece_done(&mut self, piece: u32) {
        if !self.contains(piece) {
            return;
        }
        for w in self.windows.values_mut() {
            w.retain(|&(p, _)| p != piece);
        }
        self.windows.retain(|_, w| !w.is_empty());
        self.rebuild();
    }

    /// How many readers' windows hold `piece`.
    pub fn holders(&self, piece: u32) -> usize {
        self.merged.get(&piece).map_or(0, |&(_, n)| n)
    }

    /// No reader holds any piece.
    pub fn is_empty(&self) -> bool {
        self.merged.is_empty()
    }

    fn rebuild(&mut self) {
        self.merged.clear();
        for w in self.windows.values() {
            for &(piece, at) in w {
                self.merged
                    .entry(piece)
                    .and_modify(|(t, n)| {
                        *t = (*t).min(at);
                        *n += 1;
                    })
                    .or_insert((at, 1));
            }
        }
        self.ordered = self.merged.iter().map(|(&p, &(t, _))| (p, t)).collect();
        self.ordered.sort_unstable_by_key(|&(p, t)| (t, p));
    }

    /// The pieces of a replaced or removed window that nobody holds now,
    /// ascending.
    fn orphans(&self, old: Option<Vec<(u32, Instant)>>) -> Vec<u32> {
        old.unwrap_or_default()
            .into_iter()
            .map(|(p, _)| p)
            .filter(|&p| !self.contains(p))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const A: ReaderId = ReaderId(1);
    const B: ReaderId = ReaderId(2);

    fn secs(now: Instant, s: u64) -> Instant {
        now + Duration::from_secs(s)
    }

    #[test]
    fn earliest_deadline_wins_across_readers() {
        let now = Instant::now();
        let mut d = Deadlines::default();
        assert!(d.is_empty());
        assert!(
            d.set(A, vec![(5, secs(now, 10)), (6, secs(now, 11))])
                .is_empty()
        );
        assert!(d.set(B, vec![(5, secs(now, 2))]).is_empty());
        assert_eq!(d.deadline(5), Some(secs(now, 2)));
        assert_eq!(d.deadline(6), Some(secs(now, 11)));
        assert_eq!(d.deadline(7), None);
        assert_eq!(d.holders(5), 2);
        assert_eq!(d.holders(6), 1);
        assert_eq!(d.holders(7), 0);
        assert!(d.contains(5) && d.contains(6) && !d.contains(7));
        assert!(!d.is_empty());
        // B moves on: A's later deadline is the piece's again.
        assert!(d.set(B, vec![]).is_empty());
        assert_eq!(d.deadline(5), Some(secs(now, 10)));
        assert_eq!(d.holders(5), 1);
    }

    #[test]
    fn set_returns_pieces_left_by_everyone() {
        let now = Instant::now();
        let mut d = Deadlines::default();
        d.set(A, vec![(1, now), (2, now), (3, now)]);
        d.set(B, vec![(3, now), (4, now)]);
        // A seeks: 1 and 2 are orphaned, 3 is still B's.
        assert_eq!(d.set(A, vec![(9, now)]), vec![1, 2]);
        assert!(d.contains(3) && d.contains(4) && d.contains(9));
        assert!(!d.contains(1) && !d.contains(2));
        // Setting the same window again orphans nothing.
        assert!(d.set(A, vec![(9, now)]).is_empty());
    }

    #[test]
    fn remove_returns_orphans() {
        let now = Instant::now();
        let mut d = Deadlines::default();
        d.set(A, vec![(1, now), (2, now)]);
        d.set(B, vec![(2, now), (3, now)]);
        assert_eq!(d.remove(A), vec![1]);
        assert_eq!(d.remove(A), Vec::<u32>::new());
        assert_eq!(d.remove(B), vec![2, 3]);
        assert!(d.is_empty());
        assert!(d.ordered().is_empty());
        // An empty window for a reader with none is a no-op.
        assert!(d.set(ReaderId(9), vec![]).is_empty());
        assert!(d.is_empty());
    }

    #[test]
    fn ordered_is_by_deadline_then_index() {
        let now = Instant::now();
        let mut d = Deadlines::default();
        d.set(
            A,
            vec![(7, secs(now, 3)), (2, secs(now, 5)), (4, secs(now, 3))],
        );
        d.set(B, vec![(9, secs(now, 1)), (2, secs(now, 4))]);
        assert_eq!(
            d.ordered(),
            &[
                (9, secs(now, 1)),
                (4, secs(now, 3)),
                (7, secs(now, 3)),
                (2, secs(now, 4)),
            ]
        );
    }

    #[test]
    fn piece_done_drops_it_from_every_window() {
        let now = Instant::now();
        let mut d = Deadlines::default();
        d.set(A, vec![(1, now), (2, now)]);
        d.set(B, vec![(2, now), (3, now)]);
        d.piece_done(2);
        assert!(!d.contains(2));
        assert_eq!(d.holders(2), 0);
        assert_eq!(d.deadline(2), None);
        assert_eq!(d.ordered(), &[(1, now), (3, now)]);
        // A done piece is no orphan when its readers move on.
        assert_eq!(d.remove(A), vec![1]);
        assert_eq!(d.remove(B), vec![3]);
        // The next `set` may bring it back.
        d.set(A, vec![(2, now)]);
        assert!(d.contains(2));
        // Unknown pieces are ignored.
        d.piece_done(99);
        assert_eq!(d.ordered(), &[(2, now)]);
    }
}
