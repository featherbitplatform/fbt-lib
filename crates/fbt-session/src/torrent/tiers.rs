//! BEP 12 tracker tiers: the order trackers are tried in.
//!
//! Each tier is shuffled once. An announce walks the tiers from the first:
//! a failure moves on to the next URL of the tier, then to the next tier,
//! wrapping around after the last; a success moves the URL to the front of
//! its tier so it is tried first next time.

use rand::Rng;
use rand::seq::SliceRandom;

/// Tracker URLs in tiers with a cursor on the one to try next.
#[derive(Debug)]
pub(crate) struct Tiers {
    tiers: Vec<Vec<String>>,
    tier: usize,
    idx: usize,
    /// Failures since the last success or [`restart`](Self::restart).
    failed: usize,
}

impl Tiers {
    /// Tiers from the metainfo's `announce-list`; empty URLs and tiers are
    /// dropped and every tier is shuffled once.
    pub(crate) fn new<R: Rng + ?Sized>(tiers: Vec<Vec<String>>, rng: &mut R) -> Self {
        let tiers = tiers
            .into_iter()
            .map(|mut tier| {
                tier.retain(|u| !u.is_empty());
                tier.shuffle(rng);
                tier
            })
            .filter(|tier| !tier.is_empty())
            .collect();
        Tiers {
            tiers,
            tier: 0,
            idx: 0,
            failed: 0,
        }
    }

    /// No URL left.
    pub(crate) fn is_empty(&self) -> bool {
        self.tiers.is_empty()
    }

    fn total(&self) -> usize {
        self.tiers.iter().map(Vec::len).sum()
    }

    /// The first URL of each tier: the one that answered last, if any did.
    pub(crate) fn firsts(&self) -> Vec<String> {
        self.tiers
            .iter()
            .filter_map(|t| t.first().cloned())
            .collect()
    }

    /// The URL to try next.
    pub(crate) fn current(&self) -> Option<&str> {
        self.tiers
            .get(self.tier)
            .and_then(|t| t.get(self.idx))
            .map(String::as_str)
    }

    /// The current URL answered: it moves to the front of its tier.
    pub(crate) fn report_success(&mut self) {
        if let Some(tier) = self.tiers.get_mut(self.tier)
            && self.idx < tier.len()
        {
            let url = tier.remove(self.idx);
            tier.insert(0, url);
            self.idx = 0;
        }
        self.failed = 0;
    }

    /// The current URL failed: the cursor moves to the next URL of the tier,
    /// then to the next tier, wrapping around to the first.
    pub(crate) fn report_failure(&mut self) {
        self.failed = self.failed.saturating_add(1);
        self.idx = self.idx.saturating_add(1);
        self.normalize();
    }

    /// Moves a cursor past the end of its tier to the next tier, wrapping
    /// around after the last.
    fn normalize(&mut self) {
        let len = self.tiers.get(self.tier).map_or(0, Vec::len);
        if self.idx >= len {
            self.idx = 0;
            self.tier = self.tier.saturating_add(1);
        }
        if self.tier >= self.tiers.len() {
            self.tier = 0;
        }
    }

    /// Every URL failed since the last success or restart.
    pub(crate) fn all_failed_once(&self) -> bool {
        !self.is_empty() && self.failed >= self.total()
    }

    /// Starts a new walk from the first URL of the first tier.
    pub(crate) fn restart(&mut self) {
        self.tier = 0;
        self.idx = 0;
        self.failed = 0;
    }

    /// Drops the current URL for good (an unusable URL); the cursor moves
    /// to the URL after it.
    pub(crate) fn remove_current(&mut self) {
        let Some(tier) = self.tiers.get_mut(self.tier) else {
            return;
        };
        if self.idx < tier.len() {
            tier.remove(self.idx);
        }
        if tier.is_empty() {
            self.tiers.remove(self.tier);
            self.idx = 0;
        }
        self.normalize();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tiers(t: &[&[&str]]) -> Tiers {
        let v = t
            .iter()
            .map(|tier| tier.iter().map(|s| (*s).to_owned()).collect())
            .collect();
        Tiers::new(v, &mut rand::rng())
    }

    #[test]
    fn tier_advances_on_failure() {
        let mut t = tiers(&[&["a", "b"], &["c"]]);
        let first = t.current().unwrap().to_owned();
        assert!(first == "a" || first == "b");
        t.report_failure();
        let second = t.current().unwrap().to_owned();
        assert!(second == "a" || second == "b");
        assert_ne!(first, second, "the other URL of the first tier");
        assert!(!t.all_failed_once());
        t.report_failure();
        assert_eq!(t.current(), Some("c"), "then the next tier");
        assert!(!t.all_failed_once());
    }

    #[test]
    fn success_moves_to_front() {
        let mut t = tiers(&[&["a", "b", "c"]]);
        t.report_failure();
        t.report_failure();
        let third = t.current().unwrap().to_owned();
        t.report_success();
        assert_eq!(t.tiers[0][0], third, "moved to the front of its tier");
        assert_eq!(t.tiers[0].len(), 3);
        t.restart();
        assert_eq!(t.current(), Some(third.as_str()), "tried first next time");
        assert!(!t.all_failed_once());
    }

    #[test]
    fn wraps_to_first_tier() {
        let mut t = tiers(&[&["a"], &["b"]]);
        assert_eq!(t.current(), Some("a"));
        t.report_failure();
        assert_eq!(t.current(), Some("b"));
        t.report_failure();
        assert_eq!(t.current(), Some("a"), "wrapped around");
        assert!(t.all_failed_once());
        t.restart();
        assert!(!t.all_failed_once());
    }

    #[test]
    fn removal_and_empty_input() {
        let mut t = tiers(&[&[""], &["a"], &[], &["b"]]);
        assert_eq!(t.current(), Some("a"));
        t.remove_current();
        assert_eq!(t.current(), Some("b"));
        t.remove_current();
        assert!(t.is_empty());
        assert_eq!(t.current(), None);
        assert!(tiers(&[]).is_empty());
    }
}
