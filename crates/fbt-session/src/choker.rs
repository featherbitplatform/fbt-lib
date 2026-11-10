//! Choker: upload slots by transfer rate plus one rotating optimistic slot.

use std::collections::HashSet;

use rand::RngExt;

use crate::picker::PeerKey;

/// What the choker knows about one peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerChokeInfo {
    /// The peer.
    pub key: PeerKey,
    /// Whether the peer is interested in our pieces.
    pub interested: bool,
    /// Download rate from the peer, bytes per second.
    pub rate_from_peer: u64,
    /// Upload rate to the peer, bytes per second.
    pub rate_to_peer: u64,
}

/// The optimistic slot moves to another peer every this many rechokes.
const OPTIMISTIC_ROUNDS: u64 = 3;

/// Decides which interested peers get upload slots.
///
/// `slots - 1` regular slots go to the fastest interested peers (rate from
/// them while downloading, rate to them while seeding; ties by [`PeerKey`]),
/// and one optimistic slot goes to a random remaining interested peer,
/// rotated every third [`rechoke`](Self::rechoke). With at most `slots`
/// interested peers, all of them are unchoked.
#[derive(Debug)]
pub struct Choker {
    slots: usize,
    round: u64,
    optimistic: Option<PeerKey>,
}

impl Choker {
    /// Creates a choker with `slots` upload slots (regular plus optimistic).
    pub fn new(slots: usize) -> Self {
        Self {
            slots,
            round: 0,
            optimistic: None,
        }
    }

    /// Returns the peers to unchoke; every other peer is choked.
    pub fn rechoke(
        &mut self,
        peers: &[PeerChokeInfo],
        seeding: bool,
        rng: &mut impl rand::Rng,
    ) -> HashSet<PeerKey> {
        let rotate = self.round.is_multiple_of(OPTIMISTIC_ROUNDS);
        self.round = self.round.wrapping_add(1);
        let mut interested: Vec<&PeerChokeInfo> = peers.iter().filter(|p| p.interested).collect();
        if interested.len() <= self.slots {
            return interested.into_iter().map(|p| p.key).collect();
        }
        let rate = |p: &PeerChokeInfo| {
            if seeding {
                p.rate_to_peer
            } else {
                p.rate_from_peer
            }
        };
        interested.sort_by(|a, b| rate(b).cmp(&rate(a)).then(a.key.cmp(&b.key)));
        let regular = self.slots.saturating_sub(1);
        let mut unchoked: HashSet<PeerKey> =
            interested.iter().take(regular).map(|p| p.key).collect();
        if self.slots == 0 {
            return unchoked;
        }
        // Remaining candidates, sorted by key so the rng alone decides.
        let mut rest: Vec<PeerKey> = interested.iter().skip(regular).map(|p| p.key).collect();
        rest.sort_unstable();
        let current = self.optimistic.filter(|k| rest.contains(k));
        let chosen = match current {
            Some(k) if !rotate => k,
            _ => {
                // Prefer someone other than the current optimistic peer.
                if let Some(k) = current {
                    rest.retain(|&r| r != k);
                }
                // `rest` is never empty here: more interested peers than slots.
                let at = rng.random_range(0..rest.len().max(1));
                match rest.get(at) {
                    Some(&k) => k,
                    None => return unchoked,
                }
            }
        };
        self.optimistic = Some(chosen);
        unchoked.insert(chosen);
        unchoked
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    fn rng() -> StdRng {
        StdRng::seed_from_u64(11)
    }

    fn peer(key: u64, interested: bool, from: u64, to: u64) -> PeerChokeInfo {
        PeerChokeInfo {
            key: PeerKey(key),
            interested,
            rate_from_peer: from,
            rate_to_peer: to,
        }
    }

    fn keys(set: &HashSet<PeerKey>) -> Vec<u64> {
        let mut v: Vec<u64> = set.iter().map(|k| k.0).collect();
        v.sort_unstable();
        v
    }

    #[test]
    fn choker_unchokes_fastest_plus_optimistic() {
        // Downloading: fastest uploaders to us; seeding: fastest downloaders.
        let peers: Vec<_> = (1..=6).map(|k| peer(k, true, k * 10, 100 - k)).collect();
        let mut c = Choker::new(3);
        let mut r = rng();
        let set = c.rechoke(&peers, false, &mut r);
        assert_eq!(set.len(), 3);
        assert!(set.contains(&PeerKey(6)) && set.contains(&PeerKey(5)));
        let opt: Vec<u64> = keys(&set).into_iter().filter(|k| *k < 5).collect();
        assert_eq!(opt.len(), 1);

        let mut c = Choker::new(3);
        let set = c.rechoke(&peers, true, &mut r);
        assert_eq!(set.len(), 3);
        assert!(set.contains(&PeerKey(1)) && set.contains(&PeerKey(2)));

        // Ties on rate are broken by key, so the result is stable.
        let tied: Vec<_> = (1..=4).map(|k| peer(k, true, 5, 5)).collect();
        let a = Choker::new(2).rechoke(&tied, false, &mut rng());
        let b = Choker::new(2).rechoke(&tied, false, &mut rng());
        assert_eq!(a, b);
        assert!(a.contains(&PeerKey(1)));
        assert!(Choker::new(0).rechoke(&tied, false, &mut rng()).is_empty());
    }

    #[test]
    fn choker_rotates_optimistic_every_third_round() {
        let peers: Vec<_> = (1..=6).map(|k| peer(k, true, k * 10, 0)).collect();
        let mut c = Choker::new(2);
        let mut r = rng();
        let optimistic = |set: &HashSet<PeerKey>| -> u64 {
            assert!(set.contains(&PeerKey(6)));
            assert_eq!(set.len(), 2);
            keys(set)[0]
        };
        let first = optimistic(&c.rechoke(&peers, false, &mut r));
        assert_eq!(optimistic(&c.rechoke(&peers, false, &mut r)), first);
        assert_eq!(optimistic(&c.rechoke(&peers, false, &mut r)), first);
        let second = optimistic(&c.rechoke(&peers, false, &mut r));
        assert_ne!(second, first);
        // The optimistic peer loses interest: replaced at once.
        let mut changed = peers.clone();
        changed.retain(|p| p.key != PeerKey(second));
        let third = optimistic(&c.rechoke(&changed, false, &mut r));
        assert_ne!(third, second);
        assert_eq!(optimistic(&c.rechoke(&changed, false, &mut r)), third);
    }

    #[test]
    fn choker_ignores_uninterested() {
        let mut c = Choker::new(4);
        let mut r = rng();
        let peers = vec![
            peer(1, false, 1_000, 1_000),
            peer(2, true, 1, 1),
            peer(3, false, 500, 500),
            peer(4, true, 2, 2),
            peer(5, true, 3, 3),
        ];
        // Fewer interested peers than slots: all of them, and only them.
        assert_eq!(keys(&c.rechoke(&peers, false, &mut r)), vec![2, 4, 5]);
        assert_eq!(keys(&c.rechoke(&peers, true, &mut r)), vec![2, 4, 5]);
        let none: Vec<_> = peers.iter().map(|p| peer(p.key.0, false, 9, 9)).collect();
        assert!(c.rechoke(&none, false, &mut r).is_empty());
    }
}
