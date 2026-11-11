//! Choker: which interested peers get upload slots.
//!
//! While downloading, regular slots go to the peers uploading to us
//! fastest (tit-for-tat), as many as [`ChokingAlgorithm`] opens. While
//! seeding, [`SeedChokingAlgorithm`] ranks them. On top, optimistic slots
//! go to choked peers that waited longest for one, rotated every
//! [`ChokerConfig::rotate_rounds`] rechokes. Snubbed peers only ever get
//! optimistic slots.

use std::collections::{HashMap, HashSet, VecDeque};

use rand::seq::SliceRandom;
use tokio::time::Instant;

use crate::picker::PeerKey;
use crate::settings::Settings;

/// How regular upload slots are opened while downloading.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ChokingAlgorithm {
    /// `unchoke_slots` minus the optimistic slots, to the peers uploading
    /// to us fastest.
    #[default]
    FixedSlots,
    /// As many slots as the peers' rates earn, whatever `unchoke_slots`
    /// says: peers are unchoked fastest first while each one's rate to us
    /// is at least a threshold that starts at 1 KiB/s and grows by 2 KiB/s
    /// per slot opened (libtorrent's `rate_based_choker`). At least one
    /// slot is opened.
    RateBased,
}

/// How regular upload slots are given while seeding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum SeedChokingAlgorithm {
    /// Every interested peer gets a turn: a peer keeps its slot until it
    /// has received [`Settings::seeding_piece_quota`] pieces' worth of
    /// bytes or three rechokes have passed, then waits behind every other
    /// peer.
    #[default]
    RoundRobin,
    /// The peers we upload to fastest.
    FastestUpload,
    /// The peers that just started or are nearly done first (farthest from
    /// half the torrent), so peers in the middle, which can trade with
    /// others, wait.
    AntiLeech,
}

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
    /// The peer left our requests unanswered for `snub_timeout`.
    pub snubbed: bool,
    /// The peer's share of the torrent's pieces, in parts per million
    /// (0 to 1,000,000).
    pub progress_ppm: u32,
    /// Since when we unchoke it; `None` while choked.
    pub unchoked_since: Option<Instant>,
    /// Payload bytes uploaded to it since it was last unchoked.
    pub uploaded_since_unchoke: u64,
    /// When it last held an optimistic slot; `None` if never.
    pub last_optimistic: Option<Instant>,
}

/// Choker parameters; [`Default`] matches [`Settings::default`] with
/// 256 KiB pieces.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ChokerConfig {
    /// Upload slots, regular plus optimistic (`RateBased` opens its regular
    /// slots by rate instead).
    pub slots: usize,
    /// Optimistic slots (part of `slots`).
    pub optimistic: usize,
    /// The optimistic slots rotate every this many rechokes (at least 1).
    pub rotate_rounds: u64,
    /// Regular slots while downloading.
    pub algorithm: ChokingAlgorithm,
    /// Regular slots while seeding.
    pub seed_algorithm: SeedChokingAlgorithm,
    /// Bytes a round-robin seeding slot serves before it moves on.
    pub piece_quota_bytes: u64,
}

/// Piece length [`ChokerConfig::default`] counts the seeding quota in.
const DEFAULT_PIECE_LEN: u64 = 256 * 1024;
/// `RateBased`: the rate the first slot needs, bytes per second.
const RATE_THRESHOLD_START: u64 = 1024;
/// `RateBased`: how much the threshold grows per slot opened.
const RATE_THRESHOLD_STEP: u64 = 2048;
/// `RoundRobin`: rechokes a seeding slot is held without reaching its quota.
const ROUND_ROBIN_ROUNDS: usize = 3;

impl Default for ChokerConfig {
    fn default() -> Self {
        Self::from_settings(&Settings::default(), DEFAULT_PIECE_LEN)
    }
}

impl ChokerConfig {
    /// The choker settings of `s`, the seeding quota counted in pieces of
    /// `piece_len` bytes.
    pub(crate) fn from_settings(s: &Settings, piece_len: u64) -> Self {
        // The automatic count leaves at least one regular slot, so a single
        // slot is a regular one (rotated by the seeding algorithm) rather
        // than an optimistic one pinned for `optimistic_unchoke_interval`.
        let optimistic = if s.optimistic_unchoke_slots == 0 {
            (s.unchoke_slots / 5)
                .max(1)
                .min(s.unchoke_slots.saturating_sub(1))
        } else {
            s.optimistic_unchoke_slots
        };
        let rechoke = s.rechoke_interval.as_nanos().max(1);
        let rounds = s.optimistic_unchoke_interval.as_nanos() / rechoke;
        ChokerConfig {
            slots: s.unchoke_slots,
            optimistic,
            rotate_rounds: u64::try_from(rounds).unwrap_or(u64::MAX).max(1),
            algorithm: s.choking_algorithm,
            seed_algorithm: s.seed_choking_algorithm,
            piece_quota_bytes: u64::from(s.seeding_piece_quota).saturating_mul(piece_len),
        }
    }
}

/// Decides which interested peers get upload slots.
///
/// Regular slots are ranked as [`ChokerConfig::algorithm`] (downloading)
/// or [`ChokerConfig::seed_algorithm`] (seeding) say, ties by
/// [`PeerKey`]; snubbed peers get none. The optimistic slots go to the
/// interested peers left that are choked now, those that held one longest
/// ago (or never) first, ties at random; they rotate every
/// `rotate_rounds` rechokes and a vacated one is refilled at once. Peers
/// unchoked already (by a free slot between rechokes, or a regular slot
/// they just lost) are candidates only when no choked one is left, and the
/// current holders at a rotation only after them.
#[derive(Debug)]
pub struct Choker {
    cfg: ChokerConfig,
    round: u64,
    optimistic: Vec<PeerKey>,
    /// The last [`ROUND_ROBIN_ROUNDS`] rechoke times, oldest first.
    recent: VecDeque<Instant>,
    /// `RoundRobin`: the round each peer last held a regular seeding slot,
    /// so choked peers queue by how long they waited. Only connected peers
    /// are kept.
    served: HashMap<PeerKey, u64>,
}

impl Choker {
    /// Creates a choker.
    pub fn new(cfg: ChokerConfig) -> Self {
        Self {
            cfg,
            round: 0,
            optimistic: Vec::new(),
            recent: VecDeque::with_capacity(ROUND_ROBIN_ROUNDS + 1),
            served: HashMap::new(),
        }
    }

    /// The peers holding optimistic slots after the last rechoke.
    pub fn optimistic(&self) -> &[PeerKey] {
        &self.optimistic
    }

    /// Sets the round-robin seeding quota in bytes (the piece length is
    /// known only once the metadata is).
    pub(crate) fn set_piece_quota_bytes(&mut self, bytes: u64) {
        self.cfg.piece_quota_bytes = bytes;
    }

    /// Returns the peers to unchoke; every other peer is choked.
    pub fn rechoke(
        &mut self,
        peers: &[PeerChokeInfo],
        seeding: bool,
        now: Instant,
        rng: &mut impl rand::Rng,
    ) -> HashSet<PeerKey> {
        let rotate = self.round.is_multiple_of(self.cfg.rotate_rounds.max(1));
        let round = self.round;
        self.round = self.round.wrapping_add(1);
        let present: HashSet<PeerKey> = peers.iter().map(|p| p.key).collect();
        self.served.retain(|k, _| present.contains(k));
        let interested: Vec<&PeerChokeInfo> = peers.iter().filter(|p| p.interested).collect();
        let rate_based = !seeding && self.cfg.algorithm == ChokingAlgorithm::RateBased;
        let optimistic = if rate_based {
            self.cfg.optimistic
        } else {
            self.cfg.optimistic.min(self.cfg.slots)
        };
        let regular = self.regular(&interested, seeding, rate_based, optimistic);
        if seeding && self.cfg.seed_algorithm == SeedChokingAlgorithm::RoundRobin {
            for k in &regular {
                self.served.insert(*k, round);
            }
        }
        self.recent.push_back(now);
        while self.recent.len() > ROUND_ROBIN_ROUNDS {
            self.recent.pop_front();
        }
        let mut unchoked: HashSet<PeerKey> = regular.into_iter().collect();
        self.pick_optimistic(&interested, &unchoked, optimistic, rotate, rng);
        unchoked.extend(self.optimistic.iter().copied());
        unchoked
    }

    /// The regular slots, best first.
    fn regular(
        &self,
        interested: &[&PeerChokeInfo],
        seeding: bool,
        rate_based: bool,
        optimistic: usize,
    ) -> Vec<PeerKey> {
        let mut ranked: Vec<&PeerChokeInfo> =
            interested.iter().copied().filter(|p| !p.snubbed).collect();
        if !seeding {
            ranked.sort_by(|a, b| {
                b.rate_from_peer
                    .cmp(&a.rate_from_peer)
                    .then(a.key.cmp(&b.key))
            });
            if rate_based {
                return rate_based_slots(&ranked);
            }
        } else {
            match self.cfg.seed_algorithm {
                SeedChokingAlgorithm::FastestUpload => ranked
                    .sort_by(|a, b| b.rate_to_peer.cmp(&a.rate_to_peer).then(a.key.cmp(&b.key))),
                SeedChokingAlgorithm::AntiLeech => ranked.sort_by(|a, b| {
                    let dist = |p: &PeerChokeInfo| p.progress_ppm.abs_diff(500_000);
                    dist(b)
                        .cmp(&dist(a))
                        .then(b.rate_to_peer.cmp(&a.rate_to_peer))
                        .then(a.key.cmp(&b.key))
                }),
                SeedChokingAlgorithm::RoundRobin => self.round_robin_sort(&mut ranked),
            }
        }
        let n = self.cfg.slots.saturating_sub(optimistic);
        ranked.into_iter().take(n).map(|p| p.key).collect()
    }

    /// `RoundRobin` order: peers keeping their slot (by `unchoked_since`),
    /// then choked peers (never served first, then served longest ago),
    /// then peers whose turn is over (by `unchoked_since`).
    fn round_robin_sort(&self, ranked: &mut [&PeerChokeInfo]) {
        let quota = self.cfg.piece_quota_bytes;
        // An optimistic slot is not a regular turn: such a peer queues
        // with the choked ones.
        let regular_since = |p: &PeerChokeInfo| {
            p.unchoked_since
                .filter(|_| !self.optimistic.contains(&p.key))
        };
        let group = |p: &PeerChokeInfo| match regular_since(p) {
            None => 1u8,
            Some(since) => {
                // Rechokes run since it was unchoked.
                let rounds = self.recent.iter().filter(|&&r| r >= since).count();
                let done = p.uploaded_since_unchoke >= quota || rounds >= ROUND_ROBIN_ROUNDS;
                if done { 2 } else { 0 }
            }
        };
        ranked.sort_by(|a, b| {
            let (ga, gb) = (group(a), group(b));
            ga.cmp(&gb)
                .then_with(|| {
                    if ga == 1 {
                        let served = |p: &PeerChokeInfo| self.served.get(&p.key).copied();
                        served(a).cmp(&served(b))
                    } else {
                        regular_since(a).cmp(&regular_since(b))
                    }
                })
                .then(a.key.cmp(&b.key))
        });
    }

    /// Keeps or rotates the optimistic slots over the interested peers not
    /// in `regular`.
    fn pick_optimistic(
        &mut self,
        interested: &[&PeerChokeInfo],
        regular: &HashSet<PeerKey>,
        n: usize,
        rotate: bool,
        rng: &mut impl rand::Rng,
    ) {
        // Sorted by key so the rng alone decides ties.
        let mut rest: Vec<&PeerChokeInfo> = interested
            .iter()
            .copied()
            .filter(|p| !regular.contains(&p.key))
            .collect();
        rest.sort_unstable_by_key(|p| p.key);
        let current = std::mem::take(&mut self.optimistic);
        if !rotate {
            self.optimistic = current
                .iter()
                .copied()
                .filter(|k| rest.iter().any(|p| p.key == *k))
                .take(n)
                .collect();
        }
        if self.optimistic.len() >= n {
            return;
        }
        let kept = std::mem::take(&mut self.optimistic);
        rest.retain(|p| !kept.contains(&p.key));
        rest.shuffle(rng);
        // Choked peers before the current optimistic ones and those
        // already unchoked, each by `last_optimistic` (never first).
        rest.sort_by_key(|p| {
            (
                current.contains(&p.key),
                p.unchoked_since.is_some(),
                p.last_optimistic,
            )
        });
        let want = n - kept.len();
        self.optimistic = kept;
        self.optimistic
            .extend(rest.iter().take(want).map(|p| p.key));
    }
}

/// `RateBased`: unchokes in rank order while each rate reaches the growing
/// threshold, at least one.
fn rate_based_slots(ranked: &[&PeerChokeInfo]) -> Vec<PeerKey> {
    let mut threshold = RATE_THRESHOLD_START;
    let mut out = Vec::new();
    for p in ranked {
        if !out.is_empty() && p.rate_from_peer < threshold {
            break;
        }
        out.push(p.key);
        threshold = threshold.saturating_add(RATE_THRESHOLD_STEP);
    }
    out
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
            snubbed: false,
            progress_ppm: 0,
            unchoked_since: None,
            uploaded_since_unchoke: 0,
            last_optimistic: None,
        }
    }

    fn cfg(slots: usize, optimistic: usize) -> ChokerConfig {
        ChokerConfig {
            slots,
            optimistic,
            rotate_rounds: 3,
            algorithm: ChokingAlgorithm::FixedSlots,
            seed_algorithm: SeedChokingAlgorithm::FastestUpload,
            piece_quota_bytes: 20 * 16_384,
        }
    }

    /// The milestone 1 choker: `slots` with one optimistic, fastest-upload
    /// seeding.
    fn m1(slots: usize) -> Choker {
        Choker::new(cfg(slots, 1))
    }

    fn keys(set: &HashSet<PeerKey>) -> Vec<u64> {
        let mut v: Vec<u64> = set.iter().map(|k| k.0).collect();
        v.sort_unstable();
        v
    }

    fn now() -> Instant {
        Instant::now()
    }

    #[test]
    fn choker_unchokes_fastest_plus_optimistic() {
        // Downloading: fastest uploaders to us; seeding: fastest downloaders.
        let peers: Vec<_> = (1..=6).map(|k| peer(k, true, k * 10, 100 - k)).collect();
        let mut c = m1(3);
        let mut r = rng();
        let set = c.rechoke(&peers, false, now(), &mut r);
        assert_eq!(set.len(), 3);
        assert!(set.contains(&PeerKey(6)) && set.contains(&PeerKey(5)));
        let opt: Vec<u64> = keys(&set).into_iter().filter(|k| *k < 5).collect();
        assert_eq!(opt.len(), 1);

        let mut c = m1(3);
        let set = c.rechoke(&peers, true, now(), &mut r);
        assert_eq!(set.len(), 3);
        assert!(set.contains(&PeerKey(1)) && set.contains(&PeerKey(2)));

        // Ties on rate are broken by key, so the result is stable.
        let tied: Vec<_> = (1..=4).map(|k| peer(k, true, 5, 5)).collect();
        let a = m1(2).rechoke(&tied, false, now(), &mut rng());
        let b = m1(2).rechoke(&tied, false, now(), &mut rng());
        assert_eq!(a, b);
        assert!(a.contains(&PeerKey(1)));
        assert!(m1(0).rechoke(&tied, false, now(), &mut rng()).is_empty());
    }

    #[test]
    fn choker_rotates_optimistic_every_third_round() {
        let peers: Vec<_> = (1..=6).map(|k| peer(k, true, k * 10, 0)).collect();
        let mut c = m1(2);
        let mut r = rng();
        let optimistic = |set: &HashSet<PeerKey>| -> u64 {
            assert!(set.contains(&PeerKey(6)));
            assert_eq!(set.len(), 2);
            keys(set)[0]
        };
        let first = optimistic(&c.rechoke(&peers, false, now(), &mut r));
        assert_eq!(optimistic(&c.rechoke(&peers, false, now(), &mut r)), first);
        assert_eq!(optimistic(&c.rechoke(&peers, false, now(), &mut r)), first);
        let second = optimistic(&c.rechoke(&peers, false, now(), &mut r));
        assert_ne!(second, first);
        // The optimistic peer loses interest: replaced at once.
        let mut changed = peers.clone();
        changed.retain(|p| p.key != PeerKey(second));
        let third = optimistic(&c.rechoke(&changed, false, now(), &mut r));
        assert_ne!(third, second);
        assert_eq!(
            optimistic(&c.rechoke(&changed, false, now(), &mut r)),
            third
        );
    }

    #[test]
    fn choker_ignores_uninterested() {
        let mut c = m1(4);
        let mut r = rng();
        let peers = vec![
            peer(1, false, 1_000, 1_000),
            peer(2, true, 1, 1),
            peer(3, false, 500, 500),
            peer(4, true, 2, 2),
            peer(5, true, 3, 3),
        ];
        // Fewer interested peers than slots: all of them, and only them.
        assert_eq!(
            keys(&c.rechoke(&peers, false, now(), &mut r)),
            vec![2, 4, 5]
        );
        assert_eq!(keys(&c.rechoke(&peers, true, now(), &mut r)), vec![2, 4, 5]);
        let none: Vec<_> = peers.iter().map(|p| peer(p.key.0, false, 9, 9)).collect();
        assert!(c.rechoke(&none, false, now(), &mut r).is_empty());
    }

    #[test]
    fn rate_based_opens_slots_by_threshold() {
        let mut c = Choker::new(ChokerConfig {
            algorithm: ChokingAlgorithm::RateBased,
            ..cfg(4, 1)
        });
        let peers = vec![
            peer(1, true, 10_000, 0),
            peer(2, true, 5_000, 0),
            peer(3, true, 2_000, 0),
            peer(4, true, 1_000, 0),
        ];
        let mut r = rng();
        for _ in 0..8 {
            let set = c.rechoke(&peers, false, now(), &mut r);
            assert_eq!(set.len(), 3, "{set:?}");
            assert!(set.contains(&PeerKey(1)) && set.contains(&PeerKey(2)));
            assert_eq!(c.optimistic().len(), 1);
            assert!([PeerKey(3), PeerKey(4)].contains(&c.optimistic()[0]));
        }
        // Below the first threshold, one slot still opens.
        let slow = vec![peer(1, true, 10, 0), peer(2, true, 5, 0)];
        let mut c = Choker::new(ChokerConfig {
            algorithm: ChokingAlgorithm::RateBased,
            optimistic: 0,
            ..cfg(4, 1)
        });
        assert_eq!(keys(&c.rechoke(&slow, false, now(), &mut r)), vec![1]);
        // Many fast peers: more slots than `slots`.
        let fast: Vec<_> = (1..=8).map(|k| peer(k, true, 1 << 20, 0)).collect();
        assert_eq!(c.rechoke(&fast, false, now(), &mut r).len(), 8);
    }

    fn round_robin(slots: usize) -> Choker {
        Choker::new(ChokerConfig {
            seed_algorithm: SeedChokingAlgorithm::RoundRobin,
            ..cfg(slots, 0)
        })
    }

    /// Fills in `unchoked_since` and `uploaded_since_unchoke` as the actor
    /// would after `set`.
    fn apply(peers: &mut [PeerChokeInfo], set: &HashSet<PeerKey>, at: Instant) {
        for p in peers {
            if set.contains(&p.key) {
                if p.unchoked_since.is_none() {
                    p.unchoked_since = Some(at);
                    p.uploaded_since_unchoke = 0;
                }
            } else {
                p.unchoked_since = None;
            }
        }
    }

    #[test]
    fn round_robin_rotates_after_quota() {
        let mut c = round_robin(2);
        let mut r = rng();
        let mut peers: Vec<_> = (1..=4).map(|k| peer(k, true, 0, 100)).collect();
        let t0 = now();
        let set = c.rechoke(&peers, true, t0, &mut r);
        assert_eq!(keys(&set), vec![1, 2]);
        apply(&mut peers, &set, t0);
        // Peer 1 received its quota: its turn is over at once.
        peers[0].uploaded_since_unchoke = 20 * 16_384;
        peers[1].uploaded_since_unchoke = 20 * 16_384 - 1;
        let t1 = t0 + Duration::from_secs(10);
        let set = c.rechoke(&peers, true, t1, &mut r);
        assert_eq!(keys(&set), vec![2, 3]);
        apply(&mut peers, &set, t1);
        peers[1].uploaded_since_unchoke = 20 * 16_384;
        // Peer 2 is done too: peer 4 never had a turn, peer 1 had one.
        let t2 = t1 + Duration::from_secs(10);
        let set = c.rechoke(&peers, true, t2, &mut r);
        assert_eq!(keys(&set), vec![3, 4]);
        apply(&mut peers, &set, t2);
        peers[2].uploaded_since_unchoke = 20 * 16_384;
        let set = c.rechoke(&peers, true, t2 + Duration::from_secs(10), &mut r);
        assert_eq!(keys(&set), vec![1, 4]);
    }

    #[test]
    fn round_robin_rotates_after_three_rounds_without_quota() {
        let mut c = round_robin(2);
        let mut r = rng();
        let mut peers: Vec<_> = (1..=4).map(|k| peer(k, true, 0, 100)).collect();
        let t0 = now();
        let step = Duration::from_secs(10);
        let mut at = t0;
        let mut turns = Vec::new();
        for _ in 0..7 {
            let set = c.rechoke(&peers, true, at, &mut r);
            apply(&mut peers, &set, at);
            turns.push(keys(&set));
            at += step;
        }
        let a = vec![1, 2];
        let b = vec![3, 4];
        assert_eq!(
            turns,
            vec![a.clone(), a.clone(), a.clone(), b.clone(), b.clone(), b, a]
        );
    }

    #[test]
    fn anti_leech_prefers_extremes() {
        let mut c = Choker::new(ChokerConfig {
            seed_algorithm: SeedChokingAlgorithm::AntiLeech,
            ..cfg(2, 0)
        });
        let mut peers = vec![
            peer(1, true, 0, 900),
            peer(2, true, 0, 10),
            peer(3, true, 0, 10),
        ];
        peers[0].progress_ppm = 500_000;
        peers[1].progress_ppm = 50_000;
        peers[2].progress_ppm = 950_000;
        let set = c.rechoke(&peers, true, now(), &mut rng());
        assert_eq!(keys(&set), vec![2, 3]);
        // Equally far from the middle: the faster one.
        peers[0].progress_ppm = 50_000;
        let set = c.rechoke(&peers, true, now(), &mut rng());
        assert!(set.contains(&PeerKey(1)), "{set:?}");
    }

    #[test]
    fn fastest_upload_seeding() {
        let mut c = Choker::new(cfg(2, 0));
        let peers: Vec<_> = (1..=5).map(|k| peer(k, true, 1_000 * k, 10 * k)).collect();
        let set = c.rechoke(&peers, true, now(), &mut rng());
        assert_eq!(keys(&set), vec![4, 5]);
        // The rate from a peer does not matter while seeding.
        let peers: Vec<_> = (1..=5).map(|k| peer(k, true, 1_000 * k, 100 - k)).collect();
        assert_eq!(
            keys(&c.rechoke(&peers, true, now(), &mut rng())),
            vec![1, 2]
        );
    }

    #[test]
    fn optimistic_goes_to_longest_waiting() {
        let t0 = now();
        let mut peers: Vec<_> = (1..=4).map(|k| peer(k, true, 0, 0)).collect();
        peers[0].rate_from_peer = 1_000;
        peers[1].last_optimistic = Some(t0 + Duration::from_secs(20));
        peers[2].last_optimistic = Some(t0 + Duration::from_secs(10));
        let mut r = rng();
        // Never optimistic counts as longest waiting.
        let mut c = m1(2);
        assert_eq!(keys(&c.rechoke(&peers, false, t0, &mut r)), vec![1, 4]);
        assert_eq!(c.optimistic(), &[PeerKey(4)]);
        // Then the one that held it longest ago.
        peers[3].last_optimistic = Some(t0 + Duration::from_secs(30));
        let mut c = m1(2);
        assert_eq!(keys(&c.rechoke(&peers, false, t0, &mut r)), vec![1, 3]);
        // A peer unchoked already waits behind choked ones.
        peers[2].unchoked_since = Some(t0);
        let mut c = m1(2);
        assert_eq!(keys(&c.rechoke(&peers, false, t0, &mut r)), vec![1, 2]);
        // Ties are random.
        let tied: Vec<_> = (1..=9).map(|k| peer(k, true, 0, 0)).collect();
        let picks: HashSet<PeerKey> = (0..40)
            .flat_map(|_| {
                let mut c = Choker::new(cfg(1, 1));
                c.rechoke(&tied, false, t0, &mut r);
                c.optimistic().to_vec()
            })
            .collect();
        assert!(picks.len() > 3, "{picks:?}");
    }

    #[test]
    fn snubbed_only_optimistic() {
        let mut peers = vec![
            peer(1, true, 100_000, 0),
            peer(2, true, 50_000, 0),
            peer(3, true, 10_000, 0),
            peer(4, true, 5_000, 0),
        ];
        peers[0].snubbed = true;
        let mut r = rng();
        for _ in 0..10 {
            let mut c = m1(3);
            let set = c.rechoke(&peers, false, now(), &mut r);
            assert!(set.contains(&PeerKey(2)) && set.contains(&PeerKey(3)));
            assert_eq!(set.len(), 3);
            let opt = c.optimistic()[0];
            assert!(opt == PeerKey(1) || opt == PeerKey(4));
        }
        // Only snubbed peers: no regular slot, the optimistic one only.
        let snubbed: Vec<_> = (1..=3)
            .map(|k| PeerChokeInfo {
                snubbed: true,
                ..peer(k, true, 9_000, 9_000)
            })
            .collect();
        for seeding in [false, true] {
            let mut c = m1(4);
            let set = c.rechoke(&snubbed, seeding, now(), &mut r);
            assert_eq!(set.len(), 1);
            assert_eq!(c.optimistic().len(), 1);
        }
    }

    #[test]
    fn auto_optimistic_slots() {
        let mut s = Settings {
            unchoke_slots: 10,
            ..Settings::default()
        };
        assert_eq!(ChokerConfig::from_settings(&s, 1).optimistic, 2);
        s.unchoke_slots = 4;
        assert_eq!(ChokerConfig::from_settings(&s, 1).optimistic, 1);
        // At least one regular slot stays.
        s.unchoke_slots = 2;
        assert_eq!(ChokerConfig::from_settings(&s, 1).optimistic, 1);
        s.unchoke_slots = 1;
        assert_eq!(ChokerConfig::from_settings(&s, 1).optimistic, 0);
        s.unchoke_slots = 0;
        assert_eq!(ChokerConfig::from_settings(&s, 1).optimistic, 0);
        s.unchoke_slots = 4;
        s.optimistic_unchoke_slots = 3;
        assert_eq!(ChokerConfig::from_settings(&s, 1).optimistic, 3);
        // 30 s over 10 s rechokes; the quota in bytes.
        let c = ChokerConfig::from_settings(&s, 1_000);
        assert_eq!(c.rotate_rounds, 3);
        assert_eq!(c.piece_quota_bytes, 20_000);
        // Ten slots, twenty peers: eight regular, two optimistic.
        s.unchoke_slots = 10;
        s.optimistic_unchoke_slots = 0;
        let mut c = Choker::new(ChokerConfig::from_settings(&s, 1));
        let peers: Vec<_> = (1..=20).map(|k| peer(k, true, k * 100, 0)).collect();
        let set = c.rechoke(&peers, false, now(), &mut rng());
        assert_eq!(set.len(), 10);
        assert_eq!(c.optimistic().len(), 2);
        assert!((13..=20).all(|k| set.contains(&PeerKey(k))));
    }
}
