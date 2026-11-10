//! Peers announced to us (`announce_peer`), served back by `get_peers`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use rand::{Rng, RngExt};

use crate::table::Family;

/// Announced peers older than this are dropped by [`PeerStore::expire`].
const PEER_TTL: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone, Copy)]
struct Entry {
    addr: SocketAddr,
    seen: Instant,
}

/// Info hash -> announced peers, bounded on both axes. One entry per IP: a
/// re-announce from the same IP updates its port and time.
#[derive(Debug)]
pub(crate) struct PeerStore {
    max_torrents: usize,
    max_peers: usize,
    torrents: HashMap<[u8; 20], Vec<Entry>>,
}

impl PeerStore {
    pub(crate) fn new(max_torrents: usize, max_peers: usize) -> Self {
        PeerStore {
            max_torrents,
            max_peers,
            torrents: HashMap::new(),
        }
    }

    /// Records `peer` for `ih`. At capacity, a new info hash evicts the one
    /// with the fewest peers and a new peer replaces a random one.
    pub(crate) fn announce(&mut self, ih: [u8; 20], peer: SocketAddr, now: Instant) {
        if self.max_torrents == 0 || self.max_peers == 0 {
            return;
        }
        if !self.torrents.contains_key(&ih) && self.torrents.len() >= self.max_torrents {
            let victim = self
                .torrents
                .iter()
                .min_by_key(|(_, peers)| peers.len())
                .map(|(k, _)| *k);
            if let Some(victim) = victim {
                self.torrents.remove(&victim);
            }
        }
        let peers = self.torrents.entry(ih).or_default();
        let entry = Entry {
            addr: peer,
            seen: now,
        };
        if let Some(e) = peers.iter_mut().find(|e| e.addr.ip() == peer.ip()) {
            *e = entry;
        } else if peers.len() < self.max_peers {
            peers.push(entry);
        } else {
            let i = rand::rng().random_range(0..peers.len());
            if let Some(slot) = peers.get_mut(i) {
                *slot = entry;
            }
        }
    }

    /// Up to `max` peers of `family` for `ih`, sampled at random.
    pub(crate) fn peers(
        &self,
        ih: &[u8; 20],
        family: Family,
        max: usize,
        rng: &mut impl Rng,
    ) -> Vec<SocketAddr> {
        let Some(peers) = self.torrents.get(ih) else {
            return Vec::new();
        };
        let mut out: Vec<SocketAddr> = peers
            .iter()
            .map(|e| e.addr)
            .filter(|a| Family::of(a) == family)
            .collect();
        // partial Fisher-Yates: the first `max` slots become the sample
        let take = max.min(out.len());
        for i in 0..take {
            let j = rng.random_range(i..out.len());
            out.swap(i, j);
        }
        out.truncate(take);
        out
    }

    /// Drops peers not announced for more than 30 minutes, and empty torrents.
    pub(crate) fn expire(&mut self, now: Instant) {
        self.torrents.retain(|_, peers| {
            peers.retain(|e| now.saturating_duration_since(e.seen) <= PEER_TTL);
            !peers.is_empty()
        });
    }

    /// Number of info hashes stored.
    pub(crate) fn torrents(&self) -> usize {
        self.torrents.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    const MIN: Duration = Duration::from_secs(60);

    fn peer(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn store_caps_torrents_and_peers() {
        let t0 = Instant::now();
        let mut r = StdRng::seed_from_u64(3);
        let mut s = PeerStore::new(2, 3);
        let (a, b, c) = ([1u8; 20], [2u8; 20], [3u8; 20]);
        for k in 1..=3 {
            s.announce(a, peer(&format!("1.0.0.{k}:6881")), t0);
        }
        s.announce(b, peer("2.0.0.1:6881"), t0);
        assert_eq!(s.torrents(), 2);
        // a fourth peer replaces one: still 3
        s.announce(a, peer("1.0.0.4:6881"), t0);
        let got = s.peers(&a, Family::V4, 100, &mut r);
        assert_eq!(got.len(), 3);
        assert!(got.contains(&peer("1.0.0.4:6881")));
        // one entry per IP: a re-announce on a new port updates it
        s.announce(b, peer("2.0.0.1:7000"), t0);
        assert_eq!(
            s.peers(&b, Family::V4, 100, &mut r),
            vec![peer("2.0.0.1:7000")]
        );
        // a third torrent evicts the one with the fewest peers (b)
        s.announce(c, peer("3.0.0.1:6881"), t0);
        assert_eq!(s.torrents(), 2);
        assert!(s.peers(&b, Family::V4, 100, &mut r).is_empty());
        assert_eq!(s.peers(&a, Family::V4, 100, &mut r).len(), 3);
        assert_eq!(s.peers(&c, Family::V4, 100, &mut r).len(), 1);
        // sampling honours max
        assert_eq!(s.peers(&a, Family::V4, 2, &mut r).len(), 2);
    }

    #[test]
    fn store_expires_after_30_minutes() {
        let t0 = Instant::now();
        let mut r = StdRng::seed_from_u64(4);
        let mut s = PeerStore::new(10, 10);
        let ih = [9u8; 20];
        s.announce(ih, peer("1.0.0.1:6881"), t0);
        s.announce(ih, peer("1.0.0.2:6881"), t0 + 20 * MIN);
        s.expire(t0 + 29 * MIN);
        assert_eq!(s.peers(&ih, Family::V4, 10, &mut r).len(), 2);
        s.expire(t0 + 31 * MIN);
        assert_eq!(
            s.peers(&ih, Family::V4, 10, &mut r),
            vec![peer("1.0.0.2:6881")]
        );
        s.expire(t0 + 51 * MIN);
        assert_eq!(s.torrents(), 0);
    }

    #[test]
    fn store_peers_filtered_by_family() {
        let t0 = Instant::now();
        let mut r = StdRng::seed_from_u64(5);
        let mut s = PeerStore::new(10, 10);
        let ih = [7u8; 20];
        s.announce(ih, peer("1.0.0.1:6881"), t0);
        s.announce(ih, peer("[2001:db8::1]:6881"), t0);
        s.announce(ih, peer("[2001:db8::2]:6881"), t0);
        assert_eq!(
            s.peers(&ih, Family::V4, 10, &mut r),
            vec![peer("1.0.0.1:6881")]
        );
        let v6 = s.peers(&ih, Family::V6, 10, &mut r);
        assert_eq!(v6.len(), 2);
        assert!(v6.iter().all(|p| p.is_ipv6()));
        assert!(s.peers(&[0u8; 20], Family::V4, 10, &mut r).is_empty());
    }
}
