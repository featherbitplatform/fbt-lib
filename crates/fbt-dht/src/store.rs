//! Peers announced to us (`announce_peer`), served back by `get_peers`
//! (as peers, or as BEP 33 bloom filters for a scrape) and sampled by
//! BEP 51 `sample_infohashes`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use rand::seq::IteratorRandom;
use rand::{Rng, RngExt};

use crate::bloom::BloomFilter;

use crate::id::is_local;
use crate::table::Family;

/// Announced peers older than this are dropped by [`PeerStore::expire`].
const PEER_TTL: Duration = Duration::from_secs(30 * 60);
/// Most endpoints one non-local IP may hold per info hash.
const MAX_PEERS_PER_IP: usize = 8;

#[derive(Debug, Clone, Copy)]
struct Entry {
    addr: SocketAddr,
    seen: Instant,
    /// BEP 33: the last announce said the peer is a seed.
    seed: bool,
}

/// Info hash -> announced peers, bounded on both axes. Peers are endpoints
/// (IP and port): a re-announce of the same endpoint refreshes its time. A
/// non-local IP holds at most [`MAX_PEERS_PER_IP`] endpoints per info hash;
/// beyond that its oldest endpoint is replaced. Local IPs are exempt, as in
/// the routing table. Each entry keeps the BEP 33 `seed` flag of its
/// latest announce; it changes neither bound.
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

    /// Records `peer` (a seed if `seed`) for `ih`. At capacity, a new info hash evicts the one
    /// with the fewest peers and a new endpoint replaces a random one (or,
    /// past the per-IP cap, the oldest endpoint of the same IP).
    pub(crate) fn announce(&mut self, ih: [u8; 20], peer: SocketAddr, seed: bool, now: Instant) {
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
            seed,
        };
        if let Some(e) = peers.iter_mut().find(|e| e.addr == peer) {
            *e = entry;
            return;
        }
        let ip = peer.ip();
        if !is_local(ip) && peers.iter().filter(|e| e.addr.ip() == ip).count() >= MAX_PEERS_PER_IP {
            if let Some(oldest) = peers
                .iter_mut()
                .filter(|e| e.addr.ip() == ip)
                .min_by_key(|e| e.seen)
            {
                *oldest = entry;
            }
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

    /// BEP 33 bloom filters of `ih`'s seeds and of its other peers (both
    /// families), or `None` for an unknown info hash.
    pub(crate) fn scrape(&self, ih: &[u8; 20]) -> Option<(BloomFilter, BloomFilter)> {
        let peers = self.torrents.get(ih)?;
        let (mut seeds, mut others) = (BloomFilter::default(), BloomFilter::default());
        for e in peers {
            if e.seed {
                seeds.insert(e.addr.ip());
            } else {
                others.insert(e.addr.ip());
            }
        }
        Some((seeds, others))
    }

    /// BEP 51: up to `max` distinct stored info hashes, sampled at random.
    pub(crate) fn sample(&self, rng: &mut impl Rng, max: usize) -> Vec<[u8; 20]> {
        self.torrents.keys().copied().sample(rng, max)
    }

    /// Number of info hashes stored (BEP 51 `num`).
    pub(crate) fn len(&self) -> usize {
        self.torrents.len()
    }

    /// Drops peers not announced for more than 30 minutes, and empty torrents.
    pub(crate) fn expire(&mut self, now: Instant) {
        self.torrents.retain(|_, peers| {
            peers.retain(|e| now.saturating_duration_since(e.seen) <= PEER_TTL);
            !peers.is_empty()
        });
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
            s.announce(a, peer(&format!("1.0.0.{k}:6881")), false, t0);
        }
        s.announce(b, peer("2.0.0.1:6881"), false, t0);
        assert_eq!(s.len(), 2);
        // a fourth peer replaces one: still 3
        s.announce(a, peer("1.0.0.4:6881"), false, t0);
        let got = s.peers(&a, Family::V4, 100, &mut r);
        assert_eq!(got.len(), 3);
        assert!(got.contains(&peer("1.0.0.4:6881")));
        // a re-announce of the same endpoint refreshes it, no duplicate
        s.announce(b, peer("2.0.0.1:6881"), false, t0 + MIN);
        assert_eq!(
            s.peers(&b, Family::V4, 100, &mut r),
            vec![peer("2.0.0.1:6881")]
        );
        // a third torrent evicts the one with the fewest peers (b)
        s.announce(c, peer("3.0.0.1:6881"), false, t0);
        assert_eq!(s.len(), 2);
        assert!(s.peers(&b, Family::V4, 100, &mut r).is_empty());
        assert_eq!(s.peers(&a, Family::V4, 100, &mut r).len(), 3);
        assert_eq!(s.peers(&c, Family::V4, 100, &mut r).len(), 1);
        // sampling honours max
        assert_eq!(s.peers(&a, Family::V4, 2, &mut r).len(), 2);
    }

    #[test]
    fn store_keeps_every_loopback_port() {
        let t0 = Instant::now();
        let mut r = StdRng::seed_from_u64(6);
        let mut s = PeerStore::new(10, 100);
        let ih = [4u8; 20];
        for port in 1..=(MAX_PEERS_PER_IP as u16 + 2) {
            s.announce(
                ih,
                SocketAddr::new("127.0.0.1".parse().unwrap(), port),
                false,
                t0,
            );
        }
        // local IPs are exempt from the per-IP cap: every port survives
        assert_eq!(
            s.peers(&ih, Family::V4, 100, &mut r).len(),
            MAX_PEERS_PER_IP + 2
        );
        let mut two = PeerStore::new(10, 100);
        two.announce(ih, peer("127.0.0.1:7001"), false, t0);
        two.announce(ih, peer("127.0.0.1:7002"), false, t0);
        let mut got = two.peers(&ih, Family::V4, 100, &mut r);
        got.sort();
        assert_eq!(got, vec![peer("127.0.0.1:7001"), peer("127.0.0.1:7002")]);
    }

    #[test]
    fn store_per_ip_cap_replaces_that_ips_oldest() {
        let t0 = Instant::now();
        let mut r = StdRng::seed_from_u64(8);
        let mut s = PeerStore::new(10, 100);
        let ih = [5u8; 20];
        s.announce(ih, peer("9.9.9.9:1"), false, t0);
        for k in 0..MAX_PEERS_PER_IP as u64 {
            let port = 1000 + k as u16;
            s.announce(
                ih,
                SocketAddr::new("8.8.8.8".parse().unwrap(), port),
                false,
                t0 + Duration::from_secs(k),
            );
        }
        assert_eq!(s.peers(&ih, Family::V4, 100, &mut r).len(), 9);
        // a 9th port from 8.8.8.8 replaces its oldest (port 1000)
        s.announce(ih, peer("8.8.8.8:2000"), false, t0 + MIN);
        let got = s.peers(&ih, Family::V4, 100, &mut r);
        assert_eq!(got.len(), 9);
        assert!(got.contains(&peer("8.8.8.8:2000")));
        assert!(!got.contains(&peer("8.8.8.8:1000")));
        assert!(got.contains(&peer("8.8.8.8:1001")));
        assert!(got.contains(&peer("9.9.9.9:1")), "other IPs untouched");
    }

    #[test]
    fn store_expires_after_30_minutes() {
        let t0 = Instant::now();
        let mut r = StdRng::seed_from_u64(4);
        let mut s = PeerStore::new(10, 10);
        let ih = [9u8; 20];
        s.announce(ih, peer("1.0.0.1:6881"), false, t0);
        s.announce(ih, peer("1.0.0.2:6881"), false, t0 + 20 * MIN);
        s.expire(t0 + 29 * MIN);
        assert_eq!(s.peers(&ih, Family::V4, 10, &mut r).len(), 2);
        s.expire(t0 + 31 * MIN);
        assert_eq!(
            s.peers(&ih, Family::V4, 10, &mut r),
            vec![peer("1.0.0.2:6881")]
        );
        s.expire(t0 + 51 * MIN);
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn scrape_filters_count_seeds_and_peers() {
        let t0 = Instant::now();
        let mut s = PeerStore::new(10, 100);
        let ih = [8u8; 20];
        for k in 0..10u8 {
            s.announce(ih, peer(&format!("10.1.{k}.1:6881")), k < 4, t0);
        }
        let (seeds, peers) = s.scrape(&ih).unwrap();
        assert!(
            seeds.estimate().abs_diff(4) <= 1,
            "seeds {}",
            seeds.estimate()
        );
        assert!(
            peers.estimate().abs_diff(6) <= 1,
            "peers {}",
            peers.estimate()
        );
        // a seed re-announcing as a leecher moves between the filters
        for k in 0..4u8 {
            s.announce(ih, peer(&format!("10.1.{k}.1:6881")), false, t0);
        }
        let (seeds, peers) = s.scrape(&ih).unwrap();
        assert_eq!(seeds.estimate(), 0);
        assert!(peers.estimate().abs_diff(10) <= 1);
        assert!(s.scrape(&[0u8; 20]).is_none());
    }

    #[test]
    fn sample_returns_distinct_stored_hashes() {
        let t0 = Instant::now();
        let mut r = StdRng::seed_from_u64(9);
        let mut s = PeerStore::new(100, 10);
        assert!(s.sample(&mut r, 20).is_empty());
        for k in 0..30u8 {
            s.announce([k; 20], peer("1.0.0.1:6881"), false, t0);
        }
        assert_eq!(s.len(), 30);
        let got = s.sample(&mut r, 20);
        assert_eq!(got.len(), 20);
        let distinct: std::collections::HashSet<_> = got.iter().collect();
        assert_eq!(distinct.len(), 20);
        assert!(
            got.iter()
                .all(|h| h[0] < 30 && h.iter().all(|b| *b == h[0]))
        );
        assert_eq!(s.sample(&mut r, 100).len(), 30);
    }

    #[test]
    fn store_peers_filtered_by_family() {
        let t0 = Instant::now();
        let mut r = StdRng::seed_from_u64(5);
        let mut s = PeerStore::new(10, 10);
        let ih = [7u8; 20];
        s.announce(ih, peer("1.0.0.1:6881"), false, t0);
        s.announce(ih, peer("[2001:db8::1]:6881"), false, t0);
        s.announce(ih, peer("[2001:db8::2]:6881"), false, t0);
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
