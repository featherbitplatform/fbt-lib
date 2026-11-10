//! Routing table: 160 fixed k-buckets with replacement caches (BEP 5).
//!
//! A node is good if heard from within [`QUESTIONABLE_AFTER`], questionable
//! otherwise, and bad after [`BAD_AFTER_FAILURES`] consecutive failures.
//! Inserting into a full bucket replaces a bad node, or asks for a ping of
//! the oldest questionable one (the newcomer waits in the replacement cache),
//! or drops the newcomer.

use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use rand::Rng;

use crate::id::{NODE_ID_LEN, NodeId, is_local};
use crate::krpc::NodeInfo;

/// Entries per bucket.
pub(crate) const K: usize = 8;
/// Replacement-cache entries per bucket.
const REPLACEMENTS: usize = 8;
/// A node not heard from for this long is questionable.
const QUESTIONABLE_AFTER: Duration = Duration::from_secs(15 * 60);
/// Consecutive failures that make a node bad.
const BAD_AFTER_FAILURES: u32 = 2;
/// One bucket per possible shared-prefix length.
const BUCKETS: usize = NODE_ID_LEN * 8;

/// An address family; a table holds nodes of one family only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    /// IPv4.
    V4,
    /// IPv6.
    V6,
}

impl Family {
    pub(crate) fn of(addr: &SocketAddr) -> Family {
        match addr {
            SocketAddr::V4(_) => Family::V4,
            SocketAddr::V6(_) => Family::V6,
        }
    }
}

/// The outcome of [`RoutingTable::insert`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Insert {
    /// The node is new in the table.
    Added,
    /// The node was already there; it is refreshed.
    Updated,
    /// The bucket is full: ping `ping`; if it fails, the newcomer replaces it.
    Pending { ping: NodeInfo },
    /// The node is not stored.
    Rejected,
}

#[derive(Debug, Clone, Copy)]
struct Entry {
    info: NodeInfo,
    last_seen: Instant,
    failures: u32,
}

impl Entry {
    fn new(info: NodeInfo, now: Instant) -> Self {
        Entry {
            info,
            last_seen: now,
            failures: 0,
        }
    }

    fn is_bad(&self) -> bool {
        self.failures >= BAD_AFTER_FAILURES
    }

    fn is_good(&self, now: Instant) -> bool {
        !self.is_bad() && now.saturating_duration_since(self.last_seen) < QUESTIONABLE_AFTER
    }
}

#[derive(Debug, Default)]
struct Bucket {
    nodes: Vec<Entry>,
    /// Newest last.
    cache: VecDeque<Entry>,
}

impl Bucket {
    fn push_cache(&mut self, e: Entry) {
        self.cache.retain(|c| c.info.id != e.info.id);
        while self.cache.len() >= REPLACEMENTS {
            self.cache.pop_front();
        }
        self.cache.push_back(e);
    }
}

/// The routing table of one address family.
#[derive(Debug)]
pub(crate) struct RoutingTable {
    own: NodeId,
    family: Family,
    buckets: Vec<Bucket>,
    /// Non-local IP -> the one node id stored for it.
    ips: HashMap<IpAddr, NodeId>,
}

/// Port 0, unspecified and multicast addresses cannot be dialed; an
/// IPv4-mapped IPv6 address is an IPv4 node in disguise and is refused too.
fn dialable(addr: &SocketAddr) -> bool {
    let ip = addr.ip();
    let mapped = matches!(ip, IpAddr::V6(v6) if v6.to_ipv4_mapped().is_some());
    addr.port() != 0 && !ip.is_unspecified() && !ip.is_multicast() && !mapped
}

impl RoutingTable {
    pub(crate) fn new(own: NodeId, family: Family) -> Self {
        RoutingTable {
            own,
            family,
            buckets: (0..BUCKETS).map(|_| Bucket::default()).collect(),
            ips: HashMap::new(),
        }
    }

    pub(crate) fn own_id(&self) -> NodeId {
        self.own
    }

    /// Bucket index and position of `id` in the table.
    fn find(&self, id: &NodeId) -> Option<(usize, usize)> {
        let b = self.own.bucket_index(id)?;
        let p = self
            .buckets
            .get(b)?
            .nodes
            .iter()
            .position(|e| e.info.id == *id)?;
        Some((b, p))
    }

    fn entry_mut(&mut self, b: usize, p: usize) -> Option<&mut Entry> {
        self.buckets.get_mut(b)?.nodes.get_mut(p)
    }

    fn index_ip(&mut self, info: &NodeInfo) {
        let ip = info.addr.ip();
        if !is_local(ip) {
            self.ips.insert(ip, info.id);
        }
    }

    fn unindex_ip(&mut self, info: &NodeInfo) {
        let ip = info.addr.ip();
        if self.ips.get(&ip) == Some(&info.id) {
            self.ips.remove(&ip);
        }
    }

    /// Whether `ip` is free for `id`: local, unused, or used by `id` itself.
    fn ip_free(&self, ip: IpAddr, id: &NodeId) -> bool {
        is_local(ip) || self.ips.get(&ip).is_none_or(|other| other == id)
    }

    /// Removes the table entry at `(b, p)`.
    fn remove_at(&mut self, b: usize, p: usize) {
        let Some(bucket) = self.buckets.get_mut(b) else {
            return;
        };
        if p < bucket.nodes.len() {
            let e = bucket.nodes.swap_remove(p);
            self.unindex_ip(&e.info);
        }
    }

    /// Adds `n`, heard from at `now`, following the bucket rules.
    pub(crate) fn insert(&mut self, n: NodeInfo, now: Instant) -> Insert {
        if n.id == self.own || Family::of(&n.addr) != self.family || !dialable(&n.addr) {
            return Insert::Rejected;
        }
        let Some(b) = self.own.bucket_index(&n.id) else {
            return Insert::Rejected;
        };
        let ip = n.addr.ip();

        if let Some((_, p)) = self.find(&n.id) {
            let Some(e) = self.entry_mut(b, p) else {
                return Insert::Rejected;
            };
            if e.info.addr == n.addr {
                *e = Entry::new(n, now);
                return Insert::Updated;
            }
            // anti-hijack: a known id moves only once its old entry is bad
            if !e.is_bad() {
                return Insert::Rejected;
            }
            let old = e.info;
            if !self.ip_free(ip, &n.id) {
                return Insert::Rejected;
            }
            self.unindex_ip(&old);
            if let Some(e) = self.entry_mut(b, p) {
                *e = Entry::new(n, now);
            }
            self.index_ip(&n);
            return Insert::Updated;
        }

        // one node per non-local IP; a bad holder gives way
        if !self.ip_free(ip, &n.id) {
            let holder = self.ips.get(&ip).and_then(|other| self.find(other));
            match holder {
                Some((hb, hp))
                    if self
                        .buckets
                        .get(hb)
                        .and_then(|bk| bk.nodes.get(hp))
                        .is_some_and(Entry::is_bad) =>
                {
                    self.remove_at(hb, hp);
                }
                _ => return Insert::Rejected,
            }
        }

        let Some(bucket) = self.buckets.get_mut(b) else {
            return Insert::Rejected;
        };
        bucket.cache.retain(|c| c.info.id != n.id);
        if bucket.nodes.len() < K {
            bucket.nodes.push(Entry::new(n, now));
            self.index_ip(&n);
            return Insert::Added;
        }
        if let Some(slot) = bucket.nodes.iter_mut().find(|e| e.is_bad()) {
            let old = slot.info;
            *slot = Entry::new(n, now);
            self.unindex_ip(&old);
            self.index_ip(&n);
            return Insert::Added;
        }
        let oldest_questionable = bucket
            .nodes
            .iter()
            .filter(|e| !e.is_good(now))
            .min_by_key(|e| e.last_seen)
            .map(|e| e.info);
        match oldest_questionable {
            Some(ping) => {
                bucket.push_cache(Entry::new(n, now));
                Insert::Pending { ping }
            }
            None => Insert::Rejected,
        }
    }

    /// A reply from `id` at `addr`: it is good again. Ignored when `addr` is
    /// not the address the table holds for `id`.
    pub(crate) fn heard_from(&mut self, id: &NodeId, addr: SocketAddr, now: Instant) {
        if let Some((b, p)) = self.find(id)
            && let Some(e) = self.entry_mut(b, p)
            && e.info.addr == addr
        {
            e.last_seen = now;
            e.failures = 0;
        }
    }

    /// A query to `id` failed. A node that turns bad is replaced by the
    /// newest usable entry of its bucket's replacement cache, if any.
    pub(crate) fn failed(&mut self, id: &NodeId) {
        let Some((b, p)) = self.find(id) else {
            if let Some(bucket) = self
                .own
                .bucket_index(id)
                .and_then(|b| self.buckets.get_mut(b))
            {
                bucket.cache.retain(|c| c.info.id != *id);
            }
            return;
        };
        let Some(e) = self.entry_mut(b, p) else {
            return;
        };
        e.failures = e.failures.saturating_add(1);
        if !e.is_bad() {
            return;
        }
        let old = e.info;
        while let Some(next) = self.buckets.get_mut(b).and_then(|bk| bk.cache.pop_back()) {
            if self.ip_free(next.info.addr.ip(), &next.info.id) {
                self.unindex_ip(&old);
                if let Some(slot) = self.entry_mut(b, p) {
                    *slot = next;
                }
                self.index_ip(&next.info);
                return;
            }
        }
    }

    /// Up to `n` non-bad nodes, closest to `target` first.
    pub(crate) fn closest(&self, target: &NodeId, n: usize) -> Vec<NodeInfo> {
        let mut all: Vec<&Entry> = self
            .buckets
            .iter()
            .flat_map(|b| &b.nodes)
            .filter(|e| !e.is_bad())
            .collect();
        all.sort_by_key(|e| e.info.id.distance(target));
        all.into_iter().take(n).map(|e| e.info).collect()
    }

    /// Number of nodes in the table (replacement caches excluded).
    pub(crate) fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.nodes.len()).sum()
    }

    /// Non-empty buckets with no node heard from in the last 15 minutes.
    pub(crate) fn stale_buckets(&self, now: Instant) -> Vec<usize> {
        self.buckets
            .iter()
            .enumerate()
            .filter(|(_, b)| {
                b.nodes
                    .iter()
                    .map(|e| e.last_seen)
                    .max()
                    .is_some_and(|t| now.saturating_duration_since(t) >= QUESTIONABLE_AFTER)
            })
            .map(|(i, _)| i)
            .collect()
    }

    /// A random id that falls in bucket `i` (clamped to the last bucket):
    /// it shares exactly `i` leading bits with our id.
    pub(crate) fn random_id_in_bucket(&self, i: usize, rng: &mut impl Rng) -> NodeId {
        let i = i.min(BUCKETS - 1);
        let mut id = NodeId::random(rng).0;
        for (k, (b, o)) in id.iter_mut().zip(self.own.0).enumerate() {
            let start = k * 8;
            if start + 8 <= i {
                *b = o;
            } else if start <= i {
                let r = i - start;
                let prefix = !(0xffu8 >> r);
                let flip = 0x80u8 >> r;
                *b = (o & prefix) | (!o & flip) | (*b & !(prefix | flip));
            }
        }
        NodeId(id)
    }

    /// Every node in the table.
    pub(crate) fn nodes(&self) -> impl Iterator<Item = &NodeInfo> {
        self.buckets.iter().flat_map(|b| &b.nodes).map(|e| &e.info)
    }

    /// Changes our id (BEP 42) and re-buckets every node. Nodes that no
    /// longer fit their new bucket move to its replacement cache; a node
    /// whose id equals the new own id is dropped.
    pub(crate) fn reset_own(&mut self, own: NodeId) {
        let old = std::mem::replace(
            &mut self.buckets,
            (0..BUCKETS).map(|_| Bucket::default()).collect(),
        );
        self.own = own;
        self.ips.clear();
        for bucket in &old {
            for e in &bucket.nodes {
                let Some(b) = own.bucket_index(&e.info.id) else {
                    continue;
                };
                let Some(nb) = self.buckets.get_mut(b) else {
                    continue;
                };
                if nb.nodes.len() < K {
                    nb.nodes.push(*e);
                    self.index_ip(&e.info);
                } else {
                    nb.push_cache(*e);
                }
            }
        }
        for c in old.iter().flat_map(|b| &b.cache) {
            if let Some(nb) = own
                .bucket_index(&c.info.id)
                .and_then(|b| self.buckets.get_mut(b))
            {
                nb.push_cache(*c);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use std::net::{IpAddr, Ipv4Addr};

    fn rng() -> StdRng {
        StdRng::seed_from_u64(7)
    }

    fn node(id: [u8; 20], addr: &str) -> NodeInfo {
        NodeInfo {
            id: NodeId(id),
            addr: addr.parse().unwrap(),
        }
    }

    /// A node in bucket 0 of the all-zero own id, on public IP `1.0.0.k`.
    fn far(k: u8) -> NodeInfo {
        let mut id = [0u8; 20];
        id[0] = 0x80;
        id[19] = k;
        node(id, &format!("1.0.0.{k}:6881"))
    }

    fn zero_table() -> RoutingTable {
        RoutingTable::new(NodeId([0; 20]), Family::V4)
    }

    fn contains(t: &RoutingTable, id: &NodeId) -> bool {
        t.nodes().any(|n| n.id == *id)
    }

    const MIN: Duration = Duration::from_secs(60);

    #[test]
    fn insert_until_full_then_pending_ping_then_replace_on_failure() {
        let t0 = Instant::now();
        let mut t = zero_table();
        for k in 0..8u8 {
            assert_eq!(t.insert(far(k), t0), Insert::Added);
        }
        // node 0 is the oldest: the others are heard from a bit later
        for k in 1..8u8 {
            t.heard_from(&far(k).id, far(k).addr, t0 + Duration::from_secs(k.into()));
        }
        assert_eq!(t.len(), 8);
        let ninth = far(8);
        assert_eq!(t.insert(ninth, t0), Insert::Rejected, "all good: dropped");
        assert!(!contains(&t, &ninth.id));

        let later = t0 + 16 * MIN;
        assert_eq!(t.insert(ninth, later), Insert::Pending { ping: far(0) });
        assert!(!contains(&t, &ninth.id), "waits in the replacement cache");
        t.failed(&far(0).id);
        assert!(!contains(&t, &ninth.id), "one failure is not bad yet");
        t.failed(&far(0).id);
        assert!(contains(&t, &ninth.id));
        assert!(!contains(&t, &far(0).id));
        assert_eq!(t.len(), 8);
    }

    #[test]
    fn bad_node_replaced_immediately() {
        let t0 = Instant::now();
        let mut t = zero_table();
        for k in 0..8u8 {
            t.insert(far(k), t0);
        }
        t.failed(&far(3).id);
        t.failed(&far(3).id);
        // no replacement waiting: the bad node stays, but is never returned
        assert!(contains(&t, &far(3).id));
        assert!(
            !t.closest(&NodeId([0xff; 20]), 8)
                .iter()
                .any(|n| n.id == far(3).id)
        );
        assert_eq!(t.insert(far(8), t0), Insert::Added);
        assert!(contains(&t, &far(8).id));
        assert!(!contains(&t, &far(3).id));
        assert_eq!(t.len(), 8);
    }

    #[test]
    fn closest_sorted_by_xor_distance() {
        let t0 = Instant::now();
        let mut r = rng();
        let mut t = RoutingTable::new(NodeId::random(&mut r), Family::V4);
        for k in 0..50u32 {
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::from(0x0100_0000 + k)), 6881);
            t.insert(
                NodeInfo {
                    id: NodeId::random(&mut r),
                    addr,
                },
                t0,
            );
        }
        let target = NodeId::random(&mut r);
        let mut all: Vec<NodeInfo> = t.nodes().copied().collect();
        assert!(all.len() >= 8);
        all.sort_by_key(|n| n.id.distance(&target));
        all.truncate(8);
        assert_eq!(t.closest(&target, 8), all);
        assert_eq!(t.closest(&target, 1000).len(), t.len());
    }

    #[test]
    fn one_node_per_public_ip_but_local_exempt() {
        let t0 = Instant::now();
        let mut t = zero_table();
        let mut a = [0u8; 20];
        a[0] = 0x80;
        let mut b = [0u8; 20];
        b[0] = 0x40;
        assert_eq!(t.insert(node(a, "8.8.8.8:1000"), t0), Insert::Added);
        assert_eq!(t.insert(node(b, "8.8.8.8:2000"), t0), Insert::Rejected);
        assert_eq!(t.len(), 1);
        let mut c = [0u8; 20];
        c[0] = 0x20;
        let mut d = [0u8; 20];
        d[0] = 0x10;
        assert_eq!(t.insert(node(c, "127.0.0.1:1000"), t0), Insert::Added);
        assert_eq!(t.insert(node(d, "127.0.0.1:2000"), t0), Insert::Added);
        assert_eq!(t.len(), 3);
    }

    #[test]
    fn known_id_on_new_address_is_rejected() {
        let t0 = Instant::now();
        let mut t = zero_table();
        let n = far(1);
        let moved = NodeInfo {
            id: n.id,
            addr: "2.2.2.2:6881".parse().unwrap(),
        };
        assert_eq!(t.insert(n, t0), Insert::Added);
        assert_eq!(t.insert(n, t0), Insert::Updated);
        assert_eq!(t.insert(moved, t0), Insert::Rejected);
        assert_eq!(t.nodes().next().unwrap().addr, n.addr);
        // a reply from the hijacking address does not refresh the entry
        t.heard_from(&n.id, moved.addr, t0 + 10 * MIN);
        assert_eq!(t.stale_buckets(t0 + 16 * MIN), vec![0]);
        // once the old entry is bad, the id may move
        t.failed(&n.id);
        t.failed(&n.id);
        assert_eq!(t.insert(moved, t0), Insert::Updated);
        assert_eq!(t.nodes().next().unwrap().addr, moved.addr);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn rejects_own_id_wrong_family_and_undialable() {
        let t0 = Instant::now();
        let mut t = zero_table();
        let mut id = [0u8; 20];
        id[0] = 0x80;
        for bad in [
            node([0; 20], "1.1.1.1:1"),
            node(id, "[2001:db8::1]:6881"),
            node(id, "[::ffff:1.1.1.1]:6881"),
            node(id, "1.1.1.1:0"),
            node(id, "0.0.0.0:6881"),
            node(id, "224.0.0.1:6881"),
        ] {
            assert_eq!(t.insert(bad, t0), Insert::Rejected, "{bad:?}");
        }
        let mut t6 = RoutingTable::new(NodeId([0; 20]), Family::V6);
        assert_eq!(t6.insert(node(id, "1.1.1.1:1"), t0), Insert::Rejected);
        assert_eq!(t6.insert(node(id, "[::]:6881"), t0), Insert::Rejected);
        assert_eq!(t6.insert(node(id, "[ff02::1]:6881"), t0), Insert::Rejected);
        assert_eq!(t6.insert(node(id, "[2001:db8::1]:6881"), t0), Insert::Added);
        assert_eq!(t.len(), 0);
        assert_eq!(t.own_id(), NodeId([0; 20]));
    }

    #[test]
    fn stale_buckets_after_15_minutes() {
        let t0 = Instant::now();
        let mut t = zero_table();
        let mut near = [0u8; 20];
        near[2] = 0x01; // bucket 23
        t.insert(far(1), t0);
        t.insert(node(near, "3.3.3.3:6881"), t0);
        assert!(t.stale_buckets(t0 + 14 * MIN).is_empty());
        assert_eq!(t.stale_buckets(t0 + 16 * MIN), vec![0, 23]);
        t.heard_from(&far(1).id, far(1).addr, t0 + 10 * MIN);
        assert_eq!(t.stale_buckets(t0 + 16 * MIN), vec![23]);

        let mut r = rng();
        let own = NodeId::random(&mut r);
        let t = RoutingTable::new(own, Family::V4);
        for i in 0..160 {
            for _ in 0..4 {
                let id = t.random_id_in_bucket(i, &mut r);
                assert_eq!(own.bucket_index(&id), Some(i));
            }
        }
        assert_eq!(
            own.bucket_index(&t.random_id_in_bucket(500, &mut r)),
            Some(159)
        );
    }

    #[test]
    fn reset_own_rebuckets_every_node() {
        let t0 = Instant::now();
        let mut r = rng();
        let mut t = RoutingTable::new(NodeId::random(&mut r), Family::V4);
        for k in 0..8u32 {
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::from(0x0100_0000 + k)), 6881);
            let n = NodeInfo {
                id: NodeId::random(&mut r),
                addr,
            };
            assert_eq!(t.insert(n, t0), Insert::Added);
        }
        let before = t.len();
        let new_own = NodeId::random(&mut r);
        t.reset_own(new_own);
        assert_eq!(t.own_id(), new_own);
        assert_eq!(t.len(), before);
        for (i, b) in t.buckets.iter().enumerate() {
            for e in &b.nodes {
                assert_eq!(new_own.bucket_index(&e.info.id), Some(i));
            }
        }
        assert_eq!(t.ips.len(), before);
    }

    #[test]
    fn table_size_is_bounded() {
        let t0 = Instant::now();
        let mut r = rng();
        let mut t = RoutingTable::new(NodeId::random(&mut r), Family::V4);
        for i in 0..100_000u64 {
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::from(r.random::<u32>())), 6881);
            let n = NodeInfo {
                id: NodeId::random(&mut r),
                addr,
            };
            let now = t0 + Duration::from_millis(i * 50);
            if let Insert::Pending { ping } = t.insert(n, now)
                && r.random_bool(0.5)
            {
                t.failed(&ping.id);
                t.failed(&ping.id);
            }
        }
        assert!(t.len() <= 160 * K);
        assert!(t.buckets.iter().all(|b| b.nodes.len() <= K));
        assert!(t.buckets.iter().all(|b| b.cache.len() <= REPLACEMENTS));
        let public = t.nodes().filter(|n| !is_local(n.addr.ip())).count();
        assert_eq!(t.ips.len(), public);
    }
}
