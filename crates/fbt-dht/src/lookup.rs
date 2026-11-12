//! Iterative lookups (BEP 5): a pure state machine ([`Lookup`]) and the
//! driver that runs it over [`crate::node`] queries ([`PeerLookup`]).

use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};

use crate::bloom::BloomFilter;
use crate::id::{NodeId, is_local};
use crate::krpc::{NodeInfo, Query, Response};
use crate::node::{Link, QueryError};
use crate::table::{Family, K, dialable};

/// Queries in flight at once per lookup.
pub(crate) const ALPHA: usize = 3;
/// Candidates kept, closest first; farther ones are forgotten.
pub(crate) const MAX_CANDIDATES: usize = 64;
/// Queries one lookup sends in all.
pub(crate) const MAX_LOOKUP_QUERIES: usize = 200;
/// Peers one lookup yields in all (de-duplication memory).
const MAX_LOOKUP_PEERS: usize = 4096;
/// How long a query refused with `Busy` waits before its node is retried.
const BUSY_RETRY: Duration = Duration::from_millis(100);
/// Peer batches buffered for a slow [`PeerLookup`] reader.
const PEER_BATCHES: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
enum State {
    Fresh,
    InFlight,
    Answered(Option<Vec<u8>>),
    Failed,
}

/// One scrape answer's BEP 33 filters: seeds, then other peers.
type Filters = (BloomFilter, BloomFilter);

#[derive(Debug)]
struct Candidate {
    info: NodeInfo,
    dist: [u8; 20],
    state: State,
    /// The filters of its scrape answer (boxed: most candidates have none).
    filters: Option<Box<Filters>>,
}

/// One iterative lookup towards `target`. It is done when the [`K`] closest
/// candidates that have not failed have all answered and nothing is in
/// flight, or after [`MAX_LOOKUP_QUERIES`] queries.
#[derive(Debug)]
pub(crate) struct Lookup {
    target: NodeId,
    /// Sorted by distance to `target`, closest first.
    cands: Vec<Candidate>,
    in_flight: usize,
    sent: usize,
}

impl Lookup {
    pub(crate) fn new(target: NodeId, seeds: Vec<NodeInfo>) -> Self {
        let mut l = Lookup {
            target,
            cands: Vec::new(),
            in_flight: 0,
            sent: 0,
        };
        l.add(&seeds);
        l
    }

    /// Adds new candidates. One per id, address and (for non-local
    /// addresses) IP, so one host cannot fill the candidate list with many
    /// ids on many ports.
    fn add(&mut self, nodes: &[NodeInfo]) {
        for n in nodes {
            let ip = n.addr.ip();
            let shared_ip = !is_local(ip);
            if self.cands.iter().any(|c| {
                c.info.id == n.id || c.info.addr == n.addr || (shared_ip && c.info.addr.ip() == ip)
            }) {
                continue;
            }
            let dist = n.id.distance(&self.target);
            let pos = self.cands.partition_point(|c| c.dist < dist);
            if pos >= MAX_CANDIDATES {
                continue;
            }
            self.cands.insert(
                pos,
                Candidate {
                    info: *n,
                    dist,
                    state: State::Fresh,
                    filters: None,
                },
            );
            // forget the farthest candidate that is not awaiting an answer
            if self.cands.len() > MAX_CANDIDATES
                && let Some(i) = self.cands.iter().rposition(|c| c.state != State::InFlight)
            {
                self.cands.remove(i);
            }
        }
    }

    /// The `K` closest candidates that have not failed.
    fn closest_live(&self) -> impl Iterator<Item = &Candidate> {
        self.cands
            .iter()
            .filter(|c| c.state != State::Failed)
            .take(K)
    }

    /// The next nodes to query (at most [`ALPHA`] in flight), closest first.
    pub(crate) fn next_queries(&mut self) -> Vec<NodeInfo> {
        let mut out = Vec::new();
        while self.in_flight < ALPHA && self.sent < MAX_LOOKUP_QUERIES {
            let Some(id) = self
                .closest_live()
                .find(|c| c.state == State::Fresh)
                .map(|c| c.info.id)
            else {
                break;
            };
            if let Some(c) = self.cands.iter_mut().find(|c| c.info.id == id) {
                c.state = State::InFlight;
                out.push(c.info);
                self.in_flight += 1;
                self.sent += 1;
            }
        }
        out
    }

    fn settle(&mut self, from: &NodeId, state: State) -> bool {
        match self
            .cands
            .iter_mut()
            .find(|c| c.info.id == *from && c.state == State::InFlight)
        {
            Some(c) => {
                c.state = state;
                self.in_flight = self.in_flight.saturating_sub(1);
                true
            }
            None => false,
        }
    }

    /// `from` answered with `nodes` (and an announce `token`).
    pub(crate) fn on_response(
        &mut self,
        from: &NodeId,
        nodes: &[NodeInfo],
        token: Option<Vec<u8>>,
    ) {
        if self.settle(from, State::Answered(token)) {
            self.add(nodes);
        }
    }

    /// `from`, which has answered, sent BEP 33 filters (either may be
    /// missing: it counts as empty).
    pub(crate) fn on_filters(
        &mut self,
        from: &NodeId,
        seeds: Option<Box<[u8; 256]>>,
        peers: Option<Box<[u8; 256]>>,
    ) {
        if seeds.is_none() && peers.is_none() {
            return;
        }
        let filter =
            |b: Option<Box<[u8; 256]>>| b.map(|b| BloomFilter::from_bytes(*b)).unwrap_or_default();
        if let Some(c) = self
            .cands
            .iter_mut()
            .find(|c| c.info.id == *from && matches!(c.state, State::Answered(_)))
        {
            c.filters = Some(Box::new((filter(seeds), filter(peers))));
        }
    }

    /// The union of the filters of the `K` closest candidates that answered.
    pub(crate) fn filters(&self) -> Filters {
        let mut out = Filters::default();
        for c in self
            .cands
            .iter()
            .filter(|c| matches!(c.state, State::Answered(_)))
            .take(K)
        {
            if let Some(f) = &c.filters {
                out.0.union(&f.0);
                out.1.union(&f.1);
            }
        }
        out
    }

    /// The query to `from` failed.
    pub(crate) fn on_failure(&mut self, from: &NodeId) {
        self.settle(from, State::Failed);
    }

    /// The query to `from` was never sent (our node was busy): `from` is
    /// queried again later. The attempt still counts towards
    /// [`MAX_LOOKUP_QUERIES`], so a node that stays busy cannot loop forever.
    pub(crate) fn on_busy(&mut self, from: &NodeId) {
        self.settle(from, State::Fresh);
    }

    pub(crate) fn is_done(&self) -> bool {
        self.in_flight == 0
            && (self.sent >= MAX_LOOKUP_QUERIES
                || !self.closest_live().any(|c| c.state == State::Fresh))
    }

    /// The `K` closest responders that gave a token, closest first.
    pub(crate) fn responders(&self) -> Vec<(NodeInfo, Vec<u8>)> {
        self.cands
            .iter()
            .filter_map(|c| match &c.state {
                State::Answered(Some(t)) => Some((c.info, t.clone())),
                _ => None,
            })
            .take(K)
            .collect()
    }

    #[cfg(test)]
    fn candidates(&self) -> usize {
        self.cands.len()
    }
}

/// What a lookup is for.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Goal {
    /// `find_node`: fills the routing table (bootstrap, refresh).
    FindNode,
    /// `get_peers`, yielding peers.
    GetPeers,
    /// `get_peers`, then `announce_peer` to the closest responders
    /// (`None`: `implied_port`; `true`: BEP 33 `seed`).
    Announce(Option<u16>, bool),
    /// BEP 33 `get_peers` with `scrape`: the closest answers' filters.
    Scrape,
}

/// Keeps `nodes` of `family` that can be dialed; a non-local responder
/// cannot hand out local addresses.
pub(crate) fn usable(
    responder: SocketAddr,
    family: Family,
    own: NodeId,
    nodes: &[NodeInfo],
) -> Vec<NodeInfo> {
    let remote = !is_local(responder.ip());
    nodes
        .iter()
        .filter(|n| Family::of(&n.addr) == family && dialable(&n.addr) && n.id != own)
        .filter(|n| !(remote && is_local(n.addr.ip())))
        .copied()
        .collect()
}

fn peers_from(responder: SocketAddr, values: &[SocketAddr]) -> Vec<SocketAddr> {
    let remote = !is_local(responder.ip());
    values
        .iter()
        .filter(|a| dialable(a) && !(remote && is_local(a.ip())))
        .copied()
        .collect()
}

/// Runs one lookup on `family` towards `target` from `seeds`. Peers found
/// go to `peers` (de-duplicated across batches). Returns when done, with
/// the union of the `K` closest answers' BEP 33 filters (empty unless
/// `goal` is [`Goal::Scrape`]).
pub(crate) async fn run(
    link: Link,
    family: Family,
    own: NodeId,
    target: NodeId,
    seeds: Vec<NodeInfo>,
    goal: Goal,
    peers: Option<mpsc::Sender<Vec<SocketAddr>>>,
) -> Filters {
    let mut lookup = Lookup::new(target, seeds);
    let mut seen: HashSet<SocketAddr> = HashSet::new();
    let query = match goal {
        Goal::FindNode => Query::FindNode { target },
        Goal::GetPeers | Goal::Announce(..) => Query::GetPeers {
            info_hash: target.0,
            scrape: false,
        },
        Goal::Scrape => Query::GetPeers {
            info_hash: target.0,
            scrape: true,
        },
    };
    let mut set: JoinSet<(NodeInfo, Result<Response, QueryError>)> = JoinSet::new();
    loop {
        for n in lookup.next_queries() {
            let link = link.clone();
            let q = query.clone();
            set.spawn(async move {
                let res = link.query(n.addr, q).await;
                if res == Err(QueryError::Busy) {
                    // keep the slot while the node's queue drains
                    tokio::time::sleep(BUSY_RETRY).await;
                }
                (n, res)
            });
        }
        if lookup.is_done() {
            break;
        }
        let Some(joined) = set.join_next().await else {
            break;
        };
        let Ok((n, res)) = joined else { break };
        match res {
            Ok(r) => {
                let nodes = match family {
                    Family::V4 => &r.nodes,
                    Family::V6 => &r.nodes6,
                };
                let fresh: Vec<SocketAddr> = peers_from(n.addr, &r.values)
                    .into_iter()
                    .filter(|a| seen.len() < MAX_LOOKUP_PEERS && seen.insert(*a))
                    .collect();
                if !fresh.is_empty()
                    && let Some(tx) = &peers
                    && tx.send(fresh).await.is_err()
                {
                    return lookup.filters();
                }
                lookup.on_response(&n.id, &usable(n.addr, family, own, nodes), r.token);
                if let Goal::Scrape = goal {
                    lookup.on_filters(&n.id, r.bf_seeds, r.bf_peers);
                }
            }
            Err(QueryError::Shutdown) => return lookup.filters(),
            Err(QueryError::Busy) => lookup.on_busy(&n.id),
            Err(_) => lookup.on_failure(&n.id),
        }
    }
    if let Goal::Announce(port, seed) = goal {
        let mut set = JoinSet::new();
        for (n, token) in lookup.responders() {
            let link = link.clone();
            let q = Query::AnnouncePeer {
                info_hash: target.0,
                port: port.unwrap_or(0),
                implied_port: port.is_none(),
                token,
                seed,
            };
            set.spawn(async move { link.query(n.addr, q).await });
        }
        while set.join_next().await.is_some() {}
    }
    lookup.filters()
}

/// Peers found by [`crate::Dht::get_peers`] or [`crate::Dht::announce`],
/// in batches. Dropping it cancels the lookup.
#[derive(Debug)]
pub struct PeerLookup {
    rx: mpsc::Receiver<Vec<SocketAddr>>,
    task: JoinHandle<()>,
}

impl PeerLookup {
    pub(crate) fn start(link: Link, goal: Goal, info_hash: [u8; 20]) -> PeerLookup {
        let (tx, rx) = mpsc::channel(PEER_BATCHES);
        let task = tokio::spawn(async move {
            let target = NodeId(info_hash);
            let Ok(views) = link.closest(Some(target)).await else {
                return;
            };
            let mut set = JoinSet::new();
            for v in views {
                set.spawn(run(
                    link.clone(),
                    v.family,
                    v.own,
                    target,
                    v.closest,
                    goal,
                    Some(tx.clone()),
                ));
            }
            drop(tx);
            while set.join_next().await.is_some() {}
        });
        PeerLookup { rx, task }
    }

    /// The next batch of new, dialable peers; `None` once every family's
    /// lookup (and, for an announce, every `announce_peer`) has finished.
    pub async fn next(&mut self) -> Option<Vec<SocketAddr>> {
        self.rx.recv().await
    }
}

impl Drop for PeerLookup {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// A node whose id is `d` big-endian in the last 4 bytes: its distance to
    /// the all-zero target is `d`.
    fn at(d: u32) -> NodeInfo {
        let mut id = [0u8; 20];
        id[16..].copy_from_slice(&d.to_be_bytes());
        NodeInfo {
            id: NodeId(id),
            addr: SocketAddr::from(([10, 0, (d >> 8) as u8, d as u8], 6881)),
        }
    }

    const TARGET: NodeId = NodeId([0; 20]);

    #[test]
    fn queries_alpha_closest_first() {
        let seeds: Vec<NodeInfo> = [50, 10, 40, 20, 30, 60].map(at).to_vec();
        let mut l = Lookup::new(TARGET, seeds);
        let q = l.next_queries();
        assert_eq!(q, vec![at(10), at(20), at(30)]);
        assert!(l.next_queries().is_empty(), "ALPHA in flight");
        l.on_failure(&at(20).id);
        assert_eq!(l.next_queries(), vec![at(40)]);
        assert!(!l.is_done());
    }

    #[test]
    fn closer_nodes_from_responses_are_queried_next() {
        let mut l = Lookup::new(TARGET, [100, 200, 300, 400].map(at).to_vec());
        assert_eq!(l.next_queries().len(), 3);
        l.on_response(&at(100).id, &[at(5), at(150), at(100)], None);
        assert_eq!(l.next_queries(), vec![at(5)]);
        // duplicates and unknown responders are ignored
        l.on_response(&at(999).id, &[at(1)], None);
        assert!(l.next_queries().is_empty());
    }

    #[test]
    fn done_when_k_closest_answered() {
        let seeds: Vec<NodeInfo> = (1..=20).map(at).collect();
        let mut l = Lookup::new(TARGET, seeds);
        let mut queried = Vec::new();
        while !l.is_done() {
            let q = l.next_queries();
            assert!(!q.is_empty(), "not done but nothing to do");
            for n in q {
                queried.push(n);
                l.on_response(&n.id, &[], Some(vec![n.addr.port() as u8, 1]));
            }
        }
        assert_eq!(queried, (1..=K as u32).map(at).collect::<Vec<_>>());
        let r = l.responders();
        assert_eq!(r.len(), K);
        assert_eq!(r[0].0, at(1));
        assert!(l.next_queries().is_empty());
    }

    #[test]
    fn responders_need_tokens() {
        let mut l = Lookup::new(TARGET, [1, 2].map(at).to_vec());
        for n in l.next_queries() {
            let token = (n == at(2)).then(|| b"tok".to_vec());
            l.on_response(&n.id, &[], token);
        }
        assert!(l.is_done());
        assert_eq!(l.responders(), vec![(at(2), b"tok".to_vec())]);
    }

    #[test]
    fn busy_nodes_are_retried_not_failed() {
        let mut l = Lookup::new(TARGET, [1, 2].map(at).to_vec());
        assert_eq!(l.next_queries(), vec![at(1), at(2)]);
        l.on_busy(&at(1).id);
        assert!(!l.is_done());
        assert_eq!(l.next_queries(), vec![at(1)], "queried again");
        l.on_response(&at(1).id, &[], Some(b"t".to_vec()));
        l.on_response(&at(2).id, &[], None);
        assert!(l.is_done());
        assert_eq!(l.responders(), vec![(at(1), b"t".to_vec())]);
        // always busy: the query budget still ends the lookup
        let mut l = Lookup::new(TARGET, vec![at(1)]);
        let mut rounds = 0;
        while !l.is_done() {
            for n in l.next_queries() {
                l.on_busy(&n.id);
            }
            rounds += 1;
            assert!(rounds <= MAX_LOOKUP_QUERIES);
        }
    }

    #[test]
    fn filters_union_the_k_closest_answers() {
        let mut l = Lookup::new(TARGET, (1..=12).map(at).collect());
        let bloom = |d: u32| {
            let mut b = BloomFilter::default();
            b.insert(std::net::IpAddr::from([10, 9, 0, d as u8]));
            Some(Box::new(*b.bytes()))
        };
        while !l.is_done() {
            for n in l.next_queries() {
                let d = u32::from_be_bytes(n.id.0[16..].try_into().unwrap());
                // node 1 fails; node 3 sends only a seed filter
                if d == 1 {
                    l.on_failure(&n.id);
                    continue;
                }
                l.on_response(&n.id, &[], None);
                let peers = if d == 3 { None } else { bloom(d + 100) };
                l.on_filters(&n.id, bloom(d), peers);
            }
        }
        // a filter from a node that has not answered is ignored
        l.on_filters(&at(1).id, bloom(1), bloom(1));
        let (seeds, peers) = l.filters();
        // the K closest answers are 2..=9
        assert_eq!(seeds.estimate(), K as u32);
        assert_eq!(peers.estimate(), K as u32 - 1);
    }

    #[test]
    fn failures_do_not_stall() {
        let mut l = Lookup::new(TARGET, (1..=30).map(at).collect());
        let mut rounds = 0;
        while !l.is_done() {
            for n in l.next_queries() {
                l.on_failure(&n.id);
            }
            rounds += 1;
            assert!(rounds < 100);
        }
        assert!(l.responders().is_empty());
        // an empty lookup is done at once
        assert!(Lookup::new(TARGET, vec![]).is_done());
    }

    #[test]
    fn one_candidate_per_public_ip() {
        let node = |d: u32, addr: &str| NodeInfo {
            addr: addr.parse().unwrap(),
            ..at(d)
        };
        // one host, many ids and ports: only the closest-first one is kept
        let seeds = vec![
            node(1, "203.0.113.5:6881"),
            node(2, "203.0.113.5:6882"),
            node(3, "203.0.113.5:6883"),
            node(4, "198.51.100.1:6881"),
            node(5, "[2001:db8::1]:6881"),
            node(6, "[2001:db8::1]:6882"),
            // local addresses may share an IP (several nodes on one host)
            node(7, "10.0.0.1:6881"),
            node(8, "10.0.0.1:6882"),
        ];
        let l = Lookup::new(TARGET, seeds);
        let kept: Vec<u32> = l
            .cands
            .iter()
            .map(|c| u32::from_be_bytes(c.info.id.0[16..].try_into().unwrap()))
            .collect();
        assert_eq!(kept, [1, 4, 5, 7, 8]);
        // a later answer cannot add another id on a known public IP either
        let mut l = l;
        let q = l.next_queries();
        l.on_response(&q[0].id, &[node(0, "203.0.113.5:7000")], None);
        assert_eq!(l.candidates(), 5);
    }

    #[test]
    fn bounded_queries() {
        // every answer brings 8 fresh nodes closer than anything seen
        let mut l = Lookup::new(TARGET, vec![at(u32::MAX)]);
        let mut next = u32::MAX - 1;
        let mut sent = 0;
        let mut seen = HashSet::new();
        while !l.is_done() {
            let q = l.next_queries();
            assert!(!q.is_empty());
            for n in q {
                assert!(seen.insert(n.id), "queried twice");
                sent += 1;
                let fresh: Vec<NodeInfo> = (0..8)
                    .map(|_| {
                        next -= 1;
                        at(next)
                    })
                    .collect();
                l.on_response(&n.id, &fresh, None);
            }
            assert!(l.candidates() <= MAX_CANDIDATES);
        }
        assert_eq!(sent, MAX_LOOKUP_QUERIES);
    }
}
