//! Iterative lookups (BEP 5): a pure state machine ([`Lookup`]) and the
//! driver that runs it over [`crate::node`] queries ([`PeerLookup`]).

use std::collections::HashSet;
use std::net::SocketAddr;

use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};

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
/// Peer batches buffered for a slow [`PeerLookup`] reader.
const PEER_BATCHES: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
enum State {
    Fresh,
    InFlight,
    Answered(Option<Vec<u8>>),
    Failed,
}

#[derive(Debug)]
struct Candidate {
    info: NodeInfo,
    dist: [u8; 20],
    state: State,
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

    fn add(&mut self, nodes: &[NodeInfo]) {
        for n in nodes {
            if self
                .cands
                .iter()
                .any(|c| c.info.id == n.id || c.info.addr == n.addr)
            {
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

    /// The query to `from` failed.
    pub(crate) fn on_failure(&mut self, from: &NodeId) {
        self.settle(from, State::Failed);
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
    /// (`None`: `implied_port`).
    Announce(Option<u16>),
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
/// go to `peers` (de-duplicated across batches). Returns when done.
pub(crate) async fn run(
    link: Link,
    family: Family,
    own: NodeId,
    target: NodeId,
    seeds: Vec<NodeInfo>,
    goal: Goal,
    peers: Option<mpsc::Sender<Vec<SocketAddr>>>,
) {
    let mut lookup = Lookup::new(target, seeds);
    let mut seen: HashSet<SocketAddr> = HashSet::new();
    let query = match goal {
        Goal::FindNode => Query::FindNode { target },
        Goal::GetPeers | Goal::Announce(_) => Query::GetPeers {
            info_hash: target.0,
        },
    };
    let mut set: JoinSet<(NodeInfo, Result<Response, QueryError>)> = JoinSet::new();
    loop {
        for n in lookup.next_queries() {
            let link = link.clone();
            let q = query.clone();
            set.spawn(async move { (n, link.query(n.addr, q).await) });
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
                    return;
                }
                lookup.on_response(&n.id, &usable(n.addr, family, own, nodes), r.token);
            }
            Err(QueryError::Shutdown) => return,
            Err(_) => lookup.on_failure(&n.id),
        }
    }
    if let Goal::Announce(port) = goal {
        let mut set = JoinSet::new();
        for (n, token) in lookup.responders() {
            let link = link.clone();
            let q = Query::AnnouncePeer {
                info_hash: target.0,
                port: port.unwrap_or(0),
                implied_port: port.is_none(),
                token,
            };
            set.spawn(async move { link.query(n.addr, q).await });
        }
        while set.join_next().await.is_some() {}
    }
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
