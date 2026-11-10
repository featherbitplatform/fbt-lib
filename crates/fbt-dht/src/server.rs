//! Answers to incoming queries: `ping`, `find_node`, `get_peers`,
//! `announce_peer` (BEP 5), with `want` (BEP 32) and `ip` (BEP 42).
//!
//! Pure: the node runtime passes in its tables, tokens, peer store and the
//! time. Every answer it builds fits in [`MAX_DATAGRAM`].

use std::net::SocketAddr;
use std::time::Instant;

use rand::Rng;

use crate::krpc::{
    Body, ERR_METHOD_UNKNOWN, ERR_PROTOCOL, MAX_DATAGRAM, MAX_VALUES, Message, NodeInfo, Query,
    QueryMsg, Response,
};
use crate::store::PeerStore;
use crate::table::{Family, K, RoutingTable};
use crate::token::Tokens;

/// IPv4 peers returned by one `get_peers` answer.
pub(crate) const MAX_VALUES_V4: usize = 100;
/// IPv6 peers returned by one `get_peers` answer (18-byte entries, so the
/// datagram stays within [`MAX_DATAGRAM`] next to a full `nodes6`).
pub(crate) const MAX_VALUES_V6: usize = 50;
const _: () = assert!(MAX_VALUES_V4 <= MAX_VALUES && MAX_VALUES_V6 <= MAX_VALUES);

/// Our client version: `FB` plus the crate's major and minor version.
pub(crate) fn version() -> Vec<u8> {
    let major = env!("CARGO_PKG_VERSION_MAJOR").parse().unwrap_or(0);
    let minor = env!("CARGO_PKG_VERSION_MINOR").parse().unwrap_or(0);
    vec![b'F', b'B', major, minor]
}

/// What the server reads and writes.
pub(crate) struct Server<'a> {
    pub(crate) table_v4: Option<&'a RoutingTable>,
    pub(crate) table_v6: Option<&'a RoutingTable>,
    pub(crate) tokens: &'a Tokens,
    pub(crate) store: &'a mut PeerStore,
}

impl Server<'_> {
    fn table(&self, f: Family) -> Option<&RoutingTable> {
        match f {
            Family::V4 => self.table_v4,
            Family::V6 => self.table_v6,
        }
    }

    /// The `K` closest nodes for `target`: from the requester's family by
    /// default, or from the families named in `want`. Each list holds only
    /// its own family.
    fn nodes(&self, family: Family, q: &QueryMsg, target: &crate::NodeId) -> Response {
        let (v4, v6) = if q.want.v4 || q.want.v6 {
            (q.want.v4, q.want.v6)
        } else {
            (family == Family::V4, family == Family::V6)
        };
        let pick = |f: Family, on: bool| -> Vec<NodeInfo> {
            match self.table(f) {
                Some(t) if on => t
                    .closest(target, K)
                    .into_iter()
                    .filter(|n| Family::of(&n.addr) == f)
                    .collect(),
                _ => Vec::new(),
            }
        };
        Response {
            id: crate::NodeId([0; 20]),
            nodes: pick(Family::V4, v4),
            nodes6: pick(Family::V6, v6),
            values: Vec::new(),
            token: None,
        }
    }

    /// The answer to `q` from `from` (a canonical address: IPv4-mapped
    /// sources are already IPv4), or `None` when no table can answer.
    pub(crate) fn answer(
        &mut self,
        from: SocketAddr,
        tid: &[u8],
        q: &QueryMsg,
        now: Instant,
        rng: &mut impl Rng,
    ) -> Option<Vec<u8>> {
        let family = Family::of(&from);
        let own = self
            .table(family)
            .or(self.table_v4)
            .or(self.table_v6)?
            .own_id();
        let body = match &q.query {
            Query::Ping => Body::Response(empty(own)),
            Query::FindNode { target } => Body::Response(Response {
                id: own,
                ..self.nodes(family, q, target)
            }),
            Query::GetPeers { info_hash } => {
                let max = match family {
                    Family::V4 => MAX_VALUES_V4,
                    Family::V6 => MAX_VALUES_V6,
                };
                Body::Response(Response {
                    id: own,
                    values: self.store.peers(info_hash, family, max, rng),
                    token: Some(self.tokens.issue(from.ip())),
                    ..self.nodes(family, q, &crate::NodeId(*info_hash))
                })
            }
            Query::AnnouncePeer {
                info_hash,
                port,
                implied_port,
                token,
            } => {
                if self.tokens.verify(from.ip(), token) {
                    let port = if *implied_port { from.port() } else { *port };
                    if port != 0 {
                        self.store
                            .announce(*info_hash, SocketAddr::new(from.ip(), port), now);
                    }
                    Body::Response(empty(own))
                } else {
                    Body::Error {
                        code: ERR_PROTOCOL,
                        message: "bad token".into(),
                    }
                }
            }
            Query::Unknown(_) => Body::Error {
                code: ERR_METHOD_UNKNOWN,
                message: "Method Unknown".into(),
            },
        };
        Some(bounded(Message {
            tid: tid.to_vec(),
            body,
            ip: Some(from),
            version: Some(version()),
        }))
    }
}

fn empty(id: crate::NodeId) -> Response {
    Response {
        id,
        nodes: Vec::new(),
        nodes6: Vec::new(),
        values: Vec::new(),
        token: None,
    }
}

/// Encodes `m`, dropping values, then `nodes6`, then `nodes` entries until
/// the datagram fits in [`MAX_DATAGRAM`].
pub(crate) fn bounded(mut m: Message) -> Vec<u8> {
    loop {
        let out = m.encode();
        if out.len() <= MAX_DATAGRAM {
            return out;
        }
        let Body::Response(r) = &mut m.body else {
            return out;
        };
        if r.values.pop().is_none() && r.nodes6.pop().is_none() && r.nodes.pop().is_none() {
            return out;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeId;
    use crate::krpc::Want;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    fn table(f: Family, n: u32, rng: &mut StdRng) -> RoutingTable {
        let mut t = RoutingTable::new(NodeId::random(rng), f);
        let now = Instant::now();
        for k in 0..n {
            let ip = match f {
                Family::V4 => IpAddr::V4(Ipv4Addr::from(0x0100_0000 + k)),
                Family::V6 => IpAddr::V6(Ipv6Addr::from(0x2001_0db8_u128 << 96 | u128::from(k))),
            };
            t.insert(
                NodeInfo {
                    id: NodeId::random(rng),
                    addr: SocketAddr::new(ip, 6881),
                },
                now,
            );
        }
        t
    }

    fn q(query: Query, want: Want) -> QueryMsg {
        QueryMsg {
            id: NodeId([1; 20]),
            query,
            want,
            read_only: false,
        }
    }

    #[test]
    fn nodes_follow_family_and_want_and_datagrams_fit() {
        let mut rng = StdRng::seed_from_u64(3);
        let t4 = table(Family::V4, 200, &mut rng);
        let t6 = table(Family::V6, 200, &mut rng);
        let tokens = Tokens::new(&mut rng);
        let mut store = PeerStore::new(10, 500);
        let now = Instant::now();
        let ih = [5; 20];
        for k in 0..200u32 {
            store.announce(
                ih,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::from(0x0300_0000 + k)), 1),
                now,
            );
            let v6 = IpAddr::V6(Ipv6Addr::from(0x2a00_u128 << 112 | u128::from(k)));
            store.announce(ih, SocketAddr::new(v6, 1), now);
        }
        let mut s = Server {
            table_v4: Some(&t4),
            table_v6: Some(&t6),
            tokens: &tokens,
            store: &mut store,
        };
        let from4: SocketAddr = "9.9.9.9:1".parse().unwrap();
        let from6: SocketAddr = "[2a01::1]:1".parse().unwrap();
        let tid = [0xaa; 16];
        let both = Want { v4: true, v6: true };
        for (from, want) in [
            (from4, Want::default()),
            (from6, Want::default()),
            (from4, both),
            (from6, both),
        ] {
            for query in [
                Query::FindNode {
                    target: NodeId([7; 20]),
                },
                Query::GetPeers { info_hash: ih },
            ] {
                let out = s
                    .answer(from, &tid, &q(query, want), now, &mut rng)
                    .unwrap();
                assert!(out.len() <= MAX_DATAGRAM, "{} bytes", out.len());
                let m = Message::decode(&out).unwrap();
                assert_eq!(m.ip, Some(from));
                let Body::Response(r) = m.body else { panic!() };
                assert!(r.nodes.iter().all(|n| n.addr.is_ipv4()));
                assert!(r.nodes6.iter().all(|n| n.addr.is_ipv6()));
                let (v4, v6) = if want.v4 {
                    (true, true)
                } else {
                    (from.is_ipv4(), from.is_ipv6())
                };
                assert_eq!(!r.nodes.is_empty(), v4);
                assert_eq!(!r.nodes6.is_empty(), v6);
                assert!(r.values.iter().all(|v| v.is_ipv4() == from.is_ipv4()));
                if want == Want::default() && r.token.is_some() {
                    let max = if from.is_ipv4() {
                        MAX_VALUES_V4
                    } else {
                        MAX_VALUES_V6
                    };
                    assert_eq!(r.values.len(), max);
                }
            }
        }
    }

    #[test]
    fn announce_with_port_zero_is_not_stored() {
        let mut rng = StdRng::seed_from_u64(4);
        let t4 = table(Family::V4, 0, &mut rng);
        let tokens = Tokens::new(&mut rng);
        let mut store = PeerStore::new(10, 10);
        let from: SocketAddr = "9.9.9.9:0".parse().unwrap();
        let mut s = Server {
            table_v4: Some(&t4),
            table_v6: None,
            tokens: &tokens,
            store: &mut store,
        };
        let token = tokens.issue(from.ip());
        let a = Query::AnnouncePeer {
            info_hash: [1; 20],
            port: 0,
            implied_port: true,
            token,
        };
        let out = s
            .answer(from, b"t", &q(a, Want::default()), Instant::now(), &mut rng)
            .unwrap();
        assert!(matches!(
            Message::decode(&out).unwrap().body,
            Body::Response(_)
        ));
        assert_eq!(store.torrents(), 0);
    }
}
