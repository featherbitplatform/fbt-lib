//! Answers to incoming queries: `ping`, `find_node`, `get_peers`,
//! `announce_peer` (BEP 5), with `want` (BEP 32), `ip` (BEP 42), the
//! scrape and seed flags (BEP 33), `sample_infohashes` (BEP 51) and the
//! item queries `get` and `put` (BEP 44).
//!
//! Pure: the node runtime passes in its tables, tokens, peer and item
//! stores and the time. Every answer it builds fits in [`MAX_DATAGRAM`].

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use rand::Rng;

use crate::items::{ItemStore, Put};
use crate::krpc::{
    Body, ERR_METHOD_UNKNOWN, ERR_PROTOCOL, MAX_DATAGRAM, MAX_SAMPLES, MAX_VALUES, Message,
    NodeInfo, Query, QueryMsg, Response,
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
/// BEP 51 `interval` we announce: our samples are fresh for 6 hours at
/// most (BEP 51's upper bound).
pub(crate) const SAMPLE_INTERVAL_SECS: u32 = 6 * 60 * 60;

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
    pub(crate) items: &'a mut ItemStore,
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
            nodes: pick(Family::V4, v4),
            nodes6: pick(Family::V6, v6),
            ..Response::default()
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
            Query::GetPeers { info_hash, scrape } => {
                let max = match family {
                    Family::V4 => MAX_VALUES_V4,
                    Family::V6 => MAX_VALUES_V6,
                };
                let mut r = Response {
                    id: own,
                    token: Some(self.tokens.issue(from.ip())),
                    ..self.nodes(family, q, &crate::NodeId(*info_hash))
                };
                // BEP 33: a scrape gets the two filters instead of values
                // (none for an unknown info hash), as libtorrent answers
                match (scrape, self.store.scrape(info_hash)) {
                    (true, Some((seeds, peers))) => {
                        r.bf_seeds = Some(Box::new(*seeds.bytes()));
                        r.bf_peers = Some(Box::new(*peers.bytes()));
                    }
                    (true, None) => {}
                    (false, _) => r.values = self.store.peers(info_hash, family, max, rng),
                }
                Body::Response(r)
            }
            Query::SampleInfohashes { target } => Body::Response(Response {
                id: own,
                // one sample per interval, not one per query
                samples: self.store.cached_sample(
                    rng,
                    MAX_SAMPLES,
                    now,
                    Duration::from_secs(u64::from(SAMPLE_INTERVAL_SECS)),
                ),
                num: Some(u32::try_from(self.store.len()).unwrap_or(u32::MAX)),
                interval: Some(SAMPLE_INTERVAL_SECS),
                ..self.nodes(family, q, target)
            }),
            Query::Get { target, seq } => {
                let mut r = Response {
                    id: own,
                    token: Some(self.tokens.issue(from.ip())),
                    ..self.nodes(family, q, target)
                };
                if let Some(item) = self.items.get(&target.0, now) {
                    match &item.signed {
                        None => r.v = Some(item.v.clone()),
                        // the requester has this one or a newer one: seq only
                        Some(m) if seq.is_some_and(|s| s >= m.seq) => r.seq = Some(m.seq),
                        Some(m) => {
                            r.v = Some(item.v.clone());
                            r.k = Some(m.k);
                            r.sig = Some(m.sig);
                            r.seq = Some(m.seq);
                        }
                    }
                }
                Body::Response(r)
            }
            Query::Put {
                token,
                v,
                k,
                sig,
                seq,
                salt,
                cas,
                reject,
            } => {
                let put = Put {
                    v,
                    k: k.as_ref(),
                    sig: sig.as_ref(),
                    seq: *seq,
                    salt: salt.as_deref().unwrap_or_default(),
                    cas: *cas,
                    reject: *reject,
                };
                let checked = if self.tokens.verify(from.ip(), token) {
                    self.items.put(&put, now)
                } else {
                    Err((ERR_PROTOCOL, "bad token"))
                };
                match checked {
                    Ok(()) => Body::Response(empty(own)),
                    Err((code, message)) => Body::Error {
                        code,
                        message: message.into(),
                    },
                }
            }
            Query::AnnouncePeer {
                info_hash,
                port,
                implied_port,
                token,
                seed,
            } => {
                if self.tokens.verify(from.ip(), token) {
                    let port = if *implied_port { from.port() } else { *port };
                    if port != 0 {
                        self.store.announce(
                            *info_hash,
                            SocketAddr::new(from.ip(), port),
                            *seed,
                            now,
                        );
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
        ..Response::default()
    }
}

/// Encodes `m`, dropping values, then `nodes6`, then `nodes` entries until
/// the datagram fits in [`MAX_DATAGRAM`]. Filters, samples and items are
/// never dropped, and no answer carries two of them: a scrape answer (two
/// filters) or a sample answer (20 samples) with full node lists of both
/// families comes to at most about 1,200 bytes, and a `get` answer with a
/// 1000-byte item, its key and signature to about 1,200 bytes with no
/// nodes.
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
        let mut items = ItemStore::new(10);
        let now = Instant::now();
        let ih = [5; 20];
        for k in 0..200u32 {
            store.announce(
                ih,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::from(0x0300_0000 + k)), 1),
                k % 2 == 0,
                now,
            );
            let v6 = IpAddr::V6(Ipv6Addr::from(0x2a00_u128 << 112 | u128::from(k)));
            store.announce(ih, SocketAddr::new(v6, 1), false, now);
        }
        let mut s = Server {
            table_v4: Some(&t4),
            table_v6: Some(&t6),
            tokens: &tokens,
            store: &mut store,
            items: &mut items,
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
                Query::GetPeers {
                    info_hash: ih,
                    scrape: false,
                },
                Query::GetPeers {
                    info_hash: ih,
                    scrape: true,
                },
                Query::SampleInfohashes {
                    target: NodeId([7; 20]),
                },
            ] {
                let scrape = matches!(query, Query::GetPeers { scrape: true, .. });
                let sample = matches!(query, Query::SampleInfohashes { .. });
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
                assert_eq!(r.bf_seeds.is_some(), scrape);
                assert_eq!(!r.samples.is_empty(), sample);
                if let Some(seeds) = &r.bf_seeds {
                    // scrape: filters, no values; 100 IPv4 seeds, the rest peers
                    assert!(r.values.is_empty());
                    let seeds = crate::bloom::BloomFilter::from_bytes(**seeds);
                    assert!(seeds.estimate().abs_diff(100) <= 10);
                    assert!(r.bf_peers.is_some());
                }
                if !r.samples.is_empty() {
                    assert_eq!(r.samples, vec![ih]);
                    assert_eq!(r.num, Some(1));
                    assert_eq!(r.interval, Some(SAMPLE_INTERVAL_SECS));
                }
                if want == Want::default() && r.token.is_some() && r.bf_seeds.is_none() {
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
    fn sample_infohashes_answer_is_cached_for_the_interval() {
        let mut rng = StdRng::seed_from_u64(11);
        let t4 = table(Family::V4, 20, &mut rng);
        let tokens = Tokens::new(&mut rng);
        let mut store = PeerStore::new(2000, 10);
        let mut items = ItemStore::new(10);
        let t0 = Instant::now();
        for k in 0..500u32 {
            let mut ih = [0u8; 20];
            ih[..4].copy_from_slice(&k.to_be_bytes());
            store.announce(ih, "1.0.0.1:1".parse().unwrap(), false, t0);
        }
        let mut s = Server {
            table_v4: Some(&t4),
            table_v6: None,
            tokens: &tokens,
            store: &mut store,
            items: &mut items,
        };
        let from: SocketAddr = "9.9.9.9:1".parse().unwrap();
        let mut ask = |s: &mut Server<'_>, at: Instant| {
            let query = Query::SampleInfohashes {
                target: NodeId([7; 20]),
            };
            let out = s
                .answer(from, b"t", &q(query, Want::default()), at, &mut rng)
                .unwrap();
            let Body::Response(r) = Message::decode(&out).unwrap().body else {
                panic!()
            };
            assert_eq!(r.interval, Some(SAMPLE_INTERVAL_SECS));
            assert_eq!(r.num, Some(500));
            r.samples
        };
        let interval = Duration::from_secs(u64::from(SAMPLE_INTERVAL_SECS));
        let first = ask(&mut s, t0);
        assert_eq!(first.len(), MAX_SAMPLES);
        for at in [t0, t0 + Duration::from_secs(1), t0 + interval / 2] {
            assert_eq!(ask(&mut s, at), first, "same sample within the interval");
        }
        let next = ask(&mut s, t0 + interval);
        assert_eq!(next.len(), MAX_SAMPLES);
        assert_ne!(next, first, "redrawn after the interval");
    }

    #[test]
    fn get_answers_fit_and_put_checks_the_token() {
        let mut rng = StdRng::seed_from_u64(5);
        let t4 = table(Family::V4, 200, &mut rng);
        let t6 = table(Family::V6, 200, &mut rng);
        let tokens = Tokens::new(&mut rng);
        let mut store = PeerStore::new(10, 10);
        let mut items = ItemStore::new(10);
        let now = Instant::now();
        let from: SocketAddr = "9.9.9.9:1".parse().unwrap();
        // the largest mutable item: a 1000-byte value under a 64-byte salt
        let mut v = b"996:".to_vec();
        v.extend(std::iter::repeat_n(b'z', 996));
        let salt = [0xa5u8; crate::krpc::MAX_SALT];
        let (k, sig) = crate::items::sign(&[3; 32], &salt, 7, &v);
        let target = NodeId(crate::items::mutable_target(&k, &salt));
        let mut s = Server {
            table_v4: Some(&t4),
            table_v6: Some(&t6),
            tokens: &tokens,
            store: &mut store,
            items: &mut items,
        };
        let put = |token: Vec<u8>| Query::Put {
            token,
            v: v.clone(),
            k: Some(k),
            sig: Some(sig),
            seq: Some(7),
            salt: Some(salt.to_vec()),
            cas: None,
            reject: None,
        };
        let want = Want { v4: true, v6: true };
        let mut ask = |s: &mut Server<'_>, query: Query| {
            let out = s
                .answer(from, b"tt", &q(query, want), now, &mut rng)
                .unwrap();
            assert!(out.len() <= MAX_DATAGRAM, "{} bytes", out.len());
            Message::decode(&out).unwrap().body
        };
        // a put without a valid token: 203, nothing stored
        assert!(matches!(
            ask(&mut s, put(b"12345678".to_vec())),
            Body::Error {
                code: ERR_PROTOCOL,
                ..
            }
        ));
        let get = |seq| Query::Get { target, seq };
        let Body::Response(r) = ask(&mut s, get(None)) else {
            panic!()
        };
        assert!(r.v.is_none() && !r.nodes.is_empty() && !r.nodes6.is_empty());
        let token = r.token.unwrap();
        assert!(matches!(ask(&mut s, put(token)), Body::Response(_)));
        // the full item comes back whole; nodes make room for it
        let Body::Response(r) = ask(&mut s, get(None)) else {
            panic!()
        };
        assert_eq!(r.v.as_deref(), Some(v.as_slice()));
        assert_eq!((r.k, r.sig, r.seq), (Some(k), Some(sig), Some(7)));
        assert!(r.token.is_some());
        // a requester that has seq 7 gets the seq only
        let Body::Response(r) = ask(&mut s, get(Some(7))) else {
            panic!()
        };
        assert_eq!((r.v, r.k, r.sig, r.seq), (None, None, None, Some(7)));
    }

    #[test]
    fn announce_with_port_zero_is_not_stored() {
        let mut rng = StdRng::seed_from_u64(4);
        let t4 = table(Family::V4, 0, &mut rng);
        let tokens = Tokens::new(&mut rng);
        let mut store = PeerStore::new(10, 10);
        let mut items = ItemStore::new(10);
        let from: SocketAddr = "9.9.9.9:0".parse().unwrap();
        let mut s = Server {
            table_v4: Some(&t4),
            table_v6: None,
            tokens: &tokens,
            store: &mut store,
            items: &mut items,
        };
        let token = tokens.issue(from.ip());
        let a = Query::AnnouncePeer {
            info_hash: [1; 20],
            port: 0,
            implied_port: true,
            token,
            seed: false,
        };
        let out = s
            .answer(from, b"t", &q(a, Want::default()), Instant::now(), &mut rng)
            .unwrap();
        assert!(matches!(
            Message::decode(&out).unwrap().body,
            Body::Response(_)
        ));
        assert_eq!(store.len(), 0);
    }
}
