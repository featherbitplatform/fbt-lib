//! KRPC messages (BEP 5) with the BEP 32 (`nodes6`, `want`), BEP 42 (`ip`)
//! and BEP 43 (`ro`) fields.
//!
//! [`Message::decode`] parses hostile UDP datagrams: it never panics, decodes
//! with [`KRPC_LIMITS`], and cuts every list to a named cap before allocating.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use fbt_bencode::{DecodeError, Dict, Limits, OwnedValue, Value, decode_with, encode};
use fbt_core::compact::{self, COMPACT_V4_LEN, COMPACT_V6_LEN, decode_v4, decode_v6};

use crate::id::{NODE_ID_LEN, NodeId};

/// Bencode limits for one datagram: a message nests at most
/// `dict > dict > list` deep; 600 items covers a full 1500-byte datagram of
/// realistic entries.
pub const KRPC_LIMITS: Limits = Limits::new(6, 600);
/// Longest accepted transaction id.
pub const MAX_TID_LEN: usize = 16;
/// Longest accepted announce token.
pub const MAX_TOKEN_LEN: usize = 64;
/// Peer addresses kept from a `values` list; extra entries are cut.
pub const MAX_VALUES: usize = 100;
/// Nodes kept from each of `nodes` and `nodes6`; extra entries are cut.
pub const MAX_NODES: usize = 32;
/// Bytes of `v` (client version) kept.
pub const MAX_VERSION_LEN: usize = 8;
/// Bytes of an unknown method name kept.
pub const MAX_METHOD_LEN: usize = 32;
/// Bytes of an error message kept.
pub const MAX_ERROR_MESSAGE: usize = 256;
/// Largest datagram accepted.
pub const MAX_DATAGRAM: usize = 1500;

/// KRPC error code: generic error.
pub const ERR_GENERIC: i64 = 201;
/// KRPC error code: server error.
pub const ERR_SERVER: i64 = 202;
/// KRPC error code: protocol error (malformed packet, invalid arguments, bad token).
pub const ERR_PROTOCOL: i64 = 203;
/// KRPC error code: method unknown.
pub const ERR_METHOD_UNKNOWN: i64 = 204;

/// A compact node entry: id + IPv4 address.
const NODE_V4_LEN: usize = NODE_ID_LEN + COMPACT_V4_LEN;
/// A compact node entry: id + IPv6 address.
const NODE_V6_LEN: usize = NODE_ID_LEN + COMPACT_V6_LEN;

/// A node: its id and UDP address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeInfo {
    /// The node id.
    pub id: NodeId,
    /// The node's UDP address.
    pub addr: SocketAddr,
}

/// BEP 32 `want`: which address families the requester wants nodes for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Want {
    /// `n4`: IPv4 nodes.
    pub v4: bool,
    /// `n6`: IPv6 nodes.
    pub v6: bool,
}

/// A query method and its arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    /// `ping`.
    Ping,
    /// `find_node`.
    FindNode {
        /// The id being looked for.
        target: NodeId,
    },
    /// `get_peers`.
    GetPeers {
        /// The torrent's info hash.
        info_hash: [u8; 20],
    },
    /// `announce_peer`.
    AnnouncePeer {
        /// The torrent's info hash.
        info_hash: [u8; 20],
        /// The announced port (0 when `implied_port` is set and no valid port was sent).
        port: u16,
        /// Use the datagram's source port instead of `port`.
        implied_port: bool,
        /// The token from an earlier `get_peers` response.
        token: Vec<u8>,
    },
    /// Any other method, by name (cut to [`MAX_METHOD_LEN`] bytes), so the
    /// server can answer [`ERR_METHOD_UNKNOWN`].
    Unknown(String),
}

/// A query (`y = q`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryMsg {
    /// The querying node's id.
    pub id: NodeId,
    /// The method and its arguments.
    pub query: Query,
    /// BEP 32 `want`.
    pub want: Want,
    /// BEP 43 `ro`: the sender does not answer queries; keep it out of tables.
    pub read_only: bool,
}

/// A response (`y = r`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    /// The responding node's id.
    pub id: NodeId,
    /// IPv4 nodes (`nodes`); IPv6 entries are skipped when encoding.
    pub nodes: Vec<NodeInfo>,
    /// IPv6 nodes (`nodes6`, BEP 32); IPv4 entries are skipped when encoding.
    pub nodes6: Vec<NodeInfo>,
    /// Peers (`values`).
    pub values: Vec<SocketAddr>,
    /// Announce token.
    pub token: Option<Vec<u8>>,
}

/// The message kind and payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    /// A query.
    Query(QueryMsg),
    /// A response.
    Response(Response),
    /// An error (`y = e`).
    Error {
        /// The error code (see `ERR_*`).
        code: i64,
        /// The message, cut to [`MAX_ERROR_MESSAGE`] bytes.
        message: String,
    },
}

/// One KRPC datagram.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// Transaction id (`t`).
    pub tid: Vec<u8>,
    /// The payload.
    pub body: Body,
    /// BEP 42 `ip`: the requester's address as the sender sees it.
    pub ip: Option<SocketAddr>,
    /// Client version (`v`), cut to [`MAX_VERSION_LEN`] bytes.
    pub version: Option<Vec<u8>>,
}

/// KRPC decoding failures.
#[derive(Debug, thiserror::Error)]
pub enum KrpcError {
    /// The datagram is not valid bencode within [`KRPC_LIMITS`].
    #[error("bencode: {0}")]
    Bencode(#[from] DecodeError),
    /// A required key is missing or has the wrong shape; names the key.
    #[error("malformed KRPC message: {0}")]
    Malformed(&'static str),
    /// The datagram is larger than [`MAX_DATAGRAM`].
    #[error("datagram larger than {MAX_DATAGRAM} bytes")]
    TooLarge,
}

fn bytes<'a>(d: &Dict<'a>, key: &'static str) -> Result<&'a [u8], KrpcError> {
    d.get(key.as_bytes())
        .and_then(Value::as_bytes)
        .ok_or(KrpcError::Malformed(key))
}

fn hash20(d: &Dict<'_>, key: &'static str) -> Result<[u8; 20], KrpcError> {
    bytes(d, key)?
        .try_into()
        .map_err(|_| KrpcError::Malformed(key))
}

fn int(d: &Dict<'_>, key: &str) -> Option<i64> {
    d.get(key.as_bytes()).and_then(Value::as_int)
}

/// Decodes one compact address of either size; other sizes give `None`.
fn compact_addr(b: &[u8]) -> Option<SocketAddr> {
    if let Ok(a) = <&[u8; COMPACT_V4_LEN]>::try_from(b) {
        Some(decode_v4(a))
    } else if let Ok(a) = <&[u8; COMPACT_V6_LEN]>::try_from(b) {
        Some(decode_v6(a))
    } else {
        None
    }
}

/// Decodes up to [`MAX_NODES`] whole `N`-byte node entries; a trailing
/// partial entry is ignored.
fn compact_nodes<const N: usize>(buf: &[u8]) -> Vec<NodeInfo> {
    buf.as_chunks::<N>()
        .0
        .iter()
        .take(MAX_NODES)
        .filter_map(|entry| {
            let (id, addr) = entry.split_first_chunk::<NODE_ID_LEN>()?;
            Some(NodeInfo {
                id: NodeId(*id),
                addr: compact_addr(addr)?,
            })
        })
        .collect()
}

/// UTF-8 (lossy) text cut to at most `max` bytes on a char boundary.
fn text(b: &[u8], max: usize) -> String {
    let head = b.get(..max).unwrap_or(b);
    let mut s = String::from_utf8_lossy(head).into_owned();
    let mut cut = s.len().min(max);
    while !s.is_char_boundary(cut) {
        cut -= 1;
    }
    s.truncate(cut);
    s
}

fn decode_query(top: &Dict<'_>) -> Result<QueryMsg, KrpcError> {
    let method = bytes(top, "q")?;
    let a = top
        .get(b"a")
        .and_then(Value::as_dict)
        .ok_or(KrpcError::Malformed("a"))?;
    let id = NodeId(hash20(a, "id")?);
    let query = match method {
        b"ping" => Query::Ping,
        b"find_node" => Query::FindNode {
            target: NodeId(hash20(a, "target")?),
        },
        b"get_peers" => Query::GetPeers {
            info_hash: hash20(a, "info_hash")?,
        },
        b"announce_peer" => {
            let info_hash = hash20(a, "info_hash")?;
            let implied_port = int(a, "implied_port").is_some_and(|v| v != 0);
            let port = int(a, "port")
                .and_then(|p| u16::try_from(p).ok())
                .filter(|&p| p != 0);
            let port = match (port, implied_port) {
                (Some(p), _) => p,
                (None, true) => 0,
                (None, false) => return Err(KrpcError::Malformed("port")),
            };
            let token = bytes(a, "token")?;
            if token.len() > MAX_TOKEN_LEN {
                return Err(KrpcError::Malformed("token"));
            }
            Query::AnnouncePeer {
                info_hash,
                port,
                implied_port,
                token: token.to_vec(),
            }
        }
        other => Query::Unknown(text(other, MAX_METHOD_LEN)),
    };
    let mut want = Want::default();
    if let Some(list) = a.get(b"want").and_then(Value::as_list) {
        for w in list.iter().filter_map(Value::as_bytes) {
            match w {
                b"n4" => want.v4 = true,
                b"n6" => want.v6 = true,
                _ => {}
            }
        }
    }
    let read_only = int(top, "ro").is_some_and(|v| v == 1);
    Ok(QueryMsg {
        id,
        query,
        want,
        read_only,
    })
}

fn decode_response(top: &Dict<'_>) -> Result<Response, KrpcError> {
    let r = top
        .get(b"r")
        .and_then(Value::as_dict)
        .ok_or(KrpcError::Malformed("r"))?;
    let id = NodeId(hash20(r, "id")?);
    let nodes = r
        .get(b"nodes")
        .and_then(Value::as_bytes)
        .map(compact_nodes::<NODE_V4_LEN>)
        .unwrap_or_default();
    let nodes6 = r
        .get(b"nodes6")
        .and_then(Value::as_bytes)
        .map(compact_nodes::<NODE_V6_LEN>)
        .unwrap_or_default();
    let values = r
        .get(b"values")
        .and_then(Value::as_list)
        .map(|l| {
            l.iter()
                .filter_map(Value::as_bytes)
                .filter_map(compact_addr)
                .take(MAX_VALUES)
                .collect()
        })
        .unwrap_or_default();
    let token = match r.get(b"token") {
        None => None,
        Some(t) => {
            let t = t.as_bytes().ok_or(KrpcError::Malformed("token"))?;
            if t.len() > MAX_TOKEN_LEN {
                return Err(KrpcError::Malformed("token"));
            }
            Some(t.to_vec())
        }
    };
    Ok(Response {
        id,
        nodes,
        nodes6,
        values,
        token,
    })
}

fn decode_error(top: &Dict<'_>) -> Result<Body, KrpcError> {
    let e = top
        .get(b"e")
        .and_then(Value::as_list)
        .ok_or(KrpcError::Malformed("e"))?;
    let code = e
        .first()
        .and_then(Value::as_int)
        .ok_or(KrpcError::Malformed("e"))?;
    let message = e
        .get(1)
        .and_then(Value::as_bytes)
        .map(|m| text(m, MAX_ERROR_MESSAGE))
        .unwrap_or_default();
    Ok(Body::Error { code, message })
}

fn nodes_bytes(nodes: &[NodeInfo], v6: bool) -> Option<OwnedValue> {
    let mut out = Vec::new();
    for n in nodes.iter().filter(|n| n.addr.is_ipv6() == v6) {
        out.extend_from_slice(&n.id.0);
        compact::encode(n.addr, &mut out);
    }
    (!out.is_empty()).then_some(OwnedValue::Bytes(out))
}

fn compact_bytes(addr: SocketAddr) -> OwnedValue {
    let mut out = Vec::with_capacity(COMPACT_V6_LEN);
    compact::encode(addr, &mut out);
    OwnedValue::Bytes(out)
}

impl Message {
    /// Decodes one datagram.
    pub fn decode(buf: &[u8]) -> Result<Message, KrpcError> {
        if buf.len() > MAX_DATAGRAM {
            return Err(KrpcError::TooLarge);
        }
        let value = decode_with(buf, KRPC_LIMITS)?;
        let top = value.as_dict().ok_or(KrpcError::Malformed("message"))?;
        let tid = bytes(top, "t")?;
        if tid.len() > MAX_TID_LEN {
            return Err(KrpcError::Malformed("t"));
        }
        let body = match bytes(top, "y")? {
            b"q" => Body::Query(decode_query(top)?),
            b"r" => Body::Response(decode_response(top)?),
            b"e" => decode_error(top)?,
            _ => return Err(KrpcError::Malformed("y")),
        };
        let ip = top
            .get(b"ip")
            .and_then(Value::as_bytes)
            .and_then(compact_addr);
        let version = top
            .get(b"v")
            .and_then(Value::as_bytes)
            .map(|v| v.get(..MAX_VERSION_LEN).unwrap_or(v).to_vec());
        Ok(Message {
            tid: tid.to_vec(),
            body,
            ip,
            version,
        })
    }

    /// Encodes canonically (sorted keys).
    pub fn encode(&self) -> Vec<u8> {
        let mut top = BTreeMap::new();
        top.insert(b"t".to_vec(), OwnedValue::Bytes(self.tid.clone()));
        if let Some(ip) = self.ip {
            top.insert(b"ip".to_vec(), compact_bytes(ip));
        }
        if let Some(v) = &self.version {
            top.insert(b"v".to_vec(), OwnedValue::Bytes(v.clone()));
        }
        let y: &[u8] = match &self.body {
            Body::Query(q) => {
                let mut a = BTreeMap::new();
                a.insert(b"id".to_vec(), OwnedValue::Bytes(q.id.0.to_vec()));
                let method: &[u8] = match &q.query {
                    Query::Ping => b"ping",
                    Query::FindNode { target } => {
                        a.insert(b"target".to_vec(), OwnedValue::Bytes(target.0.to_vec()));
                        b"find_node"
                    }
                    Query::GetPeers { info_hash } => {
                        a.insert(b"info_hash".to_vec(), OwnedValue::Bytes(info_hash.to_vec()));
                        b"get_peers"
                    }
                    Query::AnnouncePeer {
                        info_hash,
                        port,
                        implied_port,
                        token,
                    } => {
                        a.insert(b"info_hash".to_vec(), OwnedValue::Bytes(info_hash.to_vec()));
                        a.insert(b"port".to_vec(), OwnedValue::Int(i64::from(*port)));
                        if *implied_port {
                            a.insert(b"implied_port".to_vec(), OwnedValue::Int(1));
                        }
                        a.insert(b"token".to_vec(), OwnedValue::Bytes(token.clone()));
                        b"announce_peer"
                    }
                    Query::Unknown(name) => name.as_bytes(),
                };
                let mut want = Vec::new();
                if q.want.v4 {
                    want.push(OwnedValue::from("n4"));
                }
                if q.want.v6 {
                    want.push(OwnedValue::from("n6"));
                }
                if !want.is_empty() {
                    a.insert(b"want".to_vec(), OwnedValue::List(want));
                }
                if q.read_only {
                    top.insert(b"ro".to_vec(), OwnedValue::Int(1));
                }
                top.insert(b"q".to_vec(), OwnedValue::Bytes(method.to_vec()));
                top.insert(b"a".to_vec(), OwnedValue::Dict(a));
                b"q"
            }
            Body::Response(r) => {
                let mut d = BTreeMap::new();
                d.insert(b"id".to_vec(), OwnedValue::Bytes(r.id.0.to_vec()));
                if let Some(n) = nodes_bytes(&r.nodes, false) {
                    d.insert(b"nodes".to_vec(), n);
                }
                if let Some(n) = nodes_bytes(&r.nodes6, true) {
                    d.insert(b"nodes6".to_vec(), n);
                }
                if !r.values.is_empty() {
                    let values = r.values.iter().copied().map(compact_bytes).collect();
                    d.insert(b"values".to_vec(), OwnedValue::List(values));
                }
                if let Some(t) = &r.token {
                    d.insert(b"token".to_vec(), OwnedValue::Bytes(t.clone()));
                }
                top.insert(b"r".to_vec(), OwnedValue::Dict(d));
                b"r"
            }
            Body::Error { code, message } => {
                let e = vec![OwnedValue::Int(*code), OwnedValue::from(message.as_str())];
                top.insert(b"e".to_vec(), OwnedValue::List(e));
                b"e"
            }
        };
        top.insert(b"y".to_vec(), OwnedValue::Bytes(y.to_vec()));
        encode(&OwnedValue::Dict(top))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const BEP5: &[&[u8]] = &[
        b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe",
        b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re",
        b"d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee",
        b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe",
        b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe",
        b"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe",
        b"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re",
    ];

    fn response_with(key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut out = b"d1:rd2:id20:abcdefghij0123456789".to_vec();
        out.extend_from_slice(key);
        out.extend_from_slice(value);
        out.extend_from_slice(b"e1:t2:aa1:y1:re");
        out
    }

    fn response_with_values(values: &[u8]) -> Vec<u8> {
        let mut list = b"l".to_vec();
        for v in values.chunks(6) {
            list.extend_from_slice(format!("{}:", v.len()).as_bytes());
            list.extend_from_slice(v);
        }
        list.push(b'e');
        response_with(b"6:values", &list)
    }

    fn response_with_nodes(nodes: &[u8]) -> Vec<u8> {
        let mut s = format!("{}:", nodes.len()).into_bytes();
        s.extend_from_slice(nodes);
        response_with(b"5:nodes", &s)
    }

    #[test]
    fn bep5_examples_roundtrip_byte_exact() {
        for m in BEP5 {
            assert_eq!(Message::decode(m).unwrap().encode(), *m);
        }
    }

    #[test]
    fn bep32_43_42_fields() {
        let q = b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz1234564:wantl2:n42:n6ee1:q9:find_node2:roi1e1:t2:aa1:y1:qe";
        let Body::Query(qm) = Message::decode(q).unwrap().body else {
            panic!()
        };
        assert_eq!(qm.want, Want { v4: true, v6: true });
        assert!(qm.read_only);
        let resp = Message {
            tid: b"aa".to_vec(),
            ip: Some("10.0.0.1:6881".parse().unwrap()),
            version: None,
            body: Body::Response(Response {
                id: NodeId([1; 20]),
                nodes: vec![],
                nodes6: vec![NodeInfo {
                    id: NodeId([2; 20]),
                    addr: "[::1]:6881".parse().unwrap(),
                }],
                values: vec![],
                token: None,
            }),
        };
        assert_eq!(Message::decode(&resp.encode()).unwrap(), resp);
    }

    #[test]
    fn unknown_method_keeps_tid() {
        let m =
            Message::decode(b"d1:ad2:id20:abcdefghij0123456789e1:q4:vote1:t2:xy1:y1:qe").unwrap();
        assert_eq!(m.tid, b"xy");
        assert!(
            matches!(m.body, Body::Query(QueryMsg { query: Query::Unknown(ref n), .. }) if n == "vote")
        );
    }

    #[test]
    fn rejects_malformed() {
        assert!(matches!(
            Message::decode(b"d1:ad2:id3:abce1:q4:ping1:t2:aa1:y1:qe"),
            Err(KrpcError::Malformed("id"))
        ));
        assert!(matches!(
            Message::decode(&vec![b'x'; 1501]),
            Err(KrpcError::TooLarge)
        ));
        assert!(Message::decode(b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti0e5:token1:xe1:q13:announce_peer1:t2:aa1:y1:qe").is_err());
    }

    #[test]
    fn caps_values_and_nodes() {
        // both datagrams stay <= MAX_DATAGRAM: 150 values = 900 B, 50 IPv4 nodes = 1300 B
        let values: Vec<u8> = (0..150u32)
            .flat_map(|i| [10, 0, (i >> 8) as u8, i as u8, 0x1a, 0xe1])
            .collect();
        let r = Message::decode(&response_with_values(&values)).unwrap();
        let Body::Response(r) = r.body else { panic!() };
        assert_eq!(r.values.len(), MAX_VALUES);
        let nodes = vec![7u8; 26 * 50];
        let Body::Response(r) = Message::decode(&response_with_nodes(&nodes)).unwrap().body else {
            panic!()
        };
        assert_eq!(r.nodes.len(), MAX_NODES);
        const { assert!(MAX_VALUES * 6 <= MAX_DATAGRAM && MAX_NODES * 38 <= MAX_DATAGRAM) };
    }

    #[test]
    fn query_fields_roundtrip() {
        for query in [
            Query::Ping,
            Query::FindNode {
                target: NodeId([3; 20]),
            },
            Query::GetPeers { info_hash: [4; 20] },
            Query::AnnouncePeer {
                info_hash: [5; 20],
                port: 0,
                implied_port: true,
                token: b"tok".to_vec(),
            },
            Query::AnnouncePeer {
                info_hash: [5; 20],
                port: 51413,
                implied_port: false,
                token: vec![],
            },
            Query::Unknown("vote".into()),
        ] {
            let m = Message {
                tid: b"\x00\xff".to_vec(),
                ip: Some("[2001:db8::1]:6881".parse().unwrap()),
                version: Some(b"FB01".to_vec()),
                body: Body::Query(QueryMsg {
                    id: NodeId([9; 20]),
                    query,
                    want: Want {
                        v4: false,
                        v6: true,
                    },
                    read_only: true,
                }),
            };
            assert_eq!(Message::decode(&m.encode()).unwrap(), m);
        }
        let e = Message {
            tid: b"e".to_vec(),
            ip: None,
            version: None,
            body: Body::Error {
                code: ERR_METHOD_UNKNOWN,
                message: "Method Unknown".into(),
            },
        };
        assert_eq!(Message::decode(&e.encode()).unwrap(), e);
    }

    #[test]
    fn response_values_nodes_token_roundtrip() {
        let m = Message {
            tid: b"aa".to_vec(),
            ip: None,
            version: None,
            body: Body::Response(Response {
                id: NodeId([1; 20]),
                nodes: vec![NodeInfo {
                    id: NodeId([2; 20]),
                    addr: "1.2.3.4:5".parse().unwrap(),
                }],
                nodes6: vec![NodeInfo {
                    id: NodeId([3; 20]),
                    addr: "[2001:db8::2]:7".parse().unwrap(),
                }],
                values: vec![
                    "5.6.7.8:9".parse().unwrap(),
                    "[2001:db8::3]:10".parse().unwrap(),
                ],
                token: Some(vec![0xab; MAX_TOKEN_LEN]),
            }),
        };
        assert_eq!(Message::decode(&m.encode()).unwrap(), m);
    }

    #[test]
    fn lenient_fields_are_cut_or_skipped() {
        // misaligned nodes: the trailing partial entry is ignored
        let Body::Response(r) = Message::decode(&response_with_nodes(&[7u8; 26 + 5]))
            .unwrap()
            .body
        else {
            panic!()
        };
        assert_eq!(r.nodes.len(), 1);
        // values entries of the wrong size and non-bytes are skipped
        let Body::Response(r) = Message::decode(&response_with(
            b"6:values",
            b"l5:abcdei1e6:\x01\x02\x03\x04\x00\x05e",
        ))
        .unwrap()
        .body
        else {
            panic!()
        };
        assert_eq!(r.values, vec!["1.2.3.4:5".parse::<SocketAddr>().unwrap()]);
        // version, method name and error message are cut to their caps
        let long = "x".repeat(300);
        let m = Message::decode(
            format!(
                "d1:ad2:id20:abcdefghij0123456789e1:q40:{}1:t1:a1:v12:abcdefghijkl1:y1:qe",
                &long[..40]
            )
            .as_bytes(),
        )
        .unwrap();
        assert_eq!(m.version.as_deref(), Some(&b"abcdefgh"[..]));
        assert!(
            matches!(m.body, Body::Query(QueryMsg { query: Query::Unknown(ref n), .. }) if n.len() == MAX_METHOD_LEN)
        );
        let m = Message::decode(format!("d1:eli202e300:{long}e1:t1:a1:y1:ee").as_bytes()).unwrap();
        assert!(
            matches!(m.body, Body::Error { code: ERR_SERVER, ref message } if message.len() == MAX_ERROR_MESSAGE)
        );
        // a multi-byte char straddling the cut is dropped, not split
        let msg = format!("{}é", "x".repeat(MAX_ERROR_MESSAGE - 1));
        let m = Message::decode(format!("d1:eli201e{}:{msg}e1:t1:a1:y1:ee", msg.len()).as_bytes())
            .unwrap();
        assert!(
            matches!(m.body, Body::Error { ref message, .. } if message.len() == MAX_ERROR_MESSAGE - 1)
        );
        // a bad `ip` length is ignored
        let m =
            Message::decode(b"d2:ip3:abc1:rd2:id20:abcdefghij0123456789e1:t2:aa1:y1:re").unwrap();
        assert_eq!(m.ip, None);
    }

    #[test]
    fn rejects_more_malformed() {
        let cases: &[(&[u8], &str)] = &[
            (b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:y1:qe", "t"),
            (b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t17:aaaaaaaaaaaaaaaaa1:y1:qe", "t"),
            (b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aae", "y"),
            (b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:xe", "y"),
            (b"d1:q4:ping1:t2:aa1:y1:qe", "a"),
            (b"d1:ade1:q4:ping1:t2:aa1:y1:qe", "id"),
            (b"d1:ad2:id20:abcdefghij0123456789e1:q9:find_node1:t2:aa1:y1:qe", "target"),
            (b"d1:ad2:id20:abcdefghij0123456789e1:q9:get_peers1:t2:aa1:y1:qe", "info_hash"),
            (b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti65536e5:token1:xe1:q13:announce_peer1:t2:aa1:y1:qe", "port"),
            (b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti1ee1:q13:announce_peer1:t2:aa1:y1:qe", "token"),
            (b"d1:rde1:t2:aa1:y1:re", "id"),
            (b"d1:t2:aa1:y1:re", "r"),
            (b"d1:t2:aa1:y1:ee", "e"),
            (b"le", "message"),
        ];
        for (buf, key) in cases {
            assert!(
                matches!(Message::decode(buf), Err(KrpcError::Malformed(k)) if k == *key),
                "{key}"
            );
        }
        let token = format!(
            "d1:rd2:id20:abcdefghij01234567895:token65:{}e1:t2:aa1:y1:re",
            "x".repeat(65)
        );
        assert!(matches!(
            Message::decode(token.as_bytes()),
            Err(KrpcError::Malformed("token"))
        ));
        assert!(matches!(
            Message::decode(b"d1:t"),
            Err(KrpcError::Bencode(_))
        ));
        // implied_port makes a bad port acceptable
        let m = Message::decode(b"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti0e5:token1:xe1:q13:announce_peer1:t2:aa1:y1:qe").unwrap();
        assert!(matches!(
            m.body,
            Body::Query(QueryMsg {
                query: Query::AnnouncePeer {
                    port: 0,
                    implied_port: true,
                    ..
                },
                ..
            })
        ));
        assert!(
            Message::decode(&vec![b'x'; MAX_DATAGRAM])
                .is_err_and(|e| !matches!(e, KrpcError::TooLarge))
        );
    }

    proptest! {
        #[test]
        fn krpc_mutated_examples_never_panic(i in 0..BEP5.len(), pos in any::<usize>(), byte in any::<u8>(), cut in any::<usize>()) {
            let mut m = BEP5[i].to_vec();
            let p = pos % m.len();
            m[p] = byte;
            let _ = Message::decode(&m);
            let _ = Message::decode(&m[..cut % (m.len() + 1)]);
        }
    }

    proptest! {
        #[test]
        fn krpc_decode_never_panics(data in proptest::collection::vec(any::<u8>(), 0..2000)) {
            let _ = Message::decode(&data);
        }
    }
}
