//! KRPC messages (BEP 5) with the BEP 32 (`nodes6`, `want`), BEP 33
//! (`scrape`, `seed`, `BFsd`, `BFpe`), BEP 42 (`ip`), BEP 43 (`ro`) and
//! BEP 51 (`sample_infohashes`) fields, and BEP 44 `get` and `put`.
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
/// BEP 51 info hash samples kept from one `samples` string; extra ones (and
/// a trailing partial one) are cut.
pub const MAX_SAMPLES: usize = 20;
/// Bytes in a BEP 33 bloom filter (`BFsd`, `BFpe`); filters of any other
/// length are ignored.
pub const BLOOM_FILTER_LEN: usize = crate::bloom::BLOOM_BYTES;

/// BEP 44: longest item value (`v`, bencoded) accepted.
pub const MAX_ITEM_VALUE: usize = 1000;
/// BEP 44: longest salt accepted.
pub const MAX_SALT: usize = 64;
/// Bencode limits for an item value on its own: a `put` nests it two
/// containers deep (`message > a > v`) within [`KRPC_LIMITS`].
pub const ITEM_VALUE_LIMITS: Limits = Limits::new(KRPC_LIMITS.max_depth - 2, 500);

/// KRPC error code: generic error.
pub const ERR_GENERIC: i64 = 201;
/// KRPC error code: server error.
pub const ERR_SERVER: i64 = 202;
/// KRPC error code: protocol error (malformed packet, invalid arguments, bad token).
pub const ERR_PROTOCOL: i64 = 203;
/// KRPC error code: method unknown.
pub const ERR_METHOD_UNKNOWN: i64 = 204;
/// BEP 44 error code: the value is longer than [`MAX_ITEM_VALUE`].
pub const ERR_VALUE_TOO_BIG: i64 = 205;
/// BEP 44 error code: the signature does not verify.
pub const ERR_INVALID_SIGNATURE: i64 = 206;
/// BEP 44 error code: the salt is longer than [`MAX_SALT`].
pub const ERR_SALT_TOO_BIG: i64 = 207;
/// BEP 44 error code: `cas` does not match the stored `seq`.
pub const ERR_CAS_MISMATCH: i64 = 301;
/// BEP 44 error code: `seq` is lower than the stored one (or equal, with
/// another value).
pub const ERR_SEQ_TOO_LOW: i64 = 302;

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
        /// BEP 33 `scrape`: answer with the `BFsd`/`BFpe` bloom filters
        /// instead of `values`.
        scrape: bool,
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
        /// BEP 33 `seed`: the announcing peer is a seed.
        seed: bool,
    },
    /// BEP 51 `sample_infohashes`.
    SampleInfohashes {
        /// Where in the id space the answer's `nodes` should be close to.
        target: NodeId,
    },
    /// BEP 44 `get`.
    Get {
        /// The item's target: SHA-1 of the value (immutable) or of the
        /// public key and salt (mutable).
        target: NodeId,
        /// Only answer with the value if the stored `seq` is higher.
        seq: Option<i64>,
    },
    /// BEP 44 `put`.
    Put {
        /// The write token from an earlier `get` answer.
        token: Vec<u8>,
        /// The value: one canonical bencoded value, at most
        /// [`MAX_ITEM_VALUE`] bytes (empty when the decoder cut it, see
        /// `reject`).
        v: Vec<u8>,
        /// Mutable items: the ed25519 public key.
        k: Option<[u8; 32]>,
        /// Mutable items: the ed25519 signature.
        sig: Option<[u8; 64]>,
        /// Mutable items: the sequence number.
        seq: Option<i64>,
        /// Mutable items: the salt, at most [`MAX_SALT`] bytes.
        salt: Option<Vec<u8>>,
        /// Compare-and-swap: store only if the stored `seq` equals it.
        cas: Option<i64>,
        /// Set by the decoder only (never encoded):
        /// [`ERR_VALUE_TOO_BIG`] when `v` was too long and
        /// [`ERR_SALT_TOO_BIG`] when the salt was, so the server can answer
        /// with that code.
        reject: Option<i64>,
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
    /// BEP 33 `BFsd`: bloom filter of seeds (only one of exactly
    /// [`BLOOM_FILTER_LEN`] bytes is kept).
    pub bf_seeds: Option<Box<[u8; 256]>>,
    /// BEP 33 `BFpe`: bloom filter of downloaders (only one of exactly
    /// [`BLOOM_FILTER_LEN`] bytes is kept).
    pub bf_peers: Option<Box<[u8; 256]>>,
    /// BEP 51 `samples`: info hashes, at most [`MAX_SAMPLES`].
    pub samples: Vec<[u8; 20]>,
    /// BEP 51 `num`: info hashes the responder stores.
    pub num: Option<u32>,
    /// BEP 51 `interval`: seconds before the responder's samples change.
    pub interval: Option<u32>,
    /// BEP 44 item value (raw canonical bencode, at most
    /// [`MAX_ITEM_VALUE`] bytes; other values are dropped).
    pub v: Option<Vec<u8>>,
    /// BEP 44 public key of a mutable item.
    pub k: Option<[u8; 32]>,
    /// BEP 44 signature of a mutable item.
    pub sig: Option<[u8; 64]>,
    /// BEP 44 sequence number of a mutable item.
    pub seq: Option<i64>,
}

impl Default for Response {
    /// An empty response with an all-zero id.
    fn default() -> Self {
        Response {
            id: NodeId([0; NODE_ID_LEN]),
            nodes: Vec::new(),
            nodes6: Vec::new(),
            values: Vec::new(),
            token: None,
            bf_seeds: None,
            bf_peers: None,
            samples: Vec::new(),
            num: None,
            interval: None,
            v: None,
            k: None,
            sig: None,
            seq: None,
        }
    }
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

/// Whether `raw` is a valid item value: one bencoded value within
/// [`ITEM_VALUE_LIMITS`], at most [`MAX_ITEM_VALUE`] bytes, in canonical
/// form (sorted keys), so that re-encoding it gives the same bytes:
/// signatures and targets cover them.
pub(crate) fn is_item_value(raw: &[u8]) -> bool {
    raw.len() <= MAX_ITEM_VALUE
        && decode_with(raw, ITEM_VALUE_LIMITS).is_ok_and(|v| encode(&v.to_owned_value()) == raw)
}

fn canonical(v: &Value<'_>) -> Option<Vec<u8>> {
    is_item_value(v.raw()).then(|| v.raw().to_vec())
}

/// An optional key of fixed length: absent is `None`, any other shape is
/// malformed.
fn fixed<const N: usize>(d: &Dict<'_>, key: &'static str) -> Result<Option<[u8; N]>, KrpcError> {
    d.get(key.as_bytes())
        .map(|v| {
            v.as_bytes()
                .and_then(|b| <[u8; N]>::try_from(b).ok())
                .ok_or(KrpcError::Malformed(key))
        })
        .transpose()
}

/// An optional integer: absent is `None`, any other shape is malformed.
fn opt_int(d: &Dict<'_>, key: &'static str) -> Result<Option<i64>, KrpcError> {
    d.get(key.as_bytes())
        .map(|v| v.as_int().ok_or(KrpcError::Malformed(key)))
        .transpose()
}

fn decode_put(a: &Dict<'_>) -> Result<Query, KrpcError> {
    let token = bytes(a, "token")?;
    if token.len() > MAX_TOKEN_LEN {
        return Err(KrpcError::Malformed("token"));
    }
    let raw_v = a.get(b"v").ok_or(KrpcError::Malformed("v"))?;
    let mut reject = None;
    let v = if raw_v.raw().len() > MAX_ITEM_VALUE {
        reject = Some(ERR_VALUE_TOO_BIG);
        Vec::new()
    } else {
        canonical(raw_v).ok_or(KrpcError::Malformed("v"))?
    };
    let salt = match a.get(b"salt") {
        None => None,
        Some(s) => {
            let s = s.as_bytes().ok_or(KrpcError::Malformed("salt"))?;
            if s.len() > MAX_SALT {
                reject = reject.or(Some(ERR_SALT_TOO_BIG));
                None
            } else {
                Some(s.to_vec())
            }
        }
    };
    Ok(Query::Put {
        token: token.to_vec(),
        v,
        k: fixed::<32>(a, "k")?,
        sig: fixed::<64>(a, "sig")?,
        seq: opt_int(a, "seq")?,
        salt,
        cas: opt_int(a, "cas")?,
        reject,
    })
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
            scrape: int(a, "scrape").is_some_and(|v| v != 0),
        },
        b"sample_infohashes" => Query::SampleInfohashes {
            target: NodeId(hash20(a, "target")?),
        },
        b"get" => Query::Get {
            target: NodeId(hash20(a, "target")?),
            seq: opt_int(a, "seq")?,
        },
        b"put" => decode_put(a)?,
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
                seed: int(a, "seed").is_some_and(|v| v != 0),
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
    let bloom = |key: &[u8]| -> Option<Box<[u8; 256]>> {
        let b = r.get(key).and_then(Value::as_bytes)?;
        <[u8; BLOOM_FILTER_LEN]>::try_from(b).ok().map(Box::new)
    };
    let samples = r
        .get(b"samples")
        .and_then(Value::as_bytes)
        .map(|b| {
            b.as_chunks::<20>()
                .0
                .iter()
                .take(MAX_SAMPLES)
                .copied()
                .collect()
        })
        .unwrap_or_default();
    let u32_of = |key: &str| int(r, key).and_then(|v| u32::try_from(v).ok());
    // BEP 44 fields are optional and lenient: wrong shapes are dropped
    let v = r.get(b"v").and_then(canonical);
    let k = fixed::<32>(r, "k").ok().flatten();
    let sig = fixed::<64>(r, "sig").ok().flatten();
    Ok(Response {
        id,
        nodes,
        nodes6,
        values,
        token,
        bf_seeds: bloom(b"BFsd"),
        bf_peers: bloom(b"BFpe"),
        samples,
        num: u32_of("num"),
        interval: u32_of("interval"),
        v,
        k,
        sig,
        seq: int(r, "seq"),
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

/// An item value as an owned tree. Values in a [`Message`] are canonical
/// bencode (the decoder and the `Dht` API check it), so this re-encodes to
/// the same bytes; anything else is sent as a byte string.
fn raw_value(v: &[u8]) -> OwnedValue {
    decode_with(v, ITEM_VALUE_LIMITS)
        .map(|x| x.to_owned_value())
        .unwrap_or_else(|_| OwnedValue::Bytes(v.to_vec()))
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
                    Query::GetPeers { info_hash, scrape } => {
                        a.insert(b"info_hash".to_vec(), OwnedValue::Bytes(info_hash.to_vec()));
                        if *scrape {
                            a.insert(b"scrape".to_vec(), OwnedValue::Int(1));
                        }
                        b"get_peers"
                    }
                    Query::SampleInfohashes { target } => {
                        a.insert(b"target".to_vec(), OwnedValue::Bytes(target.0.to_vec()));
                        b"sample_infohashes"
                    }
                    Query::Get { target, seq } => {
                        a.insert(b"target".to_vec(), OwnedValue::Bytes(target.0.to_vec()));
                        if let Some(seq) = seq {
                            a.insert(b"seq".to_vec(), OwnedValue::Int(*seq));
                        }
                        b"get"
                    }
                    Query::Put {
                        token,
                        v,
                        k,
                        sig,
                        seq,
                        salt,
                        cas,
                        reject: _,
                    } => {
                        a.insert(b"token".to_vec(), OwnedValue::Bytes(token.clone()));
                        a.insert(b"v".to_vec(), raw_value(v));
                        if let Some(k) = k {
                            a.insert(b"k".to_vec(), OwnedValue::Bytes(k.to_vec()));
                        }
                        if let Some(sig) = sig {
                            a.insert(b"sig".to_vec(), OwnedValue::Bytes(sig.to_vec()));
                        }
                        if let Some(seq) = seq {
                            a.insert(b"seq".to_vec(), OwnedValue::Int(*seq));
                        }
                        if let Some(salt) = salt {
                            a.insert(b"salt".to_vec(), OwnedValue::Bytes(salt.clone()));
                        }
                        if let Some(cas) = cas {
                            a.insert(b"cas".to_vec(), OwnedValue::Int(*cas));
                        }
                        b"put"
                    }
                    Query::AnnouncePeer {
                        info_hash,
                        port,
                        implied_port,
                        token,
                        seed,
                    } => {
                        if *seed {
                            a.insert(b"seed".to_vec(), OwnedValue::Int(1));
                        }
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
                if let Some(b) = &r.bf_seeds {
                    d.insert(b"BFsd".to_vec(), OwnedValue::Bytes(b.to_vec()));
                }
                if let Some(b) = &r.bf_peers {
                    d.insert(b"BFpe".to_vec(), OwnedValue::Bytes(b.to_vec()));
                }
                if !r.samples.is_empty() {
                    d.insert(b"samples".to_vec(), OwnedValue::Bytes(r.samples.concat()));
                }
                if let Some(n) = r.num {
                    d.insert(b"num".to_vec(), OwnedValue::Int(i64::from(n)));
                }
                if let Some(i) = r.interval {
                    d.insert(b"interval".to_vec(), OwnedValue::Int(i64::from(i)));
                }
                if let Some(v) = &r.v {
                    d.insert(b"v".to_vec(), raw_value(v));
                }
                if let Some(k) = &r.k {
                    d.insert(b"k".to_vec(), OwnedValue::Bytes(k.to_vec()));
                }
                if let Some(sig) = &r.sig {
                    d.insert(b"sig".to_vec(), OwnedValue::Bytes(sig.to_vec()));
                }
                if let Some(seq) = r.seq {
                    d.insert(b"seq".to_vec(), OwnedValue::Int(seq));
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
                ..Response::default()
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
            Query::GetPeers {
                info_hash: [4; 20],
                scrape: false,
            },
            Query::AnnouncePeer {
                info_hash: [5; 20],
                port: 0,
                implied_port: true,
                token: b"tok".to_vec(),
                seed: false,
            },
            Query::AnnouncePeer {
                info_hash: [5; 20],
                port: 51413,
                implied_port: false,
                token: vec![],
                seed: false,
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
                ..Response::default()
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

    #[test]
    fn new_fields_round_trip() {
        for query in [
            Query::GetPeers {
                info_hash: [4; 20],
                scrape: true,
            },
            Query::AnnouncePeer {
                info_hash: [5; 20],
                port: 6881,
                implied_port: false,
                token: b"tok".to_vec(),
                seed: true,
            },
            Query::SampleInfohashes {
                target: NodeId([6; 20]),
            },
        ] {
            let m = Message {
                tid: b"aa".to_vec(),
                ip: None,
                version: None,
                body: Body::Query(QueryMsg {
                    id: NodeId([9; 20]),
                    query,
                    want: Want::default(),
                    read_only: false,
                }),
            };
            assert_eq!(Message::decode(&m.encode()).unwrap(), m);
        }
        let mut seeds = Box::new([0u8; 256]);
        seeds[0] = 0x81;
        let mut peers = Box::new([0u8; 256]);
        peers[255] = 0x18;
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
                bf_seeds: Some(seeds),
                bf_peers: Some(peers),
                samples: vec![[7; 20], [8; 20]],
                num: Some(1234),
                interval: Some(21600),
                ..Response::default()
            }),
        };
        let bytes = m.encode();
        assert!(bytes.windows(9).any(|w| w == b"4:BFsd256"));
        assert!(bytes.windows(9).any(|w| w == b"4:BFpe256"));
        assert_eq!(Message::decode(&bytes).unwrap(), m);
        // BEP 33 and BEP 51 key names on the wire
        let q = b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234566:scrapei1ee1:q9:get_peers1:t2:aa1:y1:qe";
        assert!(matches!(
            Message::decode(q).unwrap().body,
            Body::Query(QueryMsg {
                query: Query::GetPeers { scrape: true, .. },
                ..
            })
        ));
        assert_eq!(Message::decode(q).unwrap().encode(), q);
        let q = b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q17:sample_infohashes1:t2:aa1:y1:qe";
        assert!(matches!(
            Message::decode(q).unwrap().body,
            Body::Query(QueryMsg {
                query: Query::SampleInfohashes { .. },
                ..
            })
        ));
        assert_eq!(Message::decode(q).unwrap().encode(), q);
        let q = b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti6881e4:seedi1e5:token1:xe1:q13:announce_peer1:t2:aa1:y1:qe";
        assert!(matches!(
            Message::decode(q).unwrap().body,
            Body::Query(QueryMsg {
                query: Query::AnnouncePeer { seed: true, .. },
                ..
            })
        ));
        assert_eq!(Message::decode(q).unwrap().encode(), q);
        // non-integer `scrape` and `seed` values read as unset
        let q = b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234566:scrape1:1e1:q9:get_peers1:t2:aa1:y1:qe";
        assert!(matches!(
            Message::decode(q).unwrap().body,
            Body::Query(QueryMsg {
                query: Query::GetPeers { scrape: false, .. },
                ..
            })
        ));
        for seed in [&b"4:seed1:1"[..], b"4:seedli1ee", b"4:seedde"] {
            let mut q =
                b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti6881e"
                    .to_vec();
            q.extend_from_slice(seed);
            q.extend_from_slice(b"5:token1:xe1:q13:announce_peer1:t2:aa1:y1:qe");
            assert!(matches!(
                Message::decode(&q).unwrap().body,
                Body::Query(QueryMsg {
                    query: Query::AnnouncePeer { seed: false, .. },
                    ..
                })
            ));
        }
        // a sample_infohashes query without a target is malformed
        assert!(matches!(
            Message::decode(
                b"d1:ad2:id20:abcdefghij0123456789e1:q17:sample_infohashes1:t2:aa1:y1:qe"
            ),
            Err(KrpcError::Malformed("target"))
        ));
    }

    #[test]
    fn oversized_samples_are_cut() {
        // 25 whole samples plus 7 stray bytes: 20 kept, the tail dropped
        let mut raw = Vec::new();
        for k in 0..25u8 {
            raw.extend_from_slice(&[k; 20]);
        }
        raw.extend_from_slice(&[0xee; 7]);
        let mut v = format!("{}:", raw.len()).into_bytes();
        v.extend_from_slice(&raw);
        let Body::Response(r) = Message::decode(&response_with(b"7:samples", &v))
            .unwrap()
            .body
        else {
            panic!()
        };
        assert_eq!(r.samples.len(), MAX_SAMPLES);
        assert!(
            r.samples
                .iter()
                .enumerate()
                .all(|(k, s)| *s == [k as u8; 20])
        );
        // fewer than 20 bytes: no sample at all; num and interval out of range are ignored
        let Body::Response(r) = Message::decode(&response_with(
            b"8:intervali-1e3:numi99999999999e7:samples",
            b"19:aaaaaaaaaaaaaaaaaaa",
        ))
        .unwrap()
        .body
        else {
            panic!()
        };
        assert!(r.samples.is_empty());
        assert_eq!((r.num, r.interval), (None, None));
    }

    #[test]
    fn short_bloom_filter_is_ignored() {
        for len in [0usize, 1, 255, 257] {
            let mut v = format!("{len}:").into_bytes();
            v.extend(std::iter::repeat_n(0xffu8, len));
            for key in [&b"4:BFpe"[..], b"4:BFsd"] {
                let Body::Response(r) = Message::decode(&response_with(key, &v)).unwrap().body
                else {
                    panic!()
                };
                assert_eq!((r.bf_seeds, r.bf_peers), (None, None), "{len}");
            }
        }
        // the right length but not a string: ignored too
        let Body::Response(r) = Message::decode(&response_with(b"4:BFsd", b"i1e"))
            .unwrap()
            .body
        else {
            panic!()
        };
        assert!(r.bf_seeds.is_none());
    }

    #[test]
    fn get_put_round_trip() {
        for query in [
            Query::Get {
                target: NodeId([3; 20]),
                seq: None,
            },
            Query::Get {
                target: NodeId([3; 20]),
                seq: Some(7),
            },
            // immutable
            Query::Put {
                token: b"tok".to_vec(),
                v: b"12:Hello World!".to_vec(),
                k: None,
                sig: None,
                seq: None,
                salt: None,
                cas: None,
                reject: None,
            },
            // mutable, nested value
            Query::Put {
                token: b"tok".to_vec(),
                v: b"d1:al1:bi-3ee1:cd0:0:ee".to_vec(),
                k: Some([4; 32]),
                sig: Some([5; 64]),
                seq: Some(i64::MAX),
                salt: Some(b"foobar".to_vec()),
                cas: Some(-1),
                reject: None,
            },
        ] {
            let m = Message {
                tid: b"aa".to_vec(),
                ip: None,
                version: Some(b"FB01".to_vec()),
                body: Body::Query(QueryMsg {
                    id: NodeId([9; 20]),
                    query,
                    want: Want::default(),
                    read_only: false,
                }),
            };
            assert_eq!(Message::decode(&m.encode()).unwrap(), m);
        }
        for r in [
            Response {
                id: NodeId([1; 20]),
                token: Some(b"tok".to_vec()),
                v: Some(b"li1ei2ee".to_vec()),
                ..Response::default()
            },
            Response {
                id: NodeId([1; 20]),
                token: Some(b"tok".to_vec()),
                v: Some(b"12:Hello World!".to_vec()),
                k: Some([4; 32]),
                sig: Some([5; 64]),
                seq: Some(1),
                ..Response::default()
            },
        ] {
            let m = Message {
                tid: b"aa".to_vec(),
                ip: None,
                version: Some(b"FB01".to_vec()),
                body: Body::Response(r),
            };
            let bytes = m.encode();
            assert_eq!(Message::decode(&bytes).unwrap(), m);
        }
        // BEP 44 key names on the wire; the item's `v` is the raw value,
        // the client version stays at the top level
        let q = b"d1:ad2:id20:abcdefghij01234567893:seqi4e6:target20:mnopqrstuvwxyz123456e1:q3:get1:t2:aa1:y1:qe";
        assert!(matches!(
            Message::decode(q).unwrap().body,
            Body::Query(QueryMsg {
                query: Query::Get { seq: Some(4), .. },
                ..
            })
        ));
        assert_eq!(Message::decode(q).unwrap().encode(), q);
        let r = b"d1:rd2:id20:abcdefghij01234567895:token3:tok1:vli1ei2eee1:t2:aa1:v4:FB011:y1:re";
        let m = Message::decode(r).unwrap();
        assert_eq!(m.version.as_deref(), Some(&b"FB01"[..]));
        assert!(
            matches!(m.body, Body::Response(Response { v: Some(ref v), .. }) if v == b"li1ei2ee")
        );
        assert_eq!(m.encode(), r);
        // hostile puts: bad key or signature lengths, non-integer seq or cas,
        // a value that is not canonical bencode, a missing value or token
        let put = |args: &str| {
            format!("d1:ad2:id20:abcdefghij0123456789{args}5:token3:toke1:q3:put1:t2:aa1:y1:qe")
                .into_bytes()
        };
        for (args, key) in [
            ("1:k3:abc1:v1:x", "k"),
            (
                "1:k32:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa3:sig3:abc1:v1:x",
                "sig",
            ),
            ("3:seq1:11:v1:x", "seq"),
            ("3:casle1:v1:x", "cas"),
            ("4:salti1e1:v1:x", "salt"),
            ("1:vd1:bi1e1:ai2ee", "v"),
            ("", "v"),
        ] {
            assert!(
                matches!(Message::decode(&put(args)), Err(KrpcError::Malformed(k)) if k == key),
                "{args}"
            );
        }
        assert!(matches!(
            Message::decode(b"d1:ad2:id20:abcdefghij01234567891:vi1ee1:q3:put1:t2:aa1:y1:qe"),
            Err(KrpcError::Malformed("token"))
        ));
        assert!(matches!(
            Message::decode(b"d1:ad2:id20:abcdefghij0123456789e1:q3:get1:t2:aa1:y1:qe"),
            Err(KrpcError::Malformed("target"))
        ));
        // a seq that overflows i64 is no bencode integer at all
        assert!(matches!(
            Message::decode(&put("3:seqi9223372036854775808e1:v1:x")),
            Err(KrpcError::Bencode(_))
        ));
        // a negative seq decodes; the put rules refuse it
        assert!(matches!(
            Message::decode(&put("3:seqi-5e1:v1:x")).unwrap().body,
            Body::Query(QueryMsg {
                query: Query::Put { seq: Some(-5), .. },
                ..
            })
        ));
        // lenient responses: bad lengths and non-canonical values are dropped
        let Body::Response(r) = Message::decode(&response_with(
            b"1:k3:abc3:seq1:13:sig3:abc1:v",
            b"d1:bi1e1:ai2ee",
        ))
        .unwrap()
        .body
        else {
            panic!()
        };
        assert_eq!((r.k, r.sig, r.seq, r.v), (None, None, None, None));
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
