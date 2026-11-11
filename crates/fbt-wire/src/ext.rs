//! Extension protocol messages: BEP 10 handshake, BEP 9 `ut_metadata` and
//! BEP 11 `ut_pex`. All of it parses hostile peer input: bencode is decoded
//! with explicit limits and every list and map has a named cap.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use bitflags::bitflags;
use bytes::Bytes;
use fbt_bencode::{DecodeError, DictBuilder, Limits, Value, decode_prefix, decode_with, encode};
use fbt_core::compact;

/// Extended message id of the BEP 10 handshake.
pub const EXT_HANDSHAKE_ID: u8 = 0;
/// Extension name of BEP 9.
pub const UT_METADATA: &str = "ut_metadata";
/// Extension name of BEP 11.
pub const UT_PEX: &str = "ut_pex";
/// Our local id for `ut_pex` (libtorrent's numbering).
pub const OUR_UT_PEX_ID: u8 = 1;
/// Our local id for `ut_metadata` (libtorrent's numbering).
pub const OUR_UT_METADATA_ID: u8 = 2;
/// Extension name of BEP 21.
pub const UPLOAD_ONLY: &str = "upload_only";
/// Our local id for `upload_only` (libtorrent's numbering).
pub const OUR_UPLOAD_ONLY_ID: u8 = 3;
/// Extension name of BEP 55.
pub const UT_HOLEPUNCH: &str = "ut_holepunch";
/// Our local id for `ut_holepunch` (libtorrent's numbering).
pub const OUR_UT_HOLEPUNCH_ID: u8 = 4;
/// Extension name of BEP 54.
pub const LT_DONTHAVE: &str = "lt_donthave";
/// Our local id for `lt_donthave` (libtorrent's numbering).
pub const OUR_LT_DONTHAVE_ID: u8 = 7;
/// BEP 55 error: the target peer is unknown to the rendezvous peer.
pub const HP_NO_SUCH_PEER: u32 = 1;
/// BEP 55 error: the target peer is not connected to the rendezvous peer.
pub const HP_NOT_CONNECTED: u32 = 2;
/// BEP 55 error: the target peer does not support hole punching.
pub const HP_NO_SUPPORT: u32 = 3;
/// BEP 55 error: the target is the rendezvous peer's own requester.
pub const HP_NO_SELF: u32 = 4;
/// Size of one metadata piece.
pub const METADATA_PIECE_LEN: usize = 16_384;
/// Largest extended handshake payload accepted.
pub const MAX_EXT_HANDSHAKE_LEN: usize = 65_536;
/// Largest number of entries kept per PEX list.
pub const MAX_PEX_ENTRIES: usize = 200;

/// Most entries kept in the handshake `m` map.
const MAX_EXT_M_ENTRIES: usize = 64;
/// Longest extension name or client string kept, in bytes.
const MAX_EXT_NAME_LEN: usize = 64;
const HANDSHAKE_LIMITS: Limits = Limits::new(4, 512);
const METADATA_LIMITS: Limits = Limits::new(2, 16);
const PEX_LIMITS: Limits = Limits::new(2, 32);

/// Extension message failures.
#[derive(Debug, thiserror::Error)]
pub enum ExtError {
    /// The bencode was malformed or over the limits.
    #[error("bencode: {0}")]
    Bencode(#[from] DecodeError),
    /// The top-level value is not a dictionary.
    #[error("payload is not a dictionary")]
    NotADict,
    /// A required key is absent.
    #[error("missing field {0}")]
    MissingField(&'static str),
    /// A key has an invalid value.
    #[error("invalid field {0}")]
    InvalidField(&'static str),
    /// The payload exceeds its size cap.
    #[error("payload too large")]
    TooLarge,
}

/// The BEP 10 extended handshake.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExtHandshake {
    /// Extension name to remote message id (`m`).
    pub m: BTreeMap<String, u8>,
    /// Client name and version (`v`), cut to 64 bytes.
    pub client: Option<String>,
    /// Listen port (`p`); 0 is dropped.
    pub listen_port: Option<u16>,
    /// Outstanding request count (`reqq`).
    pub reqq: Option<u32>,
    /// Metadata size in bytes (`metadata_size`).
    pub metadata_size: Option<u32>,
    /// Our address as the peer sees it (`yourip`).
    pub your_ip: Option<IpAddr>,
    /// BEP 21 `upload_only` flag; a non-zero value is true, encoded only
    /// when `Some(true)`.
    pub upload_only: Option<bool>,
}

fn cut_str(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s.get(..end).unwrap_or("")
}

fn dict_of<'a>(v: &'a Value<'_>) -> Result<&'a fbt_bencode::Dict<'a>, ExtError> {
    v.as_dict().ok_or(ExtError::NotADict)
}

fn uint(v: &Value<'_>) -> Option<u32> {
    v.as_int().and_then(|i| u32::try_from(i).ok())
}

impl ExtHandshake {
    /// Encodes the handshake payload (without the extended id byte).
    pub fn encode(&self) -> Bytes {
        let mut m = DictBuilder::new();
        for (k, v) in &self.m {
            m.insert(k.as_bytes(), i64::from(*v));
        }
        let mut d = DictBuilder::new();
        d.insert(b"m", m.build());
        if let Some(v) = &self.client {
            d.insert(b"v", v.as_str());
        }
        if let Some(p) = self.listen_port {
            d.insert(b"p", i64::from(p));
        }
        if let Some(r) = self.reqq {
            d.insert(b"reqq", i64::from(r));
        }
        if let Some(s) = self.metadata_size {
            d.insert(b"metadata_size", i64::from(s));
        }
        match self.your_ip {
            Some(IpAddr::V4(ip)) => {
                d.insert(b"yourip", &ip.octets()[..]);
            }
            Some(IpAddr::V6(ip)) => {
                d.insert(b"yourip", &ip.octets()[..]);
            }
            None => {}
        }
        if self.upload_only == Some(true) {
            d.insert(b"upload_only", 1i64);
        }
        Bytes::from(encode(&d.build()))
    }

    /// Decodes a handshake payload from a peer.
    pub fn decode(payload: &[u8]) -> Result<ExtHandshake, ExtError> {
        if payload.len() > MAX_EXT_HANDSHAKE_LEN {
            return Err(ExtError::TooLarge);
        }
        let v = decode_with(payload, HANDSHAKE_LIMITS)?;
        let d = dict_of(&v)?;
        let mut out = ExtHandshake::default();
        if let Some(m) = d.get(b"m").and_then(Value::as_dict) {
            for (k, val) in m.iter() {
                if out.m.len() >= MAX_EXT_M_ENTRIES {
                    break;
                }
                let (Ok(name), Some(id)) = (std::str::from_utf8(k), val.as_int()) else {
                    continue;
                };
                let Ok(id) = u8::try_from(id) else { continue };
                if name.len() > MAX_EXT_NAME_LEN {
                    continue;
                }
                out.m.insert(name.to_owned(), id);
            }
        }
        out.client = d
            .get(b"v")
            .and_then(Value::as_bytes)
            .map(|b| cut_str(&String::from_utf8_lossy(b), MAX_EXT_NAME_LEN).to_owned());
        out.listen_port = d
            .get(b"p")
            .and_then(Value::as_int)
            .and_then(|i| u16::try_from(i).ok())
            .filter(|p| *p != 0);
        out.reqq = d.get(b"reqq").and_then(uint);
        out.metadata_size = d.get(b"metadata_size").and_then(uint);
        out.your_ip = d
            .get(b"yourip")
            .and_then(Value::as_bytes)
            .and_then(|b| match b.len() {
                4 => <[u8; 4]>::try_from(b)
                    .ok()
                    .map(|a| IpAddr::V4(Ipv4Addr::from(a))),
                16 => <[u8; 16]>::try_from(b)
                    .ok()
                    .map(|a| IpAddr::V6(Ipv6Addr::from(a))),
                _ => None,
            });
        out.upload_only = d
            .get(b"upload_only")
            .and_then(Value::as_int)
            .map(|i| i != 0);
        Ok(out)
    }

    /// The remote id for extension `name`; `None` when absent or 0 (disabled).
    pub fn id_of(&self, name: &str) -> Option<u8> {
        self.m.get(name).copied().filter(|id| *id != 0)
    }
}

/// A BEP 9 `ut_metadata` message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataMsg {
    /// Ask for a metadata piece.
    Request {
        /// Piece index.
        piece: u32,
    },
    /// A metadata piece.
    Data {
        /// Piece index.
        piece: u32,
        /// Total metadata size.
        total_size: u32,
        /// The piece bytes.
        data: Bytes,
    },
    /// Refusal to serve a piece.
    Reject {
        /// Piece index.
        piece: u32,
    },
}

impl MetadataMsg {
    /// Encodes the message payload (without the extended id byte).
    pub fn encode(&self) -> Bytes {
        let mut d = DictBuilder::new();
        let (ty, piece) = match self {
            MetadataMsg::Request { piece } => (0i64, *piece),
            MetadataMsg::Data { piece, .. } => (1, *piece),
            MetadataMsg::Reject { piece } => (2, *piece),
        };
        d.insert(b"msg_type", ty).insert(b"piece", i64::from(piece));
        if let MetadataMsg::Data { total_size, .. } = self {
            d.insert(b"total_size", i64::from(*total_size));
        }
        let mut out = encode(&d.build());
        if let MetadataMsg::Data { data, .. } = self {
            out.extend_from_slice(data);
        }
        Bytes::from(out)
    }

    /// Decodes a payload from a peer; the piece of `Data` is a zero-copy slice.
    pub fn decode(payload: &Bytes) -> Result<MetadataMsg, ExtError> {
        let (v, used) = decode_prefix(payload, METADATA_LIMITS)?;
        let d = dict_of(&v)?;
        let ty = d
            .get(b"msg_type")
            .and_then(Value::as_int)
            .ok_or(ExtError::MissingField("msg_type"))?;
        let piece = d
            .get(b"piece")
            .ok_or(ExtError::MissingField("piece"))
            .and_then(|p| uint(p).ok_or(ExtError::InvalidField("piece")))?;
        match ty {
            0 => Ok(MetadataMsg::Request { piece }),
            1 => {
                let total_size = d
                    .get(b"total_size")
                    .ok_or(ExtError::MissingField("total_size"))
                    .and_then(|p| uint(p).ok_or(ExtError::InvalidField("total_size")))?;
                if payload.len().saturating_sub(used) > METADATA_PIECE_LEN {
                    return Err(ExtError::InvalidField("data"));
                }
                Ok(MetadataMsg::Data {
                    piece,
                    total_size,
                    data: payload.slice(used..),
                })
            }
            2 => Ok(MetadataMsg::Reject { piece }),
            _ => Err(ExtError::InvalidField("msg_type")),
        }
    }
}

bitflags! {
    /// BEP 11 per-peer flags.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct PexFlags: u8 {
        /// Prefers encryption.
        const ENCRYPTION = 0x01;
        /// Is a seed.
        const SEED = 0x02;
        /// Supports uTP.
        const UTP = 0x04;
        /// Supports hole punching.
        const HOLEPUNCH = 0x08;
        /// Is connectable.
        const CONNECTABLE = 0x10;
    }
}

/// A BEP 11 `ut_pex` message.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PexMsg {
    /// Newly seen peers with their flags.
    pub added: Vec<(SocketAddr, PexFlags)>,
    /// Peers no longer connected.
    pub dropped: Vec<SocketAddr>,
}

impl PexMsg {
    /// Encodes the message payload (without the extended id byte).
    pub fn encode(&self) -> Bytes {
        let (mut a4, mut f4, mut a6, mut f6) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for (addr, flags) in &self.added {
            if addr.is_ipv4() {
                compact::encode(*addr, &mut a4);
                f4.push(flags.bits());
            } else {
                compact::encode(*addr, &mut a6);
                f6.push(flags.bits());
            }
        }
        let (mut d4, mut d6) = (Vec::new(), Vec::new());
        for addr in &self.dropped {
            compact::encode(*addr, if addr.is_ipv4() { &mut d4 } else { &mut d6 });
        }
        let mut d = DictBuilder::new();
        for (key, val) in [
            (&b"added"[..], a4),
            (b"added.f", f4),
            (b"added6", a6),
            (b"added6.f", f6),
            (b"dropped", d4),
            (b"dropped6", d6),
        ] {
            if !val.is_empty() {
                d.insert(key, val);
            }
        }
        Bytes::from(encode(&d.build()))
    }

    /// Decodes a payload from a peer. Lists are cut to [`MAX_PEX_ENTRIES`].
    pub fn decode(payload: &[u8]) -> Result<PexMsg, ExtError> {
        let v = decode_with(payload, PEX_LIMITS)?;
        let d = dict_of(&v)?;
        let field = |k: &[u8]| d.get(k).and_then(Value::as_bytes).unwrap_or(&[]);
        let mut out = PexMsg::default();
        let v4 = compact::decode_v4_list(field(b"added"), MAX_PEX_ENTRIES)
            .map_err(|_| ExtError::InvalidField("added"))?;
        let v6 = compact::decode_v6_list(field(b"added6"), MAX_PEX_ENTRIES)
            .map_err(|_| ExtError::InvalidField("added6"))?;
        for (list, fkey) in [(v4, &b"added.f"[..]), (v6, b"added6.f")] {
            let flags = field(fkey);
            for (i, addr) in list.into_iter().enumerate() {
                let f = flags.get(i).copied().unwrap_or(0);
                out.added.push((addr, PexFlags::from_bits_truncate(f)));
            }
        }
        for key in [&b"dropped"[..], b"dropped6"] {
            let list = if key == b"dropped" {
                compact::decode_v4_list(field(key), MAX_PEX_ENTRIES)
            } else {
                compact::decode_v6_list(field(key), MAX_PEX_ENTRIES)
            }
            .map_err(|_| ExtError::InvalidField("dropped"))?;
            out.dropped.extend(list);
        }
        Ok(out)
    }
}

/// Encodes a BEP 21 `upload_only` message payload (one byte, 0 or 1).
pub fn encode_upload_only(on: bool) -> Bytes {
    Bytes::from_static(if on { &[1] } else { &[0] })
}

/// Decodes a BEP 21 `upload_only` payload; extra bytes are ignored.
pub fn decode_upload_only(payload: &[u8]) -> Result<bool, ExtError> {
    payload
        .first()
        .map(|b| *b != 0)
        .ok_or(ExtError::InvalidField("upload_only"))
}

/// Encodes a BEP 54 `lt_donthave` payload (piece index, big-endian).
pub fn encode_donthave(piece: u32) -> Bytes {
    Bytes::copy_from_slice(&piece.to_be_bytes())
}

/// Decodes a BEP 54 `lt_donthave` payload; it must be exactly 4 bytes.
pub fn decode_donthave(payload: &[u8]) -> Result<u32, ExtError> {
    <[u8; 4]>::try_from(payload)
        .map(u32::from_be_bytes)
        .map_err(|_| ExtError::InvalidField("donthave"))
}

/// A BEP 55 `ut_holepunch` message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HolepunchMsg {
    /// Ask the receiver to introduce us to the given peer.
    Rendezvous(SocketAddr),
    /// Connect to the given peer now.
    Connect(SocketAddr),
    /// The rendezvous failed.
    Error {
        /// The peer the request was about.
        addr: SocketAddr,
        /// One of the `HP_*` codes.
        code: u32,
    },
}

impl HolepunchMsg {
    /// Encodes the message payload (without the extended id byte).
    pub fn encode(&self) -> Bytes {
        let (ty, addr, code) = match self {
            HolepunchMsg::Rendezvous(a) => (0u8, *a, None),
            HolepunchMsg::Connect(a) => (1, *a, None),
            HolepunchMsg::Error { addr, code } => (2, *addr, Some(*code)),
        };
        let mut out = vec![ty];
        match addr.ip() {
            IpAddr::V4(ip) => {
                out.push(0);
                out.extend_from_slice(&ip.octets());
            }
            IpAddr::V6(ip) => {
                out.push(1);
                out.extend_from_slice(&ip.octets());
            }
        }
        out.extend_from_slice(&addr.port().to_be_bytes());
        if let Some(code) = code {
            out.extend_from_slice(&code.to_be_bytes());
        }
        Bytes::from(out)
    }

    /// Decodes a payload from a peer; trailing bytes are ignored.
    pub fn decode(payload: &[u8]) -> Result<HolepunchMsg, ExtError> {
        const SHORT: ExtError = ExtError::InvalidField("holepunch");
        let ty = *payload.first().ok_or(SHORT)?;
        if ty > 2 {
            return Err(ExtError::InvalidField("msg_type"));
        }
        let addr_len = match payload.get(1).ok_or(SHORT)? {
            0 => 4,
            1 => 16,
            _ => return Err(ExtError::InvalidField("addr_type")),
        };
        let ip_bytes = payload.get(2..2 + addr_len).ok_or(SHORT)?;
        let ip = match <[u8; 4]>::try_from(ip_bytes) {
            Ok(a) => IpAddr::V4(Ipv4Addr::from(a)),
            Err(_) => IpAddr::V6(Ipv6Addr::from(
                <[u8; 16]>::try_from(ip_bytes).map_err(|_| SHORT)?,
            )),
        };
        let port_at = 2 + addr_len;
        let port = u16::from_be_bytes(
            payload
                .get(port_at..port_at + 2)
                .and_then(|b| <[u8; 2]>::try_from(b).ok())
                .ok_or(SHORT)?,
        );
        if port == 0 {
            return Err(ExtError::InvalidField("port"));
        }
        let addr = SocketAddr::new(ip, port);
        match ty {
            0 => Ok(HolepunchMsg::Rendezvous(addr)),
            1 => Ok(HolepunchMsg::Connect(addr)),
            _ => {
                let at = port_at + 2;
                let code = payload
                    .get(at..at + 4)
                    .and_then(|b| <[u8; 4]>::try_from(b).ok())
                    .map(u32::from_be_bytes)
                    .ok_or(SHORT)?;
                Ok(HolepunchMsg::Error { addr, code })
            }
        }
    }
}
