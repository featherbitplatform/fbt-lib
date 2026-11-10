//! Torrent info hashes (BEP 3 SHA-1 and BEP 52 SHA-256).

use std::fmt;
use std::str::FromStr;

use sha1::{Digest as _, Sha1};
use sha2::Sha256;

/// Error parsing an info hash from hex.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ParseInfoHashError {
    /// The string has the wrong number of characters.
    #[error("info hash must be {expected} hex digits")]
    WrongLength {
        /// Expected number of hex digits.
        expected: usize,
    },
    /// The string contains a non-hex character.
    #[error("info hash contains a non-hex character")]
    InvalidHex,
}

fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn parse_hex<const N: usize>(s: &str) -> Result<[u8; N], ParseInfoHashError> {
    let bytes = s.as_bytes();
    if bytes.len() != N * 2 {
        return Err(ParseInfoHashError::WrongLength { expected: N * 2 });
    }
    let mut out = [0u8; N];
    let (pairs, _) = bytes.as_chunks::<2>();
    for (slot, &[hi, lo]) in out.iter_mut().zip(pairs) {
        match (nibble(hi), nibble(lo)) {
            (Some(h), Some(l)) => *slot = (h << 4) | l,
            _ => return Err(ParseInfoHashError::InvalidHex),
        }
    }
    Ok(out)
}

fn write_hex(f: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for b in bytes {
        write!(f, "{b:02x}")?;
    }
    Ok(())
}

/// SHA-1 info hash of a v1 (or hybrid) torrent.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InfoHashV1(pub [u8; 20]);

impl InfoHashV1 {
    /// Hashes the bencoded `info` dictionary bytes with SHA-1.
    pub fn of(info_bytes: &[u8]) -> Self {
        Self(Sha1::digest(info_bytes).into())
    }
}

impl fmt::Display for InfoHashV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(f, &self.0)
    }
}

impl fmt::Debug for InfoHashV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InfoHashV1({self})")
    }
}

impl FromStr for InfoHashV1 {
    type Err = ParseInfoHashError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_hex::<20>(s).map(Self)
    }
}

/// SHA-256 info hash of a v2 (or hybrid) torrent.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InfoHashV2(pub [u8; 32]);

impl InfoHashV2 {
    /// Hashes the bencoded `info` dictionary bytes with SHA-256.
    pub fn of(info_bytes: &[u8]) -> Self {
        Self(Sha256::digest(info_bytes).into())
    }
}

impl fmt::Display for InfoHashV2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(f, &self.0)
    }
}

impl fmt::Debug for InfoHashV2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InfoHashV2({self})")
    }
}

impl FromStr for InfoHashV2 {
    type Err = ParseInfoHashError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_hex::<32>(s).map(Self)
    }
}

/// The info hash(es) identifying a torrent: v1, v2 or both (hybrid).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InfoHashes {
    /// SHA-1 hash, present for v1 and hybrid torrents.
    pub v1: Option<InfoHashV1>,
    /// SHA-256 hash, present for v2 and hybrid torrents.
    pub v2: Option<InfoHashV2>,
}

impl InfoHashes {
    /// A v1-only torrent.
    pub fn from_v1(v1: InfoHashV1) -> Self {
        Self {
            v1: Some(v1),
            v2: None,
        }
    }

    /// A v2-only torrent.
    pub fn from_v2(v2: InfoHashV2) -> Self {
        Self {
            v1: None,
            v2: Some(v2),
        }
    }

    /// A hybrid torrent carrying both hashes.
    pub fn hybrid(v1: InfoHashV1, v2: InfoHashV2) -> Self {
        Self {
            v1: Some(v1),
            v2: Some(v2),
        }
    }

    /// The 20-byte hash used on the wire and in trackers: v1 if present,
    /// else the first 20 bytes of v2 (all zero if neither is set).
    pub fn wire_hash(&self) -> [u8; 20] {
        if let Some(v1) = self.v1 {
            return v1.0;
        }
        let mut out = [0u8; 20];
        if let Some(v2) = self.v2 {
            for (o, i) in out.iter_mut().zip(v2.0.iter()) {
                *o = *i;
            }
        }
        out
    }
}

impl fmt::Display for InfoHashes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.v1, self.v2) {
            (Some(v1), _) => write!(f, "{v1}"),
            (None, Some(v2)) => write!(f, "{v2}"),
            (None, None) => Ok(()),
        }
    }
}
