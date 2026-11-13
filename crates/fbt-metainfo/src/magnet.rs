//! Magnet URIs (BEP 9 `xt`, BEP 52 `btmh`, BEP 53 `so`). The input is untrusted.

use std::fmt::Write as _;
use std::net::SocketAddr;
use std::ops::RangeInclusive;
use std::str::FromStr;

use fbt_core::{InfoHashV1, InfoHashV2, InfoHashes};

use crate::metainfo::{MAX_WEB_SEEDS, clean_web_seeds};
use crate::path::sanitize_component;

/// Longest accepted magnet URI, in bytes.
pub const MAX_MAGNET_LEN: usize = 65_536;
/// Most `tr` trackers kept; later ones are ignored (the metainfo's
/// [`MAX_TRACKERS`](crate::MAX_TRACKERS)).
pub const MAX_MAGNET_TRACKERS: usize = crate::metainfo::MAX_TRACKERS;
/// Most `x.pe` peers kept; later ones are ignored.
pub const MAX_MAGNET_PEERS: usize = 200;
/// Most comma-separated entries accepted in `so`.
pub const MAX_SELECT_ENTRIES: usize = 1000;
/// Largest file index accepted in `so`.
pub const MAX_SELECT_INDEX: usize = 1_000_000;

/// Longest value echoed back in [`MagnetError::InvalidInfoHash`].
const MAX_ECHO_LEN: usize = 80;
const SCHEME: &str = "magnet:?";
const BTIH: &str = "urn:btih:";
const BTMH: &str = "urn:btmh:";
const SHA256_MULTIHASH_PREFIX: &str = "1220";

/// Errors from [`Magnet::parse`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MagnetError {
    /// The string does not start with `magnet:?`.
    #[error("not a magnet URI")]
    NotMagnet,
    /// The URI exceeds [`MAX_MAGNET_LEN`] bytes.
    #[error("magnet URI too long")]
    TooLong,
    /// A percent escape is invalid or a value is not UTF-8 where required.
    #[error("invalid percent-encoding")]
    BadEncoding,
    /// No `xt=urn:btih:` or `xt=urn:btmh:` parameter.
    #[error("magnet URI has no info hash")]
    MissingInfoHash,
    /// An `xt` value is malformed or of an unsupported kind.
    #[error("invalid info hash `{0}`")]
    InvalidInfoHash(String),
    /// Two different btih (or two different btmh) values.
    #[error("conflicting info hashes")]
    ConflictingInfoHash,
    /// A malformed `so` selection.
    #[error("invalid file selection")]
    InvalidSelection,
}

/// A parsed magnet link.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Magnet {
    /// The info hash(es) named by `xt`.
    pub info_hashes: InfoHashes,
    /// Sanitized `dn`.
    pub display_name: Option<String>,
    /// `tr` trackers, de-duplicated, in order.
    pub trackers: Vec<String>,
    /// `ws` web seeds: `http` and `https` URLs, de-duplicated (malformed values
    /// are dropped, not errors), at most
    /// [`MAX_WEB_SEEDS`](crate::MAX_WEB_SEEDS).
    pub web_seeds: Vec<String>,
    /// `x.pe` peers that are IP literals with a non-zero port.
    pub peers: Vec<SocketAddr>,
    /// BEP 53 `so` file selection; `None` selects every file.
    pub select_only: Option<Vec<RangeInclusive<usize>>>,
}

impl Magnet {
    /// A magnet carrying only the info hashes.
    pub fn from_info_hashes(ih: InfoHashes) -> Magnet {
        Magnet {
            info_hashes: ih,
            display_name: None,
            trackers: Vec::new(),
            web_seeds: Vec::new(),
            peers: Vec::new(),
            select_only: None,
        }
    }

    /// Parses a magnet URI.
    pub fn parse(uri: &str) -> Result<Magnet, MagnetError> {
        if uri.len() > MAX_MAGNET_LEN {
            return Err(MagnetError::TooLong);
        }
        let query = match uri.get(..SCHEME.len()) {
            Some(head) if head.eq_ignore_ascii_case(SCHEME) => {
                uri.get(SCHEME.len()..).unwrap_or("")
            }
            _ => return Err(MagnetError::NotMagnet),
        };

        let mut v1: Option<InfoHashV1> = None;
        let mut v2: Option<InfoHashV2> = None;
        let mut display_name = None;
        let mut trackers: Vec<String> = Vec::new();
        let mut web_seeds: Vec<String> = Vec::new();
        let mut peers: Vec<SocketAddr> = Vec::new();
        let mut select: Option<Vec<RangeInclusive<usize>>> = None;

        for param in query.split('&') {
            let (key, value) = param.split_once('=').unwrap_or((param, ""));
            match key {
                "xt" => {
                    let value = utf8(percent_decode(value, false)?)?;
                    parse_xt(&value, &mut v1, &mut v2)?;
                }
                "dn" => {
                    let raw = percent_decode(value, true)?;
                    if display_name.is_none() {
                        display_name = sanitize_component(&raw);
                    }
                }
                k if is_indexed(k, "tr") => {
                    let url = utf8(percent_decode(value, false)?)?;
                    if !url.is_empty()
                        && trackers.len() < MAX_MAGNET_TRACKERS
                        && !trackers.contains(&url)
                    {
                        trackers.push(url);
                    }
                }
                k if is_indexed(k, "ws") => {
                    // A web seed is an optional hint: a malformed value is
                    // dropped instead of failing the magnet (unlike `tr`).
                    if let Some(url) = percent_decode(value, false).ok().and_then(|b| utf8(b).ok())
                        && web_seeds.len() < MAX_WEB_SEEDS
                    {
                        for u in clean_web_seeds([url.as_str()]) {
                            if !web_seeds.contains(&u) {
                                web_seeds.push(u);
                            }
                        }
                    }
                }
                k if is_indexed(k, "x.pe") => {
                    let text = utf8(percent_decode(value, false)?)?;
                    if let Ok(addr) = text.parse::<SocketAddr>()
                        && addr.port() != 0
                        && peers.len() < MAX_MAGNET_PEERS
                        && !peers.contains(&addr)
                    {
                        peers.push(addr);
                    }
                }
                "so" => {
                    let text = utf8(percent_decode(value, false)?)?;
                    parse_select(&text, select.get_or_insert_with(Vec::new))?;
                }
                _ => {}
            }
        }

        if v1.is_none() && v2.is_none() {
            return Err(MagnetError::MissingInfoHash);
        }
        Ok(Magnet {
            info_hashes: InfoHashes { v1, v2 },
            display_name,
            trackers,
            web_seeds,
            peers,
            select_only: select,
        })
    }

    /// Renders the magnet as a URI that [`Magnet::parse`] reads back.
    pub fn to_uri(&self) -> String {
        let mut out = String::from(SCHEME);
        let mut first = true;
        let mut param = |out: &mut String, key: &str, value: &str| {
            if !first {
                out.push('&');
            }
            first = false;
            out.push_str(key);
            out.push('=');
            out.push_str(value);
        };
        if let Some(h) = self.info_hashes.v1 {
            param(&mut out, "xt", &format!("{BTIH}{h}"));
        }
        if let Some(h) = self.info_hashes.v2 {
            param(
                &mut out,
                "xt",
                &format!("{BTMH}{SHA256_MULTIHASH_PREFIX}{h}"),
            );
        }
        if let Some(name) = &self.display_name {
            param(&mut out, "dn", &percent_encode(name));
        }
        for t in &self.trackers {
            param(&mut out, "tr", &percent_encode(t));
        }
        for w in &self.web_seeds {
            param(&mut out, "ws", &percent_encode(w));
        }
        for p in &self.peers {
            param(&mut out, "x.pe", &percent_encode(&p.to_string()));
        }
        if let Some(sel) = &self.select_only {
            let parts: Vec<String> = sel
                .iter()
                .map(|r| {
                    if r.start() == r.end() {
                        r.start().to_string()
                    } else {
                        format!("{}-{}", r.start(), r.end())
                    }
                })
                .collect();
            param(&mut out, "so", &parts.join(","));
        }
        out
    }

    /// Whether file `file` is selected: always true without `so`.
    pub fn is_selected(&self, file: usize) -> bool {
        match &self.select_only {
            None => true,
            Some(ranges) => ranges.iter().any(|r| r.contains(&file)),
        }
    }
}

impl FromStr for Magnet {
    type Err = MagnetError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Magnet::parse(s)
    }
}

/// True for `base` or `base.N` with `N` all digits.
fn is_indexed(key: &str, base: &str) -> bool {
    match key.strip_prefix(base) {
        Some("") => true,
        Some(rest) => rest
            .strip_prefix('.')
            .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit())),
        None => false,
    }
}

fn utf8(bytes: Vec<u8>) -> Result<String, MagnetError> {
    String::from_utf8(bytes).map_err(|_| MagnetError::BadEncoding)
}

fn invalid_hash(value: &str) -> MagnetError {
    MagnetError::InvalidInfoHash(value.chars().take(MAX_ECHO_LEN).collect())
}

/// Strips an ASCII prefix case-insensitively.
fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let head = s.get(..prefix.len())?;
    if head.eq_ignore_ascii_case(prefix) {
        s.get(prefix.len()..)
    } else {
        None
    }
}

fn parse_xt(
    value: &str,
    v1: &mut Option<InfoHashV1>,
    v2: &mut Option<InfoHashV2>,
) -> Result<(), MagnetError> {
    if let Some(h) = strip_prefix_ci(value, BTIH) {
        let parsed = match h.len() {
            40 => h.parse::<InfoHashV1>().map_err(|_| invalid_hash(value))?,
            32 => InfoHashV1(base32_decode_20(h).ok_or_else(|| invalid_hash(value))?),
            _ => return Err(invalid_hash(value)),
        };
        if v1.is_some_and(|old| old != parsed) {
            return Err(MagnetError::ConflictingInfoHash);
        }
        *v1 = Some(parsed);
    } else if let Some(h) = strip_prefix_ci(value, BTMH) {
        let hex = strip_prefix_ci(h, SHA256_MULTIHASH_PREFIX).ok_or_else(|| invalid_hash(value))?;
        let parsed = hex.parse::<InfoHashV2>().map_err(|_| invalid_hash(value))?;
        if v2.is_some_and(|old| old != parsed) {
            return Err(MagnetError::ConflictingInfoHash);
        }
        *v2 = Some(parsed);
    }
    Ok(())
}

fn parse_index(s: &str) -> Result<usize, MagnetError> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(MagnetError::InvalidSelection);
    }
    match s.parse::<usize>() {
        Ok(n) if n <= MAX_SELECT_INDEX => Ok(n),
        _ => Err(MagnetError::InvalidSelection),
    }
}

fn parse_select(text: &str, out: &mut Vec<RangeInclusive<usize>>) -> Result<(), MagnetError> {
    for part in text.split(',') {
        if out.len() >= MAX_SELECT_ENTRIES {
            return Err(MagnetError::InvalidSelection);
        }
        let range = match part.split_once('-') {
            None => {
                let n = parse_index(part)?;
                n..=n
            }
            Some((a, b)) => {
                let (a, b) = (parse_index(a)?, parse_index(b)?);
                if b < a {
                    return Err(MagnetError::InvalidSelection);
                }
                a..=b
            }
        };
        out.push(range);
    }
    Ok(())
}

/// RFC 4648 base32 (either case, no padding) of exactly 20 bytes.
fn base32_decode_20(s: &str) -> Option<[u8; 20]> {
    if s.len() != 32 {
        return None;
    }
    let mut out = [0u8; 20];
    let mut slots = out.iter_mut();
    let mut acc: u32 = 0;
    let mut bits = 0u32;
    for c in s.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a',
            b'2'..=b'7' => c - b'2' + 26,
            _ => return None,
        };
        acc = (acc << 5) | u32::from(v);
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            *slots.next()? = u8::try_from((acc >> bits) & 0xff).ok()?;
            acc &= (1 << bits) - 1;
        }
    }
    Some(out)
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Percent-decodes `s`; with `plus_is_space`, `+` becomes a space.
fn percent_decode(s: &str, plus_is_space: bool) -> Result<Vec<u8>, MagnetError> {
    let mut out = Vec::with_capacity(s.len());
    let mut bytes = s.bytes();
    while let Some(b) = bytes.next() {
        match b {
            b'%' => {
                let hi = bytes.next().and_then(hex_val);
                let lo = bytes.next().and_then(hex_val);
                match (hi, lo) {
                    (Some(h), Some(l)) => out.push((h << 4) | l),
                    _ => return Err(MagnetError::BadEncoding),
                }
            }
            b'+' if plus_is_space => out.push(b' '),
            _ => out.push(b),
        }
    }
    Ok(out)
}

/// Percent-encodes everything but RFC 3986 unreserved characters.
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}
