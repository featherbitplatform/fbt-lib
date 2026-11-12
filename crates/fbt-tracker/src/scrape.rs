//! Scrape (BEP 48 over HTTP, BEP 15 action 2 over UDP): the scrape URL
//! rule, request building and untrusted-reply parsing.

use fbt_bencode::{Value, decode};

use crate::types::bounded_text;
use crate::urlencode::url_encode_bytes;
use crate::{MAX_SCRAPE_HASHES, ScrapeInfo, TrackerError};

/// UDP scrape action.
pub(crate) const ACTION_SCRAPE: u32 = 2;
const ACTION_ERROR: u32 = 3;
/// Bytes per torrent in a UDP scrape reply: seeders, completed, leechers.
const UDP_ENTRY: usize = 12;

/// The scrape URL of an HTTP announce URL: its last path segment must start
/// with `announce`, which becomes `scrape`; the query is kept and a
/// fragment dropped. Anything else is [`TrackerError::ScrapeUnsupported`].
pub(crate) fn scrape_url(announce: &str) -> Result<String, TrackerError> {
    let base = announce.split('#').next().unwrap_or(announce);
    let (path_part, query) = match base.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (base, None),
    };
    let after_scheme = path_part
        .split_once("://")
        .map(|(_, r)| r)
        .ok_or_else(|| TrackerError::InvalidUrl("missing scheme".to_owned()))?;
    // A path is required: the host alone has no segment to rewrite.
    if !after_scheme.contains('/') {
        return Err(TrackerError::ScrapeUnsupported);
    }
    let (dir, last) = path_part
        .rsplit_once('/')
        .ok_or(TrackerError::ScrapeUnsupported)?;
    let rest = last
        .strip_prefix("announce")
        .ok_or(TrackerError::ScrapeUnsupported)?;
    let mut out = format!("{dir}/scrape{rest}");
    if let Some(q) = query {
        out.push('?');
        out.push_str(q);
    }
    Ok(out)
}

/// `scrape` (a scrape URL) with one `info_hash` per hash appended.
pub(crate) fn with_hashes(scrape: &str, hashes: &[[u8; 20]]) -> String {
    let mut out = String::from(scrape);
    let mut sep = if scrape.contains('?') { '&' } else { '?' };
    for h in hashes {
        out.push(sep);
        out.push_str("info_hash=");
        out.push_str(&url_encode_bytes(h));
        sep = '&';
    }
    out
}

/// A scrape count: missing or negative reads 0, above `u32::MAX`
/// saturates.
fn count(v: Option<&Value<'_>>) -> u32 {
    v.and_then(Value::as_int)
        .map_or(0, |n| u32::try_from(n.max(0)).unwrap_or(u32::MAX))
}

/// Parses a bencoded HTTP scrape reply. Input is untrusted. Entries whose
/// key is not a 20-byte hash or whose value is not a dictionary are
/// skipped, a missing or negative count reads 0 and one above `u32::MAX`
/// saturates, and at most
/// [`MAX_SCRAPE_HASHES`] entries are kept.
#[doc(hidden)]
pub fn parse_http_scrape(body: &[u8]) -> Result<Vec<ScrapeInfo>, TrackerError> {
    parse_http_scrape_for(body, None)
}

/// [`parse_http_scrape`], keeping only the entries for `wanted` when given
/// (a tracker may answer with more torrents than were asked for).
pub(crate) fn parse_http_scrape_for(
    body: &[u8],
    wanted: Option<&[[u8; 20]]>,
) -> Result<Vec<ScrapeInfo>, TrackerError> {
    let root = decode(body).map_err(|_| TrackerError::InvalidResponse("not bencode"))?;
    let dict = root.as_dict().ok_or(TrackerError::InvalidResponse(
        "response is not a dictionary",
    ))?;
    if let Some(reason) = dict.get(b"failure reason") {
        let text = reason.as_bytes().unwrap_or_default();
        return Err(TrackerError::Failure(bounded_text(text)));
    }
    let files = dict
        .get(b"files")
        .and_then(Value::as_dict)
        .ok_or(TrackerError::InvalidResponse("missing files"))?;
    let mut out = Vec::new();
    for (key, value) in files.iter() {
        if out.len() >= MAX_SCRAPE_HASHES {
            break;
        }
        let Ok(info_hash) = <[u8; 20]>::try_from(key) else {
            continue;
        };
        if wanted.is_some_and(|w| !w.contains(&info_hash)) {
            continue;
        }
        let Some(entry) = value.as_dict() else {
            continue;
        };
        out.push(ScrapeInfo {
            info_hash,
            complete: count(entry.get(b"complete")),
            downloaded: count(entry.get(b"downloaded")),
            incomplete: count(entry.get(b"incomplete")),
        });
    }
    Ok(out)
}

/// A UDP scrape request for `hashes` (at most [`MAX_SCRAPE_HASHES`]; the
/// caller checks).
pub(crate) fn build_udp_scrape(cid: u64, tid: u32, hashes: &[[u8; 20]]) -> Vec<u8> {
    let mut b = Vec::with_capacity(16 + 20 * hashes.len());
    b.extend_from_slice(&cid.to_be_bytes());
    b.extend_from_slice(&ACTION_SCRAPE.to_be_bytes());
    b.extend_from_slice(&tid.to_be_bytes());
    for h in hashes {
        b.extend_from_slice(h);
    }
    b
}

fn be_u32(buf: &[u8], off: usize) -> Option<u32> {
    let b = buf.get(off..off.checked_add(4)?)?;
    Some(u32::from_be_bytes(<[u8; 4]>::try_from(b).ok()?))
}

/// Parses a UDP scrape reply to a request for `n` hashes. Input is
/// untrusted. Returns `(complete, downloaded, incomplete)` per hash in
/// request order (BEP 15 sends seeders, completed, leechers): at most `n`,
/// fewer if the reply is short. A reply that is not a whole number of
/// entries is an error.
#[doc(hidden)]
pub fn parse_udp_scrape(
    buf: &[u8],
    tid: u32,
    n: usize,
) -> Result<Vec<(u32, u32, u32)>, TrackerError> {
    let (Some(action), Some(got)) = (be_u32(buf, 0), be_u32(buf, 4)) else {
        return Err(TrackerError::InvalidResponse("short datagram"));
    };
    if got != tid {
        return Err(TrackerError::InvalidResponse("transaction id mismatch"));
    }
    if action == ACTION_ERROR {
        let msg = buf.get(8..).unwrap_or_default();
        return Err(TrackerError::Failure(bounded_text(msg)));
    }
    if action != ACTION_SCRAPE {
        return Err(TrackerError::InvalidResponse("unexpected action"));
    }
    let body = buf.get(8..).unwrap_or_default();
    let (chunks, rest) = body.as_chunks::<UDP_ENTRY>();
    if !rest.is_empty() {
        return Err(TrackerError::InvalidResponse("misaligned scrape reply"));
    }
    Ok(chunks
        .iter()
        .take(n.min(MAX_SCRAPE_HASHES))
        .map(|c| {
            let [s0, s1, s2, s3, d0, d1, d2, d3, l0, l1, l2, l3] = *c;
            (
                u32::from_be_bytes([s0, s1, s2, s3]),
                u32::from_be_bytes([d0, d1, d2, d3]),
                u32::from_be_bytes([l0, l1, l2, l3]),
            )
        })
        .collect())
}
