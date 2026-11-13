//! Resume data and session state.
//!
//! [`TorrentHandle::save_resume_data`](crate::TorrentHandle::save_resume_data)
//! returns a torrent's state as one bencoded dictionary;
//! [`AddTorrent::from_resume`](crate::AddTorrent::from_resume) reads it
//! back. Resume data comes from disk and is hostile: it is size-capped
//! before decoding, decoded with item and depth limits, and every field is
//! checked (a failure is a [`ResumeError`] naming the field, never a panic).
//! A [`SessionState`] bundles the DHT state and every torrent's resume data.
//!
//! The dictionary (keys in bencode order; `fbt-resume-version`,
//! `save-path` and at least one of `info-hash` and `info-hash2` are
//! required):
//!
//! | key | value |
//! |---|---|
//! | `fbt-resume-version` | 2 (version 1, v1 hashes only, is still read) |
//! | `info-hash` | the v1 info hash, 20 bytes; absent for a v2-only torrent |
//! | `info-hash2` | the v2 info hash, 32 bytes; absent for a v1-only torrent |
//! | `save-path` | the directory the files are below |
//! | `info` | the raw `info` dictionary; its SHA-1 must be `info-hash` and its SHA-256 `info-hash2` (each one present) |
//! | `piece-layers` | with `info`: `pieces root` → the file's concatenated layer hashes, every layer the torrent holds (fetched or built ones included) |
//! | `magnet` | without `info`: the magnet URI; its `btih` must be `info-hash` and its `btmh` `info-hash2` (each one present) |
//! | `file-priorities` | one per file: 0 skip, 1 low, 4 normal, 7 high |
//! | `renamed` | `[index, [component, ...]]` per renamed file |
//! | `pieces` | the verified pieces, a bitfield |
//! | `paused`, `auto-managed`, `sequential` | 0 or 1 |
//! | `queue-position` | the place in the session's queue |
//! | `download-limit`, `upload-limit` | bytes per second, 0 unlimited |
//! | `total-uploaded`, `total-downloaded` | payload bytes |
//! | `active-time`, `seeding-time` | seconds |
//! | `added-time`, `completed-time` | seconds since the Unix epoch |
//! | `trackers` | tiers of announce URLs |
//! | `url-list` | the web seeds, retired ones included |
//! | `peers`, `peers6` | compact addresses, at most 200 each |
//! | `stamps` | per file then the part file: `[len, mtime]`, or `[]` for none |
//!
//! Each piece layer is checked on load against its file
//! ([`Metainfo::with_piece_layers`]); one that does not match is dropped,
//! not fatal: a v2-only torrent fetches it from peers again, a hybrid
//! builds it from its verified data. Layers that would push the data over
//! [`MAX_RESUME_BYTES`] are left out on save the same way. A hybrid whose
//! layers were neither in its metadata nor saved (resume data of version
//! 1, or left out) gets them only from data it verifies: a trusted resume
//! verifies nothing, so it lacks them until its next check (or until it
//! downloads a piece of the file).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use fbt_bencode::{DecodeError, Dict, Limits, OwnedValue, Value, decode_with, encode_into};
use fbt_core::compact::{decode_v4_list, decode_v6_list};
use fbt_core::{Bitfield, InfoHashV1, InfoHashV2, compact};
use fbt_metainfo::{
    FileEntry, MAX_PIECE_LAYER_HASHES, MAX_TORRENT_DEPTH, MAX_TORRENT_ITEMS, MAX_WEB_SEEDS, Magnet,
    MagnetError, Metainfo, MetainfoError, RelPath, sanitize_path,
};

use crate::bandwidth::RateLimit;
use crate::session::{Priority, TorrentSource};
use crate::storage::FileStamp;

/// Largest resume data accepted; longer input is refused before decoding.
/// The `.torrent` parser has no byte cap, so this one can be reached by a
/// valid torrent (an `info` over 64 MiB: more than about 3.3 million
/// piece hashes); [`TorrentHandle::save_resume_data`](crate::TorrentHandle::save_resume_data)
/// then fails with [`ResumeError::TooLarge`] rather than return data that
/// would not load.
pub const MAX_RESUME_BYTES: usize = 64 * 1024 * 1024;
/// Most bencode values in resume data: twice what
/// [`Metainfo::from_bytes`] accepts in a whole `.torrent`
/// ([`MAX_TORRENT_ITEMS`]). The raw `info` takes at most one share; every
/// file costs at least six values there and adds four here (its priority
/// and its stamp), so the per-file fields take at most two thirds of the
/// other share, and the rest (about 1.4 million values) is left for the
/// renamed paths and the fixed fields. A piece layer adds two values (its
/// root and its hashes) for a file with a `pieces root`, which costs at
/// least eight values in the `info` (more in a hybrid's), so the per-file
/// fields still take at most three quarters of the other share. Resume
/// data a torrent saves never exceeds it unless its renamed paths hold
/// more components than that;
/// [`TorrentHandle::save_resume_data`](crate::TorrentHandle::save_resume_data)
/// checks its own output and fails rather than return data that would not
/// load.
pub const MAX_RESUME_ITEMS: usize = 2 * MAX_TORRENT_ITEMS;
/// Deepest nesting of resume data: that of a `.torrent`
/// ([`MAX_TORRENT_DEPTH`]). The raw `info` sits at the same depth as in a
/// `.torrent` (below the root dictionary), so any `info` the parser
/// accepted fits (a hybrid torrent's `file tree` nests one level per
/// directory); the other fields nest at most four deep.
const MAX_RESUME_DEPTH: usize = MAX_TORRENT_DEPTH;
/// Most peers kept per address family (`peers`, `peers6`); later ones are
/// dropped, on save and on load.
pub const MAX_RESUME_PEERS: usize = 200;
/// Most torrents a [`SessionState`] holds.
pub const MAX_SESSION_TORRENTS: usize = 100_000;
/// The resume data format written; it and every earlier one are read.
const RESUME_VERSION: i64 = 2;
/// The session state format written and read.
const SESSION_VERSION: i64 = 1;

/// Why resume data (or a [`SessionState`]) was refused. Each variant names
/// what was wrong with it.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ResumeError {
    /// Not bencode, or over the item or depth limits.
    #[error("resume data is not valid bencode: {0}")]
    Bencode(#[source] DecodeError),
    /// Longer than [`MAX_RESUME_BYTES`].
    #[error("resume data is larger than 64 MiB")]
    TooLarge,
    /// A format version this library does not read.
    #[error("unsupported resume data version {0}")]
    Version(i64),
    /// A required field is missing.
    #[error("resume data has no `{0}`")]
    Missing(&'static str),
    /// A field has the wrong type or an impossible value (the wrong length
    /// for the torrent, an unsafe or colliding renamed path, ...).
    #[error("invalid `{0}` in resume data")]
    Invalid(&'static str),
    /// The `info` dictionary (or the magnet) is not that of `info-hash`
    /// or `info-hash2`.
    #[error("the resume data's torrent does not match its info hash")]
    InfoHashMismatch,
    /// The `info` dictionary does not parse.
    #[error("invalid `info` in resume data: {0}")]
    Metainfo(#[source] MetainfoError),
    /// The magnet URI does not parse.
    #[error("invalid `magnet` in resume data: {0}")]
    Magnet(#[source] MagnetError),
}

/// What resume data carries beyond [`AddTorrent`](crate::AddTorrent)'s
/// public fields.
#[derive(Debug, Clone)]
pub(crate) struct ResumeState {
    /// The verified pieces (no bits for a magnet without metadata).
    pub have: Bitfield,
    /// Per file then the part file; empty when the storage could not say
    /// or the pieces were not all checked and on disk at the save.
    pub stamps: Vec<Option<FileStamp>>,
    /// Files with a new path, relative to the save path; sanitised and
    /// collision-free.
    pub renamed: Vec<(usize, RelPath)>,
    /// Every web seed, retired ones included.
    pub web_seeds: Vec<String>,
    pub queue_position: Option<usize>,
    pub uploaded: u64,
    pub downloaded: u64,
    pub active_time: Duration,
    pub seeding_time: Duration,
    /// Seconds since the Unix epoch.
    pub added: i64,
    pub completed: Option<i64>,
    pub peers: Vec<SocketAddr>,
}

/// Decoded and checked resume data. Internal (the fuzz target and
/// [`AddTorrent::from_resume`](crate::AddTorrent::from_resume) use it); not
/// part of the stable API.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct DecodedResume {
    pub(crate) source: TorrentSource,
    pub(crate) save_path: PathBuf,
    pub(crate) paused: bool,
    pub(crate) auto_managed: bool,
    pub(crate) sequential: bool,
    pub(crate) file_priorities: Vec<Priority>,
    pub(crate) rate_limit: RateLimit,
    pub(crate) state: ResumeState,
}

impl DecodedResume {
    /// The peers kept, at most [`MAX_RESUME_PEERS`] per family.
    #[doc(hidden)]
    pub fn peers(&self) -> &[SocketAddr] {
        &self.state.peers
    }
}

/// Decodes and checks resume data (see the module docs). Internal; use
/// [`AddTorrent::from_resume`](crate::AddTorrent::from_resume).
#[doc(hidden)]
pub fn decode_resume(bytes: &[u8]) -> Result<DecodedResume, ResumeError> {
    if bytes.len() > MAX_RESUME_BYTES {
        return Err(ResumeError::TooLarge);
    }
    let root = decode_with(bytes, Limits::new(MAX_RESUME_DEPTH, MAX_RESUME_ITEMS))
        .map_err(ResumeError::Bencode)?;
    let d = root.as_dict().ok_or(ResumeError::Invalid("resume"))?;
    let version =
        int(d, "fbt-resume-version")?.ok_or(ResumeError::Missing("fbt-resume-version"))?;
    if !(1..=RESUME_VERSION).contains(&version) {
        return Err(ResumeError::Version(version));
    }
    let ih = hash::<20>(d, "info-hash")?;
    // Version 1 knew v1 hashes only.
    let ih2 = if version >= 2 {
        hash::<32>(d, "info-hash2")?
    } else {
        None
    };
    if ih.is_none() && ih2.is_none() {
        return Err(ResumeError::Missing("info-hash"));
    }
    let save_path = path_from_bytes(
        d.get(b"save-path")
            .ok_or(ResumeError::Missing("save-path"))?
            .as_bytes()
            .ok_or(ResumeError::Invalid("save-path"))?,
    )?;
    let trackers = trackers(d)?;
    let web_seeds = strings(d, "url-list")?
        .into_iter()
        .take(MAX_WEB_SEEDS)
        .collect();
    let (source, meta) = match (d.get(b"info"), d.get(b"magnet")) {
        (Some(info), _) => {
            let raw = info.raw();
            if ih.is_some_and(|h| InfoHashV1::of(raw).0 != h)
                || ih2.is_some_and(|h| InfoHashV2::of(raw).0 != h)
            {
                return Err(ResumeError::InfoHashMismatch);
            }
            let mut m = parse_info(raw, &trackers)?;
            if version >= 2 {
                let layers = piece_layers(d, &m)?;
                m = m.with_piece_layers(layers);
            }
            let source = TorrentSource::Metainfo(Box::new(m.clone()));
            (source, Some(m))
        }
        (None, Some(magnet)) => {
            let uri = magnet.as_str().ok_or(ResumeError::Invalid("magnet"))?;
            let mut m = Magnet::parse(uri).map_err(ResumeError::Magnet)?;
            if ih.is_some_and(|h| m.info_hashes.v1.map(|v| v.0) != Some(h))
                || ih2.is_some_and(|h| m.info_hashes.v2.map(|v| v.0) != Some(h))
            {
                return Err(ResumeError::InfoHashMismatch);
            }
            // `url-list` is the current set of web seeds.
            m.web_seeds.clear();
            (TorrentSource::Magnet(m), None)
        }
        (None, None) => return Err(ResumeError::Missing("info")),
    };
    let num_files = meta.as_ref().map_or(0, |m| m.info.files.len());
    let num_pieces = meta.as_ref().map_or(0, |m| m.piece_layout().num_pieces());
    let file_priorities = priorities(d, num_files)?;
    let renamed = renamed(d, meta.as_ref())?;
    let have = match d.get(b"pieces") {
        Some(v) => {
            let b = v.as_bytes().ok_or(ResumeError::Invalid("pieces"))?;
            Bitfield::from_bytes(b, num_pieces).map_err(|_| ResumeError::Invalid("pieces"))?
        }
        None => Bitfield::new(num_pieces),
    };
    let stamps = stamps(d, num_files)?;
    let mut peers = compact_list(d, "peers", false)?;
    peers.extend(compact_list(d, "peers6", true)?);
    let secs = |key| Ok::<_, ResumeError>(Duration::from_secs(non_neg(d, key)?.unwrap_or(0)));
    let state = ResumeState {
        have,
        stamps,
        renamed,
        web_seeds,
        queue_position: non_neg(d, "queue-position")?
            .map(|p| usize::try_from(p).map_err(|_| ResumeError::Invalid("queue-position")))
            .transpose()?,
        uploaded: non_neg(d, "total-uploaded")?.unwrap_or(0),
        downloaded: non_neg(d, "total-downloaded")?.unwrap_or(0),
        active_time: secs("active-time")?,
        seeding_time: secs("seeding-time")?,
        added: int(d, "added-time")?.unwrap_or(0),
        completed: int(d, "completed-time")?,
        peers,
    };
    Ok(DecodedResume {
        source,
        save_path,
        paused: flag(d, "paused")?.unwrap_or(false),
        auto_managed: flag(d, "auto-managed")?.unwrap_or(true),
        sequential: flag(d, "sequential")?.unwrap_or(false),
        file_priorities,
        rate_limit: RateLimit::new(
            non_neg(d, "download-limit")?.unwrap_or(0),
            non_neg(d, "upload-limit")?.unwrap_or(0),
        ),
        state,
    })
}

/// The torrent of a raw `info` dictionary (already checked against the
/// info hash) and its tracker tiers, parsed as a `.torrent` holding just
/// them: the same parser and limits as the torrent was added with (a
/// hybrid torrent's v2 hash included), so whatever was added can be
/// resumed.
fn parse_info(raw: &[u8], trackers: &[Vec<String>]) -> Result<Metainfo, ResumeError> {
    let mut torrent = Vec::with_capacity(raw.len().saturating_add(64));
    torrent.push(b'd');
    if !trackers.is_empty() {
        encode_into(&OwnedValue::from("announce-list"), &mut torrent);
        let tiers = trackers
            .iter()
            .map(|t| OwnedValue::List(t.iter().map(|u| OwnedValue::from(u.as_str())).collect()))
            .collect();
        encode_into(&OwnedValue::List(tiers), &mut torrent);
    }
    encode_into(&OwnedValue::from("info"), &mut torrent);
    torrent.extend_from_slice(raw);
    torrent.push(b'e');
    Metainfo::from_bytes(&torrent).map_err(ResumeError::Metainfo)
}

/// An info hash field of `N` bytes; `Invalid` if it is not one.
fn hash<const N: usize>(d: &Dict<'_>, key: &'static str) -> Result<Option<[u8; N]>, ResumeError> {
    d.get(key.as_bytes())
        .map(|v| {
            v.as_bytes()
                .and_then(|b| b.try_into().ok())
                .ok_or(ResumeError::Invalid(key))
        })
        .transpose()
}

/// The saved piece layers of `meta`'s files, not yet checked against them
/// ([`Metainfo::with_piece_layers`] does). An entry that cannot be a layer
/// of one of its files (a key that is no file's `pieces root`, a value
/// that is not whole hashes) is dropped, as is any past
/// [`MAX_PIECE_LAYER_HASHES`] hashes in all: counted before anything is
/// copied.
fn piece_layers(
    d: &Dict<'_>,
    meta: &Metainfo,
) -> Result<BTreeMap<[u8; 32], Vec<[u8; 32]>>, ResumeError> {
    let mut out = BTreeMap::new();
    let Some(v) = d.get(b"piece-layers") else {
        return Ok(out);
    };
    let layers = v.as_dict().ok_or(ResumeError::Invalid("piece-layers"))?;
    let roots: HashSet<[u8; 32]> = meta
        .info
        .files
        .iter()
        .filter_map(|f| f.pieces_root)
        .collect();
    let mut total: usize = 0;
    for (key, value) in layers.iter() {
        let Ok(root) = <[u8; 32]>::try_from(key) else {
            continue;
        };
        let Some(raw) = value.as_bytes() else {
            continue;
        };
        let (hashes, rest) = raw.as_chunks::<32>();
        if !roots.contains(&root) || !rest.is_empty() || hashes.is_empty() {
            continue;
        }
        if total.saturating_add(hashes.len()) > MAX_PIECE_LAYER_HASHES {
            continue;
        }
        total += hashes.len();
        out.insert(root, hashes.to_vec());
    }
    Ok(out)
}

/// An integer field; `Invalid` if it is not one.
fn int(d: &Dict<'_>, key: &'static str) -> Result<Option<i64>, ResumeError> {
    d.get(key.as_bytes())
        .map(|v| v.as_int().ok_or(ResumeError::Invalid(key)))
        .transpose()
}

/// A non-negative integer field.
fn non_neg(d: &Dict<'_>, key: &'static str) -> Result<Option<u64>, ResumeError> {
    int(d, key)?
        .map(|i| u64::try_from(i).map_err(|_| ResumeError::Invalid(key)))
        .transpose()
}

/// A 0 or 1 field.
fn flag(d: &Dict<'_>, key: &'static str) -> Result<Option<bool>, ResumeError> {
    int(d, key)?
        .map(|i| match i {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ResumeError::Invalid(key)),
        })
        .transpose()
}

/// A list field; empty when absent.
fn list<'a, 'b>(d: &'b Dict<'a>, key: &'static str) -> Result<&'b [Value<'a>], ResumeError> {
    match d.get(key.as_bytes()) {
        Some(v) => v.as_list().ok_or(ResumeError::Invalid(key)),
        None => Ok(&[]),
    }
}

/// A list of UTF-8 strings.
fn strings(d: &Dict<'_>, key: &'static str) -> Result<Vec<String>, ResumeError> {
    list(d, key)?
        .iter()
        .map(|v| {
            v.as_str()
                .map(str::to_owned)
                .ok_or(ResumeError::Invalid(key))
        })
        .collect()
}

fn trackers(d: &Dict<'_>) -> Result<Vec<Vec<String>>, ResumeError> {
    let bad = || ResumeError::Invalid("trackers");
    list(d, "trackers")?
        .iter()
        .map(|tier| {
            tier.as_list()
                .ok_or_else(bad)?
                .iter()
                .map(|u| u.as_str().map(str::to_owned).ok_or_else(bad))
                .collect()
        })
        .collect()
}

fn priorities(d: &Dict<'_>, num_files: usize) -> Result<Vec<Priority>, ResumeError> {
    let items = list(d, "file-priorities")?;
    if !items.is_empty() && items.len() != num_files {
        return Err(ResumeError::Invalid("file-priorities"));
    }
    items
        .iter()
        .map(|v| match v.as_int() {
            Some(0) => Ok(Priority::Skip),
            Some(1..=3) => Ok(Priority::Low),
            Some(4..=5) => Ok(Priority::Normal),
            Some(6..=7) => Ok(Priority::High),
            _ => Err(ResumeError::Invalid("file-priorities")),
        })
        .collect()
}

/// The renamed files: each index in range and not a pad file, each index
/// once, each path one that `rename_file` would accept (sanitising leaves
/// it unchanged, it collides with no other file, and it is not the part
/// file's path nor below it).
fn renamed(d: &Dict<'_>, meta: Option<&Metainfo>) -> Result<Vec<(usize, RelPath)>, ResumeError> {
    let bad = || ResumeError::Invalid("renamed");
    let items = list(d, "renamed")?;
    if items.is_empty() {
        return Ok(Vec::new());
    }
    let files = &meta.ok_or_else(bad)?.info.files;
    if items.len() > files.len() {
        return Err(bad());
    }
    let mut seen = HashSet::with_capacity(items.len());
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let [index, path] = item.as_list().ok_or_else(bad)? else {
            return Err(bad());
        };
        let index = index
            .as_int()
            .and_then(|i| usize::try_from(i).ok())
            .ok_or_else(bad)?;
        if files.get(index).is_none_or(|f| f.pad) || !seen.insert(index) {
            return Err(bad());
        }
        let raw: Vec<&[u8]> = path
            .as_list()
            .ok_or_else(bad)?
            .iter()
            .map(|c| c.as_bytes().ok_or_else(bad))
            .collect::<Result<_, _>>()?;
        let rel = sanitize_path(raw.iter().copied()).map_err(|_| bad())?;
        if rel
            .components()
            .iter()
            .map(String::as_bytes)
            .ne(raw.iter().copied())
        {
            return Err(bad());
        }
        out.push((index, rel));
    }
    let part = crate::storage::part_file_name(meta.ok_or_else(bad)?);
    if renames_collide(files, &out)
        || out
            .iter()
            .any(|(_, p)| crate::storage::is_part_path(p, &part))
    {
        return Err(bad());
    }
    Ok(out)
}

/// Whether any of `renamed`, applied to `files`, collides with another
/// file: what [`fbt_metainfo::collides`] says of each renamed file against
/// the files with every rename applied (equal keys, or one key a directory
/// of the other). The files as parsed never collide (the metainfo parser
/// renames them apart), so any collision in the whole set involves a
/// renamed file, and the set is checked at once with a trie of the
/// component keys: linear in the paths' length, where `collides` per
/// renamed file would cost renamed × files key computations on hostile
/// input.
fn renames_collide(files: &[FileEntry], renamed: &[(usize, RelPath)]) -> bool {
    let renamed_at: HashMap<usize, &RelPath> = renamed.iter().map(|(i, p)| (*i, p)).collect();
    // Node 0 is the root; a node's children are keyed by (node, component).
    let mut children: HashMap<(usize, String), usize> = HashMap::new();
    // Per node: a file ends there; some path goes on below it.
    let mut file_at = vec![false];
    let mut has_child = vec![false];
    for (i, f) in files.iter().enumerate().filter(|(_, f)| !f.pad) {
        let key = renamed_at.get(&i).map_or_else(|| f.path.key(), |p| p.key());
        let mut node = 0usize;
        for component in key.split('/') {
            if file_at.get(node).copied().unwrap_or(false) {
                // A file where this path needs a directory.
                return true;
            }
            if let Some(c) = has_child.get_mut(node) {
                *c = true;
            }
            let next = file_at.len();
            node = *children.entry((node, component.to_owned())).or_insert(next);
            if node == next {
                file_at.push(false);
                has_child.push(false);
            }
        }
        if file_at.get(node).copied().unwrap_or(false)
            || has_child.get(node).copied().unwrap_or(false)
        {
            // The same path twice, or a file where another needs a directory.
            return true;
        }
        if let Some(e) = file_at.get_mut(node) {
            *e = true;
        }
    }
    false
}

/// The stamps: none, or one per file and one for the part file.
fn stamps(d: &Dict<'_>, num_files: usize) -> Result<Vec<Option<FileStamp>>, ResumeError> {
    let bad = || ResumeError::Invalid("stamps");
    let items = list(d, "stamps")?;
    if !items.is_empty() && items.len() != num_files.saturating_add(1) {
        return Err(bad());
    }
    items
        .iter()
        .map(|v| match v.as_list().ok_or_else(bad)? {
            [] => Ok(None),
            [len, mtime] => {
                let len = len
                    .as_int()
                    .and_then(|l| u64::try_from(l).ok())
                    .ok_or_else(bad)?;
                let mtime = mtime.as_int().ok_or_else(bad)?;
                Ok(Some(FileStamp::new(len, mtime)))
            }
            _ => Err(bad()),
        })
        .collect()
}

/// Compact peers of one family, cut to [`MAX_RESUME_PEERS`].
fn compact_list(d: &Dict<'_>, key: &'static str, v6: bool) -> Result<Vec<SocketAddr>, ResumeError> {
    let Some(v) = d.get(key.as_bytes()) else {
        return Ok(Vec::new());
    };
    let b = v.as_bytes().ok_or(ResumeError::Invalid(key))?;
    let r = if v6 {
        decode_v6_list(b, MAX_RESUME_PEERS)
    } else {
        decode_v4_list(b, MAX_RESUME_PEERS)
    };
    r.map_err(|_| ResumeError::Invalid(key))
}

#[cfg(unix)]
fn path_from_bytes(b: &[u8]) -> Result<PathBuf, ResumeError> {
    use std::os::unix::ffi::OsStrExt;
    Ok(PathBuf::from(std::ffi::OsStr::from_bytes(b)))
}

#[cfg(not(unix))]
fn path_from_bytes(b: &[u8]) -> Result<PathBuf, ResumeError> {
    std::str::from_utf8(b)
        .map(PathBuf::from)
        .map_err(|_| ResumeError::Invalid("save-path"))
}

/// The save path's bytes: raw on Unix, UTF-8 elsewhere (a Windows path that
/// is not valid UTF-16 is saved lossily).
#[cfg(unix)]
fn path_bytes(p: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    p.as_os_str().as_bytes().to_vec()
}

#[cfg(not(unix))]
fn path_bytes(p: &Path) -> Vec<u8> {
    p.to_string_lossy().into_owned().into_bytes()
}

/// Everything resume data is written from.
#[derive(Debug)]
pub(crate) struct ResumeRecord<'a> {
    /// The v1 info hash; `None` for a v2-only torrent.
    pub info_hash: Option<[u8; 20]>,
    /// The v2 info hash; `None` for a v1-only torrent.
    pub info_hash2: Option<[u8; 32]>,
    pub save_path: &'a Path,
    /// The raw `info` dictionary, once the metadata is known.
    pub info: Option<&'a [u8]>,
    /// The piece layers held (written with `info` only).
    pub piece_layers: Option<&'a BTreeMap<[u8; 32], Vec<[u8; 32]>>>,
    /// Without metadata: the magnet URI.
    pub magnet: Option<String>,
    pub trackers: &'a [Vec<String>],
    pub file_priorities: &'a [Priority],
    pub paused: bool,
    pub auto_managed: bool,
    pub sequential: bool,
    pub rate_limit: RateLimit,
    pub state: &'a ResumeState,
}

/// One value of the resume dictionary: encoded, or raw bytes spliced in
/// as they are (the `info` dictionary, whose hash must not change).
enum Field<'a> {
    Encoded(Vec<u8>),
    Raw(&'a [u8]),
}

impl Field<'_> {
    fn bytes(&self) -> &[u8] {
        match self {
            Field::Encoded(b) => b,
            Field::Raw(b) => b,
        }
    }
}

/// The length of the bencoded byte string of `n` bytes.
fn bytes_len(n: usize) -> usize {
    n.to_string().len() + 1 + n
}

/// The `piece-layers` dictionary, encoded: every layer of `layers` that
/// keeps it within `budget` bytes, in root order (the others are left
/// out). `None` when none fits.
fn encode_layers(layers: &BTreeMap<[u8; 32], Vec<[u8; 32]>>, budget: usize) -> Option<Vec<u8>> {
    // `d`, `e`.
    let mut size = 2usize;
    let mut kept = Vec::new();
    for (root, layer) in layers {
        let entry = bytes_len(32) + bytes_len(layer.len().saturating_mul(32));
        if size.saturating_add(entry) <= budget {
            size += entry;
            kept.push((root, layer));
        }
    }
    if kept.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(size);
    out.push(b'd');
    for (root, layer) in kept {
        out.extend_from_slice(b"32:");
        out.extend_from_slice(root);
        out.extend_from_slice((layer.len() * 32).to_string().as_bytes());
        out.push(b':');
        for h in layer {
            out.extend_from_slice(h);
        }
    }
    out.push(b'e');
    Some(out)
}

fn int_value(n: u64) -> OwnedValue {
    OwnedValue::Int(i64::try_from(n).unwrap_or(i64::MAX))
}

fn bool_value(b: bool) -> OwnedValue {
    OwnedValue::Int(i64::from(b))
}

/// Encodes resume data (see the module docs).
pub(crate) fn encode(r: &ResumeRecord<'_>) -> Vec<u8> {
    let s = r.state;
    let mut f: BTreeMap<&'static [u8], Field<'_>> = BTreeMap::new();
    let mut put = |k: &'static str, v: OwnedValue| {
        let mut b = Vec::new();
        encode_into(&v, &mut b);
        f.insert(k.as_bytes(), Field::Encoded(b));
    };
    put("fbt-resume-version", OwnedValue::Int(RESUME_VERSION));
    if let Some(h) = r.info_hash {
        put("info-hash", OwnedValue::Bytes(h.to_vec()));
    }
    if let Some(h) = r.info_hash2 {
        put("info-hash2", OwnedValue::Bytes(h.to_vec()));
    }
    put("save-path", OwnedValue::Bytes(path_bytes(r.save_path)));
    if let Some(m) = &r.magnet {
        put("magnet", OwnedValue::from(m.as_str()));
    }
    put(
        "file-priorities",
        OwnedValue::List(
            r.file_priorities
                .iter()
                .map(|p| OwnedValue::Int(i64::from(p.piece_priority())))
                .collect(),
        ),
    );
    if !s.renamed.is_empty() {
        put(
            "renamed",
            OwnedValue::List(
                s.renamed
                    .iter()
                    .map(|(i, p)| {
                        OwnedValue::List(vec![
                            int_value(*i as u64),
                            OwnedValue::List(
                                p.components()
                                    .iter()
                                    .map(|c| OwnedValue::from(c.as_str()))
                                    .collect(),
                            ),
                        ])
                    })
                    .collect(),
            ),
        );
    }
    if r.info.is_some() {
        put("pieces", OwnedValue::Bytes(s.have.as_bytes().to_vec()));
    }
    put("paused", bool_value(r.paused));
    put("auto-managed", bool_value(r.auto_managed));
    put("sequential", bool_value(r.sequential));
    if let Some(p) = s.queue_position {
        put("queue-position", int_value(p as u64));
    }
    put("download-limit", int_value(r.rate_limit.download));
    put("upload-limit", int_value(r.rate_limit.upload));
    put("total-uploaded", int_value(s.uploaded));
    put("total-downloaded", int_value(s.downloaded));
    put("active-time", int_value(s.active_time.as_secs()));
    put("seeding-time", int_value(s.seeding_time.as_secs()));
    put("added-time", OwnedValue::Int(s.added));
    if let Some(c) = s.completed {
        put("completed-time", OwnedValue::Int(c));
    }
    put(
        "trackers",
        OwnedValue::List(
            r.trackers
                .iter()
                .map(|t| OwnedValue::List(t.iter().map(|u| OwnedValue::from(u.as_str())).collect()))
                .collect(),
        ),
    );
    put(
        "url-list",
        OwnedValue::List(
            s.web_seeds
                .iter()
                .map(|u| OwnedValue::from(u.as_str()))
                .collect(),
        ),
    );
    let (mut v4, mut v6) = (Vec::new(), Vec::new());
    let (mut n4, mut n6) = (0usize, 0usize);
    for a in &s.peers {
        let (out, n) = if a.is_ipv4() {
            (&mut v4, &mut n4)
        } else {
            (&mut v6, &mut n6)
        };
        if *n < MAX_RESUME_PEERS {
            compact::encode(*a, out);
            *n += 1;
        }
    }
    put("peers", OwnedValue::Bytes(v4));
    put("peers6", OwnedValue::Bytes(v6));
    put(
        "stamps",
        OwnedValue::List(
            s.stamps
                .iter()
                .map(|st| match st {
                    Some(st) => {
                        OwnedValue::List(vec![int_value(st.len), OwnedValue::Int(st.mtime)])
                    }
                    None => OwnedValue::List(Vec::new()),
                })
                .collect(),
        ),
    );
    if let Some(info) = r.info {
        f.insert(b"info", Field::Raw(info));
    }
    const LAYERS_KEY: &[u8] = b"piece-layers";
    // `d`, `e`, every field and the layers' key.
    let size = f
        .iter()
        .map(|(k, v)| bytes_len(k.len()) + v.bytes().len())
        .sum::<usize>()
        + 2
        + bytes_len(LAYERS_KEY.len());
    let layers = r
        .piece_layers
        .filter(|l| r.info.is_some() && !l.is_empty())
        .and_then(|l| encode_layers(l, MAX_RESUME_BYTES.saturating_sub(size)));
    if let Some(l) = layers {
        f.insert(LAYERS_KEY, Field::Encoded(l));
    }
    let mut out = Vec::with_capacity(
        f.iter()
            .map(|(k, v)| bytes_len(k.len()) + v.bytes().len())
            .sum::<usize>()
            + 2,
    );
    out.push(b'd');
    for (k, v) in &f {
        encode_into(&OwnedValue::Bytes(k.to_vec()), &mut out);
        out.extend_from_slice(v.bytes());
    }
    out.push(b'e');
    out
}

/// Whether resume data's `have` can be trusted without a check, given the
/// stamps saved with it and the storage's stamps now: every file holding a
/// byte of a saved piece has the same stamp; one that did not exist at the
/// save (its bytes were in the part file) must still not exist, and the
/// part file must have the same stamp. Stamps of the wrong count trust
/// nothing.
pub(crate) fn stamps_match(
    meta: &Metainfo,
    have: &Bitfield,
    saved: &[Option<FileStamp>],
    now: &[Option<FileStamp>],
) -> bool {
    let files = &meta.info.files;
    let expected = files.len().saturating_add(1);
    if saved.len() != expected || now.len() != expected {
        return false;
    }
    let layout = meta.piece_layout();
    let file_layout = meta.file_layout();
    let mut holds = vec![false; files.len()];
    for piece in have.iter_ones() {
        let range = file_layout.files_overlapping(
            layout.piece_offset(piece),
            u64::from(layout.piece_size(piece)),
        );
        for i in range {
            if let Some(h) = holds.get_mut(i) {
                *h = true;
            }
        }
    }
    let mut needs_part = false;
    for (i, f) in files.iter().enumerate() {
        if f.pad || f.len == 0 || !holds.get(i).copied().unwrap_or(false) {
            continue;
        }
        match (saved.get(i), now.get(i)) {
            (Some(Some(a)), Some(Some(b))) if a == b => {}
            (Some(None), Some(None)) => needs_part = true,
            _ => return false,
        }
    }
    if needs_part {
        let part = files.len();
        return matches!(
            (saved.get(part), now.get(part)),
            (Some(Some(a)), Some(Some(b))) if a == b
        );
    }
    true
}

/// The DHT's state and every torrent's resume data, in queue order: what
/// [`Session::save_state`](crate::Session::save_state) and
/// [`Session::shutdown_and_save`](crate::Session::shutdown_and_save) return
/// and [`Session::restore`](crate::Session::restore) starts from. The
/// library never writes it anywhere: [`to_bytes`](Self::to_bytes) gives the
/// bytes to store.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionState {
    /// [`Session::dht_state`](crate::Session::dht_state); `None` with the
    /// DHT off.
    pub dht: Option<Vec<u8>>,
    /// Each torrent's resume data
    /// ([`TorrentHandle::save_resume_data`](crate::TorrentHandle::save_resume_data)),
    /// in queue order.
    pub torrents: Vec<Vec<u8>>,
}

impl SessionState {
    /// The state as one bencoded dictionary (`fbt-session-version` 1).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(b'd');
        if let Some(dht) = &self.dht {
            encode_into(&OwnedValue::from("dht"), &mut out);
            encode_into(&OwnedValue::Bytes(dht.clone()), &mut out);
        }
        encode_into(&OwnedValue::from("fbt-session-version"), &mut out);
        encode_into(&OwnedValue::Int(SESSION_VERSION), &mut out);
        encode_into(&OwnedValue::from("torrents"), &mut out);
        out.push(b'l');
        for t in &self.torrents {
            out.extend_from_slice(t.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(t);
        }
        out.extend_from_slice(b"ee");
        out
    }

    /// Reads [`to_bytes`](Self::to_bytes)' output back. Hostile input: at
    /// most [`MAX_SESSION_TORRENTS`] entries, each kept as bytes (decoded
    /// and checked when [`Session::restore`](crate::Session::restore) adds
    /// it); no size cap beyond the input's own.
    pub fn from_bytes(bytes: &[u8]) -> Result<SessionState, ResumeError> {
        // The dictionary, three keys and their values, and the entries.
        let limits = Limits::new(3, MAX_SESSION_TORRENTS.saturating_add(16));
        let root = decode_with(bytes, limits).map_err(ResumeError::Bencode)?;
        let d = root.as_dict().ok_or(ResumeError::Invalid("session"))?;
        let version =
            int(d, "fbt-session-version")?.ok_or(ResumeError::Missing("fbt-session-version"))?;
        if version != SESSION_VERSION {
            return Err(ResumeError::Version(version));
        }
        let dht = d
            .get(b"dht")
            .map(|v| {
                v.as_bytes()
                    .map(<[u8]>::to_vec)
                    .ok_or(ResumeError::Invalid("dht"))
            })
            .transpose()?;
        let entries = list(d, "torrents")?;
        if entries.len() > MAX_SESSION_TORRENTS {
            return Err(ResumeError::Invalid("torrents"));
        }
        let torrents = entries
            .iter()
            .map(|v| {
                v.as_bytes()
                    .map(<[u8]>::to_vec)
                    .ok_or(ResumeError::Invalid("torrents"))
            })
            .collect::<Result<_, _>>()?;
        Ok(SessionState { dht, torrents })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbt_bencode::{DictBuilder, encode as bencode};
    use fbt_metainfo::collides;

    /// A torrent of `files` (name, length, pad) with 16 KiB pieces.
    fn torrent(files: &[(&str, u64, bool)]) -> Metainfo {
        let total: u64 = files.iter().map(|f| f.1).sum();
        let n = total.div_ceil(16_384) as usize;
        let list = files
            .iter()
            .map(|(name, len, pad)| {
                let mut f = DictBuilder::new();
                if *pad {
                    f.insert(b"attr", "p");
                }
                f.insert(b"length", *len as i64);
                f.insert(
                    b"path",
                    OwnedValue::List(name.split('/').map(OwnedValue::from).collect()),
                );
                f.build()
            })
            .collect();
        let mut info = DictBuilder::new();
        info.insert(b"files", OwnedValue::List(list));
        info.insert(b"name", "t");
        info.insert(b"piece length", 16_384i64);
        info.insert(b"pieces", vec![7u8; n * 20]);
        let mut root = DictBuilder::new();
        root.insert(b"info", info.build());
        Metainfo::from_bytes(&bencode(&root.build())).unwrap()
    }

    fn rel(c: &[&str]) -> RelPath {
        sanitize_path(c.iter().map(|s| s.as_bytes())).unwrap()
    }

    fn three() -> Metainfo {
        torrent(&[
            ("a", 20_000, false),
            (".pad/12768", 12_768, true),
            ("b", 30_000, false),
        ])
    }

    fn state(m: &Metainfo) -> ResumeState {
        let mut have = Bitfield::new(m.piece_layout().num_pieces());
        have.set(0);
        ResumeState {
            have,
            stamps: vec![
                Some(FileStamp::new(20_000, 5)),
                None,
                Some(FileStamp::new(30_000, -3)),
                None,
            ],
            renamed: vec![(2, rel(&["t", "c"]))],
            web_seeds: vec!["http://w/".into()],
            queue_position: Some(3),
            uploaded: 11,
            downloaded: 22,
            active_time: Duration::from_secs(33),
            seeding_time: Duration::from_secs(44),
            added: 1_700_000_000,
            completed: Some(1_700_000_100),
            peers: vec![
                "1.2.3.4:5".parse().unwrap(),
                "[2001:db8::1]:6".parse().unwrap(),
            ],
        }
    }

    fn record<'a>(m: &'a Metainfo, s: &'a ResumeState, prio: &'a [Priority]) -> ResumeRecord<'a> {
        ResumeRecord {
            info_hash: m.info_hashes.v1.map(|h| h.0),
            info_hash2: m.info_hashes.v2.map(|h| h.0),
            save_path: Path::new("/save/here"),
            info: Some(m.raw_info()),
            piece_layers: Some(&m.piece_layers),
            magnet: None,
            trackers: &[],
            file_priorities: prio,
            paused: true,
            auto_managed: false,
            sequential: true,
            rate_limit: RateLimit::new(100, 200),
            state: s,
        }
    }

    /// `bytes` decoded, `f` applied to the root dictionary, re-encoded.
    fn edit(bytes: &[u8], f: impl FnOnce(&mut BTreeMap<Vec<u8>, OwnedValue>)) -> Vec<u8> {
        let OwnedValue::Dict(mut d) = fbt_bencode::decode(bytes).unwrap().to_owned_value() else {
            panic!("not a dict");
        };
        f(&mut d);
        bencode(&OwnedValue::Dict(d))
    }

    /// Version 1 resume data of [`three`]: what the encoder wrote before
    /// version 2 (a v1 torrent's data differs only in the version).
    fn version_1_data() -> Vec<u8> {
        let m = three();
        let s = state(&m);
        let prio = [Priority::Low, Priority::Normal, Priority::High];
        edit(&encode(&record(&m, &s, &prio)), |d| {
            d.insert(b"fbt-resume-version".to_vec(), 1.into());
        })
    }

    /// Writes `tests/fixtures/resume-v1.bin` and the v2 and hybrid seeds
    /// of the `resume` fuzz target.
    #[test]
    #[ignore = "writes the committed fixture and fuzz seeds"]
    fn regenerate_fixtures() {
        let dir = env!("CARGO_MANIFEST_DIR");
        std::fs::write(
            format!("{dir}/tests/fixtures/resume-v1.bin"),
            version_1_data(),
        )
        .unwrap();
        for (name, version) in [
            ("v2", fbt_metainfo::TorrentVersion::V2),
            ("hybrid", fbt_metainfo::TorrentVersion::Hybrid),
        ] {
            let m = created(version);
            let mut s = plain_state(&m);
            s.have.set(0);
            let bytes = encode(&record(&m, &s, &[]));
            std::fs::write(format!("{dir}/../../fuzz/seeds/resume/{name}"), bytes).unwrap();
        }
    }

    #[test]
    fn round_trip() {
        let m = three();
        let s = state(&m);
        let prio = [Priority::Low, Priority::Normal, Priority::High];
        let bytes = encode(&record(&m, &s, &prio));
        let d = decode_resume(&bytes).unwrap();
        let TorrentSource::Metainfo(got) = &d.source else {
            panic!("metainfo expected");
        };
        assert_eq!(got.info_hashes, m.info_hashes);
        assert_eq!(got.raw_info(), m.raw_info());
        assert_eq!(d.save_path, PathBuf::from("/save/here"));
        assert!(d.paused && !d.auto_managed && d.sequential);
        assert_eq!(d.file_priorities, prio);
        assert_eq!(d.rate_limit, RateLimit::new(100, 200));
        let r = &d.state;
        assert_eq!(r.have, s.have);
        assert_eq!(r.stamps, s.stamps);
        assert_eq!(r.renamed, s.renamed);
        assert_eq!(r.web_seeds, s.web_seeds);
        assert_eq!(r.queue_position, Some(3));
        assert_eq!((r.uploaded, r.downloaded), (11, 22));
        assert_eq!(r.active_time, Duration::from_secs(33));
        assert_eq!(r.seeding_time, Duration::from_secs(44));
        assert_eq!((r.added, r.completed), (1_700_000_000, Some(1_700_000_100)));
        assert_eq!(r.peers, s.peers);
        // Keys come out in bencode order.
        assert!(fbt_bencode::decode(&bytes).is_ok());
    }

    #[test]
    fn magnet_round_trip() {
        let m = three();
        let mut magnet = m.magnet();
        magnet.web_seeds = vec!["http://stale/".into()];
        let s = ResumeState {
            have: Bitfield::new(0),
            stamps: Vec::new(),
            renamed: Vec::new(),
            ..state(&m)
        };
        let rec = ResumeRecord {
            info: None,
            magnet: Some(magnet.to_uri()),
            file_priorities: &[],
            ..record(&m, &s, &[])
        };
        let d = decode_resume(&encode(&rec)).unwrap();
        let TorrentSource::Magnet(got) = &d.source else {
            panic!("magnet expected");
        };
        assert_eq!(got.info_hashes, m.info_hashes);
        assert!(got.web_seeds.is_empty(), "url-list is the set");
        assert_eq!(d.state.web_seeds, s.web_seeds);
        assert_eq!(d.state.have.len(), 0);
    }

    #[test]
    fn hostile_input_names_the_field() {
        let m = three();
        let s = state(&m);
        let good = encode(&record(&m, &s, &[]));
        let e = |b: &[u8]| decode_resume(b).unwrap_err();
        assert!(matches!(e(b"garbage"), ResumeError::Bencode(_)));
        assert!(matches!(e(b"le"), ResumeError::Invalid("resume")));
        assert!(matches!(
            e(&vec![b'x'; MAX_RESUME_BYTES + 1]),
            ResumeError::TooLarge
        ));
        let with = |k: &str, v: OwnedValue| {
            edit(&good, |d| {
                d.insert(k.as_bytes().to_vec(), v);
            })
        };
        let without = |k: &str| {
            edit(&good, |d| {
                d.remove(k.as_bytes());
            })
        };
        assert!(matches!(
            e(&with("fbt-resume-version", 3.into())),
            ResumeError::Version(3)
        ));
        assert!(matches!(
            e(&without("fbt-resume-version")),
            ResumeError::Missing("fbt-resume-version")
        ));
        assert!(matches!(
            e(&without("info-hash")),
            ResumeError::Missing("info-hash")
        ));
        assert!(matches!(
            e(&without("save-path")),
            ResumeError::Missing("save-path")
        ));
        assert!(matches!(e(&without("info")), ResumeError::Missing("info")));
        assert!(matches!(
            e(&with("info-hash", OwnedValue::Bytes(vec![1; 19]))),
            ResumeError::Invalid("info-hash")
        ));
        assert!(matches!(
            e(&with("info-hash", OwnedValue::Bytes(vec![1; 20]))),
            ResumeError::InfoHashMismatch
        ));
        assert!(matches!(
            e(&with("pieces", OwnedValue::Bytes(vec![0xff]))),
            ResumeError::Invalid("pieces")
        ));
        assert!(matches!(
            e(&with("pieces", OwnedValue::Bytes(vec![0; 9]))),
            ResumeError::Invalid("pieces")
        ));
        assert!(matches!(
            e(&with("file-priorities", OwnedValue::List(vec![4.into()]))),
            ResumeError::Invalid("file-priorities")
        ));
        assert!(matches!(
            e(&with(
                "file-priorities",
                OwnedValue::List(vec![4.into(), 9.into(), 4.into()])
            )),
            ResumeError::Invalid("file-priorities")
        ));
        assert!(matches!(
            e(&with("paused", 2.into())),
            ResumeError::Invalid("paused")
        ));
        assert!(matches!(
            e(&with("total-uploaded", (-1).into())),
            ResumeError::Invalid("total-uploaded")
        ));
        assert!(matches!(
            e(&with("peers", OwnedValue::Bytes(vec![0; 7]))),
            ResumeError::Invalid("peers")
        ));
        assert!(matches!(
            e(&with(
                "stamps",
                OwnedValue::List(vec![OwnedValue::List(Vec::new())])
            )),
            ResumeError::Invalid("stamps")
        ));
        assert!(matches!(
            e(&with("url-list", OwnedValue::List(vec![1.into()]))),
            ResumeError::Invalid("url-list")
        ));
        assert!(matches!(
            e(&with("trackers", OwnedValue::List(vec![1.into()]))),
            ResumeError::Invalid("trackers")
        ));
        let bad_magnet = edit(&with("magnet", "magnet:?xt=urn:btih:zz".into()), |d| {
            d.remove(b"info".as_slice());
        });
        assert!(matches!(e(&bad_magnet), ResumeError::Magnet(_)));
        // Another torrent's info under this info hash.
        let other = torrent(&[("x", 5, false)]);
        let swapped = edit(&good, |d| {
            let info = fbt_bencode::decode(other.raw_info())
                .unwrap()
                .to_owned_value();
            d.insert(b"info".to_vec(), info);
        });
        assert!(matches!(e(&swapped), ResumeError::InfoHashMismatch));
    }

    #[test]
    fn version_1_still_reads() {
        let d = decode_resume(include_bytes!("../tests/fixtures/resume-v1.bin")).unwrap();
        let m = three();
        let TorrentSource::Metainfo(got) = &d.source else {
            panic!("metainfo expected");
        };
        assert_eq!(got.info_hashes, m.info_hashes);
        assert_eq!(got.raw_info(), m.raw_info());
        assert_eq!(d.state.have, state(&m).have);
        assert_eq!(d.state.renamed, state(&m).renamed);
        assert_eq!(
            d.file_priorities,
            [Priority::Low, Priority::Normal, Priority::High]
        );
        // The committed bytes are what the generator writes today.
        assert_eq!(
            include_bytes!("../tests/fixtures/resume-v1.bin").as_slice(),
            version_1_data()
        );
    }

    #[test]
    fn version_3_is_refused() {
        let m = three();
        let s = state(&m);
        let good = encode(&record(&m, &s, &[]));
        let v3 = edit(&good, |d| {
            d.insert(b"fbt-resume-version".to_vec(), 3.into());
        });
        assert!(matches!(decode_resume(&v3), Err(ResumeError::Version(3))));
        let v0 = edit(&good, |d| {
            d.insert(b"fbt-resume-version".to_vec(), 0.into());
        });
        assert!(matches!(decode_resume(&v0), Err(ResumeError::Version(0))));
    }

    const V2_PIECE: u32 = 32_768;

    /// A torrent of `version` made by `CreateTorrent` over two files of
    /// 100 000 and 300 000 bytes (a layer each) in 32 KiB pieces.
    fn created(version: fbt_metainfo::TorrentVersion) -> Metainfo {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("payload");
        std::fs::create_dir(&root).unwrap();
        for (name, len) in [("a", 100_000u32), ("c", 300_000)] {
            let data: Vec<u8> = (0..len)
                .map(|i| (i % 251) as u8 ^ name.as_bytes()[0])
                .collect();
            std::fs::write(root.join(name), data).unwrap();
        }
        let bytes = fbt_metainfo::CreateTorrent::new(&root)
            .version(version)
            .piece_len(V2_PIECE)
            .build(&mut |_, _| std::ops::ControlFlow::Continue(()))
            .unwrap();
        let m = Metainfo::from_bytes(&bytes).unwrap();
        assert_eq!(m.piece_layers.len(), 2);
        m
    }

    /// Plain resume data of `m`: its pieces all missing, nothing renamed.
    fn plain_state(m: &Metainfo) -> ResumeState {
        ResumeState {
            have: Bitfield::new(m.piece_layout().num_pieces()),
            stamps: Vec::new(),
            renamed: Vec::new(),
            ..state(m)
        }
    }

    #[test]
    fn v2_and_hybrid_round_trip_with_layers() {
        use fbt_metainfo::TorrentVersion;
        for version in [TorrentVersion::V2, TorrentVersion::Hybrid] {
            let m = created(version);
            let s = plain_state(&m);
            let bytes = encode(&record(&m, &s, &[]));
            let OwnedValue::Dict(d) = fbt_bencode::decode(&bytes).unwrap().to_owned_value() else {
                panic!("not a dict");
            };
            assert_eq!(d.get(&b"fbt-resume-version"[..]), Some(&OwnedValue::Int(2)));
            assert_eq!(
                d.get(&b"info-hash"[..]),
                m.info_hashes
                    .v1
                    .map(|h| OwnedValue::Bytes(h.0.to_vec()))
                    .as_ref()
            );
            assert_eq!(
                d.get(&b"info-hash2"[..]),
                m.info_hashes
                    .v2
                    .map(|h| OwnedValue::Bytes(h.0.to_vec()))
                    .as_ref()
            );
            let decoded = decode_resume(&bytes).unwrap();
            let TorrentSource::Metainfo(got) = &decoded.source else {
                panic!("metainfo expected");
            };
            assert_eq!(got.info_hashes, m.info_hashes);
            assert_eq!(got.piece_layers, m.piece_layers, "{version:?}");
            // Without either hash it is refused.
            let neither = edit(&bytes, |d| {
                d.remove(&b"info-hash"[..]);
                d.remove(&b"info-hash2"[..]);
            });
            assert!(matches!(
                decode_resume(&neither),
                Err(ResumeError::Missing("info-hash"))
            ));
        }
    }

    #[test]
    fn v2_resume_requires_a_matching_hash() {
        use fbt_metainfo::TorrentVersion;
        for version in [TorrentVersion::V2, TorrentVersion::Hybrid] {
            let m = created(version);
            let s = plain_state(&m);
            let good = encode(&record(&m, &s, &[]));
            let wrong = edit(&good, |d| {
                d.insert(b"info-hash2".to_vec(), OwnedValue::Bytes(vec![1; 32]));
            });
            assert!(
                matches!(decode_resume(&wrong), Err(ResumeError::InfoHashMismatch)),
                "{version:?}"
            );
            let short = edit(&good, |d| {
                d.insert(b"info-hash2".to_vec(), OwnedValue::Bytes(vec![1; 20]));
            });
            assert!(matches!(
                decode_resume(&short),
                Err(ResumeError::Invalid("info-hash2"))
            ));
        }
        // A hybrid's right v2 hash does not excuse a wrong v1 one.
        let m = created(TorrentVersion::Hybrid);
        let s = plain_state(&m);
        let wrong_v1 = edit(&encode(&record(&m, &s, &[])), |d| {
            d.insert(b"info-hash".to_vec(), OwnedValue::Bytes(vec![1; 20]));
        });
        assert!(matches!(
            decode_resume(&wrong_v1),
            Err(ResumeError::InfoHashMismatch)
        ));
    }

    #[test]
    fn v2_magnet_resume_without_info() {
        let m = created(fbt_metainfo::TorrentVersion::V2);
        let s = ResumeState {
            have: Bitfield::new(0),
            ..plain_state(&m)
        };
        let rec = ResumeRecord {
            info: None,
            magnet: Some(m.magnet().to_uri()),
            piece_layers: None,
            ..record(&m, &s, &[])
        };
        let good = encode(&rec);
        let d = decode_resume(&good).unwrap();
        let TorrentSource::Magnet(got) = &d.source else {
            panic!("magnet expected");
        };
        assert_eq!(got.info_hashes, m.info_hashes);
        let other = edit(&good, |d| {
            d.insert(b"info-hash2".to_vec(), OwnedValue::Bytes(vec![1; 32]));
        });
        assert!(matches!(
            decode_resume(&other),
            Err(ResumeError::InfoHashMismatch)
        ));
        // A v1 hash the magnet does not carry.
        let v1 = edit(&good, |d| {
            d.insert(b"info-hash".to_vec(), OwnedValue::Bytes(vec![1; 20]));
        });
        assert!(matches!(
            decode_resume(&v1),
            Err(ResumeError::InfoHashMismatch)
        ));
    }

    #[test]
    fn bad_layers_are_dropped() {
        let m = created(fbt_metainfo::TorrentVersion::V2);
        let s = plain_state(&m);
        let good = encode(&record(&m, &s, &[]));
        let (bad_root, kept_root) = {
            let mut roots = m.piece_layers.keys().copied();
            (roots.next().unwrap(), roots.next().unwrap())
        };
        type Layers = BTreeMap<Vec<u8>, OwnedValue>;
        let corrupt = |f: &dyn Fn(&mut Layers)| {
            edit(&good, |d| {
                let Some(OwnedValue::Dict(layers)) = d.get_mut(&b"piece-layers"[..]) else {
                    panic!("piece-layers expected");
                };
                f(layers);
            })
        };
        let flipped = corrupt(&|l| {
            let Some(OwnedValue::Bytes(b)) = l.get_mut(&bad_root[..]) else {
                panic!("layer expected");
            };
            b[5] ^= 1;
        });
        let ragged = corrupt(&|l| {
            let Some(OwnedValue::Bytes(b)) = l.get_mut(&bad_root[..]) else {
                panic!("layer expected");
            };
            b.pop();
        });
        let not_bytes = corrupt(&|l| {
            l.insert(bad_root.to_vec(), 7.into());
        });
        let short_key = corrupt(&|l| {
            let v = l.remove(&bad_root[..]).unwrap();
            l.insert(bad_root[..31].to_vec(), v);
        });
        for bytes in [flipped, ragged, not_bytes, short_key] {
            let d = decode_resume(&bytes).unwrap();
            let TorrentSource::Metainfo(got) = &d.source else {
                panic!("metainfo expected");
            };
            assert!(!got.piece_layers.contains_key(&bad_root));
            assert!(got.piece_layers.contains_key(&kept_root));
        }
        let not_a_dict = edit(&good, |d| {
            d.insert(b"piece-layers".to_vec(), 1.into());
        });
        assert!(matches!(
            decode_resume(&not_a_dict),
            Err(ResumeError::Invalid("piece-layers"))
        ));
    }

    #[test]
    fn oversized_layers_are_left_out() {
        let m = created(fbt_metainfo::TorrentVersion::V2);
        let s = plain_state(&m);
        // Two layers of 40 MiB each: one fits under the cap, both do not.
        // `encode` does not judge them (`decode_resume` would drop them).
        let n = 40 * 1024 * 1024 / 32;
        let layers: BTreeMap<[u8; 32], Vec<[u8; 32]>> = [
            ([1u8; 32], vec![[3u8; 32]; n]),
            ([2u8; 32], vec![[4u8; 32]; n]),
        ]
        .into_iter()
        .collect();
        let rec = ResumeRecord {
            piece_layers: Some(&layers),
            ..record(&m, &s, &[])
        };
        let bytes = encode(&rec);
        assert!(bytes.len() <= MAX_RESUME_BYTES, "{}", bytes.len());
        let OwnedValue::Dict(d) = fbt_bencode::decode(&bytes).unwrap().to_owned_value() else {
            panic!("not a dict");
        };
        let Some(OwnedValue::Dict(kept)) = d.get(&b"piece-layers"[..]) else {
            panic!("piece-layers expected");
        };
        assert_eq!(kept.len(), 1);
        assert!(decode_resume(&bytes).is_ok());
        // Layers that fit are all kept.
        let bytes = encode(&record(&m, &s, &[]));
        let OwnedValue::Dict(d) = fbt_bencode::decode(&bytes).unwrap().to_owned_value() else {
            panic!("not a dict");
        };
        let Some(OwnedValue::Dict(kept)) = d.get(&b"piece-layers"[..]) else {
            panic!("piece-layers expected");
        };
        assert_eq!(kept.len(), 2);
    }

    #[test]
    fn peers_are_capped_per_family() {
        let m = three();
        let mut s = state(&m);
        s.peers = (0..10_000u32)
            .map(|i| SocketAddr::from((i.to_be_bytes(), 1)))
            .chain((0..300u16).map(|i| SocketAddr::from(([0x2001, 0xdb8, 0, 0, 0, 0, 0, i], 1))))
            .collect();
        let bytes = encode(&record(&m, &s, &[]));
        let d = decode_resume(&bytes).unwrap();
        assert_eq!(d.peers().iter().filter(|a| a.is_ipv4()).count(), 200);
        assert_eq!(d.peers().iter().filter(|a| a.is_ipv6()).count(), 200);
        // Too many on input are cut as well.
        let many: Vec<u8> = (0..10_000u32)
            .flat_map(|i| {
                let mut b = i.to_be_bytes().to_vec();
                b.extend_from_slice(&[0, 1]);
                b
            })
            .collect();
        let bytes = edit(&bytes, |d| {
            d.insert(b"peers".to_vec(), OwnedValue::Bytes(many));
        });
        assert_eq!(decode_resume(&bytes).unwrap().peers().len(), 400);
    }

    fn renamed_input(m: &Metainfo, entries: Vec<OwnedValue>) -> Result<DecodedResume, ResumeError> {
        let s = ResumeState {
            renamed: Vec::new(),
            ..state(m)
        };
        let good = encode(&record(m, &s, &[]));
        decode_resume(&edit(&good, |d| {
            d.insert(b"renamed".to_vec(), OwnedValue::List(entries));
        }))
    }

    fn entry(index: i64, path: &[&str]) -> OwnedValue {
        OwnedValue::List(vec![
            index.into(),
            OwnedValue::List(path.iter().map(|c| OwnedValue::from(*c)).collect()),
        ])
    }

    #[test]
    fn renamed_paths_are_hostile_input() {
        let m = three();
        let ok = renamed_input(&m, vec![entry(0, &["x", "y"])]).unwrap();
        assert_eq!(ok.state.renamed[0].1.components(), ["x", "y"]);
        let part = format!(".{}.parts", m.info_hashes);
        let bad = [
            vec![entry(0, &["..", "etc"])],
            vec![entry(0, &["a", ""])],
            vec![entry(0, &["."])],
            vec![entry(0, &[])],
            vec![entry(0, &["con"])],
            vec![entry(0, &["a\\b"])],
            vec![entry(1, &["x"])],
            vec![entry(3, &["x"])],
            vec![entry(-1, &["x"])],
            vec![entry(0, &["x"]), entry(0, &["y"])],
            // Collisions: case, a file below another, a file over a dir.
            vec![entry(0, &["T", "B"])],
            vec![entry(0, &["t", "b", "c"])],
            vec![entry(0, &["t"])],
            vec![entry(0, &["q"]), entry(2, &["Q"])],
            vec![OwnedValue::List(vec![0.into()])],
            vec![0.into()],
            // The part file's name, whatever the case, or a directory of it.
            vec![entry(0, &[part.as_str()])],
            vec![entry(0, &[part.to_uppercase().as_str()])],
            vec![entry(0, &[part.as_str(), "x"])],
        ];
        for b in bad {
            assert!(
                matches!(
                    renamed_input(&m, b.clone()),
                    Err(ResumeError::Invalid("renamed"))
                ),
                "{b:?}"
            );
        }
        // A swap is fine: both new paths are free once both renames apply.
        let swap = renamed_input(&m, vec![entry(0, &["t", "b"]), entry(2, &["t", "a"])]);
        assert!(swap.is_ok());
    }

    #[test]
    fn collision_check_matches_collides() {
        let names = [
            "a", "A", "a/b", "b", "c/d", "C", "c", "é", "e\u{301}", "d/e/f", "d/e", "t", "T/P",
            "t/s", "t/q", "t/q/r/x", "t/p",
        ];
        let base = torrent(&[("p", 1, false), ("q/r", 1, false), ("s", 1, false)]);
        for (i, n1) in names.iter().enumerate() {
            for n2 in names.iter().skip(i) {
                let p1 = rel(&n1.split('/').collect::<Vec<_>>());
                let p2 = rel(&n2.split('/').collect::<Vec<_>>());
                let renamed = vec![(0, p1), (2, p2)];
                let mut files = base.info.files.clone();
                for (i, p) in &renamed {
                    files[*i].path = p.clone();
                }
                let naive = renamed.iter().any(|(i, p)| collides(&files, *i, p));
                assert_eq!(
                    renames_collide(&base.info.files, &renamed),
                    naive,
                    "{n1} {n2}"
                );
            }
        }
    }

    #[test]
    fn stamps_decide_trust() {
        let m = torrent(&[("a", 16_384, false), ("b", 16_384, false), ("c", 1, false)]);
        let st = |n| Some(FileStamp::new(16_384, n));
        let mut have = Bitfield::new(3);
        have.set(1);
        let saved = vec![st(1), st(2), st(3), None];
        // Only file b holds a saved piece: a and c may change.
        assert!(stamps_match(
            &m,
            &have,
            &saved,
            &[st(9), st(2), None, st(5)]
        ));
        assert!(!stamps_match(
            &m,
            &have,
            &saved,
            &[st(1), st(3), st(3), None]
        ));
        assert!(!stamps_match(
            &m,
            &have,
            &saved,
            &[st(1), None, st(3), None]
        ));
        assert!(!stamps_match(&m, &have, &saved, &[st(1), st(2), st(3)]));
        assert!(!stamps_match(&m, &have, &[], &[st(1), st(2), st(3), None]));
        // b was in the part file: still missing, and the part file unchanged.
        let saved = vec![st(1), None, st(3), st(7)];
        assert!(stamps_match(
            &m,
            &have,
            &saved,
            &[st(1), None, st(3), st(7)]
        ));
        assert!(!stamps_match(
            &m,
            &have,
            &saved,
            &[st(1), None, st(3), st(8)]
        ));
        assert!(!stamps_match(
            &m,
            &have,
            &saved,
            &[st(1), None, st(3), None]
        ));
        assert!(!stamps_match(
            &m,
            &have,
            &saved,
            &[st(1), st(2), st(3), st(7)]
        ));
        let none = vec![st(1), None, st(3), None];
        assert!(!stamps_match(&m, &have, &none, &none));
        // Nothing saved: nothing to doubt.
        assert!(stamps_match(
            &m,
            &Bitfield::new(3),
            &saved,
            &[None, None, None, None]
        ));
    }

    #[test]
    fn session_state_round_trip_and_limits() {
        let s = SessionState {
            dht: Some(b"dht".to_vec()),
            torrents: vec![b"one".to_vec(), Vec::new(), b"three".to_vec()],
        };
        assert_eq!(SessionState::from_bytes(&s.to_bytes()).unwrap(), s);
        let empty = SessionState::default();
        assert_eq!(SessionState::from_bytes(&empty.to_bytes()).unwrap(), empty);
        assert!(matches!(
            SessionState::from_bytes(b"d19:fbt-session-versioni2ee"),
            Err(ResumeError::Version(2))
        ));
        assert!(matches!(
            SessionState::from_bytes(b"de"),
            Err(ResumeError::Missing("fbt-session-version"))
        ));
        assert!(matches!(
            SessionState::from_bytes(b"d19:fbt-session-versioni1e8:torrentsli1eee"),
            Err(ResumeError::Invalid("torrents"))
        ));
        let many = SessionState {
            dht: None,
            torrents: vec![Vec::new(); MAX_SESSION_TORRENTS + 1],
        };
        assert!(SessionState::from_bytes(&many.to_bytes()).is_err());
        let most = SessionState {
            dht: None,
            torrents: vec![Vec::new(); MAX_SESSION_TORRENTS],
        };
        assert_eq!(
            SessionState::from_bytes(&most.to_bytes())
                .unwrap()
                .torrents
                .len(),
            MAX_SESSION_TORRENTS
        );
    }
}
