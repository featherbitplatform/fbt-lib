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
//! The dictionary (keys in bencode order; the first three are required):
//!
//! | key | value |
//! |---|---|
//! | `fbt-resume-version` | 1 |
//! | `info-hash` | the v1 info hash, 20 bytes |
//! | `save-path` | the directory the files are below |
//! | `info` | the raw `info` dictionary; its SHA-1 must be `info-hash` |
//! | `magnet` | without `info`: the magnet URI |
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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use fbt_bencode::{DecodeError, Dict, Limits, OwnedValue, Value, decode_with, encode_into};
use fbt_core::compact::{decode_v4_list, decode_v6_list};
use fbt_core::{Bitfield, InfoHashV1, InfoHashes, compact};
use fbt_metainfo::{
    FileEntry, MAX_WEB_SEEDS, Magnet, MagnetError, Metainfo, MetainfoError, RelPath, sanitize_path,
};

use crate::bandwidth::RateLimit;
use crate::session::{Priority, TorrentSource};
use crate::storage::FileStamp;

/// Largest resume data accepted; longer input is refused before decoding.
pub const MAX_RESUME_BYTES: usize = 64 * 1024 * 1024;
/// Most bencode values (the raw `info` dictionary's included) in resume
/// data.
pub const MAX_RESUME_ITEMS: usize = 4_194_304;
/// Deepest nesting of resume data.
const MAX_RESUME_DEPTH: usize = 8;
/// Most peers kept per address family (`peers`, `peers6`); later ones are
/// dropped, on save and on load.
pub const MAX_RESUME_PEERS: usize = 200;
/// Most torrents a [`SessionState`] holds.
pub const MAX_SESSION_TORRENTS: usize = 100_000;
/// The resume data format written and read.
const RESUME_VERSION: i64 = 1;
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
    /// The `info` dictionary (or the magnet) is not that of `info-hash`.
    #[error("the resume data's torrent does not match its `info-hash`")]
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
    if version != RESUME_VERSION {
        return Err(ResumeError::Version(version));
    }
    let ih: [u8; 20] = d
        .get(b"info-hash")
        .ok_or(ResumeError::Missing("info-hash"))?
        .as_bytes()
        .and_then(|b| b.try_into().ok())
        .ok_or(ResumeError::Invalid("info-hash"))?;
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
            if InfoHashV1::of(raw).0 != ih {
                return Err(ResumeError::InfoHashMismatch);
            }
            let m = Metainfo::from_info_bytes(raw, InfoHashes::from_v1(InfoHashV1(ih)), trackers)
                .map_err(ResumeError::Metainfo)?;
            let source = TorrentSource::Metainfo(Box::new(m.clone()));
            (source, Some(m))
        }
        (None, Some(magnet)) => {
            let uri = magnet.as_str().ok_or(ResumeError::Invalid("magnet"))?;
            let mut m = Magnet::parse(uri).map_err(ResumeError::Magnet)?;
            if m.info_hashes.v1.map(|h| h.0) != Some(ih) {
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
/// it unchanged, and it collides with no other file).
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
    if renames_collide(files, &out) {
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
    pub info_hash: [u8; 20],
    pub save_path: &'a Path,
    /// The raw `info` dictionary, once the metadata is known.
    pub info: Option<&'a [u8]>,
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

/// One value of the resume dictionary: owned, or raw bytes spliced in as
/// they are (the `info` dictionary, whose hash must not change).
enum Field<'a> {
    Value(OwnedValue),
    Raw(&'a [u8]),
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
        f.insert(k.as_bytes(), Field::Value(v));
    };
    put("fbt-resume-version", OwnedValue::Int(RESUME_VERSION));
    put("info-hash", OwnedValue::Bytes(r.info_hash.to_vec()));
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
    let mut out = Vec::new();
    out.push(b'd');
    for (k, v) in &f {
        encode_into(&OwnedValue::Bytes(k.to_vec()), &mut out);
        match v {
            Field::Value(v) => encode_into(v, &mut out),
            Field::Raw(raw) => out.extend_from_slice(raw),
        }
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
            info_hash: m.info_hashes.wire_hash(),
            save_path: Path::new("/save/here"),
            info: Some(m.raw_info()),
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
            e(&with("fbt-resume-version", 2.into())),
            ResumeError::Version(2)
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
