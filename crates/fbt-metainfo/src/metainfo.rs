//! v1, v2 and hybrid `.torrent` parsing. The input is untrusted.

use std::collections::{BTreeMap, HashSet};

use fbt_bencode::{DecodeError, Dict, Limits, Value, decode_with};
use fbt_core::{FileLayout, InfoHashV1, InfoHashV2, InfoHashes, PieceLayout};

use crate::collide::resolve_collisions;
use crate::magnet::Magnet;
use crate::path::{RelPath, sanitize_component, sanitize_path};
use crate::v2::{
    MAX_PIECE_LAYER_HASHES, MAX_V2_PIECES, TorrentVersion, V2Piece, layer_ok, layer_targets,
    locate, merge_hybrid, parse_file_tree, parse_piece_layers, v2_file_starts, valid_v2_piece_len,
};

/// Most web seed URLs kept (`url-list` here, `ws` in a magnet); later ones are
/// ignored.
pub const MAX_WEB_SEEDS: usize = 16;
/// Longest web seed URL kept, in bytes.
pub(crate) const MAX_URL_LEN: usize = 2048;
/// Deepest accepted file path, in components (including the root name).
const MAX_PATH_DEPTH: usize = 256;
pub(crate) const MAX_PIECE_LEN: i64 = 268_435_456;
/// Most tracker URLs kept across all tiers; later ones are ignored. The
/// session applies the same cap to trackers set by hand.
pub const MAX_TRACKERS: usize = 64;
/// Deepest nesting [`Metainfo::from_bytes`] accepts in a `.torrent`, the
/// root dictionary counting as one (the `fbt-bencode` default).
pub const MAX_TORRENT_DEPTH: usize = 64;
/// Most bencode values [`Metainfo::from_bytes`] accepts in a `.torrent`
/// (the `fbt-bencode` default).
pub const MAX_TORRENT_ITEMS: usize = 4_194_304;
/// Item cap when decoding an info dictionary received from a peer.
const MAX_INFO_ITEMS: usize = 4_194_304;
/// Nesting cap when decoding an info dictionary received from a peer.
const MAX_INFO_DEPTH: usize = 32;

/// Errors from [`Metainfo::from_bytes`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MetainfoError {
    /// The bencode itself is malformed.
    #[error("bencode: {0}")]
    Bencode(#[from] DecodeError),
    /// A required key is absent.
    #[error("missing field `{0}`")]
    MissingField(&'static str),
    /// A key has the wrong type or an out-of-range value.
    #[error("invalid field `{field}`: {reason}")]
    InvalidField {
        /// The offending key.
        field: &'static str,
        /// What is wrong with it.
        reason: &'static str,
    },
    /// Two files share a path, or a file path is a directory prefix of another.
    ///
    /// No longer produced: colliding paths are renamed at parse time. Kept
    /// for compatibility; it will be removed in a later breaking release.
    #[deprecated(note = "no longer produced: colliding paths are renamed at parse time")]
    #[error("duplicate path `{0}`")]
    DuplicatePath(String),
    /// A v2-only torrent (no v1 `pieces`).
    ///
    /// No longer produced: v2-only torrents are parsed. Kept for
    /// compatibility; it will be removed in a later breaking release.
    #[deprecated(note = "no longer produced: v2-only torrents are parsed")]
    #[error("v2-only torrents are not supported yet")]
    V2OnlyUnsupported,
    /// A hybrid torrent whose v1 and v2 halves describe different files, or
    /// whose files do not start on piece boundaries.
    #[error("hybrid torrent: the v1 and v2 file lists disagree")]
    InconsistentHybrid,
    /// The info dictionary does not hash to the expected info hash.
    #[error("info dictionary does not match the expected info hash")]
    InfoHashMismatch,
}

pub(crate) fn invalid(field: &'static str, reason: &'static str) -> MetainfoError {
    MetainfoError::InvalidField { field, reason }
}

/// One file of the torrent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    /// Sanitized relative path; for multi-file torrents the first component is
    /// the torrent name, for single-file torrents it is just the name.
    pub path: RelPath,
    /// Length in bytes.
    pub len: u64,
    /// BEP 47 padding file: occupies piece space but is never written to disk.
    pub pad: bool,
    /// BEP 52 `pieces root` of the file's Merkle tree: set for non-empty
    /// files of v2 and hybrid torrents, `None` for v1 torrents, pads and
    /// empty files.
    pub pieces_root: Option<[u8; 32]>,
}

/// The v1 `info` dictionary, validated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Info {
    /// Sanitized torrent name.
    pub name: String,
    /// Piece length in bytes.
    pub piece_len: u32,
    /// SHA-1 of each piece (empty for a v2-only torrent).
    pub pieces: Vec<[u8; 20]>,
    /// Files in torrent order (padding files included).
    pub files: Vec<FileEntry>,
    /// BEP 27 private flag.
    pub private: bool,
    /// True for a single-file torrent (`length` instead of `files`).
    pub single_file: bool,
}

/// A parsed `.torrent` file.
#[derive(Debug, Clone)]
pub struct Metainfo {
    /// Info hashes (v1 set for v1 and hybrid torrents, v2 for v2 and hybrid).
    pub info_hashes: InfoHashes,
    /// The validated info dictionary.
    pub info: Info,
    /// Announce tiers (de-duplicated, no empty tiers, at most 64 URLs in
    /// all: the first ones in tier order).
    pub trackers: Vec<Vec<String>>,
    /// BEP 19 `url-list` web seeds: `http` and `https` URLs only,
    /// de-duplicated, at most [`MAX_WEB_SEEDS`], each at most 2048 bytes.
    pub web_seeds: Vec<String>,
    /// Free-form comment.
    pub comment: Option<String>,
    /// Creating program.
    pub created_by: Option<String>,
    /// Creation time, seconds since the Unix epoch.
    pub creation_date: Option<i64>,
    /// Validated BEP 52 piece layers, keyed by `pieces root`. Empty for v1
    /// torrents and when the `.torrent` carried none.
    pub piece_layers: BTreeMap<[u8; 32], Vec<[u8; 32]>>,
    raw_info: Vec<u8>,
    layout: PieceLayout,
    v2_starts: Vec<(u64, usize)>,
}

impl Metainfo {
    /// Parses and validates `.torrent` bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Metainfo, MetainfoError> {
        let root = decode_with(bytes, Limits::new(MAX_TORRENT_DEPTH, MAX_TORRENT_ITEMS))?;
        let root = root
            .as_dict()
            .ok_or_else(|| invalid("torrent", "not a dictionary"))?;
        let info_val = root
            .get(b"info")
            .ok_or(MetainfoError::MissingField("info"))?;
        let parsed = parse_info(info_val)?;
        let piece_layers = match root.get(b"piece layers") {
            Some(v) if !parsed.v2_starts.is_empty() => {
                parse_piece_layers(v, &parsed.info.files, parsed.info.piece_len)?
            }
            _ => BTreeMap::new(),
        };

        Ok(Metainfo {
            info_hashes: parsed.info_hashes,
            info: parsed.info,
            trackers: parse_trackers(root),
            web_seeds: parse_web_seeds(root),
            comment: lossy(root, b"comment"),
            created_by: lossy(root, b"created by"),
            creation_date: root.get(b"creation date").and_then(Value::as_int),
            piece_layers,
            raw_info: parsed.raw_info,
            layout: parsed.layout,
            v2_starts: parsed.v2_starts,
        })
    }

    /// Builds a `Metainfo` from an `info` dictionary received from peers
    /// (BEP 9), verifying it against `expected` first.
    ///
    /// The bytes are decoded with explicit limits and parsed with the same
    /// rules as [`Metainfo::from_bytes`]. SHA-1 is checked against
    /// `expected.v1`, and SHA-256 against `expected.v2` when the dictionary
    /// declares `meta version` 2. `comment`, `created_by` and `creation_date`
    /// are `None`; `trackers` get the usual de-duplication and URL cap.
    pub fn from_info_bytes(
        info: &[u8],
        expected: InfoHashes,
        trackers: Vec<Vec<String>>,
    ) -> Result<Metainfo, MetainfoError> {
        // Cheap rejection first: hash the raw bytes before any parsing.
        if expected.v1.is_none() && expected.v2.is_none() {
            return Err(MetainfoError::InfoHashMismatch);
        }
        if expected.v1.is_some_and(|h| h != InfoHashV1::of(info))
            || expected.v2.is_some_and(|h| h != InfoHashV2::of(info))
        {
            return Err(MetainfoError::InfoHashMismatch);
        }
        let val = decode_with(info, Limits::new(MAX_INFO_DEPTH, MAX_INFO_ITEMS))?;
        let parsed = parse_info(&val)?;
        // A v2 hash only matches a dictionary that declares `meta version` 2.
        if expected.v1 != parsed.info_hashes.v1 && expected.v1.is_some()
            || expected.v2.is_some() && expected.v2 != parsed.info_hashes.v2
        {
            return Err(MetainfoError::InfoHashMismatch);
        }
        Ok(Metainfo {
            info_hashes: parsed.info_hashes,
            info: parsed.info,
            trackers: clean_trackers(trackers.iter().map(|t| t.iter().map(String::as_str))),
            web_seeds: Vec::new(),
            comment: None,
            created_by: None,
            creation_date: None,
            piece_layers: BTreeMap::new(),
            raw_info: parsed.raw_info,
            layout: parsed.layout,
            v2_starts: parsed.v2_starts,
        })
    }

    /// A magnet link for this torrent: info hashes, name, the first
    /// tracker of each tier and the web seeds.
    pub fn magnet(&self) -> Magnet {
        let mut m = Magnet::from_info_hashes(self.info_hashes);
        m.display_name = Some(self.info.name.clone());
        m.trackers = self
            .trackers
            .iter()
            .filter_map(|t| t.first().cloned())
            .collect();
        m.web_seeds = self.web_seeds.clone();
        m
    }

    /// Which metainfo versions this torrent carries.
    pub fn version(&self) -> TorrentVersion {
        match (self.info_hashes.v1.is_some(), self.info_hashes.v2.is_some()) {
            (true, true) => TorrentVersion::Hybrid,
            (false, true) => TorrentVersion::V2,
            _ => TorrentVersion::V1,
        }
    }

    /// How global piece `piece` is checked under BEP 52: its file, root,
    /// expected hash and geometry. `None` for v1 torrents, out-of-range
    /// pieces and pieces that hold no file bytes.
    pub fn v2_piece(&self, piece: u32) -> Option<V2Piece> {
        locate(
            &self.v2_starts,
            &self.info.files,
            &self.piece_layers,
            self.info.piece_len,
            piece,
        )
    }

    /// Merges `layers` into the piece layers held, keeping only those that
    /// validate for a file of this torrent (and fit [`MAX_PIECE_LAYER_HASHES`]).
    /// A layer already held is kept.
    #[must_use]
    pub fn with_piece_layers(mut self, layers: BTreeMap<[u8; 32], Vec<[u8; 32]>>) -> Metainfo {
        let targets = layer_targets(&self.info.files, self.info.piece_len);
        let mut total: usize = self.piece_layers.values().map(Vec::len).sum();
        for (root, layer) in layers {
            if self.piece_layers.contains_key(&root)
                || !layer_ok(&targets, self.info.piece_len, &root, &layer)
                || total.saturating_add(layer.len()) > MAX_PIECE_LAYER_HASHES
            {
                continue;
            }
            total += layer.len();
            self.piece_layers.insert(root, layer);
        }
        self
    }

    /// The exact bencoded `info` dictionary bytes (what the hashes cover).
    pub fn raw_info(&self) -> &[u8] {
        &self.raw_info
    }

    /// Piece geometry (validated at parse time).
    pub fn piece_layout(&self) -> PieceLayout {
        self.layout
    }

    /// File geometry over the concatenated content, padding files included.
    pub fn file_layout(&self) -> FileLayout {
        FileLayout::new(self.info.files.iter().map(|f| f.len))
    }

    /// Total content length in bytes.
    pub fn total_len(&self) -> u64 {
        self.layout.total_len()
    }
}

struct ParsedInfo {
    info_hashes: InfoHashes,
    info: Info,
    raw_info: Vec<u8>,
    layout: PieceLayout,
    v2_starts: Vec<(u64, usize)>,
}

/// Validates an `info` value; shared by `from_bytes` and `from_info_bytes`.
fn parse_info(info_val: &Value<'_>) -> Result<ParsedInfo, MetainfoError> {
    let info_dict = info_val
        .as_dict()
        .ok_or_else(|| invalid("info", "not a dictionary"))?;
    let raw_info = info_val.raw().to_vec();

    let v2 = match info_dict.get(b"meta version") {
        None => false,
        Some(v) => {
            v.as_int()
                .ok_or_else(|| invalid("meta version", "not an integer"))?
                == 2
        }
    };
    let pieces_raw = match info_dict.get(b"pieces") {
        Some(p) => Some(
            p.as_bytes()
                .ok_or_else(|| invalid("pieces", "not a byte string"))?,
        ),
        None if v2 => None,
        None => return Err(MetainfoError::MissingField("pieces")),
    };

    let info_hashes = match (v2, pieces_raw.is_some()) {
        (true, true) => InfoHashes::hybrid(InfoHashV1::of(&raw_info), InfoHashV2::of(&raw_info)),
        (true, false) => InfoHashes::from_v2(InfoHashV2::of(&raw_info)),
        _ => InfoHashes::from_v1(InfoHashV1::of(&raw_info)),
    };

    let piece_len = int_field(info_dict, b"piece length", "piece length")?;
    if !(1..=MAX_PIECE_LEN).contains(&piece_len) {
        return Err(invalid("piece length", "out of range"));
    }
    let piece_len =
        u32::try_from(piece_len).map_err(|_| invalid("piece length", "out of range"))?;
    if v2 && !valid_v2_piece_len(piece_len) {
        return Err(invalid(
            "piece length",
            "not a power of two of at least 16 KiB",
        ));
    }
    if pieces_raw.is_some_and(|p| p.len() % 20 != 0) {
        return Err(invalid("pieces", "length is not a multiple of 20"));
    }

    let name_bytes = info_dict
        .get(b"name")
        .ok_or(MetainfoError::MissingField("name"))?
        .as_bytes()
        .ok_or_else(|| invalid("name", "not a byte string"))?;
    let name = sanitize_component(name_bytes).unwrap_or_else(|| info_hashes.to_string());

    let private = matches!(info_dict.get(b"private").and_then(Value::as_int), Some(1));
    let v2_files = if v2 {
        let tree = info_dict
            .get(b"file tree")
            .ok_or(MetainfoError::MissingField("file tree"))?
            .as_dict()
            .ok_or_else(|| invalid("file tree", "not a dictionary"))?;
        Some(parse_file_tree(tree, &name, name_bytes, piece_len)?)
    } else {
        None
    };
    let (mut files, single_file) = match (pieces_raw, v2_files) {
        (Some(_), None) => parse_files(info_dict, &name)?,
        (Some(_), Some((tree_files, _))) => {
            let (v1_files, single) = parse_files(info_dict, &name)?;
            (merge_hybrid(v1_files, &tree_files, piece_len)?, single)
        }
        (None, Some(parsed)) => parsed,
        (None, None) => return Err(MetainfoError::MissingField("pieces")),
    };

    let mut total: u64 = 0;
    for f in &files {
        total = total
            .checked_add(f.len)
            .ok_or_else(|| invalid("length", "total length overflows"))?;
    }
    let layout = PieceLayout::new(total, piece_len)
        .map_err(|_| invalid("length", "total length is zero or too large"))?;
    if let Some(p) = pieces_raw
        && usize::try_from(layout.num_pieces()).ok() != Some(p.len() / 20)
    {
        return Err(invalid("pieces", "count does not match total length"));
    }
    // Without `pieces` nothing else bounds the count: every per-piece
    // vector of the session is sized from it.
    if pieces_raw.is_none()
        && usize::try_from(layout.num_pieces()).map_or(true, |n| n > MAX_V2_PIECES)
    {
        return Err(invalid("length", "too many pieces"));
    }
    resolve_collisions(&mut files);

    let pieces: Vec<[u8; 20]> = pieces_raw
        .map(|p| p.as_chunks::<20>().0.to_vec())
        .unwrap_or_default();
    let v2_starts = v2_file_starts(&files);

    Ok(ParsedInfo {
        info_hashes,
        info: Info {
            name,
            piece_len,
            pieces,
            files,
            private,
            single_file,
        },
        raw_info,
        layout,
        v2_starts,
    })
}

fn int_field(d: &Dict<'_>, key: &[u8], field: &'static str) -> Result<i64, MetainfoError> {
    d.get(key)
        .ok_or(MetainfoError::MissingField(field))?
        .as_int()
        .ok_or_else(|| invalid(field, "not an integer"))
}

fn length_field(d: &Dict<'_>) -> Result<u64, MetainfoError> {
    u64::try_from(int_field(d, b"length", "length")?).map_err(|_| invalid("length", "negative"))
}

fn parse_files(info: &Dict<'_>, name: &str) -> Result<(Vec<FileEntry>, bool), MetainfoError> {
    let Some(list) = info.get(b"files") else {
        let len = length_field(info)?;
        let file = FileEntry {
            path: RelPath::single(name.to_string()),
            len,
            pad: false,
            pieces_root: None,
        };
        return Ok((vec![file], true));
    };
    let list = list
        .as_list()
        .ok_or_else(|| invalid("files", "not a list"))?;
    if list.is_empty() {
        return Err(invalid("files", "empty"));
    }
    let mut out = Vec::with_capacity(list.len());
    for item in list {
        let d = item
            .as_dict()
            .ok_or_else(|| invalid("files", "entry is not a dictionary"))?;
        let mut len = length_field(d)?;
        let comps = d
            .get(b"path")
            .ok_or(MetainfoError::MissingField("path"))?
            .as_list()
            .ok_or_else(|| invalid("path", "not a list"))?;
        let raw: Vec<&[u8]> = comps
            .iter()
            .map(|c| {
                c.as_bytes()
                    .ok_or_else(|| invalid("path", "component is not a byte string"))
            })
            .collect::<Result<_, _>>()?;
        let path = sanitize_path(raw)
            .map_err(|_| invalid("path", "empty after sanitization"))?
            .with_root(name.to_string());
        if path.components().len() > MAX_PATH_DEPTH {
            return Err(invalid("path", "too deep"));
        }
        let attr = match d.get(b"attr") {
            None => &b""[..],
            Some(a) => a
                .as_bytes()
                .ok_or_else(|| invalid("attr", "not a byte string"))?,
        };
        let symlink = attr.contains(&b'l');
        if symlink {
            // Never materialised; kept as an empty regular entry.
            len = 0;
        }
        out.push(FileEntry {
            path,
            len,
            pad: !symlink && attr.contains(&b'p'),
            pieces_root: None,
        });
    }
    Ok((out, false))
}

/// Keeps the `http` and `https` URLs of `urls`: at most [`MAX_URL_LEN`] bytes
/// each, de-duplicated, at most [`MAX_WEB_SEEDS`] in all (the first ones).
pub(crate) fn clean_web_seeds<'a>(urls: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for url in urls {
        if out.len() >= MAX_WEB_SEEDS {
            break;
        }
        if is_web_url(url) && !out.iter().any(|u| u == url) {
            out.push(url.to_string());
        }
    }
    out
}

/// An `http://` or `https://` URL (scheme in any case) with something after
/// the scheme, within [`MAX_URL_LEN`].
fn is_web_url(url: &str) -> bool {
    if url.len() > MAX_URL_LEN {
        return false;
    }
    ["http://", "https://"].iter().any(|scheme| {
        url.get(..scheme.len())
            .is_some_and(|h| h.eq_ignore_ascii_case(scheme))
            && url.len() > scheme.len()
    })
}

/// `url-list` (BEP 19): one URL string or a list of them.
fn parse_web_seeds(root: &Dict<'_>) -> Vec<String> {
    let Some(v) = root.get(b"url-list") else {
        return Vec::new();
    };
    match v.as_list() {
        Some(items) => clean_web_seeds(items.iter().filter_map(Value::as_str)),
        None => clean_web_seeds(v.as_str()),
    }
}

fn lossy(root: &Dict<'_>, key: &[u8]) -> Option<String> {
    root.get(key)
        .and_then(Value::as_bytes)
        .map(|b| String::from_utf8_lossy(b).into_owned())
}

fn parse_trackers(root: &Dict<'_>) -> Vec<Vec<String>> {
    let tiers = root
        .get(b"announce-list")
        .and_then(Value::as_list)
        .map(|list| {
            clean_trackers(
                list.iter()
                    .map(|t| t.as_list().unwrap_or(&[]).iter().filter_map(Value::as_str)),
            )
        })
        .unwrap_or_default();
    if tiers.is_empty()
        && let Some(a) = root.get(b"announce").and_then(Value::as_str)
        && !a.is_empty()
    {
        return vec![vec![a.to_string()]];
    }
    tiers
}

/// Drops empty and duplicate URLs and empty tiers, keeping at most
/// `MAX_TRACKERS` URLs in tier order; only kept URLs are copied.
fn clean_trackers<'a, T, U>(tiers: T) -> Vec<Vec<String>>
where
    T: IntoIterator<Item = U>,
    U: IntoIterator<Item = &'a str>,
{
    let mut seen: HashSet<&str> = HashSet::new();
    let mut out = Vec::new();
    for tier in tiers {
        let room = MAX_TRACKERS.saturating_sub(seen.len());
        let urls: Vec<String> = tier
            .into_iter()
            .filter(|u| !u.is_empty() && seen.insert(u))
            .take(room)
            .map(str::to_string)
            .collect();
        if !urls.is_empty() {
            out.push(urls);
        }
    }
    out
}
