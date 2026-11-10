//! v1 and hybrid `.torrent` parsing. The input is untrusted.

use std::collections::HashSet;

use fbt_bencode::{DecodeError, Dict, Value, decode};
use fbt_core::{FileLayout, InfoHashV1, InfoHashV2, InfoHashes, PieceLayout};

use crate::path::{RelPath, sanitize_component, sanitize_path};

/// Deepest accepted file path, in components (including the root name).
const MAX_PATH_DEPTH: usize = 256;
const MAX_PIECE_LEN: i64 = 268_435_456;

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
    #[error("duplicate path `{0}`")]
    DuplicatePath(String),
    /// A v2-only torrent (no v1 `pieces`); supported from milestone 6.
    #[error("v2-only torrents are not supported yet")]
    V2OnlyUnsupported,
}

fn invalid(field: &'static str, reason: &'static str) -> MetainfoError {
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
}

/// The v1 `info` dictionary, validated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Info {
    /// Sanitized torrent name.
    pub name: String,
    /// Piece length in bytes.
    pub piece_len: u32,
    /// SHA-1 of each piece.
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
    /// Info hashes (v1 always set; v2 set for hybrid torrents).
    pub info_hashes: InfoHashes,
    /// The validated info dictionary.
    pub info: Info,
    /// Announce tiers (de-duplicated, no empty tiers).
    pub trackers: Vec<Vec<String>>,
    /// Free-form comment.
    pub comment: Option<String>,
    /// Creating program.
    pub created_by: Option<String>,
    /// Creation time, seconds since the Unix epoch.
    pub creation_date: Option<i64>,
    raw_info: Vec<u8>,
    layout: PieceLayout,
}

impl Metainfo {
    /// Parses and validates `.torrent` bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Metainfo, MetainfoError> {
        let root = decode(bytes)?;
        let root = root
            .as_dict()
            .ok_or_else(|| invalid("torrent", "not a dictionary"))?;
        let info_val = root
            .get(b"info")
            .ok_or(MetainfoError::MissingField("info"))?;
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
            Some(p) => p
                .as_bytes()
                .ok_or_else(|| invalid("pieces", "not a byte string"))?,
            None if v2 => return Err(MetainfoError::V2OnlyUnsupported),
            None => return Err(MetainfoError::MissingField("pieces")),
        };

        let v1 = InfoHashV1::of(&raw_info);
        let info_hashes = if v2 {
            InfoHashes::hybrid(v1, InfoHashV2::of(&raw_info))
        } else {
            InfoHashes::from_v1(v1)
        };

        let piece_len = int_field(info_dict, b"piece length", "piece length")?;
        if !(1..=MAX_PIECE_LEN).contains(&piece_len) {
            return Err(invalid("piece length", "out of range"));
        }
        let piece_len =
            u32::try_from(piece_len).map_err(|_| invalid("piece length", "out of range"))?;
        if pieces_raw.len() % 20 != 0 {
            return Err(invalid("pieces", "length is not a multiple of 20"));
        }

        let name_bytes = info_dict
            .get(b"name")
            .ok_or(MetainfoError::MissingField("name"))?
            .as_bytes()
            .ok_or_else(|| invalid("name", "not a byte string"))?;
        let name = sanitize_component(name_bytes).unwrap_or_else(|| v1.to_string());

        let private = matches!(info_dict.get(b"private").and_then(Value::as_int), Some(1));
        let (files, single_file) = parse_files(info_dict, &name)?;

        let mut total: u64 = 0;
        for f in &files {
            total = total
                .checked_add(f.len)
                .ok_or_else(|| invalid("length", "total length overflows"))?;
        }
        let layout = PieceLayout::new(total, piece_len)
            .map_err(|_| invalid("length", "total length is zero or too large"))?;
        if usize::try_from(layout.num_pieces()).ok() != Some(pieces_raw.len() / 20) {
            return Err(invalid("pieces", "count does not match total length"));
        }
        check_duplicates(&files)?;

        let pieces: Vec<[u8; 20]> = pieces_raw.as_chunks::<20>().0.to_vec();

        Ok(Metainfo {
            info_hashes,
            info: Info {
                name,
                piece_len,
                pieces,
                files,
                private,
                single_file,
            },
            trackers: parse_trackers(root),
            comment: lossy(root, b"comment"),
            created_by: lossy(root, b"created by"),
            creation_date: root.get(b"creation date").and_then(Value::as_int),
            raw_info,
            layout,
        })
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
        });
    }
    Ok((out, false))
}

/// Rejects equal paths and a file that is a directory prefix of another,
/// comparing components ASCII-case-insensitively (APFS/NTFS collide on case).
/// Padding files are exempt: writers legitimately repeat `.pad/<len>`.
///
/// Sorted lexicographically, every extension of a path sits right after it, so
/// comparing adjacent pairs finds all equal-or-prefix collisions.
fn check_duplicates(files: &[FileEntry]) -> Result<(), MetainfoError> {
    let mut keys: Vec<(Vec<String>, &FileEntry)> = files
        .iter()
        .filter(|f| !f.pad)
        .map(|f| {
            let k = f
                .path
                .components()
                .iter()
                .map(|c| c.to_ascii_lowercase())
                .collect();
            (k, f)
        })
        .collect();
    keys.sort_by(|a, b| a.0.cmp(&b.0));
    for pair in keys.windows(2) {
        if let [(a, fa), (b, _)] = pair
            && b.starts_with(a)
        {
            return Err(MetainfoError::DuplicatePath(fa.path.to_string()));
        }
    }
    Ok(())
}

fn lossy(root: &Dict<'_>, key: &[u8]) -> Option<String> {
    root.get(key)
        .and_then(Value::as_bytes)
        .map(|b| String::from_utf8_lossy(b).into_owned())
}

fn parse_trackers(root: &Dict<'_>) -> Vec<Vec<String>> {
    let mut seen = HashSet::new();
    let mut tiers = Vec::new();
    if let Some(list) = root.get(b"announce-list").and_then(Value::as_list) {
        for tier in list {
            let urls: Vec<String> = tier
                .as_list()
                .unwrap_or(&[])
                .iter()
                .filter_map(Value::as_str)
                .filter(|u| !u.is_empty() && seen.insert(u.to_string()))
                .map(str::to_string)
                .collect();
            if !urls.is_empty() {
                tiers.push(urls);
            }
        }
    }
    if tiers.is_empty()
        && let Some(a) = root.get(b"announce").and_then(Value::as_str)
        && !a.is_empty()
    {
        tiers.push(vec![a.to_string()]);
    }
    tiers
}
