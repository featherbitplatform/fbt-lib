//! Torrent creation: v1, v2 and hybrid `.torrent` files from a file or a
//! directory, shaped like libtorrent's `create_torrent`.
//!
//! The files are hashed in one pass. A v1 hash covers the padded byte stream
//! (hybrids get BEP 47 pads so every non-empty file starts on a piece
//! boundary, and the last one ends on one, as libtorrent 2.0 does), a v2
//! tree covers each file's own bytes in 16 KiB leaves.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Read};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

use fbt_bencode::{DictBuilder, OwnedValue, encode};
use fbt_core::merkle::{Hash256, MERKLE_BLOCK_LEN, file_tree_height, layer_root};
use sha1::{Digest as _, Sha1};
use sha2::Sha256;

use crate::metainfo::MAX_PIECE_LEN;
use crate::path::sanitize_component;
use crate::v2::{TorrentVersion, valid_v2_piece_len};

/// Most files one torrent is created from.
pub const MAX_CREATE_FILES: usize = 1_000_000;

/// Most pieces [`auto_piece_len`] aims for.
const AUTO_MAX_PIECES: u64 = 2048;
/// Largest piece length [`auto_piece_len`] picks.
const AUTO_MAX_PIECE_LEN: u32 = 16 << 20;

/// Why a torrent could not be created.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CreateError {
    /// Reading `path` failed.
    #[error("{}: {source}", path.display())]
    Io {
        /// The file or directory.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
    /// There is no file with content to hash.
    #[error("nothing to hash: no files, or only empty ones")]
    Empty,
    /// A name is not valid UTF-8 or cannot be used as a path component.
    #[error("unusable file name: {}", .0.display())]
    InvalidName(PathBuf),
    /// A file changed length while the torrent was being created.
    #[error("file changed while hashing: {}", .0.display())]
    FileChanged(PathBuf),
    /// The piece length is not a power of two between 16 KiB and the maximum.
    #[error("invalid piece length {0}")]
    InvalidPieceLen(u32),
    /// More than [`MAX_CREATE_FILES`] files.
    #[error("too many files")]
    TooManyFiles,
    /// The progress callback asked to stop.
    #[error("cancelled")]
    Cancelled,
}

/// Builds a `.torrent` from a file or a directory.
///
/// Hashing is synchronous and reads every byte: async callers run
/// [`CreateTorrent::build`] on `spawn_blocking`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CreateTorrent {
    path: PathBuf,
    version: TorrentVersion,
    piece_len: Option<u32>,
    trackers: Vec<Vec<String>>,
    web_seeds: Vec<String>,
    comment: Option<String>,
    created_by: Option<String>,
    creation_date: Option<i64>,
    private: bool,
}

impl CreateTorrent {
    /// A creator for `path` (a file or a directory), producing a hybrid
    /// torrent with an automatic piece length.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            version: TorrentVersion::Hybrid,
            piece_len: None,
            trackers: Vec::new(),
            web_seeds: Vec::new(),
            comment: None,
            created_by: None,
            creation_date: None,
            private: false,
        }
    }

    /// Which metainfo versions to write (default: hybrid).
    #[must_use]
    pub fn version(mut self, version: TorrentVersion) -> Self {
        self.version = version;
        self
    }

    /// The piece length: a power of two of at least 16 KiB (default: see
    /// [`auto_piece_len`]).
    #[must_use]
    pub fn piece_len(mut self, piece_len: u32) -> Self {
        self.piece_len = Some(piece_len);
        self
    }

    /// Announce tiers.
    #[must_use]
    pub fn trackers(mut self, trackers: Vec<Vec<String>>) -> Self {
        self.trackers = trackers;
        self
    }

    /// BEP 19 web seed URLs.
    #[must_use]
    pub fn web_seeds(mut self, web_seeds: Vec<String>) -> Self {
        self.web_seeds = web_seeds;
        self
    }

    /// A free-form comment.
    #[must_use]
    pub fn comment(mut self, comment: String) -> Self {
        self.comment = Some(comment);
        self
    }

    /// The creating program. Left out unless set.
    #[must_use]
    pub fn created_by(mut self, created_by: String) -> Self {
        self.created_by = Some(created_by);
        self
    }

    /// Creation time in seconds since the Unix epoch. Left out unless set, so
    /// output is deterministic.
    #[must_use]
    pub fn creation_date(mut self, creation_date: i64) -> Self {
        self.creation_date = Some(creation_date);
        self
    }

    /// The BEP 27 private flag.
    #[must_use]
    pub fn private(mut self, private: bool) -> Self {
        self.private = private;
        self
    }

    /// Hashes the files and returns the bencoded `.torrent`.
    ///
    /// `progress(done_bytes, total_bytes)` is called after each piece; it
    /// may return `Break` to stop with [`CreateError::Cancelled`]. Symlinks
    /// are skipped, hidden files are kept, files are ordered by their path
    /// components bytewise. Nothing is written to disk.
    pub fn build(
        &self,
        progress: &mut dyn FnMut(u64, u64) -> ControlFlow<()>,
    ) -> Result<Vec<u8>, CreateError> {
        if let Some(pl) = self.piece_len
            && !(valid_v2_piece_len(pl) && i64::from(pl) <= MAX_PIECE_LEN)
        {
            return Err(CreateError::InvalidPieceLen(pl));
        }
        let (name, files, single) = collect(&self.path)?;
        let total: u64 = files.iter().map(|f| f.len).sum();
        if total == 0 {
            return Err(CreateError::Empty);
        }
        let piece_len = self.piece_len.unwrap_or_else(|| auto_piece_len(total));
        let v1 = self.version != TorrentVersion::V2;
        let v2 = self.version != TorrentVersion::V1;
        let pads = self.version == TorrentVersion::Hybrid;

        let mut hasher = Hasher {
            v1: v1.then(|| V1Stream::new(piece_len)),
            v2,
            piece_len,
            progress: Progress {
                cb: progress,
                done: 0,
                total,
            },
        };
        let mut hashed = Vec::with_capacity(files.len());
        let mut v1_files: Vec<OwnedValue> = Vec::new();
        let mut offset: u64 = 0;
        let pl = u64::from(piece_len);
        for f in &files {
            if pads && f.len > 0 && !offset.is_multiple_of(pl) {
                let pad = pl - offset % pl;
                hasher.pad(pad)?;
                v1_files.push(file_entry(
                    pad,
                    &[".pad".to_string(), pad.to_string()],
                    true,
                ));
                offset += pad;
            }
            hashed.push(hasher.file(f)?);
            v1_files.push(file_entry(f.len, &f.comps, false));
            offset += f.len;
        }
        // libtorrent 2.0 pads the last file of a hybrid to a piece boundary
        // too (measured against qBittorrent 5.3). A single-file torrent has
        // no file list to hold a pad.
        if pads && !single && !offset.is_multiple_of(pl) {
            let pad = pl - offset % pl;
            hasher.pad(pad)?;
            v1_files.push(file_entry(
                pad,
                &[".pad".to_string(), pad.to_string()],
                true,
            ));
        }
        hasher.finish()?;
        // Lengths are re-checked once, after everything is hashed.
        for f in &files {
            let len = fs::metadata(&f.path)
                .map_err(|source| CreateError::Io {
                    path: f.path.clone(),
                    source,
                })?
                .len();
            if len != f.len {
                return Err(CreateError::FileChanged(f.path.clone()));
            }
        }

        let mut info = DictBuilder::new();
        info.insert(b"name", name.as_str());
        info.insert(b"piece length", i64::from(piece_len));
        if self.private {
            info.insert(b"private", 1i64);
        }
        if let Some(v1s) = hasher.v1 {
            info.insert(b"pieces", v1s.pieces);
            if single {
                info.insert(b"length", len_value(total));
            } else {
                info.insert(b"files", OwnedValue::List(v1_files));
            }
        }
        let mut layers: BTreeMap<Vec<u8>, OwnedValue> = BTreeMap::new();
        if v2 {
            info.insert(b"meta version", 2i64);
            let mut tree = BTreeMap::new();
            for (f, h) in files.iter().zip(&hashed) {
                let mut attrs = DictBuilder::new();
                attrs.insert(b"length", len_value(f.len));
                if f.len > 0 {
                    attrs.insert(b"pieces root", h.root.to_vec());
                }
                if !h.layer.is_empty() {
                    layers.insert(h.root.to_vec(), OwnedValue::Bytes(h.layer.concat()));
                }
                insert_leaf(&mut tree, &f.comps, attrs.build());
            }
            info.insert(b"file tree", OwnedValue::Dict(tree));
        }

        let mut root = DictBuilder::new();
        let tiers: Vec<&Vec<String>> = self.trackers.iter().filter(|t| !t.is_empty()).collect();
        if let Some(first) = tiers.first().and_then(|t| t.first()) {
            root.insert(b"announce", first.as_str());
            root.insert(
                b"announce-list",
                OwnedValue::List(
                    tiers
                        .iter()
                        .map(|t| {
                            OwnedValue::List(
                                t.iter().map(|u| OwnedValue::from(u.as_str())).collect(),
                            )
                        })
                        .collect(),
                ),
            );
        }
        if let Some(c) = &self.comment {
            root.insert(b"comment", c.as_str());
        }
        if let Some(c) = &self.created_by {
            root.insert(b"created by", c.as_str());
        }
        if let Some(d) = self.creation_date {
            root.insert(b"creation date", d);
        }
        root.insert(b"info", info.build());
        if v2 {
            root.insert(b"piece layers", OwnedValue::Dict(layers));
        }
        if !self.web_seeds.is_empty() {
            root.insert(
                b"url-list",
                OwnedValue::List(
                    self.web_seeds
                        .iter()
                        .map(|u| OwnedValue::from(u.as_str()))
                        .collect(),
                ),
            );
        }
        Ok(encode(&root.build()))
    }
}

/// The smallest piece length (a power of two, at least 16 KiB) that gives at
/// most 2048 pieces for `total` bytes, capped at 16 MiB.
pub fn auto_piece_len(total: u64) -> u32 {
    let mut len = MERKLE_BLOCK_LEN;
    while len < AUTO_MAX_PIECE_LEN && total.div_ceil(u64::from(len)) > AUTO_MAX_PIECES {
        len <<= 1;
    }
    len
}

fn len_value(len: u64) -> i64 {
    i64::try_from(len).unwrap_or(i64::MAX)
}

fn file_entry(len: u64, comps: &[String], pad: bool) -> OwnedValue {
    let mut d = DictBuilder::new();
    d.insert(b"length", len_value(len));
    d.insert(
        b"path",
        OwnedValue::List(comps.iter().map(|c| OwnedValue::from(c.as_str())).collect()),
    );
    if pad {
        d.insert(b"attr", "p");
    }
    d.build()
}

/// Puts a file node (`{"": attrs}`) at `comps` in the nested `file tree`.
fn insert_leaf(node: &mut BTreeMap<Vec<u8>, OwnedValue>, comps: &[String], attrs: OwnedValue) {
    match comps {
        [] => {}
        [last] => {
            let mut file = BTreeMap::new();
            file.insert(Vec::new(), attrs);
            node.insert(last.as_bytes().to_vec(), OwnedValue::Dict(file));
        }
        [first, rest @ ..] => {
            let entry = node
                .entry(first.as_bytes().to_vec())
                .or_insert_with(|| OwnedValue::Dict(BTreeMap::new()));
            if let OwnedValue::Dict(child) = entry {
                insert_leaf(child, rest, attrs);
            }
        }
    }
}

struct Entry {
    comps: Vec<String>,
    path: PathBuf,
    len: u64,
}

fn io_err(path: &Path) -> impl FnOnce(io::Error) -> CreateError + '_ {
    move |source| CreateError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn utf8_name(path: &Path, name: &std::ffi::OsStr) -> Result<String, CreateError> {
    let s = name
        .to_str()
        .ok_or_else(|| CreateError::InvalidName(path.to_path_buf()))?;
    sanitize_component(s.as_bytes()).ok_or_else(|| CreateError::InvalidName(path.to_path_buf()))?;
    Ok(s.to_string())
}

/// The torrent name, the files in order, and whether the input is one file.
fn collect(root: &Path) -> Result<(String, Vec<Entry>, bool), CreateError> {
    let meta = fs::metadata(root).map_err(io_err(root))?;
    let leaf = match root.file_name() {
        Some(n) => n.to_os_string(),
        None => fs::canonicalize(root)
            .map_err(io_err(root))?
            .file_name()
            .map(|n| n.to_os_string())
            .ok_or_else(|| CreateError::InvalidName(root.to_path_buf()))?,
    };
    let name = utf8_name(root, &leaf)?;
    if meta.is_file() {
        let entry = Entry {
            comps: vec![name.clone()],
            path: root.to_path_buf(),
            len: meta.len(),
        };
        return Ok((name, vec![entry], true));
    }
    let mut files = Vec::new();
    walk(root, &mut Vec::new(), &mut files)?;
    files.sort_by(|a, b| a.comps.cmp(&b.comps));
    Ok((name, files, false))
}

fn walk(dir: &Path, comps: &mut Vec<String>, out: &mut Vec<Entry>) -> Result<(), CreateError> {
    for item in fs::read_dir(dir).map_err(io_err(dir))? {
        let item = item.map_err(io_err(dir))?;
        let path = item.path();
        let meta = fs::symlink_metadata(&path).map_err(io_err(&path))?;
        if meta.is_symlink() {
            continue;
        }
        let name = utf8_name(&path, &item.file_name())?;
        if meta.is_dir() {
            comps.push(name);
            let r = walk(&path, comps, out);
            comps.pop();
            r?;
        } else if meta.is_file() {
            if out.len() >= MAX_CREATE_FILES {
                return Err(CreateError::TooManyFiles);
            }
            let mut c = comps.clone();
            c.push(name);
            out.push(Entry {
                comps: c,
                path,
                len: meta.len(),
            });
        }
    }
    Ok(())
}

struct Progress<'a> {
    cb: &'a mut dyn FnMut(u64, u64) -> ControlFlow<()>,
    done: u64,
    total: u64,
}

impl Progress<'_> {
    fn tick(&mut self) -> Result<(), CreateError> {
        match (self.cb)(self.done, self.total) {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(()) => Err(CreateError::Cancelled),
        }
    }
}

/// SHA-1 over the (padded) byte stream, one digest per piece.
struct V1Stream {
    sha: Sha1,
    fill: usize,
    piece_len: usize,
    pieces: Vec<u8>,
}

impl V1Stream {
    fn new(piece_len: u32) -> Self {
        Self {
            sha: Sha1::new(),
            fill: 0,
            piece_len: usize::try_from(piece_len).unwrap_or(usize::MAX),
            pieces: Vec::new(),
        }
    }

    /// Feeds bytes; true when at least one piece completed.
    fn feed(&mut self, mut data: &[u8]) -> bool {
        let mut completed = false;
        while !data.is_empty() {
            let room = self.piece_len - self.fill;
            let (now, rest) = data.split_at(room.min(data.len()));
            self.sha.update(now);
            self.fill += now.len();
            data = rest;
            if self.fill == self.piece_len {
                self.flush();
                completed = true;
            }
        }
        completed
    }

    fn flush(&mut self) {
        let digest = std::mem::take(&mut self.sha).finalize();
        self.pieces.extend_from_slice(&digest);
        self.fill = 0;
    }
}

struct FileHash {
    root: Hash256,
    /// Piece layer: empty for a file of at most one piece.
    layer: Vec<Hash256>,
}

struct Hasher<'a> {
    v1: Option<V1Stream>,
    v2: bool,
    piece_len: u32,
    progress: Progress<'a>,
}

impl Hasher<'_> {
    fn pad(&mut self, mut len: u64) -> Result<(), CreateError> {
        const ZEROS: [u8; MERKLE_BLOCK_LEN as usize] = [0; MERKLE_BLOCK_LEN as usize];
        while len > 0 {
            let n = len.min(u64::from(MERKLE_BLOCK_LEN));
            len -= n;
            let take = usize::try_from(n).unwrap_or(ZEROS.len());
            let completed = self
                .v1
                .as_mut()
                .is_some_and(|s| s.feed(ZEROS.get(..take).unwrap_or(&ZEROS)));
            if completed {
                self.progress.tick()?;
            }
        }
        Ok(())
    }

    fn file(&mut self, f: &Entry) -> Result<FileHash, CreateError> {
        let mut input = fs::File::open(&f.path).map_err(io_err(&f.path))?;
        let leaves_per_piece = (self.piece_len / MERKLE_BLOCK_LEN) as usize;
        let multi = f.len > u64::from(self.piece_len);
        let mut leaves: Vec<Hash256> = Vec::new();
        let mut layer: Vec<Hash256> = Vec::new();
        let mut block = vec![0u8; MERKLE_BLOCK_LEN as usize];
        let mut left = f.len;
        while left > 0 {
            let n = left.min(u64::from(MERKLE_BLOCK_LEN));
            let buf = block
                .get_mut(..usize::try_from(n).unwrap_or(0))
                .unwrap_or(&mut []);
            input.read_exact(buf).map_err(|e| {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    CreateError::FileChanged(f.path.clone())
                } else {
                    io_err(&f.path)(e)
                }
            })?;
            left -= n;
            self.progress.done += n;
            let mut piece_done = false;
            if self.v2 {
                leaves.push(Sha256::digest(&*buf).into());
                if multi && leaves.len() == leaves_per_piece {
                    layer.push(layer_root(&leaves, 0, leaves_per_piece));
                    leaves.clear();
                    piece_done = true;
                }
            }
            if let Some(s) = self.v1.as_mut() {
                piece_done = s.feed(buf);
            }
            if piece_done {
                self.progress.tick()?;
            }
        }
        if !self.v2 {
            return Ok(FileHash {
                root: [0; 32],
                layer,
            });
        }
        let root = if multi {
            if !leaves.is_empty() {
                layer.push(layer_root(&leaves, 0, leaves_per_piece));
                if self.v1.is_none() {
                    self.progress.tick()?;
                }
            }
            let height = leaves_per_piece.trailing_zeros();
            layer_root(&layer, height, layer.len())
        } else {
            if self.v1.is_none() && !leaves.is_empty() {
                self.progress.tick()?;
            }
            layer_root(&leaves, 0, 1usize << file_tree_height(f.len))
        };
        Ok(FileHash { root, layer })
    }

    /// Completes the last, short v1 piece.
    fn finish(&mut self) -> Result<(), CreateError> {
        if let Some(s) = self.v1.as_mut()
            && s.fill > 0
        {
            s.flush();
            self.progress.tick()?;
        }
        Ok(())
    }
}
