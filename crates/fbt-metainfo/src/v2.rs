//! BEP 52 (v2 and hybrid) parsing: the file tree, piece layers and the
//! geometry of v2 pieces. The input is untrusted.

use std::collections::BTreeMap;

use fbt_bencode::{Dict, Value};
use fbt_core::merkle::{MERKLE_BLOCK_LEN, file_tree_height, layer_root};

use crate::metainfo::{FileEntry, MetainfoError, invalid};
use crate::path::{RelPath, sanitize_path};

/// Most piece layer hashes kept in all, across every file.
pub const MAX_PIECE_LAYER_HASHES: usize = 4_194_304;

/// Which BitTorrent metainfo versions a torrent carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum TorrentVersion {
    /// BEP 3 only: SHA-1 pieces.
    V1,
    /// BEP 52 only: SHA-256 Merkle trees.
    V2,
    /// Both, with one set of files and two info hashes.
    #[default]
    Hybrid,
}

/// How one piece is checked in a v2 or hybrid torrent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V2Piece {
    /// Index into `Info::files` of the file the piece belongs to.
    pub file: usize,
    /// The file's `pieces root`.
    pub pieces_root: [u8; 32],
    /// Index of the piece within its file.
    pub index_in_file: u32,
    /// The expected subtree root: the layer entry, or `pieces_root` for a
    /// one-piece file, or `None` when the file's layer is missing.
    pub expected: Option<[u8; 32]>,
    /// The width (in 16 KiB leaves) the subtree is padded to: the piece
    /// length over 16 KiB for a file with a layer, the file's own tree width
    /// for a one-piece file.
    pub leaves: u32,
    /// Bytes of the file in this piece. The root is computed over the
    /// piece's data truncated to this length, never over pad bytes.
    pub len: u32,
}

/// True for a piece length BEP 52 allows: a power of two of at least 16 KiB.
pub(crate) fn valid_v2_piece_len(piece_len: u32) -> bool {
    piece_len >= MERKLE_BLOCK_LEN && piece_len.is_power_of_two()
}

/// Whether `layer` is the piece layer of a file with `pieces_root`: the
/// right number of hashes for `file_len` and `piece_len`, rooting at
/// `pieces_root` once padded. A file of at most one piece has no layer.
pub fn validate_layer(
    pieces_root: &[u8; 32],
    file_len: u64,
    piece_len: u32,
    layer: &[[u8; 32]],
) -> bool {
    if !valid_v2_piece_len(piece_len) || file_len <= u64::from(piece_len) {
        return false;
    }
    let expected = file_len.div_ceil(u64::from(piece_len));
    if u64::try_from(layer.len()).ok() != Some(expected) {
        return false;
    }
    let piece_height = (piece_len / MERKLE_BLOCK_LEN).trailing_zeros();
    layer_root(layer, piece_height, layer.len()) == *pieces_root
}

/// One file read from a tree, before pads are placed.
struct Leaf {
    path: RelPath,
    len: u64,
    root: Option<[u8; 32]>,
    depth: usize,
}

/// Parses a v2 `file tree` into files in key order, with a synthetic
/// `.pad/<len>` entry before every non-empty file that would not start on a
/// piece boundary. The flag is true for a single-file torrent: exactly one
/// top-level entry, a file whose raw key equals the raw `name` (libtorrent's
/// rule); its path is then just `name`. Anything else is a directory.
pub(crate) fn parse_file_tree(
    tree: &Dict<'_>,
    name: &str,
    raw_name: &[u8],
    piece_len: u32,
) -> Result<(Vec<FileEntry>, bool), MetainfoError> {
    if tree.get(b"").is_some() {
        return Err(invalid("file tree", "file at the top level has no name"));
    }
    let mut leaves = Vec::new();
    let mut comps: Vec<&[u8]> = Vec::new();
    walk_dir(tree, &mut comps, &mut leaves)?;
    if leaves.is_empty() {
        return Err(invalid("file tree", "empty"));
    }
    let single = matches!(leaves.as_slice(), [only] if only.depth == 1)
        && tree.iter().next().is_some_and(|(k, _)| k == raw_name);

    let mut out = Vec::with_capacity(leaves.len());
    let mut offset: u64 = 0;
    let piece_len = u64::from(piece_len);
    for leaf in leaves {
        if leaf.len > 0 && !offset.is_multiple_of(piece_len) {
            let pad_len = piece_len - offset % piece_len;
            let pad_name = pad_len.to_string();
            let path = sanitize_path([&b".pad"[..], pad_name.as_bytes()])
                .map_err(|_| invalid("file tree", "bad pad path"))?
                .with_root(name.to_string());
            out.push(FileEntry {
                path,
                len: pad_len,
                pad: true,
                pieces_root: None,
            });
            offset = offset
                .checked_add(pad_len)
                .ok_or_else(|| invalid("length", "total length overflows"))?;
        }
        offset = offset
            .checked_add(leaf.len)
            .ok_or_else(|| invalid("length", "total length overflows"))?;
        let path = if single {
            RelPath::single(name.to_string())
        } else {
            leaf.path.with_root(name.to_string())
        };
        out.push(FileEntry {
            path,
            len: leaf.len,
            pad: false,
            pieces_root: leaf.root,
        });
    }
    Ok((out, single))
}

fn walk_dir<'a>(
    dir: &Dict<'a>,
    comps: &mut Vec<&'a [u8]>,
    out: &mut Vec<Leaf>,
) -> Result<(), MetainfoError> {
    for (key, node) in dir.iter() {
        let node = node
            .as_dict()
            .ok_or_else(|| invalid("file tree", "node is not a dictionary"))?;
        comps.push(key);
        let r = walk_node(node, comps, out);
        comps.pop();
        r?;
    }
    Ok(())
}

fn walk_node<'a>(
    node: &Dict<'a>,
    comps: &mut Vec<&'a [u8]>,
    out: &mut Vec<Leaf>,
) -> Result<(), MetainfoError> {
    let Some(attrs) = node.get(b"") else {
        return walk_dir(node, comps, out);
    };
    if node.len() != 1 {
        return Err(invalid("file tree", "file node also has children"));
    }
    let attrs = attrs
        .as_dict()
        .ok_or_else(|| invalid("file tree", "file attributes are not a dictionary"))?;
    let mut len = u64::try_from(
        attrs
            .get(b"length")
            .ok_or(MetainfoError::MissingField("length"))?
            .as_int()
            .ok_or_else(|| invalid("length", "not an integer"))?,
    )
    .map_err(|_| invalid("length", "negative"))?;
    let symlink = match attrs.get(b"attr") {
        None => false,
        Some(a) => a
            .as_bytes()
            .ok_or_else(|| invalid("attr", "not a byte string"))?
            .contains(&b'l'),
    };
    let root = match attrs.get(b"pieces root") {
        None => None,
        Some(r) => Some(
            <[u8; 32]>::try_from(
                r.as_bytes()
                    .ok_or_else(|| invalid("pieces root", "not a byte string"))?,
            )
            .map_err(|_| invalid("pieces root", "not 32 bytes"))?,
        ),
    };
    let root = if symlink {
        // Never materialised, as in v1: an empty regular entry.
        len = 0;
        None
    } else {
        match (len > 0, root) {
            (true, None) => return Err(MetainfoError::MissingField("pieces root")),
            (false, Some(_)) => return Err(invalid("pieces root", "set on an empty file")),
            _ => root,
        }
    };
    let path = sanitize_path(comps.iter().copied())
        .map_err(|_| invalid("file tree", "path empty after sanitization"))?;
    out.push(Leaf {
        path,
        len,
        root,
        depth: comps.len(),
    });
    Ok(())
}

/// Merges the v1 file list of a hybrid with the v2 files: they must match
/// one to one (pads aside) in order and length, and every non-empty file must
/// start on a piece boundary. Returns the v1 list with `pieces_root` set.
pub(crate) fn merge_hybrid(
    v1: Vec<FileEntry>,
    v2: &[FileEntry],
    piece_len: u32,
) -> Result<Vec<FileEntry>, MetainfoError> {
    let mut theirs = v2.iter().filter(|f| !f.pad);
    let mut offset: u64 = 0;
    let piece_len = u64::from(piece_len);
    let mut out = Vec::with_capacity(v1.len());
    for mut f in v1 {
        if !f.pad {
            let other = theirs.next().ok_or(MetainfoError::InconsistentHybrid)?;
            if other.len != f.len || (f.len > 0 && !offset.is_multiple_of(piece_len)) {
                return Err(MetainfoError::InconsistentHybrid);
            }
            f.pieces_root = other.pieces_root;
        }
        offset = offset
            .checked_add(f.len)
            .ok_or_else(|| invalid("length", "total length overflows"))?;
        out.push(f);
    }
    if theirs.next().is_some() {
        return Err(MetainfoError::InconsistentHybrid);
    }
    Ok(out)
}

/// `pieces root` -> file length, for the files that have a piece layer
/// (more than one piece).
pub(crate) fn layer_targets(files: &[FileEntry], piece_len: u32) -> BTreeMap<[u8; 32], u64> {
    files
        .iter()
        .filter(|f| !f.pad && f.len > u64::from(piece_len))
        .filter_map(|f| f.pieces_root.map(|r| (r, f.len)))
        .collect()
}

/// Whether `layer` is accepted for `root` among `targets`.
pub(crate) fn layer_ok(
    targets: &BTreeMap<[u8; 32], u64>,
    piece_len: u32,
    root: &[u8; 32],
    layer: &[[u8; 32]],
) -> bool {
    targets
        .get(root)
        .is_some_and(|len| validate_layer(root, *len, piece_len, layer))
}

/// Parses the root `piece layers` dictionary: layers for roots no file has
/// are ignored, every other one must validate.
pub(crate) fn parse_piece_layers(
    layers: &Value<'_>,
    files: &[FileEntry],
    piece_len: u32,
) -> Result<BTreeMap<[u8; 32], Vec<[u8; 32]>>, MetainfoError> {
    let dict = layers
        .as_dict()
        .ok_or_else(|| invalid("piece layers", "not a dictionary"))?;
    let targets = layer_targets(files, piece_len);
    let mut out = BTreeMap::new();
    let mut total: usize = 0;
    for (key, value) in dict.iter() {
        let Ok(root) = <[u8; 32]>::try_from(key) else {
            continue;
        };
        if !targets.contains_key(&root) {
            continue;
        }
        let raw = value
            .as_bytes()
            .ok_or_else(|| invalid("piece layers", "layer is not a byte string"))?;
        let (hashes, rest) = raw.as_chunks::<32>();
        if !rest.is_empty() {
            return Err(invalid(
                "piece layers",
                "layer length is not a multiple of 32",
            ));
        }
        total = total.saturating_add(hashes.len());
        if total > MAX_PIECE_LAYER_HASHES {
            return Err(invalid("piece layers", "too many hashes"));
        }
        if !layer_ok(&targets, piece_len, &root, hashes) {
            return Err(invalid("piece layers", "layer does not match its file"));
        }
        out.insert(root, hashes.to_vec());
    }
    Ok(out)
}

/// Start offsets of the files that have a `pieces root`, ascending, with
/// their index in the file list.
pub(crate) fn v2_file_starts(files: &[FileEntry]) -> Vec<(u64, usize)> {
    let mut offset: u64 = 0;
    let mut out = Vec::new();
    for (i, f) in files.iter().enumerate() {
        if !f.pad && f.len > 0 && f.pieces_root.is_some() {
            out.push((offset, i));
        }
        offset = offset.saturating_add(f.len);
    }
    out
}

/// The file `piece` belongs to and its geometry; see [`V2Piece`].
pub(crate) fn locate(
    starts: &[(u64, usize)],
    files: &[FileEntry],
    layers: &BTreeMap<[u8; 32], Vec<[u8; 32]>>,
    piece_len: u32,
    piece: u32,
) -> Option<V2Piece> {
    let pl = u64::from(piece_len);
    let off = u64::from(piece).checked_mul(pl)?;
    let pos = starts.partition_point(|(s, _)| *s <= off);
    let &(start, file) = starts.get(pos.checked_sub(1)?)?;
    let entry = files.get(file)?;
    let root = entry.pieces_root?;
    let rel = off - start;
    if rel >= entry.len {
        return None;
    }
    let index_in_file = u32::try_from(rel / pl).ok()?;
    let len = u32::try_from((entry.len - rel).min(pl)).ok()?;
    let (expected, leaves) = if entry.len <= pl {
        (Some(root), 1u32.checked_shl(file_tree_height(entry.len))?)
    } else {
        let expected = layers
            .get(&root)
            .and_then(|l| l.get(usize::try_from(index_in_file).ok()?))
            .copied();
        (expected, piece_len / MERKLE_BLOCK_LEN)
    };
    Some(V2Piece {
        file,
        pieces_root: root,
        index_in_file,
        expected,
        leaves,
        len,
    })
}
