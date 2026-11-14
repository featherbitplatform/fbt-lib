use super::Storage;
use fbt_core::merkle::piece_root;
use fbt_core::{Bitfield, PieceLayout};
use fbt_metainfo::{Metainfo, TorrentVersion};
use sha1::{Digest, Sha1};
use std::io;
use std::ops::{ControlFlow, Range};

/// SHA-1 of one piece.
pub fn sha1_piece(data: &[u8]) -> [u8; 20] {
    Sha1::digest(data).into()
}

/// What checking one piece found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PieceCheck {
    /// Every hash the piece has passed.
    pub ok: bool,
    /// The piece's BEP 52 subtree root (v2 and hybrid torrents), computed
    /// over the file's bytes in the piece only.
    pub v2_root: Option<[u8; 32]>,
}

/// Checks `data`, the whole of piece `piece`, against every hash `meta`
/// has for it.
///
/// - v1: SHA-1.
/// - v2: the subtree root against the piece's expected hash (its layer
///   entry, or `pieces root` for a one-piece file); a piece whose file's
///   layer is missing fails.
/// - Hybrid: SHA-1, and the subtree root as well when the expected hash is
///   known. A piece failing SHA-1 is not hashed again.
pub(crate) fn verify_piece(meta: &Metainfo, piece: u32, data: &[u8]) -> PieceCheck {
    let sha1_ok = || meta.info.pieces.get(piece as usize) == Some(&sha1_piece(data));
    let v2_root = || {
        let v2 = meta.v2_piece(piece)?;
        let bytes = data.get(..v2.len as usize)?;
        Some((piece_root(bytes, v2.leaves as usize), v2.expected))
    };
    let fail = PieceCheck {
        ok: false,
        v2_root: None,
    };
    match meta.version() {
        TorrentVersion::V2 => match v2_root() {
            Some((root, expected)) => PieceCheck {
                ok: expected == Some(root),
                v2_root: Some(root),
            },
            None => fail,
        },
        TorrentVersion::Hybrid => {
            if !sha1_ok() {
                return fail;
            }
            match v2_root() {
                Some((root, expected)) => PieceCheck {
                    ok: expected.is_none_or(|e| e == root),
                    v2_root: Some(root),
                },
                None => PieceCheck {
                    ok: true,
                    v2_root: None,
                },
            }
        }
        _ => PieceCheck {
            ok: sha1_ok(),
            v2_root: None,
        },
    }
}

/// Reads every piece through `storage` and returns the ones passing every
/// hash `meta` has for them (see how pieces are verified for v1, v2 and
/// hybrid torrents), with the BEP 52 subtree root of each passing piece
/// of a v2 or hybrid torrent (`None` for the others; empty for a v1
/// torrent).
///
/// One piece-sized buffer is reused. A `NotFound` piece counts as missing; any
/// other error propagates. `on_piece(i)` runs after each piece is processed;
/// returning [`ControlFlow::Break`] cancels the check, which then fails with
/// [`io::ErrorKind::Interrupted`].
pub fn check_pieces(
    storage: &dyn Storage,
    layout: &PieceLayout,
    meta: &Metainfo,
    mut on_piece: impl FnMut(u32) -> ControlFlow<()>,
) -> io::Result<(Bitfield, Vec<Option<[u8; 32]>>)> {
    let n = layout.num_pieces();
    let mut have = Bitfield::new(n);
    let mut roots = if meta.version() == TorrentVersion::V1 {
        Vec::new()
    } else {
        vec![None; n as usize]
    };
    verify_pass(storage, layout, meta, 0..n, &mut on_piece, |piece, root| {
        have.set(piece);
        if let Some(slot) = roots.get_mut(piece as usize) {
            *slot = root;
        }
    })?;
    Ok((have, roots))
}

/// Reads the pieces of `ranges` (each clamped to the torrent's piece count)
/// and returns those passing every hash `meta` has for them, each with its
/// BEP 52 subtree root (`None` for a v1 torrent): what a piece layer arriving
/// late needs to re-verify the data already on disk, without reading the
/// whole torrent again.
///
/// One piece-sized buffer is reused. A `NotFound` piece counts as missing; any
/// other error propagates. `on_piece(i)` runs after each piece is processed;
/// returning [`ControlFlow::Break`] stops the pass with
/// [`io::ErrorKind::Interrupted`].
pub(crate) fn check_pieces_ranges(
    storage: &dyn Storage,
    layout: &PieceLayout,
    meta: &Metainfo,
    ranges: &[Range<u32>],
    mut on_piece: impl FnMut(u32) -> ControlFlow<()>,
) -> io::Result<Vec<(u32, Option<[u8; 32]>)>> {
    let n = layout.num_pieces();
    let mut passed = Vec::new();
    for r in ranges {
        let (start, end) = (r.start, r.end.min(n));
        if start >= end {
            continue;
        }
        verify_pass(
            storage,
            layout,
            meta,
            start..end,
            &mut on_piece,
            |piece, root| passed.push((piece, root)),
        )?;
    }
    Ok(passed)
}

/// Reads `pieces` through `storage` one piece buffer at a time and hands every
/// one that passes `meta`'s hashes to `found`, with its BEP 52 subtree root.
/// `on_piece(i)` runs after each piece; breaking it stops the pass with
/// [`io::ErrorKind::Interrupted`]. A `NotFound` piece counts as missing; any
/// other read error ends the pass.
fn verify_pass(
    storage: &dyn Storage,
    layout: &PieceLayout,
    meta: &Metainfo,
    pieces: impl Iterator<Item = u32>,
    on_piece: &mut impl FnMut(u32) -> ControlFlow<()>,
    mut found: impl FnMut(u32, Option<[u8; 32]>),
) -> io::Result<()> {
    let mut buf = vec![0u8; layout.piece_len() as usize];
    for i in pieces {
        let size = layout.piece_size(i) as usize;
        if let Some(slice) = buf.get_mut(..size) {
            match storage.read(layout.piece_offset(i), slice) {
                Ok(()) => {
                    let check = verify_piece(meta, i, slice);
                    if check.ok {
                        found(i, check.v2_root);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        if on_piece(i).is_break() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "piece check cancelled",
            ));
        }
    }
    Ok(())
}
