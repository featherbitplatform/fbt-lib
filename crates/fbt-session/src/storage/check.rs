use super::Storage;
use fbt_core::{Bitfield, PieceLayout};
use sha1::{Digest, Sha1};
use std::io;

/// SHA-1 of one piece.
pub fn sha1_piece(data: &[u8]) -> [u8; 20] {
    Sha1::digest(data).into()
}

/// Reads every piece through `storage` and returns the ones matching `hashes`.
///
/// One piece-sized buffer is reused. A `NotFound` piece counts as missing; any
/// other error propagates. `on_piece(i)` runs after each piece is processed.
pub fn check_pieces(
    storage: &dyn Storage,
    layout: &PieceLayout,
    hashes: &[[u8; 20]],
    mut on_piece: impl FnMut(u32),
) -> io::Result<Bitfield> {
    let n = layout.num_pieces();
    let mut have = Bitfield::new(n);
    let mut buf = vec![0u8; layout.piece_len() as usize];
    for i in 0..n {
        let size = layout.piece_size(i) as usize;
        if let Some(slice) = buf.get_mut(..size) {
            match storage.read(layout.piece_offset(i), slice) {
                Ok(()) => {
                    if hashes.get(i as usize) == Some(&sha1_piece(slice)) {
                        have.set(i);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        on_piece(i);
    }
    Ok(have)
}
