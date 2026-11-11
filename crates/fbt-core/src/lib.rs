//! fbt-core: Shared types: info hashes, peer ids, piece geometry.

#![forbid(unsafe_code)]

mod bitfield;
pub mod compact;
mod info_hash;
mod layout;
mod peer_id;
pub mod rate;
pub mod udp;

pub use bitfield::{Bitfield, BitfieldError};
pub use info_hash::{InfoHashV1, InfoHashV2, InfoHashes, ParseInfoHashError};
pub use layout::{BLOCK_SIZE, BlockRef, FileLayout, FileSlice, LayoutError, PieceLayout};
pub use peer_id::PeerId;

#[cfg(test)]
mod tests;
