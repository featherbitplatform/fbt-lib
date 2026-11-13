//! fbt-metainfo: .torrent metainfo parsing and magnet links.

#![forbid(unsafe_code)]
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic
    )
)]

mod collide;
mod magnet;
mod metainfo;
mod path;
mod v2;

pub use collide::collides;
pub use magnet::{
    MAX_MAGNET_LEN, MAX_MAGNET_PEERS, MAX_MAGNET_TRACKERS, MAX_SELECT_ENTRIES, MAX_SELECT_INDEX,
    Magnet, MagnetError,
};
pub use metainfo::{
    FileEntry, Info, MAX_TORRENT_DEPTH, MAX_TORRENT_ITEMS, MAX_TRACKERS, MAX_WEB_SEEDS, Metainfo,
    MetainfoError,
};

pub use path::{PathError, RelPath, sanitize_component, sanitize_path};
pub use v2::{MAX_PIECE_LAYER_HASHES, TorrentVersion, V2Piece, validate_layer};
