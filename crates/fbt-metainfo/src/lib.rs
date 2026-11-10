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

mod magnet;
mod metainfo;
mod path;

pub use magnet::{
    MAX_MAGNET_LEN, MAX_MAGNET_PEERS, MAX_MAGNET_TRACKERS, MAX_SELECT_ENTRIES, MAX_SELECT_INDEX,
    Magnet, MagnetError,
};
pub use metainfo::{FileEntry, Info, Metainfo, MetainfoError};

pub use path::{PathError, RelPath, sanitize_component, sanitize_path};
