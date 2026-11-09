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

mod path;

pub use path::{PathError, RelPath, sanitize_component, sanitize_path};
