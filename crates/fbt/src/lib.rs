//! fbt: pure-Rust BitTorrent library, public facade.
//!
//! This is the only crate clients depend on; its API follows SemVer.
//!
//! # Example
//!
//! Download a torrent and stop when it finishes:
//!
//! ```no_run
//! use fbt::{AddTorrent, Event, EventMask, Metainfo, Session, Settings};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let bytes = std::fs::read("example.torrent")?;
//! let metainfo = Metainfo::from_bytes(&bytes)?;
//!
//! let session = Session::new(Settings::default()).await?;
//! let mut events = session.subscribe(EventMask::STATUS);
//! let torrent = session
//!     .add_torrent(AddTorrent::new(metainfo, "downloads".into()))
//!     .await?;
//!
//! while let Some(event) = events.recv().await {
//!     if let Event::TorrentFinished { .. } = event {
//!         break;
//!     }
//! }
//! println!("{:.1}% done", torrent.status().progress * 100.0);
//! session.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Features
//!
//! - `tls` (default): HTTPS trackers over rustls with the `ring` provider,
//!   which needs a C toolchain to build.
//! - `pure-rust`: HTTPS trackers over rustls with the pure-Rust RustCrypto
//!   provider. Depend on `fbt` with
//!   `default-features = false, features = ["pure-rust"]` for a build with no
//!   C or assembly.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub use fbt_bencode::DecodeError;
pub use fbt_core::{
    Bitfield, BitfieldError, BlockRef, FileLayout, FileSlice, InfoHashV1, InfoHashV2, InfoHashes,
    LayoutError, ParseInfoHashError, PeerId, PieceLayout,
};
pub use fbt_metainfo::{FileEntry, Info, Metainfo, MetainfoError, RelPath};
pub use fbt_session::storage::{FsStorage, MemStorage, Storage};
pub use fbt_session::{
    AddTorrent, DisconnectReason, Event, EventMask, EventStream, Priority, RemoveMode, Session,
    SessionError, Settings, TorrentHandle, TorrentState, TorrentStatus,
};
pub use fbt_tracker::{TrackerConfig, TrackerError};

#[cfg(test)]
mod tests {
    #[test]
    fn builds() {}
}
