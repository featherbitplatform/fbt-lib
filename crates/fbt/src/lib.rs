//! fbt: pure-Rust BitTorrent library, public facade.
//!
//! This is the only crate clients depend on; its API follows SemVer.
//!
//! # Example
//!
//! Download a torrent and stop when it finishes (or turns out to be
//! complete already, or fails):
//!
//! ```no_run
//! use fbt::{AddTorrent, Event, EventMask, Metainfo, Session, Settings, TorrentState};
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
//!     match event {
//!         Event::TorrentFinished { .. }
//!         | Event::StateChanged {
//!             state: TorrentState::Seeding | TorrentState::Error,
//!             ..
//!         } => break,
//!         _ => {}
//!     }
//! }
//! println!("{:.1}% done", torrent.status().progress * 100.0);
//! session.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Magnet links
//!
//! Add a torrent by its info hash and wait for the metadata (BEP 9) to
//! arrive from peers; the torrent then runs like one added from a `.torrent`:
//!
//! ```no_run
//! use fbt::{AddTorrent, Event, EventMask, Magnet, Session, Settings};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let magnet = Magnet::parse(
//!     "magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335aa7c1367a88a&dn=example",
//! )?;
//! let session = Session::new(Settings::default()).await?;
//! let mut events = session.subscribe(EventMask::STATUS);
//! let torrent = session
//!     .add_torrent(AddTorrent::magnet(magnet, "downloads".into()))
//!     .await?;
//!
//! while let Some(event) = events.recv().await {
//!     if let Event::MetadataReceived { .. } = event {
//!         break;
//!     }
//! }
//! println!("got metadata for {}", torrent.status().name);
//! session.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! Peer-supplied metadata is hostile: it is size-limited
//! ([`Settings::max_metadata_size`], at most [`MAX_METADATA_SIZE`]) and
//! checked against the info hash. A magnet's privacy (BEP 27) is unknown
//! until its metadata arrives, so until then it behaves as a public torrent:
//! it advertises the DHT bit, may send `Port`, advertises `ut_pex` and runs
//! the DHT announcer, as libtorrent does. At [`Event::MetadataReceived`] a
//! private torrent stops its DHT announcer and PEX at once and ignores later
//! `Port` and `ut_pex` messages.
//!
//! # Encryption
//!
//! [`Settings::encryption`] takes an [`EncryptionPolicy`]: `Preferred` (the
//! default) tries MSE/PE on outgoing connections and falls back to a plain
//! handshake once per address, `Forced` accepts and makes encrypted
//! connections only, and `Disabled` speaks plain BitTorrent only.
//!
//! # DHT and peer exchange
//!
//! With [`Settings::dht_enabled`] (the default) the session runs a Mainline
//! DHT node (BEP 5). By default it bootstraps from the public routers in
//! [`DEFAULT_DHT_BOOTSTRAP`]; replace them (or use none, for a private
//! network or a test) through [`Settings::dht_bootstrap_nodes`]. Peers found
//! this way are reported as [`Event::PeersFound`] with [`PeerSource::Dht`],
//! and [`DhtStats`] describes the node ([`Session::dht_stats`]).
//!
//! To rejoin quickly, save [`Session::dht_state`] (typically just before
//! [`Session::shutdown`]) and pass the bytes back as [`Settings::dht_state`]
//! next time. The library never writes files: storing the state is the
//! caller's job. Peer exchange (BEP 11) is on with [`Settings::pex_enabled`].
//! Private torrents use neither the DHT nor PEX.
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
pub use fbt_metainfo::{FileEntry, Info, Magnet, MagnetError, Metainfo, MetainfoError, RelPath};
pub use fbt_session::storage::{FsStorage, MemStorage, Storage};
pub use fbt_session::{
    AddTorrent, DEFAULT_DHT_BOOTSTRAP, DhtStats, DisconnectReason, EncryptionPolicy, Event,
    EventMask, EventStream, MAX_METADATA_SIZE, PeerSource, Priority, RemoveMode, Session,
    SessionError, Settings, TorrentHandle, TorrentSource, TorrentState, TorrentStatus,
};
pub use fbt_tracker::{TrackerConfig, TrackerError};

#[cfg(test)]
mod tests {
    #[test]
    fn builds() {}
}
