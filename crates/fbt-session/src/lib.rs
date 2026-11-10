//! fbt-session: Torrent session engine: peers, pieces, storage.
//!
//! A [`Session`] owns a peer listener and any number of torrents, each run
//! by one actor task behind a [`TorrentHandle`]. Status is read from cheap
//! snapshots ([`TorrentStatus`]); everything else that happens is reported
//! as [`Event`]s through [`Session::subscribe`].

#![forbid(unsafe_code)]

pub mod choker;
pub mod error;
mod event;
mod listener;
mod peer;
pub mod picker;
pub mod rate;
mod session;
pub mod settings;
mod status;
pub mod storage;
mod torrent;

pub use error::SessionError;
pub use event::{Event, EventMask, EventStream};
pub use peer::DisconnectReason;
pub use rate::RateMeter;
pub use session::{AddTorrent, Priority, RemoveMode, Session};
pub use settings::{EncryptionPolicy, Settings};
pub use status::{TorrentState, TorrentStatus};
pub use torrent::TorrentHandle;

#[cfg(test)]
mod tests {
    #[test]
    fn builds() {}
}
