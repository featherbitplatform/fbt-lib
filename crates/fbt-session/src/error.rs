//! Session errors. Storage reports plain `std::io::Error`.

use std::io;

/// Errors returned by [`Session`](crate::Session) and
/// [`TorrentHandle`](crate::TorrentHandle) methods.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SessionError {
    /// The peer listener could not bind its address.
    #[error("cannot bind the peer listener: {0}")]
    Bind(#[source] io::Error),
    /// A torrent with the same info hashes is already in the session.
    #[error("torrent already added")]
    DuplicateTorrent,
    /// The torrent was removed.
    #[error("torrent not found")]
    TorrentNotFound,
    /// The file priority list does not match the torrent's file count.
    #[error("expected {expected} file priorities, got {got}")]
    InvalidPriorities {
        /// Files in the torrent.
        expected: usize,
        /// Priorities given.
        got: usize,
    },
    /// The session was shut down.
    #[error("session shut down")]
    Shutdown,
    /// The tracker client could not be built.
    #[error("cannot build the tracker client: {0}")]
    Tracker(#[source] fbt_tracker::TrackerError),
    /// A storage operation failed.
    #[error("storage error: {0}")]
    Storage(#[source] io::Error),
    /// A [`Settings`](crate::Settings) value is out of range; the message
    /// names the field.
    #[error("invalid settings: {0}")]
    InvalidSettings(String),
    /// The command needs the torrent's metadata, which a magnet torrent
    /// has not received yet.
    #[error("the torrent's metadata is not known yet")]
    NoMetadata,
    /// A magnet with only a v2 (`btmh`) info hash: v2-only torrents are not
    /// supported yet.
    #[error("v2-only torrents are not supported")]
    V2OnlyUnsupported,
}
