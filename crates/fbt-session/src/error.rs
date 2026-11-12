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
    /// The file index is out of range or names a pad file.
    #[error("file {index} cannot be opened (the torrent has {files} files)")]
    InvalidFileIndex {
        /// The index given.
        index: usize,
        /// Files in the torrent, pad files included.
        files: usize,
    },
    /// The torrent has as many open [`FileReader`](crate::FileReader)s as
    /// it allows (16).
    #[error("too many open file readers")]
    TooManyReaders,
    /// The SOCKS5 UDP association [`Settings::proxy`](crate::Settings::proxy)
    /// needs could not be made; the message is the proxy error.
    #[error("proxy error: {0}")]
    Proxy(String),
    /// A path given to
    /// [`TorrentHandle::rename_file`](crate::TorrentHandle::rename_file) is
    /// not a plain relative path (empty, with `.`/`..` or empty components,
    /// or anything sanitising would change), or it collides with another
    /// file of the torrent; the message is the path.
    #[error("invalid path: {0}")]
    InvalidPath(String),
    /// The torrent's [`Storage`](crate::storage::Storage) does not support
    /// the operation (a custom storage without `move_to` or
    /// `rename_file`).
    #[error("the storage does not support this operation")]
    Unsupported,
    /// The torrent's storage is moving and 64 commands already wait for
    /// it (see [`TorrentHandle::move_storage`](crate::TorrentHandle::move_storage)).
    #[error("too many commands wait for the storage move")]
    Busy,
    /// A web seed URL given to
    /// [`TorrentHandle::add_web_seed`](crate::TorrentHandle::add_web_seed)
    /// is not `http://` or `https://`, or is longer than 2048 bytes; the
    /// message is the URL.
    #[error("invalid web seed url: {0}")]
    InvalidUrl(String),
    /// The torrent already has
    /// [`MAX_WEB_SEEDS`](fbt_metainfo::MAX_WEB_SEEDS) web seeds.
    #[error("too many web seeds")]
    TooManyWebSeeds,
}

/// Why a [`FileReader`](crate::FileReader) read failed. It comes inside the
/// [`io::Error`] the read returns ([`io::Error::get_ref`] downcasts to it),
/// with the kind given on each variant. A reader is never poisoned: after
/// [`TorrentHandle::resume`](crate::TorrentHandle::resume) the next read
/// works.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum StreamError {
    /// The torrent was removed ([`io::ErrorKind::NotFound`]).
    #[error("torrent removed")]
    TorrentRemoved,
    /// The session shut down ([`io::ErrorKind::ConnectionAborted`]).
    #[error("session shut down")]
    Shutdown,
    /// The torrent stopped with an error, such as a storage failure
    /// ([`io::ErrorKind::Other`]); the message is the torrent's.
    #[error("torrent error: {0}")]
    TorrentError(String),
}

impl StreamError {
    /// The [`io::ErrorKind`] a read failing with this error has.
    pub(crate) fn kind(&self) -> io::ErrorKind {
        match self {
            StreamError::TorrentRemoved => io::ErrorKind::NotFound,
            StreamError::Shutdown => io::ErrorKind::ConnectionAborted,
            StreamError::TorrentError(_) => io::ErrorKind::Other,
        }
    }
}

impl From<StreamError> for io::Error {
    fn from(e: StreamError) -> Self {
        io::Error::new(e.kind(), e)
    }
}
