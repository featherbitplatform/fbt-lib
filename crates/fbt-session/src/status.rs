//! Plain-data torrent status snapshots.

use fbt_core::InfoHashes;

/// What a torrent is doing.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum TorrentState {
    /// A magnet torrent is fetching its metadata from peers.
    DownloadingMetadata,
    /// Waiting for one of the session's
    /// [`max_active_checks`](crate::Settings::max_active_checks) slots to
    /// check its data.
    QueuedForChecking,
    /// Existing data is being hash-checked; `progress` is in `0.0..=1.0`.
    CheckingFiles {
        /// Fraction of pieces checked.
        progress: f64,
    },
    /// Some wanted pieces are missing.
    Downloading,
    /// Every wanted piece is present.
    Seeding,
    /// Stopped by the user: no peers, no announces.
    Paused,
    /// Stopped by a storage error or invalid metadata; see
    /// [`TorrentStatus::error`].
    Error,
}

/// A snapshot of one torrent, cheap to take on every UI frame.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct TorrentStatus {
    /// The torrent's info hashes.
    pub info_hashes: InfoHashes,
    /// The torrent's name; for a magnet without metadata, its `dn` (or
    /// empty).
    pub name: String,
    /// The metadata is known (always for a `.torrent`; for a magnet once
    /// [`Event::MetadataReceived`](crate::Event::MetadataReceived) is out).
    pub has_metadata: bool,
    /// Current state.
    pub state: TorrentState,
    /// `wanted_done / total_wanted`, in `0.0..=1.0`.
    pub progress: f64,
    /// Bytes of pieces with a non-zero priority.
    pub total_wanted: u64,
    /// Bytes of wanted pieces verified and written.
    pub wanted_done: u64,
    /// Payload bytes received in accepted blocks.
    pub total_downloaded: u64,
    /// Payload bytes sent.
    pub total_uploaded: u64,
    /// Payload bytes discarded: unrequested blocks and failed pieces.
    pub total_wasted: u64,
    /// Download rate, bytes per second.
    pub download_rate: u64,
    /// Upload rate, bytes per second.
    pub upload_rate: u64,
    /// Connected peers.
    pub num_peers: u32,
    /// Connected peers that have every piece.
    pub num_seeds: u32,
    /// The storage or metadata error that stopped the torrent.
    pub error: Option<String>,
}

impl TorrentStatus {
    pub(crate) fn initial(info_hashes: InfoHashes, name: String, has_metadata: bool) -> Self {
        TorrentStatus {
            info_hashes,
            name,
            has_metadata,
            // The actor's first publish says `CheckingFiles` once it has
            // a check slot.
            state: if has_metadata {
                TorrentState::QueuedForChecking
            } else {
                TorrentState::DownloadingMetadata
            },
            progress: 0.0,
            total_wanted: 0,
            wanted_done: 0,
            total_downloaded: 0,
            total_uploaded: 0,
            total_wasted: 0,
            download_rate: 0,
            upload_rate: 0,
            num_peers: 0,
            num_seeds: 0,
            error: None,
        }
    }
}
