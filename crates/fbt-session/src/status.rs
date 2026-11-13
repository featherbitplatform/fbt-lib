//! Plain-data torrent status snapshots.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use fbt_core::InfoHashes;

use crate::event::PeerSource;
use crate::peer::Transport;
use crate::session::Priority;

/// What a torrent is doing.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum TorrentState {
    /// A magnet torrent is fetching its metadata from peers, or a v2-only
    /// torrent the piece layers it lacks (BEP 52 `hash request`s to peers
    /// that set the v2 bit).
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
    /// Some wanted pieces are missing, or the last ones are verified but
    /// not yet on disk (progress 100 %, still served from the cache).
    Downloading,
    /// Every wanted piece is verified and on disk: the final writes and the
    /// storage flush completed (reached together with
    /// [`Event::TorrentFinished`](crate::Event::TorrentFinished)).
    Seeding,
    /// Stopped by the user: no peers, no announces.
    Paused,
    /// Auto-managed and waiting for a slot of the session's queue
    /// ([`Settings::queue`](crate::Settings::queue)): stopped like
    /// [`Paused`](Self::Paused) until the queue starts it. A torrent both
    /// paused and queued shows as `Paused`.
    Queued,
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
    /// The metadata is known (for a `.torrent` at once, unless it is a v2
    /// one lacking piece layers; for a magnet, and such a `.torrent`, once
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
    /// Web seeds (BEP 19) in use: those not retired. See
    /// [`TorrentHandle::web_seeds`](crate::TorrentHandle::web_seeds).
    pub num_web_seeds: u32,
    /// The storage or metadata error that stopped the torrent.
    pub error: Option<String>,
    /// The directory the torrent's files are below, as added and changed
    /// by [`TorrentHandle::move_storage`](crate::TorrentHandle::move_storage).
    pub save_path: PathBuf,
    /// Its place in the session's queue, from 0 (runs first); dense over
    /// the session's torrents.
    pub queue_position: usize,
    /// Under the queue's control
    /// ([`AddTorrent::auto_managed`](crate::AddTorrent::auto_managed)).
    pub auto_managed: bool,
    /// Time spent announced and open to peers since it was added: past
    /// its check (a magnet's check right after its metadata counts) and
    /// neither paused, queued nor in error.
    pub active_time: Duration,
    /// Time spent running as a seed since it was added.
    pub seeding_time: Duration,
    /// Something resume data records changed since the last
    /// [`save_resume_data`](crate::TorrentHandle::save_resume_data): a
    /// piece written, a file priority, a rename, a move, a pause or resume,
    /// a rate limit or the queue. True for a torrent added without resume
    /// data until its first save.
    pub need_save_resume: bool,
    /// For a torrent added from resume data
    /// ([`AddTorrent::from_resume`](crate::AddTorrent::from_resume)):
    /// `Some(true)` once its pieces were trusted without a check, `Some(false)`
    /// once a full check ran instead (a file changed since the save). `None`
    /// without resume data, while the stamps are being compared, and for a
    /// magnet saved before its metadata arrived (it has no pieces to trust).
    pub resume_trusted: Option<bool>,
    /// Every piece of the torrent is verified, skipped files included
    /// (libtorrent's `is_seed`). [`TorrentState::Seeding`] only needs the
    /// wanted pieces; a torrent announces itself to the DHT as a seed
    /// (BEP 33) only when this is set.
    pub is_seed: bool,
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
            num_web_seeds: 0,
            error: None,
            save_path: PathBuf::new(),
            queue_position: 0,
            auto_managed: false,
            active_time: Duration::ZERO,
            seeding_time: Duration::ZERO,
            need_save_resume: false,
            resume_trusted: None,
            is_seed: false,
        }
    }
}

/// One file of a torrent, from
/// [`TorrentHandle::files`](crate::TorrentHandle::files).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct FileStatus {
    /// Its index in the metadata's file list (pad files included).
    pub index: usize,
    /// Its path relative to the save path (for a multi-file torrent it
    /// starts with the torrent's directory); a path given by
    /// [`rename_file`](crate::TorrentHandle::rename_file) replaces the
    /// metadata's.
    pub path: PathBuf,
    /// Its length in bytes.
    pub len: u64,
    /// A BEP 47 pad file: never written to disk.
    pub pad: bool,
    /// Its download priority.
    pub priority: Priority,
    /// Bytes of the file inside verified pieces.
    pub downloaded: u64,
}

/// One connected peer of a torrent, from
/// [`TorrentHandle::peers`](crate::TorrentHandle::peers).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct PeerStatus {
    /// Its address: the one we dialled, or the remote end of an incoming
    /// connection.
    pub addr: SocketAddr,
    /// Its client: the BEP 10 handshake's `v`, else decoded from an
    /// Azureus-style peer id. Peer input: control characters removed, at
    /// most 64 characters.
    pub client: Option<String>,
    /// The transport the connection runs over.
    pub transport: Transport,
    /// The connection is RC4-encrypted (MSE).
    pub encrypted: bool,
    /// The peer connected to us.
    pub incoming: bool,
    /// Payload bytes per second from it, over the last few seconds.
    pub download_rate: u64,
    /// Payload bytes per second to it, over the last few seconds.
    pub upload_rate: u64,
    /// Payload bytes received from it on this connection.
    pub total_downloaded: u64,
    /// Payload bytes sent to it on this connection.
    pub total_uploaded: u64,
    /// It chokes us.
    pub peer_choking: bool,
    /// It is interested in our pieces.
    pub peer_interested: bool,
    /// We choke it.
    pub am_choking: bool,
    /// We are interested in its pieces.
    pub am_interested: bool,
    /// The share of the torrent's pieces it has, from 0.0 to 1.0 (0.0
    /// before the metadata).
    pub progress: f64,
    /// Where we learnt of it.
    pub source: PeerSource,
}

/// One tracker of a torrent under one announced hash, from
/// [`TorrentHandle::trackers`](crate::TorrentHandle::trackers).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct TrackerStatus {
    /// The tracker's announce URL.
    pub url: String,
    /// Its tier (BEP 12), from 0.
    pub tier: usize,
    /// The hash announced to it: the v1 hash or the truncated v2 hash.
    pub info_hash: [u8; 20],
    /// The last announce's error, cleared by the next reply. A tracker
    /// dropped as unusable (unsupported scheme, invalid URL, HTTPS without
    /// TLS) keeps it.
    pub last_error: Option<String>,
    /// When it last answered an announce.
    pub last_reply: Option<SystemTime>,
    /// Time left until the next announce goes to it: set on the tracker the
    /// next walk through the tiers starts with while the torrent is
    /// announced, `None` on the others and while it is stopped.
    pub next_announce: Option<Duration>,
    /// Seeders in its last reply, if it said.
    pub seeders: Option<u32>,
    /// Leechers in its last reply, if it said.
    pub leechers: Option<u32>,
    /// Peers its last reply carried.
    pub peers_returned: Option<u32>,
}
