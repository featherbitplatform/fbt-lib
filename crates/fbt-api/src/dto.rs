//! Wire types: the JSON the API sends and takes, with their OpenAPI
//! schemas.
//!
//! Conventions (every DTO follows them): hashes are lowercase hex,
//! durations are seconds as `f64`, rates are bytes per second, states and
//! enums are snake_case strings, timestamps are integer Unix seconds, addresses and paths are strings. DTOs are
//! built by `From` impls from `fbt` types; request DTOs use
//! `#[serde(deny_unknown_fields)]`. A DTO named in a route's
//! `#[utoipa::path]` (`body = ...`) lands in the document's
//! `components.schemas` on its own.

use std::time::UNIX_EPOCH;

use fbt::{
    FileStatus, PeerSource, PeerStatus, Priority, Session, TorrentHandle, TorrentState,
    TorrentStatus, TorrentVersion, TrackerStatus, Transport,
};
use serde::Serialize;
use utoipa::ToSchema;

use crate::ids::hex;

/// The body of every error response.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ErrorBody {
    /// What went wrong.
    pub(crate) error: ErrorDetail,
}

/// What went wrong.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ErrorDetail {
    /// Machine-readable code: `invalid_request`, `invalid_id`,
    /// `invalid_priorities`, `invalid_path` (400); `unauthorized` (401);
    /// `path_not_allowed`, `forbidden` (403); `not_found` (404);
    /// `no_metadata`, `duplicate_torrent`, `busy`, `already_exists` (409);
    /// `body_too_large` (413); `range_not_satisfiable` (416); `internal`
    /// (500); `unsupported` (501); `too_many_streams`, `shutting_down`,
    /// `dht_disabled` (503).
    #[schema(example = "not_found")]
    pub(crate) code: String,
    /// Human-readable message.
    pub(crate) message: String,
}

/// The session at a glance.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct SessionInfo {
    /// The library version.
    pub(crate) version: String,
    /// The peer listener's bound addresses.
    pub(crate) listen_addrs: Vec<String>,
    /// Our external IP address, when known.
    pub(crate) external_ip: Option<String>,
    /// Nodes in the DHT routing tables; null with the DHT off.
    pub(crate) dht_nodes: Option<u64>,
    /// Payload download rate over every torrent, bytes per second.
    pub(crate) download_rate: u64,
    /// Payload upload rate over every torrent, bytes per second.
    pub(crate) upload_rate: u64,
    /// Torrents in the session.
    pub(crate) torrents: usize,
}

impl From<&Session> for SessionInfo {
    fn from(session: &Session) -> Self {
        let status = session.torrents_status();
        SessionInfo {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            listen_addrs: session
                .local_addrs()
                .iter()
                .map(ToString::to_string)
                .collect(),
            external_ip: session.external_ip().map(|ip| ip.to_string()),
            dht_nodes: session
                .dht_stats()
                .map(|s| u64::try_from(s.nodes_v4.saturating_add(s.nodes_v6)).unwrap_or(u64::MAX)),
            download_rate: status
                .iter()
                .fold(0u64, |sum, t| sum.saturating_add(t.download_rate)),
            upload_rate: status
                .iter()
                .fold(0u64, |sum, t| sum.saturating_add(t.upload_rate)),
            torrents: status.len(),
        }
    }
}

/// A file's download priority as reported (response side). Requests use
/// their own type with the four real values.
#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FilePriority {
    /// Not downloaded.
    Skip,
    /// Downloaded after everything else.
    Low,
    /// The default.
    Normal,
    /// Downloaded first.
    High,
    /// A priority this API version does not know.
    Unknown,
}

impl From<Priority> for FilePriority {
    fn from(p: Priority) -> Self {
        match p {
            Priority::Skip => FilePriority::Skip,
            Priority::Low => FilePriority::Low,
            Priority::Normal => FilePriority::Normal,
            Priority::High => FilePriority::High,
            // A priority a later `fbt` adds.
            _ => FilePriority::Unknown,
        }
    }
}

/// The snake_case name of a torrent state.
fn state_name(state: &TorrentState) -> &'static str {
    match state {
        TorrentState::DownloadingMetadata => "downloading_metadata",
        TorrentState::QueuedForChecking => "queued_for_checking",
        TorrentState::CheckingFiles { .. } => "checking_files",
        TorrentState::Downloading => "downloading",
        TorrentState::Seeding => "seeding",
        TorrentState::Paused => "paused",
        TorrentState::Queued => "queued",
        TorrentState::Error => "error",
        // A state a later `fbt` adds.
        _ => "unknown",
    }
}

/// One torrent: its status and identity.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct TorrentInfo {
    /// The id used in paths: the 40-hex wire hash (v1, or the truncated
    /// v2 hash of a v2-only torrent).
    pub(crate) id: String,
    /// The v1 info hash, lowercase hex; null for a v2-only torrent.
    pub(crate) info_hash_v1: Option<String>,
    /// The v2 info hash, lowercase hex; null for a v1-only torrent.
    pub(crate) info_hash_v2: Option<String>,
    /// `v1`, `v2` or `hybrid`; null until the metadata is known (or for a
    /// version this API does not know).
    #[schema(example = "hybrid")]
    pub(crate) version: Option<&'static str>,
    /// The torrent's name (a magnet's `dn` before the metadata).
    pub(crate) name: String,
    /// The metadata is known.
    pub(crate) has_metadata: bool,
    /// `downloading_metadata`, `queued_for_checking`, `checking_files`,
    /// `downloading`, `seeding`, `paused`, `queued` or `error`.
    #[schema(example = "seeding")]
    pub(crate) state: &'static str,
    /// Fraction of the file check done, while `state` is
    /// `checking_files`.
    pub(crate) checking_progress: Option<f64>,
    /// Fraction of the wanted bytes done, from 0 to 1.
    pub(crate) progress: f64,
    /// Bytes of wanted pieces.
    pub(crate) total_wanted: u64,
    /// Bytes of wanted pieces verified and written.
    pub(crate) wanted_done: u64,
    /// Payload bytes received.
    pub(crate) total_downloaded: u64,
    /// Payload bytes sent.
    pub(crate) total_uploaded: u64,
    /// Payload bytes discarded.
    pub(crate) total_wasted: u64,
    /// Download rate, bytes per second.
    pub(crate) download_rate: u64,
    /// Upload rate, bytes per second.
    pub(crate) upload_rate: u64,
    /// Connected peers.
    pub(crate) num_peers: u32,
    /// Connected peers that have every piece.
    pub(crate) num_seeds: u32,
    /// Web seeds in use.
    pub(crate) num_web_seeds: u32,
    /// The error that stopped the torrent.
    pub(crate) error: Option<String>,
    /// The directory its files are below.
    pub(crate) save_path: String,
    /// Its place in the queue, from 0.
    pub(crate) queue_position: usize,
    /// Under the queue's control.
    pub(crate) auto_managed: bool,
    /// Seconds spent announced and open to peers.
    pub(crate) active_time: f64,
    /// Seconds spent running as a seed.
    pub(crate) seeding_time: f64,
    /// Resume data would change if saved now.
    pub(crate) need_save_resume: bool,
    /// For a torrent added from resume data: whether its pieces were
    /// trusted without a check; null otherwise.
    pub(crate) resume_trusted: Option<bool>,
    /// Every piece is verified, skipped files included.
    pub(crate) is_seed: bool,
}

impl TorrentInfo {
    /// The info for `handle`: its latest status and its metadata version.
    pub(crate) fn of(handle: &TorrentHandle) -> Self {
        let mut info = TorrentInfo::from(&handle.status());
        info.version = handle.metainfo().and_then(|m| match m.version() {
            TorrentVersion::V1 => Some("v1"),
            TorrentVersion::V2 => Some("v2"),
            TorrentVersion::Hybrid => Some("hybrid"),
            // A version a later `fbt` adds.
            _ => None,
        });
        info
    }
}

impl From<&TorrentStatus> for TorrentInfo {
    fn from(s: &TorrentStatus) -> Self {
        TorrentInfo {
            id: hex(&s.info_hashes.wire_hash()),
            info_hash_v1: s.info_hashes.v1.map(|h| hex(&h.0)),
            info_hash_v2: s.info_hashes.v2.map(|h| hex(&h.0)),
            version: None,
            name: s.name.clone(),
            has_metadata: s.has_metadata,
            state: state_name(&s.state),
            checking_progress: match s.state {
                TorrentState::CheckingFiles { progress } => Some(progress),
                _ => None,
            },
            progress: s.progress,
            total_wanted: s.total_wanted,
            wanted_done: s.wanted_done,
            total_downloaded: s.total_downloaded,
            total_uploaded: s.total_uploaded,
            total_wasted: s.total_wasted,
            download_rate: s.download_rate,
            upload_rate: s.upload_rate,
            num_peers: s.num_peers,
            num_seeds: s.num_seeds,
            num_web_seeds: s.num_web_seeds,
            error: s.error.clone(),
            save_path: s.save_path.to_string_lossy().into_owned(),
            queue_position: s.queue_position,
            auto_managed: s.auto_managed,
            active_time: s.active_time.as_secs_f64(),
            seeding_time: s.seeding_time.as_secs_f64(),
            need_save_resume: s.need_save_resume,
            resume_trusted: s.resume_trusted,
            is_seed: s.is_seed,
        }
    }
}

/// One file of a torrent.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct FileInfo {
    /// Its index in the file list (pad files included).
    pub(crate) index: usize,
    /// Its path relative to the save path.
    pub(crate) path: String,
    /// Its length in bytes.
    pub(crate) len: u64,
    /// A BEP 47 pad file, never written to disk.
    pub(crate) pad: bool,
    /// Its download priority.
    pub(crate) priority: FilePriority,
    /// Bytes of the file inside verified pieces.
    pub(crate) downloaded: u64,
}

impl From<&FileStatus> for FileInfo {
    fn from(f: &FileStatus) -> Self {
        FileInfo {
            index: f.index,
            path: f.path.to_string_lossy().into_owned(),
            len: f.len,
            pad: f.pad,
            priority: f.priority.into(),
            downloaded: f.downloaded,
        }
    }
}

/// One connected peer.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct PeerInfo {
    /// Its address.
    pub(crate) addr: String,
    /// Its client name, when known.
    pub(crate) client: Option<String>,
    /// `tcp`, `utp` or `unknown`.
    pub(crate) transport: &'static str,
    /// The connection is encrypted (MSE).
    pub(crate) encrypted: bool,
    /// The peer connected to us.
    pub(crate) incoming: bool,
    /// Payload bytes per second from it.
    pub(crate) download_rate: u64,
    /// Payload bytes per second to it.
    pub(crate) upload_rate: u64,
    /// Payload bytes received from it.
    pub(crate) total_downloaded: u64,
    /// Payload bytes sent to it.
    pub(crate) total_uploaded: u64,
    /// It chokes us.
    pub(crate) peer_choking: bool,
    /// It is interested in our pieces.
    pub(crate) peer_interested: bool,
    /// We choke it.
    pub(crate) am_choking: bool,
    /// We are interested in its pieces.
    pub(crate) am_interested: bool,
    /// The share of the pieces it has, from 0 to 1.
    pub(crate) progress: f64,
    /// Where we learnt of it: `tracker`, `dht`, `pex`, `magnet`, `lsd`,
    /// `incoming`, `manual`, `resume` or `holepunch`.
    pub(crate) source: &'static str,
}

impl From<&PeerStatus> for PeerInfo {
    fn from(p: &PeerStatus) -> Self {
        PeerInfo {
            addr: p.addr.to_string(),
            client: p.client.clone(),
            transport: match p.transport {
                Transport::Tcp => "tcp",
                Transport::Utp => "utp",
                _ => "unknown",
            },
            encrypted: p.encrypted,
            incoming: p.incoming,
            download_rate: p.download_rate,
            upload_rate: p.upload_rate,
            total_downloaded: p.total_downloaded,
            total_uploaded: p.total_uploaded,
            peer_choking: p.peer_choking,
            peer_interested: p.peer_interested,
            am_choking: p.am_choking,
            am_interested: p.am_interested,
            progress: p.progress,
            source: match p.source {
                PeerSource::Tracker => "tracker",
                PeerSource::Dht => "dht",
                PeerSource::Pex => "pex",
                PeerSource::Magnet => "magnet",
                PeerSource::Lsd => "lsd",
                PeerSource::Incoming => "incoming",
                PeerSource::Manual => "manual",
                PeerSource::Resume => "resume",
                PeerSource::Holepunch => "holepunch",
                _ => "unknown",
            },
        }
    }
}

/// One tracker of a torrent under one announced hash.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct TrackerInfo {
    /// The announce URL.
    pub(crate) url: String,
    /// Its tier (BEP 12), from 0.
    pub(crate) tier: usize,
    /// The hash announced to it, lowercase hex (20 bytes).
    pub(crate) info_hash: String,
    /// The last announce's error.
    pub(crate) last_error: Option<String>,
    /// When it last answered, as integer Unix seconds (like every
    /// timestamp in this API).
    pub(crate) last_reply: Option<u64>,
    /// Seconds until the next announce to it.
    pub(crate) next_announce: Option<f64>,
    /// Seeders in its last reply.
    pub(crate) seeders: Option<u32>,
    /// Leechers in its last reply.
    pub(crate) leechers: Option<u32>,
    /// Peers its last reply carried.
    pub(crate) peers_returned: Option<u32>,
}

impl From<&TrackerStatus> for TrackerInfo {
    fn from(t: &TrackerStatus) -> Self {
        TrackerInfo {
            url: t.url.clone(),
            tier: t.tier,
            info_hash: hex(&t.info_hash),
            last_error: t.last_error.clone(),
            last_reply: t
                .last_reply
                .and_then(|at| at.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs()),
            next_announce: t.next_announce.map(|d| d.as_secs_f64()),
            seeders: t.seeders,
            leechers: t.leechers,
            peers_returned: t.peers_returned,
        }
    }
}

/// Which pieces a torrent has.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct PiecesInfo {
    /// The number of pieces.
    pub(crate) num_pieces: u32,
    /// The bitfield as lowercase hex, piece 0 in the top bit of the
    /// first byte (BEP 3 order); spare bits of the last byte are zero.
    pub(crate) have: String,
}

/// A magnet link.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct MagnetInfo {
    /// The `magnet:` URI.
    pub(crate) magnet: String,
}
