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

use std::fmt;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;

use fbt::{
    BlockReason, DisconnectReason, Event, ExternalIpSource, FileStatus, InfoHashes, IpFilter,
    MapMethod, MapProtocol, MoveMode, PeerSource, PeerStatus, Priority, QueueSettings, Session,
    TorrentHandle, TorrentState, TorrentStatus, TorrentVersion, TrackerStatus, Transport,
};
use serde::de::{Error as _, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::ids::hex;
use crate::routes::control::MAX_FILE_PRIORITIES;
use crate::routes::create::MAX_CREATE_URLS;

/// A JSON array of at most `N` items. A longer one is refused while it is
/// parsed, after item `N + 1`, so a request cannot inflate a run of tiny
/// items (`[[],[],...]`) into a large allocation before a handler counts
/// them.
#[derive(Debug)]
pub(crate) struct Capped<T, const N: usize>(pub(crate) Vec<T>);

impl<'de, T: Deserialize<'de>, const N: usize> Deserialize<'de> for Capped<T, N> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        /// Collects up to `N` items.
        struct CappedVisitor<T, const N: usize>(PhantomData<T>);

        impl<'de, T: Deserialize<'de>, const N: usize> Visitor<'de> for CappedVisitor<T, N> {
            type Value = Capped<T, N>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "an array of at most {N} items")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                // The hint comes from the client: never trust it past `N`.
                let hint = seq.size_hint().unwrap_or(0).min(N).min(1024);
                let mut items = Vec::with_capacity(hint);
                while let Some(item) = seq.next_element()? {
                    if items.len() == N {
                        return Err(A::Error::invalid_length(N.saturating_add(1), &self));
                    }
                    items.push(item);
                }
                Ok(Capped(items))
            }
        }

        deserializer.deserialize_seq(CappedVisitor(PhantomData))
    }
}

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
    /// `dht_disabled`, `busy` (503).
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
    /// Session-wide download limit, bytes per second; 0 is unlimited.
    pub(crate) download_limit: u64,
    /// Session-wide upload limit, bytes per second; 0 is unlimited.
    pub(crate) upload_limit: u64,
}

impl From<&Session> for SessionInfo {
    fn from(session: &Session) -> Self {
        let status = session.torrents_status();
        let limit = session.rate_limit();
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
            download_limit: limit.download,
            upload_limit: limit.upload,
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
            transport: transport_name(p.transport),
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
            source: peer_source_name(p.source),
        }
    }
}

/// The snake_case name of a peer source.
fn peer_source_name(source: PeerSource) -> &'static str {
    match source {
        PeerSource::Tracker => "tracker",
        PeerSource::Dht => "dht",
        PeerSource::Pex => "pex",
        PeerSource::Magnet => "magnet",
        PeerSource::Lsd => "lsd",
        PeerSource::Incoming => "incoming",
        PeerSource::Manual => "manual",
        PeerSource::Resume => "resume",
        PeerSource::Holepunch => "holepunch",
        // A source a later `fbt` adds.
        _ => "unknown",
    }
}

/// The snake_case name of a transport.
fn transport_name(transport: Transport) -> &'static str {
    match transport {
        Transport::Tcp => "tcp",
        Transport::Utp => "utp",
        // A transport a later `fbt` adds.
        _ => "unknown",
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

/// A file's download priority as a request sets it.
#[derive(Debug, Clone, Copy, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PriorityDto {
    /// Not downloaded.
    Skip,
    /// Downloaded after everything else.
    Low,
    /// The default.
    Normal,
    /// Downloaded first.
    High,
}

impl From<PriorityDto> for Priority {
    fn from(p: PriorityDto) -> Self {
        match p {
            PriorityDto::Skip => Priority::Skip,
            PriorityDto::Low => Priority::Low,
            PriorityDto::Normal => Priority::Normal,
            PriorityDto::High => Priority::High,
        }
    }
}

/// `POST /api/v1/torrents` as JSON: add a magnet link.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct AddTorrentRequest {
    /// The `magnet:` URI.
    pub(crate) magnet: String,
    /// The directory the files go below; inside an allowed root when
    /// roots are configured.
    #[schema(value_type = String)]
    pub(crate) save_path: PathBuf,
    /// Start paused (default false).
    pub(crate) paused: Option<bool>,
    /// Under the session's queue (default true).
    pub(crate) auto_managed: Option<bool>,
    /// Download pieces in order (default false).
    pub(crate) sequential: Option<bool>,
}

/// The query string of `POST /api/v1/torrents` with a `.torrent` body.
#[derive(Debug, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub(crate) struct AddOptions {
    /// The directory the files go below (only with an
    /// `application/x-bittorrent` body).
    #[param(value_type = String)]
    pub(crate) save_path: PathBuf,
    /// Start paused (default false).
    pub(crate) paused: Option<bool>,
    /// Under the session's queue (default true).
    pub(crate) auto_managed: Option<bool>,
    /// Download pieces in order (default false).
    pub(crate) sequential: Option<bool>,
}

/// The query string of `POST /api/v1/torrents/resume`.
#[derive(Debug, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub(crate) struct ResumeOptions {
    /// Replaces the save path stored in the resume data.
    #[param(value_type = Option<String>)]
    pub(crate) save_path: Option<PathBuf>,
}

/// The query string of `DELETE /api/v1/torrents/{id}`.
#[derive(Debug, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub(crate) struct RemoveOptions {
    /// Delete the torrent's files too (default false).
    pub(crate) delete_files: Option<bool>,
}

/// `PUT /api/v1/torrents/{id}/file-priorities`.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct FilePrioritiesRequest {
    /// One priority per file, pad files included, in file order; at most
    /// 4 194 304.
    #[schema(value_type = Vec<PriorityDto>)]
    pub(crate) priorities: Capped<PriorityDto, MAX_FILE_PRIORITIES>,
}

/// `PUT /api/v1/torrents/{id}/limits`.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct LimitsRequest {
    /// Download limit in bytes per second; null (or 0) is unlimited.
    pub(crate) download: Option<u64>,
    /// Upload limit in bytes per second; null (or 0) is unlimited.
    pub(crate) upload: Option<u64>,
}

/// `PUT /api/v1/torrents/{id}/queue-position`.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct QueuePositionRequest {
    /// The new place in the queue, from 0; past the end is the bottom.
    pub(crate) position: usize,
}

/// `PUT /api/v1/torrents/{id}/auto-managed`.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutoManagedRequest {
    /// Put the torrent under the session's queue, or take it out.
    pub(crate) auto_managed: bool,
}

/// What a move does with files already at the target.
#[derive(Debug, Clone, Copy, Default, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MoveModeDto {
    /// Move nothing if any target exists.
    #[default]
    FailIfExists,
    /// Overwrite files at the target.
    ReplaceExisting,
    /// Keep the files at the target and recheck.
    KeepExisting,
}

impl From<MoveModeDto> for MoveMode {
    fn from(m: MoveModeDto) -> Self {
        match m {
            MoveModeDto::FailIfExists => MoveMode::FailIfExists,
            MoveModeDto::ReplaceExisting => MoveMode::ReplaceExisting,
            MoveModeDto::KeepExisting => MoveMode::KeepExisting,
        }
    }
}

/// `POST /api/v1/torrents/{id}/move`.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct MoveRequest {
    /// The new save path; inside an allowed root when roots are
    /// configured.
    #[schema(value_type = String)]
    pub(crate) path: PathBuf,
    /// What to do with files already there (default `fail_if_exists`).
    #[serde(default)]
    pub(crate) mode: MoveModeDto,
}

/// `POST /api/v1/torrents/{id}/rename-file`.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct RenameFileRequest {
    /// The file's index (pad files included).
    pub(crate) index: usize,
    /// Its new path relative to the save path, `/`-separated.
    pub(crate) path: String,
}

/// `POST /api/v1/torrents/{id}/web-seeds`.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct WebSeedRequest {
    /// An `http://` or `https://` URL (BEP 19).
    pub(crate) url: String,
}

/// The query string of `DELETE /api/v1/torrents/{id}/web-seeds`.
#[derive(Debug, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub(crate) struct WebSeedQuery {
    /// The web seed to remove.
    pub(crate) url: String,
}

/// `POST /api/v1/torrents/{id}/peers`.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct AddPeerRequest {
    /// The peer's address, `ip:port` (`[ip]:port` for IPv6).
    #[schema(value_type = String, example = "192.0.2.1:6881")]
    pub(crate) addr: SocketAddr,
}

/// The session's queue limits (`GET`/`PUT /api/v1/session/queue`). `PUT`
/// replaces them all: a limit left out or null is unlimited.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct QueueSettingsDto {
    /// Auto-managed downloading torrents running at once; null is
    /// unlimited.
    pub(crate) active_downloads: Option<usize>,
    /// Auto-managed seeding torrents running at once; null is unlimited.
    pub(crate) active_seeds: Option<usize>,
    /// Auto-managed torrents running at once, downloading or seeding; null
    /// is unlimited.
    pub(crate) active_limit: Option<usize>,
    /// A slow torrent does not count against the download and seed limits.
    pub(crate) dont_count_slow_torrents: bool,
    /// Download rate below which a torrent may be slow, bytes per second.
    pub(crate) inactive_down_rate: u64,
    /// Upload rate below which a torrent may be slow, bytes per second.
    pub(crate) inactive_up_rate: u64,
    /// Seconds a torrent runs before it may count as slow.
    pub(crate) startup_grace: f64,
    /// Seconds between scheduler passes (greater than zero).
    pub(crate) interval: f64,
}

impl From<&QueueSettings> for QueueSettingsDto {
    fn from(q: &QueueSettings) -> Self {
        QueueSettingsDto {
            active_downloads: q.active_downloads,
            active_seeds: q.active_seeds,
            active_limit: q.active_limit,
            dont_count_slow_torrents: q.dont_count_slow_torrents,
            inactive_down_rate: q.inactive_down_rate,
            inactive_up_rate: q.inactive_up_rate,
            startup_grace: q.startup_grace.as_secs_f64(),
            interval: q.interval.as_secs_f64(),
        }
    }
}

impl QueueSettingsDto {
    /// The settings, or why a duration is not one.
    pub(crate) fn into_settings(self) -> Result<QueueSettings, String> {
        let secs = |name: &str, v: f64| {
            std::time::Duration::try_from_secs_f64(v)
                .map_err(|_| format!("{name} must be a non-negative number of seconds"))
        };
        // `QueueSettings` is non-exhaustive: start from the default.
        let mut q = QueueSettings::default();
        q.active_downloads = self.active_downloads;
        q.active_seeds = self.active_seeds;
        q.active_limit = self.active_limit;
        q.dont_count_slow_torrents = self.dont_count_slow_torrents;
        q.inactive_down_rate = self.inactive_down_rate;
        q.inactive_up_rate = self.inactive_up_rate;
        q.startup_grace = secs("startup_grace", self.startup_grace)?;
        q.interval = secs("interval", self.interval)?;
        Ok(q)
    }
}

/// What an IP filter rule does to its range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum IpFilterAction {
    /// Peers in the range are refused.
    Block,
    /// Peers in the range are accepted, overriding earlier blocks.
    Allow,
}

/// One IP filter rule: an inclusive range of one address family.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct IpFilterRule {
    /// The first address of the range.
    #[schema(example = "10.0.0.0")]
    pub(crate) first: String,
    /// The last address of the range (not below `first`, same family).
    #[schema(example = "10.0.0.255")]
    pub(crate) last: String,
    /// Block or allow the range.
    pub(crate) action: IpFilterAction,
}

/// The session's IP filter (`GET`/`PUT /api/v1/session/ip-filter`). `PUT`
/// replaces the filter; rules apply in order and the last one covering an
/// address decides. `GET` returns the disjoint ranges the filter holds.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct IpFilterDto {
    /// The rules.
    pub(crate) rules: Vec<IpFilterRule>,
}

/// Most rules a `PUT /api/v1/session/ip-filter` body may hold.
pub(crate) const MAX_IP_FILTER_RULES: usize = 65_536;

impl From<&IpFilter> for IpFilterDto {
    fn from(f: &IpFilter) -> Self {
        IpFilterDto {
            rules: f
                .rules()
                .into_iter()
                .map(|(range, blocked)| IpFilterRule {
                    first: range.start().to_string(),
                    last: range.end().to_string(),
                    action: if blocked {
                        IpFilterAction::Block
                    } else {
                        IpFilterAction::Allow
                    },
                })
                .collect(),
        }
    }
}

impl IpFilterDto {
    /// The filter, or what is wrong with a rule.
    pub(crate) fn into_filter(self) -> Result<IpFilter, String> {
        if self.rules.len() > MAX_IP_FILTER_RULES {
            return Err(format!(
                "at most {MAX_IP_FILTER_RULES} rules are accepted, got {}",
                self.rules.len()
            ));
        }
        let mut filter = IpFilter::default();
        for (i, rule) in self.rules.iter().enumerate() {
            let parse = |what: &str, s: &str| {
                s.parse::<std::net::IpAddr>()
                    .map(|ip| ip.to_canonical())
                    .map_err(|_| format!("rule {i}: `{what}` is not an IP address"))
            };
            let first = parse("first", &rule.first)?;
            let last = parse("last", &rule.last)?;
            if first.is_ipv4() != last.is_ipv4() {
                return Err(format!("rule {i}: `first` and `last` differ in family"));
            }
            if first > last {
                return Err(format!("rule {i}: `first` is above `last`"));
            }
            match rule.action {
                IpFilterAction::Block => filter.block(first..=last),
                IpFilterAction::Allow => filter.allow(first..=last),
            }
        }
        Ok(filter)
    }
}

/// A metainfo version to create.
#[derive(Debug, Clone, Copy, Default, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CreateVersion {
    /// BEP 3 only.
    V1,
    /// BEP 52 only.
    V2,
    /// Both (the default).
    #[default]
    Hybrid,
}

impl From<CreateVersion> for TorrentVersion {
    fn from(v: CreateVersion) -> Self {
        match v {
            CreateVersion::V1 => TorrentVersion::V1,
            CreateVersion::V2 => TorrentVersion::V2,
            CreateVersion::Hybrid => TorrentVersion::Hybrid,
        }
    }
}

/// `POST /api/v1/create-torrent`.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateTorrentRequest {
    /// The file or directory to hash; subject to `allowed_roots`.
    pub(crate) path: String,
    /// Which versions to write (default hybrid).
    #[serde(default)]
    pub(crate) version: CreateVersion,
    /// The piece length in bytes: a power of two of at least 16 KiB
    /// (default: automatic).
    pub(crate) piece_length: Option<u32>,
    /// Announce tiers. Tiers (an empty one counts as one URL), their URLs
    /// and the web seeds together are at most 256.
    #[schema(value_type = Option<Vec<Vec<String>>>)]
    pub(crate) trackers: Option<Capped<Capped<String, MAX_CREATE_URLS>, MAX_CREATE_URLS>>,
    /// BEP 19 web seed URLs.
    #[schema(value_type = Option<Vec<String>>)]
    pub(crate) web_seeds: Option<Capped<String, MAX_CREATE_URLS>>,
    /// A free-form comment.
    pub(crate) comment: Option<String>,
    /// The BEP 27 private flag.
    #[serde(default)]
    pub(crate) private: bool,
}

/// One session event, as the `data:` of a server-sent event whose
/// `event:` field equals `type`. Torrent ids are the 40-hex wire hash.
#[derive(Debug, Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum EventDto {
    /// A torrent was added.
    TorrentAdded {
        /// The torrent's id.
        torrent: String,
    },
    /// A torrent was removed.
    TorrentRemoved {
        /// The torrent's id.
        torrent: String,
    },
    /// A torrent changed state.
    StateChanged {
        /// The torrent's id.
        torrent: String,
        /// The new state, as in `TorrentInfo.state`.
        state: &'static str,
        /// Fraction of the file check done, when `state` is
        /// `checking_files`.
        checking_progress: Option<f64>,
    },
    /// A piece passed its hash check.
    PieceFinished {
        /// The torrent's id.
        torrent: String,
        /// Piece index.
        piece: u32,
    },
    /// A downloaded piece failed its hash check.
    HashFailed {
        /// The torrent's id.
        torrent: String,
        /// Piece index.
        piece: u32,
    },
    /// A magnet torrent received its metadata.
    MetadataReceived {
        /// The torrent's id.
        torrent: String,
    },
    /// Every wanted piece was downloaded, verified and written.
    TorrentFinished {
        /// The torrent's id.
        torrent: String,
    },
    /// An auto-managed seed reached its ratio or time limit and was paused.
    SeedLimitReached {
        /// The torrent's id.
        torrent: String,
    },
    /// A tracker answered an announce.
    TrackerReply {
        /// The torrent's id.
        torrent: String,
        /// The hash announced, lowercase hex (20 bytes).
        info_hash: String,
        /// Tracker URL.
        url: String,
        /// Peers in the reply.
        peers: usize,
    },
    /// An announce failed.
    TrackerError {
        /// The torrent's id.
        torrent: String,
        /// Tracker URL.
        url: String,
        /// What went wrong.
        message: String,
    },
    /// A tracker answered a scrape.
    ScrapeReply {
        /// The torrent's id.
        torrent: String,
        /// Tracker URL.
        url: String,
        /// Seeds.
        complete: u32,
        /// Leechers.
        incomplete: u32,
        /// Completed downloads the tracker has seen.
        downloaded: u32,
    },
    /// A scrape failed.
    ScrapeFailed {
        /// The torrent's id.
        torrent: String,
        /// Tracker URL.
        url: String,
        /// What went wrong.
        message: String,
    },
    /// A web seed request failed.
    WebSeedError {
        /// The torrent's id.
        torrent: String,
        /// The web seed's URL.
        url: String,
        /// What went wrong.
        message: String,
        /// The seed is never asked again.
        retired: bool,
    },
    /// A batch of peer addresses arrived.
    PeersFound {
        /// The torrent's id.
        torrent: String,
        /// Where they came from, as in `PeerInfo.source`.
        source: &'static str,
        /// Dialable addresses in the batch.
        count: usize,
    },
    /// The DHT node filled its routing table for the first time.
    DhtBootstrapped {
        /// Nodes in the routing tables.
        nodes: usize,
    },
    /// A peer completed the handshake.
    PeerConnected {
        /// The torrent's id.
        torrent: String,
        /// Remote address.
        addr: String,
        /// `tcp`, `utp` or `unknown`.
        transport: &'static str,
        /// The connection is RC4-encrypted (MSE).
        encrypted: bool,
    },
    /// A peer connection ended.
    PeerDisconnected {
        /// The torrent's id.
        torrent: String,
        /// Remote address.
        addr: String,
        /// Why: `self_connection`, `info_hash_mismatch`,
        /// `handshake_timeout`, `idle_timeout`, `encryption`, `protocol`,
        /// `io`, `closed`, `shutdown`, `duplicate`, `bad_metadata`,
        /// `upload_only`, `blocked` or `unknown`.
        reason: &'static str,
        /// Details: the error text, or for `blocked` the block reason.
        detail: Option<String>,
    },
    /// A peer was refused.
    PeerBlocked {
        /// The torrent's id; null for an incoming connection refused
        /// before its handshake.
        torrent: Option<String>,
        /// The peer's address.
        addr: String,
        /// `ip_filter`, `banned`, `per_ip_limit` or `unknown`.
        reason: &'static str,
    },
    /// Reading or writing torrent data failed.
    FileError {
        /// The torrent's id.
        torrent: String,
        /// The file involved, when known.
        path: Option<String>,
        /// The error.
        message: String,
    },
    /// A storage move finished.
    StorageMoved {
        /// The torrent's id.
        torrent: String,
        /// The new save path.
        path: String,
    },
    /// A storage move failed.
    StorageMoveFailed {
        /// The torrent's id.
        torrent: String,
        /// The error.
        message: String,
    },
    /// A file rename finished.
    FileRenamed {
        /// The torrent's id.
        torrent: String,
        /// The file's index.
        index: usize,
        /// Its new path, relative to the save path, `/`-separated.
        path: String,
    },
    /// A file rename failed.
    FileRenameFailed {
        /// The torrent's id.
        torrent: String,
        /// The file's index.
        index: usize,
        /// The error.
        message: String,
    },
    /// The router mapped a listen port.
    PortMapped {
        /// `tcp`, `udp` or `unknown`.
        protocol: &'static str,
        /// `upnp`, `nat_pmp`, `pcp` or `unknown`.
        method: &'static str,
        /// The public address and port.
        external: String,
    },
    /// Mapping a listen port failed.
    PortMappingFailed {
        /// `tcp`, `udp` or `unknown`.
        protocol: &'static str,
        /// `upnp`, `nat_pmp`, `pcp` or `unknown`.
        method: &'static str,
        /// Why.
        message: String,
    },
    /// A source learned our external address.
    ExternalIpChanged {
        /// The address.
        ip: String,
        /// `port_mapping`, `dht`, `peers` or `unknown`.
        source: &'static str,
    },
    /// The proxy failed in a way that affects the whole session.
    ProxyError {
        /// What happened.
        message: String,
    },
    /// The subscriber fell behind and `missed` events were dropped.
    Lagged {
        /// Number of events dropped.
        missed: u64,
    },
    /// An event this API version does not know.
    Other {
        /// The event's Rust `Debug` form.
        debug: String,
    },
}

impl EventDto {
    /// The snake_case `type` of this event, used as the SSE `event:` field.
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            EventDto::TorrentAdded { .. } => "torrent_added",
            EventDto::TorrentRemoved { .. } => "torrent_removed",
            EventDto::StateChanged { .. } => "state_changed",
            EventDto::PieceFinished { .. } => "piece_finished",
            EventDto::HashFailed { .. } => "hash_failed",
            EventDto::MetadataReceived { .. } => "metadata_received",
            EventDto::TorrentFinished { .. } => "torrent_finished",
            EventDto::SeedLimitReached { .. } => "seed_limit_reached",
            EventDto::TrackerReply { .. } => "tracker_reply",
            EventDto::TrackerError { .. } => "tracker_error",
            EventDto::ScrapeReply { .. } => "scrape_reply",
            EventDto::ScrapeFailed { .. } => "scrape_failed",
            EventDto::WebSeedError { .. } => "web_seed_error",
            EventDto::PeersFound { .. } => "peers_found",
            EventDto::DhtBootstrapped { .. } => "dht_bootstrapped",
            EventDto::PeerConnected { .. } => "peer_connected",
            EventDto::PeerDisconnected { .. } => "peer_disconnected",
            EventDto::PeerBlocked { .. } => "peer_blocked",
            EventDto::FileError { .. } => "file_error",
            EventDto::StorageMoved { .. } => "storage_moved",
            EventDto::StorageMoveFailed { .. } => "storage_move_failed",
            EventDto::FileRenamed { .. } => "file_renamed",
            EventDto::FileRenameFailed { .. } => "file_rename_failed",
            EventDto::PortMapped { .. } => "port_mapped",
            EventDto::PortMappingFailed { .. } => "port_mapping_failed",
            EventDto::ExternalIpChanged { .. } => "external_ip_changed",
            EventDto::ProxyError { .. } => "proxy_error",
            EventDto::Lagged { .. } => "lagged",
            EventDto::Other { .. } => "other",
        }
    }
}

/// The 40-hex wire hash of a torrent.
fn torrent_id(torrent: &InfoHashes) -> String {
    hex(&torrent.wire_hash())
}

fn disconnect_reason(reason: &DisconnectReason) -> (&'static str, Option<String>) {
    match reason {
        DisconnectReason::SelfConnection => ("self_connection", None),
        DisconnectReason::InfoHashMismatch => ("info_hash_mismatch", None),
        DisconnectReason::HandshakeTimeout => ("handshake_timeout", None),
        DisconnectReason::IdleTimeout => ("idle_timeout", None),
        DisconnectReason::Encryption(m) => ("encryption", Some(m.clone())),
        DisconnectReason::Protocol(m) => ("protocol", Some(m.clone())),
        DisconnectReason::Io(m) => ("io", Some(m.clone())),
        DisconnectReason::Closed => ("closed", None),
        DisconnectReason::Shutdown => ("shutdown", None),
        DisconnectReason::Duplicate => ("duplicate", None),
        DisconnectReason::BadMetadata => ("bad_metadata", None),
        DisconnectReason::UploadOnly => ("upload_only", None),
        DisconnectReason::Blocked(r) => ("blocked", Some(block_reason(*r).to_owned())),
        // A reason a later `fbt` adds.
        _ => ("unknown", None),
    }
}

fn block_reason(reason: BlockReason) -> &'static str {
    match reason {
        BlockReason::IpFilter => "ip_filter",
        BlockReason::Banned => "banned",
        BlockReason::PerIpLimit => "per_ip_limit",
        // A reason a later `fbt` adds.
        _ => "unknown",
    }
}

fn map_protocol(protocol: MapProtocol) -> &'static str {
    match protocol {
        MapProtocol::Tcp => "tcp",
        MapProtocol::Udp => "udp",
        // A protocol a later `fbt` adds.
        _ => "unknown",
    }
}

fn map_method(method: MapMethod) -> &'static str {
    match method {
        MapMethod::Upnp => "upnp",
        MapMethod::NatPmp => "nat_pmp",
        MapMethod::Pcp => "pcp",
        // A method a later `fbt` adds.
        _ => "unknown",
    }
}

fn external_ip_source(source: ExternalIpSource) -> &'static str {
    match source {
        ExternalIpSource::PortMapping => "port_mapping",
        ExternalIpSource::Dht => "dht",
        ExternalIpSource::Peers => "peers",
        // A source a later `fbt` adds.
        _ => "unknown",
    }
}

impl From<&Event> for EventDto {
    fn from(event: &Event) -> Self {
        match event {
            Event::TorrentAdded { torrent } => EventDto::TorrentAdded {
                torrent: torrent_id(torrent),
            },
            Event::TorrentRemoved { torrent } => EventDto::TorrentRemoved {
                torrent: torrent_id(torrent),
            },
            Event::StateChanged { torrent, state } => EventDto::StateChanged {
                torrent: torrent_id(torrent),
                state: state_name(state),
                checking_progress: match state {
                    TorrentState::CheckingFiles { progress } => Some(*progress),
                    _ => None,
                },
            },
            Event::PieceFinished { torrent, piece } => EventDto::PieceFinished {
                torrent: torrent_id(torrent),
                piece: *piece,
            },
            Event::HashFailed { torrent, piece } => EventDto::HashFailed {
                torrent: torrent_id(torrent),
                piece: *piece,
            },
            Event::MetadataReceived { torrent } => EventDto::MetadataReceived {
                torrent: torrent_id(torrent),
            },
            Event::TorrentFinished { torrent } => EventDto::TorrentFinished {
                torrent: torrent_id(torrent),
            },
            Event::SeedLimitReached { torrent } => EventDto::SeedLimitReached {
                torrent: torrent_id(torrent),
            },
            Event::TrackerReply {
                torrent,
                info_hash,
                url,
                peers,
            } => EventDto::TrackerReply {
                torrent: torrent_id(torrent),
                info_hash: hex(info_hash),
                url: url.clone(),
                peers: *peers,
            },
            Event::TrackerError {
                torrent,
                url,
                message,
            } => EventDto::TrackerError {
                torrent: torrent_id(torrent),
                url: url.clone(),
                message: message.clone(),
            },
            Event::ScrapeReply {
                torrent,
                url,
                complete,
                incomplete,
                downloaded,
            } => EventDto::ScrapeReply {
                torrent: torrent_id(torrent),
                url: url.clone(),
                complete: *complete,
                incomplete: *incomplete,
                downloaded: *downloaded,
            },
            Event::ScrapeFailed {
                torrent,
                url,
                message,
            } => EventDto::ScrapeFailed {
                torrent: torrent_id(torrent),
                url: url.clone(),
                message: message.clone(),
            },
            Event::WebSeedError {
                torrent,
                url,
                message,
                retired,
            } => EventDto::WebSeedError {
                torrent: torrent_id(torrent),
                url: url.clone(),
                message: message.clone(),
                retired: *retired,
            },
            Event::PeersFound {
                torrent,
                source,
                count,
            } => EventDto::PeersFound {
                torrent: torrent_id(torrent),
                source: peer_source_name(*source),
                count: *count,
            },
            Event::DhtBootstrapped { nodes } => EventDto::DhtBootstrapped { nodes: *nodes },
            Event::PeerConnected {
                torrent,
                addr,
                transport,
                encrypted,
            } => EventDto::PeerConnected {
                torrent: torrent_id(torrent),
                addr: addr.to_string(),
                transport: transport_name(*transport),
                encrypted: *encrypted,
            },
            Event::PeerDisconnected {
                torrent,
                addr,
                reason,
            } => {
                let (reason, detail) = disconnect_reason(reason);
                EventDto::PeerDisconnected {
                    torrent: torrent_id(torrent),
                    addr: addr.to_string(),
                    reason,
                    detail,
                }
            }
            Event::PeerBlocked {
                torrent,
                addr,
                reason,
            } => EventDto::PeerBlocked {
                torrent: torrent.as_ref().map(torrent_id),
                addr: addr.to_string(),
                reason: block_reason(*reason),
            },
            Event::FileError {
                path,
                torrent,
                message,
            } => EventDto::FileError {
                torrent: torrent_id(torrent),
                path: path.as_ref().map(|p| p.to_string_lossy().into_owned()),
                message: message.clone(),
            },
            Event::StorageMoved { torrent, path } => EventDto::StorageMoved {
                torrent: torrent_id(torrent),
                path: path.to_string_lossy().into_owned(),
            },
            Event::StorageMoveFailed { torrent, message } => EventDto::StorageMoveFailed {
                torrent: torrent_id(torrent),
                message: message.clone(),
            },
            Event::FileRenamed {
                torrent,
                index,
                path,
            } => EventDto::FileRenamed {
                torrent: torrent_id(torrent),
                index: *index,
                path: path.clone(),
            },
            Event::FileRenameFailed {
                torrent,
                index,
                message,
            } => EventDto::FileRenameFailed {
                torrent: torrent_id(torrent),
                index: *index,
                message: message.clone(),
            },
            Event::PortMapped {
                protocol,
                method,
                external,
                ..
            } => EventDto::PortMapped {
                protocol: map_protocol(*protocol),
                method: map_method(*method),
                external: external.to_string(),
            },
            Event::PortMappingFailed {
                protocol,
                method,
                message,
            } => EventDto::PortMappingFailed {
                protocol: map_protocol(*protocol),
                method: map_method(*method),
                message: message.clone(),
            },
            Event::ExternalIpChanged { ip, source } => EventDto::ExternalIpChanged {
                ip: ip.to_string(),
                source: external_ip_source(*source),
            },
            Event::ProxyError { message } => EventDto::ProxyError {
                message: message.clone(),
            },
            Event::Lagged { missed } => EventDto::Lagged { missed: *missed },
            // An event a later `fbt` adds.
            other => EventDto::Other {
                debug: format!("{other:?}"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One `EventDto` of every variant, in declaration order.
    fn every_event() -> Vec<EventDto> {
        vec![
            EventDto::TorrentAdded {
                torrent: String::new(),
            },
            EventDto::TorrentRemoved {
                torrent: String::new(),
            },
            EventDto::StateChanged {
                torrent: String::new(),
                state: "",
                checking_progress: None,
            },
            EventDto::PieceFinished {
                torrent: String::new(),
                piece: 0,
            },
            EventDto::HashFailed {
                torrent: String::new(),
                piece: 0,
            },
            EventDto::MetadataReceived {
                torrent: String::new(),
            },
            EventDto::TorrentFinished {
                torrent: String::new(),
            },
            EventDto::SeedLimitReached {
                torrent: String::new(),
            },
            EventDto::TrackerReply {
                torrent: String::new(),
                info_hash: String::new(),
                url: String::new(),
                peers: 0,
            },
            EventDto::TrackerError {
                torrent: String::new(),
                url: String::new(),
                message: String::new(),
            },
            EventDto::ScrapeReply {
                torrent: String::new(),
                url: String::new(),
                complete: 0,
                incomplete: 0,
                downloaded: 0,
            },
            EventDto::ScrapeFailed {
                torrent: String::new(),
                url: String::new(),
                message: String::new(),
            },
            EventDto::WebSeedError {
                torrent: String::new(),
                url: String::new(),
                message: String::new(),
                retired: false,
            },
            EventDto::PeersFound {
                torrent: String::new(),
                source: "",
                count: 0,
            },
            EventDto::DhtBootstrapped { nodes: 0 },
            EventDto::PeerConnected {
                torrent: String::new(),
                addr: String::new(),
                transport: "",
                encrypted: false,
            },
            EventDto::PeerDisconnected {
                torrent: String::new(),
                addr: String::new(),
                reason: "",
                detail: None,
            },
            EventDto::PeerBlocked {
                torrent: None,
                addr: String::new(),
                reason: "",
            },
            EventDto::FileError {
                torrent: String::new(),
                path: None,
                message: String::new(),
            },
            EventDto::StorageMoved {
                torrent: String::new(),
                path: String::new(),
            },
            EventDto::StorageMoveFailed {
                torrent: String::new(),
                message: String::new(),
            },
            EventDto::FileRenamed {
                torrent: String::new(),
                index: 0,
                path: String::new(),
            },
            EventDto::FileRenameFailed {
                torrent: String::new(),
                index: 0,
                message: String::new(),
            },
            EventDto::PortMapped {
                protocol: "",
                method: "",
                external: String::new(),
            },
            EventDto::PortMappingFailed {
                protocol: "",
                method: "",
                message: String::new(),
            },
            EventDto::ExternalIpChanged {
                ip: String::new(),
                source: "",
            },
            EventDto::ProxyError {
                message: String::new(),
            },
            EventDto::Lagged { missed: 0 },
            EventDto::Other {
                debug: String::new(),
            },
        ]
    }

    /// The declaration position of `e`'s variant; no wildcard, so a new
    /// variant fails to compile here until [`every_event`] lists it.
    fn ordinal(e: &EventDto) -> usize {
        match e {
            EventDto::TorrentAdded { .. } => 0,
            EventDto::TorrentRemoved { .. } => 1,
            EventDto::StateChanged { .. } => 2,
            EventDto::PieceFinished { .. } => 3,
            EventDto::HashFailed { .. } => 4,
            EventDto::MetadataReceived { .. } => 5,
            EventDto::TorrentFinished { .. } => 6,
            EventDto::SeedLimitReached { .. } => 7,
            EventDto::TrackerReply { .. } => 8,
            EventDto::TrackerError { .. } => 9,
            EventDto::ScrapeReply { .. } => 10,
            EventDto::ScrapeFailed { .. } => 11,
            EventDto::WebSeedError { .. } => 12,
            EventDto::PeersFound { .. } => 13,
            EventDto::DhtBootstrapped { .. } => 14,
            EventDto::PeerConnected { .. } => 15,
            EventDto::PeerDisconnected { .. } => 16,
            EventDto::PeerBlocked { .. } => 17,
            EventDto::FileError { .. } => 18,
            EventDto::StorageMoved { .. } => 19,
            EventDto::StorageMoveFailed { .. } => 20,
            EventDto::FileRenamed { .. } => 21,
            EventDto::FileRenameFailed { .. } => 22,
            EventDto::PortMapped { .. } => 23,
            EventDto::PortMappingFailed { .. } => 24,
            EventDto::ExternalIpChanged { .. } => 25,
            EventDto::ProxyError { .. } => 26,
            EventDto::Lagged { .. } => 27,
            EventDto::Other { .. } => 28,
        }
    }

    #[test]
    fn event_kind_is_the_serde_type_for_every_variant() {
        let events = every_event();
        let ordinals: Vec<usize> = events.iter().map(ordinal).collect();
        assert_eq!(ordinals, (0..29).collect::<Vec<_>>());
        for event in &events {
            let value = serde_json::to_value(event).unwrap();
            assert_eq!(value["type"], event.kind(), "{event:?}");
        }
    }

    #[test]
    fn capped_refuses_one_past_the_cap() {
        let ok: Capped<u8, 3> = serde_json::from_str("[1,2,3]").unwrap();
        assert_eq!(ok.0, [1, 2, 3]);
        let empty: Capped<u8, 3> = serde_json::from_str("[]").unwrap();
        assert!(empty.0.is_empty());
        let err = serde_json::from_str::<Capped<u8, 3>>("[1,2,3,4]").unwrap_err();
        assert!(err.to_string().contains("at most 3"), "{err}");
        assert!(serde_json::from_str::<Capped<u8, 3>>("null").is_err());
    }
}
