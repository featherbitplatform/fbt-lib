//! Wire types: the JSON the API sends and takes, with their OpenAPI
//! schemas.
//!
//! Conventions (every DTO follows them): hashes are lowercase hex,
//! durations are seconds as `f64`, rates are bytes per second, states and
//! enums are snake_case strings, addresses and paths are strings. DTOs are
//! built by `From` impls from `fbt` types; request DTOs use
//! `#[serde(deny_unknown_fields)]`. A DTO named in a route's
//! `#[utoipa::path]` (`body = ...`) lands in the document's
//! `components.schemas` on its own.

use fbt::Session;
use serde::Serialize;
use utoipa::ToSchema;

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
