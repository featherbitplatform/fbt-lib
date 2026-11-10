//! Announce request/response types and errors.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use fbt_core::PeerId;

/// Floor applied to every tracker-supplied announce interval.
pub(crate) const MIN_INTERVAL: Duration = Duration::from_secs(60);

/// The `event` of an announce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnounceEvent {
    /// A regular periodic announce.
    None,
    /// First announce of a download.
    Started,
    /// The download finished.
    Completed,
    /// The client is shutting down this torrent.
    Stopped,
}

/// What we tell the tracker.
#[derive(Debug, Clone)]
pub struct AnnounceRequest {
    /// The torrent's SHA-1 info hash.
    pub info_hash: [u8; 20],
    /// Our peer id.
    pub peer_id: PeerId,
    /// The port we listen on.
    pub port: u16,
    /// Bytes uploaded so far.
    pub uploaded: u64,
    /// Bytes downloaded so far.
    pub downloaded: u64,
    /// Bytes left to download.
    pub left: u64,
    /// The announce event.
    pub event: AnnounceEvent,
    /// How many peers we would like.
    pub num_want: Option<u32>,
    /// Random key identifying us across IP changes.
    pub key: u32,
    /// The tracker id from a previous response.
    pub tracker_id: Option<Vec<u8>>,
}

/// What the tracker tells us.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnounceResponse {
    /// Seconds until the next announce (at least 60 s).
    pub interval: Duration,
    /// Minimum announce interval (at least 30 s).
    pub min_interval: Option<Duration>,
    /// Peers returned.
    pub peers: Vec<SocketAddr>,
    /// Number of seeders, if reported.
    pub complete: Option<u32>,
    /// Number of leechers, if reported.
    pub incomplete: Option<u32>,
    /// Tracker id to echo back.
    pub tracker_id: Option<Vec<u8>>,
    /// Non-fatal warning from the tracker.
    pub warning: Option<String>,
    /// Our address as the tracker sees it.
    pub external_ip: Option<IpAddr>,
}

/// Tracker client errors.
#[derive(Debug, thiserror::Error)]
pub enum TrackerError {
    /// The URL could not be parsed.
    #[error("invalid tracker url: {0}")]
    InvalidUrl(String),
    /// The URL scheme is not supported.
    #[error("unsupported tracker scheme: {0}")]
    UnsupportedScheme(String),
    /// An `https` URL was used but no TLS feature is enabled.
    #[error("https tracker but no TLS feature enabled")]
    TlsDisabled,
    /// The tracker answered with a `failure reason`.
    #[error("tracker failure: {0}")]
    Failure(String),
    /// A non-success HTTP status.
    #[error("http status {0}")]
    HttpStatus(u16),
    /// More than 3 redirects.
    #[error("too many redirects")]
    TooManyRedirects,
    /// The response body exceeded the configured cap.
    #[error("response too large")]
    ResponseTooLarge,
    /// The response was malformed.
    #[error("invalid tracker response: {0}")]
    InvalidResponse(&'static str),
    /// The request timed out.
    #[error("tracker request timed out")]
    Timeout,
    /// An I/O or transport error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Tracker client configuration.
#[derive(Debug, Clone)]
pub struct TrackerConfig {
    /// `User-Agent` header value.
    pub user_agent: String,
    /// Overall timeout of one HTTP announce.
    pub http_timeout: Duration,
    /// Initial UDP retransmission timeout.
    pub udp_base_timeout: Duration,
    /// UDP retransmissions before giving up.
    pub udp_max_retries: u32,
    /// Maximum accepted response body size.
    pub max_response_bytes: usize,
}

impl Default for TrackerConfig {
    fn default() -> Self {
        TrackerConfig {
            user_agent: concat!("fbt/", env!("CARGO_PKG_VERSION")).to_owned(),
            http_timeout: Duration::from_secs(30),
            udp_base_timeout: Duration::from_secs(15),
            udp_max_retries: 2,
            max_response_bytes: 2 * 1024 * 1024,
        }
    }
}
