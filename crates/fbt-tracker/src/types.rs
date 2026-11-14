//! Announce request/response types and errors.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use fbt_core::{MAX_DISPLAY_CHARS, PeerId, display_safe};

/// Floor applied to every tracker-supplied announce interval.
pub(crate) const MIN_INTERVAL: Duration = Duration::from_secs(60);
/// Longest tracker-supplied text read (failure reasons, warnings, UDP error
/// messages) before it is made display-safe, in bytes. The cap a user sees is
/// [`MAX_DISPLAY_CHARS`] characters; this one only bounds the lossy decode of a
/// hostile reply.
pub(crate) const MAX_TEXT_BYTES: usize = 1024;
/// Longest `tracker id` kept; longer ones are dropped.
pub(crate) const MAX_TRACKER_ID_BYTES: usize = 256;

/// Tracker-supplied text as a display-safe string: the raw bytes are cut to
/// [`MAX_TEXT_BYTES`] and decoded lossily, then [`display_safe`] drops control
/// and Unicode format characters, collapses whitespace and cuts the result to
/// [`MAX_DISPLAY_CHARS`] characters.
pub(crate) fn bounded_text(raw: &[u8]) -> String {
    let head = raw.get(..MAX_TEXT_BYTES).unwrap_or(raw);
    display_safe(&String::from_utf8_lossy(head), MAX_DISPLAY_CHARS)
}

/// The `event` of an announce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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
    /// The port we listen on. HTTP announces always carry it (the
    /// tracker's address family is not known before the request).
    pub port: u16,
    /// The port UDP announces over IPv4 carry instead of `port`, for a
    /// client listening on another port over IPv4.
    pub port_v4: Option<u16>,
    /// The port UDP announces over IPv6 carry instead of `port`.
    pub port_v6: Option<u16>,
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

impl AnnounceRequest {
    /// The port to announce over IPv6 (`ipv6`) or IPv4: that family's
    /// override, else [`port`](Self::port).
    pub fn port_for(&self, ipv6: bool) -> u16 {
        let family = if ipv6 { self.port_v6 } else { self.port_v4 };
        family.unwrap_or(self.port)
    }
}

/// What the tracker tells us. Tracker-supplied text is display-safe and at most
/// [`MAX_DISPLAY_CHARS`] characters, and a tracker id longer than 256 bytes is
/// dropped.
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
#[non_exhaustive]
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
    /// The tracker answered with a `failure reason`: display-safe and at most
    /// [`MAX_DISPLAY_CHARS`] characters.
    #[error("tracker failure: {0}")]
    Failure(String),
    /// A non-success HTTP status.
    #[error("http status {0}")]
    HttpStatus(u16),
    /// More than 3 redirects.
    #[error("too many redirects")]
    TooManyRedirects,
    /// A redirect target was refused because it is, or answers with, an address inside the
    /// local network while the policy denied that ([`RedirectPolicy::allow_internal`](crate::RedirectPolicy)
    /// is false): nothing was sent to it. Only a range fetch (a web seed) reports this;
    /// announces keep the behaviour they always had.
    #[error("redirect to an internal address")]
    InternalRedirect,
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
    /// The proxy could not be reached or refused the tunnel.
    #[error("proxy error: {0}")]
    Proxy(String),
    /// A UDP tracker while trackers go through an HTTP proxy, which cannot
    /// carry UDP.
    #[error("UDP trackers cannot go through an HTTP proxy")]
    ProxyUnsupported,
    /// The HTTP tracker has no scrape URL: the last segment of its announce
    /// path does not start with `announce` (BEP 48).
    #[error("tracker does not support scrape")]
    ScrapeUnsupported,
    /// A scrape of more than [`MAX_SCRAPE_HASHES`](crate::MAX_SCRAPE_HASHES)
    /// info hashes; nothing was sent.
    #[error("too many info hashes in one scrape")]
    TooManyHashes,
    /// A range fetch (a web seed) averaged less than
    /// 4 KiB/s once it had run for 30 s.
    #[error("the server sends slower than the minimum rate")]
    TooSlow,
}

/// Most info hashes in one scrape: what fits a UDP scrape request in a
/// 1500-byte datagram (BEP 15), and the HTTP limit too.
pub const MAX_SCRAPE_HASHES: usize = 74;

/// One torrent's counts from a scrape (BEP 48, BEP 15).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ScrapeInfo {
    /// The torrent's info hash.
    pub info_hash: [u8; 20],
    /// Seeds.
    pub complete: u32,
    /// Completed downloads the tracker has seen.
    pub downloaded: u32,
    /// Leechers.
    pub incomplete: u32,
}

/// Tracker client configuration.
///
/// Build with [`TrackerConfig::default`] and assign the fields to change.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
#[non_exhaustive]
pub struct TrackerConfig {
    /// `User-Agent` header value.
    pub user_agent: String,
    /// Overall timeout of one HTTP announce; also bounds the DNS lookup of a
    /// UDP tracker.
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
