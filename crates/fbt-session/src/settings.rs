//! Session settings.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use fbt_tracker::TrackerConfig;

/// Tunables for a session and its torrents.
///
/// Build with [`Settings::default`] and assign the fields to change.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Settings {
    /// Address the peer listener binds.
    pub listen_addr: SocketAddr,
    /// Connected peers per torrent.
    pub max_peers_per_torrent: usize,
    /// Connected peers across all torrents.
    pub max_peers_global: usize,
    /// Outgoing connections still connecting or handshaking.
    pub max_half_open: usize,
    /// Upload slots per torrent (regular plus optimistic).
    pub unchoke_slots: usize,
    /// Block requests in flight to one peer.
    pub max_outstanding_requests: usize,
    /// Requests from one peer queued before further ones are dropped.
    pub max_incoming_requests: usize,
    /// TCP connect timeout.
    pub connect_timeout: Duration,
    /// Time allowed for the handshake exchange.
    pub handshake_timeout: Duration,
    /// A peer that sends nothing for this long is disconnected.
    pub peer_idle_timeout: Duration,
    /// A keepalive goes out after this much send silence.
    pub keepalive_interval: Duration,
    /// A request unanswered for this long is cancelled and re-picked.
    pub request_timeout: Duration,
    /// Wait before a failed outgoing connection is tried again. Internal
    /// tunable (tests shorten it); not part of the stable API.
    #[doc(hidden)]
    pub peer_retry_delay: Duration,
    /// Time between choker runs.
    pub rechoke_interval: Duration,
    /// Time between status updates.
    pub status_interval: Duration,
    /// Capacity of the event queues.
    pub event_queue: usize,
    /// Peers asked from each tracker announce.
    pub num_want: u32,
    /// Tracker client settings.
    pub tracker: TrackerConfig,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            listen_addr: SocketAddr::from((Ipv4Addr::UNSPECIFIED, 6881)),
            max_peers_per_torrent: 50,
            max_peers_global: 200,
            max_half_open: 10,
            unchoke_slots: 4,
            max_outstanding_requests: 32,
            max_incoming_requests: 250,
            connect_timeout: Duration::from_secs(10),
            handshake_timeout: Duration::from_secs(10),
            peer_idle_timeout: Duration::from_secs(120),
            keepalive_interval: Duration::from_secs(60),
            request_timeout: Duration::from_secs(60),
            peer_retry_delay: Duration::from_secs(60),
            rechoke_interval: Duration::from_secs(10),
            status_interval: Duration::from_millis(500),
            event_queue: 4096,
            num_want: 50,
            tracker: TrackerConfig::default(),
        }
    }
}
