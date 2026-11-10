//! Session settings.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use fbt_tracker::TrackerConfig;

/// Largest accepted [`Settings::event_queue`]; the queue is allocated up
/// front.
pub const MAX_EVENT_QUEUE: usize = 1 << 20;
/// Largest accepted [`Settings::max_incoming_requests`]; each peer's command
/// queue is sized from it.
pub const MAX_INCOMING_REQUESTS: usize = 1 << 16;
/// Largest accepted [`Settings::max_metadata_size`].
pub const MAX_METADATA_SIZE: usize = 64 * 1024 * 1024;
/// Smallest accepted [`Settings::max_metadata_size`]: one metadata piece.
const MIN_METADATA_SIZE: usize = 16_384;

/// Whether peer connections use MSE/PE (message stream encryption).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum EncryptionPolicy {
    /// Encrypted (RC4) connections only, in both directions.
    Forced,
    /// Outgoing connections try MSE (offering RC4 or plaintext) and fall
    /// back to a plain BitTorrent handshake once per address when the MSE
    /// handshake fails; incoming connections may be either.
    #[default]
    Preferred,
    /// Plain BitTorrent handshakes only; incoming MSE connections are
    /// refused.
    Disabled,
}

/// Tunables for a session and its torrents.
///
/// Build with [`Settings::default`] and assign the fields to change.
/// [`Session::new`](crate::Session::new) refuses values that cannot work
/// (zero intervals and timeouts that drive timers, queues of zero or beyond
/// [`MAX_EVENT_QUEUE`] / [`MAX_INCOMING_REQUESTS`], a metadata cap outside
/// 16 KiB..=[`MAX_METADATA_SIZE`]) with
/// [`SessionError::InvalidSettings`](crate::SessionError::InvalidSettings).
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
    /// Requests from one peer queued before further ones are dropped
    /// (1 to [`MAX_INCOMING_REQUESTS`]).
    pub max_incoming_requests: usize,
    /// TCP connect timeout.
    pub connect_timeout: Duration,
    /// Time allowed for the handshake exchange, the MSE handshake included.
    pub handshake_timeout: Duration,
    /// MSE/PE policy for outgoing and incoming connections.
    pub encryption: EncryptionPolicy,
    /// A peer that sends nothing for this long is disconnected (non-zero).
    pub peer_idle_timeout: Duration,
    /// A keepalive goes out after this much send silence (non-zero).
    pub keepalive_interval: Duration,
    /// A request unanswered for this long is cancelled and re-picked
    /// (non-zero).
    pub request_timeout: Duration,
    /// Wait before a failed outgoing connection is tried again. Internal
    /// tunable (tests shorten it); not part of the stable API.
    #[doc(hidden)]
    pub peer_retry_delay: Duration,
    /// Time between choker runs (non-zero).
    pub rechoke_interval: Duration,
    /// Time between status updates (non-zero).
    pub status_interval: Duration,
    /// Capacity of the event queues (1 to [`MAX_EVENT_QUEUE`]).
    pub event_queue: usize,
    /// Peers asked from each tracker announce.
    pub num_want: u32,
    /// Tracker client settings.
    pub tracker: TrackerConfig,
    /// First wait after every tracker failed; doubles with each further
    /// failed round, up to 30 minutes. Internal tunable (tests shorten it);
    /// not part of the stable API. Non-zero.
    #[doc(hidden)]
    pub tracker_retry_base: Duration,
    /// While a magnet's metadata is pending and its peer slots are full, a
    /// peer connected this long without offering `ut_metadata` is closed
    /// to make room. Internal tunable (tests shorten it); not part of the
    /// stable API.
    #[doc(hidden)]
    pub metadata_peer_grace: Duration,
    /// Largest info dictionary a magnet torrent fetches from peers (BEP 9),
    /// in bytes; peers advertising more are disconnected. 16 KiB to
    /// [`MAX_METADATA_SIZE`].
    pub max_metadata_size: usize,
}

impl Settings {
    /// Checks the values `Session::new` cannot work with; the error names
    /// the field.
    pub(crate) fn validate(&self) -> Result<(), String> {
        let non_zero = [
            ("peer_idle_timeout", self.peer_idle_timeout),
            ("keepalive_interval", self.keepalive_interval),
            ("request_timeout", self.request_timeout),
            ("rechoke_interval", self.rechoke_interval),
            ("status_interval", self.status_interval),
            ("tracker_retry_base", self.tracker_retry_base),
        ];
        for (name, d) in non_zero {
            if d.is_zero() {
                return Err(format!("{name} must be greater than zero"));
            }
        }
        if !(1..=MAX_EVENT_QUEUE).contains(&self.event_queue) {
            return Err(format!(
                "event_queue must be between 1 and {MAX_EVENT_QUEUE}, got {}",
                self.event_queue
            ));
        }
        if !(1..=MAX_INCOMING_REQUESTS).contains(&self.max_incoming_requests) {
            return Err(format!(
                "max_incoming_requests must be between 1 and {MAX_INCOMING_REQUESTS}, got {}",
                self.max_incoming_requests
            ));
        }
        if !(MIN_METADATA_SIZE..=MAX_METADATA_SIZE).contains(&self.max_metadata_size) {
            return Err(format!(
                "max_metadata_size must be between {MIN_METADATA_SIZE} and {MAX_METADATA_SIZE}, got {}",
                self.max_metadata_size
            ));
        }
        Ok(())
    }
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
            encryption: EncryptionPolicy::default(),
            peer_idle_timeout: Duration::from_secs(120),
            keepalive_interval: Duration::from_secs(60),
            request_timeout: Duration::from_secs(60),
            peer_retry_delay: Duration::from_secs(60),
            rechoke_interval: Duration::from_secs(10),
            status_interval: Duration::from_millis(500),
            event_queue: 4096,
            num_want: 50,
            tracker: TrackerConfig::default(),
            tracker_retry_base: Duration::from_secs(60),
            metadata_peer_grace: Duration::from_secs(30),
            max_metadata_size: 16 * 1024 * 1024,
        }
    }
}
