//! fbt-tracker: Tracker clients (HTTP and UDP).

#![forbid(unsafe_code)]
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic
    )
)]

mod connector;
mod http;
mod tls;
mod types;
mod udp;
mod urlencode;

use std::sync::Arc;

use fbt_core::udp::{DatagramHandler, DatagramSender};
use fbt_net::proxy::ProxyConfig;

#[doc(hidden)]
pub use http::parse_http_announce;
pub use types::{AnnounceEvent, AnnounceRequest, AnnounceResponse, TrackerConfig, TrackerError};
#[doc(hidden)]
pub use udp::parse_udp_announce;
pub use urlencode::url_encode_bytes;

/// A tracker client; cheap to clone.
#[derive(Debug, Clone)]
pub struct TrackerClient {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    http: http::HttpClient,
    udp: udp::UdpClient,
}

impl TrackerClient {
    /// Builds a client from `cfg`.
    pub fn new(cfg: TrackerConfig) -> Result<TrackerClient, TrackerError> {
        Ok(TrackerClient {
            inner: Arc::new(Inner {
                http: http::HttpClient::new(&cfg, None)?,
                udp: udp::UdpClient::new(&cfg),
            }),
        })
    }

    /// Like [`TrackerClient::new`], but UDP announces go out through the
    /// session's shared sockets `udp` (one per address family), so trackers
    /// see the listen port. Feed their replies to
    /// [`TrackerClient::udp_handler`]. A family without a shared socket binds
    /// its own.
    ///
    /// With a `proxy` (other than [`ProxyKind::None`](fbt_net::proxy::ProxyKind::None)):
    ///
    /// - HTTP(S) announces tunnel through it when `proxy_trackers` is set,
    ///   and fail with [`TrackerError::Proxy`] when the proxy does.
    /// - SOCKS5 (with `proxy_peers` or `proxy_trackers`): `udp` holds the one
    ///   relayed socket, and every UDP announce, of either family, goes
    ///   through it; no other socket is bound. Tracker host names are
    ///   resolved locally.
    /// - HTTP with `proxy_trackers`: UDP announces fail with
    ///   [`TrackerError::ProxyUnsupported`].
    pub fn new_with(
        cfg: TrackerConfig,
        udp: Vec<Arc<dyn DatagramSender>>,
        proxy: Option<ProxyConfig>,
    ) -> Result<TrackerClient, TrackerError> {
        Ok(TrackerClient {
            inner: Arc::new(Inner {
                http: http::HttpClient::new(&cfg, proxy.as_ref())?,
                udp: udp::UdpClient::with_sockets(&cfg, udp, proxy.as_ref()),
            }),
        })
    }

    /// The handler for tracker replies arriving on the shared sockets.
    pub fn udp_handler(&self) -> Arc<dyn DatagramHandler> {
        self.inner.udp.handler()
    }

    /// Announces to `url`, dispatching on its scheme (`http`, `https`, `udp`).
    pub async fn announce(
        &self,
        url: &str,
        req: &AnnounceRequest,
    ) -> Result<AnnounceResponse, TrackerError> {
        let scheme = url
            .split_once("://")
            .map(|(s, _)| s.to_ascii_lowercase())
            .ok_or_else(|| TrackerError::InvalidUrl("missing scheme".to_owned()))?;
        match scheme.as_str() {
            "http" | "https" => self.inner.http.announce(url, req).await,
            "udp" => self.inner.udp.announce(url, req).await,
            other => Err(TrackerError::UnsupportedScheme(other.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests;
