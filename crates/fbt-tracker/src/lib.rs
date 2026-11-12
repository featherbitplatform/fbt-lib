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
mod scrape;
mod tls;
mod types;
mod udp;
mod urlencode;

use std::sync::Arc;

use fbt_core::udp::{DatagramHandler, DatagramSender};
use fbt_net::proxy::ProxyConfig;

#[doc(hidden)]
pub use http::parse_http_announce;
#[doc(hidden)]
pub use scrape::{parse_http_scrape, parse_udp_scrape};
pub use types::{
    AnnounceEvent, AnnounceRequest, AnnounceResponse, MAX_SCRAPE_HASHES, ScrapeInfo, TrackerConfig,
    TrackerError,
};
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

    /// Scrapes `url` for `hashes` (BEP 48 over HTTP, BEP 15 action 2 over
    /// UDP), through the same transport, proxy included, as
    /// [`announce`](Self::announce).
    ///
    /// An HTTP URL is scraped when the last segment of its path starts
    /// with `announce` (replaced by `scrape`, the query kept), else this is
    /// [`TrackerError::ScrapeUnsupported`]. More than [`MAX_SCRAPE_HASHES`]
    /// hashes is [`TrackerError::TooManyHashes`], before any request. A UDP
    /// scrape shares the announces' connection ids. Hashes the tracker left
    /// out of its reply are left out of the result; no hashes, no request.
    pub async fn scrape(
        &self,
        url: &str,
        hashes: &[[u8; 20]],
    ) -> Result<Vec<ScrapeInfo>, TrackerError> {
        if hashes.len() > MAX_SCRAPE_HASHES {
            return Err(TrackerError::TooManyHashes);
        }
        let scheme = url
            .split_once("://")
            .map(|(s, _)| s.to_ascii_lowercase())
            .ok_or_else(|| TrackerError::InvalidUrl("missing scheme".to_owned()))?;
        match scheme.as_str() {
            "http" | "https" => {
                let scrape = scrape::scrape_url(url)?;
                if hashes.is_empty() {
                    return Ok(Vec::new());
                }
                self.inner.http.scrape(&scrape, hashes).await
            }
            "udp" if hashes.is_empty() => Ok(Vec::new()),
            "udp" => self.inner.udp.scrape(url, hashes).await,
            other => Err(TrackerError::UnsupportedScheme(other.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests;
