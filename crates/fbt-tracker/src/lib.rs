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

mod http;
mod tls;
mod types;
mod urlencode;

use std::sync::Arc;

#[doc(hidden)]
pub use http::parse_http_announce;
pub use types::{AnnounceEvent, AnnounceRequest, AnnounceResponse, TrackerConfig, TrackerError};
pub use urlencode::url_encode_bytes;

/// A tracker client; cheap to clone.
#[derive(Debug, Clone)]
pub struct TrackerClient {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    http: http::HttpClient,
}

impl TrackerClient {
    /// Builds a client from `cfg`.
    pub fn new(cfg: TrackerConfig) -> Result<TrackerClient, TrackerError> {
        Ok(TrackerClient {
            inner: Arc::new(Inner {
                http: http::HttpClient::new(&cfg)?,
            }),
        })
    }

    /// Announces to `url`, dispatching on its scheme (`http`, `https`, `udp`).
    ///
    /// `udp://` is not implemented yet and yields `UnsupportedScheme`.
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
            other => Err(TrackerError::UnsupportedScheme(other.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests;
