//! [`ApiConfig`], [`Auth`] and their validation.

use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use axum::http::HeaderValue;

use crate::ApiError;
use crate::paths::Root;

/// The largest request body: a `.torrent` or resume upload (32 MiB). The
/// default of [`ApiConfig::max_body_bytes`].
pub const MAX_BODY_BYTES: usize = 32 << 20;

/// How the API authenticates its clients.
#[derive(Clone)]
#[non_exhaustive]
pub enum Auth {
    /// Every request but `GET /api/v1/openapi.json` and CORS preflights
    /// needs `Authorization: Bearer <token>`. The streaming and event URLs
    /// also take `?token=<token>`, since browser media elements and
    /// `EventSource` cannot send headers. The token must not be empty.
    Token(String),
    /// No authentication. Allowed only with a loopback
    /// [`bind`](ApiConfig::bind); requests whose `Host` is not
    /// `localhost`, `127.0.0.1` or `[::1]`, or whose `Origin` is not in
    /// [`cors_origins`](ApiConfig::cors_origins), get `403` (the guard
    /// against CSRF and DNS rebinding).
    Disabled,
}

impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // The token is a secret: never in logs.
            Auth::Token(_) => f.write_str("Token(<redacted>)"),
            Auth::Disabled => f.write_str("Disabled"),
        }
    }
}

/// The API's configuration. Build it with [`ApiConfig::new`] and change
/// the public fields; [`router`](crate::router) and
/// [`ApiServer::start`](crate::ApiServer::start) validate it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ApiConfig {
    /// The address [`ApiServer`](crate::ApiServer) listens on (and the one a
    /// client mounting [`router`](crate::router) promises to serve it on).
    /// Default `127.0.0.1:8090`.
    pub bind: SocketAddr,
    /// Authentication. Default `Auth::Token("")`, which validation refuses:
    /// a token must be set.
    pub auth: Auth,
    /// Origins allowed cross-origin requests (CORS), such as
    /// `http://localhost:5173`. Empty (the default) turns CORS off.
    pub cors_origins: Vec<String>,
    /// When set, the directories every path a request names (save paths,
    /// move targets, creation sources) must be inside. Each must exist.
    /// A path must be absolute, free of `..` and written below a root (as
    /// given here, or in its canonical form); symlinks in it are resolved
    /// and must stay inside. The check is lexical before it touches the
    /// filesystem, so a path must be spelled like the root: a different
    /// letter case on case-insensitive filesystems (macOS, Windows) is
    /// refused with `403`. Default `None`: any path.
    pub allowed_roots: Option<Vec<PathBuf>>,
    /// The largest request body; larger ones get `413`. Default
    /// [`MAX_BODY_BYTES`].
    pub max_body_bytes: usize,
    /// File streams served at once; more get `503`. Default 32.
    pub max_streams: usize,
    /// Event streams served at once; more get `503`. Default 16.
    pub max_event_streams: usize,
    /// Connections [`ApiServer`](crate::ApiServer) serves at once,
    /// authenticated or not. It accepts no more until one closes: further
    /// clients wait in the listener's backlog instead of taking file
    /// descriptors from the session. Default 256; 0 counts as 1. A client
    /// mounting [`router`](crate::router) applies its own limit.
    pub max_connections: usize,
}

impl Default for ApiConfig {
    fn default() -> Self {
        ApiConfig {
            bind: SocketAddr::from(([127, 0, 0, 1], 8090)),
            auth: Auth::Token(String::new()),
            cors_origins: Vec::new(),
            allowed_roots: None,
            max_body_bytes: MAX_BODY_BYTES,
            max_streams: 32,
            max_event_streams: 16,
            max_connections: 256,
        }
    }
}

impl ApiConfig {
    /// The defaults with bearer token `token`.
    pub fn new(token: impl Into<String>) -> Self {
        ApiConfig {
            auth: Auth::Token(token.into()),
            ..ApiConfig::default()
        }
    }

    /// Checks the config and computes what the router needs from it.
    pub(crate) fn validate(&self) -> Result<Validated, ApiError> {
        match &self.auth {
            Auth::Token(token) if token.is_empty() => return Err(ApiError::EmptyToken),
            Auth::Disabled if !is_loopback(self.bind.ip()) => {
                return Err(ApiError::InsecureBind);
            }
            _ => {}
        }
        let origins = self
            .cors_origins
            .iter()
            .map(|o| HeaderValue::from_str(o).map_err(|_| ApiError::InvalidOrigin))
            .collect::<Result<Vec<_>, _>>()?;
        let roots = match &self.allowed_roots {
            None => None,
            Some(roots) => Some(
                roots
                    .iter()
                    .map(|r| {
                        let canonical = r
                            .canonicalize()
                            .map_err(|_| ApiError::InvalidRoot(r.clone()))?;
                        Ok(Root {
                            given: r.clone(),
                            canonical,
                        })
                    })
                    .collect::<Result<Arc<[Root]>, ApiError>>()?,
            ),
        };
        Ok(Validated { origins, roots })
    }
}

/// What [`ApiConfig::validate`] computed.
pub(crate) struct Validated {
    /// [`ApiConfig::cors_origins`] as header values.
    pub(crate) origins: Vec<HeaderValue>,
    /// [`ApiConfig::allowed_roots`], as given and canonicalised.
    pub(crate) roots: Option<Arc<[Root]>>,
}

/// A loopback address, IPv4-mapped IPv6 (`::ffff:127.0.0.0/104`) included.
fn is_loopback(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback(),
        IpAddr::V6(v6) => {
            v6.is_loopback() || v6.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_debug_hides_the_token() {
        let config = ApiConfig::new("s3cret");
        let shown = format!("{config:?}");
        assert!(!shown.contains("s3cret"), "{shown}");
        assert!(shown.contains("Token(<redacted>)"), "{shown}");
    }

    #[test]
    fn loopback_addresses() {
        for ip in [
            "127.0.0.1",
            "127.1.2.3",
            "::1",
            "::ffff:127.0.0.1",
            "::ffff:127.255.0.1",
        ] {
            assert!(is_loopback(ip.parse().unwrap()), "{ip}");
        }
        for ip in [
            "0.0.0.0",
            "::",
            "10.0.0.1",
            "::ffff:10.0.0.1",
            "::127.0.0.1",
        ] {
            assert!(!is_loopback(ip.parse().unwrap()), "{ip}");
        }
    }
}
