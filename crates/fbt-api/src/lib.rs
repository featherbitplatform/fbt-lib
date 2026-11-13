//! fbt-api: an HTTP/JSON API over an [`fbt::Session`]: control of every
//! torrent and session setting, server-sent events and byte-range file
//! streaming, with an OpenAPI document at `/api/v1/openapi.json`.
//!
//! Mount [`router`] in your own axum server, or run [`ApiServer`].
//!
//! # Mounting the router
//!
//! ```no_run
//! # async fn demo(session: fbt::Session) -> Result<(), Box<dyn std::error::Error>> {
//! use fbt_api::{ApiConfig, router};
//!
//! let app = router(session, &ApiConfig::new("secret-token"))?;
//! let listener = tokio::net::TcpListener::bind("127.0.0.1:8090").await?;
//! axum::serve(listener, app).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Running the standalone server
//!
//! ```no_run
//! # async fn demo(session: fbt::Session) -> Result<(), Box<dyn std::error::Error>> {
//! use fbt_api::{ApiConfig, ApiServer};
//!
//! let server = ApiServer::start(session, ApiConfig::new("secret-token")).await?;
//! println!("API on http://{}", server.local_addr());
//! // ... later: stop accepting, end streams, abort stragglers after 5 s.
//! server.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Security model
//!
//! Every client is untrusted until authenticated: request bodies, query
//! strings, path parameters and headers never panic the server, and every
//! body and collection has a size cap. A token holder is fully trusted:
//! the API is the session's own control surface
//! ([`ApiConfig::allowed_roots`] limits the paths it may name).
//!
//! - **Token.** With [`Auth::Token`] every request but
//!   `GET /api/v1/openapi.json` and CORS preflights needs
//!   `Authorization: Bearer <token>`. The file-stream and event URLs also
//!   take `?token=`, because browser media elements and `EventSource`
//!   cannot send headers; no other route does.
//! - **Loopback and no auth.** [`Auth::Disabled`] is allowed only with a
//!   loopback [`ApiConfig::bind`]. Requests whose `Host` is not
//!   `localhost`, `127.0.0.1` or `[::1]`, or whose `Origin` is not in
//!   [`ApiConfig::cors_origins`], get `403`: the guard against CSRF and DNS
//!   rebinding.
//! - **CORS.** Off unless [`ApiConfig::cors_origins`] lists origins.
//! - **`allowed_roots`.** When set, every path a request names (save
//!   paths, move targets, creation sources) must lie inside a root. The
//!   check is lexical before it touches the filesystem, so a path must be
//!   spelled like the configured root: a different letter case on
//!   case-insensitive filesystems (macOS, Windows), a Windows 8.3 short
//!   name, or a symlink alias outside the roots (`/tmp` for a
//!   `/private/tmp` root) is refused with `403`. Symlinks inside a root
//!   that lead out of it are resolved and refused too.
//! - **Caps.** Bodies are limited by [`ApiConfig::max_body_bytes`]
//!   (`413`), file streams by [`ApiConfig::max_streams`] and event
//!   streams by [`ApiConfig::max_event_streams`] (`503`). A paused client
//!   keeps its stream slot, so `max_streams` is what bounds the slots idle
//!   clients can hold.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic
    )
)]

mod auth;
mod config;
mod dto;
mod error;
mod ids;
mod paths;
pub mod range;
mod routes;
mod server;

use axum::extract::DefaultBodyLimit;
use axum::http::{Method, header};
use fbt::Session;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowOrigin, CorsLayer};

pub use config::{ApiConfig, Auth, MAX_BODY_BYTES};
pub use error::ApiError;
pub use server::ApiServer;

/// The API's axum router: every route under `/api/v1`, with the CORS,
/// auth and body-limit layers applied.
///
/// Mount it in your own server with [`axum::Router::merge`] (or `nest` it
/// under a prefix); the auth rules follow the routes. You promise to serve
/// it on [`ApiConfig::bind`]: with [`Auth::Disabled`] that address must be
/// loopback. Long-lived bodies (events, file streams) end when the client
/// hangs up; [`ApiServer`] also ends them on shutdown.
///
/// # Errors
///
/// [`ApiError::EmptyToken`] for an empty token, [`ApiError::InsecureBind`]
/// for [`Auth::Disabled`] with a bind address that is not loopback,
/// [`ApiError::InvalidOrigin`] for a CORS origin that is not a valid header
/// value, [`ApiError::InvalidRoot`] for an allowed root that does not
/// exist.
pub fn router(session: Session, config: &ApiConfig) -> Result<axum::Router, ApiError> {
    build_router(session, config, CancellationToken::new())
}

/// [`router`], whose long-lived bodies also end when `shutdown` fires.
pub(crate) fn build_router(
    session: Session,
    config: &ApiConfig,
    shutdown: CancellationToken,
) -> Result<axum::Router, ApiError> {
    let validated = config.validate()?;
    let auth = auth::AuthState {
        auth: config.auth.clone(),
        origins: validated.origins.clone().into(),
    };
    let state = routes::AppState::new(session, config.clone(), validated.roots, shutdown);
    let (router, _) = routes::api_router().split_for_parts();
    // `layer` wraps outside what came before: routes, then auth, then CORS.
    let router = router
        .layer(axum::middleware::from_fn_with_state(auth, auth::layer))
        .layer(DefaultBodyLimit::max(config.max_body_bytes))
        .with_state(state);
    if validated.origins.is_empty() {
        return Ok(router);
    }
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::list(validated.origins))
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::HEAD,
        ])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE, header::RANGE])
        .expose_headers([
            header::CONTENT_RANGE,
            header::ACCEPT_RANGES,
            header::CONTENT_LENGTH,
            header::CONTENT_DISPOSITION,
        ]);
    Ok(router.layer(cors))
}

/// The OpenAPI 3.1 document of the API, also served (without a token) at
/// `GET /api/v1/openapi.json`. For generating clients; no Swagger UI is
/// bundled.
pub fn openapi() -> utoipa::openapi::OpenApi {
    routes::api_router().split_for_parts().1
}
