//! fbt-api: an HTTP/JSON API over an [`fbt::Session`]: control of every
//! torrent and session setting, server-sent events and byte-range file
//! streaming, with an OpenAPI document at `/api/v1/openapi.json`.
//!
//! Mount [`router`] (or [`router_with_shutdown`]) in your own axum
//! server, or run [`ApiServer`].
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
//! Open event and file streams end when their client hangs up, when the
//! session shuts down, or when a client goes quiet for
//! [`ApiConfig::stream_idle_timeout`]; shut the session down before awaiting
//! a graceful shutdown of your server, or mount [`router_with_shutdown`],
//! whose streams also end when a future of yours completes.
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
//!   `localhost`, `127.0.0.1` or `[::1]`, whose `Origin` is not in
//!   [`ApiConfig::cors_origins`], or whose peer address is not loopback
//!   where the request carries axum's `ConnectInfo` (which
//!   [`ApiServer`] attaches to every request), get `403`:
//!   the guard against CSRF and DNS rebinding. A router you mount without
//!   that extension keeps the Host and Origin guard alone.
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
//!   streams by [`ApiConfig::max_event_streams`] (`503`). A client that
//!   stops reading keeps its stream slot only until
//!   [`ApiConfig::stream_idle_timeout`] ends the body, so those caps bound
//!   the slots even stalled clients can hold; `ApiServer` serves at most
//!   [`ApiConfig::max_connections`] connections at once (further clients
//!   wait in the listen backlog).

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
mod watchdog;

use std::sync::Arc;

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
/// loopback.
///
/// Long-lived bodies (events, file streams) end when the client hangs up,
/// when a client stops reading for
/// [`ApiConfig::stream_idle_timeout`](crate::ApiConfig::stream_idle_timeout),
/// or when the session shuts down (events within 1 s, file streams as
/// their torrents go). Nothing else ends them, so a graceful shutdown of
/// your server waits for them: shut the session down first, or use
/// [`router_with_shutdown`] and complete its future before (or as) your
/// server's graceful shutdown starts.
///
/// # Errors
///
/// [`ApiError::EmptyToken`] for an empty token, [`ApiError::InsecureBind`]
/// for [`Auth::Disabled`] with a bind address that is not loopback,
/// [`ApiError::InvalidOrigin`] for a CORS origin that is not a valid header
/// value, [`ApiError::InvalidRoot`] for an allowed root that does not
/// exist, [`ApiError::InvalidLimits`] for a `max_connections` that does not
/// exceed the stream caps.
pub fn router(session: Session, config: &ApiConfig) -> Result<axum::Router, ApiError> {
    build_router(session, config, CancellationToken::new())
}

/// [`router`], whose long-lived bodies (events, file streams) also end
/// when `shutdown` completes, so a server mounting it can shut down
/// gracefully while clients keep them open:
///
/// ```no_run
/// # async fn demo(session: fbt::Session) -> Result<(), Box<dyn std::error::Error>> {
/// use fbt_api::{ApiConfig, router_with_shutdown};
///
/// let (stop, stopped) = tokio::sync::watch::channel(false);
/// let mut ends_streams = stopped.clone();
/// let mut ends_server = stopped;
/// let app = router_with_shutdown(session.clone(), &ApiConfig::new("secret-token"), async move {
///     let _ = ends_streams.wait_for(|stop| *stop).await;
/// })?;
/// let listener = tokio::net::TcpListener::bind("127.0.0.1:8090").await?;
/// let server = tokio::spawn(async move {
///     axum::serve(listener, app)
///         .with_graceful_shutdown(async move {
///             let _ = ends_server.wait_for(|stop| *stop).await;
///         })
///         .await
/// });
/// // ... later: end the streams and the server, then the session.
/// stop.send(true)?;
/// server.await??;
/// session.shutdown().await;
/// # Ok(())
/// # }
/// ```
///
/// Without that future completing (or the session shutting down), open
/// event and file streams keep a graceful shutdown waiting, as with
/// [`router`]. The future runs on a task of its own until it completes
/// or every clone of the router is dropped.
///
/// # Errors
///
/// Those of [`router`], plus [`ApiError::NoRuntime`] when called outside a
/// Tokio runtime: the task needs one. (It used to panic there.)
pub fn router_with_shutdown(
    session: Session,
    config: &ApiConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<axum::Router, ApiError> {
    let token = CancellationToken::new();
    let router = build_router(session, config, token.clone())?;
    // Cancelled when the last router clone goes, so the task does not
    // outlive the router when `shutdown` never completes.
    let dropped = CancellationToken::new();
    let guard = Arc::new(dropped.clone().drop_guard());
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return Err(ApiError::NoRuntime);
    };
    runtime.spawn(async move {
        tokio::select! {
            () = shutdown => token.cancel(),
            () = dropped.cancelled() => {}
        }
    });
    Ok(router.layer(axum::Extension(guard)))
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
    let state = routes::AppState::new(session, config, validated.roots, shutdown);
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
