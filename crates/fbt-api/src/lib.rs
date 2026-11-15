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
//! Open event and file streams end when their client hangs up or the
//! session shuts down; [`ApiServer`] additionally ends a connection whose
//! writes have stopped making progress for
//! [`ApiConfig::stream_idle_timeout`] (see there). Shut the session down
//! before awaiting a graceful shutdown of your server, or mount
//! [`router_with_shutdown`], whose streams also end when a future of yours
//! completes.
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
//! [`ApiServer::start_with`] serves routes of your own beside the API, merged
//! ahead of the auth, body-limit and CORS layers so all three apply to them
//! ([`router_with`] does the same for a router you mount yourself, and says what
//! the limit does and does not reach). They must not repeat a path the API
//! serves — those are the ones [`openapi()`] lists. Here an [`Auth::Custom`]
//! authenticator and a login route share one server:
//!
//! ```no_run
//! # async fn demo(session: fbt::Session) -> Result<(), Box<dyn std::error::Error>> {
//! use axum::{Router, routing::post};
//! use fbt_api::{ApiConfig, ApiServer, Auth, AuthRejection, AuthRequest, Authenticator};
//! use std::sync::Arc;
//!
//! /// Lets through the requests carrying `x-session: letmein`, and the login
//! /// route that hands out that token. In an app the table of session tokens is
//! /// what a real authenticator would check.
//! #[derive(Debug)]
//! struct Gate;
//!
//! impl Authenticator for Gate {
//!     fn authorize(&self, request: &AuthRequest<'_>) -> Result<(), AuthRejection> {
//!         match request.headers.get("x-session") {
//!             Some(value) if value == "letmein" => Ok(()),
//!             // Nothing else is exempt — only CORS preflights skip the
//!             // authenticator — so this route must be let through by name.
//!             _ if request.matched_path == Some("/api/v1/auth/login") => Ok(()),
//!             _ => Err(AuthRejection::Unauthorized),
//!         }
//!     }
//! }
//!
//! // `Auth::Custom` is allowed with any bind: the authenticator decides.
//! let mut config = ApiConfig::default();
//! config.auth = Auth::Custom(Arc::new(Gate));
//!
//! // A route of your own, on a path fbt-api does not serve.
//! let mine = Router::new().route("/api/v1/auth/login", post(|| async { "log in" }));
//! let server = ApiServer::start_with(session, config, mine).await?;
//! println!("API on http://{}", server.local_addr());
//! server.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Security model
//!
//! Every client is untrusted until authenticated: request bodies, query
//! strings, path parameters and headers never panic the server, and every
//! body and collection has a size cap. A client the configured [`Auth`]
//! admits — a token holder, or whoever your [`Auth::Custom`] authenticator
//! lets through — is fully trusted: the API is the session's own control
//! surface ([`ApiConfig::allowed_roots`] limits the paths it may name).
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
//! - **Pluggable.** [`Auth::Custom`] hands every request — this document
//!   included, only CORS preflights answered before it — to your own
//!   [`Authenticator`]: it sees the method, matched path, headers, the
//!   `?token=` value on the same routes that take it today, and the peer
//!   address where one is known, and answers with an [`AuthRejection`]
//!   (`401`, `403` or `429`). The call is synchronous on purpose (session
//!   tokens in memory; anything slower belongs in your own login route) and
//!   runs inside `std::panic::catch_unwind`: a panic gets that request a
//!   `500` and the server keeps serving.
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
//!   stops reading keeps its stream slot only until `ApiServer` ends the
//!   stalled connection ([`ApiConfig::stream_idle_timeout`]), so those
//!   caps bound the slots even stalled clients can hold — a router you
//!   mount yourself has no stall guard and relies on hang-ups; `ApiServer`
//!   serves at most [`ApiConfig::max_connections`] connections at once
//!   (further clients wait in the listen backlog).

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

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::http::{Method, header};
use fbt::Session;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowOrigin, CorsLayer};

pub use auth::{AuthRejection, AuthRequest, Authenticator};
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
/// Long-lived bodies (events, file streams) end when the client hangs up
/// or the session shuts down (events within 1 s, file streams as their
/// torrents go). A stalled client — one whose socket stops accepting bytes
/// — is only unhooked by [`ApiServer`]'s stall guard
/// ([`ApiConfig::stream_idle_timeout`](crate::ApiConfig::stream_idle_timeout));
/// a router you mount yourself has none, and relies on the client hanging
/// up. Nothing else ends the bodies, so a graceful shutdown of your server
/// waits for them: shut the session down first, or use
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
    build_router(
        session,
        config,
        CancellationToken::new(),
        axum::Router::new(),
    )
}

/// [`router`], with `extra` — routes of your own, whose handlers carry whatever
/// state they need — merged into the API's ahead of its layers, so every layer
/// applies to them too: the same [`Auth`] decides who may reach them (the bearer
/// token under [`Auth::Token`], the loopback guard under [`Auth::Disabled`], your
/// [`Authenticator`] under [`Auth::Custom`], which sees them with their matched
/// path), they get [`ApiConfig::max_body_bytes`] and the same CORS layer as
/// fbt-api's own routes. Served by [`ApiServer::start_with`] they also hold one
/// of its connection slots and end with its stall guard.
///
/// The `?token=` query token stays fbt-api's own: only its file-stream and event
/// routes accept it, so an extra route authenticates through the header — or
/// whatever your authenticator accepts. Nothing of yours is exempt, either: only
/// CORS preflights are answered before auth, so a login route on `extra` has to
/// be let through by your [`Authenticator`] — possible under [`Auth::Custom`]
/// alone, because [`Auth::Token`] wants the bearer token from every request that
/// is not fbt-api's own document.
///
/// And `max_body_bytes` is axum's `DefaultBodyLimit`, which the body extractors
/// (`Bytes`, `String`, `Json`, `Form`, …) enforce: a handler of yours that takes
/// `Request` or `Body` and consumes frames itself is not limited by it, so cap
/// such a body yourself.
///
/// fbt-api's own exemptions are recognised at the *end* of the matched path (so
/// its router can be nested under a prefix), which an extra route can trip:
/// under [`Auth::Token`] a `GET` whose path ends in `/api/v1/openapi.json` needs
/// no token, and one ending in `/api/v1/events` or
/// `/api/v1/torrents/{id}/files/{index}/stream` takes `?token=`. End your paths
/// differently.
///
/// # Panics
///
/// When `extra` registers a route fbt-api already serves: axum panics when two
/// routers answer the same method on the same path. Everything fbt-api serves is
/// under `/api/v1/`, exactly the paths [`openapi()`] lists — pick other ones
/// (`/api/v1/auth/login` and `/api/v1/fs/tree` are free, `GET /api/v1/session`
/// is not). Do not add a method to a path fbt-api serves either: a different one
/// merges without panicking, but neither the document nor the auth rules would
/// know it, and a `HEAD` there silently takes over what that path answers to
/// `HEAD`. If `extra` has a fallback of its own, that one then answers paths
/// neither router matches.
///
/// # Errors
///
/// Those of [`router`].
pub fn router_with(
    session: Session,
    config: &ApiConfig,
    extra: axum::Router,
) -> Result<axum::Router, ApiError> {
    build_router(session, config, CancellationToken::new(), extra)
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
    let router = build_router(session, config, token.clone(), axum::Router::new())?;
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

/// [`router`], whose long-lived bodies also end when `shutdown` fires, with
/// `extra` merged in ahead of the layers ([`router_with`]).
pub(crate) fn build_router(
    session: Session,
    config: &ApiConfig,
    shutdown: CancellationToken,
    extra: axum::Router,
) -> Result<axum::Router, ApiError> {
    let validated = config.validate()?;
    let auth = auth::AuthState {
        auth: config.auth.clone(),
        origins: validated.origins.clone().into(),
    };
    let state = routes::AppState::new(session, config, validated.roots, shutdown);
    let (router, _) = routes::api_router().split_for_parts();
    // `layer` wraps outside what came before: routes, then auth, then CORS.
    // `extra` is merged before them — after the state is gone, so it needs
    // none of its own — which is what puts every layer, and the matched path
    // the auth layer reads, on its routes too.
    let router = router
        .with_state(state)
        .merge(extra)
        .layer(axum::middleware::from_fn_with_state(auth, auth::layer))
        .layer(DefaultBodyLimit::max(config.max_body_bytes));
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
