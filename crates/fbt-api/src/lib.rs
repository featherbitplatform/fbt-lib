//! fbt-api: an HTTP/JSON API over an [`fbt::Session`]: control of every
//! torrent and session setting, server-sent events and byte-range file
//! streaming, with an OpenAPI document.
//!
//! Mount [`router`] in your own axum server, or run [`ApiServer`].
//!
//! Every client is untrusted until authenticated: request bodies, query
//! strings, path parameters and headers never panic the server, and every
//! body and collection has a size cap. A token holder is fully trusted:
//! the API is the session's own control surface
//! ([`ApiConfig::allowed_roots`] limits the paths it may name).

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
