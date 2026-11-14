//! The routes, their shared [`AppState`] and the OpenAPI document.
//!
//! # Adding a route
//!
//! 1. Write the handler in the group's module (`session.rs`,
//!    `torrents.rs`, ...) with a `#[utoipa::path(...)]` giving its method,
//!    full path (`/api/v1/...`), `tag`, params and every response
//!    (errors as `body = ErrorBody`). Take inputs only through the crate
//!    extractors ([`ApiJson`](crate::error::ApiJson),
//!    [`ApiQuery`](crate::error::ApiQuery),
//!    [`ApiPath`](crate::error::ApiPath),
//!    [`ApiBytes`](crate::error::ApiBytes)) and return
//!    `Result<_, ApiErrorResponse>`.
//! 2. Register it in the group's `router()` with
//!    `.routes(utoipa_axum::routes!(handler))`: the macro adds the axum
//!    route and its OpenAPI operation together, so a route cannot be
//!    served undocumented. Handlers sharing a path and differing in
//!    method go in one `routes!(a, b)`.
//! 3. A new group module `merge`s its `router()` in [`api_router`].
//!
//! Every route sits behind the auth layer; [`crate::auth`] lists the
//! token-exempt and `?token=` routes.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use fbt::Session;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};
use utoipa::{Modify, OpenApi};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::config::ApiConfig;

pub(crate) mod control;
pub(crate) mod create;
pub(crate) mod events;
pub(crate) mod session;
pub(crate) mod stream;
pub(crate) mod torrents;

/// The state every handler gets (`State<AppState>`); cheap to clone.
#[derive(Clone)]
pub(crate) struct AppState {
    /// The session the API controls.
    pub(crate) session: Session,
    /// [`ApiConfig::allowed_roots`], as given and canonicalised once;
    /// `None` allows any path.
    pub(crate) roots: Option<Arc<[crate::paths::Root]>>,
    /// Permits for open file streams ([`ApiConfig::max_streams`]); a
    /// stream body holds one until it is dropped.
    pub(crate) streams: Arc<Semaphore>,
    /// Permits for open event streams ([`ApiConfig::max_event_streams`]).
    pub(crate) events: Arc<Semaphore>,
    /// Permits for running torrent creations
    /// ([`create::MAX_CONCURRENT_CREATES`]); each blocking build holds one
    /// until it ends, even if the client has gone.
    pub(crate) creates: Arc<Semaphore>,
    /// [`ApiConfig::stream_idle_timeout`]: every long-lived body is wrapped
    /// in a [`Watchdog`](crate::watchdog::Watchdog) that ends it when hyper
    /// stops polling for this long.
    pub(crate) stream_idle_timeout: Duration,
    /// Cancelled by [`ApiServer::shutdown`](crate::ApiServer::shutdown), or
    /// when the future given to
    /// [`router_with_shutdown`](crate::router_with_shutdown) completes.
    /// Every long-lived body (events, file streams) ends when it fires:
    /// wrap it in `.take_until(state.shutdown.clone().cancelled_owned())`.
    pub(crate) shutdown: CancellationToken,
}

impl AppState {
    /// The state for `session` and a validated `config`.
    pub(crate) fn new(
        session: Session,
        config: &ApiConfig,
        roots: Option<Arc<[crate::paths::Root]>>,
        shutdown: CancellationToken,
    ) -> Self {
        AppState {
            streams: Arc::new(Semaphore::new(config.max_streams)),
            events: Arc::new(Semaphore::new(config.max_event_streams)),
            creates: Arc::new(Semaphore::new(create::MAX_CONCURRENT_CREATES)),
            session,
            roots,
            stream_idle_timeout: config.stream_idle_timeout,
            shutdown,
        }
    }
}

/// The name of the bearer security scheme in the document.
pub(crate) const BEARER_AUTH: &str = "bearerAuth";

#[derive(OpenApi)]
#[openapi(
    info(
        title = "fbt API",
        description = "HTTP/JSON control of an fbt BitTorrent session: torrents, session \
settings, server-sent events and byte-range file streaming.\n\n\
Every route needs `Authorization: Bearer <token>`, except this document. The streaming and \
event routes also take `?token=<token>`. Errors are \
`{\"error\": {\"code\": \"<snake_case>\", \"message\": \"<text>\"}}`.",
        license(name = "Apache-2.0", identifier = "Apache-2.0"),
    ),
    modifiers(&BearerAddon),
    security(("bearerAuth" = [])),
    tags(
        (name = "session", description = "The session: status and settings."),
        (name = "torrents", description = "Torrents: add, remove and control them; list, status, files, peers, trackers, pieces."),
        (name = "events", description = "Server-sent session events."),
        (name = "meta", description = "This document."),
    ),
)]
struct ApiDoc;

/// Registers the bearer security scheme (utoipa has no attribute form for
/// it).
struct BearerAddon;

impl Modify for BearerAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        openapi
            .components
            .get_or_insert_with(Default::default)
            .add_security_scheme(
                BEARER_AUTH,
                SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
            );
    }
}

/// Every route with its OpenAPI operation. [`crate::router`] splits it
/// into the axum router; [`crate::openapi`] keeps the document.
pub(crate) fn api_router() -> OpenApiRouter<AppState> {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .merge(session::router())
        .merge(torrents::router())
        .merge(control::router())
        .merge(create::router())
        .merge(events::router())
        .merge(stream::router())
        .routes(routes!(openapi_json))
}

/// The document as JSON, serialised once.
fn document() -> &'static Result<String, String> {
    static DOC: OnceLock<Result<String, String>> = OnceLock::new();
    DOC.get_or_init(|| serde_json::to_string(&crate::openapi()).map_err(|e| e.to_string()))
}

/// The OpenAPI 3.1 document of this API. Needs no token.
#[utoipa::path(
    get,
    path = "/api/v1/openapi.json",
    tag = "meta",
    security(()),
    responses(
        (status = 200, description = "The OpenAPI document.", content_type = "application/json", body = Object),
        (status = 500, description = "The document could not be serialised (`internal`).", body = crate::dto::ErrorBody),
    ),
)]
async fn openapi_json() -> Response {
    match document() {
        Ok(json) => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            json.clone(),
        )
            .into_response(),
        Err(e) => crate::error::ApiErrorResponse::internal(e.clone()).into_response(),
    }
}
