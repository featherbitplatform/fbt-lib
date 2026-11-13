//! `/api/v1/session`: the session's status and settings: limits, queue,
//! IP filter, banned peers and saved state.

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use fbt::RateLimit;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::dto::{ErrorBody, IpFilterDto, LimitsRequest, QueueSettingsDto, SessionInfo};
use crate::error::{ApiErrorResponse, ApiJson};
use crate::routes::AppState;

/// The session routes.
pub(crate) fn router() -> OpenApiRouter<AppState> {
    OpenApiRouter::new()
        .routes(routes!(get_session))
        .routes(routes!(set_limits))
        .routes(routes!(get_queue, set_queue))
        .routes(routes!(get_ip_filter, set_ip_filter))
        .routes(routes!(banned_peers))
        .routes(routes!(session_state))
}

/// The session at a glance: version, listen addresses, external IP, DHT
/// size, total rates, limits and the number of torrents.
#[utoipa::path(
    get,
    path = "/api/v1/session",
    tag = "session",
    responses(
        (status = 200, description = "The session.", body = SessionInfo),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
    ),
)]
async fn get_session(State(state): State<AppState>) -> Json<SessionInfo> {
    Json(SessionInfo::from(&state.session))
}

/// Sets the session-wide rate limits.
#[utoipa::path(
    put,
    path = "/api/v1/session/limits",
    tag = "session",
    request_body = LimitsRequest,
    responses(
        (status = 204, description = "The limits apply."),
        (status = 400, description = "Malformed body (`invalid_request`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
    ),
)]
async fn set_limits(
    State(state): State<AppState>,
    ApiJson(body): ApiJson<LimitsRequest>,
) -> StatusCode {
    let limit = RateLimit::new(body.download.unwrap_or(0), body.upload.unwrap_or(0));
    state.session.set_rate_limit(limit);
    StatusCode::NO_CONTENT
}

/// The queue's limits.
#[utoipa::path(
    get,
    path = "/api/v1/session/queue",
    tag = "session",
    responses(
        (status = 200, description = "The queue settings.", body = QueueSettingsDto),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
    ),
)]
async fn get_queue(State(state): State<AppState>) -> Json<QueueSettingsDto> {
    Json(QueueSettingsDto::from(&state.session.queue_settings()))
}

/// Replaces the queue's limits and runs a scheduler pass.
#[utoipa::path(
    put,
    path = "/api/v1/session/queue",
    tag = "session",
    request_body = QueueSettingsDto,
    responses(
        (status = 204, description = "The settings apply."),
        (status = 400, description = "Malformed body or a zero interval (`invalid_request`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
    ),
)]
async fn set_queue(
    State(state): State<AppState>,
    ApiJson(body): ApiJson<QueueSettingsDto>,
) -> Result<StatusCode, ApiErrorResponse> {
    let queue = body
        .into_settings()
        .map_err(ApiErrorResponse::invalid_request)?;
    state.session.set_queue_settings(queue)?;
    Ok(StatusCode::NO_CONTENT)
}

/// The IP filter, as disjoint ranges.
#[utoipa::path(
    get,
    path = "/api/v1/session/ip-filter",
    tag = "session",
    responses(
        (status = 200, description = "The filter's rules.", body = IpFilterDto),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
    ),
)]
async fn get_ip_filter(
    State(state): State<AppState>,
) -> Result<Json<IpFilterDto>, ApiErrorResponse> {
    // A full filter is large to copy and list: off the async threads.
    let session = state.session.clone();
    let dto = tokio::task::spawn_blocking(move || IpFilterDto::from(&session.ip_filter()))
        .await
        .map_err(|e| ApiErrorResponse::internal(format!("reading the filter failed: {e}")))?;
    Ok(Json(dto))
}

/// Replaces the IP filter; peers it blocks are disconnected.
#[utoipa::path(
    put,
    path = "/api/v1/session/ip-filter",
    tag = "session",
    request_body = IpFilterDto,
    responses(
        (status = 204, description = "The filter applies."),
        (status = 400, description = "Malformed body, a bad rule or more than 65 536 rules (`invalid_request`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
    ),
)]
async fn set_ip_filter(
    State(state): State<AppState>,
    ApiJson(body): ApiJson<IpFilterDto>,
) -> Result<StatusCode, ApiErrorResponse> {
    let filter = body
        .into_filter()
        .map_err(ApiErrorResponse::invalid_request)?;
    state.session.set_ip_filter(filter).await;
    Ok(StatusCode::NO_CONTENT)
}

/// The peers banned for the rest of the session, oldest first. An address
/// with port 0 bans the whole IP.
#[utoipa::path(
    get,
    path = "/api/v1/session/banned-peers",
    tag = "session",
    responses(
        (status = 200, description = "Banned peer addresses.", body = Vec<String>),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
    ),
)]
async fn banned_peers(State(state): State<AppState>) -> Json<Vec<String>> {
    Json(
        state
            .session
            .banned_peers()
            .iter()
            .map(ToString::to_string)
            .collect(),
    )
}

/// The session state (torrent list in queue order, with resume data), to
/// restore with `SessionState::from_bytes`.
#[utoipa::path(
    get,
    path = "/api/v1/session/state",
    tag = "session",
    responses(
        (status = 200, description = "The bencoded session state.", content_type = "application/octet-stream", body = Vec<u8>),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 503, description = "The session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn session_state(State(state): State<AppState>) -> Result<Response, ApiErrorResponse> {
    let (saved, _failed) = state.session.save_state().await?;
    Ok((
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        )],
        saved.to_bytes(),
    )
        .into_response())
}
