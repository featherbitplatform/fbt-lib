//! `/api/v1/session`: the session's status (and, later, its settings).

use axum::Json;
use axum::extract::State;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::dto::{ErrorBody, SessionInfo};
use crate::routes::AppState;

/// The session routes.
pub(crate) fn router() -> OpenApiRouter<AppState> {
    OpenApiRouter::new().routes(routes!(get_session))
}

/// The session at a glance: version, listen addresses, external IP, DHT
/// size, total rates and the number of torrents.
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
