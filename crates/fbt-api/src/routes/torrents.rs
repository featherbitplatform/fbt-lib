//! `/api/v1/torrents`: the read endpoints of a torrent.

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderValue, header};
use axum::response::{IntoResponse, Response};
use fbt::{Magnet, SessionError, TorrentHandle};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::dto::{ErrorBody, FileInfo, MagnetInfo, PeerInfo, PiecesInfo, TorrentInfo, TrackerInfo};
use crate::error::{ApiErrorResponse, ApiPath};
use crate::ids::{hex, parse_id};
use crate::routes::AppState;

/// The torrent routes.
pub(crate) fn router() -> OpenApiRouter<AppState> {
    OpenApiRouter::new()
        .routes(routes!(list_torrents))
        .routes(routes!(get_torrent))
        .routes(routes!(get_files))
        .routes(routes!(get_peers))
        .routes(routes!(get_trackers))
        .routes(routes!(get_pieces))
        .routes(routes!(get_resume_data))
        .routes(routes!(get_magnet))
}

/// The torrent named by the hex `id`: `400 invalid_id` for a malformed
/// id, `404 not_found` for an unknown torrent.
pub(crate) fn lookup(state: &AppState, id: &str) -> Result<TorrentHandle, ApiErrorResponse> {
    let hashes = parse_id(id)?;
    state
        .session
        .torrent(&hashes)
        .ok_or_else(|| ApiErrorResponse::not_found("no such torrent"))
}

/// Every torrent, in queue order.
#[utoipa::path(
    get,
    path = "/api/v1/torrents",
    tag = "torrents",
    responses(
        (status = 200, description = "The torrents.", body = Vec<TorrentInfo>),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
    ),
)]
async fn list_torrents(State(state): State<AppState>) -> Json<Vec<TorrentInfo>> {
    Json(
        state
            .session
            .torrents()
            .iter()
            .map(TorrentInfo::of)
            .collect(),
    )
}

/// One torrent's status.
#[utoipa::path(
    get,
    path = "/api/v1/torrents/{id}",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    responses(
        (status = 200, description = "The torrent.", body = TorrentInfo),
        (status = 400, description = "Malformed id (`invalid_id`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
    ),
)]
async fn get_torrent(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
) -> Result<Json<TorrentInfo>, ApiErrorResponse> {
    Ok(Json(TorrentInfo::of(&lookup(&state, &id)?)))
}

/// The torrent's files with priority and progress.
#[utoipa::path(
    get,
    path = "/api/v1/torrents/{id}/files",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    responses(
        (status = 200, description = "The files.", body = Vec<FileInfo>),
        (status = 400, description = "Malformed id (`invalid_id`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
        (status = 409, description = "No metadata yet (`no_metadata`).", body = ErrorBody),
    ),
)]
async fn get_files(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
) -> Result<Json<Vec<FileInfo>>, ApiErrorResponse> {
    let files = lookup(&state, &id)?.files().await?;
    Ok(Json(files.iter().map(FileInfo::from).collect()))
}

/// The torrent's connected peers.
#[utoipa::path(
    get,
    path = "/api/v1/torrents/{id}/peers",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    responses(
        (status = 200, description = "The peers.", body = Vec<PeerInfo>),
        (status = 400, description = "Malformed id (`invalid_id`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
    ),
)]
async fn get_peers(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
) -> Result<Json<Vec<PeerInfo>>, ApiErrorResponse> {
    let peers = lookup(&state, &id)?.peers().await?;
    Ok(Json(peers.iter().map(PeerInfo::from).collect()))
}

/// The torrent's trackers with their last announce state.
#[utoipa::path(
    get,
    path = "/api/v1/torrents/{id}/trackers",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    responses(
        (status = 200, description = "The trackers.", body = Vec<TrackerInfo>),
        (status = 400, description = "Malformed id (`invalid_id`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
    ),
)]
async fn get_trackers(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
) -> Result<Json<Vec<TrackerInfo>>, ApiErrorResponse> {
    let trackers = lookup(&state, &id)?.trackers().await?;
    Ok(Json(trackers.iter().map(TrackerInfo::from).collect()))
}

/// The pieces the torrent has, as a hex bitfield.
#[utoipa::path(
    get,
    path = "/api/v1/torrents/{id}/pieces",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    responses(
        (status = 200, description = "The pieces.", body = PiecesInfo),
        (status = 400, description = "Malformed id (`invalid_id`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
    ),
)]
async fn get_pieces(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
) -> Result<Json<PiecesInfo>, ApiErrorResponse> {
    let pieces = lookup(&state, &id)?.pieces().await?;
    Ok(Json(PiecesInfo {
        num_pieces: pieces.len(),
        have: hex(pieces.as_bytes()),
    }))
}

/// The torrent's resume data (bencoded), for `POST /api/v1/torrents/resume`.
#[utoipa::path(
    get,
    path = "/api/v1/torrents/{id}/resume-data",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    responses(
        (status = 200, description = "The resume data.", content_type = "application/octet-stream", body = Vec<u8>),
        (status = 400, description = "Malformed id (`invalid_id`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
    ),
)]
async fn get_resume_data(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
) -> Result<Response, ApiErrorResponse> {
    let bytes = lookup(&state, &id)?
        .save_resume_data()
        .await
        .map_err(|e| match e {
            // Output side: our own state failed to encode, not bad input.
            SessionError::Resume(e) => ApiErrorResponse::internal(e.to_string()),
            e => e.into(),
        })?;
    Ok((
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        )],
        bytes,
    )
        .into_response())
}

/// A magnet link for the torrent; before its metadata is known, one built
/// from the info hashes alone.
#[utoipa::path(
    get,
    path = "/api/v1/torrents/{id}/magnet",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    responses(
        (status = 200, description = "The magnet link.", body = MagnetInfo),
        (status = 400, description = "Malformed id (`invalid_id`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
    ),
)]
async fn get_magnet(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
) -> Result<Json<MagnetInfo>, ApiErrorResponse> {
    let handle = lookup(&state, &id)?;
    let magnet = match handle.metainfo() {
        Some(m) => m.magnet(),
        None => Magnet::from_info_hashes(handle.info_hashes()),
    };
    Ok(Json(MagnetInfo {
        magnet: magnet.to_uri(),
    }))
}
