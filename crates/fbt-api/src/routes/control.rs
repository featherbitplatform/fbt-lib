//! `/api/v1/torrents`: adding, removing and controlling torrents.

use axum::Json;
use axum::extract::{FromRequest, FromRequestParts, Request, State};
use axum::http::{StatusCode, header};
use fbt::{AddTorrent, Magnet, Metainfo, RateLimit, RemoveMode};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::dto::{
    AddOptions, AddPeerRequest, AddTorrentRequest, AutoManagedRequest, ErrorBody,
    FilePrioritiesRequest, LimitsRequest, MoveRequest, QueuePositionRequest, RemoveOptions,
    RenameFileRequest, ResumeOptions, TorrentInfo, WebSeedQuery, WebSeedRequest,
};
use crate::error::{ApiBytes, ApiErrorResponse, ApiJson, ApiPath, ApiQuery};
use crate::paths::check_path;
use crate::routes::AppState;
use crate::routes::torrents::lookup;

/// Most priorities one `PUT .../file-priorities` takes: the bencode item
/// cap of a `.torrent` (`MAX_TORRENT_ITEMS` in `fbt-metainfo`), which no
/// torrent's file count reaches. More get `400 invalid_request`.
pub(crate) const MAX_FILE_PRIORITIES: usize = 4_194_304;

/// The control routes. `POST /torrents` and `DELETE /torrents/{id}` share
/// their paths with read routes; axum merges the methods.
pub(crate) fn router() -> OpenApiRouter<AppState> {
    OpenApiRouter::new()
        .routes(routes!(add_torrent))
        .routes(routes!(add_resume))
        .routes(routes!(remove_torrent))
        .routes(routes!(pause))
        .routes(routes!(resume))
        .routes(routes!(recheck))
        .routes(routes!(reannounce))
        .routes(routes!(scrape))
        .routes(routes!(set_file_priorities))
        .routes(routes!(set_limits))
        .routes(routes!(set_queue_position))
        .routes(routes!(set_auto_managed))
        .routes(routes!(move_storage))
        .routes(routes!(rename_file))
        .routes(routes!(add_web_seed, remove_web_seed))
        .routes(routes!(add_peer))
}

/// The `.torrent` media type.
const BITTORRENT: &str = "application/x-bittorrent";

/// `201 Created` with the new torrent.
type Created = (StatusCode, Json<TorrentInfo>);

/// Adds `params` after the save-path check.
async fn add(state: &AppState, mut params: AddTorrent) -> Result<Created, ApiErrorResponse> {
    params.save_path = check_path(state, &params.save_path).await?;
    let handle = state.session.add_torrent(params).await?;
    Ok((StatusCode::CREATED, Json(TorrentInfo::of(&handle))))
}

/// Applies the optional add flags.
fn flags(
    params: &mut AddTorrent,
    paused: Option<bool>,
    auto_managed: Option<bool>,
    sequential: Option<bool>,
) {
    if let Some(v) = paused {
        params.paused = v;
    }
    if let Some(v) = auto_managed {
        params.auto_managed = v;
    }
    if let Some(v) = sequential {
        params.sequential = v;
    }
}

/// Adds a torrent: a magnet link as JSON, or a `.torrent` file as an
/// `application/x-bittorrent` body with the options in the query string.
/// File priorities are set afterwards with `PUT .../file-priorities`.
#[utoipa::path(
    post,
    path = "/api/v1/torrents",
    tag = "torrents",
    params(AddOptions),
    request_body(
        description = "A magnet as JSON, or the raw `.torrent` bytes (then the query string carries the options).",
        content(
            (AddTorrentRequest = "application/json"),
            (Vec<u8> = "application/x-bittorrent"),
        ),
    ),
    responses(
        (status = 201, description = "The torrent was added.", body = TorrentInfo),
        (status = 400, description = "Malformed body, magnet, `.torrent` or query (`invalid_request`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 403, description = "Save path outside the allowed roots (`path_not_allowed`).", body = ErrorBody),
        (status = 409, description = "Already in the session (`duplicate_torrent`).", body = ErrorBody),
        (status = 413, description = "Body over the limit (`body_too_large`).", body = ErrorBody),
        (status = 500, description = "An internal failure, such as a storage error (`internal`).", body = ErrorBody),
        (status = 503, description = "The session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn add_torrent(
    State(state): State<AppState>,
    req: Request,
) -> Result<Created, ApiErrorResponse> {
    let is_torrent = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .is_some_and(|v| v.trim().eq_ignore_ascii_case(BITTORRENT));
    if is_torrent {
        let (mut parts, body) = req.into_parts();
        let ApiQuery(opts) = ApiQuery::<AddOptions>::from_request_parts(&mut parts, &()).await?;
        let ApiBytes(bytes) = ApiBytes::from_request(Request::from_parts(parts, body), &()).await?;
        let metainfo = Metainfo::from_bytes(&bytes)
            .map_err(|e| ApiErrorResponse::invalid_request(format!("invalid .torrent: {e}")))?;
        let mut params = AddTorrent::new(metainfo, opts.save_path);
        flags(&mut params, opts.paused, opts.auto_managed, opts.sequential);
        add(&state, params).await
    } else {
        // The JSON body carries every option: a query string is unknown
        // input, as it is for the `.torrent` form's `AddOptions`.
        if req.uri().query().is_some_and(|q| !q.is_empty()) {
            return Err(ApiErrorResponse::invalid_request(
                "a JSON add takes its options in the body, not the query string",
            ));
        }
        let ApiJson(body) = ApiJson::<AddTorrentRequest>::from_request(req, &()).await?;
        let magnet = body
            .magnet
            .parse::<Magnet>()
            .map_err(|e| ApiErrorResponse::invalid_request(format!("invalid magnet: {e}")))?;
        let mut params = AddTorrent::magnet(magnet, body.save_path);
        flags(&mut params, body.paused, body.auto_managed, body.sequential);
        add(&state, params).await
    }
}

/// Adds a torrent from its resume data (`GET .../resume-data`). The save
/// path stored in it, or `?save_path=` when given, must be inside an
/// allowed root when roots are configured.
#[utoipa::path(
    post,
    path = "/api/v1/torrents/resume",
    tag = "torrents",
    params(ResumeOptions),
    request_body(content = Vec<u8>, content_type = "application/octet-stream", description = "The resume data."),
    responses(
        (status = 201, description = "The torrent was added.", body = TorrentInfo),
        (status = 400, description = "Malformed resume data or query (`invalid_request`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 403, description = "Save path outside the allowed roots (`path_not_allowed`).", body = ErrorBody),
        (status = 409, description = "Already in the session (`duplicate_torrent`).", body = ErrorBody),
        (status = 413, description = "Body over the limit (`body_too_large`).", body = ErrorBody),
        (status = 500, description = "An internal failure, such as a storage error (`internal`).", body = ErrorBody),
        (status = 503, description = "The session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn add_resume(
    State(state): State<AppState>,
    ApiQuery(opts): ApiQuery<ResumeOptions>,
    ApiBytes(bytes): ApiBytes,
) -> Result<Created, ApiErrorResponse> {
    let mut params = AddTorrent::from_resume(&bytes)
        .map_err(|e| ApiErrorResponse::invalid_request(format!("invalid resume data: {e}")))?;
    if let Some(path) = opts.save_path {
        params.save_path = path;
    }
    add(&state, params).await
}

/// Removes a torrent, deleting its files with `?delete_files=true`.
#[utoipa::path(
    delete,
    path = "/api/v1/torrents/{id}",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash."), RemoveOptions),
    responses(
        (status = 204, description = "The torrent was removed."),
        (status = 400, description = "Malformed id or query (`invalid_id`, `invalid_request`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
        (status = 500, description = "An internal failure, such as a storage error (`internal`).", body = ErrorBody),
        (status = 503, description = "The session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn remove_torrent(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
    ApiQuery(opts): ApiQuery<RemoveOptions>,
) -> Result<StatusCode, ApiErrorResponse> {
    let mode = if opts.delete_files.unwrap_or(false) {
        RemoveMode::DeleteData
    } else {
        RemoveMode::KeepData
    };
    lookup(&state, &id)?.remove(mode).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Pauses a torrent: disconnects its peers and stops announcing.
#[utoipa::path(
    post,
    path = "/api/v1/torrents/{id}/pause",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    responses(
        (status = 204, description = "Paused."),
        (status = 400, description = "Malformed id (`invalid_id`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
        (status = 500, description = "An internal failure, such as a storage error (`internal`).", body = ErrorBody),
        (status = 503, description = "The session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn pause(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
) -> Result<StatusCode, ApiErrorResponse> {
    lookup(&state, &id)?.pause().await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Resumes a paused torrent (an auto-managed one may stay queued); also
/// clears a storage error.
#[utoipa::path(
    post,
    path = "/api/v1/torrents/{id}/resume",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    responses(
        (status = 204, description = "Resumed."),
        (status = 400, description = "Malformed id (`invalid_id`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
        (status = 500, description = "An internal failure, such as a storage error (`internal`).", body = ErrorBody),
        (status = 503, description = "The session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn resume(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
) -> Result<StatusCode, ApiErrorResponse> {
    lookup(&state, &id)?.resume().await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Checks the torrent's data again.
#[utoipa::path(
    post,
    path = "/api/v1/torrents/{id}/recheck",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    responses(
        (status = 204, description = "The check started."),
        (status = 400, description = "Malformed id (`invalid_id`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
        (status = 409, description = "No metadata yet (`no_metadata`).", body = ErrorBody),
        (status = 500, description = "An internal failure, such as a storage error (`internal`).", body = ErrorBody),
        (status = 503, description = "The session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn recheck(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
) -> Result<StatusCode, ApiErrorResponse> {
    lookup(&state, &id)?.force_recheck().await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Announces to the trackers now.
#[utoipa::path(
    post,
    path = "/api/v1/torrents/{id}/reannounce",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    responses(
        (status = 204, description = "The announce was asked for."),
        (status = 400, description = "Malformed id (`invalid_id`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
        (status = 500, description = "An internal failure, such as a storage error (`internal`).", body = ErrorBody),
        (status = 503, description = "The session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn reannounce(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
) -> Result<StatusCode, ApiErrorResponse> {
    lookup(&state, &id)?.force_reannounce().await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Scrapes the torrent's trackers; the counts arrive as events.
#[utoipa::path(
    post,
    path = "/api/v1/torrents/{id}/scrape",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    responses(
        (status = 204, description = "The scrape was asked for."),
        (status = 400, description = "Malformed id (`invalid_id`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
        (status = 500, description = "An internal failure, such as a storage error (`internal`).", body = ErrorBody),
        (status = 503, description = "The session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn scrape(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
) -> Result<StatusCode, ApiErrorResponse> {
    lookup(&state, &id)?.scrape_trackers().await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Sets one priority per file (pad files included).
#[utoipa::path(
    put,
    path = "/api/v1/torrents/{id}/file-priorities",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    request_body = FilePrioritiesRequest,
    responses(
        (status = 204, description = "The priorities apply."),
        (status = 400, description = "Malformed id or body, or more than 4 194 304 priorities (`invalid_id`, `invalid_request`), or not one priority per file (`invalid_priorities`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
        (status = 409, description = "No metadata yet (`no_metadata`).", body = ErrorBody),
        (status = 500, description = "An internal failure, such as a storage error (`internal`).", body = ErrorBody),
        (status = 503, description = "The session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn set_file_priorities(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
    ApiJson(body): ApiJson<FilePrioritiesRequest>,
) -> Result<StatusCode, ApiErrorResponse> {
    let handle = lookup(&state, &id)?;
    let priorities = body.priorities.0.into_iter().map(Into::into).collect();
    handle.set_file_priorities(priorities).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Sets the torrent's own rate limits.
#[utoipa::path(
    put,
    path = "/api/v1/torrents/{id}/limits",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    request_body = LimitsRequest,
    responses(
        (status = 204, description = "The limits apply."),
        (status = 400, description = "Malformed id or body (`invalid_id`, `invalid_request`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
        (status = 500, description = "An internal failure, such as a storage error (`internal`).", body = ErrorBody),
        (status = 503, description = "The session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn set_limits(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
    ApiJson(body): ApiJson<LimitsRequest>,
) -> Result<StatusCode, ApiErrorResponse> {
    let limit = RateLimit::new(body.download.unwrap_or(0), body.upload.unwrap_or(0));
    lookup(&state, &id)?.set_rate_limit(limit).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Moves the torrent in the session's queue.
#[utoipa::path(
    put,
    path = "/api/v1/torrents/{id}/queue-position",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    request_body = QueuePositionRequest,
    responses(
        (status = 204, description = "The torrent is at its new place."),
        (status = 400, description = "Malformed id or body (`invalid_id`, `invalid_request`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
        (status = 500, description = "An internal failure, such as a storage error (`internal`).", body = ErrorBody),
        (status = 503, description = "The session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn set_queue_position(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
    ApiJson(body): ApiJson<QueuePositionRequest>,
) -> Result<StatusCode, ApiErrorResponse> {
    lookup(&state, &id)?
        .set_queue_position(body.position)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Puts the torrent under the session's queue or takes it out.
#[utoipa::path(
    put,
    path = "/api/v1/torrents/{id}/auto-managed",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    request_body = AutoManagedRequest,
    responses(
        (status = 204, description = "The queue applied the change."),
        (status = 400, description = "Malformed id or body (`invalid_id`, `invalid_request`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
        (status = 500, description = "An internal failure, such as a storage error (`internal`).", body = ErrorBody),
        (status = 503, description = "The session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn set_auto_managed(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
    ApiJson(body): ApiJson<AutoManagedRequest>,
) -> Result<StatusCode, ApiErrorResponse> {
    lookup(&state, &id)?
        .set_auto_managed(body.auto_managed)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Moves the torrent's files to a new save path.
#[utoipa::path(
    post,
    path = "/api/v1/torrents/{id}/move",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    request_body = MoveRequest,
    responses(
        (status = 204, description = "The files were moved."),
        (status = 400, description = "Malformed id or body (`invalid_id`, `invalid_request`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 403, description = "Target outside the allowed roots (`path_not_allowed`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
        (status = 409, description = "A target file exists (`already_exists`) or too many waiting commands (`busy`).", body = ErrorBody),
        (status = 500, description = "An internal failure, such as a storage error (`internal`).", body = ErrorBody),
        (status = 503, description = "The session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn move_storage(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
    ApiJson(body): ApiJson<MoveRequest>,
) -> Result<StatusCode, ApiErrorResponse> {
    let handle = lookup(&state, &id)?;
    let path = check_path(&state, &body.path).await?;
    handle.move_storage(path, body.mode.into()).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Renames one file, relative to the save path.
#[utoipa::path(
    post,
    path = "/api/v1/torrents/{id}/rename-file",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    request_body = RenameFileRequest,
    responses(
        (status = 204, description = "The file was renamed."),
        (status = 400, description = "Malformed id or body (`invalid_id`, `invalid_request`), or a path that is not plain and relative or collides (`invalid_path`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent or file (`not_found`).", body = ErrorBody),
        (status = 409, description = "No metadata yet (`no_metadata`), the torrent is checking (`busy`) or the target exists (`already_exists`).", body = ErrorBody),
        (status = 500, description = "An internal failure, such as a storage error (`internal`).", body = ErrorBody),
        (status = 503, description = "The session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn rename_file(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
    ApiJson(body): ApiJson<RenameFileRequest>,
) -> Result<StatusCode, ApiErrorResponse> {
    lookup(&state, &id)?
        .rename_file(body.index, &body.path)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Adds a web seed (BEP 19).
#[utoipa::path(
    post,
    path = "/api/v1/torrents/{id}/web-seeds",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    request_body = WebSeedRequest,
    responses(
        (status = 204, description = "The web seed was added."),
        (status = 400, description = "Malformed id, body or URL (`invalid_id`, `invalid_request`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
        (status = 409, description = "The torrent has 16 web seeds (`busy`).", body = ErrorBody),
        (status = 500, description = "An internal failure, such as a storage error (`internal`).", body = ErrorBody),
        (status = 503, description = "The session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn add_web_seed(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
    ApiJson(body): ApiJson<WebSeedRequest>,
) -> Result<StatusCode, ApiErrorResponse> {
    lookup(&state, &id)?.add_web_seed(body.url).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Removes a web seed; nothing if it is not there.
#[utoipa::path(
    delete,
    path = "/api/v1/torrents/{id}/web-seeds",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash."), WebSeedQuery),
    responses(
        (status = 204, description = "The web seed is gone."),
        (status = 400, description = "Malformed id or query (`invalid_id`, `invalid_request`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
        (status = 500, description = "An internal failure, such as a storage error (`internal`).", body = ErrorBody),
        (status = 503, description = "The session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn remove_web_seed(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
    ApiQuery(query): ApiQuery<WebSeedQuery>,
) -> Result<StatusCode, ApiErrorResponse> {
    lookup(&state, &id)?.remove_web_seed(&query.url).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Connects to a peer.
#[utoipa::path(
    post,
    path = "/api/v1/torrents/{id}/peers",
    tag = "torrents",
    params(("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash.")),
    request_body = AddPeerRequest,
    responses(
        (status = 204, description = "The connection was asked for."),
        (status = 400, description = "Malformed id, body or address (`invalid_id`, `invalid_request`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent (`not_found`).", body = ErrorBody),
        (status = 500, description = "An internal failure, such as a storage error (`internal`).", body = ErrorBody),
        (status = 503, description = "The session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn add_peer(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
    ApiJson(body): ApiJson<AddPeerRequest>,
) -> Result<StatusCode, ApiErrorResponse> {
    lookup(&state, &id)?.add_peer(body.addr).await?;
    Ok(StatusCode::NO_CONTENT)
}
