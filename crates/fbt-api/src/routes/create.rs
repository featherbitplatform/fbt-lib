//! `POST /api/v1/create-torrent`: hashes a file or directory into a
//! `.torrent`.

use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use fbt::CreateTorrent;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::dto::{CreateTorrentRequest, ErrorBody};
use crate::error::{ApiErrorResponse, ApiJson};
use crate::paths::check_path;
use crate::routes::AppState;

/// Torrent creations running at once; beyond that, `503 busy`.
pub(crate) const MAX_CONCURRENT_CREATES: usize = 2;
/// Most tracker URLs (all tiers together, an empty tier counting as one)
/// and web seeds in one request.
pub(crate) const MAX_CREATE_URLS: usize = 256;
/// Longest comment, in bytes.
const MAX_COMMENT_BYTES: usize = 64 * 1024;

/// The create route.
pub(crate) fn router() -> OpenApiRouter<AppState> {
    OpenApiRouter::new().routes(routes!(create_torrent))
}

/// Sets a flag when dropped: a client that goes away stops the build at
/// its next piece.
struct CancelOnDrop(Arc<AtomicBool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Hashes the file or directory at `path` and returns the `.torrent`.
///
/// Runs on the blocking pool, two at a time; a third request gets
/// `503 busy`. Hashing reads every byte, so a large source takes a while.
#[utoipa::path(
    post,
    path = "/api/v1/create-torrent",
    tag = "torrents",
    request_body = CreateTorrentRequest,
    responses(
        (status = 200, description = "The bencoded `.torrent`.", content_type = "application/x-bittorrent", body = Vec<u8>),
        (status = 400, description = "Malformed body, empty source or bad piece length (`invalid_request`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 403, description = "The source is outside `allowed_roots` (`path_not_allowed`).", body = ErrorBody),
        (status = 404, description = "The source does not exist (`not_found`).", body = ErrorBody),
        (status = 409, description = "A file changed while it was hashed (`busy`).", body = ErrorBody),
        (status = 500, description = "Reading the source failed (`internal`).", body = ErrorBody),
        (status = 503, description = "Two creations already run (`busy`), or the session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn create_torrent(
    State(state): State<AppState>,
    ApiJson(body): ApiJson<CreateTorrentRequest>,
) -> Result<Response, ApiErrorResponse> {
    let trackers: Option<Vec<Vec<String>>> = body
        .trackers
        .map(|tiers| tiers.0.into_iter().map(|tier| tier.0).collect());
    let web_seeds = body.web_seeds.map(|w| w.0);
    // An empty tier counts as a URL, so `[[],[],...]` is capped too.
    let urls = trackers
        .iter()
        .flatten()
        .map(|tier| tier.len().max(1))
        .chain(web_seeds.iter().map(Vec::len))
        .fold(0usize, usize::saturating_add);
    if urls > MAX_CREATE_URLS {
        return Err(ApiErrorResponse::invalid_request(format!(
            "at most {MAX_CREATE_URLS} tracker URLs (an empty tier counting as one) and web seeds are accepted"
        )));
    }
    if body
        .comment
        .as_ref()
        .is_some_and(|c| c.len() > MAX_COMMENT_BYTES)
    {
        return Err(ApiErrorResponse::invalid_request(format!(
            "the comment is longer than {MAX_COMMENT_BYTES} bytes"
        )));
    }
    let permit = Arc::clone(&state.creates)
        .try_acquire_owned()
        .map_err(|_| {
            ApiErrorResponse::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "busy",
                "too many torrents are being created, retry later",
            )
        })?;
    let source = check_path(&state, &PathBuf::from(&body.path)).await?;

    let mut creator = CreateTorrent::new(source).version(body.version.into());
    if let Some(len) = body.piece_length {
        creator = creator.piece_len(len);
    }
    if let Some(trackers) = trackers {
        creator = creator.trackers(trackers);
    }
    if let Some(web_seeds) = web_seeds {
        creator = creator.web_seeds(web_seeds);
    }
    if let Some(comment) = body.comment {
        creator = creator.comment(comment);
    }
    creator = creator.private(body.private);

    let cancel = Arc::new(AtomicBool::new(false));
    let guard = CancelOnDrop(Arc::clone(&cancel));
    let shutdown = state.shutdown.clone();
    let built = tokio::task::spawn_blocking(move || {
        // The permit lives as long as the build, whether or not the
        // client is still there.
        let _permit = permit;
        creator.build(&mut |_, _| {
            if cancel.load(Ordering::Relaxed) || shutdown.is_cancelled() {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
    })
    .await
    .map_err(|e| ApiErrorResponse::internal(format!("torrent creation failed: {e}")))?;
    drop(guard);
    let bytes = built?;
    Ok((
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/x-bittorrent"),
        )],
        bytes,
    )
        .into_response())
}
