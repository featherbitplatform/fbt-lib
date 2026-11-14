//! `/api/v1/torrents/{id}/files/{index}/stream`: a torrent's file over
//! HTTP, with byte ranges, while it downloads.
//!
//! - No `Range`, or one that is malformed, multi-range or not in `bytes`:
//!   `200` with the whole file. A request with `If-Range` also gets the
//!   whole file: no validators are sent, so it can never match (RFC 9110
//!   13.1.5).
//! - One satisfiable range: `206` with `Content-Range`.
//! - An unsatisfiable range: `416` with `Content-Range: bytes */<len>`.
//!
//! The body reads a [`FileReader`](fbt::FileReader) in 64 KiB chunks: a
//! read of a piece the torrent does not have waits for it, and the
//! reader's position drives the torrent's piece deadlines. Each request opens its own
//! reader. The body holds one of [`AppState::streams`]' permits and the
//! reader until it is dropped (the client hung up, or the response is
//! done); it ends early when the torrent is removed, when
//! [`ApiServer::shutdown`](crate::ApiServer::shutdown) runs, and — under
//! [`ApiServer`] only — when the client stalls: bytes owed but unwritten
//! for [`stream_idle_timeout`](crate::ApiConfig::stream_idle_timeout) end
//! the whole connection, so the short read gives its permit back within
//! that time. A body merely waiting for a piece has nothing owed and is
//! left alone however starved it is.
//!
//! `HEAD` gets the same status and headers from the metainfo, with no
//! reader and no permit.

use std::fmt::Write as _;
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use fbt::{FileEntry, TorrentHandle};
use futures_util::{Stream, StreamExt};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::io::ReaderStream;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::dto::ErrorBody;
use crate::error::{ApiErrorResponse, ApiPath};
use crate::range::{RangeOutcome, parse_range};
use crate::routes::AppState;
use crate::routes::torrents::lookup;

/// Bytes read from the file per body chunk.
const CHUNK: usize = 64 * 1024;

/// The streaming routes.
pub(crate) fn router() -> OpenApiRouter<AppState> {
    OpenApiRouter::new().routes(routes!(stream_file, stream_file_head))
}

/// What a request will be answered with, decided from the metainfo alone.
struct Plan {
    handle: TorrentHandle,
    index: usize,
    /// First byte served.
    start: u64,
    /// Bytes served.
    count: u64,
    status: StatusCode,
    headers: HeaderMap,
}

/// The torrent, file and range of a request: a [`Plan`], or the error
/// response (`416` included) to send instead.
fn plan(
    state: &AppState,
    id: &str,
    index: usize,
    request: &HeaderMap,
) -> Result<Result<Plan, Response>, ApiErrorResponse> {
    let handle = lookup(state, id)?;
    let meta = handle
        .metainfo()
        .ok_or_else(|| ApiErrorResponse::from(fbt::SessionError::NoMetadata))?;
    let file = meta
        .info
        .files
        .get(index)
        .filter(|f| !f.pad)
        .ok_or_else(|| ApiErrorResponse::not_found("no such file"))?;
    let len = file.len;
    let mut headers = HeaderMap::new();
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(header::CONTENT_TYPE, content_type(file));
    headers.insert(header::CONTENT_DISPOSITION, content_disposition(file));
    // The body is torrent content: never let a browser sniff it into an
    // active type.
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );

    let mut ranges = request.get_all(header::RANGE).iter();
    let outcome = match (ranges.next(), ranges.next()) {
        (Some(range), None) if !request.contains_key(header::IF_RANGE) => {
            parse_range(range.as_bytes(), len)
        }
        _ => RangeOutcome::Full,
    };
    let (status, start, count) = match outcome {
        RangeOutcome::Partial {
            start,
            end_inclusive,
        } => {
            headers.insert(
                header::CONTENT_RANGE,
                header_value(&format!("bytes {start}-{end_inclusive}/{len}")),
            );
            let count = end_inclusive.saturating_sub(start).saturating_add(1);
            (StatusCode::PARTIAL_CONTENT, start, count)
        }
        RangeOutcome::Unsatisfiable => {
            let mut res = ApiErrorResponse::new(
                StatusCode::RANGE_NOT_SATISFIABLE,
                "range_not_satisfiable",
                format!("the range starts past the end of the {len}-byte file"),
            )
            .into_response();
            res.headers_mut().insert(
                header::CONTENT_RANGE,
                header_value(&format!("bytes */{len}")),
            );
            res.headers_mut()
                .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
            return Ok(Err(res));
        }
        // `Full`, and variants added later.
        _ => (StatusCode::OK, 0, len),
    };
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from(count));
    Ok(Ok(Plan {
        handle,
        index,
        start,
        count,
        status,
        headers,
    }))
}

/// A response with `status`, `headers` and `body`.
fn respond(status: StatusCode, headers: HeaderMap, body: Body) -> Response {
    let mut res = Response::new(body);
    *res.status_mut() = status;
    *res.headers_mut() = headers;
    res
}

/// One file of a torrent, whole or one byte range, read while the torrent
/// downloads. Browser media elements cannot send headers, so this route
/// also takes `?token=`.
#[utoipa::path(
    get,
    path = "/api/v1/torrents/{id}/files/{index}/stream",
    tag = "torrents",
    params(
        ("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash."),
        ("index" = usize, Path, description = "The file's index in the torrent."),
        ("Range" = Option<String>, Header, description = "One range in `bytes` (`a-b`, `a-` or `-n`); anything else gets the whole file."),
        ("If-Range" = Option<String>, Header, description = "Never matches: the whole file is sent."),
        ("token" = Option<String>, Query, description = "The token, for clients that cannot send `Authorization`."),
    ),
    responses(
        (status = 200, description = "The whole file.", content_type = "application/octet-stream"),
        (status = 206, description = "The range asked for, with `Content-Range`.", content_type = "application/octet-stream"),
        (status = 400, description = "Malformed id or index (`invalid_id`, `invalid_request`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 404, description = "Unknown torrent or file (`not_found`).", body = ErrorBody),
        (status = 409, description = "No metadata yet (`no_metadata`).", body = ErrorBody),
        (status = 416, description = "The range starts past the end (`range_not_satisfiable`), with `Content-Range: bytes */<len>`.", body = ErrorBody),
        (status = 500, description = "An internal failure, such as a storage error (`internal`).", body = ErrorBody),
        (status = 503, description = "Too many open streams (`too_many_streams`), with `Retry-After: 1`, or the session is shutting down (`shutting_down`).", body = ErrorBody),
    ),
)]
async fn stream_file(
    State(state): State<AppState>,
    ApiPath((id, index)): ApiPath<(String, usize)>,
    request: HeaderMap,
) -> Result<Response, ApiErrorResponse> {
    let plan = match plan(&state, &id, index, &request)? {
        Ok(plan) => plan,
        Err(res) => return Ok(res),
    };
    if plan.count == 0 {
        return Ok(respond(plan.status, plan.headers, Body::empty()));
    }
    let permit = state
        .streams
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiErrorResponse::too_many_streams())?;
    let mut reader = plan.handle.open_file(plan.index).await?;
    reader
        .seek(SeekFrom::Start(plan.start))
        .await
        .map_err(|e| ApiErrorResponse::internal(format!("seek failed: {e}")))?;
    let body = Permitted {
        inner: ReaderStream::with_capacity(reader.take(plan.count), CHUNK),
        _permit: permit,
    }
    .take_until(state.shutdown.clone().cancelled_owned());
    Ok(respond(plan.status, plan.headers, Body::from_stream(body)))
}

/// The headers `GET` would send, with no body.
#[utoipa::path(
    head,
    path = "/api/v1/torrents/{id}/files/{index}/stream",
    tag = "torrents",
    params(
        ("id" = String, Path, description = "40-hex wire hash or 64-hex v2 hash."),
        ("index" = usize, Path, description = "The file's index in the torrent."),
        ("Range" = Option<String>, Header, description = "As for `GET`."),
        ("If-Range" = Option<String>, Header, description = "As for `GET`."),
        ("token" = Option<String>, Query, description = "The token, for clients that cannot send `Authorization`."),
    ),
    responses(
        (status = 200, description = "The whole file's headers."),
        (status = 206, description = "The range's headers, with `Content-Range`."),
        (status = 400, description = "Malformed id or index (`invalid_id`, `invalid_request`)."),
        (status = 401, description = "No valid token (`unauthorized`)."),
        (status = 404, description = "Unknown torrent or file (`not_found`)."),
        (status = 409, description = "No metadata yet (`no_metadata`)."),
        (status = 416, description = "The range starts past the end, with `Content-Range: bytes */<len>`."),
    ),
)]
async fn stream_file_head(
    State(state): State<AppState>,
    ApiPath((id, index)): ApiPath<(String, usize)>,
    request: HeaderMap,
) -> Result<Response, ApiErrorResponse> {
    Ok(match plan(&state, &id, index, &request)? {
        Ok(plan) => respond(plan.status, plan.headers, Body::empty()),
        Err(res) => res,
    })
}

/// A body stream holding a stream permit until it is dropped.
struct Permitted<S> {
    inner: S,
    _permit: OwnedSemaphorePermit,
}

impl<S: Stream + Unpin> Stream for Permitted<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

/// A header value from text this module built (ASCII digits and
/// punctuation, always valid).
fn header_value(text: &str) -> HeaderValue {
    HeaderValue::from_str(text).unwrap_or_else(|_| HeaderValue::from_static(""))
}

/// The file's last path component.
fn file_name(file: &FileEntry) -> &str {
    file.path.components().last().map_or("file", String::as_str)
}

/// The `Content-Type` for the file's extension.
fn content_type(file: &FileEntry) -> HeaderValue {
    const TYPES: &[(&str, &str)] = &[
        ("mp4", "video/mp4"),
        ("m4v", "video/x-m4v"),
        ("mkv", "video/x-matroska"),
        ("webm", "video/webm"),
        ("mov", "video/quicktime"),
        ("avi", "video/x-msvideo"),
        ("mp3", "audio/mpeg"),
        ("m4a", "audio/mp4"),
        ("flac", "audio/flac"),
        ("ogg", "audio/ogg"),
        ("opus", "audio/opus"),
        ("wav", "audio/wav"),
        ("srt", "application/x-subrip"),
        ("vtt", "text/vtt"),
        ("txt", "text/plain; charset=utf-8"),
        ("jpg", "image/jpeg"),
        ("png", "image/png"),
        ("pdf", "application/pdf"),
    ];
    let ext = file_name(file).rsplit_once('.').map(|(_, ext)| ext);
    let mime = ext
        .and_then(|ext| {
            TYPES
                .iter()
                .find(|(e, _)| e.eq_ignore_ascii_case(ext))
                .map(|&(_, mime)| mime)
        })
        .unwrap_or("application/octet-stream");
    HeaderValue::from_static(mime)
}

/// `inline; filename="<ascii>"; filename*=UTF-8''<percent-encoded>` for
/// the file's last path component (RFC 6266, RFC 8187).
fn content_disposition(file: &FileEntry) -> HeaderValue {
    disposition(file_name(file))
}

fn disposition(name: &str) -> HeaderValue {
    let ascii: String = name
        .chars()
        .map(|c| {
            if c.is_ascii() && !c.is_ascii_control() && c != '"' && c != '\\' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let mut encoded = String::with_capacity(name.len());
    for b in name.bytes() {
        if b.is_ascii_alphanumeric() || b"!#$&+-.^_`|~".contains(&b) {
            encoded.push(char::from(b));
        } else {
            // Writing to a String cannot fail.
            let _ = write!(encoded, "%{b:02X}");
        }
    }
    header_value(&format!(
        "inline; filename=\"{ascii}\"; filename*=UTF-8''{encoded}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disposition_encodes_names() {
        assert_eq!(
            disposition("video.mp4"),
            "inline; filename=\"video.mp4\"; filename*=UTF-8''video.mp4"
        );
        assert_eq!(
            disposition("a \"b\"\\c\u{7f}é.mkv"),
            "inline; filename=\"a _b__c__.mkv\"; filename*=UTF-8''a%20%22b%22%5Cc%7F%C3%A9.mkv"
        );
        assert_eq!(
            disposition("x;y=z'*%.txt"),
            "inline; filename=\"x;y=z'*%.txt\"; filename*=UTF-8''x%3By%3Dz%27%2A%25.txt"
        );
    }
}
