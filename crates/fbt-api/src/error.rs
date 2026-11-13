//! Errors: [`ApiError`] (config and startup), [`ApiErrorResponse`] (every
//! error a request gets, with its JSON body), the mapping of
//! [`SessionError`] and [`CreateError`], and the crate extractors whose
//! rejections are [`ApiErrorResponse`]s.
//!
//! Handlers return `Result<_, ApiErrorResponse>` and use `?` on `fbt`
//! calls; they take bodies, queries and path parameters only through
//! [`ApiJson`], [`ApiQuery`], [`ApiPath`] and [`ApiBytes`], so every error
//! has the JSON shape.

use std::io;
use std::path::PathBuf;

use axum::body::Bytes;
use axum::extract::rejection::{BytesRejection, JsonRejection, PathRejection, QueryRejection};
use axum::extract::{FromRequest, FromRequestParts, Request};
use axum::http::request::Parts;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use fbt::{CreateError, SessionError};
use serde::de::DeserializeOwned;

use crate::dto::{ErrorBody, ErrorDetail};

/// Why [`router`](crate::router) or [`ApiServer::start`](crate::ApiServer::start)
/// refused to start.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ApiError {
    /// [`Auth::Token`](crate::Auth::Token) holds an empty token.
    #[error("the API token is empty")]
    EmptyToken,
    /// [`Auth::Disabled`](crate::Auth::Disabled) with a
    /// [`bind`](crate::ApiConfig::bind) address that is not loopback.
    #[error("authentication can be disabled only on a loopback bind address")]
    InsecureBind,
    /// A [`cors_origins`](crate::ApiConfig::cors_origins) entry is not a
    /// valid header value.
    #[error("invalid CORS origin")]
    InvalidOrigin,
    /// An [`allowed_roots`](crate::ApiConfig::allowed_roots) entry does not
    /// exist or cannot be canonicalised.
    #[error("allowed root {} cannot be used", .0.display())]
    InvalidRoot(PathBuf),
    /// The listener could not bind [`bind`](crate::ApiConfig::bind).
    #[error("cannot bind the API listener: {0}")]
    Bind(#[source] io::Error),
}

/// An error response: `status` with the body
/// `{"error": {"code": "<code>", "message": "<message>"}}`.
///
/// Codes and statuses (keep [`crate::dto::ErrorDetail`]'s docs in step):
/// `400` `invalid_request`, `invalid_id`, `invalid_priorities`,
/// `invalid_path`; `401` `unauthorized` (with `WWW-Authenticate: Bearer`);
/// `403` `path_not_allowed`, `forbidden`; `404` `not_found`; `409`
/// `no_metadata`, `duplicate_torrent`, `busy`, `already_exists`; `413`
/// `body_too_large`; `416` `range_not_satisfiable`; `501` `unsupported`;
/// `503` `too_many_streams` (with `Retry-After: 1`), `shutting_down`,
/// `dht_disabled`; `500` `internal`.
#[derive(Debug)]
pub(crate) struct ApiErrorResponse {
    /// The response status.
    pub(crate) status: StatusCode,
    /// The snake_case error code.
    pub(crate) code: &'static str,
    /// A human-readable message.
    pub(crate) message: String,
}

impl ApiErrorResponse {
    /// An error with any status and code.
    pub(crate) fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        ApiErrorResponse {
            status,
            code,
            message: message.into(),
        }
    }

    /// `400 invalid_request`.
    pub(crate) fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", message)
    }

    /// `401 unauthorized`.
    pub(crate) fn unauthorized() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "a valid bearer token is required",
        )
    }

    /// `403 forbidden`.
    pub(crate) fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", message)
    }

    /// `404 not_found`.
    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    /// `503 too_many_streams`, with `Retry-After: 1`.
    pub(crate) fn too_many_streams() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "too_many_streams",
            "too many open streams, retry later",
        )
    }

    /// `500 internal`.
    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
    }
}

impl IntoResponse for ApiErrorResponse {
    fn into_response(self) -> Response {
        if self.status.is_server_error() {
            tracing::debug!(code = self.code, message = %self.message, "api request failed");
        }
        let body = ErrorBody {
            error: ErrorDetail {
                code: self.code.to_owned(),
                message: self.message,
            },
        };
        let mut res = (self.status, axum::Json(body)).into_response();
        match self.code {
            "unauthorized" => {
                res.headers_mut()
                    .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
            }
            "too_many_streams" => {
                res.headers_mut()
                    .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            }
            _ => {}
        }
        res
    }
}

impl From<SessionError> for ApiErrorResponse {
    fn from(e: SessionError) -> Self {
        let message = e.to_string();
        let (status, code) = match &e {
            SessionError::TorrentNotFound | SessionError::InvalidFileIndex { .. } => {
                (StatusCode::NOT_FOUND, "not_found")
            }
            SessionError::DuplicateTorrent => (StatusCode::CONFLICT, "duplicate_torrent"),
            SessionError::NoMetadata => (StatusCode::CONFLICT, "no_metadata"),
            SessionError::Busy | SessionError::TooManyWebSeeds => (StatusCode::CONFLICT, "busy"),
            SessionError::InvalidPriorities { .. } => {
                (StatusCode::BAD_REQUEST, "invalid_priorities")
            }
            SessionError::InvalidPath(_) => (StatusCode::BAD_REQUEST, "invalid_path"),
            SessionError::InvalidSettings(_)
            | SessionError::InvalidUrl(_)
            | SessionError::Resume(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
            SessionError::TooManyReaders => return Self::too_many_streams(),
            SessionError::Shutdown => (StatusCode::SERVICE_UNAVAILABLE, "shutting_down"),
            SessionError::DhtDisabled => (StatusCode::SERVICE_UNAVAILABLE, "dht_disabled"),
            SessionError::Unsupported => (StatusCode::NOT_IMPLEMENTED, "unsupported"),
            SessionError::Storage(io) if io.kind() == io::ErrorKind::AlreadyExists => {
                (StatusCode::CONFLICT, "already_exists")
            }
            SessionError::Storage(_)
            | SessionError::Bind(_)
            | SessionError::Tracker(_)
            | SessionError::Proxy(_)
            | SessionError::Dht(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
            #[allow(deprecated)]
            SessionError::V2OnlyUnsupported => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
            // Variants added later.
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        };
        Self::new(status, code, message)
    }
}

impl From<CreateError> for ApiErrorResponse {
    fn from(e: CreateError) -> Self {
        let message = e.to_string();
        let (status, code) = match &e {
            CreateError::Empty
            | CreateError::InvalidName(_)
            | CreateError::InvalidPieceLen(_)
            | CreateError::TooManyFiles
            | CreateError::TooManyPieces => (StatusCode::BAD_REQUEST, "invalid_request"),
            CreateError::Io { source, .. } if source.kind() == io::ErrorKind::NotFound => {
                (StatusCode::NOT_FOUND, "not_found")
            }
            CreateError::Io { .. } => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
            CreateError::FileChanged(_) => (StatusCode::CONFLICT, "busy"),
            CreateError::Cancelled => (StatusCode::SERVICE_UNAVAILABLE, "shutting_down"),
            // Variants added later.
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        };
        Self::new(status, code, message)
    }
}

/// An extractor rejection: `413 body_too_large`, a server-side failure as
/// `500 internal`, anything else `400 invalid_request`.
fn rejection(status: StatusCode, text: String) -> ApiErrorResponse {
    if status == StatusCode::PAYLOAD_TOO_LARGE {
        ApiErrorResponse::new(status, "body_too_large", text)
    } else if status.is_server_error() {
        ApiErrorResponse::internal(text)
    } else {
        ApiErrorResponse::invalid_request(text)
    }
}

impl From<JsonRejection> for ApiErrorResponse {
    fn from(r: JsonRejection) -> Self {
        rejection(r.status(), r.body_text())
    }
}

impl From<BytesRejection> for ApiErrorResponse {
    fn from(r: BytesRejection) -> Self {
        rejection(r.status(), r.body_text())
    }
}

impl From<QueryRejection> for ApiErrorResponse {
    fn from(r: QueryRejection) -> Self {
        rejection(r.status(), r.body_text())
    }
}

impl From<PathRejection> for ApiErrorResponse {
    fn from(r: PathRejection) -> Self {
        rejection(r.status(), r.body_text())
    }
}

/// [`axum::Json`] whose rejection is an [`ApiErrorResponse`]. Request DTOs
/// use `#[serde(deny_unknown_fields)]`.
#[derive(Debug)]
pub(crate) struct ApiJson<T>(pub(crate) T);

impl<T: DeserializeOwned, S: Send + Sync> FromRequest<S> for ApiJson<T> {
    type Rejection = ApiErrorResponse;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let axum::Json(value) = axum::Json::<T>::from_request(req, state).await?;
        Ok(ApiJson(value))
    }
}

/// [`axum::extract::Query`] whose rejection is an [`ApiErrorResponse`].
#[derive(Debug)]
pub(crate) struct ApiQuery<T>(pub(crate) T);

impl<T: DeserializeOwned, S: Send + Sync> FromRequestParts<S> for ApiQuery<T> {
    type Rejection = ApiErrorResponse;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let axum::extract::Query(value) =
            axum::extract::Query::<T>::from_request_parts(parts, state).await?;
        Ok(ApiQuery(value))
    }
}

/// [`axum::extract::Path`] whose rejection is an [`ApiErrorResponse`].
#[derive(Debug)]
pub(crate) struct ApiPath<T>(pub(crate) T);

impl<T: DeserializeOwned + Send, S: Send + Sync> FromRequestParts<S> for ApiPath<T> {
    type Rejection = ApiErrorResponse;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let axum::extract::Path(value) =
            axum::extract::Path::<T>::from_request_parts(parts, state).await?;
        Ok(ApiPath(value))
    }
}

/// The whole request body, at most
/// [`ApiConfig::max_body_bytes`](crate::ApiConfig::max_body_bytes), whose
/// rejection is an [`ApiErrorResponse`] (`413 body_too_large` past the
/// limit).
#[derive(Debug)]
pub(crate) struct ApiBytes(pub(crate) Bytes);

impl<S: Send + Sync> FromRequest<S> for ApiBytes {
    type Rejection = ApiErrorResponse;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        Ok(ApiBytes(Bytes::from_request(req, state).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::extract::DefaultBodyLimit;
    use axum::routing::post;
    use serde::Deserialize;
    use tower::ServiceExt;

    async fn parts(res: Response) -> (StatusCode, axum::http::HeaderMap, serde_json::Value) {
        let status = res.status();
        let headers = res.headers().clone();
        let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        (status, headers, serde_json::from_slice(&body).unwrap())
    }

    fn status_code(e: impl Into<ApiErrorResponse>) -> (StatusCode, &'static str) {
        let e = e.into();
        (e.status, e.code)
    }

    #[tokio::test]
    async fn error_body_shape_and_headers() {
        let (status, headers, body) = parts(ApiErrorResponse::unauthorized().into_response()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(headers[header::WWW_AUTHENTICATE], "Bearer");
        assert_eq!(body["error"]["code"], "unauthorized");
        assert!(body["error"]["message"].is_string());

        let res = ApiErrorResponse::from(SessionError::TooManyReaders).into_response();
        let (status, headers, body) = parts(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(headers[header::RETRY_AFTER], "1");
        assert_eq!(body["error"]["code"], "too_many_streams");
    }

    #[test]
    fn session_error_mapping() {
        use StatusCode as S;
        let cases: Vec<(SessionError, StatusCode, &str)> = vec![
            (SessionError::TorrentNotFound, S::NOT_FOUND, "not_found"),
            (
                SessionError::InvalidFileIndex { index: 3, files: 1 },
                S::NOT_FOUND,
                "not_found",
            ),
            (
                SessionError::DuplicateTorrent,
                S::CONFLICT,
                "duplicate_torrent",
            ),
            (SessionError::NoMetadata, S::CONFLICT, "no_metadata"),
            (SessionError::Busy, S::CONFLICT, "busy"),
            (SessionError::TooManyWebSeeds, S::CONFLICT, "busy"),
            (
                SessionError::InvalidPriorities {
                    expected: 2,
                    got: 1,
                },
                S::BAD_REQUEST,
                "invalid_priorities",
            ),
            (
                SessionError::InvalidPath("../x".into()),
                S::BAD_REQUEST,
                "invalid_path",
            ),
            (
                SessionError::InvalidSettings("x".into()),
                S::BAD_REQUEST,
                "invalid_request",
            ),
            (
                SessionError::InvalidUrl("ftp://x".into()),
                S::BAD_REQUEST,
                "invalid_request",
            ),
            (
                SessionError::TooManyReaders,
                S::SERVICE_UNAVAILABLE,
                "too_many_streams",
            ),
            (
                SessionError::Shutdown,
                S::SERVICE_UNAVAILABLE,
                "shutting_down",
            ),
            (
                SessionError::DhtDisabled,
                S::SERVICE_UNAVAILABLE,
                "dht_disabled",
            ),
            (SessionError::Unsupported, S::NOT_IMPLEMENTED, "unsupported"),
            (
                SessionError::Storage(io::Error::from(io::ErrorKind::AlreadyExists)),
                S::CONFLICT,
                "already_exists",
            ),
            (
                SessionError::Storage(io::Error::from(io::ErrorKind::PermissionDenied)),
                S::INTERNAL_SERVER_ERROR,
                "internal",
            ),
            (
                SessionError::Bind(io::Error::from(io::ErrorKind::AddrInUse)),
                S::INTERNAL_SERVER_ERROR,
                "internal",
            ),
        ];
        for (e, status, code) in cases {
            let shown = format!("{e:?}");
            assert_eq!(status_code(e), (status, code), "{shown}");
        }
    }

    #[test]
    fn create_error_mapping() {
        use StatusCode as S;
        let io_err = |kind| CreateError::Io {
            path: "x".into(),
            source: io::Error::from(kind),
        };
        let cases: Vec<(CreateError, StatusCode, &str)> = vec![
            (CreateError::Empty, S::BAD_REQUEST, "invalid_request"),
            (
                CreateError::InvalidName("x".into()),
                S::BAD_REQUEST,
                "invalid_request",
            ),
            (
                CreateError::InvalidPieceLen(3),
                S::BAD_REQUEST,
                "invalid_request",
            ),
            (CreateError::TooManyFiles, S::BAD_REQUEST, "invalid_request"),
            (
                CreateError::TooManyPieces,
                S::BAD_REQUEST,
                "invalid_request",
            ),
            (io_err(io::ErrorKind::NotFound), S::NOT_FOUND, "not_found"),
            (
                io_err(io::ErrorKind::PermissionDenied),
                S::INTERNAL_SERVER_ERROR,
                "internal",
            ),
            (CreateError::FileChanged("x".into()), S::CONFLICT, "busy"),
            (
                CreateError::Cancelled,
                S::SERVICE_UNAVAILABLE,
                "shutting_down",
            ),
        ];
        for (e, status, code) in cases {
            let shown = format!("{e:?}");
            assert_eq!(status_code(e), (status, code), "{shown}");
        }
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body1 {
        #[allow(dead_code)]
        a: u32,
    }

    fn app(limit: usize) -> Router {
        Router::new()
            .route(
                "/json",
                post(|ApiJson(_): ApiJson<Body1>| async { StatusCode::NO_CONTENT }),
            )
            .route(
                "/bytes",
                post(|ApiBytes(b): ApiBytes| async move { b.len().to_string() }),
            )
            .route(
                "/query",
                post(|ApiQuery(_): ApiQuery<Body1>| async { StatusCode::NO_CONTENT }),
            )
            .route(
                "/path/{a}",
                post(|ApiPath(_): ApiPath<u8>| async { StatusCode::NO_CONTENT }),
            )
            .layer(DefaultBodyLimit::max(limit))
    }

    async fn call(app: Router, uri: &str, ct: &str, body: Vec<u8>) -> Response {
        let req = Request::post(uri)
            .header(header::CONTENT_TYPE, ct)
            .body(Body::from(body))
            .unwrap();
        app.oneshot(req).await.unwrap()
    }

    #[tokio::test]
    async fn extractor_rejections_are_json() {
        let json = "application/json";
        for (uri, ct, body) in [
            ("/json", json, b"{".to_vec()),
            ("/json", json, br#"{"a":1,"b":2}"#.to_vec()),
            ("/json", json, br#"{"a":"x"}"#.to_vec()),
            ("/json", "text/plain", br#"{"a":1}"#.to_vec()),
            ("/query?a=x", json, Vec::new()),
            ("/path/300", json, Vec::new()),
        ] {
            let (status, _, body) = parts(call(app(1024), uri, ct, body).await).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
            assert_eq!(body["error"]["code"], "invalid_request", "{uri}");
        }
        let res = call(app(1024), "/json", json, br#"{"a":1}"#.to_vec()).await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn body_limit_is_413_json_and_overrides_axum_default() {
        let (status, _, body) = parts(
            call(
                app(1024),
                "/bytes",
                "application/octet-stream",
                vec![0; 2048],
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(body["error"]["code"], "body_too_large");
        let (status, _, body) =
            parts(call(app(1024), "/json", "application/json", vec![b' '; 2048]).await).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(body["error"]["code"], "body_too_large");
        // Above axum's own 2 MB default.
        let res = call(
            app(4 << 20),
            "/bytes",
            "application/octet-stream",
            vec![0; 3 << 20],
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
    }
}
