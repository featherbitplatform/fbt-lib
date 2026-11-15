//! Routes an embedder hands fbt-api: `ApiServer::start_with` and `router_with`
//! merge them ahead of the auth, body-limit and CORS layers, so they are
//! served by the same server, with the same token rules and caps, as the
//! crate's own routes.

mod support;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::Request;
use axum::http::header::AUTHORIZATION;
use axum::routing::{get, post};
use fbt::Session;
use fbt_api::{ApiConfig, ApiServer, Auth, AuthRejection, AuthRequest, Authenticator, router_with};
use hyper::header::WWW_AUTHENTICATE;
use hyper::{Method, StatusCode};
use support::{TOKEN, error_code, json, send_body, session, test_config};
use tower::ServiceExt as _;

/// The embedder's own routes: a `GET` that answers, and a `POST` that reports
/// how many bytes it read — the body limit bites when it cannot read them.
fn extra() -> Router {
    Router::new()
        .route("/api/v1/x/hello", get(|| async { "hello from extra" }))
        .route(
            "/api/v1/x/echo",
            post(|body: Bytes| async move { body.len().to_string() }),
        )
}

/// An [`ApiServer`] over [`test_config`] (with `edit` applied) serving the API
/// and `extra`. Returns the session, the server, the base url and the token.
async fn api_with_extra(
    edit: impl FnOnce(&mut ApiConfig),
    extra: Router,
) -> (Session, ApiServer, String, &'static str) {
    let session = session().await;
    let mut config = test_config();
    edit(&mut config);
    let server = ApiServer::start_with(session.clone(), config, extra)
        .await
        .expect("api server starts");
    let base = format!("http://{}", server.local_addr());
    (session, server, base, TOKEN)
}

#[tokio::test]
async fn extra_routes_are_served() {
    let (_session, _server, base, token) = api_with_extra(|_| {}, extra()).await;

    // The extra route answers for the configured bearer.
    let url = format!("{base}/api/v1/x/hello");
    let (status, _, body) = support::get(&url, Some(token)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), b"hello from extra");

    // Only that bearer: a wrong one is refused.
    let mut wrong = token.to_owned();
    wrong.pop();
    wrong.push('X');
    let (status, _, _) = support::get(&url, Some(&wrong)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // The extra `POST` is served too.
    let (status, _, body) = send_body(
        Method::POST,
        &format!("{base}/api/v1/x/echo"),
        Some(token),
        None,
        "abc",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), b"3");

    // And fbt-api's own routes keep working on the same server.
    let (status, _, body) = support::get(&format!("{base}/api/v1/session"), Some(token)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&body)["torrents"], 0);
}

/// What a [`Recorder`] last saw, as owned values.
#[derive(Clone, Debug)]
struct Seen {
    method: String,
    matched_path: Option<String>,
    query_token: Option<String>,
    peer: Option<SocketAddr>,
}

/// An [`Authenticator`] that allows everything and records what it saw.
#[derive(Debug, Default)]
struct Recorder(Mutex<Option<Seen>>);

impl Authenticator for Recorder {
    fn authorize(&self, request: &AuthRequest<'_>) -> Result<(), AuthRejection> {
        let seen = Seen {
            method: request.method.as_str().to_owned(),
            matched_path: request.matched_path.map(str::to_owned),
            query_token: request.query_token.map(str::to_owned),
            peer: request.peer,
        };
        *self.0.lock().expect("no panic under the lock") = Some(seen);
        Ok(())
    }
}

impl Recorder {
    /// What the last request looked like to the authenticator.
    fn seen(&self) -> Seen {
        self.0
            .lock()
            .expect("no panic under the lock")
            .clone()
            .expect("a request was authorized")
    }
}

#[tokio::test]
async fn extra_routes_go_through_auth() {
    // `Auth::Token`: without a bearer an extra route gets the same 401 as a
    // built-in one, and `?token=` is not offered to it (only fbt-api's own
    // stream and event routes take it).
    let (_session, _server, base, token) = api_with_extra(|_| {}, extra()).await;
    let url = format!("{base}/api/v1/x/hello");
    let (status, headers, body) = support::get(&url, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(headers[WWW_AUTHENTICATE], "Bearer");
    assert_eq!(error_code(&body), "unauthorized");

    let (status, _, _) = support::get(&format!("{url}?token={token}"), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // `Auth::Custom`: the authenticator is asked for extra routes and sees
    // their matched path — proof the merged routes are matched by the same
    // router, so `MatchedPath` is set for them too.
    let session = session().await;
    let recorder = Arc::new(Recorder::default());
    let mut config = test_config();
    config.auth = Auth::Custom(recorder.clone());
    let server = ApiServer::start_with(session, config, extra())
        .await
        .expect("api server starts");
    let base = format!("http://{}", server.local_addr());

    let (status, _, body) = support::get(&format!("{base}/api/v1/x/hello"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), b"hello from extra");
    let seen = recorder.seen();
    assert_eq!(seen.method, "GET");
    assert_eq!(seen.matched_path.as_deref(), Some("/api/v1/x/hello"));
    // `ApiServer` attaches its `ConnectInfo` to every request it serves.
    assert_eq!(
        seen.peer.map(|peer| peer.ip().to_string()),
        Some("127.0.0.1".to_owned())
    );

    // A query token is not offered here either: the extra route is not one of
    // the stream or event routes fbt-api gives it to.
    let (status, _, _) = support::get(&format!("{base}/api/v1/x/hello?token={token}"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(recorder.seen().query_token, None);

    server.shutdown().await;
}

#[tokio::test]
async fn extra_routes_respect_body_limit() {
    let (_session, _server, base, token) =
        api_with_extra(|c| c.max_body_bytes = 1024, extra()).await;
    let url = format!("{base}/api/v1/x/echo");

    // At the limit the handler reads the body.
    let (status, _, body) =
        send_body(Method::POST, &url, Some(token), None, vec![0u8; 1_024]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), b"1024");

    // Past it the request never reaches the handler: `413`, and not the byte
    // count the handler would have answered with.
    let (status, _, body) =
        send_body(Method::POST, &url, Some(token), None, vec![0u8; 2_048]).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    let text = String::from_utf8_lossy(&body);
    assert!(!text.contains("2048"), "{text}");
}

/// A client mounting `router_with` in its own server gets the same layers on
/// the merged routes; no [`ApiServer`] is involved (a oneshot call).
#[tokio::test]
async fn router_with_mounts_extra_behind_the_layers() {
    let session = session().await;
    let app = router_with(session, &test_config(), extra()).expect("router");

    let (status, headers, body) = call(&app, get_req("/api/v1/x/hello")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(headers[WWW_AUTHENTICATE], "Bearer");
    assert_eq!(error_code(&body), "unauthorized");

    let req = Request::get("/api/v1/x/hello")
        .header(AUTHORIZATION, format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .expect("request builds");
    let (status, _, body) = call(&app, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), b"hello from extra");

    // The crate's own routes are served as before.
    let req = Request::get("/api/v1/session")
        .header(AUTHORIZATION, format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .expect("request builds");
    let (status, _, body) = call(&app, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&body)["torrents"], 0);

    // And so is `openapi.json`, still exempt under `Auth::Token`.
    let status = call_status(&app, get_req("/api/v1/openapi.json")).await;
    assert_eq!(status, StatusCode::OK);
}

/// A `GET` on `uri` with an empty body.
fn get_req(uri: &str) -> Request {
    Request::get(uri)
        .body(Body::empty())
        .expect("request builds")
}

/// Sends `req` through `app` and reads status, headers and the whole body.
async fn call(app: &Router, req: Request) -> (StatusCode, axum::http::HeaderMap, Bytes) {
    let res = app
        .clone()
        .oneshot(req)
        .await
        .expect("router is infallible");
    let (parts, body) = res.into_parts();
    (
        parts.status,
        parts.headers,
        to_bytes(body, 1 << 20).await.expect("body reads"),
    )
}

/// [`call`]'s status only.
async fn call_status(app: &Router, req: Request) -> StatusCode {
    let (status, _, _) = call(app, req).await;
    status
}
