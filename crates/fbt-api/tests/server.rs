//! Routes an embedder hands fbt-api: `ApiServer::start_with` and `router_with`
//! merge them ahead of the auth, body-limit and CORS layers, so they are
//! served by the same server, with the same token rules and caps, as the
//! crate's own routes.

mod support;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::Request;
use axum::http::header::AUTHORIZATION;
use axum::routing::{get, post};
use fbt::Session;
use fbt_api::{ApiConfig, ApiServer, Auth, AuthRejection, AuthRequest, Authenticator, router_with};
use http_body_util::Full;
use hyper::header::{ORIGIN, WWW_AUTHENTICATE};
use hyper::{Method, StatusCode};
use support::{TOKEN, error_code, json, request, send, send_body, session, test_config};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
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

/// An empty body for the hyper client.
fn empty() -> Full<Bytes> {
    Full::new(Bytes::new())
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

#[tokio::test]
async fn extra_route_gets_cors() {
    let (_session, _server, base, token) = api_with_extra(
        |c| c.cors_origins = vec!["http://ui.example".to_owned()],
        extra(),
    )
    .await;
    let url = format!("{base}/api/v1/x/hello");

    // The CORS layer is outside the merged routes, so a request from a listed
    // origin carries its header.
    let req = request(Method::GET, &url, Some(token))
        .header(ORIGIN, "http://ui.example")
        .body(empty())
        .expect("request builds");
    let (status, headers, body) = send(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), b"hello from extra");
    assert_eq!(headers["access-control-allow-origin"], "http://ui.example");

    // An origin fbt-api does not list carries none.
    let req = request(Method::GET, &url, Some(token))
        .header(ORIGIN, "http://other.example")
        .body(empty())
        .expect("request builds");
    let (_, headers, _) = send(req).await;
    assert!(!headers.contains_key("access-control-allow-origin"));

    // A preflight is answered by the CORS layer before auth and needs no token:
    // what an embedder's own login route relies on.
    let req = request(Method::OPTIONS, &url, None)
        .header(ORIGIN, "http://ui.example")
        .header("access-control-request-method", "POST")
        .header("access-control-request-headers", "authorization")
        .body(empty())
        .expect("request builds");
    let (status, headers, _) = send(req).await;
    assert!(status.is_success(), "{status}");
    assert_eq!(headers["access-control-allow-origin"], "http://ui.example");
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

// ---------------------------------------------------------------------------
// Request-body read deadline (body_read_timeout / body_min_rate)
// ---------------------------------------------------------------------------

/// An extra route that reads the whole body and answers with its length.
fn login() -> Router {
    Router::new().route(
        "/api/v1/auth/login",
        post(|body: Bytes| async move { body.len().to_string() }),
    )
}

/// A server for the body-deadline tests: `test_config` with `edit` applied,
/// serving [`login`] on top of the API. Returns the session, the server,
/// the bound address and the token.
async fn body_server(
    edit: impl FnOnce(&mut ApiConfig),
) -> (Session, ApiServer, SocketAddr, &'static str) {
    let session = session().await;
    let mut config = test_config();
    edit(&mut config);
    let server = ApiServer::start_with(session.clone(), config, login())
        .await
        .expect("api server starts");
    let addr = server.local_addr();
    (session, server, addr, TOKEN)
}

/// Sends the request line and headers for a `POST /api/v1/auth/login` of
/// `content_length` bytes on `client`.
async fn send_login_headers(client: &mut TcpStream, token: &str, content_length: usize) {
    client
        .write_all(
            format!(
                "POST /api/v1/auth/login HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nContent-Length: {content_length}\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .expect("headers written");
}

/// True when the socket is closed (a read returns EOF or an error) within
/// `budget`.
async fn closed_within(client: &mut TcpStream, budget: Duration) -> bool {
    let mut buf = [0u8; 4096];
    let closed = tokio::time::timeout(budget, async {
        loop {
            match client.read(&mut buf).await {
                Ok(0) | Err(_) => return true,
                Ok(_) => {}
            }
        }
    })
    .await;
    matches!(closed, Ok(true))
}

/// A stalled body (10 of 1000 bytes, then nothing) loses the connection
/// well inside the base allowance: the guard cuts it, and it never answers
/// 200.
#[tokio::test]
async fn a_body_that_stops_arriving_closes_the_connection() {
    let (_session, _server, addr, token) =
        body_server(|c| {
            c.body_read_timeout = Duration::from_millis(300);
            c.body_min_rate = 1000;
        })
        .await;
    let mut client = TcpStream::connect(addr).await.expect("connects");
    send_login_headers(&mut client, token, 1000).await;
    client
        .write_all(&[0u8; 10])
        .await
        .expect("10 body bytes written");
    // 10 bytes at 1000 B/s stretch the deadline to about 301 ms; long gone
    // by the 3 s budget. Nothing else arrives.
    assert!(
        closed_within(&mut client, Duration::from_secs(3)).await,
        "a stalled body kept its connection"
    );
    drop(client);
    _server.shutdown().await;
}

/// A body that keeps arriving (1 byte every 100 ms) stays far under the
/// minimum rate of 1000 B/s, so the deadline still runs out: an idle-gap
/// timer alone would never catch this.
#[tokio::test]
async fn a_trickled_body_is_cut_off_although_bytes_keep_coming() {
    let (_session, _server, addr, token) =
        body_server(|c| {
            c.body_read_timeout = Duration::from_millis(300);
            c.body_min_rate = 1000;
        })
        .await;
    let mut client = TcpStream::connect(addr).await.expect("connects");
    send_login_headers(&mut client, token, 100_000).await;
    // 1 byte every 100 ms: each byte stretches the deadline by 1 ms, so
    // after 300 bytes (30 ms real time) the deadline is due at about 300 ms.
    // The connection must be closed while the client is still sending:
    // an idle-gap timer (the mutation) would keep it open for the whole
    // 40 s of trickling and only close it after the client stops.
    let mut closed_while_sending = false;
    for _ in 0..400 {
        match client.write_u8(0).await {
            Ok(()) => {}
            Err(_) => {
                closed_while_sending = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        if closed_within(&mut client, Duration::from_millis(50)).await {
            closed_while_sending = true;
            break;
        }
    }
    assert!(
        closed_while_sending,
        "a trickled body kept its connection"
    );
    drop(client);
    _server.shutdown().await;
}

/// A body above the minimum rate (100 bytes every 50 ms, 2000 B/s) arrives
/// far past the 300 ms base allowance and still completes: the per-byte
/// extension keeps the deadline ahead. A fixed total deadline would cut it.
#[tokio::test]
async fn a_body_above_the_minimum_rate_completes() {
    let (_session, _server, addr, token) =
        body_server(|c| {
            c.body_read_timeout = Duration::from_millis(300);
            c.body_min_rate = 1000;
        })
        .await;
    let mut client = TcpStream::connect(addr).await.expect("connects");
    send_login_headers(&mut client, token, 3000).await;
    let mut written = 0usize;
    for _ in 0..30 {
        match client.write_all(&[0u8; 100]).await {
            Ok(()) => written += 100,
            Err(_) => break,
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        written == 3000,
        "a body above the minimum rate was cut off: only {written} of 3000 bytes were written"
    );
    let mut buf = [0u8; 4096];
    let mut got = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !got.starts_with(b"HTTP/1.1 200") {
            match client.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => got.extend_from_slice(&buf[..n]),
            }
        }
    })
    .await
    .expect("a body above the minimum rate was cut off: no response within 5 s");
    let text = String::from_utf8_lossy(&got);
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
    assert!(text.contains("3000"), "{text}");
    drop(client);
    _server.shutdown().await;
}

/// Three stalled bodies occupy all three `max_connections` slots; once the
/// guard closes them, a fresh request is served: the permits come back.
#[tokio::test]
async fn a_stalled_body_frees_its_connection_permit() {
    let (_session, _server, addr, token) =
        body_server(|c| {
            c.body_read_timeout = Duration::from_millis(300);
            c.body_min_rate = 1000;
            c.max_streams = 1;
            c.max_event_streams = 1;
            c.max_connections = 3;
        })
        .await;
    let mut stalled = Vec::new();
    for _ in 0..3 {
        let mut client = TcpStream::connect(addr).await.expect("connects");
        send_login_headers(&mut client, token, 1000).await;
        client
            .write_all(&[0u8; 10])
            .await
            .expect("10 body bytes written");
        stalled.push(client);
    }
    for client in stalled.iter_mut() {
        assert!(
            closed_within(client, Duration::from_secs(3)).await,
            "a stalled body never gave back its permit: its connection stayed open"
        );
    }
    // Every slot is free again: a fresh request is served.
    let mut client = TcpStream::connect(addr).await.expect("connects");
    client
        .write_all(b"GET /api/v1/session HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer ")
        .await
        .expect("request written");
    client
        .write_all(format!("{token}\r\n\r\n").as_bytes())
        .await
        .expect("request written");
    let mut buf = [0u8; 4096];
    let mut got = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !got.starts_with(b"HTTP/1.1 200") {
            match client.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => got.extend_from_slice(&buf[..n]),
            }
        }
    })
    .await
    .expect("a stalled body never gave back its permit: no new request served");
    assert!(
        got.starts_with(b"HTTP/1.1 200"),
        "a stalled body never gave back its permit: {got:?}"
    );
    drop(client);
    _server.shutdown().await;
}

/// `body_read_timeout = Duration::ZERO` passes the body through unwrapped:
/// a stalled body keeps its connection (no 200, no close), and completing
/// it later still gets 200.
#[tokio::test]
async fn a_zero_body_read_timeout_turns_the_guard_off() {
    let (_session, _server, addr, token) =
        body_server(|c| {
            c.body_read_timeout = Duration::ZERO;
            c.body_min_rate = 1000;
        })
        .await;
    let mut client = TcpStream::connect(addr).await.expect("connects");
    send_login_headers(&mut client, token, 1000).await;
    client
        .write_all(&[0u8; 10])
        .await
        .expect("10 body bytes written");
    // With the guard off the connection stays open: the read times out
    // rather than returning EOF or an error.
    let mut buf = [0u8; 4096];
    let pending = tokio::time::timeout(Duration::from_secs(1), client.read(&mut buf)).await;
    assert!(
        pending.is_err(),
        "the guard is off, so a stalled body closed its connection: {pending:?}"
    );
    // And completing the body still answers 200.
    client
        .write_all(&[0u8; 990])
        .await
        .expect("rest of the body written");
    let mut got = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !got.starts_with(b"HTTP/1.1 200") {
            match client.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => got.extend_from_slice(&buf[..n]),
            }
        }
    })
    .await
    .expect("the body completes once the client sends it");
    assert!(
        got.starts_with(b"HTTP/1.1 200"),
        "the body never completed: {got:?}"
    );
    drop(client);
    _server.shutdown().await;
}
