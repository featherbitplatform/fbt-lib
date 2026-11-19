//! Bearer auth, the `Auth::Disabled` guard, the `Auth::Custom`
//! authenticator, config validation, CORS and server shutdown.

mod support;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::extract::{ConnectInfo, Request, State};
use bytes::Bytes;
use fbt::Session;
use fbt_api::{ApiConfig, ApiError, Auth, AuthRejection, AuthRequest, Authenticator, router};
use http_body_util::Full;
use hyper::header::{AUTHORIZATION, HOST, ORIGIN, WWW_AUTHENTICATE};
use hyper::{Method, StatusCode};
use support::{api, api_with, error_code, get, json, request, send, session, test_config};
use tower::ServiceExt as _;

fn empty() -> Full<Bytes> {
    Full::new(Bytes::new())
}

#[tokio::test]
async fn missing_token_is_401() {
    let (_session, _server, base, _token) = api().await;
    let (status, headers, body) = get(&format!("{base}/api/v1/session"), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(headers[WWW_AUTHENTICATE], "Bearer");
    assert_eq!(error_code(&body), "unauthorized");
    assert!(json(&body)["error"]["message"].is_string());
}

#[tokio::test]
async fn wrong_token_is_401() {
    let (_session, _server, base, token) = api().await;
    let url = format!("{base}/api/v1/session");
    // Same length, one byte off.
    let mut wrong = token.to_owned();
    wrong.pop();
    wrong.push('X');
    assert_eq!(wrong.len(), token.len());
    let (status, _, body) = get(&url, Some(&wrong)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(error_code(&body), "unauthorized");
    // Longer and shorter.
    let (status, _, _) = get(&url, Some(&format!("{token}x"))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _, _) = get(&url, Some(&token[..token.len() - 1])).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // Extra space after the scheme, and another scheme.
    for value in [
        format!("Bearer  {token}"),
        format!("Basic {token}"),
        token.to_owned(),
    ] {
        let req = request(Method::GET, &url, None)
            .header(AUTHORIZATION, value)
            .body(empty())
            .unwrap();
        let (status, _, _) = send(req).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
}

#[tokio::test]
async fn bearer_scheme_is_case_insensitive() {
    let (_session, _server, base, token) = api().await;
    for scheme in ["bearer", "BEARER", "Bearer"] {
        let req = request(Method::GET, &format!("{base}/api/v1/session"), None)
            .header(AUTHORIZATION, format!("{scheme} {token}"))
            .body(empty())
            .unwrap();
        let (status, _, body) = send(req).await;
        assert_eq!(status, StatusCode::OK, "scheme {scheme}");
        let info = json(&body);
        assert!(info["version"].is_string());
        assert!(info["listen_addrs"].is_array());
        assert!(!info["listen_addrs"].as_array().unwrap().is_empty());
        assert!(info["external_ip"].is_null());
        // DHT off in the test settings.
        assert!(info["dht_nodes"].is_null());
        assert_eq!(info["download_rate"], 0);
        assert_eq!(info["upload_rate"], 0);
        assert_eq!(info["torrents"], 0);
    }
}

#[tokio::test]
async fn token_query_only_on_streams() {
    let (_session, _server, base, token) = api().await;
    let (status, headers, body) = get(&format!("{base}/api/v1/session?token={token}"), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(headers[WWW_AUTHENTICATE], "Bearer");
    assert_eq!(error_code(&body), "unauthorized");
}

#[tokio::test]
async fn openapi_needs_no_token() {
    let (_session, _server, base, _token) = api().await;
    let (status, _, body) = get(&format!("{base}/api/v1/openapi.json"), None).await;
    assert_eq!(status, StatusCode::OK);
    let doc = json(&body);
    assert!(doc["paths"]["/api/v1/session"].is_object(), "{doc}");
    assert!(doc["paths"]["/api/v1/openapi.json"].is_object());
    // The document served is the one `openapi()` returns.
    let ours = serde_json::to_value(fbt_api::openapi()).unwrap();
    assert_eq!(doc, ours);
}

fn disabled(bind: &str) -> ApiConfig {
    let mut config = test_config();
    config.auth = Auth::Disabled;
    config.bind = bind.parse::<SocketAddr>().unwrap();
    config
}

#[tokio::test]
async fn disabled_auth_needs_loopback() {
    let session = session().await;
    let err = router(session.clone(), &disabled("0.0.0.0:8090")).unwrap_err();
    assert!(matches!(err, ApiError::InsecureBind), "{err:?}");
    let err = router(session.clone(), &disabled("[::]:8090")).unwrap_err();
    assert!(matches!(err, ApiError::InsecureBind), "{err:?}");
    let err = router(session.clone(), &disabled("[::ffff:10.0.0.1]:8090")).unwrap_err();
    assert!(matches!(err, ApiError::InsecureBind), "{err:?}");
    for bind in ["127.0.0.1:0", "[::1]:0", "[::ffff:127.0.0.1]:0"] {
        assert!(router(session.clone(), &disabled(bind)).is_ok(), "{bind}");
    }

    let (_session, _server, base, _token) = api_with(|c| c.auth = Auth::Disabled).await;
    let (status, _, body) = get(&format!("{base}/api/v1/session"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&body)["torrents"], 0);
}

#[tokio::test]
async fn disabled_auth_refuses_foreign_host_and_origin() {
    let (_session, server, base, _token) = api_with(|c| c.auth = Auth::Disabled).await;
    let url = format!("{base}/api/v1/session");
    let port = server.local_addr().port();

    let req = request(Method::GET, &url, None)
        .header(HOST, "evil.example")
        .body(empty())
        .unwrap();
    let (status, _, body) = send(req).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error_code(&body), "forbidden");

    let req = request(Method::GET, &url, None)
        .header(HOST, format!("evil.example:{port}"))
        .body(empty())
        .unwrap();
    let (status, _, _) = send(req).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let req = request(Method::GET, &url, None)
        .header(ORIGIN, "http://evil.example")
        .body(empty())
        .unwrap();
    let (status, _, body) = send(req).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error_code(&body), "forbidden");

    for host in [
        format!("localhost:{port}"),
        format!("LOCALHOST:{port}"),
        "localhost".to_owned(),
        format!("127.0.0.1:{port}"),
        format!("[::1]:{port}"),
    ] {
        let req = request(Method::GET, &url, None)
            .header(HOST, &host)
            .body(empty())
            .unwrap();
        let (status, _, _) = send(req).await;
        assert_eq!(status, StatusCode::OK, "host {host}");
    }
}

#[tokio::test]
async fn disabled_auth_accepts_a_listed_origin() {
    let (_session, _server, base, _token) = api_with(|c| {
        c.auth = Auth::Disabled;
        c.cors_origins = vec!["http://ui.example".to_owned()];
    })
    .await;
    let req = request(Method::GET, &format!("{base}/api/v1/session"), None)
        .header(ORIGIN, "http://ui.example")
        .body(empty())
        .unwrap();
    let (status, headers, _) = send(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["access-control-allow-origin"], "http://ui.example");
}

/// `GET /api/v1/session` on an `Auth::Disabled` router with `peer` injected
/// as axum's `ConnectInfo` (what `ApiServer` attaches to every request). A
/// unit-level injection: a real non-loopback peer cannot exist on a
/// loopback-only CI runner. The `Host` stays `localhost` throughout, so any
/// refusal here is the peer check, not the Host guard.
async fn disabled_from_peer(session: &Session, peer: &str) -> (StatusCode, Bytes) {
    let mut config = test_config();
    config.auth = Auth::Disabled;
    let app = router(session.clone(), &config).expect("router").layer(
        axum::middleware::from_fn_with_state(
            ConnectInfo(peer.parse::<SocketAddr>().expect("peer address")),
            |State(info): State<ConnectInfo<SocketAddr>>,
             mut req: Request,
             next: axum::middleware::Next| async move {
                let _ = req.extensions_mut().insert(info);
                next.run(req).await
            },
        ),
    );
    let req = Request::get("/api/v1/session")
        .header(HOST, "localhost")
        .body(Body::empty())
        .expect("request builds");
    let res = app.oneshot(req).await.expect("router is infallible");
    let (parts, body) = res.into_parts();
    (
        parts.status,
        to_bytes(body, 1 << 20).await.expect("body reads"),
    )
}

#[tokio::test]
async fn disabled_auth_refuses_remote_peer() {
    let session = session().await;
    for peer in [
        "203.0.113.7:52000",
        "[2001:db8::7]:1",
        "[::ffff:10.0.0.1]:1",
    ] {
        let (status, body) = disabled_from_peer(&session, peer).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{peer}");
        assert_eq!(error_code(&body), "forbidden", "{peer}");
    }
    // A loopback peer — mapped forms included — passes the guard.
    for peer in ["127.0.0.1:52000", "[::1]:1", "[::ffff:127.0.0.1]:1"] {
        let (status, _) = disabled_from_peer(&session, peer).await;
        assert_eq!(status, StatusCode::OK, "{peer}");
    }
}

#[tokio::test]
async fn empty_token_is_refused() {
    let session = session().await;
    let err = router(session.clone(), &ApiConfig::new("")).unwrap_err();
    assert!(matches!(err, ApiError::EmptyToken), "{err:?}");
    // The default config has no token either.
    let err = router(session.clone(), &ApiConfig::default()).unwrap_err();
    assert!(matches!(err, ApiError::EmptyToken), "{err:?}");
    let mut config = test_config();
    config.auth = Auth::Token(String::new());
    let err = fbt_api::ApiServer::start(session, config)
        .await
        .unwrap_err();
    assert!(matches!(err, ApiError::EmptyToken), "{err:?}");
}

#[tokio::test]
async fn connection_cap_must_exceed_stream_caps() {
    let session = session().await;
    // 40 connections cannot cover 32 file streams plus 16 event streams:
    // every stream holder must leave a slot for a client that holds its
    // connection without taking a stream, or the `503` answer to an
    // eleventh kind of client could never be served.
    let mut config = test_config();
    config.max_connections = 40;
    config.max_streams = 32;
    config.max_event_streams = 16;
    let err = router(session.clone(), &config).unwrap_err();
    assert!(matches!(err, ApiError::InvalidLimits(..)), "{err:?}");
    // Exactly the sum is not more than it.
    config.max_connections = 48;
    let err = router(session.clone(), &config).unwrap_err();
    assert!(matches!(err, ApiError::InvalidLimits(..)), "{err:?}");
    // One above the sum serves, and so do the defaults (256 > 32 + 16).
    config.max_connections = 49;
    assert!(router(session.clone(), &config).is_ok());
    assert!(router(session.clone(), &test_config()).is_ok());
}

#[tokio::test]
async fn invalid_origin_is_refused() {
    let session = session().await;
    let mut config = test_config();
    config.cors_origins = vec!["http://ui.example\n".to_owned()];
    let err = router(session, &config).unwrap_err();
    assert!(matches!(err, ApiError::InvalidOrigin), "{err:?}");
}

#[tokio::test]
async fn missing_root_is_refused() {
    let session = session().await;
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing");
    let mut config = test_config();
    config.allowed_roots = Some(vec![dir.path().to_path_buf(), missing.clone()]);
    let err = router(session.clone(), &config).unwrap_err();
    match err {
        ApiError::InvalidRoot(path) => assert_eq!(path, missing),
        other => panic!("expected InvalidRoot, got {other:?}"),
    }
    config.allowed_roots = Some(vec![dir.path().to_path_buf()]);
    assert!(router(session, &config).is_ok());
}

fn preflight(url: &str, origin: &str) -> hyper::Request<Full<Bytes>> {
    request(Method::OPTIONS, url, None)
        .header(ORIGIN, origin)
        .header("access-control-request-method", "GET")
        .header("access-control-request-headers", "authorization")
        .body(empty())
        .unwrap()
}

#[tokio::test]
async fn cors_preflight_without_token() {
    let (_session, _server, base, token) =
        api_with(|c| c.cors_origins = vec!["http://ui.example".to_owned()]).await;
    let url = format!("{base}/api/v1/session");

    let (status, headers, _) = send(preflight(&url, "http://ui.example")).await;
    assert!(status.is_success(), "{status}");
    assert_eq!(headers["access-control-allow-origin"], "http://ui.example");
    let allowed = headers["access-control-allow-headers"]
        .to_str()
        .unwrap()
        .to_ascii_lowercase();
    assert!(allowed.contains("authorization"), "{allowed}");

    let (_, headers, _) = send(preflight(&url, "http://other.example")).await;
    assert!(!headers.contains_key("access-control-allow-origin"));

    // A 401 carries the CORS headers too (CORS is the outermost layer).
    let req = request(Method::GET, &url, None)
        .header(ORIGIN, "http://ui.example")
        .body(empty())
        .unwrap();
    let (status, headers, _) = send(req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(headers["access-control-allow-origin"], "http://ui.example");

    let req = request(Method::GET, &url, Some(token))
        .header(ORIGIN, "http://ui.example")
        .body(empty())
        .unwrap();
    let (status, headers, _) = send(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["access-control-allow-origin"], "http://ui.example");

    // CORS off: no CORS header at all.
    let (_session, _server, base, _token) = api().await;
    let (_, headers, _) = send(preflight(
        &format!("{base}/api/v1/session"),
        "http://ui.example",
    ))
    .await;
    assert!(
        !headers
            .keys()
            .any(|k| k.as_str().starts_with("access-control-")),
        "{headers:?}"
    );
}

#[tokio::test]
async fn shutdown_stops_serving() {
    let (_session, server, base, token) = api().await;
    let addr = server.local_addr();
    let (status, _, _) = get(&format!("{base}/api/v1/session"), Some(token)).await;
    assert_eq!(status, StatusCode::OK);
    tokio::time::timeout(std::time::Duration::from_secs(6), server.shutdown())
        .await
        .expect("shutdown returns within 6 s");
    assert!(tokio::net::TcpStream::connect(addr).await.is_err());
}

/// `router_with_shutdown` returns its error instead of panicking when no
/// runtime exists to spawn the shutdown waiter on. The session is built in a
/// temporary runtime (it needs one) and that runtime is dropped before the
/// call, so the call itself runs with no runtime in the current thread.
#[test]
fn router_with_shutdown_outside_runtime_errors() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let session = rt.block_on(support::session());
    drop(rt);
    let err = fbt_api::router_with_shutdown(
        session,
        &support::test_config(),
        std::future::pending::<()>(),
    )
    .expect_err("no runtime, so no task to spawn the shutdown waiter on");
    assert!(matches!(err, ApiError::NoRuntime), "{err:?}");
}

#[tokio::test]
async fn options_without_cors_needs_token() {
    let (_session, _server, base, _token) = api().await;
    let (status, headers, body) = send(preflight(
        &format!("{base}/api/v1/session"),
        "http://ui.example",
    ))
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(headers[WWW_AUTHENTICATE], "Bearer");
    assert_eq!(error_code(&body), "unauthorized");
}

#[tokio::test]
async fn unknown_path_and_wrong_method_need_token() {
    let (_session, _server, base, _token) = api().await;
    let (status, _, body) = get(&format!("{base}/api/v1/nope"), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(error_code(&body), "unauthorized");
    let (status, _, body) = support::post_json(
        &format!("{base}/api/v1/session"),
        None,
        &serde_json::json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(error_code(&body), "unauthorized");
}

/// Every route in the OpenAPI document but the document itself answers
/// `401` without a token, whatever its method: a route merged outside the
/// auth layer fails here.
#[tokio::test]
async fn every_documented_route_needs_the_token() {
    let (_session, server, base, _token) = api().await;
    let doc = serde_json::to_value(fbt_api::openapi()).expect("document");
    let paths = doc["paths"].as_object().expect("paths");
    let mut checked = 0;
    for (path, item) in paths {
        let concrete = path
            .replace("{id}", &"ab".repeat(20))
            .replace("{index}", "0");
        for method in item.as_object().expect("path item").keys() {
            if path == "/api/v1/openapi.json" {
                continue;
            }
            let method = Method::from_bytes(method.to_uppercase().as_bytes()).expect("method");
            let req = request(method.clone(), &format!("{base}{concrete}"), None)
                .body(empty())
                .expect("request builds");
            let (status, _, _) = send(req).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {path}");
            checked += 1;
        }
    }
    assert!(checked >= 30, "{checked} operations checked");
    server.shutdown().await;
}

/// What a [`Recorder`] last saw, as owned values.
#[derive(Clone, Debug)]
struct Seen {
    method: String,
    matched_path: Option<String>,
    path: String,
    query_token: Option<String>,
    peer: Option<SocketAddr>,
}

/// An `Authenticator` that allows everything and records what it saw.
#[derive(Debug, Default)]
struct Recorder(std::sync::Mutex<Option<Seen>>);

impl Authenticator for Recorder {
    fn authorize(&self, request: &AuthRequest<'_>) -> Result<(), AuthRejection> {
        let seen = Seen {
            method: request.method.as_str().to_owned(),
            matched_path: request.matched_path.map(str::to_owned),
            path: request.path.to_owned(),
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

/// Allows only requests carrying `x-test: ok`, rejecting the rest as
/// unauthenticated.
#[derive(Debug)]
struct HeaderGate;

impl Authenticator for HeaderGate {
    fn authorize(&self, request: &AuthRequest<'_>) -> Result<(), AuthRejection> {
        match request.headers.get("x-test") {
            Some(value) if value == "ok" => Ok(()),
            _ => Err(AuthRejection::Unauthorized),
        }
    }
}

/// Rejects everything as forbidden.
#[derive(Debug)]
struct ForbidAll;

impl Authenticator for ForbidAll {
    fn authorize(&self, _request: &AuthRequest<'_>) -> Result<(), AuthRejection> {
        Err(AuthRejection::Forbidden)
    }
}

/// Rejects everything as rate limited, asking for a 7 s retry.
#[derive(Debug)]
struct RateLimited;

impl Authenticator for RateLimited {
    fn authorize(&self, _request: &AuthRequest<'_>) -> Result<(), AuthRejection> {
        Err(AuthRejection::TooManyRequests {
            retry_after_secs: 7,
        })
    }
}

/// Panics on its first call, then allows everything.
#[derive(Debug, Default)]
struct PanicOnce(std::sync::atomic::AtomicBool);

impl Authenticator for PanicOnce {
    fn authorize(&self, _request: &AuthRequest<'_>) -> Result<(), AuthRejection> {
        if self.0.swap(true, std::sync::atomic::Ordering::Relaxed) {
            Ok(())
        } else {
            panic!("authenticator exploded");
        }
    }
}

/// Rejects everything and counts how often it was asked.
#[derive(Debug)]
struct CountingReject(std::sync::atomic::AtomicUsize);

impl Authenticator for CountingReject {
    fn authorize(&self, _request: &AuthRequest<'_>) -> Result<(), AuthRejection> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Err(AuthRejection::Unauthorized)
    }
}

/// `router` with `Auth::Custom(auth)` and a fixed loopback peer injected as
/// axum's `ConnectInfo` (what `ApiServer` attaches to every request). The
/// middleware is added with `.layer`, so it runs ahead of the auth layer and
/// the extension is visible there — the same unit-level injection
/// [`disabled_from_peer`] uses.
fn custom_router(session: &Session, auth: Arc<dyn Authenticator>) -> axum::Router {
    let peer = "127.0.0.1:52000"
        .parse::<SocketAddr>()
        .expect("peer address");
    let mut config = test_config();
    config.auth = Auth::Custom(auth);
    router(session.clone(), &config)
        .expect("router")
        .layer(axum::middleware::from_fn(
            move |mut req: Request, next: axum::middleware::Next| async move {
                let _ = req.extensions_mut().insert(ConnectInfo(peer));
                next.run(req).await
            },
        ))
}

/// Sends `req` through `app` and reads status, headers and the whole body.
async fn call(app: &axum::Router, req: Request) -> (StatusCode, axum::http::HeaderMap, Bytes) {
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

/// [`call`]'s status only: for the event stream, whose body never ends and
/// must be dropped unread.
async fn call_status(app: &axum::Router, req: Request) -> StatusCode {
    let res = app
        .clone()
        .oneshot(req)
        .await
        .expect("router is infallible");
    let (parts, _stream) = res.into_parts();
    parts.status
}

fn get_req(uri: &str) -> Request {
    Request::get(uri)
        .body(Body::empty())
        .expect("request builds")
}

#[tokio::test]
async fn custom_allows_and_rejects() {
    let session = session().await;
    let app = custom_router(&session, Arc::new(HeaderGate));

    let req = Request::get("/api/v1/session")
        .header("x-test", "ok")
        .body(Body::empty())
        .expect("request builds");
    let (status, _, body) = call(&app, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&body)["torrents"], 0);

    // The rejection's status and body, `WWW-Authenticate` included.
    let (status, headers, body) = call(&app, get_req("/api/v1/session")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(headers["www-authenticate"], "Bearer");
    assert_eq!(error_code(&body), "unauthorized");

    // With `Token`, `openapi.json` is exempt; with `Custom` the embedder's
    // authenticator decides for it too.
    let (status, _, _) = call(&app, get_req("/api/v1/openapi.json")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let req = Request::get("/api/v1/openapi.json")
        .header("x-test", "ok")
        .body(Body::empty())
        .expect("request builds");
    let (status, _, body) = call(&app, req).await;
    assert_eq!(status, StatusCode::OK);
    assert!(json(&body)["paths"]["/api/v1/session"].is_object());
}

#[tokio::test]
async fn custom_sees_peer_and_matched_path() {
    let session = session().await;
    let recorder = Arc::new(Recorder::default());
    let app = custom_router(&session, recorder.clone());
    let (status, _, body) = call(&app, get_req("/api/v1/torrents")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(json(&body).as_array().expect("list").is_empty());

    let seen = recorder.seen();
    assert_eq!(seen.method, "GET");
    assert_eq!(seen.matched_path.as_deref(), Some("/api/v1/torrents"));
    assert_eq!(seen.query_token, None);
    assert_eq!(
        seen.peer.map(|peer| peer.ip().to_string()),
        Some("127.0.0.1".to_owned())
    );
}

#[tokio::test]
async fn custom_authenticator_sees_request_path() {
    let session = session().await;
    let recorder = Arc::new(Recorder::default());
    let app = custom_router(&session, recorder.clone());
    let (status, _, _) = call(
        &app,
        get_req(&format!(
            "/api/v1/torrents/{}/files/0/stream?token=x",
            "ab".repeat(20)
        )),
    )
    .await;
    // An unknown id answers 404 from the handler behind the authenticator.
    assert_eq!(status, StatusCode::NOT_FOUND);

    let seen = recorder.seen();
    assert_eq!(
        seen.path,
        format!("/api/v1/torrents/{}/files/0/stream", "ab".repeat(20))
    );
    assert_eq!(
        seen.matched_path.as_deref(),
        Some("/api/v1/torrents/{id}/files/{index}/stream")
    );
}

#[tokio::test]
async fn path_is_percent_encoded_as_sent() {
    let session = session().await;
    let recorder = Arc::new(Recorder::default());
    let app = custom_router(&session, recorder.clone());
    let (status, _, _) = call(&app, get_req("/api/v1/torrents/%41/files/0/stream")).await;
    // `%41` decodes to `A`, an id that is not 40 hex characters: the handler
    // behind the authenticator answers 400, which proves the request got here.
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let seen = recorder.seen();
    assert!(seen.path.contains("%41"), "{}", seen.path);
}

/// `Router::nest` strips the mount prefix before the nested router's layers
/// run, so `path` is what is below the mount point (while `matched_path`
/// keeps the prefix); a parameter in the prefix would be invisible there.
/// The router is fbt-api's own, from `router`, nested under `/prefix`.
#[tokio::test]
async fn path_below_a_nest_is_stripped() {
    let session = session().await;
    let recorder = Arc::new(Recorder::default());
    let peer = "127.0.0.1:52000"
        .parse::<SocketAddr>()
        .expect("peer address");
    let mut config = test_config();
    config.auth = Auth::Custom(recorder.clone());
    let app = axum::Router::new().nest(
        "/prefix",
        router(session, &config)
            .expect("router")
            .layer(axum::middleware::from_fn(
                move |mut req: Request, next: axum::middleware::Next| async move {
                    let _ = req.extensions_mut().insert(ConnectInfo(peer));
                    next.run(req).await
                },
            )),
    );
    let (status, _, _) = call(
        &app,
        get_req(&format!(
            "/prefix/api/v1/torrents/{}/files/0/stream?token=x",
            "ab".repeat(20)
        )),
    )
    .await;
    // An unknown id answers 404 from the handler behind the authenticator.
    assert_eq!(status, StatusCode::NOT_FOUND);

    let seen = recorder.seen();
    assert_eq!(
        seen.path,
        format!("/api/v1/torrents/{}/files/0/stream", "ab".repeat(20))
    );
    assert_eq!(
        seen.matched_path.as_deref(),
        Some("/prefix/api/v1/torrents/{id}/files/{index}/stream")
    );
}

#[tokio::test]
async fn query_token_only_on_stream_and_events() {
    let session = session().await;
    let recorder = Arc::new(Recorder::default());
    let app = custom_router(&session, recorder.clone());

    // The event stream sees the percent-decoded token (its body never ends:
    // read only the status).
    let status = call_status(&app, get_req("/api/v1/events?token=a%2Bb+c")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(recorder.seen().query_token.as_deref(), Some("a+b c"));

    // The file stream too — an unknown id still proves the authenticator ran,
    // since the 404 comes from the handler behind it.
    let stream = format!(
        "/api/v1/torrents/{}/files/0/stream?token=abc",
        "ab".repeat(20)
    );
    let (status, _, body) = call(&app, get_req(&stream)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(error_code(&body), "not_found");
    assert_eq!(recorder.seen().query_token.as_deref(), Some("abc"));

    // Every other route ignores the parameter.
    let (status, _, _) = call(&app, get_req("/api/v1/session?token=abc")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(recorder.seen().query_token, None);

    // The cap is 4096 decoded bytes: exactly that reaches the authenticator,
    // one byte more does not.
    let status = call_status(
        &app,
        get_req(&format!("/api/v1/events?token={}", "a".repeat(4096))),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        recorder.seen().query_token.as_deref().map(str::len),
        Some(4096)
    );
    let status = call_status(
        &app,
        get_req(&format!("/api/v1/events?token={}", "a".repeat(4097))),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(recorder.seen().query_token, None);
}

#[tokio::test]
async fn custom_forbidden_and_429() {
    let session = session().await;
    let app = custom_router(&session, Arc::new(ForbidAll));
    let (status, _, body) = call(&app, get_req("/api/v1/session")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error_code(&body), "forbidden");

    let app = custom_router(&session, Arc::new(RateLimited));
    let (status, headers, body) = call(&app, get_req("/api/v1/session")).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(headers["retry-after"], "7");
    assert_eq!(error_code(&body), "too_many_requests");
}

#[tokio::test]
async fn custom_panic_is_500() {
    let session = session().await;
    let app = custom_router(&session, Arc::new(PanicOnce::default()));

    let (status, _, body) = call(&app, get_req("/api/v1/session")).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(error_code(&body), "internal");
    // The standard body: the panic's own message does not leak to the client.
    let text = String::from_utf8(body.to_vec()).expect("body is UTF-8");
    assert!(!text.contains("exploded"), "{text}");

    // The connection and the server keep working: the next request is served.
    let (status, _, body) = call(&app, get_req("/api/v1/session")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&body)["torrents"], 0);
}

#[tokio::test]
async fn custom_cors_preflight_skips_authenticator() {
    let session = session().await;
    let gate = Arc::new(CountingReject(std::sync::atomic::AtomicUsize::new(0)));
    let mut config = test_config();
    config.auth = Auth::Custom(gate.clone());
    config.cors_origins = vec!["http://ui.example".to_owned()];
    let app = router(session, &config).expect("router");

    // The CORS layer, outside auth, answers the preflight itself: the
    // authenticator is never asked.
    let req = Request::builder()
        .method(Method::OPTIONS)
        .uri("/api/v1/session")
        .header("origin", "http://ui.example")
        .header("access-control-request-method", "GET")
        .header("access-control-request-headers", "authorization")
        .body(Body::empty())
        .expect("request builds");
    let (status, headers, _) = call(&app, req).await;
    assert!(status.is_success(), "{status}");
    assert_eq!(headers["access-control-allow-origin"], "http://ui.example");
    assert_eq!(gate.0.load(std::sync::atomic::Ordering::Relaxed), 0);

    // A plain request is not a preflight and does reach the authenticator —
    // so the counter counts; it just never saw the preflight.
    let (status, _, _) = call(&app, get_req("/api/v1/session")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(gate.0.load(std::sync::atomic::Ordering::Relaxed), 1);
}

/// Embedders unit-test their authenticator: `AuthRequest::new` builds what
/// the auth layer would hand it, no server involved (the struct itself is
/// non-exhaustive, so outside fbt-api this constructor is how one builds it).
#[test]
fn custom_authenticator_is_callable_directly() {
    let recorder = Recorder::default();
    let method = Method::GET;
    let headers = axum::http::HeaderMap::new();
    let peer = "203.0.113.7:52000"
        .parse::<SocketAddr>()
        .expect("peer address");
    let request = AuthRequest::new(
        &method,
        Some("/api/v1/torrents"),
        &headers,
        Some("tok"),
        Some(peer),
    );
    assert!(recorder.authorize(&request).is_ok());
    let seen = recorder.seen();
    assert_eq!(seen.method, "GET");
    assert_eq!(seen.matched_path.as_deref(), Some("/api/v1/torrents"));
    assert_eq!(seen.query_token.as_deref(), Some("tok"));
    assert_eq!(seen.peer, Some(peer));

    // Rejections come back the same way.
    let request = AuthRequest::new(&method, None, &headers, None, None);
    assert!(matches!(
        HeaderGate.authorize(&request),
        Err(AuthRejection::Unauthorized)
    ));
}
