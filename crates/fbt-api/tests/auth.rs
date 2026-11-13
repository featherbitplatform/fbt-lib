//! Bearer auth, the `Auth::Disabled` guard, config validation, CORS and
//! server shutdown.

mod support;

use std::net::SocketAddr;

use bytes::Bytes;
use fbt_api::{ApiConfig, ApiError, Auth, router};
use http_body_util::Full;
use hyper::header::{AUTHORIZATION, HOST, ORIGIN, WWW_AUTHENTICATE};
use hyper::{Method, StatusCode};
use support::{api, api_with, error_code, get, json, request, send, session, test_config};

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
