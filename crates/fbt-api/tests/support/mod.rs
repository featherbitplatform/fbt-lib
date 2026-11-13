//! Shared helpers for the `fbt-api` integration tests: a loopback session,
//! a served API and a raw hyper client (there is no reqwest).

#![allow(dead_code, reason = "each test binary uses a different subset")]

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use fbt::{Session, Settings};
use fbt_api::{ApiConfig, ApiServer};
use http_body_util::{BodyExt, Full};
use hyper::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use hyper::{Method, Request, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

/// The bearer token every [`api`] server expects.
pub const TOKEN: &str = "test-token-0123456789";

/// How long a test waits for one request.
pub const T: Duration = Duration::from_secs(30);

/// Settings for tests: loopback listener on a free port, fast status
/// updates, and no DHT, LSD, UPnP or NAT-PMP, so no test contacts the
/// public internet or a real router. No queue limits.
pub fn test_settings() -> Settings {
    let mut s = Settings::default();
    s.listen_addrs = vec![SocketAddr::from(([127, 0, 0, 1], 0))];
    s.status_interval = Duration::from_millis(50);
    s.dht_enabled = false;
    s.dht_bootstrap_nodes = Vec::new();
    s.enable_lsd = false;
    s.enable_upnp = false;
    s.enable_natpmp = false;
    s.queue.active_downloads = None;
    s.queue.active_seeds = None;
    s.queue.active_limit = None;
    s
}

/// A session from [`test_settings`].
pub async fn session() -> Session {
    Session::new(test_settings()).await.expect("session starts")
}

/// The config every [`api`] server starts from: token [`TOKEN`], bind
/// `127.0.0.1:0`.
pub fn test_config() -> ApiConfig {
    let mut config = ApiConfig::new(TOKEN);
    config.bind = SocketAddr::from(([127, 0, 0, 1], 0));
    config
}

/// A session and an API server over it with [`test_config`]. Returns the
/// session, the server, the base url (`http://127.0.0.1:<port>`, no
/// trailing slash) and the token.
pub async fn api() -> (Session, ApiServer, String, &'static str) {
    api_with(|_| {}).await
}

/// [`api`] with `edit` applied to [`test_config`] first.
pub async fn api_with(
    edit: impl FnOnce(&mut ApiConfig),
) -> (Session, ApiServer, String, &'static str) {
    let session = session().await;
    let mut config = test_config();
    edit(&mut config);
    let server = ApiServer::start(session.clone(), config)
        .await
        .expect("api server starts");
    let base = format!("http://{}", server.local_addr());
    (session, server, base, TOKEN)
}

/// A request to `url` with `Authorization: Bearer <token>` when a token is
/// given.
pub fn request(method: Method, url: &str, token: Option<&str>) -> hyper::http::request::Builder {
    let mut b = Request::builder().method(method).uri(url);
    if let Some(token) = token {
        b = b.header(AUTHORIZATION, format!("Bearer {token}"));
    }
    b
}

/// Sends `req` on a fresh client and reads the whole response.
pub async fn send(req: Request<Full<Bytes>>) -> (StatusCode, HeaderMap, Bytes) {
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let res = tokio::time::timeout(T, client.request(req))
        .await
        .expect("response within the timeout")
        .expect("request succeeds");
    let (parts, body) = res.into_parts();
    let body = tokio::time::timeout(T, body.collect())
        .await
        .expect("body within the timeout")
        .expect("body reads")
        .to_bytes();
    (parts.status, parts.headers, body)
}

/// `GET url`, with the bearer token when one is given.
pub async fn get(url: &str, token: Option<&str>) -> (StatusCode, HeaderMap, Bytes) {
    let req = request(Method::GET, url, token)
        .body(Full::new(Bytes::new()))
        .expect("request builds");
    send(req).await
}

/// `POST url` with `body` as JSON, with the bearer token when one is given.
pub async fn post_json(
    url: &str,
    token: Option<&str>,
    body: &serde_json::Value,
) -> (StatusCode, HeaderMap, Bytes) {
    let req = request(Method::POST, url, token)
        .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
        .body(Full::new(Bytes::from(body.to_string())))
        .expect("request builds");
    send(req).await
}

/// The body as JSON.
pub fn json(body: &Bytes) -> serde_json::Value {
    serde_json::from_slice(body).expect("body is JSON")
}

/// The error code of an `{"error": {"code": ..}}` body.
pub fn error_code(body: &Bytes) -> String {
    json(body)["error"]["code"]
        .as_str()
        .expect("error body has a code")
        .to_owned()
}
