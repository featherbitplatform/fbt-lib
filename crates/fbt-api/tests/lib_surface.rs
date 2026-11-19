//! Pins `fbt-api`'s public surface: removing or changing a public name
//! breaks this build.

#![allow(dead_code, unused_imports)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::http::{HeaderMap, Method};
use fbt::Session;
use fbt_api::range::{RangeOutcome, parse_range};
use fbt_api::{
    ApiConfig, ApiError, ApiServer, Auth, MAX_BODY_BYTES, openapi, router, router_with,
    router_with_shutdown,
};
use fbt_api::{AuthRejection, AuthRequest, Authenticator};

const _: usize = MAX_BODY_BYTES;

fn functions() {
    let _: fn(Session, &ApiConfig) -> Result<axum::Router, ApiError> = router;
    let _: fn(Session, &ApiConfig, axum::Router) -> Result<axum::Router, ApiError> = router_with;
    let _: fn() -> utoipa::openapi::OpenApi = openapi;
    let _: ApiConfig = ApiConfig::new(String::new());
    let _: ApiConfig = ApiConfig::new("token");
    let _: fn(&ApiServer) -> SocketAddr = ApiServer::local_addr;
    let _: fn(&[u8], u64) -> RangeOutcome = parse_range;
}

fn range_variants(r: &RangeOutcome) -> bool {
    matches!(
        r,
        RangeOutcome::Full
            | RangeOutcome::Partial {
                start: _,
                end_inclusive: _
            }
            | RangeOutcome::Unsatisfiable
    )
}

fn mounted(session: Session, config: &ApiConfig) -> Result<axum::Router, ApiError> {
    router_with_shutdown(session, config, std::future::pending::<()>())
}

async fn server(session: Session, config: ApiConfig) -> Result<(), ApiError> {
    let server: ApiServer = ApiServer::start(session, config).await?;
    server.shutdown().await;
    Ok(())
}

/// [`ApiServer::start_with`] takes the embedder's own routes as a plain
/// `axum::Router`, served behind fbt-api's layers.
async fn server_with_extra(
    session: Session,
    config: ApiConfig,
    extra: axum::Router,
) -> Result<(), ApiError> {
    let server: ApiServer = ApiServer::start_with(session, config, extra).await?;
    server.shutdown().await;
    Ok(())
}

fn config_fields(c: &mut ApiConfig) {
    let _: &mut SocketAddr = &mut c.bind;
    let _: &mut Auth = &mut c.auth;
    let _: &mut Vec<String> = &mut c.cors_origins;
    let _: &mut Option<Vec<PathBuf>> = &mut c.allowed_roots;
    let _: &mut usize = &mut c.max_body_bytes;
    let _: &mut usize = &mut c.max_streams;
    let _: &mut usize = &mut c.max_event_streams;
    let _: &mut usize = &mut c.max_connections;
    let _: &mut Duration = &mut c.stream_idle_timeout;
}

fn variants(a: &Auth, e: &ApiError) -> bool {
    matches!(a, Auth::Token(_) | Auth::Disabled | Auth::Custom(_))
        && matches!(
            e,
            ApiError::EmptyToken
                | ApiError::InsecureBind
                | ApiError::InvalidOrigin
                | ApiError::InvalidRoot(_)
                | ApiError::InvalidLimits(..)
                | ApiError::NoRuntime
                | ApiError::Bind(_)
        )
}

/// The pluggable-authenticator surface: `Auth::Custom` carries an
/// `Arc<dyn Authenticator>`, the request exposes these fields, and every
/// rejection variant is one of these.
fn authenticator_surface(
    a: Arc<dyn Authenticator>,
    r: &AuthRequest<'_>,
    rej: &AuthRejection,
) -> Auth {
    let _: fn(&dyn Authenticator, &AuthRequest<'_>) -> Result<(), AuthRejection> =
        <dyn Authenticator>::authorize;
    let _: &Method = r.method;
    let _: Option<&str> = r.matched_path;
    let _: &HeaderMap = r.headers;
    let _: &str = r.path;
    let _: Option<&str> = r.query_token;
    let _: Option<SocketAddr> = r.peer;
    assert!(matches!(
        rej,
        AuthRejection::Unauthorized
            | AuthRejection::Forbidden
            | AuthRejection::TooManyRequests {
                retry_after_secs: _
            }
    ));
    Auth::Custom(a)
}

/// `AuthRequest` is non-exhaustive, so outside the crate its constructor is
/// the public way to build one; pin its signature.
fn auth_request_ctor<'a>(
    method: &'a Method,
    matched_path: Option<&'a str>,
    headers: &'a HeaderMap,
    query_token: Option<&'a str>,
    peer: Option<SocketAddr>,
) -> AuthRequest<'a> {
    AuthRequest::new(method, matched_path, headers, query_token, peer)
}

/// `with_path` overrides the path `new` defaulted to; pin its signature and
/// its effect.
#[test]
fn auth_request_with_path() {
    let method = Method::GET;
    let headers = HeaderMap::new();
    assert_eq!(
        AuthRequest::new(&method, Some("/x"), &headers, None, None).path,
        "/x"
    );
    assert_eq!(
        AuthRequest::new(&method, None, &headers, None, None)
            .with_path("/x")
            .path,
        "/x"
    );
}

#[test]
fn defaults() {
    let c = ApiConfig::default();
    assert_eq!(c.bind, SocketAddr::from(([127, 0, 0, 1], 8090)));
    assert!(matches!(&c.auth, Auth::Token(t) if t.is_empty()));
    assert!(c.cors_origins.is_empty());
    assert!(c.allowed_roots.is_none());
    assert_eq!(c.max_body_bytes, 32 << 20);
    assert_eq!(c.max_streams, 32);
    assert_eq!(c.max_event_streams, 16);
    assert_eq!(c.max_connections, 256);
    assert_eq!(c.stream_idle_timeout, Duration::from_secs(600));
    assert!(matches!(&ApiConfig::new("t").auth, Auth::Token(t) if t == "t"));
}
