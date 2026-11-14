//! Pins `fbt-api`'s public surface: removing or changing a public name
//! breaks this build.

#![allow(dead_code, unused_imports)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use fbt::Session;
use fbt_api::range::{RangeOutcome, parse_range};
use fbt_api::{
    ApiConfig, ApiError, ApiServer, Auth, MAX_BODY_BYTES, openapi, router, router_with_shutdown,
};

const _: usize = MAX_BODY_BYTES;

fn functions() {
    let _: fn(Session, &ApiConfig) -> Result<axum::Router, ApiError> = router;
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
    matches!(a, Auth::Token(_) | Auth::Disabled)
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
