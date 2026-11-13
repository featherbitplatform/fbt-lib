//! Pins `fbt-api`'s public surface: removing or changing a public name
//! breaks this build.

#![allow(dead_code, unused_imports)]

use std::net::SocketAddr;
use std::path::PathBuf;

use fbt::Session;
use fbt_api::{ApiConfig, ApiError, ApiServer, Auth, MAX_BODY_BYTES, openapi, router};

const _: usize = MAX_BODY_BYTES;

fn functions() {
    let _: fn(Session, &ApiConfig) -> Result<axum::Router, ApiError> = router;
    let _: fn() -> utoipa::openapi::OpenApi = openapi;
    let _: ApiConfig = ApiConfig::new(String::new());
    let _: ApiConfig = ApiConfig::new("token");
    let _: fn(&ApiServer) -> SocketAddr = ApiServer::local_addr;
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
}

fn variants(a: &Auth, e: &ApiError) -> bool {
    matches!(a, Auth::Token(_) | Auth::Disabled)
        && matches!(
            e,
            ApiError::EmptyToken
                | ApiError::InsecureBind
                | ApiError::InvalidOrigin
                | ApiError::InvalidRoot(_)
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
    assert!(matches!(&ApiConfig::new("t").auth, Auth::Token(t) if t == "t"));
}
