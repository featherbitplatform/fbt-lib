//! The auth layer: bearer token (header, or `?token=` on the streaming
//! and event routes), the exempt routes, the [`Auth::Disabled`]
//! loopback-peer, Host and Origin guard, and [`Auth::Custom`], which hands
//! every request to the embedder's own [`Authenticator`].
//!
//! It is added with `Router::layer`, so each matched route's
//! [`MatchedPath`] is set when it runs. Routes are recognised by the
//! suffix of their matched path, so a client that `nest`s the router
//! under a prefix keeps the same rules. A request with no matched path (a
//! 404) needs the header.
//!
//! CORS preflights never get here when CORS is on: the CORS layer, outside
//! this one, answers them itself. With CORS off an `OPTIONS` request is
//! like any other and authenticated as such.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{ConnectInfo, MatchedPath, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::config::{Auth, is_loopback};
use crate::error::ApiErrorResponse;

/// Routes that need no token, as `(method, matched path suffix)`. Only
/// [`Auth::Token`] consults them: [`Auth::Custom`] sees every request, this
/// document included, and the embedder decides.
const EXEMPT: &[(Method, &str)] = &[(Method::GET, "/api/v1/openapi.json")];

/// Routes that also take `?token=`, as `(method, matched path suffix)`:
/// the file streams (browser media elements) and the read-only event
/// stream (`EventSource`), which cannot send headers. Every other route
/// ignores the query parameter.
const QUERY_TOKEN: &[(Method, &str)] = &[
    (Method::GET, "/api/v1/torrents/{id}/files/{index}/stream"),
    (Method::HEAD, "/api/v1/torrents/{id}/files/{index}/stream"),
    (Method::GET, "/api/v1/events"),
];

/// Longest `?token=` value decoded; a longer one fails.
const MAX_QUERY_TOKEN: usize = 4096;

/// A pluggable authenticator for [`Auth::Custom`]: it decides, for every
/// request — `openapi.json` included, CORS preflights still excepted —
/// whether the client may proceed.
///
/// The check is synchronous and runs on the connection's task inside
/// `std::panic::catch_unwind`: keep it cheap (session tokens in memory;
/// anything slower belongs in a login route of your own), and a panic gets
/// that request a `500` while the server keeps serving.
pub trait Authenticator: Send + Sync + std::fmt::Debug + 'static {
    /// Allows the request (`Ok(())`) or rejects it with its [`AuthRejection`].
    fn authorize(&self, request: &AuthRequest<'_>) -> Result<(), AuthRejection>;
}

/// What an [`Authenticator`] gets from a request, built by the auth layer for
/// every request handed to [`Auth::Custom`].
#[derive(Debug)]
pub struct AuthRequest<'a> {
    /// The request's method.
    pub method: &'a Method,
    /// The matched route template (`/api/v1/torrents/{id}`), with the mount
    /// prefix when the router is nested; `None` when no route matched (a 404).
    pub matched_path: Option<&'a str>,
    /// The request headers, `Authorization` included when the client sent one.
    pub headers: &'a HeaderMap,
    /// The percent-decoded `?token=` value, offered only on the routes that
    /// take a query token today — the file streams and the event stream —
    /// and only when it decodes to valid UTF-8 of at most 4096 bytes; `None`
    /// elsewhere, when absent or malformed, and when longer.
    pub query_token: Option<&'a str>,
    /// The peer address, from axum's `ConnectInfo` when the server attaches
    /// it ([`ApiServer`](crate::ApiServer) does for every request; a router
    /// mounted elsewhere gets what that server provides).
    pub peer: Option<SocketAddr>,
}

/// How an [`Authenticator`] rejects a request. The auth layer answers each
/// variant with the crate's standard error body and the status and headers
/// named here.
#[derive(Debug)]
pub enum AuthRejection {
    /// `401`, exactly like a wrong bearer token today: the same body and
    /// `WWW-Authenticate: Bearer`.
    Unauthorized,
    /// `403 forbidden`.
    Forbidden,
    /// `429 too_many_requests` with `Retry-After: <retry_after_secs>`.
    TooManyRequests {
        /// How long the client should wait before retrying.
        retry_after_secs: u64,
    },
}

/// What the auth layer needs from the config.
#[derive(Debug, Clone)]
pub(crate) struct AuthState {
    /// How clients authenticate.
    pub(crate) auth: Auth,
    /// [`ApiConfig::cors_origins`](crate::ApiConfig::cors_origins), the
    /// origins [`Auth::Disabled`] accepts.
    pub(crate) origins: Arc<[HeaderValue]>,
}

/// The auth middleware (`axum::middleware::from_fn_with_state`).
pub(crate) async fn layer(State(state): State<AuthState>, req: Request, next: Next) -> Response {
    match check(&state, &req) {
        Ok(()) => next.run(req).await,
        Err(e) => e.into_response(),
    }
}

fn check(state: &AuthState, req: &Request) -> Result<(), ApiErrorResponse> {
    let token = match &state.auth {
        Auth::Disabled => return guard_disabled(&state.origins, req),
        Auth::Token(token) => token.as_bytes(),
        Auth::Custom(authenticator) => return check_custom(authenticator, req),
    };
    let matched = req
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str);
    if route_in(EXEMPT, req.method(), matched) {
        return Ok(());
    }
    if let Some(given) = bearer(req.headers())
        && ct_eq(given, token)
    {
        return Ok(());
    }
    if route_in(QUERY_TOKEN, req.method(), matched)
        && let Some(given) = req.uri().query().and_then(query_token)
        && ct_eq(&given, token)
    {
        return Ok(());
    }
    Err(ApiErrorResponse::unauthorized())
}

/// [`Auth::Custom`]: hand the request to the embedder's authenticator and
/// map its rejection to a response. Every request gets here — `openapi.json`
/// included; only CORS preflights are answered earlier, by the CORS layer.
/// A panicking authenticator cannot take the connection or the server down:
/// the panic is caught and that request gets a `500`.
fn check_custom(
    authenticator: &Arc<dyn Authenticator>,
    req: &Request,
) -> Result<(), ApiErrorResponse> {
    let matched = req
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str);
    // The query token only on the routes that take it today; a value that
    // is not valid UTF-8 (or longer than `MAX_QUERY_TOKEN`, which decoding
    // already refuses) is as good as absent for a `&str` API.
    let query = match route_in(QUERY_TOKEN, req.method(), matched) {
        true => req
            .uri()
            .query()
            .and_then(query_token)
            .and_then(|bytes| String::from_utf8(bytes).ok()),
        false => None,
    };
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|&ConnectInfo(peer)| peer);
    let request = AuthRequest {
        method: req.method(),
        matched_path: matched,
        headers: req.headers(),
        query_token: query.as_deref(),
        peer,
    };
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        authenticator.authorize(&request)
    })) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(rejection)) => Err(match rejection {
            AuthRejection::Unauthorized => ApiErrorResponse::unauthorized(),
            AuthRejection::Forbidden => {
                ApiErrorResponse::forbidden("the authenticator rejected this request")
            }
            AuthRejection::TooManyRequests { retry_after_secs } => {
                ApiErrorResponse::too_many_requests(retry_after_secs)
            }
        }),
        Err(payload) => {
            let detail = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("no panic message");
            tracing::error!(
                method = %req.method(),
                path = ?matched,
                "the API authenticator panicked: {detail}"
            );
            Err(ApiErrorResponse::internal("the authenticator panicked"))
        }
    }
}

fn route_in(routes: &[(Method, &str)], method: &Method, matched: Option<&str>) -> bool {
    matched.is_some_and(|path| {
        routes
            .iter()
            .any(|(m, suffix)| m == method && path.ends_with(suffix))
    })
}

/// The token of `Authorization: Bearer <token>`: the scheme in any case,
/// then exactly one space.
fn bearer(headers: &HeaderMap) -> Option<&[u8]> {
    let value = headers.get(header::AUTHORIZATION)?.as_bytes();
    let (scheme, rest) = value.split_at_checked(6)?;
    let token = rest.strip_prefix(b" ")?;
    scheme.eq_ignore_ascii_case(b"bearer").then_some(token)
}

/// The percent-decoded value of the first `token=` pair of a query string.
fn query_token(query: &str) -> Option<Vec<u8>> {
    let raw = query
        .split('&')
        .find_map(|pair| pair.strip_prefix("token="))?;
    percent_decode(raw.as_bytes())
}

/// `application/x-www-form-urlencoded` decoding (`+` is a space). `None`
/// on a malformed escape or past [`MAX_QUERY_TOKEN`] bytes.
fn percent_decode(raw: &[u8]) -> Option<Vec<u8>> {
    if raw.len() > MAX_QUERY_TOKEN * 3 {
        return None;
    }
    let mut out = Vec::with_capacity(raw.len());
    let mut bytes = raw.iter();
    while let Some(&b) = bytes.next() {
        out.push(match b {
            b'+' => b' ',
            b'%' => {
                let hi = hex_val(*bytes.next()?)?;
                let lo = hex_val(*bytes.next()?)?;
                (hi << 4) | lo
            }
            _ => b,
        });
    }
    (out.len() <= MAX_QUERY_TOKEN).then_some(out)
}

fn hex_val(b: u8) -> Option<u8> {
    char::from(b)
        .to_digit(16)
        .and_then(|d| u8::try_from(d).ok())
}

/// Constant-time comparison: the loop always runs over the whole expected
/// token, so its time does not depend on where `given` differs, nor on
/// its length.
fn ct_eq(given: &[u8], expected: &[u8]) -> bool {
    let mut diff = u8::from(given.len() != expected.len());
    for (e, g) in expected
        .iter()
        .zip(given.iter().chain(std::iter::repeat(&0u8)))
    {
        diff |= e ^ g;
    }
    diff == 0
}

/// [`Auth::Disabled`]'s guard against CSRF and DNS rebinding: the peer, when
/// the request carries axum's `ConnectInfo` (every `ApiServer` request does),
/// must be loopback; the `Host` must be `localhost`, `127.0.0.1` or `[::1]`
/// (any port); and an `Origin`, when sent, must be one of `origins`. A router
/// mounted without that extension is guarded by the Host and Origin rules
/// alone.
fn guard_disabled(origins: &[HeaderValue], req: &Request) -> Result<(), ApiErrorResponse> {
    if let Some(&ConnectInfo(peer)) = req.extensions().get::<ConnectInfo<SocketAddr>>()
        && !is_loopback(peer.ip())
    {
        return Err(ApiErrorResponse::forbidden(
            "with authentication disabled, the peer address must be loopback",
        ));
    }
    let host = req
        .headers()
        .get(header::HOST)
        .map(HeaderValue::as_bytes)
        .or_else(|| req.uri().authority().map(|a| a.as_str().as_bytes()));
    if !host.is_some_and(is_local_host) {
        return Err(ApiErrorResponse::forbidden(
            "with authentication disabled, the Host must be localhost, 127.0.0.1 or [::1]",
        ));
    }
    if let Some(origin) = req.headers().get(header::ORIGIN)
        && !origins.contains(origin)
    {
        return Err(ApiErrorResponse::forbidden(
            "with authentication disabled, the Origin must be a configured CORS origin",
        ));
    }
    Ok(())
}

/// `localhost`, `127.0.0.1` or `[::1]`, with an optional `:<port>`.
fn is_local_host(host: &[u8]) -> bool {
    let (name, port) = match host.iter().rposition(|&b| b == b':') {
        // A colon inside `[::1]` is not the port separator.
        Some(i) if !host.ends_with(b"]") => match (host.get(..i), host.get(i + 1..)) {
            (Some(name), Some(port)) => (name, port),
            _ => return false,
        },
        _ => (host, &b""[..]),
    };
    let port_ok = port.iter().all(u8::is_ascii_digit);
    let name_ok =
        name.eq_ignore_ascii_case(b"localhost") || name == b"127.0.0.1" || name == b"[::1]";
    port_ok && name_ok
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_compare() {
        assert!(ct_eq(b"secret", b"secret"));
        assert!(!ct_eq(b"secreT", b"secret"));
        assert!(!ct_eq(b"secre", b"secret"));
        assert!(!ct_eq(b"secrets", b"secret"));
        assert!(!ct_eq(b"", b"secret"));
        // A prefix padded with the zero bytes the loop pads with.
        assert!(!ct_eq(b"secret\0", b"secret"));
    }

    #[test]
    fn bearer_parsing() {
        let mut h = HeaderMap::new();
        assert_eq!(bearer(&h), None);
        for (value, want) in [
            ("Bearer abc", Some(&b"abc"[..])),
            ("bEaReR abc", Some(b"abc")),
            ("Bearer  abc", Some(b" abc")),
            ("Bearer", None),
            ("Bearerabc", None),
            ("Basic abc", None),
        ] {
            h.insert(header::AUTHORIZATION, HeaderValue::from_str(value).unwrap());
            assert_eq!(bearer(&h), want, "{value}");
        }
    }

    #[test]
    fn query_token_decoding() {
        assert_eq!(query_token("token=abc").as_deref(), Some(&b"abc"[..]));
        assert_eq!(
            query_token("a=1&token=a%2Bb+c").as_deref(),
            Some(&b"a+b c"[..])
        );
        assert_eq!(query_token("xtoken=abc"), None);
        assert_eq!(query_token("token=%zz"), None);
        assert_eq!(query_token("token=%4"), None);
        assert_eq!(
            query_token(&format!("token={}", "a".repeat(MAX_QUERY_TOKEN + 1))),
            None
        );
    }

    #[test]
    fn query_token_routes_match_by_suffix() {
        let stream = "/api/v1/torrents/{id}/files/{index}/stream";
        assert!(route_in(QUERY_TOKEN, &Method::GET, Some(stream)));
        assert!(route_in(QUERY_TOKEN, &Method::HEAD, Some(stream)));
        assert!(route_in(
            QUERY_TOKEN,
            &Method::GET,
            Some("/prefix/api/v1/events")
        ));
        assert!(!route_in(
            QUERY_TOKEN,
            &Method::POST,
            Some("/api/v1/events")
        ));
        assert!(!route_in(
            QUERY_TOKEN,
            &Method::GET,
            Some("/api/v1/session")
        ));
        assert!(!route_in(QUERY_TOKEN, &Method::GET, None));
    }

    #[test]
    fn local_hosts() {
        for host in [
            "localhost",
            "LocalHost:8090",
            "127.0.0.1",
            "127.0.0.1:1",
            "[::1]",
            "[::1]:80",
        ] {
            assert!(is_local_host(host.as_bytes()), "{host}");
        }
        for host in [
            "evil.example",
            "localhost.evil.example",
            "127.0.0.1.nip.io",
            "127.0.0.2",
            "[::2]",
            "localhost:80x",
            "",
            "[::1]x",
        ] {
            assert!(!is_local_host(host.as_bytes()), "{host}");
        }
    }
}
