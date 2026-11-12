//! HTTP(S) announce: request building, redirect handling, capped body read
//! and untrusted-response parsing; byte-range fetches for web seeds.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::ops::Range;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use fbt_bencode::{Value, ValueKind, decode};
use http_body_util::{BodyExt, Empty};
use hyper::{Request, Uri, header};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

use fbt_net::proxy::{ProxyConfig, ProxyError, ProxyKind};

use crate::connector::ProxyConnector;
use crate::tls::{self, Connector};
use crate::types::{MAX_TRACKER_ID_BYTES, MIN_INTERVAL, bounded_text};
use crate::urlencode::url_encode_bytes;
use crate::{
    AnnounceEvent, AnnounceRequest, AnnounceResponse, ScrapeInfo, TrackerConfig, TrackerError,
};

const MAX_REDIRECTS: usize = 3;
const MIN_MIN_INTERVAL: Duration = Duration::from_secs(30);
/// Most bytes reserved up front for a range body; it grows past this only
/// as bytes arrive.
const MAX_RANGE_PREALLOC: usize = 1 << 20;

/// An HTTP client with its pooled connections.
type HttpPool = Client<Connector, Empty<Bytes>>;

pub(crate) struct HttpClient {
    /// For announces: through the proxy with `proxy_trackers`.
    trackers: HttpPool,
    /// For web seeds: through the proxy with `proxy_peers`.
    web_seeds: HttpPool,
    user_agent: String,
    timeout: Duration,
    max_bytes: usize,
}

impl std::fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient").finish_non_exhaustive()
    }
}

impl HttpClient {
    /// Two clients over the same TLS setup: announces go through `proxy`
    /// when it sets `proxy_trackers`, web seeds when it sets `proxy_peers`.
    /// Proxy round trips are bounded by `cfg.http_timeout`.
    pub(crate) fn new(
        cfg: &TrackerConfig,
        proxy: Option<&ProxyConfig>,
    ) -> Result<HttpClient, TrackerError> {
        let active = proxy.filter(|p| p.kind != ProxyKind::None);
        let client = |use_proxy: bool| -> Result<_, TrackerError> {
            let c = ProxyConnector::new(active.filter(|_| use_proxy), cfg.http_timeout);
            Ok(Client::builder(TokioExecutor::new()).build(tls::build_connector(c)?))
        };
        Ok(HttpClient {
            trackers: client(active.is_some_and(|p| p.proxy_trackers))?,
            web_seeds: client(active.is_some_and(|p| p.proxy_peers))?,
            user_agent: cfg.user_agent.clone(),
            timeout: cfg.http_timeout,
            max_bytes: cfg.max_response_bytes,
        })
    }

    pub(crate) async fn announce(
        &self,
        url: &str,
        req: &AnnounceRequest,
    ) -> Result<AnnounceResponse, TrackerError> {
        let full = build_url(url, req);
        match tokio::time::timeout(self.timeout, self.fetch(full)).await {
            Ok(body) => parse_http_announce(&body?),
            Err(_) => Err(TrackerError::Timeout),
        }
    }

    /// Scrapes `scrape` (a scrape URL) for `hashes`.
    pub(crate) async fn scrape(
        &self,
        scrape: &str,
        hashes: &[[u8; 20]],
    ) -> Result<Vec<ScrapeInfo>, TrackerError> {
        let full = crate::scrape::with_hashes(scrape, hashes);
        match tokio::time::timeout(self.timeout, self.fetch(full)).await {
            Ok(body) => crate::scrape::parse_http_scrape_for(&body?, Some(hashes)),
            Err(_) => Err(TrackerError::Timeout),
        }
    }

    async fn fetch(&self, url: String) -> Result<Vec<u8>, TrackerError> {
        let resp = self.follow(&self.trackers, url, None).await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(TrackerError::HttpStatus(status.as_u16()));
        }
        self.read_capped(resp.into_body()).await
    }

    /// GETs `url` through `client` (with `range` as its `Range` header),
    /// following at most [`MAX_REDIRECTS`] redirects (never from `https`
    /// to `http`), and returns the final response, whatever its status.
    /// Each request waits at most the configured timeout for its response
    /// head.
    async fn follow(
        &self,
        client: &HttpPool,
        mut url: String,
        range: Option<&str>,
    ) -> Result<hyper::Response<hyper::body::Incoming>, TrackerError> {
        let mut redirects = 0;
        loop {
            let uri = check_uri(&url)?;
            let mut builder =
                Request::get(uri.clone()).header(header::USER_AGENT, &self.user_agent);
            if let Some(r) = range {
                builder = builder.header(header::RANGE, r);
            }
            let request = builder
                .body(Empty::<Bytes>::new())
                .map_err(|e| TrackerError::InvalidUrl(e.to_string()))?;
            let resp = tokio::time::timeout(self.timeout, client.request(request))
                .await
                .map_err(|_| TrackerError::Timeout)?
                .map_err(request_error)?;
            if !resp.status().is_redirection() {
                return Ok(resp);
            }
            redirects += 1;
            if redirects > MAX_REDIRECTS {
                return Err(TrackerError::TooManyRedirects);
            }
            let location = resp
                .headers()
                .get(header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .ok_or(TrackerError::InvalidResponse("redirect without location"))?;
            let next = resolve_location(&uri, location)?;
            if uri.scheme_str() == Some("https") && next.starts_with("http://") {
                return Err(TrackerError::InvalidResponse("insecure redirect"));
            }
            url = next;
        }
    }

    /// The bytes `range` of `url` (a web seed's file), through the web
    /// seed client. See [`TrackerClient::fetch_range`](crate::TrackerClient::fetch_range).
    pub(crate) async fn fetch_range(
        &self,
        url: &str,
        range: Range<u64>,
    ) -> Result<Bytes, TrackerError> {
        let len = range.end.saturating_sub(range.start);
        if len == 0 {
            return Ok(Bytes::new());
        }
        let last = range.end - 1;
        let header = format!("bytes={}-{last}", range.start);
        let resp = self
            .follow(&self.web_seeds, url.to_owned(), Some(&header))
            .await?;
        let status = resp.status();
        match status.as_u16() {
            206 => {
                let got = resp
                    .headers()
                    .get(header::CONTENT_RANGE)
                    .and_then(|v| v.to_str().ok())
                    .and_then(parse_content_range)
                    .ok_or(TrackerError::InvalidResponse(
                        "206 without a valid Content-Range",
                    ))?;
                if (got.0, got.1) != (range.start, last) {
                    return Err(TrackerError::InvalidResponse(
                        "Content-Range does not match the request",
                    ));
                }
            }
            200 => {
                // The whole file: our range only if it is the whole file.
                let whole = resp
                    .headers()
                    .get(header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.trim().parse::<u64>().ok());
                if range.start != 0 || whole != Some(len) {
                    return Err(TrackerError::InvalidResponse(
                        "the server ignored the range",
                    ));
                }
            }
            code if !status.is_success() => return Err(TrackerError::HttpStatus(code)),
            _ => {
                return Err(TrackerError::InvalidResponse(
                    "unexpected status for a range request",
                ));
            }
        }
        self.read_exact(resp.into_body(), len).await
    }

    /// Reads a body that must be exactly `len` bytes: a byte more is an
    /// error at once, so at most `len + 1` bytes are ever taken in, and
    /// no wait for the next frame may exceed the configured timeout (a
    /// slow but live server is not cut).
    async fn read_exact(
        &self,
        mut body: hyper::body::Incoming,
        len: u64,
    ) -> Result<Bytes, TrackerError> {
        let cap = usize::try_from(len).map_err(|_| TrackerError::ResponseTooLarge)?;
        let mut out = BytesMut::with_capacity(cap.min(MAX_RANGE_PREALLOC));
        loop {
            let frame = match tokio::time::timeout(self.timeout, body.frame()).await {
                Err(_) => return Err(TrackerError::Timeout),
                Ok(None) => break,
                Ok(Some(frame)) => frame.map_err(|e| TrackerError::Io(std::io::Error::other(e)))?,
            };
            if let Some(data) = frame.data_ref() {
                if out.len().saturating_add(data.len()) > cap {
                    return Err(TrackerError::ResponseTooLarge);
                }
                out.extend_from_slice(data);
            }
        }
        if out.len() != cap {
            return Err(TrackerError::InvalidResponse("body shorter than the range"));
        }
        Ok(out.freeze())
    }

    async fn read_capped(&self, mut body: hyper::body::Incoming) -> Result<Vec<u8>, TrackerError> {
        let mut out = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| TrackerError::Io(std::io::Error::other(e)))?;
            if let Some(data) = frame.data_ref() {
                if out.len().saturating_add(data.len()) > self.max_bytes {
                    return Err(TrackerError::ResponseTooLarge);
                }
                out.extend_from_slice(data);
            }
        }
        Ok(out)
    }
}

/// Parses a `Content-Range` header value, `bytes a-b/total` or
/// `bytes a-b/*`, into `(a, b, total)`: `a <= b`, and `b < total` when the
/// total is known. Anything else (an unsatisfied `*/total`, signs, spaces
/// inside numbers, overflow) is `None`. Input is untrusted.
#[doc(hidden)]
pub fn parse_content_range(value: &str) -> Option<(u64, u64, Option<u64>)> {
    let (unit, rest) = value.trim().split_once(' ')?;
    if !unit.eq_ignore_ascii_case("bytes") {
        return None;
    }
    let (range, total) = rest.trim_start().split_once('/')?;
    let (a, b) = range.split_once('-')?;
    let (a, b) = (decimal(a)?, decimal(b)?);
    let total = match total {
        "*" => None,
        t => Some(decimal(t)?),
    };
    if a > b || total.is_some_and(|t| b >= t) {
        return None;
    }
    Some((a, b, total))
}

/// A non-empty run of ASCII digits that fits a `u64`.
fn decimal(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// A failed request: [`TrackerError::Proxy`] when the proxy failed (a
/// [`ProxyError`] in the source chain), else [`TrackerError::Io`].
fn request_error(e: hyper_util::client::legacy::Error) -> TrackerError {
    let mut source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(&e);
    while let Some(s) = source {
        if let Some(p) = s.downcast_ref::<ProxyError>() {
            return TrackerError::Proxy(p.to_string());
        }
        source = s.source();
    }
    TrackerError::Io(std::io::Error::other(e))
}

/// Parses and scheme-checks `url`.
fn check_uri(url: &str) -> Result<Uri, TrackerError> {
    let uri: Uri = url
        .parse()
        .map_err(|e: hyper::http::uri::InvalidUri| TrackerError::InvalidUrl(e.to_string()))?;
    match uri.scheme_str() {
        Some("http") => {}
        Some("https") if tls::HTTPS_AVAILABLE => {}
        Some("https") => return Err(TrackerError::TlsDisabled),
        Some(other) => return Err(TrackerError::UnsupportedScheme(other.to_owned())),
        None => return Err(TrackerError::InvalidUrl("missing scheme".to_owned())),
    }
    if uri.authority().is_none() {
        return Err(TrackerError::InvalidUrl("missing host".to_owned()));
    }
    Ok(uri)
}

/// Resolves a `Location` header: absolute URLs and absolute paths only.
fn resolve_location(base: &Uri, location: &str) -> Result<String, TrackerError> {
    if location.starts_with("http://") || location.starts_with("https://") {
        return Ok(location.to_owned());
    }
    if location.starts_with('/')
        && !location.starts_with("//")
        && let (Some(scheme), Some(auth)) = (base.scheme_str(), base.authority())
    {
        return Ok(format!("{scheme}://{auth}{location}"));
    }
    Err(TrackerError::InvalidResponse("unsupported redirect target"))
}

fn event_str(e: AnnounceEvent) -> Option<&'static str> {
    match e {
        AnnounceEvent::None => None,
        AnnounceEvent::Started => Some("started"),
        AnnounceEvent::Completed => Some("completed"),
        AnnounceEvent::Stopped => Some("stopped"),
    }
}

/// Appends the announce query to `url`.
pub(crate) fn build_url(url: &str, req: &AnnounceRequest) -> String {
    let base = url.split('#').next().unwrap_or(url);
    let mut out = String::from(base);
    out.push(if base.contains('?') { '&' } else { '?' });
    out.push_str("info_hash=");
    out.push_str(&url_encode_bytes(&req.info_hash));
    out.push_str("&peer_id=");
    out.push_str(&url_encode_bytes(&req.peer_id.0));
    out.push_str(&format!(
        "&port={}&uploaded={}&downloaded={}&left={}&compact=1&key={:08x}",
        req.port, req.uploaded, req.downloaded, req.left, req.key
    ));
    if let Some(n) = req.num_want {
        out.push_str(&format!("&numwant={n}"));
    }
    if let Some(id) = &req.tracker_id {
        out.push_str("&trackerid=");
        out.push_str(&url_encode_bytes(id));
    }
    if let Some(ev) = event_str(req.event) {
        out.push_str("&event=");
        out.push_str(ev);
    }
    out
}

fn secs(v: &Value<'_>) -> Option<Duration> {
    v.as_int()
        .map(|n| Duration::from_secs(u64::try_from(n).unwrap_or(0)))
}

fn count(v: Option<&Value<'_>>) -> Option<u32> {
    v.and_then(Value::as_int)
        .and_then(|n| u32::try_from(n).ok())
}

fn compact_v4(b: &[u8], out: &mut Vec<SocketAddr>) {
    for &[a, b, c, d, p0, p1] in b.as_chunks::<6>().0 {
        out.push(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(a, b, c, d)),
            u16::from_be_bytes([p0, p1]),
        ));
    }
}

fn compact_v6(b: &[u8], out: &mut Vec<SocketAddr>) {
    for c in b.as_chunks::<18>().0 {
        let (ip, port) = c.split_at(16);
        if let (Ok(ip), &[p0, p1]) = (<[u8; 16]>::try_from(ip), port) {
            out.push(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::from(ip)),
                u16::from_be_bytes([p0, p1]),
            ));
        }
    }
}

/// Parses a bencoded HTTP announce response. Input is untrusted.
#[doc(hidden)]
pub fn parse_http_announce(body: &[u8]) -> Result<AnnounceResponse, TrackerError> {
    let root = decode(body).map_err(|_| TrackerError::InvalidResponse("not bencode"))?;
    let dict = root.as_dict().ok_or(TrackerError::InvalidResponse(
        "response is not a dictionary",
    ))?;
    if let Some(reason) = dict.get(b"failure reason") {
        let text = reason.as_bytes().unwrap_or_default();
        return Err(TrackerError::Failure(bounded_text(text)));
    }
    let interval = dict
        .get(b"interval")
        .and_then(secs)
        .ok_or(TrackerError::InvalidResponse("missing interval"))?
        .max(MIN_INTERVAL);
    let min_interval = dict
        .get(b"min interval")
        .and_then(secs)
        .map(|d| d.max(MIN_MIN_INTERVAL));

    let mut peers = Vec::new();
    if let Some(p) = dict.get(b"peers") {
        match p.kind() {
            ValueKind::Bytes(b) => compact_v4(b, &mut peers),
            ValueKind::List(items) => {
                for item in items {
                    let Some(d) = item.as_dict() else { continue };
                    let ip = d
                        .get(b"ip")
                        .and_then(Value::as_str)
                        .and_then(|s| s.parse::<IpAddr>().ok());
                    let port = d
                        .get(b"port")
                        .and_then(Value::as_int)
                        .and_then(|n| u16::try_from(n).ok());
                    if let (Some(ip), Some(port)) = (ip, port) {
                        peers.push(SocketAddr::new(ip, port));
                    }
                }
            }
            _ => return Err(TrackerError::InvalidResponse("bad peers")),
        }
    }
    if let Some(b) = dict.get(b"peers6").and_then(Value::as_bytes) {
        compact_v6(b, &mut peers);
    }

    let external_ip = dict
        .get(b"external ip")
        .and_then(Value::as_bytes)
        .and_then(|b| {
            if let Ok(a) = <[u8; 4]>::try_from(b) {
                Some(IpAddr::V4(Ipv4Addr::from(a)))
            } else if let Ok(a) = <[u8; 16]>::try_from(b) {
                Some(IpAddr::V6(Ipv6Addr::from(a)))
            } else {
                None
            }
        });

    Ok(AnnounceResponse {
        interval,
        min_interval,
        peers,
        complete: count(dict.get(b"complete")),
        incomplete: count(dict.get(b"incomplete")),
        tracker_id: dict
            .get(b"tracker id")
            .and_then(Value::as_bytes)
            .filter(|b| b.len() <= MAX_TRACKER_ID_BYTES)
            .map(<[u8]>::to_vec),
        warning: dict
            .get(b"warning message")
            .and_then(Value::as_bytes)
            .map(bounded_text),
        external_ip,
    })
}
