//! HTTP(S) announce: request building, redirect handling, capped body read
//! and untrusted-response parsing; byte-range fetches for web seeds.

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::ops::Range;
use std::pin::Pin;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use fbt_bencode::{Value, ValueKind, decode};
use http_body_util::{BodyExt, Empty};
use hyper::{Request, Uri, header};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

use fbt_net::addr::is_internal;
use fbt_net::proxy::{ProxyConfig, ProxyError, ProxyKind};

use crate::connector::{ProxyConnector, port_of};
use crate::tls::{self, Connector};
use crate::types::{MAX_TRACKER_ID_BYTES, MIN_INTERVAL, bounded_text};
use crate::urlencode::url_encode_bytes;
use crate::{
    AnnounceEvent, AnnounceRequest, AnnounceResponse, ScrapeInfo, TrackerConfig, TrackerError,
};

const MAX_REDIRECTS: usize = 3;
/// The most addresses a redirect target's host is asked for when a policy denies
/// internal targets: answers past it are not judged, so the check stays bounded
/// however many addresses a hostile name returns.
const MAX_REDIRECT_ADDRESSES: usize = 16;
const MIN_MIN_INTERVAL: Duration = Duration::from_secs(30);
/// Most bytes reserved up front for a range body; it grows past this only
/// as bytes arrive.
const MAX_RANGE_PREALLOC: usize = 1 << 20;

/// The slowest a range body may arrive, on average, once
/// [`RANGE_RATE_GRACE`] has passed (time spent waiting on the caller's
/// [`RangePace`] excluded): a server trickling bytes would otherwise hold
/// the request for ever, each byte arriving within the per-frame timeout.
pub(crate) const MIN_RANGE_RATE: u64 = 4 * 1024;
/// How long a range request runs before [`MIN_RANGE_RATE`] applies.
pub(crate) const RANGE_RATE_GRACE: Duration = Duration::from_secs(30);

/// Paces the body of a range fetch, as a download rate limit does for a
/// peer connection: before each read the fetch waits for
/// [`ready`](Self::ready), and after it reports the bytes read to
/// [`charge`](Self::charge). TCP backpressure then holds the server to
/// the limit.
#[doc(hidden)]
pub trait RangePace: Send + Sync {
    /// Completes once the next bytes may be read.
    fn ready(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
    /// `n` bytes of the body were read.
    fn charge(&self, n: u64);
}

/// No pacing.
struct Unpaced;

impl RangePace for Unpaced {
    fn ready(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(std::future::ready(()))
    }

    fn charge(&self, _: u64) {}
}

/// Whether `bytes` read in `active` time is below [`MIN_RANGE_RATE`], once
/// past [`RANGE_RATE_GRACE`].
pub(crate) fn too_slow(bytes: u64, active: Duration) -> bool {
    active > RANGE_RATE_GRACE
        && u128::from(bytes) * 1000 < u128::from(MIN_RANGE_RATE) * active.as_millis()
}

/// Whether a fetch may be redirected to an address inside the local network.
///
/// A `Location` header moves a URL wherever the server likes, so a public web seed can
/// point us at a host on the user's own network — a LAN service, or a router's metadata
/// endpoint at `169.254.169.254` — and read back whatever that answers. With
/// `allow_internal` false such a target is refused before anything is sent to it, on the
/// addresses its host resolves to, so a public name pointing inward is refused too. See
/// [`TrackerClient::fetch_range`](crate::TrackerClient::fetch_range) and
/// [`fbt_net::addr::is_internal`]. A non-exhaustive struct cannot be written out field by
/// field in another crate, so build the default and change what you need:
///
/// ```
/// let mut policy = fbt_tracker::RedirectPolicy::default();
/// policy.allow_internal = false;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RedirectPolicy {
    /// Whether a redirect target may resolve to an internal address: loopback, RFC 1918,
    /// IPv6 unique-local or link-local, CGNAT, the unspecified address, multicast or
    /// broadcast. `true` by default, which is how announces have always followed
    /// redirects and how a web seed configured on a private address keeps working.
    pub allow_internal: bool,
}

impl Default for RedirectPolicy {
    fn default() -> RedirectPolicy {
        RedirectPolicy {
            allow_internal: true,
        }
    }
}

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
        let resp = self
            .follow(&self.trackers, url, None, RedirectPolicy::default())
            .await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(TrackerError::HttpStatus(status.as_u16()));
        }
        self.read_capped(resp.into_body()).await
    }

    /// GETs `url` through `client` (with `range` as its `Range` header),
    /// following at most [`MAX_REDIRECTS`] redirects (never from `https`
    /// to `http`) as `policy` allows, and returns the final response,
    /// whatever its status. Each request waits at most the configured
    /// timeout for its response head.
    async fn follow(
        &self,
        client: &HttpPool,
        mut url: String,
        range: Option<&str>,
        policy: RedirectPolicy,
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
            // Only a target is judged: the URL the caller asked for is its own choice.
            if !policy.allow_internal {
                check_redirect_target(self.timeout, &next).await?;
            }
            url = next;
        }
    }

    /// The bytes `range` of `url` (a web seed's file), through the web
    /// seed client, following redirects as `policy` allows. See
    /// [`TrackerClient::fetch_range`](crate::TrackerClient::fetch_range).
    pub(crate) async fn fetch_range(
        &self,
        url: &str,
        range: Range<u64>,
        pace: Option<&dyn RangePace>,
        policy: RedirectPolicy,
    ) -> Result<Bytes, TrackerError> {
        let pace = pace.unwrap_or(&Unpaced);
        let started = tokio::time::Instant::now();
        let len = range.end.saturating_sub(range.start);
        if len == 0 {
            return Ok(Bytes::new());
        }
        let last = range.end - 1;
        let header = format!("bytes={}-{last}", range.start);
        let resp = self
            .follow(&self.web_seeds, url.to_owned(), Some(&header), policy)
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
        self.read_exact(resp.into_body(), len, pace, started).await
    }

    /// Reads a body that must be exactly `len` bytes: at most `len` bytes
    /// are kept, and the first byte past them fails the request (the
    /// frame that carries it is dropped). No wait for the next frame may
    /// exceed the configured timeout, so a slow but live server is not
    /// cut, but past [`RANGE_RATE_GRACE`] since `started` the average rate,
    /// waits on `pace` excluded, must stay at [`MIN_RANGE_RATE`] or more.
    async fn read_exact(
        &self,
        mut body: hyper::body::Incoming,
        len: u64,
        pace: &dyn RangePace,
        started: tokio::time::Instant,
    ) -> Result<Bytes, TrackerError> {
        let cap = usize::try_from(len).map_err(|_| TrackerError::ResponseTooLarge)?;
        let mut out = BytesMut::with_capacity(cap.min(MAX_RANGE_PREALLOC));
        let mut paced = Duration::ZERO;
        loop {
            let before = tokio::time::Instant::now();
            pace.ready().await;
            paced = paced.saturating_add(before.elapsed());
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
                pace.charge(data.len() as u64);
            }
            let active = started.elapsed().saturating_sub(paced);
            if out.len() < cap && too_slow(out.len() as u64, active) {
                return Err(TrackerError::TooSlow);
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

/// Whether a redirect to `url` may be asked at all under a policy that denies internal
/// targets: its host is resolved the way the connector resolves it (`getaddrinfo` on the
/// blocking pool, skipped for an IP literal) and every answer judged, so what decides is
/// the resolver's reply and not the text of the `Location` header. A target that does not
/// resolve, or not within `timeout`, is left to the request itself: a silent resolver is
/// not the seed pointing us inward. The refusal carries no address — the session reports
/// it with the seed's own URL, which is what a user recognises.
async fn check_redirect_target(timeout: Duration, url: &str) -> Result<(), TrackerError> {
    let uri = check_uri(url)?;
    let host = uri
        .host()
        .map(without_brackets)
        .ok_or_else(|| TrackerError::InvalidUrl("missing host".to_owned()))?;
    let found =
        match tokio::time::timeout(timeout, tokio::net::lookup_host((host, port_of(&uri)))).await {
            Ok(found) => found?,
            Err(_) => return Ok(()),
        };
    for ip in found.take(MAX_REDIRECT_ADDRESSES).map(|a| a.ip()) {
        if is_internal(ip) {
            return Err(TrackerError::InternalRedirect);
        }
    }
    Ok(())
}

/// A URI's host without the brackets around an IPv6 literal, as a resolver takes it.
fn without_brackets(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
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
