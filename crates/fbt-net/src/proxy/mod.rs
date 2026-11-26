//! Proxy clients: SOCKS5 (RFC 1928, with RFC 1929 username/password) for TCP
//! CONNECT and UDP ASSOCIATE, and HTTP CONNECT with Basic authentication.
//!
//! A proxy's replies are hostile input: every read is bounded and every
//! exchange runs under the caller's timeout.

use std::fmt;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use fbt_core::audit;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

mod http;
mod socks5;

pub use http::{MAX_CONNECT_HEAD, parse_connect_head};
pub use socks5::{decode_udp, encode_udp, encode_udp_host, parse_socks5_reply};

/// Which kind of proxy [`ProxyConfig`] names.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ProxyKind {
    /// No proxy: connections are made directly.
    #[default]
    None,
    /// A SOCKS5 proxy (TCP CONNECT and UDP ASSOCIATE).
    Socks5,
    /// An HTTP proxy, used through `CONNECT` (TCP only).
    Http,
}

/// Where and how to reach a proxy, and what goes through it.
///
/// Caveats:
///
/// - **SOCKS5 proxies all UDP or none.** With [`ProxyKind::Socks5`] and
///   `proxy_peers || proxy_trackers`, every UDP packet (outgoing uTP, DHT,
///   UDP trackers) goes through one UDP ASSOCIATE: one socket cannot be half
///   proxied. The session then listens on TCP only, the DHT runs over the
///   relay's address family only (IPv4 in practice) and incoming uTP arrives
///   only as the relay forwards it.
/// - **HTTP proxies cannot carry UDP.** With [`ProxyKind::Http`], outgoing uTP
///   is off when `proxy_peers` is set, UDP trackers fail with
///   `ProxyUnsupported` when `proxy_trackers` is set, and the DHT runs direct
///   unless it is disabled.
/// - **Many HTTP proxies refuse `CONNECT` to port 80** (Squid's default
///   `SSL_ports` rule, for one), so plain-HTTP trackers may fail through them.
/// - **Not every lookup uses the proxy.** `proxy_hostnames` covers TCP
///   connections. A SOCKS5 relay also carries a UDP tracker named by host
///   (RFC 1928 lets the UDP request header hold the name), so that name is
///   never looked up here; an IP literal goes to the relay as an address and
///   is never looked up either. A DHT bootstrap entry that is a name is
///   never resolved locally by a session behind a proxy, whatever the proxy
///   is: the session builds the DHT with `resolve_bootstrap_names` false, so
///   a name entry is skipped (an IP-literal entry is still used, over the
///   relay) and its bootstrap comes from the literals, a saved state and the
///   nodes it reaches. Local service discovery (LSD), UPnP and NAT-PMP stay
///   direct on the local network and are never proxied.
/// - **No proxy-only mode by default.** Whatever the proxy cannot carry goes
///   direct, from the host's own address: with an HTTP proxy the DHT (which
///   names every torrent's info hash to other nodes) and incoming uTP
///   (outgoing too, without `proxy_peers`); with any proxy LSD, UPnP and
///   NAT-PMP. The session logs a warning for each one enabled; turn them off
///   in its settings to keep all traffic on the proxy, or set
///   [`proxy_only`](Self::proxy_only), the single switch that has every
///   bypass treated as off (it also turns the three proxy switches on), so a
///   proxy-only session warns about nothing.
///
/// `Debug` prints the password as `"***"`; serialising (the `serde`
/// feature) writes it in clear text.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct ProxyConfig {
    /// The proxy protocol; [`ProxyKind::None`] (the default) dials directly.
    pub kind: ProxyKind,
    /// Proxy host name or IP address, unbracketed (an IPv6 literal is spelled
    /// bare; [`validate`](ProxyConfig::validate) rejects brackets).
    pub host: String,
    /// Proxy port (default 1080).
    pub port: u16,
    /// Username: SOCKS5 offers username/password authentication and HTTP sends
    /// `Proxy-Authorization: Basic` when it is set. SOCKS5 needs 1 to 255
    /// bytes; HTTP Basic refuses a `:` in it.
    pub username: Option<String>,
    /// Password, used with `username`. SOCKS5 needs 1 to 255 bytes; HTTP
    /// sends an unset password as empty.
    pub password: Option<String>,
    /// Route peer connections (and web seeds) through the proxy.
    pub proxy_peers: bool,
    /// Route tracker requests through the proxy.
    pub proxy_trackers: bool,
    /// Let the proxy resolve host names instead of resolving them locally.
    ///
    /// A web seed's redirect guard follows suit: with this true, a redirect target that is a
    /// host name is not looked up or judged locally at all — the proxy resolves it and decides
    /// where it goes — and only a target spelled as an address is refused for being inside the
    /// local network. With it false, names are resolved here and judged then (see
    /// `RedirectPolicy` in the `fbt-tracker` crate).
    pub proxy_hostnames: bool,
    /// Keep every bypass off: nothing leaves from the host's own address.
    ///
    /// With it (and a proxy), the session treats `proxy_peers`,
    /// `proxy_trackers` and `proxy_hostnames` as on and everything that would
    /// otherwise go direct as off (the DHT, local service discovery, UPnP,
    /// NAT-PMP, incoming TCP and uTP, outgoing uTP), whatever the switches on
    /// this config say. What the proxy cannot carry therefore does not run:
    /// with an HTTP proxy, no DHT and no uTP at all, and UDP trackers fail.
    /// The stored switches are untouched, so they come back as they were set
    /// once the proxy is removed. Ignored with [`ProxyKind::None`], which has
    /// no proxy to send through.
    pub proxy_only: bool,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        ProxyConfig {
            kind: ProxyKind::None,
            host: String::new(),
            port: 1080,
            username: None,
            password: None,
            proxy_peers: true,
            proxy_trackers: true,
            proxy_hostnames: true,
            proxy_only: false,
        }
    }
}

impl fmt::Debug for ProxyConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProxyConfig")
            .field("kind", &self.kind)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("proxy_peers", &self.proxy_peers)
            .field("proxy_trackers", &self.proxy_trackers)
            .field("proxy_hostnames", &self.proxy_hostnames)
            .field("proxy_only", &self.proxy_only)
            .finish()
    }
}

impl ProxyConfig {
    /// Checks the fields a proxy needs: unless `kind` is [`ProxyKind::None`],
    /// `host` is non-empty, not bracketed, and `port` non-zero, and the
    /// credentials can be sent: for SOCKS5 a `username` needs a `password`,
    /// each of 1 to 255 bytes (RFC 1929); for HTTP Basic the `username` has
    /// no `:`. A bracketed host is an IPv6 literal spelled for a URI; handed
    /// to the resolver as-is it cannot be dialled, so it is rejected here
    /// instead of recording a connect for a resolver call.
    pub fn validate(&self) -> Result<(), String> {
        if self.kind == ProxyKind::None {
            return Ok(());
        }
        if self.host.is_empty() {
            return Err("proxy host is empty".into());
        }
        if self.host.contains('[') || self.host.contains(']') {
            return Err("proxy host contains brackets".into());
        }
        if self.port == 0 {
            return Err("proxy port is 0".into());
        }
        let Some(user) = &self.username else {
            return Ok(());
        };
        let pass = self.password.as_deref().unwrap_or("");
        match self.kind {
            ProxyKind::Socks5 if !(1..=255).contains(&user.len()) => {
                Err("SOCKS5 proxy username must be 1 to 255 bytes".into())
            }
            ProxyKind::Socks5 if !(1..=255).contains(&pass.len()) => {
                Err("SOCKS5 proxy password must be 1 to 255 bytes".into())
            }
            ProxyKind::Http if user.contains(':') => Err("HTTP proxy username contains ':'".into()),
            _ => Ok(()),
        }
    }
}

/// Where a proxied TCP connection should go.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyTarget {
    /// An address.
    Addr(SocketAddr),
    /// A host name and port, resolved by the proxy when
    /// [`ProxyConfig::proxy_hostnames`] is set, else locally.
    ///
    /// The name is 1 to 255 bytes of `[A-Za-z0-9._-]`, else
    /// [`ProxyError::Protocol`]: internationalised names must be given in
    /// their punycode (`xn--`) form. An IP literal (`[::1]` or bare) is used
    /// as an address.
    Host(String, u16),
}

/// Why a proxied connection failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProxyError {
    /// A socket error, or the proxy closed the connection.
    #[error("proxy I/O error: {0}")]
    Io(#[from] io::Error),
    /// The proxy was not reached: the TCP dial to it failed, or the timeout
    /// ran out before it connected. The inner error is an `io::Error` of kind
    /// [`io::ErrorKind::TimedOut`] when it was the timeout.
    #[error("cannot reach the proxy: {0}")]
    Unreachable(io::Error),
    /// The exchange did not finish within the timeout, after the proxy had
    /// accepted the connection.
    #[error("proxy timed out")]
    Timeout,
    /// The proxy rejected the credentials, or wanted some and none were set.
    #[error("proxy authentication failed")]
    AuthFailed,
    /// The SOCKS5 proxy refused the request with this `REP` code.
    #[error("SOCKS5 proxy refused the request (code {0})")]
    Refused(u8),
    /// The HTTP proxy answered `CONNECT` with this non-2xx status.
    #[error("HTTP proxy answered status {0}")]
    HttpStatus(u16),
    /// The proxy broke the protocol, or a request could not be expressed in it.
    #[error("proxy protocol error: {0}")]
    Protocol(&'static str),
    /// The operation is not available with this proxy kind or reply.
    #[error("unsupported by this proxy")]
    Unsupported,
}

/// Opens a TCP connection to `target` through the proxy `cfg` names, the whole
/// exchange (dial, negotiation, reply) bounded by `timeout`. With
/// [`ProxyKind::None`] it dials `target` directly.
///
/// The dial to the proxy is its own phase: a failure there, or the timeout
/// running out before the proxy accepted the connection, is
/// [`ProxyError::Unreachable`]. [`ProxyError::Timeout`] means only that the
/// exchange after that did not finish in the time left.
pub async fn connect_tcp(
    cfg: &ProxyConfig,
    target: ProxyTarget,
    timeout: Duration,
) -> Result<TcpStream, ProxyError> {
    let deadline = deadline_in(timeout);
    match cfg.kind {
        ProxyKind::None => {
            let target = prepare_target(cfg, target).await?;
            match within(deadline, dial_direct(target)).await {
                Some(r) => r,
                None => Err(ProxyError::Timeout),
            }
        }
        ProxyKind::Socks5 => {
            let target = prepare_target_within(cfg, target, deadline).await?;
            let mut s = dial_proxy(cfg, deadline).await?;
            handshake(deadline, socks5_connect(&mut s, cfg, &target)).await?;
            Ok(s)
        }
        ProxyKind::Http => {
            let target = prepare_target_within(cfg, target, deadline).await?;
            let mut s = dial_proxy(cfg, deadline).await?;
            handshake(deadline, http::connect(&mut s, cfg, &target)).await?;
            Ok(s)
        }
    }
}

/// Negotiates and sends a SOCKS5 `CONNECT` for `target`.
async fn socks5_connect(
    s: &mut TcpStream,
    cfg: &ProxyConfig,
    target: &ProxyTarget,
) -> Result<(), ProxyError> {
    socks5::negotiate(s, cfg).await?;
    socks5::request(s, socks5::CMD_CONNECT, target)
        .await
        .map(|_| ())
}

/// Dials `target` directly, recording what leaves the host.
async fn dial_direct(target: ProxyTarget) -> Result<TcpStream, ProxyError> {
    Ok(match target {
        ProxyTarget::Addr(a) => {
            audit::record(audit::Outbound::TcpConnect(a));
            TcpStream::connect(a).await?
        }
        ProxyTarget::Host(h, p) => {
            audit::record(audit::Outbound::Lookup(h.clone()));
            TcpStream::connect((h.as_str(), p)).await?
        }
    })
}

/// Checks that the proxy `cfg` names can be used at all, within `timeout`: a
/// SOCKS5 proxy is dialled, greeted and authenticated and the connection is
/// then closed; an HTTP proxy is only dialled, because HTTP has no login check
/// without a `CONNECT`; [`ProxyKind::None`] is `Ok(())` without a socket.
///
/// A dial failure or a dial timeout is [`ProxyError::Unreachable`]; what comes
/// after the proxy accepted the connection (a rejected greeting or login) is
/// reported as it is, and only an unfinished exchange costs
/// [`ProxyError::Timeout`].
pub async fn probe(cfg: &ProxyConfig, timeout: Duration) -> Result<(), ProxyError> {
    if cfg.kind == ProxyKind::None {
        return Ok(());
    }
    let deadline = deadline_in(timeout);
    let mut s = dial_proxy(cfg, deadline).await?;
    if cfg.kind == ProxyKind::Socks5 {
        handshake(deadline, socks5::negotiate(&mut s, cfg)).await?;
    }
    Ok(())
}

/// Whether `e`, from [`udp_associate`], means the proxy will not relay UDP
/// rather than that it is not usable: it refused the association
/// ([`ProxyError::Refused`]) or cannot carry one ([`ProxyError::Unsupported`],
/// which is also any proxy kind but SOCKS5, and a relay named by domain). In
/// every case the greeting and login had succeeded, so the proxy itself is
/// reachable and only its UDP association is missing: it is a property of the
/// proxy, not something a retry will change.
pub fn udp_not_relayed(e: &ProxyError) -> bool {
    matches!(e, ProxyError::Refused(_) | ProxyError::Unsupported)
}

/// Runs `fut` with the time left until `deadline`; `None` when it ran out.
async fn within<F: Future>(deadline: Instant, fut: F) -> Option<F::Output> {
    let left = deadline.saturating_duration_since(Instant::now());
    tokio::time::timeout(left, fut).await.ok()
}

/// How far a timeout too large for an `Instant` is stretched: beyond any
/// exchange this crate bounds, and far too short to overflow.
const FAR_DEADLINE: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// The deadline `timeout` from now.
///
/// A timeout no `Instant` can hold is one a caller can hand in: a
/// `Settings::connect_timeout` is not range-checked, and a deserialised
/// `Duration::MAX` reaches the proxied dial and the UDP association. It
/// saturates at [`FAR_DEADLINE`] instead of panicking, so a settings value
/// cannot take the session down; the exchange is then still bounded, by a year
/// rather than by forever.
fn deadline_in(timeout: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(timeout)
        .or_else(|| now.checked_add(FAR_DEADLINE))
        // The clock is within a year of its own maximum: every exchange is cut
        // off at once rather than panicking.
        .unwrap_or(now)
}

/// The handshake phase of a proxied exchange, bounded by the time left until
/// `deadline`: the proxy has accepted the connection, so running out of time
/// is [`ProxyError::Timeout`] and not a reachability failure.
async fn handshake<F>(deadline: Instant, fut: F) -> Result<(), ProxyError>
where
    F: Future<Output = Result<(), ProxyError>>,
{
    match within(deadline, fut).await {
        Some(r) => r,
        None => Err(ProxyError::Timeout),
    }
}

/// The timeout of something that happens before the proxy accepted the
/// connection: the proxy has not been reached, so it is
/// [`ProxyError::Unreachable`] with a timed-out `io::Error`.
fn unreachable_timeout(msg: &'static str) -> ProxyError {
    ProxyError::Unreachable(io::Error::new(io::ErrorKind::TimedOut, msg))
}

/// [`prepare_target`] with the time left until `deadline`. Resolving a target
/// name here (only when the proxy should not) and the dial that follows are
/// both part of the dial phase, so running out of time is
/// [`ProxyError::Unreachable`].
async fn prepare_target_within(
    cfg: &ProxyConfig,
    target: ProxyTarget,
    deadline: Instant,
) -> Result<ProxyTarget, ProxyError> {
    match within(deadline, prepare_target(cfg, target)).await {
        Some(r) => r,
        None => Err(unreachable_timeout("preparing the target timed out")),
    }
}

/// Turns IP literals into addresses, refuses host names that cannot be sent
/// and scoped IPv6 addresses a proxy cannot express, and resolves names
/// locally when the proxy should not.
async fn prepare_target(cfg: &ProxyConfig, target: ProxyTarget) -> Result<ProxyTarget, ProxyError> {
    let (host, port) = match target {
        ProxyTarget::Addr(SocketAddr::V6(a))
            if a.scope_id() != 0 && cfg.kind != ProxyKind::None =>
        {
            return Err(ProxyError::Protocol(
                "a scoped IPv6 address cannot be sent to a proxy",
            ));
        }
        ProxyTarget::Addr(_) => return Ok(target),
        ProxyTarget::Host(host, port) => (host, port),
    };
    let bare = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(&host);
    if let Ok(ip) = bare.parse::<IpAddr>() {
        return Ok(ProxyTarget::Addr(SocketAddr::new(ip, port)));
    }
    if host.is_empty() || host.len() > 255 || !host.bytes().all(is_host_byte) {
        return Err(ProxyError::Protocol("invalid target host name"));
    }
    if cfg.kind == ProxyKind::None || cfg.proxy_hostnames {
        return Ok(ProxyTarget::Host(host, port));
    }
    audit::record(audit::Outbound::Lookup(host.clone()));
    match tokio::net::lookup_host((host.as_str(), port)).await?.next() {
        Some(a) => Ok(ProxyTarget::Addr(a)),
        None => Err(ProxyError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            "target host name did not resolve",
        ))),
    }
}

/// The bytes a target host name may hold: letters, digits, `.`, `_`, `-`.
fn is_host_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-')
}

/// Dials the proxy itself, within the time left until `deadline`. Every
/// failure here is [`ProxyError::Unreachable`]: nothing about the proxy has
/// been learned except that it cannot be reached.
async fn dial_proxy(cfg: &ProxyConfig, deadline: Instant) -> Result<TcpStream, ProxyError> {
    cfg.validate()
        .map_err(|_| ProxyError::Protocol("invalid proxy configuration"))?;
    audit::record(dial_outbound(&cfg.host, cfg.port));
    match within(deadline, TcpStream::connect((cfg.host.as_str(), cfg.port))).await {
        Some(Ok(s)) => Ok(s),
        Some(Err(e)) => Err(ProxyError::Unreachable(e)),
        None => Err(unreachable_timeout("connecting to the proxy timed out")),
    }
}

/// The record of a dial to `host`: the connect for an IP literal, the lookup
/// for a name. The proxy's own name is the one a proxied session looks up.
fn dial_outbound(host: &str, port: u16) -> audit::Outbound {
    let bare = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    match bare.parse::<IpAddr>() {
        Ok(ip) => audit::Outbound::TcpConnect(SocketAddr::new(ip, port)),
        Err(_) => audit::Outbound::Lookup(host.to_owned()),
    }
}

/// A SOCKS5 UDP association: datagrams framed with [`encode_udp`] and sent to
/// [`relay`](Socks5Udp::relay) go out through the proxy; replies come back
/// from the relay framed for [`decode_udp`]. The association lives as long as
/// this value (its control connection).
#[derive(Debug)]
pub struct Socks5Udp {
    control: TcpStream,
    relay: SocketAddr,
}

impl Socks5Udp {
    /// The proxy's UDP relay address.
    pub fn relay(&self) -> SocketAddr {
        self.relay
    }

    /// Completes when the proxy closes the control connection (the
    /// association is then gone). Anything the proxy sends on it is discarded.
    pub async fn closed(&mut self) {
        let mut buf = [0u8; 64];
        while let Ok(n) = self.control.read(&mut buf).await {
            if n == 0 {
                break;
            }
        }
    }
}

/// Opens a SOCKS5 UDP association, bounded by `timeout`. Any kind other than
/// [`ProxyKind::Socks5`], or a relay named by domain, is
/// [`ProxyError::Unsupported`]. A relay of `0.0.0.0` (or `::`) means the
/// proxy's own address.
///
/// As with [`connect_tcp`], a dial failure or a dial timeout is
/// [`ProxyError::Unreachable`], and [`ProxyError::Timeout`] means only that the
/// exchange after the proxy accepted the connection did not finish in the time
/// left.
pub async fn udp_associate(cfg: &ProxyConfig, timeout: Duration) -> Result<Socks5Udp, ProxyError> {
    if cfg.kind != ProxyKind::Socks5 {
        return Err(ProxyError::Unsupported);
    }
    let deadline = deadline_in(timeout);
    let mut control = dial_proxy(cfg, deadline).await?;
    let bnd = match within(deadline, associate_request(&mut control, cfg)).await {
        Some(r) => r?,
        None => return Err(ProxyError::Timeout),
    };
    let Some(mut relay) = bnd else {
        return Err(ProxyError::Unsupported);
    };
    if relay.ip().is_unspecified() {
        relay.set_ip(control.peer_addr()?.ip());
    }
    Ok(Socks5Udp { control, relay })
}

/// Greets and authenticates, then asks for a UDP association; returns the
/// bound address the proxy names (`None` when it named a domain).
async fn associate_request(
    control: &mut TcpStream,
    cfg: &ProxyConfig,
) -> Result<Option<SocketAddr>, ProxyError> {
    socks5::negotiate(control, cfg).await?;
    // DST.ADDR 0.0.0.0:0: the client's UDP address is not known yet.
    let any = ProxyTarget::Addr(SocketAddr::from(([0, 0, 0, 0], 0)));
    socks5::request(control, socks5::CMD_UDP_ASSOCIATE, &any).await
}

/// Standard base64 with padding (RFC 4648), for `Proxy-Authorization: Basic`.
fn base64(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let sym = |n: u32| char::from(TABLE.get((n & 63) as usize).copied().unwrap_or(b'='));
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let mut b = [0u8; 3];
        for (dst, src) in b.iter_mut().zip(chunk) {
            *dst = *src;
        }
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(sym(n >> 18));
        out.push(sym(n >> 12));
        out.push(if chunk.len() > 1 { sym(n >> 6) } else { '=' });
        out.push(if chunk.len() > 2 { sym(n) } else { '=' });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_rfc4648_vectors() {
        let cases = [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ];
        for (i, o) in cases {
            assert_eq!(base64(i.as_bytes()), o);
        }
    }

    /// A bracketed host is an IPv6 literal spelled for a URI, not for the dial:
    /// handed to the resolver as-is, a connect record for it would be a lie.
    /// Reject it; a bare literal is fine.
    #[test]
    fn validate_rejects_bracketed_host() {
        for kind in [ProxyKind::Socks5, ProxyKind::Http] {
            let cfg = ProxyConfig {
                kind,
                host: "[2001:db8::1]".into(),
                ..ProxyConfig::default()
            };
            assert!(cfg.validate().is_err(), "{kind:?}: a bracketed host");
        }
        let bare = ProxyConfig {
            kind: ProxyKind::Socks5,
            host: "2001:db8::1".into(),
            ..ProxyConfig::default()
        };
        assert!(bare.validate().is_ok(), "a bare literal is dialable");
    }
}
