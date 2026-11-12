//! Proxy clients: SOCKS5 (RFC 1928, with RFC 1929 username/password) for TCP
//! CONNECT and UDP ASSOCIATE, and HTTP CONNECT with Basic authentication.
//!
//! A proxy's replies are hostile input: every read is bounded and every
//! exchange runs under the caller's timeout.

use std::fmt;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

mod http;
mod socks5;

pub use http::{MAX_CONNECT_HEAD, parse_connect_head};
pub use socks5::{decode_udp, encode_udp, parse_socks5_reply};

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
///   proxied. The session then listens on TCP only, the DHT runs IPv4 only and
///   incoming uTP arrives only as the relay forwards it.
/// - **HTTP proxies cannot carry UDP.** With [`ProxyKind::Http`], outgoing uTP
///   is off when `proxy_peers` is set, UDP trackers fail with
///   `ProxyUnsupported` when `proxy_trackers` is set, and the DHT runs direct
///   unless it is disabled.
/// - **Many HTTP proxies refuse `CONNECT` to port 80** (Squid's default
///   `SSL_ports` rule, for one), so plain-HTTP trackers may fail through them.
///
/// `Debug` prints the password as `"***"`.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct ProxyConfig {
    /// The proxy protocol; [`ProxyKind::None`] (the default) dials directly.
    pub kind: ProxyKind,
    /// Proxy host name or IP address.
    pub host: String,
    /// Proxy port (default 1080).
    pub port: u16,
    /// Username: SOCKS5 offers username/password authentication and HTTP sends
    /// `Proxy-Authorization: Basic` when it is set.
    pub username: Option<String>,
    /// Password, used with `username` (empty when unset).
    pub password: Option<String>,
    /// Route peer connections (and web seeds) through the proxy.
    pub proxy_peers: bool,
    /// Route tracker requests through the proxy.
    pub proxy_trackers: bool,
    /// Let the proxy resolve host names instead of resolving them locally.
    pub proxy_hostnames: bool,
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
            .finish()
    }
}

impl ProxyConfig {
    /// Checks the fields a proxy needs: unless `kind` is [`ProxyKind::None`],
    /// `host` is non-empty and `port` non-zero.
    pub fn validate(&self) -> Result<(), String> {
        if self.kind == ProxyKind::None {
            return Ok(());
        }
        if self.host.is_empty() {
            return Err("proxy host is empty".into());
        }
        if self.port == 0 {
            return Err("proxy port is 0".into());
        }
        Ok(())
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
    Host(String, u16),
}

/// Why a proxied connection failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProxyError {
    /// A socket error, or the proxy closed the connection.
    #[error("proxy I/O error: {0}")]
    Io(#[from] io::Error),
    /// The exchange did not finish within the timeout.
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
pub async fn connect_tcp(
    cfg: &ProxyConfig,
    target: ProxyTarget,
    timeout: Duration,
) -> Result<TcpStream, ProxyError> {
    match tokio::time::timeout(timeout, connect_inner(cfg, target)).await {
        Ok(r) => r,
        Err(_) => Err(ProxyError::Timeout),
    }
}

async fn connect_inner(cfg: &ProxyConfig, target: ProxyTarget) -> Result<TcpStream, ProxyError> {
    let target = prepare_target(cfg, target).await?;
    match cfg.kind {
        ProxyKind::None => Ok(match target {
            ProxyTarget::Addr(a) => TcpStream::connect(a).await?,
            ProxyTarget::Host(h, p) => TcpStream::connect((h.as_str(), p)).await?,
        }),
        ProxyKind::Socks5 => {
            let mut s = dial_proxy(cfg).await?;
            socks5::negotiate(&mut s, cfg).await?;
            socks5::request(&mut s, socks5::CMD_CONNECT, &target).await?;
            Ok(s)
        }
        ProxyKind::Http => {
            let mut s = dial_proxy(cfg).await?;
            http::connect(&mut s, cfg, &target).await?;
            Ok(s)
        }
    }
}

/// Turns IP literals into addresses, refuses host names that cannot be sent,
/// and resolves names locally when the proxy should not.
async fn prepare_target(cfg: &ProxyConfig, target: ProxyTarget) -> Result<ProxyTarget, ProxyError> {
    let ProxyTarget::Host(host, port) = target else {
        return Ok(target);
    };
    let bare = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(&host);
    if let Ok(ip) = bare.parse::<IpAddr>() {
        return Ok(ProxyTarget::Addr(SocketAddr::new(ip, port)));
    }
    if host.is_empty()
        || host.len() > 255
        || !host
            .bytes()
            .all(|b| b.is_ascii_graphic() && b != b'/' && b != b'@')
    {
        return Err(ProxyError::Protocol("invalid target host name"));
    }
    if cfg.kind == ProxyKind::None || cfg.proxy_hostnames {
        return Ok(ProxyTarget::Host(host, port));
    }
    match tokio::net::lookup_host((host.as_str(), port)).await?.next() {
        Some(a) => Ok(ProxyTarget::Addr(a)),
        None => Err(ProxyError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            "target host name did not resolve",
        ))),
    }
}

async fn dial_proxy(cfg: &ProxyConfig) -> Result<TcpStream, ProxyError> {
    cfg.validate()
        .map_err(|_| ProxyError::Protocol("invalid proxy configuration"))?;
    Ok(TcpStream::connect((cfg.host.as_str(), cfg.port)).await?)
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
pub async fn udp_associate(cfg: &ProxyConfig, timeout: Duration) -> Result<Socks5Udp, ProxyError> {
    if cfg.kind != ProxyKind::Socks5 {
        return Err(ProxyError::Unsupported);
    }
    let inner = async {
        let mut control = dial_proxy(cfg).await?;
        socks5::negotiate(&mut control, cfg).await?;
        // DST.ADDR 0.0.0.0:0: the client's UDP address is not known yet.
        let any = ProxyTarget::Addr(SocketAddr::from(([0, 0, 0, 0], 0)));
        let bnd = socks5::request(&mut control, socks5::CMD_UDP_ASSOCIATE, &any).await?;
        let Some(mut relay) = bnd else {
            return Err(ProxyError::Unsupported);
        };
        if relay.ip().is_unspecified() {
            relay.set_ip(control.peer_addr()?.ip());
        }
        Ok(Socks5Udp { control, relay })
    };
    match tokio::time::timeout(timeout, inner).await {
        Ok(r) => r,
        Err(_) => Err(ProxyError::Timeout),
    }
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
}
