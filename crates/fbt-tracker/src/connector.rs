//! The TCP connector under the HTTP clients: hyper's own [`HttpConnector`]
//! when connecting directly, or a tunnel through a SOCKS5 or HTTP CONNECT
//! proxy.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use fbt_net::proxy::{ProxyConfig, ProxyTarget, connect_tcp};
use hyper::Uri;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Connecting = Pin<Box<dyn Future<Output = Result<TokioIo<TcpStream>, BoxError>> + Send>>;

/// Opens the TCP connection for one request URI.
#[derive(Debug, Clone)]
pub(crate) enum ProxyConnector {
    /// hyper's connector (`enforce_http(false)`, so TLS can wrap it), with
    /// its own address fallback.
    Direct(HttpConnector),
    /// Through `proxy`, the whole exchange bounded by `timeout`.
    Proxied {
        /// The proxy to tunnel through.
        proxy: ProxyConfig,
        /// Bound on dialling the proxy and the tunnel negotiation.
        timeout: Duration,
    },
}

impl ProxyConnector {
    /// A direct connector, or one through `proxy` when it names a proxy.
    pub(crate) fn new(proxy: Option<&ProxyConfig>, timeout: Duration) -> ProxyConnector {
        match proxy {
            Some(p) if p.kind != fbt_net::proxy::ProxyKind::None => ProxyConnector::Proxied {
                proxy: p.clone(),
                timeout,
            },
            _ => {
                let mut http = HttpConnector::new();
                http.enforce_http(false);
                ProxyConnector::Direct(http)
            }
        }
    }
}

/// The port of `uri`, else the default of its scheme (443 for `https`, 80
/// otherwise).
fn port_of(uri: &Uri) -> u16 {
    uri.port_u16()
        .unwrap_or(if uri.scheme_str() == Some("https") {
            443
        } else {
            80
        })
}

impl tower_service::Service<Uri> for ProxyConnector {
    type Response = TokioIo<TcpStream>;
    type Error = BoxError;
    type Future = Connecting;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self {
            ProxyConnector::Direct(http) => http.poll_ready(cx).map_err(Into::into),
            ProxyConnector::Proxied { .. } => Poll::Ready(Ok(())),
        }
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        match self {
            ProxyConnector::Direct(http) => {
                let connecting = http.call(uri);
                Box::pin(async move { connecting.await.map_err(Into::into) })
            }
            ProxyConnector::Proxied { proxy, timeout } => {
                let (proxy, timeout) = (proxy.clone(), *timeout);
                Box::pin(async move {
                    let host = uri.host().ok_or("request URI has no host")?;
                    // `connect_tcp` turns an IP literal into an address and,
                    // without `proxy_hostnames`, resolves the name locally.
                    let target = ProxyTarget::Host(host.to_owned(), port_of(&uri));
                    let stream = connect_tcp(&proxy, target, timeout).await?;
                    Ok(TokioIo::new(stream))
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ports_default_by_scheme() {
        let p = |s: &str| port_of(&s.parse::<Uri>().unwrap());
        assert_eq!(p("http://t.example/announce"), 80);
        assert_eq!(p("https://t.example/announce"), 443);
        assert_eq!(p("https://t.example:8443/announce"), 8443);
        assert_eq!(p("http://[::1]:6969/announce"), 6969);
    }
}
