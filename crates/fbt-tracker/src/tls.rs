//! TLS connector selection: `ring` by default, pure-Rust RustCrypto with
//! `tls-rustcrypto`. Without either feature `https` is unavailable.

#[cfg(any(feature = "tls-ring", feature = "tls-rustcrypto"))]
mod enabled {
    use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};

    use crate::TrackerError;
    use crate::connector::ProxyConnector;

    pub(crate) type Connector = HttpsConnector<ProxyConnector>;
    pub(crate) const HTTPS_AVAILABLE: bool = true;

    #[cfg(feature = "tls-rustcrypto")]
    fn provider() -> rustls::crypto::CryptoProvider {
        rustls_rustcrypto::provider()
    }

    #[cfg(all(feature = "tls-ring", not(feature = "tls-rustcrypto")))]
    fn provider() -> rustls::crypto::CryptoProvider {
        rustls::crypto::ring::default_provider()
    }

    /// TLS (for `https`) over `c`.
    pub(crate) fn build_connector(c: ProxyConnector) -> Result<Connector, TrackerError> {
        Ok(HttpsConnectorBuilder::new()
            .with_provider_and_webpki_roots(provider())
            .map_err(std::io::Error::other)?
            .https_or_http()
            .enable_http1()
            .wrap_connector(c))
    }
}

#[cfg(any(feature = "tls-ring", feature = "tls-rustcrypto"))]
pub(crate) use enabled::*;

#[cfg(not(any(feature = "tls-ring", feature = "tls-rustcrypto")))]
mod disabled {
    use crate::TrackerError;
    use crate::connector::ProxyConnector;

    pub(crate) type Connector = ProxyConnector;
    pub(crate) const HTTPS_AVAILABLE: bool = false;

    /// `c` itself: plain HTTP only.
    pub(crate) fn build_connector(c: ProxyConnector) -> Result<Connector, TrackerError> {
        Ok(c)
    }
}

#[cfg(not(any(feature = "tls-ring", feature = "tls-rustcrypto")))]
pub(crate) use disabled::*;
