//! TLS connector selection: `ring` by default, pure-Rust RustCrypto with
//! `tls-rustcrypto`. Without either feature `https` is unavailable.

#[cfg(any(feature = "tls-ring", feature = "tls-rustcrypto"))]
mod enabled {
    use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
    use hyper_util::client::legacy::connect::HttpConnector;

    use crate::TrackerError;

    pub(crate) type Connector = HttpsConnector<HttpConnector>;
    pub(crate) const HTTPS_AVAILABLE: bool = true;

    #[cfg(feature = "tls-rustcrypto")]
    fn provider() -> rustls::crypto::CryptoProvider {
        rustls_rustcrypto::provider()
    }

    #[cfg(all(feature = "tls-ring", not(feature = "tls-rustcrypto")))]
    fn provider() -> rustls::crypto::CryptoProvider {
        rustls::crypto::ring::default_provider()
    }

    pub(crate) fn build_connector() -> Result<Connector, TrackerError> {
        Ok(HttpsConnectorBuilder::new()
            .with_provider_and_webpki_roots(provider())
            .map_err(std::io::Error::other)?
            .https_or_http()
            .enable_http1()
            .build())
    }
}

#[cfg(any(feature = "tls-ring", feature = "tls-rustcrypto"))]
pub(crate) use enabled::*;

#[cfg(not(any(feature = "tls-ring", feature = "tls-rustcrypto")))]
mod disabled {
    use hyper_util::client::legacy::connect::HttpConnector;

    use crate::TrackerError;

    pub(crate) type Connector = HttpConnector;
    pub(crate) const HTTPS_AVAILABLE: bool = false;

    pub(crate) fn build_connector() -> Result<Connector, TrackerError> {
        Ok(HttpConnector::new())
    }
}

#[cfg(not(any(feature = "tls-ring", feature = "tls-rustcrypto")))]
pub(crate) use disabled::*;
