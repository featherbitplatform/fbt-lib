//! UPnP IGD: a bounded HTTP/1.1 client, a bounded XML scanner, SSDP discovery
//! and SOAP port mapping.
//!
//! Everything a LAN device sends is untrusted: every list, line and buffer
//! here has a named cap, every exchange a timeout.

mod http;
mod soap;
mod ssdp;
mod xml;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::Instant;

use crate::MapProtocol;

#[doc(hidden)]
pub use http::{
    HTTP_TIMEOUT, HttpError, HttpRequest, HttpResponse, MAX_HTTP_RESPONSE, parse_response, request,
    request_with_timeout,
};
#[doc(hidden)]
pub use ssdp::{Location, SSDP_V4, SsdpError, parse_search_response};
#[doc(hidden)]
pub use xml::{Service, element_text, find_services};

/// Searches sent per discovery round-trip set, at most.
const SEARCH_ROUNDS: u32 = 3;
/// Gap between search rounds.
const SEARCH_GAP: Duration = Duration::from_millis(500);
/// Search targets, newest first.
const SEARCH_TARGETS: [&str; 2] = [
    "urn:schemas-upnp-org:device:InternetGatewayDevice:2",
    "urn:schemas-upnp-org:device:InternetGatewayDevice:1",
];
/// Accepted WAN connection services, in order of preference.
const SERVICE_PREFERENCE: [&str; 3] = [
    "urn:schemas-upnp-org:service:WANIPConnection:2",
    "urn:schemas-upnp-org:service:WANIPConnection:1",
    "urn:schemas-upnp-org:service:WANPPPConnection:1",
];
/// Extra random external ports tried after a conflict (error 718).
const CONFLICT_RETRIES: u32 = 4;
/// The mapping description sent to the router.
const MAPPING_DESCRIPTION: &str = "fbt-lib";

/// Why a UPnP operation failed.
#[derive(Debug, thiserror::Error)]
pub enum UpnpError {
    /// An HTTP exchange failed.
    #[error("upnp http: {0}")]
    Http(#[from] HttpError),
    /// An SSDP reply was unusable.
    #[error("upnp ssdp: {0}")]
    Ssdp(SsdpError),
    /// The device has no usable WAN connection service.
    #[error("no usable WAN connection service")]
    NoService,
    /// The device pointed us at another host.
    #[error("device names another host")]
    Foreign,
    /// The device answered with a SOAP fault.
    #[error("soap fault {0}")]
    Soap(u32),
    /// Discovery found nothing in time.
    #[error("upnp timeout")]
    Timeout,
}

/// A discovered Internet Gateway Device's WAN connection service.
#[derive(Debug, Clone)]
pub struct Igd {
    control: SocketAddr,
    path: String,
    service_type: String,
    local_ip: Ipv4Addr,
    http_timeout: Duration,
}

impl Igd {
    /// Bound each HTTP exchange by `d` instead of [`HTTP_TIMEOUT`].
    #[doc(hidden)]
    pub fn with_http_timeout(mut self, d: Duration) -> Igd {
        self.http_timeout = d;
        self
    }

    async fn call(&self, (action, body): (String, Vec<u8>)) -> Result<Option<String>, UpnpError> {
        let headers = [
            ("Content-Type", "text/xml; charset=\"utf-8\""),
            ("SOAPAction", action.as_str()),
        ];
        let req = HttpRequest {
            method: "POST",
            host: self.control,
            path: &self.path,
            headers: &headers,
            body: &body,
        };
        let (resp, _) = request_with_timeout(&req, self.http_timeout).await?;
        soap::soap_result(&resp)
    }

    /// Map `port` for `p`, asking for the same external port first. Returns
    /// the external port granted. Conflicts (718) retry on up to four random
    /// ports; routers that only allow permanent leases (725) get one retry
    /// with lease 0.
    pub async fn add_mapping(
        &self,
        p: MapProtocol,
        port: u16,
        lease: u32,
    ) -> Result<u16, UpnpError> {
        let (mut external, mut lease_now) = (port, lease);
        let (mut conflicts, mut zero_tried) = (0, false);
        loop {
            let req = soap::add_port_mapping(
                &self.service_type,
                external,
                p,
                port,
                self.local_ip,
                lease_now,
                MAPPING_DESCRIPTION,
            );
            match self.call(req).await {
                Ok(_) => return Ok(external),
                Err(UpnpError::Soap(718)) if conflicts < CONFLICT_RETRIES => {
                    conflicts += 1;
                    external = rand::random_range(1025..=u16::MAX);
                }
                Err(UpnpError::Soap(725)) if !zero_tried && lease_now != 0 => {
                    zero_tried = true;
                    lease_now = 0;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Remove the mapping of `external` for `p`.
    pub async fn delete_mapping(&self, p: MapProtocol, external: u16) -> Result<(), UpnpError> {
        self.call(soap::delete_port_mapping(&self.service_type, external, p))
            .await
            .map(|_| ())
    }

    /// The router's external IPv4 address.
    pub async fn external_ip(&self) -> Result<Ipv4Addr, UpnpError> {
        self.call(soap::get_external_ip(&self.service_type))
            .await?
            .and_then(|s| s.trim().parse().ok())
            .ok_or(UpnpError::Http(HttpError::Malformed(
                "no external ip address",
            )))
    }
}

/// Find an IGD by sending SSDP searches to `ssdp` (normally [`SSDP_V4`]) and
/// fetching the first description whose `LOCATION` names the responder.
pub async fn discover(ssdp: SocketAddr, timeout: Duration) -> Result<Igd, UpnpError> {
    discover_with(ssdp, timeout, HTTP_TIMEOUT).await
}

/// [`discover`] with a chosen bound for the description fetch (and for later
/// SOAP calls on the returned [`Igd`]).
#[doc(hidden)]
pub async fn discover_with(
    ssdp: SocketAddr,
    timeout: Duration,
    http_timeout: Duration,
) -> Result<Igd, UpnpError> {
    tokio::time::timeout(timeout, search(ssdp, http_timeout))
        .await
        .map_err(|_| UpnpError::Timeout)?
}

async fn search(ssdp: SocketAddr, http_timeout: Duration) -> Result<Igd, UpnpError> {
    let bind: SocketAddr = match ssdp {
        SocketAddr::V4(_) => (Ipv4Addr::UNSPECIFIED, 0).into(),
        SocketAddr::V6(_) => (std::net::Ipv6Addr::UNSPECIFIED, 0).into(),
    };
    let sock = UdpSocket::bind(bind).await.map_err(HttpError::Io)?;
    let mut buf = vec![0u8; ssdp::MAX_DATAGRAM + 1];
    for _ in 0..SEARCH_ROUNDS {
        for st in SEARCH_TARGETS {
            if let Err(e) = sock.send_to(&ssdp::search_request(st), ssdp).await {
                tracing::debug!("ssdp send failed: {e}");
            }
        }
        let deadline = Instant::now() + SEARCH_GAP;
        loop {
            let Ok(recv) = tokio::time::timeout_at(deadline, sock.recv_from(&mut buf)).await else {
                break;
            };
            let Ok((n, from)) = recv else { break };
            let data = buf.get(..n).unwrap_or_default();
            let loc = match parse_search_response(data, from) {
                Ok(loc) => loc,
                Err(e) => {
                    tracing::debug!("ignoring ssdp reply from {from}: {e}");
                    continue;
                }
            };
            if !loc.addr.is_ipv4() {
                continue;
            }
            return describe(&loc, http_timeout).await;
        }
    }
    Err(UpnpError::Timeout)
}

/// Fetch the description at `loc` and pick the WAN connection service.
async fn describe(loc: &Location, http_timeout: Duration) -> Result<Igd, UpnpError> {
    let req = HttpRequest {
        method: "GET",
        host: loc.addr,
        path: &loc.path,
        headers: &[],
        body: b"",
    };
    let (resp, local) = request_with_timeout(&req, http_timeout).await?;
    if resp.status != 200 {
        return Err(HttpError::Malformed("description status").into());
    }
    let IpAddr::V4(local_ip) = local.ip() else {
        return Err(UpnpError::NoService);
    };
    let services = find_services(&String::from_utf8_lossy(&resp.body));
    let svc = SERVICE_PREFERENCE
        .iter()
        .find_map(|want| services.iter().find(|s| s.service_type == *want))
        .ok_or(UpnpError::NoService)?;
    let (control, path) = resolve_control(loc, &svc.control_url)?;
    Ok(Igd {
        control,
        path,
        service_type: svc.service_type.clone(),
        local_ip,
        http_timeout,
    })
}

/// Resolve a `controlURL` against the description's location: an absolute
/// path, or an `http://` URL naming the same host.
fn resolve_control(loc: &Location, url: &str) -> Result<(SocketAddr, String), UpnpError> {
    if url.starts_with('/') {
        return if ssdp::valid_path(url) {
            Ok((loc.addr, url.to_owned()))
        } else {
            Err(HttpError::Malformed("bad control path").into())
        };
    }
    if url
        .get(..7)
        .is_some_and(|s| s.eq_ignore_ascii_case("http://"))
    {
        let (addr, path) = ssdp::parse_http_url(url).map_err(|_| UpnpError::NoService)?;
        return if addr.ip() == loc.addr.ip() {
            Ok((addr, path))
        } else {
            Err(UpnpError::Foreign)
        };
    }
    Err(UpnpError::NoService)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_urls_resolve_safely() {
        let loc = Location {
            addr: "127.0.0.1:5000".parse().unwrap(),
            path: "/d.xml".into(),
        };
        let ok = |u| resolve_control(&loc, u);
        assert_eq!(ok("/ctl").unwrap().1, "/ctl");
        assert_eq!(ok("http://127.0.0.1:6000/c").unwrap().0.port(), 6000);
        assert!(matches!(ok("http://10.0.0.1/c"), Err(UpnpError::Foreign)));
        assert!(matches!(ok("ctl"), Err(UpnpError::NoService)));
        assert!(matches!(
            ok("https://127.0.0.1/c"),
            Err(UpnpError::NoService)
        ));
        assert!(matches!(ok("/a b"), Err(UpnpError::Http(_))));
        assert!(matches!(ok("/a\r\nb"), Err(UpnpError::Http(_))));
    }
}
