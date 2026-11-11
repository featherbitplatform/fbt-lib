//! SSDP: the M-SEARCH request and a strict parser for its replies.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// The SSDP multicast group and port.
pub const SSDP_V4: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(239, 255, 255, 250)), 1900);

/// Largest reply datagram considered; longer ones are malformed.
pub const MAX_DATAGRAM: usize = 8192;
/// Header lines read from a reply.
const MAX_HEADERS: usize = 64;

/// Why an SSDP reply was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SsdpError {
    /// Not a well-formed reply.
    #[error("malformed ssdp reply: {0}")]
    Malformed(&'static str),
    /// The status is not `HTTP/1.1 200`.
    #[error("ssdp reply is not a 200")]
    NotOk,
    /// No usable `LOCATION` header.
    #[error("ssdp reply has no location")]
    NoLocation,
    /// The `LOCATION` host is not the responder.
    #[error("ssdp location names another host")]
    Foreign,
}

/// Where a device's description lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    /// The description server's address.
    pub addr: SocketAddr,
    /// The absolute request path.
    pub path: String,
}

/// The `M-SEARCH` datagram for search target `st`.
pub fn search_request(st: &str) -> Vec<u8> {
    format!(
        "M-SEARCH * HTTP/1.1\r\nHOST: {SSDP_V4}\r\nST: {st}\r\nMAN: \"ssdp:discover\"\r\nMX: 2\r\n\r\n"
    )
    .into_bytes()
}

/// An absolute path: starts with `/`, no whitespace or control characters.
pub fn valid_path(path: &str) -> bool {
    path.starts_with('/') && !path.chars().any(|c| c.is_whitespace() || c.is_control())
}

/// Split `http://<ip literal>[:port][/path]` into address and path. Only the
/// `http` scheme and IP literals are accepted.
pub fn parse_http_url(url: &str) -> Result<(SocketAddr, String), SsdpError> {
    let bad = SsdpError::Malformed("bad location url");
    let scheme = url.get(..7).ok_or(bad)?;
    if !scheme.eq_ignore_ascii_case("http://") {
        return Err(bad);
    }
    let rest = url.get(7..).ok_or(bad)?;
    let (auth, path) = match rest.find('/') {
        Some(i) => (rest.get(..i).ok_or(bad)?, rest.get(i..).ok_or(bad)?),
        None => (rest, "/"),
    };
    let addr = match auth.parse::<SocketAddr>() {
        Ok(a) => a,
        Err(_) => SocketAddr::new(auth.parse::<IpAddr>().map_err(|_| bad)?, 80),
    };
    if addr.port() == 0 || !valid_path(path) {
        return Err(bad);
    }
    Ok((addr, path.to_owned()))
}

/// Parse a reply to an M-SEARCH received from `from`. The `LOCATION` host
/// must be `from`'s IP: a device cannot point us at anyone else.
pub fn parse_search_response(data: &[u8], from: SocketAddr) -> Result<Location, SsdpError> {
    if data.len() > MAX_DATAGRAM {
        return Err(SsdpError::Malformed("datagram too large"));
    }
    let text = std::str::from_utf8(data).map_err(|_| SsdpError::Malformed("not utf-8"))?;
    let mut lines = text.split('\n').map(|l| l.strip_suffix('\r').unwrap_or(l));
    let mut status = lines.next().unwrap_or_default().split(' ');
    if status.next() != Some("HTTP/1.1") || status.next() != Some("200") {
        return Err(SsdpError::NotOk);
    }
    let mut location = None;
    for line in lines.take(MAX_HEADERS) {
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && location.is_none()
            && name.trim().eq_ignore_ascii_case("location")
        {
            location = Some(value.trim());
        }
    }
    let (addr, path) = parse_http_url(location.ok_or(SsdpError::NoLocation)?)?;
    if addr.ip() != from.ip() {
        return Err(SsdpError::Foreign);
    }
    Ok(Location { addr, path })
}

#[cfg(test)]
mod tests {
    use super::*;

    const FROM: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1900);

    #[test]
    fn parses_location() {
        let r = b"HTTP/1.1 200 OK\r\nCACHE-CONTROL: max-age=100\r\nlocation: http://127.0.0.1:5000/rootDesc.xml\r\nST: x\r\n\r\n";
        let l = parse_search_response(r, FROM).unwrap();
        assert_eq!(l.addr, "127.0.0.1:5000".parse().unwrap());
        assert_eq!(l.path, "/rootDesc.xml");
        let r = b"HTTP/1.1 200 OK\r\nLOCATION: http://127.0.0.1\r\n\r\n";
        let l = parse_search_response(r, FROM).unwrap();
        assert_eq!((l.addr.port(), l.path.as_str()), (80, "/"));
    }

    #[test]
    fn location_must_name_the_responder() {
        let r = b"HTTP/1.1 200 OK\r\nLOCATION: http://10.9.9.9/desc.xml\r\n\r\n";
        assert_eq!(parse_search_response(r, FROM), Err(SsdpError::Foreign));
        let r = b"HTTP/1.1 200 OK\r\nLOCATION: http://localhost/desc.xml\r\n\r\n";
        assert!(parse_search_response(r, FROM).is_err());
    }

    #[test]
    fn non_200_rejected() {
        for r in [
            &b"HTTP/1.1 404 Not Found\r\nLOCATION: http://127.0.0.1/\r\n\r\n"[..],
            b"NOTIFY * HTTP/1.1\r\nLOCATION: http://127.0.0.1/\r\n\r\n",
            b"",
        ] {
            assert_eq!(parse_search_response(r, FROM), Err(SsdpError::NotOk));
        }
        let r = b"HTTP/1.1 200 OK\r\nST: x\r\n\r\n";
        assert_eq!(parse_search_response(r, FROM), Err(SsdpError::NoLocation));
        for url in [
            "https://127.0.0.1/",
            "http://127.0.0.1:0/",
            "http://127.0.0.1/a b",
        ] {
            let r = format!("HTTP/1.1 200 OK\r\nLOCATION: {url}\r\n\r\n");
            assert!(parse_search_response(r.as_bytes(), FROM).is_err(), "{url}");
        }
    }

    #[test]
    fn search_request_shape() {
        let s = String::from_utf8(search_request("urn:x:1")).unwrap();
        assert!(s.starts_with("M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\n"));
        assert!(s.contains("ST: urn:x:1\r\n") && s.contains("MAN: \"ssdp:discover\"\r\nMX: 2\r\n"));
    }

    proptest::proptest! {
        #[test]
        fn ssdp_never_panics(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..700)) {
            let _ = parse_search_response(&bytes, FROM);
        }

        #[test]
        fn ssdp_never_panics_near_valid(tail in "[ -~\\r\\n]{0,200}") {
            let msg = format!("HTTP/1.1 200 OK\r\nLOCATION: http://{tail}");
            let _ = parse_search_response(msg.as_bytes(), FROM);
        }
    }
}
