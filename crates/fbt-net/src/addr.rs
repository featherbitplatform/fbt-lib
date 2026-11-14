//! Which addresses are inside the local network, so a client that reached a public
//! host is not sent inward by what that host tells it to follow.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Whether `ip` is inside the local network: loopback, RFC 1918 (`10/8`,
/// `172.16/12`, `192.168/16`), IPv6 unique-local (`fc00::/7`), link-local (IPv4
/// `169.254/16`, IPv6 `fe80::/10`), CGNAT (`100.64/10`), the unspecified address,
/// multicast, or broadcast: everything a host outside it could not be asked to serve,
/// and what a client is told not to fetch from on somebody else's say-so.
///
/// An IPv4-mapped IPv6 address (`::ffff:127.0.0.1`) is judged as the IPv4 address it
/// names, so the same host is internal whichever family it is spelled in. The
/// deprecated IPv4-compatibility form (`::127.0.0.1`, with no `ffff`) is not treated
/// as IPv4: nothing answers on it today, and an address that is not routed to a LAN
/// service is not the one this guards against.
pub fn is_internal(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => internal_v4(v4),
        // An IPv4-mapped address names an IPv4 host: judge that, not the IPv6 form.
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map_or_else(|| internal_v6(v6), internal_v4),
    }
}

fn internal_v4(ip: Ipv4Addr) -> bool {
    let [a, b, ..] = ip.octets();
    // `100.64.0.0/10` (carrier-grade NAT), which `is_private` does not cover.
    let cgnat = a == 100 && (b & 0xc0) == 0x40;
    ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_multicast()
        || ip.is_broadcast()
        || cgnat
}

fn internal_v6(ip: Ipv6Addr) -> bool {
    let [first, ..] = ip.segments();
    ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || is_ula(first)
        || is_link_local(first)
}

/// `fc00::/7`: unique-local addresses.
fn is_ula(first: u16) -> bool {
    first & 0xfe00 == 0xfc00
}

/// `fe80::/10`: link-local addresses.
fn is_link_local(first: u16) -> bool {
    first & 0xffc0 == 0xfe80
}

#[cfg(test)]
mod tests {
    use super::is_internal;
    use std::net::IpAddr;

    /// The classifier's table: loopback, RFC 1918, IPv6 unique-local and link-local,
    /// CGNAT, the unspecified address, multicast and broadcast are internal; what a
    /// public host answers with is not. An IPv4-mapped IPv6 address is judged as the
    /// IPv4 address it names, in both directions.
    #[test]
    fn is_internal_table() {
        let internal = [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "224.0.0.1",
            "255.255.255.255",
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
            "::1",
            "fc00::1",
            "fe80::1",
            "ff02::1",
            "::",
        ];
        for s in internal {
            let ip: IpAddr = s.parse().unwrap();
            assert!(is_internal(ip), "{s} should be internal");
        }
        let public = [
            "8.8.8.8",
            "1.1.1.1",
            "2001:4860::8888",
            "100.128.0.1",
            "::ffff:8.8.8.8",
        ];
        for s in public {
            let ip: IpAddr = s.parse().unwrap();
            assert!(!is_internal(ip), "{s} should be public");
        }
    }
}
