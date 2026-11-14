//! Which addresses are inside the local network, so a client that reached a public
//! host is not sent inward by what that host tells it to follow.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Whether `ip` is inside the local network: loopback, RFC 1918 (`10/8`,
/// `172.16/12`, `192.168/16`), IPv6 unique-local (`fc00::/7`) or site-local
/// (`fec0::/10`), link-local (IPv4 `169.254/16`, IPv6 `fe80::/10`), CGNAT
/// (`100.64/10`), the benchmarking range (`198.18/15`), `0.0.0.0/8`, multicast,
/// broadcast or the unspecified address: everything a host outside it could not be
/// asked to serve, and what a client is told not to fetch from on somebody else's
/// say-so.
///
/// An IPv6 address that only spells an IPv4 one is judged as that IPv4 address, so the
/// same host is internal whichever family it is written in: `::ffff:127.0.0.1` (mapped),
/// `::127.0.0.1` (the deprecated IPv4-compatibility form), `64:ff9b::127.0.0.1` (NAT64's
/// well-known prefix) and `2002:7f00:1::` (6to4) are all `127.0.0.1`. The whole NAT64
/// local-use prefix `64:ff9b:1::/48` (RFC 8215) is internal whatever follows its `/48`:
/// it is never routed globally, and RFC 6052 lets an embedded IPv4 sit in several
/// places under it, so no guess at the layout is made. A Teredo address (`2001::/32`,
/// whose low bits hold its endpoint inverted rather than plain) is not folded, and
/// neither is any other prefix that carries an IPv4 in bits other than the last 32.
pub fn is_internal(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => internal_v4(v4),
        // `64:ff9b:1::/48` (NAT64 local use): inside by definition, before any folding.
        IpAddr::V6(v6) if nat64_local_use(v6) => true,
        // An address that names an IPv4 host: judge that, not the IPv6 form it is
        // spelled in, which on its own would look like a public one.
        IpAddr::V6(v6) => embedded_v4(v6).map_or_else(|| internal_v6(v6), internal_v4),
    }
}

/// `64:ff9b:1::/48`: the NAT64 local-use prefix (RFC 8215). Addresses under it are
/// site-local, so the whole range is internal regardless of what its bits past the
/// `/48` spell.
fn nat64_local_use(ip: Ipv6Addr) -> bool {
    let [first, second, third, ..] = ip.segments();
    first == 0x0064 && second == 0xff9b && third == 1
}

/// The IPv4 address an IPv6 address names, when it is one of the forms that hold an IPv4
/// in their last 32 bits (`::ffff:a.b.c.d`, `::a.b.c.d`, NAT64) or in the 32 after the
/// prefix (6to4). `None` for an address that is IPv6 only.
fn embedded_v4(ip: Ipv6Addr) -> Option<Ipv4Addr> {
    let [first, second, third, fourth, fifth, sixth, low, high] = ip.segments();
    // `::ffff:a.b.c.d` (mapped) and `::a.b.c.d` (IPv4-compatibility): nothing above the
    // low 32 bits but `ffff` or zeros. `::1` and `::` come out as `127.0.0.1` and
    // `0.0.0.0`, which are internal either way they are read.
    if first == 0
        && second == 0
        && third == 0
        && fourth == 0
        && fifth == 0
        && (sixth == 0 || sixth == 0xffff)
    {
        return Some(v4_of(low, high));
    }
    // NAT64's well-known prefix `64:ff9b::/96`, whose translated address sits in the
    // low 32 bits. The local-use `64:ff9b:1::/48` is internal whole-range before any
    // folding (see `nat64_local_use`). Any other `64:ff9b:...` is not a NAT64 address
    // and stays IPv6.
    let nat64 = first == 0x0064
        && second == 0xff9b
        && third == 0
        && fourth == 0
        && fifth == 0
        && sixth == 0;
    if nat64 {
        return Some(v4_of(low, high));
    }
    // 6to4 (`2002::/16`): the IPv4 address of the border router is the second and third
    // segments.
    if first == 0x2002 {
        return Some(Ipv4Addr::new(
            (second >> 8) as u8,
            second as u8,
            (third >> 8) as u8,
            third as u8,
        ));
    }
    None
}

/// An IPv4 address out of the two segments that hold it, high byte of each first.
fn v4_of(low: u16, high: u16) -> Ipv4Addr {
    Ipv4Addr::new((low >> 8) as u8, low as u8, (high >> 8) as u8, high as u8)
}

fn internal_v4(ip: Ipv4Addr) -> bool {
    let [first, second, ..] = ip.octets();
    // `100.64.0.0/10` (carrier-grade NAT) and `198.18.0.0/15` (benchmarking), neither
    // of which `is_private` covers.
    let cgnat = first == 100 && (second & 0xc0) == 0x40;
    let benchmarking = first == 198 && (second & 0xfe) == 18;
    // `0.0.0.0/8`: "this host on this network", which `is_unspecified` only covers at
    // `0.0.0.0` itself.
    ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_multicast()
        || ip.is_broadcast()
        || cgnat
        || benchmarking
        || first == 0
}

fn internal_v6(ip: Ipv6Addr) -> bool {
    let [first, ..] = ip.segments();
    ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || is_ula(first)
        || is_site_local(first)
        || is_link_local(first)
}

/// `fc00::/7`: unique-local addresses.
fn is_ula(first: u16) -> bool {
    first & 0xfe00 == 0xfc00
}

/// `fec0::/10`: site-local addresses (deprecated, still routed in places).
fn is_site_local(first: u16) -> bool {
    first & 0xffc0 == 0xfec0
}

/// `fe80::/10`: link-local addresses.
fn is_link_local(first: u16) -> bool {
    first & 0xffc0 == 0xfe80
}

#[cfg(test)]
mod tests {
    use super::is_internal;
    use std::net::IpAddr;

    /// The classifier's table: loopback, RFC 1918, IPv6 unique-local, site-local and
    /// link-local, CGNAT, the benchmarking range, `0.0.0.0/8`, the unspecified address,
    /// multicast and broadcast are internal; what a public host answers with is not. An
    /// IPv4 address spelled in an IPv6 form — mapped, IPv4-compatibility, NAT64 or 6to4
    /// — is judged as that IPv4 address, in both directions.
    #[test]
    fn is_internal_table() {
        let internal = [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            // The far edges of CGNAT (`100.64.0.0/10`) and of benchmarking (`198.18/15`).
            "100.127.255.255",
            "198.18.0.1",
            "198.19.255.255",
            // `0.0.0.0/8`, not just `0.0.0.0`.
            "0.0.0.0",
            "0.1.2.3",
            "224.0.0.1",
            "255.255.255.255",
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
            // The IPv4-compatibility form, and the loopback and unspecified addresses.
            "::127.0.0.1",
            "::1",
            "::",
            // NAT64 (well-known `/96` and local-use `/48`) and 6to4, each holding an
            // internal IPv4.
            "64:ff9b::127.0.0.1",
            "64:ff9b:1:2:3:4:10.0.0.1",
            "2002:7f00:1::",
            // The whole local-use `/48` is internal whatever follows it: RFC 6052
            // allows layouts where an embedded IPv4 sits somewhere other than the low
            // 32 bits, and no guess is made — the range is local-use by definition.
            "64:ff9b:1:7f00:1::",
            "fc00::1",
            // Site-local (`fec0::/10`), up to the end of the range.
            "fec0::1",
            "feff::1",
            "fe80::1",
            "ff02::1",
        ];
        for s in internal {
            let ip: IpAddr = s.parse().unwrap();
            assert!(is_internal(ip), "{s} should be internal");
        }
        let public = [
            "8.8.8.8",
            "1.1.1.1",
            "2001:4860::8888",
            // Just outside CGNAT (`100.64.0.0/10`, below and above) and just past
            // benchmarking.
            "100.63.255.255",
            "100.128.0.1",
            "198.20.0.1",
            "::ffff:8.8.8.8",
            "::8.8.8.8",
            // The same IPv4 forms above, holding a public address, and a `64:ff9b:...`
            // that is not one of the NAT64 prefixes (so nothing is folded out of it).
            "64:ff9b::8.8.8.8",
            "64:ff9b:2:3:4:5:127.0.0.1",
            "2002:808:808::",
            "2001:db8::1",
        ];
        for s in public {
            let ip: IpAddr = s.parse().unwrap();
            assert!(!is_internal(ip), "{s} should be public");
        }
    }
}
