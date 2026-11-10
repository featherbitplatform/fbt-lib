//! Node ids: XOR metric, BEP 42 secure ids, and the local-address exemption.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use rand::Rng;

/// Length of a node id in bytes.
pub const NODE_ID_LEN: usize = 20;

/// A 160-bit DHT node id.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub [u8; NODE_ID_LEN]);

impl NodeId {
    /// A uniformly random id (not BEP 42 bound; see [`bep42_id`]).
    pub fn random(rng: &mut impl Rng) -> NodeId {
        let mut id = [0u8; NODE_ID_LEN];
        rng.fill_bytes(&mut id);
        NodeId(id)
    }

    /// The XOR distance to `other`.
    pub fn distance(&self, other: &NodeId) -> [u8; NODE_ID_LEN] {
        let mut d = [0u8; NODE_ID_LEN];
        for ((d, a), b) in d.iter_mut().zip(self.0).zip(other.0) {
            *d = a ^ b;
        }
        d
    }

    /// The number of leading zero bits of the distance (`0..=159`), i.e. the
    /// length of the shared prefix; `None` when the ids are equal.
    pub fn bucket_index(&self, other: &NodeId) -> Option<usize> {
        let mut bits = 0usize;
        for byte in self.distance(other) {
            if byte != 0 {
                return Some(bits + byte.leading_zeros() as usize);
            }
            bits += 8;
        }
        None
    }

    /// BEP 42: whether this id was derived from `ip`. Local addresses
    /// ([`is_local`]) are exempt and always valid.
    pub fn is_valid_for(&self, ip: IpAddr) -> bool {
        if is_local(ip) {
            return true;
        }
        let [a, b, c, .., r] = self.0;
        let [p0, p1, p2, _] = bep42_prefix(ip, r).to_be_bytes();
        a == p0 && b == p1 && (c & 0xf8) == (p2 & 0xf8)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// BEP 42 IPv4 mask, applied to the 4 address bytes.
const BEP42_MASK_V4: [u8; 4] = 0x030f_3fff_u32.to_be_bytes();
/// BEP 42 IPv6 mask, applied to the first 8 address bytes.
const BEP42_MASK_V6: [u8; 8] = 0x0103_070f_1f3f_7fff_u64.to_be_bytes();

/// IPv4-mapped IPv6 addresses are treated as the IPv4 address they carry.
fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, IpAddr::V4),
        v4 => v4,
    }
}

/// `crc32c((ip & mask) | (r & 7) << top-3-bits)`, the source of the first 21
/// id bits.
fn bep42_prefix(ip: IpAddr, r: u8) -> u32 {
    let rbits = (r & 0x07) << 5;
    match canonical(ip) {
        IpAddr::V4(v4) => {
            let mut buf = v4.octets();
            for (o, m) in buf.iter_mut().zip(BEP42_MASK_V4) {
                *o &= m;
            }
            if let Some(first) = buf.first_mut() {
                *first |= rbits;
            }
            crc32c(&buf)
        }
        IpAddr::V6(v6) => {
            let mut buf = [0u8; 8];
            for ((o, s), m) in buf.iter_mut().zip(v6.octets()).zip(BEP42_MASK_V6) {
                *o = s & m;
            }
            if let Some(first) = buf.first_mut() {
                *first |= rbits;
            }
            crc32c(&buf)
        }
    }
}

/// A BEP 42 id for `ip` with random byte `r`: the first 21 bits come from
/// the CRC32C of the masked address, the rest is random, the last byte is `r`.
pub fn bep42_id(ip: IpAddr, r: u8, rng: &mut impl Rng) -> NodeId {
    let mut id = NodeId::random(rng).0;
    let [p0, p1, p2, _] = bep42_prefix(ip, r).to_be_bytes();
    let [a, b, c, ..] = &mut id;
    *a = p0;
    *b = p1;
    *c = (p2 & 0xf8) | (*c & 0x07);
    if let Some(last) = id.last_mut() {
        *last = r;
    }
    NodeId(id)
}

/// Addresses exempt from BEP 42: loopback, RFC 1918, link-local, CGNAT
/// (`100.64/10`), IPv6 unique-local (`fc00::/7`) and link-local (`fe80::/10`).
/// IPv4-mapped IPv6 addresses are judged as IPv4.
pub fn is_local(ip: IpAddr) -> bool {
    match canonical(ip) {
        IpAddr::V4(v4) => is_local_v4(v4),
        IpAddr::V6(v6) => is_local_v6(v6),
    }
}

fn is_local_v4(ip: Ipv4Addr) -> bool {
    let [a, b, ..] = ip.octets();
    let cgnat = a == 100 && (b & 0xc0) == 64;
    ip.is_loopback() || ip.is_private() || ip.is_link_local() || cgnat
}

fn is_local_v6(ip: Ipv6Addr) -> bool {
    let first = ip.segments()[0];
    ip.is_loopback() || (first & 0xfe00) == 0xfc00 || (first & 0xffc0) == 0xfe80
}

/// CRC32C (Castagnoli) lookup table for the reflected polynomial.
const CRC32C_TABLE: [u32; 256] = crc32c_table();

const fn crc32c_table() -> [u32; 256] {
    const POLY: u32 = 0x82F6_3B78;
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        // `i < 256` is the loop bound: always in range.
        #[allow(clippy::indexing_slicing)]
        {
            table[i] = crc;
        }
        i += 1;
    }
    table
}

/// Table-driven CRC32C (Castagnoli, reflected polynomial `0x82F63B78`).
pub(crate) fn crc32c(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        let idx = usize::from((crc as u8) ^ byte);
        crc = (crc >> 8) ^ CRC32C_TABLE.get(idx).copied().unwrap_or(0);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use std::net::IpAddr;

    fn rng() -> StdRng {
        StdRng::seed_from_u64(42)
    }

    fn id_from_hex(s: &str) -> NodeId {
        let mut id = [0u8; 20];
        for (i, b) in id.iter_mut().enumerate() {
            *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).unwrap();
        }
        NodeId(id)
    }

    #[test]
    fn bep42_vectors() {
        for (ip, r, id) in [
            (
                "124.31.75.21",
                1u8,
                "5fbfbff10c5d6a4ec8a88e4c6ab4c28b95eee401",
            ),
            (
                "21.75.31.124",
                86,
                "5a3ce9c14e7a08645677bbd1cfe7d8f956d53256",
            ),
            (
                "65.23.51.170",
                22,
                "a5d43220bc8f112a3d426c84764f8c2a1150e616",
            ),
            (
                "84.124.73.14",
                65,
                "1b0321dd1bb1fe518101ceef99462b947a01ff41",
            ),
            (
                "43.213.53.83",
                90,
                "e56f6cbf5b7c4be0237986d5243b87aa6d51305a",
            ),
        ] {
            let ip: IpAddr = ip.parse().unwrap();
            let want = id_from_hex(id);
            assert!(want.is_valid_for(ip));
            let ours = bep42_id(ip, r, &mut rng());
            assert_eq!(&ours.0[..2], &want.0[..2]);
            assert_eq!(ours.0[2] & 0xf8, want.0[2] & 0xf8);
            assert_eq!(ours.0[19], r);
        }
        assert!(!NodeId([0; 20]).is_valid_for("124.31.75.21".parse().unwrap()));
    }

    #[test]
    fn crc32c_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn local_addresses_exempt() {
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "192.168.0.1",
            "172.20.0.5",
            "::1",
            "fd00::1",
        ] {
            assert!(is_local(ip.parse().unwrap()));
        }
        assert!(!is_local("8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn bucket_index_by_shared_prefix() {
        let a = NodeId([0; 20]);
        let mut b = [0; 20];
        b[0] = 0x80;
        assert_eq!(a.bucket_index(&NodeId(b)), Some(0));
        let mut c = [0; 20];
        c[19] = 1;
        assert_eq!(a.bucket_index(&NodeId(c)), Some(159));
        assert_eq!(a.bucket_index(&a), None);
    }

    #[test]
    fn bep42_ids_validate_for_own_ip_only() {
        let mut r = rng();
        for ip in [
            "124.31.75.21",
            "2001:db8:1234:5678::1",
            "::ffff:124.31.75.21",
        ] {
            let ip: IpAddr = ip.parse().unwrap();
            for rand_byte in [0u8, 7, 0xff] {
                assert!(bep42_id(ip, rand_byte, &mut r).is_valid_for(ip));
            }
        }
        let id = bep42_id("124.31.75.21".parse().unwrap(), 1, &mut r);
        assert!(!id.is_valid_for("21.75.31.124".parse().unwrap()));
        // mapped and plain IPv4 agree
        assert!(id.is_valid_for("::ffff:124.31.75.21".parse().unwrap()));
        let v6 = bep42_id("2001:db8::1".parse().unwrap(), 3, &mut r);
        assert!(!v6.is_valid_for("2a00:1450::1".parse().unwrap()));
    }

    #[test]
    fn local_ranges() {
        for ip in [
            "169.254.1.1",
            "100.64.0.1",
            "100.127.255.255",
            "fe80::1",
            "fc00::1",
            "::ffff:10.0.0.1",
        ] {
            assert!(is_local(ip.parse().unwrap()), "{ip}");
        }
        for ip in [
            "100.63.255.255",
            "100.128.0.0",
            "172.32.0.1",
            "2001:db8::1",
            "fec0::1",
        ] {
            assert!(!is_local(ip.parse().unwrap()), "{ip}");
        }
        // local addresses accept any id
        assert!(NodeId([0; 20]).is_valid_for("192.168.1.1".parse().unwrap()));
    }

    #[test]
    fn distance_and_hex() {
        let a = NodeId([0xf0; 20]);
        let b = NodeId([0x0f; 20]);
        assert_eq!(a.distance(&b), [0xff; 20]);
        assert_eq!(a.bucket_index(&b), Some(0));
        let mut c = [0u8; 20];
        c[1] = 0x10;
        assert_eq!(NodeId([0; 20]).bucket_index(&NodeId(c)), Some(11));
        assert_eq!(format!("{a}"), "f0".repeat(20));
        assert_eq!(format!("{b:?}"), "0f".repeat(20));
        assert_ne!(
            NodeId::random(&mut rng()),
            NodeId::random(&mut StdRng::seed_from_u64(1))
        );
    }
}
