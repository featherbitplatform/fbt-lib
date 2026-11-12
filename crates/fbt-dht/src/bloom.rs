//! BEP 33 bloom filters: the seed and peer counts a `get_peers` scrape
//! answers with.
//!
//! A filter has `m` = 2048 bits and `k` = 2 hash functions. An address is
//! inserted by hashing its bytes (4 for IPv4, 16 for IPv6, network order)
//! with SHA-1; the first two little-endian 16-bit words of the hash, each
//! modulo `m`, are the two bit indices, and bit `i` is bit `i % 8` of byte
//! `i / 8`. With `c` the number of zero bits, BEP 33 estimates the number of
//! addresses inserted as
//!
//! ```text
//! n = ln(c / m) / (k * ln(1 - 1 / m))
//! ```
//!
//! which diverges as the filter fills; the estimate is capped at
//! [`MAX_ESTIMATE`] (6000), which a full filter (`c` = 0) gives.

use std::net::IpAddr;

use sha1::{Digest, Sha1};

/// Bytes in a BEP 33 filter.
pub(crate) const BLOOM_BYTES: usize = 256;
/// Bits in a filter (`m`).
const M: u32 = (BLOOM_BYTES * 8) as u32;
/// Hash functions (`k`).
const K: u32 = 2;
/// The largest estimate: what a full filter gives.
pub const MAX_ESTIMATE: u32 = 6000;

/// A BEP 33 bloom filter of IP addresses: 256 bytes (`m` = 2048 bits,
/// `k` = 2).
#[derive(Clone, PartialEq, Eq)]
pub struct BloomFilter([u8; BLOOM_BYTES]);

impl BloomFilter {
    /// A filter holding `bytes` as received.
    pub fn from_bytes(bytes: [u8; BLOOM_BYTES]) -> Self {
        BloomFilter(bytes)
    }

    /// The filter's bytes, as sent.
    pub fn bytes(&self) -> &[u8; BLOOM_BYTES] {
        &self.0
    }

    /// Adds `ip`.
    pub fn insert(&mut self, ip: IpAddr) {
        let hash = match ip {
            IpAddr::V4(a) => Sha1::digest(a.octets()),
            IpAddr::V6(a) => Sha1::digest(a.octets()),
        };
        for pair in hash.as_chunks::<2>().0.iter().take(K as usize) {
            self.set(u32::from(u16::from_le_bytes(*pair)) % M);
        }
    }

    fn set(&mut self, bit: u32) {
        if let Some(byte) = self.0.get_mut((bit / 8) as usize) {
            *byte |= 1 << (bit % 8);
        }
    }

    /// Adds every address of `other` (a bitwise OR).
    pub fn union(&mut self, other: &BloomFilter) {
        for (a, b) in self.0.iter_mut().zip(other.0.iter()) {
            *a |= b;
        }
    }

    /// BEP 33's estimate before rounding and capping.
    fn estimate_f64(&self) -> f64 {
        let zeros: u32 = self.0.iter().map(|b| b.count_zeros()).sum();
        if zeros == 0 {
            return f64::from(MAX_ESTIMATE);
        }
        let m = f64::from(M);
        (f64::from(zeros) / m).ln() / (f64::from(K) * (1.0 - 1.0 / m).ln())
    }

    /// The estimated number of distinct addresses inserted, rounded and
    /// capped at [`MAX_ESTIMATE`].
    pub fn estimate(&self) -> u32 {
        let e = self.estimate_f64().round();
        if e >= f64::from(MAX_ESTIMATE) {
            MAX_ESTIMATE
        } else {
            // in 0..6000: the cast is exact
            e.max(0.0) as u32
        }
    }
}

impl Default for BloomFilter {
    fn default() -> Self {
        BloomFilter([0; BLOOM_BYTES])
    }
}

impl std::fmt::Debug for BloomFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BloomFilter")
            .field(&self.estimate())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn v4(k: u32) -> IpAddr {
        IpAddr::V4(Ipv4Addr::from(0x0a00_0000 + k))
    }

    #[test]
    fn estimate_tracks_inserts() {
        for n in [100u32, 1000] {
            let mut b = BloomFilter::default();
            for k in 0..n {
                b.insert(v4(k));
            }
            let e = f64::from(b.estimate());
            let n = f64::from(n);
            assert!((e - n).abs() <= n * 0.1, "{n} inserted, estimate {e}");
        }
        assert_eq!(BloomFilter::default().estimate(), 0);
        // re-inserting an address changes nothing
        let mut b = BloomFilter::default();
        b.insert(v4(1));
        b.insert(v4(1));
        assert_eq!(b.estimate(), 1);
        // a full filter gives BEP 33's cap
        assert_eq!(BloomFilter([0xff; BLOOM_BYTES]).estimate(), 6000);
    }

    #[test]
    fn union_is_or() {
        let mut a = BloomFilter::default();
        let mut b = BloomFilter::default();
        for k in 0..50 {
            a.insert(v4(k));
        }
        for k in 50..100 {
            b.insert(v4(k));
        }
        let mut both = BloomFilter::default();
        for k in 0..100 {
            both.insert(v4(k));
        }
        let mut u = a.clone();
        u.union(&b);
        assert_eq!(u, both);
        for ((u, a), b) in u.0.iter().zip(a.0.iter()).zip(b.0.iter()) {
            assert_eq!(*u, a | b);
        }
    }

    /// BEP 33's test vector: 192.0.2.0 to 192.0.2.255 and 2001:DB8:: to
    /// 2001:DB8::3E7 (1256 addresses) give the filter starting with these
    /// 32 bytes and an estimated size of 1224.9308.
    #[test]
    fn bep33_test_vector() {
        const PREFIX: &str = "F6C3F5EAA07FFD91BDE89F777F26FB2BFF37BDB8FB2BBAA2FD3DDDE7BACFFF75";
        let mut b = BloomFilter::default();
        for k in 0..=255u8 {
            b.insert(IpAddr::V4(Ipv4Addr::new(192, 0, 2, k)));
        }
        let base = u128::from(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0));
        for k in 0..0x3e8u128 {
            b.insert(IpAddr::V6(Ipv6Addr::from(base + k)));
        }
        let hex: String = b.0.iter().take(32).map(|x| format!("{x:02X}")).collect();
        assert_eq!(hex, PREFIX);
        let published = 1224.9308_f64;
        let e = f64::from(b.estimate());
        assert!((e - published).abs() <= published * 0.01, "estimate {e}");
        assert!((b.estimate_f64() - published).abs() < 1e-3);
    }
}
