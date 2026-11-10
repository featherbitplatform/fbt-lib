//! Compact peer-address codec (BEP 23 / BEP 7 / BEP 11 / BEP 5): the address
//! octets followed by the port in big-endian. This is the one shared
//! implementation; it is pure and never panics on any input.

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

/// Length of a compact IPv4 address: 4 octets + 2 port bytes.
pub const COMPACT_V4_LEN: usize = 6;
/// Length of a compact IPv6 address: 16 octets + 2 port bytes.
pub const COMPACT_V6_LEN: usize = 18;

/// Compact list decoding failures.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum CompactError {
    /// The buffer length is not a multiple of the entry size.
    #[error("compact list length is not a multiple of the entry size")]
    Misaligned,
}

/// Encodes an IPv4 socket address.
pub fn encode_v4(addr: SocketAddrV4) -> [u8; COMPACT_V4_LEN] {
    let mut out = [0u8; COMPACT_V4_LEN];
    for (d, s) in out.iter_mut().zip(
        addr.ip()
            .octets()
            .into_iter()
            .chain(addr.port().to_be_bytes()),
    ) {
        *d = s;
    }
    out
}

/// Encodes an IPv6 socket address.
pub fn encode_v6(addr: SocketAddrV6) -> [u8; COMPACT_V6_LEN] {
    let mut out = [0u8; COMPACT_V6_LEN];
    for (d, s) in out.iter_mut().zip(
        addr.ip()
            .octets()
            .into_iter()
            .chain(addr.port().to_be_bytes()),
    ) {
        *d = s;
    }
    out
}

/// Appends the 6- or 18-byte form of `addr` to `out`.
pub fn encode(addr: SocketAddr, out: &mut Vec<u8>) {
    match addr {
        SocketAddr::V4(a) => out.extend_from_slice(&encode_v4(a)),
        SocketAddr::V6(a) => out.extend_from_slice(&encode_v6(a)),
    }
}

/// Decodes one 6-byte IPv4 entry.
pub fn decode_v4(b: &[u8; COMPACT_V4_LEN]) -> SocketAddr {
    let [a, b2, c, d, p0, p1] = *b;
    SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::new(a, b2, c, d),
        u16::from_be_bytes([p0, p1]),
    ))
}

/// Decodes one 18-byte IPv6 entry.
pub fn decode_v6(b: &[u8; COMPACT_V6_LEN]) -> SocketAddr {
    let (ip, port) = b.split_at(16);
    let mut octets = [0u8; 16];
    octets.copy_from_slice(ip);
    let mut p = [0u8; 2];
    p.copy_from_slice(port);
    SocketAddr::V6(SocketAddrV6::new(
        Ipv6Addr::from(octets),
        u16::from_be_bytes(p),
        0,
        0,
    ))
}

/// Decodes a list of 6-byte entries, cut to `max` entries before allocation.
pub fn decode_v4_list(buf: &[u8], max: usize) -> Result<Vec<SocketAddr>, CompactError> {
    if !buf.len().is_multiple_of(COMPACT_V4_LEN) {
        return Err(CompactError::Misaligned);
    }
    Ok(buf
        .as_chunks::<COMPACT_V4_LEN>()
        .0
        .iter()
        .take(max)
        .map(decode_v4)
        .collect())
}

/// Decodes a list of 18-byte entries, cut to `max` entries before allocation.
pub fn decode_v6_list(buf: &[u8], max: usize) -> Result<Vec<SocketAddr>, CompactError> {
    if !buf.len().is_multiple_of(COMPACT_V6_LEN) {
        return Err(CompactError::Misaligned);
    }
    Ok(buf
        .as_chunks::<COMPACT_V6_LEN>()
        .0
        .iter()
        .take(max)
        .map(decode_v6)
        .collect())
}
