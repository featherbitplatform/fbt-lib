//! NAT-PMP (RFC 6886) and PCP (RFC 6887) port mapping against the default
//! gateway.
//!
//! The codec is pure and bounded; the client tries PCP first and falls back to
//! NAT-PMP when the gateway answers with "unsupported version". Every reply is
//! hostile input: it is parsed with checked slicing only, and each wait is
//! bounded by the retry schedule.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::Instant;
use tracing::{debug, trace};

/// The well-known gateway port for NAT-PMP and PCP.
pub const NATPMP_PORT: u16 = 5351;

/// Waits before giving up on each attempt: RFC 6886's schedule cut short.
const RETRY_WAITS: [Duration; 4] = [
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(2),
];
/// Receive buffer; PCP replies are at most 1100 bytes.
const RECV_BUF: usize = 1100;
/// Length of a PCP MAP request or (option-free) reply.
const PCP_MAP_LEN: usize = 60;

/// Transport protocol of a mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MapProtocol {
    /// TCP.
    Tcp,
    /// UDP.
    Udp,
}

/// Which protocol produced a mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MapMethod {
    /// UPnP IGD.
    Upnp,
    /// NAT-PMP (RFC 6886).
    NatPmp,
    /// PCP (RFC 6887).
    Pcp,
}

/// Why a mapping attempt failed.
#[derive(Debug, thiserror::Error)]
pub enum NatPmpError {
    /// A socket operation failed.
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
    /// The gateway did not answer in time.
    #[error("gateway did not answer")]
    Timeout,
    /// The reply was not a valid answer to our request.
    #[error("malformed reply: {0}")]
    Malformed(&'static str),
    /// The gateway refused with this result code.
    #[error("gateway refused with result code {0}")]
    Result(u16),
}

/// A mapping granted by the gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mapping {
    /// The protocol that produced it.
    pub method: MapMethod,
    /// The public address and port.
    pub external: SocketAddr,
    /// The granted lifetime in seconds.
    pub lifetime: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    NatPmpExternal,
    NatPmpMap,
    PcpMap,
}

/// What a reply must answer: built only through the constructors, so a parsed
/// reply always matches the request it was sent for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Expect {
    kind: Kind,
    protocol: MapProtocol,
    internal: u16,
    nonce: [u8; 12],
}

impl Expect {
    /// Expect the answer to a NAT-PMP external-address request.
    pub fn natpmp_external() -> Expect {
        Expect {
            kind: Kind::NatPmpExternal,
            protocol: MapProtocol::Udp,
            internal: 0,
            nonce: [0; 12],
        }
    }

    /// Expect the answer to a NAT-PMP map request.
    pub fn natpmp_map(p: MapProtocol, internal: u16) -> Expect {
        Expect {
            kind: Kind::NatPmpMap,
            protocol: p,
            internal,
            nonce: [0; 12],
        }
    }

    /// Expect the answer to a PCP MAP request carrying `nonce`.
    pub fn pcp_map(p: MapProtocol, internal: u16, nonce: [u8; 12]) -> Expect {
        Expect {
            kind: Kind::PcpMap,
            protocol: p,
            internal,
            nonce,
        }
    }

    /// The NAT-PMP opcode this expects (0 external, 1 UDP, 2 TCP).
    fn natpmp_op(&self) -> u8 {
        match (self.kind, self.protocol) {
            (Kind::NatPmpExternal, _) => 0,
            (_, MapProtocol::Udp) => 1,
            (_, MapProtocol::Tcp) => 2,
        }
    }
}

/// A parsed gateway reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reply {
    /// Answer to a NAT-PMP external-address request.
    NatPmpExternal {
        /// Seconds since the gateway's mapping table was reset.
        epoch: u32,
        /// The gateway's public address.
        ip: Ipv4Addr,
    },
    /// Answer to a NAT-PMP map request.
    NatPmpMap {
        /// Seconds since the gateway's mapping table was reset.
        epoch: u32,
        /// The mapped internal port.
        internal: u16,
        /// The assigned external port.
        external: u16,
        /// The granted lifetime in seconds.
        lifetime: u32,
    },
    /// Answer to a PCP MAP request.
    PcpMap {
        /// The granted lifetime in seconds.
        lifetime: u32,
        /// The assigned external address and port.
        external: SocketAddr,
    },
    /// A NAT-PMP "unsupported version" reply to a PCP request: fall back.
    UnsupportedVersion,
}

/// The NAT-PMP request for the gateway's public address.
pub fn natpmp_external_request() -> [u8; 2] {
    [0, 0]
}

fn protocol_number(p: MapProtocol) -> u8 {
    match p {
        MapProtocol::Tcp => 6,
        MapProtocol::Udp => 17,
    }
}

/// A NAT-PMP map request.
pub fn natpmp_map_request(p: MapProtocol, internal: u16, external: u16, lifetime: u32) -> [u8; 12] {
    let op = match p {
        MapProtocol::Udp => 1,
        MapProtocol::Tcp => 2,
    };
    let mut b = [0u8; 12];
    b[1] = op;
    b[4..6].copy_from_slice(&internal.to_be_bytes());
    b[6..8].copy_from_slice(&external.to_be_bytes());
    b[8..12].copy_from_slice(&lifetime.to_be_bytes());
    b
}

/// A PCP MAP request (RFC 6887 sections 7.1 and 11.1).
pub fn pcp_map_request(
    nonce: [u8; 12],
    p: MapProtocol,
    internal: u16,
    external: u16,
    client: Ipv4Addr,
    lifetime: u32,
) -> [u8; 60] {
    let mut b = [0u8; PCP_MAP_LEN];
    b[0] = 2;
    b[1] = 1;
    b[4..8].copy_from_slice(&lifetime.to_be_bytes());
    b[8..24].copy_from_slice(&client.to_ipv6_mapped().octets());
    b[24..36].copy_from_slice(&nonce);
    b[36] = protocol_number(p);
    b[40..42].copy_from_slice(&internal.to_be_bytes());
    b[42..44].copy_from_slice(&external.to_be_bytes());
    b[44..60].copy_from_slice(&Ipv4Addr::UNSPECIFIED.to_ipv6_mapped().octets());
    b
}

fn be16(data: &[u8], at: usize) -> Option<u16> {
    let s = data.get(at..at.checked_add(2)?)?;
    Some(u16::from_be_bytes(s.try_into().ok()?))
}

fn be32(data: &[u8], at: usize) -> Option<u32> {
    let s = data.get(at..at.checked_add(4)?)?;
    Some(u32::from_be_bytes(s.try_into().ok()?))
}

const SHORT: NatPmpError = NatPmpError::Malformed("truncated reply");

/// Parse a gateway reply against what we asked. Never panics.
pub fn parse_reply(data: &[u8], expect: &Expect) -> Result<Reply, NatPmpError> {
    let version = *data.first().ok_or(SHORT)?;
    match (version, expect.kind) {
        (0, Kind::PcpMap) => {
            // A NAT-PMP-only gateway answers an unknown version with result 1.
            if be16(data, 2).ok_or(SHORT)? == 1 {
                Ok(Reply::UnsupportedVersion)
            } else {
                Err(NatPmpError::Malformed("version 0 reply to a PCP request"))
            }
        }
        (0, _) => parse_natpmp(data, expect),
        (2, Kind::PcpMap) => parse_pcp(data, expect),
        _ => Err(NatPmpError::Malformed("unexpected version")),
    }
}

fn parse_natpmp(data: &[u8], expect: &Expect) -> Result<Reply, NatPmpError> {
    let op = *data.get(1).ok_or(SHORT)?;
    if op != 128u8.saturating_add(expect.natpmp_op()) {
        return Err(NatPmpError::Malformed("unexpected opcode"));
    }
    let code = be16(data, 2).ok_or(SHORT)?;
    if code != 0 {
        return Err(NatPmpError::Result(code));
    }
    let epoch = be32(data, 4).ok_or(SHORT)?;
    if expect.kind == Kind::NatPmpExternal {
        if data.len() != 12 {
            return Err(NatPmpError::Malformed("bad external reply length"));
        }
        let ip = be32(data, 8).ok_or(SHORT)?;
        return Ok(Reply::NatPmpExternal {
            epoch,
            ip: Ipv4Addr::from(ip),
        });
    }
    if data.len() != 16 {
        return Err(NatPmpError::Malformed("bad map reply length"));
    }
    let internal = be16(data, 8).ok_or(SHORT)?;
    if internal != expect.internal {
        return Err(NatPmpError::Malformed("internal port mismatch"));
    }
    Ok(Reply::NatPmpMap {
        epoch,
        internal,
        external: be16(data, 10).ok_or(SHORT)?,
        lifetime: be32(data, 12).ok_or(SHORT)?,
    })
}

fn parse_pcp(data: &[u8], expect: &Expect) -> Result<Reply, NatPmpError> {
    // Response bit set, opcode MAP.
    if data.get(1).copied() != Some(0x81) {
        return Err(NatPmpError::Malformed("unexpected opcode"));
    }
    let code = *data.get(3).ok_or(SHORT)?;
    if code != 0 {
        return Err(NatPmpError::Result(u16::from(code)));
    }
    if data.len() < PCP_MAP_LEN {
        return Err(SHORT);
    }
    let lifetime = be32(data, 4).ok_or(SHORT)?;
    let nonce = data.get(24..36).ok_or(SHORT)?;
    if nonce != expect.nonce {
        return Err(NatPmpError::Malformed("nonce mismatch"));
    }
    if data.get(36).copied() != Some(protocol_number(expect.protocol)) {
        return Err(NatPmpError::Malformed("protocol mismatch"));
    }
    if be16(data, 40).ok_or(SHORT)? != expect.internal {
        return Err(NatPmpError::Malformed("internal port mismatch"));
    }
    let port = be16(data, 42).ok_or(SHORT)?;
    let raw: [u8; 16] = data
        .get(44..60)
        .and_then(|s| s.try_into().ok())
        .ok_or(SHORT)?;
    let v6 = Ipv6Addr::from(raw);
    let ip = match v6.to_ipv4_mapped() {
        Some(v4) => IpAddr::V4(v4),
        None => IpAddr::V6(v6),
    };
    Ok(Reply::PcpMap {
        lifetime,
        external: SocketAddr::new(ip, port),
    })
}

/// Send `request` on the schedule and return the first valid reply from
/// `gateway`. Datagrams from elsewhere, and invalid ones, are ignored.
async fn exchange(
    sock: &UdpSocket,
    gateway: SocketAddr,
    request: &[u8],
    expect: &Expect,
) -> Result<Reply, NatPmpError> {
    let mut buf = [0u8; RECV_BUF];
    for wait in RETRY_WAITS {
        sock.send_to(request, gateway).await?;
        let deadline = Instant::now() + wait;
        loop {
            let Ok(got) = tokio::time::timeout_at(deadline, sock.recv_from(&mut buf)).await else {
                break;
            };
            let (n, src) = got?;
            if src != gateway {
                trace!(%src, "ignoring datagram from a non-gateway source");
                continue;
            }
            let Some(slice) = buf.get(..n) else { continue };
            match parse_reply(slice, expect) {
                Ok(r) => return Ok(r),
                Err(e @ NatPmpError::Result(_)) => return Err(e),
                Err(e) => debug!(error = %e, "ignoring invalid reply"),
            }
        }
    }
    Err(NatPmpError::Timeout)
}

/// The local IPv4 address the OS would use to reach `gateway`.
async fn local_v4(gateway: SocketAddr) -> Result<Ipv4Addr, NatPmpError> {
    let probe = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
    probe.connect(gateway).await?;
    match probe.local_addr()?.ip() {
        IpAddr::V4(ip) => Ok(ip),
        IpAddr::V6(_) => Err(NatPmpError::Malformed("no IPv4 route to the gateway")),
    }
}

/// Map `internal` through `gateway` for `lifetime` seconds: PCP first, then
/// NAT-PMP if the gateway does not speak PCP. `port` is the gateway's port
/// ([`NATPMP_PORT`] outside tests).
pub async fn map(
    gateway: Ipv4Addr,
    port: u16,
    p: MapProtocol,
    internal: u16,
    lifetime: u32,
) -> Result<Mapping, NatPmpError> {
    let gw = SocketAddr::new(IpAddr::V4(gateway), port);
    let sock = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
    let client = local_v4(gw).await?;
    let nonce: [u8; 12] = rand::random();
    let req = pcp_map_request(nonce, p, internal, internal, client, lifetime);
    match exchange(&sock, gw, &req, &Expect::pcp_map(p, internal, nonce)).await? {
        Reply::PcpMap { lifetime, external } => {
            return Ok(Mapping {
                method: MapMethod::Pcp,
                external,
                lifetime,
            });
        }
        Reply::UnsupportedVersion => debug!("gateway lacks PCP, falling back to NAT-PMP"),
        _ => return Err(NatPmpError::Malformed("unexpected reply kind")),
    }
    let request = natpmp_external_request();
    let ip = match exchange(&sock, gw, &request, &Expect::natpmp_external()).await? {
        Reply::NatPmpExternal { ip, .. } => ip,
        _ => return Err(NatPmpError::Malformed("unexpected reply kind")),
    };
    let req = natpmp_map_request(p, internal, internal, lifetime);
    match exchange(&sock, gw, &req, &Expect::natpmp_map(p, internal)).await? {
        Reply::NatPmpMap {
            external, lifetime, ..
        } => Ok(Mapping {
            method: MapMethod::NatPmp,
            external: SocketAddr::new(IpAddr::V4(ip), external),
            lifetime,
        }),
        _ => Err(NatPmpError::Malformed("unexpected reply kind")),
    }
}

/// Remove a mapping: a map with lifetime 0.
pub async fn unmap(
    gateway: Ipv4Addr,
    port: u16,
    p: MapProtocol,
    internal: u16,
) -> Result<Mapping, NatPmpError> {
    map(gateway, port, p, internal, 0).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn natpmp_request_layout() {
        assert_eq!(
            natpmp_map_request(MapProtocol::Tcp, 6881, 6881, 7200),
            [0, 2, 0, 0, 0x1a, 0xe1, 0x1a, 0xe1, 0, 0, 0x1c, 0x20]
        );
    }

    #[test]
    fn natpmp_map_reply_parsed() {
        let r = [
            0, 130, 0, 0, 0, 0, 0, 9, 0x1a, 0xe1, 0x1b, 0x00, 0, 0, 0x1c, 0x20,
        ];
        assert!(matches!(
            parse_reply(&r, &Expect::natpmp_map(MapProtocol::Tcp, 6881)),
            Ok(Reply::NatPmpMap {
                external: 6912,
                lifetime: 7200,
                ..
            })
        ));
    }

    #[test]
    fn pcp_request_is_60_bytes_with_nonce() {
        let b = pcp_map_request(
            [7; 12],
            MapProtocol::Udp,
            6881,
            6881,
            "192.168.1.5".parse().unwrap(),
            7200,
        );
        assert_eq!((b[0], b[1]), (2, 1));
        assert_eq!(&b[24..36], &[7; 12]);
        assert_eq!(b[36], 17);
    }

    #[test]
    fn pcp_wrong_nonce_is_malformed() {
        let mut r = [0u8; 60];
        r[0] = 2;
        r[1] = 0x81;
        r[24..36].copy_from_slice(&[1; 12]);
        r[36] = 6;
        r[40..42].copy_from_slice(&6881u16.to_be_bytes());
        let e = Expect::pcp_map(MapProtocol::Tcp, 6881, [2; 12]);
        assert!(matches!(
            parse_reply(&r, &e),
            Err(NatPmpError::Malformed(_))
        ));
        let e = Expect::pcp_map(MapProtocol::Tcp, 6881, [1; 12]);
        assert!(matches!(parse_reply(&r, &e), Ok(Reply::PcpMap { .. })));
    }

    #[test]
    fn unsupported_version_detected() {
        let r = [0, 129, 0, 1, 0, 0, 0, 5];
        let e = Expect::pcp_map(MapProtocol::Udp, 6881, [0; 12]);
        assert!(matches!(parse_reply(&r, &e), Ok(Reply::UnsupportedVersion)));
    }

    #[test]
    fn natpmp_error_code_reported() {
        let r = [0, 130, 0, 2, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 0];
        let e = Expect::natpmp_map(MapProtocol::Tcp, 6881);
        assert!(matches!(parse_reply(&r, &e), Err(NatPmpError::Result(2))));
    }

    proptest! {
        #[test]
        fn parse_reply_never_panics(data in proptest::collection::vec(any::<u8>(), 0..1200)) {
            let _ = parse_reply(&data, &Expect::natpmp_external());
            let _ = parse_reply(&data, &Expect::natpmp_map(MapProtocol::Tcp, 6881));
            let _ = parse_reply(&data, &Expect::pcp_map(MapProtocol::Udp, 6881, [7; 12]));
        }
    }
}
