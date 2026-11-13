//! SOCKS5 (RFC 1928) with username/password authentication (RFC 1929).

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::{ProxyConfig, ProxyError, ProxyTarget};

const VERSION: u8 = 5;
const METHOD_NONE: u8 = 0x00;
const METHOD_USER_PASS: u8 = 0x02;
const METHOD_REJECTED: u8 = 0xff;
const AUTH_VERSION: u8 = 1;
pub(super) const CMD_CONNECT: u8 = 1;
pub(super) const CMD_UDP_ASSOCIATE: u8 = 3;
const ATYP_V4: u8 = 1;
const ATYP_DOMAIN: u8 = 3;
const ATYP_V6: u8 = 4;
/// The longest reply: 4 header bytes, a 255-byte domain with its length byte,
/// and the port.
const MAX_REPLY: usize = 4 + 1 + 255 + 2;

/// Greets the proxy and authenticates: offers no authentication, plus
/// username/password when a username is set.
pub(super) async fn negotiate(s: &mut TcpStream, cfg: &ProxyConfig) -> Result<(), ProxyError> {
    let creds = match &cfg.username {
        Some(user) => {
            let pass = cfg.password.as_deref().unwrap_or("");
            // RFC 1929: ULEN and PLEN are 1 to 255.
            let (Ok(ulen @ 1..), Ok(plen @ 1..)) =
                (u8::try_from(user.len()), u8::try_from(pass.len()))
            else {
                return Err(ProxyError::Protocol(
                    "SOCKS5 username and password must be 1 to 255 bytes",
                ));
            };
            let mut msg = Vec::with_capacity(3 + user.len() + pass.len());
            msg.extend_from_slice(&[AUTH_VERSION, ulen]);
            msg.extend_from_slice(user.as_bytes());
            msg.push(plen);
            msg.extend_from_slice(pass.as_bytes());
            Some(msg)
        }
        None => None,
    };
    if creds.is_some() {
        s.write_all(&[VERSION, 2, METHOD_NONE, METHOD_USER_PASS])
            .await?;
    } else {
        s.write_all(&[VERSION, 1, METHOD_NONE]).await?;
    }
    let mut choice = [0u8; 2];
    s.read_exact(&mut choice).await?;
    let [ver, method] = choice;
    if ver != VERSION {
        return Err(ProxyError::Protocol("SOCKS5 method reply version is not 5"));
    }
    match (method, creds) {
        (METHOD_NONE, _) => Ok(()),
        (METHOD_USER_PASS, Some(msg)) => {
            s.write_all(&msg).await?;
            let mut status = [0u8; 2];
            s.read_exact(&mut status).await?;
            match status {
                [AUTH_VERSION, 0] => Ok(()),
                [AUTH_VERSION, _] => Err(ProxyError::AuthFailed),
                _ => Err(ProxyError::Protocol("SOCKS5 auth reply version is not 1")),
            }
        }
        (METHOD_REJECTED, _) => Err(ProxyError::AuthFailed),
        _ => Err(ProxyError::Protocol(
            "SOCKS5 proxy chose a method not offered",
        )),
    }
}

/// Sends a request and reads its reply; returns `BND.ADDR` (`None` when the
/// proxy named a domain). Reads exactly the reply, never a tunnel byte.
pub(super) async fn request(
    s: &mut TcpStream,
    cmd: u8,
    target: &ProxyTarget,
) -> Result<Option<SocketAddr>, ProxyError> {
    let mut msg = vec![VERSION, cmd, 0];
    match target {
        ProxyTarget::Addr(a) => push_addr(&mut msg, *a),
        ProxyTarget::Host(host, port) => {
            let len = u8::try_from(host.len())
                .map_err(|_| ProxyError::Protocol("SOCKS5 host name longer than 255 bytes"))?;
            msg.extend_from_slice(&[ATYP_DOMAIN, len]);
            msg.extend_from_slice(host.as_bytes());
            msg.extend_from_slice(&port.to_be_bytes());
        }
    }
    s.write_all(&msg).await?;
    let mut buf = [0u8; MAX_REPLY];
    let mut have = 0;
    loop {
        let filled = buf.get(..have).unwrap_or_default();
        if let Some((addr, _)) = parse_socks5_reply(filled)? {
            return Ok(addr);
        }
        let want = reply_len(filled).unwrap_or(5).min(MAX_REPLY);
        if want <= have {
            // The parser wants more than the header says: never spin on an
            // empty read.
            return Err(ProxyError::Protocol("SOCKS5 reply length is inconsistent"));
        }
        let Some(dst) = buf.get_mut(have..want) else {
            return Err(ProxyError::Protocol("SOCKS5 reply too long"));
        };
        s.read_exact(dst).await?;
        have = want;
    }
}

/// The full length of the reply starting `buf`, once its header says it.
fn reply_len(buf: &[u8]) -> Option<usize> {
    match *buf.get(3)? {
        ATYP_V4 => Some(4 + 4 + 2),
        ATYP_V6 => Some(4 + 16 + 2),
        ATYP_DOMAIN => Some(4 + 1 + usize::from(*buf.get(4)?) + 2),
        _ => None,
    }
}

/// Parses a SOCKS5 reply (`VER REP RSV ATYP BND.ADDR BND.PORT`).
///
/// `Ok(None)` means more bytes are needed; `Ok(Some((addr, len)))` gives the
/// bound address (`None` when the proxy named a domain) and the reply's length.
/// A non-zero `REP` is [`ProxyError::Refused`] as soon as it is read.
#[doc(hidden)]
pub fn parse_socks5_reply(buf: &[u8]) -> Result<Option<(Option<SocketAddr>, usize)>, ProxyError> {
    let Some(&ver) = buf.first() else {
        return Ok(None);
    };
    if ver != VERSION {
        return Err(ProxyError::Protocol("SOCKS5 reply version is not 5"));
    }
    let Some(&rep) = buf.get(1) else {
        return Ok(None);
    };
    if rep != 0 {
        return Err(ProxyError::Refused(rep));
    }
    let Some(&atyp) = buf.get(3) else {
        return Ok(None);
    };
    if !matches!(atyp, ATYP_V4 | ATYP_V6 | ATYP_DOMAIN) {
        return Err(ProxyError::Protocol(
            "SOCKS5 reply has an unknown address type",
        ));
    }
    let Some(len) = reply_len(buf) else {
        return Ok(None);
    };
    let Some(reply) = buf.get(..len) else {
        return Ok(None);
    };
    if atyp == ATYP_DOMAIN {
        return Ok(Some((None, len)));
    }
    Ok(read_addr(atyp, reply.get(4..).unwrap_or_default()).map(|(a, _)| (Some(a), len)))
}

/// Reads an IPv4 or IPv6 address and port; returns it and the bytes after it.
fn read_addr(atyp: u8, b: &[u8]) -> Option<(SocketAddr, &[u8])> {
    let (ip, rest) = match atyp {
        ATYP_V4 => {
            let (ip, rest) = b.split_first_chunk::<4>()?;
            (IpAddr::from(Ipv4Addr::from(*ip)), rest)
        }
        ATYP_V6 => {
            let (ip, rest) = b.split_first_chunk::<16>()?;
            (IpAddr::from(Ipv6Addr::from(*ip)), rest)
        }
        _ => return None,
    };
    let (port, rest) = rest.split_first_chunk::<2>()?;
    Some((SocketAddr::new(ip, u16::from_be_bytes(*port)), rest))
}

fn push_addr(out: &mut Vec<u8>, a: SocketAddr) {
    match a.ip() {
        IpAddr::V4(ip) => {
            out.push(ATYP_V4);
            out.extend_from_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            out.push(ATYP_V6);
            out.extend_from_slice(&ip.octets());
        }
    }
    out.extend_from_slice(&a.port().to_be_bytes());
}

/// Frames `payload` for `dst` as a SOCKS5 UDP request
/// (`RSV RSV FRAG=0 ATYP DST.ADDR DST.PORT DATA`), replacing `out`'s contents.
pub fn encode_udp(dst: SocketAddr, payload: &[u8], out: &mut Vec<u8>) {
    out.clear();
    out.extend_from_slice(&[0, 0, 0]);
    push_addr(out, dst);
    out.extend_from_slice(payload);
}

/// Unframes a datagram from a SOCKS5 relay into its source and payload.
/// `None` for a fragment (`FRAG != 0`), a domain or unknown `ATYP`, or a
/// datagram too short for its header.
pub fn decode_udp(datagram: &[u8]) -> Option<(SocketAddr, &[u8])> {
    let ([_, _, frag, atyp], rest) = datagram.split_first_chunk::<4>()?;
    if *frag != 0 {
        return None;
    }
    read_addr(*atyp, rest)
}
