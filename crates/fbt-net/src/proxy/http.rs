//! HTTP `CONNECT` tunnels (RFC 9110 section 9.3.6) with Basic authentication.

use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::{ProxyConfig, ProxyError, ProxyTarget, base64};

/// The longest `CONNECT` response head read, terminator included (8 KiB).
pub const MAX_CONNECT_HEAD: usize = 8192;

const TERMINATOR: &[u8] = b"\r\n\r\n";

/// Sends `CONNECT` and reads the response head byte by byte, so no tunnel
/// byte is consumed. A 2xx status leaves `s` as the tunnel.
pub(super) async fn connect(
    s: &mut TcpStream,
    cfg: &ProxyConfig,
    target: &ProxyTarget,
) -> Result<(), ProxyError> {
    let authority = match target {
        ProxyTarget::Addr(a) => a.to_string(),
        ProxyTarget::Host(h, p) => format!("{h}:{p}"),
    };
    let mut req = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
    if let Some(user) = &cfg.username {
        let pass = cfg.password.as_deref().unwrap_or("");
        let token = base64(format!("{user}:{pass}").as_bytes());
        req.push_str(&format!("Proxy-Authorization: Basic {token}\r\n"));
    }
    req.push_str("\r\n");
    s.write_all(req.as_bytes()).await?;

    let mut head = Vec::with_capacity(256);
    let mut byte = [0u8; 1];
    loop {
        if s.read(&mut byte).await? == 0 {
            return Err(ProxyError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "proxy closed the connection in the CONNECT response head",
            )));
        }
        head.extend_from_slice(&byte);
        if !head.ends_with(TERMINATOR) && head.len() < MAX_CONNECT_HEAD {
            continue;
        }
        return match parse_connect_head(&head)? {
            Some((status, _)) if (200..300).contains(&status) => Ok(()),
            Some((status, _)) => Err(ProxyError::HttpStatus(status)),
            None => Err(ProxyError::Protocol("CONNECT response head is incomplete")),
        };
    }
}

/// Parses a `CONNECT` response head.
///
/// `Ok(None)` means the blank line has not arrived yet;
/// `Ok(Some((status, len)))` gives the status code and the head's length,
/// terminator included. A head that reaches [`MAX_CONNECT_HEAD`] bytes without
/// ending, or a status line that is not HTTP/1.x, is
/// [`ProxyError::Protocol`].
#[doc(hidden)]
pub fn parse_connect_head(buf: &[u8]) -> Result<Option<(u16, usize)>, ProxyError> {
    let window = buf.get(..MAX_CONNECT_HEAD).unwrap_or(buf);
    let Some(end) = window
        .windows(TERMINATOR.len())
        .position(|w| w == TERMINATOR)
        .map(|p| p + TERMINATOR.len())
    else {
        return if buf.len() >= MAX_CONNECT_HEAD {
            Err(ProxyError::Protocol("CONNECT response head too long"))
        } else {
            Ok(None)
        };
    };
    if end > MAX_CONNECT_HEAD {
        return Err(ProxyError::Protocol("CONNECT response head too long"));
    }
    let line = window.split(|&b| b == b'\r').next().unwrap_or_default();
    let rest = line
        .strip_prefix(b"HTTP/1.")
        .and_then(|r| r.get(1..))
        .and_then(|r| r.strip_prefix(b" "))
        .ok_or(ProxyError::Protocol("CONNECT response is not HTTP/1.x"))?;
    let (code, after) = rest
        .split_first_chunk::<3>()
        .ok_or(ProxyError::Protocol("CONNECT response has no status code"))?;
    if !code.iter().all(u8::is_ascii_digit) || !(after.is_empty() || after.starts_with(b" ")) {
        return Err(ProxyError::Protocol(
            "CONNECT response has a bad status code",
        ));
    }
    let status = code.iter().fold(0u16, |n, d| n * 10 + u16::from(d - b'0'));
    if !(100..=599).contains(&status) {
        return Err(ProxyError::Protocol(
            "CONNECT response has a bad status code",
        ));
    }
    Ok(Some((status, end)))
}
