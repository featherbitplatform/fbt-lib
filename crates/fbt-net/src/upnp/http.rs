//! A minimal HTTP/1.1 client: one request per connection, bounded in size and
//! time. The response parser is pure so it can be fuzzed.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// The most body bytes (and the most head bytes) accepted from a device.
pub const MAX_HTTP_RESPONSE: usize = 65_536;
/// The bound on a whole request/response exchange.
pub const HTTP_TIMEOUT: Duration = Duration::from_secs(5);
/// Headers kept from a response; more is malformed.
const MAX_HEADERS: usize = 64;
/// Longest header or status line, in bytes.
const MAX_LINE: usize = 4096;
/// Longest chunk-size line, in bytes.
const MAX_CHUNK_LINE: usize = 64;
/// Bytes read from the socket per call.
const READ_CHUNK: usize = 4096;
/// Raw bytes read in total: head plus chunk framing plus body.
const MAX_RAW: usize = MAX_HTTP_RESPONSE * 2 + MAX_HTTP_RESPONSE / 2;

/// Why an HTTP exchange failed.
#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    /// Connecting, writing or reading failed.
    #[error("http i/o: {0}")]
    Io(#[from] io::Error),
    /// The exchange took longer than its bound.
    #[error("http timeout")]
    Timeout,
    /// The response exceeded [`MAX_HTTP_RESPONSE`].
    #[error("http response too large")]
    TooLarge,
    /// The response is not valid HTTP.
    #[error("malformed http response: {0}")]
    Malformed(&'static str),
}

/// A request to build; `to_bytes` renders it as HTTP/1.1.
#[derive(Debug, Clone, Copy)]
pub struct HttpRequest<'a> {
    /// `GET`, `POST`, ...
    pub method: &'a str,
    /// The device's address; also the `Host` header.
    pub host: SocketAddr,
    /// The request target, e.g. `/ctl/IPConn`.
    pub path: &'a str,
    /// Extra headers, in order.
    pub headers: &'a [(&'a str, &'a str)],
    /// The body; may be empty.
    pub body: &'a [u8],
}

impl HttpRequest<'_> {
    /// Render the request with `Host`, `Content-Length` and
    /// `Connection: close`.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut head = format!(
            "{} {} HTTP/1.1\r\nHost: {}\r\n",
            self.method, self.path, self.host
        );
        for (name, value) in self.headers {
            head.push_str(name);
            head.push_str(": ");
            head.push_str(value);
            head.push_str("\r\n");
        }
        head.push_str(&format!(
            "Content-Length: {}\r\nConnection: close\r\n\r\n",
            self.body.len()
        ));
        let mut out = head.into_bytes();
        out.extend_from_slice(self.body);
        out
    }
}

/// A parsed response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    /// The status code.
    pub status: u16,
    /// Headers in order, names as sent.
    pub headers: Vec<(String, String)>,
    /// The decoded body.
    pub body: Vec<u8>,
}

enum Parsed {
    Complete(HttpResponse),
    Incomplete,
}

/// Parse a whole response. `bytes` is everything the device sent up to EOF.
pub fn parse_response(bytes: &[u8]) -> Result<HttpResponse, HttpError> {
    match parse(bytes, true)? {
        Parsed::Complete(r) => Ok(r),
        Parsed::Incomplete => Err(HttpError::Malformed("truncated response")),
    }
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn parse(bytes: &[u8], eof: bool) -> Result<Parsed, HttpError> {
    let Some(head_end) = find(bytes, b"\r\n\r\n") else {
        if bytes.len() > MAX_HTTP_RESPONSE {
            return Err(HttpError::TooLarge);
        }
        return if eof {
            Err(HttpError::Malformed("no end of headers"))
        } else {
            Ok(Parsed::Incomplete)
        };
    };
    if head_end > MAX_HTTP_RESPONSE {
        return Err(HttpError::TooLarge);
    }
    let head = bytes.get(..head_end).unwrap_or_default();
    let rest = bytes.get(head_end + 4..).unwrap_or_default();

    let mut lines = head.split(|&b| b == b'\n').map(|l| match l.split_last() {
        Some((b'\r', t)) => t,
        _ => l,
    });
    let status_line = lines.next().unwrap_or_default();
    if status_line.len() > MAX_LINE {
        return Err(HttpError::Malformed("status line too long"));
    }
    let status = parse_status(status_line)?;

    let mut headers = Vec::new();
    for line in lines {
        if line.len() > MAX_LINE {
            return Err(HttpError::Malformed("header line too long"));
        }
        if headers.len() >= MAX_HEADERS {
            return Err(HttpError::Malformed("too many headers"));
        }
        let Some(colon) = line.iter().position(|&b| b == b':') else {
            return Err(HttpError::Malformed("header without colon"));
        };
        let name = String::from_utf8_lossy(line.get(..colon).unwrap_or_default());
        let value = String::from_utf8_lossy(line.get(colon + 1..).unwrap_or_default());
        headers.push((name.trim().to_owned(), value.trim().to_owned()));
    }

    let header = |n: &str| {
        headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(n))
            .map(|(_, v)| v.as_str())
    };
    let chunked = header("transfer-encoding").is_some_and(|v| {
        v.to_ascii_lowercase()
            .split(',')
            .any(|t| t.trim() == "chunked")
    });

    let body = if chunked {
        match decode_chunked(rest, eof)? {
            Some(b) => b,
            None => return Ok(Parsed::Incomplete),
        }
    } else if let Some(len) = header("content-length") {
        let len: usize = len
            .parse()
            .map_err(|_| HttpError::Malformed("bad content-length"))?;
        if len > MAX_HTTP_RESPONSE {
            return Err(HttpError::TooLarge);
        }
        match rest.get(..len) {
            Some(b) => b.to_vec(),
            None if eof => return Err(HttpError::Malformed("truncated body")),
            None => return Ok(Parsed::Incomplete),
        }
    } else {
        if rest.len() > MAX_HTTP_RESPONSE {
            return Err(HttpError::TooLarge);
        }
        if !eof {
            return Ok(Parsed::Incomplete);
        }
        rest.to_vec()
    };
    Ok(Parsed::Complete(HttpResponse {
        status,
        headers,
        body,
    }))
}

fn parse_status(line: &[u8]) -> Result<u16, HttpError> {
    let bad = || HttpError::Malformed("bad status line");
    let mut parts = line.splitn(3, |&b| b == b' ');
    let version = parts.next().unwrap_or_default();
    if !version.starts_with(b"HTTP/1.") {
        return Err(bad());
    }
    let code = parts.next().ok_or_else(bad)?;
    if code.len() != 3 || !code.iter().all(u8::is_ascii_digit) {
        return Err(bad());
    }
    let code = std::str::from_utf8(code).map_err(|_| bad())?;
    code.parse().map_err(|_| bad())
}

/// Decode a chunked body. `Ok(None)` means more bytes are needed.
fn decode_chunked(mut rest: &[u8], eof: bool) -> Result<Option<Vec<u8>>, HttpError> {
    let mut body = Vec::new();
    let need_more = |eof: bool| {
        if eof {
            Err(HttpError::Malformed("truncated chunked body"))
        } else {
            Ok(None)
        }
    };
    loop {
        let Some(nl) = find(rest, b"\r\n") else {
            if rest.len() > MAX_CHUNK_LINE {
                return Err(HttpError::Malformed("chunk size line too long"));
            }
            return need_more(eof);
        };
        if nl > MAX_CHUNK_LINE {
            return Err(HttpError::Malformed("chunk size line too long"));
        }
        let line = rest.get(..nl).unwrap_or_default();
        let size_part = line.split(|&b| b == b';').next().unwrap_or_default();
        let size_str = std::str::from_utf8(size_part)
            .map_err(|_| HttpError::Malformed("bad chunk size"))?
            .trim();
        let size = usize::from_str_radix(size_str, 16)
            .map_err(|_| HttpError::Malformed("bad chunk size"))?;
        let after = rest.get(nl + 2..).unwrap_or_default();
        if size == 0 {
            return Ok(Some(body));
        }
        if size > MAX_HTTP_RESPONSE.saturating_sub(body.len()) {
            return Err(HttpError::TooLarge);
        }
        let Some(data) = after.get(..size) else {
            return need_more(eof);
        };
        let tail = after.get(size..).unwrap_or_default();
        if tail.len() < 2 {
            return need_more(eof);
        }
        if !tail.starts_with(b"\r\n") {
            return Err(HttpError::Malformed("missing chunk terminator"));
        }
        body.extend_from_slice(data);
        rest = tail.get(2..).unwrap_or_default();
    }
}

/// Send `req` and read the reply, bounded by [`HTTP_TIMEOUT`] and
/// [`MAX_HTTP_RESPONSE`]. Also returns our local address on the connection.
pub async fn request(req: &HttpRequest<'_>) -> Result<(HttpResponse, SocketAddr), HttpError> {
    request_with_timeout(req, HTTP_TIMEOUT).await
}

/// [`request`] with a caller-chosen bound for the whole exchange.
pub async fn request_with_timeout(
    req: &HttpRequest<'_>,
    timeout: Duration,
) -> Result<(HttpResponse, SocketAddr), HttpError> {
    tokio::time::timeout(timeout, exchange(req))
        .await
        .map_err(|_| HttpError::Timeout)?
}

async fn exchange(req: &HttpRequest<'_>) -> Result<(HttpResponse, SocketAddr), HttpError> {
    let mut stream = TcpStream::connect(req.host).await?;
    let local = stream.local_addr()?;
    stream.write_all(&req.to_bytes()).await?;
    let mut raw: Vec<u8> = Vec::new();
    let mut chunk = [0u8; READ_CHUNK];
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok((parse_response(&raw)?, local));
        }
        raw.extend_from_slice(chunk.get(..n).unwrap_or_default());
        if raw.len() > MAX_RAW {
            return Err(HttpError::TooLarge);
        }
        if let Parsed::Complete(resp) = parse(&raw, false)? {
            return Ok((resp, local));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[test]
    fn parses_content_length_chunked_and_eof() {
        let r = parse_response(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX: y\r\n\r\nhelloEXTRA")
            .unwrap();
        assert_eq!(r.status, 200);
        assert_eq!(r.body, b"hello");
        assert_eq!(r.headers[1], ("X".to_owned(), "y".to_owned()));

        let r = parse_response(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n3;ext=1\r\n wo\r\n0\r\n\r\n",
        )
        .unwrap();
        assert_eq!(r.body, b"hello wo");

        let r = parse_response(b"HTTP/1.0 500 Oops\r\n\r\nuntil eof").unwrap();
        assert_eq!(r.status, 500);
        assert_eq!(r.body, b"until eof");

        assert!(parse_response(b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nhi").is_err());
        assert!(matches!(
            parse(b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nhi", false),
            Ok(Parsed::Incomplete)
        ));
    }

    #[test]
    fn rejects_oversize_and_bad_status_line() {
        let big = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
            MAX_HTTP_RESPONSE + 1
        );
        assert!(matches!(
            parse_response(big.as_bytes()),
            Err(HttpError::TooLarge)
        ));
        let chunk = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n",
            MAX_HTTP_RESPONSE + 1
        );
        assert!(matches!(
            parse_response(chunk.as_bytes()),
            Err(HttpError::TooLarge)
        ));
        let mut eof = b"HTTP/1.1 200 OK\r\n\r\n".to_vec();
        eof.resize(eof.len() + MAX_HTTP_RESPONSE + 1, b'a');
        assert!(matches!(parse_response(&eof), Err(HttpError::TooLarge)));
        for bad in [
            &b"garbage\r\n\r\n"[..],
            b"HTTP/1.1 20 OK\r\n\r\n",
            b"HTTP/2 200 OK\r\n\r\n",
            b"HTTP/1.1 abc OK\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nno colon\r\n\r\n",
            b"",
        ] {
            assert!(parse_response(bad).is_err(), "{bad:?}");
        }
        let many = format!("HTTP/1.1 200 OK\r\n{}\r\n", "A: b\r\n".repeat(65));
        assert!(parse_response(many.as_bytes()).is_err());
        let long = format!("HTTP/1.1 200 OK\r\nA: {}\r\n\r\n", "b".repeat(MAX_LINE));
        assert!(parse_response(long.as_bytes()).is_err());
    }

    #[test]
    fn request_bytes_are_http11() {
        let host: SocketAddr = "192.168.1.1:5000".parse().unwrap();
        let req = HttpRequest {
            method: "POST",
            host,
            path: "/ctl/IPConn",
            headers: &[("SOAPAction", "\"x\"")],
            body: b"<a/>",
        };
        let text = String::from_utf8(req.to_bytes()).unwrap();
        assert_eq!(
            text,
            "POST /ctl/IPConn HTTP/1.1\r\nHost: 192.168.1.1:5000\r\nSOAPAction: \"x\"\r\nContent-Length: 4\r\nConnection: close\r\n\r\n<a/>"
        );
    }

    #[tokio::test]
    async fn slow_server_times_out() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let _ = s.write_all(b"H").await;
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if s.write_all(b"T").await.is_err() {
                    break;
                }
            }
        });
        let req = HttpRequest {
            method: "GET",
            host,
            path: "/",
            headers: &[],
            body: b"",
        };
        let r = request_with_timeout(&req, Duration::from_millis(300)).await;
        assert!(matches!(r, Err(HttpError::Timeout)), "{r:?}");
    }

    #[tokio::test]
    async fn request_reads_a_complete_message_without_eof() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 512];
            let _ = s.read(&mut buf).await;
            let _ = s
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await;
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let req = HttpRequest {
            method: "GET",
            host,
            path: "/",
            headers: &[],
            body: b"",
        };
        let (resp, local) = request_with_timeout(&req, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(resp.body, b"ok");
        assert!(local.ip().is_loopback());
    }
}
