//! A minimal HTTP/1.1 client: one request per connection, bounded in size and
//! time. The response parser is pure so it can be fuzzed.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// The most bytes accepted from a device in one response: head plus framed body.
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

#[derive(Clone, Copy)]
enum Framing {
    Length(usize),
    Chunked,
    UntilEof,
}

struct Head {
    status: u16,
    headers: Vec<(String, String)>,
    body_start: usize,
    framing: Framing,
}

/// Chunked-body progress: `pos` is the offset, from the body start, of the
/// next chunk-size line, so re-scans never revisit finished chunks.
#[derive(Default)]
struct ChunkState {
    pos: usize,
    body: Vec<u8>,
}

/// An incremental response parser: feed it the bytes read so far; each call
/// does work proportional to what is new (plus one partial line or chunk).
#[derive(Default)]
struct Parser {
    search_from: usize,
    head: Option<Head>,
    chunks: ChunkState,
}

/// Parse a whole response. `bytes` is everything the device sent up to EOF,
/// at most [`MAX_HTTP_RESPONSE`] of it.
pub fn parse_response(bytes: &[u8]) -> Result<HttpResponse, HttpError> {
    Parser::default()
        .advance(bytes, true)?
        .ok_or(HttpError::Malformed("truncated response"))
}

/// The end of the head: `(index of the first terminating LF, body start)`.
/// Accepts CRLF and bare-LF line endings.
fn find_head_end(bytes: &[u8], from: usize) -> Option<(usize, usize)> {
    let mut i = from;
    while let Some(off) = bytes.get(i..)?.iter().position(|&b| b == b'\n') {
        let at = i + off;
        match (bytes.get(at + 1), bytes.get(at + 2)) {
            (Some(b'\n'), _) => return Some((at, at + 2)),
            (Some(b'\r'), Some(b'\n')) => return Some((at, at + 3)),
            _ => {}
        }
        i = at + 1;
    }
    None
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

impl Parser {
    fn advance(&mut self, raw: &[u8], eof: bool) -> Result<Option<HttpResponse>, HttpError> {
        if raw.len() > MAX_HTTP_RESPONSE {
            return Err(HttpError::TooLarge);
        }
        if self.head.is_none() {
            match find_head_end(raw, self.search_from) {
                None => {
                    self.search_from = raw.len().saturating_sub(3);
                    return if eof {
                        Err(HttpError::Malformed("no end of headers"))
                    } else {
                        Ok(None)
                    };
                }
                Some((end, body_start)) => {
                    let head = raw.get(..end).unwrap_or_default();
                    self.head = Some(parse_head(head, body_start)?);
                }
            }
        }
        let Some(head) = self.head.as_ref() else {
            return Ok(None);
        };
        let rest = raw.get(head.body_start..).unwrap_or_default();
        let body = match head.framing {
            Framing::Length(n) => match rest.get(..n) {
                Some(b) => b.to_vec(),
                None if eof => return Err(HttpError::Malformed("truncated body")),
                None => return Ok(None),
            },
            Framing::Chunked => {
                if !decode_chunked(rest, &mut self.chunks, eof)? {
                    return Ok(None);
                }
                std::mem::take(&mut self.chunks.body)
            }
            Framing::UntilEof => {
                if !eof {
                    return Ok(None);
                }
                rest.to_vec()
            }
        };
        let head = self.head.take().ok_or(HttpError::Malformed("no head"))?;
        Ok(Some(HttpResponse {
            status: head.status,
            headers: head.headers,
            body,
        }))
    }
}

fn parse_head(head: &[u8], body_start: usize) -> Result<Head, HttpError> {
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
    let framing = if chunked {
        Framing::Chunked
    } else if let Some(len) = header("content-length") {
        if len.is_empty() || !len.bytes().all(|b| b.is_ascii_digit()) {
            return Err(HttpError::Malformed("bad content-length"));
        }
        let len: usize = len
            .parse()
            .map_err(|_| HttpError::Malformed("bad content-length"))?;
        if len > MAX_HTTP_RESPONSE {
            return Err(HttpError::TooLarge);
        }
        Framing::Length(len)
    } else {
        Framing::UntilEof
    };
    Ok(Head {
        status,
        headers,
        body_start,
        framing,
    })
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

/// Decode chunks from `rest` (the bytes after the head), resuming at
/// `st.pos`. `Ok(true)` once the last chunk is seen; `Ok(false)` when more
/// bytes are needed.
fn decode_chunked(rest: &[u8], st: &mut ChunkState, eof: bool) -> Result<bool, HttpError> {
    let need_more = |eof: bool| {
        if eof {
            Err(HttpError::Malformed("truncated chunked body"))
        } else {
            Ok(false)
        }
    };
    loop {
        let cur = rest.get(st.pos..).unwrap_or_default();
        let Some(nl) = find(cur, b"\r\n") else {
            if cur.len() > MAX_CHUNK_LINE {
                return Err(HttpError::Malformed("chunk size line too long"));
            }
            return need_more(eof);
        };
        if nl > MAX_CHUNK_LINE {
            return Err(HttpError::Malformed("chunk size line too long"));
        }
        let line = cur.get(..nl).unwrap_or_default();
        let size_part = line.split(|&b| b == b';').next().unwrap_or_default();
        let size_str = std::str::from_utf8(size_part)
            .map_err(|_| HttpError::Malformed("bad chunk size"))?
            .trim();
        if size_str.is_empty() || !size_str.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(HttpError::Malformed("bad chunk size"));
        }
        let size = usize::from_str_radix(size_str, 16)
            .map_err(|_| HttpError::Malformed("bad chunk size"))?;
        if size == 0 {
            return Ok(true);
        }
        if size > MAX_HTTP_RESPONSE.saturating_sub(st.body.len()) {
            return Err(HttpError::TooLarge);
        }
        let after = cur.get(nl + 2..).unwrap_or_default();
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
        st.body.extend_from_slice(data);
        st.pos += nl + 2 + size + 2;
    }
}

/// Reject anything that could split or smuggle a request.
fn validate(req: &HttpRequest<'_>) -> Result<(), HttpError> {
    let token = |s: &str, extra: &[char]| {
        !s.is_empty()
            && !s
                .chars()
                .any(|c| c.is_ascii_control() || c == ' ' || extra.contains(&c))
    };
    let value_ok = |s: &str| !s.chars().any(|c| c == '\r' || c == '\n' || c == '\0');
    if !token(req.method, &[]) {
        return Err(HttpError::Malformed("bad request method"));
    }
    if !token(req.path, &[]) {
        return Err(HttpError::Malformed("bad request path"));
    }
    for (name, value) in req.headers {
        if !token(name, &[':']) {
            return Err(HttpError::Malformed("bad request header name"));
        }
        if !value_ok(value) {
            return Err(HttpError::Malformed("bad request header value"));
        }
    }
    Ok(())
}

/// Send `req` and read the reply, bounded by [`HTTP_TIMEOUT`] and
/// [`MAX_HTTP_RESPONSE`]. Also returns our local address on the connection.
pub async fn request(req: &HttpRequest<'_>) -> Result<(HttpResponse, SocketAddr), HttpError> {
    request_with_timeout(req, HTTP_TIMEOUT).await
}

/// [`request`] with a caller-chosen bound for the whole exchange. The request
/// is validated (no CR, LF or space in method or path, no CR or LF in header
/// names or values) before anything is sent.
pub async fn request_with_timeout(
    req: &HttpRequest<'_>,
    timeout: Duration,
) -> Result<(HttpResponse, SocketAddr), HttpError> {
    validate(req)?;
    tokio::time::timeout(timeout, exchange(req))
        .await
        .map_err(|_| HttpError::Timeout)?
}

async fn exchange(req: &HttpRequest<'_>) -> Result<(HttpResponse, SocketAddr), HttpError> {
    let mut stream = TcpStream::connect(req.host).await?;
    let local = stream.local_addr()?;
    stream.write_all(&req.to_bytes()).await?;
    let mut raw: Vec<u8> = Vec::new();
    let mut parser = Parser::default();
    let mut chunk = [0u8; READ_CHUNK];
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return match parser.advance(&raw, true)? {
                Some(resp) => Ok((resp, local)),
                None => Err(HttpError::Malformed("truncated response")),
            };
        }
        raw.extend_from_slice(chunk.get(..n).unwrap_or_default());
        if let Some(resp) = parser.advance(&raw, false)? {
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
            Parser::default().advance(b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nhi", false),
            Ok(None)
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
        let long = format!("HTTP/1.1 200 OK\r\nA: {}\r\n\r\n", "b".repeat(MAX_LINE - 2));
        assert!(parse_response(long.as_bytes()).is_err());
    }

    #[test]
    fn header_and_line_limits_are_exact() {
        let ok = format!("HTTP/1.1 200 OK\r\n{}\r\n", "A: b\r\n".repeat(64));
        assert_eq!(parse_response(ok.as_bytes()).unwrap().headers.len(), 64);
        let bad = format!("HTTP/1.1 200 OK\r\n{}\r\n", "A: b\r\n".repeat(65));
        assert!(parse_response(bad.as_bytes()).is_err());
        // "A: " is 3 bytes, so 4093 value bytes make a 4096-byte line.
        let ok = format!("HTTP/1.1 200 OK\r\nA: {}\r\n\r\n", "b".repeat(MAX_LINE - 3));
        assert!(parse_response(ok.as_bytes()).is_ok());
        let bad = format!("HTTP/1.1 200 OK\r\nA: {}\r\n\r\n", "b".repeat(MAX_LINE - 2));
        assert!(parse_response(bad.as_bytes()).is_err());
    }

    #[test]
    fn chunk_size_edge_cases_are_rejected() {
        let head = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
        for tail in [
            "ffffffffffffffffffff\r\nx",
            "+5\r\nhello\r\n0\r\n\r\n",
            "-5\r\n",
            "\r\n",
        ] {
            let r = parse_response(format!("{head}{tail}").as_bytes());
            assert!(matches!(r, Err(HttpError::Malformed(_))), "{tail:?}: {r:?}");
        }
        let long_line = format!("{head}{}\r\n", "1".repeat(100));
        assert!(matches!(
            parse_response(long_line.as_bytes()),
            Err(HttpError::Malformed(_))
        ));
        let no_crlf = format!("{head}{}", "1".repeat(100));
        assert!(matches!(
            parse_response(no_crlf.as_bytes()),
            Err(HttpError::Malformed(_))
        ));
    }

    #[test]
    fn plus_sign_content_length_is_rejected() {
        let r = parse_response(b"HTTP/1.1 200 OK\r\nContent-Length: +2\r\n\r\nok");
        assert!(matches!(r, Err(HttpError::Malformed(_))), "{r:?}");
    }

    #[test]
    fn accepts_bare_lf_line_endings() {
        let r = parse_response(b"HTTP/1.1 200 OK\nContent-Length: 2\nX: y\n\nok").unwrap();
        assert_eq!((r.status, r.body.as_slice()), (200, &b"ok"[..]));
        assert_eq!(r.headers.len(), 2);
        let r = parse_response(b"HTTP/1.0 200 OK\nX: y\n\nrest").unwrap();
        assert_eq!(r.body, b"rest");
    }

    #[test]
    fn incremental_parse_matches_whole_parse() {
        let msg: &[u8] =
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n3\r\n wo\r\n0\r\n\r\n";
        let mut p = Parser::default();
        let mut done = None;
        for n in 1..=msg.len() {
            if let Some(r) = p.advance(msg.get(..n).unwrap(), false).unwrap() {
                done = Some(r);
                break;
            }
        }
        assert_eq!(done.unwrap().body, b"hello wo");
    }

    #[tokio::test]
    async fn rejects_header_injection_before_connecting() {
        // Port 9 on loopback: validation must fail first, never connecting.
        let host: SocketAddr = "127.0.0.1:9".parse().unwrap();
        type Case<'a> = (&'a str, &'a str, &'a [(&'a str, &'a str)]);
        let cases: [Case<'_>; 7] = [
            ("GET\r\nX: y", "/", &[]),
            ("GET", "/ HTTP/1.1\r\nX: y", &[]),
            ("GET", "/a\nb", &[]),
            ("GE T", "/", &[]),
            ("GET", "/", &[("A\r\nB", "c")]),
            ("GET", "/", &[("A", "b\r\nC: d")]),
            ("GET", "/", &[("A:B", "c")]),
        ];
        for (method, path, headers) in cases {
            let req = HttpRequest {
                method,
                host,
                path,
                headers,
                body: b"",
            };
            let r = request_with_timeout(&req, Duration::from_secs(1)).await;
            assert!(
                matches!(r, Err(HttpError::Malformed(_))),
                "{method:?} {path:?} {headers:?}: {r:?}"
            );
        }
    }

    #[tokio::test]
    async fn head_plus_body_over_the_cap_is_too_large() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 512];
            let _ = s.read(&mut buf).await;
            // Body alone fits the cap; head plus body does not.
            let mut msg = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                MAX_HTTP_RESPONSE - 10
            )
            .into_bytes();
            msg.resize(msg.len() + MAX_HTTP_RESPONSE - 10, b'a');
            let _ = s.write_all(&msg).await;
            tokio::time::sleep(Duration::from_secs(2)).await;
        });
        let req = HttpRequest {
            method: "GET",
            host,
            path: "/",
            headers: &[],
            body: b"",
        };
        let r = request_with_timeout(&req, Duration::from_secs(3)).await;
        assert!(matches!(r, Err(HttpError::TooLarge)), "{r:?}");
    }

    proptest::proptest! {
        #[test]
        fn parse_response_never_panics(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..600)) {
            let _ = parse_response(&bytes);
        }

        #[test]
        fn parse_response_never_panics_on_near_valid(
            tail in "[0-9a-fA-F;+\\-:\\r\\n xX]{0,200}",
            chunked in proptest::prelude::any::<bool>(),
        ) {
            let head = if chunked { "Transfer-Encoding: chunked" } else { "Content-Length: 5" };
            let msg = format!("HTTP/1.1 200 OK\r\n{head}\r\n\r\n{tail}");
            let _ = parse_response(msg.as_bytes());
            let mut p = Parser::default();
            let _ = p.advance(msg.as_bytes(), false);
        }
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
