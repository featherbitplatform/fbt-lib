//! A loopback HTTP/1.1 web seed (BEP 19) serving the files below a root
//! directory, with byte ranges and keep-alive, or misbehaving as its
//! [`Behaviour`] says. Built on tokio alone, so the client under test is
//! the only HTTP implementation involved.
#![allow(dead_code, reason = "each test uses a different subset")]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};

/// How the server answers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Behaviour {
    /// `206` with the requested range, `200` with the whole file otherwise.
    Normal,
    /// Always `200` with the whole file, whatever the `Range`.
    IgnoreRange,
    /// `206` with the requested bytes, but a `Content-Range` one byte off.
    WrongContentRange,
    /// `206` with a right `Content-Range`, then a body that never ends (no
    /// `Content-Length`, closed only when the client goes).
    EndlessBody,
    /// `302` to the same path, every time.
    RedirectLoop,
    /// `404` for this path (as requested, percent-encoded), the rest as
    /// [`Normal`](Self::Normal).
    NotFound(String),
    /// As [`Normal`](Self::Normal), with the byte at this offset of every
    /// file flipped.
    Corrupt(u64),
}

/// Most bytes of a request head the server reads.
const MAX_HEAD: usize = 16 * 1024;
/// The most an endless body sends before giving up on the client.
const ENDLESS_CAP: u64 = 256 << 20;

struct State {
    root: PathBuf,
    behaviour: Behaviour,
    requests: Mutex<Vec<(String, Option<String>)>>,
    /// Bytes per second of body; 0 is unlimited.
    rate: AtomicU64,
    /// Body bytes written.
    sent: AtomicU64,
}

pub struct WebSeedServer {
    addr: std::net::SocketAddr,
    state: Arc<State>,
    task: JoinHandle<()>,
}

impl Drop for WebSeedServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl WebSeedServer {
    /// Serves the files below `root` on a loopback port.
    pub async fn start(root: PathBuf, behaviour: Behaviour) -> WebSeedServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(State {
            root,
            behaviour,
            requests: Mutex::new(Vec::new()),
            rate: AtomicU64::new(0),
            sent: AtomicU64::new(0),
        });
        let shared = state.clone();
        let task = tokio::spawn(async move {
            // Dropped (so every connection aborted) with this task.
            let mut conns = JoinSet::new();
            while let Ok((stream, _)) = listener.accept().await {
                let state = shared.clone();
                conns.spawn(async move {
                    let _ = serve(stream, state).await;
                });
            }
        });
        WebSeedServer { addr, state, task }
    }

    /// The base URL, ending in `/`: the torrent's name and paths follow.
    pub fn url(&self) -> String {
        format!("http://{}/", self.addr)
    }

    pub fn addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    /// Every request: its path and its `Range` header, if any.
    pub fn requests(&self) -> Vec<(String, Option<String>)> {
        self.state.requests.lock().unwrap().clone()
    }

    /// Limits the body bytes sent per second over all connections (0:
    /// unlimited).
    pub fn set_rate(&self, bytes_per_sec: u64) {
        self.state.rate.store(bytes_per_sec, Ordering::SeqCst);
    }

    /// Body bytes sent so far.
    pub fn sent(&self) -> u64 {
        self.state.sent.load(Ordering::SeqCst)
    }
}

/// One connection: requests answered in turn until either side closes.
async fn serve(mut stream: TcpStream, state: Arc<State>) -> std::io::Result<()> {
    let mut buf = Vec::new();
    loop {
        let Some(head) = read_head(&mut stream, &mut buf).await? else {
            return Ok(());
        };
        let mut lines = head.split("\r\n");
        let request_line = lines.next().unwrap_or_default();
        let path = request_line
            .split(' ')
            .nth(1)
            .unwrap_or_default()
            .to_owned();
        let range = lines
            .filter_map(|l| l.split_once(':'))
            .find(|(k, _)| k.trim().eq_ignore_ascii_case("range"))
            .map(|(_, v)| v.trim().to_owned());
        state
            .requests
            .lock()
            .unwrap()
            .push((path.clone(), range.clone()));
        if !answer(&mut stream, &state, &path, range.as_deref()).await? {
            return Ok(());
        }
    }
}

/// Reads one request head (up to the blank line) out of `stream`, keeping
/// what follows in `buf`; `None` once the client closed.
async fn read_head(stream: &mut TcpStream, buf: &mut Vec<u8>) -> std::io::Result<Option<String>> {
    loop {
        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..end]).into_owned();
            buf.drain(..end + 4);
            return Ok(Some(head));
        }
        if buf.len() > MAX_HEAD {
            return Ok(None);
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// Answers one request; false when the connection must close.
async fn answer(
    stream: &mut TcpStream,
    state: &State,
    path: &str,
    range: Option<&str>,
) -> std::io::Result<bool> {
    if let Behaviour::NotFound(p) = &state.behaviour
        && p == path
    {
        not_found(stream).await?;
        return Ok(true);
    }
    if state.behaviour == Behaviour::RedirectLoop {
        let head = format!("HTTP/1.1 302 Found\r\nLocation: {path}\r\nContent-Length: 0\r\n\r\n");
        stream.write_all(head.as_bytes()).await?;
        return Ok(true);
    }
    let Some(file) = file_for(&state.root, path) else {
        not_found(stream).await?;
        return Ok(true);
    };
    let Ok(mut data) = std::fs::read(&file) else {
        not_found(stream).await?;
        return Ok(true);
    };
    if let Behaviour::Corrupt(off) = state.behaviour
        && let Some(b) = data.get_mut(off as usize)
    {
        *b = !*b;
    }
    let total = data.len() as u64;
    let wanted = range.and_then(|r| parse_range(r, total));
    match (&state.behaviour, wanted) {
        (Behaviour::IgnoreRange, _) | (_, None) => {
            let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {total}\r\n\r\n");
            stream.write_all(head.as_bytes()).await?;
            send_body(stream, state, &data).await?;
        }
        (Behaviour::EndlessBody, Some((a, b))) => {
            let head = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {a}-{b}/{total}\r\nConnection: close\r\n\r\n"
            );
            stream.write_all(head.as_bytes()).await?;
            let chunk = vec![0x5a_u8; 16 * 1024];
            let mut sent = 0u64;
            while sent < ENDLESS_CAP {
                send_body(stream, state, &chunk).await?;
                sent += chunk.len() as u64;
            }
            return Ok(false);
        }
        (behaviour, Some((a, b))) => {
            let (ca, cb) = if *behaviour == Behaviour::WrongContentRange {
                (a + 1, b + 1)
            } else {
                (a, b)
            };
            let body = &data[a as usize..=b as usize];
            let head = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {ca}-{cb}/{total}\r\nContent-Length: {}\r\n\r\n",
                body.len()
            );
            stream.write_all(head.as_bytes()).await?;
            send_body(stream, state, body).await?;
        }
    }
    Ok(true)
}

async fn not_found(s: &mut TcpStream) -> std::io::Result<()> {
    s.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
        .await
}

/// Writes `body` at the server's rate.
async fn send_body(stream: &mut TcpStream, state: &State, body: &[u8]) -> std::io::Result<()> {
    let mut rest = body;
    while !rest.is_empty() {
        let rate = state.rate.load(Ordering::SeqCst);
        let n = if rate == 0 {
            rest.len()
        } else {
            // A tenth of a second's worth, then the rest of that tenth.
            (rate / 10).max(1).min(rest.len() as u64) as usize
        };
        let (now, later) = rest.split_at(n);
        stream.write_all(now).await?;
        state.sent.fetch_add(n as u64, Ordering::SeqCst);
        rest = later;
        if rate != 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    Ok(())
}

/// `bytes=a-b` (inclusive), clamped to the file; `None` when unusable.
fn parse_range(r: &str, total: u64) -> Option<(u64, u64)> {
    let spec = r.strip_prefix("bytes=")?;
    let (a, b) = spec.split_once('-')?;
    let a: u64 = a.trim().parse().ok()?;
    let b: u64 = b.trim().parse().ok()?;
    (a <= b && b < total).then_some((a, b))
}

/// The file below `root` that the percent-encoded `path` names; `None` for
/// anything leaving `root`.
fn file_for(root: &Path, path: &str) -> Option<PathBuf> {
    let mut out = root.to_path_buf();
    for part in path.trim_start_matches('/').split('/') {
        let name = percent_decode(part)?;
        if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\']) {
            return None;
        }
        out.push(name);
    }
    Some(out)
}

fn percent_decode(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let hex = std::str::from_utf8(b.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}
