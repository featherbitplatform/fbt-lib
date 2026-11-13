//! A loopback HTTP proxy that tunnels CONNECT for real, or misbehaves as its
//! `Script` says.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Script {
    /// Every CONNECT is tunnelled.
    Ok,
    /// Every CONNECT is answered with this status, then closed.
    Status(u16),
    /// A 200 status line followed by header lines that never end.
    EndlessHead,
    /// CONNECT needs `Proxy-Authorization: Basic` with these credentials;
    /// anything else gets 407.
    RequireBasic(String, String),
    /// Tunnels like `Ok`, but these bytes follow the 200 head in the same
    /// write, as if the target had already spoken: a client that reads past
    /// the head loses them.
    Trailer(Vec<u8>),
    /// Tunnels like `Ok`, after this many `100 Continue` interim heads
    /// (`u32::MAX`: interim heads without end).
    Interim(u32),
}

pub struct FakeHttpProxy {
    addr: SocketAddr,
    connects: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<()>,
}

impl Drop for FakeHttpProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeHttpProxy {
    pub async fn start(script: Script) -> FakeHttpProxy {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connects = Arc::new(Mutex::new(Vec::new()));
        let log = connects.clone();
        let task = tokio::spawn(async move {
            let mut conns = JoinSet::new();
            while let Ok((stream, _)) = listener.accept().await {
                let script = script.clone();
                let log = log.clone();
                conns.spawn(async move {
                    let _ = serve(stream, script, log).await;
                });
            }
        });
        FakeHttpProxy {
            addr,
            connects,
            task,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// CONNECT authorities as sent (`127.0.0.1:80`, `localhost:80`).
    pub fn connects(&self) -> Vec<String> {
        self.connects.lock().unwrap().clone()
    }
}

async fn serve(
    mut s: TcpStream,
    script: Script,
    log: Arc<Mutex<Vec<String>>>,
) -> std::io::Result<()> {
    // Byte-wise to the blank line, so no tunnel byte is consumed.
    let mut head = Vec::new();
    let mut b = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if head.len() > 16384 || s.read(&mut b).await? == 0 {
            return Ok(());
        }
        head.push(b[0]);
    }
    let text = String::from_utf8_lossy(&head).into_owned();
    let mut lines = text.split("\r\n");
    let request = lines.next().unwrap_or_default();
    let mut parts = request.split(' ');
    if parts.next() != Some("CONNECT") {
        s.write_all(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
            .await?;
        return Ok(());
    }
    let authority = parts.next().unwrap_or_default().to_string();
    match &script {
        Script::Status(code) => {
            let msg =
                format!("HTTP/1.1 {code} Nope\r\nProxy-Authenticate: Basic realm=\"fake\"\r\n\r\n");
            s.write_all(msg.as_bytes()).await?;
            return Ok(());
        }
        Script::EndlessHead => {
            s.write_all(b"HTTP/1.1 200 OK\r\n").await?;
            let line = format!("X-Pad: {}\r\n", "a".repeat(100));
            loop {
                s.write_all(line.as_bytes()).await?;
            }
        }
        Script::RequireBasic(user, pass) => {
            let want = format!(
                "proxy-authorization: basic {}",
                base64(format!("{user}:{pass}").as_bytes())
            );
            if !lines.any(|l| l.eq_ignore_ascii_case(&want)) {
                s.write_all(
                    b"HTTP/1.1 407 Proxy Authentication Required\r\n\
                      Proxy-Authenticate: Basic realm=\"fake\"\r\n\r\n",
                )
                .await?;
                return Ok(());
            }
        }
        Script::Interim(n) => {
            for _ in 0..*n {
                s.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await?;
            }
        }
        Script::Ok | Script::Trailer(_) => {}
    }
    log.lock().unwrap().push(authority.clone());
    let mut up = match TcpStream::connect(authority.as_str()).await {
        Ok(up) => up,
        Err(_) => {
            s.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await?;
            return Ok(());
        }
    };
    let mut out = b"HTTP/1.1 200 Connection established\r\n\r\n".to_vec();
    if let Script::Trailer(bytes) = &script {
        out.extend_from_slice(bytes);
    }
    s.write_all(&out).await?;
    let _ = tokio::io::copy_bidirectional(&mut s, &mut up).await;
    Ok(())
}

fn base64(input: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(T[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}
