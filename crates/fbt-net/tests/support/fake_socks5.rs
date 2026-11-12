//! A loopback SOCKS5 proxy (RFC 1928, user/pass RFC 1929) that relays CONNECT
//! and UDP ASSOCIATE for real, or misbehaves as its `Script` says.
#![allow(dead_code)]

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::task::{JoinHandle, JoinSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Script {
    /// No authentication; CONNECT and UDP ASSOCIATE relayed.
    Ok,
    /// Username/password required; a mismatch is answered with status 1.
    RequireAuth(String, String),
    /// Every request is answered with this `REP` code.
    Refuse(u8),
    /// The greeting is answered with bytes that are not SOCKS5.
    Garbage,
    /// The greeting is read, then the connection is closed.
    CloseAfterGreeting,
    /// UDP is relayed, but datagrams to the client come from another socket.
    RelayFromStranger,
    /// UDP is relayed, but datagrams to the client carry `FRAG = 1`.
    Fragmented,
    /// CONNECT is relayed, but the reply names a domain (`proxy.invalid`) as
    /// `BND.ADDR` and these bytes follow it in the same write, as if the
    /// target had already spoken: a client that reads past the reply loses
    /// them.
    Trailer(Vec<u8>),
}

#[derive(Default)]
struct Log {
    connects: Vec<String>,
    udp_targets: Vec<String>,
}

pub struct FakeSocks5 {
    addr: SocketAddr,
    log: Arc<Mutex<Log>>,
    task: JoinHandle<()>,
}

impl Drop for FakeSocks5 {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeSocks5 {
    pub async fn start(script: Script) -> FakeSocks5 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let log = Arc::new(Mutex::new(Log::default()));
        let shared = log.clone();
        let task = tokio::spawn(async move {
            // Dropped (so every connection aborted) when this task is aborted.
            let mut conns = JoinSet::new();
            while let Ok((stream, _)) = listener.accept().await {
                let script = script.clone();
                let log = shared.clone();
                conns.spawn(async move {
                    let _ = serve(stream, script, log).await;
                });
            }
        });
        FakeSocks5 { addr, log, task }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// CONNECT targets as text (`127.0.0.1:80`, `localhost:80`).
    pub fn connects(&self) -> Vec<String> {
        self.log.lock().unwrap().connects.clone()
    }

    /// Destinations of relayed UDP datagrams, as text.
    pub fn udp_targets(&self) -> Vec<String> {
        self.log.lock().unwrap().udp_targets.clone()
    }
}

async fn serve(mut s: TcpStream, script: Script, log: Arc<Mutex<Log>>) -> std::io::Result<()> {
    // Greeting: VER NMETHODS METHODS.
    let mut head = [0u8; 2];
    s.read_exact(&mut head).await?;
    let mut methods = vec![0u8; head[1] as usize];
    s.read_exact(&mut methods).await?;
    match &script {
        Script::Garbage => {
            s.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await?;
            return Ok(());
        }
        Script::CloseAfterGreeting => return Ok(()),
        Script::RequireAuth(user, pass) => {
            if !methods.contains(&2) {
                s.write_all(&[5, 0xff]).await?;
                return Ok(());
            }
            s.write_all(&[5, 2]).await?;
            let mut v = [0u8; 2];
            s.read_exact(&mut v).await?;
            let mut u = vec![0u8; v[1] as usize];
            s.read_exact(&mut u).await?;
            let mut pl = [0u8; 1];
            s.read_exact(&mut pl).await?;
            let mut p = vec![0u8; pl[0] as usize];
            s.read_exact(&mut p).await?;
            if u != user.as_bytes() || p != pass.as_bytes() {
                s.write_all(&[1, 1]).await?;
                return Ok(());
            }
            s.write_all(&[1, 0]).await?;
        }
        _ => {
            if !methods.contains(&0) {
                s.write_all(&[5, 0xff]).await?;
                return Ok(());
            }
            s.write_all(&[5, 0]).await?;
        }
    }
    // Request: VER CMD RSV ATYP DST.ADDR DST.PORT.
    let mut req = [0u8; 4];
    s.read_exact(&mut req).await?;
    let target = read_addr(&mut s, req[3]).await?;
    if let Script::Refuse(code) = script {
        s.write_all(&[5, code, 0, 1, 0, 0, 0, 0, 0, 0]).await?;
        return Ok(());
    }
    match req[1] {
        1 => connect(s, target, script, log).await,
        3 => associate(s, script, log).await,
        _ => {
            s.write_all(&[5, 7, 0, 1, 0, 0, 0, 0, 0, 0]).await?;
            Ok(())
        }
    }
}

enum Target {
    Addr(SocketAddr),
    Host(String, u16),
}

async fn read_addr(s: &mut TcpStream, atyp: u8) -> std::io::Result<Target> {
    let ip = match atyp {
        1 => {
            let mut b = [0u8; 4];
            s.read_exact(&mut b).await?;
            IpAddr::from(b)
        }
        4 => {
            let mut b = [0u8; 16];
            s.read_exact(&mut b).await?;
            IpAddr::from(b)
        }
        3 => {
            let mut l = [0u8; 1];
            s.read_exact(&mut l).await?;
            let mut h = vec![0u8; l[0] as usize];
            s.read_exact(&mut h).await?;
            let port = s.read_u16().await?;
            return Ok(Target::Host(String::from_utf8_lossy(&h).into_owned(), port));
        }
        _ => return Err(std::io::Error::other("bad atyp")),
    };
    let port = s.read_u16().await?;
    Ok(Target::Addr(SocketAddr::new(ip, port)))
}

fn reply(rep: u8, bnd: SocketAddr) -> Vec<u8> {
    let mut out = vec![5, rep, 0];
    match bnd.ip() {
        IpAddr::V4(ip) => {
            out.push(1);
            out.extend_from_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            out.push(4);
            out.extend_from_slice(&ip.octets());
        }
    }
    out.extend_from_slice(&bnd.port().to_be_bytes());
    out
}

async fn connect(
    mut s: TcpStream,
    target: Target,
    script: Script,
    log: Arc<Mutex<Log>>,
) -> std::io::Result<()> {
    let (text, dialled) = match target {
        Target::Addr(a) => (a.to_string(), TcpStream::connect(a).await),
        Target::Host(h, p) => (
            format!("{h}:{p}"),
            TcpStream::connect((h.as_str(), p)).await,
        ),
    };
    log.lock().unwrap().connects.push(text);
    let mut up = match dialled {
        Ok(up) => up,
        Err(_) => {
            s.write_all(&reply(5, SocketAddr::from(([0, 0, 0, 0], 0))))
                .await?;
            return Ok(());
        }
    };
    if let Script::Trailer(bytes) = &script {
        let mut out = vec![5, 0, 0, 3, 13];
        out.extend_from_slice(b"proxy.invalid");
        out.extend_from_slice(&1080u16.to_be_bytes());
        out.extend_from_slice(bytes);
        s.write_all(&out).await?;
    } else {
        s.write_all(&reply(0, up.local_addr()?)).await?;
    }
    let _ = tokio::io::copy_bidirectional(&mut s, &mut up).await;
    Ok(())
}

async fn associate(mut s: TcpStream, script: Script, log: Arc<Mutex<Log>>) -> std::io::Result<()> {
    let relay = UdpSocket::bind("127.0.0.1:0").await?;
    let stranger = UdpSocket::bind("127.0.0.1:0").await?;
    // `0.0.0.0` means "the address you reached me at", as many proxies say.
    let bnd = SocketAddr::from(([0, 0, 0, 0], relay.local_addr()?.port()));
    s.write_all(&reply(0, bnd)).await?;
    let mut client: Option<SocketAddr> = None;
    let mut buf = vec![0u8; 65536];
    let mut ctl = [0u8; 64];
    loop {
        tokio::select! {
            r = s.read(&mut ctl) => {
                if matches!(r, Ok(0) | Err(_)) {
                    return Ok(());
                }
            }
            r = relay.recv_from(&mut buf) => {
                let (n, from) = r?;
                let data = &buf[..n];
                if client.is_none() || client == Some(from) {
                    client = Some(from);
                    let Some((dst, payload)) = decode(data) else { continue };
                    log.lock().unwrap().udp_targets.push(dst.to_string());
                    relay.send_to(payload, dst).await?;
                } else if let Some(c) = client {
                    let mut out = vec![0, 0, 0];
                    if script == Script::Fragmented {
                        out[2] = 1;
                    }
                    push_addr(&mut out, from);
                    out.extend_from_slice(data);
                    if script == Script::RelayFromStranger {
                        stranger.send_to(&out, c).await?;
                    } else {
                        relay.send_to(&out, c).await?;
                    }
                }
            }
        }
    }
}

fn push_addr(out: &mut Vec<u8>, a: SocketAddr) {
    match a.ip() {
        IpAddr::V4(ip) => {
            out.push(1);
            out.extend_from_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            out.push(4);
            out.extend_from_slice(&ip.octets());
        }
    }
    out.extend_from_slice(&a.port().to_be_bytes());
}

fn decode(d: &[u8]) -> Option<(SocketAddr, &[u8])> {
    if d.len() < 4 || d[2] != 0 {
        return None;
    }
    let (ip, rest): (IpAddr, &[u8]) = match d[3] {
        1 if d.len() >= 10 => {
            let b: [u8; 4] = d[4..8].try_into().ok()?;
            (Ipv4Addr::from(b).into(), &d[8..])
        }
        4 if d.len() >= 22 => {
            let b: [u8; 16] = d[4..20].try_into().ok()?;
            (Ipv6Addr::from(b).into(), &d[20..])
        }
        _ => return None,
    };
    let port = u16::from_be_bytes([rest[0], rest[1]]);
    Some((SocketAddr::new(ip, port), &rest[2..]))
}
