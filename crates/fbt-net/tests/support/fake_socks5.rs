//! A loopback SOCKS5 proxy (RFC 1928, user/pass RFC 1929) that relays CONNECT
//! and UDP ASSOCIATE for real, or misbehaves as its `Script` says.
#![allow(dead_code)]

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{Notify, watch};
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
    /// The connection is accepted, then nothing is read and nothing is
    /// answered: a client's greeting or request waits until its own timeout
    /// (or until the fake goes away and drops the connection).
    Silent,
    /// CONNECT is relayed, but UDP ASSOCIATE is answered with this `REP`: the
    /// proxy takes TCP and will not relay UDP.
    NoUdp(u8),
    /// As [`NoUdp`](Script::NoUdp): CONNECT is logged without dialling and UDP
    /// ASSOCIATE is answered with this `REP`. For a test whose "remote" is a
    /// documentation address or a name that must not be resolved here, and
    /// whose proxy will not carry UDP either, so nothing may leave the host.
    NoUdpRecordOnly(u8),
    /// The target is only logged, never reached: CONNECT is answered `REP 5`
    /// without dialling, and relayed UDP datagrams are logged and dropped. For
    /// tests whose "remote" is a documentation address or a name that must not
    /// be resolved here.
    RecordOnly,
    /// The UDP ASSOCIATE is granted (`REP 0`) and the control connection is
    /// closed at once: the client takes a relay and the proxy drops the
    /// association immediately, so a session that rebuilds on a relay flaps.
    DropAssociation,
}

#[derive(Default)]
struct Log {
    connects: Vec<String>,
    udp_targets: Vec<String>,
    /// The relay's own address, once a UDP ASSOCIATE has been answered.
    relay: Option<SocketAddr>,
}

/// A latch that parks a connection the fake has accepted but not answered
/// yet: a test proves a probe is in flight ([`waiting`](Self::waiting)) and
/// lets it finish ([`release`](Self::release)) instead of racing the clock.
#[derive(Clone, Debug)]
pub struct Gate {
    inner: Arc<GateInner>,
}

#[derive(Debug)]
struct GateInner {
    release: watch::Sender<bool>,
    waiting: AtomicUsize,
}

impl Gate {
    /// A gate that is closed: every connection the fake accepts from now on
    /// parks until [`release`](Self::release).
    pub fn new() -> Gate {
        let (release, _) = watch::channel(false);
        Gate {
            inner: Arc::new(GateInner {
                release,
                waiting: AtomicUsize::new(0),
            }),
        }
    }

    /// How many connections are parked at the gate now.
    pub fn waiting(&self) -> usize {
        self.inner.waiting.load(Ordering::SeqCst)
    }

    /// Opens the gate: every parked connection, and every later one, goes on.
    pub fn release(&self) {
        self.inner.release.send_replace(true);
    }

    /// Parks until the gate opens (or the fake is dropped, which lets the
    /// connection end with it).
    async fn wait(&self) {
        self.inner.waiting.fetch_add(1, Ordering::SeqCst);
        let mut open = self.inner.release.subscribe();
        while !*open.borrow() {
            if open.changed().await.is_err() {
                break;
            }
        }
        self.inner.waiting.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Default for Gate {
    fn default() -> Gate {
        Gate::new()
    }
}

pub struct FakeSocks5 {
    addr: SocketAddr,
    log: Arc<Mutex<Log>>,
    /// Asks the accept loop to stop accepting (see
    /// [`stop_accepting`](Self::stop_accepting)).
    stop: watch::Sender<bool>,
    /// Fires once the listener is gone (the port is free).
    stopped: Arc<Notify>,
    task: JoinHandle<()>,
}

impl Drop for FakeSocks5 {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeSocks5 {
    pub async fn start(script: Script) -> FakeSocks5 {
        FakeSocks5::start_on(SocketAddr::from(([127, 0, 0, 1], 0)), script).await
    }

    /// Starts the fake on `addr` (port 0 asks the OS for a free port), so a
    /// test can point a client at a port it knows before the client exists.
    pub async fn start_on(addr: SocketAddr, script: Script) -> FakeSocks5 {
        FakeSocks5::start_with(addr, script, None).await
    }

    /// [`start`](Self::start) with a [`Gate`].
    pub async fn start_gated(script: Script) -> (FakeSocks5, Gate) {
        FakeSocks5::start_on_gated(SocketAddr::from(([127, 0, 0, 1], 0)), script).await
    }

    /// [`start_on`](Self::start_on) with a [`Gate`]: every connection the
    /// fake accepts parks at the gate until the test releases it, so a test
    /// knows a probe is in flight before it lets it finish.
    pub async fn start_on_gated(addr: SocketAddr, script: Script) -> (FakeSocks5, Gate) {
        let gate = Gate::new();
        let fake = FakeSocks5::start_with(addr, script, Some(gate.clone())).await;
        (fake, gate)
    }

    async fn start_with(addr: SocketAddr, script: Script, gate: Option<Gate>) -> FakeSocks5 {
        let listener = TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let log = Arc::new(Mutex::new(Log::default()));
        let shared = log.clone();
        let (stop, mut stopping) = watch::channel(false);
        let stopped = Arc::new(Notify::new());
        let notify = stopped.clone();
        let task = tokio::spawn(async move {
            // Dropped (so every connection aborted) when this task is aborted.
            let mut conns = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break };
                        let script = script.clone();
                        let log = shared.clone();
                        let gate = gate.clone();
                        conns.spawn(async move {
                            let _ = serve(stream, script, log, gate).await;
                        });
                    }
                    r = stopping.changed() => {
                        if r.is_err() || *stopping.borrow_and_update() {
                            break;
                        }
                    }
                }
            }
            // Stop accepting: dropping the listener frees the port, while the
            // connections already accepted (a relay's control connection) stay
            // open until the fake itself is dropped.
            drop(listener);
            notify.notify_one();
            while conns.join_next().await.is_some() {}
        });
        FakeSocks5 {
            addr,
            log,
            stop,
            stopped,
            task,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stops accepting and closes the listen port at once, so a dial to
    /// [`addr`](Self::addr) is refused, while the connections already
    /// accepted -- a relay's control connection -- stay open until the fake
    /// is dropped. A dial refused this way is what makes a peer dial report
    /// `ProxyError::Unreachable` with the live relay still healthy.
    pub async fn stop_accepting(&self) {
        self.stop.send_replace(true);
        self.stopped.notified().await;
    }

    /// CONNECT targets as text (`127.0.0.1:80`, `localhost:80`).
    pub fn connects(&self) -> Vec<String> {
        self.log.lock().unwrap().connects.clone()
    }

    /// Destinations of relayed UDP datagrams, as text: an address
    /// (`127.0.0.1:6881`) or a name the proxy was asked to resolve
    /// (`tracker.invalid:6969`).
    pub fn udp_targets(&self) -> Vec<String> {
        self.log.lock().unwrap().udp_targets.clone()
    }

    /// The address a client should send relayed datagrams to, once a UDP
    /// ASSOCIATE has been answered; `None` before that. The reply names
    /// `0.0.0.0`, as many proxies do, meaning "the address you reached me at".
    pub fn relay_addr(&self) -> Option<SocketAddr> {
        self.log.lock().unwrap().relay
    }
}

/// A loopback address nothing is listening on: a port bound and dropped, so a
/// connect to it is refused at once instead of hanging.
pub fn closed_port() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    addr
}

async fn serve(
    mut s: TcpStream,
    script: Script,
    log: Arc<Mutex<Log>>,
    gate: Option<Gate>,
) -> std::io::Result<()> {
    if let Some(gate) = &gate {
        // Park before answering anything: the client's request is in flight
        // and goes no further until the test releases it.
        gate.wait().await;
    }
    if let Script::Silent = script {
        // Accept, then hold the connection open without reading or writing:
        // `pending` never resolves, so the stream lives until the fake is
        // dropped (which aborts this task and closes it).
        return std::future::pending().await;
    }
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
    if let Script::NoUdp(code) | Script::NoUdpRecordOnly(code) = script {
        // CONNECT carries on (only logged, for the record-only script); only
        // the UDP association is refused.
        if req[1] == 3 {
            s.write_all(&reply(code, SocketAddr::from(([0, 0, 0, 0], 0))))
                .await?;
            return Ok(());
        }
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

/// Where a CONNECT or a relayed datagram is meant to go.
enum Target {
    Addr(SocketAddr),
    Host(String, u16),
}

impl Target {
    /// The target as text (`127.0.0.1:80`, `localhost:80`).
    fn text(&self) -> String {
        match self {
            Target::Addr(a) => a.to_string(),
            Target::Host(h, p) => format!("{h}:{p}"),
        }
    }

    /// Dials the target of a CONNECT, resolving a name the way a proxy that
    /// resolves names does (the tests use loopback names only). Relayed UDP
    /// datagrams do not reach a name here: `associate` logs such a target and
    /// drops it, so a datagram a test means for a documentation name is never
    /// resolved nor forwarded.
    async fn dial(&self) -> std::io::Result<TcpStream> {
        match self {
            Target::Addr(a) => TcpStream::connect(*a).await,
            Target::Host(h, p) => TcpStream::connect((h.as_str(), *p)).await,
        }
    }
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
    log.lock().unwrap().connects.push(target.text());
    if matches!(script, Script::RecordOnly | Script::NoUdpRecordOnly(_)) {
        // The target must not be reached, only logged.
        return s
            .write_all(&reply(5, SocketAddr::from(([0, 0, 0, 0], 0))))
            .await;
    }
    let mut up = match target.dial().await {
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
    {
        let mut log = log.lock().unwrap();
        log.relay = Some(relay.local_addr()?);
    }
    s.write_all(&reply(0, bnd)).await?;
    if script == Script::DropAssociation {
        // Granted and dropped at once: the client has the association and the
        // control connection is gone in the same breath.
        return Ok(());
    }
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
                    log.lock().unwrap().udp_targets.push(dst.text());
                    if script == Script::RecordOnly {
                        // Logged, never forwarded: the target is not dialled.
                        continue;
                    }
                    // Only an address can be forwarded to; a name would have
                    // to be resolved, which this proxy does not do.
                    let Target::Addr(dst) = dst else { continue };
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

/// Reads a relayed datagram's destination and payload: the address (or name)
/// the client asked for, and the bytes after it.
fn decode(d: &[u8]) -> Option<(Target, &[u8])> {
    let ([_, _, frag, atyp], rest) = d.split_first_chunk::<4>()?;
    if *frag != 0 {
        return None;
    }
    let (mut target, rest) = match *atyp {
        1 => {
            let (ip, rest) = rest.split_first_chunk::<4>()?;
            (
                Target::Addr(SocketAddr::new(Ipv4Addr::from(*ip).into(), 0)),
                rest,
            )
        }
        4 => {
            let (ip, rest) = rest.split_first_chunk::<16>()?;
            (
                Target::Addr(SocketAddr::new(Ipv6Addr::from(*ip).into(), 0)),
                rest,
            )
        }
        3 => {
            let (len, rest) = rest.split_first()?;
            let (name, rest) = rest.split_at_checked(usize::from(*len))?;
            (
                Target::Host(String::from_utf8_lossy(name).into_owned(), 0),
                rest,
            )
        }
        _ => return None,
    };
    let (port, rest) = rest.split_first_chunk::<2>()?;
    match &mut target {
        Target::Addr(a) => a.set_port(u16::from_be_bytes(*port)),
        Target::Host(_, p) => *p = u16::from_be_bytes(*port),
    }
    Some((target, rest))
}
