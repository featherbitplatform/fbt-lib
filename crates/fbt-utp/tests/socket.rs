//! The tokio uTP runtime over real loopback UDP: standalone and shared
//! sockets, bulk transfer, timeouts, SYN floods, spoofed resets, the accept
//! filter, FIN on drop and shutdown.

use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use fbt_core::udp::{DatagramHandler, DatagramSender};
use fbt_net::demux::{PacketKind, SharedUdpSocket};
use fbt_utp::{Packet, PacketType, UtpConfig, UtpError, UtpSocket, UtpStream};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Upper bound on any single test step, so a bug fails instead of hanging.
const STEP: Duration = Duration::from_secs(30);

fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

async fn bind(cfg: UtpConfig) -> UtpSocket {
    let mut last = None;
    for _ in 0..5 {
        match UtpSocket::bind(loopback(), cfg.clone()).await {
            Ok(s) => return s,
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("bind failed: {last:?}");
}

fn shared_bind() -> SharedUdpSocket {
    let mut last = None;
    for _ in 0..5 {
        match SharedUdpSocket::bind(loopback()) {
            Ok(s) => return s,
            Err(e) => last = Some(e),
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("bind failed: {last:?}");
}

async fn step<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(STEP, f)
        .await
        .expect("test step timed out")
}

/// A connected (client, server) stream pair, confirmed: the server has
/// heard from the dialer after the handshake (one byte, already read).
async fn pair(client: &UtpSocket, server: &UtpSocket) -> (UtpStream, UtpStream) {
    let srv = server.clone();
    let accept = tokio::spawn(async move { srv.accept().await });
    let c = step(client.connect(server.local_addr())).await.unwrap();
    let (s, from) = step(accept).await.unwrap().unwrap();
    assert_eq!(from, client.local_addr());
    assert_eq!(c.peer_addr(), server.local_addr());
    assert_eq!(s.peer_addr(), client.local_addr());
    let (mut c, mut s) = (c, s);
    step(c.write_all(&[0])).await.unwrap();
    let mut b = [1u8];
    step(s.read_exact(&mut b)).await.unwrap();
    assert_eq!(b, [0]);
    (c, s)
}

async fn echo_roundtrip(c: &mut UtpStream, s: &mut UtpStream, msg: &[u8]) {
    step(c.write_all(msg)).await.unwrap();
    let mut got = vec![0u8; msg.len()];
    step(s.read_exact(&mut got)).await.unwrap();
    assert_eq!(got, msg);
    step(s.write_all(&got)).await.unwrap();
    let mut back = vec![0u8; msg.len()];
    step(c.read_exact(&mut back)).await.unwrap();
    assert_eq!(back, msg);
}

fn packet(ty: PacketType, conn_id: u16, seq_nr: u16) -> Vec<u8> {
    let p = Packet {
        ty,
        conn_id,
        timestamp_us: 0,
        timestamp_diff_us: 0,
        wnd_size: 1 << 20,
        seq_nr,
        ack_nr: 0,
        sack: None,
        payload: Bytes::new(),
    };
    let mut out = Vec::new();
    p.encode(&mut out);
    out
}

fn short_timeouts() -> UtpConfig {
    let mut cfg = UtpConfig::default();
    cfg.connect_timeout = Duration::from_millis(300);
    cfg
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_accept_echo() {
    let server = bind(UtpConfig::default()).await;
    let client = bind(UtpConfig::default()).await;
    // Futures and streams can move to spawned tasks (the session does).
    fn assert_send<T: Send>(_: &T) {}
    fn assert_stream<T: Send + Unpin + tokio::io::AsyncRead + tokio::io::AsyncWrite>() {}
    assert_send(&client.connect(server.local_addr()));
    assert_send(&client.accept());
    assert_stream::<UtpStream>();
    let (mut c, mut s) = pair(&client, &server).await;
    echo_roundtrip(&mut c, &mut s, b"hello over utp").await;
    assert_eq!(client.num_connections(), 1);
    assert_eq!(server.num_connections(), 1);
}

#[derive(Debug, Default)]
struct CountingHandler {
    seen: Mutex<Vec<Vec<u8>>>,
}

impl DatagramHandler for CountingHandler {
    fn handle(&self, _from: SocketAddr, data: &[u8]) {
        self.seen.lock().unwrap().push(data.to_vec());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_mode_through_a_handler() {
    let udp = shared_bind();
    let server = UtpSocket::new(udp.sender(), UtpConfig::default());
    udp.set_handler(PacketKind::Utp, server.handler());
    let dht = Arc::new(CountingHandler::default());
    udp.set_handler(PacketKind::Dht, dht.clone());
    assert_eq!(server.local_addr(), udp.local_addr());

    let client = bind(UtpConfig::default()).await;
    let (mut c, mut s) = pair(&client, &server).await;
    echo_roundtrip(&mut c, &mut s, b"shared port").await;

    // The DHT kind still reaches its own handler, untouched by uTP.
    let raw = std::net::UdpSocket::bind(loopback()).unwrap();
    let krpc = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe";
    raw.send_to(krpc, udp.local_addr()).unwrap();
    step(async {
        while dht.seen.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(dht.seen.lock().unwrap().as_slice(), &[krpc.to_vec()]);
    let stats = udp.stats();
    assert!(stats.utp > 0);
    assert_eq!(stats.dht, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_transfer_both_directions() {
    const LEN: usize = 16 << 20;
    let server = bind(UtpConfig::default()).await;
    let client = bind(UtpConfig::default()).await;
    let (c, s) = pair(&client, &server).await;

    fn data(seed: u64) -> Vec<u8> {
        let mut v = vec![0u8; LEN];
        StdRng::seed_from_u64(seed).fill(&mut v[..]);
        v
    }
    let up = data(1);
    let down = data(2);
    let up_hash = Sha1::digest(&up).to_vec();
    let down_hash = Sha1::digest(&down).to_vec();

    async fn run(stream: UtpStream, out: Vec<u8>) -> Vec<u8> {
        let (mut r, mut w) = tokio::io::split(stream);
        let writer = tokio::spawn(async move {
            w.write_all(&out).await.unwrap();
            w.shutdown().await.unwrap();
            w
        });
        let mut hasher = Sha1::new();
        let mut buf = vec![0u8; 64 * 1024];
        let mut total = 0usize;
        loop {
            let n = r.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            total += n;
            hasher.update(&buf[..n]);
        }
        assert_eq!(total, LEN);
        let _w = writer.await.unwrap();
        hasher.finalize().to_vec()
    }

    let started = Instant::now();
    let (got_up, got_down) = step(async {
        tokio::join!(
            tokio::spawn(run(s, down.clone())),
            tokio::spawn(run(c, up.clone()))
        )
    })
    .await;
    assert_eq!(got_up.unwrap(), up_hash, "client to server");
    assert_eq!(got_down.unwrap(), down_hash, "server to client");
    eprintln!("16 MiB each way in {:?}", started.elapsed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_to_silent_port_times_out() {
    // Bound but never read: no reply and no ICMP unreachable.
    let silent = std::net::UdpSocket::bind(loopback()).unwrap();
    let client = bind(short_timeouts()).await;
    let started = Instant::now();
    let r = step(client.connect(silent.local_addr().unwrap())).await;
    assert_eq!(r.unwrap_err(), UtpError::ConnectTimeout);
    assert!(started.elapsed() >= Duration::from_millis(300));
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(client.num_connections(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn syn_flood_is_bounded() {
    let mut cfg = UtpConfig::default();
    cfg.max_pending_accept = 64;
    cfg.idle_timeout = Duration::from_millis(500);
    cfg.connect_timeout = Duration::from_millis(500);
    let server = bind(cfg.clone()).await;

    let raw = tokio::net::UdpSocket::bind(loopback()).await.unwrap();
    let mut rng = rand::rng();
    for _ in 0..10_000 {
        let syn = packet(PacketType::Syn, rng.random(), 1);
        raw.send_to(&syn, server.local_addr()).await.unwrap();
        assert!(server.num_connections() <= 64);
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let n = server.num_connections();
    assert!(n <= 64, "{n} connections");
    assert!(n > 0, "no forged SYN reached the server");

    // Once the pending entries time out, a real dialer gets in.
    let client = bind(cfg).await;
    let deadline = Instant::now() + Duration::from_secs(20);
    let stream = loop {
        assert!(server.num_connections() <= 64);
        match client.connect(server.local_addr()).await {
            Ok(s) => break s,
            Err(UtpError::ConnectTimeout) if Instant::now() < deadline => continue,
            Err(e) => panic!("connect failed: {e}"),
        }
    };
    assert_eq!(stream.peer_addr(), server.local_addr());
}

/// Records the connection id of every SYN sent through it.
#[derive(Debug)]
struct RecordingSender {
    inner: Arc<dyn DatagramSender>,
    syn_ids: Mutex<Vec<u16>>,
}

impl DatagramSender for RecordingSender {
    fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr()
    }

    fn send_to(&self, data: &[u8], to: SocketAddr) -> io::Result<usize> {
        if let Ok(p) = Packet::decode(&Bytes::copy_from_slice(data))
            && p.ty == PacketType::Syn
        {
            self.syn_ids.lock().unwrap().push(p.conn_id);
        }
        self.inner.send_to(data, to)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reset_from_wrong_address_is_ignored() {
    let server = bind(UtpConfig::default()).await;
    let udp = shared_bind();
    let rec = Arc::new(RecordingSender {
        inner: udp.sender(),
        syn_ids: Mutex::new(Vec::new()),
    });
    let client = UtpSocket::new(rec.clone(), UtpConfig::default());
    udp.set_handler(PacketKind::Utp, client.handler());

    let (mut c, mut s) = pair(&client, &server).await;
    let id = *rec.syn_ids.lock().unwrap().first().unwrap();

    // The client receives with `id`, the server with `id + 1`; an attacker
    // on another port knows both and resets each side with each.
    let attacker = std::net::UdpSocket::bind(loopback()).unwrap();
    for conn_id in [id, id.wrapping_add(1)] {
        for seq in [0u16, 1, 2, 1000] {
            let rst = packet(PacketType::Reset, conn_id, seq);
            attacker.send_to(&rst, client.local_addr()).unwrap();
            attacker.send_to(&rst, server.local_addr()).unwrap();
        }
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    echo_roundtrip(&mut c, &mut s, b"still here").await;
    assert_eq!(client.num_connections(), 1);
    assert_eq!(server.num_connections(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accept_filter_drops_unsolicited_syns() {
    let server = bind(UtpConfig::default()).await;
    server.set_accept_filter(Some(Arc::new(|_| false)));
    let client = bind(short_timeouts()).await;
    let r = step(client.connect(server.local_addr())).await;
    assert_eq!(r.unwrap_err(), UtpError::ConnectTimeout);
    assert_eq!(server.num_connections(), 0);

    // A filter that admits only this client lets it in.
    let allowed = client.local_addr();
    server.set_accept_filter(Some(Arc::new(move |a| a == allowed)));
    let (mut c, mut s) = pair(&client, &server).await;
    echo_roundtrip(&mut c, &mut s, b"let in").await;

    // Others are still dropped.
    let other = bind(short_timeouts()).await;
    let r = step(other.connect(server.local_addr())).await;
    assert_eq!(r.unwrap_err(), UtpError::ConnectTimeout);
    assert_eq!(server.num_connections(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_sends_fin_and_peer_reads_eof() {
    let server = bind(UtpConfig::default()).await;
    let client = bind(UtpConfig::default()).await;
    let (mut c, mut s) = pair(&client, &server).await;
    step(c.write_all(b"last words")).await.unwrap();
    drop(c);
    let mut got = Vec::new();
    step(s.read_to_end(&mut got)).await.unwrap();
    assert_eq!(got, b"last words");
    // EOF stays EOF.
    let mut buf = [0u8; 8];
    assert_eq!(step(s.read(&mut buf)).await.unwrap(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_is_idempotent_and_wakes_streams() {
    let mut scfg = UtpConfig::default();
    scfg.idle_timeout = Duration::from_secs(1);
    let server = bind(scfg).await;
    let client = bind(UtpConfig::default()).await;
    let (mut c, mut s) = pair(&client, &server).await;

    let reader = tokio::spawn(async move {
        let mut buf = [0u8; 16];
        c.read(&mut buf).await
    });
    let acceptor = {
        let client = client.clone();
        tokio::spawn(async move { client.accept().await.map(|_| ()) })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;

    step(client.shutdown()).await;
    step(client.shutdown()).await;

    // The blocked reader and acceptor wake with errors.
    assert!(step(reader).await.unwrap().is_err());
    assert_eq!(step(acceptor).await.unwrap(), Err(UtpError::Closed));
    // The peer is reset, or (should that one datagram be lost) times out.
    let mut buf = [0u8; 16];
    let e = step(s.read(&mut buf)).await.unwrap_err();
    assert!(
        matches!(
            e.kind(),
            io::ErrorKind::ConnectionReset | io::ErrorKind::TimedOut
        ),
        "{e:?}"
    );
    // The socket refuses further work.
    assert_eq!(client.num_connections(), 0);
    assert_eq!(
        step(client.connect(server.local_addr())).await.unwrap_err(),
        UtpError::Closed
    );
    assert_eq!(step(client.accept()).await.unwrap_err(), UtpError::Closed);
}

#[tokio::test(flavor = "current_thread")]
async fn last_handle_drop_ends_tasks() {
    let metrics = tokio::runtime::Handle::current().metrics();
    let before = metrics.num_alive_tasks();
    let server = bind(UtpConfig::default()).await;
    let client = bind(UtpConfig::default()).await;
    let (c, s) = pair(&client, &server).await;
    assert!(metrics.num_alive_tasks() > before);
    drop((c, s, client, server));
    step(async {
        while metrics.num_alive_tasks() > before {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reset_by_send_id_from_the_peer_fails_the_stream() {
    // Both ends in shared mode: the server's sender can forge datagrams
    // from the server's real address.
    let server_udp = shared_bind();
    let server = UtpSocket::new(server_udp.sender(), UtpConfig::default());
    server_udp.set_handler(PacketKind::Utp, server.handler());
    let client_udp = shared_bind();
    let rec = Arc::new(RecordingSender {
        inner: client_udp.sender(),
        syn_ids: Mutex::new(Vec::new()),
    });
    let client = UtpSocket::new(rec.clone(), UtpConfig::default());
    client_udp.set_handler(PacketKind::Utp, client.handler());

    let (mut c, _s) = pair(&client, &server).await;
    // The client receives with `id` and sends with `id + 1`; libutp's reset
    // carries the id of the packet it answers, i.e. the client's send id.
    let id = *rec.syn_ids.lock().unwrap().first().unwrap();
    let rst = packet(PacketType::Reset, id.wrapping_add(1), 0);
    server_udp
        .sender()
        .send_to(&rst, client.local_addr())
        .unwrap();
    let mut buf = [0u8; 8];
    let e = step(c.read(&mut buf)).await.unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::ConnectionReset);
    step(async {
        while client.num_connections() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
}

/// Sends forged SYNs with random ids from `raw` to `to` until stopped.
fn flood(raw: Arc<tokio::net::UdpSocket>, to: SocketAddr) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            for _ in 0..200 {
                let id = rand::rng().random();
                let _ = raw.send_to(&packet(PacketType::Syn, id, 1), to).await;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn syn_flood_from_one_address_cannot_lock_out_others() {
    // Default timeouts: half-open entries live 7 s, so only the per-IP cap
    // keeps the queue open. The flood is fed straight into the handler with
    // a forged source IP (127.0.0.3), so this runs where only 127.0.0.1 is
    // bindable; the honest dialer is real.
    let udp = shared_bind();
    let server = UtpSocket::new(udp.sender(), UtpConfig::default());
    udp.set_handler(PacketKind::Utp, server.handler());
    let handler = server.handler();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flooding = {
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut rng = rand::rng();
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let from = SocketAddr::from(([127, 0, 0, 3], rng.random_range(1024..65535u16)));
                handler.handle(from, &packet(PacketType::Syn, rng.random(), 1));
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    let n = server.num_connections();
    assert!(n > 0, "no forged SYN reached the server");
    assert!(n <= fbt_utp::MAX_PENDING_PER_IP, "{n} half-open entries");

    // An honest dialer from another address gets in during the flood.
    let honest = bind(UtpConfig::default()).await;
    let started = Instant::now();
    let srv = server.clone();
    let honest_addr = honest.local_addr();
    let accept = tokio::spawn(async move {
        loop {
            let (s, from) = srv.accept().await.unwrap();
            if from == honest_addr {
                return s;
            }
        }
    });
    let mut c = tokio::time::timeout(Duration::from_secs(3), honest.connect(server.local_addr()))
        .await
        .expect("honest connect stalled during the flood")
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(3));
    let mut s = step(accept).await.unwrap();
    echo_roundtrip(&mut c, &mut s, b"honest").await;
    assert!(server.num_connections() <= fbt_utp::MAX_PENDING_PER_IP + 1);
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    flooding.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unconfirmed_syns_expire_after_connect_timeout() {
    // Even from the flooding address, a dialer gets in once the half-open
    // entries expire, long before the 60 s idle timeout.
    let mut cfg = UtpConfig::default();
    cfg.connect_timeout = Duration::from_millis(500);
    let server = bind(cfg.clone()).await;
    let raw = Arc::new(tokio::net::UdpSocket::bind(loopback()).await.unwrap());
    let flooding = flood(raw, server.local_addr());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(server.num_connections() > 0);
    flooding.abort();
    let started = Instant::now();
    step(async {
        while server.num_connections() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(started.elapsed() < Duration::from_secs(5));
    let client = bind(cfg).await;
    let (mut c, mut s) = pair(&client, &server).await;
    echo_roundtrip(&mut c, &mut s, b"after the flood").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timer_processes_only_due_connections() {
    const IDLE: usize = 100;
    let server = bind(UtpConfig::default()).await;
    let client = bind(UtpConfig::default()).await;
    let mut idle = Vec::new();
    for _ in 0..IDLE {
        idle.push(pair(&client, &server).await);
    }
    let (mut c, mut s) = pair(&client, &server).await;
    // Let delayed acks and handshake timers run out.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let visits = || client.timer_visits() + server.timer_visits();
    let v0 = visits();
    // One busy connection keeps the timer waking.
    for i in 0..20u8 {
        echo_roundtrip(&mut c, &mut s, &[i; 100]).await;
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let delta = visits() - v0;
    // The last echo leaves the client a delayed ack, so the timer ran.
    assert!(delta >= 1, "the timer never ran");
    assert!(
        delta < IDLE as u64,
        "{delta} slot visits: the idle connections were scanned"
    );
    assert_eq!(server.num_connections(), IDLE + 1);
    drop(idle);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_for_a_dropped_stream_resets_it() {
    let server = bind(UtpConfig::default()).await;
    let client = bind(UtpConfig::default()).await;
    let (mut c, s) = pair(&client, &server).await;
    drop(s);
    let mut buf = [0u8; 8];
    assert_eq!(step(c.read(&mut buf)).await.unwrap(), 0, "FIN first");
    // Nobody will read this: the server resets instead of buffering it.
    let e = step(async {
        loop {
            if let Err(e) = c.write_all(b"unwanted").await {
                return e;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(e.kind(), io::ErrorKind::ConnectionReset);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_buffers_are_clamped_to_one_packet() {
    let mut cfg = UtpConfig::default();
    cfg.recv_buffer = 0;
    cfg.send_buffer = 0;
    let server = bind(cfg.clone()).await;
    let client = bind(cfg).await;
    let (mut c, mut s) = pair(&client, &server).await;
    // With one packet of buffer each way, the reader must run alongside.
    let msg = vec![7u8; 10_000];
    let mut got = vec![0u8; msg.len()];
    let (w, r) = step(async { tokio::join!(c.write_all(&msg), s.read_exact(&mut got)) }).await;
    w.unwrap();
    r.unwrap();
    assert_eq!(got, msg);
}

/// Records the sequence number of every STATE sent, per destination.
#[derive(Debug)]
struct StateRecorder {
    inner: Arc<dyn DatagramSender>,
    states: Mutex<Vec<(SocketAddr, u16)>>,
}

impl DatagramSender for StateRecorder {
    fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr()
    }

    fn send_to(&self, data: &[u8], to: SocketAddr) -> io::Result<usize> {
        if let Ok(p) = Packet::decode(&Bytes::copy_from_slice(data))
            && p.ty == PacketType::State
        {
            self.states.lock().unwrap().push((to, p.seq_nr));
        }
        self.inner.send_to(data, to)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forged_ack_does_not_confirm_a_half_open_connection() {
    let mut cfg = UtpConfig::default();
    cfg.connect_timeout = Duration::from_millis(500);
    let udp = shared_bind();
    let rec = Arc::new(StateRecorder {
        inner: udp.sender(),
        states: Mutex::new(Vec::new()),
    });
    let server = UtpSocket::new(rec.clone(), cfg);
    udp.set_handler(PacketKind::Utp, server.handler());
    let handler = server.handler();

    // A spoofer at one IP follows each SYN with STATE and DATA packets whose
    // ack_nr misses our handshake: below our ISN or past anything we sent.
    // (The recorder tells the test the ISN; the spoofer would have to guess.)
    let started = Instant::now();
    for i in 0..20u16 {
        let from = SocketAddr::from(([127, 0, 0, 3], 2000 + i));
        let id = i * 10;
        handler.handle(from, &packet(PacketType::Syn, id, 1));
        let isn = rec
            .states
            .lock()
            .unwrap()
            .iter()
            .find(|(to, _)| *to == from)
            .map(|(_, seq)| *seq);
        let Some(isn) = isn else {
            assert!(i >= 8, "SYN {i} was refused below the cap");
            continue;
        };
        for ack in [isn.wrapping_sub(2), isn, isn.wrapping_add(5)] {
            for ty in [PacketType::State, PacketType::Data] {
                let mut p =
                    Packet::decode(&Bytes::from(packet(ty, id.wrapping_add(1), 2))).unwrap();
                p.ack_nr = ack;
                if ty == PacketType::Data {
                    p.payload = Bytes::from_static(b"spoofed");
                }
                let mut d = Vec::new();
                p.encode(&mut d);
                handler.handle(from, &d);
            }
        }
        assert!(server.num_connections() <= fbt_utp::MAX_PENDING_PER_IP);
    }
    assert_eq!(server.num_connections(), fbt_utp::MAX_PENDING_PER_IP);

    // Still half-open, so reaped at connect_timeout, not idle_timeout.
    step(async {
        while server.num_connections() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(started.elapsed() < Duration::from_secs(5));
}
