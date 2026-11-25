//! Runtime network settings: a session whose proxy and proxy-related network
//! switches change while it runs, without a restart and without any direct
//! connection in between. The loopback SOCKS5 fake of `fbt-net` stands in for
//! the proxy, so nothing here reaches a real one.

mod support;

#[path = "../../fbt-net/tests/support/fake_socks5.rs"]
mod fake_socks5;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fake_socks5::{FakeSocks5, Script};
use fbt_metainfo::Metainfo;
use fbt_session::storage::{MemStorage, Storage};
use fbt_session::{
    AddTorrent, DisconnectReason, Event, EventMask, EventStream, ProxyConfig, ProxyKind, RateLimit,
    Session, SessionError, Settings, TorrentHandle, TorrentState,
};
use support::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

fn settings() -> Settings {
    let mut s = test_settings();
    s.rechoke_interval = Duration::from_secs(1);
    s.peer_retry_delay = Duration::from_millis(200);
    s
}

fn proxy(kind: ProxyKind, addr: SocketAddr) -> ProxyConfig {
    let mut p = ProxyConfig::default();
    p.kind = kind;
    p.host = addr.ip().to_string();
    p.port = addr.port();
    p
}

fn torrent() -> (Metainfo, Vec<u8>) {
    make_torrent(&[("a", 200_000), ("b", 12_345)], 16_384)
}

async fn seeder(m: &Metainfo, payload: &[u8]) -> (Session, TorrentHandle) {
    seeder_with(settings(), m, payload).await
}

async fn seeder_with(st: Settings, m: &Metainfo, payload: &[u8]) -> (Session, TorrentHandle) {
    let s = Session::new(st).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(payload.to_vec())));
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    (s, h)
}

struct Leecher {
    session: Session,
    handle: TorrentHandle,
    events: EventStream,
    storage: Arc<MemStorage>,
}

async fn leecher(st: Settings, m: &Metainfo, len: usize) -> Leecher {
    let session = Session::new(st).await.unwrap();
    let events = session.subscribe(EventMask::ALL);
    let storage = Arc::new(MemStorage::new(len as u64));
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(storage.clone());
    let handle = session.add_torrent(p).await.unwrap();
    Leecher {
        session,
        handle,
        events,
        storage,
    }
}

impl Leecher {
    async fn finish(&self, payload: &[u8]) {
        assert!(
            wait_state(&self.handle, |s| *s == TorrentState::Seeding, T).await,
            "download did not finish"
        );
        let mut got = vec![0u8; payload.len()];
        self.storage.read(0, &mut got).unwrap();
        assert!(got == payload, "data differs");
    }
}

/// The next event matching `pred`.
async fn event_of(
    l: &mut Leecher,
    pred: impl Fn(&Event) -> bool,
    timeout: Duration,
) -> Option<Event> {
    next_matching(&mut l.events, pred, timeout).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_network_settings_round_trips() {
    let st = settings();
    let s = Session::new(st.clone()).await.unwrap();
    assert_eq!(s.network_settings(), st.network());

    let mut net = s.network_settings();
    net.dht_enabled = true;
    s.set_network_settings(net.clone()).await.unwrap();
    assert_eq!(s.network_settings(), net);
    assert!(s.dht_stats().is_some(), "the DHT did not start");

    net.dht_enabled = false;
    s.set_network_settings(net.clone()).await.unwrap();
    assert_eq!(s.network_settings(), net);
    assert!(s.dht_stats().is_none(), "the DHT did not stop");

    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_network_settings_are_refused() {
    let s = Session::new(settings()).await.unwrap();
    let before = s.network_settings();
    let mut bad = before.clone();
    // A SOCKS5 proxy with an empty host is invalid.
    bad.proxy.kind = ProxyKind::Socks5;
    match s.set_network_settings(bad).await {
        Err(SessionError::InvalidSettings(msg)) => assert!(msg.contains("proxy"), "{msg}"),
        other => panic!("expected InvalidSettings, got {other:?}"),
    }
    assert_eq!(s.network_settings(), before, "the settings changed anyway");
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enabling_proxy_closes_direct_peers() {
    let fake = FakeSocks5::start(Script::Ok).await;
    let (m, payload) = torrent();
    let (seed, _) = seeder(&m, &payload).await;
    let mut l = leecher(dial_tcp(settings()), &m, payload.len()).await;
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    assert!(
        event_of(&mut l, |e| matches!(e, Event::PeerConnected { .. }), T)
            .await
            .is_some(),
        "the direct connection never happened"
    );

    let mut net = l.session.network_settings();
    net.proxy = proxy(ProxyKind::Socks5, fake.addr());
    l.session.set_network_settings(net).await.unwrap();

    match event_of(
        &mut l,
        |e| {
            matches!(
                e,
                Event::PeerDisconnected {
                    reason: DisconnectReason::NetworkChanged,
                    ..
                }
            )
        },
        T,
    )
    .await
    {
        Some(_) => {}
        other => panic!("expected a NetworkChanged disconnect, got {other:?}"),
    }

    // It comes back through the proxy and finishes.
    l.finish(&payload).await;
    assert!(
        fake.connects().contains(&seed.local_addr().to_string()),
        "CONNECTs: {:?}",
        fake.connects()
    );
    l.session.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn half_open_direct_connection_closed_on_proxy_change() {
    let fake = FakeSocks5::start(Script::Ok).await;
    let (m, payload) = torrent();
    let l = leecher(dial_tcp(settings()), &m, payload.len()).await;

    // A listener that accepts, takes in the leecher's opening message, and
    // then holds the connection open without ever answering a handshake.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (handshaken_tx, handshaken_rx) = oneshot::channel();
    let accepted = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 2048];
        // The leecher's own opening message; nothing from us.
        let _ = s.read(&mut buf).await;
        let _ = handshaken_tx.send(());
        // Hold the connection: the leecher must close it itself, so the
        // socket is never dropped here and the deadline is what decides.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            match tokio::time::timeout_at(deadline, s.read(&mut buf)).await {
                // Anything it still sends is drained, so a later close
                // reads as EOF rather than as a reset.
                Ok(Ok(0)) | Ok(Err(_)) => break true,
                Ok(Ok(_)) => continue,
                Err(_) => break false,
            }
        }
    });

    l.handle.add_peer(addr).await.unwrap();
    handshaken_rx.await.expect("the leecher never dialled");

    let mut net = l.session.network_settings();
    net.proxy = proxy(ProxyKind::Socks5, fake.addr());
    l.session.set_network_settings(net).await.unwrap();

    assert!(
        accepted.await.unwrap(),
        "the half-open connection was not closed within 2 s"
    );
    l.session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_proxy_change_keeps_tcp_peers() {
    let (m, payload) = torrent();
    // A byte-per-second limit, applied to loopback peers, keeps the
    // download from finishing: a finished leecher drops its seed (BEP 21
    // upload-only) and the seed the leecher, which would read as a
    // disconnect the change had nothing to do with.
    let mut st = dial_tcp(settings());
    st.rate_limit = RateLimit::new(1, 1);
    st.rate_limit_local_peers = true;
    let (seed, _) = seeder_with(st.clone(), &m, &payload).await;
    let mut l = leecher(st, &m, payload.len()).await;
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    assert!(
        event_of(&mut l, |e| matches!(e, Event::PeerConnected { .. }), T)
            .await
            .is_some(),
        "the connection never happened"
    );

    let mut net = l.session.network_settings();
    net.enable_incoming_utp = !net.enable_incoming_utp;
    l.session.set_network_settings(net).await.unwrap();

    assert!(
        event_of(
            &mut l,
            |e| matches!(e, Event::PeerDisconnected { .. }),
            Duration::from_secs(1)
        )
        .await
        .is_none(),
        "a peer was disconnected by a change that did not touch the proxy"
    );
    assert_eq!(
        l.handle.status().num_peers,
        1,
        "the connected peer was dropped"
    );
    l.session.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incoming_tcp_toggle_stops_and_restarts_accepting() {
    let s = Session::new(settings()).await.unwrap();
    let addr = s.local_addr();
    assert!(TcpStream::connect(addr).await.is_ok(), "not accepting");

    let mut net = s.network_settings();
    net.enable_incoming_tcp = false;
    s.set_network_settings(net.clone()).await.unwrap();
    assert!(
        TcpStream::connect(addr).await.is_err(),
        "still accepting with incoming TCP off"
    );
    assert_eq!(s.local_addr().port(), addr.port(), "the port changed");

    net.enable_incoming_tcp = true;
    s.set_network_settings(net).await.unwrap();
    assert!(
        TcpStream::connect(addr).await.is_ok(),
        "not accepting again with incoming TCP on"
    );
    assert_eq!(s.local_addr().port(), addr.port(), "the port changed");
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_changes_apply_in_order() {
    let (m, payload) = torrent();
    let (seed, _) = seeder(&m, &payload).await;
    let l = leecher(dial_tcp(settings()), &m, payload.len()).await;
    l.handle.add_peer(seed.local_addr()).await.unwrap();

    let mut a = l.session.network_settings();
    a.enable_incoming_tcp = false;
    let mut b = l.session.network_settings();
    b.enable_incoming_utp = false;
    let (ra, rb) = tokio::join!(
        l.session.set_network_settings(a.clone()),
        l.session.set_network_settings(b.clone())
    );
    ra.unwrap();
    rb.unwrap();

    let now = l.session.network_settings();
    assert!(
        now == a || now == b,
        "network_settings() is a mix of both changes: {now:?}"
    );
    // The session still works.
    l.finish(&payload).await;
    l.session.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_during_change_completes() {
    let fake = FakeSocks5::start(Script::Silent).await;
    let mut st = settings();
    st.connect_timeout = Duration::from_secs(2);
    let s = Session::new(st).await.unwrap();

    let mut net = s.network_settings();
    net.proxy = proxy(ProxyKind::Socks5, fake.addr());
    let setter = {
        let s = s.clone();
        tokio::spawn(async move { s.set_network_settings(net).await })
    };

    // Wait until the change is in flight: it installs a closed stack, whose
    // listeners are gone (so the session reports no listen address) while
    // the new stack's UDP association waits for the silent fake.
    assert!(
        wait_until(|| s.local_addr().port() == 0, T).await,
        "the change never started"
    );

    let started = tokio::time::Instant::now();
    s.shutdown().await;
    let r = setter.await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "the change and the shutdown took {:?}",
        started.elapsed()
    );
    assert!(matches!(r, Ok(()) | Err(SessionError::Shutdown)), "{r:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removing_proxy_restores_direct() {
    let fake = FakeSocks5::start(Script::Ok).await;
    let (m, payload) = torrent();
    let (seed, _) = seeder(&m, &payload).await;
    let l = leecher(dial_tcp(settings()), &m, payload.len()).await;

    let mut net = l.session.network_settings();
    net.proxy = proxy(ProxyKind::Socks5, fake.addr());
    l.session.set_network_settings(net.clone()).await.unwrap();

    let mut expect = dial_tcp(settings()).network();
    expect.proxy = ProxyConfig::default();
    net.proxy = ProxyConfig::default();
    l.session.set_network_settings(net).await.unwrap();
    assert_eq!(
        l.session.network_settings(),
        expect,
        "the stored value is gone"
    );

    l.handle.add_peer(seed.local_addr()).await.unwrap();
    l.finish(&payload).await;
    assert!(
        !fake.connects().contains(&seed.local_addr().to_string()),
        "a direct download dialled the proxy: {:?}",
        fake.connects()
    );
    l.session.shutdown().await;
    seed.shutdown().await;
}

/// A walk stuck on a hung tracker keeps the client it started with: after
/// a proxy is set while it hangs, it must move on to the next tier through
/// the proxy, never by dialling it directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tracker_walk_in_progress_moves_to_proxy() {
    let fake = Arc::new(FakeSocks5::start(Script::Ok).await);

    // Tier 1 accepts the announce and never answers: the walk hangs there.
    let tier1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tier1_addr = tier1.local_addr().unwrap();
    let (asked_tx, asked_rx) = oneshot::channel();
    tokio::spawn(async move {
        let (mut s, _) = tier1.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = s.read(&mut buf).await;
        let _ = asked_tx.send(());
        // Holds the connection open, never answering.
        while let Ok(n) = s.read(&mut buf).await {
            if n == 0 {
                break;
            }
        }
    });

    // Tier 2 records, per request, whether the proxy relayed it: the fake
    // records its CONNECT before it answers the greeting, and the client
    // sends its request only after that, so reading the request first and
    // then asking the fake is race-free.
    let tier2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tier2_addr = tier2.local_addr().unwrap();
    let relayed = Arc::new(Mutex::new(Vec::<bool>::new()));
    let seen = Arc::clone(&relayed);
    let by_proxy = Arc::clone(&fake);
    tokio::spawn(async move {
        while let Ok((mut s, _)) = tier2.accept().await {
            let mut buf = [0u8; 4096];
            let _ = s.read(&mut buf).await;
            let relayed = by_proxy.connects().contains(&tier2_addr.to_string());
            seen.lock().unwrap().push(relayed);
        }
    });

    let (mut m, payload) = torrent();
    m.trackers = vec![
        vec![format!("http://{tier1_addr}/announce")],
        vec![format!("http://{tier2_addr}/announce")],
    ];
    let mut st = settings();
    // Tier 1 never answers, so the walk gives up on it quickly: the move
    // to tier 2 (directly, with the bug) happens within the test's
    // patience.
    st.tracker.http_timeout = Duration::from_millis(600);
    let (seed, _) = seeder_with(st, &m, &payload).await;
    tokio::time::timeout(T, asked_rx)
        .await
        .expect("tier 1 was never asked")
        .unwrap();

    // Set the proxy while the walk is in flight on tier 1.
    let mut net = seed.network_settings();
    net.proxy = proxy(ProxyKind::Socks5, fake.addr());
    seed.set_network_settings(net).await.unwrap();

    let deadline = tokio::time::Instant::now() + T;
    while relayed.lock().unwrap().is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "tier 2 was never asked after the change"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let records = relayed.lock().unwrap().clone();
    assert!(
        records.iter().all(|r| *r),
        "the walk reached tier 2 directly: {records:?}"
    );
    seed.shutdown().await;
}

/// A connection accepted on the direct listener whose handshake is still
/// arriving when a proxy is set must not end up as a direct peer: the
/// routing task reading it goes with the stack it was accepted on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incoming_handshake_in_progress_is_closed_on_proxy_change() {
    let fake = FakeSocks5::start(Script::Ok).await;
    let (m, payload) = torrent();
    let (srv, _) = seeder(&m, &payload).await;
    let mut events = srv.subscribe(EventMask::ALL);
    let addr = srv.local_addr();

    // Half a BitTorrent handshake for the seeding torrent.
    let mut hs = Vec::new();
    hs.push(19u8);
    hs.extend_from_slice(b"BitTorrent protocol");
    hs.extend_from_slice(&[0u8; 8]);
    hs.extend_from_slice(&m.info_hashes.wire_hash());
    hs.extend_from_slice(&[0x11; 20]);
    let mut client = TcpStream::connect(addr).await.expect("connect");
    client.write_all(&hs[..28]).await.expect("half a handshake");
    // The listener has accepted and is mid-handshake.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut net = srv.network_settings();
    net.proxy = proxy(ProxyKind::Socks5, fake.addr());
    srv.set_network_settings(net).await.unwrap();

    // The rest of the handshake arrives after the change: the connection
    // must not survive it.
    let _ = client.write_all(&hs[28..]).await;
    let mut buf = [0u8; 68];
    match tokio::time::timeout(Duration::from_secs(5), client.read(&mut buf)).await {
        Ok(Ok(0)) | Ok(Err(_)) => {}
        Ok(Ok(n)) => panic!("the direct handshake finished after the change ({n} bytes)"),
        Err(_) => panic!("the connection was left open across the change"),
    }
    assert!(
        next_matching(
            &mut events,
            |e| matches!(e, Event::PeerConnected { .. }),
            Duration::from_secs(1)
        )
        .await
        .is_none(),
        "a direct peer connected across the change"
    );
    srv.shutdown().await;
}

/// Two proxy changes at once, the second putting back the value the session
/// had before the first. Everything the second decides from "the value the
/// session has now" — whether it is a no-op at all, and whether the proxy
/// changed — must be read from the value stored under the change lock, not
/// from the one its caller read on the way in. Read outside, the no-op
/// shortcut sees the pre-change value, treats the restore as a no-op and
/// leaves the session on the proxy-less stack the first change installed:
/// the caller set a proxy and the session kept connecting directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_proxy_changes_close_direct_peers() {
    fn proxied(fake: &FakeSocks5, seed: &str) -> usize {
        fake.connects().iter().filter(|c| **c == seed).count()
    }

    let fake = FakeSocks5::start(Script::Ok).await;
    let mut st = dial_tcp(settings());
    st.proxy = proxy(ProxyKind::Socks5, fake.addr());
    // A byte-per-second limit on loopback peers keeps the download from
    // finishing: a finished leecher drops its seed (BEP 21 upload-only)
    // before the change after it could redial it.
    st.rate_limit = RateLimit::new(1, 1);
    st.rate_limit_local_peers = true;

    let (m, payload) = torrent();
    let (seed, _) = seeder(&m, &payload).await;

    // The leecher starts with the proxy on: its only peer is connected
    // through it.
    let mut l = leecher(st, &m, payload.len()).await;
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    assert!(
        event_of(&mut l, |e| matches!(e, Event::PeerConnected { .. }), T)
            .await
            .is_some(),
        "the proxied connection never happened"
    );
    let seed_addr = seed.local_addr().to_string();
    assert_eq!(
        proxied(&fake, &seed_addr),
        1,
        "the first connection did not go through the proxy: {:?}",
        fake.connects()
    );

    // A drops the proxy, so the session would dial the seed directly from
    // then on; B puts the same value back. Both start before either applies:
    // A takes the change first (and yields before storing), so B meets the
    // value A stored, not the one it read on the way in.
    let mut a = l.session.network_settings();
    a.proxy = ProxyConfig::default();
    let b = l.session.network_settings();
    let (ra, rb) = tokio::join!(
        l.session.set_network_settings(a),
        l.session.set_network_settings(b.clone())
    );
    ra.unwrap();
    rb.unwrap();
    assert_eq!(
        l.session.network_settings(),
        b,
        "the change that puts the proxy back was ignored"
    );

    // And the peer comes back through the proxy: whatever the first change
    // connected directly is gone, so nothing stays connected directly while
    // the proxy is set.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while proxied(&fake, &seed_addr) < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the peer did not come back through the proxy: {:?}",
            fake.connects()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    l.session.shutdown().await;
    seed.shutdown().await;
}

/// A change whose new stack cannot be built leaves the session on the
/// closed stack with nothing listening; setting the previous value again
/// must rebuild the network rather than short-circuit as a no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_after_failed_change_restores_network() {
    let s = Session::new(settings()).await.unwrap();
    let good = s.network_settings();
    assert_ne!(s.local_addr().port(), 0, "nothing listening to start with");

    // A proxy address nothing listens on: the SOCKS5 association cannot be
    // taken, so the change fails.
    let dead = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap()
    };
    let mut bad = good.clone();
    bad.proxy = proxy(ProxyKind::Socks5, dead);
    match s.set_network_settings(bad).await {
        Err(SessionError::Proxy(_)) => {}
        other => panic!("expected a proxy error, got {other:?}"),
    }
    // The stored value is unchanged, and the stack is the closed one (no
    // listener).
    assert_eq!(s.network_settings(), good, "the failed change was stored");
    assert_eq!(
        s.local_addr().port(),
        0,
        "the failed change left the stack open"
    );

    // Setting the previous value again must build the network back.
    s.set_network_settings(good.clone()).await.unwrap();
    assert_eq!(s.network_settings(), good);
    assert_ne!(s.local_addr().port(), 0, "the session has no network");
    TcpStream::connect(s.local_addr())
        .await
        .expect("not accepting again");
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dht_state_survives_rebuild() {
    let mut st = settings();
    st.dht_enabled = true;
    st.dht_bootstrap_nodes = Vec::new();
    let s = Session::new(st).await.unwrap();
    assert!(s.dht_stats().is_some(), "the DHT did not start");

    let before = s.dht_state().await.unwrap().expect("a DHT state");
    let mut net = s.network_settings();
    net.enable_incoming_utp = !net.enable_incoming_utp;
    s.set_network_settings(net).await.unwrap();
    let after = s.dht_state().await.unwrap().expect("a DHT state");

    let before = fbt_dht::DhtState::from_bytes(&before).expect("state");
    let after = fbt_dht::DhtState::from_bytes(&after).expect("state");
    assert_eq!(before.id_v4, after.id_v4, "the IPv4 node id changed");
    assert_eq!(before.id_v6, after.id_v6, "the IPv6 node id changed");
    s.shutdown().await;
}

/// Reads an HTTP request head (through the blank line) from `s`; false on
/// EOF.
async fn read_head(s: &mut TcpStream) -> bool {
    let mut buf = [0u8; 2048];
    let mut seen = Vec::new();
    while !seen.windows(4).any(|w| w == b"\r\n\r\n") {
        match s.read(&mut buf).await {
            Ok(0) | Err(_) => return false,
            Ok(n) => seen.extend_from_slice(&buf[..n]),
        }
    }
    true
}

/// A valid announce reply with no peers, then closes the connection.
async fn write_announce_reply(s: &mut TcpStream) {
    let body = b"d8:intervali1800e5:peers0:e";
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = s.write_all(head.as_bytes()).await;
    let _ = s.write_all(body).await;
    let _ = s.shutdown().await;
}

/// Reads `s` until the peer closes it (or the connection errors).
async fn read_to_eof(s: &mut TcpStream) {
    let mut buf = [0u8; 1024];
    while let Ok(n) = s.read(&mut buf).await {
        if n == 0 {
            break;
        }
    }
}

/// A stopped announce in flight when a proxy is set must be abandoned with
/// the stack it started on: its cloned client would otherwise finish the
/// request (DNS and TCP connect included) directly after
/// `set_network_settings` returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopped_announce_is_abandoned_by_a_network_change() {
    let fake = FakeSocks5::start(Script::Ok).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (started_tx, started_rx) = oneshot::channel();
    let (held_tx, held_rx) = oneshot::channel();
    let (closed_tx, closed_rx) = oneshot::channel();
    let mut started_tx = Some(started_tx);
    let mut held_tx = Some(held_tx);
    tokio::spawn(async move {
        let mut first = true;
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                return;
            };
            if !read_head(&mut s).await {
                continue;
            }
            if first {
                // The `started` announce: answered, so the URL gets the
                // `stopped` one when the torrent pauses.
                first = false;
                write_announce_reply(&mut s).await;
                if let Some(tx) = started_tx.take() {
                    let _ = tx.send(());
                }
                continue;
            }
            // The `stopped` announce: held until the change takes the
            // client that sent it with it.
            if let Some(tx) = held_tx.take() {
                let _ = tx.send(());
            }
            read_to_eof(&mut s).await;
            let _ = closed_tx.send(());
            return;
        }
    });

    let (mut m, payload) = torrent();
    m.trackers = vec![vec![format!("http://{addr}/announce")]];
    let (seed, handle) = seeder_with(settings(), &m, &payload).await;
    tokio::time::timeout(T, started_rx)
        .await
        .expect("the tracker was never asked")
        .unwrap();

    // Pause: the stopped announce goes out and the tracker holds it.
    handle.pause().await.unwrap();
    tokio::time::timeout(T, held_rx)
        .await
        .expect("the stopped announce was never sent")
        .unwrap();

    // A proxy while it is held: the request goes with the old stack, so the
    // tracker sees the connection closed well before `STOPPED_TIMEOUT` (5 s)
    // rather than the request finishing on the old proxy-less client.
    let mut net = seed.network_settings();
    net.proxy = proxy(ProxyKind::Socks5, fake.addr());
    seed.set_network_settings(net).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), closed_rx)
        .await
        .expect("the stopped request outlived the change")
        .unwrap();
    seed.shutdown().await;
}

/// A walk abandoned by a network change reports no tracker error for it: the
/// walk restarted against the closed stack would fail at once and blame the
/// tracker for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_abandoned_walk_reports_no_tracker_error() {
    let fake = FakeSocks5::start(Script::Ok).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (held_tx, held_rx) = oneshot::channel();
    let mut held_tx = Some(held_tx);
    tokio::spawn(async move {
        let mut first = true;
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                return;
            };
            if !read_head(&mut s).await {
                continue;
            }
            if first {
                // The walk hangs here until the change abandons it.
                first = false;
                if let Some(tx) = held_tx.take() {
                    let _ = tx.send(());
                }
                read_to_eof(&mut s).await;
                continue;
            }
            // The walk the change restarted on the new stack is answered.
            write_announce_reply(&mut s).await;
        }
    });

    let (mut m, payload) = torrent();
    m.trackers = vec![vec![format!("http://{addr}/announce")]];
    let (seed, _) = seeder_with(settings(), &m, &payload).await;
    let mut events = seed.subscribe(EventMask::ALL);
    tokio::time::timeout(T, held_rx)
        .await
        .expect("the tracker was never asked")
        .unwrap();

    let mut net = seed.network_settings();
    net.proxy = proxy(ProxyKind::Socks5, fake.addr());
    seed.set_network_settings(net).await.unwrap();

    // The walk restarts on the new stack and is answered; the abandoned one
    // must report nothing.
    let deadline = tokio::time::Instant::now() + T;
    loop {
        let ev = tokio::time::timeout_at(deadline, events.recv())
            .await
            .expect("the walk never reached the tracker again")
            .expect("the events ended");
        match ev {
            Event::TrackerError { url, message, .. } => {
                panic!("the abandoned walk reported a tracker error for {url}: {message}");
            }
            Event::TrackerReply { .. } => break,
            _ => {}
        }
    }
    seed.shutdown().await;
}
