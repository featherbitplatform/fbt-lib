//! Runtime network settings: a session whose proxy and proxy-related network
//! switches change while it runs, without a restart and without any direct
//! connection in between. The loopback SOCKS5 fake of `fbt-net` stands in for
//! the proxy, so nothing here reaches a real one.

mod support;

#[path = "../../fbt-net/tests/support/fake_socks5.rs"]
mod fake_socks5;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fake_socks5::{FakeSocks5, Script};
use fbt_metainfo::Metainfo;
use fbt_session::storage::{MemStorage, Storage};
use fbt_session::{
    AddTorrent, DisconnectReason, Event, EventMask, EventStream, ProxyConfig, ProxyKind, RateLimit,
    Session, SessionError, Settings, TorrentHandle, TorrentState,
};
use support::*;
use tokio::io::AsyncReadExt;
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
