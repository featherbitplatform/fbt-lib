//! Peers, uTP, UDP and HTTP trackers through a proxy: the loopback fakes of
//! `fbt-net` stand in for SOCKS5 and HTTP CONNECT proxies, so nothing here
//! reaches a real proxy.

mod support;

#[path = "../../fbt-net/tests/support/fake_http_proxy.rs"]
mod fake_http_proxy;
#[path = "../../fbt-net/tests/support/fake_socks5.rs"]
mod fake_socks5;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fake_http_proxy::FakeHttpProxy;
use fake_socks5::{FakeSocks5, Script};
use fbt_metainfo::Metainfo;
use fbt_session::storage::{MemStorage, Storage};
use fbt_session::{
    AddTorrent, Event, EventMask, EventStream, ProxyConfig, ProxyKind, Session, SessionError,
    Settings, TorrentHandle, TorrentState, Transport,
};
use support::*;

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
    let s = Session::new(settings()).await.unwrap();
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

/// A leecher for `m` on `st`, subscribed to every event before adding.
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

    async fn tracker_reply(&mut self, url: &str) {
        next_matching(
            &mut self.events,
            |e| matches!(e, Event::TrackerReply { url: u, .. } if u == url),
            T,
        )
        .await
        .expect("tracker reply");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_dial_goes_through_socks5() {
    let fake = FakeSocks5::start(Script::Ok).await;
    let (m, payload) = torrent();
    let (seed, _) = seeder(&m, &payload).await;
    let mut st = dial_tcp(settings());
    st.proxy = proxy(ProxyKind::Socks5, fake.addr());
    let l = leecher(st, &m, payload.len()).await;
    l.handle.add_peer(seed.local_addr()).await.unwrap();
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
async fn peer_dial_goes_through_http_connect() {
    let fake = FakeHttpProxy::start(fake_http_proxy::Script::Ok).await;
    let (m, payload) = torrent();
    let (seed, _) = seeder(&m, &payload).await;
    // Outgoing uTP left on: an HTTP proxy cannot carry it, so it is off.
    let mut st = settings();
    st.proxy = proxy(ProxyKind::Http, fake.addr());
    let l = leecher(st, &m, payload.len()).await;
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    l.finish(&payload).await;
    assert!(
        fake.connects().contains(&seed.local_addr().to_string()),
        "CONNECTs: {:?}",
        fake.connects()
    );
    let stats = seed.demux_stats();
    assert!(stats.iter().all(|(_, s)| s.utp == 0), "{stats:?}");
    l.session.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_tracker_through_proxy_by_hostname() {
    let fake = FakeSocks5::start(Script::Ok).await;
    let tracker = FakeTracker::http(Reply::Peers).await;
    let url = tracker.url.replace("127.0.0.1", "localhost");
    let port = url
        .trim_start_matches("http://localhost:")
        .trim_end_matches("/announce")
        .to_owned();
    let (mut m, payload) = torrent();
    m.trackers = vec![vec![url.clone()]];
    let mut st = settings();
    st.proxy = proxy(ProxyKind::Socks5, fake.addr());
    let mut l = leecher(st, &m, payload.len()).await;
    l.tracker_reply(&url).await;
    assert!(
        fake.connects().contains(&format!("localhost:{port}")),
        "CONNECTs: {:?}",
        fake.connects()
    );
    assert!(!tracker.announces().is_empty());
    l.session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn utp_and_udp_tracker_go_through_the_relay() {
    let fake = FakeSocks5::start(Script::Ok).await;
    let tracker = FakeTracker::udp(Reply::Peers).await;
    let tracker_addr = tracker.url.trim_start_matches("udp://").to_owned();
    let (m, payload) = torrent();
    // The seed knows no tracker; only the leech announces.
    let (seed, _) = seeder(&m, &payload).await;
    let mut lm = m.clone();
    lm.trackers = vec![vec![tracker.url.clone()]];
    let mut st = settings();
    st.enable_outgoing_tcp = false;
    st.proxy = proxy(ProxyKind::Socks5, fake.addr());
    let mut l = leecher(st, &lm, payload.len()).await;
    l.tracker_reply(&tracker.url).await;
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    let connected = next_matching(
        &mut l.events,
        |e| matches!(e, Event::PeerConnected { .. }),
        T,
    )
    .await;
    match connected {
        Some(Event::PeerConnected {
            addr, transport, ..
        }) => {
            assert_eq!(addr, seed.local_addr());
            assert_eq!(transport, Transport::Utp);
        }
        other => panic!("expected PeerConnected, got {other:?}"),
    }
    l.finish(&payload).await;
    let targets = fake.udp_targets();
    assert!(
        targets.contains(&seed.local_addr().to_string()),
        "{targets:?}"
    );
    assert!(targets.contains(&tracker_addr), "{targets:?}");
    // One UDP socket, the relayed one, carries everything.
    let stats = l.session.demux_stats();
    assert_eq!(stats.len(), 1, "{stats:?}");
    assert!(stats[0].1.utp > 0 && stats[0].1.tracker > 0, "{stats:?}");
    assert!(!fake.connects().contains(&seed.local_addr().to_string()));
    l.session.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxy_trackers_alone_still_relays_udp() {
    let fake = FakeSocks5::start(Script::Ok).await;
    let tracker = FakeTracker::udp(Reply::Peers).await;
    let tracker_addr = tracker.url.trim_start_matches("udp://").to_owned();
    let (mut m, payload) = torrent();
    m.trackers = vec![vec![tracker.url.clone()]];
    let mut st = settings();
    st.proxy = proxy(ProxyKind::Socks5, fake.addr());
    st.proxy.proxy_peers = false;
    st.proxy.proxy_trackers = true;
    let mut l = leecher(st, &m, payload.len()).await;
    l.tracker_reply(&tracker.url).await;
    assert!(fake.udp_targets().contains(&tracker_addr));
    assert_eq!(l.session.demux_stats().len(), 1);
    l.session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn udp_from_non_relay_is_dropped() {
    let fake = FakeSocks5::start(Script::RelayFromStranger).await;
    let tracker = FakeTracker::udp(Reply::Peers).await;
    let (mut m, payload) = torrent();
    m.trackers = vec![vec![tracker.url.clone()]];
    let mut st = settings();
    st.proxy = proxy(ProxyKind::Socks5, fake.addr());
    let l = leecher(st, &m, payload.len()).await;
    let s = l.session.clone();
    assert!(
        wait_until(|| s.demux_stats()[0].1.dropped > 0, T).await,
        "nothing dropped: {:?}",
        s.demux_stats()
    );
    let stats = s.demux_stats()[0].1;
    assert_eq!((stats.utp, stats.dht, stats.tracker), (0, 0, 0));
    l.session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_association_fails_session_new() {
    let fake = FakeSocks5::start(Script::Refuse(1)).await;
    let mut st = settings();
    st.proxy = proxy(ProxyKind::Socks5, fake.addr());
    match Session::new(st).await {
        Err(SessionError::Proxy(_)) => {}
        other => panic!("expected SessionError::Proxy, got {:?}", other.map(|_| ())),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_proxy_settings_are_refused() {
    let mut st = settings();
    st.proxy.kind = ProxyKind::Socks5;
    match Session::new(st).await {
        Err(SessionError::InvalidSettings(msg)) => assert!(msg.contains("proxy"), "{msg}"),
        other => panic!("expected InvalidSettings, got {:?}", other.map(|_| ())),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_relay_stops_the_socket() {
    let fake = FakeSocks5::start(Script::Ok).await;
    let mut st = settings();
    st.proxy = proxy(ProxyKind::Socks5, fake.addr());
    let s = Session::new(st).await.unwrap();
    assert!(!s.demux_stats()[0].1.relay_lost);
    // Dropping the fake closes the association's control connection.
    drop(fake);
    assert!(
        wait_until(|| s.demux_stats()[0].1.relay_lost, T).await,
        "relay loss not seen"
    );
    s.shutdown().await;
}

fn announce_request() -> fbt_tracker::AnnounceRequest {
    fbt_tracker::AnnounceRequest {
        info_hash: [7; 20],
        peer_id: fbt_core::PeerId([b'p'; 20]),
        port: 6881,
        port_v4: None,
        port_v6: None,
        uploaded: 0,
        downloaded: 0,
        left: 1,
        event: fbt_tracker::AnnounceEvent::Started,
        num_want: Some(10),
        key: 1,
        tracker_id: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn udp_tracker_behind_http_proxy_is_unsupported() {
    let fake = FakeHttpProxy::start(fake_http_proxy::Script::Ok).await;
    let tracker = FakeTracker::udp(Reply::Peers).await;
    let client = fbt_tracker::TrackerClient::new_with(
        fbt_tracker::TrackerConfig::default(),
        Vec::new(),
        Some(proxy(ProxyKind::Http, fake.addr())),
    )
    .unwrap();
    let r = client.announce(&tracker.url, &announce_request()).await;
    assert!(
        matches!(r, Err(fbt_tracker::TrackerError::ProxyUnsupported)),
        "{r:?}"
    );
    assert!(tracker.announces().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_tunnel_is_a_proxy_error() {
    let fake = FakeSocks5::start(Script::Refuse(5)).await;
    let tracker = FakeTracker::http(Reply::Peers).await;
    let client = fbt_tracker::TrackerClient::new_with(
        fbt_tracker::TrackerConfig::default(),
        Vec::new(),
        Some(proxy(ProxyKind::Socks5, fake.addr())),
    )
    .unwrap();
    let r = client.announce(&tracker.url, &announce_request()).await;
    match r {
        Err(fbt_tracker::TrackerError::Proxy(msg)) => assert!(msg.contains("code 5"), "{msg}"),
        other => panic!("expected TrackerError::Proxy, got {other:?}"),
    }
    assert!(tracker.announces().is_empty());
}
