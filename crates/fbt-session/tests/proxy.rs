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
use fake_socks5::{FakeSocks5, Script, closed_port};
use fbt_metainfo::Metainfo;
use fbt_session::storage::{MemStorage, Storage};
use fbt_session::{
    AddTorrent, DisconnectReason, Event, EventMask, EventStream, ProxyConfig, ProxyKind,
    ProxyState, Session, SessionError, Settings, TorrentHandle, TorrentState, Transport,
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

/// A SOCKS5 proxy that takes TCP but refuses the UDP association does not
/// fail `Session::new` any more (F2): the session starts with no UDP socket
/// at all (no DHT, no uTP, no UDP trackers), reports `UdpNotRelayed` and
/// still downloads through the proxy over TCP.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_starts_when_udp_is_not_relayed() {
    let fake = FakeSocks5::start(Script::NoUdp(7)).await;
    let (m, payload) = torrent();
    let (seed, _) = seeder(&m, &payload).await;
    let mut st = dial_tcp(settings());
    st.dht_enabled = true;
    st.dht_bootstrap_nodes = Vec::new();
    st.proxy = proxy(ProxyKind::Socks5, fake.addr());
    let l = leecher(st, &m, payload.len()).await;
    assert_eq!(l.session.proxy_state(), ProxyState::UdpNotRelayed);
    assert!(
        l.session.dht_stats().is_none(),
        "the DHT runs under a proxy that does not relay UDP"
    );
    assert!(
        l.session.demux_stats().is_empty(),
        "a proxied session binds no UDP socket"
    );
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

/// A proxy nothing listens on does not fail `Session::new` either: the
/// session starts, reports `Unreachable` with a non-empty reason, and a peer
/// dial goes through the proxy (and so fails) rather than connecting
/// directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_starts_with_proxy_unreachable() {
    let (m, payload) = torrent();
    let (seed, seed_handle) = seeder(&m, &payload).await;
    let mut st = dial_tcp(settings());
    st.proxy = proxy(ProxyKind::Socks5, closed_port());
    let mut l = leecher(st, &m, payload.len()).await;
    match l.session.proxy_state() {
        ProxyState::Unreachable { reason } => assert!(!reason.is_empty(), "empty reason"),
        other => panic!("expected Unreachable, got {other:?}"),
    }
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    assert!(
        next_matching(
            &mut l.events,
            |e| matches!(e, Event::PeerConnected { .. }),
            Duration::from_secs(1)
        )
        .await
        .is_none(),
        "a peer connected while the proxy is unreachable"
    );
    assert_eq!(
        seed_handle.status().num_peers,
        0,
        "the seeder saw a direct connection"
    );
    l.session.shutdown().await;
    seed.shutdown().await;
}

/// A proxy that wants credentials this client does not have is its own
/// state, not `Unreachable`: it answered, it just refused the login.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn login_refused_is_its_own_state() {
    let fake = FakeSocks5::start(Script::RequireAuth("u".to_owned(), "p".to_owned())).await;
    let mut st = dial_tcp(settings());
    st.proxy = proxy(ProxyKind::Socks5, fake.addr());
    st.proxy.username = Some("u".to_owned());
    st.proxy.password = Some("wrong".to_owned());
    let s = Session::new(st).await.unwrap();
    assert_eq!(s.proxy_state(), ProxyState::LoginRefused);
    s.shutdown().await;
}

/// A probe of the *old* proxy that finishes after the user changed to
/// another one does not overwrite the new proxy's state (I1): the health
/// task applies a result only while the stored settings still equal the ones
/// it probed, so a dead proxy cannot be reported `Reachable` -- a state
/// nothing re-probes -- because of a probe taken before the change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_probe_does_not_overwrite_the_new_proxy() {
    // A proxy that carries TCP only, so the probe is a greeting (not a UDP
    // association) and its result is the plain `Reachable` the health task
    // would overwrite the state with.
    let addr = closed_port();
    let mut st = settings();
    st.proxy = proxy(ProxyKind::Socks5, addr);
    st.proxy.proxy_peers = false;
    st.proxy.proxy_trackers = false;
    let s = Session::new(st).await.unwrap();
    let mut events = s.subscribe(EventMask::STATUS);
    assert!(matches!(s.proxy_state(), ProxyState::Unreachable { .. }));

    // The proxy answers again on the same address but holds the greeting:
    // the health task's probe connects and parks there, provably in flight.
    let (_fake, gate) = FakeSocks5::start_on_gated(addr, Script::Ok).await;
    assert!(
        wait_until(|| gate.waiting() >= 1, T).await,
        "no probe of the old proxy was in flight"
    );

    // The user switches to another dead proxy while that probe is parked.
    let mut net = s.network_settings();
    net.proxy = proxy(ProxyKind::Socks5, closed_port());
    s.set_network_settings(net).await.unwrap();
    assert!(matches!(s.proxy_state(), ProxyState::Unreachable { .. }));

    // Let the old probe answer: its `Reachable` must not reach the state.
    gate.release();
    assert!(
        next_matching(
            &mut events,
            |e| matches!(
                e,
                Event::ProxyStateChanged {
                    state: ProxyState::Reachable
                }
            ),
            Duration::from_millis(500)
        )
        .await
        .is_none(),
        "a probe of the old proxy reported the new one reachable"
    );
    assert!(
        matches!(s.proxy_state(), ProxyState::Unreachable { .. }),
        "the state of the new proxy was overwritten: {:?}",
        s.proxy_state()
    );
    s.shutdown().await;
}

/// Dropping every handle of a session whose proxy is down releases its
/// listen port at once, even while the health task waits out its backoff
/// (I2): the task sleeps holding only a `Weak`, so the session is not kept
/// alive by it (a strong handle would hold the port up to
/// `proxy_retry_max`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_session_releases_its_port_while_the_health_task_waits() {
    let mut st = settings();
    st.proxy = proxy(ProxyKind::Socks5, closed_port());
    // Long enough that the task is asleep, not probing, when the session
    // goes: its backoff must not hold the port.
    st.proxy_retry_base = Duration::from_secs(30);
    st.proxy_retry_max = Duration::from_secs(60);
    let s = Session::new(st).await.unwrap();
    assert!(matches!(s.proxy_state(), ProxyState::Unreachable { .. }));
    let port = s.local_addr().port();
    // Let the health task reach its backoff wait: it is asleep in it when the
    // session goes (the wait is what must not hold the port).
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        std::net::TcpListener::bind(("127.0.0.1", port)).is_err(),
        "the session stopped listening before it was dropped"
    );
    // No shutdown: dropping the last handle must be enough.
    drop(s);
    assert!(
        wait_until(
            || std::net::TcpListener::bind(("127.0.0.1", port)).is_ok(),
            Duration::from_secs(1)
        )
        .await,
        "the listen port {port} is still bound a second after the session went"
    );
}

/// A proxy that grants the UDP association and drops it at once is retried
/// on the doubling backoff, not once per `proxy_retry_base` (I3): each
/// `Reachable` is a probe success that must not reset the schedule, so a
/// flapping proxy cannot drive a state change (and a full rebuild) every
/// base interval.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flapping_proxy_is_not_retried_at_the_base_interval() {
    let fake = FakeSocks5::start(Script::DropAssociation).await;
    let mut st = settings();
    st.proxy = proxy(ProxyKind::Socks5, fake.addr());
    let s = Session::new(st).await.unwrap();
    let mut events = s.subscribe(EventMask::STATUS);
    // The relay is lost the moment it is taken: the state flaps between
    // `Reachable` and `Unreachable` (two events per cycle). Over 2 s the
    // doubling schedule (50, 100, 200, 400, 400, …) allows about ten cycles;
    // the base interval alone would allow twice as many.
    let mut changes = 0;
    let end = tokio::time::Instant::now() + Duration::from_secs(2);
    while let Ok(Some(ev)) = tokio::time::timeout_at(end, events.recv()).await {
        if matches!(ev, Event::ProxyStateChanged { .. }) {
            changes += 1;
        }
    }
    assert!(
        changes <= 24,
        "{changes} state changes in 2 s: the flapping proxy is retried at the base interval"
    );
    s.shutdown().await;
}

/// A TCP dial that gets through says nothing about the UDP relay: a proxy
/// that relays TCP fine but grants a UDP association only to drop it at once
/// keeps its `Unreachable` state and its place in the backoff. A peer dial
/// through the proxy succeeds (the TCP side is fine), but it must not wake
/// the health task to re-probe the relay, which is the one thing that is
/// broken (F1). With a long `proxy_retry_base` the scheduled probe is far
/// away, so the state must stay `Unreachable` and the stack must not be
/// rebuilt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tcp_dial_success_does_not_reset_the_proxy_backoff() {
    let fake = FakeSocks5::start(Script::DropAssociation).await;
    let (m, payload) = torrent();
    let (seed, _) = seeder(&m, &payload).await;
    let mut st = dial_tcp(settings());
    st.proxy = proxy(ProxyKind::Socks5, fake.addr());
    // Long, so the scheduled probe cannot be what moves the state here.
    st.proxy_retry_base = Duration::from_secs(30);
    st.proxy_retry_max = Duration::from_secs(60);
    let l = leecher(st, &m, payload.len()).await;
    assert!(
        wait_until(
            || matches!(l.session.proxy_state(), ProxyState::Unreachable { .. }),
            T
        )
        .await,
        "the association was not dropped: {:?}",
        l.session.proxy_state()
    );
    let generation = l.session.net_generation();
    // A peer dial through the proxy: the TCP side is fine, so it succeeds.
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    // A successful TCP dial must not repair a lost relay.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        matches!(l.session.proxy_state(), ProxyState::Unreachable { .. }),
        "a TCP dial success repaired a lost relay: {:?}",
        l.session.proxy_state()
    );
    assert_eq!(
        l.session.net_generation(),
        generation,
        "a TCP dial success rebuilt the stack"
    );
    l.session.shutdown().await;
    seed.shutdown().await;
}

/// A dial the proxy refused while its relay is healthy moves the state to
/// `Unreachable`, but the next successful probe does not rebuild the stack
/// (I4): the live relay was never lost, so rebuilding it would tear down
/// uTP, the DHT and the announces for nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reachable_probe_keeps_a_live_relay() {
    let fake = FakeSocks5::start(Script::Ok).await;
    let addr = fake.addr();
    let (m, payload) = torrent();
    let (seed, _) = seeder(&m, &payload).await;
    let mut st = dial_tcp(settings());
    st.proxy = proxy(ProxyKind::Socks5, addr);
    let l = leecher(st, &m, payload.len()).await;
    assert_eq!(l.session.proxy_state(), ProxyState::Reachable);
    assert_eq!(l.session.demux_stats().len(), 1, "no relayed UDP socket");
    assert!(!l.session.demux_stats()[0].1.relay_lost);
    let generation = l.session.net_generation();

    // The proxy stops accepting: a peer dial through it fails to connect
    // (the relay's control connection stays up), which reports the proxy
    // unreachable.
    fake.stop_accepting().await;
    l.handle.add_peer(closed_port()).await.unwrap();
    assert!(
        wait_until(
            || matches!(l.session.proxy_state(), ProxyState::Unreachable { .. }),
            T
        )
        .await,
        "the refused dial did not move the state: {:?}",
        l.session.proxy_state()
    );

    // The proxy answers again; the probe succeeds while the live stack still
    // has its relay, so the stack must be left alone.
    let fake = FakeSocks5::start_on(addr, Script::Ok).await;
    assert!(
        wait_until(|| l.session.proxy_state() == ProxyState::Reachable, T).await,
        "the proxy did not come back: {:?}",
        l.session.proxy_state()
    );
    assert_eq!(
        l.session.net_generation(),
        generation,
        "a healthy relayed stack was rebuilt"
    );
    assert_eq!(l.session.demux_stats().len(), 1);
    l.session.shutdown().await;
    seed.shutdown().await;
    drop(fake);
}

/// A new reason under the *same* state is not a state change (M2): the
/// stored state carries the latest reason (`proxy_state` reports it), but
/// `Event::ProxyStateChanged` is emitted only when the variant differs, so a
/// proxy that fails differently does not re-announce `Unreachable`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_reason_under_the_same_state_does_not_emit() {
    let mut st = dial_tcp(settings());
    // Short, so the second proxy's silent handshake times out promptly.
    st.connect_timeout = Duration::from_millis(200);
    st.proxy = proxy(ProxyKind::Socks5, closed_port());
    let s = Session::new(st).await.unwrap();
    let mut events = s.subscribe(EventMask::STATUS);
    let ProxyState::Unreachable { reason } = s.proxy_state() else {
        panic!("expected Unreachable, got {:?}", s.proxy_state())
    };

    // A proxy that accepts the connection and answers nothing: the state is
    // `Unreachable` again, with the timeout as the new reason.
    let silent = FakeSocks5::start(Script::Silent).await;
    let mut net = s.network_settings();
    net.proxy = proxy(ProxyKind::Socks5, silent.addr());
    s.set_network_settings(net).await.unwrap();
    let ProxyState::Unreachable { reason: latest } = s.proxy_state() else {
        panic!("expected Unreachable, got {:?}", s.proxy_state())
    };
    assert_ne!(latest, reason, "the reason did not follow the new proxy");
    assert!(
        next_matching(
            &mut events,
            |e| matches!(e, Event::ProxyStateChanged { .. }),
            Duration::from_millis(500)
        )
        .await
        .is_none(),
        "an Unreachable with a new reason was reported as a state change"
    );
    s.shutdown().await;
}

/// Switching to another down proxy starts the backoff over: after several
/// failed probes of `P` (which inflate `attempt`), a change to `Q` is probed
/// after `proxy_retry_base`, not after the delay the old schedule had
/// reached (F4). The probe dials the proxy, so `Q`'s parked-connection count
/// (a gate that never opens) shows the probe.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_settings_change_restarts_the_backoff() {
    let mut st = dial_tcp(settings());
    // Short, so the probes fail promptly; the base is a fraction of the
    // delay an inflated `attempt` would reach.
    st.connect_timeout = Duration::from_millis(100);
    st.proxy_retry_base = Duration::from_millis(100);
    st.proxy_retry_max = Duration::from_secs(10);
    st.proxy = proxy(ProxyKind::Socks5, closed_port());
    let s = Session::new(st).await.unwrap();
    assert!(matches!(s.proxy_state(), ProxyState::Unreachable { .. }));
    // Let `P` fail a few times: 100 ms, 200 ms, 400 ms -> `attempt` is 3 and
    // the next scheduled probe is 800 ms away.
    tokio::time::sleep(Duration::from_millis(700)).await;

    // A proxy that accepts and answers nothing: every probe dials it and is
    // parked at the gate, so `waiting` counts the probes.
    let (q, gate) = FakeSocks5::start_gated(Script::Silent).await;
    let mut net = s.network_settings();
    net.proxy = proxy(ProxyKind::Socks5, q.addr());
    s.set_network_settings(net).await.unwrap();
    // The change classified `Q` once already.
    assert!(
        wait_until(|| gate.waiting() >= 1, T).await,
        "the change did not probe Q"
    );
    let after_change = gate.waiting();
    // A fresh schedule probes `Q` after `base` (100 ms), not after 800 ms:
    // within 600 ms there must be a second probe.
    assert!(
        wait_until(|| gate.waiting() > after_change, Duration::from_millis(600)).await,
        "Q was not probed after the base delay ({} probes): the old backoff was kept",
        gate.waiting()
    );
    s.shutdown().await;
    drop(q);
}

/// Switching to a second proxy that fails with the *identical* reason is a
/// settings change all the same: the state does not change, so no event is
/// emitted, but the task must still see it and probe the new proxy after
/// `proxy_retry_base`, not after the deadline the old proxy's backoff had
/// reached (N1).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_settings_change_to_the_same_failure_restarts_the_backoff() {
    let mut st = dial_tcp(settings());
    // Short, so a probe fails promptly; the base is a fraction of the delay
    // an inflated `attempt` reaches.
    st.connect_timeout = Duration::from_millis(100);
    st.proxy_retry_base = Duration::from_millis(100);
    st.proxy_retry_max = Duration::from_secs(10);
    // A proxy that accepts and answers nothing: every probe fails with
    // "proxy timed out", which is exactly how the next one fails too.
    let (p, _gate_p) = FakeSocks5::start_gated(Script::Silent).await;
    st.proxy = proxy(ProxyKind::Socks5, p.addr());
    let s = Session::new(st).await.unwrap();
    let reason = match s.proxy_state() {
        ProxyState::Unreachable { reason } => reason,
        other => panic!("expected Unreachable, got {other:?}"),
    };
    assert_eq!(reason, "proxy timed out");
    // Let the backoff grow: probes about 100 ms, 200 ms, 400 ms and 800 ms
    // apart, so the running wait is seconds away.
    tokio::time::sleep(Duration::from_millis(4300)).await;

    let (q, gate_q) = FakeSocks5::start_gated(Script::Silent).await;
    let mut events = s.subscribe(EventMask::ALL);
    let mut net = s.network_settings();
    net.proxy = proxy(ProxyKind::Socks5, q.addr());
    s.set_network_settings(net).await.unwrap();
    // The same state, word for word: the change is invisible in it.
    match s.proxy_state() {
        ProxyState::Unreachable { reason } => assert_eq!(reason, "proxy timed out"),
        other => panic!("expected Unreachable, got {other:?}"),
    }
    // The change probed Q once by itself.
    assert!(
        wait_until(|| gate_q.waiting() >= 1, T).await,
        "the change did not probe Q"
    );
    let after_change = gate_q.waiting();
    // A fresh schedule probes Q after `base` (100 ms); the old one still had
    // seconds to run.
    assert!(
        wait_until(
            || gate_q.waiting() > after_change,
            Duration::from_millis(600)
        )
        .await,
        "Q was not probed after the base delay ({} probes): the identical state hid the change",
        gate_q.waiting()
    );
    // The state did not change, so nothing was announced.
    assert!(
        next_matching(
            &mut events,
            |e| matches!(e, Event::ProxyStateChanged { .. }),
            Duration::from_millis(200)
        )
        .await
        .is_none(),
        "the identical state emitted a state event"
    );
    s.shutdown().await;
    drop(p);
    drop(q);
}

/// A proxy that was unreachable is re-probed until it answers: when it comes
/// back on the same address the state turns `Reachable`, exactly once, the
/// relayed UDP socket replaces the missing one, and a UDP tracker announce
/// through the relay is answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn association_returns_after_proxy_comes_back() {
    let addr = closed_port();
    let tracker = FakeTracker::udp(Reply::Peers).await;
    let tracker_addr = tracker.url.trim_start_matches("udp://").to_owned();
    let (mut m, payload) = torrent();
    m.trackers = vec![vec![tracker.url.clone()]];
    let mut st = dial_tcp(settings());
    st.tracker_retry_base = Duration::from_millis(100);
    st.proxy = proxy(ProxyKind::Socks5, addr);
    let mut l = leecher(st, &m, payload.len()).await;
    match l.session.proxy_state() {
        ProxyState::Unreachable { .. } => {}
        other => panic!("expected Unreachable, got {other:?}"),
    }
    // The proxy comes back on the same address.
    let fake = FakeSocks5::start_on(addr, Script::Ok).await;
    let came_back = next_matching(
        &mut l.events,
        |e| {
            matches!(
                e,
                Event::ProxyStateChanged {
                    state: ProxyState::Reachable
                }
            )
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(came_back.is_some(), "the proxy did not come back");
    assert_eq!(l.session.proxy_state(), ProxyState::Reachable);
    assert!(!l.session.demux_stats()[0].1.relay_lost);
    // The rebuilt stack announces to the UDP tracker through the relay, and
    // that wait counts the state events: the comeback is reported exactly
    // once (the rebuild re-affirms the `Reachable` the classification under
    // the change lock already set, and a state set twice emits once).
    let mut changes = 0usize;
    let mut announced = false;
    let wait = tokio::time::timeout(T, async {
        while let Some(ev) = l.events.recv().await {
            match ev {
                Event::ProxyStateChanged { .. } => changes += 1,
                Event::TrackerReply { .. } => {
                    announced = true;
                    break;
                }
                _ => {}
            }
        }
    })
    .await;
    assert!(
        wait.is_ok() && announced,
        "no UDP tracker announce through the relay"
    );
    assert_eq!(
        changes, 0,
        "the proxy's comeback was reported as {changes} more state changes"
    );
    assert!(
        fake.udp_targets().contains(&tracker_addr),
        "{:?}",
        fake.udp_targets()
    );
    l.session.shutdown().await;
}

/// A proxy that is up and then goes away: the relay is lost, the state turns
/// `Unreachable` with exactly one legacy `Event::ProxyError`, and when the
/// proxy comes back the state returns to `Reachable` without closing the
/// connected TCP peers (the proxy did not change).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_loss_then_recovery() {
    let fake = FakeSocks5::start(Script::Ok).await;
    let addr = fake.addr();
    let (m, payload) = torrent();
    let (seed, _) = seeder(&m, &payload).await;
    let mut st = dial_tcp(settings());
    st.proxy = proxy(ProxyKind::Socks5, addr);
    let mut l = leecher(st, &m, payload.len()).await;
    assert_eq!(l.session.proxy_state(), ProxyState::Reachable);
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    assert!(
        next_matching(
            &mut l.events,
            |e| matches!(e, Event::PeerConnected { .. }),
            T
        )
        .await
        .is_some(),
        "the proxied peer never connected"
    );

    // The proxy goes away; its task abort closes the relay's control
    // connection.
    drop(fake);
    match next_matching(
        &mut l.events,
        |e| matches!(e, Event::ProxyError { .. }),
        Duration::from_secs(10),
    )
    .await
    {
        Some(Event::ProxyError { message }) => assert!(message.contains("UDP"), "{message}"),
        other => panic!("no ProxyError on the relay loss: {other:?}"),
    }
    assert!(
        next_matching(
            &mut l.events,
            |e| matches!(
                e,
                Event::ProxyStateChanged {
                    state: ProxyState::Unreachable { .. }
                }
            ),
            T
        )
        .await
        .is_some(),
        "the state did not go Unreachable"
    );
    assert!(
        next_matching(
            &mut l.events,
            |e| matches!(e, Event::ProxyError { .. }),
            Duration::from_millis(500)
        )
        .await
        .is_none(),
        "a second ProxyError"
    );

    // The proxy comes back on the same address.
    let fake = FakeSocks5::start_on(addr, Script::Ok).await;
    assert!(
        next_matching(
            &mut l.events,
            |e| matches!(
                e,
                Event::ProxyStateChanged {
                    state: ProxyState::Reachable
                }
            ),
            Duration::from_secs(5)
        )
        .await
        .is_some(),
        "the relay did not come back"
    );
    assert!(
        next_matching(
            &mut l.events,
            |e| matches!(
                e,
                Event::PeerDisconnected {
                    reason: DisconnectReason::NetworkChanged,
                    ..
                }
            ),
            Duration::from_millis(500)
        )
        .await
        .is_none(),
        "the relay rebuild closed a TCP peer as a network change"
    );
    l.session.shutdown().await;
    seed.shutdown().await;
    drop(fake);
}

/// Once the state is `Reachable`, peer dials that succeed and dials the proxy
/// refuses for the *target* (a closed loopback port: `REP 5`) say nothing
/// about the proxy: no further `ProxyStateChanged` is emitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn state_events_only_on_change() {
    let fake = FakeSocks5::start(Script::Ok).await;
    let (m, payload) = torrent();
    let (seed, _) = seeder(&m, &payload).await;
    let mut st = dial_tcp(settings());
    st.proxy = proxy(ProxyKind::Socks5, fake.addr());
    let mut l = leecher(st, &m, payload.len()).await;
    assert_eq!(l.session.proxy_state(), ProxyState::Reachable);
    // A dial that gets through.
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    assert!(
        next_matching(
            &mut l.events,
            |e| matches!(e, Event::PeerConnected { .. }),
            T
        )
        .await
        .is_some()
    );
    // Dials the proxy refuses for the target (the address is closed).
    for _ in 0..2 {
        l.handle.add_peer(closed_port()).await.unwrap();
    }
    assert_eq!(l.session.proxy_state(), ProxyState::Reachable);
    assert!(
        next_matching(
            &mut l.events,
            |e| matches!(e, Event::ProxyStateChanged { .. }),
            Duration::from_millis(500)
        )
        .await
        .is_none(),
        "the state changed with nothing wrong with the proxy"
    );
    l.session.shutdown().await;
    seed.shutdown().await;
}

/// Removing the proxy reports `NotSet`, once, and the session goes back to
/// direct operation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removing_proxy_is_not_set() {
    let fake = FakeSocks5::start(Script::Ok).await;
    let mut st = dial_tcp(settings());
    st.proxy = proxy(ProxyKind::Socks5, fake.addr());
    let s = Session::new(st).await.unwrap();
    let mut events = s.subscribe(EventMask::STATUS);
    assert_eq!(s.proxy_state(), ProxyState::Reachable);
    let mut net = s.network_settings();
    net.proxy = ProxyConfig::default();
    s.set_network_settings(net).await.unwrap();
    assert_eq!(s.proxy_state(), ProxyState::NotSet);
    match next_matching(
        &mut events,
        |e| {
            matches!(
                e,
                Event::ProxyStateChanged {
                    state: ProxyState::NotSet
                }
            )
        },
        T,
    )
    .await
    {
        Some(Event::ProxyStateChanged { state }) => assert_eq!(state, ProxyState::NotSet),
        other => panic!("no ProxyStateChanged(NotSet): {other:?}"),
    }
    assert!(
        next_matching(
            &mut events,
            |e| matches!(e, Event::ProxyStateChanged { .. }),
            Duration::from_millis(500)
        )
        .await
        .is_none(),
        "a second ProxyStateChanged"
    );
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_proxy_settings_are_refused() {
    let mut st = settings();
    st.proxy.kind = ProxyKind::Socks5;
    match Session::new(st).await {
        Err(SessionError::InvalidSettings(msg)) => assert!(msg.contains("proxy"), "{msg}"),
        other => panic!("expected InvalidSettings, got {:?}", other.map(|_| ())),
    }
    // SOCKS5 credentials of 1 to 255 bytes each, checked before any dial.
    for (user, pass) in [("", "pw"), ("user", ""), ("user", &*"p".repeat(256))] {
        let mut st = settings();
        st.proxy = proxy(ProxyKind::Socks5, "127.0.0.1:9".parse().unwrap());
        st.proxy.username = Some(user.to_owned());
        st.proxy.password = Some(pass.to_owned());
        match Session::new(st).await {
            Err(SessionError::InvalidSettings(msg)) => assert!(msg.contains("proxy"), "{msg}"),
            other => panic!(
                "{user:?}/{pass:?}: expected InvalidSettings, got {:?}",
                other.map(|_| ())
            ),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_relay_stops_the_socket() {
    let fake = FakeSocks5::start(Script::Ok).await;
    let mut st = settings();
    st.proxy = proxy(ProxyKind::Socks5, fake.addr());
    let s = Session::new(st).await.unwrap();
    let mut events = s.subscribe(EventMask::STATUS);
    assert!(!s.demux_stats()[0].1.relay_lost);
    // Dropping the fake closes the association's control connection.
    drop(fake);
    assert!(
        wait_until(|| s.demux_stats()[0].1.relay_lost, T).await,
        "relay loss not seen"
    );
    // The session says so, once.
    match next_matching(&mut events, |e| matches!(e, Event::ProxyError { .. }), T).await {
        Some(Event::ProxyError { message }) => assert!(message.contains("UDP"), "{message}"),
        other => panic!("no ProxyError: {other:?}"),
    }
    assert!(
        next_matching(
            &mut events,
            |e| matches!(e, Event::ProxyError { .. }),
            Duration::from_millis(500)
        )
        .await
        .is_none(),
        "a second ProxyError"
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
