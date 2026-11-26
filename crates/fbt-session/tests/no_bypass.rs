//! A proxy-only session leaves nothing direct: with
//! [`proxy_only`](fbt_session::ProxyConfig::proxy_only) set on a proxy, every
//! feature that would otherwise send from the host's own address is off (see
//! `NetworkSettings::effective`), and the outbound audit of `fbt-core` shows
//! nothing but the proxy.
//!
//! The loopback fakes of `fbt-net` stand in for the proxy, so nothing here
//! reaches a real one, and every target is a documentation address or a
//! `.invalid` name that must never be dialled or resolved directly.

mod support;

#[path = "../../fbt-net/tests/support/fake_http_proxy.rs"]
mod fake_http_proxy;
#[path = "../../fbt-net/tests/support/fake_socks5.rs"]
mod fake_socks5;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use fake_http_proxy::FakeHttpProxy;
use fake_socks5::{FakeSocks5, Script, closed_port};
use fbt_core::audit::{self, Outbound};
use fbt_metainfo::Metainfo;
use fbt_session::storage::MemStorage;
use fbt_session::{
    AddTorrent, ProxyConfig, ProxyKind, ProxyState, Session, Settings, TorrentHandle,
};
use support::*;

/// The torrent's one file, and the length of its storage.
const LEN: usize = 200_000;
/// A tracker by name (HTTP), a UDP tracker by name and one by IP literal.
const TRACKERS: [&str; 3] = [
    "http://tracker.invalid/announce",
    "udp://tracker.invalid:6969/announce",
    "udp://192.0.2.20:6969/announce",
];
/// A web seed by name.
const WEB_SEED: &str = "http://seed.invalid/file";
/// A peer to dial (TEST-NET-2: a documentation address).
const PEER: &str = "198.51.100.7:6881";
/// DHT bootstrap entries: a name that must never be resolved locally, and an
/// IP literal that is still used (relayed).
const BOOTSTRAPS: [&str; 2] = ["router.invalid:6881", "192.0.2.10:6881"];

/// The test settings the brief fixes: the two incoming transports on (a
/// proxy-only session must override them), the DHT on with a name and a
/// literal bootstrap entry, `proxy_hostnames` off (overridden too), LSD,
/// UPnP and NAT-PMP off (the test rule; `effective()` is unit-tested for
/// them), and short tracker and connect timeouts so a walk reaches every
/// tracker within the test's patience instead of waiting out the production
/// UDP retransmission.
fn settings() -> Settings {
    let mut s = test_settings();
    s.enable_incoming_tcp = true;
    s.enable_incoming_utp = true;
    s.dht_enabled = true;
    s.dht_bootstrap_nodes = BOOTSTRAPS.iter().map(|b| (*b).to_owned()).collect();
    s.tracker.http_timeout = Duration::from_millis(500);
    s.tracker.udp_base_timeout = Duration::from_millis(200);
    s.tracker.udp_max_retries = 0;
    s.connect_timeout = Duration::from_millis(500);
    s
}

/// A proxy config for `kind` at `addr`, `proxy_only` on and `proxy_hostnames`
/// off (which `proxy_only` must turn on): a proxied name then goes to the
/// proxy as a name rather than being resolved here. `proxy_peers` and
/// `proxy_trackers` keep their defaults (on), which is what makes the session
/// take a UDP association.
fn proxy_only(kind: ProxyKind, addr: SocketAddr) -> ProxyConfig {
    let mut p = ProxyConfig::default();
    p.kind = kind;
    p.host = addr.ip().to_string();
    p.port = addr.port();
    p.proxy_hostnames = false;
    p.proxy_only = true;
    p
}

/// A SOCKS5 config addressed by host name, `proxy_only` on.
fn proxy_by_name(host: &str, port: u16) -> ProxyConfig {
    let mut p = ProxyConfig::default();
    p.kind = ProxyKind::Socks5;
    p.host = host.to_owned();
    p.port = port;
    p.proxy_only = true;
    p
}

/// The one torrent every test uses: three trackers (HTTP and UDP, by name and
/// by literal), a web seed, and a peer to dial.
fn torrent() -> Metainfo {
    let (mut m, _payload) = make_torrent(&[("a", LEN)], 16_384);
    m.trackers = TRACKERS.iter().map(|u| vec![(*u).to_owned()]).collect();
    m.web_seeds = vec![WEB_SEED.to_owned()];
    m
}

/// A session running `proxy`, with the one torrent added and its peer queued
/// for a dial.
async fn start(proxy: ProxyConfig) -> (Session, TorrentHandle) {
    start_with(settings(), proxy).await
}

/// [`start`] over explicit settings, for the one test that needs a longer
/// `connect_timeout`.
async fn start_with(mut s: Settings, proxy: ProxyConfig) -> (Session, TorrentHandle) {
    s.proxy = proxy;
    let session = Session::new(s).await.unwrap();
    let mut p = AddTorrent::new(torrent(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::new(LEN as u64)));
    let handle = session.add_torrent(p).await.unwrap();
    handle.add_peer(PEER.parse().unwrap()).await.unwrap();
    (session, handle)
}

/// Every record is the proxy (a `TcpConnect` to `proxy`) or the relay (a
/// `UdpSend` to `relay`, `None` where the proxy carries no UDP): no direct
/// TCP, no direct UDP, and no name handed to the resolver.
#[track_caller]
fn only_proxy(cap: &[Outbound], proxy: SocketAddr, relay: Option<SocketAddr>) {
    for o in cap {
        match o {
            Outbound::TcpConnect(a) => assert_eq!(*a, proxy, "direct TCP: {o:?}"),
            Outbound::UdpSend(a) => assert_eq!(Some(*a), relay, "direct UDP: {o:?}"),
            Outbound::Lookup(h) => panic!("local lookup of {h}"),
            _ => panic!("unexpected {o:?}"),
        }
    }
}

/// A loopback listener that accepts and never answers, counting what it has
/// accepted: work that stays in flight (an announce waiting for its reply, a
/// peer handshake) for as long as the test runs, so the switching test has
/// direct work live at the moment of the switch (N3). Returns its address and
/// the count of connections it has accepted. Nothing here ever leaves the host.
async fn silent_listener() -> (SocketAddr, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&accepted);
    tokio::spawn(async move {
        // Hold every accepted socket open, unanswered, so the work stays in flight.
        let mut holding: Vec<tokio::net::TcpStream> = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            count.fetch_add(1, Ordering::SeqCst);
            holding.push(stream);
        }
    });
    (addr, accepted)
}

/// Waits (at most 10 s) until the SOCKS5 fake has been asked for all of
/// `connects` and relayed a datagram to all of `udp`.
async fn wait_logged(fake: &FakeSocks5, connects: &[&str], udp: &[&str]) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let c = fake.connects();
        let u = fake.udp_targets();
        if connects.iter().all(|w| c.iter().any(|g| g == w))
            && udp.iter().all(|w| u.iter().any(|g| g == w))
        {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A SOCKS5 proxy that records CONNECT and relayed datagrams without ever
/// dialling them: a proxy-only session asks it for every tracker, the peer
/// and the web seed, and its DHT bootstraps over the relay, while the audit
/// shows nothing but the proxy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn socks5_proxy_only_opens_nothing_else() {
    // The capture is process-wide: take it first, before any session exists,
    // so the records below belong to this test alone (every test here does
    // the same, which serialises them).
    let cap = audit::capture();
    let fake = FakeSocks5::start(Script::RecordOnly).await;
    let (session, _handle) = start(proxy_only(ProxyKind::Socks5, fake.addr())).await;

    // Everything is asked for by name: the trackers, the web seed, the peer.
    assert!(
        wait_logged(
            &fake,
            &["tracker.invalid:80", "seed.invalid:80", PEER],
            &["tracker.invalid:6969", "192.0.2.20:6969", "192.0.2.10:6881"],
        )
        .await,
        "connects: {:?} udp: {:?}",
        fake.connects(),
        fake.udp_targets()
    );
    // The bootstrap name is never resolved locally; only the literal is used.
    assert!(
        !fake
            .udp_targets()
            .iter()
            .any(|t| t.contains("router.invalid")),
        "a bootstrap name was resolved: {:?}",
        fake.udp_targets()
    );

    // Incoming is forced off: the listen port accepts nothing.
    let addr = session.local_addr();
    assert!(
        tokio::net::TcpStream::connect(addr).await.is_err(),
        "a proxy-only session still accepts on {addr}"
    );

    only_proxy(&cap.take(), fake.addr(), fake.relay_addr());
    // And again after a moment, so anything that only happens later is
    // covered too (M2).
    tokio::time::sleep(Duration::from_secs(1)).await;
    only_proxy(&cap.take(), fake.addr(), fake.relay_addr());
    session.shutdown().await;
}

/// The switches that decide whether the proxy relays UDP are read on the
/// effective settings, not the stored ones: a proxy-only SOCKS5 proxy whose
/// stored `proxy_peers` and `proxy_trackers` are *off* still relays (I2).
/// `effective()` turns both on (and documents the DHT and uTP as relayed), so
/// the association must be taken on that proxy, or the session would hold no
/// UDP at all while reporting `Reachable`. The fake records the association
/// and every datagram without dialling anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn socks5_proxy_only_relays_udp_without_the_switches() {
    let cap = audit::capture();
    let fake = FakeSocks5::start(Script::RecordOnly).await;
    let mut proxy = proxy_only(ProxyKind::Socks5, fake.addr());
    // Off as stored, turned on by `effective()` for `proxy_only`.
    proxy.proxy_peers = false;
    proxy.proxy_trackers = false;
    let (session, _handle) = start(proxy).await;

    // The DHT bootstraps over the relay (its literal entry, never the name)
    // and both UDP trackers go through it.
    assert!(
        wait_logged(
            &fake,
            &["tracker.invalid:80", PEER],
            &["tracker.invalid:6969", "192.0.2.20:6969", "192.0.2.10:6881"],
        )
        .await,
        "connects: {:?} udp: {:?}",
        fake.connects(),
        fake.udp_targets()
    );
    assert!(
        fake.relay_addr().is_some(),
        "a proxy-only session whose switches are stored off took no UDP relay"
    );
    assert!(
        session.dht_stats().is_some(),
        "the DHT does not run without a relay: {:?}",
        session.dht_stats()
    );
    assert_eq!(
        session.proxy_state(),
        ProxyState::Reachable,
        "a relayed proxy-only session is not reported reachable"
    );

    only_proxy(&cap.take(), fake.addr(), fake.relay_addr());
    session.shutdown().await;
}

/// As above, through an HTTP proxy: it cannot carry UDP, so a proxy-only
/// session runs no DHT, no uTP and no UDP tracker, and the audit is CONNECTs
/// to the proxy only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_proxy_only_opens_nothing_else() {
    let cap = audit::capture();
    let fake = FakeHttpProxy::start(fake_http_proxy::Script::RecordOnly).await;
    let (session, _handle) = start(proxy_only(ProxyKind::Http, fake.addr())).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let wanted = ["tracker.invalid:80", "seed.invalid:80", PEER];
    while !wanted
        .iter()
        .all(|w| fake.connects().iter().any(|g| g == w))
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the proxy was not asked for every target: {:?}",
            fake.connects()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        session.dht_stats().is_none(),
        "an HTTP proxy-only session ran a DHT: {:?}",
        session.dht_stats()
    );

    only_proxy(&cap.take(), fake.addr(), None);
    // And again after a moment, so anything that only happens later is
    // covered too (M2).
    tokio::time::sleep(Duration::from_secs(1)).await;
    only_proxy(&cap.take(), fake.addr(), None);
    session.shutdown().await;
}

/// A proxy nothing is listening on: the only thing the session can do is try
/// to reach it, so the audit is that attempt (a CONNECT to the closed port)
/// and nothing else, for as long as the torrent is running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unreachable_proxy_opens_nothing_else() {
    let cap = audit::capture();
    let proxy = closed_port();
    let (session, _handle) = start(proxy_only(ProxyKind::Socks5, proxy)).await;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let recs = cap.take();
    assert!(
        recs.contains(&Outbound::TcpConnect(proxy)),
        "the session never tried the proxy: {recs:?}"
    );
    only_proxy(&recs, proxy, None);
    session.shutdown().await;
}

/// A proxy that takes TCP but refuses to relay UDP: a proxy-only session
/// announces UDP through it anyway (never directly) and, with no relay to
/// send through, fails the UDP trackers rather than going direct.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn udp_not_relayed_opens_nothing_else() {
    let cap = audit::capture();
    // Refuses the UDP association, and only logs the CONNECTs it is asked for.
    let fake = FakeSocks5::start(Script::NoUdpRecordOnly(7)).await;
    let (session, _handle) = start(proxy_only(ProxyKind::Socks5, fake.addr())).await;

    assert!(
        wait_logged(&fake, &["tracker.invalid:80", PEER], &[]).await,
        "connects: {:?}",
        fake.connects()
    );
    assert!(
        fake.udp_targets().is_empty(),
        "a datagram went somewhere without a relay: {:?}",
        fake.udp_targets()
    );

    only_proxy(&cap.take(), fake.addr(), None);
    session.shutdown().await;
}

/// A session running directly, switched to a proxy-only one at runtime: from
/// the switch on, nothing it does is direct. The capture starts just before
/// the switch and is never discarded, so work in flight across the change is
/// covered too, and the new stack is seen reaching the proxy (I1); what the
/// direct phase did is excluded only by starting the capture after it. The
/// direct phase touches loopback alone (M3) and leaves a direct announce and a
/// direct peer handshake in flight across the switch (N3), so the change has to
/// tear down a stack with live direct work rather than an idle one; the brief's
/// names are added after the switch, where they only reach the fake proxy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn switching_to_proxy_opens_nothing_direct() {
    let cap = audit::capture();
    let fake = FakeSocks5::start(Script::RecordOnly).await;

    // A direct session first: a loopback tracker that accepts the announce and
    // never answers it and a loopback peer whose handshake stays open, so both
    // are in flight at the switch (N3); no DHT, and no uTP so the peer dials
    // TCP. The HTTP timeout is long enough that the announce is still waiting
    // for its reply when the switch lands.
    let mut s = settings();
    s.proxy = ProxyConfig::default();
    s.dht_enabled = false;
    s.dht_bootstrap_nodes = Vec::new();
    s.enable_outgoing_utp = false;
    s.tracker.http_timeout = Duration::from_secs(3);
    let session = Session::new(s).await.unwrap();
    let (tracker, tracker_accepted) = silent_listener().await;
    let (mut m, _payload) = make_torrent(&[("a", LEN)], 16_384);
    m.trackers = vec![vec![format!("http://{tracker}/announce")]];
    let mut p = AddTorrent::new(m, PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::new(LEN as u64)));
    let direct = session.add_torrent(p).await.unwrap();
    let (peer, peer_accepted) = silent_listener().await;
    direct.add_peer(peer).await.unwrap();
    // Let the announce and the handshake get under way.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        tracker_accepted.load(Ordering::SeqCst) >= 1,
        "the direct announce was not in flight at the switch"
    );
    assert!(
        peer_accepted.load(Ordering::SeqCst) >= 1,
        "the direct peer dial was not in flight at the switch"
    );

    // The switch: the capture starts here, so what the direct phase did is
    // excluded by starting after it, and nothing in flight across the change
    // is dropped.
    let mut net = session.network_settings();
    net.proxy = proxy_only(ProxyKind::Socks5, fake.addr());
    let _ = cap.take();
    session.set_network_settings(net).await.unwrap();

    // The new stack does its work through the proxy: the brief's torrent (its
    // names reach only the fake) and its peer.
    let mut p = AddTorrent::new(torrent(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::new(LEN as u64)));
    let handle = session.add_torrent(p).await.unwrap();
    handle.add_peer(PEER.parse().unwrap()).await.unwrap();
    assert!(
        wait_logged(
            &fake,
            &["tracker.invalid:80", PEER],
            &["tracker.invalid:6969"]
        )
        .await,
        "the new stack never reached the proxy: connects {:?} udp {:?}",
        fake.connects(),
        fake.udp_targets()
    );

    // Nothing direct in everything captured from just before the switch: the
    // loopback work in flight is on sockets that are already open, so it can
    // add no direct record — a record here would be a dial of the replaced
    // stack.
    tokio::time::sleep(Duration::from_secs(3)).await;
    only_proxy(&cap.take(), fake.addr(), fake.relay_addr());
    session.shutdown().await;
}

/// A proxy addressed by host name: the proxy's own name is the one lookup a
/// proxied session makes, and nothing else is resolved (no tracker, web seed
/// or bootstrap name reaches the local resolver).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxy_host_name_is_the_only_lookup() {
    let cap = audit::capture();
    let fake = FakeSocks5::start(Script::RecordOnly).await;
    // On Windows `localhost` resolves to `::1` first, while the fake listens on
    // IPv4 only, so the stagger spends its first attempt on `::1` (a refused
    // connect there takes about 2 s on Windows) before the IPv4 attempt 250 ms
    // in. This one test gets a 3 s `connect_timeout` as margin for that first
    // attempt plus the SOCKS5 handshake; the stagger is what makes it pass.
    let mut s = settings();
    s.connect_timeout = Duration::from_secs(3);
    let (session, _handle) = start_with(s, proxy_by_name("localhost", fake.addr().port())).await;

    assert!(
        wait_logged(
            &fake,
            &["tracker.invalid:80", PEER],
            &["tracker.invalid:6969"]
        )
        .await,
        "connects: {:?} udp: {:?}",
        fake.connects(),
        fake.udp_targets()
    );

    let recs = cap.take();
    let lookups: Vec<&str> = recs
        .iter()
        .filter_map(|o| match o {
            Outbound::Lookup(h) => Some(h.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        lookups.contains(&"localhost"),
        "the proxy's own name was not looked up: {recs:?}"
    );
    assert!(
        lookups.iter().all(|h| *h == "localhost"),
        "something else was resolved locally: {lookups:?}"
    );
    session.shutdown().await;
}
