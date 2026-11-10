//! The session's DHT: peer discovery for public torrents and magnets, `Port`
//! messages, private torrents kept off it, state save/restore and events.
//!
//! Every DHT here bootstraps from a loopback `fbt_dht::Dht`; nothing
//! contacts a public node.

mod support;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fbt_core::InfoHashes;
use fbt_dht::{Dht, DhtConfig};
use fbt_metainfo::{Magnet, Metainfo};
use fbt_session::storage::MemStorage;
use fbt_session::{
    AddTorrent, Event, EventMask, EventStream, PeerSource, Session, SessionError, Settings,
    TorrentHandle, TorrentState,
};
use fbt_wire::{Message, Reserved};
use support::*;

const T: Duration = Duration::from_secs(30);

/// A loopback DHT node, IPv4 only, bootstrapping from `bootstrap`.
async fn loopback_dht(bootstrap: Vec<String>) -> Dht {
    let mut c = DhtConfig::default();
    c.bind_v4 = Some(SocketAddr::from(([127, 0, 0, 1], 0)));
    c.bind_v6 = None;
    c.bootstrap = bootstrap;
    c.query_rate_per_ip = 10_000;
    c.query_timeout = Duration::from_millis(500);
    Dht::spawn(c).await.unwrap()
}

fn addr_of(d: &Dht) -> String {
    d.local_addrs()[0].to_string()
}

/// Session settings with the DHT on, bootstrapping from `bootstrap` only.
fn dht_settings(bootstrap: Vec<String>) -> Settings {
    let mut s = test_settings();
    s.rechoke_interval = Duration::from_secs(1);
    s.dht_enabled = true;
    s.dht_bootstrap_nodes = bootstrap;
    s.dht_query_timeout = Duration::from_millis(500);
    s.dht_query_rate_per_ip = 10_000;
    s
}

async fn seed_in(session: &Session, m: &Metainfo, payload: &[u8]) -> TorrentHandle {
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(payload.to_vec())));
    let h = session.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    h
}

async fn next_event(rx: &mut EventStream, secs: u64) -> Option<Event> {
    tokio::time::timeout(Duration::from_secs(secs), rx.recv())
        .await
        .ok()
        .flatten()
}

fn dht_reserved() -> Reserved {
    let mut r = Reserved::default();
    r.set_fast();
    r.set_extended();
    r.set_dht();
    r
}

/// Byte 7 of the reserved field (handshake byte 27) has the DHT bit.
fn has_dht_bit(r: &Reserved) -> bool {
    let wire = fbt_wire::Handshake {
        reserved: *r,
        info_hash: [0; 20],
        peer_id: fbt_core::PeerId([0; 20]),
    }
    .encode();
    wire[20 + 7] & 0x01 != 0
}

fn stored(d: &Dht) -> usize {
    d.stats().stored_info_hashes
}

#[test]
fn defaults_ship_public_bootstrap_nodes() {
    assert_eq!(
        Settings::default().dht_bootstrap_nodes,
        vec![
            "router.bittorrent.com:6881".to_owned(),
            "dht.transmissionbt.com:6881".to_owned(),
            "router.utorrent.com:6881".to_owned(),
        ]
    );
    assert!(Settings::default().dht_enabled);
    assert!(DhtConfig::default().bootstrap.is_empty());
    assert!(test_settings().dht_bootstrap_nodes.is_empty());
    assert!(!test_settings().dht_enabled);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn magnet_download_via_dht_only() {
    let boot = loopback_dht(Vec::new()).await;
    let (m, payload) = make_torrent(&[("a", 300_000), ("b/c", 70_000)], 32_768);
    assert!(m.trackers.is_empty());
    let mut ss = dht_settings(vec![addr_of(&boot)]);
    ss.dht_announce_interval = Duration::from_secs(1);
    let seeder = Session::new(ss).await.unwrap();
    let _sh = seed_in(&seeder, &m, &payload).await;
    // The seeder's announce reached the bootstrap node.
    assert!(wait_until(|| stored(&boot) >= 1, T).await);

    let leecher = Session::new(dht_settings(vec![addr_of(&boot)]))
        .await
        .unwrap();
    let mut ev = leecher.subscribe(EventMask::ALL);
    let dir = tempfile::tempdir().unwrap();
    let mg = Magnet::from_info_hashes(m.info_hashes);
    assert!(mg.trackers.is_empty() && mg.peers.is_empty());
    let h = leecher
        .add_torrent(AddTorrent::magnet(mg, dir.path().to_path_buf()))
        .await
        .unwrap();
    let (mut found, mut metadata) = (false, false);
    let finished = tokio::time::timeout(Duration::from_secs(60), async {
        while let Some(e) = ev.recv().await {
            match e {
                Event::PeersFound {
                    source: PeerSource::Dht,
                    count,
                    ..
                } if count > 0 => found = true,
                Event::MetadataReceived { .. } => metadata = true,
                Event::TorrentFinished { .. } => return true,
                _ => {}
            }
        }
        false
    })
    .await;
    assert_eq!(finished, Ok(true));
    assert!(found, "PeersFound from the DHT");
    assert!(metadata, "MetadataReceived");
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    let a = std::fs::read(dir.path().join("torrent/a")).unwrap();
    let c = std::fs::read(dir.path().join("torrent/b/c")).unwrap();
    assert_eq!([a, c].concat(), payload);
    leecher.shutdown().await;
    seeder.shutdown().await;
    boot.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_torrent_never_touches_dht() {
    let boot = loopback_dht(Vec::new()).await;
    let (m, payload) = make_private_torrent(&[("a", 100_000)], 32_768);
    assert!(m.info.private);
    let mut s = dht_settings(vec![addr_of(&boot)]);
    s.dht_announce_interval = Duration::from_secs(1);
    let session = Session::new(s).await.unwrap();
    let _h = seed_in(&session, &m, &payload).await;
    // The session's DHT itself runs and joins the bootstrap node.
    assert!(wait_until(|| session.dht_stats().unwrap().nodes_v4 >= 1, T).await);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(stored(&boot), 0, "a private torrent was announced");

    let mut peer = ScriptedPeer::connect_with(
        session.local_addr(),
        m.info_hashes.wire_hash(),
        dht_reserved(),
    )
    .await;
    assert!(!has_dht_bit(&peer.remote.reserved));
    assert!(
        peer.next_matching(|m| matches!(m, Message::Port(_)))
            .await
            .is_none()
    );
    session.shutdown().await;
    boot.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn port_message_adds_node() {
    let raw = loopback_dht(Vec::new()).await;
    let raw_port = raw.local_addrs()[0].port();
    let session = Session::new(dht_settings(Vec::new())).await.unwrap();
    assert_eq!(session.dht_stats().unwrap().nodes_v4, 0);
    let (m, payload) = make_torrent(&[("a", 100_000)], 32_768);
    let _h = seed_in(&session, &m, &payload).await;
    let mut peer = ScriptedPeer::connect_with(
        session.local_addr(),
        m.info_hashes.wire_hash(),
        dht_reserved(),
    )
    .await;
    // A public torrent advertises the DHT and sends its port.
    assert!(has_dht_bit(&peer.remote.reserved));
    let dht_port = session.dht_stats().unwrap().local_addrs[0].port();
    assert_eq!(
        peer.next_matching(|m| matches!(m, Message::Port(_))).await,
        Some(Message::Port(dht_port))
    );
    peer.send(Message::Port(raw_port)).await;
    assert!(
        wait_until(
            || session.dht_stats().unwrap().nodes_v4 >= 1,
            Duration::from_secs(3)
        )
        .await
    );
    session.shutdown().await;
    raw.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dht_state_roundtrip_through_session() {
    let boot = loopback_dht(Vec::new()).await;
    let s1 = Session::new(dht_settings(vec![addr_of(&boot)]))
        .await
        .unwrap();
    assert!(wait_until(|| s1.dht_stats().unwrap().nodes_v4 >= 1, T).await);
    let state = s1.dht_state().await.unwrap().expect("DHT running");
    s1.shutdown().await;
    assert!(matches!(s1.dht_state().await, Err(SessionError::Shutdown)));

    let mut s = dht_settings(Vec::new());
    s.dht_state = Some(state);
    let s2 = Session::new(s).await.unwrap();
    assert!(
        wait_until(
            || s2.dht_stats().unwrap().nodes_v4 >= 1,
            Duration::from_secs(3)
        )
        .await
    );
    s2.shutdown().await;
    boot.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_dht_state_is_ignored() {
    let mut s = dht_settings(Vec::new());
    s.dht_state = Some(b"garbage".to_vec());
    let session = Session::new(s).await.unwrap();
    assert!(session.dht_stats().is_some());
    assert!(session.dht_state().await.unwrap().is_some());
    session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dht_port_taken_falls_back() {
    // A UDP port whose TCP twin is free.
    let (udp, port) = loop {
        let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = udp.local_addr().unwrap().port();
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            break (udp, port);
        }
    };
    let mut s = dht_settings(Vec::new());
    let listen = SocketAddr::from(([127, 0, 0, 1], port));
    s.listen_addrs = vec![listen];
    let session = Session::new(s).await.unwrap();
    assert_eq!(session.local_addr().port(), port);
    let errors = session.listen_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].addr, listen);
    assert!(errors[0].udp_only);
    let dht = session.dht_stats().expect("DHT running");
    assert_eq!(dht.local_addrs.len(), 1);
    assert_ne!(dht.local_addrs[0].port(), port);
    assert_ne!(dht.local_addrs[0].port(), 0);
    drop(udp);
    session.shutdown().await;
}

/// A taken UDP port drops only that family's shared socket: the DHT still
/// runs over IPv4 (on a fallback socket) and over IPv6.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dht_falls_back_per_family() {
    if std::net::UdpSocket::bind("[::1]:0").is_err() {
        return;
    }
    let (udp, port) = loop {
        let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = udp.local_addr().unwrap().port();
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            break (udp, port);
        }
    };
    let mut s = dht_settings(Vec::new());
    s.listen_addrs = vec![
        SocketAddr::from(([127, 0, 0, 1], port)),
        "[::1]:0".parse().unwrap(),
    ];
    let session = Session::new(s).await.unwrap();
    assert_eq!(session.local_addrs().len(), 2);
    let dht = session.dht_stats().expect("DHT running");
    assert!(
        dht.local_addrs
            .iter()
            .any(|a| a.is_ipv4() && a.port() != port),
        "{:?}",
        dht.local_addrs
    );
    assert!(
        dht.local_addrs.iter().any(SocketAddr::is_ipv6),
        "{:?}",
        dht.local_addrs
    );
    // The fallback socket is the session's: listed and closed with it.
    assert_eq!(session.demux_stats().len(), 2);
    drop(udp);
    session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dht_on_listener_port_when_free() {
    let session = Session::new(dht_settings(Vec::new())).await.unwrap();
    let dht = session.dht_stats().expect("DHT running");
    assert_eq!(
        dht.local_addrs,
        vec![SocketAddr::from((
            [127, 0, 0, 1],
            session.local_addr().port()
        ))]
    );
    // The DHT runs on the shared socket: a ping reaches its demultiplexer.
    let node = loopback_dht(Vec::new()).await;
    node.add_node(dht.local_addrs[0]);
    assert!(
        wait_until(
            || session
                .demux_stats()
                .first()
                .is_some_and(|(_, s)| s.dht > 0),
            T
        )
        .await
    );
    assert!(wait_until(|| node.stats().nodes_v4 >= 1, T).await);
    node.shutdown().await;
    session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dht_disabled_means_none() {
    let session = Session::new(test_settings()).await.unwrap();
    assert!(session.dht_stats().is_none());
    assert!(matches!(session.dht_state().await, Ok(None)));
    session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dht_bootstrapped_event_once() {
    // A bootstrap node that already knows more than K (8) nodes.
    let boot = loopback_dht(Vec::new()).await;
    let mut others = Vec::new();
    for _ in 0..10 {
        others.push(loopback_dht(vec![addr_of(&boot)]).await);
    }
    assert!(wait_until(|| boot.stats().nodes_v4 >= 8, T).await);

    let session = Session::new(dht_settings(vec![addr_of(&boot)]))
        .await
        .unwrap();
    let mut rx = session.subscribe(EventMask::DHT);
    assert!(
        matches!(next_event(&mut rx, 10).await, Some(Event::DhtBootstrapped { nodes }) if nodes >= 1)
    );
    // exactly one: nothing more in 3 s
    assert!(
        tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .is_err()
    );
    assert!(session.dht_stats().unwrap().bootstrapped);
    session.shutdown().await;
    for d in others.iter().chain([&boot]) {
        d.shutdown().await;
    }
}

#[tokio::test]
async fn bootstrap_entries_validated() {
    let mut s = test_settings();
    s.dht_enabled = true;
    s.dht_bootstrap_nodes = vec!["nohost".into()];
    assert!(
        matches!(Session::new(s).await, Err(SessionError::InvalidSettings(m)) if m.contains("dht_bootstrap_nodes"))
    );
    for bad in [
        "host:99999",
        ":6881",
        "host:",
        "host:x",
        "host:+1",
        " host:6881",
        "host :6881",
        "host:6881\n",
        "ho st:6881",
        "::1:6881",
        "[::1:6881",
        "::1]:6881",
        "[]:6881",
    ] {
        let mut s = test_settings();
        s.dht_bootstrap_nodes = vec![bad.into()];
        assert!(
            matches!(Session::new(s).await, Err(SessionError::InvalidSettings(m)) if m.contains("dht_bootstrap_nodes")),
            "{bad}"
        );
    }
    let mut s = test_settings();
    s.dht_announce_interval = Duration::ZERO;
    assert!(
        matches!(Session::new(s).await, Err(SessionError::InvalidSettings(m)) if m.contains("dht_announce_interval"))
    );
    let mut s = test_settings();
    s.dht_query_timeout = Duration::ZERO;
    assert!(
        matches!(Session::new(s).await, Err(SessionError::InvalidSettings(m)) if m.contains("dht_query_timeout"))
    );
    let mut s = test_settings();
    s.dht_query_rate_per_ip = 0;
    assert!(
        matches!(Session::new(s).await, Err(SessionError::InvalidSettings(m)) if m.contains("dht_query_rate_per_ip"))
    );
    let mut s = test_settings();
    s.dht_bootstrap_nodes = vec![
        "[::1]:6881".into(),
        "127.0.0.1:1".into(),
        "router.example:65535".into(),
    ];
    Session::new(s).await.unwrap().shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_magnet_stops_dht_announcer_at_metadata() {
    let boot = loopback_dht(Vec::new()).await;
    let (m, payload) = make_private_torrent(&[("a", 100_000)], 32_768);
    // The seeder runs no DHT: whatever the bootstrap node stores comes from
    // the leecher.
    let seeder = Session::new(dht_settings(Vec::new())).await.unwrap();
    let _sh = seed_in(&seeder, &m, &payload).await;

    // The leecher's DHT starts with no nodes, so nothing is announced before
    // the metadata arrives through the `x.pe` hint.
    let mut s = dht_settings(Vec::new());
    s.dht_announce_interval = Duration::from_secs(1);
    let leecher = Session::new(s).await.unwrap();
    let mut ev = leecher.subscribe(EventMask::STATUS);
    let dir = tempfile::tempdir().unwrap();
    let mut mg = Magnet::from_info_hashes(m.info_hashes);
    mg.peers = vec![seeder.local_addr()];
    let _h = leecher
        .add_torrent(AddTorrent::magnet(mg, dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(
        next_matching(&mut ev, |e| matches!(e, Event::MetadataReceived { .. }), T)
            .await
            .is_some()
    );
    // Now the leecher's DHT learns the bootstrap node: a running announcer
    // would announce within a second.
    let leecher_dht = leecher.dht_stats().unwrap().local_addrs[0];
    boot.add_node(leecher_dht);
    assert!(wait_until(|| leecher.dht_stats().unwrap().nodes_v4 >= 1, T).await);
    let before = stored(&boot);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(stored(&boot), before);
    assert_eq!(before, 0);

    // New connections get neither the DHT bit nor a `Port`.
    let mut peer = ScriptedPeer::connect_with(
        leecher.local_addr(),
        m.info_hashes.wire_hash(),
        dht_reserved(),
    )
    .await;
    assert!(!has_dht_bit(&peer.remote.reserved));
    assert!(
        peer.next_matching(|m| matches!(m, Message::Port(_)))
            .await
            .is_none()
    );
    leecher.shutdown().await;
    seeder.shutdown().await;
    boot.shutdown().await;
}

/// A public magnet announces once the DHT has nodes, even before its
/// metadata (the control for the private case above).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_magnet_announces_before_metadata() {
    let boot = loopback_dht(Vec::new()).await;
    let leecher = Session::new(dht_settings(vec![addr_of(&boot)]))
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let ih = InfoHashes {
        v1: Some(fbt_core::InfoHashV1([0x5a; 20])),
        v2: None,
    };
    let h = leecher
        .add_torrent(AddTorrent::magnet(
            Magnet::from_info_hashes(ih),
            dir.path().to_path_buf(),
        ))
        .await
        .unwrap();
    assert_eq!(h.status().state, TorrentState::DownloadingMetadata);
    assert!(wait_until(|| stored(&boot) >= 1, T).await);
    leecher.shutdown().await;
    boot.shutdown().await;
}
