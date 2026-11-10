//! Peer connections over uTP: uTP-only sessions, uTP preferred by default,
//! the fallback to TCP when uTP cannot connect, MSE over uTP, and a swarm
//! mixing both transports.

mod support;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fbt_metainfo::Metainfo;
use fbt_session::storage::{MemStorage, Storage};
use fbt_session::{
    AddTorrent, DisconnectReason, EncryptionPolicy, Event, EventMask, EventStream, Session,
    SessionError, Settings, TorrentHandle, TorrentState, Transport,
};
use support::*;

const T: Duration = Duration::from_secs(30);

fn settings() -> Settings {
    let mut s = test_settings();
    s.rechoke_interval = Duration::from_secs(1);
    s.peer_retry_delay = Duration::from_millis(200);
    s
}

fn utp_only() -> Settings {
    let mut s = settings();
    s.enable_outgoing_tcp = false;
    s.enable_incoming_tcp = false;
    s
}

fn tcp_only() -> Settings {
    let mut s = settings();
    s.enable_outgoing_utp = false;
    s.enable_incoming_utp = false;
    s
}

fn torrent() -> (Metainfo, Vec<u8>) {
    make_torrent(&[("a", 300_000), ("b", 12_345)], 16_384)
}

async fn seeder(st: Settings, m: &Metainfo, payload: &[u8]) -> (Session, TorrentHandle) {
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

/// A leecher for `m`, subscribed to its events, that has not dialled yet.
async fn leecher(st: Settings, m: &Metainfo, len: usize) -> Leecher {
    let session = Session::new(st).await.unwrap();
    let storage = Arc::new(MemStorage::new(len as u64));
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(storage.clone());
    let handle = session.add_torrent(p).await.unwrap();
    assert!(wait_state(&handle, |s| *s == TorrentState::Downloading, T).await);
    let events = session.subscribe(EventMask::ALL);
    Leecher {
        session,
        handle,
        events,
        storage,
    }
}

impl Leecher {
    fn stored(&self, len: usize) -> Vec<u8> {
        let mut got = vec![0u8; len];
        self.storage.read(0, &mut got).unwrap();
        got
    }

    async fn finish(&self, payload: &[u8]) {
        assert!(
            wait_state(&self.handle, |s| *s == TorrentState::Seeding, T).await,
            "download did not finish"
        );
        assert_eq!(self.stored(payload.len()), payload);
    }
}

/// The transport and encryption of the next `PeerConnected`.
async fn connected(ev: &mut EventStream) -> (Transport, bool) {
    match next_matching(ev, |e| matches!(e, Event::PeerConnected { .. }), T).await {
        Some(Event::PeerConnected {
            transport,
            encrypted,
            ..
        }) => (transport, encrypted),
        other => panic!("expected PeerConnected, got {other:?}"),
    }
}

#[tokio::test]
async fn utp_only_download() {
    let (m, payload) = torrent();
    let (seed, _) = seeder(utp_only(), &m, &payload).await;
    let mut l = leecher(utp_only(), &m, payload.len()).await;
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    assert_eq!(connected(&mut l.events).await.0, Transport::Utp);
    l.finish(&payload).await;
}

#[tokio::test]
async fn utp_preferred_by_default() {
    let (m, payload) = torrent();
    let (seed, _) = seeder(settings(), &m, &payload).await;
    let mut seed_ev = seed.subscribe(EventMask::PEER);
    let mut l = leecher(settings(), &m, payload.len()).await;
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    assert_eq!(connected(&mut l.events).await.0, Transport::Utp);
    assert_eq!(connected(&mut seed_ev).await.0, Transport::Utp);
    l.finish(&payload).await;
}

fn is_disconnect(e: &Event) -> bool {
    matches!(e, Event::PeerDisconnected { .. })
}

#[tokio::test]
async fn utp_connect_failure_falls_back_to_tcp() {
    let (m, payload) = torrent();
    let mut seed_st = settings();
    seed_st.enable_incoming_utp = false;
    let (seed, _) = seeder(seed_st, &m, &payload).await;
    let mut st = settings();
    st.utp_connect_timeout = Duration::from_millis(300);
    let mut l = leecher(st, &m, payload.len()).await;
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    // The uTP attempt times out and is reported as a connect failure; the
    // TCP retry follows at once.
    let first = next_matching(
        &mut l.events,
        |e| is_disconnect(e) || matches!(e, Event::PeerConnected { .. }),
        T,
    )
    .await;
    assert!(
        matches!(
            first,
            Some(Event::PeerDisconnected {
                reason: DisconnectReason::Io(_),
                ..
            })
        ),
        "expected the uTP connect failure first, got {first:?}"
    );
    assert_eq!(connected(&mut l.events).await.0, Transport::Tcp);
    l.finish(&payload).await;

    // Paused and resumed, the address is dialled again: over TCP first, so
    // no further connect failure shows up (only the pause's own close).
    l.handle.pause().await.unwrap();
    let paused = next_matching(&mut l.events, is_disconnect, T).await;
    assert!(
        matches!(
            paused,
            Some(Event::PeerDisconnected {
                reason: DisconnectReason::Shutdown,
                ..
            })
        ),
        "{paused:?}"
    );
    l.handle.resume().await.unwrap();
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    let mut failures = 1;
    let transport = loop {
        match next_matching(
            &mut l.events,
            |e| is_disconnect(e) || matches!(e, Event::PeerConnected { .. }),
            T,
        )
        .await
        {
            Some(Event::PeerConnected { transport, .. }) => break transport,
            Some(_) => failures += 1,
            None => panic!("no reconnect"),
        }
    };
    assert_eq!(transport, Transport::Tcp);
    assert_eq!(failures, 1, "one connect failure in total");
}

#[tokio::test]
async fn tcp_only_peer_is_dialed_over_tcp_when_utp_disabled() {
    let (m, payload) = torrent();
    let (seed, _) = seeder(tcp_only(), &m, &payload).await;
    let mut seed_ev = seed.subscribe(EventMask::PEER);
    let mut st = settings();
    st.enable_outgoing_utp = false;
    let mut l = leecher(st, &m, payload.len()).await;
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    // No uTP attempt at all: the first peer event is the TCP connection.
    let first = next_matching(
        &mut l.events,
        |e| is_disconnect(e) || matches!(e, Event::PeerConnected { .. }),
        T,
    )
    .await;
    assert!(
        matches!(
            first,
            Some(Event::PeerConnected {
                transport: Transport::Tcp,
                ..
            })
        ),
        "{first:?}"
    );
    assert_eq!(connected(&mut seed_ev).await.0, Transport::Tcp);
    l.finish(&payload).await;
}

#[tokio::test]
async fn encrypted_over_utp() {
    let (m, payload) = torrent();
    let mut forced = settings();
    forced.encryption = EncryptionPolicy::Forced;
    let (seed, _) = seeder(forced.clone(), &m, &payload).await;
    let mut seed_ev = seed.subscribe(EventMask::PEER);
    let mut l = leecher(forced, &m, payload.len()).await;
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    assert_eq!(connected(&mut l.events).await, (Transport::Utp, true));
    assert_eq!(connected(&mut seed_ev).await, (Transport::Utp, true));
    l.finish(&payload).await;
}

#[tokio::test]
async fn all_transports_off_is_invalid() {
    let mut s = test_settings();
    s.enable_outgoing_tcp = false;
    s.enable_incoming_tcp = false;
    s.enable_outgoing_utp = false;
    s.enable_incoming_utp = false;
    match Session::new(s).await {
        Err(SessionError::InvalidSettings(msg)) => {
            assert!(msg.contains("enable_"), "{msg}");
        }
        other => panic!("expected InvalidSettings, got {other:?}"),
    }
    // Any one of them on is enough.
    let mut s = test_settings();
    s.enable_outgoing_tcp = false;
    s.enable_incoming_tcp = false;
    s.enable_outgoing_utp = false;
    let session = Session::new(s).await.unwrap();
    session.shutdown().await;
}

#[tokio::test]
async fn mixed_swarm_tcp_and_utp() {
    let (m, payload) = torrent();
    let (seed, _) = seeder(settings(), &m, &payload).await;
    let mut seed_ev = seed.subscribe(EventMask::PEER);
    let mut leechers = Vec::new();
    for (st, want) in [
        (utp_only(), Transport::Utp),
        (tcp_only(), Transport::Tcp),
        (settings(), Transport::Utp),
    ] {
        let mut l = leecher(st, &m, payload.len()).await;
        l.handle.add_peer(seed.local_addr()).await.unwrap();
        assert_eq!(connected(&mut l.events).await.0, want);
        leechers.push(l);
    }
    let mut seen = Vec::new();
    for _ in 0..3 {
        seen.push(connected(&mut seed_ev).await.0);
    }
    seen.sort_by_key(|t| *t == Transport::Utp);
    assert_eq!(seen, [Transport::Tcp, Transport::Utp, Transport::Utp]);
    for l in &leechers {
        l.finish(&payload).await;
    }
    for l in &leechers {
        l.session.shutdown().await;
    }
}
