//! MSE/PE between sessions and against hand-driven peers: the forced,
//! preferred and disabled policies, the listener telling plaintext from
//! encrypted connections, and routing encrypted ones by their SKEY.

mod support;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fbt_core::PeerId;
use fbt_metainfo::Metainfo;
use fbt_session::storage::{MemStorage, Storage};
use fbt_session::{
    AddTorrent, DisconnectReason, EncryptionPolicy, Event, EventMask, EventStream, Session,
    Settings, TorrentHandle, TorrentState,
};
use fbt_wire::{CryptoMethods, Handshake, Reserved, read_handshake, write_handshake};
use support::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const T: Duration = Duration::from_secs(30);
/// Connection attempts per address (the swarm's `MAX_ATTEMPTS`).
const MAX_ATTEMPTS: usize = 3;

fn settings(policy: EncryptionPolicy) -> Settings {
    let mut s = test_settings();
    s.rechoke_interval = Duration::from_secs(1);
    s.peer_retry_delay = Duration::from_millis(200);
    s.encryption = policy;
    s
}

async fn seeder_with(st: Settings, m: &Metainfo, payload: &[u8]) -> (Session, TorrentHandle) {
    let s = Session::new(st).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(payload.to_vec())));
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    (s, h)
}

struct Pair {
    _seeder: Session,
    leecher: Session,
    handle: TorrentHandle,
    events: EventStream,
    storage: Arc<MemStorage>,
    payload: Vec<u8>,
}

impl Pair {
    fn stored(&self) -> Vec<u8> {
        let mut got = vec![0u8; self.payload.len()];
        self.storage.read(0, &mut got).unwrap();
        got
    }
}

/// A seeder with data under `seed` and a leecher under `leech` that dials
/// it; the leecher's events are subscribed before the dial.
async fn pair(seed: EncryptionPolicy, leech: EncryptionPolicy) -> Pair {
    let (m, payload) = make_torrent(&[("a", 300_000), ("b", 12_345)], 16_384);
    let (seeder, _) = seeder_with(settings(seed), &m, &payload).await;
    let leecher = Session::new(settings(leech)).await.unwrap();
    let storage = Arc::new(MemStorage::new(payload.len() as u64));
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(storage.clone());
    let handle = leecher.add_torrent(p).await.unwrap();
    assert!(wait_state(&handle, |s| *s == TorrentState::Downloading, T).await);
    let events = leecher.subscribe(EventMask::ALL);
    handle.add_peer(seeder.local_addr()).await.unwrap();
    Pair {
        _seeder: seeder,
        leecher,
        handle,
        events,
        storage,
        payload,
    }
}

fn is_encryption_failure(e: &Event) -> bool {
    matches!(
        e,
        Event::PeerDisconnected {
            reason: DisconnectReason::Encryption(_),
            ..
        }
    )
}

/// Events matching `pred` over `window`.
async fn count_events(
    ev: &mut EventStream,
    pred: impl Fn(&Event) -> bool,
    window: Duration,
) -> usize {
    let end = tokio::time::Instant::now() + window;
    let mut n = 0;
    while let Ok(Some(e)) = tokio::time::timeout_at(end, ev.recv()).await {
        if pred(&e) {
            n += 1;
        }
    }
    n
}

fn handshake_for(info_hash: [u8; 20]) -> Handshake {
    Handshake {
        reserved: Reserved::default(),
        info_hash,
        peer_id: PeerId([0x42; 20]),
    }
}

fn handshake_bytes(info_hash: [u8; 20]) -> Vec<u8> {
    let mut v = Vec::new();
    v.push(19u8);
    v.extend_from_slice(b"BitTorrent protocol");
    v.extend_from_slice(&[0u8; 8]);
    v.extend_from_slice(&info_hash);
    v.extend_from_slice(&[0x42; 20]);
    v
}

/// Reads until EOF or reset within `within`; true if the remote closed.
async fn closed_within<S: tokio::io::AsyncRead + Unpin>(s: &mut S, within: Duration) -> bool {
    tokio::time::timeout(within, async {
        let mut buf = [0u8; 1024];
        loop {
            match s.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    })
    .await
    .is_ok()
}

/// A seeding session for a fresh torrent; returns the torrent's wire hash.
async fn seeder_for(st: Settings) -> (Session, TorrentHandle, [u8; 20]) {
    let (m, payload) = make_torrent(&[("a", 40_000)], 16_384);
    let (s, h) = seeder_with(st, &m, &payload).await;
    (s, h, m.info_hashes.wire_hash())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forced_both_sides_downloads_encrypted() {
    let mut p = pair(EncryptionPolicy::Forced, EncryptionPolicy::Forced).await;
    next_matching(
        &mut p.events,
        |e| matches!(e, Event::TorrentFinished { .. }),
        T,
    )
    .await
    .expect("TorrentFinished");
    assert!(p.stored() == p.payload, "downloaded bytes differ");
    p.leecher.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preferred_both_sides_downloads() {
    let mut p = pair(EncryptionPolicy::Preferred, EncryptionPolicy::Preferred).await;
    next_matching(
        &mut p.events,
        |e| matches!(e, Event::TorrentFinished { .. }),
        T,
    )
    .await
    .expect("TorrentFinished");
    assert!(p.stored() == p.payload, "downloaded bytes differ");
    p.leecher.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preferred_vs_disabled_falls_back_to_plaintext() {
    let mut p = pair(EncryptionPolicy::Disabled, EncryptionPolicy::Preferred).await;
    next_matching(&mut p.events, is_encryption_failure, T)
        .await
        .expect("the encrypted attempt failed");
    next_matching(
        &mut p.events,
        |e| matches!(e, Event::TorrentFinished { .. }),
        T,
    )
    .await
    .expect("TorrentFinished over plaintext");
    assert!(p.stored() == p.payload, "downloaded bytes differ");
    p.leecher.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forced_policy_against_plaintext_peer_fails_cleanly() {
    let mut p = pair(EncryptionPolicy::Disabled, EncryptionPolicy::Forced).await;
    let failures = count_events(&mut p.events, is_encryption_failure, Duration::from_secs(3)).await;
    assert_eq!(failures, MAX_ATTEMPTS);
    let st = p.handle.status();
    assert_eq!(st.num_peers, 0);
    assert_ne!(st.state, TorrentState::Seeding);
    p.leecher.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forced_listener_refuses_plaintext_handshake() {
    let (s, _h, ih) = seeder_for(settings(EncryptionPolicy::Forced)).await;
    let mut ev = s.subscribe(EventMask::PEER);
    let mut tcp = TcpStream::connect(s.local_addr()).await.unwrap();
    tcp.write_all(&handshake_bytes(ih)).await.unwrap();
    assert!(closed_within(&mut tcp, Duration::from_secs(2)).await);
    let connected = count_events(
        &mut ev,
        |e| matches!(e, Event::PeerConnected { .. }),
        Duration::from_millis(200),
    )
    .await;
    assert_eq!(connected, 0);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_listener_refuses_mse() {
    let (s, _h, ih) = seeder_for(settings(EncryptionPolicy::Disabled)).await;
    let tcp = TcpStream::connect(s.local_addr()).await.unwrap();
    let r = tokio::time::timeout(
        Duration::from_secs(5),
        fbt_wire::initiate(tcp, &ih, CryptoMethods::RC4, b""),
    )
    .await
    .expect("refused promptly");
    assert!(r.is_err());
    // A plaintext peer is still welcome.
    let p = ScriptedPeer::connect(s.local_addr(), ih).await;
    assert_eq!(p.remote.info_hash, ih);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_mse_handshake_times_out() {
    let mut st = settings(EncryptionPolicy::Preferred);
    st.handshake_timeout = Duration::from_millis(500);
    // One pending-handshake slot: the stalled peer must give it back.
    st.max_peers_global = 1;
    let (s, _h, ih) = seeder_for(st).await;
    let mut raw = TcpStream::connect(s.local_addr()).await.unwrap();
    raw.write_all(&[0x55; 96]).await.unwrap(); // Ya, then nothing
    // The responder sends Yb, then waits for step 3 until the deadline.
    assert!(
        closed_within(&mut raw, Duration::from_millis(1500)).await,
        "server did not close the stalled MSE connection"
    );
    drop(raw);
    let p = ScriptedPeer::connect(s.local_addr(), ih).await;
    assert_eq!(p.remote.info_hash, ih);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incoming_mse_routes_by_skey_among_many_torrents() {
    let s = Session::new(settings(EncryptionPolicy::Forced))
        .await
        .unwrap();
    let mut hashes = Vec::new();
    for _ in 0..3 {
        let (m, payload) = make_torrent(&[("a", 20_000)], 16_384);
        let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
        p.storage = Some(Arc::new(MemStorage::from_bytes(payload)));
        let h = s.add_torrent(p).await.unwrap();
        assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
        hashes.push(m.info_hashes.wire_hash());
    }
    let mut ev = s.subscribe(EventMask::PEER);
    let tcp = TcpStream::connect(s.local_addr()).await.unwrap();
    let ia = handshake_bytes(hashes[1]);
    let mut m = fbt_wire::initiate(tcp, &hashes[1], CryptoMethods::RC4, &ia)
        .await
        .expect("MSE handshake");
    assert!(m.is_encrypted());
    let back = tokio::time::timeout(Duration::from_secs(5), read_handshake(&mut m))
        .await
        .expect("handshake in time")
        .expect("handshake");
    assert_eq!(back.info_hash, hashes[1]);
    next_matching(&mut ev, |e| matches!(e, Event::PeerConnected { .. }), T)
        .await
        .expect("PeerConnected");

    // An initiator offering only plaintext is refused under `Forced`.
    let tcp = TcpStream::connect(s.local_addr()).await.unwrap();
    let r = fbt_wire::initiate(tcp, &hashes[2], CryptoMethods::PLAINTEXT, b"").await;
    assert!(r.is_err(), "a forced listener refuses MSE plaintext");
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incoming_mse_plaintext_method_and_late_handshake() {
    let s = Session::new(settings(EncryptionPolicy::Preferred))
        .await
        .unwrap();
    let (m, payload) = make_torrent(&[("a", 20_000)], 16_384);
    seeder_with_session(&s, &m, payload).await;
    let ih = m.info_hashes.wire_hash();
    let tcp = TcpStream::connect(s.local_addr()).await.unwrap();
    let mut m = fbt_wire::initiate(tcp, &ih, CryptoMethods::PLAINTEXT, b"")
        .await
        .expect("MSE handshake");
    assert!(!m.is_encrypted());
    write_handshake(&mut m, &handshake_for(ih)).await.unwrap();
    m.flush().await.unwrap();
    let back = tokio::time::timeout(Duration::from_secs(5), read_handshake(&mut m))
        .await
        .expect("handshake in time")
        .expect("handshake");
    assert_eq!(back.info_hash, ih);
    s.shutdown().await;
}

async fn seeder_with_session(s: &Session, m: &Metainfo, payload: Vec<u8>) -> TorrentHandle {
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(payload)));
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    h
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incoming_mse_with_mismatched_bt_handshake_is_dropped() {
    let s = Session::new(settings(EncryptionPolicy::Preferred))
        .await
        .unwrap();
    let mut hashes = Vec::new();
    for _ in 0..2 {
        let (m, payload) = make_torrent(&[("a", 20_000)], 16_384);
        seeder_with_session(&s, &m, payload).await;
        hashes.push(m.info_hashes.wire_hash());
    }
    let mut ev = s.subscribe(EventMask::PEER);
    let tcp = TcpStream::connect(s.local_addr()).await.unwrap();
    // SKEY names torrent 0, the BT handshake torrent 1.
    let ia = handshake_bytes(hashes[1]);
    let mut m = fbt_wire::initiate(tcp, &hashes[0], CryptoMethods::RC4, &ia)
        .await
        .expect("MSE handshake");
    assert!(closed_within(&mut m, Duration::from_secs(2)).await);
    let connected = count_events(
        &mut ev,
        |e| matches!(e, Event::PeerConnected { .. }),
        Duration::from_millis(200),
    )
    .await;
    assert_eq!(connected, 0);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incoming_mse_for_unknown_torrent_is_dropped() {
    let (s, _h, _ih) = seeder_for(settings(EncryptionPolicy::Preferred)).await;
    let tcp = TcpStream::connect(s.local_addr()).await.unwrap();
    let r = tokio::time::timeout(
        Duration::from_secs(5),
        fbt_wire::initiate(tcp, &[0xee; 20], CryptoMethods::RC4, b""),
    )
    .await
    .expect("refused promptly");
    assert!(r.is_err());
    s.shutdown().await;
}

/// The CLI race behind a CI hang: the leecher dials a seeder whose torrent
/// is not accepting peers yet (still checking; here paused), so the first
/// attempt ends right after the MSE handshake. The retry must come well
/// before the full `peer_retry_delay` (60 s by default), or a lone explicit
/// peer stalls the download for that long.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_retry_comes_soon_after_a_peer_that_was_not_ready() {
    let delay = Duration::from_secs(12);
    let mut st = settings(EncryptionPolicy::Forced);
    st.peer_retry_delay = delay;
    let (m, payload) = make_torrent(&[("a", 300_000)], 16_384);
    let seeder = Session::new(st.clone()).await.unwrap();
    let mut sp = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    sp.storage = Some(Arc::new(MemStorage::from_bytes(payload.clone())));
    sp.paused = true;
    let seed = seeder.add_torrent(sp).await.unwrap();
    assert!(wait_state(&seed, |s| *s == TorrentState::Paused, T).await);

    let leecher = Session::new(st).await.unwrap();
    let storage = Arc::new(MemStorage::new(payload.len() as u64));
    let mut lp = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    lp.storage = Some(storage.clone());
    let handle = leecher.add_torrent(lp).await.unwrap();
    assert!(wait_state(&handle, |s| *s == TorrentState::Downloading, T).await);
    let mut events = leecher.subscribe(EventMask::ALL);
    handle.add_peer(seeder.local_addr()).await.unwrap();

    // The seeder drops the connection: one failed attempt.
    next_matching(
        &mut events,
        |e| matches!(e, Event::PeerDisconnected { .. }),
        T,
    )
    .await
    .expect("first attempt fails");
    seed.resume().await.unwrap();
    // Well within the full delay, the retry connects and finishes.
    next_matching(
        &mut events,
        |e| matches!(e, Event::TorrentFinished { .. }),
        delay * 2 / 3,
    )
    .await
    .expect("finished before the full retry delay");
    let mut got = vec![0u8; payload.len()];
    storage.read(0, &mut got).unwrap();
    assert!(got == payload, "downloaded bytes differ");
    leecher.shutdown().await;
    seeder.shutdown().await;
}
