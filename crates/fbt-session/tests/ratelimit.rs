//! Rate limits measured by wall clock over loopback: global, per torrent,
//! per peer class, the local-peer exemption, tiny limits, runtime changes,
//! and upload accounting of sent blocks only.
//!
//! Loopback peers are local, so every test sets `rate_limit_local_peers`
//! except `local_limit_applies_when_enabled`, which toggles it.

mod support;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fbt_core::BlockRef;
use fbt_metainfo::Metainfo;
use fbt_session::storage::{MemStorage, Storage};
use fbt_session::{
    AddTorrent, Event, EventMask, PeerClassLimits, RateLimit, Session, Settings, TorrentHandle,
    TorrentState, Transport,
};
use fbt_wire::Message;
use support::*;
use tokio::time::Instant;

const T: Duration = Duration::from_secs(30);
const MIB: u64 = 1024 * 1024;
const KIB: u64 = 1024;

fn settings() -> Settings {
    let mut s = test_settings();
    s.rechoke_interval = Duration::from_secs(1);
    s.peer_retry_delay = Duration::from_millis(200);
    s.rate_limit_local_peers = true;
    s
}

fn tcp_only(mut s: Settings) -> Settings {
    s.enable_outgoing_utp = false;
    s.enable_incoming_utp = false;
    s
}

fn utp_only(mut s: Settings) -> Settings {
    s.enable_outgoing_tcp = false;
    s.enable_incoming_tcp = false;
    s
}

/// A one-file torrent of `len` bytes in 256 KiB pieces.
fn torrent(len: usize) -> (Metainfo, Vec<u8>) {
    make_torrent(&[("a", len)], 256 * 1024)
}

async fn seeder(
    st: Settings,
    m: &Metainfo,
    payload: &[u8],
    limit: RateLimit,
) -> (Session, TorrentHandle) {
    let s = Session::new(st).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(payload.to_vec())));
    p.rate_limit = limit;
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    (s, h)
}

struct Leecher {
    session: Session,
    handle: TorrentHandle,
    storage: Arc<MemStorage>,
}

async fn leecher(st: Settings, m: &Metainfo, len: usize) -> Leecher {
    let session = Session::new(st).await.unwrap();
    let storage = Arc::new(MemStorage::new(len as u64));
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(storage.clone());
    let handle = session.add_torrent(p).await.unwrap();
    assert!(wait_state(&handle, |s| *s == TorrentState::Downloading, T).await);
    Leecher {
        session,
        handle,
        storage,
    }
}

impl Leecher {
    /// Waits for the download to finish (within `limit`) and checks the
    /// data.
    async fn finish(&self, payload: &[u8], limit: Duration) {
        assert!(
            wait_state(&self.handle, |s| *s == TorrentState::Seeding, limit).await,
            "download did not finish in {limit:?}"
        );
        let mut got = vec![0u8; payload.len()];
        self.storage.read(0, &mut got).unwrap();
        assert!(got == payload, "downloaded data differs");
    }
}

/// Downloads `m` from `seed` into a new leecher with settings `st`;
/// returns the time from dialling to the finished download.
async fn timed_download(st: Settings, seed: &Session, m: &Metainfo, payload: &[u8]) -> Duration {
    let l = leecher(st, m, payload.len()).await;
    let t0 = Instant::now();
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    l.finish(payload, T).await;
    let took = t0.elapsed();
    l.session.shutdown().await;
    took
}

#[tokio::test]
async fn global_download_limit_is_honored() {
    let (m, payload) = torrent(8 << 20);
    let (seed, _) = seeder(settings(), &m, &payload, RateLimit::default()).await;
    let mut st = settings();
    st.rate_limit = RateLimit::new(MIB, 0);
    let took = timed_download(st, &seed, &m, &payload).await;
    assert!(
        took >= Duration::from_millis(6500) && took <= Duration::from_secs(10),
        "8 MiB at 1 MiB/s took {took:?}"
    );
    seed.shutdown().await;
}

#[tokio::test]
async fn torrent_upload_limit_is_honored() {
    let (m, payload) = torrent(8 << 20);
    let (seed, _) = seeder(settings(), &m, &payload, RateLimit::new(0, MIB)).await;
    let took = timed_download(settings(), &seed, &m, &payload).await;
    assert!(
        took >= Duration::from_millis(6500) && took <= Duration::from_secs(10),
        "8 MiB at 1 MiB/s took {took:?}"
    );
    seed.shutdown().await;
}

#[tokio::test]
async fn class_limit_applies_to_utp_only() {
    let (m, payload) = torrent(2 << 20);
    let mut st = settings();
    let mut classes = PeerClassLimits::default();
    classes.utp = RateLimit::new(0, 512 * KIB);
    st.peer_class_limits = classes;
    let (seed, _) = seeder(st, &m, &payload, RateLimit::default()).await;
    let (utp, tcp) = tokio::join!(
        timed_download(utp_only(settings()), &seed, &m, &payload),
        timed_download(tcp_only(settings()), &seed, &m, &payload),
    );
    // 2 MiB at 512 KiB/s: about 4 s.
    assert!(utp >= Duration::from_millis(3200), "uTP took {utp:?}");
    assert!(utp >= tcp * 3, "uTP {utp:?} vs TCP {tcp:?}");
    seed.shutdown().await;
}

#[tokio::test]
async fn local_limit_applies_when_enabled() {
    let (m, payload) = torrent(2 << 20);
    let mut seed_st = settings();
    seed_st.rate_limit_local_peers = false;
    let (seed, _) = seeder(seed_st, &m, &payload, RateLimit::default()).await;
    // 2 MiB at 512 KiB/s would take about 4 s.
    let limited = |local: bool| {
        let mut st = settings();
        st.rate_limit = RateLimit::new(512 * KIB, 0);
        st.rate_limit_local_peers = local;
        st
    };
    let exempt = timed_download(limited(false), &seed, &m, &payload).await;
    assert!(
        exempt < Duration::from_secs(2),
        "a loopback peer is exempt by default, took {exempt:?}"
    );
    let honored = timed_download(limited(true), &seed, &m, &payload).await;
    assert!(
        honored >= Duration::from_millis(3200),
        "the limit applies to local peers when enabled, took {honored:?}"
    );
    seed.shutdown().await;
}

/// Datagrams the session's shared UDP sockets handed to uTP so far.
fn utp_datagrams(s: &Session) -> u64 {
    s.demux_stats().iter().map(|(_, st)| st.utp).sum()
}

#[tokio::test]
async fn tiny_limit_keeps_connections_alive() {
    let (m, payload) = torrent(1 << 20);
    let tiny = || {
        let mut st = settings();
        st.rate_limit = RateLimit::new(1, 1);
        st.peer_idle_timeout = Duration::from_secs(2);
        st.keepalive_interval = Duration::from_millis(500);
        st
    };
    let (seed, sh) = seeder(tiny(), &m, &payload, RateLimit::default()).await;
    let l = leecher(tiny(), &m, payload.len()).await;
    let mut ev = l.session.subscribe(EventMask::PEER);
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    match next_matching(&mut ev, |e| matches!(e, Event::PeerConnected { .. }), T).await {
        Some(Event::PeerConnected { transport, .. }) => assert_eq!(transport, Transport::Utp),
        other => panic!("expected PeerConnected, got {other:?}"),
    }
    assert!(wait_until(|| sh.status().num_peers == 1, T).await);
    let (seed_before, leech_before) = (utp_datagrams(&seed), utp_datagrams(&l.session));
    tokio::time::sleep(Duration::from_secs(5)).await;
    let gone = next_matching(
        &mut ev,
        |e| matches!(e, Event::PeerDisconnected { .. }),
        Duration::from_millis(100),
    )
    .await;
    assert!(gone.is_none(), "disconnected: {gone:?}");
    assert_eq!(l.handle.status().num_peers, 1);
    assert_eq!(sh.status().num_peers, 1);
    // Keepalives (and their acks) kept flowing both ways.
    assert!(utp_datagrams(&seed) >= seed_before + 5);
    assert!(utp_datagrams(&l.session) >= leech_before + 5);
    l.session.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test]
async fn limit_change_applies_mid_transfer() {
    let (m, payload) = torrent(8 << 20);
    let (seed, _) = seeder(settings(), &m, &payload, RateLimit::default()).await;
    let mut st = settings();
    st.rate_limit = RateLimit::new(256 * KIB, 0);
    let l = leecher(st, &m, payload.len()).await;
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(l.handle.status().state, TorrentState::Downloading);
    l.session.set_rate_limit(RateLimit::default());
    let t0 = Instant::now();
    l.finish(&payload, T).await;
    let rest = t0.elapsed();
    assert!(rest < Duration::from_secs(3), "rest took {rest:?}");
    l.session.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test]
async fn upload_counts_only_sent_blocks() {
    const BLOCK: u32 = 16_384;
    let (m, payload) = make_torrent(&[("a", 16 * BLOCK as usize)], 4 * BLOCK);
    let (seed, sh) = seeder(settings(), &m, &payload, RateLimit::new(0, 16 * KIB)).await;
    let mut peer = ScriptedPeer::connect_fast(seed.local_addr(), m.info_hashes.wire_hash()).await;
    peer.send(Message::HaveNone).await;
    peer.send(Message::Interested).await;
    peer.next_matching(|m| *m == Message::Unchoke)
        .await
        .expect("Unchoke");
    let blocks: Vec<BlockRef> = (0..10)
        .map(|i| BlockRef {
            piece: i / 4,
            offset: (i % 4) * BLOCK,
            len: BLOCK,
        })
        .collect();
    for b in &blocks {
        peer.send(Message::Request(*b)).await;
    }
    let next_block = async |peer: &mut ScriptedPeer| match peer
        .next_matching(|m| matches!(m, Message::Piece { .. }))
        .await
    {
        Some(Message::Piece { piece, offset, .. }) => (piece, offset),
        other => panic!("expected a block, got {other:?}"),
    };
    // The first block goes out at once; at 16 KiB/s the next is due a
    // second later, so the other nine are read and queued meanwhile.
    let first = next_block(&mut peer).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let rest: Vec<BlockRef> = blocks
        .iter()
        .filter(|b| (b.piece, b.offset) != first)
        .copied()
        .collect();
    let (keep, cancel) = rest.split_at(4);
    for b in cancel {
        peer.send(Message::Cancel(*b)).await;
    }
    let mut got = Vec::new();
    for _ in 0..4 {
        got.push(next_block(&mut peer).await);
    }
    // In any order: piece reads finish in their own order.
    got.sort_unstable();
    let want: Vec<(u32, u32)> = keep.iter().map(|b| (b.piece, b.offset)).collect();
    assert_eq!(got, want);
    // Nothing cancelled follows (the next block would be due in a second).
    let more = tokio::time::timeout(
        Duration::from_secs(2),
        peer.next_matching(|m| matches!(m, Message::Piece { .. })),
    )
    .await;
    assert!(
        matches!(more, Err(_) | Ok(None)),
        "a cancelled block was sent"
    );
    let want = 5 * u64::from(BLOCK);
    assert!(wait_until(|| sh.status().total_uploaded == want, T).await);
    assert_eq!(sh.status().total_uploaded, want);
    seed.shutdown().await;
}
