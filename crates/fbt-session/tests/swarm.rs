//! Downloads between sessions on loopback: multi-file transfers, several
//! seeders, a seeder leaving, corrupt data, skipped files, self-connections
//! and invalid requests.

mod support;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use fbt_core::BlockRef;
use fbt_metainfo::Metainfo;
use fbt_session::storage::{MemStorage, Storage};
use fbt_session::{
    AddTorrent, DisconnectReason, Event, EventMask, Priority, Session, Settings, TorrentHandle,
    TorrentState,
};
use fbt_wire::Message;
use support::*;

const T: Duration = Duration::from_secs(30);

fn settings() -> Settings {
    let mut s = test_settings();
    s.rechoke_interval = Duration::from_secs(1);
    s
}

/// A session seeding `m` from `storage`, waited until `Seeding`.
async fn seeder(m: &Metainfo, storage: Arc<dyn Storage>) -> (Session, TorrentHandle) {
    let s = Session::new(settings()).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(storage);
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    (s, h)
}

/// A leecher session for `m` over fresh memory storage, waited until
/// `Downloading`.
async fn mem_leecher(m: &Metainfo, len: usize) -> (Session, TorrentHandle, Arc<MemStorage>) {
    mem_leecher_with(settings(), m, len).await
}

/// [`mem_leecher`] with the given settings.
async fn mem_leecher_with(
    st: Settings,
    m: &Metainfo,
    len: usize,
) -> (Session, TorrentHandle, Arc<MemStorage>) {
    let s = Session::new(st).await.unwrap();
    let storage = Arc::new(MemStorage::new(len as u64));
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(storage.clone());
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);
    (s, h, storage)
}

fn stored(storage: &MemStorage, len: usize) -> Vec<u8> {
    let mut got = vec![0u8; len];
    storage.read(0, &mut got).unwrap();
    got
}

async fn finished(ev: &mut fbt_session::EventStream) {
    next_matching(ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_sessions_download_multi_file() {
    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    let files = [("a", 1 << 20), ("e", 0), ("b", 5 << 19)];
    let (m, payload) = make_torrent(&files, 65_536);
    write_payload(seed_dir.path(), &m, &payload);
    let seeder = Session::new(settings()).await.unwrap();
    let sh = seeder
        .add_torrent(AddTorrent::new(m.clone(), seed_dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&sh, |s| *s == TorrentState::Seeding, T).await);
    let leecher = Session::new(settings()).await.unwrap();
    let mut ev = leecher.subscribe(EventMask::ALL);
    let lh = leecher
        .add_torrent(AddTorrent::new(m.clone(), leech_dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&lh, |s| *s == TorrentState::Downloading, T).await);
    lh.add_peer(seeder.local_addr()).await.unwrap();
    finished(&mut ev).await;
    assert!(wait_state(&lh, |s| *s == TorrentState::Seeding, T).await);
    let mut off = 0;
    for (name, len) in files {
        let got = std::fs::read(leech_dir.path().join("torrent").join(name)).unwrap();
        assert!(got == payload[off..off + len], "file {name} differs");
        off += len;
    }
    let total = payload.len() as u64;
    assert!(wait_until(|| sh.status().total_uploaded >= total, T).await);
    leecher.shutdown().await;
    seeder.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_from_two_seeds_uses_both() {
    let (m, payload) = make_torrent(&[("a", 1 << 20), ("b", 1 << 20)], 65_536);
    // Throttled so neither seeder can finish the job before the other joins.
    let delay = Duration::from_millis(2);
    let (sa, ha) = seeder(&m, Arc::new(SlowStorage::new(payload.clone(), delay))).await;
    let (sb, hb) = seeder(&m, Arc::new(SlowStorage::new(payload.clone(), delay))).await;
    let (l, lh, storage) = mem_leecher(&m, payload.len()).await;
    let mut ev = l.subscribe(EventMask::ALL);
    lh.add_peer(sa.local_addr()).await.unwrap();
    lh.add_peer(sb.local_addr()).await.unwrap();
    finished(&mut ev).await;
    assert!(stored(&storage, payload.len()) == payload, "data differs");
    assert!(wait_until(|| ha.status().total_uploaded > 0, T).await);
    assert!(wait_until(|| hb.status().total_uploaded > 0, T).await);
    assert_eq!(lh.status().total_downloaded, payload.len() as u64);
    l.shutdown().await;
    sa.shutdown().await;
    sb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_survives_seeder_leaving_mid_piece() {
    let piece = 1 << 20;
    let (m, payload) = make_torrent(&[("a", 12 * piece)], piece as u32);
    // About 4 MiB/s each: the download takes seconds, not milliseconds.
    let delay = Duration::from_millis(4);
    let (sa, ha) = seeder(&m, Arc::new(SlowStorage::new(payload.clone(), delay))).await;
    let (sb, _hb) = seeder(&m, Arc::new(SlowStorage::new(payload.clone(), delay))).await;
    let (l, lh, storage) = mem_leecher(&m, payload.len()).await;
    let mut ev = l.subscribe(EventMask::ALL);
    lh.add_peer(sa.local_addr()).await.unwrap();
    lh.add_peer(sb.local_addr()).await.unwrap();
    assert!(wait_until(|| lh.status().progress > 0.3, T).await);
    assert!(
        lh.status().progress < 1.0,
        "the download must still be running when A leaves"
    );
    assert!(ha.status().total_uploaded > 0, "A took part");
    sa.shutdown().await;
    finished(&mut ev).await;
    assert!(stored(&storage, payload.len()) == payload, "data differs");
    assert!(wait_state(&lh, |s| *s == TorrentState::Seeding, T).await);
    l.shutdown().await;
    sb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_seeder_causes_hash_failure_then_recovery() {
    let piece_len = 65_536u64;
    let (m, payload) = make_torrent(&[("a", 300_000), ("b", 200_000)], piece_len as u32);
    let bad = Arc::new(CorruptStorage::new(
        payload.clone(),
        2 * piece_len..3 * piece_len,
    ));
    let (sa, _ha) = seeder(&m, bad.clone()).await;
    // The check passed on good data; from now on piece 2 reads corrupt.
    bad.armed.store(true, Ordering::SeqCst);
    let (l, lh, storage) = mem_leecher(&m, payload.len()).await;
    let mut ev = l.subscribe(EventMask::ALL);
    // Only the corrupt seeder at first, so piece 2 must come from it.
    lh.add_peer(sa.local_addr()).await.unwrap();
    next_matching(
        &mut ev,
        |e| matches!(e, Event::HashFailed { piece: 2, .. }),
        T,
    )
    .await
    .expect("HashFailed for piece 2");
    let (sb, _hb) = seeder(&m, Arc::new(MemStorage::from_bytes(payload.clone()))).await;
    lh.add_peer(sb.local_addr()).await.unwrap();
    finished(&mut ev).await;
    assert!(stored(&storage, payload.len()) == payload, "data differs");
    assert!(lh.status().total_wasted >= piece_len);
    l.shutdown().await;
    sa.shutdown().await;
    sb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skipped_file_is_not_downloaded() {
    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    let piece_len = 65_536usize;
    // Lengths off piece boundaries, so both ends of "b" share pieces.
    let files = [("a", 400_000), ("b", 700_000), ("c", 300_000)];
    let (m, payload) = make_torrent(&files, piece_len as u32);
    write_payload(seed_dir.path(), &m, &payload);
    let seeder = Session::new(settings()).await.unwrap();
    let sh = seeder
        .add_torrent(AddTorrent::new(m.clone(), seed_dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&sh, |s| *s == TorrentState::Seeding, T).await);

    let leecher = Session::new(settings()).await.unwrap();
    let mut ev = leecher.subscribe(EventMask::ALL);
    let mut p = AddTorrent::new(m.clone(), leech_dir.path().to_path_buf());
    p.file_priorities = vec![Priority::Normal, Priority::Skip, Priority::Normal];
    let lh = leecher.add_torrent(p).await.unwrap();
    assert!(wait_state(&lh, |s| *s == TorrentState::Downloading, T).await);
    lh.add_peer(seeder.local_addr()).await.unwrap();
    finished(&mut ev).await;
    assert!(wait_state(&lh, |s| *s == TorrentState::Seeding, T).await);

    let dir = leech_dir.path().join("torrent");
    assert!(std::fs::read(dir.join("a")).unwrap() == payload[..400_000]);
    assert!(std::fs::read(dir.join("c")).unwrap() == payload[1_100_000..]);
    // Pieces lying wholly inside "b" were never fetched: wherever the file
    // exists (a boundary piece may have created it), those bytes are zero.
    let (b_start, b_end) = (400_000usize, 1_100_000usize);
    let first = b_start.div_ceil(piece_len);
    let last = b_end / piece_len; // exclusive
    assert!(last > first + 5, "the test needs interior pieces");
    let have = lh.pieces().await.unwrap();
    let b = std::fs::read(dir.join("b")).unwrap_or_default();
    for piece in first..last {
        assert!(!have.get(piece as u32), "interior piece {piece} downloaded");
        let lo = piece * piece_len - b_start;
        let hi = (lo + piece_len).min(b.len());
        if lo < hi {
            assert!(
                b[lo..hi].iter().all(|&x| x == 0),
                "interior piece {piece} written"
            );
        }
    }
    let st = lh.status();
    assert_eq!(st.progress, 1.0);
    assert!(st.total_downloaded < payload.len() as u64);
    leecher.shutdown().await;
    seeder.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn self_connection_is_dropped_and_not_retried() {
    let (m, payload) = make_torrent(&[("a", 100_000)], 16_384);
    // A short retry delay: were the self-connection treated as an ordinary
    // failure, it would be dialled again many times within the window below.
    let (s, h, _) = mem_leecher_with(retry_settings(), &m, payload.len()).await;
    let mut ev = s.subscribe(EventMask::ALL);
    h.add_peer(s.local_addr()).await.unwrap();
    let first = next_matching(&mut ev, |e| matches!(e, Event::PeerDisconnected { .. }), T)
        .await
        .expect("PeerDisconnected");
    assert!(
        matches!(
            &first,
            Event::PeerDisconnected {
                reason: DisconnectReason::SelfConnection,
                ..
            }
        ),
        "got {first:?}"
    );
    // Asking again does nothing, and no retry follows over 15 retry delays.
    h.add_peer(s.local_addr()).await.unwrap();
    let again = next_matching(
        &mut ev,
        |e| {
            matches!(
                e,
                Event::PeerDisconnected { .. } | Event::PeerConnected { .. }
            )
        },
        Duration::from_secs(3),
    )
    .await;
    assert!(again.is_none(), "unexpected {again:?}");
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeder_rejects_invalid_requests() {
    let piece_len = 32_768u32;
    let (m, payload) = make_torrent(&[("a", 100_000)], piece_len);
    let (s, _h) = seeder(&m, Arc::new(MemStorage::from_bytes(payload.clone()))).await;
    let mut peer = ScriptedPeer::connect_fast(s.local_addr(), m.info_hashes.wire_hash()).await;
    peer.send(Message::HaveNone).await;
    peer.send(Message::Interested).await;
    peer.next_matching(|m| *m == Message::Unchoke)
        .await
        .expect("Unchoke");

    let out_of_range = BlockRef {
        piece: 1000,
        offset: 0,
        len: 16_384,
    };
    let too_big = BlockRef {
        piece: 0,
        offset: 0,
        len: 32_768,
    };
    let valid = BlockRef {
        piece: 1,
        offset: 16_384,
        len: 16_384,
    };
    for b in [out_of_range, too_big, valid] {
        peer.send(Message::Request(b)).await;
    }
    let mut rejected = Vec::new();
    let data = loop {
        match peer
            .next_matching(|m| matches!(m, Message::RejectRequest(_) | Message::Piece { .. }))
            .await
        {
            Some(Message::RejectRequest(b)) => rejected.push(b),
            Some(Message::Piece {
                piece,
                offset,
                data,
            }) => {
                assert_eq!((piece, offset), (valid.piece, valid.offset));
                break data;
            }
            other => panic!("connection ended or timed out: {other:?}"),
        }
    };
    let start = (piece_len + 16_384) as usize;
    assert!(data[..] == payload[start..start + 16_384]);
    // Both invalid requests are refused (an out-of-range index may also be
    // dropped by the peer task before reaching the torrent).
    assert!(rejected.contains(&too_big), "rejected {rejected:?}");
    // The connection is still up: a second valid request is served.
    peer.send(Message::Request(BlockRef {
        piece: 0,
        offset: 0,
        len: 16_384,
    }))
    .await;
    let again = peer
        .next_matching(|m| matches!(m, Message::Piece { .. }))
        .await;
    assert!(again.is_some(), "connection stays up");
    s.shutdown().await;
}

/// One upload slot and two interested peers: the choker gives the slot to
/// one of them and rotates it to the other within a few rechokes, sending
/// `Choke`/`Unchoke` only on change.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rechoke_rotates_a_single_slot() {
    let (m, payload) = make_torrent(&[("a", 100_000)], 32_768);
    let mut st = settings();
    st.unchoke_slots = 1;
    let s = Session::new(st).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(payload)));
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    for id in 0..2usize {
        let mut peer = ScriptedPeer::connect(s.local_addr(), m.info_hashes.wire_hash()).await;
        peer.send(Message::Interested).await;
        let tx = tx.clone();
        tokio::spawn(async move {
            use futures_util::StreamExt;
            while let Some(Ok(msg)) = peer.framed.next().await {
                if matches!(msg, Message::Choke | Message::Unchoke) && tx.send((id, msg)).is_err() {
                    return;
                }
            }
        });
    }
    // Replays the choke messages: no repeated message, both peers get the
    // slot at some point, and never two unchoked for longer than it takes
    // the `Choke` of one rechoke to arrive (the two connections are not
    // ordered with each other).
    let mut unchoked = [false; 2];
    let mut ever = [false; 2];
    let deadline = tokio::time::Instant::now() + T;
    while !(ever[0] && ever[1]) || (unchoked[0] && unchoked[1]) {
        let overlap = unchoked[0] && unchoked[1];
        let wait = if overlap {
            tokio::time::Instant::now() + Duration::from_secs(1)
        } else {
            deadline
        };
        let (id, msg) = tokio::time::timeout_at(wait, rx.recv())
            .await
            .expect("choke messages in time")
            .expect("peers alive");
        let on = msg == Message::Unchoke;
        assert!(!(overlap && on), "two peers in one slot");
        assert_ne!(unchoked[id], on, "repeated {msg:?} to peer {id}");
        unchoked[id] = on;
        ever[id] |= on;
    }
    s.shutdown().await;
}

fn retry_settings() -> Settings {
    let mut s = settings();
    s.peer_retry_delay = Duration::from_millis(200);
    s
}

/// A failed outgoing connection is dialled exactly three times in all, the
/// retries after `peer_retry_delay`, and then given up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_connect_is_retried_three_times_then_dropped() {
    // A loopback port nobody listens on.
    let closed = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    let (m, payload) = make_torrent(&[("a", 100_000)], 16_384);
    let (s, h, _) = mem_leecher_with(retry_settings(), &m, payload.len()).await;
    let mut ev = s.subscribe(EventMask::ALL);
    h.add_peer(closed).await.unwrap();
    let ours = |e: &Event| {
        matches!(e, Event::PeerDisconnected { addr, .. } | Event::PeerConnected { addr, .. }
            if *addr == closed)
    };
    for attempt in 1..=3 {
        let e = next_matching(&mut ev, ours, T)
            .await
            .unwrap_or_else(|| panic!("attempt {attempt} reported"));
        assert!(
            matches!(e, Event::PeerDisconnected { .. }),
            "attempt {attempt}: {e:?}"
        );
    }
    // Given up: nothing more over five retry delays.
    let more = next_matching(&mut ev, ours, Duration::from_millis(1500)).await;
    assert!(more.is_none(), "a fourth attempt: {more:?}");
    s.shutdown().await;
}

/// A fast-extension peer flooding `AllowedFast` while choking us: the actor
/// stays responsive, requests nothing the peer lacks, and requests an
/// allowed-fast piece as soon as the peer announces it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn allowed_fast_flood_keeps_the_actor_responsive() {
    let (m, payload) = make_torrent(&[("a", 64 * 16_384)], 16_384);
    let (s, h, _) = mem_leecher(&m, payload.len()).await;
    let mut peer = ScriptedPeer::connect_fast(s.local_addr(), m.info_hashes.wire_hash()).await;
    peer.send(Message::HaveNone).await;
    for i in 0..10_000u32 {
        peer.send(Message::AllowedFast(i % 64)).await;
    }
    let t0 = tokio::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(2), h.pieces())
        .await
        .expect("actor answers during the flood")
        .unwrap();
    assert!(t0.elapsed() < Duration::from_secs(2));
    // Still choked, the peer gets a request for the allowed-fast piece it
    // just announced, and for nothing else.
    peer.send(Message::Have(3)).await;
    let Some(Message::Request(b)) = peer
        .next_matching(|m| matches!(m, Message::Request(_)))
        .await
    else {
        panic!("no request for the allowed-fast piece");
    };
    assert_eq!(b.piece, 3);
    s.shutdown().await;
}

/// A piece that finishes after an outgoing connection was started but
/// before its handshake completed is announced to that peer with `Have`
/// (its initial bitfield predates the piece).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn piece_finished_while_a_peer_connects_is_announced() {
    use fbt_wire::{Handshake, MessageCodec, Reserved, read_handshake, write_handshake};
    use futures_util::StreamExt;

    let (m, payload) = make_torrent(&[("a", 4 * 16_384)], 16_384);
    let ih = m.info_hashes.wire_hash();
    let (s, h, _) = mem_leecher(&m, payload.len()).await;
    let mut ev = s.subscribe(EventMask::PIECE);

    // A remote that accepts our connection but holds back its handshake.
    let slow = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    h.add_peer(slow.local_addr().unwrap()).await.unwrap();
    let (mut stream, _) = tokio::time::timeout(T, slow.accept())
        .await
        .expect("dialled in time")
        .unwrap();
    read_handshake(&mut stream).await.unwrap();

    // Meanwhile another peer delivers piece 0.
    let mut seed = ScriptedPeer::connect(s.local_addr(), ih).await;
    seed.send(Message::Bitfield(bytes::Bytes::from_static(&[0x80])))
        .await;
    seed.send(Message::Unchoke).await;
    let data = payload.clone();
    tokio::spawn(async move { seed.serve(&data, 16_384).await });
    next_matching(
        &mut ev,
        |e| matches!(e, Event::PieceFinished { piece: 0, .. }),
        T,
    )
    .await
    .expect("piece 0 finished");

    // Now the slow remote answers: it learns about piece 0.
    let theirs = Handshake {
        reserved: Reserved::default(),
        info_hash: ih,
        peer_id: fbt_core::PeerId([0x77; 20]),
    };
    write_handshake(&mut stream, &theirs).await.unwrap();
    let mut wire = tokio_util::codec::Framed::new(stream, MessageCodec::new(1 << 20));
    let got_have = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(Ok(msg)) = wire.next().await {
            match msg {
                Message::Have(0) => return true,
                Message::Bitfield(b) if b[0] & 0x80 != 0 => return true,
                _ => {}
            }
        }
        false
    })
    .await
    .unwrap_or(false);
    assert!(got_have, "piece 0 never announced to the slow peer");
    s.shutdown().await;
}

/// A second connection under a peer id that is already connected is closed
/// with `Duplicate`; the first one keeps working.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_peer_id_is_rejected() {
    let (m, payload) = make_torrent(&[("a", 100_000)], 32_768);
    let (s, _h) = seeder(&m, Arc::new(MemStorage::from_bytes(payload))).await;
    let ih = m.info_hashes.wire_hash();
    let id = fbt_core::PeerId([0x42; 20]);
    let mut ev = s.subscribe(EventMask::PEER);
    let mut first = ScriptedPeer::connect_as(s.local_addr(), ih, id, false).await;
    next_matching(&mut ev, |e| matches!(e, Event::PeerConnected { .. }), T)
        .await
        .expect("first connected");
    let mut second = ScriptedPeer::connect_as(s.local_addr(), ih, id, false).await;
    assert_eq!(second.wait_closed(T).await, PeerEnd::Closed);
    let e = next_matching(&mut ev, |e| matches!(e, Event::PeerDisconnected { .. }), T)
        .await
        .expect("second disconnected");
    assert!(
        matches!(
            e,
            Event::PeerDisconnected {
                reason: DisconnectReason::Duplicate,
                ..
            }
        ),
        "{e:?}"
    );
    first.send(Message::Interested).await;
    first
        .next_matching(|m| *m == Message::Unchoke)
        .await
        .expect("the first connection still works");
    s.shutdown().await;
}
