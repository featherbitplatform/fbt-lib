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
    AddTorrent, DisconnectReason, EncryptionPolicy, Event, EventMask, Priority, Session, Settings,
    TorrentHandle, TorrentState,
};
use fbt_wire::{
    ExtHandshake, LT_DONTHAVE, Message, MetadataMsg, OUR_LT_DONTHAVE_ID, OUR_UT_METADATA_ID,
    OUR_UT_PEX_ID, Reserved, UT_METADATA, UT_PEX, decode_donthave, encode_donthave,
};
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
/// retries after (a twelfth of, then all of) `peer_retry_delay`, and then
/// given up. Over TCP alone: nothing listens there at all, and the uTP
/// detour of each attempt is `utp_connect_failure_falls_back_to_tcp`'s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_connect_is_retried_three_times_then_dropped() {
    // A loopback port nobody listens on.
    let closed = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    let (m, payload) = make_torrent(&[("a", 100_000)], 16_384);
    let (s, h, _) = mem_leecher_with(dial_tcp(retry_settings()), &m, payload.len()).await;
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
    // The hand-driven remote below speaks plaintext, over TCP only.
    let mut st = dial_tcp(settings());
    st.encryption = EncryptionPolicy::Disabled;
    let (s, h, _) = mem_leecher_with(st, &m, payload.len()).await;
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

/// A second connection under a peer id that is already connected is
/// closed (the older one stays, same direction) without any event: the
/// peer was reported connected once and still is. The first connection
/// keeps working and alone reports its end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_peer_id_is_rejected() {
    let (m, payload) = make_torrent(&[("a", 100_000)], 32_768);
    let (s, _h) = seeder(&m, Arc::new(MemStorage::from_bytes(payload))).await;
    let ih = m.info_hashes.wire_hash();
    let id = fbt_core::PeerId([0x42; 20]);
    let mut ev = s.subscribe(EventMask::PEER);
    let mut first = ScriptedPeer::connect_as(s.local_addr(), ih, id, false).await;
    let first_addr = match next_matching(&mut ev, |e| matches!(e, Event::PeerConnected { .. }), T)
        .await
        .expect("first connected")
    {
        Event::PeerConnected { addr, .. } => addr,
        _ => unreachable!(),
    };
    let mut second = ScriptedPeer::connect_as(s.local_addr(), ih, id, false).await;
    assert_eq!(second.wait_closed(T).await, PeerEnd::Closed);
    let e = next_matching(
        &mut ev,
        |e| {
            matches!(
                e,
                Event::PeerConnected { .. } | Event::PeerDisconnected { .. }
            )
        },
        Duration::from_secs(1),
    )
    .await;
    assert!(e.is_none(), "the duplicate was reported: {e:?}");
    first.send(Message::Interested).await;
    first
        .next_matching(|m| *m == Message::Unchoke)
        .await
        .expect("the first connection still works");
    drop(first);
    let e = next_matching(&mut ev, |e| matches!(e, Event::PeerDisconnected { .. }), T)
        .await
        .expect("the first connection reports its end");
    assert!(
        matches!(e, Event::PeerDisconnected { addr, .. } if addr == first_addr),
        "{e:?}"
    );
    s.shutdown().await;
}

/// Two sessions dialling each other at once end up with exactly one
/// connection: both ends keep the one the lower peer id initiated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simultaneous_dial_leaves_one_connection() {
    let (m, payload) = make_torrent(&[("a", 1 << 20)], 65_536);
    // Slow reads keep the download going past the check: once the leecher
    // is a seed too, the two close (BEP 21).
    let slow = Arc::new(SlowStorage::new(
        payload.clone(),
        Duration::from_millis(500),
    ));
    let (a, ha) = seeder(&m, slow).await;
    let (b, hb, storage) = mem_leecher(&m, payload.len()).await;
    let mut ev = b.subscribe(EventMask::ALL);
    let (ra, rb) = tokio::join!(ha.add_peer(b.local_addr()), hb.add_peer(a.local_addr()));
    ra.unwrap();
    rb.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(ha.status().num_peers, 1, "seeder peers");
    assert_eq!(hb.status().num_peers, 1, "leecher peers");
    finished(&mut ev).await;
    assert!(stored(&storage, payload.len()) == payload, "data differs");
    b.shutdown().await;
    a.shutdown().await;
}

/// Two upload slots, three interested peers and a leecher: peer A uploads
/// to us fastest and keeps its slot, while the optimistic slot rotates, so a
/// peer that was unchoked gets a `Choke`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rechoke_chokes_peer_that_lost_its_slot() {
    use futures_util::{SinkExt, StreamExt};
    let piece_len = 32_768u32;
    let (m, payload) = make_torrent(&[("a", 2 << 20)], piece_len);
    let mut st = settings();
    st.unchoke_slots = 2;
    st.rechoke_interval = Duration::from_millis(200);
    let (s, _h, _storage) = mem_leecher_with(st, &m, payload.len()).await;
    let pieces = fbt_core::Bitfield::full(m.info.pieces.len() as u32);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    for id in 0..3usize {
        let mut peer = ScriptedPeer::connect(s.local_addr(), m.info_hashes.wire_hash()).await;
        let fastest = id == 0;
        if fastest {
            peer.send(Message::Bitfield(bytes::Bytes::copy_from_slice(
                pieces.as_bytes(),
            )))
            .await;
            peer.send(Message::Unchoke).await;
        }
        peer.send(Message::Interested).await;
        let tx = tx.clone();
        let payload = payload.clone();
        tokio::spawn(async move {
            while let Some(Ok(msg)) = peer.framed.next().await {
                match msg {
                    Message::Choke | Message::Unchoke => {
                        if tx.send((id, msg)).is_err() {
                            return;
                        }
                    }
                    // A throttled uploader: about 1.6 MB/s.
                    Message::Request(b) if fastest => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        let at = b.piece as usize * piece_len as usize + b.offset as usize;
                        let data = bytes::Bytes::copy_from_slice(&payload[at..at + b.len as usize]);
                        let piece = Message::Piece {
                            piece: b.piece,
                            offset: b.offset,
                            data,
                        };
                        if peer.framed.send(piece).await.is_err() {
                            return;
                        }
                    }
                    _ => {}
                }
            }
        });
    }
    let mut unchoked = [false; 3];
    let deadline = tokio::time::Instant::now() + T;
    loop {
        let (id, msg) = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .expect("a Choke after an Unchoke in time")
            .expect("peers alive");
        if msg == Message::Choke {
            assert_ne!(id, 0, "the fastest uploader keeps its slot");
            if unchoked[id] {
                break;
            }
        } else {
            unchoked[id] = true;
        }
    }
    s.shutdown().await;
}

/// A round-robin seeder with two upload slots (one regular, one
/// optimistic) and six leechers that only know the seeder. Its upload is
/// throttled so that a turn (three rechokes) is far shorter than the
/// torrent: the regular slot moves on, so at least four leechers have
/// data before the first one finishes (without rotation only the two
/// holders would), and every leecher finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seeding_round_robin_serves_every_leecher() {
    let (m, payload) = make_torrent(&[("a", 384 << 10)], 32_768);
    let mut st = settings();
    st.unchoke_slots = 2;
    st.rechoke_interval = Duration::from_millis(200);
    st.seeding_piece_quota = 4;
    st.pex_enabled = false;
    st.rate_limit = fbt_session::RateLimit::new(0, 256 << 10);
    st.rate_limit_local_peers = true;
    let s = Session::new(st).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(payload.clone())));
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);

    let mut leechers = Vec::new();
    for _ in 0..6 {
        let mut ls = settings();
        ls.pex_enabled = false;
        leechers.push(mem_leecher_with(ls, &m, payload.len()).await);
    }
    for (_, lh, _) in &leechers {
        lh.add_peer(s.local_addr()).await.unwrap();
    }
    let deadline = tokio::time::Instant::now() + T;
    let served = loop {
        let statuses: Vec<_> = leechers.iter().map(|(_, lh, _)| lh.status()).collect();
        if statuses.iter().any(|st| st.state == TorrentState::Seeding) {
            break statuses.iter().filter(|st| st.wanted_done > 0).count();
        }
        assert!(tokio::time::Instant::now() < deadline, "nobody finished");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(
        served >= 4,
        "only {served} leechers served before the first finished"
    );
    for (_, lh, storage) in &leechers {
        assert!(wait_state(lh, |s| *s == TorrentState::Seeding, T).await);
        assert!(stored(storage, payload.len()) == payload, "data differs");
    }
    for (l, ..) in leechers {
        l.shutdown().await;
    }
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_while_choked_gets_reject_with_fast() {
    let (m, payload) = make_torrent(&[("a", 100_000)], 32_768);
    let (s, _h) = seeder(&m, Arc::new(MemStorage::from_bytes(payload))).await;
    let mut peer = ScriptedPeer::connect_fast(s.local_addr(), m.info_hashes.wire_hash()).await;
    peer.send(Message::HaveNone).await;
    // Not interested, so still choked.
    let block = BlockRef {
        piece: 0,
        offset: 0,
        len: 16_384,
    };
    peer.send(Message::Request(block)).await;
    let got = peer
        .next_matching(|m| matches!(m, Message::RejectRequest(_) | Message::Piece { .. }))
        .await;
    assert_eq!(got, Some(Message::RejectRequest(block)));
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_while_choked_without_fast_is_dropped() {
    let (m, payload) = make_torrent(&[("a", 100_000)], 32_768);
    let (s, _h) = seeder(&m, Arc::new(MemStorage::from_bytes(payload))).await;
    let mut peer = ScriptedPeer::connect(s.local_addr(), m.info_hashes.wire_hash()).await;
    peer.send(Message::Request(BlockRef {
        piece: 0,
        offset: 0,
        len: 16_384,
    }))
    .await;
    let mut piece = false;
    let end = tokio::time::timeout(Duration::from_secs(1), async {
        use futures_util::StreamExt;
        while let Some(Ok(m)) = peer.framed.next().await {
            piece |= matches!(m, Message::Piece { .. });
        }
    })
    .await;
    assert!(!piece, "no Piece while choked");
    assert!(end.is_err(), "the connection stays up");
    s.shutdown().await;
}

/// The one upload slot rotates away from a peer with queued requests: its
/// `Choke` is followed by a `RejectRequest` for each request that was not
/// served yet (fast extension), and none of those is also served.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn choke_rejects_queued_requests_with_fast() {
    use futures_util::StreamExt;
    use std::collections::HashSet;
    let (m, payload) = make_torrent(&[("a", 100_000)], 32_768);
    let mut st = settings();
    st.unchoke_slots = 1;
    st.rechoke_interval = Duration::from_millis(100);
    let s = Session::new(st).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(SlowStorage::new(
        payload,
        Duration::from_millis(400),
    )));
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);

    let blocks: Vec<BlockRef> = (0..6)
        .map(|i| BlockRef {
            piece: 0,
            offset: i * 2_000,
            len: 2_000,
        })
        .collect();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    for id in 0..2usize {
        let mut peer = ScriptedPeer::connect_fast(s.local_addr(), m.info_hashes.wire_hash()).await;
        peer.send(Message::HaveNone).await;
        peer.send(Message::Interested).await;
        let tx = tx.clone();
        let blocks = blocks.clone();
        tokio::spawn(async move {
            let mut asked = false;
            while let Some(Ok(msg)) = peer.framed.next().await {
                let first_unchoke = msg == Message::Unchoke && !asked;
                if tx.send((id, msg)).is_err() {
                    return;
                }
                if first_unchoke {
                    asked = true;
                    for b in &blocks {
                        peer.send(Message::Request(*b)).await;
                    }
                }
            }
        });
    }
    let mut unchoked = [false; 2];
    let mut served: [HashSet<BlockRef>; 2] = Default::default();
    let mut rejected: [HashSet<BlockRef>; 2] = Default::default();
    let mut choked_at = None;
    let mut choked = None;
    let deadline = tokio::time::Instant::now() + T;
    loop {
        let wait = match choked_at {
            Some(t) => t,
            None => deadline,
        };
        let Ok(Some((id, msg))) = tokio::time::timeout_at(wait, rx.recv()).await else {
            break;
        };
        match msg {
            Message::Unchoke => unchoked[id] = true,
            Message::Choke if unchoked[id] && choked_at.is_none() => {
                // The rejects follow in the same flush; give them a moment.
                choked_at = Some(tokio::time::Instant::now() + Duration::from_millis(300));
                unchoked = [false; 2];
                unchoked[id] = true;
                choked = Some(id);
            }
            Message::RejectRequest(b) => {
                rejected[id].insert(b);
            }
            Message::Piece { offset, data, .. } if choked.is_none() => {
                served[id].insert(BlockRef {
                    piece: 0,
                    offset,
                    len: data.len() as u32,
                });
            }
            _ => {}
        }
    }
    assert!(choked_at.is_some(), "a peer got choked");
    let id = choked.expect("the choked peer");
    assert!(!rejected[id].is_empty(), "queued requests rejected");
    assert!(rejected[id].iter().all(|b| blocks.contains(b)));
    assert!(rejected[id].is_disjoint(&served[id]));
    s.shutdown().await;
}

/// A request for a piece we lack, from a peer we do unchoke, is refused on
/// the wire (the `Reject` command reaches the peer).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_for_missing_piece_is_rejected_on_the_wire() {
    let (m, payload) = make_torrent(&[("a", 100_000)], 32_768);
    let (s, _h, _storage) = mem_leecher(&m, payload.len()).await;
    let mut peer = ScriptedPeer::connect_fast(s.local_addr(), m.info_hashes.wire_hash()).await;
    peer.send(Message::HaveNone).await;
    peer.send(Message::Interested).await;
    peer.next_matching(|m| *m == Message::Unchoke)
        .await
        .expect("Unchoke");
    let block = BlockRef {
        piece: 1,
        offset: 0,
        len: 16_384,
    };
    peer.send(Message::Request(block)).await;
    let got = peer
        .next_matching(|m| matches!(m, Message::RejectRequest(_) | Message::Piece { .. }))
        .await;
    assert_eq!(got, Some(Message::RejectRequest(block)));
    s.shutdown().await;
}

/// A BEP 10 peer gets our extension handshake (listen port, metadata size,
/// `ut_metadata` and `ut_pex` for a public torrent) before our pieces, and
/// its own handshake and extended messages leave the connection up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extension_handshake_with_a_session() {
    let (m, payload) = make_torrent(&[("a", 100_000)], 32_768);
    let (s, _h) = seeder(&m, Arc::new(MemStorage::from_bytes(payload))).await;
    let mut reserved = Reserved::default();
    reserved.set_fast();
    reserved.set_extended();
    let mut peer =
        ScriptedPeer::connect_with(s.local_addr(), m.info_hashes.wire_hash(), reserved).await;
    assert!(peer.remote.reserved.supports_extended());
    assert!(!peer.remote.reserved.supports_dht());
    let first = peer.next_matching(|_| true).await.expect("first message");
    let Message::Extended { id: 0, payload } = first else {
        panic!("expected the extension handshake first, got {first:?}");
    };
    let hs = ExtHandshake::decode(&payload).expect("decode");
    assert_eq!(hs.id_of(UT_METADATA), Some(OUR_UT_METADATA_ID));
    assert_eq!(hs.id_of(UT_PEX), Some(OUR_UT_PEX_ID));
    assert_eq!(hs.listen_port, Some(s.local_addr().port()));
    assert_eq!(hs.metadata_size, Some(m.raw_info().len() as u32));
    assert!(matches!(
        peer.next_matching(|_| true).await,
        Some(Message::HaveAll)
    ));

    let mut theirs = ExtHandshake {
        listen_port: Some(7000),
        ..ExtHandshake::default()
    };
    theirs.m.insert(UT_METADATA.to_owned(), 3);
    peer.send_ext_handshake(&theirs).await;
    // A metadata request and a `Port` leave the connection up.
    peer.send(Message::Extended {
        id: OUR_UT_METADATA_ID,
        payload: MetadataMsg::Request { piece: 0 }.encode(),
    })
    .await;
    peer.send(Message::Port(7001)).await;
    peer.send(Message::Interested).await;
    assert!(
        peer.next_matching(|m| *m == Message::Unchoke)
            .await
            .is_some(),
        "the seeder unchokes the interested peer"
    );
    s.shutdown().await;
}

/// BEP 21: two seeds of one torrent have nothing to trade; the one that
/// learns the other is a seed while it is seeding itself closes with
/// `UploadOnly`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seeds_disconnect_from_upload_only_peers() {
    let (m, payload) = make_torrent(&[("a", 100_000)], 32_768);
    let (sa, _ha) = seeder(&m, Arc::new(MemStorage::from_bytes(payload.clone()))).await;
    let (sb, hb) = seeder(&m, Arc::new(MemStorage::from_bytes(payload))).await;
    let mut ea = sa.subscribe(EventMask::ALL);
    let mut eb = sb.subscribe(EventMask::ALL);
    hb.add_peer(sa.local_addr()).await.unwrap();
    let within = Duration::from_secs(2);
    let a = next_matching(
        &mut ea,
        |e| matches!(e, Event::PeerDisconnected { .. }),
        within,
    )
    .await;
    let b = next_matching(
        &mut eb,
        |e| matches!(e, Event::PeerDisconnected { .. }),
        within,
    )
    .await;
    let reasons: Vec<DisconnectReason> = [a, b]
        .into_iter()
        .map(|e| match e {
            Some(Event::PeerDisconnected { reason, .. }) => reason,
            other => panic!("no disconnect within 2 s: {other:?}"),
        })
        .collect();
    assert!(
        reasons.contains(&DisconnectReason::UploadOnly),
        "reasons {reasons:?}"
    );
    sa.shutdown().await;
    sb.shutdown().await;
}

/// BEP 54: a remote that sends `HaveAll` and then `lt_donthave` for piece 3
/// does not have it any more, so it is never requested from that peer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn donthave_from_remote_lowers_availability() {
    let (m, payload) = make_torrent(&[("a", 6 * 16_384)], 16_384);
    let (s, _h, _storage) = mem_leecher(&m, payload.len()).await;
    let mut reserved = Reserved::default();
    reserved.set_fast();
    reserved.set_extended();
    let mut peer =
        ScriptedPeer::connect_with(s.local_addr(), m.info_hashes.wire_hash(), reserved).await;
    peer.send(Message::HaveAll).await;
    peer.send(Message::Extended {
        id: OUR_LT_DONTHAVE_ID,
        payload: encode_donthave(3),
    })
    .await;
    peer.send(Message::Unchoke).await;
    // The pipeline deepens as blocks arrive, so every request is answered.
    let mut requested = std::collections::BTreeSet::new();
    while requested.len() < 5 {
        match peer
            .next_matching(|m| matches!(m, Message::Request(_)))
            .await
        {
            Some(Message::Request(b)) => {
                requested.insert(b.piece);
                let start = b.piece as usize * 16_384;
                peer.send(Message::Piece {
                    piece: b.piece,
                    offset: b.offset,
                    data: payload[start..start + 16_384].to_vec().into(),
                })
                .await;
            }
            other => panic!("expected requests, got {other:?} after {requested:?}"),
        }
    }
    // Anything still to come would be piece 3.
    let extra = tokio::time::timeout(
        Duration::from_millis(500),
        peer.next_matching(|m| matches!(m, Message::Request(_))),
    )
    .await;
    if let Ok(Some(Message::Request(b))) = extra {
        requested.insert(b.piece);
    }
    assert_eq!(
        requested.into_iter().collect::<Vec<_>>(),
        vec![0, 1, 2, 4, 5]
    );
    s.shutdown().await;
}

/// BEP 54: a piece that turns out to be gone from disk while serving is
/// announced lost (`lt_donthave` to every peer, `FileError`, the request
/// rejected), downloaded again from another seed, and the leecher that
/// asked finishes from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_piece_sends_donthave_and_is_redownloaded() {
    let piece_len = 65_536u32;
    let (m, payload) = make_torrent(&[("a", 2 * 65_536), ("b", 2 * 65_536)], piece_len);
    let (sa, _ha) = seeder(&m, Arc::new(MemStorage::from_bytes(payload.clone()))).await;

    // B holds file `a` only (pieces 0 and 1), with no clean cache, so a
    // read always reaches the disk.
    let dir = tempfile::tempdir().unwrap();
    write_payload(dir.path(), &m, &payload);
    std::fs::remove_file(dir.path().join("torrent").join("b")).unwrap();
    let mut st = settings();
    st.cache_size = 0;
    let sb = Session::new(st).await.unwrap();
    let mut eb = sb.subscribe(EventMask::ALL);
    let hb = sb
        .add_torrent(AddTorrent::new(m.clone(), dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&hb, |s| *s == TorrentState::Downloading, T).await);
    assert!(wait_until(|| hb.status().wanted_done == 2 * 65_536, T).await);

    // The observer advertises `lt_donthave` and sits there.
    let mut reserved = Reserved::default();
    reserved.set_fast();
    reserved.set_extended();
    let mut observer =
        ScriptedPeer::connect_with(sb.local_addr(), m.info_hashes.wire_hash(), reserved).await;
    let mut hs = ExtHandshake::default();
    hs.m.insert(LT_DONTHAVE.to_owned(), 9);
    observer.send_ext_handshake(&hs).await;
    observer.send(Message::HaveNone).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // `a` is emptied under B. (FsStorage keeps its read handle, so the file
    // is truncated, not removed: the read then fails with NotFound on every
    // platform.)
    let a = dir.path().join("torrent").join("a");
    std::fs::OpenOptions::new()
        .write(true)
        .open(&a)
        .unwrap()
        .set_len(0)
        .unwrap();

    let (sc, hc, storage_c) = mem_leecher(&m, payload.len()).await;
    let mut ec = sc.subscribe(EventMask::ALL);
    hc.add_peer(sb.local_addr()).await.unwrap();

    let note = observer.next_ext(9).await.expect("lt_donthave");
    let lost = decode_donthave(&note).unwrap();
    assert!(lost < 2, "piece {lost}");
    let ev = next_matching(&mut eb, |e| matches!(e, Event::FileError { .. }), T)
        .await
        .expect("FileError");
    let Event::FileError { path, .. } = ev else {
        unreachable!()
    };
    assert_eq!(path.as_deref(), Some(a.as_path()));
    // The torrent keeps running and gets the data again.
    assert!(hb.status().error.is_none());
    hb.add_peer(sa.local_addr()).await.unwrap();
    assert!(wait_state(&hb, |s| *s == TorrentState::Seeding, T).await);
    assert!(std::fs::read(&a).unwrap() == payload[..2 * 65_536]);

    finished(&mut ec).await;
    assert!(stored(&storage_c, payload.len()) == payload, "data differs");
    sc.shutdown().await;
    sb.shutdown().await;
    sa.shutdown().await;
}

/// BEP 21 and un-finishing: a torrent that is seeding (its other file
/// skipped) closes a seed it knows; once the skipped file is wanted, the
/// seed is dialled again without another `add_peer`, and the file arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seeds_are_redialled_when_the_torrent_is_unfinished() {
    let (m, payload) = make_torrent(&[("a", 2 * 65_536), ("b", 2 * 65_536)], 65_536);
    let (sx, _hx) = seeder(&m, Arc::new(MemStorage::from_bytes(payload.clone()))).await;
    let dir = tempfile::tempdir().unwrap();
    write_payload(dir.path(), &m, &payload);
    std::fs::remove_file(dir.path().join("torrent").join("b")).unwrap();
    let sy = Session::new(settings()).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), dir.path().to_path_buf());
    p.file_priorities = vec![Priority::Normal, Priority::Skip];
    let hy = sy.add_torrent(p).await.unwrap();
    assert!(wait_state(&hy, |s| *s == TorrentState::Seeding, T).await);
    let mut ey = sy.subscribe(EventMask::ALL);
    hy.add_peer(sx.local_addr()).await.unwrap();
    let ev = next_matching(
        &mut ey,
        |e| matches!(e, Event::PeerDisconnected { .. }),
        Duration::from_secs(5),
    )
    .await
    .expect("the seeds part");
    // Both sides are seeds, so either may close first: we see UploadOnly
    // when we close, Closed when the remote seed beats us to it.
    assert!(
        matches!(
            ev,
            Event::PeerDisconnected {
                reason: DisconnectReason::UploadOnly | DisconnectReason::Closed,
                ..
            }
        ),
        "{ev:?}"
    );
    assert!(wait_until(|| hy.status().num_peers == 0, T).await);
    // It does not come back while we are finished.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(hy.status().num_peers, 0);

    hy.set_file_priorities(vec![Priority::Normal, Priority::Normal])
        .await
        .unwrap();
    assert!(wait_state(&hy, |s| *s == TorrentState::Seeding, T).await);
    let b = std::fs::read(dir.path().join("torrent").join("b")).unwrap();
    assert!(b == payload[2 * 65_536..]);
    sy.shutdown().await;
    sx.shutdown().await;
}

/// The whole-piece read path (a clean cache with room) reports a lost
/// file once, and every piece of it at once: all of them are announced
/// lost, then downloaded again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn whole_piece_read_loss_reports_the_file_once() {
    let (m, payload) = make_torrent(&[("a", 3 * 65_536), ("b", 65_536)], 65_536);
    let (sa, _ha) = seeder(&m, Arc::new(MemStorage::from_bytes(payload.clone()))).await;
    let dir = tempfile::tempdir().unwrap();
    write_payload(dir.path(), &m, &payload);
    std::fs::remove_file(dir.path().join("torrent").join("b")).unwrap();
    // The default cache size leaves room for a clean whole-piece read.
    let sb = Session::new(settings()).await.unwrap();
    let mut eb = sb.subscribe(EventMask::ALL);
    let hb = sb
        .add_torrent(AddTorrent::new(m.clone(), dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_until(|| hb.status().wanted_done == 3 * 65_536, T).await);
    let mut reserved = Reserved::default();
    reserved.set_fast();
    reserved.set_extended();
    let mut observer =
        ScriptedPeer::connect_with(sb.local_addr(), m.info_hashes.wire_hash(), reserved).await;
    let mut hs = ExtHandshake::default();
    hs.m.insert(LT_DONTHAVE.to_owned(), 9);
    observer.send_ext_handshake(&hs).await;
    observer.send(Message::HaveNone).await;
    observer.send(Message::Interested).await;
    observer
        .next_matching(|m| *m == Message::Unchoke)
        .await
        .expect("Unchoke");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let a = dir.path().join("torrent").join("a");
    std::fs::OpenOptions::new()
        .write(true)
        .open(&a)
        .unwrap()
        .set_len(0)
        .unwrap();
    observer
        .send(Message::Request(BlockRef {
            piece: 1,
            offset: 0,
            len: 16_384,
        }))
        .await;
    // All three pieces of `a` are announced lost from the one request.
    let mut lost = std::collections::BTreeSet::new();
    while lost.len() < 3 {
        let note = observer.next_ext(9).await.expect("lt_donthave");
        lost.insert(decode_donthave(&note).unwrap());
    }
    assert_eq!(lost.into_iter().collect::<Vec<_>>(), vec![0, 1, 2]);
    next_matching(&mut eb, |e| matches!(e, Event::FileError { .. }), T)
        .await
        .expect("FileError");
    // A second request for another lost piece adds no second report.
    observer
        .send(Message::Request(BlockRef {
            piece: 2,
            offset: 0,
            len: 16_384,
        }))
        .await;
    let again = next_matching(
        &mut eb,
        |e| matches!(e, Event::FileError { .. }),
        Duration::from_millis(700),
    )
    .await;
    assert!(again.is_none(), "second FileError {again:?}");
    assert!(hb.status().error.is_none());
    hb.add_peer(sa.local_addr()).await.unwrap();
    assert!(wait_state(&hb, |s| *s == TorrentState::Seeding, T).await);
    assert!(std::fs::read(&a).unwrap() == payload[..3 * 65_536]);
    sb.shutdown().await;
    sa.shutdown().await;
}
