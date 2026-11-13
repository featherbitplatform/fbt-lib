//! Smart ban on loopback: after a failed piece passes, only the peer whose
//! block differed is banned; an honest contributor keeps its connection; a
//! peer whose data never passes is banned by trust points after its fourth
//! failed piece; a banned address is never dialled again.
//!
//! The scripted peers listen and the leech dials them (`ban_endpoints`), so
//! each peer's ban key is its listen address.

mod support;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use fbt_core::BlockRef;
use fbt_metainfo::Metainfo;
use fbt_session::storage::MemStorage;
use fbt_session::{
    AddTorrent, BlockReason, EncryptionPolicy, Event, EventMask, EventStream, Priority, Session,
    Settings, TorrentHandle, TorrentState,
};
use fbt_wire::{Message, Reserved};
use support::*;
use tokio::net::TcpListener;

/// Plaintext, TCP-only dials (scripted peers listen on TCP alone), with
/// bans keyed by endpoint.
fn plain() -> Settings {
    let mut s = dial_tcp(test_settings());
    s.encryption = EncryptionPolicy::Disabled;
    s.ban_endpoints = true;
    s
}

type Log = Arc<Mutex<Vec<BlockRef>>>;

fn banned(e: &Event) -> Option<SocketAddr> {
    match e {
        Event::PeerBlocked {
            addr,
            reason: BlockReason::Banned,
            ..
        } => Some(*addr),
        _ => None,
    }
}

async fn listener() -> (TcpListener, SocketAddr) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    (l, a)
}

/// Piece length of the liar scenario: 8 blocks per piece.
const PIECE: u32 = 131_072;

/// The liar scenario, up to the ban.
struct Liar {
    s: Session,
    h: TorrentHandle,
    m: Metainfo,
    a: SocketAddr,
    b: SocketAddr,
    log_a: Log,
    log_b: Log,
    /// The block A corrupted.
    bad: BlockRef,
    /// Every event seen until A's ban, in order.
    seen: Vec<Event>,
    ev: EventStream,
    _listeners: (TcpListener, TcpListener),
}

/// Five one-piece files; the leech wants only the last two (pieces 3 and 4).
/// A advertises only piece 3 and corrupts its first block once; B, which has
/// pieces 0 to 3, joins after A's first request arrived. Piece 3 fails, is
/// retried from A and B, passes, and A is banned.
///
/// Piece 4 — wanted by us, held by nobody in the swarm — is what keeps the
/// torrent unfinished after piece 3 passes. Without it the leecher seeds the
/// moment the ban happens, and `close_if_upload_only` drops B as an
/// upload-only peer (BEP 21; pinned by `swarm.rs`'s
/// `seeds_disconnect_from_upload_only_peers`), which is exactly what
/// `honest_contributor_keeps_its_trust` then raced and called a failure.
async fn liar_scenario() -> Liar {
    let p = PIECE as usize;
    let (m, payload) = make_torrent(&[("a", p), ("b", p), ("c", p), ("d", p), ("e", p)], PIECE);
    let (la, a) = listener().await;
    let (lb, b) = listener().await;
    let s = Session::new(plain()).await.unwrap();
    let mut ev = s.subscribe(EventMask::PEER | EventMask::PIECE | EventMask::STATUS);
    let mut add = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    add.storage = Some(Arc::new(MemStorage::new(payload.len() as u64)));
    // Pieces 0 to 2 are skipped; 3 is the one A lies about and 4 nobody has.
    add.file_priorities = vec![
        Priority::Skip,
        Priority::Skip,
        Priority::Skip,
        Priority::Normal,
        Priority::Normal,
    ];
    let h = s.add_torrent(add).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);

    let log_a: Log = Arc::default();
    let log_b: Log = Arc::default();
    h.add_peer(a).await.unwrap();
    let mut pa = ScriptedPeer::accept_with(&la, Reserved::default()).await;
    // Five pieces in one byte: A has only piece 3.
    pa.send(Message::Bitfield(Bytes::from_static(&[0x10])))
        .await;
    pa.send(Message::Unchoke).await;
    let Some(Message::Request(bad)) = pa.next_matching(|m| matches!(m, Message::Request(_))).await
    else {
        panic!("A gets no request");
    };
    assert_eq!(bad.piece, 3);
    let mut bad_a: HashSet<BlockRef> = HashSet::from([bad]);
    assert!(
        pa.answer_corrupt(bad, &payload, PIECE, &mut bad_a, &log_a)
            .await
    );
    assert!(bad_a.is_empty(), "corrupted once");

    h.add_peer(b).await.unwrap();
    let mut pb = ScriptedPeer::accept_with(&lb, Reserved::default()).await;
    // B has pieces 0 to 3 and not 4: it holds what we want next, and it is not
    // a seed of the whole torrent, which an upload-only us would close.
    pb.send(Message::Bitfield(Bytes::from_static(&[0xf0])))
        .await;
    pb.send(Message::Unchoke).await;
    let Some(Message::Request(first_b)) =
        pb.next_matching(|m| matches!(m, Message::Request(_))).await
    else {
        panic!("B gets no request");
    };
    assert_eq!(first_b.piece, 3, "only piece 3 is wanted");
    let mut none = HashSet::new();
    assert!(
        pb.answer_corrupt(first_b, &payload, PIECE, &mut none, &log_b)
            .await
    );
    let payload = Arc::new(payload);
    {
        let (payload, log) = (Arc::clone(&payload), Arc::clone(&log_a));
        tokio::spawn(async move {
            pa.serve_corrupt(&payload, PIECE, bad_a, log).await;
        });
    }
    {
        let (payload, log) = (Arc::clone(&payload), Arc::clone(&log_b));
        tokio::spawn(async move {
            pb.serve_corrupt(&payload, PIECE, HashSet::new(), log).await;
        });
    }

    let mut seen = Vec::new();
    let mut passed = false;
    let mut a_banned = false;
    let end = tokio::time::Instant::now() + T;
    while !(passed && a_banned) {
        let left = end.saturating_duration_since(tokio::time::Instant::now());
        let e = next_matching(&mut ev, |_| true, left)
            .await
            .expect("piece 3 passes and A is banned");
        passed |= matches!(e, Event::PieceFinished { piece: 3, .. });
        a_banned |= banned(&e) == Some(a);
        seen.push(e);
    }
    Liar {
        s,
        h,
        m,
        a,
        b,
        log_a,
        log_b,
        bad,
        seen,
        ev,
        _listeners: (la, lb),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_the_liar_is_banned() {
    let l = liar_scenario().await;
    let failed = l
        .seen
        .iter()
        .position(|e| matches!(e, Event::HashFailed { piece: 3, .. }))
        .expect("piece 3 failed first");
    let passed = l
        .seen
        .iter()
        .position(|e| matches!(e, Event::PieceFinished { piece: 3, .. }))
        .unwrap();
    assert!(failed < passed);
    // Only A and B were ever connected: the retry came from them.
    for e in &l.seen {
        if let Event::PeerConnected { addr, .. } = e {
            assert!(*addr == l.a || *addr == l.b, "{addr}");
        }
    }
    // The banned set is the set of peers that served a bad block: a block
    // of their own corrupt set (A's is `bad`, B's is empty; B may serve
    // that block too, uncorrupted, in the retry).
    let bad_sets = [(l.a, &l.log_a, vec![l.bad]), (l.b, &l.log_b, Vec::new())];
    let liars: Vec<SocketAddr> = bad_sets
        .iter()
        .filter(|(_, log, bad)| log.lock().unwrap().iter().any(|b| bad.contains(b)))
        .map(|(addr, _, _)| *addr)
        .collect();
    assert_eq!(liars, vec![l.a]);
    assert_eq!(l.s.banned_peers(), liars);
    let named: HashSet<SocketAddr> = l.seen.iter().filter_map(banned).collect();
    assert_eq!(named, HashSet::from([l.a]));
    // Both contributed blocks of piece 3.
    assert!(l.log_a.lock().unwrap().iter().any(|b| b.piece == 3));
    assert!(l.log_b.lock().unwrap().iter().any(|b| b.piece == 3));
    l.s.shutdown().await;
}

/// The peer that served the good blocks of a failed piece keeps its trust and
/// its connection: it is not banned, and once the files only it has are wanted
/// it is asked for blocks again.
///
/// Cause of the 0.7 flake (0.8 deflake; reproduced here at 3 in 100 runs with
/// 36 copies going, 9 in 300 at 72): with four one-piece files the leecher was
/// finished — every *wanted* file complete — the moment piece 3 passed, so
/// between `liar_scenario` returning and the priorities widening the actor had
/// become upload-only and closed B as an upload-only seed (BEP 21). That
/// `PeerDisconnected { reason: UploadOnly }` sat in the event stream and the
/// loop below reported it as B being disconnected. Closing a fellow seed while
/// seeding is correct (pinned by `swarm.rs`'s
/// `seeds_disconnect_from_upload_only_peers`), so the fix is in the scenario:
/// `liar_scenario` now wants a piece nobody in the swarm has, which keeps us
/// downloading across that window and makes "B stays connected" the condition
/// it was meant to check. After the change: 100 runs at 4 copies, then 100 more
/// at 72 copies with the machine under CPU load, pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn honest_contributor_keeps_its_trust() {
    let mut l = liar_scenario().await;
    let disconnected_b =
        |e: &Event| matches!(e, Event::PeerDisconnected { addr, .. } if *addr == l.b);
    assert!(!l.seen.iter().any(disconnected_b), "B stays connected");
    // B lost 2 points for the failed piece and got them back when the
    // retry proved its blocks right (plus 1 if it sent blocks of the
    // retry): its trust is back where it was.
    let trust = l.h.peer_trust(l.b).await.unwrap();
    assert!((0..=1).contains(&trust), "B's trust is {trust}");
    // Now the rest is wanted; only B has any of it (nobody holds piece 4).
    let served_before = l.log_b.lock().unwrap().len();
    l.h.set_file_priorities(vec![Priority::Normal; 5])
        .await
        .unwrap();
    let ih = l.m.info_hashes;
    let end = tokio::time::Instant::now() + T;
    loop {
        let left = end.saturating_duration_since(tokio::time::Instant::now());
        let e = next_matching(&mut l.ev, |_| true, left)
            .await
            .expect("a piece only B has arrives");
        assert!(!disconnected_b(&e), "B was disconnected: {e:?}");
        assert!(banned(&e).is_none_or(|a| a == l.a), "{e:?}");
        // A piece only B has passes (before the finished torrent would
        // close B as a fellow seed).
        if matches!(e, Event::PieceFinished { torrent, piece } if torrent == ih && piece < 3) {
            break;
        }
    }
    let log = l.log_b.lock().unwrap().clone();
    assert!(log.len() > served_before, "B is asked for blocks again");
    assert!(log[served_before..].iter().any(|b| b.piece < 3));
    assert_eq!(l.s.banned_peers(), vec![l.a]);
    l.s.shutdown().await;
}

/// A seed whose data never matches the torrent (every byte flipped) is
/// the leech's only peer; returns once it is banned, with the number of
/// failed pieces seen before the ban.
async fn always_lies() -> (
    Session,
    TorrentHandle,
    TcpListener,
    SocketAddr,
    EventStream,
    usize,
) {
    let (m, payload) = make_torrent(&[("a", 8 * 16_384)], 16_384);
    let wrong: Vec<u8> = payload.iter().map(|x| !x).collect();
    let (la, a) = listener().await;
    let s = Session::new(plain()).await.unwrap();
    let mut ev = s.subscribe(EventMask::PEER | EventMask::PIECE);
    let mut add = AddTorrent::new(m, PathBuf::from("unused"));
    add.storage = Some(Arc::new(MemStorage::new(payload.len() as u64)));
    let h = s.add_torrent(add).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);
    h.add_peer(a).await.unwrap();
    let mut pa = ScriptedPeer::accept_with(&la, Reserved::default()).await;
    pa.send(Message::Bitfield(Bytes::from_static(&[0xff])))
        .await;
    pa.send(Message::Unchoke).await;
    tokio::spawn(async move {
        pa.serve_corrupt(&wrong, 16_384, HashSet::new(), Arc::default())
            .await;
    });
    // Failures keep coming until the ban: the whole wait is bounded.
    let failures = tokio::time::timeout(T, async {
        let mut failures = 0;
        while let Some(e) = ev.recv().await {
            assert!(!matches!(e, Event::PieceFinished { .. }), "{e:?}");
            if matches!(e, Event::HashFailed { .. }) {
                failures += 1;
            }
            if let Some(addr) = banned(&e) {
                assert_eq!(addr, a);
                return failures;
            }
        }
        panic!("event stream ended");
    })
    .await
    .expect("the liar is banned");
    (s, h, la, a, ev, failures)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_liar_banned_by_trust_points() {
    let (s, _h, _la, a, mut ev, failures) = always_lies().await;
    assert_eq!(failures, 4, "banned after its fourth failed piece");
    assert_eq!(s.banned_peers(), vec![a]);
    let closed = next_matching(
        &mut ev,
        |e| matches!(e, Event::PeerDisconnected { addr, .. } if *addr == a),
        T,
    )
    .await
    .expect("the liar is closed");
    let Event::PeerDisconnected { reason, .. } = closed else {
        unreachable!()
    };
    assert_eq!(
        reason,
        fbt_session::DisconnectReason::Blocked(BlockReason::Banned)
    );
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn banned_peer_is_not_redialled() {
    let (s, h, la, a, mut ev, _) = always_lies().await;
    h.add_peer(a).await.unwrap();
    let e = next_matching(&mut ev, |e| banned(e).is_some(), T)
        .await
        .expect("the add is refused");
    assert_eq!(banned(&e), Some(a));
    assert!(
        tokio::time::timeout(Duration::from_secs(1), la.accept())
            .await
            .is_err(),
        "no connection reaches the banned peer"
    );
    s.shutdown().await;
}

/// A peer that sends bad data to one torrent is banned session-wide: its
/// connection to another torrent closes too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ban_reaches_every_torrent() {
    let (m1, payload) = make_torrent(&[("a", 8 * 16_384)], 16_384);
    let (m2, _) = make_torrent(&[("b", 8 * 16_384)], 16_384);
    let wrong: Vec<u8> = payload.iter().map(|x| !x).collect();
    let (la, a) = listener().await;
    let s = Session::new(plain()).await.unwrap();
    let mut ev = s.subscribe(EventMask::PEER);
    let mut handles = Vec::new();
    for m in [&m1, &m2] {
        let mut add = AddTorrent::new(m.clone(), PathBuf::from("unused"));
        add.storage = Some(Arc::new(MemStorage::new(payload.len() as u64)));
        let h = s.add_torrent(add).await.unwrap();
        assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);
        handles.push(h);
    }
    // Torrent 2 first: A stays connected there, idle (it has nothing).
    handles[1].add_peer(a).await.unwrap();
    let mut idle = ScriptedPeer::accept_with(&la, Reserved::default()).await;
    assert_eq!(idle.remote.info_hash, m2.info_hashes.v1.unwrap().0);
    next_matching(
        &mut ev,
        |e| matches!(e, Event::PeerConnected { torrent, .. } if *torrent == m2.info_hashes),
        T,
    )
    .await
    .expect("A connects to torrent 2");
    // Torrent 1: A sends data that never matches, and is banned.
    handles[0].add_peer(a).await.unwrap();
    let mut liar = ScriptedPeer::accept_with(&la, Reserved::default()).await;
    assert_eq!(liar.remote.info_hash, m1.info_hashes.v1.unwrap().0);
    liar.send(Message::Bitfield(Bytes::from_static(&[0xff])))
        .await;
    liar.send(Message::Unchoke).await;
    tokio::spawn(async move {
        liar.serve_corrupt(&wrong, 16_384, HashSet::new(), Arc::default())
            .await;
    });
    let closed = next_matching(
        &mut ev,
        |e| {
            matches!(e, Event::PeerDisconnected { torrent, addr, .. }
                if *torrent == m2.info_hashes && *addr == a)
        },
        T,
    )
    .await
    .expect("A's connection to torrent 2 closes");
    let Event::PeerDisconnected { reason, .. } = closed else {
        unreachable!()
    };
    assert_eq!(
        reason,
        fbt_session::DisconnectReason::Blocked(BlockReason::Banned)
    );
    assert_eq!(s.banned_peers(), vec![a]);
    assert_eq!(idle.wait_closed(T).await, PeerEnd::Closed);
    s.shutdown().await;
}
