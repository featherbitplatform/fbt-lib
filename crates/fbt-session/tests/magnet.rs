//! Magnet torrents: metadata fetched from peers (BEP 9) and verified,
//! hostile metadata refused, our metadata served, and what a magnet without
//! metadata does with commands and announces. The session has no DHT yet:
//! peers come from `x.pe` hints, `add_peer`, trackers or connect to us.

mod support;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fbt_core::{InfoHashV1, InfoHashV2, InfoHashes};
use fbt_metainfo::{Magnet, Metainfo};
use fbt_session::storage::MemStorage;
use fbt_session::{
    AddTorrent, DisconnectReason, Event, EventMask, EventStream, Priority, RemoveMode, Session,
    SessionError, Settings, TorrentHandle, TorrentState,
};
use fbt_wire::{ExtHandshake, Message, MetadataMsg, OUR_UT_METADATA_ID, Reserved, UT_METADATA};
use futures_util::{SinkExt, StreamExt};
use support::*;

const T: Duration = Duration::from_secs(30);
/// The `ut_metadata` id our scripted peers assign.
const THEIR_UT_METADATA: u8 = 3;

fn settings() -> Settings {
    let mut s = test_settings();
    s.rechoke_interval = Duration::from_secs(1);
    s
}

/// A seeding session for `m`, waited until it seeds.
async fn seeder(m: &Metainfo, payload: &[u8]) -> (Session, TorrentHandle) {
    let s = Session::new(settings()).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(payload.to_vec())));
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    (s, h)
}

fn magnet_of(ih: InfoHashes, peers: Vec<SocketAddr>) -> Magnet {
    let mut mg = Magnet::from_info_hashes(ih);
    mg.peers = peers;
    mg
}

fn fast_ext() -> Reserved {
    let mut r = Reserved::default();
    r.set_fast();
    r.set_extended();
    r
}

/// A BEP 10 handshake offering `ut_metadata` with `size`.
fn their_hs(size: Option<u32>) -> ExtHandshake {
    let mut hs = ExtHandshake::default();
    hs.m.insert(UT_METADATA.to_owned(), THEIR_UT_METADATA);
    hs.metadata_size = size;
    hs
}

fn ut_metadata(msg: &MetadataMsg) -> Message {
    Message::Extended {
        id: OUR_UT_METADATA_ID,
        payload: msg.encode(),
    }
}

/// The next `ut_metadata` message the session sends a scripted peer.
async fn next_metadata(peer: &mut ScriptedPeer) -> Option<MetadataMsg> {
    let payload = peer.next_ext(THEIR_UT_METADATA).await?;
    Some(MetadataMsg::decode(&payload).expect("valid ut_metadata"))
}

fn is_bad_metadata(e: &Event, addr: SocketAddr) -> bool {
    matches!(
        e,
        Event::PeerDisconnected { addr: a, reason: DisconnectReason::BadMetadata, .. } if *a == addr
    )
}

async fn wait_event(ev: &mut EventStream, what: &str, pred: impl Fn(&Event) -> bool) -> Event {
    next_matching(ev, pred, T)
        .await
        .unwrap_or_else(|| panic!("no {what}"))
}

fn file(dir: &Path, rel: &str) -> Vec<u8> {
    std::fs::read(dir.join("torrent").join(rel)).unwrap_or_default()
}

fn dir_is_empty(dir: &Path) -> bool {
    std::fs::read_dir(dir).unwrap().next().is_none()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn magnet_with_peer_hint_downloads() {
    let (m, payload) = make_torrent(&[("a", 300_000), ("b/c", 70_000)], 32_768);
    let (seed, _sh) = seeder(&m, &payload).await;
    let leech = Session::new(settings()).await.unwrap();
    let mut ev = leech.subscribe(EventMask::STATUS);
    let dir = tempfile::tempdir().unwrap();
    let mg = magnet_of(m.info_hashes, vec![seed.local_addr()]);
    let h = leech
        .add_torrent(AddTorrent::magnet(mg, dir.path().to_path_buf()))
        .await
        .unwrap();

    let mut seen = Vec::new();
    loop {
        let e = wait_event(&mut ev, "TorrentFinished", |_| true).await;
        let last = matches!(e, Event::TorrentFinished { .. });
        seen.push(e);
        if last {
            break;
        }
    }
    let ih = m.info_hashes;
    assert_eq!(seen[0], Event::TorrentAdded { torrent: ih });
    assert_eq!(
        seen[1],
        Event::StateChanged {
            torrent: ih,
            state: TorrentState::DownloadingMetadata
        }
    );
    assert_eq!(seen[2], Event::MetadataReceived { torrent: ih }, "{seen:?}");
    assert!(
        matches!(
            seen[3],
            Event::StateChanged {
                state: TorrentState::CheckingFiles { .. },
                ..
            }
        ),
        "{seen:?}"
    );

    assert!(file(dir.path(), "a") == payload[..300_000], "a differs");
    assert!(file(dir.path(), "b/c") == payload[300_000..], "b/c differs");
    let meta = h.metainfo().expect("metadata");
    assert_eq!(meta.info.name, "torrent");
    assert_eq!(meta.raw_info(), m.raw_info());
    assert!(wait_until(|| h.status().has_metadata, T).await);
    assert_eq!(h.status().name, "torrent");
    assert_eq!(h.info_hashes(), ih, "the magnet's identity stays");
    leech.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_from_lying_peer_is_rejected_then_fetched_from_honest_one() {
    let (m, payload) = make_torrent(&[("a", 100_000)], 32_768);
    let (seed, _sh) = seeder(&m, &payload).await;
    let leech = Session::new(settings()).await.unwrap();
    let mut ev = leech.subscribe(EventMask::STATUS | EventMask::PEER);
    let dir = tempfile::tempdir().unwrap();
    let h = leech
        .add_torrent(AddTorrent::magnet(
            magnet_of(m.info_hashes, vec![]),
            dir.path().to_path_buf(),
        ))
        .await
        .unwrap();

    // The liar has the right size but flips every byte it serves.
    let raw = m.raw_info().to_vec();
    let mut liar =
        ScriptedPeer::connect_with(leech.local_addr(), m.info_hashes.wire_hash(), fast_ext()).await;
    let liar_addr = liar.framed.get_ref().local_addr().unwrap();
    liar.send_ext_handshake(&their_hs(Some(raw.len() as u32)))
        .await;
    let serve = tokio::spawn(async move {
        while let Some(msg) = next_metadata(&mut liar).await {
            if let MetadataMsg::Request { piece } = msg {
                let start = piece as usize * 16_384;
                let end = (start + 16_384).min(raw.len());
                let flipped: Vec<u8> = raw[start..end].iter().map(|b| !b).collect();
                let data = MetadataMsg::Data {
                    piece,
                    total_size: raw.len() as u32,
                    data: Bytes::from(flipped),
                };
                if liar.framed.send(ut_metadata(&data)).await.is_err() {
                    break;
                }
            }
        }
    });
    wait_event(&mut ev, "the liar's BadMetadata", |e| {
        is_bad_metadata(e, liar_addr)
    })
    .await;
    assert!(h.metainfo().is_none());
    serve.abort();

    h.add_peer(seed.local_addr()).await.unwrap();
    wait_event(&mut ev, "MetadataReceived", |e| {
        matches!(e, Event::MetadataReceived { .. })
    })
    .await;
    wait_event(&mut ev, "TorrentFinished", |e| {
        matches!(e, Event::TorrentFinished { .. })
    })
    .await;
    assert!(file(dir.path(), "a") == payload, "data differs");
    leech.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_or_inconsistent_metadata_is_refused() {
    let mut s = settings();
    s.max_metadata_size = 16_384;
    let leech = Session::new(s).await.unwrap();
    let mut ev = leech.subscribe(EventMask::PEER);
    let ih = InfoHashes::from_v1(InfoHashV1([7; 20]));
    let dir = tempfile::tempdir().unwrap();
    let _h = leech
        .add_torrent(AddTorrent::magnet(
            magnet_of(ih, vec![]),
            dir.path().to_path_buf(),
        ))
        .await
        .unwrap();

    // Claims 1 GiB: never asked, closed with BadMetadata.
    let mut huge = ScriptedPeer::connect_with(leech.local_addr(), ih.wire_hash(), fast_ext()).await;
    let huge_addr = huge.framed.get_ref().local_addr().unwrap();
    huge.send_ext_handshake(&their_hs(Some(1 << 30))).await;
    let asked = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match huge.framed.next().await {
                Some(Ok(Message::Extended { id, .. })) if id == THEIR_UT_METADATA => return true,
                Some(Ok(_)) => {}
                Some(Err(_)) | None => return false,
            }
        }
    })
    .await;
    assert_eq!(asked, Ok(false), "no ut_metadata request, and closed");
    wait_event(&mut ev, "BadMetadata for the 1 GiB claim", |e| {
        is_bad_metadata(e, huge_addr)
    })
    .await;

    // Advertises 10 000 bytes, then serves a different total_size.
    let mut liar = ScriptedPeer::connect_with(leech.local_addr(), ih.wire_hash(), fast_ext()).await;
    let liar_addr = liar.framed.get_ref().local_addr().unwrap();
    liar.send_ext_handshake(&their_hs(Some(10_000))).await;
    let Some(MetadataMsg::Request { piece: 0 }) = next_metadata(&mut liar).await else {
        panic!("expected a request for piece 0");
    };
    liar.send(ut_metadata(&MetadataMsg::Data {
        piece: 0,
        total_size: 10_001,
        data: Bytes::from(vec![0u8; 10_000]),
    }))
    .await;
    wait_event(&mut ev, "BadMetadata for the inconsistent size", |e| {
        is_bad_metadata(e, liar_addr)
    })
    .await;
    assert_eq!(liar.wait_closed(T).await, PeerEnd::Closed);
    assert!(dir_is_empty(dir.path()));
    leech.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn so_selection_skips_unselected_files() {
    let (m, payload) = make_torrent(&[("a", 100_000), ("b", 200_000), ("c", 100_000)], 32_768);
    let (seed, _sh) = seeder(&m, &payload).await;
    let leech = Session::new(settings()).await.unwrap();
    let mut ev = leech.subscribe(EventMask::STATUS);
    let dir = tempfile::tempdir().unwrap();
    let mut mg = magnet_of(m.info_hashes, vec![seed.local_addr()]);
    mg.select_only = Some(vec![0..=0, 2..=2]);
    let h = leech
        .add_torrent(AddTorrent::magnet(mg, dir.path().to_path_buf()))
        .await
        .unwrap();
    wait_event(&mut ev, "TorrentFinished", |e| {
        matches!(e, Event::TorrentFinished { .. })
    })
    .await;
    assert!(file(dir.path(), "a") == payload[..100_000], "a differs");
    assert!(file(dir.path(), "c") == payload[300_000..], "c differs");
    // Pieces 4..=8 lie inside b (bytes 131 072..294 912): never fetched.
    let b = file(dir.path(), "b");
    let inner = (4 * 32_768 - 100_000)..(9 * 32_768 - 100_000);
    assert!(
        b.get(inner.clone())
            .is_none_or(|bytes| bytes.iter().all(|&x| x == 0)),
        "b's inner pieces were downloaded"
    );
    let st = h.status();
    assert!(st.total_wanted < m.total_len());
    assert_eq!(st.wanted_done, st.total_wanted);
    leech.shutdown().await;
    seed.shutdown().await;
}

/// A peer that accepts connections and never says anything.
async fn silent_peer() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((s, _)) = listener.accept().await {
            held.push(s);
        }
    });
    (addr, task)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remove_before_metadata_creates_no_files() {
    let (silent, task) = silent_peer().await;
    let session = Session::new(settings()).await.unwrap();
    let add = |seed: u8, dir: &Path| {
        let mg = magnet_of(InfoHashes::from_v1(InfoHashV1([seed; 20])), vec![silent]);
        AddTorrent::magnet(mg, dir.to_path_buf())
    };

    // Remove with DeleteData: quick, and nothing was ever created.
    let dir = tempfile::tempdir().unwrap();
    let h = session.add_torrent(add(1, dir.path())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(h.pieces().await.unwrap().len(), 0);
    let start = tokio::time::Instant::now();
    h.remove(RemoveMode::DeleteData).await.unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "{:?}",
        start.elapsed()
    );
    assert!(dir_is_empty(dir.path()));

    // Pause and resume.
    let dir = tempfile::tempdir().unwrap();
    let h = session.add_torrent(add(2, dir.path())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    h.pause().await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Paused, T).await);
    h.resume().await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::DownloadingMetadata, T).await);
    assert!(dir_is_empty(dir.path()));

    // Shutdown.
    let start = tokio::time::Instant::now();
    session.shutdown().await;
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "{:?}",
        start.elapsed()
    );
    assert!(dir_is_empty(dir.path()));
    task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seeder_serves_metadata_and_rejects_out_of_range() {
    let (m, payload) = make_torrent(&[("a", 100_000)], 32_768);
    let (seed, _sh) = seeder(&m, &payload).await;
    let raw = m.raw_info().to_vec();
    let mut peer =
        ScriptedPeer::connect_with(seed.local_addr(), m.info_hashes.wire_hash(), fast_ext()).await;
    let hs = ExtHandshake::decode(&peer.next_ext(0).await.expect("their ext handshake")).unwrap();
    assert_eq!(hs.metadata_size, Some(raw.len() as u32));
    assert_eq!(hs.id_of(UT_METADATA), Some(OUR_UT_METADATA_ID));
    peer.send_ext_handshake(&their_hs(None)).await;

    let whole = MetadataMsg::Data {
        piece: 0,
        total_size: raw.len() as u32,
        data: Bytes::from(raw.clone()),
    };
    peer.send(ut_metadata(&MetadataMsg::Request { piece: 0 }))
        .await;
    assert_eq!(next_metadata(&mut peer).await, Some(whole.clone()));
    peer.send(ut_metadata(&MetadataMsg::Request { piece: 5 }))
        .await;
    assert_eq!(
        next_metadata(&mut peer).await,
        Some(MetadataMsg::Reject { piece: 5 })
    );
    // 256 requests per connection are answered; the 257th is rejected.
    for _ in 2..256 {
        peer.send(ut_metadata(&MetadataMsg::Request { piece: 0 }))
            .await;
        assert_eq!(next_metadata(&mut peer).await, Some(whole.clone()));
    }
    peer.send(ut_metadata(&MetadataMsg::Request { piece: 0 }))
        .await;
    assert_eq!(
        next_metadata(&mut peer).await,
        Some(MetadataMsg::Reject { piece: 0 })
    );
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn magnet_announces_left_16384_until_metadata() {
    let tracker = FakeTracker::http(Reply::Peers).await;

    // Removed before any metadata: `started` and `stopped` both say 16 384.
    let session = Session::new(settings()).await.unwrap();
    let port = session.local_addr().port();
    let mut mg = magnet_of(InfoHashes::from_v1(InfoHashV1([9; 20])), vec![]);
    mg.trackers = vec![tracker.url.clone()];
    let h = session
        .add_torrent(AddTorrent::magnet(mg, PathBuf::from("unused")))
        .await
        .unwrap();
    assert!(wait_until(|| tracker.events_of(port) == ["started"], T).await);
    h.remove(RemoveMode::KeepData).await.unwrap();
    assert!(
        wait_until(|| tracker.events_of(port) == ["started", "stopped"], T).await,
        "{:?}",
        tracker.events_of(port)
    );
    let lefts: Vec<u64> = tracker
        .announces()
        .iter()
        .filter(|a| a.port == port)
        .map(|a| a.left)
        .collect();
    assert_eq!(lefts, [16_384, 16_384]);
    session.shutdown().await;

    // With metadata from a seeder found through the tracker: the true left
    // after it.
    let (mut m, payload) = make_torrent(&[("a", 100_000)], 32_768);
    m.trackers = vec![vec![tracker.url.clone()]];
    let (seed, _sh) = seeder(&m, &payload).await;
    let seed_port = seed.local_addr().port();
    assert!(wait_until(|| tracker.events_of(seed_port) == ["started"], T).await);
    let leech = Session::new(settings()).await.unwrap();
    let port = leech.local_addr().port();
    let mut ev = leech.subscribe(EventMask::STATUS);
    let mut mg = magnet_of(m.info_hashes, vec![]);
    mg.trackers = vec![tracker.url.clone()];
    let mut p = AddTorrent::magnet(mg, PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::new(payload.len() as u64)));
    let _h = leech.add_torrent(p).await.unwrap();
    wait_event(&mut ev, "MetadataReceived", |e| {
        matches!(e, Event::MetadataReceived { .. })
    })
    .await;
    let before: Vec<u64> = tracker
        .announces()
        .iter()
        .filter(|a| a.port == port)
        .map(|a| a.left)
        .collect();
    assert!(
        !before.is_empty() && before.iter().all(|&l| l == 16_384),
        "{before:?}"
    );
    wait_event(&mut ev, "TorrentFinished", |e| {
        matches!(e, Event::TorrentFinished { .. })
    })
    .await;
    assert!(
        wait_until(
            || tracker.events_of(port).iter().any(|e| e == "completed"),
            T
        )
        .await
    );
    leech.shutdown().await;
    let all: Vec<(String, u64)> = tracker
        .announces()
        .into_iter()
        .filter(|a| a.port == port)
        .map(|a| (a.event, a.left))
        .collect();
    assert_eq!(
        all,
        [
            ("started".to_owned(), 16_384),
            ("completed".to_owned(), 0),
            ("stopped".to_owned(), 0)
        ]
    );
    seed.shutdown().await;
}

#[tokio::test]
async fn magnet_v2_only_rejected() {
    let session = Session::new(settings()).await.unwrap();
    let mg = Magnet::from_info_hashes(InfoHashes::from_v2(InfoHashV2([3; 32])));
    let r = session
        .add_torrent(AddTorrent::magnet(mg, PathBuf::from("unused")))
        .await;
    assert!(matches!(r, Err(SessionError::V2OnlyUnsupported)), "{r:?}");
    assert!(session.torrents().is_empty());
    session.shutdown().await;
}

#[tokio::test]
async fn magnet_priorities_rejected_until_metadata() {
    let session = Session::new(settings()).await.unwrap();
    let mg = Magnet::from_info_hashes(InfoHashes::from_v1(InfoHashV1([4; 20])));
    let mut p = AddTorrent::magnet(mg.clone(), PathBuf::from("unused"));
    p.file_priorities = vec![Priority::Normal];
    let r = session.add_torrent(p).await;
    assert!(
        matches!(
            r,
            Err(SessionError::InvalidPriorities {
                expected: 0,
                got: 1
            })
        ),
        "{r:?}"
    );
    let h = session
        .add_torrent(AddTorrent::magnet(mg, PathBuf::from("unused")))
        .await
        .unwrap();
    let r = h.set_file_priorities(vec![Priority::Normal]).await;
    assert!(matches!(r, Err(SessionError::NoMetadata)), "{r:?}");
    let r = h.force_recheck().await;
    assert!(matches!(r, Err(SessionError::NoMetadata)), "{r:?}");
    assert_eq!(h.pieces().await.unwrap().len(), 0);
    assert!(h.metainfo().is_none());
    // Commands that do not need the metadata work.
    h.force_reannounce().await.unwrap();
    h.add_peer(SocketAddr::from(([127, 0, 0, 1], 9)))
        .await
        .unwrap();
    let st = h.status();
    assert!(!st.has_metadata);
    assert_eq!(st.state, TorrentState::DownloadingMetadata);
    session.shutdown().await;
}
