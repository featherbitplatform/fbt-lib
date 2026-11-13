//! Detail calls: a torrent's files, peers and trackers, and the session's
//! limit and queue getters.

mod support;

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

use fbt_core::{InfoHashV1, InfoHashes};
use fbt_metainfo::{Magnet, Metainfo, TorrentVersion};
use fbt_session::{
    AddTorrent, Event, EventMask, PeerClassLimits, PeerSource, PeerStatus, QueueSettings,
    RateLimit, Session, SessionError, Settings, TorrentHandle, TorrentState, Transport,
};
use support::*;

const T: Duration = Duration::from_secs(30);

fn settings() -> Settings {
    let mut s = test_settings();
    s.rechoke_interval = Duration::from_secs(1);
    s
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn files_report_progress() {
    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    let (m, payload) = make_torrent_with_pad(&[("a", 100_000), ("b", 70_000)], 0, 32_768);
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

    let before = lh.files().await.unwrap();
    assert_eq!(before.len(), m.info.files.len());
    for (i, (f, e)) in before.iter().zip(&m.info.files).enumerate() {
        assert_eq!(f.index, i);
        assert_eq!(f.path, e.path.to_path_buf());
        assert_eq!(f.len, e.len);
        assert_eq!(f.pad, e.pad);
        assert_eq!(f.downloaded, 0, "file {i} before the download");
    }

    lh.add_peer(seeder.local_addr()).await.unwrap();
    next_matching(&mut ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    let after = lh.files().await.unwrap();
    for f in after.iter().filter(|f| !f.pad) {
        assert_eq!(f.downloaded, f.len, "file {} after the finish", f.index);
    }

    lh.rename_file(0, "x/renamed.bin").await.unwrap();
    let renamed = lh.files().await.unwrap();
    assert!(
        renamed[0].path.ends_with("renamed.bin"),
        "{:?}",
        renamed[0].path
    );
    assert_eq!(renamed[0].path, PathBuf::from("x").join("renamed.bin"));
    leecher.shutdown().await;
    seeder.shutdown().await;
}

#[tokio::test]
async fn files_without_metadata() {
    let session = Session::new(settings()).await.unwrap();
    let mg = Magnet::from_info_hashes(InfoHashes::from_v1(InfoHashV1([4; 20])));
    let h = session
        .add_torrent(AddTorrent::magnet(mg, PathBuf::from("unused")))
        .await
        .unwrap();
    let r = h.files().await;
    assert!(matches!(r, Err(SessionError::NoMetadata)), "{r:?}");
    session.shutdown().await;
}

/// Polls `h.peers()` until `pred` holds for the list; returns that list.
async fn wait_peers(
    h: &TorrentHandle,
    pred: impl Fn(&[PeerStatus]) -> bool,
) -> Option<Vec<PeerStatus>> {
    let deadline = tokio::time::Instant::now() + T;
    loop {
        let peers = h.peers().await.unwrap();
        if pred(&peers) {
            return Some(peers);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peers_report_the_seed() {
    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    // Upload-limited, so the leech is still downloading when it looks.
    let (m, payload) = make_torrent(&[("a", 1024 * 1024 + 4096)], 32_768);
    write_payload(seed_dir.path(), &m, &payload);
    let mut seed_settings = settings();
    seed_settings.rate_limit_local_peers = true;
    seed_settings.rate_limit = RateLimit::new(0, 64 * 1024);
    let seeder = Session::new(seed_settings).await.unwrap();
    let sh = seeder
        .add_torrent(AddTorrent::new(m.clone(), seed_dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&sh, |s| *s == TorrentState::Seeding, T).await);
    let leecher = Session::new(dial_tcp(settings())).await.unwrap();
    let mut ev = leecher.subscribe(EventMask::ALL);
    let lh = leecher
        .add_torrent(AddTorrent::new(m.clone(), leech_dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&lh, |s| *s == TorrentState::Downloading, T).await);
    assert!(lh.peers().await.unwrap().is_empty());

    let seed_addr = seeder.local_addr();
    lh.add_peer(seed_addr).await.unwrap();
    let peers = wait_peers(&lh, |ps| {
        ps.iter()
            .any(|p| p.addr == seed_addr && p.total_downloaded > 0)
    })
    .await
    .expect("the seed listed with payload from it");
    let st = lh.status();
    assert_eq!(
        st.state,
        TorrentState::Downloading,
        "snapshot before the finish"
    );
    let p = peers.iter().find(|p| p.addr == seed_addr).unwrap();
    assert_eq!(p.source, PeerSource::Manual);
    assert_eq!(p.transport, Transport::Tcp);
    assert!(!p.incoming);
    assert!(p.am_interested);
    assert!((p.progress - 1.0).abs() < f64::EPSILON, "{p:?}");
    assert!(p.client.is_some(), "{p:?}");

    let seen = wait_peers(&sh, |ps| !ps.is_empty())
        .await
        .expect("the leech listed by the seed");
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(seen[0].incoming, "{seen:?}");
    assert_eq!(seen[0].source, PeerSource::Incoming);

    next_matching(&mut ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    wait_peers(&lh, |ps| ps.iter().all(|p| p.addr != seed_addr))
        .await
        .expect("the seed closed after the finish");
    leecher.shutdown().await;
    seeder.shutdown().await;
}

fn is_reply(e: &Event, url: &str) -> bool {
    matches!(e, Event::TrackerReply { url: u, .. } if u == url)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trackers_report_replies() {
    let tracker = FakeTracker::http(Reply::Peers).await;
    let s = Session::new(settings()).await.unwrap();

    // One tracker, one hash.
    let (mut m, _) = make_torrent(&[("a", 100_000)], 32_768);
    m.trackers = vec![vec![tracker.url.clone()]];
    let mut ev = s.subscribe(EventMask::TRACKER);
    let h = s
        .add_torrent(AddTorrent::new(m.clone(), PathBuf::from("unused")))
        .await
        .unwrap();
    next_matching(&mut ev, |e| is_reply(e, &tracker.url), T)
        .await
        .expect("tracker reply");
    let t = h.trackers().await.unwrap();
    assert_eq!(t.len(), 1, "{t:?}");
    assert_eq!(t[0].url, tracker.url);
    assert_eq!(t[0].tier, 0);
    assert_eq!(t[0].info_hash, m.info_hashes.wire_hash());
    assert!(t[0].last_reply.is_some(), "{t:?}");
    assert_eq!(t[0].last_error, None);
    assert!(t[0].next_announce.is_some(), "{t:?}");
    assert!(t[0].peers_returned.is_some(), "{t:?}");

    // A hybrid: one entry per announced hash.
    let dir = tempfile::tempdir().unwrap();
    let bytes = create_torrent(
        &write_v2_payload(dir.path()),
        TorrentVersion::Hybrid,
        32_768,
    );
    let mut hm = Metainfo::from_bytes(&bytes).unwrap();
    hm.trackers = vec![vec![tracker.url.clone()]];
    let v1 = hm.info_hashes.v1.expect("v1 hash").0;
    let v2 = InfoHashes::from_v2(hm.info_hashes.v2.expect("v2 hash")).wire_hash();
    let mut ev = s.subscribe(EventMask::TRACKER);
    let hh = s
        .add_torrent(AddTorrent::new(hm.clone(), dir.path().to_path_buf()))
        .await
        .unwrap();
    let mut replied = HashSet::new();
    while replied.len() < 2 {
        let e = next_matching(
            &mut ev,
            |e| matches!(e, Event::TrackerReply { torrent, .. } if *torrent == hm.info_hashes),
            T,
        )
        .await
        .expect("tracker replies");
        if let Event::TrackerReply { info_hash, .. } = e {
            replied.insert(info_hash);
        }
    }
    let t = hh.trackers().await.unwrap();
    assert_eq!(t.len(), 2, "{t:?}");
    let hashes: HashSet<[u8; 20]> = t.iter().map(|t| t.info_hash).collect();
    assert_eq!(hashes, HashSet::from([v1, v2]));
    for e in &t {
        assert_eq!(e.url, tracker.url);
        assert!(e.last_reply.is_some(), "{t:?}");
    }

    // A tracker refusing connections.
    let dead = "http://127.0.0.1:1/announce".to_owned();
    let (mut dm, _) = make_torrent(&[("d", 50_000)], 32_768);
    dm.trackers = vec![vec![dead.clone()]];
    let mut ev = s.subscribe(EventMask::TRACKER);
    let dh = s
        .add_torrent(AddTorrent::new(dm, PathBuf::from("unused")))
        .await
        .unwrap();
    next_matching(
        &mut ev,
        |e| matches!(e, Event::TrackerError { url, .. } if *url == dead),
        T,
    )
    .await
    .expect("tracker error");
    let t = dh.trackers().await.unwrap();
    assert_eq!(t.len(), 1, "{t:?}");
    assert_eq!(t[0].url, dead);
    assert!(t[0].last_error.is_some(), "{t:?}");
    assert!(t[0].last_reply.is_none(), "{t:?}");
    s.shutdown().await;
}

#[tokio::test]
async fn session_readers_round_trip() {
    let s = Session::new(settings()).await.unwrap();
    let limit = RateLimit::new(1_000_000, 250_000);
    s.set_rate_limit(limit);
    assert_eq!(s.rate_limit(), limit);

    let mut classes = PeerClassLimits::default();
    classes.tcp = RateLimit::new(10, 20);
    classes.utp = RateLimit::new(30, 40);
    classes.local = RateLimit::new(50, 60);
    s.set_peer_class_limits(classes);
    assert_eq!(s.peer_class_limits(), classes);

    let mut queue = QueueSettings::default();
    queue.active_downloads = Some(2);
    queue.active_seeds = Some(3);
    queue.active_limit = Some(4);
    s.set_queue_settings(queue.clone()).unwrap();
    assert_eq!(s.queue_settings(), queue);
    s.shutdown().await;
}

#[tokio::test]
async fn is_shut_down_follows_shutdown() {
    let s = Session::new(settings()).await.unwrap();
    let clone = s.clone();
    assert!(!s.is_shut_down());
    assert!(!clone.is_shut_down());
    s.shutdown().await;
    assert!(clone.is_shut_down());
}
