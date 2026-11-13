//! Tracker announcing against fake HTTP and UDP trackers on loopback: peers
//! meeting through a tracker, the announce events, tier fallback and
//! failure replies.

mod support;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fbt_metainfo::Metainfo;
use fbt_session::storage::MemStorage;
use fbt_session::{AddTorrent, Event, EventMask, Session, Settings, TorrentHandle, TorrentState};
use support::*;

const T: Duration = Duration::from_secs(30);

fn settings() -> Settings {
    let mut s = test_settings();
    s.rechoke_interval = Duration::from_secs(1);
    s
}

/// A torrent announcing to `tiers`.
fn torrent(tiers: Vec<Vec<String>>) -> (Metainfo, Vec<u8>) {
    let (mut m, payload) = make_torrent(&[("a", 300_000), ("b", 70_000)], 32_768);
    m.trackers = tiers;
    (m, payload)
}

fn is_reply(e: &Event, url: &str) -> bool {
    matches!(e, Event::TrackerReply { url: u, .. } if u == url)
}

/// A seeding session for `m`, waited until its first tracker reply from
/// `url`.
async fn seeder(m: &Metainfo, payload: &[u8], url: &str) -> (Session, TorrentHandle) {
    let s = Session::new(settings()).await.unwrap();
    let mut ev = s.subscribe(EventMask::TRACKER);
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(payload.to_vec())));
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    next_matching(&mut ev, |e| is_reply(e, url), T)
        .await
        .expect("seeder's tracker reply");
    (s, h)
}

/// A leecher for `m` on memory storage; returns the session, its handle,
/// the storage and an event stream subscribed before adding.
async fn leecher(
    m: &Metainfo,
    len: usize,
) -> (
    Session,
    TorrentHandle,
    Arc<MemStorage>,
    fbt_session::EventStream,
) {
    let s = Session::new(settings()).await.unwrap();
    let ev = s.subscribe(EventMask::ALL);
    let storage = Arc::new(MemStorage::new(len as u64));
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(storage.clone());
    let h = s.add_torrent(p).await.unwrap();
    (s, h, storage, ev)
}

fn stored(storage: &MemStorage, len: usize) -> Vec<u8> {
    use fbt_session::storage::Storage;
    let mut got = vec![0u8; len];
    storage.read(0, &mut got).unwrap();
    got
}

async fn finished(ev: &mut fbt_session::EventStream) {
    next_matching(ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
}

/// Seeder and leecher find each other through `tracker` alone.
async fn meet_via(tracker: FakeTracker) {
    let (m, payload) = torrent(vec![vec![tracker.url.clone()]]);
    let (seed, _sh) = seeder(&m, &payload, &tracker.url).await;
    let (leech, _lh, storage, mut ev) = leecher(&m, payload.len()).await;
    finished(&mut ev).await;
    assert!(stored(&storage, payload.len()) == payload, "data differs");
    leech.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seeder_and_leecher_meet_via_http_tracker() {
    meet_via(FakeTracker::http(Reply::Peers).await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seeder_and_leecher_meet_via_udp_tracker() {
    meet_via(FakeTracker::udp(Reply::Peers).await).await;
}

/// UDP announces leave from the shared socket, so the tracker sees the
/// listen port as their source port (and the stopped announce still goes
/// out at shutdown).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn udp_tracker_sees_the_listen_port_as_source() {
    let tracker = FakeTracker::udp(Reply::Peers).await;
    let (m, payload) = torrent(vec![vec![tracker.url.clone()]]);
    let (seed, _sh) = seeder(&m, &payload, &tracker.url).await;
    let port = seed.local_addr().port();
    let announces = tracker.announces();
    assert!(!announces.is_empty());
    for a in &announces {
        assert_eq!(a.port, port);
        assert_eq!(a.source.port(), port, "announce from {}", a.source);
    }
    assert!(seed.demux_stats()[0].1.tracker > 0);
    seed.shutdown().await;
    assert_eq!(tracker.events_of(port), ["started", "stopped"]);
    assert!(tracker.announces().iter().all(|a| a.source.port() == port));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn announce_events_sequence() {
    let tracker = FakeTracker::http(Reply::Peers).await;
    let (m, payload) = torrent(vec![vec![tracker.url.clone()]]);
    let (seed, _sh) = seeder(&m, &payload, &tracker.url).await;
    let (leech, _lh, _storage, mut ev) = leecher(&m, payload.len()).await;
    let port = leech.local_addr().port();
    finished(&mut ev).await;
    assert!(
        wait_until(
            || tracker.events_of(port).iter().any(|e| e == "completed"),
            T
        )
        .await,
        "completed announced: {:?}",
        tracker.events_of(port)
    );
    leech.shutdown().await;
    assert_eq!(
        tracker.events_of(port),
        ["started", "completed", "stopped"],
        "leecher's announces"
    );
    seed.shutdown().await;
    let seed_events = tracker.events_of(seed.local_addr().port());
    assert_eq!(
        seed_events,
        ["started", "stopped"],
        "a seed never completes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failing_first_tracker_falls_back_to_second_tier() {
    let bad = FakeTracker::http(Reply::ServerError).await;
    let good = FakeTracker::http(Reply::Peers).await;
    let (m, payload) = torrent(vec![vec![bad.url.clone()], vec![good.url.clone()]]);
    let (seed, _sh) = seeder(&m, &payload, &good.url).await;
    let (leech, _lh, storage, mut ev) = leecher(&m, payload.len()).await;
    let err = next_matching(
        &mut ev,
        |e| matches!(e, Event::TrackerError { url, .. } if *url == bad.url),
        T,
    )
    .await
    .expect("error from the first tier");
    assert!(
        matches!(&err, Event::TrackerError { message, .. } if message.contains("500")),
        "{err:?}"
    );
    next_matching(&mut ev, |e| is_reply(e, &good.url), T)
        .await
        .expect("reply from the second tier");
    finished(&mut ev).await;
    assert!(stored(&storage, payload.len()) == payload, "data differs");
    let port = leech.local_addr().port();
    assert_eq!(
        bad.events_of(port).first().map(String::as_str),
        Some("started")
    );
    assert_eq!(
        good.events_of(port).first().map(String::as_str),
        Some("started")
    );
    leech.shutdown().await;
    seed.shutdown().await;
}

/// Events from `ev` up to and including the first matching `pred`.
async fn until(ev: &mut fbt_session::EventStream, pred: impl Fn(&Event) -> bool) -> Vec<Event> {
    let mut seen = Vec::new();
    while let Some(e) = next_matching(ev, |_| true, T).await {
        let done = pred(&e);
        seen.push(e);
        if done {
            return seen;
        }
    }
    panic!("no matching event; saw {seen:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tracker_failure_reason_emits_event() {
    let tracker = FakeTracker::http(Reply::Failure("torrent not registered".into())).await;
    let unsupported = "wss://tracker.invalid/announce".to_owned();
    let (m, payload) = torrent(vec![vec![unsupported.clone()], vec![tracker.url.clone()]]);
    let (leech, lh, _storage, mut ev) = leecher(&m, payload.len()).await;
    let is_err_of = |e: &Event, of: &str| matches!(e, Event::TrackerError { url, .. } if url == of);
    let seen = until(&mut ev, |e| is_err_of(e, &tracker.url)).await;
    let Some(Event::TrackerError {
        torrent, message, ..
    }) = seen.last()
    else {
        unreachable!()
    };
    assert_eq!(*torrent, m.info_hashes);
    assert!(message.contains("torrent not registered"), "{message}");
    let wss = |seen: &[Event]| seen.iter().filter(|e| is_err_of(e, &unsupported)).count();
    assert_eq!(
        wss(&seen),
        1,
        "one error for the unsupported scheme: {seen:?}"
    );
    // A forced re-announce skips the unsupported tracker for good.
    lh.force_reannounce().await.unwrap();
    let seen = until(&mut ev, |e| is_err_of(e, &tracker.url)).await;
    assert_eq!(wss(&seen), 0, "{seen:?}");
    assert_eq!(tracker.events_of(leech.local_addr().port()).len(), 2);
    leech.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_announces_back_off_and_retry() {
    let tracker = FakeTracker::http(Reply::ServerError).await;
    let (m, _) = torrent(vec![vec![tracker.url.clone()]]);
    let mut st = settings();
    st.tracker_retry_base = Duration::from_millis(100);
    let s = Session::new(st).await.unwrap();
    let port = s.local_addr().port();
    let start = std::time::Instant::now();
    s.add_torrent(AddTorrent::new(m, PathBuf::from("unused")))
        .await
        .unwrap();
    // Retries after 100 ms, then 200 ms: three announces after >= 300 ms.
    assert!(wait_until(|| tracker.events_of(port).len() >= 3, T).await);
    assert!(start.elapsed() >= Duration::from_millis(300));
    assert!(tracker.events_of(port).iter().all(|e| e == "started"));
    s.shutdown().await;
    // Nothing was ever answered, so there is nothing to stop.
    assert!(!tracker.events_of(port).iter().any(|e| e == "stopped"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_announces_stopped_and_resume_started() {
    let tracker = FakeTracker::http(Reply::Peers).await;
    let (m, payload) = torrent(vec![vec![tracker.url.clone()]]);
    let (s, h) = seeder(&m, &payload, &tracker.url).await;
    let port = s.local_addr().port();
    h.pause().await.unwrap();
    assert!(wait_until(|| tracker.events_of(port).len() == 2, T).await);
    h.resume().await.unwrap();
    assert!(wait_until(|| tracker.events_of(port).len() == 3, T).await);
    h.force_reannounce().await.unwrap();
    assert!(wait_until(|| tracker.events_of(port).len() == 4, T).await);
    s.shutdown().await;
    assert_eq!(
        tracker.events_of(port),
        ["started", "stopped", "started", "none", "stopped"]
    );
}

/// Pausing while the `completed` reply is pending abandons that walk; after
/// the resume `completed` is not sent a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupted_completed_walk_is_not_repeated() {
    let tracker = FakeTracker::http(Reply::PeersDelayCompleted(Duration::from_secs(2))).await;
    let (m, payload) = torrent(vec![vec![tracker.url.clone()]]);
    let (seed, _sh) = seeder(&m, &payload, &tracker.url).await;
    let (leech, lh, _storage, mut ev) = leecher(&m, payload.len()).await;
    let port = leech.local_addr().port();
    finished(&mut ev).await;
    let completed = |t: &FakeTracker| {
        t.events_of(port)
            .iter()
            .filter(|e| *e == "completed")
            .count()
    };
    assert!(wait_until(|| completed(&tracker) == 1, T).await);
    // The reply is still 2 s away.
    lh.pause().await.unwrap();
    assert!(wait_until(|| tracker.events_of(port).iter().any(|e| e == "stopped"), T).await);
    lh.resume().await.unwrap();
    assert!(
        wait_until(
            || tracker
                .events_of(port)
                .iter()
                .filter(|e| *e == "started")
                .count()
                == 2,
            T
        )
        .await
    );
    // Long enough for a repeated `completed` walk to show up.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(completed(&tracker), 1, "{:?}", tracker.events_of(port));
    leech.shutdown().await;
    seed.shutdown().await;
}

/// A recheck stops the torrent for the files check; the `stopped` announce
/// reports the `left` from before it, not the transient figure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recheck_stopped_reports_previous_left() {
    let tracker = FakeTracker::http(Reply::Peers).await;
    let (m, payload) = torrent(vec![vec![tracker.url.clone()]]);
    // The first half of the pieces is on disk already.
    let mut have = vec![0u8; payload.len()];
    let half = 6 * 32_768;
    have[..half].copy_from_slice(&payload[..half]);
    let s = Session::new(settings()).await.unwrap();
    let port = s.local_addr().port();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(have)));
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);
    let st = h.status();
    let left = st.total_wanted - st.wanted_done;
    assert!(left > 0 && left < st.total_wanted, "half done: {left}");
    assert!(wait_until(|| !tracker.events_of(port).is_empty(), T).await);
    h.force_recheck().await.unwrap();
    assert!(
        wait_until(|| tracker.events_of(port).iter().any(|e| e == "stopped"), T).await,
        "{:?}",
        tracker.events_of(port)
    );
    let stopped = tracker
        .announces()
        .into_iter()
        .find(|a| a.event == "stopped")
        .unwrap();
    assert_eq!(stopped.left, left);
    s.shutdown().await;
}

/// A `completed` that no tracker took (every walk failed) is retried.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completed_is_retried_after_a_failed_walk() {
    let tracker = FakeTracker::http(Reply::ServerErrorOnFirstCompleted).await;
    let (m, payload) = torrent(vec![vec![tracker.url.clone()]]);
    let (seed, _sh) = seeder(&m, &payload, &tracker.url).await;
    let mut st = settings();
    st.tracker_retry_base = Duration::from_millis(100);
    let leech = Session::new(st).await.unwrap();
    let port = leech.local_addr().port();
    let storage = Arc::new(MemStorage::new(payload.len() as u64));
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(storage);
    leech.add_torrent(p).await.unwrap();
    let completed = |t: &FakeTracker| {
        t.events_of(port)
            .iter()
            .filter(|e| *e == "completed")
            .count()
    };
    assert!(
        wait_until(|| completed(&tracker) == 2, T).await,
        "{:?}",
        tracker.events_of(port)
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(completed(&tracker), 2, "{:?}", tracker.events_of(port));
    leech.shutdown().await;
    seed.shutdown().await;
}

/// Every tracker that took a `started` in this run gets the `stopped`:
/// tier 1 answers the first announce, then fails, so a forced re-announce
/// reaches tier 2, which takes a `started` of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopped_goes_to_every_started_tracker() {
    let t1 = FakeTracker::http(Reply::Peers).await;
    let t2 = FakeTracker::http(Reply::Peers).await;
    let (m, payload) = torrent(vec![vec![t1.url.clone()], vec![t2.url.clone()]]);
    let (s, h) = seeder(&m, &payload, &t1.url).await;
    let port = s.local_addr().port();
    assert!(t2.events_of(port).is_empty(), "tier 2 not reached yet");
    t1.set_reply(Reply::ServerError);
    h.force_reannounce().await.unwrap();
    assert!(
        wait_until(|| t2.events_of(port) == ["started"], T).await,
        "tier 2: {:?}, tier 1: {:?}",
        t2.events_of(port),
        t1.events_of(port)
    );
    s.shutdown().await;
    assert_eq!(t1.events_of(port), ["started", "none", "stopped"]);
    assert_eq!(t2.events_of(port), ["started", "stopped"]);
}

/// A tracker that never answered gets no `stopped`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopped_skips_trackers_that_never_answered() {
    let bad = FakeTracker::http(Reply::ServerError).await;
    let good = FakeTracker::http(Reply::Peers).await;
    let (m, payload) = torrent(vec![vec![bad.url.clone()], vec![good.url.clone()]]);
    let (s, _h) = seeder(&m, &payload, &good.url).await;
    let port = s.local_addr().port();
    s.shutdown().await;
    let bad_events = bad.events_of(port);
    assert!(!bad_events.is_empty());
    assert!(bad_events.iter().all(|e| e == "started"), "{bad_events:?}");
    assert_eq!(good.events_of(port), ["started", "stopped"]);
}

/// `scrape_trackers` scrapes the first tracker of each tier, over HTTP and
/// UDP, and reports each as an event.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scrape_trackers_reports_counts() {
    let http = FakeTracker::http(Reply::Peers).await;
    let udp = FakeTracker::udp(Reply::Peers).await;
    let (m, payload) = torrent(vec![vec![http.url.clone()], vec![udp.url.clone()]]);
    let (seed, sh) = seeder(&m, &payload, &http.url).await;
    let (leech, _lh, _storage, mut lev) = leecher(&m, payload.len()).await;
    finished(&mut lev).await;
    let lport = leech.local_addr().port();
    assert!(wait_until(|| http.events_of(lport).iter().any(|e| e == "completed"), T).await);

    let mut ev = seed.subscribe(EventMask::TRACKER);
    sh.scrape_trackers().await.unwrap();
    let mut replies = std::collections::HashMap::new();
    while replies.len() < 2 {
        let e = next_matching(&mut ev, |e| matches!(e, Event::ScrapeReply { .. }), T)
            .await
            .expect("scrape replies");
        if let Event::ScrapeReply {
            torrent,
            url,
            complete,
            incomplete,
            downloaded,
        } = e
        {
            assert_eq!(torrent, m.info_hashes);
            replies.insert(url, (complete, incomplete, downloaded));
        }
    }
    // Seed and finished leecher; the UDP tier was never announced to.
    assert_eq!(replies[&http.url], (2, 0, 1));
    assert_eq!(replies[&udp.url], (0, 0, 0));

    // A failing scrape is reported; it never drops the tracker.
    http.set_reply(Reply::Failure("no scrape for you".into()));
    sh.scrape_trackers().await.unwrap();
    let failed = next_matching(&mut ev, |e| matches!(e, Event::ScrapeFailed { .. }), T)
        .await
        .expect("scrape failure");
    assert!(
        matches!(&failed, Event::ScrapeFailed { url, message, .. }
            if *url == http.url && message.contains("no scrape for you")),
        "{failed:?}"
    );
    http.set_reply(Reply::Peers);
    sh.scrape_trackers().await.unwrap();
    next_matching(
        &mut ev,
        |e| matches!(e, Event::ScrapeReply { url, .. } if *url == http.url),
        T,
    )
    .await
    .expect("scraped again");
    leech.shutdown().await;
    seed.shutdown().await;
}

/// A hybrid announces under its v1 hash and its truncated v2 hash, each
/// with its own `started` and `stopped`, and says which in `TrackerReply`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hybrid_announces_both_hashes() {
    use fbt_core::InfoHashes;
    use fbt_metainfo::TorrentVersion;
    let tracker = FakeTracker::http(Reply::Peers).await;
    let dir = tempfile::tempdir().unwrap();
    let bytes = create_torrent(
        &write_v2_payload(dir.path()),
        TorrentVersion::Hybrid,
        32_768,
    );
    let mut m = Metainfo::from_bytes(&bytes).unwrap();
    m.trackers = vec![vec![tracker.url.clone()]];
    let v1 = m.info_hashes.v1.expect("v1 hash").0;
    let v2 = InfoHashes::from_v2(m.info_hashes.v2.expect("v2 hash")).wire_hash();
    assert_ne!(v1, v2);
    let s = Session::new(settings()).await.unwrap();
    let mut ev = s.subscribe(EventMask::TRACKER);
    let h = s
        .add_torrent(AddTorrent::new(m.clone(), dir.path().to_path_buf()))
        .await
        .unwrap();
    let mut replied = std::collections::HashSet::new();
    while replied.len() < 2 {
        let e = next_matching(&mut ev, |e| is_reply(e, &tracker.url), T)
            .await
            .expect("tracker replies");
        if let Event::TrackerReply {
            torrent, info_hash, ..
        } = e
        {
            assert_eq!(torrent, m.info_hashes, "the registry key");
            replied.insert(info_hash);
        }
    }
    assert_eq!(replied, std::collections::HashSet::from([v1, v2]));
    let port = s.local_addr().port();
    let events = |hash: [u8; 20]| -> Vec<String> {
        tracker
            .announces()
            .into_iter()
            .filter(|a| a.port == port && a.info_hash == hash)
            .map(|a| a.event)
            .collect()
    };
    assert_eq!(events(v1), ["started"]);
    assert_eq!(events(v2), ["started"]);
    h.pause().await.unwrap();
    assert!(
        wait_until(|| events(v1).len() == 2 && events(v2).len() == 2, T).await,
        "v1: {:?}, v2: {:?}",
        events(v1),
        events(v2)
    );
    assert_eq!(events(v1), ["started", "stopped"]);
    assert_eq!(events(v2), ["started", "stopped"]);
    s.shutdown().await;
}

/// A v1 torrent announces under its one hash: one announce per interval.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v1_announces_once() {
    let tracker = FakeTracker::http(Reply::Peers).await;
    let (m, payload) = torrent(vec![vec![tracker.url.clone()]]);
    let (s, _h) = seeder(&m, &payload, &tracker.url).await;
    let port = s.local_addr().port();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let announces: Vec<_> = tracker
        .announces()
        .into_iter()
        .filter(|a| a.port == port)
        .collect();
    assert_eq!(announces.len(), 1, "{announces:?}");
    assert_eq!(announces[0].info_hash, m.info_hashes.wire_hash());
    assert_eq!(announces[0].event, "started");
    s.shutdown().await;
}
