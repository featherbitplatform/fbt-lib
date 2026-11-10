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
