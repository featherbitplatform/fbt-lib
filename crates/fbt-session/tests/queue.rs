//! Auto-managed queueing and seed limits: torrents beyond the download and
//! seed limits wait as `Queued` (no peers, no announces), in queue order,
//! and a seed that reaches its ratio or time limit pauses itself.

mod support;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fbt_core::InfoHashes;
use fbt_metainfo::Metainfo;
use fbt_session::storage::MemStorage;
use fbt_session::{
    AddTorrent, Event, EventMask, Priority, QueueSettings, RemoveMode, Session, Settings,
    TorrentHandle, TorrentState,
};
use support::*;

const T: Duration = Duration::from_secs(30);

/// `test_settings()` with at most `downloads` running downloads; the
/// periodic pass is an hour away, so every pass a test sees was triggered
/// by an add, remove, pause, resume, finish or limit change.
fn settings(downloads: Option<usize>) -> Settings {
    let mut s = test_settings();
    let mut q = QueueSettings::default();
    q.active_downloads = downloads;
    q.active_seeds = None;
    q.active_limit = None;
    q.interval = Duration::from_secs(3600);
    s.queue = q;
    s
}

/// A small torrent of random data announcing to `tracker` (if any).
fn torrent(tracker: Option<&str>) -> (Metainfo, Vec<u8>) {
    let (mut m, payload) = make_torrent(&[("a", 100_000)], 16_384);
    if let Some(url) = tracker {
        m.trackers = vec![vec![url.to_owned()]];
    }
    (m, payload)
}

/// Adds `m` on empty memory storage (a download nobody serves).
async fn add_download(s: &Session, m: &Metainfo, len: usize) -> TorrentHandle {
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::new(len as u64)));
    s.add_torrent(p).await.unwrap()
}

/// Adds `m` with its payload in memory (a seed).
async fn add_seed(s: &Session, m: &Metainfo, payload: &[u8]) -> TorrentHandle {
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(payload.to_vec())));
    s.add_torrent(p).await.unwrap()
}

async fn add_downloads(s: &Session, n: usize) -> Vec<TorrentHandle> {
    let mut out = Vec::new();
    for _ in 0..n {
        let (m, payload) = torrent(None);
        out.push(add_download(s, &m, payload.len()).await);
    }
    out
}

fn is(state: TorrentState) -> impl Fn(&TorrentState) -> bool {
    move |s| *s == state
}

fn state_events(ev: &[Event], ih: InfoHashes) -> Vec<TorrentState> {
    ev.iter()
        .filter_map(|e| match e {
            Event::StateChanged { torrent, state } if *torrent == ih => Some(*state),
            _ => None,
        })
        .collect()
}

/// Every event received so far (until none arrives for 100 ms).
async fn drain(ev: &mut fbt_session::EventStream, into: &mut Vec<Event>) {
    while let Ok(Some(e)) = tokio::time::timeout(Duration::from_millis(100), ev.recv()).await {
        into.push(e);
    }
}

/// Positions of `handles` once they settle into a permutation of `0..n`.
async fn dense_positions(handles: &[TorrentHandle]) -> Vec<usize> {
    let n = handles.len();
    let ok = wait_until(
        || {
            let mut p: Vec<usize> = handles.iter().map(|h| h.status().queue_position).collect();
            p.sort_unstable();
            p == (0..n).collect::<Vec<_>>()
        },
        T,
    )
    .await;
    let got: Vec<usize> = handles.iter().map(|h| h.status().queue_position).collect();
    assert!(ok, "positions not a permutation of 0..{n}: {got:?}");
    got
}

/// The fourth download waits as `Queued` (never `Downloading` first, never
/// announced) and starts, announcing `started`, once one of the three
/// running ones finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fourth_download_is_queued() {
    let tracker = FakeTracker::http(Reply::Peers).await;
    let s = Session::new(settings(Some(3))).await.unwrap();
    let port = s.local_addr().port();
    let mut ev = s.subscribe(EventMask::STATUS);
    let mut handles = Vec::new();
    let mut metas = Vec::new();
    for _ in 0..4 {
        let (m, payload) = torrent(Some(&tracker.url));
        handles.push(add_download(&s, &m, payload.len()).await);
        metas.push(m);
    }
    let fourth = handles[3].clone();
    let ih = fourth.info_hashes();
    let wire = ih.wire_hash();
    assert!(wait_state(&fourth, is(TorrentState::Queued), T).await);
    for h in &handles[..3] {
        assert!(wait_state(h, is(TorrentState::Downloading), T).await);
    }
    // The three running ones announced; the queued one never did.
    assert!(
        wait_until(
            || {
                let a = tracker.announces();
                handles[..3].iter().all(|h| {
                    let w = h.info_hashes().wire_hash();
                    a.iter().any(|x| x.info_hash == w && x.event == "started")
                })
            },
            T
        )
        .await
    );
    assert!(tracker.announces().iter().all(|a| a.info_hash != wire));
    let mut seen = Vec::new();
    drain(&mut ev, &mut seen).await;
    let states = state_events(&seen, ih);
    assert!(
        !states.contains(&TorrentState::Downloading),
        "the queued torrent ran first: {states:?}"
    );
    assert_eq!(states.last(), Some(&TorrentState::Queued), "{states:?}");
    // Skipping every file finishes the first one: a seed now, freeing a
    // download slot.
    let files = metas[0].info.files.len();
    handles[0]
        .set_file_priorities(vec![Priority::Skip; files])
        .await
        .unwrap();
    assert!(wait_state(&handles[0], is(TorrentState::Seeding), T).await);
    assert!(wait_state(&fourth, is(TorrentState::Downloading), T).await);
    assert!(
        wait_until(
            || tracker
                .announces()
                .iter()
                .any(|a| a.info_hash == wire && a.event == "started" && a.port == port),
            T
        )
        .await,
        "the started torrent announced"
    );
    s.shutdown().await;
}

/// A queued torrent neither accepts nor dials peers; once it starts it
/// does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_torrent_has_no_peers() {
    let s = Session::new(settings(Some(1))).await.unwrap();
    let _first = add_downloads(&s, 1).await;
    let (m, payload) = torrent(None);
    let queued = add_download(&s, &m, payload.len()).await;
    assert!(wait_state(&queued, is(TorrentState::Queued), T).await);
    // A seed elsewhere dials the queued torrent, which also gets the seed's
    // address to dial.
    let seeder = Session::new(test_settings()).await.unwrap();
    let seed = add_seed(&seeder, &m, &payload).await;
    assert!(wait_state(&seed, is(TorrentState::Seeding), T).await);
    seed.add_peer(s.local_addr()).await.unwrap();
    queued.add_peer(seeder.local_addr()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(queued.status().num_peers, 0);
    assert_eq!(seed.status().num_peers, 0);
    assert_eq!(queued.status().state, TorrentState::Queued);
    // Started, it connects and downloads.
    let mut q = settings(None).queue;
    q.active_downloads = None;
    s.set_queue_settings(q).unwrap();
    queued.add_peer(seeder.local_addr()).await.unwrap();
    assert!(wait_state(&queued, is(TorrentState::Seeding), T).await);
    s.shutdown().await;
    seeder.shutdown().await;
}

/// Positions are dense in add order and close up after a removal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn positions_stay_dense() {
    let s = Session::new(settings(Some(2))).await.unwrap();
    let mut handles = add_downloads(&s, 5).await;
    assert_eq!(dense_positions(&handles).await, [0, 1, 2, 3, 4]);
    let gone = handles.remove(1);
    gone.remove(RemoveMode::KeepData).await.unwrap();
    assert!(
        wait_until(
            || handles
                .iter()
                .map(|h| h.status().queue_position)
                .eq([0, 1, 2, 3]),
            T
        )
        .await,
        "positions after removal: {:?}",
        handles
            .iter()
            .map(|h| h.status().queue_position)
            .collect::<Vec<_>>()
    );
    // The first two run, by position: the old third moved up into a slot.
    assert!(wait_state(&handles[1], is(TorrentState::Downloading), T).await);
    for h in &handles[2..] {
        assert!(wait_state(h, is(TorrentState::Queued), T).await);
    }
    // Moves keep them dense.
    handles[3].set_queue_position(0).await.unwrap();
    handles[0].set_queue_position(usize::MAX).await.unwrap();
    let p = dense_positions(&handles).await;
    assert_eq!(p[3], 0);
    assert_eq!(p[0], 3);
    s.shutdown().await;
}

/// Moving a queued torrent to the front starts it and queues the one it
/// passed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_queue_position_moves_it_up() {
    let s = Session::new(settings(Some(1))).await.unwrap();
    let h = add_downloads(&s, 3).await;
    assert!(wait_state(&h[0], is(TorrentState::Downloading), T).await);
    assert!(wait_state(&h[2], is(TorrentState::Queued), T).await);
    h[2].set_queue_position(0).await.unwrap();
    // Returned once applied: its own position is already there.
    assert_eq!(h[2].status().queue_position, 0);
    assert_eq!(dense_positions(&h).await, [1, 2, 0]);
    assert!(wait_state(&h[2], is(TorrentState::Downloading), T).await);
    assert!(wait_state(&h[0], is(TorrentState::Queued), T).await);
    assert_eq!(h[1].status().state, TorrentState::Queued);
    // Clamped: past the end is the bottom.
    h[2].set_queue_position(99).await.unwrap();
    assert_eq!(h[2].status().queue_position, 2);
    assert!(wait_state(&h[0], is(TorrentState::Downloading), T).await);
    s.shutdown().await;
}

/// A torrent the user paused is never started by the queue, even with
/// free slots; resumed, it takes its place in the queue again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_pause_beats_the_queue() {
    let s = Session::new(settings(Some(2))).await.unwrap();
    let (m, payload) = torrent(None);
    let mut p = AddTorrent::new(m, PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::new(payload.len() as u64)));
    p.paused = true;
    let paused = s.add_torrent(p).await.unwrap();
    let rest = add_downloads(&s, 2).await;
    assert_eq!(paused.status().queue_position, 0);
    // Ahead in the queue, but it takes no slot: both others run.
    for h in &rest {
        assert!(wait_state(h, is(TorrentState::Downloading), T).await);
    }
    assert!(wait_state(&paused, is(TorrentState::Paused), T).await);
    // No limit at all: still paused.
    let mut q = settings(None).queue;
    q.active_downloads = None;
    s.set_queue_settings(q.clone()).unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(paused.status().state, TorrentState::Paused);
    // Back to two slots; resumed, it runs from position 0 and the last one
    // is queued.
    q.active_downloads = Some(2);
    s.set_queue_settings(q).unwrap();
    paused.resume().await.unwrap();
    assert!(wait_state(&paused, is(TorrentState::Downloading), T).await);
    assert!(wait_state(&rest[1], is(TorrentState::Queued), T).await);
    assert_eq!(rest[0].status().state, TorrentState::Downloading);
    // Paused again: the queued one gets its slot back.
    paused.pause().await.unwrap();
    assert!(wait_state(&rest[1], is(TorrentState::Downloading), T).await);
    assert_eq!(paused.status().state, TorrentState::Paused);
    s.shutdown().await;
}

/// A seed reaching its share ratio pauses, stops being auto-managed and
/// says so with `SeedLimitReached`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seed_ratio_limit_pauses_and_emits() {
    let mut cfg = settings(None);
    cfg.seed_ratio_limit = Some(0.5);
    let seeder = Session::new(cfg).await.unwrap();
    let mut ev = seeder.subscribe(EventMask::STATUS);
    let (m, payload) = make_torrent(&[("a", 1_000_000)], 16_384);
    let seed = add_seed(&seeder, &m, &payload).await;
    assert!(wait_state(&seed, is(TorrentState::Seeding), T).await);
    assert!(seed.status().auto_managed);
    let leecher = Session::new(test_settings()).await.unwrap();
    let leech = add_download(&leecher, &m, payload.len()).await;
    leech.add_peer(seeder.local_addr()).await.unwrap();
    let ih = seed.info_hashes();
    next_matching(
        &mut ev,
        |e| matches!(e, Event::SeedLimitReached { torrent } if *torrent == ih),
        T,
    )
    .await
    .expect("SeedLimitReached");
    assert!(wait_state(&seed, is(TorrentState::Paused), T).await);
    let st = seed.status();
    assert!(!st.auto_managed);
    assert!(
        st.total_uploaded * 2 >= st.total_wanted,
        "paused at ratio {} / {}",
        st.total_uploaded,
        st.total_wanted
    );
    // Resumed by the user, it seeds on: the limit applies to auto-managed
    // torrents only.
    seed.resume().await.unwrap();
    assert!(wait_state(&seed, is(TorrentState::Seeding), T).await);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(seed.status().state, TorrentState::Seeding);
    leecher.shutdown().await;
    seeder.shutdown().await;
}

/// A seed reaching its seeding time pauses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seed_time_limit_pauses() {
    let mut cfg = settings(None);
    cfg.seed_time_limit = Some(Duration::from_millis(300));
    let s = Session::new(cfg).await.unwrap();
    let mut ev = s.subscribe(EventMask::STATUS);
    let (m, payload) = torrent(None);
    let seed = add_seed(&s, &m, &payload).await;
    let ih = seed.info_hashes();
    next_matching(
        &mut ev,
        |e| matches!(e, Event::SeedLimitReached { torrent } if *torrent == ih),
        T,
    )
    .await
    .expect("SeedLimitReached");
    assert!(wait_state(&seed, is(TorrentState::Paused), T).await);
    let st = seed.status();
    assert!(!st.auto_managed);
    assert!(st.seeding_time >= Duration::from_millis(300), "{st:?}");
    assert!(st.active_time >= st.seeding_time, "{st:?}");
    s.shutdown().await;
}

/// Queueing a running torrent announces `stopped` to the tracker that took
/// its `started`; starting it again announces `started`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_torrent_announces_stopped() {
    let tracker = FakeTracker::http(Reply::Peers).await;
    let s = Session::new(settings(None)).await.unwrap();
    let port = s.local_addr().port();
    let (m, payload) = torrent(Some(&tracker.url));
    let h = add_download(&s, &m, payload.len()).await;
    assert!(wait_state(&h, is(TorrentState::Downloading), T).await);
    assert!(wait_until(|| tracker.events_of(port) == ["started"], T).await);
    let mut q = settings(None).queue;
    q.active_downloads = Some(0);
    s.set_queue_settings(q.clone()).unwrap();
    assert!(wait_state(&h, is(TorrentState::Queued), T).await);
    assert!(
        wait_until(|| tracker.events_of(port) == ["started", "stopped"], T).await,
        "{:?}",
        tracker.events_of(port)
    );
    assert_eq!(h.status().num_peers, 0);
    q.active_downloads = None;
    s.set_queue_settings(q).unwrap();
    assert!(wait_state(&h, is(TorrentState::Downloading), T).await);
    assert!(
        wait_until(
            || tracker.events_of(port) == ["started", "stopped", "started"],
            T
        )
        .await,
        "{:?}",
        tracker.events_of(port)
    );
    s.shutdown().await;
}

/// Raising a limit starts the queued torrent at once, without waiting for
/// the periodic pass (an hour away here); invalid settings are refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn limits_change_at_runtime() {
    let s = Session::new(settings(Some(1))).await.unwrap();
    let h = add_downloads(&s, 2).await;
    assert!(wait_state(&h[1], is(TorrentState::Queued), T).await);
    let mut q = settings(None).queue;
    q.active_downloads = Some(2);
    s.set_queue_settings(q.clone()).unwrap();
    assert!(
        wait_state(
            &h[1],
            is(TorrentState::Downloading),
            Duration::from_secs(10)
        )
        .await
    );
    assert_eq!(h[0].status().state, TorrentState::Downloading);
    // Lowered: the last one in the queue stops.
    q.active_downloads = Some(1);
    s.set_queue_settings(q.clone()).unwrap();
    assert!(wait_state(&h[1], is(TorrentState::Queued), Duration::from_secs(10)).await);
    q.interval = Duration::ZERO;
    assert!(s.set_queue_settings(q).is_err());
    // Not auto-managed: out of the queue's hands, so it runs.
    h[1].set_auto_managed(false).await.unwrap();
    assert!(!h[1].status().auto_managed);
    assert!(wait_state(&h[1], is(TorrentState::Downloading), T).await);
    assert_eq!(h[0].status().state, TorrentState::Downloading);
    s.shutdown().await;
}
