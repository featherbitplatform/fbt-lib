//! Session and torrent lifecycle: adding, checking, pausing, removing,
//! storage errors, events and shutdown.

mod support;

use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use fbt_session::{AddTorrent, Event, EventMask, RemoveMode, Session, SessionError, TorrentState};
use fbt_wire::Message;
use support::*;

const T: Duration = Duration::from_secs(10);

fn is_state(e: &Event, want: fn(&TorrentState) -> bool) -> bool {
    matches!(e, Event::StateChanged { state, .. } if want(state))
}

#[tokio::test]
async fn add_then_status_downloading_with_no_data() {
    let s = Session::new(test_settings()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = make_torrent(&[("a", 100_000), ("b", 7)], 16_384);
    let mut ev = s.subscribe(EventMask::ALL);
    let h = s
        .add_torrent(AddTorrent::new(m.clone(), dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(matches!(
        ev.recv().await,
        Some(Event::TorrentAdded { torrent }) if torrent == m.info_hashes
    ));
    next_matching(
        &mut ev,
        |e| is_state(e, |s| *s == TorrentState::Downloading),
        T,
    )
    .await
    .expect("Downloading");
    let st = h.status();
    assert_eq!(st.state, TorrentState::Downloading);
    assert_eq!(st.info_hashes, m.info_hashes);
    assert_eq!(st.name, "torrent");
    assert_eq!(st.total_wanted, 100_007);
    assert_eq!(st.wanted_done, 0);
    assert_eq!(st.progress, 0.0);
    assert_eq!(st.num_peers, 0);
    assert!(st.error.is_none());
    assert_eq!(h.info_hashes(), m.info_hashes);
    assert_eq!(h.pieces().await.unwrap().count_ones(), 0);
    assert!(s.torrent(&m.info_hashes).is_some());
    assert_eq!(s.torrents().len(), 1);
    assert_eq!(s.torrents_status().len(), 1);
    s.shutdown().await;
}

#[tokio::test]
async fn add_with_complete_data_becomes_seeding() {
    let s = Session::new(test_settings()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (m, payload) = make_torrent(&[("a", 40_000), ("sub/b", 30_000)], 16_384);
    write_payload(dir.path(), &m, &payload);
    let h = s
        .add_torrent(AddTorrent::new(m.clone(), dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    let st = h.status();
    assert_eq!(st.progress, 1.0);
    assert_eq!(st.wanted_done, 70_000);
    assert!(h.pieces().await.unwrap().is_full());
    s.shutdown().await;
}

#[tokio::test]
async fn duplicate_add_rejected() {
    let s = Session::new(test_settings()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = make_torrent(&[("a", 1000)], 16_384);
    s.add_torrent(AddTorrent::new(m.clone(), dir.path().to_path_buf()))
        .await
        .unwrap();
    let e = s
        .add_torrent(AddTorrent::new(m, dir.path().to_path_buf()))
        .await
        .unwrap_err();
    assert!(matches!(e, SessionError::DuplicateTorrent), "{e:?}");
    s.shutdown().await;
}

#[tokio::test]
async fn invalid_priorities_rejected() {
    let s = Session::new(test_settings()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = make_torrent(&[("a", 1000), ("b", 1000)], 16_384);
    let h = s
        .add_torrent(AddTorrent::new(m, dir.path().to_path_buf()))
        .await
        .unwrap();
    let e = h
        .set_file_priorities(vec![fbt_session::Priority::Skip])
        .await
        .unwrap_err();
    assert!(
        matches!(
            e,
            SessionError::InvalidPriorities {
                expected: 2,
                got: 1
            }
        ),
        "{e:?}"
    );
    s.shutdown().await;
}

#[tokio::test]
async fn pause_resume_state_events() {
    let s = Session::new(test_settings()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = make_torrent(&[("a", 50_000)], 16_384);
    let mut ev = s.subscribe(EventMask::STATUS);
    let h = s
        .add_torrent(AddTorrent::new(m, dir.path().to_path_buf()))
        .await
        .unwrap();
    next_matching(
        &mut ev,
        |e| is_state(e, |s| *s == TorrentState::Downloading),
        T,
    )
    .await
    .expect("Downloading");
    h.pause().await.unwrap();
    next_matching(&mut ev, |e| is_state(e, |s| *s == TorrentState::Paused), T)
        .await
        .expect("Paused");
    assert_eq!(h.status().state, TorrentState::Paused);
    h.resume().await.unwrap();
    next_matching(
        &mut ev,
        |e| is_state(e, |s| *s == TorrentState::Downloading),
        T,
    )
    .await
    .expect("Downloading again");
    assert_eq!(h.status().state, TorrentState::Downloading);
    s.shutdown().await;
}

#[tokio::test]
async fn added_paused_stays_paused_after_checking() {
    let s = Session::new(test_settings()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = make_torrent(&[("a", 50_000)], 16_384);
    let mut p = AddTorrent::new(m, dir.path().to_path_buf());
    p.paused = true;
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Paused, T).await);
    s.shutdown().await;
}

#[tokio::test]
async fn remove_delete_data_removes_files() {
    let s = Session::new(test_settings()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (m, payload) = make_torrent(&[("a", 40_000), ("b", 3000)], 16_384);
    write_payload(dir.path(), &m, &payload);
    let mut ev = s.subscribe(EventMask::STATUS);
    let h = s
        .add_torrent(AddTorrent::new(m.clone(), dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    let root = dir.path().join("torrent");
    assert!(root.join("a").exists());
    h.remove(RemoveMode::DeleteData).await.unwrap();
    next_matching(&mut ev, |e| matches!(e, Event::TorrentRemoved { .. }), T)
        .await
        .expect("TorrentRemoved");
    assert!(!root.join("a").exists());
    assert!(!root.join("b").exists());
    assert!(s.torrent(&m.info_hashes).is_none());
    assert!(s.torrents().is_empty());
    assert!(matches!(
        h.pause().await,
        Err(SessionError::TorrentNotFound)
    ));
    s.shutdown().await;
}

#[tokio::test]
async fn checking_reports_progress() {
    let s = Session::new(test_settings()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (m, payload) = make_torrent(&[("big", 64 * 65_536)], 65_536);
    assert_eq!(m.info.pieces.len(), 64);
    write_payload(dir.path(), &m, &payload);
    let mut ev = s.subscribe(EventMask::STATUS);
    let h = s
        .add_torrent(AddTorrent::new(m, dir.path().to_path_buf()))
        .await
        .unwrap();
    next_matching(
        &mut ev,
        |e| is_state(e, |s| matches!(s, TorrentState::CheckingFiles { .. })),
        T,
    )
    .await
    .expect("CheckingFiles observed");
    next_matching(&mut ev, |e| is_state(e, |s| *s == TorrentState::Seeding), T)
        .await
        .expect("Seeding after the check");
    assert_eq!(h.status().progress, 1.0);
    s.shutdown().await;
}

/// A save path the torrent cannot write below.
struct Unwritable {
    _dir: tempfile::TempDir,
    path: PathBuf,
}

#[cfg(unix)]
fn unwritable_save_path() -> Unwritable {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ro");
    std::fs::create_dir(&path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o555)).unwrap();
    Unwritable { _dir: dir, path }
}

#[cfg(unix)]
impl Drop for Unwritable {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(0o755));
    }
}

#[cfg(windows)]
fn unwritable_save_path() -> Unwritable {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("file");
    std::fs::write(&file, b"x").unwrap();
    let path = file.join("sub");
    Unwritable { _dir: dir, path }
}

#[tokio::test]
async fn unwritable_save_path_puts_torrent_in_error() {
    let s = Session::new(test_settings()).await.unwrap();
    let mut ev = s.subscribe(EventMask::ALL);

    // A healthy seeding torrent in the same session.
    let good_dir = tempfile::tempdir().unwrap();
    let (gm, gpayload) = make_torrent(&[("g", 20_000)], 16_384);
    write_payload(good_dir.path(), &gm, &gpayload);
    let good = s
        .add_torrent(AddTorrent::new(gm, good_dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&good, |s| *s == TorrentState::Seeding, T).await);

    // One 16 KiB piece, fed by a scripted peer, that cannot be written.
    let bad_path = unwritable_save_path();
    let (m, payload) = make_torrent(&[("a", 16_384)], 16_384);
    let bad = s
        .add_torrent(AddTorrent::new(m.clone(), bad_path.path.clone()))
        .await
        .unwrap();
    assert!(wait_state(&bad, |s| *s == TorrentState::Downloading, T).await);

    let mut peer = ScriptedPeer::connect(s.local_addr(), m.info_hashes.wire_hash()).await;
    assert_eq!(peer.remote.info_hash, m.info_hashes.wire_hash());
    peer.send(Message::Bitfield(Bytes::from_static(&[0x80])))
        .await;
    peer.send(Message::Unchoke).await;
    let Some(Message::Request(block)) = peer
        .next_matching(|m| matches!(m, Message::Request(_)))
        .await
    else {
        panic!("no request from the session");
    };
    assert_eq!((block.piece, block.offset, block.len), (0, 0, 16_384));
    peer.send(Message::Piece {
        piece: 0,
        offset: 0,
        data: Bytes::copy_from_slice(&payload),
    })
    .await;

    let fe = next_matching(&mut ev, |e| matches!(e, Event::FileError { .. }), T)
        .await
        .expect("FileError");
    assert!(matches!(fe, Event::FileError { torrent, .. } if torrent == m.info_hashes));
    // The file that could not be written is named.
    let Event::FileError { path, .. } = fe else {
        unreachable!()
    };
    assert_eq!(
        path.as_deref(),
        Some(
            bad_path
                .path
                .join(m.info.files[0].path.to_path_buf())
                .as_path()
        )
    );
    assert!(wait_state(&bad, |s| *s == TorrentState::Error, T).await);
    assert!(bad.status().error.is_some());
    // The peer is disconnected.
    assert_eq!(peer.wait_closed(T).await, PeerEnd::Closed);
    // The other torrent is unaffected.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(good.status().state, TorrentState::Seeding);
    s.shutdown().await;
}

#[tokio::test]
async fn event_mask_filters_and_lag_reported() {
    // Filtering: a PEER-only subscriber sees none of the status traffic.
    let s = Session::new(test_settings()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = make_torrent(&[("a", 1000)], 16_384);
    let mut peers_only = s.subscribe(EventMask::PEER);
    let mut status = s.subscribe(EventMask::STATUS | EventMask::STORAGE);
    let h = s
        .add_torrent(AddTorrent::new(m, dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(matches!(
        status.recv().await,
        Some(Event::TorrentAdded { .. })
    ));
    h.pause().await.unwrap();
    next_matching(
        &mut status,
        |e| is_state(e, |s| *s == TorrentState::Paused),
        T,
    )
    .await
    .expect("Paused");
    assert!(
        next_matching(&mut peers_only, |_| true, Duration::from_millis(300))
            .await
            .is_none()
    );
    assert_eq!(
        Event::TorrentAdded {
            torrent: h.info_hashes()
        }
        .category(),
        EventMask::STATUS
    );
    s.shutdown().await;

    // Lag: a subscriber that never reads a 4-slot queue is told what it missed.
    let mut settings = test_settings();
    settings.event_queue = 4;
    let s = Session::new(settings).await.unwrap();
    let mut lagging = s.subscribe(EventMask::PEER);
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = make_torrent(&[("a", 1000)], 16_384);
    let h = s
        .add_torrent(AddTorrent::new(m, dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);
    for _ in 0..3 {
        h.pause().await.unwrap();
        h.resume().await.unwrap();
    }
    match lagging.recv().await {
        Some(Event::Lagged { missed }) => assert!(missed > 0),
        other => panic!("expected Lagged, got {other:?}"),
    }
    s.shutdown().await;
}

#[tokio::test]
async fn shutdown_is_idempotent_and_rejects_new_commands() {
    let s = Session::new(test_settings()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = make_torrent(&[("a", 1000)], 16_384);
    let h = s
        .add_torrent(AddTorrent::new(m, dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);
    s.shutdown().await;
    s.clone().shutdown().await;
    assert!(matches!(h.pause().await, Err(SessionError::Shutdown)));
    assert!(matches!(h.resume().await, Err(SessionError::Shutdown)));
    assert!(matches!(h.pieces().await, Err(SessionError::Shutdown)));
    assert!(matches!(
        h.add_peer("127.0.0.1:1".parse().unwrap()).await,
        Err(SessionError::Shutdown)
    ));
    let (m2, _) = make_torrent(&[("b", 1000)], 16_384);
    assert!(matches!(
        s.add_torrent(AddTorrent::new(m2, dir.path().to_path_buf()))
            .await,
        Err(SessionError::Shutdown)
    ));
    // The last snapshot stays readable.
    assert_eq!(h.status().name, "torrent");
}

/// Smoke test of the data path between two sessions; the full swarm suite
/// lives in `swarm.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leecher_downloads_from_seeding_session() {
    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    let (m, payload) = make_torrent(&[("a", 300_000), ("e", 0), ("b", 123_457)], 32_768);
    write_payload(seed_dir.path(), &m, &payload);
    let seeder = Session::new(test_settings()).await.unwrap();
    let leecher = Session::new(test_settings()).await.unwrap();
    let sh = seeder
        .add_torrent(AddTorrent::new(m.clone(), seed_dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&sh, |s| *s == TorrentState::Seeding, T).await);
    let mut ev = leecher.subscribe(EventMask::STATUS | EventMask::PIECE | EventMask::PEER);
    let lh = leecher
        .add_torrent(AddTorrent::new(m.clone(), leech_dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&lh, |s| *s == TorrentState::Downloading, T).await);
    lh.add_peer(seeder.local_addr()).await.unwrap();
    next_matching(&mut ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    assert!(wait_state(&lh, |s| *s == TorrentState::Seeding, T).await);
    let mut got = Vec::new();
    for f in ["a", "e", "b"] {
        got.extend(std::fs::read(leech_dir.path().join("torrent").join(f)).unwrap());
    }
    assert!(got == payload, "downloaded bytes differ");
    let st = lh.status();
    assert_eq!(st.total_downloaded, payload.len() as u64);
    assert_eq!(st.num_peers, 1);
    assert_eq!(st.num_seeds, 1);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(sh.status().total_uploaded >= payload.len() as u64);
    leecher.shutdown().await;
    seeder.shutdown().await;
}

/// Memory storage whose writes at offset 0 fail while `fail` is set.
#[derive(Debug)]
struct FlakyStorage {
    inner: fbt_session::storage::MemStorage,
    fail: std::sync::atomic::AtomicBool,
}

impl fbt_session::storage::Storage for FlakyStorage {
    fn read(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
        self.inner.read(offset, buf)
    }
    fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        if offset == 0 && self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(std::io::Error::other("injected write failure"));
        }
        self.inner.write(offset, data)
    }
    fn flush(&self) -> std::io::Result<()> {
        Ok(())
    }
    fn delete_files(&self) -> std::io::Result<()> {
        self.inner.delete_files()
    }
}

/// An error hit while other pieces are being hashed and written; resume
/// rechecks from storage and the torrent still finishes with correct data.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resume_after_error_rechecks_and_finishes() {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    let s = Session::new(test_settings()).await.unwrap();
    let mut ev = s.subscribe(EventMask::ALL);
    let (m, payload) = make_torrent(&[("a", 8 * 16_384)], 16_384);
    let storage = Arc::new(FlakyStorage {
        inner: fbt_session::storage::MemStorage::new(payload.len() as u64),
        fail: std::sync::atomic::AtomicBool::new(true),
    });
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(storage.clone());
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);

    // The first peer delivers every piece; the write of piece 0 fails while
    // the others are in flight.
    let mut peer = ScriptedPeer::connect(s.local_addr(), m.info_hashes.wire_hash()).await;
    peer.send(Message::Bitfield(Bytes::from_static(&[0xff])))
        .await;
    peer.send(Message::Unchoke).await;
    let data = payload.clone();
    let first = tokio::spawn(async move { peer.serve(&data, 16_384).await });
    next_matching(&mut ev, |e| matches!(e, Event::FileError { .. }), T)
        .await
        .expect("FileError");
    assert!(wait_state(&h, |s| *s == TorrentState::Error, T).await);
    tokio::time::timeout(T, first)
        .await
        .expect("first peer closed")
        .unwrap();

    storage.fail.store(false, Ordering::SeqCst);
    h.resume().await.unwrap();
    next_matching(
        &mut ev,
        |e| is_state(e, |s| matches!(s, TorrentState::CheckingFiles { .. })),
        T,
    )
    .await
    .expect("resume rechecks");
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);
    assert!(h.status().error.is_none());

    let mut peer = ScriptedPeer::connect(s.local_addr(), m.info_hashes.wire_hash()).await;
    peer.send(Message::Bitfield(Bytes::from_static(&[0xff])))
        .await;
    peer.send(Message::Unchoke).await;
    let data = payload.clone();
    tokio::spawn(async move { peer.serve(&data, 16_384).await });
    next_matching(&mut ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    let mut got = vec![0u8; payload.len()];
    fbt_session::storage::Storage::read(&*storage, 0, &mut got).unwrap();
    assert!(got == payload, "stored bytes differ");
    assert!(h.pieces().await.unwrap().is_full());
    s.shutdown().await;
}

/// A torrent whose check takes about 6.4 s (64 reads of 100 ms each).
async fn slow_checking(s: &Session) -> fbt_session::TorrentHandle {
    use std::sync::Arc;
    let (m, payload) = make_torrent(&[("a", 64 * 16_384)], 16_384);
    let mut p = AddTorrent::new(m, PathBuf::from("unused"));
    p.storage = Some(Arc::new(SlowStorage::new(
        payload,
        Duration::from_millis(100),
    )));
    let h = s.add_torrent(p).await.unwrap();
    assert!(matches!(
        h.status().state,
        TorrentState::CheckingFiles { .. }
    ));
    h
}

/// Shutdown and remove during a long check (and after rechecks that would
/// otherwise stack up) return without waiting for the checks to finish.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_during_check_is_prompt() {
    let s = Session::new(test_settings()).await.unwrap();
    let h = slow_checking(&s).await;
    for _ in 0..3 {
        h.force_recheck().await.unwrap();
    }
    let t0 = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(5), s.shutdown())
        .await
        .expect("shutdown in time");
    assert!(t0.elapsed() < Duration::from_secs(2), "{:?}", t0.elapsed());

    let s = Session::new(test_settings()).await.unwrap();
    let h = slow_checking(&s).await;
    let t0 = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(5), h.remove(RemoveMode::KeepData))
        .await
        .expect("remove in time")
        .unwrap();
    assert!(t0.elapsed() < Duration::from_secs(2), "{:?}", t0.elapsed());
    s.shutdown().await;
}

/// Dropping a `remove` future after the command went out still removes the
/// torrent from the session: it can be added again afterwards.
#[tokio::test]
async fn dropped_remove_future_still_unregisters() {
    use futures_util::FutureExt;
    let s = Session::new(test_settings()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = make_torrent(&[("a", 1000)], 16_384);
    let mut ev = s.subscribe(EventMask::STATUS);
    let h = s
        .add_torrent(AddTorrent::new(m.clone(), dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);
    // Polled once: the command is queued, then the future is dropped.
    assert!(h.remove(RemoveMode::KeepData).now_or_never().is_none());
    next_matching(&mut ev, |e| matches!(e, Event::TorrentRemoved { .. }), T)
        .await
        .expect("TorrentRemoved");
    assert!(s.torrent(&m.info_hashes).is_none());
    s.add_torrent(AddTorrent::new(m, dir.path().to_path_buf()))
        .await
        .expect("re-added");
    s.shutdown().await;
}

/// Settings that would panic or spin are refused with `InvalidSettings`.
#[tokio::test]
async fn invalid_settings_are_rejected() {
    type Tweak = fn(&mut fbt_session::Settings);
    let bad: [(&str, Tweak); 14] = [
        ("status_interval", |s| s.status_interval = Duration::ZERO),
        ("rechoke_interval", |s| s.rechoke_interval = Duration::ZERO),
        ("keepalive_interval", |s| {
            s.keepalive_interval = Duration::ZERO
        }),
        ("request_timeout", |s| s.request_timeout = Duration::ZERO),
        ("peer_idle_timeout", |s| {
            s.peer_idle_timeout = Duration::ZERO
        }),
        ("tracker_retry_base", |s| {
            s.tracker_retry_base = Duration::ZERO
        }),
        ("event_queue", |s| s.event_queue = 0),
        ("event_queue", |s| s.event_queue = usize::MAX),
        ("max_incoming_requests", |s| {
            s.max_incoming_requests = usize::MAX
        }),
        ("max_incoming_requests", |s| s.max_incoming_requests = 0),
        ("disk_threads", |s| s.disk_threads = 0),
        ("disk_threads", |s| {
            s.disk_threads = fbt_session::MAX_DISK_THREADS + 1
        }),
        ("max_active_checks", |s| s.max_active_checks = 0),
        ("max_active_checks", |s| s.max_active_checks = 65),
    ];
    for (field, tweak) in bad {
        let mut st = test_settings();
        tweak(&mut st);
        match Session::new(st).await {
            Err(SessionError::InvalidSettings(msg)) => {
                assert!(msg.contains(field), "{field}: {msg}");
            }
            other => panic!("{field}: expected InvalidSettings, got {other:?}"),
        }
    }
}

/// Extreme but meaningful settings work without panicking.
#[tokio::test]
async fn extreme_settings_do_not_panic() {
    // It dials a closed port below: over TCP alone, refused at once.
    let mut st = dial_tcp(test_settings());
    st.max_peers_global = usize::MAX;
    st.max_peers_per_torrent = usize::MAX;
    st.max_half_open = usize::MAX;
    st.unchoke_slots = usize::MAX;
    st.max_outstanding_requests = usize::MAX;
    st.max_incoming_requests = 1 << 16;
    st.rechoke_interval = Duration::MAX;
    st.keepalive_interval = Duration::MAX;
    st.peer_idle_timeout = Duration::MAX;
    st.request_timeout = Duration::MAX;
    st.connect_timeout = Duration::MAX;
    st.handshake_timeout = Duration::MAX;
    st.peer_retry_delay = Duration::MAX;
    st.tracker_retry_base = Duration::MAX;
    st.event_queue = 1 << 16;
    let s = Session::new(st).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (m, payload) = make_torrent(&[("a", 40_000)], 16_384);
    write_payload(dir.path(), &m, &payload);
    let h = s
        .add_torrent(AddTorrent::new(m.clone(), dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    // A peer connects through the listener and is served.
    let mut peer = ScriptedPeer::connect(s.local_addr(), m.info_hashes.wire_hash()).await;
    peer.send(Message::Interested).await;
    peer.next_matching(|m| *m == Message::Unchoke)
        .await
        .expect("unchoked");
    // A failed outgoing connection schedules its retry "never".
    let closed = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let mut ev = s.subscribe(EventMask::PEER);
    h.add_peer(closed).await.unwrap();
    next_matching(
        &mut ev,
        |e| matches!(e, Event::PeerDisconnected { addr, .. } if *addr == closed),
        T,
    )
    .await
    .expect("connect failed");
    assert_eq!(h.status().state, TorrentState::Seeding, "actor alive");
    h.pieces().await.expect("actor alive");
    s.shutdown().await;
}

// ----- incoming handshakes through the listener ------------------------------

/// A seeding session with a short handshake timeout; returns its torrent's
/// wire hash.
async fn listener_session() -> (Session, [u8; 20]) {
    let mut st = test_settings();
    st.handshake_timeout = Duration::from_millis(500);
    let s = Session::new(st).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (m, payload) = make_torrent(&[("a", 40_000)], 16_384);
    let mut p = AddTorrent::new(m.clone(), dir.path().to_path_buf());
    p.storage = Some(std::sync::Arc::new(
        fbt_session::storage::MemStorage::from_bytes(payload),
    ));
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    (s, m.info_hashes.wire_hash())
}

fn raw_handshake(info_hash: [u8; 20]) -> Vec<u8> {
    let mut v = vec![19u8];
    v.extend_from_slice(b"BitTorrent protocol");
    v.extend_from_slice(&[0u8; 8]);
    v.extend_from_slice(&info_hash);
    v.extend_from_slice(&[0x24; 20]);
    v
}

/// Reads until the remote closes within `within`; returns the bytes read,
/// or `None` if it stayed open.
async fn read_to_close(s: &mut tokio::net::TcpStream, within: Duration) -> Option<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    tokio::time::timeout(within, async {
        let mut got = Vec::new();
        let mut buf = [0u8; 1024];
        loop {
            match s.read(&mut buf).await {
                Ok(0) | Err(_) => return got,
                Ok(n) => got.extend_from_slice(&buf[..n]),
            }
        }
    })
    .await
    .ok()
}

async fn no_peer_connected(ev: &mut fbt_session::EventStream) -> bool {
    next_matching(
        ev,
        |e| matches!(e, Event::PeerConnected { .. }),
        Duration::from_millis(200),
    )
    .await
    .is_none()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incoming_unknown_info_hash_is_dropped() {
    use tokio::io::AsyncWriteExt;
    let (s, _) = listener_session().await;
    let mut ev = s.subscribe(EventMask::PEER);
    let mut tcp = tokio::net::TcpStream::connect(s.local_addr())
        .await
        .unwrap();
    tcp.write_all(&raw_handshake([0xab; 20])).await.unwrap();
    let got = read_to_close(&mut tcp, Duration::from_secs(2))
        .await
        .expect("closed");
    assert!(got.is_empty(), "no handshake for an unknown torrent");
    assert!(no_peer_connected(&mut ev).await);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incoming_garbage_is_dropped() {
    use tokio::io::AsyncWriteExt;
    let (s, ih) = listener_session().await;
    let mut ev = s.subscribe(EventMask::PEER);
    // Neither a BitTorrent handshake nor a valid MSE exchange.
    let mut tcp = tokio::net::TcpStream::connect(s.local_addr())
        .await
        .unwrap();
    tcp.write_all(&[0xff; 300]).await.unwrap();
    assert!(
        read_to_close(&mut tcp, Duration::from_secs(2))
            .await
            .is_some(),
        "garbage connection still open"
    );
    // A close right after a short write is dropped too.
    let mut tcp = tokio::net::TcpStream::connect(s.local_addr())
        .await
        .unwrap();
    tcp.write_all(b"GET / HTTP/1.1").await.unwrap();
    tcp.shutdown().await.unwrap();
    assert!(
        read_to_close(&mut tcp, Duration::from_secs(2))
            .await
            .is_some()
    );
    assert!(no_peer_connected(&mut ev).await);
    // The listener still serves real peers.
    let p = ScriptedPeer::connect(s.local_addr(), ih).await;
    assert_eq!(p.remote.info_hash, ih);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incoming_slow_handshake_times_out() {
    use tokio::io::AsyncWriteExt;
    let (s, ih) = listener_session().await;
    let mut tcp = tokio::net::TcpStream::connect(s.local_addr())
        .await
        .unwrap();
    // Half a plaintext handshake, then nothing.
    tcp.write_all(&raw_handshake(ih)[..30]).await.unwrap();
    let t0 = tokio::time::Instant::now();
    let got = read_to_close(&mut tcp, Duration::from_secs(2))
        .await
        .expect("closed at the handshake timeout");
    assert!(got.is_empty());
    assert!(
        t0.elapsed() >= Duration::from_millis(300),
        "closed too early"
    );
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incoming_valid_plaintext_handshake_is_routed() {
    let (s, ih) = listener_session().await;
    let mut ev = s.subscribe(EventMask::PEER);
    let p = ScriptedPeer::connect(s.local_addr(), ih).await;
    assert_eq!(p.remote.info_hash, ih);
    assert_eq!(p.remote.peer_id.0[..3], *b"-FB");
    next_matching(&mut ev, |e| matches!(e, Event::PeerConnected { .. }), T)
        .await
        .expect("PeerConnected");
    s.shutdown().await;
}

/// True when the host can bind `[::1]`; IPv6 tests return early otherwise.
fn ipv6_available() -> bool {
    std::net::TcpListener::bind("[::1]:0").is_ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listens_on_every_address() {
    if !ipv6_available() {
        return;
    }
    let mut st = test_settings();
    st.listen_addrs = vec![
        std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        "[::1]:0".parse().unwrap(),
    ];
    let s = Session::new(st).await.unwrap();
    let addrs = s.local_addrs();
    assert_eq!(addrs.len(), 2, "{addrs:?}");
    assert!(addrs[0].is_ipv4() && addrs[1].is_ipv6(), "{addrs:?}");
    assert_eq!(s.local_addr(), addrs[0]);
    assert!(s.listen_errors().is_empty());
    let (m, payload) = make_torrent(&[("a", 16_384)], 16_384);
    let dir = tempfile::tempdir().unwrap();
    write_payload(dir.path(), &m, &payload);
    let h = s
        .add_torrent(AddTorrent::new(m.clone(), dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    for addr in addrs {
        let peer = ScriptedPeer::connect(addr, m.info_hashes.wire_hash()).await;
        assert_eq!(peer.remote.info_hash, m.info_hashes.wire_hash(), "{addr}");
    }
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ipv6_bind_failure_is_reported_and_skipped() {
    let Ok(held) = std::net::TcpListener::bind("[::1]:0") else {
        return;
    };
    let taken = held.local_addr().unwrap();
    let mut st = test_settings();
    st.listen_addrs = vec![std::net::SocketAddr::from(([127, 0, 0, 1], 0)), taken];
    let s = Session::new(st).await.unwrap();
    assert_eq!(s.local_addrs().len(), 1);
    assert!(s.local_addrs()[0].is_ipv4());
    let errors = s.listen_errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].addr, taken);
    assert!(!errors[0].udp_only);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unspecified_v4_and_v6_share_a_port() {
    if !ipv6_available() {
        return;
    }
    // A port free for TCP and UDP on both families. Another process may
    // take it before the session binds it, so a start that reports an
    // error is retried with a new port.
    let free_port = || {
        (0..50).find_map(|_| {
            let l = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
            let p = l.local_addr().unwrap().port();
            drop(l);
            let free = std::net::UdpSocket::bind(("0.0.0.0", p)).is_ok()
                && std::net::TcpListener::bind(("::", p)).is_ok()
                && std::net::UdpSocket::bind(("::", p)).is_ok();
            free.then_some(p)
        })
    };
    let mut last_errors = Vec::new();
    for _ in 0..5 {
        let port = free_port().expect("a port free on both families in 50 tries");
        let mut st = test_settings();
        st.listen_addrs = vec![
            std::net::SocketAddr::from(([0, 0, 0, 0], port)),
            std::net::SocketAddr::from(([0u16; 8], port)),
        ];
        let s = Session::new(st).await.unwrap();
        last_errors = s.listen_errors();
        if !last_errors.is_empty() {
            s.shutdown().await;
            continue;
        }
        assert_eq!(s.local_addrs().len(), 2);
        assert!(s.local_addrs().iter().all(|a| a.port() == port));
        s.shutdown().await;
        return;
    }
    panic!("no start without listen errors in 5 tries: {last_errors:?}");
}

#[tokio::test]
async fn empty_listen_addrs_is_invalid() {
    let mut st = test_settings();
    st.listen_addrs = Vec::new();
    match Session::new(st).await {
        Err(SessionError::InvalidSettings(msg)) => assert!(msg.contains("listen_addrs"), "{msg}"),
        other => panic!("expected InvalidSettings, got {other:?}"),
    }
}

#[tokio::test]
async fn duplicate_listen_addrs_is_invalid() {
    let mut st = test_settings();
    let a = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
    st.listen_addrs = vec![a, a];
    match Session::new(st).await {
        Err(SessionError::InvalidSettings(msg)) => assert!(msg.contains("listen_addrs"), "{msg}"),
        other => panic!("expected InvalidSettings, got {other:?}"),
    }
}

#[tokio::test]
async fn nothing_binds_is_a_bind_error() {
    let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut st = test_settings();
    st.listen_addrs = vec![held.local_addr().unwrap()];
    assert!(matches!(Session::new(st).await, Err(SessionError::Bind(_))));
}

/// Concurrent `read` calls across every `CountingStorage` sharing it.
#[derive(Debug, Default)]
struct ReadCounter {
    now: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
    total: std::sync::atomic::AtomicUsize,
}

/// Memory storage whose reads take `delay` each and are counted (in
/// flight, peak in flight, total) into a shared `ReadCounter`.
#[derive(Debug)]
struct CountingStorage {
    inner: fbt_session::storage::MemStorage,
    delay: Duration,
    counter: std::sync::Arc<ReadCounter>,
}

impl fbt_session::storage::Storage for CountingStorage {
    fn read(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
        use std::sync::atomic::Ordering::SeqCst;
        let now = self.counter.now.fetch_add(1, SeqCst) + 1;
        self.counter.peak.fetch_max(now, SeqCst);
        self.counter.total.fetch_add(1, SeqCst);
        std::thread::sleep(self.delay);
        let r = self.inner.read(offset, buf);
        self.counter.now.fetch_sub(1, SeqCst);
        r
    }
    fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        self.inner.write(offset, data)
    }
    fn flush(&self) -> std::io::Result<()> {
        Ok(())
    }
    fn delete_files(&self) -> std::io::Result<()> {
        self.inner.delete_files()
    }
}

/// A complete torrent of `pieces` 16 KiB pieces on a `CountingStorage`
/// whose reads take `delay`.
fn counted_torrent(
    pieces: usize,
    delay: Duration,
    counter: &std::sync::Arc<ReadCounter>,
) -> AddTorrent {
    let (m, payload) = make_torrent(&[("a", pieces * 16_384)], 16_384);
    let mut p = AddTorrent::new(m, PathBuf::from("unused"));
    p.storage = Some(std::sync::Arc::new(CountingStorage {
        inner: fbt_session::storage::MemStorage::from_bytes(payload),
        delay,
        counter: std::sync::Arc::clone(counter),
    }));
    p
}

/// With `max_active_checks: 1` the checks of three torrents run one after
/// the other: the others wait as `QueuedForChecking`, and all three seed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checks_run_one_at_a_time() {
    use std::sync::atomic::Ordering::SeqCst;
    let mut st = test_settings();
    st.max_active_checks = 1;
    let s = Session::new(st).await.unwrap();
    let mut ev = s.subscribe(EventMask::STATUS);
    let counter = std::sync::Arc::new(ReadCounter::default());
    let mut hs = Vec::new();
    for _ in 0..3 {
        let p = counted_torrent(8, Duration::from_millis(30), &counter);
        hs.push(s.add_torrent(p).await.unwrap());
    }
    let one_checking_two_queued = || {
        let states: Vec<TorrentState> = hs.iter().map(|h| h.status().state).collect();
        let checking = states
            .iter()
            .filter(|s| matches!(s, TorrentState::CheckingFiles { .. }))
            .count();
        let queued = states
            .iter()
            .filter(|s| **s == TorrentState::QueuedForChecking)
            .count();
        checking == 1 && queued == 2
    };
    assert!(
        wait_until(one_checking_two_queued, T).await,
        "one checking, two queued"
    );
    next_matching(
        &mut ev,
        |e| is_state(e, |s| *s == TorrentState::QueuedForChecking),
        T,
    )
    .await
    .expect("QueuedForChecking event");
    for h in &hs {
        assert!(wait_state(h, |s| *s == TorrentState::Seeding, T).await);
    }
    assert_eq!(counter.total.load(SeqCst), 24);
    assert_eq!(counter.peak.load(SeqCst), 1, "checks overlapped");
    s.shutdown().await;
}

/// A torrent waiting for its check can be paused, rechecked and removed
/// without ever reading its data, and shutdown does not wait for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_check_can_be_removed() {
    use std::sync::atomic::Ordering::SeqCst;
    let mut st = test_settings();
    st.max_active_checks = 1;
    let s = Session::new(st).await.unwrap();
    let mut ev = s.subscribe(EventMask::ALL);
    let first = slow_checking(&s).await;
    let counter = std::sync::Arc::new(ReadCounter::default());
    let queued = s
        .add_torrent(counted_torrent(4, Duration::ZERO, &counter))
        .await
        .unwrap();
    let ih = queued.info_hashes();
    assert!(wait_state(&queued, |s| *s == TorrentState::QueuedForChecking, T).await);
    assert!(matches!(
        first.status().state,
        TorrentState::CheckingFiles { .. }
    ));

    // Paused and rechecked, it keeps its place in the queue.
    queued.pause().await.unwrap();
    assert_eq!(queued.status().state, TorrentState::QueuedForChecking);
    queued.force_recheck().await.unwrap();
    queued.resume().await.unwrap();
    assert_eq!(queued.status().state, TorrentState::QueuedForChecking);

    let t0 = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(5), queued.remove(RemoveMode::KeepData))
        .await
        .expect("remove in time")
        .unwrap();
    assert!(t0.elapsed() < Duration::from_secs(2), "{:?}", t0.elapsed());
    next_matching(
        &mut ev,
        |e| matches!(e, Event::TorrentRemoved { torrent } if *torrent == ih),
        T,
    )
    .await
    .expect("TorrentRemoved");
    assert!(s.torrent(&ih).is_none());
    assert_eq!(
        counter.total.load(SeqCst),
        0,
        "a removed queued torrent read"
    );
    assert!(matches!(
        first.status().state,
        TorrentState::CheckingFiles { .. }
    ));

    // Shutdown with one torrent checking and another queued is prompt.
    let waiting = s
        .add_torrent(counted_torrent(4, Duration::ZERO, &counter))
        .await
        .unwrap();
    assert!(wait_state(&waiting, |s| *s == TorrentState::QueuedForChecking, T).await);
    let t0 = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(5), s.shutdown())
        .await
        .expect("shutdown in time");
    assert!(t0.elapsed() < Duration::from_secs(2), "{:?}", t0.elapsed());
    assert_eq!(counter.total.load(SeqCst), 0);
}
