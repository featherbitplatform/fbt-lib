//! Moving a torrent's storage and renaming its files: dirty pieces are
//! written before anything moves, readers survive a move, the same path is
//! a no-op, `MoveMode` decides what happens to files already at the target,
//! a failing move puts back what it moved, and a renamed file is served
//! from its new path.
//!
//! Torrent: `torrent/a` 200,000 B and `torrent/b` 200,000 B, 16 KiB pieces
//! (25 pieces, piece 12 straddles the boundary).

mod support;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fbt_metainfo::Metainfo;
use fbt_session::storage::MemStorage;
use fbt_session::{
    AddTorrent, Event, EventMask, EventStream, MoveMode, Session, SessionError, Settings,
    TorrentHandle, TorrentState,
};
use fbt_wire::{Handshake, Message, MessageCodec, Reserved, read_handshake, write_handshake};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

use support::*;

const PL: u32 = 16_384;
const A: usize = 200_000;
const B: usize = 200_000;
const FILES: [(&str, usize); 2] = [("a", A), ("b", B)];
/// Pieces wholly inside file `a`.
const FIRST: u32 = 12;

fn file(dir: &Path, name: &str) -> PathBuf {
    dir.join("torrent").join(name)
}

/// A session seeding the torrent from `dir`, where its files are written
/// first; its events subscribed before the add.
async fn disk_seed(
    m: &Metainfo,
    payload: &[u8],
    dir: &Path,
) -> (Session, TorrentHandle, EventStream) {
    write_payload(dir, m, payload);
    let s = Session::new(test_settings()).await.unwrap();
    let ev = s.subscribe(EventMask::ALL);
    let h = s
        .add_torrent(AddTorrent::new(m.clone(), dir.to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    (s, h, ev)
}

/// A session seeding the whole torrent from memory.
async fn mem_seed(m: &Metainfo, payload: &[u8]) -> Session {
    let s = Session::new(test_settings()).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(payload.to_vec())));
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    s
}

/// A seed on loopback (TCP, no encryption) that has only the first `have`
/// pieces and serves them.
async fn partial_seed(payload: &[u8], num_pieces: u32, have: u32) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let payload = Arc::new(payload.to_vec());
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let payload = Arc::clone(&payload);
            tokio::spawn(async move {
                let Ok(Ok(remote)) = tokio::time::timeout(T, read_handshake(&mut stream)).await
                else {
                    return;
                };
                let ours = Handshake {
                    reserved: Reserved::default(),
                    info_hash: remote.info_hash,
                    peer_id: fbt_core::PeerId([9; 20]),
                };
                if write_handshake(&mut stream, &ours).await.is_err() {
                    return;
                }
                let mut bits = vec![0u8; num_pieces.div_ceil(8) as usize];
                for p in 0..have {
                    bits[(p / 8) as usize] |= 0x80 >> (p % 8);
                }
                let mut peer = ScriptedPeer {
                    framed: Framed::new(stream, MessageCodec::new(1 << 20)),
                    remote,
                };
                peer.send(Message::Bitfield(Bytes::from(bits))).await;
                peer.send(Message::Unchoke).await;
                peer.serve(&payload, PL).await;
            });
        }
    });
    addr
}

/// The next `StorageMoved` or `StorageMoveFailed` of the stream.
async fn move_outcome(ev: &mut EventStream) -> Event {
    next_matching(
        ev,
        |e| {
            matches!(
                e,
                Event::StorageMoved { .. } | Event::StorageMoveFailed { .. }
            )
        },
        T,
    )
    .await
    .expect("move event")
}

fn assert_files(dir: &Path, payload: &[u8]) {
    assert!(
        std::fs::read(file(dir, "a")).unwrap() == payload[..A],
        "a differs"
    );
    assert!(
        std::fs::read(file(dir, "b")).unwrap() == payload[A..],
        "b differs"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn move_flushes_dirty_pieces_first() {
    let (m, payload) = make_torrent(&FILES, PL);
    let num_pieces = m.piece_layout().num_pieces();
    let half = partial_seed(&payload, num_pieces, FIRST).await;
    let old = tempfile::tempdir().unwrap();
    let new = tempfile::tempdir().unwrap();
    let mut st: Settings = dial_tcp(test_settings());
    // Verified pieces stay dirty in the cache: neither pressure nor
    // expiry writes them while the test runs.
    st.cache_size = 256 * 1024 * 1024;
    st.cache_expiry = Duration::from_secs(3600);
    let s = Session::new(st).await.unwrap();
    let mut ev = s.subscribe(EventMask::ALL);
    let h = s
        .add_torrent(AddTorrent::new(m.clone(), old.path().to_path_buf()))
        .await
        .unwrap();
    h.add_peer(half).await.unwrap();
    let mut count = 0;
    let deadline = tokio::time::Instant::now() + T;
    while count < FIRST as usize {
        assert!(
            tokio::time::Instant::now() < deadline,
            "first pieces in time"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        count = h.pieces().await.unwrap().iter_ones().count();
    }
    assert_eq!(h.status().state, TorrentState::Downloading);
    assert!(!file(old.path(), "a").exists(), "the pieces are not dirty");

    h.move_storage(new.path().to_path_buf(), MoveMode::FailIfExists)
        .await
        .unwrap();
    match move_outcome(&mut ev).await {
        Event::StorageMoved { path, .. } => assert_eq!(path, new.path()),
        e => panic!("{e:?}"),
    }
    assert_eq!(h.status().save_path, new.path());
    assert!(
        !old.path().join("torrent").exists(),
        "a torrent file stayed"
    );
    let got = std::fs::read(file(new.path(), "a")).unwrap();
    let n = FIRST as usize * PL as usize;
    assert!(
        got[..n] == payload[..n],
        "dirty pieces were not written first"
    );

    let seed = mem_seed(&m, &payload).await;
    h.add_peer(seed.local_addr()).await.unwrap();
    next_matching(&mut ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    assert_files(new.path(), &payload);
    assert!(!old.path().join("torrent").exists());
    s.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reader_survives_a_move() {
    let (m, payload) = make_torrent(&FILES, PL);
    let old = tempfile::tempdir().unwrap();
    let new = tempfile::tempdir().unwrap();
    let (s, h, _ev) = disk_seed(&m, &payload, old.path()).await;
    let mut r = h.open_file(1).await.unwrap();
    let mut head = vec![0u8; 1000];
    tokio::time::timeout(T, r.read_exact(&mut head))
        .await
        .expect("read in time")
        .unwrap();
    assert!(head == payload[A..A + 1000]);
    let read = async {
        let mut rest = vec![0u8; B - 1000];
        tokio::time::timeout(T, r.read_exact(&mut rest))
            .await
            .expect("read in time")
            .unwrap();
        rest
    };
    let mv = h.move_storage(new.path().to_path_buf(), MoveMode::FailIfExists);
    let (rest, moved) = tokio::join!(read, mv);
    moved.unwrap();
    assert!(rest == payload[A + 1000..], "the reader read wrong bytes");
    // Reads after the move come from the new location.
    r.seek(std::io::SeekFrom::Start(0)).await.unwrap();
    let mut again = vec![0u8; B];
    tokio::time::timeout(T, r.read_exact(&mut again))
        .await
        .expect("read in time")
        .unwrap();
    assert!(again == payload[A..]);
    assert_files(new.path(), &payload);
    assert!(!file(old.path(), "b").exists());
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn move_to_same_path_is_a_noop() {
    let (m, payload) = make_torrent(&FILES, PL);
    let dir = tempfile::tempdir().unwrap();
    let (s, h, _ev) = disk_seed(&m, &payload, dir.path()).await;
    h.move_storage(dir.path().to_path_buf(), MoveMode::FailIfExists)
        .await
        .unwrap();
    assert_files(dir.path(), &payload);
    assert_eq!(h.status().save_path, dir.path());
    assert_eq!(h.status().state, TorrentState::Seeding);
    // Another spelling of the same directory moves nothing either.
    let spelled = dir.path().join(".");
    h.move_storage(spelled.clone(), MoveMode::ReplaceExisting)
        .await
        .unwrap();
    assert_files(dir.path(), &payload);
    assert_eq!(h.status().save_path, spelled);
    assert_eq!(h.status().state, TorrentState::Seeding);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fail_if_exists_moves_nothing() {
    let (m, payload) = make_torrent(&FILES, PL);
    let old = tempfile::tempdir().unwrap();
    let new = tempfile::tempdir().unwrap();
    let (s, h, mut ev) = disk_seed(&m, &payload, old.path()).await;
    std::fs::create_dir_all(new.path().join("torrent")).unwrap();
    std::fs::write(file(new.path(), "b"), b"theirs").unwrap();
    let e = h
        .move_storage(new.path().to_path_buf(), MoveMode::FailIfExists)
        .await
        .unwrap_err();
    assert!(
        matches!(&e, SessionError::Storage(io) if io.kind() == std::io::ErrorKind::AlreadyExists),
        "{e:?}"
    );
    assert!(matches!(
        move_outcome(&mut ev).await,
        Event::StorageMoveFailed { .. }
    ));
    assert_files(old.path(), &payload);
    assert!(!file(new.path(), "a").exists(), "a moved");
    assert_eq!(std::fs::read(file(new.path(), "b")).unwrap(), b"theirs");
    assert_eq!(h.status().save_path, old.path());
    assert_eq!(h.status().state, TorrentState::Seeding);
    // Still serving from the old path.
    let leech = Session::new(test_settings()).await.unwrap();
    let mut lev = leech.subscribe(EventMask::STATUS);
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::new((A + B) as u64)));
    let lh = leech.add_torrent(p).await.unwrap();
    lh.add_peer(s.local_addr()).await.unwrap();
    next_matching(&mut lev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("served from the old path");
    leech.shutdown().await;
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replace_existing_overwrites() {
    let (m, payload) = make_torrent(&FILES, PL);
    let old = tempfile::tempdir().unwrap();
    let new = tempfile::tempdir().unwrap();
    let (s, h, _ev) = disk_seed(&m, &payload, old.path()).await;
    std::fs::create_dir_all(new.path().join("torrent")).unwrap();
    std::fs::write(file(new.path(), "b"), vec![0x5a; B]).unwrap();
    h.move_storage(new.path().to_path_buf(), MoveMode::ReplaceExisting)
        .await
        .unwrap();
    assert_files(new.path(), &payload);
    assert!(!old.path().join("torrent").exists());
    assert_eq!(h.status().save_path, new.path());
    assert_eq!(h.status().state, TorrentState::Seeding);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keep_existing_rechecks() {
    let (m, payload) = make_torrent(&FILES, PL);
    let old = tempfile::tempdir().unwrap();
    let new = tempfile::tempdir().unwrap();
    let (s, h, mut ev) = disk_seed(&m, &payload, old.path()).await;
    std::fs::create_dir_all(new.path().join("torrent")).unwrap();
    // Theirs: b with its last piece wrong.
    let mut theirs = payload[A..].to_vec();
    let last = theirs.len() - 1;
    theirs[last] ^= 0xff;
    std::fs::write(file(new.path(), "b"), &theirs).unwrap();
    h.move_storage(new.path().to_path_buf(), MoveMode::KeepExisting)
        .await
        .unwrap();
    assert!(matches!(
        move_outcome(&mut ev).await,
        Event::StorageMoved { .. }
    ));
    next_matching(
        &mut ev,
        |e| {
            matches!(
                e,
                Event::StateChanged {
                    state: TorrentState::CheckingFiles { .. },
                    ..
                }
            )
        },
        T,
    )
    .await
    .expect("rechecked");
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);
    let have: Vec<u32> = h.pieces().await.unwrap().iter_ones().collect();
    let n = m.piece_layout().num_pieces();
    assert_eq!(
        have,
        (0..n - 1).collect::<Vec<_>>(),
        "the last piece is theirs"
    );
    // Ours moved, theirs kept.
    assert!(std::fs::read(file(new.path(), "a")).unwrap() == payload[..A]);
    assert_eq!(std::fs::read(file(new.path(), "b")).unwrap(), theirs);
    assert_eq!(h.status().save_path, new.path());
    s.shutdown().await;
}

/// The move fails on its second file: the first one goes back, and the
/// torrent keeps seeding from the old path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_move_rolls_back() {
    let (m, payload) = make_torrent(&[("a", A), ("z/b", B)], PL);
    let old = tempfile::tempdir().unwrap();
    let new = tempfile::tempdir().unwrap();
    let blocked = new.path().join("torrent").join("z");
    std::fs::create_dir_all(&blocked).unwrap();
    let Some(lock) = block_target(&blocked) else {
        eprintln!("skipped: a read-only directory does not stop this user (root)");
        return;
    };
    let (s, h, mut ev) = disk_seed(&m, &payload, old.path()).await;
    let r = h
        .move_storage(new.path().to_path_buf(), MoveMode::ReplaceExisting)
        .await;
    drop(lock);
    unblock(&blocked);
    assert!(r.is_err(), "{r:?}");
    assert!(matches!(
        move_outcome(&mut ev).await,
        Event::StorageMoveFailed { .. }
    ));
    assert!(std::fs::read(old.path().join("torrent/a")).unwrap() == payload[..A]);
    assert!(std::fs::read(old.path().join("torrent/z/b")).unwrap() == payload[A..]);
    assert!(
        !new.path().join("torrent/a").exists(),
        "a was not moved back"
    );
    assert_eq!(h.status().save_path, old.path());
    assert_eq!(h.status().state, TorrentState::Seeding);
    s.shutdown().await;
}

/// Makes `dir/b` impossible to create or replace; `None` when this user
/// is not stopped by it (root ignores directory permissions).
#[cfg(unix)]
fn block_target(dir: &Path) -> Option<PathBuf> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let probe = dir.join("probe");
    if std::fs::write(&probe, b"x").is_ok() {
        let _ = std::fs::remove_file(&probe);
        unblock(dir);
        return None;
    }
    Some(dir.to_path_buf())
}

#[cfg(unix)]
fn unblock(dir: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// Holds `dir/b` open with no sharing: Windows refuses to replace, delete
/// or open it until the handle closes.
#[cfg(windows)]
fn block_target(dir: &Path) -> Option<std::fs::File> {
    use std::os::windows::fs::OpenOptionsExt;
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .share_mode(0)
        .open(dir.join("b"))
        .unwrap();
    Some(f)
}

#[cfg(windows)]
fn unblock(_dir: &Path) {}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rename_file_then_serve() {
    let (m, payload) = make_torrent(&FILES, PL);
    let dir = tempfile::tempdir().unwrap();
    let (s, h, mut ev) = disk_seed(&m, &payload, dir.path()).await;
    let part = format!(".{}.parts", m.info_hashes);
    let part_upper = part.to_uppercase();
    let part_dir = format!("{part}/x");
    h.rename_file(1, "t/sub/x.bin").await.unwrap();
    let e = next_matching(&mut ev, |e| matches!(e, Event::FileRenamed { .. }), T)
        .await
        .expect("FileRenamed");
    assert!(
        matches!(&e, Event::FileRenamed { index: 1, path, .. } if path == "t/sub/x.bin"),
        "{e:?}"
    );
    let moved = dir.path().join("t").join("sub").join("x.bin");
    assert!(std::fs::read(&moved).unwrap() == payload[A..]);
    assert!(!file(dir.path(), "b").exists());

    for bad in [
        "torrent/a",
        "TORRENT/A",
        "torrent",
        "torrent/a/x",
        "../x",
        "",
        "a//b",
        "/x",
        "a\\b",
        "C:x",
        "CON",
        "a\0b",
        // The part file's name, whatever the case, and a directory of it.
        part.as_str(),
        part_upper.as_str(),
        part_dir.as_str(),
    ] {
        let e = h.rename_file(1, bad).await.unwrap_err();
        assert!(matches!(e, SessionError::InvalidPath(_)), "{bad:?}: {e:?}");
    }
    assert!(std::fs::read(&moved).unwrap() == payload[A..]);
    // A file that is not the torrent's is never taken over.
    std::fs::write(dir.path().join("theirs"), b"theirs").unwrap();
    let e = h.rename_file(1, "theirs").await.unwrap_err();
    assert!(
        matches!(&e, SessionError::Storage(io) if io.kind() == std::io::ErrorKind::AlreadyExists),
        "{e:?}"
    );
    assert!(matches!(
        next_matching(&mut ev, |e| matches!(e, Event::FileRenameFailed { .. }), T).await,
        Some(Event::FileRenameFailed { index: 1, .. })
    ));
    assert_eq!(std::fs::read(dir.path().join("theirs")).unwrap(), b"theirs");
    assert!(std::fs::read(&moved).unwrap() == payload[A..]);

    // A leecher downloads file 1 from its new place.
    let leech = Session::new(test_settings()).await.unwrap();
    let mut lev = leech.subscribe(EventMask::STATUS);
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    let mem = Arc::new(MemStorage::new((A + B) as u64));
    p.storage = Some(mem.clone());
    let lh = leech.add_torrent(p).await.unwrap();
    lh.add_peer(s.local_addr()).await.unwrap();
    next_matching(&mut lev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    let mut got = vec![0u8; A + B];
    fbt_session::storage::Storage::read(&*mem, 0, &mut got).unwrap();
    assert!(got == payload);
    assert_eq!(h.status().state, TorrentState::Seeding);
    leech.shutdown().await;
    s.shutdown().await;
}

/// A move or rename asked for while the torrent checks (or waits to) is
/// refused with `Busy` at once, and never holds a pause or the session's
/// shutdown until the check ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn move_during_a_check_is_busy() {
    let mut st = test_settings();
    st.max_active_checks = 1;
    let s = Session::new(st).await.unwrap();
    // Every read of a check takes 200 ms: 25 pieces, about 5 s.
    let slow = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let add = |m: Metainfo, payload: Vec<u8>| {
        let mut p = AddTorrent::new(m, PathBuf::from("unused"));
        p.storage = Some(Arc::new(ToggleStorage::new(
            payload,
            Duration::from_millis(200),
            Arc::clone(&slow),
        )));
        s.add_torrent(p)
    };
    let (m, payload) = make_torrent(&FILES, PL);
    let checking = add(m, payload).await.unwrap();
    assert!(
        wait_state(
            &checking,
            |st| matches!(st, TorrentState::CheckingFiles { .. }),
            T
        )
        .await,
        "{:?}",
        checking.status().state
    );
    let (m, payload) = make_torrent(&FILES, PL);
    let queued = add(m, payload).await.unwrap();
    assert!(
        wait_state(&queued, |st| *st == TorrentState::QueuedForChecking, T).await,
        "{:?}",
        queued.status().state
    );
    let quick = Duration::from_secs(2);
    for h in [&queued, &checking] {
        let r = tokio::time::timeout(
            quick,
            h.move_storage(PathBuf::from("elsewhere"), MoveMode::FailIfExists),
        )
        .await
        .expect("the move waited for the check");
        assert!(matches!(r, Err(SessionError::Busy)), "{r:?}");
        let r = tokio::time::timeout(quick, h.rename_file(0, "renamed"))
            .await
            .expect("the rename waited for the check");
        assert!(matches!(r, Err(SessionError::Busy)), "{r:?}");
        assert_eq!(h.status().save_path, PathBuf::from("unused"));
    }
    tokio::time::timeout(quick, queued.pause())
        .await
        .expect("the pause waited for the check")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), s.shutdown())
        .await
        .expect("the shutdown waited for the check");
}
