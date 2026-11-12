//! Reading a torrent's files while they download: `FileReader` opens,
//! reads, seeks and waits for pieces from the normal download, is fed only
//! verified bytes, reads pieces we have from the cache or the disk, and
//! fails a waiting read only when the torrent is removed, the session shuts
//! down or the storage fails.

mod support;

use std::io::{self, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use fbt_core::BlockRef;
use fbt_metainfo::{Magnet, Metainfo};
use fbt_session::storage::{MemStorage, Storage};
use fbt_session::{
    AddTorrent, DisconnectReason, Event, EventMask, FileReader, Priority, RateLimit, ReadAhead,
    RemoveMode, Session, SessionError, Settings, StreamError, TorrentHandle, TorrentState,
};
use fbt_wire::Message;
use futures_util::{SinkExt, StreamExt};
use support::*;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

const T: Duration = Duration::from_secs(30);
const MIB: usize = 1024 * 1024;
/// Piece length of every test torrent.
const PL: usize = 256 * 1024;
/// Three files: 3 MiB (pieces 0 to 11), empty, 5 MiB (pieces 12 to 31).
const FILES: [(&str, usize); 3] = [("a", 3 * MIB), ("empty", 0), ("b", 5 * MIB)];
/// Where file 2 starts in the torrent.
const B_START: usize = 3 * MIB;
const B_LEN: usize = 5 * MIB;

fn settings() -> Settings {
    let mut s = dial_tcp(test_settings());
    s.rechoke_interval = Duration::from_secs(1);
    s
}

fn torrent() -> (Metainfo, Vec<u8>) {
    make_torrent(&FILES, PL as u32)
}

/// A session with `m` over `storage`, waited until `state`.
async fn session_with(
    st: Settings,
    m: &Metainfo,
    storage: Arc<dyn Storage>,
    state: TorrentState,
) -> (Session, TorrentHandle) {
    let s = Session::new(st).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(storage);
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |x| *x == state, T).await, "{state:?}");
    (s, h)
}

async fn seeder(m: &Metainfo, payload: &[u8]) -> (Session, TorrentHandle) {
    let storage = Arc::new(MemStorage::from_bytes(payload.to_vec()));
    session_with(settings(), m, storage, TorrentState::Seeding).await
}

/// A seeder holding only the first `have` pieces (the rest zeroed).
async fn partial_seeder(m: &Metainfo, payload: &[u8], have: usize) -> Session {
    let mut data = payload.to_vec();
    data[have * PL..].fill(0);
    let storage = Arc::new(MemStorage::from_bytes(data));
    session_with(settings(), m, storage, TorrentState::Downloading)
        .await
        .0
}

/// An empty leech over memory.
async fn leech(m: &Metainfo) -> (Session, TorrentHandle) {
    let storage = Arc::new(MemStorage::new(m.total_len()));
    session_with(settings(), m, storage, TorrentState::Downloading).await
}

/// Waits until `h` has `n` pieces.
async fn wait_pieces(h: &TorrentHandle, n: u32) {
    let end = tokio::time::Instant::now() + T;
    loop {
        if h.pieces().await.unwrap().count_ones() >= n {
            return;
        }
        assert!(tokio::time::Instant::now() < end, "{n} pieces in time");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Seeks to `offset` and reads `n` bytes (fewer at the end of the file).
async fn read_at(r: &mut FileReader, offset: u64, n: usize) -> io::Result<Vec<u8>> {
    r.seek(SeekFrom::Start(offset)).await?;
    let n = n.min(r.len().saturating_sub(offset) as usize);
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

/// The `StreamError` inside a failed read.
fn stream_error(e: &io::Error) -> Option<&StreamError> {
    e.get_ref().and_then(|e| e.downcast_ref::<StreamError>())
}

#[test]
fn file_reader_is_send_and_unpin() {
    fn check<T: Send + Unpin>() {}
    check::<FileReader>();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_whole_file_while_downloading() {
    let (m, payload) = torrent();
    let (seed, _sh) = seeder(&m, &payload).await;
    let (l, lh) = leech(&m).await;
    let mut ev = l.subscribe(EventMask::STATUS);
    let mut r = lh.open_file(2).await.unwrap();
    assert_eq!(r.len(), B_LEN as u64);
    assert_eq!(r.index(), 2);
    assert!(!r.is_empty());
    let read = tokio::spawn(async move {
        let mut out = Vec::new();
        r.read_to_end(&mut out).await.map(|_| out)
    });
    lh.add_peer(seed.local_addr()).await.unwrap();
    let out = tokio::time::timeout(T, read)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(out == payload[B_START..], "file 2's bytes");
    next_matching(&mut ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    l.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seek_then_read_returns_the_right_bytes() {
    let (m, payload) = torrent();
    let (seed, _sh) = seeder(&m, &payload).await;
    let (l, lh) = leech(&m).await;
    let mut r = lh.open_file(2).await.unwrap();
    lh.add_peer(seed.local_addr()).await.unwrap();
    let len = B_LEN as u64;
    for offset in [0, 1, 262_143, 262_144, len - 1, 2 * PL as u64 - 2048] {
        let got = tokio::time::timeout(T, read_at(&mut r, offset, 4096))
            .await
            .expect("read in time")
            .unwrap();
        let start = B_START + offset as usize;
        let end = (start + 4096).min(B_START + B_LEN);
        assert!(got == payload[start..end], "bytes at {offset}");
        assert_eq!(r.position(), offset + got.len() as u64);
    }
    l.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_at_and_past_eof() {
    let (m, payload) = torrent();
    let (l, lh) = leech(&m).await;
    let mut r = lh.open_file(2).await.unwrap();
    let len = r.len();
    let mut buf = [0u8; 4096];
    // No peer: only a read that needs no piece can return.
    assert_eq!(r.seek(SeekFrom::End(0)).await.unwrap(), len);
    let n = tokio::time::timeout(Duration::from_secs(5), r.read(&mut buf))
        .await
        .expect("EOF at once")
        .unwrap();
    assert_eq!(n, 0);
    assert_eq!(
        r.seek(SeekFrom::Start(len + 1000)).await.unwrap(),
        len + 1000
    );
    let n = tokio::time::timeout(Duration::from_secs(5), r.read(&mut buf))
        .await
        .expect("EOF at once")
        .unwrap();
    assert_eq!(n, 0);
    assert_eq!(r.position(), len + 1000);
    l.shutdown().await;

    // On the seed, a read straddling the end stops there.
    let (seed, sh) = seeder(&m, &payload).await;
    let mut r = sh.open_file(2).await.unwrap();
    r.seek(SeekFrom::Start(len - 100)).await.unwrap();
    let n = tokio::time::timeout(T, r.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 100);
    assert!(buf[..100] == payload[B_START + B_LEN - 100..]);
    assert_eq!(r.read(&mut buf).await.unwrap(), 0);
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn negative_seek_is_invalid_input() {
    let (m, _payload) = torrent();
    let (l, lh) = leech(&m).await;
    let mut r = lh.open_file(2).await.unwrap();
    let len = r.len() as i64;
    let e = r.seek(SeekFrom::Current(-1)).await.unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(r.position(), 0);
    assert_eq!(r.seek(SeekFrom::Start(10)).await.unwrap(), 10);
    let e = r.seek(SeekFrom::End(-(len + 1))).await.unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(r.position(), 10);
    // The last valid one still works.
    assert_eq!(r.seek(SeekFrom::End(-len)).await.unwrap(), 0);
    l.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_length_file_reads_eof_without_deadlines() {
    let (m, _payload) = torrent();
    let (l, lh) = leech(&m).await;
    let mut r = lh.open_file(1).await.unwrap();
    assert_eq!(r.len(), 0);
    assert!(r.is_empty());
    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(5), r.read(&mut buf))
        .await
        .expect("EOF at once")
        .unwrap();
    assert_eq!(n, 0);
    assert!(lh.debug_deadlines().await.is_empty());
    // A reader on file 2 does set some, all on missing pieces of file 2.
    let _r2 = lh.open_file(2).await.unwrap();
    let d = lh.debug_deadlines().await;
    assert!(!d.is_empty());
    assert!(d.iter().all(|&(p, _)| (12..32).contains(&p)), "{d:?}");
    l.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn open_file_index_checks() {
    let (m, _payload) = torrent();
    let (l, lh) = leech(&m).await;
    match lh.open_file(3).await {
        Err(SessionError::InvalidFileIndex { index: 3, files: 3 }) => {}
        other => panic!("{other:?}"),
    }
    // A pad file is no file to read.
    let (pm, ppayload) =
        make_torrent_with_pad(&[("a", 100_000), ("b", 50_000), ("c", 10)], 0, 32_768);
    assert!(pm.info.files[1].pad);
    let mut p = AddTorrent::new(pm.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(ppayload)));
    let ph = l.add_torrent(p).await.unwrap();
    match ph.open_file(1).await {
        Err(SessionError::InvalidFileIndex { index: 1, files: 4 }) => {}
        other => panic!("{other:?}"),
    }
    assert!(ph.open_file(2).await.is_ok());
    // Zero time is refused before anything is opened.
    match lh
        .open_file_with(0, ReadAhead::new(1024, Duration::ZERO, None))
        .await
    {
        Err(SessionError::InvalidSettings(_)) => {}
        other => panic!("{other:?}"),
    }
    let mut open = Vec::new();
    for _ in 0..16 {
        open.push(lh.open_file(0).await.unwrap());
    }
    match lh.open_file(0).await {
        Err(SessionError::TooManyReaders) => {}
        other => panic!("{other:?}"),
    }
    // Another torrent has its own slots.
    assert!(ph.open_file(0).await.is_ok());
    l.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn open_file_on_magnet_without_metadata() {
    let (m, payload) = torrent();
    let (seed, _sh) = seeder(&m, &payload).await;
    let l = Session::new(settings()).await.unwrap();
    let mut ev = l.subscribe(EventMask::STATUS);
    let dir = tempfile::tempdir().unwrap();
    let mg = Magnet::from_info_hashes(m.info_hashes);
    let h = l
        .add_torrent(AddTorrent::magnet(mg, dir.path().to_path_buf()))
        .await
        .unwrap();
    match h.open_file(0).await {
        Err(SessionError::NoMetadata) => {}
        other => panic!("{other:?}"),
    }
    h.add_peer(seed.local_addr()).await.unwrap();
    next_matching(&mut ev, |e| matches!(e, Event::MetadataReceived { .. }), T)
        .await
        .expect("MetadataReceived");
    let mut r = h.open_file(0).await.unwrap();
    assert_eq!(r.len(), 3 * MIB as u64);
    let got = tokio::time::timeout(T, read_at(&mut r, 1000, 4096))
        .await
        .unwrap()
        .unwrap();
    assert!(got == payload[1000..5096]);
    l.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiting_read_is_answered_from_the_hash_pass() {
    let (m, payload) = torrent();
    let (seed, _sh) = seeder(&m, &payload).await;
    let gate = Arc::new(AtomicBool::new(true));
    let storage = Arc::new(GatedWriteStorage::new(
        payload.len() as u64,
        Arc::clone(&gate),
    ));
    let (l, lh) = session_with(settings(), &m, storage, TorrentState::Downloading).await;
    let mut ev = l.subscribe(EventMask::PIECE | EventMask::STATUS);
    let mut r = lh.open_file(0).await.unwrap();
    let read = tokio::spawn(async move { read_at(&mut r, 5 * PL as u64, 4096).await });
    // Nothing to read from yet: the read waits.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!read.is_finished());
    lh.add_peer(seed.local_addr()).await.unwrap();
    next_matching(
        &mut ev,
        |e| matches!(e, Event::PieceFinished { piece: 5, .. }),
        T,
    )
    .await
    .expect("piece 5 passed");
    let got = tokio::time::timeout(T, read)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // Every write is still held (the storage is all zeros), so bytes that
    // match the payload came from memory.
    assert!(got == payload[5 * PL..5 * PL + 4096]);
    gate.store(false, Ordering::SeqCst);
    next_matching(&mut ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    l.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reader_never_sees_unverified_bytes() {
    let (m, payload) = torrent();
    let (l, lh) = leech(&m).await;
    let mut ev = l.subscribe(EventMask::PIECE);
    let mut r = lh.open_file(0).await.unwrap();
    let read = tokio::spawn(async move { read_at(&mut r, 2 * PL as u64, 4096).await });
    // A peer with only piece 2 sends it corrupt.
    let mut peer = ScriptedPeer::connect(l.local_addr(), m.info_hashes.wire_hash()).await;
    peer.send(Message::Bitfield(Bytes::from_static(&[0x20, 0, 0, 0])))
        .await;
    peer.send(Message::Unchoke).await;
    let corrupt = payload.clone();
    let serve = tokio::spawn(async move {
        while let Some(Ok(msg)) = peer.framed.next().await {
            if let Message::Request(b) = msg {
                let start = b.piece as usize * PL + b.offset as usize;
                let mut data = corrupt[start..start + b.len as usize].to_vec();
                data.iter_mut().for_each(|x| *x ^= 0xff);
                let reply = Message::Piece {
                    piece: b.piece,
                    offset: b.offset,
                    data: Bytes::from(data),
                };
                if peer.framed.send(reply).await.is_err() {
                    return;
                }
            }
        }
    });
    next_matching(
        &mut ev,
        |e| matches!(e, Event::HashFailed { piece: 2, .. }),
        T,
    )
    .await
    .expect("HashFailed for piece 2");
    serve.abort();
    assert!(!read.is_finished(), "a failed piece answers no read");
    let (seed, _sh) = seeder(&m, &payload).await;
    lh.add_peer(seed.local_addr()).await.unwrap();
    let got = tokio::time::timeout(T, read)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(got == payload[2 * PL..2 * PL + 4096]);
    l.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn have_pieces_are_read_from_disk_and_cached() {
    let (m, payload) = torrent();
    let storage = Arc::new(CountingStorage::new(payload.clone()));
    let (s, h) = session_with(
        settings(),
        &m,
        Arc::clone(&storage) as Arc<dyn Storage>,
        TorrentState::Seeding,
    )
    .await;
    // Counted from the end of the initial check.
    storage.reset();
    let clean_before = s.cache_stats().clean;
    let mut r = h.open_file(2).await.unwrap();
    let mut out = Vec::new();
    tokio::time::timeout(T, r.read_to_end(&mut out))
        .await
        .unwrap()
        .unwrap();
    assert!(out == payload[B_START..]);
    assert!(s.cache_stats().clean > clean_before);
    let reads = storage.reads();
    assert!(reads > 0);
    r.seek(SeekFrom::Start(0)).await.unwrap();
    out.clear();
    tokio::time::timeout(T, r.read_to_end(&mut out))
        .await
        .unwrap()
        .unwrap();
    assert!(out == payload[B_START..]);
    assert_eq!(storage.reads(), reads, "the second pass reads no storage");
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequential_reader_prefetches() {
    let (m, payload) = torrent();
    let storage = Arc::new(CountingStorage::new(payload.clone()));
    let mut st = settings();
    st.cache_size = 2 * MIB as u64;
    let (s, h) = session_with(
        st,
        &m,
        Arc::clone(&storage) as Arc<dyn Storage>,
        TorrentState::Seeding,
    )
    .await;
    storage.reset();
    let mut r = h.open_file(2).await.unwrap();
    // Pieces 12 to 15, then a pause after piece q = 15.
    let mut buf = vec![0u8; PL];
    for i in 0..4 {
        tokio::time::timeout(T, r.read_exact(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert!(buf == payload[B_START + i * PL..B_START + (i + 1) * PL]);
    }
    let q = 15u64;
    let ahead = |p: u64| storage.offsets().contains(&(p * PL as u64));
    assert!(
        wait_until(|| ahead(q + 1) && ahead(q + 2), Duration::from_secs(5)).await,
        "pieces after {q} read ahead: {:?}",
        storage.offsets()
    );
    let mut rest = Vec::new();
    tokio::time::timeout(T, r.read_to_end(&mut rest))
        .await
        .unwrap()
        .unwrap();
    assert!(rest == payload[B_START + 4 * PL..]);
    // Every piece of the file read once, whole.
    let offsets = storage.offsets();
    assert!(
        offsets.iter().all(|o| o % PL as u64 == 0),
        "whole pieces only: {offsets:?}"
    );
    for piece in 12..32u64 {
        let n = offsets.iter().filter(|&&o| o == piece * PL as u64).count();
        assert_eq!(n, 1, "piece {piece} read {n} times: {offsets:?}");
    }
    assert_eq!(offsets.len(), 20, "{offsets:?}");
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_serves_have_pieces_and_waits_for_missing() {
    let (m, payload) = torrent();
    let half = partial_seeder(&m, &payload, 16).await;
    let (l, lh) = leech(&m).await;
    lh.add_peer(half.local_addr()).await.unwrap();
    wait_pieces(&lh, 16).await;
    lh.pause().await.unwrap();
    let mut have = lh.open_file(0).await.unwrap();
    let got = tokio::time::timeout(T, read_at(&mut have, 3 * PL as u64, 4096))
        .await
        .expect("a piece we have is served while paused")
        .unwrap();
    assert!(got == payload[3 * PL..3 * PL + 4096]);
    let mut missing = lh.open_file(2).await.unwrap();
    // Piece 20.
    let offset = (20 * PL - B_START) as u64;
    let read = tokio::spawn(async move { read_at(&mut missing, offset, 4096).await });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!read.is_finished(), "a missing piece waits");
    let (seed, _sh) = seeder(&m, &payload).await;
    lh.add_peer(seed.local_addr()).await.unwrap();
    lh.resume().await.unwrap();
    let got = tokio::time::timeout(T, read)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(got == payload[20 * PL..20 * PL + 4096]);
    l.shutdown().await;
    half.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recheck_keeps_waiters() {
    let (m, payload) = torrent();
    let half = partial_seeder(&m, &payload, 16).await;
    // Write-through, so a read of a piece we have goes to the disk; and
    // reads that the test can hold there.
    let gate = Arc::new(AtomicBool::new(false));
    let storage = Arc::new(GatedReadStorage::new(
        payload.len() as u64,
        Arc::clone(&gate),
    ));
    let mut st = settings();
    st.cache_size = 0;
    let (l, lh) = session_with(
        st,
        &m,
        Arc::clone(&storage) as Arc<dyn Storage>,
        TorrentState::Downloading,
    )
    .await;
    lh.add_peer(half.local_addr()).await.unwrap();
    wait_pieces(&lh, 16).await;
    let mut missing = lh.open_file(2).await.unwrap();
    let offset = (20 * PL - B_START) as u64;
    let waiting = tokio::spawn(async move { read_at(&mut missing, offset, 4096).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!waiting.is_finished());
    // A read of a piece we have, its whole-piece disk read held in flight
    // when the recheck drops it.
    gate.store(true, Ordering::SeqCst);
    let mut have = lh.open_file(0).await.unwrap();
    let have_read = tokio::spawn(async move { read_at(&mut have, 7 * PL as u64, 4096).await });
    let piece_7 = 7 * PL as u64;
    assert!(
        wait_until(
            || storage.offsets().contains(&piece_7),
            Duration::from_secs(5)
        )
        .await,
        "the disk read of piece 7 started"
    );
    lh.force_recheck().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !have_read.is_finished(),
        "the read waits through the recheck"
    );
    gate.store(false, Ordering::SeqCst);
    let got = tokio::time::timeout(T, have_read)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(got == payload[7 * PL..7 * PL + 4096]);
    assert!(wait_state(&lh, |s| *s == TorrentState::Downloading, T).await);
    let (seed, _sh) = seeder(&m, &payload).await;
    lh.add_peer(seed.local_addr()).await.unwrap();
    let got = tokio::time::timeout(T, waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(got == payload[20 * PL..20 * PL + 4096]);
    l.shutdown().await;
    half.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remove_while_waiting_fails_the_read() {
    let (m, _payload) = torrent();
    let (l, lh) = leech(&m).await;
    let mut r = lh.open_file(2).await.unwrap();
    let read = tokio::spawn(async move { read_at(&mut r, 0, 4096).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!read.is_finished());
    lh.remove(RemoveMode::KeepData).await.unwrap();
    let e = tokio::time::timeout(T, read)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::NotFound);
    assert_eq!(stream_error(&e), Some(&StreamError::TorrentRemoved));
    l.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_while_waiting_fails_the_read() {
    let (m, _payload) = torrent();
    let (l, lh) = leech(&m).await;
    let mut r = lh.open_file(2).await.unwrap();
    let read = tokio::spawn(async move { read_at(&mut r, 0, 4096).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!read.is_finished());
    l.shutdown().await;
    let e = tokio::time::timeout(T, read)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::ConnectionAborted);
    assert_eq!(stream_error(&e), Some(&StreamError::Shutdown));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_error_fails_waiting_reads() {
    let (m, payload) = torrent();
    let half = partial_seeder(&m, &payload, 16).await;
    let fail = Arc::new(AtomicBool::new(true));
    let storage = Arc::new(FailWritesStorage::new(
        payload.len() as u64,
        Arc::clone(&fail),
    ));
    let mut st = settings();
    // Write-through: the first verified piece's write fails.
    st.cache_size = 0;
    let (l, lh) = session_with(st, &m, storage, TorrentState::Downloading).await;
    let mut r = lh.open_file(2).await.unwrap();
    // Piece 20, which the peer does not have.
    let offset = (20 * PL - B_START) as u64;
    let read = tokio::spawn(async move {
        let res = read_at(&mut r, offset, 4096).await;
        (r, res)
    });
    lh.add_peer(half.local_addr()).await.unwrap();
    assert!(wait_state(&lh, |s| *s == TorrentState::Error, T).await);
    let (mut r, res) = tokio::time::timeout(T, read).await.unwrap().unwrap();
    let e = res.unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::Other);
    assert!(
        matches!(stream_error(&e), Some(StreamError::TorrentError(_))),
        "{e:?}"
    );
    // The reader is not poisoned: once the storage works and the torrent
    // runs again, a read of a piece the peer has works.
    fail.store(false, Ordering::SeqCst);
    lh.resume().await.unwrap();
    let mut a = lh.open_file(0).await.unwrap();
    let got = tokio::time::timeout(T, read_at(&mut a, PL as u64, 4096))
        .await
        .unwrap()
        .unwrap();
    assert!(got == payload[PL..PL + 4096]);
    // The same reader too, on a piece of file 2 the peer has (piece 13).
    let got = tokio::time::timeout(T, read_at(&mut r, PL as u64, 4096))
        .await
        .unwrap()
        .unwrap();
    assert!(got == payload[13 * PL..13 * PL + 4096]);
    l.shutdown().await;
    half.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_reader_frees_its_slot() {
    let (m, _payload) = torrent();
    let (l, lh) = leech(&m).await;
    let mut open = Vec::new();
    for _ in 0..15 {
        open.push(lh.open_file(0).await.unwrap());
    }
    // The 16th reads file 2, whose pieces no other reader's window holds.
    let mut dropped = lh.open_file(2).await.unwrap();
    assert!(lh.debug_deadlines().await.iter().any(|&(p, _)| p >= 12));
    assert!(matches!(
        lh.open_file(0).await,
        Err(SessionError::TooManyReaders)
    ));
    let read = tokio::spawn(async move { read_at(&mut dropped, 0, 4096).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!read.is_finished());
    // Dropped mid-read.
    read.abort();
    let _ = read.await;
    let end = tokio::time::Instant::now() + Duration::from_secs(5);
    let seventeenth = loop {
        match lh.open_file(0).await {
            Ok(r) => break r,
            Err(SessionError::TooManyReaders) => {
                assert!(tokio::time::Instant::now() < end, "a slot came free");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(e) => panic!("{e:?}"),
        }
    };
    let d = lh.debug_deadlines().await;
    assert!(!d.is_empty());
    assert!(d.iter().all(|&(p, _)| p < 12), "{d:?}");
    drop(seventeenth);
    l.shutdown().await;
}

// ----- deadline picking -------------------------------------------------------

/// A leech of `m` over memory with `prio` as its file priorities, waited
/// until `state`.
async fn leech_with(
    st: Settings,
    m: &Metainfo,
    prio: Vec<Priority>,
    state: TorrentState,
) -> (Session, TorrentHandle) {
    let s = Session::new(st).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::new(m.total_len())));
    p.file_priorities = prio;
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |x| *x == state, T).await, "{state:?}");
    (s, h)
}

/// A bitfield message of `n` pieces with `has(i)` set.
fn bitfield(n: usize, has: impl Fn(usize) -> bool) -> Message {
    let mut bytes = vec![0u8; n.div_ceil(8)];
    for i in (0..n).filter(|&i| has(i)) {
        bytes[i / 8] |= 0x80 >> (i % 8);
    }
    Message::Bitfield(Bytes::from(bytes))
}

/// A scripted peer connected to `l`, with `has` of `m`'s pieces, that
/// unchokes it.
async fn scripted_seed(l: &Session, m: &Metainfo, has: impl Fn(usize) -> bool) -> ScriptedPeer {
    let mut peer = ScriptedPeer::connect(l.local_addr(), m.info_hashes.wire_hash()).await;
    let n = m.piece_layout().num_pieces() as usize;
    peer.send(bitfield(n, has)).await;
    peer.send(Message::Unchoke).await;
    peer
}

type Log = Arc<std::sync::Mutex<Vec<BlockRef>>>;

fn log() -> Log {
    Arc::new(std::sync::Mutex::new(Vec::new()))
}

fn logged(log: &Log) -> Vec<BlockRef> {
    log.lock().unwrap().clone()
}

/// Waits up to `within` until the merged deadlines satisfy `pred`.
async fn wait_deadlines(
    h: &TorrentHandle,
    within: Duration,
    pred: impl Fn(&[(u32, Duration)]) -> bool,
) -> Vec<(u32, Duration)> {
    let end = tokio::time::Instant::now() + within;
    loop {
        let d = h.debug_deadlines().await;
        if pred(&d) || tokio::time::Instant::now() >= end {
            return d;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seek_reads_ahead_of_the_rest() {
    let (m, payload) = make_torrent(&[("big", 64 * MIB)], PL as u32);
    let (seed, _sh) = seeder(&m, &payload).await;
    let mut st = settings();
    st.rate_limit_local_peers = true;
    st.rate_limit = RateLimit::new(4 * MIB as u64, 0);
    let storage = Arc::new(MemStorage::new(m.total_len()));
    let (l, lh) = session_with(st, &m, storage, TorrentState::Downloading).await;
    let mut r = lh.open_file(0).await.unwrap();
    let at = 48 * MIB;
    r.seek(SeekFrom::Start(at as u64)).await.unwrap();
    let read = tokio::spawn(async move {
        let mut buf = vec![0u8; MIB];
        r.read_exact(&mut buf).await.map(|_| buf)
    });
    lh.add_peer(seed.local_addr()).await.unwrap();
    let got = tokio::time::timeout(T, read)
        .await
        .expect("read in time")
        .unwrap()
        .unwrap();
    let downloaded = lh.status().total_downloaded;
    assert!(got == payload[at..at + MIB]);
    // Rarest first alone reaches 48 MiB at a random point of the 64 MiB.
    assert!(
        downloaded < 24 * MIB as u64,
        "{downloaded} bytes downloaded before the read completed"
    );
    l.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_readers_on_one_torrent() {
    let (m, payload) = torrent();
    let (seed, _sh) = seeder(&m, &payload).await;
    let (l, lh) = leech(&m).await;
    let mut r0 = lh.open_file(0).await.unwrap();
    let mut r2 = lh.open_file(2).await.unwrap();
    let d = lh.debug_deadlines().await;
    assert!(d.iter().any(|&(p, _)| p < 12), "file 0's window: {d:?}");
    assert!(d.iter().any(|&(p, _)| p >= 12), "file 2's window: {d:?}");
    let a = tokio::spawn(async move {
        let mut out = Vec::new();
        r0.read_to_end(&mut out).await.map(|_| out)
    });
    let b = tokio::spawn(async move {
        let mut out = Vec::new();
        r2.read_to_end(&mut out).await.map(|_| out)
    });
    lh.add_peer(seed.local_addr()).await.unwrap();
    let a = tokio::time::timeout(T, a).await.unwrap().unwrap().unwrap();
    let b = tokio::time::timeout(T, b).await.unwrap().unwrap().unwrap();
    assert!(a == payload[..3 * MIB], "file 0");
    assert!(b == payload[B_START..], "file 2");
    l.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_reader_clears_its_deadlines_only() {
    let (m, _payload) = torrent();
    let (l, lh) = leech(&m).await;
    let _r0 = lh.open_file(0).await.unwrap();
    let r2 = lh.open_file(2).await.unwrap();
    let d = lh.debug_deadlines().await;
    assert!(d.iter().any(|&(p, _)| p < 12) && d.iter().any(|&(p, _)| p >= 12));
    drop(r2);
    let d = wait_deadlines(&lh, Duration::from_secs(5), |d| {
        !d.is_empty() && d.iter().all(|&(p, _)| p < 12)
    })
    .await;
    assert!(!d.is_empty(), "file 0's window stays");
    assert!(
        d.iter().all(|&(p, _)| p < 12),
        "only file 0's window: {d:?}"
    );
    l.shutdown().await;
}

/// A (3 MiB + 1000 B), B (2 MiB + 100 B) and C (3 MiB), in 256 KiB pieces:
/// A is pieces 0 to 12, B 12 to 20, C 20 to 32.
const STRADDLE: [(&str, usize); 3] = [("a", 3 * MIB + 1000), ("b", 2 * MIB + 100), ("c", 3 * MIB)];
const STRADDLE_B: usize = 3 * MIB + 1000;
const STRADDLE_C: usize = 5 * MIB + 1100;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn straddling_pieces_fetch_only_the_boundaries() {
    let (m, payload) = make_torrent(&STRADDLE, PL as u32);
    let (l, lh) = leech_with(
        settings(),
        &m,
        vec![Priority::Skip, Priority::Normal, Priority::Skip],
        TorrentState::Downloading,
    )
    .await;
    let mut r = lh.open_file(1).await.unwrap();
    let requests = log();
    let mut seed = scripted_seed(&l, &m, |_| true).await;
    let (p, rq) = (payload.clone(), Arc::clone(&requests));
    let serve = tokio::spawn(async move { seed.serve_recording(&p, PL as u32, rq).await });
    let mut out = Vec::new();
    tokio::time::timeout(T, r.read_to_end(&mut out))
        .await
        .expect("read in time")
        .unwrap();
    assert!(out == payload[STRADDLE_B..STRADDLE_C], "B exactly");
    let asked = logged(&requests);
    assert!(!asked.is_empty());
    // Pieces 0 to 11 lie inside A, 21 to 32 inside C.
    assert!(
        asked.iter().all(|b| (12..=20).contains(&b.piece)),
        "{:?}",
        asked
            .iter()
            .map(|b| b.piece)
            .collect::<std::collections::BTreeSet<_>>()
    );
    serve.abort();
    l.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reader_on_a_skipped_file_downloads_its_window() {
    let (m, payload) = make_torrent(&STRADDLE, PL as u32);
    let (l, lh) = leech_with(
        settings(),
        &m,
        vec![Priority::Normal, Priority::Normal, Priority::Skip],
        TorrentState::Downloading,
    )
    .await;
    let mut r = lh
        .open_file_with(2, ReadAhead::new(MIB as u64, Duration::from_secs(1), None))
        .await
        .unwrap();
    let requests = log();
    let mut seed = scripted_seed(&l, &m, |_| true).await;
    let (p, rq) = (payload.clone(), Arc::clone(&requests));
    let serve = tokio::spawn(async move { seed.serve_recording(&p, PL as u32, rq).await });
    let mut buf = vec![0u8; MIB];
    tokio::time::timeout(T, r.read_exact(&mut buf))
        .await
        .expect("read in time")
        .unwrap();
    assert!(buf == payload[STRADDLE_C..STRADDLE_C + MIB]);
    // The window after the read ends at C + 2 MiB.
    let limit = STRADDLE_C + 2 * MIB;
    let asked = logged(&requests);
    assert!(
        asked.iter().all(|b| (b.piece as usize) * PL < limit),
        "{:?}",
        asked
            .iter()
            .map(|b| b.piece)
            .collect::<std::collections::BTreeSet<_>>()
    );
    serve.abort();
    l.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finished_torrent_keeps_seeds_for_a_skipped_file_reader() {
    let (m, payload) = make_torrent(&STRADDLE, PL as u32);
    let (seed, _sh) = seeder(&m, &payload).await;
    let seed_addr = seed.local_addr();
    let (l, lh) = leech_with(
        settings(),
        &m,
        vec![Priority::Normal, Priority::Normal, Priority::Skip],
        TorrentState::Downloading,
    )
    .await;
    let mut ev = l.subscribe(EventMask::PEER);
    lh.add_peer(seed_addr).await.unwrap();
    assert!(wait_state(&lh, |s| *s == TorrentState::Seeding, T).await);
    // The only peer is a seed: closed as upload-only.
    next_matching(
        &mut ev,
        |e| matches!(e, Event::PeerDisconnected { addr, .. } if *addr == seed_addr),
        T,
    )
    .await
    .expect("the seed is closed");
    // A peer that hears our upload-only state.
    let mut reserved = fbt_wire::Reserved::default();
    reserved.set_extended();
    let mut observer =
        ScriptedPeer::connect_with(l.local_addr(), m.info_hashes.wire_hash(), reserved).await;
    let mut hs = fbt_wire::ExtHandshake::default();
    hs.m.insert(fbt_wire::UPLOAD_ONLY.to_owned(), 7);
    observer.send_ext_handshake(&hs).await;
    let upload_only = |m: &Message| matches!(m, Message::Extended { id: 7, .. });
    match observer.next_matching(upload_only).await {
        Some(Message::Extended { payload, .. }) => {
            assert!(fbt_wire::decode_upload_only(&payload).unwrap());
        }
        other => panic!("no UploadOnly(true): {other:?}"),
    }
    // Every peer event from here on.
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let collect = {
        let seen = Arc::clone(&seen);
        tokio::spawn(async move {
            while let Some(e) = ev.recv().await {
                seen.lock().unwrap().push(e);
            }
        })
    };
    // A reader on the skipped file.
    let mut r = lh.open_file(2).await.unwrap();
    match observer.next_matching(upload_only).await {
        Some(Message::Extended { payload, .. }) => {
            assert!(!fbt_wire::decode_upload_only(&payload).unwrap());
        }
        other => panic!("no UploadOnly(false): {other:?}"),
    }
    let observe = tokio::spawn(async move { observer.wait_closed(T).await });
    let got = tokio::time::timeout(T, read_at(&mut r, 0, 64 * 1024))
        .await
        .expect("read in time")
        .unwrap();
    assert!(got == payload[STRADDLE_C..STRADDLE_C + 64 * 1024]);
    let events = seen.lock().unwrap().clone();
    // Our redial, or the seed's own dial if it wins the race: the only
    // peer with C's bytes is connected again (the observer connected
    // before these events).
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::PeerConnected { .. })),
        "the seed is connected again: {events:?}"
    );
    // The default window covers all of C: still missing, so nobody is
    // closed as upload-only.
    assert!(
        !events.iter().any(|e| matches!(
            e,
            Event::PeerDisconnected {
                reason: DisconnectReason::UploadOnly,
                ..
            }
        )),
        "{events:?}"
    );
    collect.abort();
    observe.abort();
    l.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seek_away_from_skipped_file_drops_its_partial_pieces() {
    // W (pieces 0 to 3) is wanted, S (pieces 4 to 11) skipped.
    let (m, payload) = make_torrent(&[("w", MIB), ("s", 2 * MIB)], PL as u32);
    let (l, lh) = leech_with(
        settings(),
        &m,
        vec![Priority::Normal, Priority::Skip],
        TorrentState::Downloading,
    )
    .await;
    let mut rs = lh.open_file(1).await.unwrap();
    let pending = tokio::spawn(async move { read_at(&mut rs, 0, 4096).await });
    let (requests, cancels) = (log(), log());
    let answer = Arc::new(AtomicBool::new(false));
    let mut seed = scripted_seed(&l, &m, |i| i >= 4).await;
    let serve = {
        let (p, rq, cn, an) = (
            payload.clone(),
            Arc::clone(&requests),
            Arc::clone(&cancels),
            Arc::clone(&answer),
        );
        tokio::spawn(async move { seed.serve_recording_with(&p, PL as u32, rq, cn, an).await })
    };
    assert!(
        wait_until(
            || !logged(&requests).is_empty() && l.cache_stats().partial > 0,
            T
        )
        .await,
        "S's window is requested"
    );
    // The reader moves to W (a reader reads one file: the one on S goes,
    // one on W comes). No peer has W, so nothing new starts.
    let mut rw = lh.open_file(0).await.unwrap();
    pending.abort();
    let _ = pending.await;
    let waiting = tokio::spawn(async move { read_at(&mut rw, 0, 4096).await });
    assert!(
        wait_until(|| l.cache_stats().partial == 0, Duration::from_secs(5)).await,
        "partial pieces dropped: {:?}",
        l.cache_stats()
    );
    let all_cancelled = || {
        let asked: std::collections::HashSet<BlockRef> = logged(&requests).into_iter().collect();
        let cancelled: std::collections::HashSet<BlockRef> = logged(&cancels).into_iter().collect();
        asked == cancelled
    };
    assert!(
        wait_until(all_cancelled, Duration::from_secs(5)).await,
        "every held block cancelled: {} requests, {} cancels",
        logged(&requests).len(),
        logged(&cancels).len()
    );
    assert!(logged(&requests).iter().all(|b| b.piece >= 4));
    waiting.abort();
    serve.abort();
    l.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn choked_deadline_blocks_go_to_another_peer() {
    let (m, payload) = torrent();
    let (l, lh) = leech(&m).await;
    let ih = m.info_hashes.wire_hash();
    let mut r = lh.open_file(0).await.unwrap();
    let read = tokio::spawn(async move { read_at(&mut r, 0, 4096).await });
    // The first seed takes the window's first requests, then chokes us
    // without rejecting them (fast extension on).
    let mut first = ScriptedPeer::connect_fast(l.local_addr(), ih).await;
    first.send(Message::HaveAll).await;
    first.send(Message::Unchoke).await;
    let held = match first
        .next_matching(|m| matches!(m, Message::Request(_)))
        .await
    {
        Some(Message::Request(b)) => b,
        other => panic!("no request: {other:?}"),
    };
    first.send(Message::Choke).await;
    let keep = tokio::spawn(async move { first.wait_closed(T).await });
    let requests = log();
    let mut second = ScriptedPeer::connect_fast(l.local_addr(), ih).await;
    second.send(Message::HaveAll).await;
    second.send(Message::Unchoke).await;
    let (p, rq) = (payload.clone(), Arc::clone(&requests));
    let serve = tokio::spawn(async move { second.serve_recording(&p, PL as u32, rq).await });
    assert!(
        wait_until(|| logged(&requests).contains(&held), Duration::from_secs(5)).await,
        "{held:?} asked of the second seed: {:?}",
        logged(&requests)
    );
    let got = tokio::time::timeout(T, read)
        .await
        .expect("read in time")
        .unwrap()
        .unwrap();
    assert!(got == payload[..4096]);
    keep.abort();
    serve.abort();
    l.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_peer_chokes_then_unchokes() {
    let (m, payload) = torrent();
    let (l, lh) = leech(&m).await;
    let mut r = lh.open_file(0).await.unwrap();
    let read = tokio::spawn(async move { read_at(&mut r, 0, 4096).await });
    let mut seed = scripted_seed(&l, &m, |_| true).await;
    // Requests the seed receives, with when.
    let log: Arc<std::sync::Mutex<Vec<(tokio::time::Instant, BlockRef)>>> = Arc::default();
    let choked_at: Arc<std::sync::Mutex<Option<tokio::time::Instant>>> = Arc::default();
    let (go, mut go_rx) = tokio::sync::oneshot::channel::<()>();
    let task = {
        let (log, choked_at) = (Arc::clone(&log), Arc::clone(&choked_at));
        let payload = payload.clone();
        tokio::spawn(async move {
            let reply = |b: BlockRef| {
                let start = b.piece as usize * PL + b.offset as usize;
                Message::Piece {
                    piece: b.piece,
                    offset: b.offset,
                    data: Bytes::copy_from_slice(&payload[start..start + b.len as usize]),
                }
            };
            // Two blocks of the first piece, then a choke.
            let mut answered = 0;
            while answered < 2 {
                match seed.framed.next().await {
                    Some(Ok(Message::Request(b))) => {
                        log.lock().unwrap().push((tokio::time::Instant::now(), b));
                        seed.send(reply(b)).await;
                        answered += 1;
                    }
                    Some(Ok(_)) => {}
                    _ => return,
                }
            }
            seed.send(Message::Choke).await;
            *choked_at.lock().unwrap() = Some(tokio::time::Instant::now());
            loop {
                tokio::select! {
                    _ = &mut go_rx => break,
                    m = seed.framed.next() => match m {
                        Some(Ok(Message::Request(b))) => {
                            log.lock().unwrap().push((tokio::time::Instant::now(), b));
                        }
                        Some(Ok(_)) => {}
                        _ => return,
                    },
                }
            }
            seed.send(Message::Unchoke).await;
            while let Some(Ok(m)) = seed.framed.next().await {
                if let Message::Request(b) = m
                    && seed.framed.send(reply(b)).await.is_err()
                {
                    return;
                }
            }
        })
    };
    assert!(
        wait_until(|| choked_at.lock().unwrap().is_some(), T).await,
        "the seed choked us"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!read.is_finished(), "the read waits, with no error");
    let at = choked_at.lock().unwrap().expect("choked");
    // Requests already on the wire when the choke went out may still
    // arrive; none is sent once it is a second old.
    let late: Vec<BlockRef> = log
        .lock()
        .unwrap()
        .iter()
        .filter(|(t, _)| *t > at + Duration::from_secs(1))
        .map(|(_, b)| *b)
        .collect();
    assert!(late.is_empty(), "requests while choked: {late:?}");
    let _ = go.send(());
    let got = tokio::time::timeout(T, read)
        .await
        .expect("read in time after the unchoke")
        .unwrap()
        .unwrap();
    assert!(got == payload[..4096]);
    task.abort();
    l.shutdown().await;
}
