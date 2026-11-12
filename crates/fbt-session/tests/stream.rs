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
use fbt_metainfo::{Magnet, Metainfo};
use fbt_session::storage::{MemStorage, Storage};
use fbt_session::{
    AddTorrent, Event, EventMask, FileReader, ReadAhead, RemoveMode, Session, SessionError,
    Settings, StreamError, TorrentHandle, TorrentState,
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
    // Every write is still held: the bytes came from memory.
    assert!(gate.load(Ordering::SeqCst));
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
    let (l, lh) = leech(&m).await;
    lh.add_peer(half.local_addr()).await.unwrap();
    wait_pieces(&lh, 16).await;
    let mut missing = lh.open_file(2).await.unwrap();
    let offset = (20 * PL - B_START) as u64;
    let waiting = tokio::spawn(async move { read_at(&mut missing, offset, 4096).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!waiting.is_finished());
    // A read of a piece we have, issued just before the recheck.
    let mut have = lh.open_file(0).await.unwrap();
    let have_read = tokio::spawn(async move { read_at(&mut have, 7 * PL as u64, 4096).await });
    lh.force_recheck().await.unwrap();
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
