//! The write-back cache: verified pieces are served from memory before
//! they are written, writes are coalesced, pause and shutdown never lose
//! dirty data, delete discards it, a failed flush stops the torrent, and
//! memory stays within `cache_size` (plus the progress floor) under a slow
//! disk.

mod support;

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use fbt_metainfo::Metainfo;
use fbt_session::storage::{MemStorage, Storage};
use fbt_session::{
    AddTorrent, EncryptionPolicy, Event, EventMask, EventStream, RemoveMode, Session, Settings,
    TorrentHandle, TorrentState,
};
use fbt_wire::Message;
use support::*;

const T: Duration = Duration::from_secs(30);
const MIB: u64 = 1024 * 1024;

fn settings() -> Settings {
    let mut s = dial_tcp(test_settings());
    s.rechoke_interval = Duration::from_secs(1);
    s
}

/// Settings that keep every verified piece dirty: a big cache and an
/// expiry no test outlives.
fn hold_settings() -> Settings {
    let mut s = settings();
    s.cache_size = 256 * MIB;
    s.cache_expiry = Duration::from_secs(600);
    s
}

/// What a [`RecordingStorage`] saw, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    /// A completed write: (offset, len).
    Write(u64, usize),
    DeleteFiles,
}

/// Memory storage that logs every completed write and `delete_files`, can
/// delay writes and fail the next one, and checks every write against the
/// payload. A coalesced run is one `write_vectored` call, logged as one
/// write.
#[derive(Debug)]
struct RecordingStorage {
    inner: MemStorage,
    expected: Vec<u8>,
    ops: Mutex<Vec<Op>>,
    /// Writes whose bytes differ from the payload.
    mismatches: AtomicUsize,
    delay: Duration,
    /// Writes still to be delayed by `delay`.
    slow_writes: AtomicUsize,
    fail_next: AtomicBool,
    /// Writes begun (before their delay).
    started: AtomicUsize,
    written_bytes: AtomicU64,
    /// `write_vectored` calls.
    vectored_calls: AtomicUsize,
}

impl RecordingStorage {
    fn new(expected: Vec<u8>) -> Self {
        RecordingStorage {
            inner: MemStorage::new(expected.len() as u64),
            expected,
            ops: Mutex::new(Vec::new()),
            mismatches: AtomicUsize::new(0),
            delay: Duration::ZERO,
            slow_writes: AtomicUsize::new(0),
            fail_next: AtomicBool::new(false),
            started: AtomicUsize::new(0),
            written_bytes: AtomicU64::new(0),
            vectored_calls: AtomicUsize::new(0),
        }
    }

    /// Every write sleeps `delay` first.
    fn delayed(expected: Vec<u8>, delay: Duration) -> Self {
        Self::delayed_first(expected, delay, usize::MAX)
    }

    /// The first `n` writes sleep `delay` first.
    fn delayed_first(expected: Vec<u8>, delay: Duration, n: usize) -> Self {
        let mut s = Self::new(expected);
        s.delay = delay;
        s.slow_writes = AtomicUsize::new(n);
        s
    }

    fn ops(&self) -> Vec<Op> {
        self.ops.lock().unwrap().clone()
    }

    fn writes(&self) -> Vec<(u64, usize)> {
        self.ops()
            .into_iter()
            .filter_map(|op| match op {
                Op::Write(o, l) => Some((o, l)),
                Op::DeleteFiles => None,
            })
            .collect()
    }

    /// Whether piece `i` (of `piece_len`) reads back as the payload.
    fn holds_piece(&self, i: u32, piece_len: usize) -> bool {
        let start = i as usize * piece_len;
        let end = (start + piece_len).min(self.expected.len());
        let mut buf = vec![0u8; end - start];
        self.inner.read(start as u64, &mut buf).is_ok() && buf == self.expected[start..end]
    }

    fn record_write(&self, offset: u64, bufs: &[&[u8]]) -> io::Result<()> {
        self.started.fetch_add(1, Ordering::SeqCst);
        let slow = self
            .slow_writes
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if slow {
            std::thread::sleep(self.delay);
        }
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(io::Error::new(io::ErrorKind::StorageFull, "disk full"));
        }
        let mut at = offset;
        for data in bufs {
            let start = at as usize;
            if self.expected.get(start..start + data.len()) != Some(*data) {
                self.mismatches.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.write(at, data)?;
            at += data.len() as u64;
        }
        let len = (at - offset) as usize;
        self.written_bytes.fetch_add(len as u64, Ordering::SeqCst);
        self.ops.lock().unwrap().push(Op::Write(offset, len));
        Ok(())
    }
}

impl Storage for RecordingStorage {
    fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, buf)
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.record_write(offset, &[data])
    }
    fn write_vectored(&self, offset: u64, bufs: &[&[u8]]) -> io::Result<()> {
        self.vectored_calls.fetch_add(1, Ordering::SeqCst);
        self.record_write(offset, bufs)
    }
    fn flush(&self) -> io::Result<()> {
        Ok(())
    }
    fn delete_files(&self) -> io::Result<()> {
        self.ops.lock().unwrap().push(Op::DeleteFiles);
        self.inner.delete_files()
    }
}

/// A session with `m` over `storage`, waited until `state`.
async fn session_with(
    st: Settings,
    m: &Metainfo,
    storage: Arc<dyn Storage>,
    sequential: bool,
    state: TorrentState,
) -> (Session, TorrentHandle) {
    let s = Session::new(st).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(storage);
    p.sequential = sequential;
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |x| *x == state, T).await);
    (s, h)
}

/// A seeder holding only the first `have` pieces of `payload` (the rest
/// zeroed), so whoever downloads from it never finishes.
async fn partial_seeder(m: &Metainfo, payload: &[u8], piece_len: usize, have: usize) -> Session {
    let mut data = payload.to_vec();
    data[have * piece_len..].fill(0);
    let storage = Arc::new(MemStorage::from_bytes(data));
    let (s, _h) = session_with(settings(), m, storage, false, TorrentState::Downloading).await;
    s
}

async fn piece_finished(ev: &mut EventStream) -> u32 {
    match next_matching(ev, |e| matches!(e, Event::PieceFinished { .. }), T).await {
        Some(Event::PieceFinished { piece, .. }) => piece,
        other => panic!("no PieceFinished: {other:?}"),
    }
}

/// Every `PieceFinished` still queued in `ev`.
async fn drain_finished(ev: &mut EventStream) -> Vec<u32> {
    let mut out = Vec::new();
    while let Some(Event::PieceFinished { piece, .. }) = next_matching(
        ev,
        |e| matches!(e, Event::PieceFinished { .. }),
        Duration::from_millis(300),
    )
    .await
    {
        out.push(piece);
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cached_piece_is_served_before_it_is_written() {
    let piece_len = 65_536usize;
    let (m, payload) = make_torrent(&[("a", 4 * piece_len)], piece_len as u32);
    let seed = partial_seeder(&m, &payload, piece_len, 2).await;
    let a_storage = Arc::new(RecordingStorage::new(payload.clone()));
    let (a, ah) = session_with(
        hold_settings(),
        &m,
        a_storage.clone(),
        false,
        TorrentState::Downloading,
    )
    .await;
    let mut a_ev = a.subscribe(EventMask::PIECE);
    ah.add_peer(seed.local_addr()).await.unwrap();
    piece_finished(&mut a_ev).await;

    // B talks to A alone (no PEX to learn the seeder from).
    let mut bst = settings();
    bst.pex_enabled = false;
    let b_storage: Arc<dyn Storage> = Arc::new(MemStorage::new(payload.len() as u64));
    let (b, bh) = session_with(bst, &m, b_storage, false, TorrentState::Downloading).await;
    let mut b_ev = b.subscribe(EventMask::PIECE);
    bh.add_peer(a.local_addr()).await.unwrap();
    let got = piece_finished(&mut b_ev).await;
    assert!(got < 2, "B got piece {got}, which only A could have");
    assert!(
        a_storage.writes().is_empty(),
        "A wrote before serving: {:?}",
        a_storage.writes()
    );
    assert!(a.cache_stats().dirty > 0);
    b.shutdown().await;
    a.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_are_coalesced() {
    let piece_len = 32_768usize;
    let pieces = 32;
    let (m, payload) = make_torrent(&[("a", pieces * piece_len)], piece_len as u32);
    let seed_storage: Arc<dyn Storage> = Arc::new(MemStorage::from_bytes(payload.clone()));
    let (seed, _sh) =
        session_with(settings(), &m, seed_storage, false, TorrentState::Seeding).await;
    let storage = Arc::new(RecordingStorage::new(payload.clone()));
    let (l, lh) = session_with(
        settings(),
        &m,
        storage.clone(),
        true,
        TorrentState::Downloading,
    )
    .await;
    let mut ev = l.subscribe(EventMask::STATUS);
    lh.add_peer(seed.local_addr()).await.unwrap();
    next_matching(&mut ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    let writes = storage.writes();
    let calls = storage.vectored_calls.load(Ordering::SeqCst);
    assert_eq!(calls, writes.len(), "every run is one vectored write");
    assert!(calls < pieces, "{calls} writes for {pieces} pieces");
    assert_eq!(
        storage.written_bytes.load(Ordering::SeqCst),
        payload.len() as u64
    );
    assert_eq!(storage.mismatches.load(Ordering::SeqCst), 0);
    assert!((0..pieces as u32).all(|i| storage.holds_piece(i, piece_len)));
    l.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_and_pause_flush_dirty_pieces() {
    let piece_len = 16_384usize;
    let pieces = 64;
    let (m, payload) = make_torrent(&[("a", pieces * piece_len)], piece_len as u32);
    // About 50 pieces a second: the download is still running when paused.
    let seed_storage: Arc<dyn Storage> =
        Arc::new(SlowStorage::new(payload.clone(), Duration::from_millis(20)));
    let (seed, _sh) =
        session_with(settings(), &m, seed_storage, false, TorrentState::Seeding).await;
    let storage = Arc::new(RecordingStorage::delayed(
        payload.clone(),
        Duration::from_millis(200),
    ));
    let (l, lh) = session_with(
        hold_settings(),
        &m,
        storage.clone(),
        false,
        TorrentState::Downloading,
    )
    .await;
    let mut ev = l.subscribe(EventMask::PIECE);
    lh.add_peer(seed.local_addr()).await.unwrap();
    for _ in 0..3 {
        piece_finished(&mut ev).await;
    }
    assert!(
        storage.writes().is_empty(),
        "nothing is written before pause"
    );
    lh.pause().await.unwrap();
    let have = lh.pieces().await.unwrap();
    assert!(have.count_ones() >= 3);
    for i in have.iter_ones() {
        assert!(storage.holds_piece(i, piece_len), "piece {i} lost on pause");
    }
    assert_eq!(l.cache_stats().dirty, 0);

    lh.resume().await.unwrap();
    // The resume rechecks nothing: the download goes on from where it was.
    let before = have.count_ones();
    assert!(
        wait_until(
            || {
                let s = lh.status();
                s.state == TorrentState::Downloading
                    && s.wanted_done >= ((before as usize + 3) * piece_len) as u64
            },
            T
        )
        .await
    );
    let writes_before = storage.writes().len();
    l.shutdown().await;
    let finished = drain_finished(&mut ev).await;
    assert!(!finished.is_empty());
    for i in have.iter_ones().chain(finished) {
        assert!(
            storage.holds_piece(i, piece_len),
            "piece {i} lost on shutdown"
        );
    }
    assert!(storage.writes().len() > writes_before);
    assert_eq!(storage.mismatches.load(Ordering::SeqCst), 0);
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remove_with_delete_discards_dirty_pieces() {
    let piece_len = 65_536usize;
    let (m, payload) = make_torrent(&[("a", 4 * piece_len)], piece_len as u32);
    let seed = partial_seeder(&m, &payload, piece_len, 2).await;
    let storage = Arc::new(RecordingStorage::new(payload.clone()));
    let (l, lh) = session_with(
        hold_settings(),
        &m,
        storage.clone(),
        false,
        TorrentState::Downloading,
    )
    .await;
    let mut ev = l.subscribe(EventMask::PIECE);
    lh.add_peer(seed.local_addr()).await.unwrap();
    piece_finished(&mut ev).await;
    assert!(l.cache_stats().dirty > 0);
    lh.remove(RemoveMode::DeleteData).await.unwrap();
    assert!(
        storage.writes().is_empty(),
        "written after a delete: {:?}",
        storage.writes()
    );
    // Every byte the torrent held went back to the budget.
    let stats = l.cache_stats();
    assert_eq!((stats.dirty, stats.clean, stats.partial), (0, 0, 0));
    l.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flush_failure_stops_the_torrent() {
    let piece_len = 65_536usize;
    let (m, payload) = make_torrent(&[("a", 4 * piece_len)], piece_len as u32);
    let seed_storage: Arc<dyn Storage> = Arc::new(MemStorage::from_bytes(payload.clone()));
    let (seed, _sh) =
        session_with(settings(), &m, seed_storage, false, TorrentState::Seeding).await;
    let storage = Arc::new(RecordingStorage::new(payload.clone()));
    storage.fail_next.store(true, Ordering::SeqCst);
    let (l, lh) = session_with(
        settings(),
        &m,
        storage.clone(),
        false,
        TorrentState::Downloading,
    )
    .await;
    let mut ev = l.subscribe(EventMask::ALL);
    lh.add_peer(seed.local_addr()).await.unwrap();
    let e = next_matching(&mut ev, |e| matches!(e, Event::FileError { .. }), T)
        .await
        .expect("FileError");
    let Event::FileError { message, .. } = e else {
        unreachable!()
    };
    assert!(message.contains("disk full"), "{message}");
    assert!(wait_state(&lh, |s| *s == TorrentState::Error, T).await);
    // The dropped pieces leave the budget once the writes still in flight
    // return.
    assert!(wait_until(|| l.cache_stats().dirty == 0, T).await);

    lh.resume().await.unwrap();
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
    .expect("a recheck after resume");
    // The storage works again: the download completes.
    next_matching(&mut ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    assert!((0..4).all(|i| storage.holds_piece(i, piece_len)));
    l.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_stays_within_budget() {
    const PROGRESS_FLOOR_PIECES: u64 = 4;
    let piece_len = 256 * 1024usize;
    let (m, payload) = make_torrent(&[("a", 64 * MIB as usize)], piece_len as u32);
    let seed_storage: Arc<dyn Storage> = Arc::new(MemStorage::from_bytes(payload.clone()));
    let (seed, _sh) =
        session_with(settings(), &m, seed_storage, false, TorrentState::Seeding).await;
    let mut st = settings();
    st.cache_size = 4 * MIB;
    // The disk lags behind the network for the first writes (2 s each),
    // long enough to fill the budget; the rest run at memory speed so the
    // test stays short.
    let storage = Arc::new(RecordingStorage::delayed_first(
        payload.clone(),
        Duration::from_secs(2),
        16,
    ));
    let (l, lh) = session_with(st, &m, storage.clone(), true, TorrentState::Downloading).await;
    let mut ev = l.subscribe(EventMask::STATUS);
    lh.add_peer(seed.local_addr()).await.unwrap();
    let bound = 4 * MIB + PROGRESS_FLOOR_PIECES * piece_len as u64;
    let mut peak = 0u64;
    let mut ticker = tokio::time::interval(Duration::from_millis(50));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let finished = loop {
        tokio::select! {
            e = ev.recv() => {
                if matches!(e, Some(Event::TorrentFinished { .. })) {
                    // The slow phase was part of the download.
                    assert_eq!(storage.slow_writes.load(Ordering::SeqCst), 0);
                    break true;
                }
            }
            _ = ticker.tick() => {
                let s = l.cache_stats();
                assert_eq!(s.limit, 4 * MIB);
                let used = s.dirty + s.clean + s.partial;
                peak = peak.max(used);
                assert!(used <= bound, "{used} bytes cached, bound {bound}: {s:?}");
                if tokio::time::Instant::now() >= deadline {
                    break false;
                }
            }
        }
    };
    assert!(finished, "download did not finish");
    // The budget was actually filled while the disk lagged.
    assert!(peak >= 4 * MIB, "peak {peak}");
    assert_eq!(storage.mismatches.load(Ordering::SeqCst), 0);
    assert_eq!(
        storage.written_bytes.load(Ordering::SeqCst),
        payload.len() as u64
    );
    l.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_unverified_bytes_reach_storage() {
    let piece_len = 16_384usize;
    let pieces = 8u32;
    let (m, payload) = make_torrent(&[("a", pieces as usize * piece_len)], piece_len as u32);
    let mut st = settings();
    // Write-through: every verified piece is written at once.
    st.cache_size = 0;
    st.encryption = EncryptionPolicy::Disabled;
    let storage = Arc::new(RecordingStorage::new(payload.clone()));
    let (l, lh) = session_with(st, &m, storage.clone(), false, TorrentState::Downloading).await;
    let mut ev = l.subscribe(EventMask::PIECE);
    let mut peer = ScriptedPeer::connect(l.local_addr(), m.info_hashes.wire_hash()).await;
    peer.send(Message::Bitfield(Bytes::from_static(&[0xff])))
        .await;
    peer.send(Message::Unchoke).await;
    // Piece 0 comes corrupt; everything else is good.
    let serve = tokio::spawn(async move {
        use futures_util::{SinkExt, StreamExt};
        while let Some(Ok(msg)) = peer.framed.next().await {
            if let Message::Request(b) = msg {
                let start = b.piece as usize * piece_len + b.offset as usize;
                let mut data = payload[start..start + b.len as usize].to_vec();
                if b.piece == 0 {
                    data.iter_mut().for_each(|x| *x ^= 0xff);
                }
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
        |e| matches!(e, Event::HashFailed { piece: 0, .. }),
        T,
    )
    .await
    .expect("HashFailed for piece 0");
    // The good pieces are written; piece 0's bytes never are.
    assert!(
        wait_until(
            || storage.writes().len() as u32 >= pieces - 1,
            Duration::from_secs(10)
        )
        .await
    );
    assert_eq!(storage.mismatches.load(Ordering::SeqCst), 0);
    assert!(
        storage
            .writes()
            .iter()
            .all(|&(off, len)| off >= piece_len as u64 || len == 0),
        "piece 0 reached storage: {:?}",
        storage.writes()
    );
    assert!(!lh.pieces().await.unwrap().get(0));
    serve.abort();
    l.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remove_with_delete_waits_for_writes_in_flight() {
    let piece_len = 65_536usize;
    let (m, payload) = make_torrent(&[("a", 4 * piece_len)], piece_len as u32);
    let seed = partial_seeder(&m, &payload, piece_len, 2).await;
    let mut st = settings();
    // Write-through, so a verified piece's write starts at once.
    st.cache_size = 0;
    let storage = Arc::new(RecordingStorage::delayed(
        payload.clone(),
        Duration::from_millis(500),
    ));
    let (l, lh) = session_with(st, &m, storage.clone(), false, TorrentState::Downloading).await;
    lh.add_peer(seed.local_addr()).await.unwrap();
    assert!(wait_until(|| storage.started.load(Ordering::SeqCst) > 0, T).await);
    assert!(
        storage.writes().is_empty(),
        "the first write is still running"
    );
    lh.remove(RemoveMode::DeleteData).await.unwrap();
    let ops = storage.ops();
    let started = storage.started.load(Ordering::SeqCst);
    assert_eq!(ops.last(), Some(&Op::DeleteFiles), "{ops:?}");
    // Every write that started completed, before the delete.
    assert_eq!(storage.writes().len(), started, "{ops:?}");
    let stats = l.cache_stats();
    assert_eq!((stats.dirty, stats.clean, stats.partial), (0, 0, 0));
    l.shutdown().await;
    seed.shutdown().await;
}

/// `Seeding` means complete on disk: with slow writes the torrent stays
/// `Downloading` at 100 % until the last write and the flush are done.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seeding_comes_after_the_last_write() {
    let piece_len = 65_536usize;
    let pieces = 8;
    let (m, payload) = make_torrent(&[("a", pieces * piece_len)], piece_len as u32);
    let seed_storage: Arc<dyn Storage> = Arc::new(MemStorage::from_bytes(payload.clone()));
    let (seed, _sh) =
        session_with(settings(), &m, seed_storage, false, TorrentState::Seeding).await;
    let storage = Arc::new(RecordingStorage::delayed(
        payload.clone(),
        Duration::from_millis(300),
    ));
    let (l, lh) = session_with(
        settings(),
        &m,
        storage.clone(),
        false,
        TorrentState::Downloading,
    )
    .await;
    lh.add_peer(seed.local_addr()).await.unwrap();
    let mut saw_complete_in_memory = false;
    let end = tokio::time::Instant::now() + T;
    loop {
        let s = lh.status();
        if s.state == TorrentState::Seeding {
            break;
        }
        if s.state == TorrentState::Downloading && s.wanted_done == s.total_wanted {
            saw_complete_in_memory = true;
        }
        assert!(tokio::time::Instant::now() < end, "never seeding");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        storage.written_bytes.load(Ordering::SeqCst),
        payload.len() as u64
    );
    assert!((0..pieces as u32).all(|i| storage.holds_piece(i, piece_len)));
    assert!(
        saw_complete_in_memory,
        "100 % while the writes ran is still Downloading"
    );
    l.shutdown().await;
    seed.shutdown().await;
}

/// One torrent's full read cache does not starve another torrent's
/// download: the clean entries are one session-wide LRU, so the download
/// evicts them, and floor pieces are written at once instead of at
/// `cache_expiry`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seeding_read_cache_does_not_starve_downloads() {
    let piece_len = 256 * 1024usize;
    let mut st = settings();
    st.cache_size = 2 * MIB;
    st.cache_expiry = Duration::from_secs(600);
    let (mx, px) = make_torrent(&[("x", 4 * MIB as usize)], piece_len as u32);
    let (my, py) = make_torrent(&[("y", 4 * MIB as usize)], piece_len as u32);
    // Session S seeds X; a leecher fills S's read cache with X's pieces.
    let x_storage: Arc<dyn Storage> = Arc::new(MemStorage::from_bytes(px.clone()));
    let (s, _xh) = session_with(st, &mx, x_storage, false, TorrentState::Seeding).await;
    let l1_storage: Arc<dyn Storage> = Arc::new(MemStorage::new(px.len() as u64));
    let (l1, l1h) = session_with(
        settings(),
        &mx,
        l1_storage,
        false,
        TorrentState::Downloading,
    )
    .await;
    let mut l1_ev = l1.subscribe(EventMask::STATUS);
    l1h.add_peer(s.local_addr()).await.unwrap();
    next_matching(
        &mut l1_ev,
        |e| matches!(e, Event::TorrentFinished { .. }),
        T,
    )
    .await
    .expect("X downloaded");
    let full = s.cache_stats();
    assert!(full.clean >= 3 * MIB / 2, "read cache not full: {full:?}");

    // Now S downloads Y from another seeder while X's entries fill its
    // cache.
    let y_seed_storage: Arc<dyn Storage> = Arc::new(MemStorage::from_bytes(py.clone()));
    let (z, _zh) = session_with(
        settings(),
        &my,
        y_seed_storage,
        false,
        TorrentState::Seeding,
    )
    .await;
    let y_storage = Arc::new(RecordingStorage::new(py.clone()));
    let mut p = AddTorrent::new(my.clone(), PathBuf::from("unused"));
    p.storage = Some(y_storage.clone());
    let yh = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&yh, |x| *x == TorrentState::Downloading, T).await);
    let mut ev = s.subscribe(EventMask::STATUS);
    let t0 = tokio::time::Instant::now();
    yh.add_peer(z.local_addr()).await.unwrap();
    next_matching(
        &mut ev,
        |e| matches!(e, Event::TorrentFinished { torrent } if *torrent == my.info_hashes),
        Duration::from_secs(20),
    )
    .await
    .expect("Y finished well before cache_expiry");
    assert!(t0.elapsed() < Duration::from_secs(20));
    assert!((0..16).all(|i| y_storage.holds_piece(i, piece_len)));
    z.shutdown().await;
    l1.shutdown().await;
    s.shutdown().await;
}
