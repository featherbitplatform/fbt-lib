//! The part file: skipped files are never created. Their bytes in the
//! boundary pieces (and in pieces a reader streams) live in
//! `<save_path>/.<info hash>.parts` until the file is wanted again.
//!
//! Torrent: A 300,000 B (skipped), B 500,000 B, C 300,000 B (skipped),
//! 256 KiB pieces: pieces 1 (A/B) and 3 (B/C) straddle a boundary, 0 and 4
//! lie wholly in a skipped file.

mod support;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fbt_core::BlockRef;
use fbt_metainfo::Metainfo;
use fbt_session::storage::{FsStorage, MemStorage};
use fbt_session::{
    AddTorrent, Event, EventMask, EventStream, Priority, RemoveMode, Session, Settings,
    TorrentHandle, TorrentState,
};
use fbt_wire::{Handshake, Message, MessageCodec, Reserved, read_handshake, write_handshake};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

use support::*;

const PL: u32 = 262_144;
const A: usize = 300_000;
const B: usize = 500_000;
const C: usize = 300_000;
const FILES: [(&str, usize); 3] = [("a", A), ("b", B), ("c", C)];

fn settings() -> Settings {
    let mut s = test_settings();
    s.rechoke_interval = Duration::from_secs(1);
    s
}

fn skip_a_and_c() -> Vec<Priority> {
    vec![Priority::Skip, Priority::Normal, Priority::Skip]
}

/// A session seeding the whole torrent from memory.
async fn seeder(m: &Metainfo, payload: &[u8]) -> Session {
    let s = Session::new(settings()).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(payload.to_vec())));
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    s
}

/// A leecher on disk below `dir` with `prio`, its events subscribed before
/// the add.
async fn leecher(
    st: Settings,
    m: &Metainfo,
    dir: &Path,
    prio: Vec<Priority>,
) -> (Session, TorrentHandle, EventStream) {
    let s = Session::new(st).await.unwrap();
    let ev = s.subscribe(EventMask::ALL);
    let mut p = AddTorrent::new(m.clone(), dir.to_path_buf());
    p.file_priorities = prio;
    let h = s.add_torrent(p).await.unwrap();
    (s, h, ev)
}

fn file(dir: &Path, name: &str) -> PathBuf {
    dir.join("torrent").join(name)
}

fn part_path(dir: &Path, m: &Metainfo) -> PathBuf {
    FsStorage::new(dir.to_path_buf(), m).part_path()
}

async fn finished(ev: &mut EventStream) {
    next_matching(ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
}

/// The torrent's pieces, once it is seeding.
async fn have(h: &TorrentHandle) -> Vec<u32> {
    assert!(wait_state(h, |s| *s == TorrentState::Seeding, T).await);
    h.pieces().await.unwrap().iter_ones().collect()
}

/// Reads `n` bytes of file `index` from `offset`, within `T`.
async fn read_file(h: &TorrentHandle, index: usize, offset: u64, n: usize) -> Vec<u8> {
    let mut r = h.open_file(index).await.unwrap();
    r.seek(std::io::SeekFrom::Start(offset)).await.unwrap();
    let mut buf = vec![0u8; n];
    tokio::time::timeout(T, r.read_exact(&mut buf))
        .await
        .expect("read in time")
        .unwrap();
    buf
}

/// A seed listening on loopback (TCP, no encryption): every connection
/// gets `HaveAll` and `Unchoke`, and every request is answered and logged.
async fn recording_seed(payload: &[u8]) -> (SocketAddr, Arc<Mutex<Vec<BlockRef>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    let payload = Arc::new(payload.to_vec());
    let l = Arc::clone(&log);
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let log = Arc::clone(&l);
            let payload = Arc::clone(&payload);
            tokio::spawn(async move {
                let Ok(Ok(remote)) = tokio::time::timeout(T, read_handshake(&mut stream)).await
                else {
                    return;
                };
                let mut reserved = Reserved::default();
                reserved.set_fast();
                let ours = Handshake {
                    reserved,
                    info_hash: remote.info_hash,
                    peer_id: fbt_core::PeerId([7; 20]),
                };
                if write_handshake(&mut stream, &ours).await.is_err() {
                    return;
                }
                let mut peer = ScriptedPeer {
                    framed: Framed::new(stream, MessageCodec::new(1 << 20)),
                    remote,
                };
                peer.send(Message::HaveAll).await;
                peer.send(Message::Unchoke).await;
                peer.serve_recording(&payload, PL, log).await;
            });
        }
    });
    (addr, log)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skipped_neighbour_is_never_created() {
    let (m, payload) = make_torrent(&FILES, PL);
    let seed = seeder(&m, &payload).await;
    let dir = tempfile::tempdir().unwrap();
    let (s, h, mut ev) = leecher(settings(), &m, dir.path(), skip_a_and_c()).await;
    h.add_peer(seed.local_addr()).await.unwrap();
    finished(&mut ev).await;
    assert_eq!(have(&h).await, vec![1, 2, 3]);
    // finished its wanted pieces but not a full seed: no BEP 33 `seed`
    assert!(!h.status().is_seed);
    assert!(!file(dir.path(), "a").exists(), "A created");
    assert!(!file(dir.path(), "c").exists(), "C created");
    assert!(part_path(dir.path(), &m).exists(), "no part file");
    assert!(std::fs::read(file(dir.path(), "b")).unwrap() == payload[A..A + B]);
    s.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unskipping_exports_from_the_part_file() {
    let (m, payload) = make_torrent(&FILES, PL);
    let (seed, log) = recording_seed(&payload).await;
    let dir = tempfile::tempdir().unwrap();
    let (s, h, mut ev) = leecher(dial_tcp(settings()), &m, dir.path(), skip_a_and_c()).await;
    h.add_peer(seed).await.unwrap();
    finished(&mut ev).await;
    assert!(!file(dir.path(), "a").exists());
    assert!(part_path(dir.path(), &m).exists());
    assert!(requests_of(&log, 1, 0) > 0, "piece 1 came from the seed");
    let before = log.lock().unwrap().len();

    h.set_file_priorities(vec![Priority::Normal, Priority::Normal, Priority::Skip])
        .await
        .unwrap();
    finished(&mut ev).await;
    assert_eq!(have(&h).await, vec![0, 1, 2, 3]);
    assert!(std::fs::read(file(dir.path(), "a")).unwrap() == payload[..A]);
    assert!(!file(dir.path(), "c").exists());
    // Piece 3 still holds C's bytes.
    assert!(part_path(dir.path(), &m).exists());
    // Piece 1 came from the part file, not the seed.
    assert_eq!(requests_of(&log, 1, before), 0, "piece 1 requested again");
    s.shutdown().await;
}

/// Requests for `piece` logged from entry `from` on.
fn requests_of(log: &Mutex<Vec<BlockRef>>, piece: u32, from: usize) -> usize {
    let log = log.lock().unwrap();
    log.get(from..)
        .unwrap_or_default()
        .iter()
        .filter(|b| b.piece == piece)
        .count()
}

/// The part file outlives its session: a restart with the same priorities
/// keeps the boundary pieces, and one with A wanted exports A's bytes from
/// the part file instead of discarding them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn part_file_survives_a_restart() {
    let (m, payload) = make_torrent(&FILES, PL);
    let (seed, log) = recording_seed(&payload).await;
    let dir = tempfile::tempdir().unwrap();
    let (s, h, mut ev) = leecher(dial_tcp(settings()), &m, dir.path(), skip_a_and_c()).await;
    h.add_peer(seed).await.unwrap();
    finished(&mut ev).await;
    assert!(requests_of(&log, 1, 0) > 0);
    s.shutdown().await;
    let before = log.lock().unwrap().len();

    // Same priorities, no peers: the check finds 1 and 3 in the part file.
    let (s, h, _ev) = leecher(dial_tcp(settings()), &m, dir.path(), skip_a_and_c()).await;
    assert_eq!(have(&h).await, vec![1, 2, 3]);
    assert!(!file(dir.path(), "a").exists());
    s.shutdown().await;

    // A wanted at add: its bytes in piece 1 are exported, not freed.
    let prio = vec![Priority::Normal, Priority::Normal, Priority::Skip];
    let (s, h, mut ev) = leecher(dial_tcp(settings()), &m, dir.path(), prio).await;
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);
    assert!(
        h.pieces().await.unwrap().get(1),
        "piece 1 lost at the restart"
    );
    h.add_peer(seed).await.unwrap();
    finished(&mut ev).await;
    assert_eq!(have(&h).await, vec![0, 1, 2, 3]);
    assert!(std::fs::read(file(dir.path(), "a")).unwrap() == payload[..A]);
    assert!(!file(dir.path(), "c").exists());
    assert_eq!(requests_of(&log, 1, before), 0, "piece 1 requested again");
    s.shutdown().await;
}

/// A corrupt part file found at add is set aside as `.parts.bad`; the
/// torrent downloads its boundary pieces again and finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_part_file_at_add_is_set_aside() {
    let (m, payload) = make_torrent(&FILES, PL);
    let seed = seeder(&m, &payload).await;
    let dir = tempfile::tempdir().unwrap();
    let part = part_path(dir.path(), &m);
    let junk = b"FBTPART1\xff\xff\xff\xffjunk".to_vec();
    std::fs::write(&part, &junk).unwrap();
    let (s, h, mut ev) = leecher(settings(), &m, dir.path(), skip_a_and_c()).await;
    h.add_peer(seed.local_addr()).await.unwrap();
    finished(&mut ev).await;
    assert_eq!(have(&h).await, vec![1, 2, 3]);
    let mut bad = part.clone().into_os_string();
    bad.push(".bad");
    assert_eq!(std::fs::read(&bad).unwrap(), junk);
    assert!(part.exists(), "a fresh part file holds the boundary bytes");
    assert!(!file(dir.path(), "a").exists());
    s.shutdown().await;
    seed.shutdown().await;
}

/// A zero-length skipped file is never created; once wanted, it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_length_skipped_file_is_never_created() {
    let (m, payload) = make_torrent(&[("a", A), ("e", 0), ("b", B)], PL);
    let seed = seeder(&m, &payload).await;
    let dir = tempfile::tempdir().unwrap();
    let prio = vec![Priority::Normal, Priority::Skip, Priority::Normal];
    let (s, h, mut ev) = leecher(settings(), &m, dir.path(), prio).await;
    h.add_peer(seed.local_addr()).await.unwrap();
    finished(&mut ev).await;
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    let e = file(dir.path(), "e");
    assert!(file(dir.path(), "a").exists());
    assert!(!e.exists(), "skipped empty file created");
    h.set_file_priorities(vec![Priority::Normal; 3])
        .await
        .unwrap();
    assert!(e.exists(), "wanted empty file missing");
    s.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reskipping_keeps_data_readable() {
    let (m, payload) = make_torrent(&FILES, PL);
    let seed = seeder(&m, &payload).await;
    let dir = tempfile::tempdir().unwrap();
    let prio = vec![Priority::Normal, Priority::Normal, Priority::Skip];
    let (s, h, mut ev) = leecher(settings(), &m, dir.path(), prio).await;
    h.add_peer(seed.local_addr()).await.unwrap();
    finished(&mut ev).await;
    assert_eq!(have(&h).await, vec![0, 1, 2, 3]);
    let a = file(dir.path(), "a");
    assert!(std::fs::read(&a).unwrap() == payload[..A]);

    // A exists, so skipping it again keeps it where it is.
    h.set_file_priorities(skip_a_and_c()).await.unwrap();
    assert!(std::fs::read(&a).unwrap() == payload[..A]);
    h.force_recheck().await.unwrap();
    assert_eq!(have(&h).await, vec![0, 1, 2, 3]);
    assert!(std::fs::read(&a).unwrap() == payload[..A]);
    assert!(read_file(&h, 0, 0, A).await == payload[..A]);
    s.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recheck_reads_the_part_file() {
    let (m, payload) = make_torrent(&FILES, PL);
    let seed = seeder(&m, &payload).await;
    let dir = tempfile::tempdir().unwrap();
    let (s, h, mut ev) = leecher(settings(), &m, dir.path(), skip_a_and_c()).await;
    h.add_peer(seed.local_addr()).await.unwrap();
    finished(&mut ev).await;
    assert_eq!(have(&h).await, vec![1, 2, 3]);
    h.force_recheck().await.unwrap();
    // The boundary pieces pass from the part file and B together.
    assert_eq!(have(&h).await, vec![1, 2, 3]);
    assert!(!file(dir.path(), "a").exists());
    assert!(!file(dir.path(), "c").exists());
    assert!(part_path(dir.path(), &m).exists());
    s.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_removes_the_part_file() {
    let (m, payload) = make_torrent(&FILES, PL);
    let seed = seeder(&m, &payload).await;
    let dir = tempfile::tempdir().unwrap();
    let (s, h, mut ev) = leecher(settings(), &m, dir.path(), skip_a_and_c()).await;
    h.add_peer(seed.local_addr()).await.unwrap();
    finished(&mut ev).await;
    let part = part_path(dir.path(), &m);
    assert!(part.exists());
    h.remove(RemoveMode::DeleteData).await.unwrap();
    assert!(!part.exists(), "part file left behind");
    assert!(!file(dir.path(), "b").exists());
    assert!(!dir.path().join("torrent").exists());
    s.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reader_on_a_skipped_file_reads_the_part_file() {
    let (m, payload) = make_torrent(&FILES, PL);
    let seed = seeder(&m, &payload).await;
    let dir = tempfile::tempdir().unwrap();
    let (s, h, mut ev) = leecher(settings(), &m, dir.path(), skip_a_and_c()).await;
    h.add_peer(seed.local_addr()).await.unwrap();
    finished(&mut ev).await;
    // All of C: piece 3 is there, piece 4 is fetched for the reader.
    assert!(read_file(&h, 2, 0, C).await == payload[A + B..]);
    assert!(!file(dir.path(), "c").exists(), "C created by the reader");
    assert!(part_path(dir.path(), &m).exists());
    s.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn existing_skipped_file_is_used_in_place() {
    let (m, payload) = make_torrent(&FILES, PL);
    let dir = tempfile::tempdir().unwrap();
    write_payload(dir.path(), &m, &payload);
    let (s, h, _ev) = leecher(settings(), &m, dir.path(), skip_a_and_c()).await;
    assert_eq!(have(&h).await, vec![0, 1, 2, 3, 4]);
    h.force_recheck().await.unwrap();
    assert_eq!(have(&h).await, vec![0, 1, 2, 3, 4]);
    assert!(!part_path(dir.path(), &m).exists(), "part file created");
    assert!(std::fs::read(file(dir.path(), "a")).unwrap() == payload[..A]);
    assert!(std::fs::read(file(dir.path(), "c")).unwrap() == payload[A + B..]);
    s.shutdown().await;
}

/// Milestone 4 deferral: losing a piece no wanted file overlaps does not
/// make the torrent unfinished, so getting it back emits no second
/// `TorrentFinished`.
///
/// Review note (0.8 deflake; not reproduced: 100 runs at 4 copies, then 100
/// more at 24 copies with the machine under CPU load): every wait here is a
/// bounded condition loop on a real condition — the part file's size is polled
/// until the bytes the test is about to cut away are actually on disk, and both
/// the read and the `FileError` get `T` — so nothing waits on a fixed sleep. The
/// one budget under this plan's 5 s floor was the absence check below: 3 s of
/// silence after a re-download that starts with a fresh dial on a runner of
/// unknown load. An absence check is only as strong as its window, so it gets
/// 5 s now.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_skipped_piece_does_not_refinish() {
    let (m, payload) = make_torrent(&FILES, PL);
    let seed = seeder(&m, &payload).await;
    let dir = tempfile::tempdir().unwrap();
    // No clean cache: every read reaches the disk.
    let mut st = settings();
    st.cache_size = 0;
    let (s, h, mut ev) = leecher(st, &m, dir.path(), skip_a_and_c()).await;
    h.add_peer(seed.local_addr()).await.unwrap();
    finished(&mut ev).await;
    // A reader brings piece 4 (wholly in C) into the part file.
    assert!(read_file(&h, 2, 0, C).await == payload[A + B..]);
    // On disk, then gone: the part file keeps its header and table only.
    // (Pieces 1 and 3 lose their bytes too, but nothing reads them here.)
    let part = part_path(dir.path(), &m);
    // Pieces 1 and 3 took slots 0 and 1, piece 4 slot 2.
    let written = (16 + 5 * 4 + 2 * PL as usize + (A + B + C - 4 * PL as usize)) as u64;
    assert!(
        wait_until(
            || std::fs::metadata(&part).is_ok_and(|md| md.len() >= written),
            T
        )
        .await
    );
    std::fs::OpenOptions::new()
        .write(true)
        .open(&part)
        .unwrap()
        .set_len(16 + 5 * 4)
        .unwrap();
    // C's bytes in piece 4 only: reading them finds the piece lost, and the
    // reader waits for it to be downloaded again.
    let in_piece_4 = 4 * PL as usize - (A + B);
    let got = read_file(&h, 2, in_piece_4 as u64, C - in_piece_4).await;
    assert!(got == payload[4 * PL as usize..]);
    let lost = next_matching(&mut ev, |e| matches!(e, Event::FileError { .. }), T)
        .await
        .expect("the piece was lost");
    let Event::FileError { path, .. } = lost else {
        unreachable!()
    };
    assert_eq!(path.as_deref(), Some(part.as_path()));
    // Exactly one `TorrentFinished`: none follows the re-download.
    let again = next_matching(
        &mut ev,
        |e| matches!(e, Event::TorrentFinished { .. }),
        Duration::from_secs(5),
    )
    .await;
    assert!(again.is_none(), "finished twice");
    s.shutdown().await;
    seed.shutdown().await;
}
