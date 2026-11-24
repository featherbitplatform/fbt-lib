//! Per-file in-order download, switchable at runtime
//! (`TorrentHandle::set_file_in_order`): one file's pieces complete in
//! index order while the torrent's other pieces keep downloading rarest
//! first.

mod support;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fbt_core::{InfoHashV1, InfoHashes};
use fbt_metainfo::Magnet;
use fbt_session::storage::{MemStorage, Storage};
use fbt_session::{AddTorrent, Event, EventMask, Session, SessionError, Settings, TorrentState};
use support::*;

const T: Duration = Duration::from_secs(30);
const MIB: usize = 1024 * 1024;
/// Piece length: 64 KiB, so each 1 MiB file is 16 pieces.
const PL: usize = 64 * 1024;
/// Three files of 1 MiB each: 48 pieces total.
/// File 0: pieces 0 to 15, file 1: pieces 16 to 31, file 2: pieces 32 to 47.
const FILES: [(&str, usize); 3] = [("a", MIB), ("b", MIB), ("c", MIB)];
/// Total pieces.
const NPIECES: u32 = 48;
/// File 1's first piece.
const F1_FIRST: u32 = 16;
/// Number of pieces in file 1.
const F1_N: u32 = 16;

fn settings() -> Settings {
    let mut s = test_settings();
    s.rechoke_interval = Duration::from_secs(1);
    s
}

fn torrent() -> (fbt_metainfo::Metainfo, Vec<u8>) {
    make_torrent(&FILES, PL as u32)
}

/// A session with `m` over `storage`, waited until `state`.
async fn session_with(
    st: Settings,
    m: &fbt_metainfo::Metainfo,
    storage: Arc<dyn Storage>,
    state: TorrentState,
) -> (Session, fbt_session::TorrentHandle) {
    let s = Session::new(st).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(storage);
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |x| *x == state, T).await, "{state:?}");
    (s, h)
}

async fn seeder(
    m: &fbt_metainfo::Metainfo,
    payload: &[u8],
) -> (Session, fbt_session::TorrentHandle) {
    let storage = Arc::new(MemStorage::from_bytes(payload.to_vec()));
    session_with(settings(), m, storage, TorrentState::Seeding).await
}

/// An empty leech over memory.
async fn leech(m: &fbt_metainfo::Metainfo) -> (Session, fbt_session::TorrentHandle) {
    let storage = Arc::new(MemStorage::new(m.total_len()));
    session_with(settings(), m, storage, TorrentState::Downloading).await
}

/// A piece of file 1.
fn is_f1(p: u32) -> bool {
    (F1_FIRST..F1_FIRST + F1_N).contains(&p)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_in_order_downloads_its_pieces_in_order() {
    let (m, payload) = torrent();
    let (seed, _sh) = seeder(&m, &payload).await;
    let (leech, lh) = leech(&m).await;
    let mut events = leech.subscribe(EventMask::PIECE | EventMask::STATUS);

    // Before a piece of file 1 completes, file 1 downloads in order: its
    // first completed piece is the first one, and the order has no
    // inversion greater than the pipeline depth (blocks of a few pieces can
    // cross in flight).
    let r = lh.set_file_in_order(1, true).await;
    assert!(r.is_ok(), "{r:?}");
    lh.add_peer(seed.local_addr()).await.unwrap();

    // With one seed and request pipelining a small bounded inversion is
    // acceptable (the brief says so). The pipeline depth is bounded by
    // max_outstanding_requests / blocks-per-piece; with 64 KiB pieces and
    // 16 KiB blocks that is 4 blocks per piece, so at most a handful of
    // pieces can be in flight at once.
    let max_inversion = 8;
    let end = tokio::time::Instant::now() + T;
    let mut order: Vec<u32> = Vec::new();
    let mut max_inv = 0usize;
    while order.len() < F1_N as usize {
        assert!(
            tokio::time::Instant::now() < end,
            "file 1 completed in time"
        );
        let Some(ev) = events.recv().await else {
            continue;
        };
        if let Event::PieceFinished { torrent, piece } = &ev
            && *torrent == lh.info_hashes()
            && is_f1(*piece)
        {
            order.push(*piece);
            let k = order.len();
            for i in 0..k.saturating_sub(1) {
                if order[i] > order[i + 1] {
                    max_inv = max_inv.max((order[i] - order[i + 1]) as usize);
                }
            }
        }
    }
    assert_eq!(
        order.first(),
        Some(&F1_FIRST),
        "the first completed piece of file 1 is its first: {order:?}"
    );
    assert!(
        max_inv <= max_inversion,
        "no inversion greater than {max_inversion}: {order:?}"
    );
    assert!(
        wait_state(&lh, |s| *s == TorrentState::Seeding, T).await,
        "the leech finished"
    );
    let haves = lh.pieces().await.unwrap();
    assert_eq!(haves.count_ones(), NPIECES);
    let _ = seed;
}

#[tokio::test]
async fn file_in_order_rejects_bad_index() {
    let (m, payload) = torrent();
    let (seed, _sh) = seeder(&m, &payload).await;
    let (_leech, lh) = leech(&m).await;
    let r = lh.set_file_in_order(3, true).await;
    assert!(
        matches!(
            r,
            Err(SessionError::InvalidFileIndex { index: 3, files: 3 })
        ),
        "{r:?}"
    );
    // A valid file is accepted: the torrent keeps downloading to the end.
    assert!(lh.set_file_in_order(1, true).await.is_ok());
    lh.add_peer(seed.local_addr()).await.unwrap();
    assert!(
        wait_state(&lh, |s| *s == TorrentState::Seeding, T).await,
        "the leech finished and is seeding"
    );
    let have = lh.pieces().await.unwrap();
    assert_eq!(have.count_ones(), NPIECES);
    let _ = seed;
}

#[tokio::test]
async fn file_in_order_needs_metadata() {
    let session = Session::new(settings()).await.unwrap();
    let mg = Magnet::from_info_hashes(InfoHashes::from_v1(InfoHashV1([5; 20])));
    let mut p = AddTorrent::magnet(mg.clone(), PathBuf::from("unused"));
    p.paused = true;
    let h = session.add_torrent(p).await.unwrap();
    let r = h.set_file_in_order(0, true).await;
    assert!(matches!(r, Err(SessionError::NoMetadata)), "{r:?}");
    let r = h.set_file_in_order(0, false).await;
    assert!(matches!(r, Err(SessionError::NoMetadata)), "{r:?}");
    assert!(h.metainfo().is_none());
    session.shutdown().await;
}

/// An empty file has no pieces: turning it on is harmless.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_in_order_empty_file_is_harmless() {
    let files: [(&str, usize); 3] = [("a", MIB), ("empty", 0), ("b", MIB)];
    let (m, payload) = make_torrent(&files, PL as u32);
    let (seed, _sh) = seeder(&m, &payload).await;
    let storage = Arc::new(MemStorage::new(m.total_len()));
    let (_leech, lh) = session_with(settings(), &m, storage, TorrentState::Downloading).await;
    // The empty file (index 1) has no pieces at all.
    assert!(lh.set_file_in_order(1, true).await.is_ok());
    assert!(lh.set_file_in_order(1, false).await.is_ok());
    // File 0 is accepted: the picker marks its pieces.
    assert!(lh.set_file_in_order(0, true).await.is_ok());
    lh.add_peer(seed.local_addr()).await.unwrap();
    assert!(
        wait_state(&lh, |s| *s == TorrentState::Seeding, T).await,
        "still downloads"
    );
    let have = lh.pieces().await.unwrap();
    assert_eq!(have.count_ones(), 32);
    let _ = seed;
}
