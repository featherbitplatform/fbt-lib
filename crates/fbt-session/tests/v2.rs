//! BEP 52 v2 and hybrid torrents between sessions on loopback: downloads,
//! pieces that must pass both hashes of a hybrid, layers built from
//! verified data, and recheck.
//!
//! Every torrent is made by `CreateTorrent` over three files of 100 000, 0
//! and 300 000 bytes with 32 KiB pieces: four pieces for `a` (then a pad),
//! ten for `c`.

mod support;

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fbt_bencode::{OwnedValue, decode, encode};
use fbt_metainfo::{Metainfo, TorrentVersion};
use fbt_session::storage::{MemStorage, check_pieces};
use fbt_session::{AddTorrent, Event, EventMask, Session, TorrentHandle, TorrentState};
use fbt_wire::Message;
use support::*;

const PIECE: u32 = 32_768;
/// Pieces of the payload: four for `a`, ten for `c`.
const PIECES: u32 = 14;
/// The first piece of `c`.
const C_FIRST: u32 = 4;

/// A seed of `bytes` over the files below `save_path`, waited until
/// `Seeding`.
async fn seed(bytes: &[u8], save_path: &Path) -> (Session, TorrentHandle) {
    let m = Metainfo::from_bytes(bytes).unwrap();
    let s = Session::new(test_settings()).await.unwrap();
    let h = s
        .add_torrent(AddTorrent::new(m, save_path.to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    (s, h)
}

/// Downloads `leech_bytes` (the same torrent as `seed_bytes`) from a seed of
/// the files below `seed_dir` into `leech_dir`; returns the leech, finished,
/// with its handle.
async fn download(
    seed_bytes: &[u8],
    leech_bytes: &[u8],
    seed_dir: &Path,
    leech_dir: &Path,
) -> (Session, TorrentHandle, Session) {
    let (seeder, _sh) = seed(seed_bytes, seed_dir).await;
    let m = Metainfo::from_bytes(leech_bytes).unwrap();
    let leecher = Session::new(test_settings()).await.unwrap();
    let mut ev = leecher.subscribe(EventMask::ALL);
    let lh = leecher
        .add_torrent(AddTorrent::new(m.clone(), leech_dir.to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&lh, |s| *s == TorrentState::Downloading, T).await);
    lh.add_peer(seeder.local_addr()).await.unwrap();
    next_matching(&mut ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    assert!(wait_state(&lh, |s| *s == TorrentState::Seeding, T).await);
    assert!(
        payload_of(&m, leech_dir) == payload_of(&m, seed_dir),
        "data differs"
    );
    (leecher, lh, seeder)
}

/// The value at `path` (dictionary keys) inside `v`.
fn at<'a>(v: &'a mut OwnedValue, path: &[&[u8]]) -> &'a mut OwnedValue {
    path.iter().fold(v, |v, k| match v {
        OwnedValue::Dict(d) => d.get_mut(*k).expect("key present"),
        _ => panic!("not a dictionary"),
    })
}

fn owned(bytes: &[u8]) -> OwnedValue {
    decode(bytes).unwrap().to_owned_value()
}

/// `bytes` without their `piece layers`.
fn strip_layers(bytes: &[u8]) -> Vec<u8> {
    let mut root = owned(bytes);
    let OwnedValue::Dict(d) = &mut root else {
        panic!("not a dictionary");
    };
    assert!(d.remove(&b"piece layers"[..]).is_some());
    encode(&root)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v2_torrent_downloads_and_seeds() {
    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    let bytes = create_torrent(
        &write_v2_payload(seed_dir.path()),
        TorrentVersion::V2,
        PIECE,
    );
    let m = Metainfo::from_bytes(&bytes).unwrap();
    assert_eq!(m.version(), TorrentVersion::V2);
    assert!(m.info.pieces.is_empty());
    assert_eq!(m.piece_layout().num_pieces(), PIECES);
    let (l, lh, s) = download(&bytes, &bytes, seed_dir.path(), leech_dir.path()).await;
    assert!(lh.pieces().await.unwrap().is_full());
    l.shutdown().await;
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hybrid_torrent_downloads() {
    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    let bytes = create_torrent(
        &write_v2_payload(seed_dir.path()),
        TorrentVersion::Hybrid,
        PIECE,
    );
    let m = Metainfo::from_bytes(&bytes).unwrap();
    assert_eq!(m.version(), TorrentVersion::Hybrid);
    let (l, lh, s) = download(&bytes, &bytes, seed_dir.path(), leech_dir.path()).await;
    assert_eq!(lh.piece_layers_known().await, 2);
    l.shutdown().await;
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hybrid_piece_must_pass_both() {
    // The right files, and the same files with `c` replaced by other bytes
    // of the same length.
    let good_dir = tempfile::tempdir().unwrap();
    let alt_dir = tempfile::tempdir().unwrap();
    let good_root = write_v2_payload(good_dir.path());
    let alt_root = write_v2_payload(alt_dir.path());
    std::fs::copy(good_root.join("a"), alt_root.join("a")).unwrap();
    let good = create_torrent(&good_root, TorrentVersion::Hybrid, PIECE);
    let alt = create_torrent(&alt_root, TorrentVersion::Hybrid, PIECE);

    // The right torrent with `c`'s v2 half (pieces root and layer) taken
    // from the other one: v1 says the right bytes, v2 says the others.
    let mut spliced = owned(&good);
    let mut alt_v = owned(&alt);
    let c_root_path: &[&[u8]] = &[b"info", b"file tree", b"c", b"", b"pieces root"];
    let alt_c_root = at(&mut alt_v, c_root_path).clone();
    *at(&mut spliced, c_root_path) = alt_c_root;
    *at(&mut spliced, &[b"piece layers"]) = at(&mut alt_v, &[b"piece layers"]).clone();
    let m = Metainfo::from_bytes(&encode(&spliced)).expect("spliced hybrid parses");
    assert_eq!(m.version(), TorrentVersion::Hybrid);
    assert_eq!(m.piece_layers.len(), 2, "both layers validate");
    let payload = payload_of(&m, good_dir.path());

    // The verifier: `c`'s pieces pass SHA-1 but not their v2 root.
    let storage = MemStorage::from_bytes(payload.clone());
    let (have, _) = check_pieces(&storage, &m.piece_layout(), &m, |_| {
        ControlFlow::Continue(())
    })
    .unwrap();
    assert_eq!(have.iter_ones().collect::<Vec<_>>(), vec![0, 1, 2, 3]);
    assert!(!have.get(C_FIRST + 1));

    // A peer sending the right bytes: `c`'s pieces fail.
    let s = Session::new(test_settings()).await.unwrap();
    let mut ev = s.subscribe(EventMask::ALL);
    let mut add = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    add.storage = Some(Arc::new(MemStorage::new(payload.len() as u64)));
    let h = s.add_torrent(add).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);
    let v1 = m.info_hashes.v1.expect("v1 hash").0;
    let mut peer = ScriptedPeer::connect_fast(s.local_addr(), v1).await;
    peer.send(Message::HaveAll).await;
    peer.send(Message::Unchoke).await;
    let server = tokio::spawn(async move { peer.serve(&payload, PIECE).await });
    next_matching(
        &mut ev,
        |e| matches!(e, Event::HashFailed { piece, .. } if *piece >= C_FIRST),
        T,
    )
    .await
    .expect("HashFailed for a piece of c");
    server.abort();
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hybrid_without_layers_builds_them() {
    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    let bytes = create_torrent(
        &write_v2_payload(seed_dir.path()),
        TorrentVersion::Hybrid,
        PIECE,
    );
    let stripped = strip_layers(&bytes);
    let m = Metainfo::from_bytes(&stripped).unwrap();
    assert!(m.piece_layers.is_empty());
    let multi_piece = m
        .info
        .files
        .iter()
        .filter(|f| !f.pad && f.len > u64::from(PIECE))
        .count();
    assert_eq!(multi_piece, 2);
    let (l, lh, s) = download(&bytes, &stripped, seed_dir.path(), leech_dir.path()).await;
    assert_eq!(lh.piece_layers_known().await, multi_piece);
    // The handle's metainfo is the one it was added with.
    assert!(lh.metainfo().unwrap().piece_layers.is_empty());
    l.shutdown().await;
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recheck_verifies_v2() {
    let dir = tempfile::tempdir().unwrap();
    let root = write_v2_payload(dir.path());
    let bytes = create_torrent(&root, TorrentVersion::V2, PIECE);
    let (s, h) = seed(&bytes, dir.path()).await;
    assert_eq!(h.pieces().await.unwrap().count_ones(), PIECES);
    // One byte of `c`'s fifth piece (global piece 8).
    let path = root.join("c");
    let mut data = std::fs::read(&path).unwrap();
    data[4 * PIECE as usize + 7] ^= 0x55;
    std::fs::write(&path, data).unwrap();
    h.force_recheck().await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);
    let have = h.pieces().await.unwrap();
    assert_eq!(have.count_ones(), PIECES - 1);
    assert!(!have.get(C_FIRST + 4));
    s.shutdown().await;
}
