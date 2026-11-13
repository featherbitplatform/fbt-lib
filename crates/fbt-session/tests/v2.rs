//! BEP 52 v2 and hybrid torrents between sessions on loopback: downloads,
//! pieces that must pass both hashes of a hybrid, layers built from
//! verified data, recheck, and one torrent under each of its wire hashes.
//!
//! Every torrent is made by `CreateTorrent` over three files of 100 000, 0
//! and 300 000 bytes with 32 KiB pieces: four pieces for `a` (then a pad),
//! ten for `c`.

mod support;

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fbt_bencode::{OwnedValue, decode, encode};
use fbt_core::InfoHashes;
use fbt_metainfo::{Magnet, Metainfo, TorrentVersion};
use fbt_session::storage::{MemStorage, check_pieces};
use fbt_session::{
    AddTorrent, Event, EventMask, Session, SessionError, TorrentHandle, TorrentState,
};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hybrid_is_one_torrent_under_both_hashes() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = create_torrent(&write_v2_payload(dir.path()), TorrentVersion::Hybrid, PIECE);
    let m = Metainfo::from_bytes(&bytes).unwrap();
    let v1 = m.info_hashes.v1.expect("v1 hash");
    let v2 = m.info_hashes.v2.expect("v2 hash");
    let s = Session::new(test_settings()).await.unwrap();
    let h = s
        .add_torrent(AddTorrent::new(m, dir.path().to_path_buf()))
        .await
        .unwrap();
    // Its `btmh`-only magnet, then its `btih`-only magnet.
    for ih in [InfoHashes::from_v2(v2), InfoHashes::from_v1(v1)] {
        let mg = Magnet::from_info_hashes(ih);
        let r = s
            .add_torrent(AddTorrent::magnet(mg, PathBuf::from("unused")))
            .await;
        assert!(
            matches!(r, Err(SessionError::DuplicateTorrent)),
            "{ih}: {r:?}"
        );
    }
    assert_eq!(s.torrents().len(), 1);
    let found = s
        .torrent(&InfoHashes::from_v2(v2))
        .expect("found by its v2 hash");
    assert_eq!(found.info_hashes(), h.info_hashes());
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v2_handshake_reaches_a_hybrid() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = create_torrent(&write_v2_payload(dir.path()), TorrentVersion::Hybrid, PIECE);
    let (s, h) = seed(&bytes, dir.path()).await;
    let v2 = h.info_hashes().v2.expect("v2 hash");
    let truncated = InfoHashes::from_v2(v2).wire_hash();
    assert_ne!(truncated, h.info_hashes().wire_hash());
    let peer = ScriptedPeer::connect(s.local_addr(), truncated).await;
    assert_eq!(peer.remote.info_hash, truncated);
    assert!(peer.remote.reserved.supports_v2());
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v2_magnet_registers_the_revealed_v1_hash() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = create_torrent(&write_v2_payload(dir.path()), TorrentVersion::Hybrid, PIECE);
    let (seeder, sh) = seed(&bytes, dir.path()).await;
    let ih = sh.info_hashes();
    let v2_only = InfoHashes::from_v2(ih.v2.expect("v2 hash"));
    let leecher = Session::new(test_settings()).await.unwrap();
    let mut ev = leecher.subscribe(EventMask::ALL);
    let leech_dir = tempfile::tempdir().unwrap();
    let mg = Magnet::from_info_hashes(v2_only);
    let lh = leecher
        .add_torrent(AddTorrent::magnet(mg, leech_dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(
        leecher
            .torrent(&InfoHashes::from_v1(ih.v1.expect("v1")))
            .is_none()
    );
    lh.add_peer(seeder.local_addr()).await.unwrap();
    next_matching(&mut ev, |e| matches!(e, Event::MetadataReceived { .. }), T)
        .await
        .expect("MetadataReceived");
    let v1_only = InfoHashes::from_v1(ih.v1.expect("v1 hash"));
    assert!(
        wait_until(|| leecher.torrent(&v1_only).is_some(), T).await,
        "the v1 hash names the torrent"
    );
    assert_eq!(lh.info_hashes(), v2_only, "the key stays the magnet's");
    leecher.shutdown().await;
    seeder.shutdown().await;
}

fn hash_request(root: [u8; 32], base_layer: u32, index: u32, length: u32) -> fbt_wire::HashRequest {
    fbt_wire::HashRequest {
        pieces_root: root,
        base_layer,
        index,
        length,
        proof_layers: 10,
    }
}

fn v2_reserved() -> fbt_wire::Reserved {
    let mut r = fbt_wire::Reserved::default();
    r.set_fast();
    r.set_v2();
    r
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seed_answers_hash_requests() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = create_torrent(&write_v2_payload(dir.path()), TorrentVersion::V2, PIECE);
    let m = Metainfo::from_bytes(&bytes).unwrap();
    let c = m.info.files.iter().find(|f| f.len == 300_000).unwrap();
    let root = c.pieces_root.unwrap();
    let tree_height = fbt_core::merkle::file_tree_height(c.len);
    let (s, _h) = seed(&bytes, dir.path()).await;
    let wire = m.info_hashes.wire_hash();

    let mut peer = ScriptedPeer::connect_with(s.local_addr(), wire, v2_reserved()).await;
    assert!(peer.remote.reserved.supports_v2());
    // A piece-layer span with its proof; then refusals for what we lack.
    peer.send(Message::HashRequest(hash_request(root, 1, 8, 8)))
        .await;
    peer.send(Message::HashRequest(hash_request(root, 0, 0, 8)))
        .await;
    peer.send(Message::HashRequest(hash_request([3; 32], 1, 0, 8)))
        .await;
    // Served off the actor in parallel: the answers come in any order.
    let mut rejects = Vec::new();
    let mut answer = None;
    while answer.is_none() || rejects.len() < 2 {
        match peer
            .next_matching(|m| matches!(m, Message::Hashes { .. } | Message::HashReject(_)))
            .await
        {
            Some(Message::Hashes { req, hashes }) => answer = Some((req, hashes)),
            Some(Message::HashReject(req)) => rejects.push(req),
            other => panic!("expected hash answers, got {other:?}"),
        }
    }
    let (req, hashes) = answer.unwrap();
    assert_eq!(req, hash_request(root, 1, 8, 8));
    let all: Vec<[u8; 32]> = hashes.as_chunks::<32>().0.to_vec();
    let (span, proof) = all.split_at(8);
    assert!(fbt_core::merkle::verify_hashes(
        &root,
        tree_height,
        1,
        8,
        span,
        proof
    ));
    rejects.sort_by_key(|r| r.base_layer);
    assert_eq!(
        rejects,
        [hash_request(root, 0, 0, 8), hash_request([3; 32], 1, 0, 8)]
    );
    // A malformed request closes the connection.
    peer.send(Message::HashRequest(hash_request(root, 1, 0, 3)))
        .await;
    assert_eq!(peer.wait_closed(T).await, PeerEnd::Closed);

    // Hash messages from a peer without the v2 bit close it.
    let mut peer = ScriptedPeer::connect_fast(s.local_addr(), wire).await;
    peer.send(Message::HashRequest(hash_request(root, 1, 0, 8)))
        .await;
    assert_eq!(peer.wait_closed(T).await, PeerEnd::Closed);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hash_messages_on_a_v1_torrent_close() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = create_torrent(&write_v2_payload(dir.path()), TorrentVersion::V1, PIECE);
    let m = Metainfo::from_bytes(&bytes).unwrap();
    let (s, _h) = seed(&bytes, dir.path()).await;
    let wire = m.info_hashes.wire_hash();
    for msg in [
        Message::HashRequest(hash_request([1; 32], 1, 0, 8)),
        Message::HashReject(hash_request([1; 32], 1, 0, 8)),
    ] {
        let mut peer = ScriptedPeer::connect_with(s.local_addr(), wire, v2_reserved()).await;
        peer.send(msg).await;
        assert_eq!(peer.wait_closed(T).await, PeerEnd::Closed);
    }
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hash_request_burst_is_answered_and_frees_its_slots() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = create_torrent(&write_v2_payload(dir.path()), TorrentVersion::V2, PIECE);
    let m = Metainfo::from_bytes(&bytes).unwrap();
    let root = m
        .info
        .files
        .iter()
        .find(|f| f.len == 300_000)
        .and_then(|f| f.pieces_root)
        .unwrap();
    let (s, _h) = seed(&bytes, dir.path()).await;
    let mut peer =
        ScriptedPeer::connect_with(s.local_addr(), m.info_hashes.wire_hash(), v2_reserved()).await;
    // Far more than the per-torrent serving cap (8) at once: every request
    // gets an answer, hashes or a reject past the cap.
    const BURST: usize = 40;
    for i in 0..BURST as u32 {
        peer.send(Message::HashRequest(hash_request(root, 1, (i % 8) * 2, 2)))
            .await;
    }
    let mut answered = 0;
    while answered < BURST {
        let got = peer
            .next_matching(|m| matches!(m, Message::Hashes { .. } | Message::HashReject(_)))
            .await;
        assert!(got.is_some(), "only {answered} of {BURST} answered");
        answered += 1;
    }
    // Every slot came back: a lone request is served.
    peer.send(Message::HashRequest(hash_request(root, 1, 0, 2)))
        .await;
    let got = peer
        .next_matching(|m| matches!(m, Message::Hashes { .. } | Message::HashReject(_)))
        .await;
    assert!(
        matches!(got, Some(Message::Hashes { req, .. }) if req == hash_request(root, 1, 0, 2)),
        "{got:?}"
    );
    s.shutdown().await;
}

/// Adds `magnet` to a fresh leech with the seed of `seed_bytes` (files
/// below `seed_dir`) as its peer; waits for the metadata and the finish and
/// compares the data. Returns the leech and its handle, with the seed.
async fn download_magnet(
    seed_bytes: &[u8],
    magnet: Magnet,
    seed_dir: &Path,
    leech_dir: &Path,
) -> (Session, TorrentHandle, Session) {
    let (seeder, _sh) = seed(seed_bytes, seed_dir).await;
    let leecher = Session::new(test_settings()).await.unwrap();
    let mut ev = leecher.subscribe(EventMask::ALL);
    let lh = leecher
        .add_torrent(AddTorrent::magnet(magnet, leech_dir.to_path_buf()))
        .await
        .unwrap();
    lh.add_peer(seeder.local_addr()).await.unwrap();
    next_matching(&mut ev, |e| matches!(e, Event::MetadataReceived { .. }), T)
        .await
        .expect("MetadataReceived");
    next_matching(&mut ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    assert!(wait_state(&lh, |s| *s == TorrentState::Seeding, T).await);
    let m = Metainfo::from_bytes(seed_bytes).unwrap();
    assert!(
        payload_of(&m, leech_dir) == payload_of(&m, seed_dir),
        "data differs"
    );
    (leecher, lh, seeder)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v2_magnet_downloads() {
    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    let bytes = create_torrent(
        &write_v2_payload(seed_dir.path()),
        TorrentVersion::V2,
        PIECE,
    );
    let m = Metainfo::from_bytes(&bytes).unwrap();
    assert_eq!(m.piece_layers.len(), 2);
    let mg = Magnet::from_info_hashes(m.info_hashes);
    assert!(mg.info_hashes.v1.is_none());
    let (l, lh, s) = download_magnet(&bytes, mg, seed_dir.path(), leech_dir.path()).await;
    // The metadata came with the fetched layers.
    assert_eq!(lh.metainfo().unwrap().piece_layers, m.piece_layers);
    assert_eq!(lh.piece_layers_known().await, 2);
    l.shutdown().await;
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v2_torrent_without_layers_fetches_them() {
    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    let bytes = create_torrent(
        &write_v2_payload(seed_dir.path()),
        TorrentVersion::V2,
        PIECE,
    );
    let full = Metainfo::from_bytes(&bytes).unwrap();
    let m = Metainfo::from_bytes(&strip_layers(&bytes)).unwrap();
    assert!(m.piece_layers.is_empty());
    let (seeder, _sh) = seed(&bytes, seed_dir.path()).await;
    let leecher = Session::new(test_settings()).await.unwrap();
    let mut ev = leecher.subscribe(EventMask::ALL);
    let lh = leecher
        .add_torrent(AddTorrent::new(m.clone(), leech_dir.path().to_path_buf()))
        .await
        .unwrap();
    // Held before its download until the layers are in.
    let st = lh.status();
    assert!(!st.has_metadata);
    assert_eq!(st.state, TorrentState::DownloadingMetadata);
    assert!(lh.metainfo().is_none());
    lh.add_peer(seeder.local_addr()).await.unwrap();
    next_matching(&mut ev, |e| matches!(e, Event::MetadataReceived { .. }), T)
        .await
        .expect("MetadataReceived");
    assert_eq!(lh.metainfo().unwrap().piece_layers, full.piece_layers);
    next_matching(&mut ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    assert!(wait_state(&lh, |s| *s == TorrentState::Seeding, T).await);
    assert!(
        payload_of(&m, leech_dir.path()) == payload_of(&m, seed_dir.path()),
        "data differs"
    );
    leecher.shutdown().await;
    seeder.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hybrid_magnet_downloads_without_layer_fetch() {
    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    let bytes = create_torrent(
        &write_v2_payload(seed_dir.path()),
        TorrentVersion::Hybrid,
        PIECE,
    );
    let m = Metainfo::from_bytes(&bytes).unwrap();
    let mg = Magnet::from_info_hashes(InfoHashes::from_v2(m.info_hashes.v2.expect("v2 hash")));
    let (l, lh, s) = download_magnet(&bytes, mg, seed_dir.path(), leech_dir.path()).await;
    let multi_piece = m
        .info
        .files
        .iter()
        .filter(|f| !f.pad && f.len > u64::from(PIECE))
        .count();
    // Built from the verified data, not fetched: the metadata had none.
    assert!(lh.metainfo().unwrap().piece_layers.is_empty());
    assert_eq!(lh.piece_layers_known().await, multi_piece);
    l.shutdown().await;
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seed_without_v2_peers_stays_in_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = create_torrent(&write_v2_payload(dir.path()), TorrentVersion::V2, PIECE);
    let m = Metainfo::from_bytes(&strip_layers(&bytes)).unwrap();
    // Dialling the scripted peer: plain TCP.
    let mut settings = dial_tcp(test_settings());
    settings.encryption = fbt_session::EncryptionPolicy::Disabled;
    let leecher = Session::new(settings).await.unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    let lh = leecher
        .add_torrent(AddTorrent::new(m, leech_dir.path().to_path_buf()))
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    lh.add_peer(listener.local_addr().unwrap()).await.unwrap();
    let mut reserved = fbt_wire::Reserved::default();
    reserved.set_fast();
    let mut peer = ScriptedPeer::accept_with(&listener, reserved).await;
    assert!(!reserved.supports_v2());
    peer.send(Message::HaveAll).await;
    peer.send(Message::Unchoke).await;
    let got = peer
        .next_matching_within(std::time::Duration::from_secs(3), |m| {
            matches!(m, Message::Request(_) | Message::HashRequest(_))
        })
        .await;
    assert!(got.is_none(), "{got:?}");
    let st = lh.status();
    assert_eq!(st.state, TorrentState::DownloadingMetadata);
    assert!(!st.has_metadata);
    leecher.shutdown().await;
}
