//! BEP 52 v2 and hybrid torrents between sessions on loopback: downloads,
//! pieces that must pass both hashes of a hybrid, layers built from
//! verified data, layers fetched lazily while the torrent downloads,
//! recheck, and one torrent under each of its wire hashes.
//!
//! Every torrent but `one_piece_files_download_before_layers` is made by
//! `CreateTorrent` over three files of 100 000, 0 and 300 000 bytes with
//! 32 KiB pieces: four pieces for `a` (then a pad), ten for `c`.

mod support;

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use fbt_bencode::{OwnedValue, decode, encode};
use fbt_core::InfoHashes;
use fbt_metainfo::{Magnet, Metainfo, TorrentVersion};
use fbt_session::storage::{MemStorage, check_pieces};
use fbt_session::{
    AddTorrent, Event, EventMask, FileReader, Priority, Session, SessionError, TorrentHandle,
    TorrentState, decode_resume,
};
use fbt_wire::Message;
use rand::RngExt;
use support::*;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

const PIECE: u32 = 32_768;
/// Pieces of the payload: four for `a`, ten for `c`.
const PIECES: u32 = 14;
/// The first piece of `c`.
const C_FIRST: u32 = 4;

/// A seed of `bytes` over the files below `save_path`, waited until
/// `Seeding`: it answers peers' hash requests (`true`) or refuses every one
/// of them (`false`, [`Settings::serve_hashes`](fbt_session::Settings)).
async fn seed_serving(
    bytes: &[u8],
    save_path: &Path,
    serve_hashes: bool,
) -> (Session, TorrentHandle) {
    let m = Metainfo::from_bytes(bytes).unwrap();
    let mut settings = test_settings();
    settings.serve_hashes = serve_hashes;
    let s = Session::new(settings).await.unwrap();
    let h = s
        .add_torrent(AddTorrent::new(m, save_path.to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    (s, h)
}

/// A seed that answers hash requests.
async fn seed(bytes: &[u8], save_path: &Path) -> (Session, TorrentHandle) {
    seed_serving(bytes, save_path, true).await
}

/// The files below `from` copied below `to/to_name`, which is returned: a
/// second seed of the same payload needs the same bytes.
fn copy_payload(from: &Path, to: &Path, to_name: &str) -> PathBuf {
    let root = to.join(to_name);
    std::fs::create_dir_all(&root).expect("mkdir");
    for entry in std::fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("entry");
        if entry.path().is_file() {
            std::fs::copy(entry.path(), root.join(entry.file_name())).expect("copy");
        }
    }
    root
}

/// A 20 000-byte file (one piece) and a 300 000-byte file (ten pieces) below
/// `dir/payload`, which is returned: the torrent's name is `payload`.
fn write_two_file_payload(dir: &Path) -> PathBuf {
    let root = dir.join("payload");
    std::fs::create_dir_all(&root).expect("mkdir");
    for (name, len) in [("small", 20_000usize), ("big", 300_000)] {
        let mut data = vec![0u8; len];
        rand::rng().fill(data.as_mut_slice());
        std::fs::write(root.join(name), data).expect("write payload");
    }
    root
}

/// The index of the file of `len` bytes that is not a pad.
fn file_of(m: &Metainfo, len: u64) -> usize {
    m.info
        .files
        .iter()
        .position(|f| !f.pad && f.len == len)
        .expect("file")
}

/// Every file's verified bytes, from [`TorrentHandle::files`].
async fn file_bytes(h: &TorrentHandle) -> Vec<u64> {
    h.files()
        .await
        .expect("files")
        .iter()
        .map(|f| f.downloaded)
        .collect()
}

/// Waits until file `file` shows `n` verified bytes.
async fn wait_file_bytes(h: &TorrentHandle, file: usize, n: u64) {
    let end = tokio::time::Instant::now() + T;
    loop {
        let got = file_bytes(h).await;
        if got.get(file).copied() == Some(n) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < end,
            "file {file} to {n} verified bytes in time (got {got:?})"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Seeks to `offset` and reads `n` bytes.
async fn read_at(r: &mut FileReader, offset: u64, n: usize) -> std::io::Result<Vec<u8>> {
    use std::io::SeekFrom;
    r.seek(SeekFrom::Start(offset)).await?;
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await?;
    Ok(buf)
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

    // A connection under the truncated v2 hash is v2 without the bit, as
    // libtorrent decides (`bt_peer_connection.cpp`): it sends none on a
    // v2-only torrent.
    let mut peer = ScriptedPeer::connect_fast(s.local_addr(), wire).await;
    assert!(peer.remote.reserved.supports_v2());
    peer.send(Message::HashRequest(hash_request(root, 1, 8, 8)))
        .await;
    match peer
        .next_matching(|m| matches!(m, Message::Hashes { .. } | Message::HashReject(_)))
        .await
    {
        Some(Message::Hashes { req, .. }) => assert_eq!(req, hash_request(root, 1, 8, 8)),
        other => panic!("expected hashes, got {other:?}"),
    }
    s.shutdown().await;

    // On a hybrid, a v1-hash connection without the bit is not v2: hash
    // messages close it.
    let dir = tempfile::tempdir().unwrap();
    let bytes = create_torrent(&write_v2_payload(dir.path()), TorrentVersion::Hybrid, PIECE);
    let m = Metainfo::from_bytes(&bytes).unwrap();
    let root = m
        .info
        .files
        .iter()
        .find(|f| f.len == 300_000)
        .and_then(|f| f.pieces_root)
        .unwrap();
    let (s, _h) = seed(&bytes, dir.path()).await;
    let v1 = m.info_hashes.v1.unwrap().0;
    let mut peer = ScriptedPeer::connect_fast(s.local_addr(), v1).await;
    // We offer the upgrade: the hybrid has a v2 hash.
    assert!(peer.remote.reserved.supports_v2());
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
    // The metadata arrived without the layers (they live outside `info`) and
    // was published at once; the handle keeps what it first saw.
    assert!(lh.metainfo().unwrap().piece_layers.is_empty());
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
    // The metadata is known at once and the download runs while the layers
    // are fetched; nothing of `a` or `c` can be verified until then.
    let st = lh.status();
    assert!(st.has_metadata);
    assert_eq!(st.layers_missing, 2, "both multi-piece files lack theirs");
    assert!(lh.metainfo().is_some());
    assert!(wait_state(&lh, |s| *s == TorrentState::Downloading, T).await);
    lh.add_peer(seeder.local_addr()).await.unwrap();
    assert!(
        wait_until(|| lh.status().layers_missing == 0, T).await,
        "both layers fetched"
    );
    assert_eq!(lh.piece_layers_known().await, 2);
    // The handle's metainfo is the one it was added with.
    assert!(lh.metainfo().unwrap().piece_layers.is_empty());
    next_matching(&mut ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    assert!(wait_state(&lh, |s| *s == TorrentState::Seeding, T).await);
    assert_eq!(lh.piece_layers_known().await, 2);
    assert!(
        payload_of(&m, leech_dir.path()) == payload_of(&full, seed_dir.path()),
        "data differs"
    );
    // Resume data taken after the lazy fetch still loads back (the layers it
    // fetched are saved with it).
    let saved = lh.save_resume_data().await.expect("resume data");
    decode_resume(&saved).expect("the resume data loads back");
    leecher.shutdown().await;
    seeder.shutdown().await;
}

/// A v2 torrent whose metadata lacks the layers does not wait for them. The
/// one-piece file downloads at once: its `pieces root` is the expected root
/// of its only piece, so no layer is needed. The pieces of the multi-piece
/// file are not requested at all — without the layer they could not be
/// verified — and the torrent stays `Downloading`, not
/// `DownloadingMetadata`, while `layers_missing` counts that file. The seed
/// here refuses every hash request, so the layers never arrive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_piece_files_download_before_layers() {
    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    let root = write_two_file_payload(seed_dir.path());
    let bytes = create_torrent(&root, TorrentVersion::V2, PIECE);
    let m = Metainfo::from_bytes(&strip_layers(&bytes)).unwrap();
    assert!(m.piece_layers.is_empty());
    let (seeder, _sh) = seed_serving(&bytes, seed_dir.path(), false).await;
    let leecher = Session::new(test_settings()).await.unwrap();
    let lh = leecher
        .add_torrent(AddTorrent::new(m.clone(), leech_dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&lh, |s| *s == TorrentState::Downloading, T).await);
    let st = lh.status();
    assert!(st.has_metadata, "the metadata was in the .torrent");
    assert_eq!(st.layers_missing, 1, "the ten-piece file lacks its layer");
    lh.add_peer(seeder.local_addr()).await.unwrap();
    // The one-piece file completes without a layer.
    let small = file_of(&m, 20_000);
    wait_file_bytes(&lh, small, 20_000).await;
    // Nothing of the other file is asked for: it could not be verified.
    let big = file_of(&m, 300_000);
    assert_eq!(file_bytes(&lh).await.get(big).copied(), Some(0));
    // `status()` is the last published snapshot, so wait for the tick that
    // counts the completed piece. The exact number depends on how the
    // padding lands in pieces, so only its direction is asserted here: what
    // was verified counts, and the file still without a layer does not.
    assert!(
        wait_until(|| lh.status().wanted_done > 0, T).await,
        "the one-piece file counts as done"
    );
    let before = lh.status();
    assert!(
        before.wanted_done < before.total_wanted,
        "the ten-piece file is still wanted"
    );
    // Long on purpose: this seed refuses every hash request, so no span can
    // land during the window and nothing may move.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let after = lh.status();
    assert_eq!(after.state, TorrentState::Downloading);
    assert_eq!(after.layers_missing, 1);
    assert_eq!(after.wanted_done, before.wanted_done, "no piece started");
    assert_eq!(
        after.total_downloaded, before.total_downloaded,
        "no block asked for"
    );
    assert_eq!(file_bytes(&lh).await.get(big).copied(), Some(0));
    leecher.shutdown().await;
    seeder.shutdown().await;
}

/// A v2-only `.torrent` without its layers, added over data that is already
/// complete on disk. Nothing could be hashed before the layers arrived, so the
/// check finds no piece and none is asked for; when a peer supplies the layers
/// their pieces are read off disk again, pass, and the torrent seeds without
/// fetching a single payload byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn complete_data_seeds_when_its_layers_arrive() {
    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    let root = write_v2_payload(seed_dir.path());
    let bytes = create_torrent(&root, TorrentVersion::V2, PIECE);
    let m = Metainfo::from_bytes(&strip_layers(&bytes)).unwrap();
    // The whole payload is already below the leech's save path.
    copy_payload(&root, leech_dir.path(), "payload");
    let (seeder, _sh) = seed(&bytes, seed_dir.path()).await;
    let leecher = Session::new(test_settings()).await.unwrap();
    let mut ev = leecher.subscribe(EventMask::ALL);
    let lh = leecher
        .add_torrent(AddTorrent::new(m.clone(), leech_dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&lh, |s| *s == TorrentState::Downloading, T).await);
    let before = lh.status();
    assert_eq!(before.layers_missing, 2);
    // Every piece of a multi-piece file fails its hash check without the
    // layer, so the check on complete data verifies nothing and asks for
    // nothing: `wanted_done` says so, and no byte is on its way.
    assert_eq!(before.wanted_done, 0, "nothing verifiable yet");
    assert_eq!(before.total_downloaded, 0, "the pieces are not asked for");
    lh.add_peer(seeder.local_addr()).await.unwrap();
    next_matching(&mut ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    assert!(wait_state(&lh, |s| *s == TorrentState::Seeding, T).await);
    let st = lh.status();
    assert_eq!(st.layers_missing, 0);
    // The layers made the bytes that were already there verifiable: they came
    // off disk. Hash answers are metadata, so no payload was downloaded.
    assert_eq!(st.total_downloaded, 0, "the data was fetched a second time");
    assert!(st.is_seed, "every piece passed");
    assert_eq!(lh.piece_layers_known().await, 2);
    let (a, c) = (file_of(&m, 100_000), file_of(&m, 300_000));
    let got = file_bytes(&lh).await;
    assert_eq!(got.get(a).copied(), Some(100_000));
    assert_eq!(got.get(c).copied(), Some(300_000));
    assert!(
        payload_of(&m, leech_dir.path()) == payload_of(&m, seed_dir.path()),
        "data differs"
    );
    leecher.shutdown().await;
    seeder.shutdown().await;
}

/// A torrent whose wanted pieces are all done while a *skipped* file still
/// waits for its layer is seeding: the layer of a file nobody downloads is no
/// work left, so `TorrentFinished` finds the state already `Seeding`. The seed
/// here refuses every hash request, so that layer never arrives and the skipped
/// file's pieces are never verified.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seeding_ignores_a_skipped_files_layer() {
    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    let root = write_two_file_payload(seed_dir.path());
    let bytes = create_torrent(&root, TorrentVersion::V2, PIECE);
    let m = Metainfo::from_bytes(&strip_layers(&bytes)).unwrap();
    let (small, big) = (file_of(&m, 20_000), file_of(&m, 300_000));
    let (seeder, _sh) = seed_serving(&bytes, seed_dir.path(), false).await;
    let leecher = Session::new(test_settings()).await.unwrap();
    let mut ev = leecher.subscribe(EventMask::ALL);
    let lh = leecher
        .add_torrent(AddTorrent::new(m.clone(), leech_dir.path().to_path_buf()))
        .await
        .unwrap();
    let mut prio = vec![Priority::Normal; m.info.files.len()];
    prio[big] = Priority::Skip;
    lh.set_file_priorities(prio).await.expect("priorities");
    assert!(wait_state(&lh, |s| *s == TorrentState::Downloading, T).await);
    assert_eq!(
        lh.status().layers_missing,
        1,
        "the skipped file lacks its layer"
    );
    lh.add_peer(seeder.local_addr()).await.unwrap();
    // The one-piece file needs no layer: it downloads and finishes the torrent.
    next_matching(&mut ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    let st = lh.status();
    assert_eq!(
        st.state,
        TorrentState::Seeding,
        "the wanted pieces are all done"
    );
    // The skipped file is still waiting for hashes it never asked for first:
    // seeding does not mean every piece of the torrent passed.
    assert_eq!(st.layers_missing, 1);
    assert!(!st.is_seed, "the skipped file is unverified");
    assert_eq!(st.total_downloaded, 20_000, "only the wanted file moved");
    let got = file_bytes(&lh).await;
    assert_eq!(got.get(small).copied(), Some(20_000));
    assert_eq!(
        got.get(big).copied(),
        Some(0),
        "its pieces were not asked for"
    );
    leecher.shutdown().await;
    seeder.shutdown().await;
}

/// A reader on a file whose layer is missing waits for it, as it waits for a
/// missing piece: the read hangs while the only source refuses hash requests
/// and is served with the right bytes once a seed that answers them joins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reader_waits_for_its_layer() {
    let seed_dir = tempfile::tempdir().unwrap();
    let second_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    let root = write_v2_payload(seed_dir.path());
    let bytes = create_torrent(&root, TorrentVersion::V2, PIECE);
    let m = Metainfo::from_bytes(&strip_layers(&bytes)).unwrap();
    let c = file_of(&m, 300_000);
    let data = std::fs::read(root.join("c")).expect("read c");
    let (slow, _sh) = seed_serving(&bytes, seed_dir.path(), false).await;
    let leecher = Session::new(test_settings()).await.unwrap();
    let lh = leecher
        .add_torrent(AddTorrent::new(m.clone(), leech_dir.path().to_path_buf()))
        .await
        .unwrap();
    lh.add_peer(slow.local_addr()).await.unwrap();
    assert!(wait_state(&lh, |s| *s == TorrentState::Downloading, T).await);
    let mut r = lh.open_file(c).await.expect("open c");
    let read = tokio::spawn(async move { read_at(&mut r, 0, 4096).await });
    // Its layer is missing, so none of its pieces is even requested and the
    // read waits. Long on purpose: a short look would only prove nothing.
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(!read.is_finished(), "no layer, no bytes");
    assert_eq!(lh.status().layers_missing, 2);
    // A seed that answers hash requests joins: the layers arrive, the pieces
    // download, and the read is served from the verified piece.
    copy_payload(&root, second_dir.path(), "payload");
    let (hasher, _h) = seed(&bytes, second_dir.path()).await;
    lh.add_peer(hasher.local_addr()).await.unwrap();
    let got = tokio::time::timeout(T, read)
        .await
        .expect("the read was answered")
        .expect("no panic")
        .expect("read");
    assert_eq!(got, data[..4096]);
    assert!(
        wait_until(|| lh.status().layers_missing == 0, T).await,
        "the layers came"
    );
    leecher.shutdown().await;
    slow.shutdown().await;
    hasher.shutdown().await;
}

/// The first source answers a span and goes while the fetch still has spans
/// outstanding: what it answered survives, the rest is asked of the next
/// source, and the torrent finishes. The answering peer is scripted — it
/// replays hashes a real seed gave it for that very request — so when it
/// leaves is fixed rather than raced: `layers_missing` falls to one before the
/// close, and the other file's span is still in flight with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fetch_survives_source_churn() {
    let seed_dir = tempfile::tempdir().unwrap();
    let second_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    let root = write_v2_payload(seed_dir.path());
    let bytes = create_torrent(&root, TorrentVersion::V2, PIECE);
    let m = Metainfo::from_bytes(&strip_layers(&bytes)).unwrap();
    let wire = m.info_hashes.wire_hash();
    // A seed holding the layers: the scripted source replays its answers, so
    // every hash it sends is the torrent's own.
    let (helper, _h) = seed(&bytes, seed_dir.path()).await;

    let leecher = Session::new(test_settings()).await.unwrap();
    let mut ev = leecher.subscribe(EventMask::ALL);
    let lh = leecher
        .add_torrent(AddTorrent::new(m.clone(), leech_dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&lh, |s| *s == TorrentState::Downloading, T).await);
    assert_eq!(lh.status().layers_missing, 2);

    // The first source: a v2 peer that answers exactly one of the requests it
    // is given. `HaveNone` keeps the download off it: it carries hashes only.
    let mut source = ScriptedPeer::connect_with(leecher.local_addr(), wire, v2_reserved()).await;
    source.send(Message::HaveNone).await;
    let asked = source
        .next_matching_within(T, |m| matches!(m, Message::HashRequest(_)))
        .await
        .expect("the fetch asked the first source for hashes");
    let Message::HashRequest(req) = asked else {
        unreachable!()
    };
    // Its answer, taken from a seed that holds the layers.
    let mut ask = ScriptedPeer::connect_with(helper.local_addr(), wire, v2_reserved()).await;
    ask.send(Message::HashRequest(req)).await;
    let hashes = match ask
        .next_matching_within(T, |m| {
            matches!(m, Message::Hashes { .. } | Message::HashReject(_))
        })
        .await
    {
        Some(Message::Hashes { req: echo, hashes }) => {
            assert_eq!(echo, req, "the seed answered the request we asked it");
            hashes
        }
        other => panic!("no answer for {req:?}: {other:?}"),
    };
    source.send(Message::Hashes { req, hashes }).await;
    // Mid-fetch: that file's layer merged, while the span this source was also
    // asked for is outstanding and it never answers it.
    assert!(
        wait_until(|| lh.status().layers_missing == 1, T).await,
        "the answered layer merged"
    );
    drop(source);
    assert_eq!(
        lh.piece_layers_known().await,
        1,
        "answered spans survive their source"
    );
    assert_eq!(lh.status().layers_missing, 1);

    // The second source finishes what the first left.
    copy_payload(&root, second_dir.path(), "payload");
    let (second, _h) = seed(&bytes, second_dir.path()).await;
    lh.add_peer(second.local_addr()).await.unwrap();
    next_matching(&mut ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    assert!(wait_state(&lh, |s| *s == TorrentState::Seeding, T).await);
    assert_eq!(lh.status().layers_missing, 0);
    assert_eq!(lh.piece_layers_known().await, 2);
    assert!(
        payload_of(&m, leech_dir.path()) == payload_of(&m, seed_dir.path()),
        "data differs"
    );
    drop(ask);
    leecher.shutdown().await;
    helper.shutdown().await;
    second.shutdown().await;
}

/// The torrent pauses while its layer fetch is running and resumes: the spans
/// outstanding when it paused are given back to the queue, asked of the source
/// that arrives after the resume, and the torrent finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_resume_mid_fetch() {
    let seed_dir = tempfile::tempdir().unwrap();
    let second_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    let root = write_v2_payload(seed_dir.path());
    let bytes = create_torrent(&root, TorrentVersion::V2, PIECE);
    let m = Metainfo::from_bytes(&strip_layers(&bytes)).unwrap();
    // The only source refuses hash requests: the fetch runs and nothing lands.
    let (slow, _sh) = seed_serving(&bytes, seed_dir.path(), false).await;
    let leecher = Session::new(test_settings()).await.unwrap();
    let mut ev = leecher.subscribe(EventMask::ALL);
    let lh = leecher
        .add_torrent(AddTorrent::new(m.clone(), leech_dir.path().to_path_buf()))
        .await
        .unwrap();
    lh.add_peer(slow.local_addr()).await.unwrap();
    assert!(wait_state(&lh, |s| *s == TorrentState::Downloading, T).await);
    assert_eq!(
        lh.status().layers_missing,
        2,
        "spans are outstanding with the only source"
    );
    lh.pause().await.expect("pause");
    assert!(wait_state(&lh, |s| *s == TorrentState::Paused, T).await);
    assert_eq!(
        lh.status().layers_missing,
        2,
        "the fetch paused with the torrent"
    );
    lh.resume().await.expect("resume");
    assert!(wait_state(&lh, |s| *s == TorrentState::Downloading, T).await);
    // A source that answers them joins after the resume.
    copy_payload(&root, second_dir.path(), "payload");
    let (second, _h) = seed(&bytes, second_dir.path()).await;
    lh.add_peer(second.local_addr()).await.unwrap();
    next_matching(&mut ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    assert!(wait_state(&lh, |s| *s == TorrentState::Seeding, T).await);
    assert_eq!(lh.status().layers_missing, 0);
    assert_eq!(lh.piece_layers_known().await, 2);
    assert!(
        payload_of(&m, leech_dir.path()) == payload_of(&m, seed_dir.path()),
        "data differs"
    );
    leecher.shutdown().await;
    slow.shutdown().await;
    second.shutdown().await;
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

/// A v2-only torrent's peer is a layer source without the v2 bit: the
/// connection runs under the truncated v2 hash, which libtorrent takes as
/// v2 (it sends no bit then). The layers are fetched while the torrent
/// downloads, so it asks for hashes right away and requests no block of a
/// file whose layer is still missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seed_without_the_v2_bit_is_asked_for_layers() {
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
    assert!(matches!(got, Some(Message::HashRequest(_))), "{got:?}");
    let st = lh.status();
    // The download runs beside the fetch: the metadata is known and the
    // state is `Downloading`, but every piece of a multi-piece file (both
    // here) is held back until its layer arrives.
    assert!(st.has_metadata);
    assert_eq!(st.state, TorrentState::Downloading);
    assert_eq!(st.layers_missing, 2);
    leecher.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_torrent_waiting_for_layers_keeps_its_flags() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = fbt_metainfo::CreateTorrent::new(write_v2_payload(dir.path()))
        .version(TorrentVersion::V2)
        .piece_len(PIECE)
        .private(true)
        .build(&mut |_, _| ControlFlow::Continue(()))
        .unwrap();
    let m = Metainfo::from_bytes(&strip_layers(&bytes)).unwrap();
    assert!(m.info.private);
    let s = Session::new(test_settings()).await.unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    let h = s
        .add_torrent(AddTorrent::new(m.clone(), leech_dir.path().to_path_buf()))
        .await
        .unwrap();
    // Its layers are still fetched, but the metadata is known and its flags
    // apply at once: private, so no `ut_pex`. Peers connect once its data
    // has been checked, as for any other `.torrent`.
    assert_eq!(h.status().layers_missing, 2);
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);
    let mut reserved = v2_reserved();
    reserved.set_extended();
    let mut peer =
        ScriptedPeer::connect_with(s.local_addr(), m.info_hashes.wire_hash(), reserved).await;
    let payload = peer.next_ext(0).await.expect("our extension handshake");
    let hs = fbt_wire::ExtHandshake::decode(&payload).expect("valid handshake");
    assert_eq!(
        hs.id_of(fbt_wire::UT_PEX),
        None,
        "no ut_pex for a private torrent"
    );
    assert_eq!(hs.metadata_size, Some(m.raw_info().len() as u32));
    assert!(h.status().has_metadata, "its metadata is installed");
    s.shutdown().await;
}
