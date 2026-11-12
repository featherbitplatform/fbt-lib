//! Resume data and session state: a torrent saved after a pause comes back
//! without a check when its files are unchanged, and with a full check
//! when one changed; hostile resume data is refused with the field named.
//!
//! Torrent: a 300,000 B, b 500,000 B, c 300,000 B; 256 KiB pieces, so
//! piece 1 straddles a/b and piece 3 straddles b/c.

mod support;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use fbt_bencode::{OwnedValue, encode};
use fbt_core::{BlockRef, InfoHashes};
use fbt_dht::{Dht, DhtConfig};
use fbt_metainfo::{Magnet, Metainfo};
use fbt_session::storage::{FsStorage, MemStorage, Storage};
use fbt_session::{
    AddTorrent, Event, EventMask, EventStream, Priority, RateLimit, ResumeError, Session,
    SessionError, SessionState, Settings, TorrentHandle, TorrentSource, TorrentState,
    decode_resume,
};
use fbt_wire::{ExtHandshake, Handshake, Message, MessageCodec, Reserved, UT_METADATA};
use fbt_wire::{read_handshake, write_handshake};
use futures_util::StreamExt;
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

fn file(dir: &Path, name: &str) -> PathBuf {
    dir.join("torrent").join(name)
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

/// A seed on loopback (TCP, no encryption) that has only `pieces`: every
/// connection gets that bitfield and `Unchoke`, and every request is
/// answered and logged.
async fn partial_seed(
    payload: &[u8],
    num_pieces: u32,
    pieces: &[u32],
) -> (SocketAddr, Arc<Mutex<Vec<BlockRef>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    let payload = Arc::new(payload.to_vec());
    let mut bits = vec![0u8; num_pieces.div_ceil(8) as usize];
    for &p in pieces {
        bits[(p / 8) as usize] |= 0x80 >> (p % 8);
    }
    let bits = Bytes::from(bits);
    let l = Arc::clone(&log);
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let log = Arc::clone(&l);
            let payload = Arc::clone(&payload);
            let bits = bits.clone();
            tokio::spawn(async move {
                let Ok(Ok(remote)) = tokio::time::timeout(T, read_handshake(&mut stream)).await
                else {
                    return;
                };
                let ours = Handshake {
                    reserved: Reserved::default(),
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
                peer.send(Message::Bitfield(bits)).await;
                peer.send(Message::Unchoke).await;
                peer.serve_recording(&payload, PL, log).await;
            });
        }
    });
    (addr, log)
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

/// The torrent's verified pieces.
async fn have(h: &TorrentHandle) -> Vec<u32> {
    h.pieces().await.unwrap().iter_ones().collect()
}

/// Waits until the torrent has exactly `want`.
async fn wait_have(h: &TorrentHandle, want: &[u32]) {
    let end = tokio::time::Instant::now() + T;
    loop {
        if have(h).await == want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < end,
            "pieces {:?}",
            have(h).await
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Waits until the resume data's verdict is in.
async fn trusted(h: &TorrentHandle) -> bool {
    assert!(wait_until(|| h.status().resume_trusted.is_some(), T).await);
    h.status().resume_trusted.unwrap()
}

/// Every event the stream holds now.
async fn drain(ev: &mut EventStream) -> Vec<Event> {
    let mut out = Vec::new();
    while let Ok(Some(e)) = tokio::time::timeout(Duration::from_millis(200), ev.recv()).await {
        out.push(e);
    }
    out
}

/// Whether `events` show torrent `ih` checking its files.
fn checked_files(events: &[Event], ih: InfoHashes) -> bool {
    events.iter().any(|e| {
        matches!(
            e,
            Event::StateChanged {
                torrent,
                state: TorrentState::CheckingFiles { .. },
            } if *torrent == ih
        )
    })
}

/// A leecher below `dir` that downloads `pieces` from a partial seed,
/// pauses and saves; returns the resume data and the seed's log.
async fn half_downloaded(
    m: &Metainfo,
    payload: &[u8],
    dir: &Path,
    pieces: &[u32],
) -> (Vec<u8>, SocketAddr, Arc<Mutex<Vec<BlockRef>>>) {
    let n = m.piece_layout().num_pieces();
    let (seed, log) = partial_seed(payload, n, pieces).await;
    let s = Session::new(dial_tcp(settings())).await.unwrap();
    let h = s
        .add_torrent(AddTorrent::new(m.clone(), dir.to_path_buf()))
        .await
        .unwrap();
    h.add_peer(seed).await.unwrap();
    wait_have(&h, pieces).await;
    h.pause().await.unwrap();
    let data = h.save_resume_data().await.unwrap();
    assert!(!h.status().need_save_resume);
    s.shutdown().await;
    (data, seed, log)
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_round_trip_without_recheck() {
    let (m, payload) = make_torrent(&FILES, PL);
    let dir = tempfile::tempdir().unwrap();
    let (data, _seed, _log) = half_downloaded(&m, &payload, dir.path(), &[0, 1]).await;

    let s = Session::new(dial_tcp(settings())).await.unwrap();
    let mut ev = s.subscribe(EventMask::ALL);
    let p = AddTorrent::from_resume(&data).unwrap();
    assert!(p.paused);
    assert_eq!(p.save_path, dir.path());
    let h = s.add_torrent(p).await.unwrap();
    assert!(trusted(&h).await);
    assert!(wait_state(&h, |s| *s == TorrentState::Paused, T).await);
    assert_eq!(have(&h).await, vec![0, 1]);
    assert!(!h.status().need_save_resume);

    let full = seeder(&m, &payload).await;
    h.resume().await.unwrap();
    h.add_peer(full.local_addr()).await.unwrap();
    next_matching(&mut ev, |e| matches!(e, Event::TorrentFinished { .. }), T)
        .await
        .expect("TorrentFinished");
    let events = drain(&mut ev).await;
    assert!(
        !checked_files(&events, m.info_hashes),
        "checked: {events:?}"
    );
    let all: Vec<u8> = [
        std::fs::read(file(dir.path(), "a")).unwrap(),
        std::fs::read(file(dir.path(), "b")).unwrap(),
        std::fs::read(file(dir.path(), "c")).unwrap(),
    ]
    .concat();
    assert!(all == payload);
    s.shutdown().await;
    full.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn touched_file_forces_a_recheck() {
    let (m, payload) = make_torrent(&FILES, PL);
    let dir = tempfile::tempdir().unwrap();
    let (data, seed, log) = half_downloaded(&m, &payload, dir.path(), &[0, 1]).await;
    let before = log.lock().unwrap().len();
    assert!(requests_of(&log, 0, 0) > 0);

    // One byte of piece 0 rewritten, its modification time moved 2 s on.
    let a = file(dir.path(), "a");
    let mtime = std::fs::metadata(&a).unwrap().modified().unwrap();
    let mut bytes = std::fs::read(&a).unwrap();
    bytes[10] ^= 0xff;
    std::fs::write(&a, &bytes).unwrap();
    std::fs::File::options()
        .write(true)
        .open(&a)
        .unwrap()
        .set_modified(mtime + Duration::from_secs(2))
        .unwrap();

    let s = Session::new(dial_tcp(settings())).await.unwrap();
    let h = s
        .add_torrent(AddTorrent::from_resume(&data).unwrap())
        .await
        .unwrap();
    assert!(!trusted(&h).await);
    assert!(wait_state(&h, |s| *s == TorrentState::Paused, T).await);
    assert_eq!(have(&h).await, vec![1], "the check found piece 0 bad");
    h.resume().await.unwrap();
    h.add_peer(seed).await.unwrap();
    wait_have(&h, &[0, 1]).await;
    assert!(requests_of(&log, 0, before) > 0, "piece 0 fetched again");
    assert_eq!(requests_of(&log, 1, before), 0);
    s.shutdown().await;
}

fn skip_a_and_c() -> Vec<Priority> {
    vec![Priority::Skip, Priority::Normal, Priority::Skip]
}

/// b downloaded with a and c skipped: pieces 1 and 3 keep their a and c
/// bytes in the part file. Paused and saved.
async fn b_only(m: &Metainfo, payload: &[u8], dir: &Path) -> Vec<u8> {
    let full = seeder(m, payload).await;
    let s = Session::new(settings()).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), dir.to_path_buf());
    p.file_priorities = skip_a_and_c();
    let h = s.add_torrent(p).await.unwrap();
    h.add_peer(full.local_addr()).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    assert_eq!(have(&h).await, vec![1, 2, 3]);
    h.pause().await.unwrap();
    let data = h.save_resume_data().await.unwrap();
    s.shutdown().await;
    full.shutdown().await;
    assert!(!file(dir, "a").exists());
    assert!(FsStorage::new(dir.to_path_buf(), m).part_path().exists());
    data
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_part_file_forces_a_recheck() {
    let (m, payload) = make_torrent(&FILES, PL);
    let dir = tempfile::tempdir().unwrap();
    let data = b_only(&m, &payload, dir.path()).await;
    std::fs::remove_file(FsStorage::new(dir.path().to_path_buf(), &m).part_path()).unwrap();

    let s = Session::new(settings()).await.unwrap();
    let h = s
        .add_torrent(AddTorrent::from_resume(&data).unwrap())
        .await
        .unwrap();
    assert!(!trusted(&h).await);
    assert!(wait_state(&h, |s| *s == TorrentState::Paused, T).await);
    assert_eq!(have(&h).await, vec![2], "the boundary pieces went with it");
    s.shutdown().await;
}

/// Carried from the part file's review: a trusted resume skips the check,
/// yet the storage still learns the priorities before anything reads, so
/// a skipped file's boundary bytes are found in the part file (and
/// exported once it is wanted).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trusted_resume_reads_the_part_file() {
    let (m, payload) = make_torrent(&FILES, PL);
    let dir = tempfile::tempdir().unwrap();
    let data = b_only(&m, &payload, dir.path()).await;

    let s = Session::new(settings()).await.unwrap();
    let h = s
        .add_torrent(AddTorrent::from_resume(&data).unwrap())
        .await
        .unwrap();
    assert!(trusted(&h).await);
    assert_eq!(have(&h).await, vec![1, 2, 3]);
    h.resume().await.unwrap();
    // a's bytes in piece 1: from PL to the end of a.
    let got = read_file(&h, 0, u64::from(PL), A - PL as usize).await;
    assert!(got == payload[PL as usize..A], "a's boundary bytes");
    assert!(!file(dir.path(), "a").exists(), "a created");
    // Wanted now: exported out of the part file.
    h.set_file_priorities(vec![Priority::Normal, Priority::Normal, Priority::Skip])
        .await
        .unwrap();
    let a = std::fs::read(file(dir.path(), "a")).unwrap();
    assert!(a[PL as usize..] == payload[PL as usize..A]);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn custom_storage_is_trusted() {
    let (m, payload) = make_torrent(&FILES, PL);
    let s = Session::new(settings()).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(CountingStorage::new(payload.clone())));
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    h.pause().await.unwrap();
    let data = h.save_resume_data().await.unwrap();
    s.shutdown().await;

    let s = Session::new(settings()).await.unwrap();
    let storage = Arc::new(CountingStorage::new(payload.clone()));
    let mut p = AddTorrent::from_resume(&data).unwrap();
    p.storage = Some(Arc::clone(&storage) as Arc<dyn Storage>);
    let h = s.add_torrent(p).await.unwrap();
    assert!(trusted(&h).await);
    assert!(wait_state(&h, |s| *s == TorrentState::Paused, T).await);
    h.resume().await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    assert_eq!(storage.reads(), 0, "a piece was read before any request");

    let mut peer = ScriptedPeer::connect(s.local_addr(), m.info_hashes.wire_hash()).await;
    peer.send(Message::Interested).await;
    assert!(
        peer.next_matching(|m| matches!(m, Message::Unchoke))
            .await
            .is_some()
    );
    let block = BlockRef {
        piece: 2,
        offset: 0,
        len: 16_384,
    };
    peer.send(Message::Request(block)).await;
    let got = peer
        .next_matching(|m| matches!(m, Message::Piece { .. }))
        .await;
    let Some(Message::Piece { data, .. }) = got else {
        panic!("no piece");
    };
    let at = 2 * PL as usize;
    assert!(data[..] == payload[at..at + 16_384]);
    assert!(storage.reads() > 0);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn magnet_resume_skips_metadata_fetch() {
    let (m, payload) = make_torrent(&FILES, PL);
    let full = seeder(&m, &payload).await;
    let dir = tempfile::tempdir().unwrap();
    let s = Session::new(settings()).await.unwrap();
    let mut mg = Magnet::from_info_hashes(m.info_hashes);
    mg.peers = vec![full.local_addr()];
    let h = s
        .add_torrent(AddTorrent::magnet(mg, dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    h.pause().await.unwrap();
    let data = h.save_resume_data().await.unwrap();
    s.shutdown().await;
    full.shutdown().await;

    let s = Session::new(settings()).await.unwrap();
    let mut ev = s.subscribe(EventMask::ALL);
    let p = AddTorrent::from_resume(&data).unwrap();
    assert!(matches!(p.source, TorrentSource::Metainfo(_)));
    let h = s.add_torrent(p).await.unwrap();
    assert!(h.status().has_metadata);
    assert!(trusted(&h).await);
    h.resume().await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);

    // A peer offering the metadata is never asked for it.
    let mut reserved = Reserved::default();
    reserved.set_fast();
    reserved.set_extended();
    let mut peer =
        ScriptedPeer::connect_with(s.local_addr(), m.info_hashes.wire_hash(), reserved).await;
    let mut hs = ExtHandshake::default();
    hs.m.insert(UT_METADATA.to_owned(), 3);
    hs.metadata_size = Some(m.raw_info().len() as u32);
    peer.send_ext_handshake(&hs).await;
    let asked = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(Ok(msg)) = peer.framed.next().await {
            if matches!(msg, Message::Extended { id: 3, .. }) {
                return true;
            }
        }
        false
    })
    .await;
    assert_ne!(asked, Ok(true), "ut_metadata request sent");
    let events = drain(&mut ev).await;
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::MetadataReceived { .. })),
        "{events:?}"
    );
    assert!(!checked_files(&events, m.info_hashes));
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_keeps_priorities_renames_queue_and_web_seeds() {
    let (m, payload) = make_torrent(&FILES, PL);
    let (other, _) = make_torrent(&[("x", 1000)], 16_384);
    let (third, _) = make_torrent(&[("y", 1000)], 16_384);
    let dir = tempfile::tempdir().unwrap();
    write_payload(dir.path(), &m, &payload);
    let s = Session::new(settings()).await.unwrap();
    let mem = |len| -> Option<Arc<dyn fbt_session::storage::Storage>> {
        Some(Arc::new(MemStorage::new(len)))
    };
    let mut p = AddTorrent::new(other.clone(), PathBuf::from("unused"));
    p.storage = mem(1000);
    s.add_torrent(p).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), dir.path().to_path_buf());
    p.file_priorities = vec![Priority::Low, Priority::High, Priority::Normal];
    p.rate_limit = RateLimit::new(1_000_000, 2_000_000);
    p.sequential = true;
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    h.rename_file(0, "moved/first.bin").await.unwrap();
    let seed_url = "http://127.0.0.1:9/seed/".to_owned();
    h.add_web_seed(seed_url.clone()).await.unwrap();
    h.set_queue_position(0).await.unwrap();
    assert_eq!(h.status().queue_position, 0);
    h.pause().await.unwrap();
    let data = h.save_resume_data().await.unwrap();
    s.shutdown().await;
    assert!(dir.path().join("moved/first.bin").exists());
    assert!(!file(dir.path(), "a").exists());

    let s = Session::new(settings()).await.unwrap();
    let mut ev = s.subscribe(EventMask::ALL);
    let mut p = AddTorrent::new(third.clone(), PathBuf::from("unused"));
    p.storage = mem(1000);
    let t = s.add_torrent(p).await.unwrap();
    let p = AddTorrent::from_resume(&data).unwrap();
    assert_eq!(
        p.file_priorities,
        vec![Priority::Low, Priority::High, Priority::Normal]
    );
    assert_eq!(p.rate_limit, RateLimit::new(1_000_000, 2_000_000));
    assert!(p.sequential && p.paused && p.auto_managed);
    let h = s.add_torrent(p).await.unwrap();
    assert!(trusted(&h).await, "the renamed file was not found");
    assert_eq!(h.status().queue_position, 0);
    assert!(wait_until(|| t.status().queue_position == 1, T).await);
    assert_eq!(h.web_seeds().await.unwrap(), vec![seed_url]);
    assert_eq!(have(&h).await, vec![0, 1, 2, 3, 4]);
    h.resume().await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    assert!(read_file(&h, 0, 0, A).await == payload[..A]);
    assert!(!checked_files(&drain(&mut ev).await, m.info_hashes));
    s.shutdown().await;
}

/// `bytes` decoded, `f` applied to the root dictionary, re-encoded.
fn edit(
    bytes: &[u8],
    f: impl FnOnce(&mut std::collections::BTreeMap<Vec<u8>, OwnedValue>),
) -> Vec<u8> {
    let OwnedValue::Dict(mut d) = fbt_bencode::decode(bytes).unwrap().to_owned_value() else {
        panic!("not a dict");
    };
    f(&mut d);
    encode(&OwnedValue::Dict(d))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_rejects_garbage_and_mismatches() {
    let (m, payload) = make_torrent(&FILES, PL);
    let s = Session::new(settings()).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(payload.clone())));
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    let good = h.save_resume_data().await.unwrap();
    s.shutdown().await;
    assert!(AddTorrent::from_resume(&good).is_ok());
    let err = |b: &[u8]| AddTorrent::from_resume(b).unwrap_err();

    // Random bytes, deterministic.
    let mut x = 0x2545_f491_4f6c_dd1du64;
    let random: Vec<u8> = (0..4096)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect();
    assert!(matches!(err(&random), ResumeError::Bencode(_)));
    assert!(matches!(
        err(&good[..good.len() / 2]),
        ResumeError::Bencode(_)
    ));
    let v2 = edit(&good, |d| {
        d.insert(b"fbt-resume-version".to_vec(), OwnedValue::Int(2));
    });
    assert!(matches!(err(&v2), ResumeError::Version(2)));
    let (other, _) = make_torrent(&[("x", 1000)], 16_384);
    let swapped = edit(&good, |d| {
        let info = fbt_bencode::decode(other.raw_info())
            .unwrap()
            .to_owned_value();
        d.insert(b"info".to_vec(), info);
    });
    assert!(matches!(err(&swapped), ResumeError::InfoHashMismatch));
    let short = edit(&good, |d| {
        d.insert(b"pieces".to_vec(), OwnedValue::Bytes(vec![0xff; 2]));
    });
    assert!(matches!(err(&short), ResumeError::Invalid("pieces")));
    let huge = vec![b'0'; 65 * 1024 * 1024];
    assert!(matches!(err(&huge), ResumeError::TooLarge));
    let peers: Vec<u8> = (0..10_000u32)
        .flat_map(|i| {
            let mut b = (0x0a00_0000 + i).to_be_bytes().to_vec();
            b.extend_from_slice(&6881u16.to_be_bytes());
            b
        })
        .collect();
    let many = edit(&good, |d| {
        d.insert(b"peers".to_vec(), OwnedValue::Bytes(peers));
    });
    assert!(AddTorrent::from_resume(&many).is_ok());
    assert_eq!(decode_resume(&many).unwrap().peers().len(), 200);

    // `restore` adds what it can and names what it could not.
    let mut state = SessionState::default();
    state.torrents = vec![random, good.clone()];
    let (s, failed) = Session::restore(settings(), &state).await.unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].0, 0);
    assert!(matches!(
        failed[0].1,
        SessionError::Resume(ResumeError::Bencode(_))
    ));
    assert_eq!(s.torrents().len(), 1);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn need_save_resume_follows_changes() {
    let (m, payload) = make_torrent(&FILES, PL);
    let (other, _) = make_torrent(&[("x", 1000)], 16_384);
    let full = seeder(&m, &payload).await;
    let dir = tempfile::tempdir().unwrap();
    let s = Session::new(settings()).await.unwrap();
    let mut p = AddTorrent::new(other, PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::new(1000)));
    s.add_torrent(p).await.unwrap();
    let h = s
        .add_torrent(AddTorrent::new(m.clone(), dir.path().to_path_buf()))
        .await
        .unwrap();
    let need = |h: &TorrentHandle| h.status().need_save_resume;
    assert!(need(&h), "never saved");
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);
    h.save_resume_data().await.unwrap();
    assert!(!need(&h));

    h.add_peer(full.local_addr()).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    assert!(need(&h), "pieces written");
    h.save_resume_data().await.unwrap();
    assert!(!need(&h));

    h.set_file_priorities(vec![Priority::High, Priority::Normal, Priority::Normal])
        .await
        .unwrap();
    assert!(need(&h), "priority");
    h.save_resume_data().await.unwrap();
    h.pause().await.unwrap();
    assert!(need(&h), "pause");
    h.save_resume_data().await.unwrap();
    h.resume().await.unwrap();
    assert!(need(&h), "resume");
    h.save_resume_data().await.unwrap();
    h.set_rate_limit(RateLimit::new(1, 0)).await.unwrap();
    assert!(need(&h), "limit");
    h.save_resume_data().await.unwrap();
    h.set_queue_position(0).await.unwrap();
    assert!(need(&h), "queue");
    h.save_resume_data().await.unwrap();
    h.rename_file(2, "torrent/c2").await.unwrap();
    assert!(need(&h), "rename");
    h.save_resume_data().await.unwrap();
    let moved = tempfile::tempdir().unwrap();
    h.move_storage(moved.path().to_path_buf(), Default::default())
        .await
        .unwrap();
    assert!(need(&h), "move");
    h.save_resume_data().await.unwrap();
    assert!(!need(&h));
    s.shutdown().await;
    full.shutdown().await;
}

/// A loopback DHT node, IPv4 only.
async fn loopback_dht() -> Dht {
    let mut c = DhtConfig::default();
    c.bind_v4 = Some(SocketAddr::from(([127, 0, 0, 1], 0)));
    c.bind_v6 = None;
    c.bootstrap = Vec::new();
    c.query_rate_per_ip = 10_000;
    c.query_timeout = Duration::from_millis(500);
    Dht::spawn(c).await.unwrap()
}

/// Settings with the DHT on loopback, bootstrapping from `bootstrap` only.
fn dht_settings(bootstrap: Vec<String>) -> Settings {
    let mut s = settings();
    s.dht_enabled = true;
    s.dht_bootstrap_nodes = bootstrap;
    s.dht_query_timeout = Duration::from_millis(500);
    s.dht_query_rate_per_ip = 10_000;
    s
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_state_round_trip() {
    let boot = loopback_dht().await;
    let s = Session::new(dht_settings(vec![boot.local_addrs()[0].to_string()]))
        .await
        .unwrap();
    let dirs: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut order: Vec<InfoHashes> = Vec::new();
    for dir in &dirs {
        let (m, payload) = make_torrent(&[("a", 100_000), ("b", 50_000)], 32_768);
        write_payload(dir.path(), &m, &payload);
        let h = s
            .add_torrent(AddTorrent::new(m.clone(), dir.path().to_path_buf()))
            .await
            .unwrap();
        assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
        order.push(m.info_hashes);
    }
    // Last first.
    s.torrent(&order[2])
        .unwrap()
        .set_queue_position(0)
        .await
        .unwrap();
    order.rotate_right(1);
    assert!(wait_until(|| s.dht_stats().unwrap().nodes_v4 >= 1, T).await);
    let state = s.shutdown_and_save().await;
    assert_eq!(state.torrents.len(), 3);
    assert!(state.dht.is_some());
    let state = SessionState::from_bytes(&state.to_bytes()).unwrap();

    let (s, failed) = Session::restore(dht_settings(Vec::new()), &state)
        .await
        .unwrap();
    assert!(failed.is_empty(), "{failed:?}");
    let got: Vec<InfoHashes> = s
        .torrents()
        .iter()
        .map(TorrentHandle::info_hashes)
        .collect();
    assert_eq!(got, order);
    for h in s.torrents() {
        assert!(trusted(&h).await);
        assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    }
    // From the saved state alone: nothing to bootstrap from.
    assert!(
        wait_until(
            || s.dht_stats().unwrap().nodes_v4 > 0,
            Duration::from_secs(3)
        )
        .await
    );
    s.shutdown().await;
    boot.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dirty_pieces_at_shutdown_are_in_the_state() {
    let (m, payload) = make_torrent(&FILES, PL);
    let n = m.piece_layout().num_pieces();
    let (seed, _log) = partial_seed(&payload, n, &[0, 1, 2]).await;
    let mut st = dial_tcp(settings());
    st.cache_size = 64 << 20;
    st.cache_expiry = Duration::from_secs(3600);
    let s = Session::new(st).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let h = s
        .add_torrent(AddTorrent::new(m.clone(), dir.path().to_path_buf()))
        .await
        .unwrap();
    h.add_peer(seed).await.unwrap();
    wait_have(&h, &[0, 1, 2]).await;
    assert!(s.cache_stats().dirty > 0, "nothing left dirty to test");
    let state = s.shutdown_and_save().await;
    assert_eq!(state.torrents.len(), 1);

    let (s, failed) = Session::restore(dial_tcp(settings()), &state)
        .await
        .unwrap();
    assert!(failed.is_empty());
    let h = s.torrents().remove(0);
    assert!(trusted(&h).await);
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);
    assert_eq!(have(&h).await, vec![0, 1, 2]);
    let a = std::fs::read(file(dir.path(), "a")).unwrap();
    assert!(a == payload[..A]);
    s.shutdown().await;
}
