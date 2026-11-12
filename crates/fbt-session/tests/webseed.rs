//! Web seeds (BEP 19) against the in-repo HTTP server of
//! `support/web_seed.rs`, which serves files on loopback and misbehaves on
//! demand: nothing here reaches a real web server.

mod support;

#[path = "support/web_seed.rs"]
mod web_seed;

#[path = "../../fbt-net/tests/support/fake_socks5.rs"]
mod fake_socks5;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fake_socks5::{FakeSocks5, Script};
use fbt_bencode::{DictBuilder, encode};
use fbt_metainfo::Metainfo;
use fbt_session::storage::{MemStorage, Storage, sha1_piece};
use fbt_session::{
    AddTorrent, Event, EventMask, EventStream, ProxyKind, RateLimit, Session, Settings,
    TorrentHandle, TorrentState,
};
use rand::RngExt;
use support::*;
use web_seed::{Behaviour, WebSeedServer};

/// A generous bound for a slow download (1 MiB at 64 KiB/s is 16 s).
const SLOW: Duration = Duration::from_secs(90);

/// A single-file torrent `name` over random data.
fn make_single(name: &str, len: usize, piece_len: u32) -> (Metainfo, Vec<u8>) {
    let mut payload = vec![0u8; len];
    rand::rng().fill(payload.as_mut_slice());
    let mut pieces = Vec::new();
    for chunk in payload.chunks(piece_len as usize) {
        pieces.extend_from_slice(&sha1_piece(chunk));
    }
    let mut info = DictBuilder::new();
    info.insert(b"length", len as i64);
    info.insert(b"name", name);
    info.insert(b"piece length", i64::from(piece_len));
    info.insert(b"pieces", pieces);
    let mut root = DictBuilder::new();
    root.insert(b"info", info.build());
    let m = Metainfo::from_bytes(&encode(&root.build())).expect("valid torrent");
    (m, payload)
}

/// The files of `m` below a new temporary directory, and a server for them.
async fn server(
    m: &Metainfo,
    payload: &[u8],
    behaviour: Behaviour,
) -> (tempfile::TempDir, WebSeedServer) {
    let dir = tempfile::tempdir().unwrap();
    write_payload(dir.path(), m, payload);
    let s = WebSeedServer::start(dir.path().to_path_buf(), behaviour).await;
    (dir, s)
}

struct Leecher {
    session: Session,
    handle: TorrentHandle,
    events: EventStream,
    storage: Arc<MemStorage>,
}

/// A leecher for `m` (web seeds included) on `st`, subscribed to every
/// event before adding; its storage starts as `data`.
async fn leecher_with(st: Settings, m: &Metainfo, data: Vec<u8>, paused: bool) -> Leecher {
    let session = Session::new(st).await.unwrap();
    let events = session.subscribe(EventMask::ALL);
    let storage = Arc::new(MemStorage::from_bytes(data));
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(storage.clone());
    p.paused = paused;
    let handle = session.add_torrent(p).await.unwrap();
    Leecher {
        session,
        handle,
        events,
        storage,
    }
}

async fn leecher(st: Settings, m: &Metainfo, len: usize) -> Leecher {
    leecher_with(st, m, vec![0; len], false).await
}

impl Leecher {
    async fn finish(&self, payload: &[u8], timeout: Duration) {
        assert!(
            wait_state(&self.handle, |s| *s == TorrentState::Seeding, timeout).await,
            "download did not finish: {:?}",
            self.handle.status()
        );
        let mut got = vec![0u8; payload.len()];
        self.storage.read(0, &mut got).unwrap();
        assert!(got == payload, "data differs");
    }

    /// The next `WebSeedError`: its message and whether it retired the seed.
    async fn web_seed_error(&mut self) -> (String, String, bool) {
        match next_matching(
            &mut self.events,
            |e| matches!(e, Event::WebSeedError { .. }),
            T,
        )
        .await
        {
            Some(Event::WebSeedError {
                url,
                message,
                retired,
                ..
            }) => (url, message, retired),
            other => panic!("no WebSeedError: {other:?}"),
        }
    }
}

/// A seeding session for `m`, uploading at most `upload` bytes per second
/// (0: unlimited).
async fn seeder(m: &Metainfo, payload: &[u8], upload: u64) -> (Session, TorrentHandle) {
    let mut st = test_settings();
    // The leecher is on loopback.
    st.rate_limit_local_peers = true;
    let s = Session::new(st).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(payload.to_vec())));
    p.rate_limit = RateLimit::new(0, upload);
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    (s, h)
}

/// `bytes=a-b` as `(a, b + 1)`.
fn range_of(header: &str) -> (u64, u64) {
    let spec = header.strip_prefix("bytes=").expect("bytes= range");
    let (a, b) = spec.split_once('-').expect("a-b");
    (a.parse().unwrap(), b.parse::<u64>().unwrap() + 1)
}

/// Every request of `server` carried a `Range`, and per file the ranges
/// asked for cover it exactly once.
fn assert_ranges_cover(server: &WebSeedServer, files: &[(String, u64)]) {
    let mut per_file: BTreeMap<String, Vec<(u64, u64)>> = BTreeMap::new();
    for (path, range) in server.requests() {
        let range = range.unwrap_or_else(|| panic!("request for {path} without a Range"));
        per_file.entry(path).or_default().push(range_of(&range));
    }
    for (path, len) in files {
        let mut ranges = per_file.remove(path).unwrap_or_default();
        ranges.sort_unstable();
        let mut at = 0;
        for (a, b) in ranges {
            assert_eq!(a, at, "{path}: ranges overlap or leave a gap");
            at = b;
        }
        assert_eq!(at, *len, "{path}: ranges do not cover the file");
    }
    assert!(
        per_file.is_empty(),
        "requests for other paths: {per_file:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn downloads_from_a_web_seed_alone() {
    // Single file: the base URL ending in `/` gets the name.
    let (mut m, payload) = make_single("single.bin", 300_000, 32_768);
    let (_dir, srv) = server(&m, &payload, Behaviour::Normal).await;
    m.web_seeds = vec![srv.url()];
    let l = leecher(test_settings(), &m, payload.len()).await;
    l.finish(&payload, T).await;
    assert_eq!(l.handle.web_seeds().await.unwrap(), vec![srv.url()]);
    assert_eq!(l.handle.status().num_web_seeds, 1);
    assert_ranges_cover(&srv, &[("/single.bin".to_owned(), 300_000)]);
    l.session.shutdown().await;

    // Multi-file: the name and each path component follow; pieces that
    // span files become one range per file.
    let files = [("a", 200_000), ("dir/b", 150_000), ("c", 1_000)];
    let (mut m, payload) = make_torrent(&files, 32_768);
    let (_dir, srv) = server(&m, &payload, Behaviour::Normal).await;
    m.web_seeds = vec![srv.url()];
    let l = leecher(test_settings(), &m, payload.len()).await;
    l.finish(&payload, T).await;
    let expect: Vec<(String, u64)> = files
        .iter()
        .map(|(p, len)| (format!("/torrent/{p}"), *len as u64))
        .collect();
    assert_ranges_cover(&srv, &expect);
    l.session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn web_seed_and_peer_together() {
    const MIB: u64 = 1024 * 1024;
    let (mut m, payload) = make_torrent(&[("big", 16 * MIB as usize)], 256 * 1024);
    let (_dir, srv) = server(&m, &payload, Behaviour::Normal).await;
    srv.set_rate(MIB);
    m.web_seeds = vec![srv.url()];
    let (seed, seed_h) = seeder(&m, &payload, MIB).await;
    let l = leecher(dial_tcp(test_settings()), &m, payload.len()).await;
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    l.finish(&payload, SLOW).await;
    let uploaded = seed_h.status().total_uploaded;
    let downloaded = l.handle.status().total_downloaded;
    assert!(uploaded > 0, "the peer delivered nothing");
    // Every accepted byte the peer did not send came from the web seed.
    assert!(
        downloaded > uploaded,
        "the web seed delivered nothing: downloaded {downloaded}, peer sent {uploaded}"
    );
    l.session.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ignored_range_is_an_error() {
    let piece = 32_768usize;
    let (mut m, payload) = make_single("f.bin", 8 * piece, piece as u32);
    let (_dir, srv) = server(&m, &payload, Behaviour::IgnoreRange).await;
    m.web_seeds = vec![srv.url()];
    // The first half is already here, so every range asked for starts
    // past 0, where a whole-file 200 can never stand for it.
    let mut data = payload.clone();
    data[4 * piece..].fill(0);
    let mut l = leecher_with(test_settings(), &m, data, false).await;
    let (url, message, retired) = l.web_seed_error().await;
    assert_eq!(url, srv.url());
    assert!(!retired, "an ignored range only backs off: {message}");
    let (_, range) = srv.requests().first().cloned().expect("a request");
    assert!(range_of(&range.expect("a Range")).0 >= 4 * piece as u64);
    let st = l.handle.status();
    assert_eq!(st.total_downloaded, 0, "nothing of the 200 is accepted");
    assert_eq!(st.num_web_seeds, 1, "backing off, not retired");
    l.session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_content_range_is_an_error() {
    let (mut m, payload) = make_single("f.bin", 100_000, 16_384);
    let (_dir, srv) = server(&m, &payload, Behaviour::WrongContentRange).await;
    m.web_seeds = vec![srv.url()];
    let mut l = leecher(test_settings(), &m, payload.len()).await;
    let (_, message, retired) = l.web_seed_error().await;
    assert!(!retired, "{message}");
    assert_eq!(l.handle.status().total_downloaded, 0);
    l.session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn endless_body_is_capped() {
    let (mut m, payload) = make_single("f.bin", 100_000, 16_384);
    let (_dir, srv) = server(&m, &payload, Behaviour::EndlessBody).await;
    m.web_seeds = vec![srv.url()];
    let mut l = leecher(test_settings(), &m, payload.len()).await;
    let (_, message, retired) = l.web_seed_error().await;
    assert!(!retired, "{message}");
    assert_eq!(l.handle.status().total_downloaded, 0);
    // The client stopped reading: the server's writes fail soon after
    // whatever the socket buffers held.
    let before = srv.sent();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let after = srv.sent();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(srv.sent(), after, "the body is still being read");
    assert!(after < 64 << 20, "read {before}..{after} bytes of the body");
    l.session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_live_seed_is_not_cut() {
    let (mut m, payload) = make_single("f.bin", 1024 * 1024, 64 * 1024);
    let (_dir, srv) = server(&m, &payload, Behaviour::Normal).await;
    srv.set_rate(64 * 1024);
    m.web_seeds = vec![srv.url()];
    let mut st = test_settings();
    // Far shorter than the whole request, longer than any gap in it.
    st.tracker.http_timeout = Duration::from_secs(2);
    let l = leecher(st, &m, payload.len()).await;
    l.finish(&payload, SLOW).await;
    l.session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redirect_loop_stops() {
    let (mut m, payload) = make_single("f.bin", 100_000, 16_384);
    let (_dir, srv) = server(&m, &payload, Behaviour::RedirectLoop).await;
    m.web_seeds = vec![srv.url()];
    let mut l = leecher(test_settings(), &m, payload.len()).await;
    let (_, message, retired) = l.web_seed_error().await;
    assert!(!retired, "{message}");
    assert!(message.contains("redirect"), "{message}");
    // The first request and three redirects; the seed then backs off.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(srv.requests().len(), 4, "{:?}", srv.requests());
    l.session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_file_retires_the_seed() {
    let files = [("a", 50_000), ("b", 50_000)];
    let (mut m, payload) = make_torrent(&files, 16_384);
    let (_dir, srv) = server(&m, &payload, Behaviour::NotFound("/torrent/b".into())).await;
    m.web_seeds = vec![srv.url()];
    let mut l = leecher(test_settings(), &m, payload.len()).await;
    let (url, message, retired) = l.web_seed_error().await;
    assert_eq!(url, srv.url());
    assert!(retired, "a 404 retires the seed: {message}");
    assert!(message.contains("404"), "{message}");
    assert_eq!(l.handle.status().num_web_seeds, 0);
    // Listed still, but never asked again.
    assert_eq!(l.handle.web_seeds().await.unwrap(), vec![srv.url()]);
    let n = srv.requests().len();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(srv.requests().len(), n);
    l.session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_web_seed_is_retired() {
    let piece = 16_384usize;
    let (mut m, payload) = make_single("f.bin", 8 * piece, piece as u32);
    // Piece 2 is wrong every time the seed serves it.
    let (_dir, srv) = server(&m, &payload, Behaviour::Corrupt(2 * piece as u64 + 7)).await;
    m.web_seeds = vec![srv.url()];
    let mut l = leecher(dial_tcp(test_settings()), &m, payload.len()).await;
    let failed = next_matching(
        &mut l.events,
        |e| matches!(e, Event::HashFailed { piece: 2, .. }),
        T,
    )
    .await;
    assert!(failed.is_some(), "piece 2 never failed");
    // A peer has the good piece: once it passes, the seed's block differs.
    let (seed, _) = seeder(&m, &payload, 0).await;
    l.handle.add_peer(seed.local_addr()).await.unwrap();
    let (_, message, retired) = l.web_seed_error().await;
    assert!(retired, "smart ban retires the seed: {message}");
    l.finish(&payload, T).await;
    assert_eq!(l.handle.status().num_web_seeds, 0);
    l.session.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn percent_encoding_of_paths() {
    let (mut m, payload) = make_torrent(&[("a b#c.bin", 40_000)], 16_384);
    let (_dir, srv) = server(&m, &payload, Behaviour::Normal).await;
    m.web_seeds = vec![srv.url()];
    let l = leecher(test_settings(), &m, payload.len()).await;
    l.finish(&payload, T).await;
    let paths: Vec<String> = srv.requests().into_iter().map(|(p, _)| p).collect();
    assert!(!paths.is_empty());
    assert!(
        paths.iter().all(|p| p == "/torrent/a%20b%23c.bin"),
        "{paths:?}"
    );
    l.session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn web_seed_through_socks5() {
    let fake = FakeSocks5::start(Script::Ok).await;
    let (mut m, payload) = make_single("f.bin", 100_000, 16_384);
    let (_dir, srv) = server(&m, &payload, Behaviour::Normal).await;
    m.web_seeds = vec![srv.url()];
    let mut st = test_settings();
    st.proxy.kind = ProxyKind::Socks5;
    st.proxy.host = fake.addr().ip().to_string();
    st.proxy.port = fake.addr().port();
    st.proxy.proxy_peers = true;
    st.proxy.proxy_trackers = false;
    let l = leecher(st, &m, payload.len()).await;
    l.finish(&payload, T).await;
    assert!(
        fake.connects().contains(&srv.addr().to_string()),
        "CONNECTs: {:?}",
        fake.connects()
    );
    l.session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_torrent_fetches_nothing() {
    let (m, payload) = make_single("f.bin", 100_000, 16_384);
    let (_dir, srv) = server(&m, &payload, Behaviour::Normal).await;
    let l = leecher_with(test_settings(), &m, vec![0; payload.len()], true).await;
    l.handle.add_web_seed(srv.url()).await.unwrap();
    l.handle.add_web_seed(srv.url()).await.unwrap();
    assert_eq!(l.handle.web_seeds().await.unwrap(), vec![srv.url()]);
    assert!(
        l.handle.add_web_seed("ftp://x/".into()).await.is_err(),
        "only http and https"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(srv.requests().is_empty(), "a paused torrent fetched");
    l.handle.resume().await.unwrap();
    l.finish(&payload, T).await;
    l.handle.remove_web_seed(&srv.url()).await.unwrap();
    assert!(l.handle.web_seeds().await.unwrap().is_empty());
    assert_eq!(l.handle.status().num_web_seeds, 0);
    l.session.shutdown().await;
}
