//! `GET`/`HEAD /api/v1/torrents/{id}/files/{index}/stream`: byte-range
//! streaming of a torrent's files.

mod support;

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;
use fbt::{
    AddTorrent, CreateTorrent, Magnet, Metainfo, RateLimit, Session, TorrentHandle, TorrentState,
    TorrentVersion,
};
use fbt_api::{ApiConfig, ApiServer};
use http_body_util::{BodyExt, Full};
use hyper::header::{HeaderMap, RANGE};
use hyper::{Method, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use support::{T, TOKEN, api, api_with, error_code, request, send, send_body, test_settings};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// `n` bytes that differ from offset to offset, so a wrong range shows.
fn pattern(n: usize, salt: u8) -> Vec<u8> {
    (0..n)
        .map(|i| (i.wrapping_mul(31) ^ (i >> 9)) as u8 ^ salt)
        .collect()
}

/// Creates a v1 torrent of `files` (name, bytes) in `dir/payload`; the
/// metainfo and the save path that seeds it.
fn create(dir: &Path, files: &[(&str, &[u8])], piece_len: u32) -> (Metainfo, PathBuf) {
    let src = dir.join("payload");
    std::fs::create_dir(&src).expect("mkdir");
    for (name, data) in files {
        std::fs::write(src.join(name), data).expect("write");
    }
    // A single-file torrent is its file, saved in the file's directory.
    let (path, save) = if let [(name, _)] = files {
        (src.join(name), src)
    } else {
        (src, dir.to_path_buf())
    };
    let bytes = CreateTorrent::new(path)
        .version(TorrentVersion::V1)
        .piece_len(piece_len)
        .build(&mut |_, _| ControlFlow::Continue(()))
        .expect("create");
    (Metainfo::from_bytes(&bytes).expect("parse"), save)
}

fn id_of(m: &Metainfo) -> String {
    m.info_hashes.v1.expect("v1").to_string()
}

/// The index of the file named `name`.
fn index_of(m: &Metainfo, name: &str) -> usize {
    m.info
        .files
        .iter()
        .position(|f| f.path.components().last().is_some_and(|c| c == name))
        .expect("file in the torrent")
}

/// Waits until `h` is in `state`.
async fn wait_state(h: &TorrentHandle, state: TorrentState) {
    tokio::time::timeout(T, async {
        while h.status().state != state {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("torrent state in time");
}

/// An API over a session seeding three files: an empty one, a 300 000-byte
/// `video.mp4` and a 70 000-byte `data.bin`.
struct Seeded {
    _session: Session,
    _server: ApiServer,
    base: String,
    id: String,
    video: Vec<u8>,
    video_index: usize,
    empty_index: usize,
    _dir: tempfile::TempDir,
}

impl Seeded {
    fn url(&self, index: usize) -> String {
        format!(
            "{}/api/v1/torrents/{}/files/{index}/stream",
            self.base, self.id
        )
    }

    fn video_url(&self) -> String {
        self.url(self.video_index)
    }
}

async fn seeded() -> Seeded {
    let (session, server, base, _) = api().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let video = pattern(300_000, 1);
    let data = pattern(70_000, 2);
    let (m, save) = create(
        dir.path(),
        &[
            ("empty.txt", &[]),
            ("video.mp4", &video),
            ("data.bin", &data),
        ],
        16384,
    );
    let handle = session
        .add_torrent(AddTorrent::new(m.clone(), save))
        .await
        .expect("add");
    wait_state(&handle, TorrentState::Seeding).await;
    Seeded {
        _session: session,
        _server: server,
        base,
        id: id_of(&m),
        video,
        video_index: index_of(&m, "video.mp4"),
        empty_index: index_of(&m, "empty.txt"),
        _dir: dir,
    }
}

/// `method url` with the bearer token and `headers`; the whole response.
async fn fetch(
    method: Method,
    url: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, HeaderMap, Bytes) {
    let mut b = request(method, url, Some(TOKEN));
    for (name, value) in headers {
        b = b.header(*name, *value);
    }
    send(b.body(Full::new(Bytes::new())).expect("request builds")).await
}

#[tokio::test]
async fn serves_whole_file() {
    let s = seeded().await;
    let (status, headers, body) = fetch(Method::GET, &s.video_url(), &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-length"], "300000");
    assert_eq!(headers["accept-ranges"], "bytes");
    assert_eq!(headers["content-type"], "video/mp4");
    let disposition = headers["content-disposition"].to_str().expect("ascii");
    assert!(
        disposition.contains("filename=\"video.mp4\""),
        "{disposition}"
    );
    assert!(!headers.contains_key("content-range"));
    assert!(body == s.video, "the file's bytes");
}

#[tokio::test]
async fn serves_partial_content() {
    let s = seeded().await;
    let (status, headers, body) =
        fetch(Method::GET, &s.video_url(), &[("range", "bytes=1000-1999")]).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(headers["content-range"], "bytes 1000-1999/300000");
    assert_eq!(headers["content-length"], "1000");
    assert!(body == s.video[1000..2000], "bytes 1000 to 1999");

    let (status, headers, body) =
        fetch(Method::GET, &s.video_url(), &[("range", "bytes=-500")]).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(headers["content-range"], "bytes 299500-299999/300000");
    assert!(body == s.video[299_500..], "the last 500 bytes");
}

#[tokio::test]
async fn unsatisfiable_range_is_416() {
    let s = seeded().await;
    let (status, headers, body) =
        fetch(Method::GET, &s.video_url(), &[("range", "bytes=300000-")]).await;
    assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(headers["content-range"], "bytes */300000");
    assert_eq!(error_code(&body), "range_not_satisfiable");
}

#[tokio::test]
async fn multi_range_serves_whole_file() {
    let s = seeded().await;
    let (status, headers, body) =
        fetch(Method::GET, &s.video_url(), &[("range", "bytes=0-1,5-6")]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-length"], "300000");
    assert!(body == s.video, "the whole file");
}

#[tokio::test]
async fn if_range_serves_whole_file() {
    let s = seeded().await;
    let (status, _, body) = fetch(
        Method::GET,
        &s.video_url(),
        &[("range", "bytes=0-9"), ("if-range", "\"x\"")],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body == s.video, "the whole file");
}

#[tokio::test]
async fn head_has_no_body() {
    let s = seeded().await;
    for range in [None, Some("bytes=1000-1999"), Some("bytes=300000-")] {
        let headers: Vec<(&str, &str)> = range.map(|r| ("range", r)).into_iter().collect();
        let (get_status, get_headers, _) = fetch(Method::GET, &s.video_url(), &headers).await;
        let (status, head_headers, body) = fetch(Method::HEAD, &s.video_url(), &headers).await;
        assert_eq!(status, get_status, "{range:?}");
        assert!(body.is_empty(), "{range:?}");
        for name in [
            "content-length",
            "content-range",
            "accept-ranges",
            "content-type",
            "content-disposition",
        ] {
            assert_eq!(
                head_headers.get(name),
                get_headers.get(name),
                "{name} for {range:?}"
            );
        }
    }
    let (status, headers, _) = fetch(Method::HEAD, &s.video_url(), &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-length"], "300000");
}

#[tokio::test]
async fn empty_file_streams() {
    let s = seeded().await;
    let (status, headers, body) = fetch(Method::GET, &s.url(s.empty_index), &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-length"], "0");
    assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
    assert!(body.is_empty());
    let (status, _, _) = fetch(Method::GET, &s.url(s.empty_index), &[("range", "bytes=0-")]).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn token_in_query_is_accepted() {
    let s = seeded().await;
    let url = format!("{}?token={TOKEN}", s.video_url());
    let req = request(Method::GET, &url, None)
        .header(RANGE, "bytes=0-99")
        .body(Full::new(Bytes::new()))
        .expect("request builds");
    let (status, _, body) = send(req).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert!(body == s.video[..100]);
    let req = request(Method::HEAD, &url, None)
        .body(Full::new(Bytes::new()))
        .expect("request builds");
    assert_eq!(send(req).await.0, StatusCode::OK);
    let wrong = format!("{}?token=wrong", s.video_url());
    let (status, _, _) = support::get(&wrong, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn bad_index_is_404() {
    let s = seeded().await;
    for method in [Method::GET, Method::HEAD] {
        let (status, _, body) = fetch(method.clone(), &s.url(99), &[]).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{method}");
        if method == Method::GET {
            assert_eq!(error_code(&body), "not_found");
        }
    }
    let url = format!(
        "{}/api/v1/torrents/{}/files/0/stream",
        s.base,
        "ab".repeat(20)
    );
    let (status, _, body) = fetch(Method::GET, &url, &[]).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(error_code(&body), "not_found");
    let url = format!("{}/api/v1/torrents/{}/files/x/stream", s.base, s.id);
    let (status, _, body) = fetch(Method::GET, &url, &[]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "invalid_request");
}

#[tokio::test]
async fn stream_without_metadata_is_409() {
    let (session, _server, base, _) = api().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let magnet =
        Magnet::parse(&format!("magnet:?xt=urn:btih:{}", "22".repeat(20))).expect("magnet");
    session
        .add_torrent(AddTorrent::magnet(magnet, dir.path().to_path_buf()))
        .await
        .expect("add");
    let url = format!("{base}/api/v1/torrents/{}/files/0/stream", "22".repeat(20));
    for method in [Method::GET, Method::HEAD] {
        let (status, _, body) = fetch(method.clone(), &url, &[]).await;
        assert_eq!(status, StatusCode::CONFLICT, "{method}");
        if method == Method::GET {
            assert_eq!(error_code(&body), "no_metadata");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_while_downloading() {
    // The seed uploads at 128 KiB/s, so the 2 MiB file takes about 16 s.
    let mut settings = test_settings();
    settings.rate_limit_local_peers = true;
    settings.rate_limit = RateLimit::new(0, 128 * 1024);
    let seed = Session::new(settings).await.expect("seed session");
    let seed_dir = tempfile::tempdir().expect("tempdir");
    let video = pattern(2 << 20, 3);
    let (m, save) = create(seed_dir.path(), &[("video.mp4", &video)], 16384);
    let sh = seed
        .add_torrent(AddTorrent::new(m.clone(), save))
        .await
        .expect("seed add");
    wait_state(&sh, TorrentState::Seeding).await;

    let (session, _server, base, _) = api().await;
    let leech_dir = tempfile::tempdir().expect("tempdir");
    let lh = session
        .add_torrent(AddTorrent::new(m.clone(), leech_dir.path().to_path_buf()))
        .await
        .expect("leech add");
    lh.add_peer(seed.local_addr()).await.expect("add peer");
    tokio::time::timeout(T, async {
        while lh.status().num_peers == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("connected in time");

    let url = format!(
        "{base}/api/v1/torrents/{}/files/{}/stream",
        id_of(&m),
        index_of(&m, "video.mp4")
    );
    let progress = lh.status().progress;
    assert!(
        progress < 1.0,
        "still downloading at request time: {progress}"
    );
    let (status, headers, body) = fetch(Method::GET, &url, &[("range", "bytes=-65536")]).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    let len = video.len();
    assert_eq!(
        headers["content-range"],
        format!("bytes {}-{}/{len}", len - 65536, len - 1).as_str()
    );
    assert!(body == video[len - 65536..], "the last 64 KiB");
    session.shutdown().await;
    seed.shutdown().await;
}

/// A leech in an API whose only seed is paused: its file can never
/// complete, so a stream of it stalls.
struct Stalled {
    session: Session,
    server: ApiServer,
    base: String,
    handle: TorrentHandle,
    id: String,
    /// The path of the stalled stream.
    path: String,
    seed: Session,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
}

async fn stalled(edit: impl FnOnce(&mut ApiConfig)) -> Stalled {
    let seed = Session::new(test_settings()).await.expect("seed session");
    let seed_dir = tempfile::tempdir().expect("tempdir");
    let (m, save) = create(
        seed_dir.path(),
        &[("video.mp4", &pattern(200_000, 4))],
        16384,
    );
    let sh = seed
        .add_torrent(AddTorrent::new(m.clone(), save))
        .await
        .expect("seed add");
    wait_state(&sh, TorrentState::Seeding).await;
    sh.pause().await.expect("pause");

    let (session, server, base, _) = api_with(edit).await;
    let leech_dir = tempfile::tempdir().expect("tempdir");
    let handle = session
        .add_torrent(AddTorrent::new(m.clone(), leech_dir.path().to_path_buf()))
        .await
        .expect("leech add");
    handle.add_peer(seed.local_addr()).await.expect("add peer");
    let id = id_of(&m);
    let path = format!("/api/v1/torrents/{id}/files/0/stream");
    Stalled {
        session,
        server,
        base,
        handle,
        id,
        path,
        seed,
        _dirs: (seed_dir, leech_dir),
    }
}

/// `GET path` with the bearer token on a raw connection; the connection
/// and the response head.
async fn raw_get(base: &str, path: &str) -> (TcpStream, String) {
    let addr = base.trim_start_matches("http://");
    let mut conn = TcpStream::connect(addr).await.expect("connect");
    let req =
        format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {TOKEN}\r\n\r\n");
    conn.write_all(req.as_bytes()).await.expect("write");
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    tokio::time::timeout(T, async {
        while !head.ends_with(b"\r\n\r\n") {
            let n = conn.read(&mut byte).await.expect("read");
            assert_eq!(n, 1, "connection closed before the head ended");
            head.push(byte[0]);
        }
    })
    .await
    .expect("head within the timeout");
    (conn, String::from_utf8(head).expect("utf-8 head"))
}

/// Waits up to `within` until `h`'s deadlines are empty (`empty`) or not.
async fn wait_deadlines(h: &TorrentHandle, empty: bool, within: Duration) -> bool {
    tokio::time::timeout(within, async {
        while h.debug_deadlines().await.is_empty() != empty {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .is_ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hang_up_releases_the_reader() {
    let s = stalled(|_| {}).await;
    let (conn, head) = raw_get(&s.base, &s.path).await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert!(
        wait_deadlines(&s.handle, false, Duration::from_secs(10)).await,
        "the open stream sets deadlines"
    );
    drop(conn);
    assert!(
        wait_deadlines(&s.handle, true, Duration::from_secs(2)).await,
        "the deadlines are cleared within 2 s of the hang-up"
    );
    s.session.shutdown().await;
    s.seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_cap_is_503() {
    let s = stalled(|c| c.max_streams = 1).await;
    let (_conn, head) = raw_get(&s.base, &s.path).await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    let (status, headers, body) = fetch(Method::GET, &format!("{}{}", s.base, s.path), &[]).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(headers["retry-after"], "1");
    assert_eq!(error_code(&body), "too_many_streams");
    // HEAD takes no permit.
    let (status, _, _) = fetch(Method::HEAD, &format!("{}{}", s.base, s.path), &[]).await;
    assert_eq!(status, StatusCode::OK);
    s.session.shutdown().await;
    s.seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remove_ends_open_streams() {
    let s = stalled(|_| {}).await;
    let req = request(Method::GET, &format!("{}{}", s.base, s.path), Some(TOKEN))
        .body(Full::new(Bytes::new()))
        .expect("request builds");
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let res = tokio::time::timeout(T, client.request(req))
        .await
        .expect("head within the timeout")
        .expect("request succeeds");
    assert_eq!(res.status(), StatusCode::OK);
    let len: usize = res.headers()["content-length"]
        .to_str()
        .expect("ascii")
        .parse()
        .expect("number");
    let body = tokio::spawn(res.into_body().collect());

    let (status, _, _) = send_body(
        Method::DELETE,
        &format!("{}/api/v1/torrents/{}", s.base, s.id),
        Some(TOKEN),
        None,
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let ended = tokio::time::timeout(Duration::from_secs(5), body)
        .await
        .expect("the body ends within 5 s of the removal")
        .expect("body task");
    if let Ok(collected) = ended {
        assert!(collected.to_bytes().len() < len, "the body ended early");
    }
    s.session.shutdown().await;
    s.seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_ends_open_sse_and_streams() {
    let s = stalled(|_| {}).await;
    let (_events, head) = raw_get(&s.base, "/api/v1/events").await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    let (_stream, head) = raw_get(&s.base, &s.path).await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    tokio::time::timeout(Duration::from_secs(6), s.server.shutdown())
        .await
        .expect("shutdown returns within 6 s");
    s.session.shutdown().await;
    s.seed.shutdown().await;
}

#[test]
fn stream_routes_are_documented() {
    let doc = serde_json::to_value(fbt_api::openapi()).expect("document");
    let path = &doc["paths"]["/api/v1/torrents/{id}/files/{index}/stream"];
    assert!(path["get"].is_object(), "{doc}");
    assert!(path["head"].is_object(), "{doc}");
    assert!(path["get"]["responses"]["206"].is_object());
}
