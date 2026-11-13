//! The torrent control endpoints and the `allowed_roots` check.

mod support;

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use bytes::Bytes;
use fbt::{AddTorrent, CreateTorrent, Metainfo, RemoveMode};
use fbt_api::ApiServer;
use hyper::{Method, StatusCode};
use serde_json::{Value, json as j};
use support::{TOKEN, api, api_with, error_code, get, json, query_escape, send_body, send_json};

const TORRENT: &str = "application/x-bittorrent";
const OCTETS: &str = "application/octet-stream";

/// A single-file torrent of `a.bin` (40 000 bytes) whose data sits in
/// `data`, so a torrent added with `data` as its save path seeds.
struct Payload {
    bytes: Vec<u8>,
    metainfo: Metainfo,
    data: tempfile::TempDir,
}

fn payload() -> Payload {
    let data = tempfile::tempdir().expect("tempdir");
    let file = data.path().join("a.bin");
    std::fs::write(&file, vec![7u8; 40_000]).expect("write");
    let bytes = CreateTorrent::new(&file)
        .piece_len(16384)
        .build(&mut |_, _| ControlFlow::Continue(()))
        .expect("create");
    let metainfo = Metainfo::from_bytes(&bytes).expect("parse");
    Payload {
        bytes,
        metainfo,
        data,
    }
}

fn id(p: &Payload) -> String {
    p.metainfo.info_hashes.v1.expect("v1").to_string()
}

fn path_str(p: &Path) -> String {
    p.to_str().expect("utf-8 path").to_owned()
}

/// `POST /torrents` with the `.torrent` bytes and `query`.
async fn add_file(base: &str, bytes: &[u8], query: &str) -> (StatusCode, Bytes) {
    let (status, _, body) = send_body(
        Method::POST,
        &format!("{base}/api/v1/torrents?{query}"),
        Some(TOKEN),
        Some(TORRENT),
        bytes.to_vec(),
    )
    .await;
    (status, body)
}

/// Adds `p` with its data dir as the save path and waits until it seeds.
async fn add_seeding(base: &str, p: &Payload) -> String {
    let query = format!("save_path={}", query_escape(&path_str(p.data.path())));
    let (status, body) = add_file(base, &p.bytes, &query).await;
    assert_eq!(status, StatusCode::CREATED, "{body:?}");
    let id = id(p);
    wait_for(base, &id, |t| t["state"] == "seeding").await;
    id
}

/// Polls `GET /torrents/{id}` every 50 ms for up to 5 s until `pred`
/// holds; returns that snapshot.
async fn wait_for(base: &str, id: &str, pred: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (status, _, body) = get(&format!("{base}/api/v1/torrents/{id}"), Some(TOKEN)).await;
        assert_eq!(status, StatusCode::OK);
        let t = json(&body);
        if pred(&t) {
            return t;
        }
        assert!(Instant::now() < deadline, "timed out; last status {t}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Polls `GET /torrents/{id}/files` likewise until `pred` holds.
async fn wait_for_files(base: &str, id: &str, pred: impl Fn(&Value) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (status, _, body) =
            get(&format!("{base}/api/v1/torrents/{id}/files"), Some(TOKEN)).await;
        assert_eq!(status, StatusCode::OK);
        let files = json(&body);
        if pred(&files) {
            return;
        }
        assert!(Instant::now() < deadline, "timed out; last files {files}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn post_empty(url: &str) -> StatusCode {
    send_body(Method::POST, url, Some(TOKEN), None, Bytes::new())
        .await
        .0
}

#[tokio::test]
async fn add_magnet_then_remove() {
    let (_session, _server, base, _) = api().await;
    let p = payload();
    let save = tempfile::tempdir().expect("tempdir");
    let req = j!({
        "magnet": p.metainfo.magnet().to_uri(),
        "save_path": path_str(save.path()),
    });
    let url = format!("{base}/api/v1/torrents");
    let (status, _, body) = send_json(Method::POST, &url, Some(TOKEN), &req).await;
    assert_eq!(status, StatusCode::CREATED, "{body:?}");
    let t = json(&body);
    assert_eq!(t["state"], "downloading_metadata");
    assert_eq!(t["id"], id(&p));

    let (status, _, body) = send_json(Method::POST, &url, Some(TOKEN), &req).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error_code(&body), "duplicate_torrent");

    let one = format!("{base}/api/v1/torrents/{}", id(&p));
    let (status, _, _) = send_body(
        Method::DELETE,
        &format!("{one}?delete_files=false"),
        Some(TOKEN),
        None,
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _, body) = get(&one, Some(TOKEN)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(error_code(&body), "not_found");
}

#[tokio::test]
async fn add_torrent_file() {
    let (_session, _server, base, _) = api().await;
    let p = payload();
    let query = format!(
        "save_path={}&paused=true",
        query_escape(&path_str(p.data.path()))
    );
    let (status, body) = add_file(&base, &p.bytes, &query).await;
    assert_eq!(status, StatusCode::CREATED, "{body:?}");
    assert_eq!(json(&body)["id"], id(&p));
    wait_for(&base, &id(&p), |t| t["state"] == "paused").await;
}

#[tokio::test]
async fn pause_resume_recheck() {
    let (_session, _server, base, _) = api().await;
    let p = payload();
    let id = add_seeding(&base, &p).await;
    let one = format!("{base}/api/v1/torrents/{id}");

    assert_eq!(
        post_empty(&format!("{one}/pause")).await,
        StatusCode::NO_CONTENT
    );
    wait_for(&base, &id, |t| t["state"] == "paused").await;
    assert_eq!(
        post_empty(&format!("{one}/resume")).await,
        StatusCode::NO_CONTENT
    );
    wait_for(&base, &id, |t| t["state"] != "paused").await;
    assert_eq!(
        post_empty(&format!("{one}/recheck")).await,
        StatusCode::NO_CONTENT
    );
    wait_for(&base, &id, |t| t["state"] == "seeding").await;
    for action in ["reannounce", "scrape"] {
        assert_eq!(
            post_empty(&format!("{one}/{action}")).await,
            StatusCode::NO_CONTENT,
            "{action}"
        );
    }
    let unknown = format!("{base}/api/v1/torrents/{}/pause", "0".repeat(40));
    assert_eq!(post_empty(&unknown).await, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn wrong_priority_count_is_400() {
    let (_session, _server, base, _) = api().await;
    let p = payload();
    let id = add_seeding(&base, &p).await;
    let url = format!("{base}/api/v1/torrents/{id}/file-priorities");

    let (status, _, body) = send_json(
        Method::PUT,
        &url,
        Some(TOKEN),
        &j!({"priorities": ["normal", "high"]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "invalid_priorities");

    // `unknown` is a response-only value.
    let (status, _, body) = send_json(
        Method::PUT,
        &url,
        Some(TOKEN),
        &j!({"priorities": ["unknown"]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "invalid_request");

    let (status, _, _) = send_json(
        Method::PUT,
        &url,
        Some(TOKEN),
        &j!({"priorities": ["high"]}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    wait_for_files(&base, &id, |f| f[0]["priority"] == "high").await;
}

#[tokio::test]
async fn limits_and_queue() {
    let (_session, _server, base, _) = api().await;
    let p = payload();
    let id = add_seeding(&base, &p).await;
    let one = format!("{base}/api/v1/torrents/{id}");

    let (status, _, _) = send_json(
        Method::PUT,
        &format!("{one}/limits"),
        Some(TOKEN),
        &j!({"download": 1000, "upload": null}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _, _) = send_json(
        Method::PUT,
        &format!("{one}/queue-position"),
        Some(TOKEN),
        &j!({"position": 0}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _, _) = send_json(
        Method::PUT,
        &format!("{one}/auto-managed"),
        Some(TOKEN),
        &j!({"auto_managed": false}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    wait_for(&base, &id, |t| t["auto_managed"] == false).await;
}

#[tokio::test]
async fn move_and_rename() {
    let (_session, _server, base, _) = api().await;
    let p = payload();
    let id = add_seeding(&base, &p).await;
    let one = format!("{base}/api/v1/torrents/{id}");

    let target = tempfile::tempdir().expect("tempdir");
    let target = path_str(target.path());
    let (status, _, body) = send_json(
        Method::POST,
        &format!("{one}/move"),
        Some(TOKEN),
        &j!({"path": target, "mode": "fail_if_exists"}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body:?}");
    wait_for(&base, &id, |t| t["save_path"] == target.as_str()).await;

    let (status, _, body) = send_json(
        Method::POST,
        &format!("{one}/rename-file"),
        Some(TOKEN),
        &j!({"index": 0, "path": "renamed.bin"}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body:?}");
    wait_for_files(&base, &id, |f| f[0]["path"] == "renamed.bin").await;
    assert!(Path::new(&target).join("renamed.bin").is_file());
}

#[tokio::test]
async fn rename_traversal_is_400() {
    let (_session, _server, base, _) = api().await;
    let p = payload();
    let id = add_seeding(&base, &p).await;
    let (status, _, body) = send_json(
        Method::POST,
        &format!("{base}/api/v1/torrents/{id}/rename-file"),
        Some(TOKEN),
        &j!({"index": 0, "path": "../x"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "invalid_path");
}

#[tokio::test]
async fn web_seeds_and_peers() {
    let (session, _server, base, _) = api().await;
    let p = payload();
    let id = add_seeding(&base, &p).await;
    let one = format!("{base}/api/v1/torrents/{id}");
    let seed = "http://127.0.0.1:9/files/";

    let (status, _, _) = send_json(
        Method::POST,
        &format!("{one}/web-seeds"),
        Some(TOKEN),
        &j!({"url": seed}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let handle = session.torrents().into_iter().next().expect("torrent");
    assert_eq!(handle.web_seeds().await.expect("seeds"), vec![seed]);
    let (status, _, body) = send_json(
        Method::POST,
        &format!("{one}/web-seeds"),
        Some(TOKEN),
        &j!({"url": "ftp://x/"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "invalid_request");
    let (status, _, _) = send_body(
        Method::DELETE,
        &format!("{one}/web-seeds?url={}", query_escape(seed)),
        Some(TOKEN),
        None,
        Bytes::new(),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(handle.web_seeds().await.expect("seeds").is_empty());

    let (status, _, _) = send_json(
        Method::POST,
        &format!("{one}/peers"),
        Some(TOKEN),
        &j!({"addr": "127.0.0.1:9"}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _, body) = send_json(
        Method::POST,
        &format!("{one}/peers"),
        Some(TOKEN),
        &j!({"addr": "not an address"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "invalid_request");
}

/// Two dirs, with only the first allowed.
async fn rooted() -> (ApiServer, String, tempfile::TempDir, tempfile::TempDir) {
    let dir_a = tempfile::tempdir().expect("tempdir");
    let dir_b = tempfile::tempdir().expect("tempdir");
    let root = dir_a.path().to_path_buf();
    let (_session, server, base, _) = api_with(|c| c.allowed_roots = Some(vec![root])).await;
    (server, base, dir_a, dir_b)
}

#[tokio::test]
async fn save_path_outside_roots_is_403() {
    let (_server, base, dir_a, dir_b) = rooted().await;
    let p = payload();
    let url = format!("{base}/api/v1/torrents");

    // A magnet and a `.torrent` saved in `dir_b`.
    let (status, _, body) = send_json(
        Method::POST,
        &url,
        Some(TOKEN),
        &j!({"magnet": p.metainfo.magnet().to_uri(), "save_path": path_str(dir_b.path())}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error_code(&body), "path_not_allowed");
    let query = format!("save_path={}", query_escape(&path_str(dir_b.path())));
    let (status, body) = add_file(&base, &p.bytes, &query).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error_code(&body), "path_not_allowed");
    // `..` out of the root, and a relative path.
    for bad in [
        dir_a
            .path()
            .join("..")
            .join(dir_b.path().file_name().expect("name")),
        PathBuf::from("relative"),
    ] {
        let query = format!("save_path={}", query_escape(&path_str(&bad)));
        let (status, body) = add_file(&base, &p.bytes, &query).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{}", bad.display());
        assert_eq!(error_code(&body), "path_not_allowed");
    }

    // Inside the root (a dir that does not exist yet) is fine.
    let inside = dir_a.path().join("new").join("dir");
    let query = format!("save_path={}", query_escape(&path_str(&inside)));
    let (status, body) = add_file(&base, &p.bytes, &query).await;
    assert_eq!(status, StatusCode::CREATED, "{body:?}");
    let move_url = format!("{base}/api/v1/torrents/{}/move", id(&p));

    let escape = dir_a
        .path()
        .join("..")
        .join(dir_b.path().file_name().expect("name"));
    let (status, _, body) = send_json(
        Method::POST,
        &move_url,
        Some(TOKEN),
        &j!({"path": path_str(&escape), "mode": "fail_if_exists"}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error_code(&body), "path_not_allowed");

    #[cfg(unix)]
    {
        let link = dir_a.path().join("link");
        std::os::unix::fs::symlink(dir_b.path(), &link).expect("symlink");
        for target in [link.clone(), link.join("below")] {
            let (status, _, body) = send_json(
                Method::POST,
                &move_url,
                Some(TOKEN),
                &j!({"path": path_str(&target), "mode": "fail_if_exists"}),
            )
            .await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{}", target.display());
            assert_eq!(error_code(&body), "path_not_allowed");
        }
        // A dangling link cannot be resolved: refused too.
        let dangling = dir_a.path().join("dangling");
        std::os::unix::fs::symlink(dir_b.path().join("missing"), &dangling).expect("symlink");
        let (status, _, body) = send_json(
            Method::POST,
            &move_url,
            Some(TOKEN),
            &j!({"path": path_str(&dangling.join("x")), "mode": "fail_if_exists"}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(error_code(&body), "path_not_allowed");
    }
}

#[tokio::test]
async fn resume_outside_roots_is_403() {
    let dir_a = tempfile::tempdir().expect("tempdir");
    let dir_b = tempfile::tempdir().expect("tempdir");
    let root = dir_a.path().to_path_buf();
    let (session, _server, base, _) = api_with(|c| c.allowed_roots = Some(vec![root])).await;

    // Resume data saved by a torrent in `dir_b`, added past the API.
    let p = payload();
    let handle = session
        .add_torrent(AddTorrent::new(
            p.metainfo.clone(),
            dir_b.path().to_path_buf(),
        ))
        .await
        .expect("add");
    let resume = handle.save_resume_data().await.expect("resume data");
    handle.remove(RemoveMode::KeepData).await.expect("remove");

    let url = format!("{base}/api/v1/torrents/resume");
    let (status, _, body) = send_body(
        Method::POST,
        &url,
        Some(TOKEN),
        Some(OCTETS),
        resume.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error_code(&body), "path_not_allowed");

    let inside = dir_a.path().join("restored");
    let (status, _, body) = send_body(
        Method::POST,
        &format!("{url}?save_path={}", query_escape(&path_str(&inside))),
        Some(TOKEN),
        Some(OCTETS),
        resume,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body:?}");
    assert_eq!(json(&body)["id"], id(&p));
}

#[tokio::test]
async fn body_too_large_is_413() {
    let (_session, _server, base, _) = api_with(|c| c.max_body_bytes = 1024).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let query = format!("save_path={}", query_escape(&path_str(dir.path())));
    let (status, body) = add_file(&base, &[0u8; 2048], &query).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(error_code(&body), "body_too_large");
}

#[tokio::test]
async fn upload_over_2_mib_is_accepted() {
    let (_session, _server, base, _) = api().await;
    let (status, _, body) = send_body(
        Method::POST,
        &format!("{base}/api/v1/torrents/resume"),
        Some(TOKEN),
        Some(OCTETS),
        vec![b'x'; 3 << 20],
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "invalid_request");
}

#[tokio::test]
async fn malformed_json_is_400() {
    let (_session, _server, base, _) = api().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let url = format!("{base}/api/v1/torrents");
    let save = path_str(dir.path());
    let magnet = format!("magnet:?xt=urn:btih:{}", "22".repeat(20));
    for body in [
        "{".to_owned(),
        j!({"magnet": magnet, "save_path": save, "extra": 1}).to_string(),
        j!({"magnet": magnet, "save_path": save, "paused": "yes"}).to_string(),
        j!({"magnet": "not a magnet", "save_path": save}).to_string(),
    ] {
        let (status, _, res) = send_body(
            Method::POST,
            &url,
            Some(TOKEN),
            Some("application/json"),
            body.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(error_code(&res), "invalid_request", "{body}");
    }
    // A JSON add takes no query string.
    let (status, _, res) = send_json(
        Method::POST,
        &format!("{url}?paused=true"),
        Some(TOKEN),
        &j!({"magnet": magnet, "save_path": save}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&res), "invalid_request");
    // A bad `.torrent` and a bad query string.
    let query = format!("save_path={}", query_escape(&save));
    let (status, res) = add_file(&base, b"not bencode", &query).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&res), "invalid_request");
    let (status, res) = add_file(&base, b"d", &format!("{query}&paused=maybe")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&res), "invalid_request");
}

#[test]
fn control_routes_are_documented() {
    let doc = serde_json::to_value(fbt_api::openapi()).expect("document");
    let paths = &doc["paths"];
    for (path, methods) in [
        ("/api/v1/torrents", &["get", "post"][..]),
        ("/api/v1/torrents/resume", &["post"]),
        ("/api/v1/torrents/{id}", &["get", "delete"]),
        ("/api/v1/torrents/{id}/pause", &["post"]),
        ("/api/v1/torrents/{id}/resume", &["post"]),
        ("/api/v1/torrents/{id}/recheck", &["post"]),
        ("/api/v1/torrents/{id}/reannounce", &["post"]),
        ("/api/v1/torrents/{id}/scrape", &["post"]),
        ("/api/v1/torrents/{id}/file-priorities", &["put"]),
        ("/api/v1/torrents/{id}/limits", &["put"]),
        ("/api/v1/torrents/{id}/queue-position", &["put"]),
        ("/api/v1/torrents/{id}/auto-managed", &["put"]),
        ("/api/v1/torrents/{id}/move", &["post"]),
        ("/api/v1/torrents/{id}/rename-file", &["post"]),
        ("/api/v1/torrents/{id}/web-seeds", &["post", "delete"]),
        ("/api/v1/torrents/{id}/peers", &["get", "post"]),
    ] {
        for method in methods {
            assert!(paths[path][method].is_object(), "{method} {path}");
        }
    }
    let add = &paths["/api/v1/torrents"]["post"]["requestBody"]["content"];
    assert!(add["application/json"].is_object());
    assert!(add["application/x-bittorrent"].is_object());
}
