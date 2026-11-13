//! The session settings and torrent creation endpoints.

mod support;

use fbt::{Metainfo, SessionState, TorrentVersion};
use hyper::{Method, StatusCode};
use serde_json::{Value, json as j};
use support::{TOKEN, api, api_with, error_code, get, json, send_json};

const TORRENT: &str = "application/x-bittorrent";

#[tokio::test]
async fn session_limits_apply() {
    let (session, server, base, _) = api().await;
    let (status, _, body) = send_json(
        Method::PUT,
        &format!("{base}/api/v1/session/limits"),
        Some(TOKEN),
        &j!({"download": 1_000_000, "upload": 250_000}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body:?}");
    let limit = session.rate_limit();
    assert_eq!((limit.download, limit.upload), (1_000_000, 250_000));

    let (status, _, body) = get(&format!("{base}/api/v1/session"), Some(TOKEN)).await;
    assert_eq!(status, StatusCode::OK);
    let info = json(&body);
    assert_eq!(info["download_limit"], 1_000_000);
    assert_eq!(info["upload_limit"], 250_000);

    let (status, _, body) = send_json(
        Method::PUT,
        &format!("{base}/api/v1/session/limits"),
        Some(TOKEN),
        &j!({"download": 1, "bogus": 2}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "invalid_request");
    server.shutdown().await;
}

#[tokio::test]
async fn queue_round_trip() {
    let (session, server, base, _) = api().await;
    let url = format!("{base}/api/v1/session/queue");
    let queue = j!({
        "active_downloads": 7,
        "active_seeds": null,
        "active_limit": 40,
        "dont_count_slow_torrents": false,
        "inactive_down_rate": 100,
        "inactive_up_rate": 200,
        "startup_grace": 12.5,
        "interval": 9.0,
    });
    let (status, _, body) = send_json(Method::PUT, &url, Some(TOKEN), &queue).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body:?}");
    let (status, _, body) = get(&url, Some(TOKEN)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&body), queue);
    assert_eq!(session.queue_settings().active_downloads, Some(7));

    let mut bad = queue.clone();
    bad["interval"] = j!(0.0);
    let (status, _, body) = send_json(Method::PUT, &url, Some(TOKEN), &bad).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "invalid_request");
    bad["interval"] = j!(-1.0);
    let (status, _, _) = send_json(Method::PUT, &url, Some(TOKEN), &bad).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    server.shutdown().await;
}

#[tokio::test]
async fn ip_filter_round_trip() {
    let (session, server, base, _) = api().await;
    let url = format!("{base}/api/v1/session/ip-filter");
    let rules = j!({"rules": [
        {"first": "10.0.0.0", "last": "10.0.0.255", "action": "block"},
        {"first": "10.0.0.7", "last": "10.0.0.7", "action": "allow"},
    ]});
    let (status, _, body) = send_json(Method::PUT, &url, Some(TOKEN), &rules).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body:?}");
    let filter = session.ip_filter();
    assert!(filter.is_blocked("10.0.0.9".parse().unwrap()));
    assert!(!filter.is_blocked("10.0.0.7".parse().unwrap()));
    assert!(!filter.is_blocked("10.0.1.1".parse().unwrap()));

    let (status, _, body) = get(&url, Some(TOKEN)).await;
    assert_eq!(status, StatusCode::OK);
    let got = json(&body);
    let blocks: Vec<&Value> = got["rules"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["action"] == "block")
        .collect();
    assert_eq!(blocks.len(), 2, "{got}");
    assert_eq!(blocks[0]["first"], "10.0.0.0");
    assert_eq!(blocks[0]["last"], "10.0.0.6");
    assert_eq!(blocks[1]["first"], "10.0.0.8");
    assert_eq!(blocks[1]["last"], "10.0.0.255");

    // A bad address, a reversed range and mixed families are refused.
    for rule in [
        j!({"first": "nope", "last": "10.0.0.1", "action": "block"}),
        j!({"first": "10.0.0.9", "last": "10.0.0.1", "action": "block"}),
        j!({"first": "10.0.0.1", "last": "::1", "action": "block"}),
        j!({"first": "10.0.0.1", "last": "10.0.0.2", "action": "drop"}),
    ] {
        let (status, _, body) =
            send_json(Method::PUT, &url, Some(TOKEN), &j!({"rules": [rule]})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{rule}");
        assert_eq!(error_code(&body), "invalid_request");
    }
    server.shutdown().await;
}

#[tokio::test]
async fn ip_filter_cap() {
    let (_session, server, base, _) = api().await;
    let url = format!("{base}/api/v1/session/ip-filter");
    let rule = j!({"first": "10.0.0.1", "last": "10.0.0.1", "action": "block"});
    let rules: Vec<&Value> = std::iter::repeat_n(&rule, 65_537).collect();
    let (status, _, body) =
        send_json(Method::PUT, &url, Some(TOKEN), &j!({ "rules": rules })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "invalid_request");
    server.shutdown().await;
}

#[tokio::test]
async fn banned_peers_lists_empty() {
    let (_session, server, base, _) = api().await;
    let (status, _, body) = get(&format!("{base}/api/v1/session/banned-peers"), Some(TOKEN)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&body), j!([]));
    server.shutdown().await;
}

fn two_files() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("pack");
    std::fs::create_dir(&root).expect("mkdir");
    std::fs::write(root.join("a.bin"), vec![1u8; 20_000]).expect("write");
    std::fs::write(root.join("b.bin"), vec![2u8; 30_000]).expect("write");
    dir
}

#[tokio::test]
async fn create_torrent_endpoint() {
    let (_session, server, base, _) = api().await;
    let dir = two_files();
    let path = dir.path().join("pack").to_str().unwrap().to_owned();
    let (status, headers, body) = send_json(
        Method::POST,
        &format!("{base}/api/v1/create-torrent"),
        Some(TOKEN),
        &j!({"path": path, "trackers": [["http://127.0.0.1:1/announce"]], "comment": "hi"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(headers["content-type"], TORRENT);
    let meta = Metainfo::from_bytes(&body).expect("parses");
    assert_eq!(meta.version(), TorrentVersion::Hybrid);

    let (status, headers, body) = send_json(
        Method::POST,
        &format!("{base}/api/v1/create-torrent"),
        Some(TOKEN),
        &j!({"path": path, "version": "v1", "piece_length": 16384, "private": true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(headers["content-type"], TORRENT);
    let meta = Metainfo::from_bytes(&body).expect("parses");
    assert_eq!(meta.version(), TorrentVersion::V1);

    let (status, _, body) = send_json(
        Method::POST,
        &format!("{base}/api/v1/create-torrent"),
        Some(TOKEN),
        &j!({"path": path, "piece_length": 1000}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "invalid_request");

    let missing = dir.path().join("missing").to_str().unwrap().to_owned();
    let (status, _, body) = send_json(
        Method::POST,
        &format!("{base}/api/v1/create-torrent"),
        Some(TOKEN),
        &j!({"path": missing}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body:?}");
    server.shutdown().await;
}

#[tokio::test]
async fn create_counts_empty_tiers_against_the_url_cap() {
    let (_session, server, base, _) = api().await;
    let dir = two_files();
    let path = dir.path().join("pack").to_str().unwrap().to_owned();
    let url = format!("{base}/api/v1/create-torrent");
    let empty_tiers: Vec<Vec<String>> = vec![Vec::new(); 257];
    let one_tier_of_blanks = vec![vec![String::new(); 257]];
    for trackers in [empty_tiers, one_tier_of_blanks] {
        let (status, _, body) = send_json(
            Method::POST,
            &url,
            Some(TOKEN),
            &j!({"path": path, "trackers": trackers}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
        assert_eq!(error_code(&body), "invalid_request");
    }
    // 200 empty tiers and 57 web seeds: one past the cap together.
    let (status, _, body) = send_json(
        Method::POST,
        &url,
        Some(TOKEN),
        &j!({
            "path": path,
            "trackers": vec![Vec::<String>::new(); 200],
            "web_seeds": vec!["http://127.0.0.1:1/"; 57],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    assert_eq!(error_code(&body), "invalid_request");
    server.shutdown().await;
}

#[tokio::test]
async fn create_outside_roots_is_403() {
    let allowed = tempfile::tempdir().expect("tempdir");
    let allowed_path = allowed.path().to_path_buf();
    let (_session, server, base, _) =
        api_with(move |c| c.allowed_roots = Some(vec![allowed_path])).await;
    let dir = two_files();
    let (status, _, body) = send_json(
        Method::POST,
        &format!("{base}/api/v1/create-torrent"),
        Some(TOKEN),
        &j!({"path": dir.path().join("pack").to_str().unwrap()}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error_code(&body), "path_not_allowed");
    server.shutdown().await;
}

#[tokio::test]
async fn state_bytes_load() {
    let (_session, server, base, _) = api().await;
    let (status, headers, body) = get(&format!("{base}/api/v1/session/state"), Some(TOKEN)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-type"], "application/octet-stream");
    SessionState::from_bytes(&body).expect("state parses");
    server.shutdown().await;
}
