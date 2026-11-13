//! The torrent read endpoints.

mod support;

use std::ops::ControlFlow;
use std::time::Duration;

use fbt::Session;
use fbt::{AddTorrent, CreateTorrent, Magnet, Metainfo, TorrentHandle, TorrentState};
use fbt_api::ApiServer;
use support::{api, error_code, get, json};

/// A hybrid torrent of two files (one pad file between them comes from the
/// hybrid layout) seeded from its source dir, in a running API.
struct Fixture {
    session: Session,
    _server: ApiServer,
    base: String,
    handle: TorrentHandle,
    metainfo: Metainfo,
    _dir: tempfile::TempDir,
}

async fn fixture() -> Fixture {
    let (session, server, base, _) = api().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let src = dir.path().join("payload");
    std::fs::create_dir(&src).expect("mkdir");
    std::fs::write(src.join("a.bin"), vec![7u8; 40_000]).expect("write a");
    std::fs::write(src.join("b.bin"), vec![9u8; 25_000]).expect("write b");
    let bytes = CreateTorrent::new(&src)
        .piece_len(16384)
        .build(&mut |_, _| ControlFlow::Continue(()))
        .expect("create");
    let metainfo = Metainfo::from_bytes(&bytes).expect("parse");
    let handle = session
        .add_torrent(AddTorrent::new(metainfo.clone(), dir.path().to_path_buf()))
        .await
        .expect("add");
    tokio::time::timeout(Duration::from_secs(30), async {
        while handle.status().state != TorrentState::Seeding {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("seeding");
    Fixture {
        session,
        _server: server,
        base,
        handle,
        metainfo,
        _dir: dir,
    }
}

fn v1_hex(f: &Fixture) -> String {
    f.metainfo.info_hashes.v1.expect("v1").to_string()
}

#[tokio::test]
async fn lists_torrents() {
    let f = fixture().await;
    let (status, _, body) = get(&format!("{}/api/v1/torrents", f.base), Some(support::TOKEN)).await;
    assert_eq!(status, 200);
    let list = json(&body);
    let list = list.as_array().expect("array");
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["id"], v1_hex(&f));
    assert_eq!(list[0]["version"], "hybrid");
    assert_eq!(list[0]["state"], "seeding");
    assert_eq!(list[0]["progress"], 1.0);
    assert!(list[0]["info_hash_v2"].is_string());
    assert!(list[0]["checking_progress"].is_null());
    drop(f.session);
}

#[tokio::test]
async fn get_by_v1_and_v2_ids() {
    let f = fixture().await;
    let v1 = v1_hex(&f);
    let v2 = f.metainfo.info_hashes.v2.expect("v2").to_string();
    for id in [v1.clone(), v1.to_uppercase(), v2] {
        let (status, _, body) = get(
            &format!("{}/api/v1/torrents/{id}", f.base),
            Some(support::TOKEN),
        )
        .await;
        assert_eq!(status, 200, "{id}");
        assert_eq!(json(&body)["id"], v1);
    }
}

#[tokio::test]
async fn bad_ids() {
    let f = fixture().await;
    for (id, status, code) in [
        ("abc".to_owned(), 400, "invalid_id"),
        ("a".repeat(39), 400, "invalid_id"),
        ("0".repeat(40), 404, "not_found"),
    ] {
        let (s, _, body) = get(
            &format!("{}/api/v1/torrents/{id}", f.base),
            Some(support::TOKEN),
        )
        .await;
        assert_eq!(s, status, "{id}");
        assert_eq!(error_code(&body), code, "{id}");
    }
}

#[tokio::test]
async fn files_peers_trackers_shapes() {
    let f = fixture().await;
    let url = format!("{}/api/v1/torrents/{}", f.base, v1_hex(&f));
    let (status, _, body) = get(&format!("{url}/files"), Some(support::TOKEN)).await;
    assert_eq!(status, 200);
    let files = json(&body);
    let files = files.as_array().expect("array");
    assert_eq!(files.len(), f.metainfo.info.files.len());
    assert!(files.iter().any(|x| x["pad"] == true), "a pad file");
    for (file, meta) in files.iter().zip(&f.metainfo.info.files) {
        assert_eq!(file["priority"], "normal");
        assert_eq!(file["len"], meta.len);
        if !meta.pad {
            assert_eq!(file["downloaded"], meta.len);
        }
    }
    let (status, _, body) = get(&format!("{url}/peers"), Some(support::TOKEN)).await;
    assert_eq!(status, 200);
    assert_eq!(json(&body), serde_json::json!([]));
    let (status, _, body) = get(&format!("{url}/trackers"), Some(support::TOKEN)).await;
    assert_eq!(status, 200);
    assert_eq!(json(&body), serde_json::json!([]));
}

#[tokio::test]
async fn pieces_hex() {
    let f = fixture().await;
    let url = format!("{}/api/v1/torrents/{}/pieces", f.base, v1_hex(&f));
    let (status, _, body) = get(&url, Some(support::TOKEN)).await;
    assert_eq!(status, 200);
    let v = json(&body);
    let n = f.metainfo.piece_layout().num_pieces();
    assert_eq!(v["num_pieces"], n);
    let have = v["have"].as_str().expect("hex");
    let full_bytes = (n / 8) as usize;
    assert_eq!(have.len(), 2 * n.div_ceil(8) as usize);
    assert!(have[..2 * full_bytes].chars().all(|c| c == 'f'));
    if n % 8 != 0 {
        let last = u8::from_str_radix(&have[2 * full_bytes..], 16).expect("hex byte");
        assert_eq!(last, 0xffu8 << (8 - n % 8));
    }
}

#[tokio::test]
async fn resume_data_round_trips() {
    let f = fixture().await;
    let url = format!("{}/api/v1/torrents/{}/resume-data", f.base, v1_hex(&f));
    let (status, headers, body) = get(&url, Some(support::TOKEN)).await;
    assert_eq!(status, 200);
    assert_eq!(headers["content-type"], "application/octet-stream");
    assert!(AddTorrent::from_resume(&body).is_ok());
}

#[tokio::test]
async fn magnet_has_both_hashes() {
    let f = fixture().await;
    let url = format!("{}/api/v1/torrents/{}/magnet", f.base, v1_hex(&f));
    let (status, _, body) = get(&url, Some(support::TOKEN)).await;
    assert_eq!(status, 200);
    let uri = json(&body)["magnet"].as_str().expect("magnet").to_owned();
    assert!(uri.contains("xt=urn:btih:"), "{uri}");
    assert!(uri.contains("xt=urn:btmh:"), "{uri}");

    // A magnet-added torrent has no metadata: the link is built from its hashes.
    let hash = "11".repeat(20);
    let magnet: Magnet = format!("magnet:?xt=urn:btih:{hash}")
        .parse()
        .expect("magnet parses");
    let dir = tempfile::tempdir().expect("tempdir");
    f.session
        .add_torrent(AddTorrent::magnet(magnet, dir.path().to_path_buf()))
        .await
        .expect("add magnet");
    let url = format!("{}/api/v1/torrents/{hash}/magnet", f.base);
    let (status, _, body) = get(&url, Some(support::TOKEN)).await;
    assert_eq!(status, 200);
    let uri = json(&body)["magnet"].as_str().expect("magnet").to_owned();
    assert!(uri.contains(&format!("urn:btih:{hash}")), "{uri}");
    let _ = &f.handle;
}
