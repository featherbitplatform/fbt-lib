//! `GET /api/v1/events`: the server-sent event stream.

mod support;

use std::ops::ControlFlow;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use fbt::{CreateTorrent, Metainfo};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{CONTENT_TYPE, HeaderMap};
use hyper::{Method, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::Value;
use support::{T, TOKEN, api, api_with, error_code, get, query_escape, request, send_body};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// An open event stream read message by message.
struct Sse {
    body: Incoming,
    buf: BytesMut,
}

impl Sse {
    /// `GET url` with the bearer token when one is given; the stream when
    /// the status is 200.
    async fn open(url: &str, token: Option<&str>) -> (StatusCode, HeaderMap, Option<Sse>) {
        let req = request(Method::GET, url, token)
            .body(Full::new(Bytes::new()))
            .expect("request builds");
        let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
        let res = tokio::time::timeout(T, client.request(req))
            .await
            .expect("response head within the timeout")
            .expect("request succeeds");
        let (parts, body) = res.into_parts();
        let sse = (parts.status == StatusCode::OK).then(|| Sse {
            body,
            buf: BytesMut::new(),
        });
        (parts.status, parts.headers, sse)
    }

    /// The next message with an `event:` field, as `(event, data)`; `None`
    /// once the body ends. Comments (keep-alives) are skipped.
    async fn next(&mut self) -> Option<(String, Value)> {
        loop {
            if let Some(end) = self.buf.windows(2).position(|w| w == b"\n\n") {
                let msg = self.buf.split_to(end + 2);
                let text = std::str::from_utf8(&msg).expect("utf-8 message");
                let mut event = None;
                let mut data = String::new();
                for line in text.lines() {
                    if let Some(v) = line.strip_prefix("event:") {
                        event = Some(v.trim_start().to_owned());
                    } else if let Some(v) = line.strip_prefix("data:") {
                        data.push_str(v.trim_start());
                    }
                }
                if let Some(event) = event {
                    let data = serde_json::from_str(&data).expect("data is JSON");
                    return Some((event, data));
                }
                continue;
            }
            match self.body.frame().await {
                Some(Ok(frame)) => {
                    if let Ok(chunk) = frame.into_data() {
                        self.buf.extend_from_slice(&chunk);
                    }
                }
                Some(Err(_)) | None => return None,
            }
        }
    }
}

/// A single-file torrent whose data sits in `data`, so it seeds when added
/// with `data` as its save path.
struct Payload {
    bytes: Vec<u8>,
    id: String,
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
    let id = metainfo.info_hashes.v1.expect("v1").to_string();
    Payload { bytes, id, data }
}

/// Adds `p` through the API with its data dir as the save path.
async fn add(base: &str, p: &Payload) {
    let save = p.data.path().to_str().expect("utf-8 path");
    let (status, _, body) = send_body(
        Method::POST,
        &format!("{base}/api/v1/torrents?save_path={}", query_escape(save)),
        Some(TOKEN),
        Some("application/x-bittorrent"),
        p.bytes.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body:?}");
}

#[tokio::test]
async fn events_report_add_and_finish() {
    let (_session, _server, base, token) = api().await;
    let (status, headers, sse) = Sse::open(&format!("{base}/api/v1/events"), Some(token)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[CONTENT_TYPE], "text/event-stream");
    assert_eq!(headers["x-accel-buffering"], "no");
    let mut sse = sse.expect("stream");
    let p = payload();
    add(&base, &p).await;
    let done = tokio::time::timeout(Duration::from_secs(10), async {
        let mut added = false;
        while let Some((event, data)) = sse.next().await {
            assert_eq!(data["type"], event.as_str(), "event: equals type");
            if data["torrent"] != p.id.as_str() {
                continue;
            }
            match event.as_str() {
                "torrent_added" => added = true,
                "torrent_finished" => return added,
                "state_changed" if data["state"] == "seeding" => return added,
                _ => {}
            }
        }
        false
    })
    .await
    .expect("events within 10 s");
    assert!(done, "torrent_added, then finished or seeding");
}

#[tokio::test]
async fn categories_filter() {
    let (_session, _server, base, token) = api().await;
    let url = format!("{base}/api/v1/events?categories=tracker");
    let (status, _, sse) = Sse::open(&url, Some(token)).await;
    assert_eq!(status, StatusCode::OK);
    let mut sse = sse.expect("stream");
    let p = payload();
    add(&base, &p).await;
    let _ = tokio::time::timeout(Duration::from_secs(1), async {
        while let Some((event, _)) = sse.next().await {
            assert_ne!(event, "torrent_added", "status events are filtered out");
        }
    })
    .await;
}

#[tokio::test]
async fn bad_category_is_400() {
    let (_session, _server, base, token) = api().await;
    for query in ["categories=bogus", "categories=status,bogus", "categories="] {
        let (status, _, body) = get(&format!("{base}/api/v1/events?{query}"), Some(token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
        assert_eq!(error_code(&body), "invalid_request", "{query}");
    }
    // Every category name is known.
    let url = format!("{base}/api/v1/events?categories=status,piece,tracker,peer,dht,storage,nat");
    let (status, _, _) = Sse::open(&url, Some(token)).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn token_query_on_events() {
    let (_session, _server, base, token) = api().await;
    let (status, _, _) = Sse::open(&format!("{base}/api/v1/events?token={token}"), None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = Sse::open(&format!("{base}/api/v1/events?token=wrong"), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn event_stream_cap_is_503() {
    let (_session, _server, base, token) = api_with(|c| c.max_event_streams = 2).await;
    let url = format!("{base}/api/v1/events");
    let (s1, _, _a) = Sse::open(&url, Some(token)).await;
    let (s2, _, _b) = Sse::open(&url, Some(token)).await;
    assert_eq!((s1, s2), (StatusCode::OK, StatusCode::OK));
    let (status, headers, body) = get(&url, Some(token)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error_code(&body), "too_many_streams");
    assert_eq!(headers["retry-after"], "1");
}

/// Opens an event stream on a raw connection and reads the response head;
/// returns the connection and the head.
async fn raw_open(base: &str) -> (TcpStream, String) {
    let addr = base.trim_start_matches("http://");
    let mut conn = TcpStream::connect(addr).await.expect("connect");
    let req = format!(
        "GET /api/v1/events HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {TOKEN}\r\n\r\n"
    );
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

#[tokio::test]
async fn disconnect_frees_the_slot() {
    let (_session, _server, base, token) = api_with(|c| c.max_event_streams = 2).await;
    let (a, head_a) = raw_open(&base).await;
    let (_b, head_b) = raw_open(&base).await;
    assert!(head_a.starts_with("HTTP/1.1 200"), "{head_a}");
    assert!(head_b.starts_with("HTTP/1.1 200"), "{head_b}");
    let url = format!("{base}/api/v1/events");
    let (status, _, _) = get(&url, Some(token)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    drop(a);
    let freed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (status, _, sse) = Sse::open(&url, Some(token)).await;
            if status == StatusCode::OK {
                return sse;
            }
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(freed.is_ok(), "the slot is free within 5 s");
}

/// An event stream whose client read only the response head ends on its own:
/// hyper stops polling the body, the idle timeout ends it (the 15 s keep-alive
/// is longer than the 1 s timeout here), and the permit comes free while the
/// stalled client stays connected.
#[tokio::test]
async fn stalled_sse_is_ended() {
    let (_session, _server, base, token) = api_with(|c| {
        c.stream_idle_timeout = Duration::from_secs(1);
        c.max_event_streams = 1;
    })
    .await;
    let (_conn, head) = raw_open(&base).await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");

    let url = format!("{base}/api/v1/events");
    let opened = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (status, _, sse) = Sse::open(&url, Some(token)).await;
            match status {
                StatusCode::OK => return sse.expect("the second stream opens"),
                StatusCode::SERVICE_UNAVAILABLE => {
                    tokio::time::sleep(Duration::from_millis(50)).await
                }
                other => panic!("unexpected status {other}"),
            }
        }
    })
    .await;
    let mut second = match opened {
        Ok(sse) => sse,
        Err(_) => panic!("the stalled stream's slot is not free within 5 s"),
    };

    // The freed slot serves a live stream: an add reaches it.
    let p = payload();
    add(&base, &p).await;
    let first = tokio::time::timeout(T, second.next())
        .await
        .expect("an event on the new stream");
    assert!(first.is_some(), "the new stream carries events");
}

#[tokio::test]
async fn ends_on_session_shutdown() {
    let (session, _server, base, token) = api().await;
    let (status, _, sse) = Sse::open(&format!("{base}/api/v1/events"), Some(token)).await;
    assert_eq!(status, StatusCode::OK);
    let mut sse = sse.expect("stream");
    session.shutdown().await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while sse.next().await.is_some() {}
    })
    .await
    .expect("the body ends within 5 s of session shutdown");
}

/// The future `router_with_shutdown` waits on ends an open event stream.
///
/// Review note (0.8 deflake; most likely cause of the 0.7 flake, which did
/// not reproduce here — 100 runs at 4 copies and 100 more at 72 copies with
/// CPU load pass): this test held the shortest timing budget in the suite —
/// 2 s for a body hyper ends only when its task is next polled, on the
/// single-threaded runtime `#[tokio::test]` gives it. A loaded runner can
/// delay that poll past two seconds, and because the stream was never shown to
/// carry anything, a slow start looked exactly like the failure the test is
/// about. The fix proves the stream is live first (the add must reach it) and
/// then waits `T`, the suite's bounded-wait budget, which is also the 5 s
/// floor the plan sets for timing-based waits.
#[tokio::test]
async fn router_with_shutdown_ends_open_events() {
    let session = support::session().await;
    let (fire, fired) = tokio::sync::oneshot::channel::<()>();
    let app = fbt_api::router_with_shutdown(session.clone(), &support::test_config(), async {
        let _ = fired.await;
    })
    .expect("router");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let base = format!("http://{}", listener.local_addr().expect("addr"));
    let serving = tokio::spawn(async move { axum::serve(listener, app).await });

    let (status, _, sse) = Sse::open(&format!("{base}/api/v1/events"), Some(TOKEN)).await;
    assert_eq!(status, StatusCode::OK);
    let mut sse = sse.expect("stream");
    // Show the stream is live before asking it to end: an add has to reach it.
    let p = payload();
    add(&base, &p).await;
    let first = tokio::time::timeout(T, sse.next())
        .await
        .expect("an event on the open stream");
    assert!(first.is_some(), "the stream ended before the add arrived");
    fire.send(()).expect("the router waits on the future");
    tokio::time::timeout(T, async { while sse.next().await.is_some() {} })
        .await
        .expect("the body ends within T of the shutdown future");
    serving.abort();
    session.shutdown().await;
}

#[test]
fn events_route_is_documented() {
    let doc = serde_json::to_value(fbt_api::openapi()).expect("document");
    let get = &doc["paths"]["/api/v1/events"]["get"];
    assert!(get.is_object(), "{doc}");
    assert!(get["responses"]["200"]["content"]["text/event-stream"].is_object());
}
