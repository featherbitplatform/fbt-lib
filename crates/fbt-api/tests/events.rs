//! `GET /api/v1/events`: the server-sent event stream.

mod support;

use std::ops::ControlFlow;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use fbt::{AddTorrent, CreateTorrent, Metainfo, Session, TorrentHandle, TorrentState};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{CONTENT_TYPE, HeaderMap};
use hyper::{Method, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::Value;
use support::{
    T, TOKEN, api, api_with, error_code, get, query_escape, request, send_body, test_settings,
};
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

/// Waits until `h` is seeding.
async fn wait_seeding(h: &TorrentHandle) {
    tokio::time::timeout(T, async {
        while h.status().state != TorrentState::Seeding {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("seeding in time");
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

/// Opens an event stream (with `query`, e.g. `"?categories=dht"`) on a raw
/// connection and reads the response head; returns the connection and the
/// head.
async fn raw_open(base: &str, query: &str) -> (TcpStream, String) {
    let addr = base.trim_start_matches("http://");
    let mut conn = TcpStream::connect(addr).await.expect("connect");
    let req = format!(
        "GET /api/v1/events{query} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer \
         {TOKEN}\r\n\r\n"
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
    let (a, head_a) = raw_open(&base, "").await;
    let (_b, head_b) = raw_open(&base, "").await;
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

/// A client that reads the event-stream head and nothing more stalls the
/// connection's writes: two sources flood the stream faster than a stalled
/// socket pair can absorb — 900 tiny torrents whose trackers point at a
/// closed port with 2 KiB URLs (each failed announce is one ~2.2 KB
/// `tracker_error` event, over 2 MB of SSE together) and three 16 MiB
/// leeches whose piece verification adds another ~300 KB of
/// `piece_finished` events. On macOS the kernel grows a stalled socket's
/// send buffer to well over a megabyte before a write blocks (measured:
/// 0.5–1.2 MB absorbed), so the flood is sized against that, not against
/// hyper's 400 KiB write queue alone. Once it overtakes the buffers, no
/// byte the kernel accepts moves for `stream_idle_timeout`, and
/// `ApiServer` ends the whole connection — freeing the single
/// event-stream permit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_sse_is_ended() {
    // The seed: three 16 MiB files, 1024 pieces each, every verified piece
    // an event — the parallel piece streams keep the SSE busy faster than
    // any single download could.
    let seed = Session::new(test_settings()).await.expect("seed session");
    let mut metas = Vec::new();
    let mut seed_handles = Vec::new();
    let mut seed_dirs = Vec::new();
    for i in 0..3u8 {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join(format!("flood{i}.bin"));
        std::fs::write(&file, vec![7 + i; 16 << 20]).expect("write");
        let bytes = CreateTorrent::new(&file)
            .piece_len(16384)
            .build(&mut |_, _| ControlFlow::Continue(()))
            .expect("create");
        let m = Metainfo::from_bytes(&bytes).expect("parse");
        let h = seed
            .add_torrent(AddTorrent::new(m.clone(), dir.path().to_path_buf()))
            .await
            .expect("seed add");
        seed_dirs.push(dir);
        metas.push(m);
        seed_handles.push(h);
    }
    for h in &seed_handles {
        wait_seeding(h).await;
    }

    let (session, _server, base, token) = api_with(|c| {
        c.stream_idle_timeout = Duration::from_secs(1);
        c.max_event_streams = 1;
        // Tighten every cap so that at proof time both connection slots are
        // necessarily held — one by the stalled client, one by the second
        // stream — and a fresh request waits for the killed task to return.
        // `validate` requires max_connections to exceed max_streams plus
        // max_event_streams; this test opens no file streams, so 2 is the
        // smallest legal value.
        c.max_streams = 0;
        c.max_connections = 2;
    })
    .await;
    let (conn, head) = raw_open(&base, "").await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    // Clamp the stalled client's receive buffer: the socket pair that stops
    // accepting bytes stays small, so the flood overtakes it early and on
    // any runner.
    socket2::SockRef::from(&conn)
        .set_recv_buffer_size(4 * 1024)
        .expect("small receive buffer");

    // The flood starts: the API session leeches all three seeds, and each
    // verified piece is an event for the client that stopped reading.
    let leech_dir = tempfile::tempdir().expect("tempdir");
    for m in &metas {
        let leech = session
            .add_torrent(AddTorrent::new(m.clone(), leech_dir.path().to_path_buf()))
            .await
            .expect("leech add");
        leech.add_peer(seed.local_addr()).await.expect("add peer");
    }

    // The bulk of the flood: tiny unique torrents added straight through
    // the session, each announcing to a closed port through a 2 KiB URL.
    // Every announce fails at once as one fat `tracker_error` event —
    // together more SSE than a stalled socket pair absorbs.
    let dead_port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("dead port");
        l.local_addr().expect("addr").port()
    }; // dropped: nothing listens here any more
    let churn_dir = tempfile::tempdir().expect("churn dir");
    let prefix = format!("http://127.0.0.1:{dead_port}/");
    let url_pad = 2048 - prefix.len(); // fbt-metainfo's MAX_URL_LEN
    for i in 0..900usize {
        let file = churn_dir.path().join(format!("c{i}.bin"));
        std::fs::write(&file, [i as u8; 1]).expect("write");
        let bytes = CreateTorrent::new(&file)
            .trackers(vec![vec![format!("{prefix}{}", "a".repeat(url_pad))]])
            .build(&mut |_, _| ControlFlow::Continue(()))
            .expect("create");
        let m = Metainfo::from_bytes(&bytes).expect("parse");
        session
            .add_torrent(AddTorrent::new(m, churn_dir.path().to_path_buf()))
            .await
            .expect("churn add");
    }

    // The freed slot proves the kill: a second stream opens while the
    // stalled client, still connected, would hold it forever.
    let url = format!("{base}/api/v1/events");
    let opened = tokio::time::timeout(Duration::from_secs(10), async {
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
        Err(_) => panic!("the stalled stream's slot is not free within 10 s"),
    };

    // The stalled connection is gone from the server's side. Both slots of
    // `max_connections` were held — by it and by the second stream — so this
    // fresh request is accepted only once some connection task has returned,
    // and no other task can end here; a connection slot releases exactly
    // when hyper drops the socket, which is the closure being claimed. (The
    // probe above proves the event permit freed, which also happens if a
    // body merely ends while its connection lives — this one cannot.)
    // Proving closure by draining the stalled client instead would test the
    // kernel's buffering, not the server: at kill time Linux has grown the
    // send buffer and queued megabytes of the flood behind the FIN it closes
    // with, and delivers that queue to a client whose receive buffer is
    // clamped small — needed for the stall to fire there at all — at
    // persist-timer pace, so EOF arrives long after 8 s. macOS and Windows
    // drop or drain the queued bytes promptly; the freed slot is what every
    // platform agrees on.
    let answered = tokio::time::timeout(Duration::from_secs(8), get(&url, Some(token))).await;
    let (status, _, _) = answered.expect(
        "a fresh request is served within 8 s: the stalled connection's task \
         returned, closing its socket",
    );
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "the second stream still holds the event slot"
    );
    drop(conn);

    // The freed slot serves a live stream: an add reaches it.
    let p = payload();
    add(&base, &p).await;
    let first = tokio::time::timeout(T, second.next())
        .await
        .expect("an event on the new stream");
    assert!(first.is_some(), "the new stream carries events");
    session.shutdown().await;
    seed.shutdown().await;
}

/// A quiet event stream — nothing to send but a keep-alive comment every
/// 15 s — whose client stays connected has nothing pending to write, so the
/// stall watch never fires: three timeout windows on it still holds its
/// slot, and at fifteen windows the comment still reaches it.
#[tokio::test]
async fn quiet_sse_survives_the_idle_timeout() {
    let (_session, _server, base, token) = api_with(|c| {
        c.stream_idle_timeout = Duration::from_secs(1);
        c.max_event_streams = 1;
    })
    .await;
    // The test session's DHT is off, so a `dht`-only stream stays quiet.
    let (mut conn, head) = raw_open(&base, "?categories=dht").await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");

    tokio::time::sleep(Duration::from_secs(3)).await;
    let (status, _, body) = get(&format!("{base}/api/v1/events?categories=dht"), Some(token)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body:?}");
    assert_eq!(error_code(&body), "too_many_streams", "still open");

    // The first keep-alive comment (due 15 s after the stream starts)
    // arrives: still open and still being written to.
    let comment = tokio::time::timeout(Duration::from_secs(18), async {
        let mut got = Vec::new();
        let mut buf = [0u8; 256];
        while !got.contains(&b':') {
            let n = conn.read(&mut buf).await.expect("read");
            assert!(n > 0, "the quiet stream was closed out under its reader");
            got.extend_from_slice(&buf[..n]);
        }
    })
    .await;
    assert!(comment.is_ok(), "the keep-alive comment never arrived");
    drop(conn);
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
/// delay that poll past two seconds, and because the stream was never shown
/// to carry anything, a slow start looked exactly like the failure the test
/// is about. The fix proves the stream is live first (the add must reach
/// it) and then waits `T`, the suite's bounded-wait budget, which is also
/// the 5 s floor the plan sets for timing-based waits.
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
