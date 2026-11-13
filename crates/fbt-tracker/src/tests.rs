use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fbt_bencode::{DictBuilder, OwnedValue, encode};
use fbt_core::PeerId;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::*;

fn dict(f: impl FnOnce(&mut DictBuilder)) -> Vec<u8> {
    let mut b = DictBuilder::new();
    f(&mut b);
    encode(&b.build())
}

fn request() -> AnnounceRequest {
    AnnounceRequest {
        info_hash: [0xaa; 20],
        peer_id: PeerId([b'x'; 20]),
        port: 6881,
        port_v4: None,
        port_v6: None,
        uploaded: 1,
        downloaded: 2,
        left: 3,
        event: AnnounceEvent::Started,
        num_want: Some(50),
        key: 0xbeef,
        tracker_id: None,
    }
}

#[test]
fn urlencode_info_hash() {
    assert_eq!(
        url_encode_bytes(&[0x12, 0x34, b'a', b'~', b' ', 0xff]),
        "%124a~%20%FF"
    );
}

#[test]
fn parses_compact_and_dict_peers_and_peers6() {
    let compact = dict(|b| {
        b.insert(b"interval", 900);
        b.insert(b"peers", vec![1, 2, 3, 4, 0x1a, 0xe1, 9, 9]);
        let mut v6 = vec![0u8; 15];
        v6.push(1);
        v6.extend([0x1a, 0xe2]);
        b.insert(b"peers6", v6);
        b.insert(b"complete", 5);
        b.insert(b"incomplete", 7);
        b.insert(b"tracker id", &b"tid"[..]);
        b.insert(b"external ip", vec![9, 8, 7, 6]);
    });
    let r = parse_http_announce(&compact).unwrap();
    assert_eq!(r.interval, Duration::from_secs(900));
    assert_eq!(
        r.peers,
        vec![
            "1.2.3.4:6881".parse::<SocketAddr>().unwrap(),
            "[::1]:6882".parse().unwrap()
        ]
    );
    assert_eq!((r.complete, r.incomplete), (Some(5), Some(7)));
    assert_eq!(r.tracker_id.as_deref(), Some(&b"tid"[..]));
    assert_eq!(r.external_ip, Some("9.8.7.6".parse().unwrap()));

    let mut p1 = DictBuilder::new();
    p1.insert(b"ip", "10.0.0.1");
    p1.insert(b"port", 51413);
    let mut bad = DictBuilder::new();
    bad.insert(b"ip", "not an ip");
    bad.insert(b"port", 1);
    let listed = dict(|b| {
        b.insert(b"interval", 900);
        b.insert(b"peers", OwnedValue::List(vec![p1.build(), bad.build()]));
    });
    let r = parse_http_announce(&listed).unwrap();
    assert_eq!(
        r.peers,
        vec!["10.0.0.1:51413".parse::<SocketAddr>().unwrap()]
    );
}

#[test]
fn failure_reason_maps_to_failure() {
    let body = dict(|b| {
        b.insert(b"failure reason", "torrent not registered");
    });
    match parse_http_announce(&body) {
        Err(TrackerError::Failure(s)) => assert_eq!(s, "torrent not registered"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn interval_clamped_and_required() {
    let body = dict(|b| {
        b.insert(b"interval", 5);
        b.insert(b"min interval", 1);
    });
    let r = parse_http_announce(&body).unwrap();
    assert_eq!(r.interval, Duration::from_secs(60));
    assert_eq!(r.min_interval, Some(Duration::from_secs(30)));
    let body = dict(|b| {
        b.insert(b"peers", Vec::<u8>::new());
    });
    assert!(matches!(
        parse_http_announce(&body),
        Err(TrackerError::InvalidResponse(_))
    ));
    assert!(parse_http_announce(b"garbage").is_err());
}

/// Serves one raw response per connection from `responses`, in order,
/// recording each request line.
async fn serve(responses: Vec<Vec<u8>>) -> (SocketAddr, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let lines = Arc::new(Mutex::new(Vec::new()));
    let seen = lines.clone();
    tokio::spawn(async move {
        for resp in responses {
            let Ok((mut s, _)) = listener.accept().await else {
                return;
            };
            let mut buf = vec![0u8; 8192];
            let mut got = Vec::new();
            while !got.windows(4).any(|w| w == b"\r\n\r\n") {
                match s.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => got.extend_from_slice(&buf[..n]),
                }
            }
            let text = String::from_utf8_lossy(&got).into_owned();
            seen.lock()
                .unwrap()
                .push(text.lines().next().unwrap_or("").to_owned());
            let _ = s.write_all(&resp).await;
            let _ = s.shutdown().await;
        }
    });
    (addr, lines)
}

fn http_ok(body: &[u8]) -> Vec<u8> {
    let mut v = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    v.extend_from_slice(body);
    v
}

fn redirect(to: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 302 Found\r\nLocation: {to}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    )
    .into_bytes()
}

fn client() -> TrackerClient {
    TrackerClient::new(TrackerConfig::default()).unwrap()
}

#[tokio::test]
async fn announce_against_local_http_server() {
    let body = dict(|b| {
        b.insert(b"interval", 1800);
        b.insert(b"peers", vec![127, 0, 0, 1, 0x1a, 0xe1]);
    });
    let (addr, lines) = serve(vec![http_ok(&body)]).await;
    let r = client()
        .announce(&format!("http://{addr}/announce?passkey=k"), &request())
        .await
        .unwrap();
    assert_eq!(
        r.peers,
        vec!["127.0.0.1:6881".parse::<SocketAddr>().unwrap()]
    );
    let line = lines.lock().unwrap()[0].clone();
    assert!(
        line.starts_with("GET /announce?passkey=k&info_hash=%AA%AA"),
        "{line}"
    );
    assert!(
        line.contains("&compact=1&key=0000beef&numwant=50&event=started"),
        "{line}"
    );
}

#[tokio::test]
async fn oversized_body_rejected() {
    let body = vec![b'a'; 3 * 1024 * 1024];
    let (addr, _) = serve(vec![http_ok(&body)]).await;
    let err = client()
        .announce(&format!("http://{addr}/a"), &request())
        .await
        .unwrap_err();
    assert!(matches!(err, TrackerError::ResponseTooLarge), "{err:?}");
}

#[tokio::test]
async fn redirect_limit() {
    let (addr, _) = serve(vec![
        redirect("/r1"),
        redirect("/r2"),
        redirect("/r3"),
        redirect("/r4"),
        redirect("/r5"),
    ])
    .await;
    let err = client()
        .announce(&format!("http://{addr}/a"), &request())
        .await
        .unwrap_err();
    assert!(matches!(err, TrackerError::TooManyRedirects), "{err:?}");
}

#[tokio::test]
async fn http_error_status_and_unsupported_scheme() {
    let (addr, _) = serve(vec![
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            .to_vec(),
    ])
    .await;
    let err = client()
        .announce(&format!("http://{addr}/a"), &request())
        .await
        .unwrap_err();
    assert!(matches!(err, TrackerError::HttpStatus(503)), "{err:?}");
    let err = client()
        .announce("wss://127.0.0.1:1/announce", &request())
        .await
        .unwrap_err();
    assert!(matches!(err, TrackerError::UnsupportedScheme(_)), "{err:?}");
}

#[cfg(not(any(feature = "tls-ring", feature = "tls-rustcrypto")))]
#[tokio::test]
async fn https_without_tls_feature() {
    let err = client()
        .announce("https://127.0.0.1:1/announce", &request())
        .await
        .unwrap_err();
    assert!(matches!(err, TrackerError::TlsDisabled), "{err:?}");
}

#[test]
fn tracker_strings_are_bounded() {
    // Failure reasons are cut to 1 KiB on a character boundary.
    let long = "é".repeat(5000);
    let body = dict(|b| {
        b.insert(b"failure reason", long.as_str());
    });
    match parse_http_announce(&body) {
        Err(TrackerError::Failure(m)) => {
            assert!(m.len() <= 1024 && m.len() >= 1020, "{}", m.len());
            assert!(m.chars().all(|c| c == 'é'));
        }
        other => panic!("{other:?}"),
    }
    // Warnings too; tracker ids above 256 bytes are dropped, shorter kept.
    let body = dict(|b| {
        b.insert(b"interval", 900);
        b.insert(b"warning message", "w".repeat(10_000).as_str());
        b.insert(b"tracker id", vec![b'i'; 257]);
    });
    let r = parse_http_announce(&body).unwrap();
    assert_eq!(r.warning.map(|w| w.len()), Some(1024));
    assert_eq!(r.tracker_id, None);
    let body = dict(|b| {
        b.insert(b"interval", 900);
        b.insert(b"tracker id", vec![b'i'; 256]);
    });
    assert_eq!(
        parse_http_announce(&body)
            .unwrap()
            .tracker_id
            .map(|t| t.len()),
        Some(256)
    );
    // A UDP error action's text as well.
    let mut dgram = vec![0, 0, 0, 3, 0, 0, 0, 7];
    dgram.extend(std::iter::repeat_n(b'x', 60_000));
    match parse_udp_announce(&dgram, 7, false) {
        Err(TrackerError::Failure(m)) => assert_eq!(m.len(), 1024),
        other => panic!("{other:?}"),
    }
}

#[test]
fn http_scrape_url_rule() {
    use crate::scrape::scrape_url;
    assert_eq!(
        scrape_url("http://t.example:80/announce").unwrap(),
        "http://t.example:80/scrape"
    );
    assert_eq!(
        scrape_url("http://t.example/x/announce.php?k=1").unwrap(),
        "http://t.example/x/scrape.php?k=1"
    );
    assert_eq!(
        scrape_url("https://t.example/announce?a=b#frag").unwrap(),
        "https://t.example/scrape?a=b"
    );
    for bad in [
        "http://t.example/a",
        "http://t.example/x/a/announce/y",
        "http://t.example",
        "http://t.example/",
        "http://t.example/x?q=/announce",
    ] {
        assert!(
            matches!(scrape_url(bad), Err(TrackerError::ScrapeUnsupported)),
            "{bad}"
        );
    }
}

fn scrape_files(entries: &[([u8; 20], i64, i64, i64)]) -> Vec<u8> {
    let mut files = DictBuilder::new();
    for (h, c, d, i) in entries {
        let mut e = DictBuilder::new();
        e.insert(b"complete", *c);
        e.insert(b"downloaded", *d);
        e.insert(b"incomplete", *i);
        files.insert(h, e.build());
    }
    dict(|b| {
        b.insert(b"files", files.build());
    })
}

#[tokio::test]
async fn http_scrape_parses_files() {
    let body = scrape_files(&[([1; 20], 5, 9, 7), ([2; 20], 1, 2, 3)]);
    let mut got = parse_http_scrape(&body).unwrap();
    got.sort_by_key(|s| s.info_hash);
    assert_eq!(got.len(), 2);
    assert_eq!(
        (
            got[0].info_hash,
            got[0].complete,
            got[0].downloaded,
            got[0].incomplete
        ),
        ([1; 20], 5, 9, 7)
    );
    assert_eq!(
        (
            got[1].info_hash,
            got[1].complete,
            got[1].downloaded,
            got[1].incomplete
        ),
        ([2; 20], 1, 2, 3)
    );
    // Keys that are not info hashes and non-dict entries are skipped.
    let mut files = DictBuilder::new();
    files.insert(b"short", 1);
    files.insert(&[3u8; 20], 1);
    let odd = dict(|b| {
        b.insert(b"files", files.build());
    });
    assert!(parse_http_scrape(&odd).unwrap().is_empty());
    // A failure reason, a missing `files` and garbage are errors.
    let fail = dict(|b| {
        b.insert(b"failure reason", "no scrape here");
    });
    assert!(matches!(
        parse_http_scrape(&fail),
        Err(TrackerError::Failure(m)) if m == "no scrape here"
    ));
    assert!(matches!(
        parse_http_scrape(&dict(|b| {
            b.insert(b"x", 1);
        })),
        Err(TrackerError::InvalidResponse(_))
    ));
    assert!(parse_http_scrape(b"garbage").is_err());

    // Against a server: one `info_hash` per hash on the scrape path, the
    // query kept; a hash the tracker left out is left out, one we did not
    // ask for is dropped.
    let reply = scrape_files(&[([1; 20], 5, 9, 7), ([9; 20], 1, 1, 1)]);
    let (addr, lines) = serve(vec![http_ok(&reply)]).await;
    let got = client()
        .scrape(
            &format!("http://{addr}/x/announce?passkey=k"),
            &[[1; 20], [2; 20]],
        )
        .await
        .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(
        (
            got[0].info_hash,
            got[0].complete,
            got[0].downloaded,
            got[0].incomplete
        ),
        ([1; 20], 5, 9, 7)
    );
    let line = lines.lock().unwrap()[0].clone();
    let one = "%01".repeat(20);
    let two = "%02".repeat(20);
    assert!(
        line.starts_with(&format!(
            "GET /x/scrape?passkey=k&info_hash={one}&info_hash={two} "
        )),
        "{line}"
    );
}

#[tokio::test]
async fn scrape_caps_hashes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = accepted.clone();
    tokio::spawn(async move {
        while listener.accept().await.is_ok() {
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    });
    let udp = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_addr = udp.local_addr().unwrap();
    let hashes = vec![[7u8; 20]; MAX_SCRAPE_HASHES + 1];
    assert_eq!(hashes.len(), 75);
    for url in [
        format!("http://{addr}/announce"),
        format!("udp://{udp_addr}"),
    ] {
        let err = client().scrape(&url, &hashes).await.unwrap_err();
        assert!(matches!(err, TrackerError::TooManyHashes), "{err:?}");
    }
    // 74 is fine as far as the cap goes (the tracker then fails to answer).
    assert!(matches!(
        client()
            .scrape(&format!("http://{addr}/a"), &hashes[..74])
            .await,
        Err(TrackerError::ScrapeUnsupported)
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(accepted.load(std::sync::atomic::Ordering::SeqCst), 0);
    let mut buf = [0u8; 64];
    let got = tokio::time::timeout(Duration::from_millis(100), udp.recv_from(&mut buf)).await;
    assert!(got.is_err(), "no datagram sent");
}

#[test]
fn http_scrape_counts_saturate() {
    let body = scrape_files(&[([1; 20], i64::from(u32::MAX) + 1, i64::MAX, -5)]);
    let got = parse_http_scrape(&body).unwrap();
    assert_eq!(
        (got[0].complete, got[0].downloaded, got[0].incomplete),
        (u32::MAX, u32::MAX, 0)
    );
    let body = scrape_files(&[([1; 20], i64::from(u32::MAX), 0, 1)]);
    assert_eq!(parse_http_scrape(&body).unwrap()[0].complete, u32::MAX);
}

#[test]
fn http_scrape_output_is_capped() {
    let entries: Vec<_> = (0..100u8).map(|i| ([i; 20], 1, 1, 1)).collect();
    let body = scrape_files(&entries);
    assert_eq!(parse_http_scrape(&body).unwrap().len(), MAX_SCRAPE_HASHES);
}

#[test]
fn content_range_parsing() {
    // Valid, with and without a known length, and any case of the unit.
    assert_eq!(
        parse_content_range("bytes 0-99/1000"),
        Some((0, 99, Some(1000)))
    );
    assert_eq!(parse_content_range("bytes 5-5/*"), Some((5, 5, None)));
    assert_eq!(
        parse_content_range("Bytes 10-19/20"),
        Some((10, 19, Some(20)))
    );
    // `*` alone is an unsatisfied range, not one we can use.
    assert_eq!(parse_content_range("bytes */1000"), None);
    // Malformed.
    for bad in [
        "",
        "bytes",
        "bytes ",
        "bytes 0-99",
        "bytes 0-/100",
        "bytes -99/100",
        "bytes 9-0/100",
        "bytes 0-99/99",
        "bytes 0-99/0",
        "items 0-99/100",
        "bytes0-99/100",
        "bytes +0-99/100",
        "bytes 0-+99/100",
        "bytes 0-99/+100",
        "bytes 0x0-99/100",
        "bytes 0-99/100/1",
        "bytes 0 -99/100",
    ] {
        assert_eq!(parse_content_range(bad), None, "{bad:?}");
    }
    // Overflow.
    assert_eq!(parse_content_range("bytes 0-18446744073709551616/*"), None);
    assert_eq!(
        parse_content_range("bytes 0-18446744073709551614/18446744073709551615"),
        Some((0, u64::MAX - 1, Some(u64::MAX)))
    );
    assert_eq!(
        parse_content_range("bytes 0-1/99999999999999999999999999"),
        None
    );
    assert_eq!(
        parse_content_range(&format!("bytes {}-1/2", "9".repeat(10_000))),
        None
    );
}

fn partial(content_range: &str, body: &[u8]) -> Vec<u8> {
    let mut v = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Range: {content_range}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    v.extend_from_slice(body);
    v
}

/// Counts what a paced fetch charges.
struct Counting(Mutex<(u64, u32)>);

impl RangePace for Counting {
    fn ready(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        self.0.lock().unwrap().1 += 1;
        Box::pin(std::future::ready(()))
    }

    fn charge(&self, n: u64) {
        self.0.lock().unwrap().0 += n;
    }
}

#[tokio::test]
async fn fetch_range_accepts_a_matching_206() {
    let (addr, _) = serve(vec![partial("bytes 2-5/10", b"cdef")]).await;
    let pace = Counting(Mutex::new((0, 0)));
    let got = client()
        .fetch_range_paced(&format!("http://{addr}/f"), 2..6, &pace)
        .await
        .unwrap();
    assert_eq!(&got[..], b"cdef");
    let (charged, waits) = *pace.0.lock().unwrap();
    assert_eq!(charged, 4, "every body byte is charged");
    assert!(waits >= 1, "every read waits for the pace first");
}

#[tokio::test]
async fn fetch_range_takes_a_200_only_for_the_whole_file_from_0() {
    let (addr, _) = serve(vec![http_ok(b"0123456789"), http_ok(b"0123456789")]).await;
    let url = format!("http://{addr}/f");
    assert_eq!(
        &client().fetch_range(&url, 0..10).await.unwrap()[..],
        b"0123456789"
    );
    // Asked for part of it: the server ignored the range.
    assert!(matches!(
        client().fetch_range(&url, 0..5).await,
        Err(TrackerError::InvalidResponse(_))
    ));
}

#[tokio::test]
async fn fetch_range_refuses_a_wrong_content_range() {
    let (addr, _) = serve(vec![
        partial("bytes 3-6/10", b"defg"),
        partial("bytes 2-5", b"cdef"),
    ])
    .await;
    let url = format!("http://{addr}/f");
    for _ in 0..2 {
        assert!(matches!(
            client().fetch_range(&url, 2..6).await,
            Err(TrackerError::InvalidResponse(_))
        ));
    }
}

#[tokio::test]
async fn fetch_range_refuses_a_body_too_long_or_short() {
    // The right Content-Range, but more bytes than it says.
    let long = partial("bytes 2-5/10", b"cdefXXXX");
    // Close-delimited and longer.
    let mut endless =
        b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-5/10\r\nConnection: close\r\n\r\n"
            .to_vec();
    endless.extend_from_slice(&[b'x'; 64 * 1024]);
    let short = partial("bytes 2-5/10", b"cd");
    let not_found = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    let (addr, _) = serve(vec![long, endless, short, not_found.to_vec()]).await;
    let url = format!("http://{addr}/f");
    let c = client();
    assert!(matches!(
        c.fetch_range(&url, 2..6).await,
        Err(TrackerError::ResponseTooLarge)
    ));
    assert!(matches!(
        c.fetch_range(&url, 2..6).await,
        Err(TrackerError::ResponseTooLarge)
    ));
    assert!(matches!(
        c.fetch_range(&url, 2..6).await,
        Err(TrackerError::InvalidResponse(_))
    ));
    assert!(matches!(
        c.fetch_range(&url, 2..6).await,
        Err(TrackerError::HttpStatus(404))
    ));
    // An empty range asks nothing.
    assert!(c.fetch_range(&url, 6..6).await.unwrap().is_empty());
}

#[test]
fn minimum_rate_after_the_grace() {
    use crate::http::{MIN_RANGE_RATE, RANGE_RATE_GRACE, too_slow};
    let grace = RANGE_RATE_GRACE;
    // Within the grace nothing is too slow.
    assert!(!too_slow(0, grace));
    let later = grace + Duration::from_secs(10);
    let at_min = MIN_RANGE_RATE * later.as_secs();
    assert!(!too_slow(at_min, later));
    assert!(too_slow(at_min - 1, later));
    assert!(too_slow(1, later));
}
