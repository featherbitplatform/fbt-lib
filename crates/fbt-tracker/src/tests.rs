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
async fn http_error_status_and_udp_unsupported() {
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
        .announce("udp://127.0.0.1:1/announce", &request())
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
