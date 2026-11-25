use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::{Duration, Instant};

use fbt_net::proxy::{
    MAX_CONNECT_HEAD, ProxyConfig, ProxyError, ProxyKind, ProxyTarget, connect_tcp, decode_udp,
    encode_udp, encode_udp_host, parse_connect_head, probe, udp_associate, udp_not_relayed,
};
use proptest::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};

#[path = "support/fake_http_proxy.rs"]
mod fake_http_proxy;
#[path = "support/fake_socks5.rs"]
mod fake_socks5;

use fake_http_proxy::{FakeHttpProxy, Script as HttpScript};
use fake_socks5::{FakeSocks5, Script, closed_port};

const T: Duration = Duration::from_secs(5);

fn cfg(kind: ProxyKind, addr: SocketAddr) -> ProxyConfig {
    let mut c = ProxyConfig::default();
    c.kind = kind;
    c.host = addr.ip().to_string();
    c.port = addr.port();
    c
}

/// A SOCKS5 config pointing at `addr`.
fn socks5(addr: SocketAddr) -> ProxyConfig {
    cfg(ProxyKind::Socks5, addr)
}

/// A target nothing listens on: loopback, port 9.
fn addr_target() -> ProxyTarget {
    ProxyTarget::Addr(SocketAddr::from(([127, 0, 0, 1], 9)))
}

/// A SOCKS5 config pointing at a fresh fake running `script`; keep the fake
/// alive for as long as its address is used.
async fn socks5_fake(script: Script) -> (FakeSocks5, ProxyConfig) {
    let fake = FakeSocks5::start(script).await;
    let c = socks5(fake.addr());
    (fake, c)
}

/// A listener that accepts every connection and never says a word.
async fn silent_listener() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let _keep = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((s, _)) = l.accept().await {
            held.push(s);
        }
    });
    addr
}

/// A TCP server on loopback that says `hello` and then echoes.
async fn echo_server() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            tokio::spawn(async move {
                s.write_all(b"hello").await.unwrap();
                let mut buf = [0u8; 1024];
                while let Ok(n) = s.read(&mut buf).await {
                    if n == 0 || s.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    addr
}

async fn check_echo(s: &mut tokio::net::TcpStream) {
    let mut hello = [0u8; 5];
    s.read_exact(&mut hello).await.unwrap();
    assert_eq!(&hello, b"hello");
    s.write_all(b"ping").await.unwrap();
    let mut back = [0u8; 4];
    s.read_exact(&mut back).await.unwrap();
    assert_eq!(&back, b"ping");
}

#[tokio::test]
async fn socks5_connect_by_addr_and_host() {
    let echo = echo_server().await;
    let proxy = FakeSocks5::start(Script::Ok).await;
    let c = cfg(ProxyKind::Socks5, proxy.addr());
    let mut s = connect_tcp(&c, ProxyTarget::Addr(echo), T).await.unwrap();
    check_echo(&mut s).await;
    let mut s = connect_tcp(&c, ProxyTarget::Host("localhost".into(), echo.port()), T)
        .await
        .unwrap();
    check_echo(&mut s).await;
    assert_eq!(
        proxy.connects(),
        vec![
            format!("127.0.0.1:{}", echo.port()),
            format!("localhost:{}", echo.port())
        ]
    );
}

async fn expect_early(s: &mut tokio::net::TcpStream) {
    let mut early = [0u8; 5];
    s.read_exact(&mut early).await.unwrap();
    assert_eq!(&early, b"early");
}

#[tokio::test]
async fn socks5_reply_and_tunnel_bytes_in_one_segment() {
    // The reply names a domain `BND.ADDR` and `early` follows it in the same
    // write: the client reads exactly the reply.
    let echo = echo_server().await;
    let proxy = FakeSocks5::start(Script::Trailer(b"early".to_vec())).await;
    let c = cfg(ProxyKind::Socks5, proxy.addr());
    let mut s = connect_tcp(&c, ProxyTarget::Addr(echo), T).await.unwrap();
    expect_early(&mut s).await;
    check_echo(&mut s).await;
}

#[tokio::test]
async fn target_host_allowlist() {
    let socks = FakeSocks5::start(Script::Ok).await;
    let http = FakeHttpProxy::start(HttpScript::Ok).await;
    let bad = [
        "".to_string(),
        "a:1".into(),
        "a b".into(),
        "x\r\nX-Evil: 1".into(),
        "user@host".into(),
        "a/b".into(),
        "[fe80::1%eth0]".into(),
        "fe80::1%2".into(),
        "b\u{fc}cher.example".into(),
        "a".repeat(256),
    ];
    for c in [
        cfg(ProxyKind::Socks5, socks.addr()),
        cfg(ProxyKind::Http, http.addr()),
    ] {
        for host in &bad {
            let err = connect_tcp(&c, ProxyTarget::Host(host.clone(), 80), T)
                .await
                .unwrap_err();
            assert!(matches!(err, ProxyError::Protocol(_)), "{host:?}: {err:?}");
        }
    }
    // Nothing reached either proxy's CONNECT.
    assert!(socks.connects().is_empty());
    assert!(http.connects().is_empty());
    // Allowed bytes and IP literals still go through.
    let echo = echo_server().await;
    let c = cfg(ProxyKind::Http, http.addr());
    for host in ["localhost", "127.0.0.1", "[127.0.0.1]"] {
        let mut s = connect_tcp(&c, ProxyTarget::Host(host.into(), echo.port()), T)
            .await
            .unwrap();
        check_echo(&mut s).await;
    }
}

#[tokio::test]
async fn scoped_ipv6_is_refused() {
    let socks = FakeSocks5::start(Script::Ok).await;
    let http = FakeHttpProxy::start(HttpScript::Ok).await;
    let scoped = SocketAddr::V6(std::net::SocketAddrV6::new(
        "fe80::1".parse().unwrap(),
        80,
        0,
        2,
    ));
    for c in [
        cfg(ProxyKind::Socks5, socks.addr()),
        cfg(ProxyKind::Http, http.addr()),
    ] {
        let err = connect_tcp(&c, ProxyTarget::Addr(scoped), T)
            .await
            .unwrap_err();
        assert!(matches!(err, ProxyError::Protocol(_)), "{err:?}");
    }
    assert!(socks.connects().is_empty());
    assert!(http.connects().is_empty());
}

#[tokio::test]
async fn basic_username_with_colon_is_refused() {
    let proxy = FakeHttpProxy::start(HttpScript::RequireBasic("a:b".into(), "c".into())).await;
    let mut c = cfg(ProxyKind::Http, proxy.addr());
    c.username = Some("a:b".into());
    c.password = Some("c".into());
    let target = ProxyTarget::Addr(SocketAddr::from(([127, 0, 0, 1], 9)));
    let err = connect_tcp(&c, target, T).await.unwrap_err();
    assert!(matches!(err, ProxyError::Protocol(_)), "{err:?}");
    assert!(proxy.connects().is_empty());
}

#[tokio::test]
async fn socks5_empty_credentials_are_refused() {
    let proxy = FakeSocks5::start(Script::RequireAuth("a".into(), "b".into())).await;
    let target = ProxyTarget::Addr(SocketAddr::from(([127, 0, 0, 1], 9)));
    for (user, pass) in [
        (Some(""), Some("b")),
        (Some("a"), None),
        (Some("a"), Some("")),
        (Some(""), None),
    ] {
        let mut c = cfg(ProxyKind::Socks5, proxy.addr());
        c.username = user.map(String::from);
        c.password = pass.map(String::from);
        let err = connect_tcp(&c, target.clone(), T).await.unwrap_err();
        assert!(
            matches!(err, ProxyError::Protocol(_)),
            "{user:?}/{pass:?}: {err:?}"
        );
    }
}

#[tokio::test]
async fn socks5_user_pass() {
    let echo = echo_server().await;
    let proxy = FakeSocks5::start(Script::RequireAuth("alice".into(), "s3cret".into())).await;
    let mut c = cfg(ProxyKind::Socks5, proxy.addr());
    c.username = Some("alice".into());
    c.password = Some("s3cret".into());
    let mut s = connect_tcp(&c, ProxyTarget::Addr(echo), T).await.unwrap();
    check_echo(&mut s).await;
}

#[tokio::test]
async fn socks5_wrong_password_is_auth_failed() {
    let echo = echo_server().await;
    let proxy = FakeSocks5::start(Script::RequireAuth("alice".into(), "s3cret".into())).await;
    let mut c = cfg(ProxyKind::Socks5, proxy.addr());
    c.username = Some("alice".into());
    c.password = Some("wrong".into());
    let err = connect_tcp(&c, ProxyTarget::Addr(echo), T)
        .await
        .unwrap_err();
    assert!(matches!(err, ProxyError::AuthFailed), "{err:?}");
    // Offering no credentials to a proxy that needs them fails the same way.
    c.username = None;
    c.password = None;
    let err = connect_tcp(&c, ProxyTarget::Addr(echo), T)
        .await
        .unwrap_err();
    assert!(matches!(err, ProxyError::AuthFailed), "{err:?}");
}

#[tokio::test]
async fn socks5_refusal_carries_the_code() {
    let echo = echo_server().await;
    let proxy = FakeSocks5::start(Script::Refuse(5)).await;
    let c = cfg(ProxyKind::Socks5, proxy.addr());
    let err = connect_tcp(&c, ProxyTarget::Addr(echo), T)
        .await
        .unwrap_err();
    assert!(matches!(err, ProxyError::Refused(5)), "{err:?}");
}

#[tokio::test]
async fn garbage_reply_is_a_protocol_error() {
    let proxy = FakeSocks5::start(Script::Garbage).await;
    let c = cfg(ProxyKind::Socks5, proxy.addr());
    let target = ProxyTarget::Addr(SocketAddr::from(([127, 0, 0, 1], 9)));
    let err = connect_tcp(&c, target, T).await.unwrap_err();
    assert!(matches!(err, ProxyError::Protocol(_)), "{err:?}");
}

#[tokio::test]
async fn closed_mid_handshake() {
    let proxy = FakeSocks5::start(Script::CloseAfterGreeting).await;
    let c = cfg(ProxyKind::Socks5, proxy.addr());
    let target = ProxyTarget::Addr(SocketAddr::from(([127, 0, 0, 1], 9)));
    let start = Instant::now();
    let err = connect_tcp(&c, target, T).await.unwrap_err();
    assert!(
        matches!(err, ProxyError::Io(_) | ProxyError::Protocol(_)),
        "{err:?}"
    );
    assert!(start.elapsed() < T);
}

#[tokio::test]
async fn silent_proxy_times_out() {
    // Accepts and never says a word.
    let addr = silent_listener().await;
    let target = ProxyTarget::Addr(SocketAddr::from(([127, 0, 0, 1], 9)));
    for kind in [ProxyKind::Socks5, ProxyKind::Http] {
        let c = cfg(kind, addr);
        let start = Instant::now();
        let err = connect_tcp(&c, target.clone(), Duration::from_millis(300))
            .await
            .unwrap_err();
        assert!(matches!(err, ProxyError::Timeout), "{err:?}");
        assert!(start.elapsed() < Duration::from_secs(3));
    }
}

#[tokio::test]
async fn http_connect_tunnels_and_keeps_bytes() {
    let echo = echo_server().await;
    // `early` follows the 200 head in the same write.
    let proxy = FakeHttpProxy::start(HttpScript::Trailer(b"early".to_vec())).await;
    let c = cfg(ProxyKind::Http, proxy.addr());
    let mut s = connect_tcp(&c, ProxyTarget::Addr(echo), T).await.unwrap();
    expect_early(&mut s).await;
    check_echo(&mut s).await;
    let mut s = connect_tcp(&c, ProxyTarget::Host("localhost".into(), echo.port()), T)
        .await
        .unwrap();
    expect_early(&mut s).await;
    check_echo(&mut s).await;
    assert_eq!(
        proxy.connects(),
        vec![
            format!("127.0.0.1:{}", echo.port()),
            format!("localhost:{}", echo.port())
        ]
    );
}

#[tokio::test]
async fn http_basic_auth() {
    let echo = echo_server().await;
    let proxy = FakeHttpProxy::start(HttpScript::RequireBasic("bob".into(), "pw:x".into())).await;
    let mut c = cfg(ProxyKind::Http, proxy.addr());
    let err = connect_tcp(&c, ProxyTarget::Addr(echo), T)
        .await
        .unwrap_err();
    assert!(matches!(err, ProxyError::HttpStatus(407)), "{err:?}");
    c.username = Some("bob".into());
    c.password = Some("pw:x".into());
    let mut s = connect_tcp(&c, ProxyTarget::Addr(echo), T).await.unwrap();
    check_echo(&mut s).await;
}

#[tokio::test]
async fn http_407_is_http_status() {
    let proxy = FakeHttpProxy::start(HttpScript::Status(407)).await;
    let c = cfg(ProxyKind::Http, proxy.addr());
    let target = ProxyTarget::Addr(SocketAddr::from(([127, 0, 0, 1], 9)));
    let err = connect_tcp(&c, target, T).await.unwrap_err();
    assert!(matches!(err, ProxyError::HttpStatus(407)), "{err:?}");
}

#[tokio::test]
async fn endless_connect_head_is_capped() {
    let proxy = FakeHttpProxy::start(HttpScript::EndlessHead).await;
    let c = cfg(ProxyKind::Http, proxy.addr());
    let target = ProxyTarget::Addr(SocketAddr::from(([127, 0, 0, 1], 9)));
    let start = Instant::now();
    let err = connect_tcp(&c, target, T).await.unwrap_err();
    assert!(matches!(err, ProxyError::Protocol(_)), "{err:?}");
    assert!(start.elapsed() < T);
    // The parser gives up at the cap, not after it.
    let head = vec![b'a'; MAX_CONNECT_HEAD - 1];
    assert!(matches!(parse_connect_head(&head), Ok(None)));
    let head = vec![b'a'; MAX_CONNECT_HEAD];
    assert!(matches!(
        parse_connect_head(&head),
        Err(ProxyError::Protocol(_))
    ));
    let mut head = b"HTTP/1.1 200 OK\r\n".to_vec();
    head.resize(MAX_CONNECT_HEAD - 3, b'a');
    head.extend_from_slice(b"\r\n\r\n");
    assert!(matches!(
        parse_connect_head(&head),
        Err(ProxyError::Protocol(_))
    ));
}

#[test]
fn connect_head_parses_status_and_length() {
    let head = b"HTTP/1.1 200 Connection established\r\nVia: x\r\n\r\ntunnel";
    assert_eq!(
        parse_connect_head(head).unwrap(),
        Some((200, head.len() - 6))
    );
    assert_eq!(parse_connect_head(b"HTTP/1.0 407 No\r\n").unwrap(), None);
    assert!(parse_connect_head(b"SSH-2.0-x\r\n\r\n").is_err());
    // The minor version is a digit.
    assert!(parse_connect_head(b"HTTP/1.x 200 OK\r\n\r\n").is_err());
    assert!(parse_connect_head(b"HTTP/1. 200 OK\r\n\r\n").is_err());
    assert_eq!(
        parse_connect_head(b"HTTP/1.0 204 OK\r\n\r\n").unwrap(),
        Some((204, 19))
    );
    // Bare-LF heads are not recognised.
    assert_eq!(parse_connect_head(b"HTTP/1.1 200 OK\n\n").unwrap(), None);
}

/// A UDP server on loopback that echoes every datagram.
async fn udp_echo() -> SocketAddr {
    let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = s.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = [0u8; 2048];
        while let Ok((n, from)) = s.recv_from(&mut buf).await {
            let _ = s.send_to(&buf[..n], from).await;
        }
    });
    addr
}

#[tokio::test]
async fn udp_associate_round_trip() {
    let echo = udp_echo().await;
    let proxy = FakeSocks5::start(Script::Ok).await;
    let c = cfg(ProxyKind::Socks5, proxy.addr());
    let mut assoc = udp_associate(&c, T).await.unwrap();
    // The fake answers `0.0.0.0`: the proxy's own address is meant.
    assert_eq!(assoc.relay().ip(), proxy.addr().ip());
    assert_ne!(assoc.relay().port(), 0);
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut out = Vec::new();
    encode_udp(echo, b"ping", &mut out);
    sock.send_to(&out, assoc.relay()).await.unwrap();
    let mut buf = [0u8; 2048];
    let (n, from) = tokio::time::timeout(T, sock.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(from, assoc.relay());
    let (src, payload) = decode_udp(&buf[..n]).unwrap();
    assert_eq!(src, echo);
    assert_eq!(payload, b"ping");
    assert_eq!(proxy.udp_targets(), vec![echo.to_string()]);
    // The association lives as long as the control connection.
    drop(proxy);
    tokio::time::timeout(T, assoc.closed()).await.unwrap();
}

#[tokio::test]
async fn udp_associate_needs_socks5() {
    let proxy = FakeHttpProxy::start(HttpScript::Ok).await;
    let c = cfg(ProxyKind::Http, proxy.addr());
    let err = udp_associate(&c, T).await.unwrap_err();
    assert!(matches!(err, ProxyError::Unsupported), "{err:?}");
}

#[test]
fn fragmented_udp_is_dropped() {
    let mut d = Vec::new();
    encode_udp(SocketAddr::from(([127, 0, 0, 1], 6881)), b"x", &mut d);
    assert!(decode_udp(&d).is_some());
    d[2] = 1;
    assert!(decode_udp(&d).is_none(), "FRAG = 1");
    // A domain ATYP: `localhost:6881`.
    let mut dom = vec![0, 0, 0, 3, 9];
    dom.extend_from_slice(b"localhost");
    dom.extend_from_slice(&6881u16.to_be_bytes());
    dom.push(b'x');
    assert!(decode_udp(&dom).is_none(), "domain");
    assert!(decode_udp(&[0, 0, 0, 1, 127]).is_none(), "5 bytes");
}

#[test]
fn socks5_reply_parses_every_address_type() {
    use fbt_net::proxy::parse_socks5_reply;
    let v4 = [5, 0, 0, 1, 10, 0, 0, 1, 0x1a, 0xe1, 0xff];
    assert_eq!(
        parse_socks5_reply(&v4).unwrap(),
        Some((Some(SocketAddr::from(([10, 0, 0, 1], 6881))), 10))
    );
    let mut v6 = vec![5, 0, 0, 4];
    v6.extend_from_slice(&Ipv6Addr::LOCALHOST.octets());
    v6.extend_from_slice(&80u16.to_be_bytes());
    assert_eq!(
        parse_socks5_reply(&v6).unwrap(),
        Some((Some(SocketAddr::from((Ipv6Addr::LOCALHOST, 80))), 22))
    );
    let dom = [5, 0, 0, 3, 3, b'a', b'b', b'c', 0, 80];
    assert_eq!(parse_socks5_reply(&dom).unwrap(), Some((None, 10)));
    assert_eq!(parse_socks5_reply(&dom[..9]).unwrap(), None);
    assert!(matches!(
        parse_socks5_reply(&[5, 2]),
        Err(ProxyError::Refused(2))
    ));
    assert!(matches!(
        parse_socks5_reply(&[5, 0, 0, 9]),
        Err(ProxyError::Protocol(_))
    ));
}

proptest! {
    #[test]
    fn encode_decode_round_trip(
        v6 in any::<bool>(),
        ip4 in any::<[u8; 4]>(),
        ip6 in any::<[u8; 16]>(),
        port in any::<u16>(),
        payload in proptest::collection::vec(any::<u8>(), 0..2048),
    ) {
        let dst = if v6 {
            SocketAddr::from((Ipv6Addr::from(ip6), port))
        } else {
            SocketAddr::from((Ipv4Addr::from(ip4), port))
        };
        let mut out = vec![0xaa; 3];
        encode_udp(dst, &payload, &mut out);
        let (a, p) = decode_udp(&out).unwrap();
        prop_assert_eq!(a, dst);
        prop_assert_eq!(p, &payload[..]);
    }
}

#[tokio::test]
async fn none_kind_dials_direct() {
    let echo = echo_server().await;
    let c = ProxyConfig::default();
    assert_eq!(c.kind, ProxyKind::None);
    let mut s = connect_tcp(&c, ProxyTarget::Addr(echo), T).await.unwrap();
    check_echo(&mut s).await;
    let mut s = connect_tcp(&c, ProxyTarget::Host("localhost".into(), echo.port()), T)
        .await
        .unwrap();
    check_echo(&mut s).await;
}

#[test]
fn debug_hides_the_password() {
    let mut c = ProxyConfig::default();
    c.kind = ProxyKind::Socks5;
    c.host = "proxy.example".into();
    c.username = Some("alice".into());
    c.password = Some("hunter2".into());
    let d = format!("{c:?}");
    assert!(!d.contains("hunter2"), "{d}");
    assert!(d.contains(r#"password: Some("***")"#), "{d}");
    assert!(d.contains("alice"), "{d}");
}

#[test]
fn defaults_and_validation() {
    let c = ProxyConfig::default();
    assert_eq!(c.port, 1080);
    assert!(c.proxy_peers && c.proxy_trackers && c.proxy_hostnames);
    assert!(c.validate().is_ok());
    let mut c = c;
    c.kind = ProxyKind::Http;
    assert!(c.validate().is_err(), "empty host");
    c.host = "127.0.0.1".into();
    assert!(c.validate().is_ok());
    c.port = 0;
    assert!(c.validate().is_err(), "port 0");
}

#[test]
fn credential_lengths_are_validated() {
    let mut c = ProxyConfig::default();
    c.kind = ProxyKind::Socks5;
    c.host = "127.0.0.1".into();
    let long = "u".repeat(256);
    let max = "u".repeat(255);
    // SOCKS5 (RFC 1929): a username needs a password, each 1 to 255 bytes.
    for (user, pass, ok) in [
        (None, None, true),
        (Some("a"), Some("b"), true),
        (Some(max.as_str()), Some(max.as_str()), true),
        (Some(""), Some("b"), false),
        (Some("a"), None, false),
        (Some("a"), Some(""), false),
        (Some(long.as_str()), Some("b"), false),
        (Some("a"), Some(long.as_str()), false),
    ] {
        c.username = user.map(String::from);
        c.password = pass.map(String::from);
        assert_eq!(c.validate().is_ok(), ok, "socks5 {user:?}/{pass:?}");
    }
    // HTTP Basic: no `:` in the username; an unset password is empty.
    c.kind = ProxyKind::Http;
    for (user, pass, ok) in [
        (Some("a"), None, true),
        (Some("a"), Some("b:c"), true),
        (Some("a:b"), Some("c"), false),
    ] {
        c.username = user.map(String::from);
        c.password = pass.map(String::from);
        assert_eq!(c.validate().is_ok(), ok, "http {user:?}/{pass:?}");
    }
}

#[tokio::test]
async fn http_interim_responses_are_skipped() {
    let echo = echo_server().await;
    let proxy = FakeHttpProxy::start(HttpScript::Interim(2)).await;
    let c = cfg(ProxyKind::Http, proxy.addr());
    let mut s = connect_tcp(&c, ProxyTarget::Addr(echo), T).await.unwrap();
    check_echo(&mut s).await;
}

#[tokio::test]
async fn endless_interim_responses_are_capped() {
    let proxy = FakeHttpProxy::start(HttpScript::Interim(u32::MAX)).await;
    let c = cfg(ProxyKind::Http, proxy.addr());
    let target = ProxyTarget::Addr(SocketAddr::from(([127, 0, 0, 1], 9)));
    let start = Instant::now();
    let err = connect_tcp(&c, target, T).await.unwrap_err();
    assert!(matches!(err, ProxyError::Protocol(_)), "{err:?}");
    assert!(start.elapsed() < T);
}

#[tokio::test]
async fn overlong_username_is_a_protocol_error() {
    let proxy = FakeSocks5::start(Script::RequireAuth("a".into(), "b".into())).await;
    let mut c = cfg(ProxyKind::Socks5, proxy.addr());
    c.username = Some("u".repeat(256));
    let target = ProxyTarget::Addr(SocketAddr::from(([127, 0, 0, 1], 9)));
    let err = connect_tcp(&c, target, T).await.unwrap_err();
    assert!(matches!(err, ProxyError::Protocol(_)), "{err:?}");
}

#[tokio::test]
async fn audit_records_direct_and_proxied_dials() {
    use fbt_core::audit::{self, Outbound};
    let listener = echo_server().await;
    let proxy = FakeSocks5::start(Script::Ok).await;
    let mut proxied = cfg(ProxyKind::Socks5, proxy.addr());
    proxied.proxy_hostnames = true;
    let target = ProxyTarget::Host("example.invalid".into(), 80);

    // The capture is process-wide and parallel tests dial their own proxies:
    // assert the records only this test can make (its listener and its fake
    // are unknown to them), and the absence of the lookup this dial must not
    // make.
    let cap = audit::capture();
    let mut s = connect_tcp(
        &cfg(ProxyKind::None, listener),
        ProxyTarget::Addr(listener),
        T,
    )
    .await
    .unwrap();
    check_echo(&mut s).await;
    let recs = cap.take();
    drop(cap);
    assert!(
        recs.contains(&Outbound::TcpConnect(listener)),
        "direct dial to the address: {recs:?}"
    );

    let cap = audit::capture();
    // The fake cannot dial example.invalid, so the tunnel is refused: the
    // audit is about what we did here, not what the proxy did there.
    let err = connect_tcp(&proxied, target, T).await.unwrap_err();
    let recs = cap.take();
    drop(cap);
    assert!(
        matches!(err, ProxyError::Refused(_) | ProxyError::Timeout),
        "{err:?}"
    );
    assert!(
        recs.contains(&Outbound::TcpConnect(proxy.addr())),
        "proxied dial to the proxy: {recs:?}"
    );
    assert!(
        !recs
            .iter()
            .any(|o| matches!(o, Outbound::Lookup(h) if h == "example.invalid")),
        "the proxy resolves the target name, not us: {recs:?}"
    );
}

/// A closed port is no proxy at all: the dial is what failed, so both the
/// connect and the probe report a reachability failure, which is not the
/// timeout of a proxy that accepted and went quiet.
#[tokio::test]
async fn dial_to_closed_port_is_unreachable() {
    let cfg = socks5(closed_port());
    assert!(matches!(
        connect_tcp(&cfg, addr_target(), T).await,
        Err(ProxyError::Unreachable(_))
    ));
    assert!(matches!(
        probe(&cfg, T).await,
        Err(ProxyError::Unreachable(_))
    ));
}

/// A proxy that wants credentials and gets the wrong ones is reachable: the
/// probe reports the login, not a reachability failure, and the right
/// password goes through.
#[tokio::test]
async fn probe_reports_login_refused() {
    let (_fake, mut c) = socks5_fake(Script::RequireAuth("u".into(), "p".into())).await;
    c.username = Some("u".into());
    c.password = Some("wrong".into());
    assert!(matches!(probe(&c, T).await, Err(ProxyError::AuthFailed)));
    c.password = Some("p".into());
    probe(&c, T).await.unwrap();
}

/// An HTTP proxy has no login check without a CONNECT, so a probe dials and
/// stops there: a listener that accepts and never answers still probes fine,
/// nothing is ever CONNECTed, and a port nothing listens on is still a
/// reachability failure.
#[tokio::test]
async fn http_probe_dials_only() {
    let fake = FakeHttpProxy::start(HttpScript::Ok).await;
    probe(&cfg(ProxyKind::Http, fake.addr()), T).await.unwrap();
    assert!(fake.connects().is_empty());
    probe(&cfg(ProxyKind::Http, silent_listener().await), T)
        .await
        .unwrap();
    assert!(matches!(
        probe(&cfg(ProxyKind::Http, fake_http_proxy::closed_port()), T).await,
        Err(ProxyError::Unreachable(_))
    ));
}

/// A proxy that greeted and authenticated us and then refused the association
/// is reachable: only its UDP relay is missing, which is a property of the
/// proxy and not something a retry fixes. A dial failure and a proxy that went
/// quiet after accepting are not that.
#[tokio::test]
async fn udp_associate_refused_is_not_relayed() {
    let (_fake, c) = socks5_fake(Script::NoUdp(7)).await;
    let e = udp_associate(&c, T).await.unwrap_err();
    assert!(matches!(e, ProxyError::Refused(7)), "{e:?}");
    assert!(udp_not_relayed(&e));
    let e = udp_associate(&socks5(silent_listener().await), Duration::from_millis(300))
        .await
        .unwrap_err();
    assert!(matches!(e, ProxyError::Timeout), "{e:?}");
    assert!(!udp_not_relayed(&e));
    let e = udp_associate(&socks5(closed_port()), T).await.unwrap_err();
    assert!(matches!(e, ProxyError::Unreachable(_)), "{e:?}");
    assert!(!udp_not_relayed(&e));
}

/// A datagram to a domain is framed for the proxy to resolve (`ATYP = 3`),
/// and only a name a length byte and the alphabet can carry is ever written.
#[test]
fn encode_udp_host_frames_domain() {
    let mut out = Vec::new();
    encode_udp_host("tracker.invalid", 6969, b"x", &mut out).unwrap();
    assert_eq!(
        out,
        [
            &[0, 0, 0, 3, 15][..],
            b"tracker.invalid",
            &[0x1b, 0x39],
            b"x"
        ]
        .concat()
    );
    for bad in [
        "",
        "a b",
        &"a".repeat(256),
        "a\r\nX-Evil: 1",
        "w\u{fc}rld.example",
        "[::1]",
    ] {
        assert!(
            matches!(
                encode_udp_host(bad, 1, b"", &mut out),
                Err(ProxyError::Protocol(_))
            ),
            "{bad:?}"
        );
    }
    // A rejected name writes nothing: the caller's buffer keeps what it had.
    out.clear();
    out.push(0xaa);
    assert!(encode_udp_host("a b", 1, b"", &mut out).is_err());
    assert_eq!(out, vec![0xaa]);
    // The longest name a length byte can carry is still framed.
    let host = "a".repeat(255);
    encode_udp_host(&host, 1, b"", &mut out).unwrap();
    assert_eq!(out.len(), 4 + 1 + 255 + 2);
}

/// `proxy_only` defaults off, survives a round trip through serde, reads its
/// default from a document that does not name it, shows in `Debug`, and is
/// nothing `validate` looks at.
#[cfg(feature = "serde")]
#[test]
fn proxy_only_defaults_off_and_round_trips() {
    assert!(!ProxyConfig::default().proxy_only);
    let mut c = ProxyConfig::default();
    c.kind = ProxyKind::Socks5;
    c.host = "127.0.0.1".into();
    c.proxy_only = true;
    assert!(c.validate().is_ok(), "proxy_only is not validated");
    assert!(
        format!("{c:?}").contains("proxy_only: true"),
        "Debug shows it: {c:?}"
    );
    let json = serde_json::to_string(&c).unwrap();
    assert!(json.contains("\"proxy_only\":true"), "{json}");
    assert_eq!(serde_json::from_str::<ProxyConfig>(&json).unwrap(), c);
    let mut doc = serde_json::to_value(ProxyConfig::default()).unwrap();
    assert_eq!(doc.get("proxy_only"), Some(&false.into()), "{doc}");
    doc.as_object_mut().unwrap().remove("proxy_only");
    assert!(
        !serde_json::from_value::<ProxyConfig>(doc)
            .unwrap()
            .proxy_only,
        "a document without the key reads the default"
    );
}
