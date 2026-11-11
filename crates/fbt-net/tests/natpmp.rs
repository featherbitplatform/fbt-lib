use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use fbt_net::natpmp::{MapMethod, MapProtocol, NatPmpError, map, unmap};

#[path = "support/fake_natpmp.rs"]
mod fake_natpmp;
use fake_natpmp::{EXTERNAL_IP, FakeGateway, Mode};

const LO: Ipv4Addr = Ipv4Addr::LOCALHOST;

#[tokio::test]
async fn pcp_mapping() {
    let gw = FakeGateway::start(Mode::Pcp, false, 0).await;
    let m = map(LO, gw.port, MapProtocol::Tcp, 6881, 7200)
        .await
        .unwrap();
    assert_eq!(m.method, MapMethod::Pcp);
    assert_eq!(m.lifetime, 7200);
    assert_eq!(m.external, SocketAddr::from((EXTERNAL_IP, 6881)));
    assert_eq!(gw.request_count(), 1);
}

#[tokio::test]
async fn pcp_nonce_is_random_per_request() {
    let gw = FakeGateway::start(Mode::Pcp, false, 0).await;
    map(LO, gw.port, MapProtocol::Udp, 6881, 60).await.unwrap();
    map(LO, gw.port, MapProtocol::Udp, 6881, 60).await.unwrap();
    let reqs = gw.requests.lock().unwrap();
    assert_ne!(reqs[0][24..36], reqs[1][24..36]);
}

#[tokio::test]
async fn falls_back_to_natpmp() {
    let gw = FakeGateway::start(Mode::NatPmpOnly, false, 0).await;
    let m = map(LO, gw.port, MapProtocol::Udp, 6881, 7200)
        .await
        .unwrap();
    assert_eq!(m.method, MapMethod::NatPmp);
    assert_eq!(m.external, SocketAddr::from((EXTERNAL_IP, 6881)));
    assert_eq!(m.lifetime, 7200);
    // PCP probe, external-address request, map request.
    assert_eq!(gw.request_count(), 3);
}

#[tokio::test]
async fn reply_from_wrong_source_is_ignored() {
    let gw = FakeGateway::start(Mode::Pcp, true, 0).await;
    let r = map(LO, gw.port, MapProtocol::Tcp, 6881, 7200).await;
    assert!(matches!(r, Err(NatPmpError::Timeout)), "{r:?}");
}

#[tokio::test]
async fn error_result_code_reported() {
    let gw = FakeGateway::start(Mode::Pcp, false, 2).await;
    let r = map(LO, gw.port, MapProtocol::Tcp, 6881, 7200).await;
    assert!(matches!(r, Err(NatPmpError::Result(2))), "{r:?}");
}

#[tokio::test]
async fn silent_gateway_times_out_after_four_attempts() {
    let gw = FakeGateway::start(Mode::Silent, false, 0).await;
    let started = std::time::Instant::now();
    let r = map(LO, gw.port, MapProtocol::Tcp, 6881, 7200).await;
    assert!(matches!(r, Err(NatPmpError::Timeout)), "{r:?}");
    assert_eq!(gw.request_count(), 4);
    assert!(started.elapsed() >= Duration::from_millis(3700));
}

#[tokio::test]
async fn unmap_sends_zero_lifetime() {
    let gw = FakeGateway::start(Mode::NatPmpOnly, false, 0).await;
    unmap(LO, gw.port, MapProtocol::Tcp, 6881).await.unwrap();
    let last = gw.last_request();
    assert_eq!(last.len(), 12);
    assert_eq!(&last[8..12], &[0, 0, 0, 0]);
}
