use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use fbt_net::natpmp::{MapMethod, MapProtocol, NatPmpError, map, renew, unmap};

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
    assert_eq!(m.external, SocketAddr::from((EXTERNAL_IP, 6882)));
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
    assert_eq!(m.external, SocketAddr::from((EXTERNAL_IP, 6882)));
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
async fn natpmp_unmap_sends_zero_lifetime_and_external_port_zero() {
    let gw = FakeGateway::start(Mode::NatPmpOnly, false, 0).await;
    let m = map(LO, gw.port, MapProtocol::Tcp, 6881, 7200)
        .await
        .unwrap();
    unmap(LO, gw.port, &m, MapProtocol::Tcp, 6881)
        .await
        .unwrap();
    let last = gw.last_request();
    assert_eq!(last.len(), 12);
    assert_eq!(&last[6..8], &[0, 0]);
    assert_eq!(&last[8..12], &[0, 0, 0, 0]);
}

#[tokio::test]
async fn pcp_nonce_persists_across_map_renew_and_unmap() {
    let gw = FakeGateway::start(Mode::Pcp, false, 0).await;
    let m = map(LO, gw.port, MapProtocol::Tcp, 6881, 7200)
        .await
        .unwrap();
    assert_eq!(m.epoch, 9);
    let r = renew(LO, gw.port, &m, MapProtocol::Tcp, 6881, 7200)
        .await
        .unwrap();
    assert_eq!(r.nonce, m.nonce);
    unmap(LO, gw.port, &r, MapProtocol::Tcp, 6881)
        .await
        .unwrap();
    let reqs = gw.requests.lock().unwrap();
    assert_eq!(reqs.len(), 3);
    for q in reqs.iter() {
        assert_eq!(q[24..36], m.nonce);
    }
    // The renewal suggests the port we were granted; the delete has lifetime 0.
    assert_eq!(&reqs[1][42..44], &6882u16.to_be_bytes());
    assert_eq!(&reqs[2][4..8], &[0, 0, 0, 0]);
}

#[tokio::test]
async fn natpmp_renew_skips_pcp_probe() {
    let gw = FakeGateway::start(Mode::NatPmpOnly, false, 0).await;
    let m = map(LO, gw.port, MapProtocol::Udp, 6881, 7200)
        .await
        .unwrap();
    let before = gw.request_count();
    let r = renew(LO, gw.port, &m, MapProtocol::Udp, 6881, 3600)
        .await
        .unwrap();
    assert_eq!(r.method, MapMethod::NatPmp);
    // External-address request and map request only.
    assert_eq!(gw.request_count() - before, 2);
    assert_ne!(gw.last_request()[0..2], [2, 1]);
}

#[tokio::test]
async fn stale_unsupported_reply_does_not_abort_fallback() {
    let gw = FakeGateway::start_stale(Mode::NatPmpOnly).await;
    let m = map(LO, gw.port, MapProtocol::Udp, 6881, 7200)
        .await
        .unwrap();
    assert_eq!(m.method, MapMethod::NatPmp);
    assert_eq!(m.external.port(), 6882);
}
