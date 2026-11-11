use std::time::Duration;

use fbt_net::MapProtocol;
use fbt_net::upnp::{HttpError, SsdpError, UpnpError, discover, discover_with};

#[path = "support/fake_igd.rs"]
mod fake_igd;
use fake_igd::{Description, EXTERNAL_IP, FakeIgd, Script};

const WAIT: Duration = Duration::from_secs(3);

#[tokio::test]
async fn discover_and_map_tcp_and_udp() {
    let igd = FakeIgd::start(Script::default()).await;
    let dev = discover(igd.ssdp, WAIT).await.unwrap();
    assert_eq!(
        dev.add_mapping(MapProtocol::Tcp, 6881, 3600).await.unwrap(),
        6881
    );
    assert_eq!(
        dev.add_mapping(MapProtocol::Udp, 6881, 3600).await.unwrap(),
        6881
    );
    let adds = igd.recorded("AddPortMapping");
    assert_eq!(adds.len(), 2);
    assert_eq!(adds[0].arg("NewProtocol"), "TCP");
    assert_eq!(adds[1].arg("NewProtocol"), "UDP");
    assert_eq!(adds[0].arg("NewInternalPort"), "6881");
    assert_eq!(adds[0].arg("NewInternalClient"), "127.0.0.1");
    assert_eq!(adds[0].arg("NewLeaseDuration"), "3600");
    assert_eq!(adds[0].arg("NewRemoteHost"), "");
    assert_eq!(adds[0].arg("NewEnabled"), "1");
    assert_eq!(adds[0].arg("NewPortMappingDescription"), "fbt-lib");
}

#[tokio::test]
async fn conflict_picks_another_port() {
    let igd = FakeIgd::start(Script {
        conflict_ports: vec![6881],
        ..Script::default()
    })
    .await;
    let dev = discover(igd.ssdp, WAIT).await.unwrap();
    let ext = dev.add_mapping(MapProtocol::Tcp, 6881, 3600).await.unwrap();
    assert_ne!(ext, 6881);
    assert!(ext >= 1025);
    let adds = igd.recorded("AddPortMapping");
    assert_eq!(adds.len(), 2);
    assert_eq!(adds[0].arg("NewExternalPort"), "6881");
    assert_eq!(adds[1].arg("NewExternalPort"), ext.to_string());
    assert_eq!(adds[1].arg("NewInternalPort"), "6881");
}

#[tokio::test]
async fn permanent_only_retries_with_zero_lease() {
    let igd = FakeIgd::start(Script {
        permanent_only: true,
        ..Script::default()
    })
    .await;
    let dev = discover(igd.ssdp, WAIT).await.unwrap();
    assert_eq!(
        dev.add_mapping(MapProtocol::Tcp, 6881, 3600).await.unwrap(),
        6881
    );
    let adds = igd.recorded("AddPortMapping");
    assert_eq!(adds.len(), 2);
    assert_eq!(adds[0].arg("NewLeaseDuration"), "3600");
    assert_eq!(adds[1].arg("NewLeaseDuration"), "0");
}

#[tokio::test]
async fn external_ip_reported() {
    let igd = FakeIgd::start(Script::default()).await;
    let dev = discover(igd.ssdp, WAIT).await.unwrap();
    assert_eq!(dev.external_ip().await.unwrap().to_string(), EXTERNAL_IP);
}

#[tokio::test]
async fn delete_on_request() {
    let igd = FakeIgd::start(Script::default()).await;
    let dev = discover(igd.ssdp, WAIT).await.unwrap();
    dev.delete_mapping(MapProtocol::Udp, 6881).await.unwrap();
    let dels = igd.recorded("DeletePortMapping");
    assert_eq!(dels.len(), 1);
    assert_eq!(dels[0].arg("NewExternalPort"), "6881");
    assert_eq!(dels[0].arg("NewProtocol"), "UDP");
}

#[tokio::test]
async fn oversized_or_slow_description_is_cut() {
    let big = FakeIgd::start(Script {
        description: Description::Oversized,
        ..Script::default()
    })
    .await;
    let r = discover_with(big.ssdp, WAIT, Duration::from_millis(300)).await;
    assert!(
        matches!(r, Err(UpnpError::Http(HttpError::TooLarge))),
        "{r:?}"
    );

    let slow = FakeIgd::start(Script {
        description: Description::Slow,
        ..Script::default()
    })
    .await;
    let r = discover_with(slow.ssdp, WAIT, Duration::from_millis(300)).await;
    assert!(
        matches!(r, Err(UpnpError::Http(HttpError::Timeout))),
        "{r:?}"
    );
}

#[tokio::test]
async fn foreign_location_is_ignored() {
    if tokio::net::UdpSocket::bind("[::1]:0").await.is_err() {
        return; // no IPv6 loopback on this host
    }
    // The reply comes from [::1] but names 127.0.0.1, where the fake counts
    // connections: only the foreign-host check keeps us away from it.
    let igd = FakeIgd::start(Script {
        foreign_location: true,
        ..Script::default()
    })
    .await;
    let r = discover_with(igd.ssdp, Duration::from_secs(2), Duration::from_millis(50)).await;
    assert!(
        matches!(r, Err(UpnpError::Ssdp(SsdpError::Foreign))),
        "{r:?}"
    );
    assert_eq!(
        igd.http_connections
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}
