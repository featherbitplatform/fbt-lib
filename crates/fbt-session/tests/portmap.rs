//! Port mapping and the external address, against the loopback NAT-PMP/PCP
//! and IGD fakes of `fbt-net` (never a real router).

mod support;

#[path = "../../fbt-net/tests/support/fake_igd.rs"]
mod fake_igd;
#[path = "../../fbt-net/tests/support/fake_natpmp.rs"]
mod fake_natpmp;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use fbt_session::{
    AddTorrent, Event, EventMask, ExternalIpSource, MapMethod, MapProtocol, Session, Settings,
    TorrentState,
};
use fbt_wire::{ExtHandshake, Handshake, MessageCodec, Reserved, read_handshake, write_handshake};
use tokio::net::TcpSocket;
use tokio_util::codec::Framed;

use fake_igd::{FakeIgd, Script};
use fake_natpmp::{FakeGateway, Mode};
use support::{ScriptedPeer, make_torrent, next_matching, test_settings, wait_state};

const T: Duration = Duration::from_secs(10);

/// Both methods on, aimed at the fakes through the hidden settings.
fn nat_settings(gw: &FakeGateway, igd: &FakeIgd) -> Settings {
    let mut s = test_settings();
    s.enable_upnp = true;
    s.enable_natpmp = true;
    s.nat_gateway = Some(Ipv4Addr::LOCALHOST);
    s.natpmp_port = gw.port;
    s.upnp_ssdp_addr = igd.ssdp;
    s
}

#[tokio::test]
async fn session_maps_its_ports_and_reports_events() {
    let gw = FakeGateway::start(Mode::Pcp, false, 0).await;
    let igd = FakeIgd::start(Script::default()).await;
    let session = Session::new(nat_settings(&gw, &igd)).await.unwrap();
    let mut events = session.subscribe(EventMask::NAT);
    let port = session.local_addr().port();
    let ext_ip: Ipv4Addr = fake_igd::EXTERNAL_IP.parse().unwrap();
    assert_eq!(ext_ip, Ipv4Addr::from(fake_natpmp::EXTERNAL_IP));

    let mut left = vec![
        (MapProtocol::Tcp, MapMethod::Pcp),
        (MapProtocol::Udp, MapMethod::Pcp),
        (MapProtocol::Tcp, MapMethod::Upnp),
        (MapProtocol::Udp, MapMethod::Upnp),
    ];
    while !left.is_empty() {
        let ev = next_matching(&mut events, |e| matches!(e, Event::PortMapped { .. }), T)
            .await
            .unwrap_or_else(|| panic!("no PortMapped for {left:?}"));
        let Event::PortMapped {
            protocol,
            method,
            external,
            ..
        } = ev
        else {
            unreachable!()
        };
        assert_eq!(external.ip(), IpAddr::V4(ext_ip));
        // The PCP fake grants the next port up; the IGD the one asked for.
        let want_port = if method == MapMethod::Upnp {
            port
        } else {
            port + 1
        };
        assert_eq!(external.port(), want_port, "{method:?}");
        left.retain(|w| *w != (protocol, method));
    }
    assert_eq!(session.external_ip(), Some(IpAddr::V4(ext_ip)));

    // The UPnP mappings name our port, both protocols.
    let adds = igd.recorded("AddPortMapping");
    for proto in ["TCP", "UDP"] {
        assert!(
            adds.iter()
                .any(|r| r.arg("NewProtocol") == proto
                    && r.arg("NewInternalPort") == port.to_string()),
            "{proto}"
        );
    }

    let started = std::time::Instant::now();
    session.shutdown().await;
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(igd.recorded("DeletePortMapping").len(), 2);
}

#[tokio::test]
async fn no_mapping_with_nat_disabled() {
    let gw = FakeGateway::start(Mode::Pcp, false, 0).await;
    let igd = FakeIgd::start(Script::default()).await;
    let mut s = nat_settings(&gw, &igd);
    s.enable_upnp = false;
    s.enable_natpmp = false;
    let session = Session::new(s).await.unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(gw.request_count(), 0);
    assert!(igd.searches.lock().unwrap().is_empty());
    assert_eq!(
        igd.http_connections
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(session.external_ip(), None);
    session.shutdown().await;
}

/// Connects from `src` to `to` and exchanges handshakes advertising BEP 10;
/// `None` where `src` cannot be bound (127.0.0.2 on macOS, say).
async fn connect_from(src: Ipv4Addr, to: SocketAddr, info_hash: [u8; 20]) -> Option<ScriptedPeer> {
    let sock = TcpSocket::new_v4().ok()?;
    sock.bind(SocketAddr::from((src, 0))).ok()?;
    let mut stream = tokio::time::timeout(T, sock.connect(to)).await.ok()?.ok()?;
    let mut reserved = Reserved::default();
    reserved.set_extended();
    let ours = Handshake {
        reserved,
        info_hash,
        peer_id: fbt_core::PeerId::generate(),
    };
    write_handshake(&mut stream, &ours).await.unwrap();
    let remote = tokio::time::timeout(T, read_handshake(&mut stream))
        .await
        .expect("handshake in time")
        .expect("remote handshake");
    Some(ScriptedPeer {
        framed: Framed::new(stream, MessageCodec::new(1 << 20)),
        remote,
    })
}

#[tokio::test]
async fn yourip_votes_set_external_ip() {
    let reporters = [
        Ipv4Addr::new(127, 0, 0, 1),
        Ipv4Addr::new(127, 0, 0, 2),
        Ipv4Addr::new(127, 0, 0, 3),
    ];
    // 127.0.0.2 and .3 route only where all of 127/8 is loopback (Linux,
    // Windows); elsewhere this test has nothing to check.
    for ip in &reporters[1..] {
        let probe = TcpSocket::new_v4().unwrap();
        if probe.bind(SocketAddr::from((*ip, 0))).is_err() {
            return;
        }
    }
    let (m, _) = make_torrent(&[("a", 64 * 1024)], 16 * 1024);
    let session = Session::new(test_settings()).await.unwrap();
    let mut events = session.subscribe(EventMask::NAT);
    let dir = tempfile::tempdir().unwrap();
    let h = session
        .add_torrent(AddTorrent::new(m.clone(), dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);

    let yourip = Ipv4Addr::new(203, 0, 113, 7);
    let mut peers = Vec::new();
    for ip in reporters {
        let Some(mut p) = connect_from(ip, session.local_addr(), m.info_hashes.wire_hash()).await
        else {
            return; // the address binds but does not route here
        };
        let hs = ExtHandshake {
            your_ip: Some(IpAddr::V4(yourip)),
            ..ExtHandshake::default()
        };
        p.send_ext_handshake(&hs).await;
        peers.push(p);
    }
    let ev = next_matching(
        &mut events,
        |e| matches!(e, Event::ExternalIpChanged { .. }),
        T,
    )
    .await;
    assert_eq!(
        ev,
        Some(Event::ExternalIpChanged {
            ip: IpAddr::V4(yourip),
            source: ExternalIpSource::Peers,
        })
    );
    assert_eq!(session.external_ip(), Some(IpAddr::V4(yourip)));
    session.shutdown().await;
}
