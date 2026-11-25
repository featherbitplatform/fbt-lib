use std::net::{SocketAddr, UdpSocket};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fbt_core::udp::DatagramHandler;
use fbt_net::demux::{DemuxStats, PacketKind, SharedUdpSocket};

#[path = "support/fake_socks5.rs"]
mod fake_socks5;

#[derive(Debug, Default)]
struct Recorder(Mutex<Vec<(SocketAddr, Vec<u8>)>>);

impl DatagramHandler for Recorder {
    fn handle(&self, from: SocketAddr, data: &[u8]) {
        if let Ok(mut v) = self.0.lock() {
            v.push((from, data.to_vec()));
        }
    }
}

impl Recorder {
    fn count(&self) -> usize {
        self.0.lock().map(|v| v.len()).unwrap_or(0)
    }
}

async fn wait_for(mut cond: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + Duration::from_secs(5);
    while Instant::now() < end {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cond()
}

fn utp_syn() -> [u8; 20] {
    let mut d = [0u8; 20];
    d[0] = 0x41;
    d
}

const LO: &str = "127.0.0.1:0";

fn lo() -> SocketAddr {
    LO.parse().unwrap()
}

#[tokio::test]
async fn routes_each_kind_to_its_handler() {
    let s = SharedUdpSocket::bind(lo()).unwrap();
    let (u, d, t) = (
        Arc::new(Recorder::default()),
        Arc::new(Recorder::default()),
        Arc::new(Recorder::default()),
    );
    s.set_handler(PacketKind::Utp, u.clone());
    s.set_handler(PacketKind::Dht, d.clone());
    s.set_handler(PacketKind::Tracker, t.clone());
    let raw = UdpSocket::bind(lo()).unwrap();
    raw.send_to(&utp_syn(), s.local_addr()).unwrap();
    raw.send_to(b"d1:y1:qe", s.local_addr()).unwrap();
    raw.send_to(&[0, 0, 0, 1, 0, 0, 0, 7], s.local_addr())
        .unwrap();
    assert!(wait_for(|| u.count() == 1 && d.count() == 1 && t.count() == 1).await);
    let from = u.0.lock().unwrap()[0].0;
    assert_eq!(from, raw.local_addr().unwrap());
    let st = s.stats();
    assert_eq!((st.utp, st.dht, st.tracker, st.dropped), (1, 1, 1, 0));
}

#[tokio::test]
async fn unknown_and_unhandled_are_dropped() {
    let s = SharedUdpSocket::bind(lo()).unwrap();
    let u = Arc::new(Recorder::default());
    s.set_handler(PacketKind::Utp, u.clone());
    let raw = UdpSocket::bind(lo()).unwrap();
    raw.send_to(b"x", s.local_addr()).unwrap();
    raw.send_to(b"d1:y1:qe", s.local_addr()).unwrap(); // no DHT handler
    raw.send_to(&utp_syn(), s.local_addr()).unwrap();
    assert!(wait_for(|| u.count() == 1).await);
    assert!(wait_for(|| s.stats().dropped == 2).await);
    assert_eq!(s.stats().utp, 1);
    assert_eq!(s.stats().dht, 0);
}

#[tokio::test]
async fn sender_shares_the_port() {
    let s = SharedUdpSocket::bind(lo()).unwrap();
    let raw = UdpSocket::bind(lo()).unwrap();
    raw.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let tx = s.sender();
    assert_eq!(tx.local_addr(), s.local_addr());
    tx.send_to(b"reply", raw.local_addr().unwrap()).unwrap();
    let mut buf = [0u8; 16];
    let (n, from) = raw.recv_from(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"reply");
    assert_eq!(from, s.local_addr());
}

#[tokio::test]
async fn ipv6_socket_is_v6_only() {
    let Ok(v6) = SharedUdpSocket::bind("[::]:0".parse().unwrap()) else {
        return; // no IPv6 on this host
    };
    let port = v6.local_addr().port();
    let v4 = SharedUdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], port)));
    assert!(
        v4.is_ok(),
        "IPv4 must share the port of an IPv6-only socket"
    );
}

#[tokio::test]
async fn close_stops_delivery() {
    let s = SharedUdpSocket::bind(lo()).unwrap();
    let u = Arc::new(Recorder::default());
    s.set_handler(PacketKind::Utp, u.clone());
    let raw = UdpSocket::bind(lo()).unwrap();
    raw.send_to(&utp_syn(), s.local_addr()).unwrap();
    assert!(wait_for(|| u.count() == 1).await);
    s.close();
    s.close(); // idempotent
    tokio::time::sleep(Duration::from_millis(100)).await;
    raw.send_to(&utp_syn(), s.local_addr()).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(u.count(), 1);
    let st: DemuxStats = s.stats();
    assert_eq!((st.utp, st.dht, st.tracker, st.dropped), (1, 0, 0, 0));
}

#[tokio::test]
async fn close_releases_the_port_and_fails_sends() {
    let s = SharedUdpSocket::bind(lo()).unwrap();
    let tx = s.sender();
    let addr = s.local_addr();
    s.close();
    let to = UdpSocket::bind(lo()).unwrap().local_addr().unwrap();
    let err = tx.send_to(b"x", to).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::NotConnected);
    assert!(
        wait_for(|| UdpSocket::bind(addr).is_ok()).await,
        "the port must be free after close"
    );
}

#[tokio::test]
async fn relayed_send_records_the_relay_only() {
    use fbt_core::audit::{self, Outbound};
    use fbt_net::proxy::{ProxyConfig, ProxyKind, udp_associate};

    let proxy = fake_socks5::FakeSocks5::start(fake_socks5::Script::Ok).await;
    let mut cfg = ProxyConfig::default();
    cfg.kind = ProxyKind::Socks5;
    cfg.host = proxy.addr().ip().to_string();
    cfg.port = proxy.addr().port();
    let assoc = udp_associate(&cfg, Duration::from_secs(5)).await.unwrap();
    let relay = assoc.relay();

    let sock = SharedUdpSocket::bind_relayed(lo(), assoc).unwrap();
    let tx = sock.sender();
    let nominal: SocketAddr = "192.0.2.1:6881".parse().unwrap();

    // The datagram goes to the relay framed; the audit sees the relay only.
    // Parallel tests may add their own records; the relay and the nominal
    // target are known only to this test.
    let cap = audit::capture();
    tx.send_to(b"x", nominal).unwrap();
    let recs = cap.take();
    drop(cap);
    assert!(recs.contains(&Outbound::UdpSend(relay)), "{recs:?}");
    assert!(
        !recs
            .iter()
            .any(|o| matches!(o, Outbound::UdpSend(a) if *a == nominal)),
        "a relayed send is recorded to the relay, not the nominal target: {recs:?}"
    );
}

#[tokio::test]
async fn relayed_socket_sends_to_host() {
    use fbt_core::audit::{self, Outbound};
    use fbt_net::proxy::{ProxyConfig, ProxyKind, udp_associate};

    let proxy = fake_socks5::FakeSocks5::start(fake_socks5::Script::RecordOnly).await;
    let mut cfg = ProxyConfig::default();
    cfg.kind = ProxyKind::Socks5;
    cfg.host = proxy.addr().ip().to_string();
    cfg.port = proxy.addr().port();
    let assoc = udp_associate(&cfg, Duration::from_secs(5)).await.unwrap();
    let relay = assoc.relay();

    let relayed = SharedUdpSocket::bind_relayed(lo(), assoc).unwrap();
    let direct = SharedUdpSocket::bind(lo()).unwrap();

    // The name goes to the relay framed for the proxy to resolve (the fake
    // logs the target it was asked for and never forwards), and the send is
    // recorded to the relay, not to anything the name stands for.
    let cap = audit::capture();
    let sent = relayed
        .sender()
        .send_to_host(b"hi", "tracker.invalid", 6969)
        .unwrap();
    let recs = cap.take();
    drop(cap);
    assert_eq!(sent, 2, "the bytes sent are the payload's");
    assert!(recs.contains(&Outbound::UdpSend(relay)), "{recs:?}");
    assert!(
        wait_for(|| proxy.udp_targets() == vec!["tracker.invalid:6969".to_string()]).await,
        "the relay is asked for the name, not an address: {:?}",
        proxy.udp_targets()
    );

    // A direct socket cannot do this: it would have to resolve the name here.
    let e = direct
        .sender()
        .send_to_host(b"hi", "tracker.invalid", 6969)
        .unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::Unsupported);

    // A name no proxy may be given is refused before anything is sent.
    let e = relayed.sender().send_to_host(b"hi", "a b", 1).unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(
        proxy.udp_targets(),
        vec!["tracker.invalid:6969".to_string()]
    );
}
