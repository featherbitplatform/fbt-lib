use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use fbt_core::udp::DatagramSender;
use fbt_net::lsd::{LSD_V4, Lsd, LsdConfig, LsdMessage};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

fn cfg(cookie: &str) -> LsdConfig {
    let mut c = LsdConfig::default();
    c.cookie = cookie.to_owned();
    c
}

#[tokio::test]
async fn two_instances_find_each_other_via_injected_transport() {
    // Two sockets we own; each Lsd announces to the other's socket and the
    // receive loop feeds the other's handler.
    let std_a = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let std_b = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let (aa, ab) = (std_a.local_addr().unwrap(), std_b.local_addr().unwrap());
    let (tx_sa, tx_sb) = (std_a.try_clone().unwrap(), std_b.try_clone().unwrap());
    std_a.set_nonblocking(true).unwrap();
    std_b.set_nonblocking(true).unwrap();
    let sa = UdpSocket::from_std(std_a).unwrap();
    let sb = UdpSocket::from_std(std_b).unwrap();
    let (tx_a, mut rx_a) = mpsc::channel(16);
    let (tx_b, mut rx_b) = mpsc::channel(16);

    #[derive(Debug)]
    struct Tx(std::net::UdpSocket, SocketAddr);
    impl DatagramSender for Tx {
        fn local_addr(&self) -> SocketAddr {
            self.1
        }
        fn send_to(&self, data: &[u8], to: SocketAddr) -> std::io::Result<usize> {
            self.0.send_to(data, to)
        }
    }
    let (lsd_a, h_a) = Lsd::spawn_on(
        cfg("aaaaaaaa"),
        vec![(Arc::new(Tx(tx_sa, aa)) as Arc<dyn DatagramSender>, ab)],
        tx_a,
    );
    let (lsd_b, h_b) = Lsd::spawn_on(
        cfg("bbbbbbbb"),
        vec![(Arc::new(Tx(tx_sb, ab)) as Arc<dyn DatagramSender>, aa)],
        tx_b,
    );
    for (s, h) in [(sa, h_a), (sb, h_b)] {
        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            while let Ok((n, from)) = s.recv_from(&mut buf).await {
                h.handle(from, &buf[..n]);
            }
        });
    }
    lsd_a.announce(6881, &[[1; 20], [2; 20]]);
    lsd_b.announce(7000, &[[1; 20]]);
    let got_b = tokio::time::timeout(Duration::from_secs(5), rx_b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got_b.info_hash, [1; 20]);
    assert_eq!(got_b.addr, SocketAddr::new(aa.ip(), 6881));
    let got_a = tokio::time::timeout(Duration::from_secs(5), rx_a.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got_a.addr, SocketAddr::new(ab.ip(), 7000));
    // the second hash of A's message arrives too
    let second = tokio::time::timeout(Duration::from_secs(5), rx_b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.info_hash, [2; 20]);
    lsd_a.shutdown().await;
    lsd_b.shutdown().await;
}

#[tokio::test]
async fn lsd_flood_is_rate_limited() {
    let (tx, mut rx) = mpsc::channel(4096);
    let (_lsd, handler) = Lsd::spawn_on(cfg("flood"), Vec::new(), tx);
    let from: SocketAddr = "10.9.9.9:4000".parse().unwrap();
    let msg = LsdMessage {
        port: 6881,
        info_hashes: vec![[3; 20]],
        cookie: None,
    }
    .encode(LSD_V4);
    for _ in 0..1000 {
        handler.handle(from, &msg);
    }
    let mut n = 0;
    while rx.try_recv().is_ok() {
        n += 1;
    }
    assert!((1..=6).contains(&n), "got {n}");
    // another IP has its own budget
    handler.handle("10.9.9.8:4000".parse().unwrap(), &msg);
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn real_multicast_loopback() {
    let group = SocketAddr::from((
        Ipv4Addr::new(239, 255, rand::random::<u8>(), rand::random::<u8>()),
        20_000 + rand::random::<u16>() % 20_000,
    ));
    let mk = |cookie: &str| {
        let mut c = cfg(cookie);
        c.group_v4 = Some(group);
        c.group_v6 = None;
        c.interface_v4 = Ipv4Addr::LOCALHOST;
        c.multicast_ttl = Some(0);
        c
    };
    let (tx_a, _rx_a) = mpsc::channel(16);
    let (tx_b, mut rx_b) = mpsc::channel(16);
    let (a, b) = match (
        Lsd::spawn(mk("aaaaaaaa"), tx_a),
        Lsd::spawn(mk("bbbbbbbb"), tx_b),
    ) {
        (Ok(a), Ok(b)) => (a, b),
        (a, b) => {
            eprintln!("skipped: cannot join {group}: {:?} {:?}", a.err(), b.err());
            return;
        }
    };
    a.announce(6881, &[[9; 20]]);
    let got = tokio::time::timeout(Duration::from_secs(5), rx_b.recv())
        .await
        .expect("looped-back multicast arrives")
        .expect("channel open");
    assert_eq!(got.info_hash, [9; 20]);
    assert_eq!(got.addr.port(), 6881);
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn shutdown_releases_the_port() {
    let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let bind = probe.local_addr().unwrap();
    drop(probe);
    let (tx, _rx) = mpsc::channel(4);
    let lsd = Lsd::spawn_unicast(cfg("port"), bind, vec![bind], tx).unwrap();
    assert!(
        std::net::UdpSocket::bind(bind).is_err(),
        "held while running"
    );
    lsd.shutdown().await;
    assert!(
        std::net::UdpSocket::bind(bind).is_ok(),
        "free after shutdown"
    );
}
