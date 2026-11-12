//! Loopback swarms of real nodes. Nothing here contacts a public DHT node.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use fbt_core::udp::{DatagramHandler, DatagramSender};
use fbt_dht::{Body, Dht, DhtConfig, MAX_STATE_NODES, Message, Query, QueryMsg};
use tokio::net::UdpSocket;
use tokio::time::{Instant, sleep, timeout};

fn cfg(bind: &str) -> DhtConfig {
    let mut c = DhtConfig::default();
    let bind: SocketAddr = bind.parse().unwrap();
    if bind.is_ipv4() {
        c.bind_v4 = Some(bind);
        c.bind_v6 = None;
    } else {
        c.bind_v4 = None;
        c.bind_v6 = Some(bind);
    }
    c.query_rate_per_ip = 10_000;
    c.query_timeout = Duration::from_millis(500);
    c
}

fn has_ipv6() -> bool {
    std::net::UdpSocket::bind("[::1]:0").is_ok()
}

/// Node 0 plus `n - 1` nodes bootstrapping from it.
async fn swarm(n: usize, bind: &str) -> Vec<Dht> {
    let first = Dht::spawn(cfg(bind)).await.unwrap();
    let seed = first.local_addrs()[0].to_string();
    let mut nodes = vec![first];
    for _ in 1..n {
        let mut c = cfg(bind);
        c.bootstrap = vec![seed.clone()];
        nodes.push(Dht::spawn(c).await.unwrap());
    }
    nodes
}

async fn wait_until(limit: Duration, mut ok: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + limit;
    while Instant::now() < end {
        if ok() {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    ok()
}

async fn shutdown(nodes: &[Dht]) {
    for n in nodes {
        n.shutdown().await;
    }
}

async fn find(dht: &Dht, ih: [u8; 20], want: SocketAddr) -> bool {
    let mut lookup = dht.get_peers(ih);
    while let Some(batch) = lookup.next().await {
        if batch.contains(&want) {
            return true;
        }
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn twenty_nodes_bootstrap_and_fill_tables() {
    let nodes = swarm(21, "127.0.0.1:0").await;
    let filled = wait_until(Duration::from_secs(10), || {
        nodes.iter().all(|n| {
            let s = n.stats();
            s.nodes_v4 >= 8 && s.bootstrapped
        })
    })
    .await;
    let counts: Vec<usize> = nodes.iter().map(|n| n.stats().nodes_v4).collect();
    assert!(filled, "tables: {counts:?}");
    shutdown(&nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn announce_then_get_peers_finds_the_peer() {
    let nodes = swarm(21, "127.0.0.1:0").await;
    assert!(
        wait_until(Duration::from_secs(10), || nodes
            .iter()
            .all(|n| n.stats().bootstrapped))
        .await
    );
    let ih = [0x42; 20];
    let mut announce = nodes[19].announce(ih, Some(4242), false);
    while announce.next().await.is_some() {}
    let want: SocketAddr = "127.0.0.1:4242".parse().unwrap();
    let found = timeout(Duration::from_secs(5), find(&nodes[3], ih, want)).await;
    assert_eq!(found, Ok(true));
    shutdown(&nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scrape_round_trip() {
    // every node is 127.0.0.1, so each filter holds at most one address
    // (synthetic IPs are counted in the store's unit tests)
    let nodes = swarm(10, "127.0.0.1:0").await;
    assert!(
        wait_until(Duration::from_secs(10), || nodes
            .iter()
            .all(|n| n.stats().nodes_v4 >= 5))
        .await
    );
    let ih = [0x33; 20];
    let empty = nodes[2].scrape(ih).await.unwrap();
    assert_eq!((empty.seeds, empty.peers), (0, 0));
    let mut seed = nodes[8].announce(ih, Some(5001), true);
    while seed.next().await.is_some() {}
    let mut leech = nodes[9].announce(ih, Some(5002), false);
    while leech.next().await.is_some() {}
    let got = timeout(Duration::from_secs(10), nodes[2].scrape(ih))
        .await
        .unwrap()
        .unwrap();
    assert_eq!((got.seeds, got.peers), (1, 1));
    // only a seed: no leecher counted
    let other = [0x34; 20];
    let mut seed = nodes[8].announce(other, Some(5001), true);
    while seed.next().await.is_some() {}
    let got = nodes[3].scrape(other).await.unwrap();
    assert_eq!((got.seeds, got.peers), (1, 0));
    shutdown(&nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sample_infohashes_returns_stored_hashes() {
    let nodes = swarm(4, "127.0.0.1:0").await;
    assert!(
        wait_until(Duration::from_secs(10), || nodes
            .iter()
            .all(|n| n.stats().nodes_v4 >= 3))
        .await
    );
    // with 4 nodes every announce reaches node 0 (one of the K closest)
    let hashes: Vec<[u8; 20]> = (1..=3u8).map(|k| [k; 20]).collect();
    for ih in &hashes {
        let mut a = nodes[1].announce(*ih, Some(6000), false);
        while a.next().await.is_some() {}
    }
    let target = fbt_dht::NodeId([0x55; 20]);
    let addr = nodes[0].local_addrs()[0];
    let got = nodes[3].sample_infohashes(addr, target).await.unwrap();
    assert_eq!(got.interval, Duration::from_secs(21600));
    assert_eq!(got.num, 3);
    let mut samples = got.samples.clone();
    samples.sort();
    assert_eq!(samples, hashes);
    assert!(!got.nodes.is_empty());
    assert!(got.nodes.iter().all(|n| n.addr.ip().is_loopback()));
    // an address nothing listens on times out
    let dead = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let dead_addr = dead.local_addr().unwrap();
    drop(dead);
    assert!(nodes[3].sample_infohashes(dead_addr, target).await.is_err());
    shutdown(&nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ipv6_swarm_converges_when_available() {
    if !has_ipv6() {
        eprintln!("no IPv6 on this host: skipped");
        return;
    }
    let nodes = swarm(10, "[::1]:0").await;
    assert!(
        wait_until(Duration::from_secs(10), || nodes
            .iter()
            .all(|n| n.stats().nodes_v6 >= 5))
        .await
    );
    assert!(nodes.iter().all(|n| n.stats().nodes_v4 == 0));
    let ih = [0x66; 20];
    let mut announce = nodes[9].announce(ih, Some(4242), false);
    while announce.next().await.is_some() {}
    let want: SocketAddr = "[::1]:4242".parse().unwrap();
    let found = timeout(Duration::from_secs(5), find(&nodes[2], ih, want)).await;
    assert_eq!(found, Ok(true));
    shutdown(&nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restored_state_rejoins_without_bootstrap() {
    let nodes = swarm(21, "127.0.0.1:0").await;
    assert!(wait_until(Duration::from_secs(10), || nodes[5].stats().nodes_v4 >= 8).await);
    let state = nodes[5].state().await.unwrap();
    assert!(state.nodes_v4.len() >= 8 && state.nodes_v4.len() <= MAX_STATE_NODES);
    let id = state.id_v4.expect("an IPv4 id");
    nodes[5].shutdown().await;
    let mut c = cfg("127.0.0.1:0");
    c.state = Some(fbt_dht::DhtState::from_bytes(&state.to_bytes()).unwrap());
    assert!(c.bootstrap.is_empty());
    let back = Dht::spawn(c).await.unwrap();
    assert!(wait_until(Duration::from_secs(10), || back.stats().nodes_v4 >= 8).await);
    assert_eq!(back.state().await.unwrap().id_v4, Some(id));
    back.shutdown().await;
    shutdown(&nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn silent_bootstrap_is_retried_with_backoff() {
    let silent = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut c = cfg("127.0.0.1:0");
    c.bootstrap = vec![silent.local_addr().unwrap().to_string()];
    c.bootstrap_retry_base = Duration::from_millis(50);
    c.bootstrap_retry_max = Duration::from_millis(400);
    // each attempt ends (its query times out) well before the next is due
    c.query_timeout = Duration::from_millis(20);
    let start = Instant::now();
    let dht = Dht::spawn(c).await.unwrap();
    let mut arrivals = Vec::new();
    let mut buf = [0u8; 1500];
    let end = start + Duration::from_secs(3);
    while let Ok(Ok((n, _))) = tokio::time::timeout_at(end, silent.recv_from(&mut buf)).await {
        if let Ok(Message {
            body:
                Body::Query(QueryMsg {
                    query: Query::FindNode { .. },
                    ..
                }),
            ..
        }) = Message::decode(&buf[..n])
        {
            arrivals.push(Instant::now());
        }
    }
    assert!(
        (5..=12).contains(&arrivals.len()),
        "{} find_node datagrams",
        arrivals.len()
    );
    let gaps: Vec<Duration> = arrivals.windows(2).map(|w| w[1] - w[0]).collect();
    let jitter = Duration::from_millis(20);
    let cap = Duration::from_millis(400);
    for w in gaps.windows(2) {
        if w[0] + jitter >= cap {
            break;
        }
        assert!(w[1] + jitter >= w[0], "gaps shrink: {gaps:?}");
    }
    assert!(
        gaps.iter()
            .all(|g| *g + jitter >= Duration::from_millis(50)),
        "{gaps:?}"
    );
    assert!(
        gaps.iter().all(|g| *g <= cap + Duration::from_millis(150)),
        "{gaps:?}"
    );
    assert!(!dht.stats().bootstrapped);
    dht.shutdown().await;
}

/// An attempt that outlasts the retry delay is not overlapped by the next:
/// each one starts only after the last one's query has timed out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_attempts_never_overlap() {
    let silent = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut c = cfg("127.0.0.1:0");
    c.bootstrap = vec![silent.local_addr().unwrap().to_string()];
    c.bootstrap_retry_base = Duration::from_millis(20);
    c.bootstrap_retry_max = Duration::from_millis(20);
    c.query_timeout = Duration::from_millis(300);
    let dht = Dht::spawn(c).await.unwrap();
    let mut arrivals = Vec::new();
    let mut buf = [0u8; 1500];
    let end = Instant::now() + Duration::from_millis(1500);
    while let Ok(Ok((n, _))) = tokio::time::timeout_at(end, silent.recv_from(&mut buf)).await {
        if Message::decode(&buf[..n]).is_ok() {
            arrivals.push(Instant::now());
        }
    }
    let gaps: Vec<Duration> = arrivals.windows(2).map(|w| w[1] - w[0]).collect();
    assert!((2..=6).contains(&arrivals.len()), "{gaps:?}");
    assert!(
        gaps.iter()
            .all(|g| *g + Duration::from_millis(20) >= Duration::from_millis(300)),
        "an attempt started while the last one ran: {gaps:?}"
    );
    dht.shutdown().await;
}

#[tokio::test]
async fn malformed_bootstrap_entries_are_skipped() {
    let mut c = cfg("127.0.0.1:0");
    c.bootstrap = vec!["no-port".into(), "127.0.0.1:notaport".into()];
    let dht = Dht::spawn(c).await.unwrap();
    sleep(Duration::from_millis(500)).await;
    let s = dht.stats();
    assert!(!s.bootstrapped);
    assert_eq!(s.nodes_v4, 0);
    // still serving
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let ping = Message {
        tid: b"aa".to_vec(),
        ip: None,
        version: None,
        body: Body::Query(QueryMsg {
            id: fbt_dht::NodeId([1; 20]),
            query: Query::Ping,
            want: Default::default(),
            read_only: false,
        }),
    };
    sock.send_to(&ping.encode(), dht.local_addrs()[0])
        .await
        .unwrap();
    let mut buf = [0u8; 1500];
    let (n, _) = timeout(Duration::from_secs(1), sock.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        Message::decode(&buf[..n]).unwrap().body,
        Body::Response(_)
    ));
    dht.shutdown().await;
}

/// A loopback socket the way a shared-socket owner sees it.
#[derive(Debug)]
struct Shared(std::net::UdpSocket);

impl DatagramSender for Shared {
    fn local_addr(&self) -> SocketAddr {
        self.0.local_addr().unwrap()
    }

    fn send_to(&self, data: &[u8], to: SocketAddr) -> std::io::Result<usize> {
        self.0.send_to(data, to)
    }
}

async fn spawn_shared(c: DhtConfig) -> Dht {
    let std_sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    std_sock.set_nonblocking(true).unwrap();
    let send = std_sock.try_clone().unwrap();
    let (dht, input) = Dht::spawn_on(c, vec![Arc::new(Shared(send))])
        .await
        .unwrap();
    let recv = UdpSocket::from_std(std_sock).unwrap();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 2048];
        while let Ok((n, from)) = recv.recv_from(&mut buf).await {
            input.handle(from, &buf[..n]);
        }
    });
    dht
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_standalone_and_shared_nodes_converge() {
    let first = Dht::spawn(cfg("127.0.0.1:0")).await.unwrap();
    let seed = first.local_addrs()[0].to_string();
    let mut nodes = vec![first];
    for k in 1..10 {
        let mut c = cfg("127.0.0.1:0");
        c.bootstrap = vec![seed.clone()];
        nodes.push(if k % 2 == 0 {
            spawn_shared(c).await
        } else {
            Dht::spawn(c).await.unwrap()
        });
    }
    assert!(
        wait_until(Duration::from_secs(15), || nodes
            .iter()
            .all(|n| n.stats().nodes_v4 >= 5))
        .await,
        "{:?}",
        nodes.iter().map(|n| n.stats().nodes_v4).collect::<Vec<_>>()
    );
    let ih = [7u8; 20];
    // node 4 is on a shared socket, node 9 on a standalone one
    let mut announce = nodes[4].announce(ih, Some(4242), false);
    while announce.next().await.is_some() {}
    let want: SocketAddr = "127.0.0.1:4242".parse().unwrap();
    let found = timeout(Duration::from_secs(5), find(&nodes[9], ih, want)).await;
    assert_eq!(found, Ok(true));
    shutdown(&nodes).await;
}

/// A bootstrap node that answers each `find_node` after 300 ms: attempts
/// last that long, and the next one is due a backoff after the last ended.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn long_bootstrap_attempts_are_spaced_from_their_end() {
    let slow = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let mut c = cfg("127.0.0.1:0");
    c.bootstrap = vec![slow.local_addr().unwrap().to_string()];
    c.bootstrap_retry_base = Duration::from_millis(50);
    c.bootstrap_retry_max = Duration::from_millis(400);
    c.query_timeout = Duration::from_secs(2);
    let dht = Dht::spawn(c).await.unwrap();
    let mut arrivals = Vec::new();
    let mut ends = Vec::new();
    let mut buf = [0u8; 1500];
    let end = Instant::now() + Duration::from_millis(3500);
    while let Ok(Ok((n, from))) = tokio::time::timeout_at(end, slow.recv_from(&mut buf)).await {
        let Ok(Message {
            tid,
            body:
                Body::Query(QueryMsg {
                    query: Query::FindNode { .. },
                    ..
                }),
            ..
        }) = Message::decode(&buf[..n])
        else {
            continue;
        };
        arrivals.push(Instant::now());
        sleep(Duration::from_millis(300)).await;
        let reply = Message {
            tid,
            ip: None,
            version: None,
            body: Body::Error {
                code: fbt_dht::ERR_SERVER,
                message: "busy".into(),
            },
        };
        slow.send_to(&reply.encode(), from).await.unwrap();
        ends.push(Instant::now());
    }
    assert!(arrivals.len() >= 3, "{} attempts", arrivals.len());
    // expected backoff after the first, second, ... attempts: 50, 100, 200 ms
    for (k, next) in arrivals.iter().enumerate().skip(1) {
        let gap = *next - ends[k - 1];
        let backoff = Duration::from_millis(50 << (k - 1).min(3)).min(Duration::from_millis(400));
        assert!(
            gap + Duration::from_millis(20) >= backoff,
            "attempt {k}: {gap:?} after the last ended, backoff {backoff:?}"
        );
    }
    dht.shutdown().await;
}
