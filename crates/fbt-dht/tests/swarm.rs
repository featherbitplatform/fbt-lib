//! Loopback swarms of real nodes. Nothing here contacts a public DHT node.

use std::net::SocketAddr;
use std::time::Duration;

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
    let mut announce = nodes[19].announce(ih, Some(4242));
    while announce.next().await.is_some() {}
    let want: SocketAddr = "127.0.0.1:4242".parse().unwrap();
    let found = timeout(Duration::from_secs(5), find(&nodes[3], ih, want)).await;
    assert_eq!(found, Ok(true));
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
    let mut announce = nodes[9].announce(ih, Some(4242));
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
