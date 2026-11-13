//! BEP 44 items through the session's DHT. Every DHT here bootstraps from
//! loopback `fbt_dht::Dht` nodes; nothing contacts a public node.

mod support;

use std::net::SocketAddr;
use std::time::Duration;

use fbt_dht::{Dht, DhtConfig};
use fbt_session::{MutableItem, Session, SessionError, Settings};
use support::*;

const T: Duration = Duration::from_secs(30);

async fn loopback_dht(bootstrap: Vec<String>) -> Dht {
    let mut c = DhtConfig::default();
    c.bind_v4 = Some(SocketAddr::from(([127, 0, 0, 1], 0)));
    c.bind_v6 = None;
    c.bootstrap = bootstrap;
    c.query_rate_per_ip = 10_000;
    c.query_timeout = Duration::from_millis(500);
    Dht::spawn(c).await.unwrap()
}

fn dht_settings(bootstrap: Vec<String>) -> Settings {
    let mut s = test_settings();
    s.dht_enabled = true;
    s.dht_bootstrap_nodes = bootstrap;
    s.dht_query_timeout = Duration::from_millis(500);
    s.dht_query_rate_per_ip = 10_000;
    s
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_put_get_round_trip() {
    let boot = loopback_dht(Vec::new()).await;
    let seed_addr = boot.local_addrs()[0].to_string();
    let mut others = Vec::new();
    for _ in 0..4 {
        others.push(loopback_dht(vec![seed_addr.clone()]).await);
    }
    let writer = Session::new(dht_settings(vec![seed_addr.clone()]))
        .await
        .unwrap();
    let reader = Session::new(dht_settings(vec![seed_addr.clone()]))
        .await
        .unwrap();
    for s in [&writer, &reader] {
        assert!(wait_until(|| s.dht_stats().is_some_and(|d| d.nodes_v4 >= 3), T).await);
    }

    let value = b"l4:spam4:eggse".to_vec();
    let (target, stored) = writer.dht_put_immutable(value.clone()).await.unwrap();
    assert!(stored >= 1);
    assert_eq!(reader.dht_get_immutable(target).await.unwrap(), Some(value));

    let seed = [0x5eu8; 32];
    let public_key = ed25519_dalek::SigningKey::from_bytes(&seed)
        .verifying_key()
        .to_bytes();
    let salt = b"profile".to_vec();
    for seq in 1..=2 {
        let v = format!("i{seq}e").into_bytes();
        let stored = writer
            .dht_put_mutable(seed, salt.clone(), v, seq, None)
            .await
            .unwrap();
        assert!(stored >= 1);
    }
    let item: MutableItem = reader
        .dht_get_mutable(public_key, &salt)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            item.public_key,
            item.salt.as_slice(),
            item.seq,
            item.value.as_slice()
        ),
        (public_key, salt.as_slice(), 2, &b"i2e"[..])
    );
    // an invalid value is a DHT error, not a panic or a silent success
    assert!(matches!(
        writer.dht_put_immutable(b"nope".to_vec()).await,
        Err(SessionError::Dht(_))
    ));

    writer.shutdown().await;
    assert!(matches!(
        writer.dht_get_immutable(target).await,
        Err(SessionError::Shutdown)
    ));
    reader.shutdown().await;
    for d in others.iter().chain([&boot]) {
        d.shutdown().await;
    }

    // with the DHT off
    let off = Session::new(test_settings()).await.unwrap();
    assert!(matches!(
        off.dht_get_immutable(target).await,
        Err(SessionError::DhtDisabled)
    ));
    assert!(matches!(
        off.dht_put_immutable(b"i1e".to_vec()).await,
        Err(SessionError::DhtDisabled)
    ));
    assert!(matches!(
        off.dht_get_mutable(public_key, b"").await,
        Err(SessionError::DhtDisabled)
    ));
    assert!(matches!(
        off.dht_put_mutable(seed, Vec::new(), b"i1e".to_vec(), 1, None)
            .await,
        Err(SessionError::DhtDisabled)
    ));
    off.shutdown().await;
}
