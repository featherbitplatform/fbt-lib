//! The port mapper against the loopback NAT-PMP/PCP and IGD fakes.
//!
//! With a paused clock tokio auto-advances while a loopback reply is in
//! flight, so retries inside one attempt may fire early and request counts
//! drift: these tests assert on what the fakes saw and when, never on exact
//! counts. A [`ticker`] keeps each idle jump to a few milliseconds, so a 5 s
//! HTTP timeout cannot fire in one jump while a loopback exchange runs.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::Ordering;
use std::time::Duration;

use fbt_net::portmap::{MapEvent, PortMapConfig, PortMapper};
use fbt_net::{MapMethod, MapProtocol};
use tokio::sync::mpsc;
use tokio::time::Instant;

#[path = "support/fake_igd.rs"]
mod fake_igd;
#[path = "support/fake_natpmp.rs"]
mod fake_natpmp;
use fake_igd::{FakeIgd, Script};
use fake_natpmp::{FakeGateway, Mode};

const PORT: u16 = 6881;
/// Slack on scheduled times: a round's first request may trail its start.
const SLACK: Duration = Duration::from_secs(2);

/// A timer every 5 ms: the paused clock then advances in 5 ms steps, with
/// the I/O driver polled between them.
fn ticker() -> tokio::task::JoinHandle<()> {
    tokio::spawn(async {
        loop {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
}

fn config(gw: &FakeGateway, igd: &FakeIgd, lease: u32) -> PortMapConfig {
    let mut c = PortMapConfig::default();
    c.gateway = Some(Ipv4Addr::LOCALHOST);
    c.natpmp_port = gw.port;
    c.ssdp = igd.ssdp;
    c.lease = lease;
    c
}

/// Waits for `Mapped` from each of `methods` for `protocols`.
async fn wait_mapped(
    rx: &mut mpsc::Receiver<MapEvent>,
    want: &[(MapProtocol, MapMethod)],
) -> Vec<MapEvent> {
    let mut seen = Vec::new();
    let mut left: Vec<_> = want.to_vec();
    tokio::time::timeout(Duration::from_secs(30), async {
        while !left.is_empty() {
            let ev = rx.recv().await.expect("mapper alive");
            if let MapEvent::Mapped {
                protocol, method, ..
            } = &ev
            {
                left.retain(|w| *w != (*protocol, *method));
            }
            seen.push(ev);
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no Mapped for {left:?}; saw {seen:?}"));
    seen
}

/// PCP MAP requests for `proto` (6 TCP, 17 UDP): (time, nonce, lifetime).
fn pcp_maps(gw: &FakeGateway, proto: u8) -> Vec<(Instant, Vec<u8>, u32)> {
    gw.timed_requests()
        .into_iter()
        .filter(|(_, r)| r.len() == 60 && r[0] == 2 && r[1] == 1 && r[36] == proto)
        .map(|(t, r)| {
            let life = u32::from_be_bytes(r[4..8].try_into().unwrap());
            (t, r[24..36].to_vec(), life)
        })
        .collect()
}

/// The first time of each burst: times more than 10 s after the previous.
fn round_starts(times: &[Instant]) -> Vec<Instant> {
    let mut out: Vec<Instant> = Vec::new();
    let mut last: Option<Instant> = None;
    for &t in times {
        if last.is_none_or(|l| t.duration_since(l) > Duration::from_secs(10)) {
            out.push(t);
        }
        last = Some(t);
    }
    out
}

fn assert_near(got: Duration, want: Duration, what: &str) {
    assert!(
        got + SLACK >= want && got <= want + SLACK,
        "{what}: {got:?}, expected {want:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn maps_with_both_methods_and_renews_at_half_lease() {
    let _tick = ticker();
    let gw = FakeGateway::start(Mode::Pcp, false, 0).await;
    let igd = FakeIgd::start(Script::default()).await;
    let (tx, mut rx) = mpsc::channel(64);
    let mapper = PortMapper::spawn(config(&gw, &igd, 120), tx);
    mapper.map(MapProtocol::Tcp, PORT);
    mapper.map(MapProtocol::Udp, PORT);
    let events = wait_mapped(
        &mut rx,
        &[
            (MapProtocol::Tcp, MapMethod::Pcp),
            (MapProtocol::Udp, MapMethod::Pcp),
            (MapProtocol::Tcp, MapMethod::Upnp),
            (MapProtocol::Udp, MapMethod::Upnp),
        ],
    )
    .await;
    let ext: Ipv4Addr = fake_igd::EXTERNAL_IP.parse().unwrap();
    assert!(events.contains(&MapEvent::Mapped {
        protocol: MapProtocol::Tcp,
        method: MapMethod::Pcp,
        internal: PORT,
        external: SocketAddr::from((fake_natpmp::EXTERNAL_IP, PORT + 1)),
    }));
    assert!(events.contains(&MapEvent::Mapped {
        protocol: MapProtocol::Udp,
        method: MapMethod::Upnp,
        internal: PORT,
        external: SocketAddr::from((ext, PORT)),
    }));

    tokio::time::sleep(Duration::from_secs(75)).await;

    for proto in [6, 17] {
        let maps = pcp_maps(&gw, proto);
        let (t0, nonce, life) = maps.first().cloned().expect("a PCP map");
        assert_eq!(life, 120);
        // The renewal: same nonce, about 60 s later, nothing in between.
        let later: Vec<_> = maps
            .iter()
            .filter(|(t, ..)| t.duration_since(t0) > Duration::from_secs(10))
            .collect();
        let (t1, n1, l1) = later.first().expect("a PCP renewal");
        // Only the first attempt's own retries come before the renewal.
        assert!(
            maps.iter()
                .all(|(t, ..)| t.duration_since(t0) <= Duration::from_secs(5) || t >= t1),
            "PCP request between map and renewal"
        );
        assert_eq!(*n1, nonce, "renewal keeps the nonce");
        assert_eq!(*l1, 120);
        assert_near(
            t1.duration_since(t0),
            Duration::from_secs(60),
            "PCP renewal",
        );
    }
    for proto in ["TCP", "UDP"] {
        let adds: Vec<_> = igd
            .recorded("AddPortMapping")
            .into_iter()
            .filter(|r| r.arg("NewProtocol") == proto)
            .collect();
        let first = adds.first().expect("a UPnP map");
        assert_eq!(first.arg("NewLeaseDuration"), "120");
        let renewal = adds
            .iter()
            .find(|r| r.at.duration_since(first.at) > Duration::from_secs(10))
            .expect("a UPnP renewal");
        assert!(
            adds.iter()
                .all(|r| r.at.duration_since(first.at) <= Duration::from_secs(5)
                    || r.at >= renewal.at),
            "UPnP request between map and renewal"
        );
        assert_eq!(renewal.arg("NewExternalPort"), first.arg("NewExternalPort"));
        assert_near(
            renewal.at.duration_since(first.at),
            Duration::from_secs(60),
            "UPnP renewal",
        );
    }
    mapper.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn failure_backs_off() {
    let _tick = ticker();
    let gw = FakeGateway::start(Mode::Silent, false, 0).await;
    let igd = FakeIgd::start(Script {
        fail_add: true,
        ..Script::default()
    })
    .await;
    let (tx, mut rx) = mpsc::channel(64);
    let mapper = PortMapper::spawn(config(&gw, &igd, 3600), tx);
    mapper.map(MapProtocol::Tcp, PORT);

    // Rounds at 0, 60, 180 and 420 s.
    tokio::time::sleep(Duration::from_secs(440)).await;

    let want = [60, 120, 240].map(Duration::from_secs);
    let natpmp: Vec<Instant> = gw.timed_requests().into_iter().map(|(t, _)| t).collect();
    let upnp: Vec<Instant> = igd.searches.lock().unwrap().clone();
    for (name, times) in [("PCP", natpmp), ("UPnP", upnp)] {
        let starts = round_starts(&times);
        assert!(starts.len() >= 4, "{name} rounds: {starts:?}");
        for (k, w) in want.iter().enumerate() {
            assert_near(
                starts[k + 1].duration_since(starts[k]),
                *w,
                &format!("{name} gap {k}"),
            );
        }
    }
    let mut failed = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        if let MapEvent::Failed {
            method, message, ..
        } = ev
        {
            failed.push(method);
            if method == MapMethod::Upnp {
                assert!(message.contains("501"), "{message}");
            }
        }
    }
    assert!(failed.contains(&MapMethod::Pcp), "{failed:?}");
    assert!(failed.contains(&MapMethod::Upnp), "{failed:?}");
    mapper.shutdown().await;
}

#[tokio::test]
async fn shutdown_unmaps() {
    let gw = FakeGateway::start(Mode::Pcp, false, 0).await;
    let igd = FakeIgd::start(Script::default()).await;
    let (tx, mut rx) = mpsc::channel(64);
    let mapper = PortMapper::spawn(config(&gw, &igd, 3600), tx);
    mapper.map(MapProtocol::Tcp, PORT);
    wait_mapped(
        &mut rx,
        &[
            (MapProtocol::Tcp, MapMethod::Pcp),
            (MapProtocol::Tcp, MapMethod::Upnp),
        ],
    )
    .await;

    let started = std::time::Instant::now();
    mapper.shutdown().await;
    assert!(started.elapsed() < Duration::from_secs(3));

    let maps = pcp_maps(&gw, 6);
    let (_, nonce, _) = maps.first().cloned().unwrap();
    let (_, n, life) = maps.last().cloned().unwrap();
    assert_eq!(life, 0, "a PCP delete");
    assert_eq!(n, nonce);
    let dels = igd.recorded("DeletePortMapping");
    assert_eq!(dels.len(), 1);
    assert_eq!(dels[0].arg("NewProtocol"), "TCP");
    assert_eq!(dels[0].arg("NewExternalPort"), PORT.to_string());
    // Idempotent.
    mapper.shutdown().await;
}

#[tokio::test]
async fn shutdown_is_bounded_with_a_silent_gateway() {
    // The gateway answers the map, then the unmap goes unanswered.
    let gw = FakeGateway::start(Mode::Pcp, false, 0).await;
    let igd = FakeIgd::start(Script::default()).await;
    let mut cfg = config(&gw, &igd, 3600);
    cfg.upnp = false;
    let (tx, mut rx) = mpsc::channel(64);
    let mapper = PortMapper::spawn(cfg, tx);
    mapper.map(MapProtocol::Tcp, PORT);
    wait_mapped(&mut rx, &[(MapProtocol::Tcp, MapMethod::Pcp)]).await;
    // Stop the responder: the delete would wait 3.75 s unless cut.
    drop(gw);
    let started = std::time::Instant::now();
    mapper.shutdown().await;
    let took = started.elapsed();
    assert!(took < Duration::from_millis(2500), "{took:?}");
}

#[tokio::test(start_paused = true)]
async fn renews_at_half_the_granted_lifetime_floored_at_a_minute() {
    let _tick = ticker();
    // Asked for an hour; granted 300 s (renew at 150 s) and 100 s (renew at
    // max(50 s, 60 s) = 60 s).
    for (grant, renew_after) in [(300, 150), (100, 60)] {
        let gw = FakeGateway::start_granting(Mode::Pcp, grant).await;
        let igd = FakeIgd::start(Script::default()).await;
        let mut cfg = config(&gw, &igd, 3600);
        cfg.upnp = false;
        let (tx, mut rx) = mpsc::channel(64);
        let mapper = PortMapper::spawn(cfg, tx);
        mapper.map(MapProtocol::Tcp, PORT);
        wait_mapped(&mut rx, &[(MapProtocol::Tcp, MapMethod::Pcp)]).await;
        tokio::time::sleep(Duration::from_secs(renew_after + 15)).await;

        let maps = pcp_maps(&gw, 6);
        let (t0, nonce, asked) = maps.first().cloned().expect("a PCP map");
        assert_eq!(asked, 3600);
        let (t1, n1, _) = maps
            .iter()
            .find(|(t, ..)| t.duration_since(t0) > Duration::from_secs(10))
            .cloned()
            .expect("a renewal");
        assert_eq!(n1, nonce);
        assert_near(
            t1.duration_since(t0),
            Duration::from_secs(renew_after),
            &format!("renewal of a {grant} s grant"),
        );
        mapper.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn backoff_resets_after_a_success() {
    let _tick = ticker();
    let gw = FakeGateway::start(Mode::Pcp, false, 0).await;
    let igd = FakeIgd::start(Script::default()).await;
    let mut cfg = config(&gw, &igd, 120);
    cfg.upnp = false;
    gw.mute.store(true, Ordering::SeqCst);
    let (tx, mut rx) = mpsc::channel(256);
    let mapper = PortMapper::spawn(cfg, tx);
    mapper.map(MapProtocol::Tcp, PORT);

    // Fails at 0 and 60; answered at 180 (renewal due at 240).
    tokio::time::sleep(Duration::from_secs(170)).await;
    gw.mute.store(false, Ordering::SeqCst);
    wait_mapped(&mut rx, &[(MapProtocol::Tcp, MapMethod::Pcp)]).await;
    // The renewal at 240 fails: the retry comes 60 s later, not 240 s.
    tokio::time::sleep(Duration::from_secs(20)).await;
    gw.mute.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(120)).await;

    let times: Vec<Instant> = gw.timed_requests().into_iter().map(|(t, _)| t).collect();
    let starts = round_starts(&times);
    assert!(starts.len() >= 5, "rounds: {starts:?}");
    let t0 = starts[0];
    for (k, want) in [60, 180, 240, 300].into_iter().enumerate() {
        assert_near(
            starts[k + 1].duration_since(t0),
            Duration::from_secs(want),
            &format!("round {}", k + 1),
        );
    }
    mapper.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn upnp_mappings_are_removed_after_a_failed_renewal() {
    let _tick = ticker();
    let gw = FakeGateway::start(Mode::Pcp, false, 0).await;
    let igd = FakeIgd::start(Script::default()).await;
    let mut cfg = config(&gw, &igd, 120);
    cfg.natpmp = false;
    let (tx, mut rx) = mpsc::channel(64);
    let mapper = PortMapper::spawn(cfg, tx);
    mapper.map(MapProtocol::Tcp, PORT);
    wait_mapped(&mut rx, &[(MapProtocol::Tcp, MapMethod::Upnp)]).await;

    // The renewal at 60 s fails: the IGD is looked up again next round, yet
    // the mapping it holds must still be removed at shutdown.
    igd.fail_add.store(true, Ordering::SeqCst);
    let failed = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if let Some(MapEvent::Failed { method, .. }) = rx.recv().await {
                break method;
            }
        }
    })
    .await
    .expect("the renewal failed");
    assert_eq!(failed, MapMethod::Upnp);

    mapper.shutdown().await;
    let dels = igd.recorded("DeletePortMapping");
    assert_eq!(dels.len(), 1, "the held mapping was removed");
    assert_eq!(dels[0].arg("NewExternalPort"), PORT.to_string());
}
