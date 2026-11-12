//! The IP filter and the per-IP connection limit on loopback: blocked
//! addresses are never dialled, blocked incoming connections are closed
//! unread, a new filter closes live peers, and a second connection from
//! one IP is refused unless loopback is exempt.

mod support;

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fbt_metainfo::Metainfo;
use fbt_session::storage::MemStorage;
use fbt_session::{
    AddTorrent, BlockReason, DisconnectReason, EncryptionPolicy, Event, EventMask, IpFilter,
    Session, Settings, TorrentHandle, TorrentState,
};
use fbt_wire::{Handshake, Reserved, write_handshake};
use support::*;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

/// Plaintext, TCP-only dials, so scripted peers that listen can answer.
fn plain() -> Settings {
    let mut s = dial_tcp(test_settings());
    s.encryption = EncryptionPolicy::Disabled;
    s
}

fn only(ip: IpAddr) -> IpFilter {
    let mut f = IpFilter::default();
    f.block(ip..=ip);
    f
}

/// A session leeching an empty torrent from memory storage.
async fn leecher(st: Settings) -> (Session, TorrentHandle, Metainfo) {
    let (m, payload) = make_torrent(&[("a", 100_000)], 16_384);
    let s = Session::new(st).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::new(payload.len() as u64)));
    let h = s.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);
    (s, h, m)
}

fn blocked(e: &Event) -> bool {
    matches!(e, Event::PeerBlocked { .. })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_seed_is_never_dialled() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seed = listener.local_addr().unwrap();
    let (s, h, m) = leecher(plain()).await;
    let f = only(seed.ip());
    s.set_ip_filter(f.clone()).await;
    assert_eq!(s.ip_filter(), f);
    let mut ev = s.subscribe(EventMask::PEER);
    h.add_peer(seed).await.unwrap();
    let e = next_matching(&mut ev, blocked, T)
        .await
        .expect("PeerBlocked");
    assert_eq!(
        e,
        Event::PeerBlocked {
            torrent: Some(m.info_hashes),
            addr: seed,
            reason: BlockReason::IpFilter,
        }
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .is_err(),
        "a blocked address is never dialled"
    );
    assert_eq!(h.status().num_peers, 0);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_incoming_is_closed_before_handshake() {
    let (s, _h, m) = leecher(test_settings()).await;
    s.set_ip_filter(only(s.local_addr().ip())).await;
    let mut ev = s.subscribe(EventMask::PEER);
    let mut stream = TcpStream::connect(s.local_addr()).await.unwrap();
    let from = stream.local_addr().unwrap();
    let hs = Handshake {
        reserved: Reserved::default(),
        info_hash: m.info_hashes.wire_hash(),
        peer_id: fbt_core::PeerId::generate(),
    };
    // The session may have closed already: a failed write is fine.
    let _ = write_handshake(&mut stream, &hs).await;
    let mut got = Vec::new();
    let read = tokio::time::timeout(T, stream.read_to_end(&mut got)).await;
    assert!(read.is_ok(), "closed in time");
    assert!(got.is_empty(), "no handshake comes back");
    let e = next_matching(&mut ev, blocked, T)
        .await
        .expect("PeerBlocked");
    assert_eq!(
        e,
        Event::PeerBlocked {
            torrent: None,
            addr: from,
            reason: BlockReason::IpFilter,
        }
    );
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn setting_a_filter_closes_live_peers() {
    let (s, h, m) = leecher(test_settings()).await;
    let mut ev = s.subscribe(EventMask::PEER);
    let mut ev_blocked = s.subscribe(EventMask::PEER);
    let mut peer = ScriptedPeer::connect(s.local_addr(), m.info_hashes.wire_hash()).await;
    next_matching(&mut ev, |e| matches!(e, Event::PeerConnected { .. }), T)
        .await
        .expect("PeerConnected");
    s.set_ip_filter(only(s.local_addr().ip())).await;
    let e = next_matching(&mut ev, |e| matches!(e, Event::PeerDisconnected { .. }), T)
        .await
        .expect("PeerDisconnected");
    let Event::PeerDisconnected { reason, .. } = e else {
        unreachable!()
    };
    assert_eq!(reason, DisconnectReason::Blocked(BlockReason::IpFilter));
    let e = next_matching(&mut ev_blocked, blocked, T)
        .await
        .expect("PeerBlocked");
    assert!(matches!(
        e,
        Event::PeerBlocked {
            torrent: Some(t),
            reason: BlockReason::IpFilter,
            ..
        } if t == m.info_hashes
    ));
    assert!(matches!(peer.wait_closed(T).await, PeerEnd::Closed));
    assert!(wait_until(|| h.status().num_peers == 0, T).await);
    // An empty filter lets it back in.
    s.set_ip_filter(IpFilter::default()).await;
    assert!(s.ip_filter().is_empty());
    let _again = ScriptedPeer::connect(s.local_addr(), m.info_hashes.wire_hash()).await;
    next_matching(&mut ev, |e| matches!(e, Event::PeerConnected { .. }), T)
        .await
        .expect("PeerConnected after the filter is lifted");
    s.shutdown().await;
}

/// Dials two scripted peers listening on `127.0.0.1`, the second once the
/// first is connected. Returns the session, its handle, both peers'
/// addresses and both scripted peers, with one event stream subscribed
/// before anything was dialled.
async fn dial_two(
    st: Settings,
) -> (
    Session,
    TorrentHandle,
    Metainfo,
    [SocketAddr; 2],
    [ScriptedPeer; 2],
    [fbt_session::EventStream; 2],
) {
    let la = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let lb = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (a, b) = (la.local_addr().unwrap(), lb.local_addr().unwrap());
    let (s, h, m) = leecher(st).await;
    let mut ev = s.subscribe(EventMask::PEER);
    let ev2 = s.subscribe(EventMask::PEER);
    h.add_peer(a).await.unwrap();
    let pa = ScriptedPeer::accept_with(&la, Reserved::default()).await;
    next_matching(
        &mut ev,
        |e| matches!(e, Event::PeerConnected { addr, .. } if *addr == a),
        T,
    )
    .await
    .expect("the first peer connects");
    h.add_peer(b).await.unwrap();
    let pb = ScriptedPeer::accept_with(&lb, Reserved::default()).await;
    (s, h, m, [a, b], [pa, pb], [ev, ev2])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_connection_from_one_ip_is_refused() {
    let mut st = plain();
    st.per_ip_exempt_loopback = false;
    assert_eq!(st.max_peers_per_ip, 1);
    let (s, h, m, [a, b], [_pa, mut pb], [mut ev, mut ev_blocked]) = dial_two(st).await;
    let e = next_matching(
        &mut ev,
        |e| {
            matches!(
                e,
                Event::PeerDisconnected { .. } | Event::PeerConnected { .. }
            )
        },
        T,
    )
    .await
    .expect("the second connection ends");
    assert_eq!(
        e,
        Event::PeerDisconnected {
            torrent: m.info_hashes,
            addr: b,
            reason: DisconnectReason::Blocked(BlockReason::PerIpLimit),
        }
    );
    let e = next_matching(&mut ev_blocked, blocked, T)
        .await
        .expect("PeerBlocked");
    assert_eq!(
        e,
        Event::PeerBlocked {
            torrent: Some(m.info_hashes),
            addr: b,
            reason: BlockReason::PerIpLimit,
        }
    );
    assert!(matches!(pb.wait_closed(T).await, PeerEnd::Closed));
    // An incoming connection from the same IP is refused too, unread.
    let mut stream = TcpStream::connect(s.local_addr()).await.unwrap();
    let from = stream.local_addr().unwrap();
    let hs = Handshake {
        reserved: Reserved::default(),
        info_hash: m.info_hashes.wire_hash(),
        peer_id: fbt_core::PeerId::generate(),
    };
    let _ = write_handshake(&mut stream, &hs).await;
    let e = next_matching(&mut ev_blocked, blocked, T)
        .await
        .expect("PeerBlocked for the incoming connection");
    assert_eq!(
        e,
        Event::PeerBlocked {
            torrent: Some(m.info_hashes),
            addr: from,
            reason: BlockReason::PerIpLimit,
        }
    );
    let mut rest = Vec::new();
    let _ = tokio::time::timeout(T, stream.read_to_end(&mut rest)).await;
    // The first peer is still connected (the status catches up within a
    // status interval).
    assert!(wait_until(|| h.status().num_peers == 1, T).await);
    assert!(
        next_matching(
            &mut ev,
            |e| matches!(e, Event::PeerDisconnected { addr, .. } if *addr == a),
            Duration::from_millis(300),
        )
        .await
        .is_none()
    );
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loopback_is_exempt_by_default() {
    let st = plain();
    assert!(st.per_ip_exempt_loopback);
    let (s, h, _m, [_a, b], _peers, [mut ev, mut ev_blocked]) = dial_two(st).await;
    next_matching(
        &mut ev,
        |e| matches!(e, Event::PeerConnected { addr, .. } if *addr == b),
        T,
    )
    .await
    .expect("the second peer connects too");
    assert!(wait_until(|| h.status().num_peers == 2, T).await);
    assert!(
        next_matching(&mut ev_blocked, blocked, Duration::from_millis(300))
            .await
            .is_none(),
        "nothing is blocked"
    );
    s.shutdown().await;
}
