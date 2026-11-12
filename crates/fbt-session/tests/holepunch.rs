//! BEP 55 holepunch: two NAT-emulated sessions reach each other through a
//! relay they both dialled, the relay answers a rendezvous for a peer it
//! does not hold with an error, private torrents never relay, and NAT
//! emulation really drops unsolicited uTP SYNs.
//!
//! NAT emulation (`Settings::nat_emulation`) refuses incoming TCP and
//! accepts a uTP SYN only from an address the session sent a datagram to
//! in the last 30 s, like an endpoint-dependent NAT.

mod support;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fbt_metainfo::Metainfo;
use fbt_session::storage::MemStorage;
use fbt_session::{
    AddTorrent, Event, EventMask, EventStream, RateLimit, Session, Settings, TorrentHandle,
    TorrentState, Transport,
};
use fbt_wire::{
    ExtHandshake, HP_NOT_CONNECTED, HolepunchMsg, Message, OUR_UT_HOLEPUNCH_ID, Reserved,
    UT_HOLEPUNCH,
};
use support::*;

const T: Duration = Duration::from_secs(30);
/// The `ut_holepunch` id our scripted peers assign.
const THEIR_UT_HOLEPUNCH: u8 = 9;

fn settings() -> Settings {
    let mut s = test_settings();
    s.pex_interval = Duration::from_millis(200);
    s
}

/// A peer behind an emulated endpoint-dependent NAT.
fn natted(mut s: Settings) -> Settings {
    s.nat_emulation = true;
    s.enable_incoming_tcp = false;
    s.utp_connect_timeout = Duration::from_millis(300);
    s
}

/// The relay: a leecher with nothing that never uploads (no upload slots)
/// and downloads slowly, so it neither serves A nor finishes and parts
/// from the seed C (BEP 21) while the test runs.
fn relay_settings(mut s: Settings) -> Settings {
    s.unchoke_slots = 0;
    s.rate_limit = RateLimit::new(16 * 1024, 0);
    s.rate_limit_local_peers = true;
    s
}

/// A leecher session over fresh memory storage.
async fn leecher(s: Settings, m: &Metainfo, len: usize) -> (Session, TorrentHandle, EventStream) {
    let session = Session::new(s).await.unwrap();
    let ev = session.subscribe(EventMask::ALL);
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::new(len as u64)));
    let h = session.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Downloading, T).await);
    (session, h, ev)
}

/// A session seeding `payload` from memory.
async fn seed(s: Settings, m: &Metainfo, payload: Vec<u8>) -> (Session, TorrentHandle) {
    let session = Session::new(s).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(payload)));
    let h = session.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    (session, h)
}

/// R, C and A as the holepunch tests use them: C (seed) and A (leecher)
/// NAT-emulated, each given R and dialling it. C runs no PEX, so it never
/// learns A and never dials it on its own: an unsolicited SYN from C would
/// open C's emulated NAT for A by coincidence. A only reaches C through a
/// rendezvous at R.
async fn three(
    m: &Metainfo,
    payload: Vec<u8>,
    holepunch: bool,
) -> (Session, Session, Session, TorrentHandle, EventStream) {
    let len = payload.len();
    let mut rs = relay_settings(settings());
    rs.enable_holepunch = holepunch;
    let (r, _hr, _evr) = leecher(rs, m, len).await;
    let mut cs = natted(settings());
    cs.enable_holepunch = holepunch;
    cs.pex_enabled = false;
    let (c, hc) = seed(cs, m, payload).await;
    let mut a_s = natted(settings());
    a_s.enable_holepunch = holepunch;
    let (a, ha, eva) = leecher(a_s, m, len).await;
    hc.add_peer(r.local_addr()).await.unwrap();
    ha.add_peer(r.local_addr()).await.unwrap();
    (r, c, a, ha, eva)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn holepunch_connects_two_natted_peers() {
    let (m, payload) = make_torrent(&[("a", 1 << 20)], 16_384);
    let (r, c, a, ha, mut eva) = three(&m, payload, true).await;
    let c_addr = c.local_addr();
    // Paired events: once C is reported connected, nothing reports it
    // gone until the window ends, not even a holepunch dial of A's that
    // lost the race to C's (it never got through).
    let mut connected = Vec::new();
    let mut ended_after_connect = Vec::new();
    let end = tokio::time::Instant::now() + Duration::from_secs(20);
    // The window ends with A's last piece: its `Have` shows C that A is a
    // seed too, and C parts from it (BEP 21), ahead of A's
    // `TorrentFinished` (which waits for the final flush).
    let mut pieces_left = m.info.pieces.len();
    while pieces_left > 0
        && let Ok(Some(e)) = tokio::time::timeout_at(end, eva.recv()).await
    {
        match e {
            Event::PeerConnected {
                addr, transport, ..
            } if addr == c_addr => connected.push(transport),
            Event::PeerDisconnected { addr, reason, .. }
                if addr == c_addr && !connected.is_empty() =>
            {
                ended_after_connect.push(reason)
            }
            Event::PieceFinished { .. } => pieces_left -= 1,
            _ => {}
        }
    }
    assert_eq!(
        pieces_left, 0,
        "A did not finish within 20 s ({connected:?})"
    );
    assert!(wait_state(&ha, |s| *s == TorrentState::Seeding, T).await);
    assert_eq!(connected, vec![Transport::Utp], "PeerConnected for C at A");
    assert!(
        ended_after_connect.is_empty(),
        "PeerDisconnected for C at A while connected: {ended_after_connect:?}"
    );
    assert!(ha.status().progress >= 1.0);
    // The exchange happened: A asked R, R introduced A and C, and both
    // acted on R's `Connect`.
    let (a_sent, _, a_connects) = a.holepunch_stats();
    let (_, r_relayed, _) = r.holepunch_stats();
    let (_, _, c_connects) = c.holepunch_stats();
    assert!(a_sent >= 1, "A sent no Rendezvous");
    assert!(r_relayed >= 1, "R relayed nothing");
    assert!(
        a_connects >= 1 && c_connects >= 1,
        "{a_connects} {c_connects}"
    );
    for s in [a, c, r] {
        s.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nat_emulation_blocks_unsolicited() {
    let (m, payload) = make_torrent(&[("a", 1 << 20)], 16_384);
    let (r, c, a, _ha, mut eva) = three(&m, payload, false).await;
    let c_addr = c.local_addr();
    let got = next_matching(
        &mut eva,
        |e| matches!(e, Event::PeerConnected { addr, .. } if *addr == c_addr),
        Duration::from_secs(5),
    )
    .await;
    assert!(got.is_none(), "A reached C through its NAT: {got:?}");
    assert_eq!(a.holepunch_stats(), (0, 0, 0));
    assert_eq!(r.holepunch_stats(), (0, 0, 0));
    for s in [a, c, r] {
        s.shutdown().await;
    }
}

fn ext_reserved() -> Reserved {
    let mut r = Reserved::default();
    r.set_fast();
    r.set_extended();
    r
}

/// A BEP 10 handshake offering `ut_holepunch`, with listen port `port`.
fn hp_hs(port: Option<u16>) -> ExtHandshake {
    let mut hs = ExtHandshake {
        listen_port: port,
        ..ExtHandshake::default()
    };
    hs.m.insert(UT_HOLEPUNCH.to_owned(), THEIR_UT_HOLEPUNCH);
    hs
}

/// The session's extension handshake to `peer`.
async fn our_ext_hs(peer: &mut ScriptedPeer) -> ExtHandshake {
    let payload = peer.next_ext(0).await.expect("our extension handshake");
    ExtHandshake::decode(&payload).expect("valid handshake")
}

fn rendezvous(target: SocketAddr) -> Message {
    Message::Extended {
        id: OUR_UT_HOLEPUNCH_ID,
        payload: HolepunchMsg::Rendezvous(target).encode(),
    }
}

/// A scripted peer connected to `relay` over TCP, extension handshakes
/// exchanged (ours advertising `ut_holepunch`). Returns it and the
/// relay's handshake.
async fn scripted(
    relay: &Session,
    m: &Metainfo,
    port: Option<u16>,
) -> (ScriptedPeer, ExtHandshake) {
    let mut p = ScriptedPeer::connect_with(
        relay.local_addr(),
        m.info_hashes.wire_hash(),
        ext_reserved(),
    )
    .await;
    let theirs = our_ext_hs(&mut p).await;
    p.send_ext_handshake(&hp_hs(port)).await;
    (p, theirs)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_answers_not_connected() {
    let (m, payload) = make_torrent(&[("a", 100_000)], 16_384);
    let (r, _hr, _evr) = leecher(settings(), &m, payload.len()).await;
    let (mut p, hs) = scripted(&r, &m, Some(7000)).await;
    assert_eq!(hs.id_of(UT_HOLEPUNCH), Some(OUR_UT_HOLEPUNCH_ID));
    let target: SocketAddr = "127.0.0.1:1".parse().unwrap();
    p.send(rendezvous(target)).await;
    let reply = p.next_ext(THEIR_UT_HOLEPUNCH).await.expect("an answer");
    assert_eq!(
        HolepunchMsg::decode(&reply).expect("valid ut_holepunch"),
        HolepunchMsg::Error {
            addr: target,
            code: HP_NOT_CONNECTED
        }
    );
    r.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_torrent_never_relays() {
    let (m, payload) = make_private_torrent(&[("a", 100_000)], 16_384);
    let (r, hr, _evr) = leecher(settings(), &m, payload.len()).await;
    let (mut p1, hs) = scripted(&r, &m, Some(7001)).await;
    assert_eq!(
        hs.id_of(UT_HOLEPUNCH),
        None,
        "advertised for a private torrent"
    );
    let (mut p2, _) = scripted(&r, &m, Some(7002)).await;
    assert!(wait_until(|| hr.status().num_peers == 2, T).await);
    // p2 is connected to R and says it speaks `ut_holepunch`: a public
    // torrent would relay this (and p1 would get `Connect`).
    p1.send(rendezvous("127.0.0.1:7002".parse().unwrap())).await;
    let is_hp = |m: &Message| {
        matches!(
            m,
            Message::Extended {
                id: THEIR_UT_HOLEPUNCH,
                ..
            }
        )
    };
    let (a, b) = tokio::join!(
        p1.next_matching_within(Duration::from_secs(5), is_hp),
        p2.next_matching_within(Duration::from_secs(5), is_hp)
    );
    assert!(a.is_none() && b.is_none(), "relayed: {a:?} {b:?}");
    assert_eq!(r.holepunch_stats(), (0, 0, 0));
    r.shutdown().await;
}

/// The control for `private_torrent_never_relays`: the same setup over a
/// public torrent relays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_torrent_relays_between_scripted_peers() {
    let (m, payload) = make_torrent(&[("a", 100_000)], 16_384);
    let (r, hr, _evr) = leecher(settings(), &m, payload.len()).await;
    let (mut p1, _) = scripted(&r, &m, Some(7001)).await;
    let (mut p2, _) = scripted(&r, &m, Some(7002)).await;
    assert!(wait_until(|| hr.status().num_peers == 2, T).await);
    p1.send(rendezvous("127.0.0.1:7002".parse().unwrap())).await;
    let to_p1 = p1
        .next_ext(THEIR_UT_HOLEPUNCH)
        .await
        .expect("Connect to p1");
    let to_p2 = p2
        .next_ext(THEIR_UT_HOLEPUNCH)
        .await
        .expect("Connect to p2");
    assert_eq!(
        HolepunchMsg::decode(&to_p1).unwrap(),
        HolepunchMsg::Connect("127.0.0.1:7002".parse().unwrap())
    );
    assert_eq!(
        HolepunchMsg::decode(&to_p2).unwrap(),
        HolepunchMsg::Connect("127.0.0.1:7001".parse().unwrap())
    );
    assert_eq!(r.holepunch_stats(), (0, 1, 0));
    r.shutdown().await;
}
