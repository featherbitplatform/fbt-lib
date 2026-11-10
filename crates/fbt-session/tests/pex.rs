//! Peer exchange (BEP 11): peers learned through a connected peer, no PEX in
//! either direction for private torrents or with `pex_enabled` off, a
//! private magnet stopping PEX at its metadata, and floods of hostile PEX
//! messages bounded. `pex_interval` is 500 ms, so the receive gap is 375 ms.

mod support;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fbt_metainfo::{Magnet, Metainfo};
use fbt_session::storage::MemStorage;
use fbt_session::{
    AddTorrent, EncryptionPolicy, Event, EventMask, EventStream, PeerSource, Session, Settings,
    TorrentHandle, TorrentState, Transport,
};
use fbt_wire::{
    ExtHandshake, Message, MetadataMsg, OUR_UT_METADATA_ID, OUR_UT_PEX_ID, PexFlags, PexMsg,
    Reserved, UT_METADATA, UT_PEX,
};
use futures_util::StreamExt;
use support::*;
use tokio::net::TcpListener;

const T: Duration = Duration::from_secs(30);
const PEX_INTERVAL: Duration = Duration::from_millis(500);
/// The `ut_pex` id our scripted peers assign.
const THEIR_UT_PEX: u8 = 5;
/// The `ut_metadata` id our scripted peers assign.
const THEIR_UT_METADATA: u8 = 3;

fn settings() -> Settings {
    let mut s = test_settings();
    s.rechoke_interval = Duration::from_secs(1);
    s.pex_interval = PEX_INTERVAL;
    s
}

fn ext_reserved() -> Reserved {
    let mut r = Reserved::default();
    r.set_fast();
    r.set_extended();
    r
}

fn is_pex_found(e: &Event) -> bool {
    matches!(
        e,
        Event::PeersFound {
            source: PeerSource::Pex,
            ..
        }
    )
}

/// Every `PeersFound { source: Pex }` count arriving within `d`.
async fn pex_counts_within(ev: &mut EventStream, d: Duration) -> Vec<usize> {
    let mut out = Vec::new();
    let end = tokio::time::Instant::now() + d;
    while let Ok(Some(e)) = tokio::time::timeout_at(end, ev.recv()).await {
        if let Event::PeersFound {
            source: PeerSource::Pex,
            count,
            ..
        } = e
        {
            out.push(count);
        }
    }
    out
}

/// A session seeding `m` from memory, waited until it seeds.
async fn seed_session(s: Settings, m: &Metainfo, payload: &[u8]) -> (Session, TorrentHandle) {
    let session = Session::new(s).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(payload.to_vec())));
    let h = session.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    (session, h)
}

/// A session leeching `m` into a fresh directory.
async fn leech_session(
    s: Settings,
    m: &Metainfo,
) -> (Session, TorrentHandle, EventStream, tempfile::TempDir) {
    let session = Session::new(s).await.unwrap();
    let ev = session.subscribe(EventMask::PEER | EventMask::STATUS);
    let dir = tempfile::tempdir().unwrap();
    let h = session
        .add_torrent(AddTorrent::new(m.clone(), dir.path().to_path_buf()))
        .await
        .unwrap();
    (session, h, ev, dir)
}

/// A BEP 10 handshake offering `ut_pex` with listen port `port`.
fn pex_hs(port: Option<u16>) -> ExtHandshake {
    let mut hs = ExtHandshake {
        listen_port: port,
        ..ExtHandshake::default()
    };
    hs.m.insert(UT_PEX.to_owned(), THEIR_UT_PEX);
    hs
}

/// The session's (first or repeated) extension handshake to `peer`.
async fn our_ext_hs(peer: &mut ScriptedPeer) -> ExtHandshake {
    let payload = peer.next_ext(0).await.expect("our extension handshake");
    ExtHandshake::decode(&payload).expect("valid handshake")
}

/// A `ut_pex` message to the session.
fn pex_to_us(added: &[SocketAddr]) -> Message {
    let msg = PexMsg {
        added: added.iter().map(|a| (*a, PexFlags::CONNECTABLE)).collect(),
        dropped: Vec::new(),
    };
    Message::Extended {
        id: OUR_UT_PEX_ID,
        payload: msg.encode(),
    }
}

/// The next `ut_pex` message the session sends `peer` within `d`.
async fn pex_from_us(peer: &mut ScriptedPeer, d: Duration) -> Option<PexMsg> {
    let r = tokio::time::timeout(d, async {
        while let Some(Ok(m)) = peer.framed.next().await {
            if let Message::Extended {
                id: THEIR_UT_PEX,
                payload,
            } = m
            {
                return Some(PexMsg::decode(&payload).expect("valid ut_pex"));
            }
        }
        None
    })
    .await;
    r.ok().flatten()
}

fn loopback(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn third_peer_learned_through_pex() {
    let (m, payload) = make_torrent(&[("a", 300_000)], 32_768);
    let (a, _ha) = seed_session(settings(), &m, &payload).await;
    let (b, hb, _evb, _db) = leech_session(settings(), &m).await;
    hb.add_peer(a.local_addr()).await.unwrap();
    assert!(wait_until(|| hb.status().num_peers >= 1, T).await);

    let (c, hc, mut evc, _dc) = leech_session(settings(), &m).await;
    hc.add_peer(b.local_addr()).await.unwrap();
    assert!(
        next_matching(&mut evc, is_pex_found, T).await.is_some(),
        "C learned no peers through PEX"
    );
    // Only B was given to C: the second peer is A, learned through PEX.
    assert!(wait_until(|| hc.status().num_peers == 2, T).await);
    assert!(wait_state(&hc, |s| *s == TorrentState::Seeding, T).await);
    for s in [c, b, a] {
        s.shutdown().await;
    }
}

/// A peer connected over uTP is listed with the `UTP` flag; the scripted
/// peer receiving the list is on TCP.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn utp_peer_is_advertised_with_the_utp_flag() {
    let (m, payload) = make_torrent(&[("a", 300_000)], 32_768);
    let (a, _ha) = seed_session(settings(), &m, &payload).await;
    let mut eva = a.subscribe(EventMask::PEER);
    let (b, hb, _evb, _db) = leech_session(settings(), &m).await;
    hb.add_peer(a.local_addr()).await.unwrap();
    let e = next_matching(&mut eva, |e| matches!(e, Event::PeerConnected { .. }), T).await;
    assert!(
        matches!(
            e,
            Some(Event::PeerConnected {
                transport: Transport::Utp,
                ..
            })
        ),
        "{e:?}"
    );
    let mut peer =
        ScriptedPeer::connect_with(a.local_addr(), m.info_hashes.wire_hash(), ext_reserved()).await;
    our_ext_hs(&mut peer).await;
    peer.send_ext_handshake(&pex_hs(Some(7000))).await;
    let msg = pex_from_us(&mut peer, T).await.expect("a PEX message");
    let (_, flags) = msg
        .added
        .iter()
        .find(|(addr, _)| *addr == b.local_addr())
        .unwrap_or_else(|| panic!("B not listed: {msg:?}"));
    assert!(
        flags.contains(PexFlags::UTP | PexFlags::CONNECTABLE),
        "{flags:?}"
    );
    for s in [b, a] {
        s.shutdown().await;
    }
}

/// A, B and C as in [`third_peer_learned_through_pex`], plus a scripted
/// peer at B advertising `ut_pex`: nobody exchanges peers.
async fn no_pex_anywhere(m: &Metainfo, payload: &[u8], s: Settings) {
    let (a, _ha) = seed_session(s.clone(), m, payload).await;
    let (b, hb, mut evb, _db) = leech_session(s.clone(), m).await;
    hb.add_peer(a.local_addr()).await.unwrap();
    assert!(wait_until(|| hb.status().num_peers >= 1, T).await);
    let (c, hc, mut evc, _dc) = leech_session(s, m).await;
    hc.add_peer(b.local_addr()).await.unwrap();
    assert!(wait_until(|| hb.status().num_peers >= 2, T).await);

    let mut peer =
        ScriptedPeer::connect_with(b.local_addr(), m.info_hashes.wire_hash(), ext_reserved()).await;
    let hs = our_ext_hs(&mut peer).await;
    assert_eq!(hs.id_of(UT_PEX), None, "ut_pex advertised");
    peer.send_ext_handshake(&pex_hs(Some(7000))).await;
    peer.send(pex_to_us(&[loopback(40_001)])).await;
    // B has A and C to share: PEX running would send them within 500 ms.
    assert_eq!(pex_from_us(&mut peer, Duration::from_secs(5)).await, None);
    assert!(
        pex_counts_within(&mut evc, Duration::from_millis(100))
            .await
            .is_empty()
    );
    assert!(
        pex_counts_within(&mut evb, Duration::from_millis(100))
            .await
            .is_empty()
    );
    for s in [c, b, a] {
        s.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_torrent_sends_and_accepts_no_pex() {
    let (m, payload) = make_private_torrent(&[("a", 300_000)], 32_768);
    assert!(m.info.private);
    no_pex_anywhere(&m, &payload, settings()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pex_disabled_setting_disables_both_directions() {
    let (m, payload) = make_torrent(&[("a", 300_000)], 32_768);
    let mut s = settings();
    s.pex_enabled = false;
    no_pex_anywhere(&m, &payload, s).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_magnet_stops_pex_at_metadata() {
    let (m, _payload) = make_private_torrent(&[("a", 100_000)], 32_768);
    let raw = m.raw_info().to_vec();
    let mut s = settings();
    // The scripted metadata source answers a plain handshake only, over
    // TCP only.
    s.encryption = EncryptionPolicy::Disabled;
    let leech = Session::new(dial_tcp(s)).await.unwrap();
    let mut ev = leech.subscribe(EventMask::PEER | EventMask::STATUS);
    let listener = TcpListener::bind(loopback(0)).await.unwrap();
    let source_addr = listener.local_addr().unwrap();
    let mut mg = Magnet::from_info_hashes(m.info_hashes);
    mg.peers = vec![source_addr];
    let dir = tempfile::tempdir().unwrap();
    let _h = leech
        .add_torrent(AddTorrent::magnet(mg, dir.path().to_path_buf()))
        .await
        .unwrap();

    // The `x.pe` peer holds the metadata; privacy is unknown until it is
    // in, so PEX is advertised.
    let mut source = ScriptedPeer::accept_with(&listener, ext_reserved()).await;
    let hs = our_ext_hs(&mut source).await;
    assert_eq!(hs.id_of(UT_PEX), Some(OUR_UT_PEX_ID));
    let mut theirs = pex_hs(None);
    theirs.m.insert(UT_METADATA.to_owned(), THEIR_UT_METADATA);
    theirs.metadata_size = Some(raw.len() as u32);
    source.send_ext_handshake(&theirs).await;
    // The session has registered the source as connected (its handshake
    // alone does not say so) before another peer dials in, so the two
    // connections never race in the torrent's bookkeeping.
    assert!(
        next_matching(
            &mut ev,
            |e| matches!(e, Event::PeerConnected { addr, .. } if *addr == source_addr),
            T
        )
        .await
        .is_some(),
        "the session registers the metadata source"
    );

    let mut other = ScriptedPeer::connect_with(
        leech.local_addr(),
        m.info_hashes.wire_hash(),
        ext_reserved(),
    )
    .await;
    let hs = our_ext_hs(&mut other).await;
    assert_eq!(hs.id_of(UT_PEX), Some(OUR_UT_PEX_ID));
    other.send_ext_handshake(&pex_hs(Some(7777))).await;

    // Before the metadata, PEX runs both ways.
    let to_other = pex_from_us(&mut other, Duration::from_secs(5))
        .await
        .expect("PEX before the metadata");
    assert!(to_other.added.iter().any(|(a, _)| *a == source_addr));
    other.send(pex_to_us(&[loopback(40_002)])).await;
    assert!(next_matching(&mut ev, is_pex_found, T).await.is_some());

    // The source gets the other peer's listen address and the metadata
    // request; then it serves the metadata.
    let mut told = false;
    let mut asked = false;
    tokio::time::timeout(T, async {
        while !(told && asked) {
            let msg = source.framed.next().await;
            let Some(Ok(msg)) = msg else {
                panic!("source connection ended: {msg:?}");
            };
            let Message::Extended { id, payload } = msg else {
                continue;
            };
            if id == THEIR_UT_PEX {
                let msg = PexMsg::decode(&payload).unwrap();
                told |= msg.added.iter().any(|(a, _)| *a == loopback(7777));
            } else if id == THEIR_UT_METADATA {
                asked |=
                    MetadataMsg::decode(&payload).unwrap() == MetadataMsg::Request { piece: 0 };
            }
        }
    })
    .await
    .expect("PEX and the metadata request reach the source");
    let data = MetadataMsg::Data {
        piece: 0,
        total_size: raw.len() as u32,
        data: Bytes::from(raw.clone()),
    };
    source
        .send(Message::Extended {
            id: OUR_UT_METADATA_ID,
            payload: data.encode(),
        })
        .await;
    assert!(
        next_matching(&mut ev, |e| matches!(e, Event::MetadataReceived { .. }), T)
            .await
            .is_some()
    );

    // Private now: the repeated handshake turns `ut_pex` off, ...
    let repeat = our_ext_hs(&mut other).await;
    assert_eq!(repeat.m.get(UT_PEX), Some(&0));
    // ... a new peer is not offered it, and is not shared with the others.
    let mut late = ScriptedPeer::connect_with(
        leech.local_addr(),
        m.info_hashes.wire_hash(),
        ext_reserved(),
    )
    .await;
    let hs = our_ext_hs(&mut late).await;
    assert_eq!(hs.id_of(UT_PEX), None);
    late.send_ext_handshake(&pex_hs(Some(7778))).await;
    assert_eq!(pex_from_us(&mut other, PEX_INTERVAL * 4).await, None);
    // An incoming `ut_pex` (well past the receive gap) is ignored.
    other.send(pex_to_us(&[loopback(40_003)])).await;
    assert!(
        pex_counts_within(&mut ev, Duration::from_secs(1))
            .await
            .is_empty()
    );
    leech.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pex_flood_is_bounded() {
    let (m, payload) = make_torrent(&[("a", 100_000)], 32_768);
    let (s, _h) = seed_session(settings(), &m, &payload).await;
    let mut ev = s.subscribe(EventMask::PEER);
    let mut peer =
        ScriptedPeer::connect_with(s.local_addr(), m.info_hashes.wire_hash(), ext_reserved()).await;
    our_ext_hs(&mut peer).await;
    peer.send_ext_handshake(&pex_hs(Some(7000))).await;
    let burst = |round: u16| -> Vec<Message> {
        (0..20u16)
            .map(|i| {
                let base = 20_000 + round * 4_000 + i * 200;
                let added: Vec<SocketAddr> = (0..200).map(|j| loopback(base + j)).collect();
                pex_to_us(&added)
            })
            .collect()
    };
    for msg in burst(0) {
        peer.send(msg).await;
    }
    // One message per gap: 50 of its 200 addresses, the other 19 dropped.
    assert_eq!(
        pex_counts_within(&mut ev, Duration::from_secs(2)).await,
        vec![50]
    );
    // After the gap, the next burst gets exactly one message in again.
    for msg in burst(1) {
        peer.send(msg).await;
    }
    assert_eq!(
        pex_counts_within(&mut ev, Duration::from_secs(2)).await,
        vec![50]
    );
    s.shutdown().await;
}
