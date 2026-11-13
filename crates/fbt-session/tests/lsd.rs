//! BEP 14 local service discovery over unicast loopback (`lsd_bind` and
//! `lsd_groups`): nothing here ever touches a multicast group or the LAN.

mod support;

use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fbt_metainfo::Metainfo;
use fbt_net::lsd::{LSD_V4, LsdMessage};
use fbt_session::storage::MemStorage;
use fbt_session::{
    AddTorrent, Event, EventMask, PeerSource, Session, Settings, TorrentHandle, TorrentState,
};
use support::*;

const T: Duration = Duration::from_secs(30);

fn free_udp() -> SocketAddr {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    s.local_addr().unwrap()
}

/// LSD on, bound to `bind` and announcing to `to`.
fn lsd_settings(bind: SocketAddr, to: SocketAddr) -> Settings {
    let mut s = test_settings();
    s.enable_lsd = true;
    s.lsd_bind = Some(bind);
    s.lsd_groups = vec![to];
    s
}

/// A plain socket standing in for the LAN: records what the session sends
/// it and can inject announces.
struct Lan {
    socket: tokio::net::UdpSocket,
    addr: SocketAddr,
}

impl Lan {
    async fn new() -> Lan {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        Lan { socket, addr }
    }

    /// Every announced hash within `d`, as `(hash, port)`.
    async fn announces_within(&self, d: Duration) -> Vec<([u8; 20], u16)> {
        let mut out = Vec::new();
        let end = tokio::time::Instant::now() + d;
        let mut buf = [0u8; 2048];
        while let Ok(Ok((n, _))) =
            tokio::time::timeout_at(end, self.socket.recv_from(&mut buf)).await
        {
            if let Ok(m) = LsdMessage::decode(&buf[..n]) {
                out.extend(m.info_hashes.iter().map(|h| (*h, m.port)));
            }
        }
        out
    }

    /// Every message received within `d`.
    async fn messages_within(&self, d: Duration) -> Vec<LsdMessage> {
        let mut out = Vec::new();
        let end = tokio::time::Instant::now() + d;
        let mut buf = [0u8; 2048];
        while let Ok(Ok((n, _))) =
            tokio::time::timeout_at(end, self.socket.recv_from(&mut buf)).await
        {
            if let Ok(m) = LsdMessage::decode(&buf[..n]) {
                out.push(m);
            }
        }
        out
    }

    async fn announce_to(&self, to: SocketAddr, hash: [u8; 20], port: u16) {
        let m = LsdMessage {
            port,
            info_hashes: vec![hash],
            cookie: Some("lan-host".to_owned()),
        };
        self.socket.send_to(&m.encode(LSD_V4), to).await.unwrap();
    }
}

async fn seeder(s: Settings, m: &Metainfo, payload: &[u8]) -> (Session, TorrentHandle) {
    let session = Session::new(s).await.unwrap();
    let mut p = AddTorrent::new(m.clone(), PathBuf::from("unused"));
    p.storage = Some(Arc::new(MemStorage::from_bytes(payload.to_vec())));
    let h = session.add_torrent(p).await.unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    (session, h)
}

#[tokio::test]
async fn lsd_peer_found_and_download_completes() {
    let (m, payload) = make_torrent(&[("a", 200_000)], 16 * 1024);
    let (seed_lsd, leech_lsd) = (free_udp(), free_udp());
    let mut ss = lsd_settings(seed_lsd, leech_lsd);
    let mut ls = lsd_settings(leech_lsd, seed_lsd);
    ss.lsd_announce_interval = Duration::from_secs(1);
    ls.lsd_announce_interval = Duration::from_secs(1);

    let leech = Session::new(ls).await.unwrap();
    let mut ev = leech.subscribe(EventMask::PEER | EventMask::STATUS);
    let dir = tempfile::tempdir().unwrap();
    let h = leech
        .add_torrent(AddTorrent::new(m.clone(), dir.path().to_path_buf()))
        .await
        .unwrap();
    let (seed, _sh) = seeder(ss, &m, &payload).await;

    let found = next_matching(
        &mut ev,
        |e| {
            matches!(
                e,
                Event::PeersFound {
                    source: PeerSource::Lsd,
                    ..
                }
            )
        },
        T,
    )
    .await;
    assert!(found.is_some(), "the leecher learns the seeder through LSD");
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    seed.shutdown().await;
    leech.shutdown().await;
}

#[tokio::test]
async fn resume_does_not_reannounce_within_interval() {
    let (m, payload) = make_torrent(&[("a", 32_000)], 16 * 1024);
    let lan = Lan::new().await;
    let (session, h) = seeder(lsd_settings(free_udp(), lan.addr), &m, &payload).await;
    let hash = m.info_hashes.wire_hash();
    let first = lan.announces_within(Duration::from_secs(3)).await;
    assert_eq!(first.iter().filter(|(x, _)| *x == hash).count(), 1);
    for _ in 0..3 {
        h.pause().await.unwrap();
        h.resume().await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let more = lan.announces_within(Duration::from_secs(1)).await;
    assert_eq!(
        more.iter().filter(|(x, _)| *x == hash).count(),
        0,
        "pause and resume stay within the announce interval"
    );
    session.shutdown().await;
}

#[tokio::test]
async fn private_torrent_not_announced() {
    let (public, p_payload) = make_torrent(&[("a", 32_000)], 16 * 1024);
    let (private, v_payload) = make_private_torrent(&[("b", 32_000)], 16 * 1024);
    let lan = Lan::new().await;
    let bind = free_udp();
    let (session, _h) = seeder(lsd_settings(bind, lan.addr), &public, &p_payload).await;
    let mut ev = session.subscribe(EventMask::PEER);
    let _p = {
        let mut p = AddTorrent::new(private.clone(), PathBuf::from("unused"));
        p.storage = Some(Arc::new(MemStorage::from_bytes(v_payload)));
        session.add_torrent(p).await.unwrap()
    };
    let (pub_hash, priv_hash) = (
        public.info_hashes.wire_hash(),
        private.info_hashes.wire_hash(),
    );
    let seen = lan.announces_within(Duration::from_secs(3)).await;
    assert!(
        seen.iter().any(|(x, _)| *x == pub_hash),
        "control: public is announced"
    );
    assert!(!seen.iter().any(|(x, _)| *x == priv_hash), "private is not");

    // Peers announced for the private hash are dropped; for the public one
    // they are accepted.
    lan.announce_to(bind, priv_hash, 9).await;
    let none = next_matching(
        &mut ev,
        |e| {
            matches!(
                e,
                Event::PeersFound {
                    source: PeerSource::Lsd,
                    ..
                }
            )
        },
        Duration::from_secs(1),
    )
    .await;
    assert!(
        none.is_none(),
        "LSD peers for a private torrent are dropped"
    );
    lan.announce_to(bind, pub_hash, 9).await;
    let some = next_matching(
        &mut ev,
        |e| {
            matches!(
                e,
                Event::PeersFound {
                    source: PeerSource::Lsd,
                    ..
                }
            )
        },
        T,
    )
    .await;
    assert!(some.is_some());
    session.shutdown().await;
}

/// A hybrid goes out in one message carrying both of its hashes: the v1
/// hash, then the truncated v2 hash.
#[tokio::test]
async fn lsd_carries_both_hashes() {
    use fbt_core::InfoHashes;
    use fbt_metainfo::TorrentVersion;
    let dir = tempfile::tempdir().unwrap();
    let bytes = create_torrent(
        &write_v2_payload(dir.path()),
        TorrentVersion::Hybrid,
        32_768,
    );
    let m = Metainfo::from_bytes(&bytes).unwrap();
    let v1 = m.info_hashes.v1.expect("v1 hash").0;
    let v2 = InfoHashes::from_v2(m.info_hashes.v2.expect("v2 hash")).wire_hash();
    let lan = Lan::new().await;
    let session = Session::new(lsd_settings(free_udp(), lan.addr))
        .await
        .unwrap();
    let h = session
        .add_torrent(AddTorrent::new(m, dir.path().to_path_buf()))
        .await
        .unwrap();
    assert!(wait_state(&h, |s| *s == TorrentState::Seeding, T).await);
    let messages = lan.messages_within(Duration::from_secs(3)).await;
    assert_eq!(messages.len(), 1, "{messages:?}");
    assert_eq!(messages[0].info_hashes, vec![v1, v2]);
    session.shutdown().await;
}
