//! The adaptive status tick: an idle torrent (seeding or paused, with
//! nothing going on) ticks every `idle_status_interval`, and anything that
//! needs the fast tick — a peer, an open reader, a resume — switches back
//! to `status_interval` at once.

mod support;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fbt_metainfo::Metainfo;
use fbt_session::storage::MemStorage;
use fbt_session::{AddTorrent, EncryptionPolicy, Session, Settings, TorrentHandle, TorrentState};
use fbt_wire::{Message, Reserved};
use support::*;
use tokio::net::TcpListener;

/// `test_settings` with the slow tick switched on: 50 ms normally and 2 s
/// for an idle torrent, so the windows below measure the switch itself.
fn idle_settings() -> Settings {
    let mut s = test_settings();
    s.idle_status_interval = Duration::from_secs(2);
    s
}

/// Adds `m` with its whole payload in memory and waits for it to seed: a
/// torrent that is idle the moment the check ends.
async fn seed_all_data(s: &Session, m: &Metainfo, payload: &[u8]) -> TorrentHandle {
    let mut p = AddTorrent::new(m.clone(), "unused".into());
    p.storage = Some(Arc::new(MemStorage::from_bytes(payload.to_vec())));
    let h = s.add_torrent(p).await.unwrap();
    assert!(
        wait_state(&h, |st| *st == TorrentState::Seeding, T).await,
        "the seed never reached Seeding"
    );
    h
}

/// Polls `h.status_ticks()` until it reaches `want`; false if that takes
/// longer than `budget`.
async fn ticks_reach(h: &TorrentHandle, want: u64, budget: Duration) -> bool {
    let end = tokio::time::Instant::now() + budget;
    loop {
        if h.status_ticks().await >= want {
            return true;
        }
        if tokio::time::Instant::now() >= end {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A seeding torrent with no peers ticks about once per
/// `idle_status_interval` (2 s here), not once per `status_interval` (50 ms).
#[tokio::test]
async fn idle_torrent_uses_the_slow_tick() {
    let s = Session::new(idle_settings()).await.unwrap();
    let (m, payload) = make_torrent(&[("a", 40_000), ("sub/b", 30_000)], 16_384);
    let h = seed_all_data(&s, &m, &payload).await;
    let base = h.status_ticks().await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let ticks = h.status_ticks().await - base;
    assert!(
        (1..=2).contains(&ticks),
        "{ticks} ticks in 3 s: expected about one every 2 s, got {} per second",
        ticks as f64 / 3.0
    );
    s.shutdown().await;
}

/// `add_peer` of a live leech ends the idleness at once: the tick runs at
/// `status_interval` again within a second, not whenever the slow tick that
/// was already due comes round.
#[tokio::test]
async fn peer_arrival_restores_the_fast_tick() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let leech_addr = listener.local_addr().unwrap();
    // A scripted peer answers a plain handshake, so dial it TCP-only and
    // with encryption off (as `swarm.rs`'s `plain_dialler` does).
    let mut cfg = idle_settings();
    cfg.encryption = EncryptionPolicy::Disabled;
    let s = Session::new(dial_tcp(cfg)).await.unwrap();
    let (m, payload) = make_torrent(&[("a", 40_000)], 16_384);
    let h = seed_all_data(&s, &m, &payload).await;
    // The leech: accepts our dial, claims no pieces, stays interested and
    // keeps the connection up for the whole window.
    tokio::spawn(async move {
        let mut p = ScriptedPeer::accept_with(&listener, Reserved::default()).await;
        p.send(Message::Bitfield(Bytes::from_static(&[0x00]))).await;
        p.send(Message::Interested).await;
        tokio::time::sleep(Duration::from_secs(30)).await;
    });
    let base = h.status_ticks().await;
    h.add_peer(leech_addr).await.unwrap();
    assert!(
        ticks_reach(&h, base + 10, Duration::from_secs(1)).await,
        "the tick stayed slow after a peer arrived"
    );
    s.shutdown().await;
}

/// An open reader is tick-driven work (its window and deadlines move with
/// the clock), so a seeding torrent with one still ticks fast.
#[tokio::test]
async fn reader_keeps_the_fast_tick() {
    let s = Session::new(idle_settings()).await.unwrap();
    let (m, payload) = make_torrent(&[("a", 40_000), ("sub/b", 30_000)], 16_384);
    let h = seed_all_data(&s, &m, &payload).await;
    let reader = h.open_file(0).await.unwrap();
    let base = h.status_ticks().await;
    assert!(
        ticks_reach(&h, base + 10, Duration::from_secs(1)).await,
        "the tick slowed down with a reader open"
    );
    drop(reader);
    s.shutdown().await;
}

/// Leaving idle publishes at once: after `pause` then `resume` the state is
/// fresh within 200 ms, not whenever the slow tick next comes round.
#[tokio::test]
async fn status_is_fresh_after_leaving_idle() {
    let s = Session::new(idle_settings()).await.unwrap();
    let (m, payload) = make_torrent(&[("a", 40_000)], 16_384);
    let h = seed_all_data(&s, &m, &payload).await;
    h.pause().await.unwrap();
    assert_eq!(h.status().state, TorrentState::Paused);
    h.resume().await.unwrap();
    let fresh = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            if h.status().state != TorrentState::Paused {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        fresh,
        "status still {:?} 200 ms after resume",
        h.status().state
    );
    s.shutdown().await;
}
