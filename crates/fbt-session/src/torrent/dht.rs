//! The DHT announcer: one task per public torrent while the session runs a
//! DHT, looking up peers and announcing us (`get_peers` + `announce_peer`).
//!
//! Driven by the same [`Signals`] watch as the tracker announcer: it runs
//! while `running` is set and exits at `exit` (or when the actor is gone).
//! A run announces once the DHT knows a node, then every
//! `Settings::dht_announce_interval`; while the torrent fetches its metadata,
//! every [`METADATA_INTERVAL`] instead, never more often. A torrent with
//! every piece ([`TorrentStatus::is_seed`], libtorrent's `is_seed`)
//! announces with the BEP 33 `seed` flag; one that only finished its wanted
//! pieces does not, though the tracker announcer reports `left` = 0 for it
//! (`left` counts wanted bytes, as libtorrent's does). Found peers reach
//! the actor through a bounded channel it polls; the actor never waits.
//!
//! Private torrents get no announcer. A magnet's privacy is unknown until
//! its metadata arrives, so it runs one meanwhile; the actor drops it (which
//! aborts the task) once the metadata says private.

use std::net::SocketAddr;
use std::time::Duration;

use fbt_dht::Dht;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::announcer::Signals;
use crate::status::TorrentStatus;

/// Time between announces while a magnet fetches its metadata.
pub(super) const METADATA_INTERVAL: Duration = Duration::from_secs(60);
/// Peer batches waiting for the actor; more are dropped.
const PEER_QUEUE: usize = 16;
/// Peers forwarded from one lookup batch.
const MAX_PEERS_PER_BATCH: usize = 200;

/// The actor's side of a running DHT announcer; dropping it stops the task.
#[derive(Debug)]
pub(super) struct DhtAnnouncer {
    task: JoinHandle<()>,
}

impl Drop for DhtAnnouncer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Everything a DHT announcer task needs.
#[derive(Debug)]
pub(super) struct DhtParams {
    pub dht: Dht,
    pub info_hash: [u8; 20],
    /// The peer listener's port, announced to the DHT.
    pub port: u16,
    pub interval: Duration,
    pub signals: watch::Receiver<Signals>,
    pub status: watch::Receiver<TorrentStatus>,
}

/// Starts the DHT announcer; peers found arrive on the returned receiver.
pub(super) fn spawn(p: DhtParams) -> (DhtAnnouncer, mpsc::Receiver<Vec<SocketAddr>>) {
    let (tx, rx) = mpsc::channel(PEER_QUEUE);
    let task = tokio::spawn(run(p, tx));
    (DhtAnnouncer { task }, rx)
}

/// Whether the task goes on after a wait.
enum Next {
    Go,
    Exit,
}

async fn run(mut p: DhtParams, peers: mpsc::Sender<Vec<SocketAddr>>) {
    let mut nodes = p.dht.subscribe_stats();
    // When the last announce began; pause and resume do not reset it.
    let mut last: Option<Instant> = None;
    loop {
        // Wait for a running torrent.
        match p.signals.wait_for(|s| s.running || s.exit).await {
            Ok(s) if s.exit => return,
            Ok(_) => {}
            Err(_) => return,
        }
        let starts = p.signals.borrow().starts;
        let interval = if p.status.borrow().has_metadata {
            p.interval
        } else {
            METADATA_INTERVAL
        };
        if let Some(due) = last.map(|t| t + interval) {
            tokio::select! {
                () = tokio::time::sleep_until(due) => {}
                n = stopped(&mut p.signals, starts) => match n {
                    Next::Exit => return,
                    Next::Go => continue,
                },
            }
            if !still_running(&p.signals, starts) {
                continue;
            }
        }
        // A lookup through an empty table finds nothing: wait for a node.
        tokio::select! {
            r = nodes.wait_for(|s| s.nodes_v4 > 0 || s.nodes_v6 > 0) => if r.is_err() {
                return;
            },
            n = stopped(&mut p.signals, starts) => match n {
                Next::Exit => return,
                Next::Go => continue,
            },
        }
        last = Some(Instant::now());
        let seed = p.status.borrow().is_seed;
        let mut lookup = p.dht.announce(p.info_hash, Some(p.port), seed);
        loop {
            tokio::select! {
                batch = lookup.next() => match batch {
                    Some(mut batch) => {
                        batch.truncate(MAX_PEERS_PER_BATCH);
                        // Dropped if the actor is behind; the next lookup
                        // brings more.
                        let _ = peers.try_send(batch);
                    }
                    None => break,
                },
                n = stopped(&mut p.signals, starts) => match n {
                    Next::Exit => return,
                    // Stopped or restarted: drop the lookup and re-plan.
                    Next::Go => break,
                },
            }
        }
    }
}

fn still_running(signals: &watch::Receiver<Signals>, starts: u64) -> bool {
    let s = signals.borrow();
    s.running && !s.exit && s.starts == starts
}

/// Resolves once the torrent stops, restarts (a new `starts`) or exits.
async fn stopped(signals: &mut watch::Receiver<Signals>, starts: u64) -> Next {
    match signals
        .wait_for(|s| s.exit || !s.running || s.starts != starts)
        .await
    {
        Ok(s) if s.exit => Next::Exit,
        Ok(_) => Next::Go,
        Err(_) => Next::Exit,
    }
}
