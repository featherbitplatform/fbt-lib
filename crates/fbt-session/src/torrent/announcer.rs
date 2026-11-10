//! The announcer: one task per torrent with trackers, announcing through
//! the tiers (BEP 12).
//!
//! The actor never waits on it: it publishes [`Signals`] through a watch
//! channel and receives peers through a bounded channel it polls. Transfer
//! totals come from the torrent's status watch.
//!
//! - `started` when the torrent becomes active (after checking, on resume);
//! - `none` after the tracker's `interval`, or on a forced re-announce once
//!   `min interval` has passed since the last announce;
//! - `completed` once, when the torrent finished during this session;
//! - `stopped` when it stops being active (pause, error, remove, shutdown):
//!   one attempt to the last tracker that answered, with a 5 s timeout.
//!
//! A walk through the tiers that fails everywhere backs off for
//! `Settings::tracker_retry_base` × 2^n (n = earlier consecutive failed
//! walks), capped at 30 minutes. Trackers whose URL cannot work (unsupported
//! scheme, invalid URL, HTTPS without TLS) are reported once and dropped.
//!
//! Private torrents (BEP 27) need nothing special here: milestone 1 has no
//! DHT, PEX or LSD, so trackers are already the only peer source.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use fbt_core::InfoHashes;
use fbt_tracker::{AnnounceEvent, AnnounceRequest, AnnounceResponse, TrackerClient, TrackerError};
use rand::RngExt;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::debug;

use super::tiers::Tiers;
use crate::event::Event;
use crate::session::Shared;
use crate::status::TorrentStatus;

/// Timeout of a `stopped` announce, and how long shutdown waits for it.
pub(super) const STOPPED_TIMEOUT: Duration = Duration::from_secs(5);
/// Longest wait between announces, whatever the tracker asks for.
const MAX_INTERVAL: Duration = Duration::from_secs(2 * 60 * 60);
/// Longest backoff after failed walks through the tiers.
const MAX_BACKOFF: Duration = Duration::from_secs(30 * 60);
/// Peer batches waiting for the actor; more are dropped.
const PEER_QUEUE: usize = 16;
/// Peers taken from one reply.
const MAX_PEERS_PER_REPLY: usize = 200;

/// What the actor wants from the announcer.
#[derive(Debug, Clone, Default)]
pub(super) struct Signals {
    /// The torrent is active (checked, not paused, no error).
    pub running: bool,
    /// Bumped each time the torrent becomes active: a new `started`.
    pub starts: u64,
    /// The torrent finished during this session.
    pub completed: bool,
    /// Bumped by each forced re-announce.
    pub reannounces: u64,
    /// The torrent is going away: announce `stopped` and exit.
    pub exit: bool,
}

/// The actor's side of a running announcer.
#[derive(Debug)]
pub(super) struct Announcer {
    pub signals: watch::Sender<Signals>,
    pub task: JoinHandle<()>,
}

/// Everything an announcer task needs.
#[derive(Debug)]
pub(super) struct Params {
    pub ih: InfoHashes,
    pub trackers: Vec<Vec<String>>,
    pub shared: Arc<Shared>,
    pub status: watch::Receiver<TorrentStatus>,
}

/// Starts the announcer for a torrent; `None` if it has no tracker. Peers
/// from replies arrive on the returned receiver.
pub(super) fn spawn(p: Params) -> Option<(Announcer, mpsc::Receiver<Vec<SocketAddr>>)> {
    let client = p.shared.tracker.clone();
    let mut rng = rand::rng();
    let tiers = Tiers::new(p.trackers, &mut rng);
    if tiers.is_empty() {
        return None;
    }
    let (signals, signals_rx) = watch::channel(Signals::default());
    let (peers_tx, peers_rx) = mpsc::channel(PEER_QUEUE);
    let task = Task {
        ih: p.ih,
        info_hash: p.ih.wire_hash(),
        key: rng.random(),
        client,
        tiers,
        shared: p.shared,
        status: p.status,
        signals: signals_rx,
        peers: peers_tx,
        tracker_ids: HashMap::new(),
        last_ok: None,
        failed_walks: 0,
    };
    let task = tokio::spawn(task.run());
    Some((Announcer { signals, task }, peers_rx))
}

/// How a walk through the tiers ended.
enum Walk {
    Ok(AnnounceResponse),
    Failed,
    /// Every tracker was dropped as unusable.
    NoTrackers,
}

/// What the announcer was told while waiting or announcing.
enum Interrupt {
    /// `running`, `starts` or `exit` changed: re-plan now.
    Lifecycle,
    /// Every sender is gone.
    Closed,
}

struct Task {
    ih: InfoHashes,
    info_hash: [u8; 20],
    key: u32,
    client: TrackerClient,
    tiers: Tiers,
    shared: Arc<Shared>,
    status: watch::Receiver<TorrentStatus>,
    signals: watch::Receiver<Signals>,
    peers: mpsc::Sender<Vec<SocketAddr>>,
    /// `tracker id` per URL, echoed back.
    tracker_ids: HashMap<String, Vec<u8>>,
    /// The tracker that last answered: `stopped` goes there.
    last_ok: Option<String>,
    failed_walks: u32,
}

/// Where the current run (since the last `started`) stands.
#[derive(Debug, Default)]
struct Run {
    /// `starts` value this run belongs to.
    starts: u64,
    /// `started` was answered.
    started: bool,
    /// `completed` was answered (at most once per session).
    completed: bool,
    /// `reannounces` already served.
    reannounces: u64,
    /// When the last announce was answered and the intervals it gave.
    last: Option<(Instant, Duration, Option<Duration>)>,
    /// No announce before this (backoff).
    not_before: Option<Instant>,
}

impl Task {
    async fn run(mut self) {
        let mut run = Run::default();
        let mut completed_sent = false;
        loop {
            let sig = self.signals.borrow_and_update().clone();
            if sig.exit || !sig.running {
                if run.started {
                    self.stopped().await;
                }
                run = Run::default();
                if sig.exit {
                    return;
                }
                if let Interrupt::Closed = self.wait_lifecycle(None).await {
                    return;
                }
                continue;
            }
            if run.starts != sig.starts {
                run = Run {
                    starts: sig.starts,
                    reannounces: sig.reannounces,
                    ..Run::default()
                };
            }
            let (event, due) = self.plan(&run, &sig, completed_sent);
            let now = Instant::now();
            if due > now {
                match self.wait_lifecycle(Some(due)).await {
                    Interrupt::Closed => return,
                    Interrupt::Lifecycle => continue,
                }
            }
            match self.walk_interruptible(event).await {
                Err(Interrupt::Closed) => return,
                Err(Interrupt::Lifecycle) => continue,
                Ok(Walk::NoTrackers) => {
                    // Nothing left to announce to; wait for the end.
                    let _ = self.signals.wait_for(|s| s.exit).await;
                    return;
                }
                Ok(Walk::Failed) => {
                    run.not_before = Some(Instant::now() + self.backoff());
                    self.failed_walks = self.failed_walks.saturating_add(1);
                    run.reannounces = sig.reannounces;
                }
                Ok(Walk::Ok(resp)) => {
                    self.failed_walks = 0;
                    run.not_before = None;
                    run.reannounces = sig.reannounces;
                    run.last = Some((
                        Instant::now(),
                        resp.interval.min(MAX_INTERVAL),
                        resp.min_interval.map(|d| d.min(MAX_INTERVAL)),
                    ));
                    match event {
                        AnnounceEvent::Started => run.started = true,
                        AnnounceEvent::Completed => {
                            run.completed = true;
                            completed_sent = true;
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    /// The next announce of an active torrent and when it is due.
    fn plan(&self, run: &Run, sig: &Signals, completed_sent: bool) -> (AnnounceEvent, Instant) {
        let now = Instant::now();
        let backoff = run.not_before.unwrap_or(now);
        let forced = sig.reannounces != run.reannounces;
        if !run.started {
            let due = if forced { now } else { backoff };
            return (AnnounceEvent::Started, due);
        }
        let event = if sig.completed && !completed_sent && !run.completed {
            AnnounceEvent::Completed
        } else {
            AnnounceEvent::None
        };
        let Some((at, interval, min_interval)) = run.last else {
            return (event, backoff);
        };
        let due = if forced {
            at + min_interval.unwrap_or(Duration::ZERO)
        } else if event == AnnounceEvent::Completed {
            backoff
        } else {
            (at + interval).max(backoff)
        };
        (event, due)
    }

    fn backoff(&self) -> Duration {
        let base = self.shared.settings.tracker_retry_base;
        let factor = 1u32.checked_shl(self.failed_walks).unwrap_or(u32::MAX);
        base.saturating_mul(factor).min(MAX_BACKOFF)
    }

    /// Waits until `until` (or forever) for a lifecycle change. A forced
    /// re-announce or completion counts too, since either can move the next
    /// announce earlier.
    async fn wait_lifecycle(&mut self, until: Option<Instant>) -> Interrupt {
        let sleep = async {
            match until {
                Some(t) => tokio::time::sleep_until(t).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            r = self.signals.changed() => match r {
                Ok(()) => Interrupt::Lifecycle,
                Err(_) => Interrupt::Closed,
            },
            () = sleep => Interrupt::Lifecycle,
        }
    }

    /// Walks the tiers, abandoning the walk if the torrent stops or starts
    /// over meanwhile. Other signal changes are handled after the walk.
    async fn walk_interruptible(&mut self, event: AnnounceEvent) -> Result<Walk, Interrupt> {
        let before = self.signals.borrow_and_update().clone();
        let mut signals = self.signals.clone();
        let req = self.request_template(event);
        let walk = walk(
            &self.client,
            &mut self.tiers,
            req,
            &self.shared,
            self.ih,
            &mut self.tracker_ids,
        );
        tokio::pin!(walk);
        loop {
            tokio::select! {
                (w, ok) = &mut walk => {
                    if let (Walk::Ok(resp), Some(url)) = (&w, ok) {
                        self.last_ok = Some(url);
                        if !resp.peers.is_empty() {
                            let mut peers = resp.peers.clone();
                            peers.truncate(MAX_PEERS_PER_REPLY);
                            // Dropped if the actor is behind; the next reply
                            // brings more.
                            let _ = self.peers.try_send(peers);
                        }
                    }
                    return Ok(w);
                }
                r = signals.changed() => {
                    if r.is_err() {
                        return Err(Interrupt::Closed);
                    }
                    let now = signals.borrow_and_update().clone();
                    if now.exit || now.running != before.running || now.starts != before.starts {
                        return Err(Interrupt::Lifecycle);
                    }
                }
            }
        }
    }

    fn request_template(&self, event: AnnounceEvent) -> AnnounceRequest {
        let st = self.status.borrow();
        AnnounceRequest {
            info_hash: self.info_hash,
            peer_id: self.shared.peer_id,
            port: self.shared.listen_port,
            uploaded: st.total_uploaded,
            downloaded: st.total_downloaded,
            left: st.total_wanted.saturating_sub(st.wanted_done),
            event,
            num_want: Some(self.shared.settings.num_want),
            key: self.key,
            tracker_id: None,
        }
    }

    /// One `stopped` announce to the last tracker that answered, never
    /// retried.
    async fn stopped(&mut self) {
        let Some(url) = self.last_ok.clone() else {
            return;
        };
        let mut req = self.request_template(AnnounceEvent::Stopped);
        req.num_want = Some(0);
        req.tracker_id = self.tracker_ids.get(&url).cloned();
        match tokio::time::timeout(STOPPED_TIMEOUT, self.client.announce(&url, &req)).await {
            Ok(Ok(resp)) => self.shared.emit(Event::TrackerReply {
                torrent: self.ih,
                url,
                peers: resp.peers.len(),
            }),
            Ok(Err(e)) => self.error(url, &e),
            Err(_) => self.error(url, &TrackerError::Timeout),
        }
    }

    fn error(&self, url: String, e: &TrackerError) {
        emit_error(&self.shared, self.ih, url, e);
    }
}

fn emit_error(shared: &Shared, ih: InfoHashes, url: String, e: &TrackerError) {
    debug!(%url, error = %e, "announce failed");
    shared.emit(Event::TrackerError {
        torrent: ih,
        url,
        message: e.to_string(),
    });
}

/// A URL that can never work in this build.
fn unusable(e: &TrackerError) -> bool {
    matches!(
        e,
        TrackerError::UnsupportedScheme(_)
            | TrackerError::InvalidUrl(_)
            | TrackerError::TlsDisabled
    )
}

/// One walk through the tiers from the first URL until a tracker answers or
/// every one has failed. Returns the outcome and the URL that answered.
async fn walk(
    client: &TrackerClient,
    tiers: &mut Tiers,
    mut req: AnnounceRequest,
    shared: &Shared,
    ih: InfoHashes,
    tracker_ids: &mut HashMap<String, Vec<u8>>,
) -> (Walk, Option<String>) {
    tiers.restart();
    loop {
        let Some(url) = tiers.current().map(str::to_owned) else {
            return (Walk::NoTrackers, None);
        };
        req.tracker_id = tracker_ids.get(&url).cloned();
        match client.announce(&url, &req).await {
            Ok(resp) => {
                tiers.report_success();
                if let Some(id) = &resp.tracker_id {
                    tracker_ids.insert(url.clone(), id.clone());
                }
                shared.emit(Event::TrackerReply {
                    torrent: ih,
                    url: url.clone(),
                    peers: resp.peers.len(),
                });
                return (Walk::Ok(resp), Some(url));
            }
            Err(e) => {
                let drop_it = unusable(&e);
                emit_error(shared, ih, url, &e);
                if drop_it {
                    tiers.remove_current();
                    if tiers.is_empty() {
                        return (Walk::NoTrackers, None);
                    }
                } else {
                    tiers.report_failure();
                }
                if tiers.all_failed_once() {
                    return (Walk::Failed, None);
                }
            }
        }
    }
}
