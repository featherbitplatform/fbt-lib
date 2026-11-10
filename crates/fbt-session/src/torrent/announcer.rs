//! The announcer: one task per torrent with trackers, announcing through
//! the tiers (BEP 12).
//!
//! The actor never waits on it: it publishes [`Signals`] through a watch
//! channel and receives peers through a bounded channel it polls. Transfer
//! totals come from the torrent's status watch.
//!
//! - `started` when the torrent becomes active (after checking, on resume;
//!   a magnet at once, and it keeps running through the check that follows
//!   its metadata);
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
use crate::status::{TorrentState, TorrentStatus};

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
/// `left` reported while a magnet has no metadata, so trackers treat us as
/// a leecher (what libtorrent reports).
pub(super) const NO_METADATA_LEFT: u64 = 16_384;

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
    /// Bytes left as of the last moment the torrent was not checking its
    /// files; reported while a recheck makes the live figure meaningless.
    pub left: u64,
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
                    self.stopped(sig.left).await;
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
            let now = Instant::now();
            let (event, due) = plan(&run, &sig, completed_sent, now);
            if due > now {
                match self.wait_lifecycle(Some(due)).await {
                    Interrupt::Closed => return,
                    Interrupt::Lifecycle => continue,
                }
            }
            if event == AnnounceEvent::Completed {
                // At most once per session: an abandoned walk has sent (or
                // may have sent) the request already.
                completed_sent = true;
            }
            match self.walk_interruptible(event, sig.left).await {
                Err(Interrupt::Closed) => return,
                Err(Interrupt::Lifecycle) => continue,
                Ok(Walk::NoTrackers) => {
                    // Nothing left to announce to; wait for the end.
                    let _ = self.signals.wait_for(|s| s.exit).await;
                    return;
                }
                Ok(Walk::Failed) => {
                    if event == AnnounceEvent::Completed {
                        // Every tracker answered with an error: nothing took
                        // the `completed`, so it is tried again.
                        completed_sent = false;
                    }
                    run.not_before = Some(Instant::now() + self.backoff());
                    self.failed_walks = self.failed_walks.saturating_add(1);
                    run.reannounces = sig.reannounces;
                }
                Ok(Walk::Ok(resp)) => {
                    self.failed_walks = 0;
                    run.not_before = None;
                    run.reannounces = sig.reannounces;
                    let (interval, min_interval) = clamp_intervals(&resp);
                    run.last = Some((Instant::now(), interval, min_interval));
                    match event {
                        AnnounceEvent::Started => run.started = true,
                        AnnounceEvent::Completed => run.completed = true,
                        _ => {}
                    }
                }
            }
        }
    }

    fn backoff(&self) -> Duration {
        backoff(self.shared.settings.tracker_retry_base, self.failed_walks)
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
    async fn walk_interruptible(
        &mut self,
        event: AnnounceEvent,
        checking_left: u64,
    ) -> Result<Walk, Interrupt> {
        let before = self.signals.borrow_and_update().clone();
        let mut signals = self.signals.clone();
        let req = self.request_template(event, checking_left);
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
                // The walk goes first, so its first request is issued
                // before a lifecycle change can abandon it.
                biased;
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

    /// `checking_left` stands in for the live figure while the files are
    /// being checked (the picker is gone then); without metadata the figure
    /// is [`NO_METADATA_LEFT`], `stopped` included.
    fn request_template(&self, event: AnnounceEvent, checking_left: u64) -> AnnounceRequest {
        let st = self.status.borrow();
        let left = announce_left(&st, checking_left);
        AnnounceRequest {
            info_hash: self.info_hash,
            peer_id: self.shared.peer_id,
            port: self.shared.listen_port,
            uploaded: st.total_uploaded,
            downloaded: st.total_downloaded,
            left,
            event,
            num_want: Some(self.shared.settings.num_want),
            key: self.key,
            tracker_id: None,
        }
    }

    /// One `stopped` announce to the last tracker that answered, never
    /// retried.
    async fn stopped(&mut self, left: u64) {
        let Some(url) = self.last_ok.clone() else {
            return;
        };
        let mut req = request_for(
            &url,
            &self.request_template(AnnounceEvent::Stopped, left),
            &self.tracker_ids,
        );
        req.num_want = Some(0);
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

/// The `left` to announce for status `st`.
fn announce_left(st: &TorrentStatus, checking_left: u64) -> u64 {
    if !st.has_metadata {
        NO_METADATA_LEFT
    } else if matches!(st.state, TorrentState::CheckingFiles { .. }) {
        checking_left
    } else {
        st.total_wanted.saturating_sub(st.wanted_done)
    }
}

/// The next announce of an active torrent and when it is due. Pure: `now`
/// is passed in.
fn plan(run: &Run, sig: &Signals, completed_sent: bool, now: Instant) -> (AnnounceEvent, Instant) {
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

/// A reply's `interval` and `min interval`, both capped at [`MAX_INTERVAL`].
fn clamp_intervals(resp: &AnnounceResponse) -> (Duration, Option<Duration>) {
    (
        resp.interval.min(MAX_INTERVAL),
        resp.min_interval.map(|d| d.min(MAX_INTERVAL)),
    )
}

/// Wait after `failed_walks` earlier failed walks: `base` x 2^n, capped.
fn backoff(base: Duration, failed_walks: u32) -> Duration {
    let factor = 1u32.checked_shl(failed_walks).unwrap_or(u32::MAX);
    base.saturating_mul(factor).min(MAX_BACKOFF)
}

/// `base` for one tracker: only that URL's `tracker id` is echoed.
fn request_for(
    url: &str,
    base: &AnnounceRequest,
    tracker_ids: &HashMap<String, Vec<u8>>,
) -> AnnounceRequest {
    AnnounceRequest {
        tracker_id: tracker_ids.get(url).cloned(),
        ..base.clone()
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
    req: AnnounceRequest,
    shared: &Shared,
    ih: InfoHashes,
    tracker_ids: &mut HashMap<String, Vec<u8>>,
) -> (Walk, Option<String>) {
    tiers.restart();
    loop {
        let Some(url) = tiers.current().map(str::to_owned) else {
            return (Walk::NoTrackers, None);
        };
        let req = request_for(&url, &req, tracker_ids);
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

#[cfg(test)]
mod tests {
    use super::*;

    const S: Duration = Duration::from_secs(1);

    fn started_run(at: Instant) -> Run {
        Run {
            started: true,
            last: Some((at, Duration::from_secs(1800), Some(Duration::from_secs(60)))),
            ..Run::default()
        }
    }

    #[test]
    fn first_announce_is_started_now_or_after_backoff() {
        let now = Instant::now();
        let sig = Signals::default();
        assert_eq!(
            plan(&Run::default(), &sig, false, now),
            (AnnounceEvent::Started, now)
        );
        let run = Run {
            not_before: Some(now + 10 * S),
            ..Run::default()
        };
        assert_eq!(
            plan(&run, &sig, false, now),
            (AnnounceEvent::Started, now + 10 * S)
        );
        // A forced re-announce ignores the backoff.
        let sig = Signals {
            reannounces: 1,
            ..Signals::default()
        };
        assert_eq!(plan(&run, &sig, false, now), (AnnounceEvent::Started, now));
    }

    #[test]
    fn regular_announce_after_interval() {
        let now = Instant::now();
        let run = started_run(now);
        assert_eq!(
            plan(&run, &Signals::default(), false, now + 5 * S),
            (AnnounceEvent::None, now + Duration::from_secs(1800))
        );
    }

    #[test]
    fn forced_reannounce_waits_min_interval() {
        let now = Instant::now();
        let run = started_run(now);
        let sig = Signals {
            reannounces: 1,
            ..Signals::default()
        };
        assert_eq!(
            plan(&run, &sig, false, now + 5 * S),
            (AnnounceEvent::None, now + Duration::from_secs(60))
        );
    }

    #[test]
    fn completed_once_even_after_restart() {
        let now = Instant::now();
        let run = started_run(now);
        let sig = Signals {
            completed: true,
            ..Signals::default()
        };
        assert_eq!(plan(&run, &sig, false, now).0, AnnounceEvent::Completed);
        // A new run (restart) after `completed` went out never repeats it.
        assert_eq!(plan(&run, &sig, true, now).0, AnnounceEvent::None);
    }

    fn response(interval: Duration, min_interval: Option<Duration>) -> AnnounceResponse {
        AnnounceResponse {
            interval,
            min_interval,
            peers: Vec::new(),
            complete: None,
            incomplete: None,
            tracker_id: None,
            warning: None,
            external_ip: None,
        }
    }

    #[test]
    fn interval_above_two_hours_is_clamped() {
        let h = Duration::from_secs(3600);
        let (i, m) = clamp_intervals(&response(10 * h, Some(5 * h)));
        assert_eq!((i, m), (MAX_INTERVAL, Some(MAX_INTERVAL)));
        let (i, m) = clamp_intervals(&response(h, None));
        assert_eq!((i, m), (h, None));
    }

    #[test]
    fn backoff_doubles_and_caps_at_thirty_minutes() {
        let base = Duration::from_secs(30);
        assert_eq!(backoff(base, 0), base);
        assert_eq!(backoff(base, 2), 4 * base);
        assert_eq!(backoff(base, 10), MAX_BACKOFF);
        assert_eq!(backoff(base, 200), MAX_BACKOFF);
    }

    #[test]
    fn left_is_16384_without_metadata() {
        let ih = InfoHashes::from_v1(fbt_core::InfoHashV1([1; 20]));
        let mut st = TorrentStatus::initial(ih, String::new(), false);
        // A `DownloadingMetadata` status says nothing is wanted or done.
        assert_eq!(announce_left(&st, 0), NO_METADATA_LEFT);
        st.state = TorrentState::Paused;
        assert_eq!(announce_left(&st, 7), NO_METADATA_LEFT, "stopped too");
        st.has_metadata = true;
        st.total_wanted = 1000;
        st.wanted_done = 400;
        assert_eq!(announce_left(&st, 7), 600);
        st.state = TorrentState::CheckingFiles { progress: 0.5 };
        assert_eq!(announce_left(&st, 7), 7);
    }

    #[test]
    fn tracker_id_echoed_to_same_url_only() {
        let base = AnnounceRequest {
            info_hash: [0; 20],
            peer_id: fbt_core::PeerId([0; 20]),
            port: 1,
            uploaded: 0,
            downloaded: 0,
            left: 0,
            event: AnnounceEvent::None,
            num_want: None,
            key: 0,
            tracker_id: None,
        };
        let ids = HashMap::from([("http://a/".to_owned(), b"id-a".to_vec())]);
        assert_eq!(
            request_for("http://a/", &base, &ids).tracker_id.as_deref(),
            Some(&b"id-a"[..])
        );
        assert_eq!(request_for("http://b/", &base, &ids).tracker_id, None);
    }
}
