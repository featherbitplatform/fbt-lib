//! Torrent queueing, as libtorrent's auto-managed torrents: which torrents
//! run within the session's download, seed and total limits.
//!
//! [`plan`] is pure: the session's scheduler feeds it every torrent in queue
//! order and applies the answer (see `Session`'s scheduler in `session.rs`).

use std::time::{Duration, Instant};

/// Limits on the auto-managed torrents running at once
/// ([`Settings::queue`](crate::Settings::queue), changed at runtime with
/// [`Session::set_queue_settings`](crate::Session::set_queue_settings)).
///
/// Torrents are auto-managed by default
/// ([`AddTorrent::auto_managed`](crate::AddTorrent::auto_managed)). In queue
/// order ([`TorrentStatus::queue_position`](crate::TorrentStatus::queue_position)),
/// each one runs while its class (downloading or seeding) and the total are
/// within their limits; the others are
/// [`TorrentState::Queued`](crate::TorrentState::Queued): stopped like a
/// paused torrent (no peers, `stopped` sent to the trackers) until a slot
/// frees. A torrent the user paused is never started by the queue; resuming
/// it returns it to the queue, which may queue it again at once. Checking
/// is not queued here ([`Settings::max_active_checks`](crate::Settings::max_active_checks)
/// bounds it). Torrents that are not auto-managed are neither queued nor
/// counted.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
#[non_exhaustive]
pub struct QueueSettings {
    /// Auto-managed downloading torrents running at once (3); `None` is
    /// unlimited.
    pub active_downloads: Option<usize>,
    /// Auto-managed seeding torrents running at once (5); `None` is
    /// unlimited.
    pub active_seeds: Option<usize>,
    /// Auto-managed torrents running at once, downloading or seeding (500);
    /// `None` is unlimited. Slow torrents count against it.
    pub active_limit: Option<usize>,
    /// A slow torrent (both rates below `inactive_down_rate` and
    /// `inactive_up_rate` once running for `startup_grace`) does not count
    /// against `active_downloads` or `active_seeds` (true), so it does not
    /// hold back the next one in the queue.
    pub dont_count_slow_torrents: bool,
    /// Download rate below which a torrent may be slow, bytes per second
    /// (2048).
    pub inactive_down_rate: u64,
    /// Upload rate below which a torrent may be slow, bytes per second
    /// (2048).
    pub inactive_up_rate: u64,
    /// How long a torrent runs before it may count as slow (60 s).
    pub startup_grace: Duration,
    /// Time between scheduler passes (30 s; non-zero). A pass also runs at
    /// once on every add, remove, pause, resume, finish and limit change.
    pub interval: Duration,
}

impl Default for QueueSettings {
    fn default() -> Self {
        QueueSettings {
            active_downloads: Some(3),
            active_seeds: Some(5),
            active_limit: Some(500),
            dont_count_slow_torrents: true,
            inactive_down_rate: 2048,
            inactive_up_rate: 2048,
            startup_grace: Duration::from_secs(60),
            interval: Duration::from_secs(30),
        }
    }
}

impl QueueSettings {
    /// Checks the values the scheduler cannot work with; the error names
    /// the field.
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.interval.is_zero() {
            return Err("queue.interval must be greater than zero".to_owned());
        }
        Ok(())
    }

    /// No limit at all: nothing is ever queued.
    pub(crate) fn unlimited(&self) -> bool {
        self.active_downloads.is_none()
            && self.active_seeds.is_none()
            && self.active_limit.is_none()
    }
}

/// One torrent as the scheduler sees it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QueueEntry {
    /// Its place in the queue; lower runs first.
    pub position: usize,
    /// Every wanted piece is verified: it counts as a seed.
    pub seeding: bool,
    /// When it last started running; `None` while it is not running.
    pub running_since: Option<Instant>,
    /// Download rate, bytes per second.
    pub down_rate: u64,
    /// Upload rate, bytes per second.
    pub up_rate: u64,
    /// Paused by the user (or stopped by an error): never started here.
    pub user_paused: bool,
    /// Under the queue's control.
    pub auto_managed: bool,
}

/// `e` is running, past its startup grace, with both rates below the
/// inactive thresholds.
pub(crate) fn is_slow(e: &QueueEntry, cfg: &QueueSettings, now: Instant) -> bool {
    e.running_since
        .is_some_and(|since| now.saturating_duration_since(since) >= cfg.startup_grace)
        && e.down_rate < cfg.inactive_down_rate
        && e.up_rate < cfg.inactive_up_rate
}

/// Which torrents run: `true` at index `i` lets `entries[i]` run. Entries
/// are taken in `position` order (ties by index). A user-paused or
/// non-auto-managed entry is always `false` (the scheduler leaves those
/// alone). With `dont_count_slow_torrents`, a slow entry ([`is_slow`]) runs
/// without taking a download or seed slot, but every running entry counts
/// against `active_limit`.
pub(crate) fn plan(entries: &[QueueEntry], cfg: &QueueSettings, now: Instant) -> Vec<bool> {
    let mut order: Vec<usize> = (0..entries.len()).collect();
    order.sort_by_key(|&i| entries.get(i).map_or(usize::MAX, |e| e.position));
    let mut out = vec![false; entries.len()];
    let within = |n: usize, limit: Option<usize>| limit.is_none_or(|l| n < l);
    let (mut downloads, mut seeds, mut total) = (0usize, 0usize, 0usize);
    for i in order {
        let Some(e) = entries.get(i) else {
            continue;
        };
        if !e.auto_managed || e.user_paused || !within(total, cfg.active_limit) {
            continue;
        }
        let exempt = cfg.dont_count_slow_torrents && is_slow(e, cfg, now);
        let (count, limit) = if e.seeding {
            (&mut seeds, cfg.active_seeds)
        } else {
            (&mut downloads, cfg.active_downloads)
        };
        if !exempt {
            if !within(*count, limit) {
                continue;
            }
            *count += 1;
        }
        total += 1;
        if let Some(slot) = out.get_mut(i) {
            *slot = true;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn entry(position: usize, seeding: bool) -> QueueEntry {
        QueueEntry {
            position,
            seeding,
            running_since: None,
            down_rate: 0,
            up_rate: 0,
            user_paused: false,
            auto_managed: true,
        }
    }

    fn cfg(downloads: Option<usize>, seeds: Option<usize>, limit: Option<usize>) -> QueueSettings {
        QueueSettings {
            active_downloads: downloads,
            active_seeds: seeds,
            active_limit: limit,
            ..QueueSettings::default()
        }
    }

    proptest! {
        #[test]
        fn plan_never_exceeds_limits(
            raw in proptest::collection::vec(
                (any::<bool>(), 0u8..3, 0u64..5000, 0u64..5000, any::<bool>(), any::<bool>(), any::<u16>()),
                0..=40,
            ),
            downloads in proptest::option::of(0usize..6),
            seeds in proptest::option::of(0usize..6),
            limit in proptest::option::of(0usize..10),
            slow_exempt in any::<bool>(),
        ) {
            let now = Instant::now();
            let mut c = cfg(downloads, seeds, limit);
            c.dont_count_slow_torrents = slow_exempt;
            let n = raw.len();
            // A shuffled permutation of 0..n as positions.
            let mut positions: Vec<usize> = (0..n).collect();
            positions.sort_by_key(|&p| raw.get(p).map_or(0, |r| r.6));
            let entries: Vec<QueueEntry> = raw
                .iter()
                .zip(positions)
                .map(|(&(seeding, age, down, up, paused, managed, _), position)| QueueEntry {
                    position,
                    seeding,
                    // Not running, running within its grace, or past it.
                    running_since: match age {
                        0 => None,
                        1 => Some(now),
                        _ => now.checked_sub(Duration::from_secs(120)),
                    },
                    down_rate: down,
                    up_rate: up,
                    user_paused: paused,
                    auto_managed: managed,
                })
                .collect();
            let run = plan(&entries, &c, now);
            prop_assert_eq!(run.len(), n);
            let counted = |want_seed: bool| {
                entries
                    .iter()
                    .zip(&run)
                    .filter(|(e, r)| **r && e.seeding == want_seed)
                    .filter(|(e, _)| !(c.dont_count_slow_torrents && is_slow(e, &c, now)))
                    .count()
            };
            if let Some(l) = c.active_downloads {
                prop_assert!(counted(false) <= l);
            }
            if let Some(l) = c.active_seeds {
                prop_assert!(counted(true) <= l);
            }
            if let Some(l) = c.active_limit {
                prop_assert!(run.iter().filter(|r| **r).count() <= l);
            }
            for (e, r) in entries.iter().zip(&run) {
                if e.user_paused || !e.auto_managed {
                    prop_assert!(!r, "started a paused or unmanaged entry: {:?}", e);
                }
            }
        }
    }

    #[test]
    fn positions_order_wins() {
        // Listed out of order: positions decide, not the slice order.
        let entries = [
            entry(3, false),
            entry(0, false),
            entry(2, false),
            entry(1, false),
        ];
        let run = plan(&entries, &cfg(Some(2), None, None), Instant::now());
        assert_eq!(run, [false, true, false, true]);
        // Seeds have their own limit; the total caps both.
        let mixed = [
            entry(0, true),
            entry(1, false),
            entry(2, true),
            entry(3, false),
        ];
        let run = plan(&mixed, &cfg(Some(1), Some(1), None), Instant::now());
        assert_eq!(run, [true, true, false, false]);
        let run = plan(&mixed, &cfg(None, None, Some(3)), Instant::now());
        assert_eq!(run, [true, true, true, false]);
        // A paused entry ahead takes no slot.
        let mut paused = [entry(0, false), entry(1, false), entry(2, false)];
        paused[0].user_paused = true;
        let run = plan(&paused, &cfg(Some(1), None, None), Instant::now());
        assert_eq!(run, [false, true, false]);
        // Nothing is queued without limits.
        assert_eq!(
            plan(&entries, &cfg(None, None, None), Instant::now()),
            [true; 4]
        );
    }

    #[test]
    fn slow_torrents_do_not_count_after_grace() {
        let now = Instant::now();
        let c = cfg(Some(1), Some(1), Some(3));
        let long_ago = now.checked_sub(Duration::from_secs(120));
        let mut slow = entry(0, false);
        slow.running_since = long_ago;
        let entries = [slow.clone(), entry(1, false), entry(2, false)];
        // The slow first one takes no download slot.
        assert_eq!(plan(&entries, &c, now), [true, true, false]);
        // Within its grace it still counts.
        let mut young = slow.clone();
        young.running_since = Some(now);
        let entries = [young, entry(1, false), entry(2, false)];
        assert_eq!(plan(&entries, &c, now), [true, false, false]);
        // A fast one counts.
        let mut fast = slow.clone();
        fast.down_rate = 10_000;
        let entries = [fast, entry(1, false)];
        assert_eq!(plan(&entries, &c, now), [true, false]);
        // Slow seeds likewise, but every running one counts toward the
        // total: three slow ones fill `active_limit` = 3.
        let mut s = entry(0, true);
        s.running_since = long_ago;
        let seeds: Vec<QueueEntry> = (0..5)
            .map(|p| QueueEntry {
                position: p,
                ..s.clone()
            })
            .collect();
        assert_eq!(plan(&seeds, &c, now), [true, true, true, false, false]);
        // Off, slow ones count like any other.
        let mut strict = c.clone();
        strict.dont_count_slow_torrents = false;
        assert_eq!(
            plan(&seeds, &strict, now),
            [true, false, false, false, false]
        );
    }
}
