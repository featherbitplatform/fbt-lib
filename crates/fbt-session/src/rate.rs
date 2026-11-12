//! Transfer rate over a short sliding window.

use tokio::time::Instant;

/// Window length in one-second buckets.
const WINDOW_SECS: u64 = 5;

/// Bytes per second over the last five seconds, in one-second buckets.
#[derive(Debug, Clone)]
pub struct RateMeter {
    start: Instant,
    /// `(second since start, bytes)` per bucket; slot = second % WINDOW_SECS.
    buckets: [(u64, u64); WINDOW_SECS as usize],
    total: u64,
}

impl Default for RateMeter {
    fn default() -> Self {
        Self::new()
    }
}

impl RateMeter {
    /// An empty meter starting now.
    pub fn new() -> Self {
        RateMeter {
            start: Instant::now(),
            buckets: [(0, 0); WINDOW_SECS as usize],
            total: 0,
        }
    }

    fn second(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.start).as_secs()
    }

    /// Records `bytes` transferred now.
    pub fn record(&mut self, bytes: u64) {
        let sec = self.second(Instant::now());
        self.total = self.total.saturating_add(bytes);
        let slot = (sec % WINDOW_SECS) as usize;
        if let Some(b) = self.buckets.get_mut(slot) {
            if b.0 != sec {
                *b = (sec, 0);
            }
            b.1 = b.1.saturating_add(bytes);
        }
    }

    /// Average bytes per second over the five seconds ending at `now`; a
    /// meter younger than that divides by the seconds it has been running
    /// (counting the current one), so it does not under-report its start.
    pub fn rate(&self, now: Instant) -> u64 {
        let sec = self.second(now);
        let sum: u64 = self
            .buckets
            .iter()
            .filter(|(s, _)| *s <= sec && sec - *s < WINDOW_SECS)
            .map(|(_, b)| *b)
            .fold(0, u64::saturating_add);
        sum / sec.saturating_add(1).min(WINDOW_SECS)
    }

    /// All bytes ever recorded.
    pub fn total(&self) -> u64 {
        self.total
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn rate_meter_window() {
        let mut m = RateMeter::new();
        m.record(1000);
        tokio::time::advance(Duration::from_secs(1)).await;
        m.record(4000);
        assert_eq!(m.rate(Instant::now()), 2500);
        assert_eq!(m.total(), 5000);
        // The first second leaves the window after five seconds.
        tokio::time::advance(Duration::from_secs(4)).await;
        assert_eq!(m.rate(Instant::now()), 800);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(m.rate(Instant::now()), 0);
        // A bucket reused after wrapping starts from zero.
        m.record(500);
        assert_eq!(m.rate(Instant::now()), 100);
        assert_eq!(m.total(), 5500);
    }

    #[tokio::test(start_paused = true)]
    async fn fresh_meter_is_not_diluted() {
        let mut m = RateMeter::new();
        let t0 = Instant::now();
        m.record(10_000);
        assert_eq!(m.rate(t0 + Duration::from_millis(500)), 10_000);
        assert_eq!(m.rate(t0 + Duration::from_millis(1500)), 5_000);
        tokio::time::advance(Duration::from_secs(6)).await;
        assert_eq!(m.rate(Instant::now()), 0);
    }
}
