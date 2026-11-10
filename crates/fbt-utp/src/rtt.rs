//! RTT estimation and the retransmission timeout (BEP 29 / RFC 6298 style).

use std::time::Duration;

/// Lower bound of the retransmission timeout.
pub const MIN_RTO: Duration = Duration::from_millis(500);
/// Retransmission timeout before any RTT sample exists.
pub const INITIAL_RTO: Duration = Duration::from_secs(1);
/// Upper bound of the retransmission timeout, also the backoff cap.
pub const MAX_RTO: Duration = Duration::from_secs(60);

/// Smoothed RTT, variance and the (possibly backed-off) RTO.
#[derive(Debug, Clone)]
pub struct RttEstimator {
    /// `(rtt, var)` in seconds, once a sample exists.
    state: Option<(f64, f64)>,
    rto: Duration,
}

impl Default for RttEstimator {
    fn default() -> Self {
        Self::new()
    }
}

fn secs_to_duration(s: f64) -> Duration {
    if !s.is_finite() || s <= 0.0 {
        return Duration::ZERO;
    }
    Duration::from_secs_f64(s.min(MAX_RTO.as_secs_f64()))
}

impl RttEstimator {
    pub fn new() -> RttEstimator {
        RttEstimator {
            state: None,
            rto: INITIAL_RTO,
        }
    }

    /// Feed one RTT measurement. Resets any backoff.
    pub fn on_sample(&mut self, rtt: Duration) {
        let s = rtt.as_secs_f64().min(MAX_RTO.as_secs_f64());
        let (r, v) = match self.state {
            None => (s, s / 2.0),
            Some((r, v)) => {
                let v = v + ((r - s).abs() - v) / 4.0;
                let r = r + (s - r) / 8.0;
                (r, v)
            }
        };
        self.state = Some((r, v));
        self.rto = secs_to_duration(r + 4.0 * v).clamp(MIN_RTO, MAX_RTO);
    }

    /// Current retransmission timeout.
    pub fn rto(&self) -> Duration {
        self.rto
    }

    /// Smoothed RTT, if any sample was seen.
    pub fn rtt(&self) -> Option<Duration> {
        self.state.map(|(r, _)| secs_to_duration(r))
    }

    /// Double the RTO (capped at `MAX_RTO`) until the next sample.
    pub fn backoff(&mut self) {
        self.rto = self.rto.saturating_mul(2).min(MAX_RTO);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rto_follows_bep29_formula() {
        let mut e = RttEstimator::new();
        assert_eq!(e.rto(), INITIAL_RTO);
        for _ in 0..8 {
            e.on_sample(Duration::from_millis(100));
        }
        assert_eq!(e.rto(), MIN_RTO);
        e.on_sample(Duration::from_secs(2));
        assert!(e.rto() > MIN_RTO);

        let mut b = RttEstimator::new();
        b.backoff();
        b.backoff();
        assert_eq!(b.rto(), Duration::from_secs(4));
        for _ in 0..20 {
            b.backoff();
        }
        assert_eq!(b.rto(), MAX_RTO);
    }

    #[test]
    fn sample_resets_backoff() {
        let mut e = RttEstimator::new();
        e.backoff();
        e.on_sample(Duration::from_millis(100));
        assert_eq!(e.rto(), MIN_RTO);
    }
}
