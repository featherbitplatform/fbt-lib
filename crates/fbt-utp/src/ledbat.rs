//! LEDBAT congestion control (BEP 29) and the one-way delay history.

use std::time::{Duration, Instant};

/// Queueing delay the controller aims for.
pub const TARGET_DELAY: Duration = Duration::from_millis(100);
/// Maximum window growth per RTT, in bytes (BEP 29).
pub const MAX_CWND_INCREASE_PER_RTT: f64 = 3000.0;
/// Window floor in packets (except right after a timeout).
pub const MIN_CWND_PACKETS: u32 = 2;

const BUCKET: Duration = Duration::from_secs(60);

/// `a` is earlier than or equal to `b` in wrapping 32-bit microseconds.
fn wrapping_le(a: u32, b: u32) -> bool {
    b.wrapping_sub(a) < 0x8000_0000
}

fn wrapping_min(a: u32, b: u32) -> u32 {
    if wrapping_le(a, b) { a } else { b }
}

fn min_opt(a: Option<u32>, b: Option<u32>) -> Option<u32> {
    match (a, b) {
        (Some(a), Some(b)) => Some(wrapping_min(a, b)),
        (x, None) | (None, x) => x,
    }
}

/// Minimum one-way delay over the last two one-minute buckets.
#[derive(Debug, Clone, Default)]
pub struct DelayHistory {
    current: Option<u32>,
    previous: Option<u32>,
    bucket_start: Option<Instant>,
}

impl DelayHistory {
    pub fn new() -> DelayHistory {
        DelayHistory::default()
    }

    pub fn add_sample(&mut self, us: u32, now: Instant) {
        match self.bucket_start {
            None => self.bucket_start = Some(now),
            Some(start) => {
                let elapsed = now.saturating_duration_since(start);
                if elapsed >= BUCKET {
                    self.previous = if elapsed >= BUCKET * 2 {
                        None
                    } else {
                        self.current
                    };
                    self.current = None;
                    self.bucket_start = Some(now);
                }
            }
        }
        self.current = min_opt(self.current, Some(us));
    }

    /// The base delay, if any sample was recorded.
    pub fn base(&self) -> Option<u32> {
        min_opt(self.current, self.previous)
    }

    /// Delay of `sample_us` over the base (0 before any base).
    pub fn delay(&self, sample_us: u32) -> u32 {
        match self.base() {
            Some(b) => sample_us.wrapping_sub(b),
            None => 0,
        }
    }
}

/// LEDBAT congestion window.
#[derive(Debug, Clone)]
pub struct Ledbat {
    cwnd: f64,
    mss: u32,
    history: DelayHistory,
    /// Set by a timeout: the window may sit below the floor until it
    /// grows back to it.
    below_floor: bool,
}

impl Ledbat {
    pub fn new(mss: u32) -> Ledbat {
        Ledbat {
            cwnd: 2.0 * f64::from(mss),
            mss,
            history: DelayHistory::new(),
            below_floor: false,
        }
    }

    fn floor(&self) -> f64 {
        f64::from(MIN_CWND_PACKETS) * f64::from(self.mss)
    }

    pub fn on_ack(&mut self, bytes_acked: u32, delay_sample_us: u32, flight: u32, now: Instant) {
        self.history.add_sample(delay_sample_us, now);
        let queuing = f64::from(self.history.delay(delay_sample_us));
        let target = TARGET_DELAY.as_micros() as f64;
        let off_target = ((target - queuing) / target).clamp(-1.0, 1.0);
        let cwnd = self.cwnd.max(1.0);
        let delta = MAX_CWND_INCREASE_PER_RTT * off_target * f64::from(bytes_acked) / cwnd;
        if !delta.is_finite() {
            return;
        }
        if delta > 0.0 && f64::from(flight) < self.cwnd {
            return; // application-limited: no growth
        }
        self.cwnd += delta;
        if self.cwnd >= self.floor() {
            self.below_floor = false;
        }
        let lower = if self.below_floor {
            f64::from(self.mss)
        } else {
            self.floor()
        };
        self.cwnd = self.cwnd.clamp(lower, f64::from(u32::MAX));
    }

    /// Halve the window (caller limits this to once per RTT).
    pub fn on_loss(&mut self) {
        self.below_floor = false;
        self.cwnd = (self.cwnd / 2.0).max(self.floor());
    }

    /// Drop to one packet; the only way below the floor.
    pub fn on_timeout(&mut self) {
        self.cwnd = f64::from(self.mss);
        self.below_floor = true;
    }

    pub fn cwnd(&self) -> u32 {
        let lower = if self.below_floor {
            f64::from(self.mss)
        } else {
            self.floor()
        };
        let c = if self.cwnd.is_finite() {
            self.cwnd
        } else {
            lower
        };
        c.clamp(lower, f64::from(u32::MAX)) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MSS: u32 = 1400;

    fn full_ack(l: &mut Ledbat, sample: u32, now: Instant) {
        let c = l.cwnd();
        l.on_ack(MSS, sample, c, now);
    }

    #[test]
    fn ledbat_grows_below_target_and_shrinks_above() {
        let now = Instant::now();
        let mut l = Ledbat::new(MSS);
        l.on_ack(MSS, 10_000, 2 * MSS, now); // establishes base 10 ms
        let mut prev = l.cwnd();
        for _ in 0..100 {
            full_ack(&mut l, 10_000, now);
            assert!(l.cwnd() >= prev);
            prev = l.cwnd();
        }
        assert!(prev > 2 * MSS);
        for _ in 0..1000 {
            full_ack(&mut l, 250_000, now);
            assert!(l.cwnd() >= 2 * MSS);
        }
        assert!(l.cwnd() < prev);
        assert_eq!(l.cwnd(), 2 * MSS);
    }

    #[test]
    fn growth_is_capped_per_rtt() {
        let now = Instant::now();
        let mut l = Ledbat::new(MSS);
        let before = l.cwnd();
        l.on_ack(before, 0, before, now);
        assert!(l.cwnd() - before <= 3000);
        assert!(l.cwnd() > before);
    }

    #[test]
    fn no_growth_when_application_limited() {
        let now = Instant::now();
        let mut l = Ledbat::new(MSS);
        let before = l.cwnd();
        l.on_ack(MSS, 0, 0, now);
        assert_eq!(l.cwnd(), before);
    }

    #[test]
    fn timeout_drops_to_one_packet_then_recovers() {
        let now = Instant::now();
        let mut l = Ledbat::new(MSS);
        for _ in 0..50 {
            full_ack(&mut l, 0, now);
        }
        assert!(l.cwnd() > 2 * MSS);
        l.on_timeout();
        assert_eq!(l.cwnd(), MSS);
        for _ in 0..50 {
            full_ack(&mut l, 0, now);
        }
        assert!(l.cwnd() >= 2 * MSS);
    }

    #[test]
    fn loss_halves_to_floor() {
        let mut l = Ledbat::new(MSS);
        l.on_loss();
        assert_eq!(l.cwnd(), 2 * MSS);
    }

    #[test]
    fn base_delay_forgets_after_two_minutes() {
        let t0 = Instant::now();
        let mut h = DelayHistory::new();
        h.add_sample(5_000, t0);
        for s in 1..=121u64 {
            h.add_sample(50_000, t0 + Duration::from_secs(s));
            if s == 61 {
                assert_eq!(h.base(), Some(5_000));
            }
        }
        assert_eq!(h.base(), Some(50_000));
    }

    #[test]
    fn wrapped_timestamps_do_not_inflate_delay() {
        let now = Instant::now();
        let mut h = DelayHistory::new();
        h.add_sample(0xffff_fff0, now);
        h.add_sample(0x10, now);
        assert_eq!(h.base(), Some(0xffff_fff0));
        assert_eq!(h.delay(0x10), 32);
    }
}
