//! Per-IP token-bucket rate limiter for inbound queries.

use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;
use std::time::{Duration, Instant};

/// Default cap on the number of tracked IPs.
pub(crate) const MAX_IPS: usize = 4096;
/// Fixed-point scale: one query costs this many units.
const UNIT: u128 = 1_000_000_000;

#[derive(Debug)]
struct Bucket {
    /// Available tokens, in `UNIT`s.
    tokens: u128,
    last: Instant,
    /// Key of this bucket in the LRU index.
    stamp: u64,
}

/// Token bucket per IP: refills `per_sec` tokens a second up to a burst of
/// `2 × per_sec`. At most `max_ips` buckets; the least recently used goes.
#[derive(Debug)]
pub(crate) struct RateLimiter {
    per_sec: u32,
    max_ips: usize,
    buckets: HashMap<IpAddr, Bucket>,
    /// Use stamp -> IP, oldest first.
    lru: BTreeMap<u64, IpAddr>,
    next_stamp: u64,
}

impl RateLimiter {
    pub(crate) fn new(per_sec: u32, max_ips: usize) -> Self {
        RateLimiter {
            per_sec,
            max_ips,
            buckets: HashMap::new(),
            lru: BTreeMap::new(),
            next_stamp: 0,
        }
    }

    fn burst(&self) -> u128 {
        2 * u128::from(self.per_sec) * UNIT
    }

    /// Whether a query from `ip` at `now` is within its rate.
    pub(crate) fn allow(&mut self, ip: IpAddr, now: Instant) -> bool {
        if self.max_ips == 0 {
            return false;
        }
        let burst = self.burst();
        let per_sec = u128::from(self.per_sec);
        let stamp = self.next_stamp;
        self.next_stamp = self.next_stamp.wrapping_add(1);
        let bucket = match self.buckets.get_mut(&ip) {
            Some(b) => {
                self.lru.remove(&b.stamp);
                // a full refill takes 2 s; cap the elapsed time so the
                // arithmetic stays small
                let elapsed = now
                    .saturating_duration_since(b.last)
                    .min(Duration::from_secs(2));
                let refill = elapsed.as_nanos() * per_sec;
                b.tokens = b.tokens.saturating_add(refill).min(burst);
                b.last = now;
                b
            }
            None => {
                while self.buckets.len() >= self.max_ips {
                    let Some((_, old)) = self.lru.pop_first() else {
                        break;
                    };
                    self.buckets.remove(&old);
                }
                self.buckets.entry(ip).or_insert(Bucket {
                    tokens: burst,
                    last: now,
                    stamp,
                })
            }
        };
        bucket.stamp = stamp;
        self.lru.insert(stamp, ip);
        if bucket.tokens >= UNIT {
            bucket.tokens -= UNIT;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn rate_limiter_burst_then_refill() {
        let t0 = Instant::now();
        let mut rl = RateLimiter::new(10, MAX_IPS);
        let ip: IpAddr = "1.2.3.4".parse().unwrap();
        for _ in 0..20 {
            assert!(rl.allow(ip, t0));
        }
        assert!(!rl.allow(ip, t0), "burst is 2 x per_sec");
        assert!(rl.allow("5.6.7.8".parse().unwrap(), t0), "per IP");
        let t1 = t0 + Duration::from_millis(100);
        assert!(rl.allow(ip, t1), "100 ms refills one token");
        assert!(!rl.allow(ip, t1));
        let t2 = t1 + Duration::from_secs(60);
        for _ in 0..20 {
            assert!(rl.allow(ip, t2));
        }
        assert!(!rl.allow(ip, t2), "refill is capped at the burst");
    }

    #[test]
    fn rate_limiter_memory_bounded() {
        let t0 = Instant::now();
        let mut rl = RateLimiter::new(10, 100);
        let hot: IpAddr = "9.9.9.9".parse().unwrap();
        for _ in 0..20 {
            rl.allow(hot, t0);
        }
        for k in 0..10_000u32 {
            rl.allow(IpAddr::V4(Ipv4Addr::from(0x0100_0000 + k)), t0);
            if k % 50 == 0 {
                // keep the hot IP recently used: it survives and stays drained
                assert!(!rl.allow(hot, t0));
            }
            assert!(rl.buckets.len() <= 100);
        }
        assert_eq!(rl.buckets.len(), 100);
        assert_eq!(rl.lru.len(), 100);
        let cold: IpAddr = IpAddr::V4(Ipv4Addr::from(0x0100_0000));
        assert!(!rl.buckets.contains_key(&cold), "LRU entry dropped");
    }
}
