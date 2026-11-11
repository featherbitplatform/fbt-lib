//! Per-IP token-bucket rate limiter for inbound datagrams (DHT queries, LSD
//! announces), shared by `fbt-dht` and `fbt-net`.

use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;
use std::time::{Duration, Instant};

/// Fixed-point scale: one message costs this many units.
const UNIT: u128 = 1_000_000_000;
/// Longest elapsed time counted for a refill; keeps the arithmetic small
/// (a bucket idle for longer is full anyway, for any sane rate).
const MAX_ELAPSED: Duration = Duration::from_secs(3600);

#[derive(Debug)]
struct Bucket {
    /// Available tokens, in `UNIT`s.
    tokens: u128,
    last: Instant,
    /// Key of this bucket in the LRU index.
    stamp: u64,
}

/// Token bucket per IP: refills `per_sec` tokens a second up to `burst`. At
/// most `max_ips` buckets; the least recently used goes.
#[derive(Debug)]
pub struct IpRateLimiter {
    per_sec: u32,
    burst: u32,
    max_ips: usize,
    buckets: HashMap<IpAddr, Bucket>,
    /// Use stamp -> IP, oldest first.
    lru: BTreeMap<u64, IpAddr>,
    next_stamp: u64,
}

impl IpRateLimiter {
    /// A limiter allowing `per_sec` messages a second per IP, with bursts
    /// of up to `burst`, tracking at most `max_ips` IPs.
    pub fn new(per_sec: u32, burst: u32, max_ips: usize) -> Self {
        IpRateLimiter {
            per_sec,
            burst,
            max_ips,
            buckets: HashMap::new(),
            lru: BTreeMap::new(),
            next_stamp: 0,
        }
    }

    /// Whether a message from `ip` at `now` is within its rate.
    pub fn allow(&mut self, ip: IpAddr, now: Instant) -> bool {
        if self.max_ips == 0 {
            return false;
        }
        let burst = u128::from(self.burst) * UNIT;
        let per_sec = u128::from(self.per_sec);
        let stamp = self.next_stamp;
        self.next_stamp = self.next_stamp.wrapping_add(1);
        let bucket = match self.buckets.get_mut(&ip) {
            Some(b) => {
                self.lru.remove(&b.stamp);
                let elapsed = now.saturating_duration_since(b.last).min(MAX_ELAPSED);
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
    fn ip_rate_limiter_burst_then_refill() {
        let t0 = Instant::now();
        let mut rl = IpRateLimiter::new(10, 20, 4096);
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
    fn ip_rate_limiter_memory_bounded() {
        let t0 = Instant::now();
        let mut rl = IpRateLimiter::new(10, 20, 100);
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

    #[test]
    fn burst_is_independent_of_the_rate() {
        let t0 = Instant::now();
        let mut rl = IpRateLimiter::new(1, 5, 16);
        let ip: IpAddr = "1.2.3.4".parse().unwrap();
        for _ in 0..5 {
            assert!(rl.allow(ip, t0));
        }
        assert!(!rl.allow(ip, t0));
        assert!(
            rl.allow(ip, t0 + Duration::from_secs(1)),
            "one token a second"
        );
        assert!(!rl.allow(ip, t0 + Duration::from_secs(1)));
        let mut none = IpRateLimiter::new(0, 0, 16);
        assert!(!none.allow(ip, t0), "no burst, no rate: nothing passes");
    }
}
