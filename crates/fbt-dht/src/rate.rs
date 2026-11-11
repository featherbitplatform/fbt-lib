//! The DHT's inbound query limiter: `fbt_core`'s per-IP token bucket with a
//! burst of twice the rate.

pub(crate) use fbt_core::rate::IpRateLimiter;

/// Default cap on the number of tracked IPs.
pub(crate) const MAX_IPS: usize = 4096;

/// A limiter of `per_sec` queries a second per IP with a burst of
/// `2 x per_sec`.
pub(crate) fn query_limiter(per_sec: u32, max_ips: usize) -> IpRateLimiter {
    IpRateLimiter::new(per_sec, per_sec.saturating_mul(2), max_ips)
}
