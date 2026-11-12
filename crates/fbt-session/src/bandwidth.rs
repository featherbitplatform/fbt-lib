//! Rate limiting: debt-based token buckets, chained per peer and direction.
//!
//! Every limit is a [`TokenBucket`] that refills at its rate and may go
//! into debt: a frame is always read or written whole, then charged, and
//! the connection waits until the debt is repaid before the next one. A
//! peer's [`Throttle`] chains the buckets that apply to it:
//!
//! - its transport class (`tcp` or `utp`, at the user's class limit),
//!   always;
//! - `local`, for a peer on the local network ([`fbt_dht::is_local`]);
//! - its transport's mixed-mode share, the global and its torrent's
//!   buckets, for a non-local peer, and for a local one only with
//!   [`Settings::rate_limit_local_peers`].
//!
//! **Uploads queue fairly.** Blocks do not wait for the debt to be repaid
//! and then race for the tokens: whichever connection a timer wakes first
//! would win every time, and one busy connection could starve the others
//! sharing a bucket (a seeder's upload slot that receives nothing). A
//! connection instead reserves its next block's bytes ([`Throttle::reserve`]),
//! charged at once, and writes it once the debt owed before the
//! reservation is repaid ([`Reservation::due_at`]). Blocks so go out in
//! the order they were reserved, and backlogged connections take turns.
//!
//! **Mixed mode.** While a global limit is set, a task re-evaluated every
//! second sets the TCP and uTP share buckets to their part of it (see
//! [`MixedMode`]), counting only the connected peers the global limit
//! applies to. Peers exempt from the global limit never chain a share
//! bucket, so mixed mode never limits them.
//!
//! Rates are bytes per second, 0 meaning unlimited. No value from 0 to
//! `u64::MAX` overflows or panics: every step saturates.

use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::time::{Instant, MissedTickBehavior, sleep_until};

use crate::peer::transport::Transport;
use crate::settings::Settings;

/// A download and an upload limit in bytes per second; 0 means unlimited.
///
/// Whole messages are read and written, then charged, so a connection may
/// run up to one message ahead of its limit. A download limit holds a
/// connection's reads for at most
/// [`Settings::peer_idle_timeout`](crate::Settings::peer_idle_timeout) at a
/// time: then one message is read whatever the debt, so a closed or dead
/// connection is still noticed. That is the floor of any download limit,
/// about one message per idle timeout and connection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
#[non_exhaustive]
pub struct RateLimit {
    /// Bytes per second received; 0 is unlimited.
    pub download: u64,
    /// Bytes per second sent; 0 is unlimited.
    pub upload: u64,
}

impl RateLimit {
    /// A limit of `download` and `upload` bytes per second (0: unlimited).
    pub fn new(download: u64, upload: u64) -> Self {
        RateLimit { download, upload }
    }
}

/// Limits per peer class. Every peer is in its transport's class (`tcp`
/// or `utp`); a peer on the local network (loopback, RFC 1918, link-local,
/// CGNAT, IPv6 unique-local) is in `local` as well. Class limits apply to
/// local peers whatever [`Settings::rate_limit_local_peers`] says.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
#[non_exhaustive]
pub struct PeerClassLimits {
    /// Peers connected over TCP.
    pub tcp: RateLimit,
    /// Peers connected over uTP.
    pub utp: RateLimit,
    /// Peers on the local network.
    pub local: RateLimit,
}

/// How a global limit is balanced between TCP and uTP peers. It applies
/// only while a global limit is set; without one, LEDBAT's own yielding to
/// TCP is the balance.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum MixedMode {
    /// The global limit is split between the TCP and uTP classes by their
    /// connected-peer counts, re-evaluated every second (libtorrent's
    /// default).
    #[default]
    PeerProportional,
    /// The uTP class gets a tenth of the global limit while any TCP peer
    /// is connected.
    PreferTcp,
}

/// A bucket holds at least this much, so one 16 KiB block always fits.
const MIN_CAPACITY: u64 = 16 * 1024;
/// The deepest debt a bucket can owe.
const MAX_DEBT: i64 = i64::MAX / 2;
/// The longest a waiter sleeps before it checks its buckets again, so a
/// raised or removed limit takes effect within this.
const MAX_WAIT: Duration = Duration::from_secs(1);
/// How often mixed mode re-evaluates the class shares.
const REBALANCE_INTERVAL: Duration = Duration::from_secs(1);
const NANOS_PER_SEC: u128 = 1_000_000_000;

fn to_i64(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// `t + d`, saturating: the latest instant up to `t + d` that exists.
fn later(t: Instant, d: Duration) -> Instant {
    let mut d = d;
    loop {
        if let Some(x) = t.checked_add(d) {
            return x;
        }
        if d.is_zero() {
            return t;
        }
        d /= 2;
    }
}

/// A debt-based token bucket.
#[derive(Debug)]
pub(crate) struct TokenBucket {
    /// Bytes per second; 0 is unlimited.
    rate: u64,
    /// Bytes that may be used now; negative while in debt.
    balance: i64,
    /// The most `balance` can hold: `max(rate, 16 KiB)`.
    capacity: i64,
    /// The time `balance` was refilled up to.
    last: Instant,
    /// Every token credited so far (refills and forgiven debt): a
    /// [`Reservation`] is due once this reaches its mark.
    credited: u128,
}

impl TokenBucket {
    /// A bucket at `rate`, starting empty (balance 0: no free burst).
    pub(crate) fn new(rate: u64, now: Instant) -> Self {
        let mut b = TokenBucket {
            rate: 0,
            balance: 0,
            capacity: to_i64(MIN_CAPACITY),
            last: now,
            credited: 0,
        };
        b.set_rate(rate, now);
        b
    }

    #[cfg(test)]
    pub(crate) fn rate(&self) -> u64 {
        self.rate
    }

    /// The tokens gained between `last` and `now`, and the time they were
    /// gained up to (`now` when the balance is full or the count
    /// saturated, so fractions of a token are only kept while filling).
    fn gained(&self, now: Instant) -> (i64, Instant) {
        let elapsed = now.saturating_duration_since(self.last);
        let rate = u128::from(self.rate);
        let raw = rate.saturating_mul(elapsed.as_nanos()) / NANOS_PER_SEC;
        if raw == 0 || rate == 0 {
            return (0, self.last);
        }
        let add = i64::try_from(raw).unwrap_or(i64::MAX);
        let room = self.capacity.saturating_sub(self.balance);
        if raw == u128::MAX / NANOS_PER_SEC || add >= room {
            return (add, now);
        }
        // The time `add` whole tokens took: the remainder carries over.
        let used = raw.saturating_mul(NANOS_PER_SEC) / rate;
        let used = Duration::from_nanos(u64::try_from(used).unwrap_or(u64::MAX));
        (add, later(self.last, used).min(now))
    }

    fn refill(&mut self, now: Instant) {
        if self.rate == 0 {
            self.balance = 0;
            self.last = now;
            return;
        }
        let (add, upto) = self.gained(now);
        let before = self.balance;
        self.balance = self.balance.saturating_add(add).min(self.capacity);
        self.credit(before);
        self.last = upto;
    }

    /// Counts the tokens the balance gained since it was `before`.
    fn credit(&mut self, before: i64) {
        let gained = i128::from(self.balance) - i128::from(before);
        if let Ok(gained) = u128::try_from(gained) {
            self.credited = self.credited.saturating_add(gained);
        }
    }

    /// The tokens credited by `now`, without changing the bucket.
    fn credited_at(&self, now: Instant) -> u128 {
        let gained = i128::from(self.balance_at(now)) - i128::from(self.balance);
        self.credited
            .saturating_add(u128::try_from(gained).unwrap_or(0))
    }

    /// Charges `n` bytes at `now` for a frame that waits its turn: returns
    /// the credit mark at which the debt owed before it is repaid (see
    /// [`Self::due_at`]), `None` for an unlimited bucket.
    fn reserve(&mut self, n: u64, now: Instant) -> Option<u128> {
        if self.rate == 0 {
            return None;
        }
        self.refill(now);
        let debt = u128::from(self.balance.min(0).unsigned_abs());
        let mark = self.credited.saturating_add(debt);
        self.consume(n, now);
        Some(mark)
    }

    /// When the credit reaches `mark` at the current rate (at most
    /// [`MAX_WAIT`] away, so a changed rate is seen); `None` once it has,
    /// or with an unlimited rate.
    fn due_at(&self, mark: u128, now: Instant) -> Option<Instant> {
        if self.rate == 0 {
            return None;
        }
        let owed = mark.checked_sub(self.credited_at(now)).filter(|&o| o > 0)?;
        let rate = u128::from(self.rate);
        let nanos = owed.saturating_mul(NANOS_PER_SEC).div_ceil(rate);
        let max = MAX_WAIT.as_nanos();
        if nanos >= max {
            return Some(later(now, MAX_WAIT));
        }
        let nanos = u64::try_from(nanos).unwrap_or(u64::MAX);
        Some(later(now, Duration::from_nanos(nanos)))
    }

    /// The balance at `now`, without changing the bucket.
    fn balance_at(&self, now: Instant) -> i64 {
        if self.rate == 0 {
            return 0;
        }
        let (add, _) = self.gained(now);
        self.balance.saturating_add(add).min(self.capacity)
    }

    /// Changes the rate from `now` on; the capacity follows it. 0 makes
    /// the bucket unlimited and forgives its debt.
    pub(crate) fn set_rate(&mut self, rate: u64, now: Instant) {
        self.refill(now);
        self.rate = rate;
        self.capacity = to_i64(rate.max(MIN_CAPACITY));
        if rate == 0 {
            // Forgiven debt counts as credit: every reservation is due.
            let before = self.balance;
            self.balance = self.balance.max(0);
            self.credit(before);
            self.balance = 0;
            self.last = now;
        } else {
            self.balance = self.balance.min(self.capacity);
        }
    }

    /// Uses `n` bytes at `now`; the balance may go into debt, down to
    /// `-(i64::MAX / 2)`. Unlimited buckets ignore it.
    pub(crate) fn consume(&mut self, n: u64, now: Instant) {
        if self.rate == 0 {
            return;
        }
        self.refill(now);
        self.balance = self.balance.saturating_sub(to_i64(n)).max(-MAX_DEBT);
    }

    /// When the debt is repaid at the current rate (uncapped, saturating);
    /// `None` without debt or with an unlimited rate.
    pub(crate) fn repaid_at(&self, now: Instant) -> Option<Instant> {
        if self.rate == 0 {
            return None;
        }
        let balance = self.balance_at(now);
        if balance >= 0 {
            return None;
        }
        let debt = balance.unsigned_abs();
        let secs = debt / self.rate;
        let rem = u128::from(debt % self.rate);
        let nanos = (rem * NANOS_PER_SEC).div_ceil(u128::from(self.rate));
        let nanos = u64::try_from(nanos).unwrap_or(u64::MAX);
        let d = Duration::from_secs(secs).saturating_add(Duration::from_nanos(nanos));
        Some(later(now, d))
    }

    /// When to check again: `repaid_at`, but at most a second away.
    pub(crate) fn ready_at(&self, now: Instant) -> Option<Instant> {
        self.repaid_at(now).map(|t| t.min(later(now, MAX_WAIT)))
    }
}

/// A shared bucket.
pub(crate) type Bucket = Arc<Mutex<TokenBucket>>;

fn lock(b: &Bucket) -> MutexGuard<'_, TokenBucket> {
    b.lock().unwrap_or_else(|e| e.into_inner())
}

fn bucket(rate: u64) -> Bucket {
    Arc::new(Mutex::new(TokenBucket::new(rate, Instant::now())))
}

/// A download and an upload bucket.
#[derive(Debug, Clone)]
pub(crate) struct BucketPair {
    pub(crate) down: Bucket,
    pub(crate) up: Bucket,
}

impl BucketPair {
    pub(crate) fn new(limit: RateLimit) -> Self {
        BucketPair {
            down: bucket(limit.download),
            up: bucket(limit.upload),
        }
    }

    /// Applies `limit` from now on.
    pub(crate) fn set(&self, limit: RateLimit) {
        let now = Instant::now();
        lock(&self.down).set_rate(limit.download, now);
        lock(&self.up).set_rate(limit.upload, now);
    }
}

/// The buckets one direction of one connection is charged to.
#[derive(Debug, Clone, Default)]
pub(crate) struct Throttle {
    chain: Vec<Bucket>,
}

impl Throttle {
    /// Charges `n` bytes to every bucket of the chain.
    pub(crate) fn charge(&self, n: u64) {
        let now = Instant::now();
        for b in &self.chain {
            lock(b).consume(n, now);
        }
    }

    /// When to check again, `None` once every bucket is out of debt.
    pub(crate) fn ready_at(&self, now: Instant) -> Option<Instant> {
        self.chain
            .iter()
            .filter_map(|b| lock(b).ready_at(now))
            .max()
    }

    /// Sleeps until every bucket of the chain is out of debt.
    pub(crate) async fn wait_ready(&self) {
        while let Some(t) = self.ready_at(Instant::now()) {
            sleep_until(t).await;
        }
    }

    /// Charges `n` bytes to every bucket of the chain now, for a frame
    /// written once the reservation is due: after every frame reserved
    /// before it, so connections sharing a bucket take turns.
    pub(crate) fn reserve(&self, n: u64) -> Reservation {
        let now = Instant::now();
        let marks = self
            .chain
            .iter()
            .filter_map(|b| lock(b).reserve(n, now).map(|m| (Arc::clone(b), m)))
            .collect();
        Reservation { marks }
    }
}

/// Bytes charged to a [`Throttle`] ahead of the frame they pay for.
#[derive(Debug)]
pub(crate) struct Reservation {
    /// Each limited bucket, and the credit at which it is the frame's turn.
    marks: Vec<(Bucket, u128)>,
}

impl Reservation {
    /// When to check again, `None` once the frame may be written: every
    /// bucket repaid the debt owed before it was reserved.
    pub(crate) fn due_at(&self, now: Instant) -> Option<Instant> {
        self.marks
            .iter()
            .filter_map(|(b, m)| lock(b).due_at(*m, now))
            .max()
    }
}

/// A connected peer counted for mixed mode; uncounted when dropped.
#[derive(Debug)]
pub(crate) struct Counted {
    bandwidth: Arc<Bandwidth>,
    transport: Transport,
}

impl Drop for Counted {
    fn drop(&mut self) {
        let n = self.bandwidth.count(self.transport);
        let _ = n.try_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
    }
}

/// A connection's throttles, and its mixed-mode count while it lives.
#[derive(Debug, Default)]
pub(crate) struct PeerThrottles {
    pub(crate) down: Throttle,
    pub(crate) up: Throttle,
    #[allow(dead_code, reason = "held for its Drop, which uncounts the peer")]
    pub(crate) counted: Option<Counted>,
}

/// The configured limits.
#[derive(Debug, Clone, Copy, Default)]
struct Limits {
    global: RateLimit,
    classes: PeerClassLimits,
}

/// The session's rate limits: global and class buckets, and the peer
/// counts mixed mode balances by. Torrents hold their own [`BucketPair`].
#[derive(Debug)]
pub(crate) struct Bandwidth {
    limit_local_peers: bool,
    mode: MixedMode,
    limits: Mutex<Limits>,
    global: BucketPair,
    tcp: BucketPair,
    utp: BucketPair,
    local: BucketPair,
    /// The mixed-mode shares of the global limit, chained only by peers
    /// the global limit applies to.
    tcp_share: BucketPair,
    utp_share: BucketPair,
    /// Connected TCP and uTP peers the global limit applies to.
    tcp_peers: AtomicUsize,
    utp_peers: AtomicUsize,
}

impl Bandwidth {
    pub(crate) fn new(settings: &Settings) -> Self {
        let classes = settings.peer_class_limits;
        let bw = Bandwidth {
            limit_local_peers: settings.rate_limit_local_peers,
            mode: settings.mixed_mode,
            limits: Mutex::new(Limits {
                global: settings.rate_limit,
                classes,
            }),
            global: BucketPair::new(settings.rate_limit),
            tcp: BucketPair::new(classes.tcp),
            utp: BucketPair::new(classes.utp),
            local: BucketPair::new(classes.local),
            tcp_share: BucketPair::new(RateLimit::default()),
            utp_share: BucketPair::new(RateLimit::default()),
            tcp_peers: AtomicUsize::new(0),
            utp_peers: AtomicUsize::new(0),
        };
        bw.rebalance();
        bw
    }

    fn limits(&self) -> MutexGuard<'_, Limits> {
        self.limits.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn count(&self, t: Transport) -> &AtomicUsize {
        match t {
            Transport::Tcp => &self.tcp_peers,
            Transport::Utp => &self.utp_peers,
        }
    }

    fn class(&self, t: Transport) -> &BucketPair {
        match t {
            Transport::Tcp => &self.tcp,
            Transport::Utp => &self.utp,
        }
    }

    fn share(&self, t: Transport) -> &BucketPair {
        match t {
            Transport::Tcp => &self.tcp_share,
            Transport::Utp => &self.utp_share,
        }
    }

    /// Sets the global limit, at once.
    pub(crate) fn set_global(&self, limit: RateLimit) {
        let mut l = self.limits();
        l.global = limit;
        self.global.set(limit);
        self.apply(&l);
    }

    /// Sets the class limits, at once.
    pub(crate) fn set_classes(&self, classes: PeerClassLimits) {
        let mut l = self.limits();
        l.classes = classes;
        self.tcp.set(classes.tcp);
        self.utp.set(classes.utp);
        self.local.set(classes.local);
    }

    /// Sets the TCP and uTP share rates from the global limit and the
    /// current peer counts.
    pub(crate) fn rebalance(&self) {
        let l = self.limits();
        self.apply(&l);
    }

    fn apply(&self, l: &Limits) {
        let tcp = self.tcp_peers.load(Ordering::Acquire);
        let utp = self.utp_peers.load(Ordering::Acquire);
        let (tcp_down, utp_down) = mixed_shares(self.mode, l.global.download, tcp, utp);
        let (tcp_up, utp_up) = mixed_shares(self.mode, l.global.upload, tcp, utp);
        let now = Instant::now();
        let set = |b: &Bucket, rate: u64| {
            let mut b = lock(b);
            if b.rate != rate {
                b.set_rate(rate, now);
            }
        };
        set(&self.tcp_share.down, tcp_down);
        set(&self.tcp_share.up, tcp_up);
        set(&self.utp_share.down, utp_down);
        set(&self.utp_share.up, utp_up);
    }

    /// Re-evaluates mixed mode every second while the session lives.
    pub(crate) fn spawn_rebalance(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(REBALANCE_INTERVAL);
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(bw) = weak.upgrade() else {
                    return;
                };
                bw.rebalance();
            }
        });
    }

    /// The download throttle of a torrent's web seeds: the session's
    /// global download limit, then the torrent's `torrent`. Web seeds are
    /// not peers: no transport class, no local exemption, not counted for
    /// mixed mode.
    pub(crate) fn web_seed(&self, torrent: &BucketPair) -> Throttle {
        Throttle {
            chain: vec![Arc::clone(&self.global.down), Arc::clone(&torrent.down)],
        }
    }

    /// The throttles of a connection over `transport` to `ip`, for a
    /// torrent with the buckets `torrent`. A peer the global limit applies
    /// to is counted for mixed mode while the result lives.
    pub(crate) fn peer(
        self: &Arc<Self>,
        transport: Transport,
        ip: IpAddr,
        torrent: &BucketPair,
    ) -> PeerThrottles {
        let class = self.class(transport);
        let mut down = vec![Arc::clone(&class.down)];
        let mut up = vec![Arc::clone(&class.up)];
        let local = fbt_dht::is_local(ip);
        if local {
            down.push(Arc::clone(&self.local.down));
            up.push(Arc::clone(&self.local.up));
        }
        let global = !local || self.limit_local_peers;
        if global {
            let share = self.share(transport);
            down.extend([&share.down, &self.global.down, &torrent.down].map(Arc::clone));
            up.extend([&share.up, &self.global.up, &torrent.up].map(Arc::clone));
            let n = self.count(transport);
            let _ = n.try_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1));
        }
        PeerThrottles {
            down: Throttle { chain: down },
            up: Throttle { chain: up },
            counted: global.then(|| Counted {
                bandwidth: Arc::clone(self),
                transport,
            }),
        }
    }
}

/// The TCP and uTP shares of a `global` rate (0: no share). A class without counted peers gets none; one
/// with peers never gets less than 1 byte per second.
fn mixed_shares(mode: MixedMode, global: u64, tcp: usize, utp: usize) -> (u64, u64) {
    if global == 0 {
        return (0, 0);
    }
    match mode {
        MixedMode::PeerProportional => {
            let total = (tcp as u128).saturating_add(utp as u128);
            let share = |n: usize| {
                if n == 0 {
                    0
                } else {
                    let s = u128::from(global) * n as u128 / total;
                    u64::try_from(s).unwrap_or(u64::MAX).max(1)
                }
            };
            (share(tcp), share(utp))
        }
        MixedMode::PreferTcp => {
            let tcp_share = if tcp == 0 { 0 } else { global };
            let utp_share = match (utp, tcp) {
                (0, _) => 0,
                (_, 0) => global,
                _ => (global / 10).max(1),
            };
            (tcp_share, utp_share)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    fn close(a: Option<Instant>, b: Instant) -> bool {
        a.is_some_and(|a| a.max(b) - a.min(b) <= MS)
    }

    #[test]
    fn bucket_refills_at_rate_and_caps_burst() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(1000, t0);
        assert_eq!(b.balance_at(t0), 0, "no free initial burst");
        assert_eq!(b.balance_at(t0 + Duration::from_millis(500)), 500);
        b.consume(300, t0 + Duration::from_millis(500));
        assert_eq!(b.balance_at(t0 + Duration::from_secs(1)), 700);
        // A long idle time refills up to the 16 KiB floor of the capacity.
        assert_eq!(b.balance_at(t0 + Duration::from_secs(100)), 16_384);
        // A fast bucket holds one second of its rate.
        let mut fast = TokenBucket::new(1_000_000, t0);
        fast.consume(0, t0 + Duration::from_secs(10));
        assert_eq!(fast.balance, 1_000_000);
        // Many small refills lose nothing to rounding.
        let mut slow = TokenBucket::new(3, t0);
        for i in 1..=1000u64 {
            slow.consume(0, t0 + Duration::from_millis(i));
        }
        assert_eq!(slow.balance, 3);
    }

    #[test]
    fn debt_delays_until_repaid() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(1000, t0);
        b.consume(3000, t0);
        assert!(close(b.repaid_at(t0), t0 + Duration::from_secs(3)));
        assert!(close(b.ready_at(t0), t0 + Duration::from_secs(1)));
        let t1 = t0 + Duration::from_secs(1);
        assert!(close(b.ready_at(t1), t1 + Duration::from_secs(1)));
        assert!(close(b.repaid_at(t1), t1 + Duration::from_secs(2)));
        assert_eq!(b.ready_at(t0 + Duration::from_secs(3)), None);
        assert_eq!(b.repaid_at(t0 + Duration::from_secs(3)), None);
    }

    /// Reservations are served in the order they were made: a connection
    /// that keeps asking cannot jump ahead of one that waits.
    #[test]
    fn reservations_take_turns() {
        let t0 = Instant::now();
        let s = Duration::from_millis(500);
        let mut b = TokenBucket::new(1000, t0);
        // A's first frame goes at once; B's waits for it, A's second for B's.
        let a1 = b.reserve(500, t0).expect("limited");
        let b1 = b.reserve(500, t0).expect("limited");
        let a2 = b.reserve(500, t0).expect("limited");
        assert_eq!(b.due_at(a1, t0), None);
        assert!(close(b.due_at(b1, t0), t0 + s));
        assert!(close(b.due_at(a2, t0), t0 + 2 * s));
        assert_eq!(b.due_at(b1, t0 + s), None);
        assert!(b.due_at(a2, t0 + s).is_some());
        assert_eq!(b.due_at(a2, t0 + 2 * s), None);
        // A wait is checked again within a second, and a raised rate
        // shortens it.
        let far = b.reserve(10_000, t0 + 2 * s).expect("limited");
        let c = b.reserve(1, t0 + 2 * s).expect("limited");
        let t1 = t0 + 2 * s;
        assert!(close(b.due_at(c, t1), t1 + Duration::from_secs(1)));
        b.set_rate(20_000, t1);
        assert!(close(b.due_at(c, t1), t1 + Duration::from_millis(525)));
        // Lifting the limit makes every reservation due.
        b.set_rate(0, t1);
        assert_eq!(b.due_at(far, t1), None);
        b.set_rate(1000, t1);
        assert_eq!(b.due_at(c, t1), None);
        assert_eq!(b.reserve(1, t1).map(|m| b.due_at(m, t1)), Some(None));
        // Through a throttle: unlimited buckets hold nothing back.
        let th = Throttle {
            chain: vec![bucket(0), bucket(1000)],
        };
        let first = th.reserve(2000);
        let second = th.reserve(1);
        let now = Instant::now();
        assert_eq!(first.due_at(now), None);
        assert!(second.due_at(now).is_some());
        assert_eq!(Throttle::default().reserve(u64::MAX).due_at(now), None);
    }

    #[test]
    fn zero_rate_is_unlimited() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(0, t0);
        b.consume(u64::MAX, t0);
        assert_eq!(b.repaid_at(t0), None);
        assert_eq!(b.ready_at(t0), None);
        // Lifting a limit forgives the debt at once.
        let mut limited = TokenBucket::new(10, t0);
        limited.consume(1_000_000, t0);
        assert!(limited.ready_at(t0).is_some());
        limited.set_rate(0, t0);
        assert_eq!(limited.ready_at(t0), None);
        let th = Throttle {
            chain: vec![bucket(0), bucket(0)],
        };
        th.charge(u64::MAX);
        assert_eq!(th.ready_at(Instant::now()), None);
        assert_eq!(Throttle::default().ready_at(t0), None);
    }

    #[test]
    fn edge_limits_never_panic() {
        let t0 = Instant::now();
        let century = Duration::from_secs(100 * 365 * 86_400);
        let far = t0.checked_add(century).expect("a century ahead");
        let rates = [1, u64::MAX, i64::MAX as u64 + 1, 0, MIN_CAPACITY];
        for &rate in &rates {
            let mut b = TokenBucket::new(rate, t0);
            b.consume(u64::MAX, t0);
            b.consume(u64::MAX, t0);
            for &now in &[t0, far] {
                let _ = b.repaid_at(now);
                if let Some(r) = b.ready_at(now) {
                    assert!(r <= now + Duration::from_secs(1), "rate {rate}");
                }
            }
            for &other in &rates {
                b.set_rate(other, t0);
                b.consume(u64::MAX, t0);
                let _ = b.repaid_at(far);
                if let Some(r) = b.ready_at(t0) {
                    assert!(r <= t0 + Duration::from_secs(1));
                }
                for _ in 0..3 {
                    if let Some(m) = b.reserve(u64::MAX, t0) {
                        for &now in &[t0, far] {
                            if let Some(r) = b.due_at(m, now) {
                                assert!(r <= now + Duration::from_secs(1));
                            }
                        }
                    }
                }
                b.set_rate(rate, far);
                b.consume(u64::MAX, far);
                let _ = b.repaid_at(t0);
                if let Some(r) = b.ready_at(far) {
                    assert!(r <= far + Duration::from_secs(1));
                }
            }
            // Refilling over a century cannot overflow either.
            b.consume(1, far);
            assert!(b.balance >= -MAX_DEBT && b.balance <= b.capacity);
        }
        // The deepest debt at 1 B/s is due long after a century.
        let mut slow = TokenBucket::new(1, t0);
        slow.consume(u64::MAX, t0);
        assert!(slow.repaid_at(t0).is_some_and(|r| r > far));
        assert!(close(slow.ready_at(t0), t0 + Duration::from_secs(1)));
    }

    fn bw(settings: &Settings) -> Arc<Bandwidth> {
        Arc::new(Bandwidth::new(settings))
    }

    fn rates(p: &BucketPair) -> (u64, u64) {
        (lock(&p.down).rate(), lock(&p.up).rate())
    }

    #[test]
    fn peer_proportional_split() {
        let s = Settings {
            rate_limit: RateLimit::new(900, 900),
            ..Settings::default()
        };
        let b = bw(&s);
        b.tcp_peers.store(2, Ordering::Release);
        b.utp_peers.store(1, Ordering::Release);
        b.rebalance();
        assert_eq!(rates(&b.tcp_share), (600, 600));
        assert_eq!(rates(&b.utp_share), (300, 300));
        // The class buckets keep the user's own class limits.
        assert_eq!(rates(&b.tcp), (0, 0));
        let classes = PeerClassLimits {
            utp: RateLimit::new(100, 0),
            ..PeerClassLimits::default()
        };
        b.set_classes(classes);
        assert_eq!(rates(&b.utp), (100, 0));
        assert_eq!(rates(&b.utp_share), (300, 300));
        // Without a global limit there are no shares.
        b.set_global(RateLimit::default());
        assert_eq!(rates(&b.tcp_share), (0, 0));
        assert_eq!(rates(&b.utp_share), (0, 0));
        assert_eq!(rates(&b.utp), (100, 0));
        // Huge counts and limits do not overflow.
        b.set_global(RateLimit::new(u64::MAX, 1));
        b.tcp_peers.store(usize::MAX, Ordering::Release);
        b.utp_peers.store(usize::MAX, Ordering::Release);
        b.rebalance();
        assert_eq!(rates(&b.tcp_share), (u64::MAX / 2, 1));
    }

    #[test]
    fn prefer_tcp_throttles_utp_while_tcp_present() {
        let s = Settings {
            rate_limit: RateLimit::new(1000, 1000),
            mixed_mode: MixedMode::PreferTcp,
            ..Settings::default()
        };
        let b = bw(&s);
        b.tcp_peers.store(1, Ordering::Release);
        b.utp_peers.store(3, Ordering::Release);
        b.rebalance();
        assert_eq!(rates(&b.utp_share), (100, 100));
        assert_eq!(rates(&b.tcp_share), (1000, 1000));
        b.tcp_peers.store(0, Ordering::Release);
        b.rebalance();
        assert_eq!(rates(&b.utp_share), (1000, 1000));
        assert_eq!(rates(&b.tcp_share), (0, 0));
    }

    fn has(t: &Throttle, b: &Bucket) -> bool {
        t.chain.iter().any(|c| Arc::ptr_eq(c, b))
    }

    #[test]
    fn local_peers_skip_global_by_default() {
        let lan: IpAddr = "192.168.1.20".parse().unwrap();
        let wan: IpAddr = "203.0.113.7".parse().unwrap();
        let s = Settings::default();
        let b = bw(&s);
        let torrent = BucketPair::new(RateLimit::default());
        let p = b.peer(Transport::Utp, lan, &torrent);
        assert_eq!(p.down.chain.len(), 2);
        assert!(has(&p.down, &b.utp.down) && has(&p.down, &b.local.down));
        assert!(has(&p.up, &b.utp.up) && has(&p.up, &b.local.up));
        assert!(p.counted.is_none());
        assert_eq!(b.utp_peers.load(Ordering::Acquire), 0);
        // No mixed-mode share either: exempt peers are never capped by it.
        for share in [&b.tcp_share, &b.utp_share] {
            assert!(!has(&p.down, &share.down) && !has(&p.up, &share.up));
        }
        // A remote peer: its class, its share, the global and the torrent
        // buckets.
        let w = b.peer(Transport::Tcp, wan, &torrent);
        assert_eq!(w.up.chain.len(), 4);
        assert!(has(&w.up, &b.tcp.up) && has(&w.up, &b.global.up));
        assert!(has(&w.up, &b.tcp_share.up) && !has(&w.up, &b.utp_share.up));
        assert!(has(&w.up, &torrent.up) && !has(&w.up, &b.local.up));
        assert_eq!(b.tcp_peers.load(Ordering::Acquire), 1);
        drop(w);
        assert_eq!(b.tcp_peers.load(Ordering::Acquire), 0);
        // With `rate_limit_local_peers`, local peers get every bucket.
        let s = Settings {
            rate_limit_local_peers: true,
            ..Settings::default()
        };
        let b = bw(&s);
        let p = b.peer(Transport::Tcp, lan, &torrent);
        assert_eq!(p.down.chain.len(), 5);
        let all = [
            &b.tcp.down,
            &b.local.down,
            &b.tcp_share.down,
            &b.global.down,
            &torrent.down,
        ];
        for want in all {
            assert!(has(&p.down, want));
        }
        assert_eq!(b.tcp_peers.load(Ordering::Acquire), 1);
    }
}
