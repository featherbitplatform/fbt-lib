//! Outgoing connection retries and the per-address transport and
//! encryption fallbacks.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use tokio::time::Instant;

use crate::peer::transport::Transport;

/// Connection attempts per address before it is given up.
const MAX_ATTEMPTS: u32 = 3;
/// The first retry after a fresh failure waits `peer_retry_delay` divided by
/// this (5 s of the default 60 s); later ones wait the full delay. A peer
/// that was merely not ready (starting, checking its files, briefly full)
/// usually accepts a few seconds later, and a lone explicit peer must not
/// stall a download for a minute. Transmission's first retry is 5 s too.
const FIRST_RETRY_DIVISOR: u32 = 12;
/// Most never-retry addresses kept; the oldest is forgotten beyond it.
/// Also bounds the plaintext-only and TCP-only addresses and the pending
/// transport fallbacks.
const MAX_NEVER: usize = 1000;
/// A connection that lasted this long resets its address's drop count.
const STABLE_CONNECTION: Duration = Duration::from_secs(300);
/// Longest wait before redialling a peer that keeps dropping.
const MAX_REDIAL_DELAY: Duration = Duration::from_secs(30 * 60);

/// What a failed attempt leads to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AfterFailure {
    /// Tried again after the delay.
    Retry,
    /// Attempts used up (or a never-retry address): forget it.
    GiveUp,
}

/// Outgoing connection retries: failed addresses come back after `delay`
/// (`Settings::peer_retry_delay`; the first retry of a series sooner, see
/// [`FIRST_RETRY_DIVISOR`]), at most [`MAX_ATTEMPTS`] attempts in all;
/// an address that turned out to be ourselves is never tried again (the
/// last [`MAX_NEVER`] such addresses are kept).
#[derive(Debug)]
pub(super) struct Retries {
    delay: Duration,
    /// Failed attempts since the last successful connection; dropped once
    /// the attempts are used up.
    attempts: HashMap<SocketAddr, u32>,
    /// Addresses waiting for their retry time.
    pending: HashMap<SocketAddr, Instant>,
    never: HashSet<SocketAddr>,
    /// `never` in insertion order, to forget the oldest.
    never_order: VecDeque<SocketAddr>,
    /// Addresses whose MSE handshake failed under `Preferred`: dialled with
    /// a plain BitTorrent handshake from then on (the last [`MAX_NEVER`]).
    plaintext_only: HashSet<SocketAddr>,
    /// `plaintext_only` in insertion order, to forget the oldest.
    plaintext_order: VecDeque<SocketAddr>,
    /// IPs that sent bad metadata, refused while the metadata is pending
    /// (the last [`MAX_NEVER`]).
    metadata_ips: HashSet<IpAddr>,
    /// `metadata_ips` in insertion order, to forget the oldest.
    metadata_ip_order: VecDeque<IpAddr>,
    /// Addresses whose connect over this transport failed and which are
    /// being retried over the other one, until that attempt ends (the last
    /// [`MAX_NEVER`]).
    transport_failed: HashMap<SocketAddr, Transport>,
    /// `transport_failed` in insertion order, to forget the oldest.
    transport_failed_order: VecDeque<SocketAddr>,
    /// Addresses a uTP connect failed to and a TCP one then reached: dialled
    /// over TCP first from then on (the last [`MAX_NEVER`]).
    tcp_only: HashSet<SocketAddr>,
    /// `tcp_only` in insertion order, to forget the oldest.
    tcp_only_order: VecDeque<SocketAddr>,
    /// Drops of productive connections per address (the last
    /// [`MAX_NEVER`]); not reset by a new connection, only by a stable one.
    drops: HashMap<SocketAddr, u32>,
    /// `drops` in insertion order, to forget the oldest.
    drops_order: VecDeque<SocketAddr>,
}

impl Retries {
    pub(super) fn new(delay: Duration) -> Self {
        Retries {
            delay,
            attempts: HashMap::new(),
            pending: HashMap::new(),
            never: HashSet::new(),
            never_order: VecDeque::new(),
            plaintext_only: HashSet::new(),
            plaintext_order: VecDeque::new(),
            metadata_ips: HashSet::new(),
            metadata_ip_order: VecDeque::new(),
            transport_failed: HashMap::new(),
            transport_failed_order: VecDeque::new(),
            tcp_only: HashSet::new(),
            tcp_only_order: VecDeque::new(),
            drops: HashMap::new(),
            drops_order: VecDeque::new(),
        }
    }

    /// Connecting to `addr` over `t` failed. True the first time: the
    /// address is then retried at once over the other transport (without
    /// counting an attempt); a failure of that retry is an ordinary one.
    pub(super) fn transport_failed(&mut self, addr: SocketAddr, t: Transport) -> bool {
        if self.transport_failed.contains_key(&addr) {
            return false;
        }
        self.transport_failed.insert(addr, t);
        self.transport_failed_order.push_back(addr);
        while self.transport_failed_order.len() > MAX_NEVER {
            if let Some(old) = self.transport_failed_order.pop_front() {
                self.transport_failed.remove(&old);
            }
        }
        true
    }

    /// The transport to retry `addr` over after a connect failure, `None`
    /// when no such retry is due.
    pub(super) fn fallback_transport(&self, addr: &SocketAddr) -> Option<Transport> {
        self.transport_failed.get(addr).map(|t| t.other())
    }

    /// The fallback attempt to `addr` ended (or will not happen); the next
    /// one starts over.
    pub(super) fn clear_transport_failed(&mut self, addr: &SocketAddr) -> Option<Transport> {
        let t = self.transport_failed.remove(addr)?;
        self.transport_failed_order.retain(|a| a != addr);
        Some(t)
    }

    /// Dial `addr` over TCP first.
    pub(super) fn is_tcp_only(&self, addr: &SocketAddr) -> bool {
        self.tcp_only.contains(addr)
    }

    fn mark_tcp_only(&mut self, addr: SocketAddr) {
        if !self.tcp_only.insert(addr) {
            return;
        }
        self.tcp_only_order.push_back(addr);
        while self.tcp_only_order.len() > MAX_NEVER {
            if let Some(old) = self.tcp_only_order.pop_front() {
                self.tcp_only.remove(&old);
            }
        }
    }

    /// `ip` sent bad metadata.
    pub(super) fn ban_metadata_ip(&mut self, ip: IpAddr) {
        if !self.metadata_ips.insert(ip) {
            return;
        }
        self.metadata_ip_order.push_back(ip);
        while self.metadata_ip_order.len() > MAX_NEVER {
            if let Some(old) = self.metadata_ip_order.pop_front() {
                self.metadata_ips.remove(&old);
            }
        }
    }

    pub(super) fn is_metadata_banned(&self, ip: &IpAddr) -> bool {
        self.metadata_ips.contains(ip)
    }

    /// An MSE handshake to `addr` failed. True when a plaintext retry is
    /// due: the first such failure marks the address plaintext-only (and
    /// counts no attempt); later ones are ordinary failures.
    pub(super) fn encryption_failed(&mut self, addr: SocketAddr) -> bool {
        if !self.plaintext_only.insert(addr) {
            return false;
        }
        self.plaintext_order.push_back(addr);
        while self.plaintext_order.len() > MAX_NEVER {
            if let Some(old) = self.plaintext_order.pop_front() {
                self.plaintext_only.remove(&old);
            }
        }
        true
    }

    /// Dial `addr` without MSE.
    pub(super) fn is_plaintext_only(&self, addr: &SocketAddr) -> bool {
        self.plaintext_only.contains(addr)
    }

    /// Never connect to `addr` again.
    pub(super) fn never(&mut self, addr: SocketAddr) {
        self.pending.remove(&addr);
        self.attempts.remove(&addr);
        self.clear_transport_failed(&addr);
        if !self.never.insert(addr) {
            return;
        }
        self.never_order.push_back(addr);
        while self.never_order.len() > MAX_NEVER {
            if let Some(old) = self.never_order.pop_front() {
                self.never.remove(&old);
            }
        }
    }

    pub(super) fn is_never(&self, addr: &SocketAddr) -> bool {
        self.never.contains(addr)
    }

    /// Waiting for a retry: neither queued again nor connected early.
    pub(super) fn is_pending(&self, addr: &SocketAddr) -> bool {
        self.pending.contains_key(addr)
    }

    /// An attempt failed; schedules a retry unless attempts are used up.
    pub(super) fn failed(&mut self, addr: SocketAddr, now: Instant) -> AfterFailure {
        self.clear_transport_failed(&addr);
        if self.never.contains(&addr) {
            return AfterFailure::GiveUp;
        }
        let n = self.attempts.entry(addr).or_insert(0);
        *n = n.saturating_add(1);
        if *n < MAX_ATTEMPTS {
            let wait = if *n == 1 {
                self.delay / FIRST_RETRY_DIVISOR
            } else {
                self.delay
            };
            // A huge delay means "not in this process's lifetime".
            let at = now
                .checked_add(wait)
                .unwrap_or_else(|| now + Duration::from_secs(86_400 * 365));
            self.pending.insert(addr, at);
            AfterFailure::Retry
        } else {
            self.attempts.remove(&addr);
            AfterFailure::GiveUp
        }
    }

    /// A productive connection to `addr` that lasted `connected_for` was
    /// dropped by the peer or its transport; schedules the redial. The
    /// first drop redials after `delay / FIRST_RETRY_DIVISOR`, drop `n`
    /// after `delay * (n - 1)`, at most [`MAX_REDIAL_DELAY`]; a connection
    /// that lasted [`STABLE_CONNECTION`] starts the count over.
    pub(super) fn dropped(&mut self, addr: SocketAddr, now: Instant, connected_for: Duration) {
        if self.never.contains(&addr) {
            return;
        }
        if connected_for >= STABLE_CONNECTION {
            self.drops.remove(&addr);
            self.drops_order.retain(|a| *a != addr);
        }
        let n = match self.drops.get_mut(&addr) {
            Some(n) => {
                *n = n.saturating_add(1);
                *n
            }
            None => {
                self.drops.insert(addr, 1);
                self.drops_order.push_back(addr);
                while self.drops_order.len() > MAX_NEVER {
                    if let Some(old) = self.drops_order.pop_front() {
                        self.drops.remove(&old);
                    }
                }
                1
            }
        };
        let wait = if n == 1 {
            self.delay / FIRST_RETRY_DIVISOR
        } else {
            self.delay.saturating_mul(n - 1)
        }
        .min(MAX_REDIAL_DELAY);
        let at = now
            .checked_add(wait)
            .unwrap_or_else(|| now + Duration::from_secs(86_400 * 365));
        self.pending.insert(addr, at);
    }

    /// The handshake over `transport` succeeded: the attempts start over.
    /// After a fallback, the transport that worked is remembered: a TCP
    /// success after a failed uTP connect makes the address TCP-only (and
    /// returns true, also when it already was), a uTP success after a
    /// failed TCP connect makes it uTP-first again.
    pub(super) fn connected(&mut self, addr: &SocketAddr, transport: Transport) -> bool {
        self.attempts.remove(addr);
        self.pending.remove(addr);
        match (self.clear_transport_failed(addr), transport) {
            (Some(Transport::Utp), Transport::Tcp) => {
                self.mark_tcp_only(*addr);
                true
            }
            (Some(Transport::Tcp), Transport::Utp) => {
                self.unmark_tcp_only(addr);
                false
            }
            _ => false,
        }
    }

    fn unmark_tcp_only(&mut self, addr: &SocketAddr) {
        if self.tcp_only.remove(addr) {
            self.tcp_only_order.retain(|a| a != addr);
        }
    }

    /// Removes and returns the addresses whose retry time has come.
    pub(super) fn due(&mut self, now: Instant) -> Vec<SocketAddr> {
        let due: Vec<SocketAddr> = self
            .pending
            .iter()
            .filter(|(_, at)| **at <= now)
            .map(|(a, _)| *a)
            .collect();
        for a in &due {
            self.pending.remove(a);
        }
        due
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::torrent::swarm::tests::addr;

    const DELAY: Duration = Duration::from_secs(60);

    #[tokio::test(start_paused = true)]
    async fn failed_connects_retry_after_the_delay_three_attempts_in_all() {
        let a = addr(1);
        let mut r = Retries::new(DELAY);
        let mut attempts = 1;
        while r.failed(a, Instant::now()) == AfterFailure::Retry {
            assert!(r.is_pending(&a));
            // The first retry comes after 5 s of the 60 s, the next after
            // all of it.
            let wait = if attempts == 1 {
                Duration::from_secs(5)
            } else {
                DELAY
            };
            tokio::time::advance(wait - Duration::from_secs(1)).await;
            assert!(r.due(Instant::now()).is_empty(), "not before the delay");
            tokio::time::advance(Duration::from_secs(1)).await;
            assert_eq!(r.due(Instant::now()), vec![a]);
            assert!(!r.is_pending(&a));
            attempts += 1;
        }
        assert_eq!(attempts, MAX_ATTEMPTS);
        // Given up: nothing pending and the counter is gone.
        assert!(!r.is_pending(&a));
        assert!(r.attempts.is_empty());
        tokio::time::advance(DELAY * 10).await;
        assert!(r.due(Instant::now()).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn success_resets_attempts_and_self_is_never_retried() {
        let a = addr(1);
        let me = addr(2);
        let mut r = Retries::new(DELAY);
        r.failed(a, Instant::now());
        r.failed(a, Instant::now());
        r.connected(&a, Transport::Tcp);
        assert!(!r.is_pending(&a) && r.attempts.is_empty());
        // A fresh series: two more failures still schedule retries.
        assert_eq!(r.failed(a, Instant::now()), AfterFailure::Retry);
        assert_eq!(r.failed(a, Instant::now()), AfterFailure::Retry);
        r.failed(me, Instant::now());
        r.never(me);
        assert!(r.is_never(&me) && !r.is_pending(&me));
        assert_eq!(r.failed(me, Instant::now()), AfterFailure::GiveUp);
        assert!(!r.is_pending(&me));
        tokio::time::advance(DELAY).await;
        assert_eq!(r.due(Instant::now()), vec![a]);
    }

    #[test]
    fn plaintext_retry_is_due_once_per_address_and_bounded() {
        let a = addr(1);
        let mut r = Retries::new(DELAY);
        assert!(!r.is_plaintext_only(&a));
        assert!(
            r.encryption_failed(a),
            "first MSE failure: retry in plaintext"
        );
        assert!(r.is_plaintext_only(&a));
        assert!(!r.encryption_failed(a), "only once");
        assert!(
            r.attempts.is_empty() && !r.is_pending(&a),
            "no attempt counted"
        );
        for i in 2..(MAX_NEVER as u32 + 12) {
            r.encryption_failed(addr(i));
        }
        assert_eq!(r.plaintext_only.len(), MAX_NEVER);
        assert_eq!(r.plaintext_order.len(), MAX_NEVER);
        assert!(!r.is_plaintext_only(&a), "the oldest is forgotten");
        assert!(r.is_plaintext_only(&addr(MAX_NEVER as u32 + 11)));
    }

    #[tokio::test(start_paused = true)]
    async fn transport_fallback_is_due_once_per_attempt_and_bounded() {
        let a = addr(1);
        let mut r = Retries::new(DELAY);
        assert_eq!(r.fallback_transport(&a), None);
        assert!(r.transport_failed(a, Transport::Utp), "first uTP failure");
        assert_eq!(r.fallback_transport(&a), Some(Transport::Tcp));
        // The TCP fallback failing too is an ordinary failure.
        assert!(!r.transport_failed(a, Transport::Tcp));
        assert!(r.attempts.is_empty() && !r.is_pending(&a), "no attempt yet");
        assert_eq!(r.failed(a, Instant::now()), AfterFailure::Retry);
        // The next attempt starts over: uTP first, one fallback again.
        assert_eq!(r.fallback_transport(&a), None);
        assert!(r.transport_failed(a, Transport::Utp));
        // uTP failed and TCP got through: TCP-only from then on.
        assert!(r.connected(&a, Transport::Tcp));
        assert!(r.is_tcp_only(&a) && r.fallback_transport(&a).is_none());
        // A second fallback the same way (a PEX `UTP` flag made uTP first
        // again) reports TCP-only again, so the flag is cleared again.
        assert!(r.transport_failed(a, Transport::Utp));
        assert!(
            r.connected(&a, Transport::Tcp),
            "already TCP-only: still true"
        );
        assert!(r.is_tcp_only(&a));
        // TCP failing and the uTP fallback working: uTP first again.
        assert!(r.transport_failed(a, Transport::Tcp));
        assert_eq!(r.fallback_transport(&a), Some(Transport::Utp));
        assert!(!r.connected(&a, Transport::Utp));
        assert!(!r.is_tcp_only(&a) && !r.tcp_only_order.contains(&a));
        // A plain success marks nothing.
        let b = addr(2);
        assert!(!r.connected(&b, Transport::Tcp) && !r.is_tcp_only(&b));
        // A cleared fallback (skipped, or closed by us) is not dialled.
        assert!(r.transport_failed(b, Transport::Utp));
        assert_eq!(r.clear_transport_failed(&b), Some(Transport::Utp));
        assert_eq!(r.fallback_transport(&b), None);
        assert!(!r.connected(&b, Transport::Tcp) && !r.is_tcp_only(&b));
        for i in 10..(MAX_NEVER as u32 + 20) {
            r.transport_failed(addr(i), Transport::Utp);
            r.connected(&addr(i), Transport::Tcp);
            r.transport_failed(addr(i + 100_000), Transport::Utp);
        }
        assert_eq!(r.tcp_only.len(), MAX_NEVER);
        assert_eq!(r.tcp_only_order.len(), MAX_NEVER);
        assert!(!r.is_tcp_only(&addr(10)), "the oldest is forgotten");
        assert!(r.transport_failed.len() <= MAX_NEVER);
        assert_eq!(r.transport_failed.len(), r.transport_failed_order.len());
    }

    #[test]
    fn address_maps_are_bounded() {
        let mut r = Retries::new(DELAY);
        for i in 0..(MAX_NEVER as u32 + 10) {
            r.never(addr(i));
            r.never(addr(i));
        }
        assert_eq!(r.never.len(), MAX_NEVER);
        assert_eq!(r.never_order.len(), MAX_NEVER);
        // The oldest were forgotten, the newest kept.
        assert!(!r.is_never(&addr(0)) && !r.is_never(&addr(9)));
        assert!(r.is_never(&addr(10)) && r.is_never(&addr(MAX_NEVER as u32 + 9)));
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_drops_back_off() {
        let a = addr(1);
        let mut r = Retries::new(DELAY);
        for (i, secs) in [5u64, 60, 120, 180].into_iter().enumerate() {
            let t = Instant::now();
            r.dropped(a, t, Duration::from_secs(1));
            assert!(r.is_pending(&a));
            assert!(r.due(t + Duration::from_secs(secs - 1)).is_empty(), "{i}");
            assert_eq!(r.due(t + Duration::from_secs(secs)), vec![a], "{i}");
            tokio::time::advance(Duration::from_secs(10)).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn drops_are_capped_at_thirty_minutes() {
        let a = addr(1);
        let mut r = Retries::new(DELAY);
        let t = Instant::now();
        for _ in 0..100 {
            r.dropped(a, t, Duration::from_secs(1));
        }
        assert!(
            r.due(t + MAX_REDIAL_DELAY - Duration::from_secs(1))
                .is_empty()
        );
        assert_eq!(r.due(t + MAX_REDIAL_DELAY), vec![a]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stable_connection_resets_the_count() {
        let a = addr(1);
        let mut r = Retries::new(DELAY);
        let t = Instant::now();
        for _ in 0..4 {
            r.dropped(a, t, Duration::from_secs(1));
        }
        r.dropped(a, t, STABLE_CONNECTION);
        assert!(r.due(t + Duration::from_secs(4)).is_empty());
        assert_eq!(r.due(t + Duration::from_secs(5)), vec![a]);
        // A new connection alone does not reset the count.
        r.connected(&a, Transport::Tcp);
        r.dropped(a, t, Duration::from_secs(1));
        assert!(r.due(t + Duration::from_secs(59)).is_empty());
        assert_eq!(r.due(t + Duration::from_secs(60)), vec![a]);
    }

    #[tokio::test(start_paused = true)]
    async fn drop_map_is_bounded() {
        let mut r = Retries::new(DELAY);
        let t = Instant::now();
        for i in 0..(MAX_NEVER as u32 + 10) {
            r.dropped(addr(i), t, Duration::from_secs(1));
        }
        assert_eq!(r.drops.len(), MAX_NEVER);
        assert_eq!(r.drops_order.len(), MAX_NEVER);
        assert!(!r.drops.contains_key(&addr(0)));
        assert!(r.drops.contains_key(&addr(MAX_NEVER as u32 + 9)));
    }
}
