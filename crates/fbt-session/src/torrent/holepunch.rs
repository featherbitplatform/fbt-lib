//! BEP 55 holepunch, the pure parts: what a relay answers a `Rendezvous`
//! with, the per-peer rate limit on rendezvous work, and the addresses we
//! asked a relay about. The actor glue lives in `peers.rs` (incoming
//! messages) and `swarm.rs` (asking a relay after a failed connect).
//!
//! Holepunch messages are hostile input: every map here has a named cap,
//! and nothing a peer sends makes us dial without either our own request
//! or the [`RendezvousLimiter`]'s say-so.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::time::Duration;

use fbt_wire::{HP_NO_SELF, HP_NO_SUPPORT, HP_NOT_CONNECTED};
use tokio::time::Instant;

use crate::picker::PeerKey;

/// Rendezvous work (a relayed `Rendezvous`, or an unsolicited `Connect`)
/// accepted from one peer per [`RENDEZVOUS_WINDOW`]; beyond that its
/// messages are ignored.
pub(super) const MAX_RENDEZVOUS_PER_MIN: u32 = 10;
/// The window [`MAX_RENDEZVOUS_PER_MIN`] counts over.
const RENDEZVOUS_WINDOW: Duration = Duration::from_secs(60);
/// Peers the limiter tracks at once. Entries go when their peer
/// disconnects, so the per-torrent peer limit keeps it far below this; a
/// new peer beyond it is refused until an entry's window ends.
const MAX_LIMITED_PEERS: usize = 1024;
/// Addresses we asked a relay about and still wait on.
pub(super) const MAX_PENDING: usize = 64;
/// How long a rendezvous we asked for stays pending.
pub(super) const PENDING_TTL: Duration = Duration::from_secs(30);

/// What a relay does with a `Rendezvous(target)` from a requester.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelayAction {
    /// Answer the requester with `Error { addr: target, code }`.
    Error(u32),
    /// Send `Connect(to_target)` to the target and `Connect(to_requester)`
    /// to the requester.
    Connect {
        to_target: SocketAddr,
        to_requester: SocketAddr,
    },
}

/// The relay's answer to a `Rendezvous(target)` from `requester` (the
/// address the target should dial it at). `our_addrs` are our listen
/// addresses; `target_peer` is our connection whose dialable address is
/// `target`, if any, and whether it advertised `ut_holepunch`.
///
/// An unspecified listen address (`0.0.0.0:p`) matches a loopback target
/// on its port: other local addresses of ours are not known here.
pub(super) fn relay(
    target: SocketAddr,
    requester: SocketAddr,
    our_addrs: &[SocketAddr],
    target_peer: Option<(PeerKey, bool)>,
) -> RelayAction {
    let ours = our_addrs.iter().any(|a| {
        *a == target
            || (a.ip().is_unspecified()
                && a.port() == target.port()
                && target.ip().to_canonical().is_loopback())
    });
    if ours {
        return RelayAction::Error(HP_NO_SELF);
    }
    match target_peer {
        None => RelayAction::Error(HP_NOT_CONNECTED),
        Some((_, false)) => RelayAction::Error(HP_NO_SUPPORT),
        Some((_, true)) => RelayAction::Connect {
            to_target: requester,
            to_requester: target,
        },
    }
}

/// At most [`MAX_RENDEZVOUS_PER_MIN`] accepted per peer per minute.
#[derive(Debug, Default)]
pub(super) struct RendezvousLimiter {
    /// Per peer: when its current window started and what it used of it.
    peers: HashMap<PeerKey, (Instant, u32)>,
}

impl RendezvousLimiter {
    /// Counts one message from `key`; false when it is over its limit (or
    /// the table is full of live windows), and the message is ignored.
    pub(super) fn allow(&mut self, key: PeerKey, now: Instant) -> bool {
        let expired = |start: &Instant| now.saturating_duration_since(*start) >= RENDEZVOUS_WINDOW;
        if !self.peers.contains_key(&key) && self.peers.len() >= MAX_LIMITED_PEERS {
            self.peers.retain(|_, (start, _)| !expired(start));
            if self.peers.len() >= MAX_LIMITED_PEERS {
                return false;
            }
        }
        let (start, used) = self.peers.entry(key).or_insert((now, 0));
        if expired(start) {
            *start = now;
            *used = 0;
        }
        if *used >= MAX_RENDEZVOUS_PER_MIN {
            return false;
        }
        *used += 1;
        true
    }

    /// `key` disconnected.
    pub(super) fn forget(&mut self, key: PeerKey) {
        self.peers.remove(&key);
    }
}

/// The addresses we sent a `Rendezvous` for: a `Connect` for one of them
/// is the answer we asked for. At most [`MAX_PENDING`], each for
/// [`PENDING_TTL`].
#[derive(Debug, Default)]
pub(super) struct Pending {
    since: HashMap<SocketAddr, Instant>,
    /// `since` in insertion order, oldest first.
    order: VecDeque<SocketAddr>,
}

impl Pending {
    /// Forgets the expired entries.
    fn expire(&mut self, now: Instant) {
        while let Some(a) = self.order.front().copied() {
            let live = self
                .since
                .get(&a)
                .is_some_and(|t| now.saturating_duration_since(*t) < PENDING_TTL);
            if live {
                break;
            }
            self.order.pop_front();
            self.since.remove(&a);
        }
    }

    /// Remembers that we asked about `addr`; false (and nothing changes)
    /// when [`MAX_PENDING`] requests are already waiting.
    pub(super) fn insert(&mut self, addr: SocketAddr, now: Instant) -> bool {
        self.expire(now);
        if self.since.contains_key(&addr) {
            self.remove(&addr);
        } else if self.since.len() >= MAX_PENDING {
            return false;
        }
        self.since.insert(addr, now);
        self.order.push_back(addr);
        true
    }

    /// Whether we asked about `addr` within [`PENDING_TTL`]; it is no
    /// longer pending afterwards.
    pub(super) fn take(&mut self, addr: &SocketAddr, now: Instant) -> bool {
        self.expire(now);
        self.remove(addr)
    }

    /// Forgets `addr`; returns whether it was pending.
    pub(super) fn remove(&mut self, addr: &SocketAddr) -> bool {
        if self.since.remove(addr).is_none() {
            return false;
        }
        self.order.retain(|a| a != addr);
        true
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.since.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sa(s: &str) -> SocketAddr {
        s.parse().expect("address")
    }

    fn addr(i: u32) -> SocketAddr {
        SocketAddr::from((std::net::Ipv4Addr::from(0x0a00_0000 + i), 6881))
    }

    #[test]
    fn relay_errors_follow_bep55() {
        let target = sa("10.0.0.2:6881");
        let requester = sa("10.0.0.3:7000");
        let ours = [sa("10.0.0.1:6881")];
        let k = PeerKey(7);
        // The target is us.
        assert_eq!(
            relay(sa("10.0.0.1:6881"), requester, &ours, Some((k, true))),
            RelayAction::Error(HP_NO_SELF)
        );
        // A wildcard listener matches a loopback target on its port only.
        let wild = [sa("0.0.0.0:6881")];
        assert_eq!(
            relay(sa("127.0.0.1:6881"), requester, &wild, None),
            RelayAction::Error(HP_NO_SELF)
        );
        assert_eq!(
            relay(sa("127.0.0.1:6882"), requester, &wild, None),
            RelayAction::Error(HP_NOT_CONNECTED)
        );
        // Not connected to it.
        assert_eq!(
            relay(target, requester, &ours, None),
            RelayAction::Error(HP_NOT_CONNECTED)
        );
        // Connected, but it lacks `ut_holepunch`.
        assert_eq!(
            relay(target, requester, &ours, Some((k, false))),
            RelayAction::Error(HP_NO_SUPPORT)
        );
        // Both learn the other's address.
        assert_eq!(
            relay(target, requester, &ours, Some((k, true))),
            RelayAction::Connect {
                to_target: requester,
                to_requester: target,
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn rendezvous_rate_limited() {
        let mut l = RendezvousLimiter::default();
        let a = PeerKey(1);
        let b = PeerKey(2);
        for _ in 0..MAX_RENDEZVOUS_PER_MIN {
            assert!(l.allow(a, Instant::now()));
        }
        assert!(!l.allow(a, Instant::now()), "the eleventh is ignored");
        // Another peer has its own budget.
        assert!(l.allow(b, Instant::now()));
        tokio::time::advance(RENDEZVOUS_WINDOW - Duration::from_secs(1)).await;
        assert!(!l.allow(a, Instant::now()), "still within the minute");
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(l.allow(a, Instant::now()), "a new minute");
        // A disconnected peer leaves nothing behind.
        l.forget(a);
        l.forget(b);
        assert!(l.peers.is_empty());
        // The table is bounded: a new peer beyond it waits for a window to
        // end.
        for i in 0..MAX_LIMITED_PEERS as u64 {
            assert!(l.allow(PeerKey(100 + i), Instant::now()));
        }
        assert!(!l.allow(PeerKey(5), Instant::now()));
        assert_eq!(l.peers.len(), MAX_LIMITED_PEERS);
        tokio::time::advance(RENDEZVOUS_WINDOW).await;
        assert!(l.allow(PeerKey(5), Instant::now()));
        assert_eq!(l.peers.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn pending_expires_and_is_capped() {
        let mut p = Pending::default();
        let a = addr(1);
        assert!(!p.take(&a, Instant::now()), "never asked");
        assert!(p.insert(a, Instant::now()));
        assert!(p.take(&a, Instant::now()));
        assert!(!p.take(&a, Instant::now()), "taken once");
        // Expires after 30 s.
        assert!(p.insert(a, Instant::now()));
        tokio::time::advance(PENDING_TTL - Duration::from_millis(1)).await;
        assert!(p.take(&a, Instant::now()), "just in time");
        assert!(p.insert(a, Instant::now()));
        tokio::time::advance(PENDING_TTL).await;
        assert!(!p.take(&a, Instant::now()), "expired");
        assert_eq!(p.len(), 0);
        // At most 64 at once; an expired one makes room.
        for i in 0..MAX_PENDING as u32 {
            assert!(p.insert(addr(i), Instant::now()));
        }
        assert!(!p.insert(addr(1000), Instant::now()), "full");
        assert_eq!(p.len(), MAX_PENDING);
        // Asking again about a pending address renews it.
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(p.insert(addr(0), Instant::now()));
        assert_eq!(p.len(), MAX_PENDING);
        tokio::time::advance(PENDING_TTL - Duration::from_secs(5)).await;
        assert!(p.insert(addr(1000), Instant::now()), "the old ones expired");
        assert_eq!(p.len(), 2);
        assert!(p.take(&addr(0), Instant::now()));
        assert!(p.remove(&addr(1000)));
        assert_eq!(p.len(), 0);
        assert!(p.order.is_empty());
    }
}
