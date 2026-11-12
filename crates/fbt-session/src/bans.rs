//! Why a peer is refused, and the session's ban list.
//!
//! Bans are session-wide and in memory, separate from the
//! [`IpFilter`](fbt_net::ipfilter::IpFilter), so replacing the filter never
//! lifts one. They are also separate from the per-torrent ban of peers that
//! sent bad metadata.

use std::collections::{HashSet, VecDeque};
use std::net::SocketAddr;

/// Most entries the ban list holds; beyond it the oldest ban is lifted.
pub(crate) const MAX_BANNED_IPS: usize = 4096;

/// Why a peer connection was refused or closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BlockReason {
    /// The session's [`IpFilter`](crate::IpFilter) blocks its address.
    IpFilter,
    /// The session banned it (it sent data that failed the hash check).
    Banned,
    /// The torrent already has
    /// [`Settings::max_peers_per_ip`](crate::Settings::max_peers_per_ip)
    /// connections from its IP.
    PerIpLimit,
}

/// Banned peers, keyed by address: port 0 bans the whole IP, any other
/// port only that endpoint. At most [`MAX_BANNED_IPS`] entries, the oldest
/// lifted first. IPv4-mapped addresses are stored as IPv4.
#[derive(Debug, Default)]
pub(crate) struct BanList {
    set: HashSet<SocketAddr>,
    /// `set` in ban order, oldest first.
    order: VecDeque<SocketAddr>,
}

/// `addr` with an IPv4-mapped IP written as IPv4.
fn canonical(addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(addr.ip().to_canonical(), addr.port())
}

impl BanList {
    /// The key banning `addr`: its whole IP (port 0), or with `endpoint`
    /// only `addr` itself.
    #[allow(dead_code, reason = "smart ban is its first caller")]
    pub(crate) fn key(addr: SocketAddr, endpoint: bool) -> SocketAddr {
        if endpoint {
            addr
        } else {
            SocketAddr::new(addr.ip(), 0)
        }
    }

    /// Bans `key` (see [`BanList::key`]); false if it was banned already.
    #[allow(dead_code, reason = "smart ban is its first caller")]
    pub(crate) fn ban(&mut self, key: SocketAddr) -> bool {
        let key = canonical(key);
        if !self.set.insert(key) {
            return false;
        }
        self.order.push_back(key);
        while self.order.len() > MAX_BANNED_IPS {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        true
    }

    /// Whether `addr` is banned: its endpoint, or its whole IP.
    pub(crate) fn is_banned(&self, addr: SocketAddr) -> bool {
        if self.set.is_empty() {
            return false;
        }
        let addr = canonical(addr);
        self.set.contains(&addr) || self.set.contains(&SocketAddr::new(addr.ip(), 0))
    }

    /// Every ban, oldest first (port 0 for a whole IP).
    pub(crate) fn list(&self) -> Vec<SocketAddr> {
        self.order.iter().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sa(s: &str) -> SocketAddr {
        s.parse().expect("address")
    }

    #[test]
    fn whole_ip_and_endpoint_bans() {
        let mut b = BanList::default();
        assert!(!b.is_banned(sa("10.0.0.1:6881")));
        assert!(b.ban(BanList::key(sa("10.0.0.1:6881"), false)));
        assert!(!b.ban(sa("10.0.0.1:0")), "banned already");
        assert!(b.is_banned(sa("10.0.0.1:1")));
        assert!(b.is_banned(sa("[::ffff:10.0.0.1]:6881")));
        assert!(!b.is_banned(sa("10.0.0.2:6881")));
        assert!(b.ban(BanList::key(sa("[::ffff:10.0.0.2]:7000"), true)));
        assert!(b.is_banned(sa("10.0.0.2:7000")));
        assert!(!b.is_banned(sa("10.0.0.2:7001")));
        assert_eq!(b.list(), vec![sa("10.0.0.1:0"), sa("10.0.0.2:7000")]);
    }

    #[test]
    fn oldest_ban_is_lifted_when_full() {
        let mut b = BanList::default();
        for i in 0..=MAX_BANNED_IPS as u32 {
            let ip = std::net::Ipv4Addr::from(0x0a00_0000 + i);
            assert!(b.ban(SocketAddr::from((ip, 0))));
        }
        assert_eq!(b.list().len(), MAX_BANNED_IPS);
        assert!(!b.is_banned(sa("10.0.0.0:1")), "the oldest is lifted");
        assert!(b.is_banned(sa("10.0.0.1:1")));
        assert_eq!(b.set.len(), MAX_BANNED_IPS);
    }
}
