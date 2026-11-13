//! The addresses a torrent may connect to.

use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};

use crate::event::PeerSource;
use crate::picker::PeerKey;

/// Most addresses a torrent remembers for connecting; beyond it the oldest
/// address not in use is forgotten.
pub(super) const MAX_KNOWN: usize = 1000;

/// What a torrent knows about one address it may connect to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct KnownPeer {
    /// Where we first learnt of it; its dials carry it.
    pub source: PeerSource,
    /// A peer listed it with the PEX `UTP` flag since it last turned out
    /// TCP-only: it is dialled over uTP first again.
    pub utp: bool,
    /// The peer whose PEX message listed it last (BEP 55: the relay we ask
    /// for a rendezvous when a connection to it fails).
    pub pex_source: Option<PeerKey>,
    /// That message listed it with the `HOLEPUNCH` flag.
    pub holepunch: bool,
    /// We asked a relay for a rendezvous with it (once per address).
    pub rendezvous_sent: bool,
}

/// The addresses a torrent connects to (from `add_peer` and trackers),
/// reconnected on resume. At most [`MAX_KNOWN`]; when full, the oldest
/// address not in use makes room for a new one.
#[derive(Debug, Default)]
pub(super) struct Known {
    set: HashMap<SocketAddr, KnownPeer>,
    /// `set` in insertion order, oldest first.
    order: VecDeque<SocketAddr>,
}

impl Known {
    /// Remembers `addr`, learnt from `source` (an address known already
    /// keeps the source it was first learnt from). When full, forgets the oldest address for which
    /// `in_use` is false and returns it as the second value. Returns
    /// whether `addr` is known now (false only when every address is in
    /// use).
    pub(super) fn remember(
        &mut self,
        addr: SocketAddr,
        source: PeerSource,
        in_use: impl Fn(&SocketAddr) -> bool,
    ) -> (bool, Option<SocketAddr>) {
        if self.set.contains_key(&addr) {
            return (true, None);
        }
        let mut evicted = None;
        if self.set.len() >= MAX_KNOWN {
            let Some(at) = self.order.iter().position(|a| !in_use(a)) else {
                return (false, None);
            };
            evicted = self.order.remove(at);
            if let Some(old) = &evicted {
                self.set.remove(old);
            }
        }
        self.set.insert(
            addr,
            KnownPeer {
                source,
                utp: false,
                pex_source: None,
                holepunch: false,
                rendezvous_sent: false,
            },
        );
        self.order.push_back(addr);
        (true, evicted)
    }

    pub(super) fn remove(&mut self, addr: &SocketAddr) {
        if self.set.remove(addr).is_some() {
            self.order.retain(|a| a != addr);
        }
    }

    pub(super) fn get(&self, addr: &SocketAddr) -> Option<&KnownPeer> {
        self.set.get(addr)
    }

    pub(super) fn get_mut(&mut self, addr: &SocketAddr) -> Option<&mut KnownPeer> {
        self.set.get_mut(addr)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &SocketAddr> {
        self.order.iter()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.set.len()
    }
}

/// An address a tracker may hand out that we would actually dial: not port
/// 0, not unspecified, multicast or broadcast.
pub(super) fn dialable(addr: &SocketAddr) -> bool {
    let ip_ok = match addr.ip() {
        IpAddr::V4(ip) => !(ip.is_unspecified() || ip.is_multicast() || ip.is_broadcast()),
        IpAddr::V6(ip) => !(ip.is_unspecified() || ip.is_multicast()),
    };
    addr.port() != 0 && ip_ok
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::torrent::swarm::tests::addr;

    #[test]
    fn full_known_table_evicts_the_oldest_idle_address() {
        let mut known = Known::default();
        for i in 0..MAX_KNOWN as u32 {
            assert_eq!(
                known.remember(addr(i), PeerSource::Tracker, |_| false),
                (true, None)
            );
        }
        // Full and everything in use: new addresses are ignored, known ones
        // still answer true.
        assert_eq!(
            known.remember(addr(5000), PeerSource::Tracker, |_| true),
            (false, None)
        );
        assert_eq!(
            known.remember(addr(0), PeerSource::Tracker, |_| true),
            (true, None)
        );
        assert_eq!(known.len(), MAX_KNOWN);

        // Addresses 0..10 are connected or waiting for a retry: the oldest
        // other one (10) makes room.
        let busy = |a: &SocketAddr| (0..10).any(|i| addr(i) == *a);
        assert_eq!(
            known.remember(addr(5000), PeerSource::Tracker, busy),
            (true, Some(addr(10)))
        );
        assert_eq!(known.len(), MAX_KNOWN);
        assert!(known.iter().any(|a| *a == addr(5000)));
        assert!(!known.iter().any(|a| *a == addr(10)));
        // The next one evicts 11; a removed address frees a slot outright.
        assert_eq!(
            known.remember(addr(5001), PeerSource::Tracker, busy),
            (true, Some(addr(11)))
        );
        known.remove(&addr(3));
        assert_eq!(
            known.remember(addr(5002), PeerSource::Tracker, busy),
            (true, None)
        );
        assert_eq!(known.len(), MAX_KNOWN);
    }

    #[test]
    fn undialable_tracker_addresses_are_filtered() {
        let ok = |s: &str| dialable(&s.parse::<SocketAddr>().unwrap());
        assert!(ok("10.0.0.1:6881"));
        assert!(ok("[2001:db8::1]:6881"));
        assert!(!ok("10.0.0.1:0"));
        assert!(!ok("0.0.0.0:6881"));
        assert!(!ok("[::]:6881"));
        assert!(!ok("224.0.0.1:6881"));
        assert!(!ok("255.255.255.255:6881"));
        assert!(!ok("[ff02::1]:6881"));
    }
}
