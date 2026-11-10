//! Peer exchange (BEP 11, `ut_pex`): every `pex_interval` each peer that
//! speaks it learns which connected peers were added or dropped since the
//! last message, and what peers send us joins the known addresses.
//!
//! Only when `pex_enabled`, the torrent is not private (a magnet counts as
//! public until its metadata says otherwise) and the peer advertised
//! `ut_pex`. Incoming lists are hostile: one message per receive gap is
//! taken, at most [`MAX_PEX_ADDED`] of its addresses, through the same
//! bounded, dialable-only path as tracker replies.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use fbt_wire::{PexFlags, PexMsg};
use tokio::time::Instant;
use tracing::debug;

use super::actor::Actor;
use super::swarm::{Peer, dialable};
use crate::event::PeerSource;
use crate::peer::messages::PeerCommand;
use crate::picker::PeerKey;

/// Most addresses sent in, or taken from, one message's `added` list.
pub(super) const MAX_PEX_ADDED: usize = 50;
/// Most addresses sent in one message's `dropped` list.
const MAX_PEX_DROPPED: usize = 50;

/// PEX state for one peer that speaks `ut_pex`.
#[derive(Debug, Default)]
pub(super) struct PexPeer {
    /// The addresses this peer holds from us: added and not dropped since.
    /// Only ever our own connections' addresses, so bounded by them.
    sent: HashSet<SocketAddr>,
    last_sent: Option<Instant>,
    last_received: Option<Instant>,
}

impl PexPeer {
    /// `interval` has passed since the last message (or none went out yet).
    fn due(&self, now: Instant, interval: Duration) -> bool {
        self.last_sent
            .is_none_or(|t| now.saturating_duration_since(t) >= interval)
    }

    /// The next message for this peer from the `live` connectable
    /// addresses, never listing `exclude` (the recipient). `None` before
    /// `interval` has passed since the last one or when nothing changed.
    pub(super) fn build(
        &mut self,
        live: &[(SocketAddr, PexFlags)],
        exclude: SocketAddr,
        now: Instant,
        interval: Duration,
    ) -> Option<PexMsg> {
        if !self.due(now, interval) {
            return None;
        }
        let mut current: HashSet<SocketAddr> = HashSet::with_capacity(live.len());
        let mut added = Vec::new();
        for &(addr, flags) in live {
            if addr == exclude || !current.insert(addr) {
                continue;
            }
            if added.len() < MAX_PEX_ADDED && !self.sent.contains(&addr) {
                added.push((addr, flags));
            }
        }
        let dropped: Vec<SocketAddr> = self
            .sent
            .iter()
            .filter(|a| !current.contains(a))
            .take(MAX_PEX_DROPPED)
            .copied()
            .collect();
        if added.is_empty() && dropped.is_empty() {
            return None;
        }
        for a in &dropped {
            self.sent.remove(a);
        }
        self.sent.extend(added.iter().map(|(a, _)| *a));
        self.last_sent = Some(now);
        Some(PexMsg { added, dropped })
    }

    /// The receive rate gate: true for at most one message per `gap`.
    pub(super) fn accept(&mut self, now: Instant, gap: Duration) -> bool {
        if self
            .last_received
            .is_some_and(|t| now.saturating_duration_since(t) < gap)
        {
            return false;
        }
        self.last_received = Some(now);
        true
    }
}

/// The address others can dial `p` at: its address for an outgoing peer,
/// `(ip, listen_port)` for an incoming one whose extension handshake gave
/// `p`, `None` otherwise.
pub(super) fn connectable_addr(p: &Peer) -> Option<SocketAddr> {
    if p.outgoing {
        Some(p.addr)
    } else {
        p.listen_port.map(|port| SocketAddr::new(p.addr.ip(), port))
    }
}

/// The flags we list `p` with.
fn flags_of(p: &Peer) -> PexFlags {
    let mut f = PexFlags::CONNECTABLE;
    if p.encrypted {
        f |= PexFlags::ENCRYPTION;
    }
    if p.is_seed() {
        f |= PexFlags::SEED;
    }
    f
}

impl Actor {
    /// PEX runs: enabled in the settings and the torrent not (known to be)
    /// private.
    pub(super) fn pex_active(&self) -> bool {
        self.shared.settings.pex_enabled && !self.is_private()
    }

    /// The receive gap: three quarters of `pex_interval`.
    fn pex_gap(&self) -> Duration {
        self.shared.settings.pex_interval * 3 / 4
    }

    /// Sends each due PEX peer what changed since its last message.
    pub(super) fn pex_tick(&mut self) {
        if !self.pex_active() {
            return;
        }
        let now = Instant::now();
        let interval = self.shared.settings.pex_interval;
        let live_peer = |p: &Peer| p.connected && p.tx.is_some();
        let due: Vec<PeerKey> = self
            .peers
            .iter()
            .filter(|(_, p)| live_peer(p) && p.pex.as_ref().is_some_and(|x| x.due(now, interval)))
            .map(|(k, _)| *k)
            .collect();
        if due.is_empty() {
            return;
        }
        let live: Vec<(SocketAddr, PexFlags)> = self
            .peers
            .values()
            .filter(|p| live_peer(p))
            .filter_map(|p| {
                connectable_addr(p)
                    .filter(dialable)
                    .map(|a| (a, flags_of(p)))
            })
            .collect();
        for key in due {
            let Some(p) = self.peers.get_mut(&key) else {
                continue;
            };
            let exclude = connectable_addr(p).unwrap_or(p.addr);
            let Some(msg) = p
                .pex
                .as_mut()
                .and_then(|x| x.build(&live, exclude, now, interval))
            else {
                continue;
            };
            self.send(key, PeerCommand::SendPex(msg));
        }
    }

    /// A `ut_pex` message from a peer: dropped unless PEX runs with it and
    /// the gap since its last accepted one has passed; otherwise the first
    /// [`MAX_PEX_ADDED`] dialable `added` addresses join the known ones.
    /// `dropped` is ignored: known peers stay known until connecting fails.
    pub(super) fn on_pex(&mut self, key: PeerKey, msg: PexMsg) {
        if !self.pex_active() {
            return;
        }
        let gap = self.pex_gap();
        let Some(pex) = self.peers.get_mut(&key).and_then(|p| p.pex.as_mut()) else {
            return;
        };
        if !pex.accept(Instant::now(), gap) {
            debug!(key = key.0, "PEX message within the receive gap; dropped");
            return;
        }
        let addrs: Vec<SocketAddr> = msg
            .added
            .into_iter()
            .map(|(a, _)| a)
            .filter(dialable)
            .take(MAX_PEX_ADDED)
            .collect();
        self.on_found_peers(PeerSource::Pex, addrs);
    }

    /// Stops PEX with every peer (a magnet's metadata said private).
    pub(super) fn drop_pex_peers(&mut self) {
        for p in self.peers.values_mut() {
            p.pex = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    const INTERVAL: Duration = Duration::from_secs(60);

    fn addr(i: u32) -> SocketAddr {
        SocketAddr::from((std::net::Ipv4Addr::from(0x0a00_0000 + i), 6881))
    }

    fn live(range: std::ops::Range<u32>) -> Vec<(SocketAddr, PexFlags)> {
        range.map(|i| (addr(i), PexFlags::CONNECTABLE)).collect()
    }

    fn recipient() -> SocketAddr {
        addr(9999)
    }

    fn sorted(mut v: Vec<SocketAddr>) -> Vec<SocketAddr> {
        v.sort();
        v
    }

    fn added_addrs(m: &PexMsg) -> Vec<SocketAddr> {
        sorted(m.added.iter().map(|(a, _)| *a).collect())
    }

    #[tokio::test(start_paused = true)]
    async fn first_message_lists_connected_peers() {
        let mut p = PexPeer::default();
        let live = vec![
            (addr(1), PexFlags::CONNECTABLE | PexFlags::SEED),
            (addr(2), PexFlags::CONNECTABLE | PexFlags::ENCRYPTION),
        ];
        let msg = p
            .build(&live, recipient(), Instant::now(), INTERVAL)
            .expect("first message");
        assert_eq!(msg.added, live);
        assert!(msg.dropped.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn diff_lists_added_and_dropped() {
        let mut p = PexPeer::default();
        p.build(&live(0..3), recipient(), Instant::now(), INTERVAL)
            .expect("first");
        tokio::time::advance(INTERVAL).await;
        let msg = p
            .build(&live(1..5), recipient(), Instant::now(), INTERVAL)
            .expect("diff");
        assert_eq!(added_addrs(&msg), vec![addr(3), addr(4)]);
        assert_eq!(msg.dropped, vec![addr(0)]);
        // Nothing changed since: no message.
        tokio::time::advance(INTERVAL).await;
        assert_eq!(
            p.build(&live(1..5), recipient(), Instant::now(), INTERVAL),
            None
        );
    }

    #[tokio::test(start_paused = true)]
    async fn never_sends_before_interval() {
        let mut p = PexPeer::default();
        p.build(&live(0..2), recipient(), Instant::now(), INTERVAL)
            .expect("first");
        tokio::time::advance(INTERVAL - Duration::from_millis(1)).await;
        assert_eq!(
            p.build(&live(0..4), recipient(), Instant::now(), INTERVAL),
            None
        );
        tokio::time::advance(Duration::from_millis(1)).await;
        let msg = p
            .build(&live(0..4), recipient(), Instant::now(), INTERVAL)
            .expect("after the interval");
        assert_eq!(added_addrs(&msg), vec![addr(2), addr(3)]);
    }

    #[tokio::test(start_paused = true)]
    async fn caps_added_and_dropped_at_50() {
        let mut p = PexPeer::default();
        let first = p
            .build(&live(0..120), recipient(), Instant::now(), INTERVAL)
            .expect("first");
        assert_eq!(first.added.len(), MAX_PEX_ADDED);
        // The rest follows in later messages, 50 at a time.
        tokio::time::advance(INTERVAL).await;
        let second = p
            .build(&live(0..120), recipient(), Instant::now(), INTERVAL)
            .expect("second");
        assert_eq!(second.added.len(), MAX_PEX_ADDED);
        assert!(second.dropped.is_empty());
        // Everything sent so far (100) is gone at once: 50 dropped per message.
        tokio::time::advance(INTERVAL).await;
        let third = p
            .build(&[], recipient(), Instant::now(), INTERVAL)
            .expect("third");
        assert!(third.added.is_empty());
        assert_eq!(third.dropped.len(), MAX_PEX_DROPPED);
        tokio::time::advance(INTERVAL).await;
        let fourth = p
            .build(&[], recipient(), Instant::now(), INTERVAL)
            .expect("fourth");
        assert_eq!(fourth.dropped.len(), MAX_PEX_DROPPED);
        let all = sorted([third.dropped, fourth.dropped].concat());
        assert_eq!(all, sorted((0..100).map(addr).collect()));
        tokio::time::advance(INTERVAL).await;
        assert_eq!(p.build(&[], recipient(), Instant::now(), INTERVAL), None);
    }

    #[tokio::test(start_paused = true)]
    async fn excludes_recipient() {
        let mut p = PexPeer::default();
        let me = addr(1);
        let msg = p
            .build(&live(0..3), me, Instant::now(), INTERVAL)
            .expect("first");
        assert_eq!(added_addrs(&msg), vec![addr(0), addr(2)]);
        // Only the recipient connected: nothing to tell it.
        let mut q = PexPeer::default();
        assert_eq!(q.build(&live(1..2), me, Instant::now(), INTERVAL), None);
    }

    #[tokio::test(start_paused = true)]
    async fn receive_gate_drops_bursts() {
        let gap = INTERVAL * 3 / 4;
        let mut p = PexPeer::default();
        assert!(p.accept(Instant::now(), gap));
        for _ in 0..20 {
            assert!(!p.accept(Instant::now(), gap));
        }
        tokio::time::advance(gap - Duration::from_millis(1)).await;
        assert!(!p.accept(Instant::now(), gap));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(p.accept(Instant::now(), gap));
        assert!(!p.accept(Instant::now(), gap));
    }

    #[test]
    fn incoming_peer_without_listen_port_is_not_shared() {
        let (tx, _rx) = mpsc::channel(1);
        let mut p = Peer::new(addr(1), tx, 0, 0);
        assert_eq!(connectable_addr(&p), None);
        p.listen_port = Some(7000);
        assert_eq!(
            connectable_addr(&p),
            Some(SocketAddr::new(addr(1).ip(), 7000))
        );
        p.outgoing = true;
        assert_eq!(connectable_addr(&p), Some(addr(1)));
    }
}
