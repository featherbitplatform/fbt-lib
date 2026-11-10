//! The swarm side of a torrent: which addresses to connect to, connecting
//! and accepting within the limits, sending to peers and closing them.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use fbt_core::{Bitfield, BlockRef, PeerId};
use fbt_wire::{CryptoMethods, ExtHandshake};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::Instant;
use tracing::debug;

use super::actor::Actor;
use crate::peer::DisconnectReason;
use crate::peer::conn::{MAX_ALLOWED_FAST, PeerCtx, PeerStream, spawn_incoming, spawn_outgoing};
use crate::peer::ext::ExtConfig;
use crate::peer::messages::PeerCommand;
use crate::picker::{PeerKey, PeerPieces};
use crate::rate::RateMeter;
use crate::session::{release, try_reserve};
use crate::settings::EncryptionPolicy;

/// Connection attempts per address before it is given up.
const MAX_ATTEMPTS: u32 = 3;
/// Most addresses a torrent remembers for connecting; beyond it the oldest
/// address not in use is forgotten.
const MAX_KNOWN: usize = 1000;
/// Most never-retry addresses kept; the oldest is forgotten beyond it.
/// Also bounds the plaintext-only addresses.
const MAX_NEVER: usize = 1000;

/// The addresses a torrent connects to (from `add_peer` and trackers),
/// reconnected on resume. At most [`MAX_KNOWN`]; when full, the oldest
/// address not in use makes room for a new one.
#[derive(Debug, Default)]
pub(super) struct Known {
    set: HashSet<SocketAddr>,
    /// `set` in insertion order, oldest first.
    order: VecDeque<SocketAddr>,
}

impl Known {
    /// Remembers `addr`. When full, forgets the oldest address for which
    /// `in_use` is false and returns it as the second value. Returns
    /// whether `addr` is known now (false only when every address is in
    /// use).
    pub(super) fn remember(
        &mut self,
        addr: SocketAddr,
        in_use: impl Fn(&SocketAddr) -> bool,
    ) -> (bool, Option<SocketAddr>) {
        if self.set.contains(&addr) {
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
        self.set.insert(addr);
        self.order.push_back(addr);
        (true, evicted)
    }

    pub(super) fn remove(&mut self, addr: &SocketAddr) {
        if self.set.remove(addr) {
            self.order.retain(|a| a != addr);
        }
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

/// Which of two connections to the same peer id survives: when they differ
/// in direction, the one initiated by the side with the lower peer id
/// (bytewise); in the same direction, the older one. Both ends compute the
/// same answer, so a simultaneous dial leaves exactly one connection.
/// Returns whether the new connection is kept (and the old one closed).
pub(super) fn keep_new_connection(
    our_id: &PeerId,
    their_id: &PeerId,
    new_outgoing: bool,
    old_outgoing: bool,
) -> bool {
    if new_outgoing == old_outgoing || our_id == their_id {
        return false;
    }
    // Keep the connection we initiated exactly when our id is the lower.
    new_outgoing == (our_id.0 < their_id.0)
}

/// What a failed attempt leads to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AfterFailure {
    /// Tried again after the delay.
    Retry,
    /// Attempts used up (or a never-retry address): forget it.
    GiveUp,
}

/// Outgoing connection retries: failed addresses come back after `delay`
/// (`Settings::peer_retry_delay`), at most [`MAX_ATTEMPTS`] attempts in all;
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
        }
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
        if self.never.contains(&addr) {
            return AfterFailure::GiveUp;
        }
        let n = self.attempts.entry(addr).or_insert(0);
        *n = n.saturating_add(1);
        if *n < MAX_ATTEMPTS {
            // A huge delay means "not in this process's lifetime".
            let at = now
                .checked_add(self.delay)
                .unwrap_or_else(|| now + Duration::from_secs(86_400 * 365));
            self.pending.insert(addr, at);
            AfterFailure::Retry
        } else {
            self.attempts.remove(&addr);
            AfterFailure::GiveUp
        }
    }

    /// The handshake succeeded: the attempts start over.
    pub(super) fn connected(&mut self, addr: &SocketAddr) {
        self.attempts.remove(addr);
        self.pending.remove(addr);
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

/// One peer connection, from spawn until its `Disconnected` event.
#[derive(Debug)]
pub(super) struct Peer {
    pub addr: SocketAddr,
    /// `None` once we closed it; it stays until `Disconnected` arrives.
    pub tx: Option<mpsc::Sender<PeerCommand>>,
    /// Picker generation the peer belongs to.
    pub generation: u64,
    /// We dialled it.
    pub outgoing: bool,
    /// Outgoing and still holding a half-open slot.
    pub connecting: bool,
    pub connected: bool,
    /// Our pieces as the peer task will announce them (its initial
    /// bitfield), until `Connected`; pieces finished since then are
    /// announced on connect, since `Have`s go to connected peers only.
    pub have_sent: Option<Bitfield>,
    pub peer_id: Option<PeerId>,
    /// `HaveAll` or a full bitfield.
    pub seed: bool,
    pub bits: Bitfield,
    pub choked_us: bool,
    pub am_interested: bool,
    pub peer_interested: bool,
    pub am_choking: bool,
    /// Pieces the peer lets us request while it chokes us (at most
    /// [`MAX_ALLOWED_FAST`]).
    pub allowed_fast: HashSet<u32>,
    /// The `allowed_fast` pieces the peer has, as the picker wants them;
    /// allocated with the first allowed-fast piece and kept up to date
    /// instead of being rebuilt on every fill.
    pub fast_has: Option<Bitfield>,
    /// Our requests the peer task holds.
    pub requested: HashSet<BlockRef>,
    /// Why we closed it, reported instead of `Shutdown` on `Disconnected`.
    pub close_reason: Option<DisconnectReason>,
    /// Blocks the peer asked for (`WantBlocks`) and we have not handed out.
    pub demand: usize,
    /// Payload the peer uploads to us.
    pub from_rate: RateMeter,
    /// Payload we upload to the peer.
    pub to_rate: RateMeter,
    /// The peer's latest BEP 10 extension handshake (untrusted values).
    pub ext: Option<Box<ExtHandshake>>,
    /// The listen port from that handshake (`p`).
    pub listen_port: Option<u16>,
    /// The connection is RC4-encrypted (for PEX flags).
    pub encrypted: bool,
}

impl Peer {
    pub(super) fn new(
        addr: SocketAddr,
        tx: mpsc::Sender<PeerCommand>,
        generation: u64,
        num_pieces: u32,
    ) -> Self {
        Peer {
            addr,
            tx: Some(tx),
            generation,
            outgoing: false,
            connecting: false,
            connected: false,
            have_sent: None,
            peer_id: None,
            seed: false,
            bits: Bitfield::new(num_pieces),
            choked_us: true,
            am_interested: false,
            peer_interested: false,
            am_choking: true,
            allowed_fast: HashSet::new(),
            fast_has: None,
            requested: HashSet::new(),
            close_reason: None,
            demand: 0,
            from_rate: RateMeter::new(),
            to_rate: RateMeter::new(),
            ext: None,
            listen_port: None,
            encrypted: false,
        }
    }

    pub(super) fn pieces(&self) -> PeerPieces<'_> {
        if self.seed {
            PeerPieces::All
        } else {
            PeerPieces::Some(&self.bits)
        }
    }

    pub(super) fn has(&self, piece: u32) -> bool {
        self.seed || self.bits.get(piece)
    }

    pub(super) fn is_seed(&self) -> bool {
        self.seed || self.bits.is_full()
    }

    /// Adds an allowed-fast piece; returns whether it is new and the peer
    /// has it (only then can it be requested now).
    pub(super) fn allow_fast(&mut self, piece: u32) -> bool {
        if self.allowed_fast.len() >= MAX_ALLOWED_FAST || !self.allowed_fast.insert(piece) {
            return false;
        }
        self.sync_fast(piece)
    }

    /// Mirrors `piece` into `fast_has` if it is allowed-fast and the peer
    /// has it; returns whether it is set there.
    pub(super) fn sync_fast(&mut self, piece: u32) -> bool {
        if !self.allowed_fast.contains(&piece) || !self.has(piece) {
            return false;
        }
        let n = self.bits.len();
        self.fast_has
            .get_or_insert_with(|| Bitfield::new(n))
            .set(piece);
        true
    }

    /// Some allowed-fast piece can be requested.
    pub(super) fn can_request_fast(&self) -> bool {
        self.fast_has.is_some() && self.allowed_fast.iter().any(|&i| self.has(i))
    }

    /// Connected, not closed, and we do not choke it.
    pub(super) fn unchoked(&self) -> bool {
        self.connected && self.tx.is_some() && !self.am_choking
    }
}

impl Actor {
    // ----- connections ----------------------------------------------------------

    /// The context of a new peer task. Without metadata the task runs in
    /// no-metadata mode (no layout, an empty `have`).
    pub(super) fn ctx(&self, key: PeerKey) -> PeerCtx {
        let l = self.loaded.as_ref();
        let have = l.map_or_else(
            || Bitfield::new(0),
            |l| {
                l.picker.as_ref().map_or_else(
                    || Bitfield::new(l.layout.num_pieces()),
                    |p| p.have().clone(),
                )
            },
        );
        PeerCtx {
            key,
            info_hash: self.ih.wire_hash(),
            our_id: self.shared.peer_id,
            layout: l.map(|l| l.layout),
            have,
            ext: self.ext_config(),
            settings: Arc::clone(&self.shared.settings),
            events: self.peer_tx.clone(),
            encrypted: false,
        }
    }

    /// What new connections advertise. A torrent without metadata may be
    /// private; until it is known, PEX is advertised (the magnet exception).
    fn ext_config(&self) -> ExtConfig {
        let meta = self.loaded.as_ref().map(|l| &l.meta);
        ExtConfig {
            listen_port: self.shared.listen_port,
            metadata_size: meta.and_then(|m| u32::try_from(m.raw_info().len()).ok()),
            pex: !meta.is_some_and(|m| m.info.private),
            // Set once the session runs a DHT.
            dht_port: None,
            reqq: u32::try_from(self.shared.settings.max_incoming_requests).unwrap_or(u32::MAX),
        }
    }

    pub(super) fn new_key(&mut self) -> PeerKey {
        self.next_key = self.next_key.wrapping_add(1);
        PeerKey(self.next_key)
    }

    pub(super) fn requeue_known(&mut self) {
        if !self.active() {
            return;
        }
        let known: Vec<SocketAddr> = self.known.iter().copied().collect();
        for addr in known {
            self.queue_addr(addr);
        }
    }

    /// Queues `addr` for connecting unless it is ourselves, waiting for a
    /// retry, already connected or already queued.
    pub(super) fn queue_addr(&mut self, addr: SocketAddr) {
        if self.retries.is_never(&addr) || self.retries.is_pending(&addr) {
            return;
        }
        if !self.addr_busy(&addr) && !self.candidates.contains(&addr) {
            self.candidates.push_back(addr);
        }
    }

    /// A live connection (connecting or connected) to `addr` exists.
    fn addr_busy(&self, addr: &SocketAddr) -> bool {
        self.peers
            .values()
            .any(|p| p.addr == *addr && p.tx.is_some())
    }

    /// Remembers `addr` for connecting; when the table is full, the oldest
    /// address that is neither connected nor waiting for a retry is
    /// forgotten (and unqueued). Returns whether it is known now.
    pub(super) fn remember(&mut self, addr: SocketAddr) -> bool {
        let peers = &self.peers;
        let retries = &self.retries;
        let (known, evicted) = self.known.remember(addr, |a| {
            retries.is_pending(a) || peers.values().any(|p| p.addr == *a && p.tx.is_some())
        });
        if let Some(old) = evicted {
            self.candidates.retain(|a| *a != old);
        }
        known
    }

    /// Queues the addresses whose retry time has come.
    pub(super) fn queue_retries(&mut self) {
        if !self.active() {
            return;
        }
        for addr in self.retries.due(Instant::now()) {
            self.queue_addr(addr);
        }
    }

    /// Starts outgoing connections while every limit allows.
    pub(super) fn try_connect(&mut self) {
        let s = Arc::clone(&self.shared.settings);
        while self.active() && !self.candidates.is_empty() {
            if self.peers.len() >= s.max_peers_per_torrent {
                break;
            }
            if !try_reserve(&self.shared.global_peers, s.max_peers_global) {
                break;
            }
            if !try_reserve(&self.shared.half_open, s.max_half_open) {
                release(&self.shared.global_peers);
                break;
            }
            let Some(addr) = self.candidates.pop_front() else {
                release(&self.shared.half_open);
                release(&self.shared.global_peers);
                break;
            };
            // It may have connected (or connected to us) since it was queued.
            if self.addr_busy(&addr) {
                release(&self.shared.half_open);
                release(&self.shared.global_peers);
                continue;
            }
            let key = self.new_key();
            let ctx = self.ctx(key);
            let have = ctx.have.clone();
            let num_pieces = ctx.have.len();
            let tx = spawn_outgoing(addr, ctx, self.mse_provide(&addr));
            let mut peer = Peer::new(addr, tx, self.generation, num_pieces);
            peer.outgoing = true;
            peer.connecting = true;
            peer.have_sent = Some(have);
            self.peers.insert(key, peer);
        }
    }

    /// The MSE `crypto_provide` for dialling `addr`, `None` for a plain
    /// BitTorrent handshake.
    fn mse_provide(&self, addr: &SocketAddr) -> Option<CryptoMethods> {
        match self.shared.settings.encryption {
            EncryptionPolicy::Forced => Some(CryptoMethods::RC4),
            EncryptionPolicy::Preferred if !self.retries.is_plaintext_only(addr) => {
                Some(CryptoMethods::RC4 | CryptoMethods::PLAINTEXT)
            }
            EncryptionPolicy::Preferred | EncryptionPolicy::Disabled => None,
        }
    }

    pub(super) fn on_incoming(
        &mut self,
        stream: PeerStream,
        hs: fbt_wire::Handshake,
        addr: SocketAddr,
    ) {
        let s = &self.shared.settings;
        if !self.active() || self.peers.len() >= s.max_peers_per_torrent {
            debug!(%addr, "torrent inactive or full; dropping incoming connection");
            return;
        }
        if !try_reserve(&self.shared.global_peers, s.max_peers_global) {
            debug!(%addr, "global peer limit reached; dropping incoming connection");
            return;
        }
        let key = self.new_key();
        let mut ctx = self.ctx(key);
        ctx.encrypted = stream.is_encrypted();
        let have = ctx.have.clone();
        let num_pieces = ctx.have.len();
        let tx = spawn_incoming(stream, hs, addr, ctx);
        let mut peer = Peer::new(addr, tx, self.generation, num_pieces);
        peer.have_sent = Some(have);
        self.peers.insert(key, peer);
    }

    /// Sends a command without waiting. A full queue closes the peer as
    /// stalled. Returns whether the command was queued.
    pub(super) fn send(&mut self, key: PeerKey, cmd: PeerCommand) -> bool {
        let Some(tx) = self.peers.get(&key).and_then(|p| p.tx.as_ref()) else {
            return false;
        };
        match tx.try_send(cmd) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                debug!(key = key.0, "peer command queue full; closing stalled peer");
                self.close_peer(key);
                false
            }
            Err(TrySendError::Closed(_)) => {
                if let Some(p) = self.peers.get_mut(&key) {
                    p.tx = None;
                }
                false
            }
        }
    }

    /// Closes a peer: it stays until its `Disconnected` event, but its
    /// requests return to the picker now.
    pub(super) fn close_peer(&mut self, key: PeerKey) {
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        if let Some(tx) = p.tx.take() {
            // If the queue is full, dropping the sender ends the task too.
            let _ = tx.try_send(PeerCommand::Close);
        }
        p.demand = 0;
        p.requested.clear();
        if p.generation == self.generation
            && let Some(picker) = self.loaded.as_mut().and_then(|l| l.picker.as_mut())
        {
            picker.release_peer(key);
        }
    }

    pub(super) fn close_all(&mut self) {
        let keys: Vec<PeerKey> = self.peers.keys().copied().collect();
        for key in keys {
            self.close_peer(key);
        }
        self.candidates.clear();
    }

    /// Bookkeeping for a finished handshake: slots, retries and the
    /// duplicate peer-id guard. Returns whether the connection stays; the
    /// caller then catches it up on our pieces.
    pub(super) fn on_connected(
        &mut self,
        key: PeerKey,
        peer_id: PeerId,
        addr: SocketAddr,
        encrypted: bool,
    ) -> bool {
        let duplicate = self
            .peers
            .iter()
            .find(|(k, o)| {
                **k != key && o.connected && o.tx.is_some() && o.peer_id == Some(peer_id)
            })
            .map(|(k, o)| (*k, o.outgoing));
        let Some(p) = self.peers.get_mut(&key) else {
            return false;
        };
        if p.connecting {
            p.connecting = false;
            release(&self.shared.half_open);
        }
        p.connected = true;
        p.peer_id = Some(peer_id);
        p.encrypted = encrypted;
        if p.outgoing {
            self.retries.connected(&p.addr);
        }
        let Some((old, old_outgoing)) = duplicate else {
            return true;
        };
        // Already connected to this peer: both dialled each other, or it
        // reconnected. Both ends agree on which connection stays.
        let keep_new =
            keep_new_connection(&self.shared.peer_id, &peer_id, p.outgoing, old_outgoing);
        let (loser, survives) = if keep_new { (old, true) } else { (key, false) };
        debug!(%addr, keep_new, "duplicate peer id; closing one connection");
        if let Some(l) = self.peers.get_mut(&loser) {
            l.close_reason = Some(DisconnectReason::Duplicate);
        }
        self.close_peer(loser);
        survives
    }

    /// Bookkeeping for a closed connection: slots, retries and the known
    /// addresses. Returns the peer and the reason to report, or `None` for
    /// an unknown key.
    pub(super) fn on_disconnected(
        &mut self,
        key: PeerKey,
        reason: DisconnectReason,
    ) -> Option<(Peer, DisconnectReason)> {
        let mut p = self.peers.remove(&key)?;
        let reason = match p.close_reason.take() {
            Some(ours) if reason == DisconnectReason::Shutdown => ours,
            _ => reason,
        };
        if p.connecting {
            release(&self.shared.half_open);
        }
        release(&self.shared.global_peers);
        if reason == DisconnectReason::SelfConnection {
            self.retries.never(p.addr);
            self.known.remove(&p.addr);
            self.candidates.retain(|a| *a != p.addr);
        } else if p.outgoing
            && !p.connected
            && matches!(reason, DisconnectReason::Encryption(_))
            && self.shared.settings.encryption == EncryptionPolicy::Preferred
            && self.retries.encryption_failed(p.addr)
        {
            // The first failed MSE handshake to this address: dial it again
            // at once, in plaintext, without counting an attempt.
            debug!(addr = %p.addr, "MSE handshake failed; retrying in plaintext");
            self.queue_addr(p.addr);
        } else if p.outgoing && !p.connected && reason != DisconnectReason::Shutdown {
            // The connection or handshake failed (we did not close it).
            if self.retries.failed(p.addr, Instant::now()) == AfterFailure::GiveUp {
                // Forgotten: `requeue_known` must not dial it again.
                self.known.remove(&p.addr);
                self.candidates.retain(|a| *a != p.addr);
            }
        }
        match &reason {
            DisconnectReason::Protocol(msg) => {
                debug!(addr = %p.addr, %msg, "peer broke the protocol");
            }
            r => debug!(addr = %p.addr, reason = ?r, "peer disconnected"),
        }
        Some((p, reason))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tie_break_keeps_lower_id_initiator() {
        let lo = PeerId([1; 20]);
        let hi = PeerId([2; 20]);
        // we are lo: keep our outgoing, drop their incoming duplicate
        assert!(keep_new_connection(&lo, &hi, true, false));
        assert!(!keep_new_connection(&lo, &hi, false, true));
        // we are hi: keep their (incoming) connection
        assert!(keep_new_connection(&hi, &lo, false, true));
        assert!(!keep_new_connection(&hi, &lo, true, false));
        // same direction: older wins
        assert!(!keep_new_connection(&lo, &hi, true, true));
    }

    const DELAY: Duration = Duration::from_secs(60);

    fn addr(i: u32) -> SocketAddr {
        SocketAddr::from((std::net::Ipv4Addr::from(0x0a00_0000 + i), 6881))
    }

    #[tokio::test(start_paused = true)]
    async fn failed_connects_retry_after_the_delay_three_attempts_in_all() {
        let a = addr(1);
        let mut r = Retries::new(DELAY);
        let mut attempts = 1;
        while r.failed(a, Instant::now()) == AfterFailure::Retry {
            assert!(r.is_pending(&a));
            tokio::time::advance(DELAY - Duration::from_secs(1)).await;
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
        r.connected(&a);
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
    fn address_maps_are_bounded() {
        let mut known = Known::default();
        for i in 0..MAX_KNOWN as u32 {
            assert_eq!(known.remember(addr(i), |_| false), (true, None));
        }
        // Full and everything in use: new addresses are ignored, known ones
        // still answer true.
        assert_eq!(known.remember(addr(5000), |_| true), (false, None));
        assert_eq!(known.remember(addr(0), |_| true), (true, None));
        assert_eq!(known.len(), MAX_KNOWN);

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

    #[test]
    fn full_known_table_evicts_the_oldest_idle_address() {
        let mut known = Known::default();
        for i in 0..MAX_KNOWN as u32 {
            known.remember(addr(i), |_| false);
        }
        // Addresses 0..10 are connected or waiting for a retry: the oldest
        // other one (10) makes room.
        let busy = |a: &SocketAddr| (0..10).any(|i| addr(i) == *a);
        assert_eq!(known.remember(addr(5000), busy), (true, Some(addr(10))));
        assert_eq!(known.len(), MAX_KNOWN);
        assert!(known.iter().any(|a| *a == addr(5000)));
        assert!(!known.iter().any(|a| *a == addr(10)));
        // The next one evicts 11; a removed address frees a slot outright.
        assert_eq!(known.remember(addr(5001), busy), (true, Some(addr(11))));
        known.remove(&addr(3));
        assert_eq!(known.remember(addr(5002), busy), (true, None));
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
