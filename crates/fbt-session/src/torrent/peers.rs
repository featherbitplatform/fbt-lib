//! Peer connections of a torrent: connecting and accepting within the
//! limits, peer events, interest and handing out block requests.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use fbt_core::{Bitfield, BlockRef, PeerId};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::Instant;
use tracing::debug;

use super::actor::Actor;
use crate::choker::PeerChokeInfo;
use crate::event::Event;
use crate::peer::DisconnectReason;
use crate::peer::conn::{MAX_ALLOWED_FAST, PeerCtx, spawn_incoming, spawn_outgoing};
use crate::peer::messages::{PeerCommand, PeerEvent, PeerPiecesOwned};
use crate::picker::{PeerKey, PeerPieces, PiecePicker};
use crate::rate::RateMeter;
use crate::session::{release, try_reserve};

/// Connection attempts per address before it is given up.
const MAX_ATTEMPTS: u32 = 3;
/// Most addresses a torrent remembers for connecting; beyond it the oldest
/// address not in use is forgotten.
const MAX_KNOWN: usize = 1000;
/// Most never-retry addresses kept; the oldest is forgotten beyond it.
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
}

impl Retries {
    pub(super) fn new(delay: Duration) -> Self {
        Retries {
            delay,
            attempts: HashMap::new(),
            pending: HashMap::new(),
            never: HashSet::new(),
            never_order: VecDeque::new(),
        }
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
    fn can_request_fast(&self) -> bool {
        self.fast_has.is_some() && self.allowed_fast.iter().any(|&i| self.has(i))
    }

    /// Connected, not closed, and we do not choke it.
    pub(super) fn unchoked(&self) -> bool {
        self.connected && self.tx.is_some() && !self.am_choking
    }
}

impl Actor {
    // ----- connections ----------------------------------------------------------

    pub(super) fn ctx(&self, key: PeerKey) -> PeerCtx {
        PeerCtx {
            key,
            info_hash: self.ih.wire_hash(),
            our_id: self.shared.peer_id,
            layout: self.layout,
            have: self.picker.as_ref().map_or_else(
                || Bitfield::new(self.layout.num_pieces()),
                |p| p.have().clone(),
            ),
            settings: Arc::clone(&self.shared.settings),
            events: self.peer_tx.clone(),
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
            let tx = spawn_outgoing(addr, ctx);
            let mut peer = Peer::new(addr, tx, self.generation, self.layout.num_pieces());
            peer.outgoing = true;
            peer.connecting = true;
            peer.have_sent = Some(have);
            self.peers.insert(key, peer);
        }
    }

    pub(super) fn on_incoming(
        &mut self,
        stream: tokio::net::TcpStream,
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
        let ctx = self.ctx(key);
        let have = ctx.have.clone();
        let tx = spawn_incoming(stream, hs, addr, ctx);
        let mut peer = Peer::new(addr, tx, self.generation, self.layout.num_pieces());
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
            && let Some(picker) = self.picker.as_mut()
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

    // ----- peer events --------------------------------------------------------

    pub(super) fn on_peer_event(&mut self, ev: PeerEvent) {
        match ev {
            PeerEvent::Connected {
                key, peer_id, addr, ..
            } => self.on_connected(key, peer_id, addr),
            PeerEvent::Pieces { key, pieces } => self.on_pieces(key, pieces),
            PeerEvent::Have { key, piece } => self.on_have(key, piece),
            PeerEvent::Choked { key } => {
                if let Some(p) = self.peers.get_mut(&key) {
                    p.choked_us = true;
                    p.demand = 0;
                }
            }
            PeerEvent::Unchoked { key } => {
                if let Some(p) = self.peers.get_mut(&key) {
                    p.choked_us = false;
                }
            }
            PeerEvent::Interested { key } => self.on_interested(key, true),
            PeerEvent::NotInterested { key } => self.on_interested(key, false),
            PeerEvent::Block { key, block, data } => self.on_block(key, block, data),
            PeerEvent::Rejected { key, block } => self.on_rejected(key, block),
            PeerEvent::AllowedFast { key, piece } => {
                // Only a piece the peer has can be requested now; one it
                // lacks becomes requestable with its `Have`.
                if self
                    .peers
                    .get_mut(&key)
                    .is_some_and(|p| p.allow_fast(piece))
                {
                    self.fill(key);
                }
            }
            PeerEvent::WantBlocks { key, count } => {
                if let Some(p) = self.peers.get_mut(&key) {
                    p.demand = count;
                }
                self.fill(key);
            }
            PeerEvent::Request { key, block } => self.on_request(key, block),
            // The peer task drops the cancelled request itself; an answer
            // already being read is discarded there.
            PeerEvent::Cancel { .. } => {}
            PeerEvent::Wasted { bytes, .. } => {
                self.counters.wasted = self.counters.wasted.saturating_add(bytes);
            }
            PeerEvent::Disconnected {
                key,
                reason,
                outstanding,
            } => self.on_disconnected(key, reason, &outstanding),
        }
    }

    pub(super) fn on_connected(&mut self, key: PeerKey, peer_id: PeerId, addr: SocketAddr) {
        let duplicate = self
            .peers
            .iter()
            .any(|(k, o)| *k != key && o.connected && o.tx.is_some() && o.peer_id == Some(peer_id));
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        if p.connecting {
            p.connecting = false;
            release(&self.shared.half_open);
        }
        p.connected = true;
        p.peer_id = Some(peer_id);
        if p.outgoing {
            self.retries.connected(&p.addr);
        }
        if duplicate {
            // Already connected to this peer (both dialled each other, or it
            // reconnected): keep the older connection.
            debug!(%addr, "duplicate peer id; closing the newer connection");
            p.close_reason = Some(DisconnectReason::Duplicate);
            self.close_peer(key);
            return;
        }
        // Pieces finished while it connected were skipped by `on_written`.
        let snapshot = p.have_sent.take();
        if let (Some(sent), Some(picker)) = (snapshot, self.picker.as_ref()) {
            let missed: Vec<u32> = picker
                .have()
                .iter_ones()
                .filter(|&i| !sent.get(i))
                .collect();
            if !missed.is_empty() {
                self.send(key, PeerCommand::Haves(missed));
            }
        }
        debug!(%addr, "peer connected");
        self.emit(Event::PeerConnected {
            torrent: self.ih,
            addr,
        });
    }

    pub(super) fn on_pieces(&mut self, key: PeerKey, pieces: PeerPiecesOwned) {
        let generation = self.generation;
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        match pieces {
            PeerPiecesOwned::All => p.seed = true,
            PeerPiecesOwned::None => {}
            PeerPiecesOwned::Bits(bits) => p.bits = bits,
        }
        let fast: Vec<u32> = p.allowed_fast.iter().copied().collect();
        for piece in fast {
            p.sync_fast(piece);
        }
        if p.generation == generation
            && let Some(picker) = self.picker.as_mut()
        {
            picker.add_peer_pieces(p.pieces());
        }
        self.update_interest(key);
        self.fill(key);
    }

    pub(super) fn on_have(&mut self, key: PeerKey, piece: u32) {
        let generation = self.generation;
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        if p.has(piece) {
            return;
        }
        p.bits.set(piece);
        p.sync_fast(piece);
        if p.generation == generation
            && let Some(picker) = self.picker.as_mut()
        {
            picker.peer_has(piece);
        }
        let interested = p.am_interested;
        if !interested && self.wants_piece(piece) {
            self.update_interest(key);
        }
        self.fill(key);
    }

    pub(super) fn on_interested(&mut self, key: PeerKey, on: bool) {
        let active = self.active();
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        p.peer_interested = on;
        if !active {
            return;
        }
        // The choker runs every `rechoke_interval`; in between a newly
        // interested peer takes a free slot at once and a peer losing
        // interest gives its slot back.
        if on {
            let unchoked = self.peers.values().filter(|p| p.unchoked()).count();
            if unchoked < self.shared.settings.unchoke_slots {
                self.set_choke(key, false);
            }
        } else {
            self.set_choke(key, true);
        }
    }

    /// Chokes or unchokes a peer, sending only on change.
    pub(super) fn set_choke(&mut self, key: PeerKey, choke: bool) {
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        if p.tx.is_none() || p.am_choking == choke {
            return;
        }
        p.am_choking = choke;
        let cmd = if choke {
            PeerCommand::Choke
        } else {
            PeerCommand::Unchoke
        };
        self.send(key, cmd);
    }

    /// Runs the choker over the connected peers: rate from each peer while
    /// downloading, rate to it while seeding.
    pub(super) fn rechoke(&mut self) {
        if !self.active() {
            return;
        }
        let now = Instant::now();
        let infos: Vec<PeerChokeInfo> = self
            .peers
            .iter()
            .filter(|(_, p)| p.connected && p.tx.is_some())
            .map(|(&key, p)| PeerChokeInfo {
                key,
                interested: p.peer_interested,
                rate_from_peer: p.from_rate.rate(now),
                rate_to_peer: p.to_rate.rate(now),
            })
            .collect();
        let seeding = self.picker.as_ref().is_some_and(PiecePicker::is_finished);
        let unchoke = self.choker.rechoke(&infos, seeding, &mut self.rng);
        for info in infos {
            self.set_choke(info.key, !unchoke.contains(&info.key));
        }
    }

    pub(super) fn on_rejected(&mut self, key: PeerKey, block: BlockRef) {
        let generation = self.generation;
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        if p.requested.remove(&block)
            && p.generation == generation
            && let Some(picker) = self.picker.as_mut()
        {
            picker.release(key, block);
        }
        // The peer asks again itself; others may take the block now.
        self.top_up_all(Some(key));
    }

    pub(super) fn on_disconnected(
        &mut self,
        key: PeerKey,
        reason: DisconnectReason,
        outstanding: &[BlockRef],
    ) {
        let Some(mut p) = self.peers.remove(&key) else {
            return;
        };
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
        if p.generation == self.generation
            && let Some(picker) = self.picker.as_mut()
        {
            for b in outstanding {
                picker.release(key, *b);
            }
            picker.release_peer(key);
            picker.remove_peer_pieces(p.pieces());
        }
        self.emit(Event::PeerDisconnected {
            torrent: self.ih,
            addr: p.addr,
            reason,
        });
        self.top_up_all(None);
        self.try_connect();
    }

    // ----- interest and requests ----------------------------------------------

    /// We want `piece`: wanted priority and not verified yet.
    pub(super) fn wants_piece(&self, piece: u32) -> bool {
        self.piece_prio.get(piece as usize).is_some_and(|&p| p > 0)
            && self.picker.as_ref().is_some_and(|pk| !pk.have().get(piece))
    }

    /// Sends `Interested` exactly when the peer has a piece we want.
    pub(super) fn update_interest(&mut self, key: PeerKey) {
        let Some(p) = self.peers.get(&key) else {
            return;
        };
        if p.tx.is_none() {
            return;
        }
        let want = self.active()
            && if p.seed {
                self.picker.as_ref().is_some_and(|pk| !pk.is_finished())
            } else {
                p.bits.iter_ones().any(|i| self.wants_piece(i))
            };
        if want != p.am_interested {
            if let Some(p) = self.peers.get_mut(&key) {
                p.am_interested = want;
            }
            self.send(key, PeerCommand::Interested(want));
        }
    }

    /// Hands blocks to a peer up to its unmet demand.
    pub(super) fn fill(&mut self, key: PeerKey) {
        if !self.active() {
            return;
        }
        let max = self.shared.settings.max_outstanding_requests;
        let generation = self.generation;
        let cap = partial_cap(self.layout.piece_len());
        let (Some(picker), Some(p)) = (self.picker.as_mut(), self.peers.get(&key)) else {
            return;
        };
        let evicted = evict_orphans(picker, cap, &mut self.bufs);
        if evicted > 0 {
            debug!(evicted, "dropped partial pieces no connected peer has");
        }
        if !p.connected || p.tx.is_none() || p.generation != generation {
            return;
        }
        let room = max.saturating_sub(p.requested.len()).min(p.demand);
        if room == 0 {
            return;
        }
        let blocks = if p.choked_us {
            let (true, Some(fast)) = (p.can_request_fast(), p.fast_has.as_ref()) else {
                return;
            };
            pick_capped(
                picker,
                key,
                PeerPieces::Some(fast),
                room,
                cap,
                &mut self.rng,
            )
        } else {
            pick_capped(picker, key, p.pieces(), room, cap, &mut self.rng)
        };
        if blocks.is_empty() {
            return;
        }
        if let Some(p) = self.peers.get_mut(&key) {
            p.demand = p.demand.saturating_sub(blocks.len());
            p.requested.extend(blocks.iter().copied());
        }
        self.send(key, PeerCommand::Request(blocks));
    }

    /// Tops up every peer with unmet demand, except `skip`.
    pub(super) fn top_up_all(&mut self, skip: Option<PeerKey>) {
        let keys: Vec<PeerKey> = self
            .peers
            .iter()
            .filter(|(k, p)| p.demand > 0 && Some(**k) != skip)
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            self.fill(key);
        }
    }
}

/// Memory budget for pieces being downloaded (buffered in memory).
const PARTIAL_BYTES: u64 = 64 * 1024 * 1024;
/// Pieces in progress are never fewer than this, whatever the piece size.
const MIN_PARTIAL_PIECES: usize = 8;

/// Most pieces a torrent downloads at once: `max(8, 64 MiB / piece_len)`.
pub(super) fn partial_cap(piece_len: u32) -> usize {
    let by_bytes = PARTIAL_BYTES / u64::from(piece_len.max(1));
    usize::try_from(by_bytes)
        .unwrap_or(usize::MAX)
        .max(MIN_PARTIAL_PIECES)
}

fn peer_has(has: PeerPieces, piece: u32) -> bool {
    match has {
        PeerPieces::Some(bf) => bf.get(piece),
        _ => true,
    }
}

/// Picks up to `room` blocks for `key` without ever having more than `cap`
/// pieces in progress: below the cap at most `cap - in_progress` blocks are
/// picked per round (each can start at most one piece); at the cap only
/// blocks of partial pieces are handed out.
pub(super) fn pick_capped(
    picker: &mut PiecePicker,
    key: PeerKey,
    has: PeerPieces,
    room: usize,
    cap: usize,
    rng: &mut impl rand::Rng,
) -> Vec<BlockRef> {
    let mut out = Vec::new();
    while out.len() < room {
        let left = room - out.len();
        let new_allowed = cap.saturating_sub(picker.num_in_progress());
        if new_allowed == 0 {
            let mut partial = Bitfield::new(has_len(picker));
            for i in picker.partial_pieces() {
                if peer_has(has, i) {
                    partial.set(i);
                }
            }
            out.extend(picker.pick(key, PeerPieces::Some(&partial), left, rng));
            break;
        }
        let got = picker.pick(key, has, left.min(new_allowed), rng);
        if got.is_empty() {
            break;
        }
        out.extend(got);
    }
    out
}

/// At the cap, partial pieces no connected peer has (availability 0) can
/// never complete and would block every new piece: they are failed back to
/// unrequested and their buffers freed. Returns how many were evicted.
pub(super) fn evict_orphans(
    picker: &mut PiecePicker,
    cap: usize,
    bufs: &mut HashMap<u32, Vec<u8>>,
) -> usize {
    if picker.num_in_progress() < cap {
        return 0;
    }
    let orphans: Vec<u32> = picker
        .partial_pieces()
        .filter(|&i| picker.availability(i) == 0)
        .collect();
    for &piece in &orphans {
        picker.piece_failed(piece);
        bufs.remove(&piece);
    }
    orphans.len()
}

fn has_len(picker: &PiecePicker) -> u32 {
    picker.have().len()
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use fbt_core::{Bitfield, PieceLayout};
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::*;

    fn picker(pieces: u32, piece_len: u32) -> PiecePicker {
        let layout =
            PieceLayout::new(u64::from(pieces) * u64::from(piece_len), piece_len).expect("layout");
        let mut p = PiecePicker::new(layout, &Bitfield::new(pieces));
        p.add_peer_pieces(PeerPieces::All);
        p
    }

    fn pieces_of(blocks: &[BlockRef]) -> HashSet<u32> {
        blocks.iter().map(|b| b.piece).collect()
    }

    #[test]
    fn orphaned_partials_are_evicted_when_the_cap_is_full() {
        let mut rng = StdRng::seed_from_u64(3);
        // Four blocks per piece, cap 3.
        let layout = PieceLayout::new(20 * 65_536, 65_536).expect("layout");
        let mut p = PiecePicker::new(layout, &Bitfield::new(20));
        let mut a_has = Bitfield::new(20);
        for i in 0..3 {
            a_has.set(i);
        }
        let mut b_has = Bitfield::new(20);
        for i in 10..20 {
            b_has.set(i);
        }
        p.add_peer_pieces(PeerPieces::Some(&a_has));
        p.add_peer_pieces(PeerPieces::Some(&b_has));
        let a = PeerKey(1);
        let b = PeerKey(2);
        let got = pick_capped(&mut p, a, PeerPieces::Some(&a_has), 12, 3, &mut rng);
        assert_eq!(got.len(), 12);
        let mut bufs: HashMap<u32, Vec<u8>> = HashMap::new();
        // One block of each piece arrives; the rest is still outstanding.
        for blk in got.iter().filter(|b| b.offset == 0) {
            assert!(p.on_block(a, *blk).accepted);
            bufs.insert(blk.piece, vec![0; 65_536]);
        }
        assert_eq!(p.num_in_progress(), 3);
        assert_eq!(bufs.len(), 3);
        // The only holder leaves for good.
        p.release_peer(a);
        p.remove_peer_pieces(PeerPieces::Some(&a_has));
        // The cap is full of stranded partials: B gets nothing.
        assert!(pick_capped(&mut p, b, PeerPieces::Some(&b_has), 8, 3, &mut rng).is_empty());
        assert_eq!(evict_orphans(&mut p, 3, &mut bufs), 3);
        assert!(bufs.is_empty());
        assert_eq!(p.num_in_progress(), 0);
        let blocks = pick_capped(&mut p, b, PeerPieces::Some(&b_has), 8, 3, &mut rng);
        assert_eq!(blocks.len(), 8);
        assert!(blocks.iter().all(|blk| blk.piece >= 10));
        // Below the cap nothing is evicted.
        assert_eq!(evict_orphans(&mut p, 3, &mut bufs), 0);
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

    #[test]
    fn partial_cap_by_piece_size() {
        assert_eq!(partial_cap(16_384), 4096);
        assert_eq!(partial_cap(1 << 20), 64);
        assert_eq!(partial_cap(16 << 20), 8);
        assert_eq!(partial_cap(0), 64 * 1024 * 1024);
    }

    #[test]
    fn single_block_pieces_never_exceed_the_cap() {
        let mut rng = StdRng::seed_from_u64(1);
        let mut p = picker(20, 16_384);
        let got = pick_capped(&mut p, PeerKey(1), PeerPieces::All, 32, 8, &mut rng);
        assert_eq!(got.len(), 8);
        assert_eq!(pieces_of(&got).len(), 8);
        assert_eq!(p.num_in_progress(), 8);
        // At the cap with nothing partial left to hand out: nothing new starts.
        assert!(pick_capped(&mut p, PeerKey(2), PeerPieces::All, 32, 8, &mut rng).is_empty());
        // A finished piece frees a slot.
        p.on_block(PeerKey(1), got[0]);
        p.piece_passed(got[0].piece);
        let more = pick_capped(&mut p, PeerKey(2), PeerPieces::All, 32, 8, &mut rng);
        assert_eq!(more.len(), 1);
        assert_eq!(p.num_in_progress(), 8);
    }

    #[test]
    fn at_the_cap_only_partial_pieces_are_served() {
        let mut rng = StdRng::seed_from_u64(2);
        // Four blocks per piece.
        let mut p = picker(20, 65_536);
        let first = pick_capped(&mut p, PeerKey(1), PeerPieces::All, 6, 3, &mut rng);
        assert_eq!(first.len(), 6);
        assert!(pieces_of(&first).len() <= 3);
        let rest = pick_capped(&mut p, PeerKey(2), PeerPieces::All, 32, 3, &mut rng);
        let all: Vec<BlockRef> = first.iter().chain(rest.iter()).copied().collect();
        assert_eq!(pieces_of(&all).len(), 3);
        assert_eq!(
            all.len(),
            12,
            "every block of the three pieces, nothing more"
        );
        assert_eq!(p.num_in_progress(), 3);
        // A peer without any partial piece gets nothing.
        let mut only_other = Bitfield::new(20);
        for i in 0..20 {
            if !pieces_of(&all).contains(&i) {
                only_other.set(i);
            }
        }
        assert!(
            pick_capped(
                &mut p,
                PeerKey(3),
                PeerPieces::Some(&only_other),
                32,
                3,
                &mut rng
            )
            .is_empty()
        );
    }
}
