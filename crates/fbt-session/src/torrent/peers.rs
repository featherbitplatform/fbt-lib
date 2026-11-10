//! Peer connections of a torrent: connecting and accepting within the
//! limits, peer events, interest and handing out block requests.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use fbt_core::{Bitfield, BlockRef, PeerId};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::debug;

use super::actor::Actor;
use crate::event::Event;
use crate::peer::DisconnectReason;
use crate::peer::conn::{PeerCtx, spawn_incoming, spawn_outgoing};
use crate::peer::messages::{PeerCommand, PeerEvent, PeerPiecesOwned};
use crate::picker::{PeerKey, PeerPieces, PiecePicker};
use crate::session::{release, try_reserve};

/// One peer connection, from spawn until its `Disconnected` event.
#[derive(Debug)]
pub(super) struct Peer {
    pub addr: SocketAddr,
    /// `None` once we closed it; it stays until `Disconnected` arrives.
    pub tx: Option<mpsc::Sender<PeerCommand>>,
    /// Picker generation the peer belongs to.
    pub generation: u64,
    /// Outgoing and still holding a half-open slot.
    pub connecting: bool,
    pub connected: bool,
    pub peer_id: Option<PeerId>,
    /// `HaveAll` or a full bitfield.
    pub seed: bool,
    pub bits: Bitfield,
    pub choked_us: bool,
    pub am_interested: bool,
    pub peer_interested: bool,
    pub am_choking: bool,
    pub allowed_fast: HashSet<u32>,
    /// Our requests the peer task holds.
    pub requested: HashSet<BlockRef>,
    /// Blocks the peer asked for (`WantBlocks`) and we have not handed out.
    pub demand: usize,
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
            connecting: false,
            connected: false,
            peer_id: None,
            seed: false,
            bits: Bitfield::new(num_pieces),
            choked_us: true,
            am_interested: false,
            peer_interested: false,
            am_choking: true,
            allowed_fast: HashSet::new(),
            requested: HashSet::new(),
            demand: 0,
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
            let busy = self
                .peers
                .values()
                .any(|p| p.addr == addr && p.tx.is_some());
            if !busy && !self.candidates.contains(&addr) {
                self.candidates.push_back(addr);
            }
        }
    }

    /// Starts outgoing connections while every limit allows.
    // Task 14: retry with backoff and a never-retry set.
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
            let key = self.new_key();
            let tx = spawn_outgoing(addr, self.ctx(key));
            let mut peer = Peer::new(addr, tx, self.generation, self.layout.num_pieces());
            peer.connecting = true;
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
        let tx = spawn_incoming(stream, hs, addr, self.ctx(key));
        let peer = Peer::new(addr, tx, self.generation, self.layout.num_pieces());
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
                if let Some(p) = self.peers.get_mut(&key) {
                    p.allowed_fast.insert(piece);
                }
                self.fill(key);
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
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        if p.connecting {
            p.connecting = false;
            release(&self.shared.half_open);
        }
        p.connected = true;
        p.peer_id = Some(peer_id);
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
        // Task 14: the choker decides; until then every interested peer is
        // unchoked and an uninterested one choked.
        if !active {
            return;
        }
        if on && p.am_choking {
            p.am_choking = false;
            self.send(key, PeerCommand::Unchoke);
        } else if !on && !p.am_choking {
            p.am_choking = true;
            self.send(key, PeerCommand::Choke);
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
        let Some(p) = self.peers.remove(&key) else {
            return;
        };
        if p.connecting {
            release(&self.shared.half_open);
        }
        release(&self.shared.global_peers);
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
        if !p.connected || p.tx.is_none() || p.generation != generation {
            return;
        }
        let room = max.saturating_sub(p.requested.len()).min(p.demand);
        if room == 0 {
            return;
        }
        let blocks = if p.choked_us {
            if p.allowed_fast.is_empty() {
                return;
            }
            let mut fast = Bitfield::new(self.layout.num_pieces());
            for &i in &p.allowed_fast {
                if p.has(i) {
                    fast.set(i);
                }
            }
            pick_capped(
                picker,
                key,
                PeerPieces::Some(&fast),
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

fn has_len(picker: &PiecePicker) -> u32 {
    picker.have().len()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

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
