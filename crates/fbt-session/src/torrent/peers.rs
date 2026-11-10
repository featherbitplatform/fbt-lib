//! The piece side of a torrent's peers: peer events, interest, choking and
//! handing out block requests.

use std::collections::HashMap;
use std::net::SocketAddr;

use fbt_core::{Bitfield, BlockRef, PeerId};
use fbt_metainfo::Metainfo;
use fbt_wire::{ExtHandshake, MetadataMsg, UT_METADATA};
use tokio::time::Instant;
use tracing::debug;

use super::actor::Actor;
use super::data::Job;
use super::metadata::{FetchStep, MAX_METADATA_REQUESTS_PER_PEER, serve_metadata};
use super::swarm::BanTarget;
use crate::choker::PeerChokeInfo;
use crate::event::Event;
use crate::peer::DisconnectReason;
use crate::peer::messages::{PeerCommand, PeerEvent, PeerPiecesOwned};
use crate::picker::{PeerKey, PeerPieces, PiecePicker};

impl Actor {
    // ----- peer events --------------------------------------------------------

    pub(super) fn on_peer_event(&mut self, ev: PeerEvent) {
        match ev {
            PeerEvent::Connected {
                key,
                peer_id,
                addr,
                encrypted,
                ..
            } => self.on_peer_connected(key, peer_id, addr, encrypted),
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
            PeerEvent::ExtHandshake { key, hs } => self.on_ext_handshake(key, hs),
            PeerEvent::Metadata { key, msg } => self.on_metadata_msg(key, msg),
            // PEX and the DHT come with Tasks 14 and 15 (a private torrent,
            // magnets included once their metadata says so, ignores both).
            PeerEvent::Pex { .. } | PeerEvent::DhtPort { .. } => {}
            PeerEvent::Disconnected {
                key,
                reason,
                outstanding,
            } => self.on_peer_disconnected(key, reason, &outstanding),
        }
    }

    /// A handshake finished: unless the swarm side closed it as a
    /// duplicate, the peer learns the pieces finished while it connected.
    fn on_peer_connected(
        &mut self,
        key: PeerKey,
        peer_id: PeerId,
        addr: SocketAddr,
        encrypted: bool,
    ) {
        if !self.on_connected(key, peer_id, addr, encrypted) {
            return;
        }
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        // Pieces finished while it connected were skipped by `on_written`.
        let snapshot = p.have_sent.take();
        if let (Some(sent), Some(picker)) = (
            snapshot,
            self.loaded.as_ref().and_then(|l| l.picker.as_ref()),
        ) {
            let missed: Vec<u32> = picker
                .have()
                .iter_ones()
                .filter(|&i| !sent.get(i))
                .collect();
            if !missed.is_empty() {
                self.send(key, PeerCommand::Haves(missed));
            }
        }
        debug!(%addr, encrypted, "peer connected");
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
            && let Some(picker) = self.loaded.as_mut().and_then(|l| l.picker.as_mut())
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
            && let Some(picker) = self.loaded.as_mut().and_then(|l| l.picker.as_mut())
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
        let seeding = self
            .loaded
            .as_ref()
            .and_then(|l| l.picker.as_ref())
            .is_some_and(PiecePicker::is_finished);
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
            && let Some(picker) = self.loaded.as_mut().and_then(|l| l.picker.as_mut())
        {
            picker.release(key, block);
        }
        // The peer asks again itself; others may take the block now.
        self.top_up_all(Some(key));
    }

    fn on_peer_disconnected(
        &mut self,
        key: PeerKey,
        reason: DisconnectReason,
        outstanding: &[BlockRef],
    ) {
        let Some((p, reason)) = self.on_disconnected(key, reason) else {
            return;
        };
        if let Some(m) = self.magnet.as_mut() {
            let sends = m.fetch.on_peer_gone(key);
            self.send_metadata(sends);
        }
        if p.generation == self.generation
            && let Some(picker) = self.loaded.as_mut().and_then(|l| l.picker.as_mut())
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

    // ----- metadata exchange (BEP 9) ------------------------------------------

    fn on_ext_handshake(&mut self, key: PeerKey, hs: Box<ExtHandshake>) {
        let size = hs.metadata_size;
        let supports = hs.id_of(UT_METADATA).is_some();
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        p.listen_port = hs.listen_port;
        p.ext = Some(hs);
        let Some(m) = self.magnet.as_mut() else {
            return;
        };
        // 0 is merely "no metadata": not a source, not banned.
        if size.is_some_and(|s| s != 0 && !m.fetch.acceptable_size(s)) {
            debug!(key = key.0, ?size, "unacceptable metadata_size");
            self.ban(key);
            return;
        }
        let sends = m
            .fetch
            .on_ext_handshake(key, size, supports, Instant::now());
        self.send_metadata(sends);
    }

    fn on_metadata_msg(&mut self, key: PeerKey, msg: MetadataMsg) {
        if let MetadataMsg::Request { piece } = msg {
            self.serve_metadata_request(key, piece);
            return;
        }
        let Some(m) = self.magnet.as_mut() else {
            return;
        };
        match m.fetch.on_message(key, msg, Instant::now()) {
            FetchStep::Nothing => {}
            FetchStep::Send(sends) => self.send_metadata(sends),
            FetchStep::Bad(key) => self.ban(key),
            // Only from a fetch that hashes inline, which the actor does not
            // use: verify it the same way.
            FetchStep::Done(bytes) | FetchStep::Verify(_, bytes) => {
                // Taken now: the peer may be gone when the result is in.
                let expected = m.magnet.info_hashes;
                let trackers = m.magnet.trackers.iter().map(|t| vec![t.clone()]).collect();
                let ban = self.ban_target(key);
                // Hashing up to `max_metadata_size` bytes and parsing stay off
                // the actor.
                self.jobs.spawn_blocking(move || Job::Metadata {
                    key,
                    ban,
                    result: Metainfo::from_info_bytes(&bytes, expected, trackers).map(Box::new),
                });
            }
        }
    }

    /// Answers a `ut_metadata` request from our metadata; beyond
    /// [`MAX_METADATA_REQUESTS_PER_PEER`] per connection, with `Reject`.
    fn serve_metadata_request(&mut self, key: PeerKey, piece: u32) {
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        p.metadata_requests = p.metadata_requests.saturating_add(1);
        let msg = if p.metadata_requests > MAX_METADATA_REQUESTS_PER_PEER {
            MetadataMsg::Reject { piece }
        } else {
            serve_metadata(self.loaded.as_ref().map(|l| l.meta.raw_info()), piece)
        };
        self.send(key, PeerCommand::SendMetadata(msg));
    }

    pub(super) fn send_metadata(&mut self, sends: Vec<(PeerKey, MetadataMsg)>) {
        for (key, msg) in sends {
            self.send(key, PeerCommand::SendMetadata(msg));
        }
    }

    /// Closes a peer that sent bad metadata; its address is never dialled
    /// again.
    pub(super) fn ban(&mut self, key: PeerKey) {
        let target = self.ban_target(key);
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        p.close_reason = Some(DisconnectReason::BadMetadata);
        if let Some(t) = target {
            self.apply_ban(t);
        }
        self.close_peer(key);
    }

    /// What to ban for peer `key`: the address we dialled; for an incoming
    /// peer its advertised listen address, else its IP.
    pub(super) fn ban_target(&self, key: PeerKey) -> Option<BanTarget> {
        let p = self.peers.get(&key)?;
        Some(if p.outgoing {
            BanTarget::Addr(p.addr)
        } else if let Some(port) = p.listen_port {
            BanTarget::Addr(SocketAddr::new(p.addr.ip(), port))
        } else {
            BanTarget::Ip(p.addr.ip())
        })
    }

    /// Never dials the address again; an IP is refused, both ways, for
    /// the rest of the metadata fetch.
    pub(super) fn apply_ban(&mut self, target: BanTarget) {
        match target {
            BanTarget::Addr(addr) => {
                self.retries.never(addr);
                self.known.remove(&addr);
                self.candidates.retain(|a| *a != addr);
            }
            BanTarget::Ip(ip) => self.retries.ban_metadata_ip(ip),
        }
    }

    /// While the metadata is pending and every peer slot is taken, closes
    /// one peer connected for `Settings::metadata_peer_grace` that never
    /// offered `ut_metadata`, so useless peers cannot stall the fetch.
    pub(super) fn close_useless_metadata_peer(&mut self, now: Instant) {
        let s = &self.shared.settings;
        if self.peers.len() < s.max_peers_per_torrent {
            return;
        }
        let grace = s.metadata_peer_grace;
        let useless = self
            .peers
            .iter()
            .filter(|(_, p)| {
                p.tx.is_some()
                    && p.connected_at
                        .is_some_and(|t| now.duration_since(t) >= grace)
                    && p.ext
                        .as_ref()
                        .is_none_or(|e| e.id_of(UT_METADATA).is_none())
            })
            .min_by_key(|(_, p)| p.connected_at)
            .map(|(k, _)| *k);
        if let Some(key) = useless {
            debug!(
                key = key.0,
                "closing a peer without ut_metadata to free a slot"
            );
            if let Some(p) = self.peers.get_mut(&key) {
                p.close_reason = Some(DisconnectReason::Closed);
            }
            self.close_peer(key);
        }
    }

    /// After the first check of a magnet: the peers connected meanwhile
    /// were told no pieces; they learn ours, and interest and requests
    /// start.
    pub(super) fn catch_up_peers(&mut self) {
        let generation = self.generation;
        let Some(have) = self
            .loaded
            .as_ref()
            .and_then(|l| l.picker.as_ref())
            .map(|pk| pk.have().iter_ones().collect::<Vec<u32>>())
        else {
            return;
        };
        let keys: Vec<PeerKey> = self
            .peers
            .iter()
            .filter(|(_, p)| p.connected && p.tx.is_some() && p.generation == generation)
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            if !have.is_empty() {
                self.send(key, PeerCommand::Haves(have.clone()));
            }
            self.update_interest(key);
        }
        self.top_up_all(None);
    }

    // ----- interest and requests ----------------------------------------------

    /// We want `piece`: wanted priority and not verified yet.
    pub(super) fn wants_piece(&self, piece: u32) -> bool {
        self.loaded.as_ref().is_some_and(|l| {
            l.piece_prio.get(piece as usize).is_some_and(|&p| p > 0)
                && l.picker.as_ref().is_some_and(|pk| !pk.have().get(piece))
        })
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
                self.loaded
                    .as_ref()
                    .and_then(|l| l.picker.as_ref())
                    .is_some_and(|pk| !pk.is_finished())
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
        let Some(l) = self.loaded.as_mut() else {
            return;
        };
        let cap = partial_cap(l.layout.piece_len());
        let (Some(picker), Some(p)) = (l.picker.as_mut(), self.peers.get(&key)) else {
            return;
        };
        let evicted = evict_orphans(picker, cap, &mut l.bufs);
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
