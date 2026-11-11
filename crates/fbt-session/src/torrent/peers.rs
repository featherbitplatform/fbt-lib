//! The piece side of a torrent's peers: peer events, interest, choking and
//! handing out block requests.

use std::collections::HashMap;
use std::net::SocketAddr;

use fbt_core::{Bitfield, BlockRef, PeerId};
use fbt_metainfo::Metainfo;
use fbt_wire::{ExtHandshake, LT_DONTHAVE, MetadataMsg, UPLOAD_ONLY, UT_METADATA, UT_PEX};
use tokio::time::Instant;
use tracing::debug;

use super::actor::Actor;
use super::data::Job;
use super::metadata::{FetchStep, MAX_METADATA_REQUESTS_PER_PEER, serve_metadata};
use super::pex::PexPeer;
use super::swarm::BanTarget;
use crate::choker::PeerChokeInfo;
use crate::event::Event;
use crate::peer::DisconnectReason;
use crate::peer::messages::{PeerCommand, PeerEvent, PeerPiecesOwned};
use crate::peer::transport::Transport;
use crate::picker::{PeerKey, PeerPieces, PiecePicker};
use crate::status::TorrentState;

impl Actor {
    // ----- peer events --------------------------------------------------------

    pub(super) fn on_peer_event(&mut self, ev: PeerEvent) {
        match ev {
            PeerEvent::Connected {
                key,
                peer_id,
                addr,
                encrypted,
                transport,
                ..
            } => self.on_peer_connected(key, peer_id, addr, encrypted, transport),
            PeerEvent::Pieces { key, pieces } => self.on_pieces(key, pieces),
            PeerEvent::Have { key, piece } => self.on_have(key, piece),
            PeerEvent::DontHave { key, piece } => self.on_donthave(key, piece),
            PeerEvent::UploadOnly { key, on } => self.on_upload_only(key, on),
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
            PeerEvent::Snubbed { key, snubbed } => {
                if let Some(p) = self.peers.get_mut(&key) {
                    p.snubbed = snubbed;
                    // Asked for at the old depth: the peer task asks again
                    // for its single request.
                    if snubbed {
                        p.demand = 0;
                    }
                }
            }
            // The peer task drops the cancelled request itself; an answer
            // already being read is discarded there.
            PeerEvent::Cancel { .. } => {}
            PeerEvent::BlockSent { key, len, .. } => self.on_block_sent(key, len),
            PeerEvent::Wasted { bytes, .. } => {
                self.counters.wasted = self.counters.wasted.saturating_add(bytes);
            }
            PeerEvent::ExtHandshake { key, hs } => self.on_ext_handshake(key, hs),
            PeerEvent::Metadata { key, msg } => self.on_metadata_msg(key, msg),
            PeerEvent::Pex { key, msg } => self.on_pex(key, msg),
            PeerEvent::DhtPort { key, port } => self.on_dht_port(key, port),
            PeerEvent::Disconnected {
                key,
                reason,
                outstanding,
                connect_failed,
            } => self.on_peer_disconnected(key, reason, &outstanding, connect_failed),
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
        transport: Transport,
    ) {
        if !self.on_connected(key, peer_id, addr, encrypted, transport) {
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
        debug!(%addr, encrypted, ?transport, "peer connected");
        self.emit(Event::PeerConnected {
            torrent: self.ih,
            addr,
            transport,
            encrypted,
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
        self.close_if_upload_only(key);
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
        self.close_if_upload_only(key);
    }

    /// The remote retracted a piece (BEP 54): it no longer counts in the
    /// availability, and our interest is re-evaluated. A `HaveAll` peer is
    /// first turned into the equivalent full bitfield.
    fn on_donthave(&mut self, key: PeerKey, piece: u32) {
        let generation = self.generation;
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        if !p.has(piece) {
            return;
        }
        let counted = p.generation == generation;
        let picker = self.loaded.as_mut().and_then(|l| l.picker.as_mut());
        let mut picker = picker.filter(|_| counted);
        if p.seed {
            let full = Bitfield::full(p.bits.len());
            if let Some(pk) = picker.as_deref_mut() {
                pk.remove_peer_pieces(PeerPieces::All);
                pk.add_peer_pieces(PeerPieces::Some(&full));
            }
            p.seed = false;
            p.bits = full;
        }
        p.bits.clear(piece);
        if let Some(f) = p.fast_has.as_mut() {
            f.clear(piece);
        }
        // Blocks asked of it for this piece will not come: back to the
        // picker, and cancelled so the peer does not answer them.
        let gone: Vec<BlockRef> = p
            .requested
            .iter()
            .filter(|b| b.piece == piece)
            .copied()
            .collect();
        for b in &gone {
            p.requested.remove(b);
        }
        if let Some(pk) = picker {
            pk.peer_lost(piece);
            for b in &gone {
                pk.release(key, *b);
            }
        }
        for b in gone {
            self.send(key, PeerCommand::Cancel(b));
        }
        self.update_interest(key);
        self.top_up_all(None);
    }

    /// The remote is (no longer) upload-only (BEP 21).
    fn on_upload_only(&mut self, key: PeerKey, on: bool) {
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        p.upload_only = on;
        self.close_if_upload_only(key);
    }

    /// While we are finished and on disk, a peer that is a seed or
    /// upload-only has nothing to trade with us: closed.
    pub(super) fn close_if_upload_only(&mut self, key: PeerKey) {
        if !self.upload_only_sent {
            return;
        }
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        if p.connected && p.tx.is_some() && (p.upload_only || p.is_seed()) {
            debug!(key = key.0, "closing an upload-only peer while seeding");
            p.close_reason = Some(DisconnectReason::UploadOnly);
            self.close_peer(key);
        }
    }

    /// Follows the seeding state: peers learn when we become (or stop
    /// being) upload-only, and upload-only peers are dropped once we are.
    /// "Finished" means seeding, with the data on disk, not merely every
    /// piece verified.
    pub(super) fn sync_upload_only(&mut self) {
        let on = matches!(self.last_state, TorrentState::Seeding);
        if on == self.upload_only_sent {
            return;
        }
        self.upload_only_sent = on;
        if !on {
            // No longer finished: seeds we turned away may have what we
            // lack, so they are dialled again.
            self.seed_addrs.clear();
        } else {
            self.lost_files.clear();
        }
        let keys: Vec<PeerKey> = self
            .peers
            .iter()
            .filter(|(_, p)| p.connected && p.tx.is_some())
            .map(|(k, _)| *k)
            .collect();
        for &key in &keys {
            self.send(key, PeerCommand::UploadOnly(on));
        }
        if on {
            for key in keys {
                self.close_if_upload_only(key);
            }
        } else {
            self.requeue_known();
            self.try_connect();
        }
    }

    /// Remembers the address of a seed or upload-only peer that is gone
    /// while we are finished, so it is not redialled in a loop.
    fn note_seed_gone(&mut self, p: &super::swarm::Peer) {
        if !self.upload_only_sent
            || !p.connected
            || !(p.upload_only || p.is_seed())
            || self.seed_addrs.len() >= super::swarm::MAX_KNOWN
        {
            return;
        }
        // An incoming peer is dialled at its listen address, if it has one.
        let addr = if p.outgoing {
            Some(p.addr)
        } else {
            p.listen_port.map(|port| SocketAddr::new(p.addr.ip(), port))
        };
        if let Some(a) = addr {
            self.seed_addrs.insert(a);
        }
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
        self.set_choke_at(key, choke, Instant::now());
    }

    /// [`Self::set_choke`] at `now`: an unchoke starts the peer's turn
    /// (`unchoked_since`, `uploaded_since_unchoke`).
    fn set_choke_at(&mut self, key: PeerKey, choke: bool, now: Instant) {
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        if p.tx.is_none() || p.am_choking == choke {
            return;
        }
        p.am_choking = choke;
        if choke {
            p.unchoked_since = None;
        } else {
            p.unchoked_since = Some(now);
            p.uploaded_since_unchoke = 0;
        }
        let cmd = if choke {
            PeerCommand::Choke
        } else {
            PeerCommand::Unchoke
        };
        self.send(key, cmd);
    }

    /// Runs the choker over the connected peers (see
    /// [`Choker`](crate::choker::Choker)), then records who holds the
    /// optimistic slots.
    pub(super) fn rechoke(&mut self) {
        if !self.active() {
            return;
        }
        let now = Instant::now();
        let num_pieces = self.loaded.as_ref().map(|l| l.layout.num_pieces());
        if let Some(l) = self.loaded.as_ref() {
            let quota = u64::from(self.shared.settings.seeding_piece_quota)
                .saturating_mul(u64::from(l.layout.piece_len()));
            self.choker.set_piece_quota_bytes(quota);
        }
        let infos: Vec<PeerChokeInfo> = self
            .peers
            .iter()
            .filter(|(_, p)| p.connected && p.tx.is_some())
            .map(|(&key, p)| PeerChokeInfo {
                key,
                interested: p.peer_interested,
                rate_from_peer: p.from_rate.rate(now),
                rate_to_peer: p.to_rate.rate(now),
                snubbed: p.snubbed,
                progress_ppm: progress_ppm(p, num_pieces),
                unchoked_since: p.unchoked_since,
                uploaded_since_unchoke: p.uploaded_since_unchoke,
                last_optimistic: p.last_optimistic,
            })
            .collect();
        let seeding = self
            .loaded
            .as_ref()
            .and_then(|l| l.picker.as_ref())
            .is_some_and(PiecePicker::is_finished);
        let unchoke = self.choker.rechoke(&infos, seeding, now, &mut self.rng);
        for info in infos {
            self.set_choke_at(info.key, !unchoke.contains(&info.key), now);
        }
        for key in self.choker.optimistic() {
            if let Some(p) = self.peers.get_mut(key) {
                p.last_optimistic = Some(now);
            }
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
        connect_failed: Option<Transport>,
    ) {
        let Some((p, reason)) = self.on_disconnected(key, reason, connect_failed) else {
            return;
        };
        self.note_seed_gone(&p);
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

    /// A peer's `Port`: its DHT node joins ours (pinged first), unless the
    /// torrent is private. Never waits.
    fn on_dht_port(&mut self, key: PeerKey, port: u16) {
        if self.is_private() {
            return;
        }
        let (Some(dht), Some(p)) = (self.shared.dht.as_ref(), self.peers.get(&key)) else {
            return;
        };
        dht.add_node(SocketAddr::new(p.addr.ip(), port));
    }

    fn on_ext_handshake(&mut self, key: PeerKey, hs: Box<ExtHandshake>) {
        let size = hs.metadata_size;
        let supports = hs.id_of(UT_METADATA).is_some();
        let pex = self.pex_active() && hs.id_of(UT_PEX).is_some();
        // Its PEX address (or its PEX state) may change.
        self.on_peer_set_changed();
        let Some(p) = self.peers.get_mut(&key) else {
            return;
        };
        p.listen_port = hs.listen_port;
        // A repeat keeps the PEX state; one turning `ut_pex` off ends it.
        if !pex {
            p.pex = None;
        } else if p.pex.is_none() {
            p.pex = Some(PexPeer::default());
        }
        let ids = (
            hs.id_of(UPLOAD_ONLY).is_some(),
            hs.id_of(LT_DONTHAVE).is_some(),
        );
        p.ext = Some(hs);
        // The handshake we sent may predate the end of our download, and
        // commands sent before the remote's ids were known were dropped:
        // say it again now (a repeat is harmless).
        if self.upload_only_sent && ids.0 {
            self.send(key, PeerCommand::UploadOnly(true));
        }
        if ids.1 {
            let lost: Vec<u32> = self.lost_pieces.iter().copied().collect();
            for piece in lost {
                if !self.send_lossy(key, PeerCommand::DontHave(piece)) {
                    break;
                }
            }
        }
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
        let (Some(picker), Some(p)) = (l.picker.as_mut(), self.peers.get(&key)) else {
            return;
        };
        let cap = partial_cap(
            picker.num_in_progress(),
            self.cache.partial_room(u64::from(l.layout.piece_len())),
        );
        let evicted = evict_orphans(picker, cap, &mut l.bufs);
        if !evicted.is_empty() {
            debug!(
                evicted = evicted.len(),
                "dropped partial pieces no connected peer has"
            );
            for piece in evicted {
                self.cache.release_partial(piece);
            }
        }
        if !p.connected || p.tx.is_none() || p.generation != generation {
            return;
        }
        let room = max.saturating_sub(p.requested.len()).min(p.demand);
        if room == 0 {
            return;
        }
        let mut blocks = if p.choked_us {
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
        // Every piece started here reserves its buffer from the cache
        // budget; one that cannot (the budget moved since `partial_room`)
        // goes back to the picker.
        let mut refused: Vec<u32> = Vec::new();
        for b in &blocks {
            if refused.contains(&b.piece) || self.cache.has_partial(b.piece) {
                continue;
            }
            let size = u64::from(l.layout.piece_size(b.piece));
            if !self.cache.reserve_partial(b.piece, size) {
                refused.push(b.piece);
            }
        }
        if !refused.is_empty() {
            blocks.retain(|b| {
                let keep = !refused.contains(&b.piece);
                if !keep {
                    picker.release(key, *b);
                }
                keep
            });
        }
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

/// The peer's share of the torrent's `num_pieces` pieces in parts per
/// million; 0 while the metadata is pending.
fn progress_ppm(p: &super::swarm::Peer, num_pieces: Option<u32>) -> u32 {
    match num_pieces {
        _ if p.seed => 1_000_000,
        Some(n) if n > 0 => {
            let have = u64::from(p.bits.count_ones().min(n));
            u32::try_from(have * 1_000_000 / u64::from(n)).unwrap_or(1_000_000)
        }
        _ => 0,
    }
}

/// Most pieces a torrent downloads at once: those already in progress
/// plus the new buffers the cache budget has `room` for (see
/// [`PieceCache::partial_room`](crate::storage::cache::PieceCache::partial_room)).
pub(super) fn partial_cap(in_progress: usize, room: usize) -> usize {
    in_progress.saturating_add(room)
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
/// unrequested and their buffers freed. Returns the evicted pieces, whose
/// budget reservations the caller gives back.
pub(super) fn evict_orphans(
    picker: &mut PiecePicker,
    cap: usize,
    bufs: &mut HashMap<u32, Vec<u8>>,
) -> Vec<u32> {
    if picker.num_in_progress() < cap {
        return Vec::new();
    }
    let orphans: Vec<u32> = picker
        .partial_pieces()
        .filter(|&i| picker.availability(i) == 0)
        .collect();
    for &piece in &orphans {
        picker.piece_failed(piece);
        bufs.remove(&piece);
    }
    orphans
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
        assert_eq!(evict_orphans(&mut p, 3, &mut bufs).len(), 3);
        assert!(bufs.is_empty());
        assert_eq!(p.num_in_progress(), 0);
        let blocks = pick_capped(&mut p, b, PeerPieces::Some(&b_has), 8, 3, &mut rng);
        assert_eq!(blocks.len(), 8);
        assert!(blocks.iter().all(|blk| blk.piece >= 10));
        // Below the cap nothing is evicted.
        assert!(evict_orphans(&mut p, 3, &mut bufs).is_empty());
    }

    #[test]
    fn partial_cap_reads_the_budget_room() {
        assert_eq!(partial_cap(3, 0), 3);
        assert_eq!(partial_cap(3, 5), 8);
        assert_eq!(partial_cap(usize::MAX, 5), usize::MAX);
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

    /// A 500-deep pipeline asks for far more blocks than the cache budget
    /// has pieces for: it gets only the blocks of `cap` pieces.
    #[test]
    fn deep_pipeline_stays_within_the_partial_cap() {
        let mut rng = StdRng::seed_from_u64(4);
        // Four blocks per piece, room for 5 pieces.
        let mut p = picker(200, 65_536);
        let got = pick_capped(&mut p, PeerKey(1), PeerPieces::All, 500, 5, &mut rng);
        assert_eq!(got.len(), 20);
        assert_eq!(pieces_of(&got).len(), 5);
        assert_eq!(p.num_in_progress(), 5);
        assert!(pick_capped(&mut p, PeerKey(2), PeerPieces::All, 500, 5, &mut rng).is_empty());
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
