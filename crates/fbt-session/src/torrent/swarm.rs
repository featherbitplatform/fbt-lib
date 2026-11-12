//! The swarm side of a torrent: which addresses to connect to, connecting
//! and accepting within the limits, sending to peers and closing them.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use fbt_core::{Bitfield, PeerId};
use fbt_wire::{CryptoMethods, HolepunchMsg};
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::Instant;
use tracing::debug;

use super::actor::{Actor, Phase};
use super::peer::Peer;
use super::retries::AfterFailure;
use crate::bans::BlockReason;
use crate::event::Event;
use crate::peer::DisconnectReason;
use crate::peer::conn::{PeerCtx, PeerStream, spawn_incoming, spawn_outgoing};
use crate::peer::ext::ExtConfig;
use crate::peer::messages::PeerCommand;
use crate::peer::transport::Transport;
use crate::picker::PeerKey;
use crate::session::{HolepunchCounters, dht_port_for, release, try_reserve};
use crate::settings::EncryptionPolicy;

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

/// What became of a connection whose handshake finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConnectOutcome {
    /// It stays; the first connection to this peer.
    New,
    /// It stays and an older connection to the same peer id was closed in
    /// its favour: the peer was already reported connected.
    Replaced,
    /// It was closed as a duplicate of an older connection.
    Dropped,
}

/// What a bad-metadata ban applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BanTarget {
    /// An address never dialled again.
    Addr(SocketAddr),
    /// An incoming peer with no known listen address: its IP is refused
    /// for the rest of the metadata fetch.
    Ip(IpAddr),
}

impl Actor {
    // ----- connections ----------------------------------------------------------

    /// The context of a new peer task. Without metadata the task runs in
    /// no-metadata mode (no layout, an empty `have`).
    pub(super) fn ctx(&self, key: PeerKey, addr: &SocketAddr) -> PeerCtx {
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
            ext: self.ext_config(addr),
            settings: Arc::clone(&self.shared.settings),
            events: self.peer_tx.clone(),
            encrypted: false,
            transport: Transport::Tcp,
            bandwidth: Arc::clone(&self.shared.bandwidth),
            torrent_rate: self.rate.clone(),
            external_ip: Arc::clone(&self.shared.external_ip),
        }
    }

    /// What a connection to `addr` advertises. A torrent without metadata
    /// may be private; until it is known, PEX is advertised (the magnet
    /// exception) when `pex_enabled`.
    pub(super) fn ext_config(&self, addr: &SocketAddr) -> ExtConfig {
        let meta = self.loaded.as_ref().map(|l| &l.meta);
        ExtConfig {
            listen_port: self.shared.listen_port_for(addr),
            metadata_size: meta.and_then(|m| u32::try_from(m.raw_info().len()).ok()),
            pex: self.shared.settings.pex_enabled && !meta.is_some_and(|m| m.info.private),
            // No DHT bit or `Port` for a private torrent (a magnet's
            // privacy is unknown until its metadata: it advertises both).
            dht_port: dht_port_for(&self.shared.dht_addrs, addr).filter(|_| !self.is_private()),
            reqq: u32::try_from(self.shared.settings.max_incoming_requests).unwrap_or(u32::MAX),
            upload_only: self.upload_only_sent,
            holepunch: self.holepunch_active(),
        }
    }

    /// BEP 55 holepunch runs: enabled, uTP on in both directions with a
    /// socket to use, and the torrent not (known to be) private.
    pub(super) fn holepunch_active(&self) -> bool {
        let s = &self.shared.settings;
        s.enable_holepunch
            && s.enable_outgoing_utp
            && s.enable_incoming_utp
            && !self.shared.utp.is_empty()
            && !self.is_private()
    }

    pub(super) fn new_key(&mut self) -> PeerKey {
        self.next_key = self.next_key.wrapping_add(1);
        PeerKey(self.next_key)
    }

    pub(super) fn requeue_known(&mut self) {
        if !self.connectable() {
            return;
        }
        let known: Vec<SocketAddr> = self.known.iter().copied().collect();
        for addr in known {
            self.queue_addr(addr);
        }
    }

    /// Queues `addr` for connecting unless it is ourselves, waiting for a
    /// retry, already connected, already queued, or blocked by the session
    /// (silently: where it came from reported that).
    pub(super) fn queue_addr(&mut self, addr: SocketAddr) {
        if self.retries.is_never(&addr)
            || self.retries.is_pending(&addr)
            || self.shared.blocked(addr).is_some()
        {
            return;
        }
        // Finished: a seed has nothing to trade (BEP 21).
        if self.upload_only_sent && self.seed_addrs.contains(&addr) {
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
        if !self.connectable() {
            return;
        }
        for addr in self.retries.due(Instant::now()) {
            self.queue_addr(addr);
        }
    }

    /// Takes a peer slot, a global one and a half-open one for an outgoing
    /// connection; false (nothing taken) when a limit is reached.
    fn reserve_dial(&self) -> bool {
        let s = &self.shared.settings;
        if self.peers.len() >= s.max_peers_per_torrent {
            return false;
        }
        if !try_reserve(&self.shared.global_peers, s.max_peers_global) {
            return false;
        }
        if !try_reserve(&self.shared.half_open, s.max_half_open) {
            release(&self.shared.global_peers);
            return false;
        }
        true
    }

    /// Gives back what [`reserve_dial`](Self::reserve_dial) took.
    fn unreserve_dial(&self) {
        release(&self.shared.half_open);
        release(&self.shared.global_peers);
    }

    /// Dials `addr` over `transport` in slots already reserved.
    fn spawn_dial(&mut self, addr: SocketAddr, transport: Transport) -> PeerKey {
        let utp = match transport {
            Transport::Utp => self.shared.utp_for(&addr).cloned(),
            Transport::Tcp => None,
        };
        let key = self.new_key();
        let ctx = self.ctx(key, &addr);
        let have = ctx.have.clone();
        let num_pieces = ctx.have.len();
        let tx = spawn_outgoing(addr, ctx, self.mse_provide(&addr), transport, utp);
        let mut peer = Peer::new(addr, tx, self.generation, num_pieces, transport);
        peer.outgoing = true;
        peer.connecting = true;
        peer.have_sent = Some(have);
        self.peers.insert(key, peer);
        key
    }

    /// Starts outgoing connections while every limit allows.
    pub(super) fn try_connect(&mut self) {
        while self.connectable() && !self.candidates.is_empty() {
            if !self.reserve_dial() {
                break;
            }
            let Some(addr) = self.candidates.pop_front() else {
                self.unreserve_dial();
                break;
            };
            // The filter may have changed since it was queued. Through a
            // proxy too it is the peer's own address that counts.
            if self.refuse_blocked(addr) {
                self.retries.clear_transport_failed(&addr);
                self.unreserve_dial();
                continue;
            }
            // It may have connected (or connected to us) since it was
            // queued; an IP that sent bad metadata waits for the metadata.
            if self.addr_busy(&addr) || self.metadata_ip_banned(&addr) {
                // A fallback queued for it is not dialled now: forget it.
                self.retries.clear_transport_failed(&addr);
                self.unreserve_dial();
                continue;
            }
            // A pending fallback goes first; otherwise the transport the
            // settings and what we know of the address prefer.
            let transport = self
                .retries
                .fallback_transport(&addr)
                .or_else(|| self.first_transport(&addr));
            let Some(transport) = transport else {
                debug!(%addr, "no enabled transport can reach this address");
                self.unreserve_dial();
                continue;
            };
            self.spawn_dial(addr, transport);
        }
    }

    /// A relay told us to connect to `addr` (BEP 55 `Connect`): dialled
    /// over uTP at once, ahead of the queue and any retry wait, unless it
    /// is connected or being dialled already, a seed we turned away while
    /// finished, or a limit is reached. Only this one attempt: see
    /// [`Peer::holepunch`]. Returns whether it dialled.
    pub(super) fn dial_holepunch(&mut self, addr: SocketAddr) -> bool {
        if !self.connectable()
            || self.retries.is_never(&addr)
            || (self.upload_only_sent && self.seed_addrs.contains(&addr))
            || self.addr_busy(&addr)
            || self.metadata_ip_banned(&addr)
            || !self.transport_usable(&addr, Transport::Utp)
        {
            return false;
        }
        if self.refuse_blocked(addr) {
            return false;
        }
        if !self.reserve_dial() {
            debug!(%addr, "no connection slot for a holepunch dial");
            return false;
        }
        debug!(%addr, "holepunch: dialling over uTP");
        let key = self.spawn_dial(addr, Transport::Utp);
        if let Some(p) = self.peers.get_mut(&key) {
            p.holepunch = true;
        }
        true
    }

    /// An outgoing connection to `addr` failed at connect over every
    /// transport we may use: if PEX listed it with the `HOLEPUNCH` flag and
    /// that source is still connected and speaks `ut_holepunch`, it is
    /// asked for a rendezvous (once per address).
    fn request_rendezvous(&mut self, addr: SocketAddr) {
        if !self.holepunch_active() {
            return;
        }
        let Some(k) = self.known.get(&addr) else {
            return;
        };
        if !k.holepunch || k.rendezvous_sent {
            return;
        }
        let Some(relay) = k.pex_source else {
            return;
        };
        if !self
            .peers
            .get(&relay)
            .is_some_and(|p| p.live() && p.supports_holepunch())
        {
            return;
        }
        if !self.hp_pending.insert(addr, relay, Instant::now()) {
            debug!(%addr, "too many rendezvous requests pending; not asking");
            return;
        }
        if let Some(k) = self.known.get_mut(&addr) {
            k.rendezvous_sent = true;
        }
        debug!(%addr, relay = relay.0, "connect failed; asking for a rendezvous");
        if self.send(
            relay,
            PeerCommand::SendHolepunch(HolepunchMsg::Rendezvous(addr)),
        ) {
            HolepunchCounters::bump(&self.shared.holepunch.rendezvous_sent);
        }
    }

    /// The transport to dial `addr` over first: uTP when outgoing uTP is
    /// on, a UDP socket of the address's family exists and the address is
    /// not TCP-only (or a PEX `UTP` flag came for it since); otherwise TCP
    /// when outgoing TCP is on. `None` when neither can reach it.
    pub(super) fn first_transport(&self, addr: &SocketAddr) -> Option<Transport> {
        let tcp_only =
            self.retries.is_tcp_only(addr) && !self.known.get(addr).is_some_and(|k| k.utp);
        if self.transport_usable(addr, Transport::Utp) && !tcp_only {
            Some(Transport::Utp)
        } else if self.transport_usable(addr, Transport::Tcp) {
            Some(Transport::Tcp)
        } else {
            None
        }
    }

    /// `t` is enabled for outgoing connections and, for uTP, a socket of
    /// `addr`'s family exists.
    fn transport_usable(&self, addr: &SocketAddr, t: Transport) -> bool {
        let s = &self.shared.settings;
        match t {
            Transport::Tcp => s.enable_outgoing_tcp,
            Transport::Utp => s.enable_outgoing_utp && self.shared.utp_for(addr).is_some(),
        }
    }

    /// `addr`'s IP sent bad metadata and the metadata is still pending.
    fn metadata_ip_banned(&self, addr: &SocketAddr) -> bool {
        matches!(self.phase, Phase::Metadata) && self.retries.is_metadata_banned(&addr.ip())
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
        // While announced: through the check after a magnet's metadata too,
        // which keeps the peers it has (they catch up when it ends).
        if !self.running() || self.peers.len() >= s.max_peers_per_torrent {
            debug!(%addr, "torrent inactive or full; dropping incoming connection");
            return;
        }
        if self.metadata_ip_banned(&addr) {
            debug!(%addr, "IP sent bad metadata; dropping incoming connection");
            return;
        }
        if self.refuse_blocked(addr) {
            return;
        }
        if self.over_ip_limit(None, &addr) {
            self.emit_blocked(addr, BlockReason::PerIpLimit);
            return;
        }
        if !try_reserve(&self.shared.global_peers, s.max_peers_global) {
            debug!(%addr, "global peer limit reached; dropping incoming connection");
            return;
        }
        let key = self.new_key();
        let mut ctx = self.ctx(key, &addr);
        ctx.encrypted = stream.is_encrypted();
        ctx.transport = stream.get_ref().kind();
        let transport = ctx.transport;
        let have = ctx.have.clone();
        let num_pieces = ctx.have.len();
        let tx = spawn_incoming(stream, hs, addr, ctx);
        let mut peer = Peer::new(addr, tx, self.generation, num_pieces, transport);
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
                self.on_peer_set_changed();
                if let Some(p) = self.peers.get_mut(&key) {
                    p.tx = None;
                }
                false
            }
        }
    }

    /// Sends an optional command only while the peer's queue has room to
    /// spare (a whole file of `DontHave`s must not fill it: a full queue
    /// closes the peer as stalled). Returns false when it was left out.
    pub(super) fn send_lossy(&mut self, key: PeerKey, cmd: PeerCommand) -> bool {
        let room = self
            .peers
            .get(&key)
            .and_then(|p| p.tx.as_ref())
            .is_some_and(|tx| tx.capacity() > 32);
        room && self.send(key, cmd)
    }

    /// Closes a peer: it stays until its `Disconnected` event, but its
    /// requests return to the picker now.
    pub(super) fn close_peer(&mut self, key: PeerKey) {
        self.on_peer_set_changed();
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

    /// Reports a refused peer of this torrent ([`Event::PeerBlocked`]).
    pub(super) fn emit_blocked(&self, addr: SocketAddr, reason: BlockReason) {
        debug!(%addr, ?reason, "peer blocked");
        self.emit(Event::PeerBlocked {
            torrent: Some(self.ih),
            addr,
            reason,
        });
    }

    /// Whether the session blocks `addr` (IP filter or ban); a blocked
    /// address is reported.
    pub(super) fn refuse_blocked(&self, addr: SocketAddr) -> bool {
        match self.shared.blocked(addr) {
            Some(reason) => {
                self.emit_blocked(addr, reason);
                true
            }
            None => false,
        }
    }

    /// Whether `addr`'s IP already has `max_peers_per_ip` connected peers
    /// in this torrent, besides `key` (IPv4-mapped addresses count as
    /// IPv4; loopback is exempt under `per_ip_exempt_loopback`).
    fn over_ip_limit(&self, key: Option<PeerKey>, addr: &SocketAddr) -> bool {
        let s = &self.shared.settings;
        let ip = addr.ip().to_canonical();
        if s.per_ip_exempt_loopback && ip.is_loopback() {
            return false;
        }
        let same_ip = self
            .peers
            .iter()
            .filter(|(k, p)| {
                Some(**k) != key
                    && p.connected
                    && p.tx.is_some()
                    && p.addr.ip().to_canonical() == ip
            })
            .count();
        same_ip >= s.max_peers_per_ip
    }

    /// The session's IP filter changed: closes every live peer it (or a
    /// ban) now blocks, and unqueues blocked addresses.
    pub(super) fn on_filter_changed(&mut self) {
        let blocked: Vec<(PeerKey, SocketAddr, BlockReason)> = self
            .peers
            .iter()
            .filter(|(_, p)| p.tx.is_some())
            .filter_map(|(k, p)| self.shared.blocked(p.addr).map(|r| (*k, p.addr, r)))
            .collect();
        for (key, addr, reason) in blocked {
            if let Some(p) = self.peers.get_mut(&key) {
                p.close_reason = Some(DisconnectReason::Blocked(reason));
            }
            self.emit_blocked(addr, reason);
            self.close_peer(key);
        }
        let shared = &self.shared;
        self.candidates.retain(|a| shared.blocked(*a).is_none());
    }

    pub(super) fn close_all(&mut self) {
        let keys: Vec<PeerKey> = self.peers.keys().copied().collect();
        for key in keys {
            self.close_peer(key);
        }
        self.candidates.clear();
    }

    /// Bookkeeping for a finished handshake: slots, retries and the
    /// duplicate peer-id guard. Says whether the connection stays (the
    /// caller then catches it up on our pieces) and whether it took over
    /// from an older connection to the same peer.
    pub(super) fn on_connected(
        &mut self,
        key: PeerKey,
        peer_id: PeerId,
        addr: SocketAddr,
        encrypted: bool,
        transport: Transport,
    ) -> ConnectOutcome {
        let duplicate = self
            .peers
            .iter()
            .find(|(k, o)| {
                **k != key && o.connected && o.tx.is_some() && o.peer_id == Some(peer_id)
            })
            .map(|(k, o)| (*k, o.outgoing));
        self.on_peer_set_changed();
        // Another connection to the same peer is the tie-break's to settle
        // (it leaves the IP's count as it was); any other one counts
        // against the per-IP limit. Refused, it never was connected: its
        // end is reported as a failed attempt.
        if duplicate.is_none() && self.over_ip_limit(Some(key), &addr) {
            let Some(p) = self.peers.get_mut(&key) else {
                return ConnectOutcome::Dropped;
            };
            p.close_reason = Some(DisconnectReason::Blocked(BlockReason::PerIpLimit));
            self.emit_blocked(addr, BlockReason::PerIpLimit);
            self.close_peer(key);
            return ConnectOutcome::Dropped;
        }
        let Some(p) = self.peers.get_mut(&key) else {
            return ConnectOutcome::Dropped;
        };
        if p.connecting {
            p.connecting = false;
            release(&self.shared.half_open);
        }
        p.connected = true;
        p.connected_at = Some(Instant::now());
        p.peer_id = Some(peer_id);
        p.encrypted = encrypted;
        p.transport = transport;
        if p.outgoing && self.retries.connected(&p.addr, transport) {
            // TCP-only now; an older PEX `UTP` flag no longer counts.
            if let Some(k) = self.known.get_mut(&p.addr) {
                k.utp = false;
            }
        }
        let Some((old, old_outgoing)) = duplicate else {
            return ConnectOutcome::New;
        };
        // Already connected to this peer: both dialled each other (a
        // simultaneous dial, or both ends of a holepunch), or it
        // reconnected. Both ends agree on which connection stays.
        let keep_new =
            keep_new_connection(&self.shared.peer_id, &peer_id, p.outgoing, old_outgoing);
        debug!(%addr, keep_new, "duplicate peer id; closing one connection");
        let (loser, outcome) = if keep_new {
            (old, ConnectOutcome::Replaced)
        } else {
            (key, ConnectOutcome::Dropped)
        };
        if let Some(l) = self.peers.get_mut(&loser) {
            l.close_reason = Some(DisconnectReason::Duplicate);
        }
        if keep_new {
            // The peer stays connected through the new connection, which
            // reports its end from now on.
            let reported = self.peers.get_mut(&old).and_then(|o| o.reported.take());
            if let Some(n) = self.peers.get_mut(&key) {
                n.reported = reported;
            }
        }
        self.close_peer(loser);
        outcome
    }

    /// Bookkeeping for a closed connection: slots, retries and the known
    /// addresses. `connect_failed` names the transport an outgoing connect
    /// failed over. Returns the peer and the reason to report, or `None`
    /// for an unknown key.
    pub(super) fn on_disconnected(
        &mut self,
        key: PeerKey,
        reason: DisconnectReason,
        connect_failed: Option<Transport>,
    ) -> Option<(Peer, DisconnectReason)> {
        let mut p = self.peers.remove(&key)?;
        self.on_peer_set_changed();
        let reason = match p.close_reason.take() {
            Some(ours) if reason == DisconnectReason::Shutdown => ours,
            _ => reason,
        };
        if p.connecting {
            release(&self.shared.half_open);
        }
        release(&self.shared.global_peers);
        if matches!(reason, DisconnectReason::Blocked(_)) {
            // We refused it: no fallback, retry or rendezvous. An address
            // the filter or a ban blocks is not queued again while it is;
            // one refused for the per-IP limit may be, once there is room.
            if p.outgoing && !p.connected {
                self.retries.clear_transport_failed(&p.addr);
            }
        } else if p.holepunch && p.outgoing && !p.connected {
            // A relay's introduction that did not work out. The address
            // came from a remote: no fallback, no rendezvous, and retries
            // only for an address we knew before (never from a `Connect`
            // alone).
            if reason != DisconnectReason::Shutdown
                && self.known.get(&p.addr).is_some()
                && self.retries.failed(p.addr, Instant::now()) == AfterFailure::GiveUp
            {
                self.known.remove(&p.addr);
                self.candidates.retain(|a| *a != p.addr);
            }
        } else if reason == DisconnectReason::SelfConnection {
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
        } else if let Some(t) = connect_failed.filter(|_| p.outgoing && !p.connected)
            && self.transport_usable(&p.addr, t.other())
            && self.retries.transport_failed(p.addr, t)
        {
            // The first connect failure on this transport: dial the other
            // one at once, without counting an attempt.
            debug!(addr = %p.addr, failed = ?t, "connect failed; retrying over the other transport");
            self.queue_addr(p.addr);
        } else if p.outgoing && !p.connected && reason == DisconnectReason::Shutdown {
            // We closed the attempt: a fallback it was does not carry over.
            self.retries.clear_transport_failed(&p.addr);
        } else if p.outgoing && !p.connected {
            // Failed at connect with no other transport left to try: a
            // relay may still introduce us (before a give-up forgets it).
            if connect_failed.is_some() {
                self.request_rendezvous(p.addr);
            }
            // The connection or handshake failed (we did not close it).
            if self.retries.failed(p.addr, Instant::now()) == AfterFailure::GiveUp {
                // Forgotten: `requeue_known` must not dial it again.
                self.known.remove(&p.addr);
                self.candidates.retain(|a| *a != p.addr);
            }
        } else if p.outgoing
            && p.connected
            && matches!(
                reason,
                DisconnectReason::IdleTimeout | DisconnectReason::Io(_) | DisconnectReason::Closed
            )
            && (p.from_rate.total() > 0 || p.to_rate.total() > 0)
            // A seed or upload-only peer that leaves while we are finished
            // has nothing to trade: `note_seed_gone` keeps it for the
            // moment we stop being finished, which redials it at once (a
            // pending backoff would hold that redial back for minutes).
            && !(self.upload_only_sent && (p.upload_only || p.is_seed()))
        {
            // A peer that traded data and then went away (it closed, its
            // transport timed out, or it went idle) is dialled again after a
            // twelfth of `peer_retry_delay`, and later each time it drops
            // again (`Retries::dropped`).
            // Otherwise only a tracker or PEX naming it again would, and a
            // lone seed lost mid-download stalls it for an announce
            // interval. One that never traded data is left alone, so a peer
            // that accepts and hangs up cannot keep us dialling it.
            debug!(addr = %p.addr, "productive peer dropped; redialling soon");
            let now = Instant::now();
            let connected_for = p
                .connected_at
                .map_or(Duration::ZERO, |at| now.saturating_duration_since(at));
            self.retries.dropped(p.addr, now, connected_for);
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
pub(super) mod tests {
    use super::*;
    use tokio::sync::mpsc;

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

    pub(in crate::torrent) fn addr(i: u32) -> SocketAddr {
        SocketAddr::from((std::net::Ipv4Addr::from(0x0a00_0000 + i), 6881))
    }

    /// The session state of a test actor, with a cache budget of
    /// `cache_size`.
    fn shared(cache_size: u64) -> Arc<crate::session::Shared> {
        use std::sync::atomic::{AtomicBool, AtomicUsize};

        use fbt_tracker::TrackerClient;
        use tokio::sync::broadcast;

        use crate::session::Shared;
        use crate::settings::Settings;

        let settings = Settings {
            dht_enabled: false,
            dht_bootstrap_nodes: Vec::new(),
            ..Settings::default()
        };
        let tracker = TrackerClient::new(settings.tracker.clone()).expect("tracker client");
        Arc::new(Shared {
            settings: Arc::new(settings),
            peer_id: PeerId::generate(),
            listen_port: 6881,
            listen_port_v4: Some(6881),
            listen_port_v6: None,
            tracker,
            events: broadcast::channel(64).0,
            shutdown: AtomicBool::new(false),
            global_peers: AtomicUsize::new(0),
            half_open: AtomicUsize::new(0),
            dht: None,
            dht_addrs: Vec::new(),
            utp: Vec::new(),
            disk: crate::disk::DiskPool::new(1, 1),
            cache: Arc::new(crate::storage::cache::CacheBudget::new(cache_size)),
            bandwidth: Arc::new(crate::bandwidth::Bandwidth::new(&Settings::default())),
            holepunch: Default::default(),
            lsd: crate::net::LsdHub::disabled(),
            portmap: None,
            external_ip: Arc::new(crate::net::ExternalIp::new(broadcast::channel(1).0)),
            ip_filter: Default::default(),
            bans: Default::default(),
        })
    }

    /// An actor for `meta` on `storage`, as spawned (checking).
    fn new_actor(
        meta: fbt_metainfo::Metainfo,
        storage: Option<Arc<dyn crate::storage::Storage>>,
        cache_size: u64,
    ) -> Actor {
        use std::path::PathBuf;
        use std::sync::{OnceLock, Weak};

        use tokio::sync::watch;

        use crate::session::AddTorrent;
        use crate::status::TorrentStatus;

        let mut add = AddTorrent::new(meta, PathBuf::from("unused"));
        add.storage = storage.clone();
        let ih = add.source.info_hashes();
        let (status_tx, _) = watch::channel(TorrentStatus::initial(ih, "a".into(), true));
        let (_cmd_tx, cmd_rx) = mpsc::channel(1);
        Actor::new(
            add,
            storage,
            Arc::new(OnceLock::new()),
            shared(cache_size),
            Weak::new(),
            cmd_rx,
            status_tx,
        )
    }

    /// An actor for a one-piece torrent, with nothing connected.
    pub(in crate::torrent) fn actor() -> Actor {
        use fbt_metainfo::Metainfo;

        let mut bytes =
            b"d4:infod6:lengthi16384e4:name1:a12:piece lengthi16384e6:pieces20:".to_vec();
        bytes.extend_from_slice(&[0u8; 20]);
        bytes.extend_from_slice(b"ee");
        let meta = Metainfo::from_bytes(&bytes).expect("metainfo");
        new_actor(meta, None, 0)
    }

    /// A ready actor (not paused) for a torrent of files of `files` bytes
    /// in pieces of `piece_len`, over zeroed memory storage, with no piece
    /// yet, a picker, and a cache budget of `cache_size`. Nothing is
    /// connected; peers are added as `pex.rs`'s `actor_with_peers` does.
    pub(in crate::torrent) fn actor_with(files: &[u64], piece_len: u32, cache_size: u64) -> Actor {
        use fbt_core::Bitfield;
        use fbt_metainfo::Metainfo;

        use crate::picker::PiecePicker;
        use crate::storage::MemStorage;
        use crate::torrent::actor::Phase;

        let total: u64 = files.iter().sum();
        let pieces = total.div_ceil(u64::from(piece_len)) as usize;
        let mut b = b"d4:infod5:filesl".to_vec();
        for (i, len) in files.iter().enumerate() {
            let name = format!("f{i}");
            b.extend_from_slice(
                format!("d6:lengthi{len}e4:pathl{}:{name}ee", name.len()).as_bytes(),
            );
        }
        b.extend_from_slice(
            format!(
                "e4:name1:t12:piece lengthi{piece_len}e6:pieces{}:",
                pieces * 20
            )
            .as_bytes(),
        );
        b.extend(std::iter::repeat_n(0u8, pieces * 20));
        b.extend_from_slice(b"ee");
        let meta = Metainfo::from_bytes(&b).expect("metainfo");
        let storage: Arc<dyn crate::storage::Storage> = Arc::new(MemStorage::new(total));
        let mut a = new_actor(meta, Some(storage), cache_size);
        let l = a.loaded.as_mut().expect("loaded");
        let mut picker = PiecePicker::new(l.layout, &Bitfield::new(l.layout.num_pieces()));
        picker.set_piece_priorities(&l.piece_prio);
        l.picker = Some(picker);
        a.phase = Phase::Ready;
        a.paused = false;
        a
    }

    /// A TCP connection that got through after a failed uTP connect clears
    /// the address's PEX `UTP` flag every time, also when it was TCP-only
    /// already, so the flag never leaves it uTP-first for good.
    #[tokio::test]
    async fn tcp_success_after_utp_failure_clears_the_pex_utp_flag() {
        let mut a = actor();
        let peer_addr = addr(1);
        assert!(a.remember(peer_addr));
        for round in 0..2 {
            // A PEX `UTP` flag came in (again).
            a.known.get_mut(&peer_addr).expect("known").utp = true;
            assert!(a.retries.transport_failed(peer_addr, Transport::Utp));
            let key = a.new_key();
            let (tx, _rx) = mpsc::channel(4);
            let mut p = Peer::new(peer_addr, tx, a.generation, 1, Transport::Tcp);
            p.outgoing = true;
            a.peers.insert(key, p);
            assert_eq!(
                a.on_connected(
                    key,
                    PeerId([round + 1; 20]),
                    peer_addr,
                    false,
                    Transport::Tcp
                ),
                ConnectOutcome::New
            );
            assert!(a.retries.is_tcp_only(&peer_addr), "round {round}");
            assert!(
                !a.known.get(&peer_addr).expect("known").utp,
                "round {round}"
            );
            a.peers.remove(&key);
        }
    }

    /// A failed holepunch dial to an address we never knew (it came only
    /// in a relay's `Connect`) leaves nothing behind: no fallback, no retry,
    /// nothing queued. One to a known address is an ordinary failure, still
    /// without a fallback.
    #[tokio::test]
    async fn failed_holepunch_dial_never_feeds_retries() {
        let mut a = actor();
        let stranger = addr(1);
        let known = addr(2);
        assert!(a.remember(known));
        for target in [stranger, known] {
            let key = a.new_key();
            let (tx, _rx) = mpsc::channel(4);
            let mut p = Peer::new(target, tx, a.generation, 1, Transport::Utp);
            p.outgoing = true;
            p.holepunch = true;
            a.peers.insert(key, p);
            a.on_disconnected(
                key,
                DisconnectReason::Io("connect timed out".into()),
                Some(Transport::Utp),
            );
        }
        assert!(!a.retries.is_pending(&stranger));
        assert_eq!(a.retries.fallback_transport(&stranger), None);
        assert!(a.known.get(&stranger).is_none());
        assert!(a.candidates.is_empty(), "nothing queued");
        assert!(a.retries.is_pending(&known), "a known address retries");
        assert_eq!(a.retries.fallback_transport(&known), None, "no fallback");
    }

    /// A productive seed that closes us while we are upload-only gets no
    /// drop backoff (it is kept for the redial when we stop being
    /// finished); while we are not, it does.
    #[tokio::test]
    async fn seed_closing_an_upload_only_torrent_gets_no_drop_backoff() {
        for upload_only in [true, false] {
            let mut a = actor();
            a.upload_only_sent = upload_only;
            let seed_addr = addr(1);
            assert!(a.remember(seed_addr));
            let key = a.new_key();
            let (tx, _rx) = mpsc::channel(4);
            let mut p = Peer::new(seed_addr, tx, a.generation, 1, Transport::Tcp);
            p.outgoing = true;
            p.connected = true;
            p.seed = true;
            p.from_rate.record(16_384);
            a.peers.insert(key, p);
            a.on_disconnected(key, DisconnectReason::Closed, None);
            assert_eq!(
                a.retries.is_pending(&seed_addr),
                !upload_only,
                "upload-only sent: {upload_only}"
            );
        }
    }

    /// `PeerDisconnected` stays paired with `PeerConnected`: an attempt
    /// that never got through (a holepunch dial losing the race) says
    /// nothing while another connection to that address is the reported
    /// one; that one reports its own end, and a failed attempt to an
    /// address nobody reported still reports its failure.
    #[tokio::test]
    async fn failed_attempt_to_a_reported_address_stays_silent() {
        use crate::event::Event;
        use crate::peer::messages::PeerEvent;
        use crate::torrent::actor::EMITTED;

        let disconnects = || -> Vec<(SocketAddr, DisconnectReason)> {
            EMITTED
                .take()
                .into_iter()
                .filter_map(|(e, _)| match e {
                    Event::PeerDisconnected { addr, reason, .. } => Some((addr, reason)),
                    _ => None,
                })
                .collect()
        };
        let gone = |key: PeerKey, reason: DisconnectReason, failed: Option<Transport>| {
            PeerEvent::Disconnected {
                key,
                reason,
                outstanding: Vec::new(),
                connect_failed: failed,
            }
        };
        let timeout = DisconnectReason::Io("uTP connect timed out".into());
        let mut a = actor();
        let c = addr(1);
        let (tx, _rx1) = mpsc::channel(4);
        let live = a.new_key();
        let mut p = Peer::new(c, tx, a.generation, 1, Transport::Utp);
        p.connected = true;
        p.reported = Some(c);
        a.peers.insert(live, p);
        let (tx, _rx2) = mpsc::channel(4);
        let attempt = a.new_key();
        let mut p = Peer::new(c, tx, a.generation, 1, Transport::Utp);
        p.outgoing = true;
        p.holepunch = true;
        a.peers.insert(attempt, p);
        EMITTED.take();

        a.on_peer_event(gone(attempt, timeout.clone(), Some(Transport::Utp)));
        assert_eq!(disconnects(), vec![], "the losing attempt is silent");
        a.on_peer_event(gone(live, DisconnectReason::Closed, None));
        assert_eq!(disconnects(), vec![(c, DisconnectReason::Closed)]);

        // Nobody reported for the address: the failure is reported.
        let (tx, _rx3) = mpsc::channel(4);
        let lone = a.new_key();
        let mut p = Peer::new(c, tx, a.generation, 1, Transport::Utp);
        p.outgoing = true;
        a.peers.insert(lone, p);
        a.on_peer_event(gone(lone, timeout.clone(), Some(Transport::Utp)));
        assert_eq!(disconnects(), vec![(c, timeout)]);
    }
}
