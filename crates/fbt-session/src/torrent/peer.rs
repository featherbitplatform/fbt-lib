//! One peer connection's state.

use std::collections::HashSet;
use std::net::SocketAddr;

use fbt_core::{Bitfield, BlockRef, PeerId};
use fbt_wire::{ExtHandshake, UT_HOLEPUNCH};
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::pex::PexPeer;
use crate::peer::DisconnectReason;
use crate::peer::conn::MAX_ALLOWED_FAST;
use crate::peer::messages::PeerCommand;
use crate::peer::transport::Transport;
use crate::picker::PeerPieces;
use crate::rate::RateMeter;

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
    /// The peer task reports our requests to it unanswered for
    /// `snub_timeout`.
    pub snubbed: bool,
    /// Since when we unchoke it; `None` while choked.
    pub unchoked_since: Option<Instant>,
    /// Payload sent to it (`BlockSent`) since it was last unchoked.
    pub uploaded_since_unchoke: u64,
    /// When it last held an optimistic upload slot.
    pub last_optimistic: Option<Instant>,
    /// The peer's latest BEP 10 extension handshake (untrusted values).
    pub ext: Option<Box<ExtHandshake>>,
    /// The listen port from that handshake (`p`).
    pub listen_port: Option<u16>,
    /// The connection is RC4-encrypted (for PEX flags).
    pub encrypted: bool,
    /// The transport it is dialled or accepted over.
    pub transport: Transport,
    /// When the handshake finished.
    pub connected_at: Option<Instant>,
    /// `ut_metadata` requests it sent (served up to
    /// [`MAX_METADATA_REQUESTS_PER_PEER`](super::metadata::MAX_METADATA_REQUESTS_PER_PEER)).
    pub metadata_requests: u32,
    /// Its `hash request`s being served (at most
    /// [`MAX_PENDING_HASH_REQUESTS`](super::layers::MAX_PENDING_HASH_REQUESTS)).
    pub hash_requests: usize,
    /// PEX state; `Some` while PEX runs with this peer (it advertised
    /// `ut_pex`, the setting is on and the torrent is not private).
    pub pex: Option<PexPeer>,
    /// The peer said it is upload-only (BEP 21).
    pub upload_only: bool,
    /// The address its `PeerConnected` named, `Some` while this
    /// connection is the one the session reported for the peer: it then
    /// reports its end. A tie-break that replaces a connection with a newer
    /// one to the same peer moves it to the newer one.
    pub reported: Option<SocketAddr>,
    /// Dialled on a relay's BEP 55 `Connect`: a failure neither falls back
    /// to TCP nor asks for a rendezvous, and is retried only for an address
    /// we knew already.
    pub holepunch: bool,
}

impl Peer {
    pub(super) fn new(
        addr: SocketAddr,
        tx: mpsc::Sender<PeerCommand>,
        generation: u64,
        num_pieces: u32,
        transport: Transport,
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
            snubbed: false,
            unchoked_since: None,
            uploaded_since_unchoke: 0,
            last_optimistic: None,
            ext: None,
            listen_port: None,
            encrypted: false,
            transport,
            connected_at: None,
            metadata_requests: 0,
            hash_requests: 0,
            pex: None,
            upload_only: false,
            reported: None,
            holepunch: false,
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

    /// Its extension handshake advertised `ut_holepunch` (BEP 55).
    pub(super) fn supports_holepunch(&self) -> bool {
        self.ext
            .as_ref()
            .is_some_and(|h| h.id_of(UT_HOLEPUNCH).is_some())
    }

    /// Connected and not closed.
    pub(super) fn live(&self) -> bool {
        self.connected && self.tx.is_some()
    }

    /// Connected, not closed, and we do not choke it.
    pub(super) fn unchoked(&self) -> bool {
        self.connected && self.tx.is_some() && !self.am_choking
    }
}
