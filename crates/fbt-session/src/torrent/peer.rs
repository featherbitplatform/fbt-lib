//! One peer connection's state.

use std::collections::HashSet;
use std::net::SocketAddr;

use fbt_core::{Bitfield, BlockRef, PeerId, display_safe};
use fbt_wire::{ExtHandshake, UT_HOLEPUNCH};
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::pex::PexPeer;
use crate::event::PeerSource;
use crate::peer::DisconnectReason;
use crate::peer::conn::MAX_ALLOWED_FAST;
use crate::peer::messages::PeerCommand;
use crate::peer::transport::Transport;
use crate::picker::PeerPieces;
use crate::rate::RateMeter;
use crate::status::PeerStatus;

/// Most characters of a peer's client name kept in a [`PeerStatus`].
pub(crate) const MAX_CLIENT_CHARS: usize = 64;

/// Azureus-style client codes (`-XXnnnn-`) we name.
const AZUREUS_CLIENTS: &[(&[u8; 2], &str)] = &[
    (b"AZ", "Vuze"),
    (b"BI", "BiglyBT"),
    (b"BT", "BitTorrent"),
    (b"DE", "Deluge"),
    (b"FB", "fbt"),
    (b"FD", "Free Download Manager"),
    (b"KT", "KTorrent"),
    (b"LT", "libtorrent"),
    (b"lt", "libTorrent"),
    (b"PI", "PicoTorrent"),
    (b"qB", "qBittorrent"),
    (b"SD", "Thunder"),
    (b"TR", "Transmission"),
    (b"UM", "uTorrent Mac"),
    (b"UT", "uTorrent"),
    (b"UW", "uTorrent Web"),
    (b"WW", "WebTorrent"),
    (b"XL", "Xunlei"),
];

/// One Azureus version character: `0-9`, then `A-Z` for 10 to 35.
fn version_digit(c: u8) -> Option<u32> {
    match c {
        b'0'..=b'9' => Some(u32::from(c - b'0')),
        b'A'..=b'Z' => Some(u32::from(c - b'A') + 10),
        _ => None,
    }
}

/// The client an Azureus-style peer id (`-XXnnnn-`, BEP 20) names, as
/// "name major.minor.patch" (".build" added when not 0); `None` for any
/// other id and for a client code we do not know.
pub(crate) fn azureus_client(id: &[u8; 20]) -> Option<String> {
    let [b'-', c0, c1, v0, v1, v2, v3, b'-', ..] = *id else {
        return None;
    };
    let (_, name) = AZUREUS_CLIENTS.iter().find(|(c, _)| **c == [c0, c1])?;
    let [a, b, c, d] = [
        version_digit(v0)?,
        version_digit(v1)?,
        version_digit(v2)?,
        version_digit(v3)?,
    ];
    Some(if d == 0 {
        format!("{name} {a}.{b}.{c}")
    } else {
        format!("{name} {a}.{b}.{c}.{d}")
    })
}

/// A client name from a peer, safe to show: [`display_safe`] at
/// [`MAX_CLIENT_CHARS`] characters, so control and format (bidi, zero-width)
/// characters are gone, separators are single spaces and the name is short;
/// `None` when nothing is left.
pub(crate) fn sanitise_client(raw: &str) -> Option<String> {
    let clean = display_safe(raw, MAX_CLIENT_CHARS);
    (!clean.is_empty()).then_some(clean)
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
    /// A v2 connection (the v2 reserved bit, or the truncated v2 hash): a
    /// source for the layer fetch.
    pub v2: bool,
    /// Where we learnt of it.
    pub source: PeerSource,
}

impl Peer {
    pub(super) fn new(
        addr: SocketAddr,
        tx: mpsc::Sender<PeerCommand>,
        generation: u64,
        num_pieces: u32,
        transport: Transport,
        source: PeerSource,
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
            v2: false,
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
            source,
        }
    }

    /// Its [`PeerStatus`] at `now`.
    pub(super) fn status(&self, now: Instant) -> PeerStatus {
        let len = self.bits.len();
        let progress = if self.seed {
            1.0
        } else if len == 0 {
            0.0
        } else {
            f64::from(self.bits.count_ones()) / f64::from(len)
        };
        let client = self
            .ext
            .as_ref()
            .and_then(|e| e.client.as_deref())
            .and_then(sanitise_client)
            .or_else(|| self.peer_id.and_then(|id| azureus_client(&id.0)));
        PeerStatus {
            addr: self.addr,
            client,
            transport: self.transport,
            encrypted: self.encrypted,
            incoming: !self.outgoing,
            download_rate: self.from_rate.rate(now),
            upload_rate: self.to_rate.rate(now),
            total_downloaded: self.from_rate.total(),
            total_uploaded: self.to_rate.total(),
            peer_choking: self.choked_us,
            peer_interested: self.peer_interested,
            am_choking: self.am_choking,
            am_interested: self.am_interested,
            progress,
            source: self.source,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn id(prefix: &[u8]) -> [u8; 20] {
        let mut id = [b'x'; 20];
        for (d, s) in id.iter_mut().zip(prefix) {
            *d = *s;
        }
        id
    }

    #[test]
    fn azureus_client_ids() {
        assert_eq!(
            azureus_client(&id(b"-qB4650-")).as_deref(),
            Some("qBittorrent 4.6.5")
        );
        assert_eq!(
            azureus_client(&id(b"-TR4060-")).as_deref(),
            Some("Transmission 4.0.6")
        );
        assert_eq!(
            azureus_client(&id(b"-LT20A0-")).as_deref(),
            Some("libtorrent 2.0.10")
        );
        assert_eq!(
            azureus_client(&id(b"-lt0D80-")).as_deref(),
            Some("libTorrent 0.13.8")
        );
        assert_eq!(
            azureus_client(&id(b"-DE2112-")).as_deref(),
            Some("Deluge 2.1.1.2")
        );
        assert_eq!(
            azureus_client(&id(b"-FB0100-")).as_deref(),
            Some("fbt 0.1.0")
        );
        // Not Azureus style: Shad0w style, random bytes, a broken frame.
        assert_eq!(azureus_client(&id(b"M7-2-2--")), None);
        assert_eq!(azureus_client(&[0xff; 20]), None);
        assert_eq!(azureus_client(&id(b"-qB4650x")), None);
        assert_eq!(azureus_client(&id(b"-qB4\x0050-")), None);
        // An unknown client code.
        assert_eq!(azureus_client(&id(b"-ZZ1000-")), None);
    }

    #[test]
    fn client_names_are_sanitised() {
        // `display_safe` at `MAX_CLIENT_CHARS`: control and format characters
        // go, and a separator (here `\n`) becomes one space rather than gluing
        // the words together.
        assert_eq!(
            sanitise_client("evil\u{1b}[31m\nname\u{7f}").as_deref(),
            Some("evil[31m name")
        );
        assert_eq!(sanitise_client(" \t\r\n "), None);
        // Bidi overrides and isolates, zero-width characters, BOM.
        assert_eq!(
            sanitise_client("a\u{202E}b\u{202A}c\u{2066}d\u{2069}e").as_deref(),
            Some("abcde")
        );
        assert_eq!(
            sanitise_client("x\u{200B}y\u{200D}z\u{200F}\u{FEFF}").as_deref(),
            Some("xyz")
        );
        assert_eq!(sanitise_client("\u{202E}\u{200B}"), None);
        // Other non-ASCII text stays.
        assert_eq!(
            sanitise_client("Überclient µ").as_deref(),
            Some("Überclient µ")
        );
        let long = "a".repeat(500);
        assert_eq!(
            sanitise_client(&long).map(|s| s.chars().count()),
            Some(MAX_CLIENT_CHARS)
        );
    }
}
