//! Messages between a peer task and its torrent.

use std::net::SocketAddr;

use bytes::Bytes;
use fbt_core::{Bitfield, BlockRef, PeerId, PieceLayout};
use fbt_wire::{ExtHandshake, HashRequest, HolepunchMsg, MetadataMsg, PexMsg, Reserved};

use super::ext::ExtConfig;
use super::transport::Transport;
use crate::bans::BlockReason;
use crate::picker::PeerKey;

/// Why a peer connection ended.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DisconnectReason {
    /// The remote peer id is our own.
    SelfConnection,
    /// The remote handshake names another torrent.
    InfoHashMismatch,
    /// The handshake did not complete in time.
    HandshakeTimeout,
    /// The remote sent nothing for the idle timeout.
    IdleTimeout,
    /// The MSE/PE handshake failed or was refused.
    Encryption(String),
    /// The remote broke the wire protocol.
    Protocol(String),
    /// Transport error, including connect failures and stalled writes.
    Io(String),
    /// The remote closed the connection.
    Closed,
    /// We closed the connection (`PeerCommand::Close` or the torrent went away).
    Shutdown,
    /// We closed it because the same peer id is already connected to the
    /// torrent and the tie-break kept the other connection. Not reported in
    /// events: the peer stays connected through the other one.
    Duplicate,
    /// The peer advertised an unacceptable metadata size or sent metadata
    /// that is inconsistent or fails the info-hash check; it is never
    /// dialled again.
    BadMetadata,
    /// We are finished and the peer is a seed or upload-only (BEP 21):
    /// neither side has anything to gain.
    UploadOnly,
    /// We closed it: the IP filter blocks it (set while it was connected),
    /// it is banned, or its IP has too many connections to the torrent.
    Blocked(BlockReason),
}

/// The pieces a remote announced, owned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PeerPiecesOwned {
    /// `HaveAll`.
    All,
    /// `HaveNone`.
    None,
    /// A validated `bitfield`.
    Bits(Bitfield),
}

/// Peer task to torrent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PeerEvent {
    /// The handshake succeeded.
    Connected {
        key: PeerKey,
        peer_id: PeerId,
        reserved: Reserved,
        addr: SocketAddr,
        /// RC4-encrypted (MSE); false for plaintext, MSE-negotiated or not.
        encrypted: bool,
        /// The transport the connection runs over.
        transport: Transport,
    },
    /// The remote's initial piece set.
    Pieces {
        key: PeerKey,
        pieces: PeerPiecesOwned,
    },
    /// The remote completed a piece (index already range-checked).
    Have { key: PeerKey, piece: u32 },
    /// The remote retracted a piece (BEP 54 `lt_donthave`; index already
    /// range-checked).
    DontHave { key: PeerKey, piece: u32 },
    /// The remote is upload-only (BEP 21), from its extension handshake or
    /// an `upload_only` message.
    UploadOnly { key: PeerKey, on: bool },
    /// The remote choked us.
    Choked { key: PeerKey },
    /// The remote unchoked us.
    Unchoked { key: PeerKey },
    /// The remote is interested in our pieces.
    Interested { key: PeerKey },
    /// The remote is no longer interested.
    NotInterested { key: PeerKey },
    /// A block we requested arrived.
    Block {
        key: PeerKey,
        block: BlockRef,
        data: Bytes,
    },
    /// A request of ours will not be served (rejected, choked away or timed out).
    Rejected { key: PeerKey, block: BlockRef },
    /// The remote lets us request this piece while choked.
    AllowedFast { key: PeerKey, piece: u32 },
    /// The pipeline has room for this many more requests.
    WantBlocks { key: PeerKey, count: usize },
    /// The remote left our requests unanswered for `snub_timeout`
    /// (`snubbed: true`), or a block arrived after that (`false`); sent on
    /// every change.
    Snubbed { key: PeerKey, snubbed: bool },
    /// The remote requests a block (not validated).
    Request { key: PeerKey, block: BlockRef },
    /// A `Piece` message answering the remote's request for `block` was
    /// written and flushed: `len` payload bytes uploaded.
    BlockSent {
        key: PeerKey,
        block: BlockRef,
        len: u64,
    },
    /// The remote cancels an earlier request.
    Cancel { key: PeerKey, block: BlockRef },
    /// Payload bytes dropped because they matched no outstanding request.
    Wasted { key: PeerKey, bytes: u64 },
    /// The remote's BEP 10 extension handshake (again after each repeat).
    /// Every field is untrusted: `metadata_size` is not capped here (the
    /// metadata fetch applies `max_metadata_size`), `reqq` is clamped to
    /// [`MAX_REMOTE_REQQ`](super::ext::MAX_REMOTE_REQQ).
    ExtHandshake { key: PeerKey, hs: Box<ExtHandshake> },
    /// A BEP 9 `ut_metadata` message.
    Metadata { key: PeerKey, msg: MetadataMsg },
    /// A BEP 11 `ut_pex` message.
    Pex { key: PeerKey, msg: PexMsg },
    /// A BEP 55 `ut_holepunch` message (only while we advertise it).
    Holepunch { key: PeerKey, msg: HolepunchMsg },
    /// The remote's DHT port (BEP 5 `Port`), at most once per connection.
    DhtPort { key: PeerKey, port: u16 },
    /// A BEP 52 `hash request` (from a v2 connection; not
    /// validated).
    HashRequest { key: PeerKey, req: HashRequest },
    /// BEP 52 `hashes` (from a v2 connection; a whole number
    /// of 32-byte hashes, unverified).
    Hashes {
        key: PeerKey,
        req: HashRequest,
        hashes: Bytes,
    },
    /// A BEP 52 `hash reject` (from a v2 connection).
    HashReject { key: PeerKey, req: HashRequest },
    /// The connection ended; `outstanding` lists requests never satisfied.
    Disconnected {
        key: PeerKey,
        reason: DisconnectReason,
        outstanding: Vec<BlockRef>,
        /// An outgoing connection could not be made over this transport
        /// (TCP refused or timed out, a uTP connect timed out or was
        /// reset): the torrent may retry over the other one. `None` for
        /// every other end, a failed handshake included.
        connect_failed: Option<Transport>,
    },
}

/// Torrent to peer task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PeerCommand {
    /// Send these requests.
    Request(Vec<BlockRef>),
    /// Cancel an outstanding request.
    Cancel(BlockRef),
    /// Choke the remote.
    Choke,
    /// Unchoke the remote.
    Unchoke,
    /// Change our interest.
    Interested(bool),
    /// Announce a piece we completed.
    Have(u32),
    /// Announce several pieces at once: those completed while the
    /// connection was being set up, after its initial bitfield was taken.
    Haves(Vec<u32>),
    /// Tell the remote we are (or are no longer) upload-only (BEP 21);
    /// dropped if it lacks the extension.
    UploadOnly(bool),
    /// Tell the remote we lost a piece (BEP 54); dropped if it lacks the
    /// extension.
    DontHave(u32),
    /// Answer a queued request: the block waits in the connection's upload
    /// queue for its rate limit, and is dropped there if cancelled or
    /// choked away first.
    SendBlock { block: BlockRef, data: Bytes },
    /// Refuse a queued request.
    Reject(BlockRef),
    /// The metadata arrived: validate and replay the buffered piece
    /// announcements against `layout`, then run normally. `ext` is what we
    /// advertise now (our `metadata_size`, and `ut_pex` off for a private
    /// torrent): a remote speaking BEP 10 gets a repeated handshake.
    MetadataReady { layout: PieceLayout, ext: ExtConfig },
    /// Answer a `hash request` with `hashes` (base layer, then uncles;
    /// a whole number of 32-byte hashes).
    SendHashes { req: HashRequest, hashes: Bytes },
    /// Refuse a `hash request`.
    RejectHashes(HashRequest),
    /// Ask for hashes of a piece layer (BEP 52 `hash request`); sent only
    /// to a v2 connection.
    RequestHashes(HashRequest),
    /// Send a `ut_metadata` message (dropped if the remote lacks it).
    SendMetadata(MetadataMsg),
    /// Send a `ut_pex` message (dropped if the remote lacks it).
    SendPex(PexMsg),
    /// Send a `ut_holepunch` message (dropped if the remote lacks it).
    SendHolepunch(HolepunchMsg),
    /// Close the connection.
    Close,
}
