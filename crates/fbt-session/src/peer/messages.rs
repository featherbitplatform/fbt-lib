//! Messages between a peer task and its torrent.

use std::net::SocketAddr;

use bytes::Bytes;
use fbt_core::{Bitfield, BlockRef, PeerId};
use fbt_wire::Reserved;

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
    /// The remote broke the wire protocol.
    Protocol(String),
    /// Transport error, including connect failures and stalled writes.
    Io(String),
    /// The remote closed the connection.
    Closed,
    /// We closed the connection (`PeerCommand::Close` or the torrent went away).
    Shutdown,
    /// We closed it because the same peer id is already connected to the
    /// torrent; the older connection stays.
    Duplicate,
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
    },
    /// The remote's initial piece set.
    Pieces {
        key: PeerKey,
        pieces: PeerPiecesOwned,
    },
    /// The remote completed a piece (index already range-checked).
    Have { key: PeerKey, piece: u32 },
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
    /// The remote requests a block (not validated).
    Request { key: PeerKey, block: BlockRef },
    /// The remote cancels an earlier request.
    Cancel { key: PeerKey, block: BlockRef },
    /// Payload bytes dropped because they matched no outstanding request.
    Wasted { key: PeerKey, bytes: u64 },
    /// The connection ended; `outstanding` lists requests never satisfied.
    Disconnected {
        key: PeerKey,
        reason: DisconnectReason,
        outstanding: Vec<BlockRef>,
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
    /// Answer a queued request.
    SendBlock { block: BlockRef, data: Bytes },
    /// Refuse a queued request.
    Reject(BlockRef),
    /// Close the connection.
    Close,
}
