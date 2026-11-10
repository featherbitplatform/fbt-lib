//! Peer wire messages (BEP 3, 5, 6, 10).

use bytes::Bytes;
use fbt_core::BlockRef;

/// One decoded peer wire message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Zero-length frame.
    KeepAlive,
    /// Id 0.
    Choke,
    /// Id 1.
    Unchoke,
    /// Id 2.
    Interested,
    /// Id 3.
    NotInterested,
    /// Id 4.
    Have(u32),
    /// Id 5.
    Bitfield(Bytes),
    /// Id 6.
    Request(BlockRef),
    /// Id 7.
    Piece {
        /// Piece index.
        piece: u32,
        /// Offset within the piece.
        offset: u32,
        /// Block data.
        data: Bytes,
    },
    /// Id 8.
    Cancel(BlockRef),
    /// Id 9.
    Port(u16),
    /// Id 13 (BEP 6).
    SuggestPiece(u32),
    /// Id 14 (BEP 6).
    HaveAll,
    /// Id 15 (BEP 6).
    HaveNone,
    /// Id 16 (BEP 6).
    RejectRequest(BlockRef),
    /// Id 17 (BEP 6).
    AllowedFast(u32),
    /// Id 20 (BEP 10).
    Extended {
        /// Extension message id.
        id: u8,
        /// Payload after the extension id.
        payload: Bytes,
    },
    /// Any other id, kept verbatim.
    Unknown {
        /// Message id.
        id: u8,
        /// Raw payload.
        payload: Bytes,
    },
}
