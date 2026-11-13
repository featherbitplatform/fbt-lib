//! Peer wire messages (BEP 3, 5, 6, 10).

use bytes::Bytes;
use fbt_core::BlockRef;

/// Largest `length` a BEP 52 hash request may ask for (hashes per request).
pub const MAX_HASH_REQUEST_LEN: u32 = 8192;

/// The 48-byte header shared by BEP 52 hash request, hashes and hash reject.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HashRequest {
    /// Root of the file's merkle tree (`pieces root`).
    pub pieces_root: [u8; 32],
    /// Layer the hashes are requested from (0 = leaves).
    pub base_layer: u32,
    /// Index of the first hash within the layer.
    pub index: u32,
    /// Number of hashes requested (a power of two, judged by the session).
    pub length: u32,
    /// Number of uncle-proof layers requested.
    pub proof_layers: u32,
}

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
    /// Id 21 (BEP 52).
    HashRequest(HashRequest),
    /// Id 22 (BEP 52).
    Hashes {
        /// The request being answered.
        req: HashRequest,
        /// The 32-byte hashes after the header: base layer, then uncles.
        hashes: Bytes,
    },
    /// Id 23 (BEP 52).
    HashReject(HashRequest),
    /// Any other id, kept verbatim.
    Unknown {
        /// Message id.
        id: u8,
        /// Raw payload.
        payload: Bytes,
    },
}
