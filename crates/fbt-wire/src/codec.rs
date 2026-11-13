//! Length-prefixed frame codec.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use fbt_core::BlockRef;
use tokio_util::codec::{Decoder, Encoder};

use crate::{HashRequest, Message};

/// Codec errors.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    /// Length prefix exceeds the configured maximum.
    #[error("frame length {len} exceeds maximum {max}")]
    TooLong {
        /// Announced length.
        len: u32,
        /// Configured maximum.
        max: u32,
    },
    /// Frame length is invalid for its message id. `len` is the length prefix
    /// (id byte plus payload).
    #[error("invalid length {len} for message id {id}")]
    BadLength {
        /// Message id.
        id: u8,
        /// Length prefix value.
        len: u32,
    },
    /// Outgoing message does not fit in a `u32` length prefix.
    #[error("message too large to encode")]
    Oversized,
    /// Transport error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Encodes and decodes [`Message`] frames.
#[derive(Debug, Clone, Copy)]
pub struct MessageCodec {
    max_len: u32,
}

impl MessageCodec {
    /// `max_len` bounds the length prefix (id byte plus payload) on decode.
    pub fn new(max_len: u32) -> Self {
        Self { max_len }
    }
}

fn block(body: &mut Bytes) -> BlockRef {
    BlockRef {
        piece: body.get_u32(),
        offset: body.get_u32(),
        len: body.get_u32(),
    }
}

fn put_block(dst: &mut BytesMut, id: u8, b: BlockRef) {
    dst.put_u32(13);
    dst.put_u8(id);
    dst.put_u32(b.piece);
    dst.put_u32(b.offset);
    dst.put_u32(b.len);
}

/// Reads the 48-byte header; the caller has checked `body.len() >= 48`.
fn hash_request(body: &mut Bytes) -> HashRequest {
    let mut pieces_root = [0u8; 32];
    body.copy_to_slice(&mut pieces_root);
    HashRequest {
        pieces_root,
        base_layer: body.get_u32(),
        index: body.get_u32(),
        length: body.get_u32(),
        proof_layers: body.get_u32(),
    }
}

fn put_hash_request(dst: &mut BytesMut, r: &HashRequest) {
    dst.put_slice(&r.pieces_root);
    dst.put_u32(r.base_layer);
    dst.put_u32(r.index);
    dst.put_u32(r.length);
    dst.put_u32(r.proof_layers);
}

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = WireError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Message>, WireError> {
        let Some((prefix, _)) = src.split_first_chunk::<4>() else {
            return Ok(None);
        };
        let len = u32::from_be_bytes(*prefix);
        if len > self.max_len {
            return Err(WireError::TooLong {
                len,
                max: self.max_len,
            });
        }
        let total = 4 + len as usize;
        if src.len() < total {
            src.reserve(total - src.len());
            return Ok(None);
        }
        src.advance(4);
        if len == 0 {
            return Ok(Some(Message::KeepAlive));
        }
        let mut body = src.split_to(len as usize).freeze();
        let id = body.get_u8();
        let bad = || WireError::BadLength { id, len };
        // `body` now holds len - 1 payload bytes; every get_* below is
        // preceded by an exact or minimum length check.
        let payload = body.len();
        let msg = match id {
            0..=3 | 14 | 15 => {
                if payload != 0 {
                    return Err(bad());
                }
                match id {
                    0 => Message::Choke,
                    1 => Message::Unchoke,
                    2 => Message::Interested,
                    3 => Message::NotInterested,
                    14 => Message::HaveAll,
                    _ => Message::HaveNone,
                }
            }
            4 | 13 | 17 => {
                if payload != 4 {
                    return Err(bad());
                }
                let n = body.get_u32();
                match id {
                    4 => Message::Have(n),
                    13 => Message::SuggestPiece(n),
                    _ => Message::AllowedFast(n),
                }
            }
            5 => Message::Bitfield(body),
            6 | 8 | 16 => {
                if payload != 12 {
                    return Err(bad());
                }
                let b = block(&mut body);
                match id {
                    6 => Message::Request(b),
                    8 => Message::Cancel(b),
                    _ => Message::RejectRequest(b),
                }
            }
            7 => {
                if payload < 8 {
                    return Err(bad());
                }
                let piece = body.get_u32();
                let offset = body.get_u32();
                Message::Piece {
                    piece,
                    offset,
                    data: body,
                }
            }
            9 => {
                if payload != 2 {
                    return Err(bad());
                }
                Message::Port(body.get_u16())
            }
            20 => {
                if payload < 1 {
                    return Err(bad());
                }
                let ext = body.get_u8();
                Message::Extended {
                    id: ext,
                    payload: body,
                }
            }
            21 | 23 => {
                if payload != 48 {
                    return Err(bad());
                }
                let r = hash_request(&mut body);
                if id == 21 {
                    Message::HashRequest(r)
                } else {
                    Message::HashReject(r)
                }
            }
            22 => {
                if payload < 48 || !(payload - 48).is_multiple_of(32) {
                    return Err(bad());
                }
                let req = hash_request(&mut body);
                Message::Hashes { req, hashes: body }
            }
            _ => Message::Unknown { id, payload: body },
        };
        Ok(Some(msg))
    }
}

impl Encoder<Message> for MessageCodec {
    type Error = WireError;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), WireError> {
        // Frame length = id byte + payload.
        let frame = |payload: usize| u32::try_from(payload + 1).map_err(|_| WireError::Oversized);
        match item {
            Message::KeepAlive => dst.put_u32(0),
            Message::Choke => dst.put_slice(&[0, 0, 0, 1, 0]),
            Message::Unchoke => dst.put_slice(&[0, 0, 0, 1, 1]),
            Message::Interested => dst.put_slice(&[0, 0, 0, 1, 2]),
            Message::NotInterested => dst.put_slice(&[0, 0, 0, 1, 3]),
            Message::HaveAll => dst.put_slice(&[0, 0, 0, 1, 14]),
            Message::HaveNone => dst.put_slice(&[0, 0, 0, 1, 15]),
            Message::Have(n) | Message::SuggestPiece(n) | Message::AllowedFast(n) => {
                let id = match item {
                    Message::Have(_) => 4,
                    Message::SuggestPiece(_) => 13,
                    _ => 17,
                };
                dst.put_u32(5);
                dst.put_u8(id);
                dst.put_u32(n);
            }
            Message::Bitfield(b) => {
                dst.put_u32(frame(b.len())?);
                dst.put_u8(5);
                dst.put_slice(&b);
            }
            Message::Request(b) => put_block(dst, 6, b),
            Message::Cancel(b) => put_block(dst, 8, b),
            Message::RejectRequest(b) => put_block(dst, 16, b),
            Message::Piece {
                piece,
                offset,
                data,
            } => {
                dst.put_u32(frame(8 + data.len())?);
                dst.put_u8(7);
                dst.put_u32(piece);
                dst.put_u32(offset);
                dst.put_slice(&data);
            }
            Message::Port(p) => {
                dst.put_u32(3);
                dst.put_u8(9);
                dst.put_u16(p);
            }
            Message::Extended { id, payload } => {
                dst.put_u32(frame(1 + payload.len())?);
                dst.put_u8(20);
                dst.put_u8(id);
                dst.put_slice(&payload);
            }
            Message::HashRequest(r) | Message::HashReject(r) => {
                let id = if matches!(item, Message::HashRequest(_)) {
                    21
                } else {
                    23
                };
                dst.put_u32(49);
                dst.put_u8(id);
                put_hash_request(dst, &r);
            }
            Message::Hashes { req, hashes } => {
                dst.put_u32(frame(48 + hashes.len())?);
                dst.put_u8(22);
                put_hash_request(dst, &req);
                dst.put_slice(&hashes);
            }
            Message::Unknown { id, payload } => {
                dst.put_u32(frame(payload.len())?);
                dst.put_u8(id);
                dst.put_slice(&payload);
            }
        }
        Ok(())
    }
}
