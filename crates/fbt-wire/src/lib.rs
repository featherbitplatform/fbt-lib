//! fbt-wire: Peer wire protocol messages and codecs.
//!
//! Everything here decodes bytes from untrusted peers: no panics, and no
//! buffering beyond the configured maximum frame length.

#![forbid(unsafe_code)]
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic
    )
)]

mod codec;
mod ext;
mod fast;
mod handshake;
mod message;

pub use codec::{MessageCodec, WireError};
pub use ext::{
    EXT_HANDSHAKE_ID, ExtError, ExtHandshake, MAX_EXT_HANDSHAKE_LEN, MAX_PEX_ENTRIES,
    METADATA_PIECE_LEN, MetadataMsg, OUR_UT_METADATA_ID, OUR_UT_PEX_ID, PexFlags, PexMsg,
    UT_METADATA, UT_PEX,
};
pub use fast::allowed_fast_set;
pub use handshake::{
    HANDSHAKE_LEN, Handshake, HandshakeError, Reserved, read_handshake, write_handshake,
};
pub use message::Message;
