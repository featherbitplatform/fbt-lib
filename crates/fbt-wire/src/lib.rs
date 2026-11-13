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
mod mse;

pub use codec::{MessageCodec, WireError};
pub use ext::{
    EXT_HANDSHAKE_ID, ExtError, ExtHandshake, HP_NO_SELF, HP_NO_SUCH_PEER, HP_NO_SUPPORT,
    HP_NOT_CONNECTED, HolepunchMsg, LT_DONTHAVE, MAX_EXT_HANDSHAKE_LEN, MAX_PEX_ENTRIES,
    METADATA_PIECE_LEN, MetadataMsg, OUR_LT_DONTHAVE_ID, OUR_UPLOAD_ONLY_ID, OUR_UT_HOLEPUNCH_ID,
    OUR_UT_METADATA_ID, OUR_UT_PEX_ID, PexFlags, PexMsg, UPLOAD_ONLY, UT_HOLEPUNCH, UT_METADATA,
    UT_PEX, decode_donthave, decode_upload_only, encode_donthave, encode_upload_only,
};
pub use fast::allowed_fast_set;
pub use handshake::{
    HANDSHAKE_LEN, Handshake, HandshakeError, Reserved, read_handshake, write_handshake,
};
pub use message::{HashRequest, MAX_HASH_REQUEST_LEN, Message};
pub use mse::{
    CryptoMethods, MSE_MAX_IA, MSE_MAX_PAD, MseError, MseStream, initiate,
    is_plaintext_handshake_prefix, req2_hash, respond,
};

/// Fixed-key MSE handshake for the fuzz crate. Not part of the API.
#[doc(hidden)]
pub mod mse_fuzzing {
    pub use crate::mse::fuzzing::{initiate_fixed, respond_fixed};
}
