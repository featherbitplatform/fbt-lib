//! fbt-bencode: Bencode encoder and decoder.

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

mod decode;
mod value;

pub use decode::{DecodeError, Limits, decode, decode_prefix, decode_with};
pub use value::{Dict, Value, ValueKind};
mod encode;
pub use encode::{DictBuilder, OwnedValue, encode, encode_into};
