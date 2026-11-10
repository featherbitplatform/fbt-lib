//! fbt-utp: the uTP transport (BEP 29).
//!
//! Everything here receives datagrams from untrusted hosts: no panics, no
//! unbounded allocation.

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

#[doc(hidden)]
pub mod packet;

pub use packet::{
    HEADER_LEN, MAX_EXTENSIONS, MAX_SACK_LEN, Packet, PacketError, PacketType, sack_acks, seq_le,
    seq_lt,
};
