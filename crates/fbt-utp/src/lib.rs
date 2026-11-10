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
pub mod conn;
mod ledbat;
#[doc(hidden)]
pub mod packet;
mod rtt;

pub use conn::UtpConfig;

pub use packet::{
    HEADER_LEN, MAX_EXTENSIONS, MAX_SACK_LEN, Packet, PacketError, PacketType, sack_acks, seq_le,
    seq_lt,
};

/// Why a uTP connection ended or could not be made.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UtpError {
    #[error("uTP connect timed out")]
    ConnectTimeout,
    #[error("uTP connection reset by the peer")]
    Reset,
    #[error("uTP connection timed out")]
    TimedOut,
    #[error("uTP connection closed")]
    Closed,
    #[error("too many uTP connections")]
    TooManyConnections,
    #[error("uTP I/O error: {0}")]
    Io(String),
}
