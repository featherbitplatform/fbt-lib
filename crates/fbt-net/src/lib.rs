//! fbt-net: the shared UDP socket and, in later tasks, the LAN and NAT helpers.
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

pub mod demux;
pub mod lsd;
