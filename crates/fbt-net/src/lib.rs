//! fbt-net: the shared UDP socket, local service discovery and port mapping
//! (UPnP IGD, PCP and NAT-PMP).
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
pub mod gateway;
pub mod lsd;
pub mod natpmp;
pub mod portmap;
pub mod upnp;

pub use natpmp::{MapMethod, MapProtocol};
pub use portmap::{MapEvent, PortMapConfig, PortMapper};
