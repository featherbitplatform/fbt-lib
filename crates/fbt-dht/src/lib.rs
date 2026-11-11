//! fbt-dht: Mainline DHT (BEP 5), written from scratch.
//!
//! Everything here decodes datagrams from untrusted nodes: no panics, no
//! unbounded allocation, and every remote list is cut to a named cap.

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

mod id;
/// KRPC message codec. Public for the fuzz targets; use the re-exports.
#[doc(hidden)]
pub mod krpc;
mod lookup;
mod node;
mod rate;
mod server;
mod state;
mod store;
mod table;
mod token;

pub use id::{NODE_ID_LEN, NodeId, bep42_id, is_local};
pub use krpc::{
    Body, ERR_GENERIC, ERR_METHOD_UNKNOWN, ERR_PROTOCOL, ERR_SERVER, KRPC_LIMITS, KrpcError,
    MAX_DATAGRAM, MAX_ERROR_MESSAGE, MAX_METHOD_LEN, MAX_NODES, MAX_TID_LEN, MAX_TOKEN_LEN,
    MAX_VALUES, MAX_VERSION_LEN, Message, NodeInfo, Query, QueryMsg, Response, Want,
};
pub use lookup::PeerLookup;
pub use node::{DHT_INPUT_QUEUE, Dht, DhtConfig, DhtError, DhtInput, DhtStats, ExternalIpVotes};
pub use state::{DhtState, MAX_STATE_NODES};
