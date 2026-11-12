//! fbt-dht: Mainline DHT (BEP 5), written from scratch, with BEP 32, 33,
//! 42, 43 and 51.
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

mod bloom;
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

pub use bloom::{BloomFilter, MAX_ESTIMATE};
pub use id::{NODE_ID_LEN, NodeId, bep42_id, is_local};
pub use krpc::{
    BLOOM_FILTER_LEN, Body, ERR_GENERIC, ERR_METHOD_UNKNOWN, ERR_PROTOCOL, ERR_SERVER, KRPC_LIMITS,
    KrpcError, MAX_DATAGRAM, MAX_ERROR_MESSAGE, MAX_METHOD_LEN, MAX_NODES, MAX_SAMPLES,
    MAX_TID_LEN, MAX_TOKEN_LEN, MAX_VALUES, MAX_VERSION_LEN, Message, NodeInfo, Query, QueryMsg,
    Response, Want,
};
pub use lookup::PeerLookup;
pub use node::{
    DHT_INPUT_QUEUE, Dht, DhtConfig, DhtError, DhtInput, DhtSamples, DhtScrape, DhtStats,
    ExternalIpVotes,
};
pub use state::{DhtState, MAX_STATE_NODES};
