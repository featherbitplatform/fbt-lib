//! fbt-dht: Mainline DHT (BEP 5), written from scratch, with BEP 32, 33,
//! 42, 43, 44 (immutable and mutable items) and 51.
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
mod items;
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
pub use items::MutableItem;
pub use krpc::{
    BLOOM_FILTER_LEN, Body, ERR_CAS_MISMATCH, ERR_GENERIC, ERR_INVALID_SIGNATURE,
    ERR_METHOD_UNKNOWN, ERR_PROTOCOL, ERR_SALT_TOO_BIG, ERR_SEQ_TOO_LOW, ERR_SERVER,
    ERR_VALUE_TOO_BIG, ITEM_VALUE_LIMITS, KRPC_LIMITS, KrpcError, MAX_DATAGRAM, MAX_ERROR_MESSAGE,
    MAX_ITEM_VALUE, MAX_METHOD_LEN, MAX_NODES, MAX_SALT, MAX_SAMPLES, MAX_TID_LEN, MAX_TOKEN_LEN,
    MAX_VALUES, MAX_VERSION_LEN, Message, NodeInfo, Query, QueryMsg, Response, Want,
};
pub use lookup::PeerLookup;
pub use node::{
    DHT_INPUT_QUEUE, Dht, DhtConfig, DhtError, DhtInput, DhtSamples, DhtScrape, DhtStats,
    ExternalIpVotes,
};
pub use state::{DhtState, MAX_STATE_NODES};
