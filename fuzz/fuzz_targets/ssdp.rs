#![no_main]
use std::net::SocketAddr;

use fbt_net::upnp::parse_search_response;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let from: SocketAddr = ([192, 168, 1, 1], 1900).into();
    let _ = parse_search_response(data, from);
});
