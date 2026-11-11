#![no_main]
use fbt_net::MapProtocol;
use fbt_net::natpmp::{Expect, parse_reply};
use libfuzzer_sys::fuzz_target;

// The first byte picks what the reply is checked against, the rest is the reply.
fuzz_target!(|data: &[u8]| {
    let Some((&sel, reply)) = data.split_first() else {
        return;
    };
    let expect = match sel % 5 {
        0 => Expect::natpmp_external(),
        1 => Expect::natpmp_map(MapProtocol::Tcp, 6881),
        2 => Expect::natpmp_map(MapProtocol::Udp, 6881),
        3 => Expect::pcp_map(MapProtocol::Tcp, 6881, [7; 12]),
        _ => Expect::pcp_map(MapProtocol::Udp, 6881, [7; 12]),
    };
    let _ = parse_reply(reply, &expect);
});
