#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Flags and transaction id come from the input, so both the match and
    // mismatch branches and both address families are reached.
    let Some((head, body)) = data.split_first_chunk::<5>() else {
        return;
    };
    let ipv6 = head[0] & 1 == 1;
    let tid = u32::from_be_bytes([head[1], head[2], head[3], head[4]]);
    let _ = fbt_tracker::parse_udp_announce(body, tid, ipv6);
    // Also the common case: the datagram's own transaction id.
    if let Some(own) = body.get(4..8) {
        let own = u32::from_be_bytes([own[0], own[1], own[2], own[3]]);
        let _ = fbt_tracker::parse_udp_announce(body, own, ipv6);
    }
});
