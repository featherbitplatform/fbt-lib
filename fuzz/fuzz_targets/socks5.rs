#![no_main]
use fbt_net::proxy::{decode_udp, encode_udp, parse_socks5_reply};
use libfuzzer_sys::fuzz_target;

// The same bytes as a TCP reply (and every prefix of it, as it arrives) and as
// a relayed UDP datagram.
fuzz_target!(|data: &[u8]| {
    if let Ok(Some((_, len))) = parse_socks5_reply(data) {
        assert!(len <= data.len());
    }
    for end in 0..data.len().min(262) {
        let _ = parse_socks5_reply(&data[..end]);
    }
    if let Some((src, payload)) = decode_udp(data) {
        let mut out = Vec::new();
        encode_udp(src, payload, &mut out);
        assert_eq!(decode_udp(&out), Some((src, payload)));
    }
});
