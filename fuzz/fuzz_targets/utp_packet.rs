#![no_main]
use bytes::Bytes;
use fbt_utp::packet::Packet;
use libfuzzer_sys::fuzz_target;

// A decoded packet must survive an encode/decode round trip unchanged.
fuzz_target!(|data: &[u8]| {
    if let Ok(p) = Packet::decode(&Bytes::copy_from_slice(data)) {
        let mut out = Vec::new();
        p.encode(&mut out);
        let again = Packet::decode(&Bytes::from(out)).expect("re-decode of our own encoding");
        assert_eq!(p, again);
    }
});
