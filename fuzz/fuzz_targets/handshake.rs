#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(buf) = <&[u8; fbt_wire::HANDSHAKE_LEN]>::try_from(data) {
        let _ = fbt_wire::Handshake::decode(buf);
    }
});
