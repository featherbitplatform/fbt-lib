#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(v) = fbt_bencode::decode(data) {
        let encoded = fbt_bencode::encode(&v.to_owned_value());
        assert!(
            fbt_bencode::decode(&encoded).is_ok(),
            "re-encoded value must decode"
        );
    }
});
