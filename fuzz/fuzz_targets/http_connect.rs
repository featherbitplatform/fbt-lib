#![no_main]
use fbt_net::proxy::{MAX_CONNECT_HEAD, parse_connect_head};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(Some((status, len))) = parse_connect_head(data) {
        assert!(len <= data.len() && len <= MAX_CONNECT_HEAD);
        assert!((100..=599).contains(&status));
    }
});
