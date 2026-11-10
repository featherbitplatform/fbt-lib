#![no_main]
use fbt_metainfo::Magnet;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(m) = Magnet::parse(&String::from_utf8_lossy(data)) {
        assert!(
            Magnet::parse(&m.to_uri()).is_ok(),
            "to_uri output must parse again"
        );
    }
});
