#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(msg) = fbt_dht::krpc::Message::decode(data) {
        let again = fbt_dht::krpc::Message::decode(&msg.encode())
            .expect("a decoded message re-encodes to a decodable one");
        assert_eq!(msg, again);
    }
});
