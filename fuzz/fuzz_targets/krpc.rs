#![no_main]
use fbt_dht::krpc::{Body, Message, Query, QueryMsg};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(msg) = Message::decode(data) {
        let again = Message::decode(&msg.encode())
            .expect("a decoded message re-encodes to a decodable one");
        let mut expected = msg.clone();
        // a BEP 44 put whose value or salt the decoder cut (`reject`) cannot
        // re-encode what was cut: a cut value (205, which wins when the salt
        // was cut too) is empty and goes out as `0:`, a cut salt (207) is
        // none, and the re-decoded put is not cut.
        // Every other field must round-trip.
        if let Body::Query(QueryMsg {
            query: Query::Put { v, salt, reject, .. },
            ..
        }) = &mut expected.body
            && let Some(code) = reject.take()
        {
            assert!(code == 205 || code == 207, "reject is 205 or 207");
            if code == 207 {
                assert!(salt.is_none(), "a cut salt is dropped");
            } else {
                assert!(v.is_empty(), "a cut value is empty");
                *v = b"0:".to_vec();
            }
        }
        assert_eq!(expected, again);
    }
});
