#![no_main]
use fbt_dht::krpc::{Body, Message, Query, QueryMsg};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(msg) = Message::decode(data) {
        let again = Message::decode(&msg.encode())
            .expect("a decoded message re-encodes to a decodable one");
        // a BEP 44 put whose value or salt the decoder cut (`reject`) cannot
        // re-encode what was cut; every other message round-trips
        let cut = matches!(
            msg.body,
            Body::Query(QueryMsg {
                query: Query::Put {
                    reject: Some(_),
                    ..
                },
                ..
            })
        );
        if !cut {
            assert_eq!(msg, again);
        }
    }
});
