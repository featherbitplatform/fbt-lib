#![no_main]
use bytes::Bytes;
use fbt_utp::packet::{sack_acks, Packet};
use libfuzzer_sys::fuzz_target;

// A decoded packet must survive an encode/decode round trip. The one change
// allowed is the SACK mask: we accept any length (libtorrent sends only the
// bytes its bits need) but encode a multiple of four, as BEP 29 asks, so the
// mask comes back zero-padded and acks exactly the same packets.
fuzz_target!(|data: &[u8]| {
    if let Ok(p) = Packet::decode(&Bytes::copy_from_slice(data)) {
        let mut out = Vec::new();
        p.encode(&mut out);
        let again = Packet::decode(&Bytes::from(out)).expect("re-decode of our own encoding");
        let mut expected = p.clone();
        if let Some(s) = expected.sack.as_mut() {
            s.resize(s.len().div_ceil(4) * 4, 0);
        }
        assert_eq!(expected, again);
        let acks = |q: &Packet| -> Vec<u16> {
            q.sack.as_deref().map(|s| sack_acks(s, q.ack_nr).collect()).unwrap_or_default()
        };
        assert_eq!(acks(&p), acks(&again));
    }
});
