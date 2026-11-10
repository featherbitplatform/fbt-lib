#![no_main]
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use fbt_wire::{ExtHandshake, MetadataMsg, PexMsg};

// The first byte picks the decoder, the rest is the payload.
fuzz_target!(|data: &[u8]| {
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 3 {
        0 => {
            if let Ok(h) = ExtHandshake::decode(payload) {
                let _ = h.encode();
            }
        }
        1 => {
            if let Ok(m) = MetadataMsg::decode(&Bytes::copy_from_slice(payload)) {
                let _ = m.encode();
            }
        }
        _ => {
            if let Ok(p) = PexMsg::decode(payload) {
                let _ = p.encode();
            }
        }
    }
});
