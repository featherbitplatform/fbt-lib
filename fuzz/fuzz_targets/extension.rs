#![no_main]
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use fbt_wire::{
    ExtHandshake, HolepunchMsg, MetadataMsg, PexMsg, decode_donthave, decode_upload_only,
};

// The first byte picks the decoder, the rest is the payload.
fuzz_target!(|data: &[u8]| {
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 6 {
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
        2 => {
            if let Ok(p) = PexMsg::decode(payload) {
                let _ = p.encode();
            }
        }
        3 => {
            if let Ok(m) = HolepunchMsg::decode(payload) {
                let _ = m.encode();
            }
        }
        4 => {
            let _ = decode_donthave(payload);
        }
        _ => {
            let _ = decode_upload_only(payload);
        }
    }
});
