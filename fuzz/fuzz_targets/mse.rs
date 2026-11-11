#![no_main]
use fbt_wire::mse_fuzzing::{initiate_fixed, respond_fixed};
use fbt_wire::{CryptoMethods, req2_hash};
use libfuzzer_sys::fuzz_target;
use tokio::io::{join, sink};

// The first byte picks the role; the rest is what the remote sends. The
// stream ends after it, so both roles must return (Ok or Err) without hanging.
//
// Both roles use the fixed private keys of the deterministic test (initiator
// [7; 20], responder [9; 20], no padding) and the info hash [0xaa; 20], so the
// seeds are captured transcripts: the req1/SKEY synchronisation succeeds on
// them and mutations reach the code behind it.
const INFO_HASH: [u8; 20] = [0xaa; 20];

fuzz_target!(|data: &[u8]| {
    let Some((&role, rest)) = data.split_first() else {
        return;
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    rt.block_on(async {
        if role & 1 == 0 {
            let lookup = |h: &[u8; 20]| (*h == req2_hash(&INFO_HASH)).then_some(INFO_HASH);
            let _ = respond_fixed(
                join(rest, sink()),
                Vec::new(),
                &lookup,
                CryptoMethods::all(),
                [9; 20],
                0,
            )
            .await;
        } else {
            let _ = initiate_fixed(
                join(rest, sink()),
                &INFO_HASH,
                CryptoMethods::all(),
                b"",
                [7; 20],
                0,
            )
            .await;
        }
    });
});
