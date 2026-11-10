#![no_main]
use fbt_wire::{CryptoMethods, initiate, respond};
use libfuzzer_sys::fuzz_target;
use tokio::io::{join, sink};

// The first byte picks the role; the rest is what the remote sends. The
// stream ends after it, so both roles must return (Ok or Err) without hanging.
fuzz_target!(|data: &[u8]| {
    let Some((&role, rest)) = data.split_first() else {
        return;
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    rt.block_on(async {
        if role & 1 == 0 {
            let _ = respond(
                join(rest, sink()),
                Vec::new(),
                &|_| Some([0; 20]),
                CryptoMethods::all(),
            )
            .await;
        } else {
            let _ = initiate(join(rest, sink()), &[0; 20], CryptoMethods::all(), b"").await;
        }
    });
});
