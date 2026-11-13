#![no_main]
//! Resume data and session state from disk are hostile: decoding either
//! never panics, and a session state that decodes round-trips.

use fbt_session::{SessionState, decode_resume};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = decode_resume(data);
    if let Ok(state) = SessionState::from_bytes(data) {
        let again = SessionState::from_bytes(&state.to_bytes()).expect("own output decodes");
        assert_eq!(again, state);
        for t in &state.torrents {
            let _ = decode_resume(t);
        }
    }
});
