#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(m) = fbt_metainfo::Metainfo::from_bytes(data) {
        // A v2-only torrent's piece count is capped at parse time: every
        // per-piece vector of the session is sized from it.
        if m.version() == fbt_metainfo::TorrentVersion::V2 {
            assert!(
                usize::try_from(m.piece_layout().num_pieces())
                    .is_ok_and(|n| n <= fbt_metainfo::MAX_V2_PIECES)
            );
        }
    }
});
