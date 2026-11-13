#![no_main]
use fbt_api::range::{RangeOutcome, parse_range};
use libfuzzer_sys::fuzz_target;

// The first 8 bytes are the file length (u64 big-endian), the rest the
// `Range` header value.
fuzz_target!(|data: &[u8]| {
    let Some((len, header)) = data.split_first_chunk::<8>() else {
        return;
    };
    let len = u64::from_be_bytes(*len);
    if let RangeOutcome::Partial {
        start,
        end_inclusive,
    } = parse_range(header, len)
    {
        assert!(
            start <= end_inclusive && end_inclusive < len,
            "Partial {start}..={end_inclusive} out of a {len}-byte file"
        );
    }
});
