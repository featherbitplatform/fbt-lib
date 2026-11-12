#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // A web seed's `Content-Range` header value, as the client reads it:
    // only text reaches the parser.
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    if let Some((a, b, total)) = fbt_tracker::parse_content_range(text) {
        assert!(a <= b, "{text:?}");
        assert!(total.is_none_or(|t| b < t), "{text:?}");
    }
});
