#![no_main]
use libfuzzer_sys::fuzz_target;
use std::path::Component;

fuzz_target!(|data: &[u8]| {
    // 0xFF never appears in valid UTF-8, so it is a clean component separator.
    let Ok(rel) = fbt_metainfo::sanitize_path(data.split(|&b| b == 0xFF)) else {
        return;
    };
    assert!(!rel.components().is_empty());
    for c in rel.components() {
        assert!(!c.is_empty(), "empty component");
        assert!(c != "." && c != "..", "dot component: {c:?}");
        assert!(
            !c.contains(['/', '\\', ':', '\0']),
            "forbidden character in {c:?}"
        );
    }
    let p = rel.to_path_buf();
    assert!(
        p.components().all(|c| matches!(c, Component::Normal(_))),
        "path escapes: {p:?}"
    );
});
