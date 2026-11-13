#![no_main]
//! BEP 52 `hashes` verification: arbitrary bytes as `pieces root` (32),
//! tree height, base layer, index and hash count (u32 LE each), then the
//! hashes and the proof (32 bytes each; a trailing partial hash dropped).
use fbt_core::merkle::verify_hashes;
use libfuzzer_sys::fuzz_target;

fn u32_at(data: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(data.get(at..at + 4)?.try_into().ok()?))
}

fuzz_target!(|data: &[u8]| {
    let Some(root) = data.get(..32).and_then(|r| <[u8; 32]>::try_from(r).ok()) else {
        return;
    };
    let (Some(tree_height), Some(base), Some(index), Some(count)) = (
        u32_at(data, 32),
        u32_at(data, 36),
        u32_at(data, 40),
        u32_at(data, 44),
    ) else {
        return;
    };
    let all: Vec<[u8; 32]> = data
        .get(48..)
        .unwrap_or_default()
        .as_chunks::<32>()
        .0
        .to_vec();
    let split = (count as usize).min(all.len());
    let (hashes, proof) = all.split_at(split);
    let ok = verify_hashes(&root, tree_height, base, index as usize, hashes, proof);
    if ok {
        // A proof binds its root: any other root fails.
        let mut other = root;
        other[0] ^= 1;
        assert!(!verify_hashes(
            &other,
            tree_height,
            base,
            index as usize,
            hashes,
            proof
        ));
    }
});
