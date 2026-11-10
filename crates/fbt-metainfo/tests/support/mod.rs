//! Deterministic torrent builders shared by the tests and the fixture generator.
#![allow(dead_code)]

use fbt_bencode::{DictBuilder, OwnedValue, encode};
use sha1::{Digest as _, Sha1};

/// Bencodes `{"info": info, ..extra}`.
pub fn torrent(info: OwnedValue, extra: &[(&str, OwnedValue)]) -> Vec<u8> {
    let mut b = DictBuilder::new();
    b.insert(b"info", info);
    for (k, v) in extra {
        b.insert(k.as_bytes(), v.clone());
    }
    encode(&b.build())
}

pub fn bytes(s: &[u8]) -> OwnedValue {
    OwnedValue::Bytes(s.to_vec())
}

pub fn list(items: Vec<OwnedValue>) -> OwnedValue {
    OwnedValue::List(items)
}

pub fn path_list(parts: &[&str]) -> OwnedValue {
    list(parts.iter().map(|p| bytes(p.as_bytes())).collect())
}

/// A multi-file entry.
pub fn file(len: i64, parts: &[&str]) -> OwnedValue {
    DictBuilder::new()
        .insert(b"length", len)
        .insert(b"path", path_list(parts))
        .build()
}

/// A multi-file entry with an `attr` string.
pub fn file_attr(len: i64, parts: &[&str], attr: &str) -> OwnedValue {
    DictBuilder::new()
        .insert(b"length", len)
        .insert(b"path", path_list(parts))
        .insert(b"attr", attr)
        .build()
}

/// Deterministic pseudo-random data (xorshift64, fixed seed).
pub fn data(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

/// SHA-1 of each `piece_len` chunk, concatenated.
pub fn pieces_of(content: &[u8], piece_len: usize) -> Vec<u8> {
    content
        .chunks(piece_len)
        .flat_map(|c| Sha1::digest(c).to_vec())
        .collect()
}

pub fn announce_list(tiers: &[&[&str]]) -> OwnedValue {
    list(
        tiers
            .iter()
            .map(|t| list(t.iter().map(|u| bytes(u.as_bytes())).collect()))
            .collect(),
    )
}

pub fn single_info(name: &str, content: &[u8], piece_len: i64) -> OwnedValue {
    DictBuilder::new()
        .insert(b"name", name)
        .insert(b"length", content.len() as i64)
        .insert(b"piece length", piece_len)
        .insert(b"pieces", pieces_of(content, piece_len as usize))
        .build()
}

pub fn single_fixture() -> Vec<u8> {
    let content = data(1, 40_000);
    torrent(
        single_info("single.bin", &content, 16_384),
        &[
            ("announce", "http://tracker.example/announce".into()),
            ("comment", "fbt-metainfo single fixture".into()),
            ("created by", "fbt-metainfo tests".into()),
            ("creation date", 1_700_000_000i64.into()),
        ],
    )
}

pub fn multi_fixture() -> Vec<u8> {
    let sizes = [20_000usize, 5, 30_000];
    let mut content = Vec::new();
    for (i, s) in sizes.iter().enumerate() {
        content.extend(data(10 + i as u64, *s));
    }
    let info = DictBuilder::new()
        .insert(b"name", "multi")
        .insert(
            b"files",
            list(vec![
                file(sizes[0] as i64, &["a.bin"]),
                file(sizes[1] as i64, &["sub", "b.txt"]),
                file(sizes[2] as i64, &["sub", "deep", "c.bin"]),
            ]),
        )
        .insert(b"piece length", 16_384i64)
        .insert(b"pieces", pieces_of(&content, 16_384))
        .build();
    torrent(
        info,
        &[
            ("announce", "http://tracker.example/announce".into()),
            (
                "announce-list",
                announce_list(&[
                    &["http://tracker.example/announce"],
                    &["udp://tracker2.example:6969", "udp://tracker3.example:6969"],
                ]),
            ),
        ],
    )
}

/// Synthetic hybrid: v1 keys plus `meta version` 2 and a minimal `file tree`.
pub fn hybrid_fixture() -> Vec<u8> {
    let content = data(3, 40_000);
    let file_tree = DictBuilder::new()
        .insert(
            b"hybrid.bin",
            DictBuilder::new()
                .insert(
                    b"",
                    DictBuilder::new()
                        .insert(b"length", content.len() as i64)
                        .build(),
                )
                .build(),
        )
        .build();
    let info = DictBuilder::new()
        .insert(b"name", "hybrid.bin")
        .insert(b"length", content.len() as i64)
        .insert(b"piece length", 16_384i64)
        .insert(b"pieces", pieces_of(&content, 16_384))
        .insert(b"meta version", 2i64)
        .insert(b"file tree", file_tree)
        .build();
    torrent(
        info,
        &[("announce", "http://tracker.example/announce".into())],
    )
}

/// A large single-file torrent with synthetic piece hashes (no content data).
pub fn ubuntu_like_fixture() -> Vec<u8> {
    let total: i64 = 6_114_666_496;
    let piece_len: i64 = 4_194_304;
    let n = (total + piece_len - 1) / piece_len;
    let pieces: Vec<u8> = (0..n)
        .flat_map(|i| Sha1::digest((i as u64).to_be_bytes()).to_vec())
        .collect();
    let info = DictBuilder::new()
        .insert(b"name", "ubuntu-like-desktop-amd64.iso")
        .insert(b"length", total)
        .insert(b"piece length", piece_len)
        .insert(b"pieces", pieces)
        .build();
    torrent(
        info,
        &[
            ("announce", "https://tracker.example/announce".into()),
            (
                "announce-list",
                announce_list(&[
                    &["https://tracker.example/announce"],
                    &["https://ipv6.tracker.example/announce"],
                ]),
            ),
            ("comment", "synthetic large torrent".into()),
            ("created by", "fbt-metainfo tests".into()),
            ("creation date", 1_700_000_000i64.into()),
        ],
    )
}
