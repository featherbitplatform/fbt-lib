//! Deterministic torrent builders shared by the tests and the fixture generator.
#![allow(dead_code)]

use std::collections::BTreeMap;

use fbt_bencode::{DictBuilder, OwnedValue, encode};
use fbt_core::merkle::{file_tree_height, layer_root, leaf_hashes, piece_root};
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

/// One file of a v2 fixture: its path components and length.
pub type V2File<'a> = (&'a [&'a str], u64);

/// A file's deterministic content (index-dependent seed).
pub fn v2_content(index: usize, len: u64) -> Vec<u8> {
    data(100 + index as u64, len as usize)
}

/// `pieces root` of non-empty `content`.
pub fn v2_root(content: &[u8]) -> [u8; 32] {
    let width = 1usize << file_tree_height(content.len() as u64);
    layer_root(&leaf_hashes(content), 0, width)
}

/// The piece layer of `content` (one root per `piece_len` chunk).
pub fn v2_layer(content: &[u8], piece_len: u32) -> Vec<[u8; 32]> {
    content
        .chunks(piece_len as usize)
        .map(|c| piece_root(c, piece_len as usize / 16_384))
        .collect()
}

fn tree_insert(node: &mut BTreeMap<Vec<u8>, OwnedValue>, parts: &[&str], leaf: OwnedValue) {
    match parts {
        [] => {}
        [last] => {
            let file = DictBuilder::new().insert(b"", leaf).build();
            node.insert(last.as_bytes().to_vec(), file);
        }
        [first, rest @ ..] => {
            let entry = node
                .entry(first.as_bytes().to_vec())
                .or_insert_with(|| OwnedValue::Dict(BTreeMap::new()));
            if let OwnedValue::Dict(d) = entry {
                tree_insert(d, rest, leaf);
            }
        }
    }
}

/// The v1 file list of `files` as libtorrent lays out a hybrid: a `.pad`
/// entry before each non-empty file at an unaligned offset. Also returns the
/// padded content.
pub fn v1_padded(files: &[V2File<'_>], piece_len: u32) -> (Vec<OwnedValue>, Vec<u8>) {
    let pl = u64::from(piece_len);
    let (mut list_out, mut content, mut offset) = (Vec::new(), Vec::new(), 0u64);
    for (i, (parts, len)) in files.iter().enumerate() {
        if *len > 0 && offset % pl != 0 {
            let pad = pl - offset % pl;
            let pad_name = pad.to_string();
            list_out.push(file_attr(pad as i64, &[".pad", &pad_name], "p"));
            content.extend(vec![0u8; pad as usize]);
            offset += pad;
        }
        list_out.push(file(*len as i64, parts));
        content.extend(v2_content(i, *len));
        offset += len;
    }
    (list_out, content)
}

/// The v2 `info` dictionary of `files`, and the root-level `piece layers`
/// dictionary. `v1` is `None` for v2-only; `Some((files, pieces))` makes a
/// hybrid out of the given v1 half (a single `length` entry when the list
/// holds exactly one file at the top level and `single` is set).
pub fn v2_info_with(
    files: &[V2File<'_>],
    piece_len: u32,
    v1: Option<(Vec<OwnedValue>, Vec<u8>)>,
) -> (OwnedValue, OwnedValue) {
    let mut tree = BTreeMap::new();
    let mut layers = DictBuilder::new();
    for (i, (parts, len)) in files.iter().enumerate() {
        let mut attrs = DictBuilder::new();
        attrs.insert(b"length", *len as i64);
        if *len > 0 {
            let content = v2_content(i, *len);
            let root = v2_root(&content);
            attrs.insert(b"pieces root", root.to_vec());
            if *len > u64::from(piece_len) {
                let layer: Vec<u8> = v2_layer(&content, piece_len).concat();
                layers.insert(&root, layer);
            }
        }
        tree_insert(&mut tree, parts, attrs.build());
    }
    let single = matches!(files, [(parts, _)] if parts.len() == 1);
    let name = if single { files[0].0[0] } else { "v2t" };
    let mut info = DictBuilder::new();
    info.insert(b"name", name)
        .insert(b"piece length", i64::from(piece_len))
        .insert(b"meta version", 2i64)
        .insert(b"file tree", OwnedValue::Dict(tree));
    if let Some((v1_files, pieces)) = v1 {
        if single {
            info.insert(b"length", files[0].1 as i64);
        } else {
            info.insert(b"files", list(v1_files));
        }
        info.insert(b"pieces", pieces);
    }
    (info.build(), layers.build())
}

/// A v2 (or hybrid) torrent with real roots and, if asked, layers.
pub fn build_v2(files: &[V2File<'_>], piece_len: u32, hybrid: bool, with_layers: bool) -> Vec<u8> {
    let v1 = hybrid.then(|| {
        let (list_out, content) = v1_padded(files, piece_len);
        (list_out, pieces_of(&content, piece_len as usize))
    });
    let (info, layers) = v2_info_with(files, piece_len, v1);
    let mut extra: Vec<(&str, OwnedValue)> =
        vec![("announce", "http://tracker.example/announce".into())];
    if with_layers {
        extra.push(("piece layers", layers));
    }
    torrent(info, &extra)
}

pub const V2_FILES: [V2File<'static>; 3] = [
    (&["f0.bin"], 20_000),
    (&["f1.empty"], 0),
    (&["sub", "f2.bin"], 70_000),
];

pub fn v2_fixture() -> Vec<u8> {
    build_v2(&V2_FILES, 32_768, false, true)
}

pub fn v2_single_fixture() -> Vec<u8> {
    build_v2(&[(&["v2-single.bin"], 50_000)], 32_768, false, true)
}

pub fn hybrid_fixture() -> Vec<u8> {
    build_v2(&V2_FILES, 32_768, true, true)
}
