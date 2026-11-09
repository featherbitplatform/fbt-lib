mod support;

use fbt_bencode::{DictBuilder, OwnedValue};
use fbt_metainfo::{Metainfo, MetainfoError};
use sha1::{Digest as _, Sha1};
use sha2::Sha256;
use support::*;

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!(
        "{}/tests/fixtures/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}

/// A single-file info dict with `len` bytes and `pieces_len` bytes of hashes.
fn info_with(len: i64, pieces: Vec<u8>) -> OwnedValue {
    DictBuilder::new()
        .insert(b"name", "t")
        .insert(b"length", len)
        .insert(b"piece length", 16_384i64)
        .insert(b"pieces", pieces)
        .build()
}

/// A multi-file info dict with correct piece hashes for `total` bytes.
fn multi_info(files: Vec<OwnedValue>, total: usize) -> OwnedValue {
    DictBuilder::new()
        .insert(b"name", "root")
        .insert(b"files", list(files))
        .insert(b"piece length", 16_384i64)
        .insert(b"pieces", vec![0u8; 20 * total.div_ceil(16_384)])
        .build()
}

#[test]
fn single_file_parses() {
    let bytes = fixture("single.torrent");
    let m = Metainfo::from_bytes(&bytes).unwrap();
    assert_eq!(m.info.name, "single.bin");
    assert_eq!(m.total_len(), 40_000);
    assert_eq!(m.info.files.len(), 1);
    assert!(m.info.single_file);
    assert_eq!(m.info.files[0].path.components(), ["single.bin"]);
    assert_eq!(m.info.pieces.len(), 3);
    assert_eq!(m.piece_layout().num_pieces(), 3);
    let sha1: [u8; 20] = Sha1::digest(m.raw_info()).into();
    assert_eq!(m.info_hashes.v1.unwrap().0, sha1);
    assert!(m.info_hashes.v2.is_none());
    assert_eq!(m.trackers, vec![vec!["http://tracker.example/announce"]]);
    assert_eq!(m.creation_date, Some(1_700_000_000));
    assert_eq!(m.created_by.as_deref(), Some("fbt-metainfo tests"));
    assert!(bytes.windows(m.raw_info().len()).any(|w| w == m.raw_info()));
}

#[test]
fn multi_file_paths_rooted_at_name() {
    let m = Metainfo::from_bytes(&fixture("multi.torrent")).unwrap();
    assert!(!m.info.single_file);
    assert_eq!(m.info.files.len(), 3);
    for f in &m.info.files {
        assert_eq!(f.path.components()[0], m.info.name);
    }
    assert_eq!(m.info.files[2].path.to_string(), "multi/sub/deep/c.bin");
    assert_eq!(m.total_len(), 50_005);
    assert_eq!(m.file_layout().total_len(), 50_005);
    assert_eq!(m.trackers.len(), 2);
    assert_eq!(m.trackers[1].len(), 2);
}

#[test]
fn hybrid_sets_both_hashes() {
    let m = Metainfo::from_bytes(&fixture("hybrid.torrent")).unwrap();
    let sha1: [u8; 20] = Sha1::digest(m.raw_info()).into();
    let sha256: [u8; 32] = Sha256::digest(m.raw_info()).into();
    assert_eq!(m.info_hashes.v1.unwrap().0, sha1);
    assert_eq!(m.info_hashes.v2.unwrap().0, sha256);
}

#[test]
fn ubuntu_like_parses() {
    let m = Metainfo::from_bytes(&fixture("ubuntu-like.torrent")).unwrap();
    assert_eq!(m.total_len(), 6_114_666_496);
    assert_eq!(m.info.pieces.len(), 1458);
    let sha1: [u8; 20] = Sha1::digest(m.raw_info()).into();
    assert_eq!(m.info_hashes.v1.unwrap().0, sha1);
    assert_eq!(m.trackers.len(), 2);
}

#[test]
fn v2_only_is_unsupported() {
    let info = DictBuilder::new()
        .insert(b"name", "v2")
        .insert(b"meta version", 2i64)
        .insert(b"piece length", 16_384i64)
        .build();
    assert_eq!(
        Metainfo::from_bytes(&torrent(info, &[])).unwrap_err(),
        MetainfoError::V2OnlyUnsupported
    );
}

#[test]
fn announce_list_overrides_announce_and_dedupes() {
    let content = data(1, 100);
    let t = torrent(
        single_info("a", &content, 16_384),
        &[
            ("announce", "http://ignored.example/a".into()),
            (
                "announce-list",
                announce_list(&[
                    &[
                        "http://a.example/",
                        "http://b.example/",
                        "http://a.example/",
                    ],
                    &[],
                    &["http://b.example/"],
                    &["http://c.example/"],
                ]),
            ),
        ],
    );
    let m = Metainfo::from_bytes(&t).unwrap();
    assert_eq!(
        m.trackers,
        vec![
            vec!["http://a.example/", "http://b.example/"],
            vec!["http://c.example/"]
        ]
    );
}

#[test]
fn non_utf8_tracker_dropped() {
    let content = data(1, 100);
    let tiers = list(vec![list(vec![bytes(b"http://\xff.example/")])]);
    let t = torrent(
        single_info("a", &content, 16_384),
        &[
            ("announce", "http://ok.example/".into()),
            ("announce-list", tiers),
        ],
    );
    let m = Metainfo::from_bytes(&t).unwrap();
    assert_eq!(m.trackers, vec![vec!["http://ok.example/"]]);
}

#[test]
fn rejects_bad_pieces_length() {
    let t = torrent(info_with(100, vec![0u8; 39]), &[]);
    assert!(matches!(
        Metainfo::from_bytes(&t),
        Err(MetainfoError::InvalidField {
            field: "pieces",
            ..
        })
    ));
    // Well-formed multiple of 20 but wrong count for the length.
    let t = torrent(info_with(100, vec![0u8; 40]), &[]);
    assert!(matches!(
        Metainfo::from_bytes(&t),
        Err(MetainfoError::InvalidField {
            field: "pieces",
            ..
        })
    ));
}

#[test]
fn rejects_piece_length_out_of_range() {
    for bad in [0i64, -1, 268_435_457] {
        let info = DictBuilder::new()
            .insert(b"name", "t")
            .insert(b"length", 10i64)
            .insert(b"piece length", bad)
            .insert(b"pieces", vec![0u8; 20])
            .build();
        assert!(matches!(
            Metainfo::from_bytes(&torrent(info, &[])),
            Err(MetainfoError::InvalidField {
                field: "piece length",
                ..
            })
        ));
    }
}

#[test]
fn rejects_zero_total_and_overflow() {
    let t = torrent(info_with(0, vec![]), &[]);
    assert!(matches!(
        Metainfo::from_bytes(&t),
        Err(MetainfoError::InvalidField {
            field: "length",
            ..
        })
    ));
    let info = multi_info(vec![file(0, &["a"]), file_attr(0, &["b"], "p")], 0);
    assert!(matches!(
        Metainfo::from_bytes(&torrent(info, &[])),
        Err(MetainfoError::InvalidField {
            field: "length",
            ..
        })
    ));
    let info = multi_info(
        vec![
            file(i64::MAX, &["a"]),
            file(i64::MAX, &["b"]),
            file(2, &["c"]),
        ],
        0,
    );
    assert!(matches!(
        Metainfo::from_bytes(&torrent(info, &[])),
        Err(MetainfoError::InvalidField {
            field: "length",
            ..
        })
    ));
}

#[test]
fn rejects_negative_length() {
    let info = multi_info(vec![file(-1, &["a"])], 0);
    assert!(matches!(
        Metainfo::from_bytes(&torrent(info, &[])),
        Err(MetainfoError::InvalidField {
            field: "length",
            ..
        })
    ));
}

#[test]
fn rejects_duplicate_and_prefix_paths() {
    let info = multi_info(vec![file(10, &["a", "b"]), file(10, &["a", "b"])], 20);
    assert!(matches!(
        Metainfo::from_bytes(&torrent(info, &[])),
        Err(MetainfoError::DuplicatePath(_))
    ));
    let info = multi_info(vec![file(10, &["a"]), file(10, &["a", "b"])], 20);
    assert!(matches!(
        Metainfo::from_bytes(&torrent(info, &[])),
        Err(MetainfoError::DuplicatePath(_))
    ));
    // Paths made equal only by sanitization collide too.
    let info = multi_info(vec![file(10, &["a", "b"]), file(10, &["..", "a", "b"])], 20);
    assert!(matches!(
        Metainfo::from_bytes(&torrent(info, &[])),
        Err(MetainfoError::DuplicatePath(_))
    ));
}

#[test]
fn pad_files_flagged() {
    let info = multi_info(
        vec![
            file(10, &["a"]),
            file_attr(16_374, &[".pad", "16374"], "p"),
            file(10, &["b"]),
            file_attr(16_374, &[".pad", "16374"], "p"),
        ],
        32_768,
    );
    let m = Metainfo::from_bytes(&torrent(info, &[])).unwrap();
    let pads: Vec<bool> = m.info.files.iter().map(|f| f.pad).collect();
    assert_eq!(pads, [false, true, false, true]);
    assert_eq!(m.total_len(), 32_768);
}

#[test]
fn symlink_kept_as_empty_regular_entry() {
    let info = multi_info(vec![file(10, &["a"]), file_attr(99, &["link"], "l")], 10);
    let m = Metainfo::from_bytes(&torrent(info, &[])).unwrap();
    assert_eq!(m.info.files[1].len, 0);
    assert!(!m.info.files[1].pad);
}

#[test]
fn traversal_in_file_path_is_neutralised() {
    let info = multi_info(vec![file(10, &["..", "..", "x"])], 10);
    let m = Metainfo::from_bytes(&torrent(info, &[])).unwrap();
    assert_eq!(m.info.files[0].path.to_string(), "root/x");
}

#[test]
fn hostile_name_is_sanitized_or_hex() {
    let content = data(1, 10);
    let mut t = single_info("../../etc/passwd", &content, 16_384);
    let m = Metainfo::from_bytes(&torrent(t.clone(), &[])).unwrap();
    assert_eq!(m.info.name, "etc_passwd");
    if let OwnedValue::Dict(d) = &mut t {
        d.insert(b"name".to_vec(), bytes(b".."));
    }
    let m = Metainfo::from_bytes(&torrent(t, &[])).unwrap();
    assert_eq!(m.info.name, m.info_hashes.v1.unwrap().to_string());
}

#[test]
fn garbage_is_an_error_not_a_panic() {
    assert!(matches!(
        Metainfo::from_bytes(b"xyz"),
        Err(MetainfoError::Bencode(_))
    ));
    assert!(matches!(
        Metainfo::from_bytes(b"i1e"),
        Err(MetainfoError::InvalidField {
            field: "torrent",
            ..
        })
    ));
    assert!(matches!(
        Metainfo::from_bytes(b"de"),
        Err(MetainfoError::MissingField("info"))
    ));
}

#[test]
#[ignore = "writes the committed fixtures"]
fn regenerate_fixtures() {
    let dir = format!("{}/tests/fixtures", env!("CARGO_MANIFEST_DIR"));
    for (name, bytes) in [
        ("single", single_fixture()),
        ("multi", multi_fixture()),
        ("hybrid", hybrid_fixture()),
        ("ubuntu-like", ubuntu_like_fixture()),
    ] {
        std::fs::write(format!("{dir}/{name}.torrent"), bytes).unwrap();
    }
}
