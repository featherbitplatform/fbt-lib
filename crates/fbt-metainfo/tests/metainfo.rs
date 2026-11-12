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

/// The paths of a multi-file torrent built from `files`, after parsing.
fn parsed_paths(files: Vec<OwnedValue>) -> Vec<String> {
    let total: usize = files.len() * 10;
    let m = Metainfo::from_bytes(&torrent(multi_info(files, total), &[])).unwrap();
    m.info.files.iter().map(|f| f.path.to_string()).collect()
}

#[test]
fn duplicate_and_prefix_paths_are_renamed() {
    let got = parsed_paths(vec![file(10, &["a", "b"]), file(10, &["a", "b"])]);
    assert_eq!(got, ["root/a/b", "root/a/b.1"]);
    let got = parsed_paths(vec![file(10, &["a"]), file(10, &["a", "b"])]);
    assert_eq!(got, ["root/a", "root/a.1/b"]);
    // Paths made equal only by sanitization collide too.
    let got = parsed_paths(vec![file(10, &["a", "b"]), file(10, &["..", "a", "b"])]);
    assert_eq!(got, ["root/a/b", "root/a/b.1"]);
}

#[test]
fn case_insensitive_collisions_are_renamed() {
    let got = parsed_paths(vec![file(10, &["A.bin"]), file(10, &["a.bin"])]);
    assert_eq!(got, ["root/A.bin", "root/a.1.bin"]);
    let got = parsed_paths(vec![file(10, &["Dir"]), file(10, &["dir", "x"])]);
    assert_eq!(got, ["root/Dir", "root/dir.1/x"]);
}

#[test]
fn case_collision_is_renamed() {
    let got = parsed_paths(vec![file(10, &["A.txt"]), file(10, &["a.txt"])]);
    assert_eq!(got, ["root/A.txt", "root/a.1.txt"]);
}

#[test]
fn normalisation_collision_is_renamed() {
    let got = parsed_paths(vec![file(10, &["caf\u{e9}"]), file(10, &["cafe\u{301}"])]);
    assert_eq!(got, ["root/caf\u{e9}", "root/cafe\u{301}.1"]);
}

#[test]
fn file_and_directory_collide() {
    let got = parsed_paths(vec![file(10, &["a"]), file(10, &["a", "b"])]);
    assert_eq!(got, ["root/a", "root/a.1/b"]);
    // The directory first, then a file of the same name.
    let got = parsed_paths(vec![file(10, &["a", "b"]), file(10, &["a"])]);
    assert_eq!(got, ["root/a/b", "root/a.1"]);
}

#[test]
fn later_files_follow_a_renamed_directory() {
    let got = parsed_paths(vec![
        file(10, &["a"]),
        file(10, &["a", "b"]),
        file(10, &["a", "c"]),
        file(10, &["A", "d", "e"]),
    ]);
    assert_eq!(got, ["root/a", "root/a.1/b", "root/a.1/c", "root/a.1/d/e"]);
}

#[test]
fn renamed_name_avoids_existing() {
    let got = parsed_paths(vec![
        file(10, &["x.txt"]),
        file(10, &["x.1.txt"]),
        file(10, &["X.txt"]),
    ]);
    assert_eq!(got, ["root/x.txt", "root/x.1.txt", "root/X.2.txt"]);
    // Dotfiles keep no extension.
    let got = parsed_paths(vec![file(10, &[".rc"]), file(10, &[".RC"])]);
    assert_eq!(got, ["root/.rc", "root/.RC.1"]);
}

#[test]
fn pad_files_never_renamed() {
    let info = multi_info(
        vec![
            file(10, &["a"]),
            file_attr(16_374, &[".pad", "16374"], "p"),
            file(10, &["A"]),
            file_attr(16_374, &[".pad", "16374"], "p"),
        ],
        32_768,
    );
    let m = Metainfo::from_bytes(&torrent(info, &[])).unwrap();
    let paths: Vec<String> = m.info.files.iter().map(|f| f.path.to_string()).collect();
    assert_eq!(
        paths,
        ["root/a", "root/.pad/16374", "root/A.1", "root/.pad/16374"]
    );
}

#[test]
fn long_component_stays_within_the_cap() {
    let lower = format!("{}.bin", "\u{e9}".repeat(125));
    let upper = format!("{}.bin", "\u{c9}".repeat(125));
    assert!(lower.len() > 250);
    let got = parsed_paths(vec![file(10, &[&lower]), file(10, &[&upper])]);
    let second = got[1].strip_prefix("root/").unwrap();
    assert!(second.len() <= 255, "{}", second.len());
    assert!(second.ends_with(".1.bin"), "{second}");
    assert_ne!(got[0], got[1]);
}

#[test]
fn info_hash_unchanged_by_renames() {
    let info = multi_info(vec![file(10, &["A"]), file(10, &["a"])], 20);
    let raw = fbt_bencode::encode(&info);
    let m = Metainfo::from_bytes(&torrent(info, &[])).unwrap();
    assert_eq!(m.info.files[1].path.to_string(), "root/a.1");
    let sha1: [u8; 20] = Sha1::digest(&raw).into();
    assert_eq!(m.info_hashes.v1.unwrap().0, sha1);
    assert_eq!(m.raw_info(), raw.as_slice());
}

#[test]
fn collides_sees_prefixes() {
    let info = multi_info(vec![file(10, &["x", "y"]), file(10, &["p", "q"])], 20);
    let m = Metainfo::from_bytes(&torrent(info, &[])).unwrap();
    let files = &m.info.files;
    let with_root = |parts: &[&str]| {
        let mut v = vec!["root"];
        v.extend_from_slice(parts);
        fbt_metainfo::sanitize_path(v.iter().map(|s| s.as_bytes())).unwrap()
    };
    // `root/x` is a directory prefix of file 0.
    assert!(fbt_metainfo::collides(files, 1, &with_root(&["x"])));
    // `root/x/y/z` lies under file 0, which is a file.
    assert!(fbt_metainfo::collides(
        files,
        1,
        &with_root(&["x", "y", "z"])
    ));
    // Case and equality.
    assert!(fbt_metainfo::collides(files, 1, &with_root(&["X", "Y"])));
    // The file's own path is not a collision with itself.
    assert!(!fbt_metainfo::collides(files, 0, &with_root(&["x", "y"])));
    assert!(!fbt_metainfo::collides(files, 1, &with_root(&["x", "w"])));
    assert_eq!(with_root(&["X", "Y"]).key(), with_root(&["x", "y"]).key());
}

#[test]
fn url_list_as_string_and_list() {
    let content = data(1, 100);
    let t = torrent(
        single_info("a", &content, 16_384),
        &[("url-list", bytes(b"http://seed.example/a"))],
    );
    let m = Metainfo::from_bytes(&t).unwrap();
    assert_eq!(m.web_seeds, ["http://seed.example/a"]);
    let t = torrent(
        single_info("a", &content, 16_384),
        &[(
            "url-list",
            list(vec![
                bytes(b"https://one.example/"),
                bytes(b"http://two.example/"),
                bytes(b"https://one.example/"),
            ]),
        )],
    );
    let m = Metainfo::from_bytes(&t).unwrap();
    assert_eq!(m.web_seeds, ["https://one.example/", "http://two.example/"]);
    let m = Metainfo::from_bytes(&torrent(single_info("a", &content, 16_384), &[])).unwrap();
    assert!(m.web_seeds.is_empty());
}

#[test]
fn url_list_drops_non_http_and_caps() {
    let content = data(1, 100);
    let long = format!("http://seed.example/{}", "x".repeat(3000));
    let mut items = vec![
        bytes(b"ftp://seed.example/a"),
        bytes(long.as_bytes()),
        bytes(b"not a url"),
        bytes(b"http://"),
    ];
    items.extend((0..20).map(|i| bytes(format!("http://s{i}.example/").as_bytes())));
    let t = torrent(
        single_info("a", &content, 16_384),
        &[("url-list", list(items))],
    );
    let m = Metainfo::from_bytes(&t).unwrap();
    assert_eq!(m.web_seeds.len(), fbt_metainfo::MAX_WEB_SEEDS);
    assert_eq!(m.web_seeds.len(), 16);
    assert_eq!(m.web_seeds[0], "http://s0.example/");
    assert!(m.web_seeds.iter().all(|u| u.starts_with("http://s")));
}

#[test]
fn deep_paths_rejected_quickly() {
    let parts: Vec<&str> = vec!["a"; 100_000];
    let info = multi_info(vec![file(10, &parts)], 10);
    let t = torrent(info, &[]);
    let start = std::time::Instant::now();
    assert!(matches!(
        Metainfo::from_bytes(&t),
        Err(MetainfoError::InvalidField {
            field: "path",
            reason: "too deep"
        })
    ));
    // Also via one component split by sanitization.
    let one = "a/".repeat(100_000);
    let info = multi_info(vec![file(10, &[one.as_str()])], 10);
    assert!(Metainfo::from_bytes(&torrent(info, &[])).is_err());
    assert!(start.elapsed() < std::time::Duration::from_secs(1));
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

#[test]
fn trackers_capped_at_64_urls() {
    let content = data(1, 100);
    let urls: Vec<String> = (0..100).map(|i| format!("http://t{i}.example/")).collect();
    // Tiers of 30, 30 and 40 URLs: 64 in all survive, in order.
    let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
    let tiers: [&[&str]; 3] = [&refs[..30], &refs[30..60], &refs[60..]];
    let t = torrent(
        single_info("a", &content, 16_384),
        &[("announce-list", announce_list(&tiers))],
    );
    let m = Metainfo::from_bytes(&t).unwrap();
    let sizes: Vec<usize> = m.trackers.iter().map(Vec::len).collect();
    assert_eq!(sizes, vec![30, 30, 4]);
    assert_eq!(m.trackers.concat(), urls[..64].to_vec());
}
