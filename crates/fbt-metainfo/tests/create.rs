//! Torrent creation: every piece of a created torrent verifies against the
//! files it was made from.

use std::fs;
use std::ops::ControlFlow;
use std::path::Path;

use fbt_bencode::decode;
use fbt_core::merkle::piece_root;
use fbt_metainfo::{CreateError, CreateTorrent, Metainfo, TorrentVersion, auto_piece_len};
use proptest::prelude::*;
use sha1::{Digest as _, Sha1};

fn build(c: &CreateTorrent) -> Result<Vec<u8>, CreateError> {
    c.build(&mut |_, _| ControlFlow::Continue(()))
}

fn write_tree(root: &Path, files: &[(usize, usize, Vec<u8>)]) {
    for (dir, leaf, data) in files {
        let d = root.join(format!("d{dir}"));
        fs::create_dir_all(&d).unwrap();
        fs::write(d.join(format!("f{leaf}")), data).unwrap();
    }
}

fn file_tree_bytes(torrent: &[u8]) -> Vec<u8> {
    let v = decode(torrent).unwrap();
    let info = v.as_dict().unwrap().get(b"info").unwrap();
    info.as_dict()
        .unwrap()
        .get(b"file tree")
        .unwrap()
        .raw()
        .to_vec()
}

/// Up to five files with distinct (dir, leaf) slots.
fn files_strategy() -> impl Strategy<Value = Vec<(usize, usize, Vec<u8>)>> {
    (
        proptest::collection::vec(0usize..200_000, 1..=5),
        any::<u8>(),
    )
        .prop_map(|(sizes, seed)| {
            let mut files: Vec<(usize, usize, Vec<u8>)> = sizes
                .iter()
                .enumerate()
                .map(|(i, n)| {
                    let data = (0..*n)
                        .map(|k| (k as u8).wrapping_mul(31).wrapping_add(seed ^ i as u8))
                        .collect();
                    (i % 3, i, data)
                })
                .collect();
            if files.iter().all(|f| f.2.is_empty()) {
                files[0].2 = vec![seed; 1];
            }
            files
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    #[test]
    fn created_torrents_verify_every_piece(
        files in files_strategy(),
        big in any::<bool>(),
        version in prop_oneof![
            Just(TorrentVersion::V1),
            Just(TorrentVersion::V2),
            Just(TorrentVersion::Hybrid),
        ],
    ) {
        let dir = tempfile::TempDir::new().unwrap();
        let root = dir.path().join("content");
        fs::create_dir(&root).unwrap();
        write_tree(&root, &files);
        let piece_len: u32 = if big { 32 * 1024 } else { 16 * 1024 };
        let bytes = build(
            &CreateTorrent::new(&root).version(version).piece_len(piece_len),
        ).unwrap();
        let m = Metainfo::from_bytes(&bytes).unwrap();
        prop_assert_eq!(m.version(), version);
        prop_assert_eq!(m.info.piece_len, piece_len);

        // The torrent's byte stream, pads as zeros. For v2-only the
        // geometry is the parser's padded layout.
        let mut stream = Vec::new();
        let mut owner = Vec::new();
        for (i, f) in m.info.files.iter().enumerate() {
            if f.pad {
                stream.resize(stream.len() + f.len as usize, 0);
                continue;
            }
            let data = fs::read(dir.path().join(f.path.to_path_buf())).unwrap();
            prop_assert_eq!(data.len() as u64, f.len);
            stream.extend_from_slice(&data);
            owner.push(i);
        }
        let pl = piece_len as usize;
        if version != TorrentVersion::V2 {
            for (p, expected) in m.info.pieces.iter().enumerate() {
                let end = ((p + 1) * pl).min(stream.len());
                let h: [u8; 20] = Sha1::digest(&stream[p * pl..end]).into();
                prop_assert_eq!(&h, expected);
            }
            prop_assert_eq!(m.info.pieces.len(), stream.len().div_ceil(pl));
        }
        if version != TorrentVersion::V1 {
            for f in m.info.files.iter().filter(|f| f.len > u64::from(piece_len)) {
                prop_assert!(m.piece_layers.contains_key(&f.pieces_root.unwrap()));
            }
            for p in 0..stream.len().div_ceil(pl) {
                let Some(v2) = m.v2_piece(p as u32) else { continue };
                let data = &stream[p * pl..((p + 1) * pl).min(stream.len())];
                let got = piece_root(&data[..v2.len as usize], v2.leaves as usize);
                prop_assert_eq!(Some(got), v2.expected);
            }
        }
    }
}

#[test]
fn hybrid_and_v2_hash_the_same_tree() {
    let dir = tempfile::TempDir::new().unwrap();
    let root = dir.path().join("c");
    write_tree(
        &root,
        &[(0, 0, vec![1; 40_000]), (1, 1, vec![2; 10]), (2, 2, vec![])],
    );
    let v2 = build(&CreateTorrent::new(&root).version(TorrentVersion::V2)).unwrap();
    let hy = build(&CreateTorrent::new(&root).version(TorrentVersion::Hybrid)).unwrap();
    assert_eq!(file_tree_bytes(&v2), file_tree_bytes(&hy));
}

#[test]
fn deterministic_output() {
    let dir = tempfile::TempDir::new().unwrap();
    let root = dir.path().join("c");
    write_tree(&root, &[(0, 0, vec![7; 50_000]), (1, 1, vec![9; 3])]);
    let c = CreateTorrent::new(&root);
    let a = build(&c).unwrap();
    assert_eq!(a, build(&c).unwrap());
    assert_eq!(Metainfo::from_bytes(&a).unwrap().creation_date, None);
    let dated = build(&c.clone().creation_date(1_700_000_000)).unwrap();
    assert_eq!(
        Metainfo::from_bytes(&dated).unwrap().creation_date,
        Some(1_700_000_000)
    );
}

#[test]
fn auto_piece_len_table() {
    assert_eq!(auto_piece_len(1), 16384);
    assert_eq!(auto_piece_len(2048 * 16384), 16384);
    assert_eq!(auto_piece_len(2048 * 16384 + 1), 32768);
    assert_eq!(auto_piece_len(u64::MAX), 16 << 20);
}

#[test]
fn empty_input_is_refused() {
    let dir = tempfile::TempDir::new().unwrap();
    let empty = dir.path().join("empty");
    fs::create_dir(&empty).unwrap();
    assert!(matches!(
        build(&CreateTorrent::new(&empty)),
        Err(CreateError::Empty)
    ));
    let zeros = dir.path().join("zeros");
    write_tree(&zeros, &[(0, 0, vec![]), (1, 1, vec![])]);
    assert!(matches!(
        build(&CreateTorrent::new(&zeros)),
        Err(CreateError::Empty)
    ));
}

#[cfg(unix)]
#[test]
fn symlinks_are_skipped() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::TempDir::new().unwrap();
    let root = dir.path().join("c");
    write_tree(&root, &[(0, 0, vec![1; 100])]);
    symlink(root.join("d0/f0"), root.join("link_file")).unwrap();
    symlink(root.join("d0"), root.join("link_dir")).unwrap();
    symlink(root.join("nowhere"), root.join("dangling")).unwrap();
    for v in [
        TorrentVersion::V1,
        TorrentVersion::V2,
        TorrentVersion::Hybrid,
    ] {
        let m =
            Metainfo::from_bytes(&build(&CreateTorrent::new(&root).version(v)).unwrap()).unwrap();
        let names: Vec<_> = m.info.files.iter().map(|f| f.path.key()).collect();
        assert_eq!(names.len(), 1, "{v:?}: {names:?}");
        assert!(names[0].ends_with("f0"));
    }
}

#[cfg(target_os = "linux")]
#[test]
fn non_utf8_name_is_refused() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    let dir = tempfile::TempDir::new().unwrap();
    let root = dir.path().join("c");
    fs::create_dir(&root).unwrap();
    fs::write(root.join(OsStr::from_bytes(b"\xff.bin")), b"x").unwrap();
    assert!(matches!(
        build(&CreateTorrent::new(&root)),
        Err(CreateError::InvalidName(_))
    ));
}

#[test]
fn file_changed_while_hashing() {
    let dir = tempfile::TempDir::new().unwrap();
    let root = dir.path().join("c");
    write_tree(&root, &[(0, 0, vec![1; 60_000]), (1, 1, vec![2; 60_000])]);
    let last = root.join("d1/f1");
    let mut first = true;
    let r = CreateTorrent::new(&root).build(&mut |_, _| {
        if std::mem::take(&mut first) {
            use std::io::Write as _;
            let mut f = fs::OpenOptions::new().append(true).open(&last).unwrap();
            f.write_all(b"more").unwrap();
        }
        ControlFlow::Continue(())
    });
    assert!(matches!(r, Err(CreateError::FileChanged(_))), "{r:?}");
}

#[test]
fn single_file_torrent() {
    let dir = tempfile::TempDir::new().unwrap();
    let file = dir.path().join("movie.bin");
    fs::write(&file, vec![5u8; 70_000]).unwrap();
    for v in [
        TorrentVersion::V1,
        TorrentVersion::V2,
        TorrentVersion::Hybrid,
    ] {
        let m =
            Metainfo::from_bytes(&build(&CreateTorrent::new(&file).version(v)).unwrap()).unwrap();
        assert!(m.info.single_file, "{v:?}");
        assert_eq!(m.info.name, "movie.bin");
        assert_eq!(m.total_len(), 70_000);
    }
}

#[test]
fn cancel_stops_hashing() {
    let dir = tempfile::TempDir::new().unwrap();
    let file = dir.path().join("a.bin");
    fs::write(&file, vec![5u8; 100_000]).unwrap();
    let r = CreateTorrent::new(&file).build(&mut |_, _| ControlFlow::Break(()));
    assert!(matches!(r, Err(CreateError::Cancelled)));
}

#[test]
fn bad_piece_len_is_refused() {
    let dir = tempfile::TempDir::new().unwrap();
    let file = dir.path().join("a.bin");
    fs::write(&file, b"x").unwrap();
    for bad in [0, 1000, 16384 + 1, 8192, 512 << 20] {
        assert!(matches!(
            build(&CreateTorrent::new(&file).piece_len(bad)),
            Err(CreateError::InvalidPieceLen(n)) if n == bad
        ));
    }
}

#[test]
fn private_trackers_and_seeds_round_trip() {
    let dir = tempfile::TempDir::new().unwrap();
    let file = dir.path().join("a.bin");
    fs::write(&file, vec![1u8; 20_000]).unwrap();
    let bytes = build(
        &CreateTorrent::new(&file)
            .private(true)
            .trackers(vec![
                vec!["http://t1.example/a".into(), "http://t2.example/a".into()],
                vec!["udp://t3.example:80".into()],
            ])
            .web_seeds(vec!["https://seed.example/a.bin".into()])
            .comment("hello".into())
            .created_by("fbt".into()),
    )
    .unwrap();
    let m = Metainfo::from_bytes(&bytes).unwrap();
    assert!(m.info.private);
    assert_eq!(
        m.trackers,
        vec![
            vec![
                "http://t1.example/a".to_string(),
                "http://t2.example/a".into()
            ],
            vec!["udp://t3.example:80".to_string()],
        ]
    );
    assert_eq!(m.web_seeds, vec!["https://seed.example/a.bin".to_string()]);
    assert_eq!(m.comment.as_deref(), Some("hello"));
    assert_eq!(m.created_by.as_deref(), Some("fbt"));
}

#[test]
fn one_file_directory_stays_a_directory() {
    let dir = tempfile::TempDir::new().unwrap();
    let root = dir.path().join("content");
    fs::create_dir(&root).unwrap();
    let data: Vec<u8> = (0..50_000u32).map(|k| k as u8).collect();
    fs::write(root.join("a.bin"), &data).unwrap();
    let bytes = build(&CreateTorrent::new(&root).version(TorrentVersion::V2)).unwrap();
    let m = Metainfo::from_bytes(&bytes).unwrap();
    assert!(!m.info.single_file);
    assert_eq!(m.info.files.len(), 1);
    assert_eq!(m.info.files[0].path.components(), ["content", "a.bin"]);
    let pl = m.info.piece_len as usize;
    for p in 0..data.len().div_ceil(pl) {
        let v2 = m.v2_piece(p as u32).unwrap();
        let chunk = &data[p * pl..((p + 1) * pl).min(data.len())];
        let got = piece_root(&chunk[..v2.len as usize], v2.leaves as usize);
        assert_eq!(Some(got), v2.expected);
    }
}
