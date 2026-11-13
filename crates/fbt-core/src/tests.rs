use super::*;

#[test]
fn info_hash_hex_roundtrip() {
    let h: InfoHashV1 = "aabbccddeeff00112233445566778899aabbccdd".parse().unwrap();
    assert_eq!(h.to_string(), "aabbccddeeff00112233445566778899aabbccdd");
    let h2: InfoHashV2 = "ab".repeat(32).parse().unwrap();
    assert_eq!(h2.to_string(), "ab".repeat(32));
    assert!("zz".repeat(20).parse::<InfoHashV1>().is_err());
    assert!("aa".repeat(19).parse::<InfoHashV1>().is_err());
    assert!("é".repeat(20).parse::<InfoHashV1>().is_err());
}

#[test]
fn info_hash_of_known_vectors() {
    assert_eq!(
        InfoHashV1::of(b"abc").to_string(),
        "a9993e364706816aba3e25717850c26c9cd0d89d"
    );
    assert_eq!(
        InfoHashV2::of(b"abc").to_string(),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[test]
fn wire_hash_prefers_v1_else_truncates_v2() {
    let v1 = InfoHashV1([1; 20]);
    let mut raw = [2u8; 32];
    raw[20..].fill(9);
    let v2 = InfoHashV2(raw);
    assert_eq!(InfoHashes::hybrid(v1, v2).wire_hash(), [1; 20]);
    assert_eq!(InfoHashes::from_v1(v1).wire_hash(), [1; 20]);
    assert_eq!(InfoHashes::from_v2(v2).wire_hash(), [2; 20]);
    assert_eq!(InfoHashes::hybrid(v1, v2).to_string(), v1.to_string());
    assert_eq!(InfoHashes::from_v2(v2).to_string(), v2.to_string());
}

#[test]
fn peer_id_prefix() {
    // The prefix tracks the crate version: -FB<major><minor><patch>0-.
    let digit = |v: &str| {
        v.parse::<u8>().map_or(b'?', |n| {
            b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"[usize::from(n.min(35))]
        })
    };
    let expected = [
        b'-',
        b'F',
        b'B',
        digit(env!("CARGO_PKG_VERSION_MAJOR")),
        digit(env!("CARGO_PKG_VERSION_MINOR")),
        digit(env!("CARGO_PKG_VERSION_PATCH")),
        b'0',
        b'-',
    ];
    let id = PeerId::generate();
    assert_eq!(&id.0[..8], &expected);
    assert_eq!(id.client_prefix(), expected);
    assert!(id.0[8..].iter().all(u8::is_ascii_alphanumeric));
}

#[test]
fn bitfield_msb_first_and_spare_bits() {
    let mut b = Bitfield::new(10);
    b.set(0);
    b.set(9);
    assert_eq!(b.as_bytes(), &[0x80, 0x40]);
    assert!(matches!(
        Bitfield::from_bytes(&[0xff, 0xff], 10),
        Err(BitfieldError::SpareBitsSet)
    ));
    assert!(matches!(
        Bitfield::from_bytes(&[0xff], 10),
        Err(BitfieldError::WrongLength)
    ));
    assert!(!b.get(1000));
    b.set(1000);
    b.clear(1000);
    assert_eq!(b.count_ones(), 2);
    assert_eq!(b.iter_ones().collect::<Vec<_>>(), vec![0, 9]);
    b.clear(0);
    assert!(!b.get(0));
}

#[test]
fn bitfield_full_and_empty() {
    let f = Bitfield::full(10);
    assert_eq!(f.as_bytes(), &[0xff, 0xc0]);
    assert!(f.is_full());
    assert_eq!(f.count_ones(), 10);
    let e = Bitfield::new(0);
    assert!(e.is_empty());
    assert!(e.as_bytes().is_empty());
    assert!(Bitfield::from_bytes(&[], 0).is_ok());
    assert_eq!(Bitfield::full(16).iter_ones().count(), 16);
}

#[test]
fn piece_layout_last_piece_and_blocks() {
    let l = PieceLayout::new(100_000, 32_768).unwrap();
    assert_eq!(l.num_pieces(), 4);
    assert_eq!(l.piece_size(3), 100_000 - 3 * 32_768);
    assert_eq!(l.blocks_in_piece(0), 2);
    assert_eq!(
        l.block(3, 0),
        BlockRef {
            piece: 3,
            offset: 0,
            len: 1_696
        }
    );
    assert!(!l.is_valid_block(&BlockRef {
        piece: 3,
        offset: 0,
        len: 16_384
    }));
}

#[test]
fn piece_layout_hostile_inputs() {
    assert_eq!(PieceLayout::new(0, 1), Err(LayoutError::EmptyTotal));
    assert_eq!(PieceLayout::new(1, 0), Err(LayoutError::ZeroPieceLen));
    assert_eq!(
        PieceLayout::new(u64::MAX, 1),
        Err(LayoutError::TooManyPieces)
    );
    let l = PieceLayout::new(100_000, 32_768).unwrap();
    assert_eq!(l.piece_size(4), 0);
    assert_eq!(l.piece_offset(u32::MAX), 0);
    assert_eq!(l.blocks_in_piece(u32::MAX), 0);
    assert_eq!(l.block(0, 5).len, 0);
    assert_eq!(l.block(0, u32::MAX).len, 0);
    let ok = BlockRef {
        piece: 0,
        offset: 16_384,
        len: 16_384,
    };
    assert!(l.is_valid_block(&ok));
    assert!(!l.is_valid_block(&BlockRef { len: 0, ..ok }));
    assert!(!l.is_valid_block(&BlockRef { len: 16_385, ..ok }));
    assert!(!l.is_valid_block(&BlockRef { offset: 100, ..ok }));
    assert!(!l.is_valid_block(&BlockRef {
        offset: 32_768,
        ..ok
    }));
    assert!(!l.is_valid_block(&BlockRef {
        offset: u32::MAX - 16_383,
        len: 16_384,
        piece: 0
    }));
    assert!(!l.is_valid_block(&BlockRef { piece: 9, ..ok }));
}

#[test]
fn file_layout_spans_tiny_and_empty_files() {
    let f = FileLayout::new([3, 0, 1, 0, 10]);
    assert_eq!(
        f.map(2, 4),
        vec![
            FileSlice {
                file: 0,
                file_offset: 2,
                len: 1
            },
            FileSlice {
                file: 2,
                file_offset: 0,
                len: 1
            },
            FileSlice {
                file: 4,
                file_offset: 0,
                len: 2
            },
        ]
    );
    assert_eq!(f.files_overlapping(2, 4), 0..5);
    assert_eq!(f.total_len(), 14);
    assert_eq!(f.num_files(), 5);
    assert_eq!(f.map(12, u64::MAX).len(), 1);
    assert!(f.map(14, 5).is_empty());
    assert!(f.map(u64::MAX, u64::MAX).is_empty());
    assert!(FileLayout::new([]).map(0, 1).is_empty());
}

mod merkle_tests {
    use crate::merkle::*;
    use proptest::prelude::*;
    use sha2::{Digest, Sha256};

    fn sha(data: &[u8]) -> Hash256 {
        Sha256::digest(data).into()
    }

    fn cat(a: &Hash256, b: &Hash256) -> Hash256 {
        let mut h = Sha256::new();
        h.update(a);
        h.update(b);
        h.finalize().into()
    }

    #[test]
    fn pad_hash_chain() {
        assert_eq!(pad_hash(0), [0; 32]);
        assert_eq!(pad_hash(1), sha(&[0u8; 64]));
        assert_eq!(pad_hash(2), cat(&pad_hash(1), &pad_hash(1)));
        assert_eq!(pad_hash(MAX_TREE_HEIGHT + 5), pad_hash(MAX_TREE_HEIGHT));
    }

    #[test]
    fn single_block_root_is_its_hash() {
        assert_eq!(piece_root(b"abc", 1), sha(b"abc"));
    }

    #[test]
    fn short_file_pads_with_zero_leaves() {
        let data: Vec<u8> = (0..20_000u32).map(|i| (i % 251) as u8).collect();
        let leaf0 = sha(&data[..16_384]);
        let leaf1 = sha(&data[16_384..]);
        assert_eq!(data[16_384..].len(), 3_616);
        let n = 2usize.pow(file_tree_height(data.len() as u64));
        assert_eq!(piece_root(&data, n), cat(&leaf0, &leaf1));

        let data: Vec<u8> = (0..40_000u32).map(|i| (i % 241) as u8).collect();
        let l: Vec<Hash256> = data.chunks(16_384).map(sha).collect();
        assert_eq!(l.len(), 3);
        let expect = cat(&cat(&l[0], &l[1]), &cat(&l[2], &[0; 32]));
        assert_eq!(piece_root(&data, 4), expect);
        assert_eq!(file_tree_height(40_000), 2);
        assert_eq!(file_tree_height(0), 0);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        #[test]
        fn piece_roots_match_file_roots(
            len in 1usize..=40 * 16_384,
            shift in 0u32..3,
            seed in any::<u8>(),
        ) {
            let file: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed)).collect();
            let piece_leaves = 1usize << shift;
            let piece_len = piece_leaves * 16_384;
            let n_leaves = len.div_ceil(16_384);
            let file_root = layer_root(&leaf_hashes(&file), 0, n_leaves.next_power_of_two());
            if len > piece_len {
                let layer: Vec<Hash256> =
                    file.chunks(piece_len).map(|c| piece_root(c, piece_leaves)).collect();
                let n_pieces = layer.len();
                prop_assert_eq!(layer_root(&layer, shift, n_pieces.next_power_of_two()), file_root);
            } else {
                let w = 1usize << file_tree_height(len as u64);
                prop_assert_eq!(piece_root(&file, w), file_root);
            }
        }

        #[test]
        fn proofs_verify(
            raw in proptest::collection::vec(any::<[u8; 32]>(), 1..=300),
            len_pick in 0u32..7,
            idx_pick in any::<usize>(),
            flip_pick in any::<usize>(),
            flip_bit in 0u8..8,
        ) {
            let n = raw.len();
            let width = n.next_power_of_two();
            let tree_height = 2 + width.trailing_zeros();
            let tree = MerkleTree::from_layer(&raw, 2, tree_height).unwrap();
            let root = tree.root();
            prop_assert_eq!(root, layer_root(&raw, 2, width));

            let max_len = width.min(64);
            let len = (1usize << len_pick).min(max_len);
            let index = (idx_pick % (width / len)) * len;
            let s = len.trailing_zeros();
            let need = tree_height - 2 - s;
            for layers in 0..=20u32 {
                let (hs, proof) = tree.hashes(2, index, len, layers).unwrap();
                prop_assert_eq!(hs.len(), len);
                prop_assert_eq!(proof.len() as u32, layers.min(need));
                prop_assert_eq!(
                    verify_hashes(&root, tree_height, 2, index, &hs, &proof),
                    layers >= need
                );
            }
            let (hs, proof) = tree.hashes(2, index, len, 20).unwrap();
            let all: Vec<Hash256> = hs.iter().chain(proof.iter()).copied().collect();
            let pos = flip_pick % all.len();
            let mut bad = all.clone();
            if let Some(h) = bad.get_mut(pos)
                && let Some(b) = h.get_mut(flip_pick % 32)
            {
                *b ^= 1 << flip_bit;
            }
            let (bh, bp) = bad.split_at(len);
            prop_assert!(!verify_hashes(&root, tree_height, 2, index, bh, bp));
        }
    }

    #[test]
    fn verify_rejects_garbage() {
        let layer: Vec<Hash256> = (0..8u8).map(|i| [i; 32]).collect();
        let tree = MerkleTree::from_layer(&layer, 0, 3).unwrap();
        let root = tree.root();
        let (hs, proof) = tree.hashes(0, 2, 2, 5).unwrap();
        assert_eq!(proof.len(), 2);
        assert!(verify_hashes(&root, 3, 0, 2, &hs, &proof));
        // too few / too many uncles
        assert!(!verify_hashes(&root, 3, 0, 2, &hs, &proof[..1]));
        let mut more = proof.clone();
        more.push([0; 32]);
        assert!(!verify_hashes(&root, 3, 0, 2, &hs, &more));
        // wrong counts
        assert!(!verify_hashes(&root, 3, 0, 2, &hs[..1], &proof));
        let three = [hs[0], hs[1], hs[0]];
        assert!(!verify_hashes(&root, 3, 0, 2, &three, &proof));
        // index not a multiple of span, or out of range
        assert!(!verify_hashes(&root, 3, 0, 3, &hs, &proof));
        assert!(!verify_hashes(&root, 3, 0, 10, &hs, &proof));
        // base / height bounds
        assert!(!verify_hashes(&root, 3, 4, 2, &hs, &proof));
        assert!(!verify_hashes(
            &root,
            MAX_TREE_HEIGHT + 1,
            0,
            2,
            &hs,
            &proof
        ));
        assert!(!verify_hashes(&root, u32::MAX, 0, 2, &hs, &proof));
        // empty
        assert!(!verify_hashes(&root, 3, 0, 0, &[], &proof));
        assert!(!verify_hashes(&root, 0, 0, 0, &[], &[]));
        // tree refusals
        assert!(tree.hashes(0, 1, 2, 1).is_none());
        assert!(tree.hashes(0, 0, 3, 1).is_none());
        assert!(tree.hashes(0, 8, 1, 1).is_none());
        assert!(tree.hashes(4, 0, 1, 1).is_none());
        assert!(MerkleTree::from_layer(&layer, 0, 2).is_none());
        assert!(MerkleTree::from_layer(&layer, 0, MAX_TREE_HEIGHT + 1).is_none());
        let t2 = MerkleTree::from_layer(&layer[..4], 1, 4).unwrap();
        assert!(t2.hashes(0, 0, 1, 1).is_none());
    }
}
