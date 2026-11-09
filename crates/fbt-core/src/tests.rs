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
    let id = PeerId::generate();
    assert_eq!(&id.0[..8], b"-FB0000-");
    assert_eq!(id.client_prefix(), *b"-FB0000-");
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
