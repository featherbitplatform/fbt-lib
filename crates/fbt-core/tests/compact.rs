use std::net::SocketAddr;

use fbt_core::compact::{
    COMPACT_V6_LEN, CompactError, decode_v4, decode_v4_list, decode_v6_list, encode, encode_v4,
};

#[test]
fn v4_roundtrip_and_layout() {
    let a: SocketAddr = "10.1.2.3:6881".parse().unwrap();
    let mut out = Vec::new();
    encode(a, &mut out);
    assert_eq!(out, [10, 1, 2, 3, 0x1a, 0xe1]);
    assert_eq!(decode_v4(&encode_v4("10.1.2.3:6881".parse().unwrap())), a);
}

#[test]
fn v6_roundtrip_and_layout() {
    let a: SocketAddr = "[::1]:51413".parse().unwrap();
    let mut out = Vec::new();
    encode(a, &mut out);
    assert_eq!(out.len(), COMPACT_V6_LEN);
    assert_eq!(&out[16..], [0xc8, 0xd5]);
}

#[test]
fn list_cut_to_max_and_misaligned_rejected() {
    assert_eq!(decode_v4_list(&[1u8; 6 * 1000], 200).unwrap().len(), 200);
    assert_eq!(
        decode_v4_list(&[1u8; 7], 200),
        Err(CompactError::Misaligned)
    );
    assert_eq!(
        decode_v6_list(&[1u8; 19], 200),
        Err(CompactError::Misaligned)
    );
}
