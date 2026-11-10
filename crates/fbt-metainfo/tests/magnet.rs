mod support;

use fbt_core::{InfoHashV1, InfoHashes};
use fbt_metainfo::{Magnet, MagnetError, Metainfo, MetainfoError};
use proptest::prelude::*;

const HEX: &str = "c12fe1c06bba254a9dc9f519b335aa7c1367a88a";

#[test]
fn parses_full_magnet() {
    let m = Magnet::parse(&format!("magnet:?xt=urn:btih:{HEX}&dn=My+Name&tr=udp%3A%2F%2Ft.example%3A80&tr=http%3A%2F%2Fa.example%2Fannounce&tr=udp%3A%2F%2Ft.example%3A80&x.pe=10.0.0.1:6881&x.pe=[::1]:51413&x.pe=host.example:1&so=0,2,4-6")).unwrap();
    assert_eq!(m.info_hashes.v1.unwrap().to_string(), HEX);
    assert_eq!(m.display_name.as_deref(), Some("My Name"));
    assert_eq!(
        m.trackers,
        ["udp://t.example:80", "http://a.example/announce"]
    );
    assert_eq!(m.peers.len(), 2);
    assert!(m.is_selected(5) && !m.is_selected(3) && !m.is_selected(7));
}

#[test]
fn base32_equals_hex() {
    let a = Magnet::parse("magnet:?xt=urn:btih:YEX6DQDLXISUVHOJ6UM3GNNKPQJWPKEK").unwrap();
    assert_eq!(a.info_hashes.v1.unwrap().to_string(), HEX);
    let b = Magnet::parse("MAGNET:?xt=urn:btih:yex6dqdlxisuvhoj6um3gnnkpqjwpkek").unwrap();
    assert_eq!(a, b);
}

#[test]
fn btmh_and_hybrid() {
    let v2 = "a".repeat(64);
    let m = Magnet::parse(&format!("magnet:?xt=urn:btih:{HEX}&xt=urn:btmh:1220{v2}")).unwrap();
    assert!(m.info_hashes.v1.is_some() && m.info_hashes.v2.is_some());
    assert!(matches!(
        Magnet::parse(&format!("magnet:?xt=urn:btmh:1114{v2}")),
        Err(MagnetError::InvalidInfoHash(_))
    ));
}

#[test]
fn rejects_bad_magnets() {
    assert!(matches!(
        Magnet::parse("http://x"),
        Err(MagnetError::NotMagnet)
    ));
    assert!(matches!(
        Magnet::parse("magnet:?dn=x"),
        Err(MagnetError::MissingInfoHash)
    ));
    assert!(matches!(
        Magnet::parse("magnet:?xt=urn:btih:zz"),
        Err(MagnetError::InvalidInfoHash(_))
    ));
    assert!(matches!(
        Magnet::parse(&format!(
            "magnet:?xt=urn:btih:{HEX}&xt=urn:btih:{}",
            "b".repeat(40)
        )),
        Err(MagnetError::ConflictingInfoHash)
    ));
    assert!(matches!(
        Magnet::parse(&format!("magnet:?xt=urn:btih:{HEX}&dn=%zz")),
        Err(MagnetError::BadEncoding)
    ));
    assert!(matches!(
        Magnet::parse(&format!("magnet:?xt=urn:btih:{HEX}&so=5-2")),
        Err(MagnetError::InvalidSelection)
    ));
    assert!(matches!(
        Magnet::parse(&format!(
            "magnet:?xt=urn:btih:{HEX}&tr={}",
            "a".repeat(70_000)
        )),
        Err(MagnetError::TooLong)
    ));
}

#[test]
fn to_uri_roundtrips() {
    let m = Magnet::parse(&format!("magnet:?xt=urn:btih:{HEX}&dn=My+Name&tr=udp%3A%2F%2Ft.example%3A80&x.pe=10.0.0.1:6881&x.pe=[::1]:51413&so=0,2,4-6")).unwrap();
    assert_eq!(Magnet::parse(&m.to_uri()).unwrap(), m);
}

#[test]
fn from_info_bytes_verifies_hash() {
    let meta = Metainfo::from_bytes(&support::single_fixture()).unwrap();
    let raw = meta.raw_info().to_vec();
    let again =
        Metainfo::from_info_bytes(&raw, meta.info_hashes, vec![vec!["http://t/a".into()]]).unwrap();
    assert_eq!(again.info, meta.info);
    assert_eq!(again.trackers, vec![vec!["http://t/a".to_string()]]);
    let wrong = InfoHashes::from_v1(InfoHashV1([0; 20]));
    assert!(matches!(
        Metainfo::from_info_bytes(&raw, wrong, vec![]),
        Err(MetainfoError::InfoHashMismatch)
    ));
}

#[test]
fn from_info_bytes_uses_explicit_limits() {
    let deep = [b"l".repeat(40), b"e".repeat(40)].concat();
    // depth 40 > 32, fails before any hash check
    assert!(
        Metainfo::from_info_bytes(&deep, InfoHashes::from_v1(InfoHashV1([0; 20])), vec![]).is_err()
    );
}

#[test]
fn metainfo_magnet_link() {
    let meta = Metainfo::from_bytes(&support::single_fixture()).unwrap();
    let back = Magnet::parse(&meta.magnet().to_uri()).unwrap();
    assert_eq!(back.info_hashes, meta.info_hashes);
    assert_eq!(back.display_name.as_deref(), Some(meta.info.name.as_str()));
}

proptest! {
    #[test]
    fn magnet_parse_never_panics(s in ".*") {
        let _ = Magnet::parse(&format!("magnet:?{s}"));
    }
}
