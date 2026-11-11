use bytes::Bytes;
use fbt_bencode::{DictBuilder, encode};
use fbt_wire::{
    ExtError, ExtHandshake, HP_NOT_CONNECTED, HolepunchMsg, MAX_EXT_HANDSHAKE_LEN, MAX_PEX_ENTRIES,
    MetadataMsg, PexFlags, PexMsg, Reserved, UT_METADATA, UT_PEX, decode_donthave,
    decode_upload_only, encode_donthave, encode_upload_only,
};
use proptest::prelude::*;

#[test]
fn reserved_bits() {
    let mut r = Reserved::default();
    r.set_extended();
    r.set_dht();
    r.set_fast();
    assert_eq!(r.0, [0, 0, 0, 0, 0, 0x10, 0, 0x05]);
    assert!(r.supports_extended() && r.supports_dht() && r.supports_fast());
}

#[test]
fn ext_handshake_roundtrip_and_disable() {
    let mut h = ExtHandshake {
        listen_port: Some(6881),
        reqq: Some(250),
        metadata_size: Some(31_235),
        client: Some("fbt/0.2.0".into()),
        your_ip: Some("10.1.2.3".parse().unwrap()),
        ..Default::default()
    };
    h.m.insert(UT_METADATA.into(), 2);
    h.m.insert(UT_PEX.into(), 0);
    let back = ExtHandshake::decode(&h.encode()).unwrap();
    assert_eq!(back, h);
    assert_eq!(back.id_of(UT_METADATA), Some(2));
    assert_eq!(back.id_of(UT_PEX), None);
}

#[test]
fn ext_handshake_hostile_fields() {
    let mut m = DictBuilder::new();
    for i in 0..100 {
        m.insert(format!("x{i}").as_bytes(), 1i64);
    }
    let mut d = DictBuilder::new();
    d.insert(b"m", m.build())
        .insert(b"metadata_size", -5i64)
        .insert(b"p", 0i64)
        .insert(b"yourip", &b"abc"[..]);
    let h = ExtHandshake::decode(&encode(&d.build())).unwrap();
    assert_eq!(
        (h.metadata_size, h.listen_port, h.your_ip),
        (None, None, None)
    );
    assert_eq!(h.m.len(), 64);
    assert!(matches!(
        ExtHandshake::decode(&vec![b'x'; MAX_EXT_HANDSHAKE_LEN + 1]),
        Err(ExtError::TooLarge)
    ));
}

#[test]
fn ut_metadata_bep9_vectors() {
    assert_eq!(
        MetadataMsg::Request { piece: 0 }.encode(),
        &b"d8:msg_typei0e5:piecei0ee"[..]
    );
    assert_eq!(
        MetadataMsg::Reject { piece: 0 }.encode(),
        &b"d8:msg_typei2e5:piecei0ee"[..]
    );
    let wire = Bytes::from_static(b"d8:msg_typei1e5:piecei0e10:total_sizei34256eeDATA");
    assert_eq!(
        MetadataMsg::decode(&wire).unwrap(),
        MetadataMsg::Data {
            piece: 0,
            total_size: 34_256,
            data: Bytes::from_static(b"DATA")
        }
    );
}

#[test]
fn ut_metadata_rejects_oversized_data_and_bad_type() {
    let mut big = b"d8:msg_typei1e5:piecei0e10:total_sizei99999ee".to_vec();
    big.extend(vec![0; 16_385]);
    assert!(matches!(
        MetadataMsg::decode(&Bytes::from(big)),
        Err(ExtError::InvalidField("data"))
    ));
    assert!(matches!(
        MetadataMsg::decode(&Bytes::from_static(b"d8:msg_typei7e5:piecei0ee")),
        Err(ExtError::InvalidField("msg_type"))
    ));
}

#[test]
fn pex_roundtrip_flags_and_caps() {
    let m = PexMsg {
        added: vec![
            (
                "10.0.0.1:6881".parse().unwrap(),
                PexFlags::SEED | PexFlags::CONNECTABLE,
            ),
            ("[::1]:51413".parse().unwrap(), PexFlags::ENCRYPTION),
        ],
        dropped: vec!["10.0.0.2:1".parse().unwrap()],
    };
    assert_eq!(PexMsg::decode(&m.encode()).unwrap(), m);
    let mut d = DictBuilder::new();
    d.insert(b"added", vec![1u8; 6 * 1000]);
    assert_eq!(
        PexMsg::decode(&encode(&d.build())).unwrap().added.len(),
        MAX_PEX_ENTRIES
    );
    let mut d = DictBuilder::new();
    d.insert(b"added", vec![1u8; 7]);
    assert!(matches!(
        PexMsg::decode(&encode(&d.build())),
        Err(ExtError::InvalidField(_))
    ));
}

proptest! {
    #[test]
    fn ext_decoders_never_panic(b in proptest::collection::vec(any::<u8>(), 0..512)) {
        let _ = ExtHandshake::decode(&b);
        let _ = MetadataMsg::decode(&Bytes::from(b.clone()));
        let _ = PexMsg::decode(&b);
        let _ = HolepunchMsg::decode(&b);
        let _ = decode_donthave(&b);
        let _ = decode_upload_only(&b);
    }
}

#[test]
fn holepunch_bep55_layout() {
    let m = HolepunchMsg::Rendezvous("10.0.0.1:6881".parse().unwrap());
    assert_eq!(&m.encode()[..], &[0, 0, 10, 0, 0, 1, 0x1a, 0xe1]);
    let e = HolepunchMsg::Error {
        addr: "[::1]:80".parse().unwrap(),
        code: HP_NOT_CONNECTED,
    };
    let wire = e.encode();
    assert_eq!(wire.len(), 1 + 1 + 16 + 2 + 4);
    assert_eq!(HolepunchMsg::decode(&wire).unwrap(), e);
    assert!(matches!(
        HolepunchMsg::decode(&[9, 0, 1, 2, 3, 4, 0, 1]),
        Err(ExtError::InvalidField("msg_type"))
    ));
    assert!(matches!(
        HolepunchMsg::decode(&[1, 2, 1, 2, 3, 4, 0, 1]),
        Err(ExtError::InvalidField("addr_type"))
    ));
    assert!(matches!(
        HolepunchMsg::decode(&[1, 0, 1, 2, 3, 4, 0, 0]),
        Err(ExtError::InvalidField("port"))
    ));
    assert!(matches!(
        HolepunchMsg::decode(&[2, 0, 1, 2, 3, 4, 0, 1]),
        Err(ExtError::InvalidField("holepunch"))
    ));
}

#[test]
fn donthave_and_upload_only() {
    assert_eq!(
        decode_donthave(&encode_donthave(0xdead_beef)).unwrap(),
        0xdead_beef
    );
    assert!(decode_donthave(&[1, 2, 3]).is_err());
    assert!(decode_upload_only(&encode_upload_only(true)).unwrap());
    assert!(decode_upload_only(&[]).is_err());
    let h = ExtHandshake {
        upload_only: Some(true),
        ..Default::default()
    };
    assert_eq!(
        ExtHandshake::decode(&h.encode()).unwrap().upload_only,
        Some(true)
    );
}
