use bytes::{Bytes, BytesMut};
use fbt_core::{BlockRef, PeerId};
use fbt_wire::{
    HANDSHAKE_LEN, Handshake, HandshakeError, Message, MessageCodec, Reserved, WireError,
    allowed_fast_set, read_handshake, write_handshake,
};
use proptest::prelude::*;
use tokio_util::codec::{Decoder, Encoder};

fn hs() -> Handshake {
    let mut reserved = Reserved([0; 8]);
    reserved.set_fast();
    Handshake {
        reserved,
        info_hash: [7; 20],
        peer_id: PeerId([9; 20]),
    }
}

#[test]
fn handshake_roundtrip_and_bad_protocol() {
    let h = hs();
    let buf = h.encode();
    assert_eq!(buf[0], 19);
    assert_eq!(&buf[1..20], b"BitTorrent protocol");
    assert_eq!(Handshake::decode(&buf).unwrap(), h);
    assert!(h.reserved.supports_fast());
    assert!(!h.reserved.supports_extended());
    assert!(!h.reserved.supports_dht());
    let mut bad = buf;
    bad[0] = 18;
    assert!(matches!(
        Handshake::decode(&bad),
        Err(HandshakeError::BadProtocol)
    ));
    let mut bad = buf;
    bad[1] = b'X';
    assert!(matches!(
        Handshake::decode(&bad),
        Err(HandshakeError::BadProtocol)
    ));
}

#[tokio::test]
async fn handshake_async_roundtrip() {
    let h = hs();
    let mut v = Vec::new();
    write_handshake(&mut v, &h).await.unwrap();
    assert_eq!(v.len(), HANDSHAKE_LEN);
    let got = read_handshake(&mut &v[..]).await.unwrap();
    assert_eq!(got, h);
    let short = &v[..30];
    assert!(matches!(
        read_handshake(&mut &short[..]).await,
        Err(HandshakeError::Io(_))
    ));
}

fn br() -> BlockRef {
    BlockRef {
        piece: 3,
        offset: 16384,
        len: 16384,
    }
}

fn all_variants() -> Vec<Message> {
    vec![
        Message::KeepAlive,
        Message::Choke,
        Message::Unchoke,
        Message::Interested,
        Message::NotInterested,
        Message::Have(42),
        Message::Bitfield(Bytes::from_static(&[0xff, 0x80])),
        Message::Request(br()),
        Message::Piece {
            piece: 1,
            offset: 2,
            data: Bytes::from_static(b"hello"),
        },
        Message::Cancel(br()),
        Message::Port(6881),
        Message::SuggestPiece(5),
        Message::HaveAll,
        Message::HaveNone,
        Message::RejectRequest(br()),
        Message::AllowedFast(8),
        Message::Extended {
            id: 0,
            payload: Bytes::from_static(b"d1:ai1ee"),
        },
        Message::Unknown {
            id: 99,
            payload: Bytes::from_static(b"xyz"),
        },
    ]
}

#[test]
fn message_roundtrip_all_variants() {
    let mut c = MessageCodec::new(1 << 20);
    for m in all_variants() {
        let mut b = BytesMut::new();
        c.encode(m.clone(), &mut b).unwrap();
        assert_eq!(c.decode(&mut b).unwrap(), Some(m));
        assert!(b.is_empty());
    }
}

#[test]
fn partial_frames_wait_for_more() {
    let mut c = MessageCodec::new(1 << 20);
    let mut full = BytesMut::new();
    c.encode(Message::Have(1), &mut full).unwrap();
    let mut b = BytesMut::from(&full[..3]);
    assert_eq!(c.decode(&mut b).unwrap(), None);
    b.extend_from_slice(&full[3..8]);
    assert_eq!(c.decode(&mut b).unwrap(), None);
    b.extend_from_slice(&full[8..]);
    assert_eq!(c.decode(&mut b).unwrap(), Some(Message::Have(1)));
}

#[test]
fn oversized_frame_rejected_without_buffering() {
    let mut c = MessageCodec::new(1 << 20);
    let mut b = BytesMut::from(&[0xff, 0xff, 0xff, 0xff, 7][..]);
    assert!(matches!(c.decode(&mut b), Err(WireError::TooLong { .. })));
}

#[test]
fn fixed_length_violations() {
    let mut c = MessageCodec::new(1 << 20);
    let mut b = BytesMut::from(&[0, 0, 0, 4, 4, 0, 0, 0][..]);
    assert!(matches!(
        c.decode(&mut b),
        Err(WireError::BadLength { id: 4, len: 4 })
    ));
    // Piece with a short header, Extended with no id byte, Choke with payload.
    let mut b = BytesMut::from(&[0, 0, 0, 5, 7, 0, 0, 0, 1][..]);
    assert!(matches!(
        c.decode(&mut b),
        Err(WireError::BadLength { id: 7, .. })
    ));
    let mut b = BytesMut::from(&[0, 0, 0, 1, 20][..]);
    assert!(matches!(
        c.decode(&mut b),
        Err(WireError::BadLength { id: 20, .. })
    ));
    let mut b = BytesMut::from(&[0, 0, 0, 2, 0, 1][..]);
    assert!(matches!(
        c.decode(&mut b),
        Err(WireError::BadLength { id: 0, len: 2 })
    ));
}

#[test]
fn bep6_allowed_fast_vectors() {
    let ih = [0xaa; 20];
    let ip = "80.4.4.200".parse().unwrap();
    assert_eq!(
        allowed_fast_set(&ih, ip, 1313, 7),
        vec![1059, 431, 808, 1217, 287, 376, 1188]
    );
    assert_eq!(
        allowed_fast_set(&ih, ip, 1313, 9),
        vec![1059, 431, 808, 1217, 287, 376, 1188, 353, 508]
    );
    assert!(allowed_fast_set(&ih, ip, 0, 5).is_empty());
    assert_eq!(allowed_fast_set(&ih, ip, 3, 10).len(), 3);
}

proptest! {
    #[test]
    fn decode_never_panics(data in proptest::collection::vec(any::<u8>(), 0..2048),
                           chunks in proptest::collection::vec(1usize..64, 1..64)) {
        let mut c = MessageCodec::new(4096);
        let mut b = BytesMut::new();
        let mut pos = 0;
        let mut i = 0;
        while pos < data.len() {
            let n = chunks[i % chunks.len()].min(data.len() - pos);
            i += 1;
            b.extend_from_slice(&data[pos..pos + n]);
            pos += n;
            loop {
                match c.decode(&mut b) {
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(_) => return Ok(()),
                }
            }
        }
    }
}
