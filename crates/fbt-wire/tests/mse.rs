//! MSE/PE handshake: in-memory initiator <-> responder runs.

use fbt_wire::{
    CryptoMethods, MseError, MseStream, initiate, is_plaintext_handshake_prefix, req2_hash, respond,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn lookup_for(ih: [u8; 20]) -> impl Fn(&[u8; 20]) -> Option<[u8; 20]> + Sync {
    move |h| (*h == req2_hash(&ih)).then_some(ih)
}

#[tokio::test]
async fn rc4_roundtrip_both_directions() {
    let (a, b) = tokio::io::duplex(8192);
    let ih = [0xaa; 20];
    let l = lookup_for(ih);
    let (i, r) = tokio::join!(
        initiate(a, &ih, CryptoMethods::RC4, b""),
        respond(b, Vec::new(), &l, CryptoMethods::all())
    );
    let (mut i, (mut r, skey)) = (i.unwrap(), r.unwrap());
    assert_eq!(skey, ih);
    assert!(i.is_encrypted() && r.is_encrypted());
    i.write_all(b"hello").await.unwrap();
    i.flush().await.unwrap();
    let mut buf = [0; 5];
    r.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");
    r.write_all(b"world").await.unwrap();
    r.flush().await.unwrap();
    i.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"world");
}

#[tokio::test]
async fn plaintext_selected_when_responder_allows_only_plaintext() {
    let (a, b) = tokio::io::duplex(8192);
    let ih = [0xaa; 20];
    let l = lookup_for(ih);
    let (i, r) = tokio::join!(
        initiate(a, &ih, CryptoMethods::all(), b""),
        respond(b, Vec::new(), &l, CryptoMethods::PLAINTEXT)
    );
    let (mut i, (mut r, _)) = (i.unwrap(), r.unwrap());
    assert!(!i.is_encrypted() && !r.is_encrypted());
    i.write_all(b"hello").await.unwrap();
    i.flush().await.unwrap();
    let mut buf = [0; 5];
    r.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");
}

#[tokio::test]
async fn forced_initiator_vs_plaintext_only_responder() {
    let (a, b) = tokio::io::duplex(8192);
    let ih = [0xaa; 20];
    let l = lookup_for(ih);
    let (i, r) = tokio::join!(
        initiate(a, &ih, CryptoMethods::RC4, b""),
        respond(b, Vec::new(), &l, CryptoMethods::PLAINTEXT)
    );
    assert!(matches!(
        i,
        Err(MseError::NoCommonMethod) | Err(MseError::Io(_))
    ));
    assert!(matches!(r, Err(MseError::NoCommonMethod)));
}

#[tokio::test]
async fn initial_payload_is_first_read_on_responder() {
    let (a, b) = tokio::io::duplex(8192);
    let ih = [0xaa; 20];
    let l = lookup_for(ih);
    let ia: Vec<u8> = [&b"\x13BitTorrent protocol"[..], &[7u8; 48]].concat();
    assert_eq!(ia.len(), 68);
    let (i, r) = tokio::join!(
        initiate(a, &ih, CryptoMethods::RC4, &ia),
        respond(b, Vec::new(), &l, CryptoMethods::all())
    );
    let (_i, (mut r, _)) = (i.unwrap(), r.unwrap());
    let mut got = [0u8; 68];
    r.read_exact(&mut got).await.unwrap();
    assert_eq!(&got[..], &ia[..]);
}

#[tokio::test]
async fn unknown_torrent_rejected() {
    let (a, b) = tokio::io::duplex(8192);
    let (_i, r) = tokio::join!(
        initiate(a, &[0xaa; 20], CryptoMethods::RC4, b""),
        respond(b, Vec::new(), &|_: &[u8; 20]| None, CryptoMethods::all())
    );
    assert!(matches!(r, Err(MseError::UnknownTorrent)));
}

#[tokio::test]
async fn respond_gives_up_without_sync() {
    let junk: Vec<u8> = (0..2000u32).map(|i| (i * 7 + 3) as u8).collect();
    let r = respond(
        tokio::io::join(&junk[..], tokio::io::sink()),
        Vec::new(),
        &|_: &[u8; 20]| None,
        CryptoMethods::all(),
    )
    .await;
    assert!(matches!(r, Err(MseError::NoSync)));
}

#[test]
fn plaintext_prefix_detection() {
    assert!(is_plaintext_handshake_prefix(b"\x13BitTorrent protocol"));
    assert!(is_plaintext_handshake_prefix(b"\x13BitT"));
    assert!(!is_plaintext_handshake_prefix(&[0x13; 20]));
}

#[test]
fn req2_hash_vector() {
    assert_eq!(
        hex(&req2_hash(&[0xaa; 20])),
        "c9b0599dfa950cfbde978efc6aa0588f510fa315"
    );
}

// Beyond the brief's list: the remaining crypto_select outcomes, detection
// prefix handling and both payload directions after a plaintext select.

#[tokio::test]
async fn plaintext_only_initiator_gets_plaintext_from_permissive_responder() {
    let (a, b) = tokio::io::duplex(8192);
    let ih = [0x11; 20];
    let l = lookup_for(ih);
    let (i, r) = tokio::join!(
        initiate(a, &ih, CryptoMethods::PLAINTEXT, b"ia"),
        respond(b, Vec::new(), &l, CryptoMethods::all())
    );
    let (mut i, (mut r, _)) = (i.unwrap(), r.unwrap());
    assert!(!i.is_encrypted() && !r.is_encrypted());
    let mut buf = [0; 2];
    r.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ia");
    r.write_all(b"back").await.unwrap();
    r.flush().await.unwrap();
    let mut buf = [0; 4];
    i.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"back");
}

#[tokio::test]
async fn rc4_preferred_when_both_offered() {
    let (a, b) = tokio::io::duplex(8192);
    let ih = [0x22; 20];
    let l = lookup_for(ih);
    let (i, r) = tokio::join!(
        initiate(a, &ih, CryptoMethods::all(), b""),
        respond(b, Vec::new(), &l, CryptoMethods::all())
    );
    let (i, (r, _)) = (i.unwrap(), r.unwrap());
    assert!(i.is_encrypted() && r.is_encrypted());
}

#[tokio::test]
async fn plaintext_initiator_vs_rc4_only_responder() {
    let (a, b) = tokio::io::duplex(8192);
    let ih = [0x33; 20];
    let l = lookup_for(ih);
    let (i, r) = tokio::join!(
        initiate(a, &ih, CryptoMethods::PLAINTEXT, b""),
        respond(b, Vec::new(), &l, CryptoMethods::RC4)
    );
    assert!(matches!(
        i,
        Err(MseError::NoCommonMethod) | Err(MseError::Io(_))
    ));
    assert!(matches!(r, Err(MseError::NoCommonMethod)));
}

#[tokio::test]
async fn responder_uses_detection_prefix() {
    // The responder's caller already read a few bytes to rule out a
    // plaintext handshake; they must count as the start of Ya.
    let (a, mut b) = tokio::io::duplex(8192);
    let ih = [0x44; 20];
    let l = lookup_for(ih);
    let init = tokio::spawn(async move {
        let mut s = initiate(a, &[0x44; 20], CryptoMethods::RC4, b"")
            .await
            .unwrap();
        s.write_all(b"ping").await.unwrap();
        s.flush().await.unwrap();
        s
    });
    let mut first = [0u8; 20];
    b.read_exact(&mut first).await.unwrap();
    let (mut r, _) = respond(b, first.to_vec(), &l, CryptoMethods::all())
        .await
        .unwrap();
    let mut buf = [0; 4];
    r.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");
    init.await.unwrap();
}

#[tokio::test]
async fn degenerate_public_key_rejected() {
    let mut bytes = vec![0u8; 96];
    bytes.extend_from_slice(&[0u8; 100]);
    let r = respond(
        tokio::io::join(&bytes[..], tokio::io::sink()),
        Vec::new(),
        &|_: &[u8; 20]| None,
        CryptoMethods::all(),
    )
    .await;
    assert!(matches!(r, Err(MseError::BadPublicKey)));
}

#[tokio::test]
async fn plaintext_stream_serves_prefix_first() {
    let (a, mut b) = tokio::io::duplex(64);
    b.write_all(b"rest").await.unwrap();
    let mut s = MseStream::plaintext(a, b"pre".to_vec());
    assert!(!s.is_encrypted());
    let mut buf = [0; 7];
    s.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"prerest");
}
