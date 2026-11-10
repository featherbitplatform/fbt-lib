//! The five-step MSE handshake.
//!
//! ```text
//! 1 A->B: Ya, PadA
//! 2 B->A: Yb, PadB
//! 3 A->B: HASH('req1', S), HASH('req2', SKEY) xor HASH('req3', S),
//!         ENCRYPT(VC, crypto_provide, len(PadC), PadC, len(IA)), ENCRYPT(IA)
//! 4 B->A: ENCRYPT(VC, crypto_select, len(PadD), PadD), ENCRYPT2(payload)
//! 5 A->B: ENCRYPT2(payload)
//! ```
//!
//! `ENCRYPT` is always RC4 (A with `keyA`, B with `keyB`); `ENCRYPT2` is the
//! selected method. We send random-length PadA/PadB and, like the spec's
//! "standard practice", zero-length PadC/PadD.

use rand::RngExt as _;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::dh::{DH_LEN, KeyPair};
use super::stream::{Cipher, MseStream};
use super::{CryptoMethods, MSE_MAX_IA, MSE_MAX_PAD, MseError, hash};

/// Verification constant: 8 zero bytes.
const VC: [u8; 8] = [0; 8];
/// B finds `HASH('req1', S)` within this many bytes after `Ya`
/// (512 PadA + 20 hash + slack).
const REQ1_SYNC_WINDOW: usize = 628;
/// A finds the encrypted VC within this many bytes after `Yb`
/// (512 PadB + 8 VC + slack).
const VC_SYNC_WINDOW: usize = 616;

/// Bytes received from the peer but not yet consumed. Every read is sized by
/// what the handshake still needs or by a sync window, so the buffer stays
/// bounded.
struct Rx {
    buf: Vec<u8>,
}

impl Rx {
    async fn read_more<S: AsyncRead + Unpin>(
        &mut self,
        s: &mut S,
        max: usize,
    ) -> Result<(), MseError> {
        let mut tmp = vec![0u8; max];
        let n = s.read(&mut tmp).await?;
        if n == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
        }
        self.buf.extend_from_slice(tmp.get(..n).unwrap_or_default());
        Ok(())
    }

    /// Consumes exactly `n` bytes, reading no more than are missing.
    async fn take<S: AsyncRead + Unpin>(
        &mut self,
        s: &mut S,
        n: usize,
    ) -> Result<Vec<u8>, MseError> {
        while self.buf.len() < n {
            let missing = n - self.buf.len();
            self.read_more(s, missing).await?;
        }
        let rest = self.buf.split_off(n);
        Ok(std::mem::replace(&mut self.buf, rest))
    }

    async fn take_array<const N: usize, S: AsyncRead + Unpin>(
        &mut self,
        s: &mut S,
    ) -> Result<[u8; N], MseError> {
        let v = self.take(s, N).await?;
        let mut out = [0u8; N];
        for (d, b) in out.iter_mut().zip(v) {
            *d = b;
        }
        Ok(out)
    }

    /// Consumes up to and including `pattern`, which must lie entirely
    /// within the next `window` bytes; `NoSync` otherwise.
    async fn sync<S: AsyncRead + Unpin>(
        &mut self,
        s: &mut S,
        pattern: &[u8],
        window: usize,
    ) -> Result<(), MseError> {
        loop {
            let win = self.buf.get(..window).unwrap_or(&self.buf);
            if let Some(k) = win.windows(pattern.len()).position(|w| w == pattern) {
                self.buf.drain(..k + pattern.len());
                return Ok(());
            }
            if self.buf.len() >= window {
                return Err(MseError::NoSync);
            }
            let missing = window - self.buf.len();
            self.read_more(s, missing).await?;
        }
    }
}

fn xor20(a: &[u8; 20], b: &[u8; 20]) -> [u8; 20] {
    let mut out = *a;
    for (o, x) in out.iter_mut().zip(b) {
        *o ^= x;
    }
    out
}

/// A random-length (0..=512) pad of random bytes.
fn random_pad() -> Vec<u8> {
    let mut rng = rand::rng();
    let mut pad = vec![0u8; rng.random_range(0..=MSE_MAX_PAD)];
    rng.fill(&mut pad[..]);
    pad
}

async fn send<S: AsyncWrite + Unpin>(s: &mut S, bytes: &[u8]) -> Result<(), MseError> {
    s.write_all(bytes).await?;
    s.flush().await?;
    Ok(())
}

fn keys(s: &[u8; DH_LEN], skey: &[u8; 20]) -> Result<(Cipher, Cipher), MseError> {
    let a = Cipher::new(&hash(b"keyA", &[s, skey]))?;
    let b = Cipher::new(&hash(b"keyB", &[s, skey]))?;
    Ok((a, b))
}

/// Opens an MSE connection as the initiator (A). `provide` is our
/// `crypto_provide`: `RC4` alone forces encryption, `all()` prefers it.
/// `initial_payload` (IA, at most [`MSE_MAX_IA`] bytes) is sent RC4-encrypted
/// inside step 3, typically the BitTorrent handshake.
pub async fn initiate<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    info_hash: &[u8; 20],
    provide: CryptoMethods,
    initial_payload: &[u8],
) -> Result<MseStream<S>, MseError> {
    let provide = provide & CryptoMethods::all();
    if provide.is_empty() {
        return Err(MseError::NoCommonMethod);
    }
    let ia_len = u16::try_from(initial_payload.len())
        .ok()
        .filter(|&n| usize::from(n) <= MSE_MAX_IA)
        .ok_or(MseError::BadLength("IA"))?;

    // Step 1.
    let me = KeyPair::generate();
    send(&mut stream, &[&me.public[..], &random_pad()].concat()).await?;

    // Step 2.
    let mut rx = Rx { buf: Vec::new() };
    let yb: [u8; DH_LEN] = rx.take_array(&mut stream).await?;
    let s = me.shared(&yb)?;
    let (mut enc, mut dec) = keys(&s, info_hash)?;

    // Step 3.
    let mut step3 = Vec::with_capacity(40 + 16 + initial_payload.len());
    step3.extend_from_slice(&hash(b"req1", &[&s]));
    step3.extend_from_slice(&xor20(&hash(b"req2", &[info_hash]), &hash(b"req3", &[&s])));
    let mut tail = Vec::with_capacity(16 + initial_payload.len());
    tail.extend_from_slice(&VC);
    tail.extend_from_slice(&provide.bits().to_be_bytes());
    tail.extend_from_slice(&0u16.to_be_bytes()); // len(PadC); PadC is empty
    tail.extend_from_slice(&ia_len.to_be_bytes());
    tail.extend_from_slice(initial_payload);
    enc.apply(&mut tail);
    step3.extend_from_slice(&tail);
    send(&mut stream, &step3).await?;

    // Step 4: find ENCRYPT(VC), which is B's first 8 keystream bytes.
    let mut vc = VC;
    dec.apply(&mut vc);
    rx.sync(&mut stream, &vc, VC_SYNC_WINDOW).await?;
    let mut hdr: [u8; 6] = rx.take_array(&mut stream).await?;
    dec.apply(&mut hdr);
    let [s0, s1, s2, s3, p0, p1] = hdr;
    let select = u32::from_be_bytes([s0, s1, s2, s3]);
    let selected = if select == CryptoMethods::RC4.bits() {
        CryptoMethods::RC4
    } else if select == CryptoMethods::PLAINTEXT.bits() {
        CryptoMethods::PLAINTEXT
    } else {
        return Err(MseError::NoCommonMethod);
    };
    if !provide.contains(selected) {
        return Err(MseError::NoCommonMethod);
    }
    let pad_d = usize::from(u16::from_be_bytes([p0, p1]));
    if pad_d > MSE_MAX_PAD {
        return Err(MseError::BadLength("PadD"));
    }
    let mut pad = rx.take(&mut stream, pad_d).await?;
    dec.apply(&mut pad);

    // Bytes read past PadD already belong to the payload stream.
    let mut rest = rx.buf;
    if selected == CryptoMethods::RC4 {
        dec.apply(&mut rest);
        Ok(MseStream::new(stream, rest, Some(enc), Some(dec)))
    } else {
        Ok(MseStream::new(stream, rest, None, None))
    }
}

/// Accepts an MSE connection as the responder (B). `prefix` holds the bytes
/// already read to tell MSE from a plaintext handshake; `lookup` maps
/// `HASH('req2', SKEY)` (see [`req2_hash`](super::req2_hash)) to the info
/// hash. `allowed` is what we accept: RC4 wins whenever both sides offer it.
///
/// Returns the stream, whose first readable bytes are the initiator's IA,
/// and the info hash.
pub async fn respond<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    prefix: Vec<u8>,
    lookup: &(dyn Fn(&[u8; 20]) -> Option<[u8; 20]> + Sync),
    allowed: CryptoMethods,
) -> Result<(MseStream<S>, [u8; 20]), MseError> {
    // Step 1.
    let mut rx = Rx { buf: prefix };
    let ya: [u8; DH_LEN] = rx.take_array(&mut stream).await?;
    let me = KeyPair::generate();
    let s = me.shared(&ya)?;

    // Step 2.
    send(&mut stream, &[&me.public[..], &random_pad()].concat()).await?;

    // Step 3.
    rx.sync(&mut stream, &hash(b"req1", &[&s]), REQ1_SYNC_WINDOW)
        .await?;
    let obfuscated: [u8; 20] = rx.take_array(&mut stream).await?;
    let req2 = xor20(&obfuscated, &hash(b"req3", &[&s]));
    let skey = lookup(&req2).ok_or(MseError::UnknownTorrent)?;
    let (mut dec, mut enc) = keys(&s, &skey)?;

    let mut hdr: [u8; 14] = rx.take_array(&mut stream).await?;
    dec.apply(&mut hdr);
    let [v0, v1, v2, v3, v4, v5, v6, v7, c0, c1, c2, c3, p0, p1] = hdr;
    if [v0, v1, v2, v3, v4, v5, v6, v7] != VC {
        return Err(MseError::NoSync);
    }
    let provide = CryptoMethods::from_bits_truncate(u32::from_be_bytes([c0, c1, c2, c3]));
    let pad_c = usize::from(u16::from_be_bytes([p0, p1]));
    if pad_c > MSE_MAX_PAD {
        return Err(MseError::BadLength("PadC"));
    }
    let mut pad = rx.take(&mut stream, pad_c).await?;
    dec.apply(&mut pad);
    let mut ia_len: [u8; 2] = rx.take_array(&mut stream).await?;
    dec.apply(&mut ia_len);
    let ia_len = usize::from(u16::from_be_bytes(ia_len));
    if ia_len > MSE_MAX_IA {
        return Err(MseError::BadLength("IA"));
    }
    let mut ia = rx.take(&mut stream, ia_len).await?;
    dec.apply(&mut ia);

    let common = provide & allowed;
    let selected = if common.contains(CryptoMethods::RC4) {
        CryptoMethods::RC4
    } else if common.contains(CryptoMethods::PLAINTEXT) {
        CryptoMethods::PLAINTEXT
    } else {
        return Err(MseError::NoCommonMethod);
    };

    // Step 4.
    let mut step4 = Vec::with_capacity(14);
    step4.extend_from_slice(&VC);
    step4.extend_from_slice(&selected.bits().to_be_bytes());
    step4.extend_from_slice(&0u16.to_be_bytes()); // len(PadD); PadD is empty
    enc.apply(&mut step4);
    send(&mut stream, &step4).await?;

    // IA first, then anything read past it (already payload).
    let mut rest = rx.buf;
    let stream = if selected == CryptoMethods::RC4 {
        dec.apply(&mut rest);
        ia.extend_from_slice(&rest);
        MseStream::new(stream, ia, Some(enc), Some(dec))
    } else {
        ia.extend_from_slice(&rest);
        MseStream::new(stream, ia, None, None)
    };
    Ok((stream, skey))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha1::{Digest, Sha1};

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    /// Fixed private keys; expected values computed independently (Python
    /// `pow`, `hashlib` and a textbook RC4).
    #[test]
    fn key_derivation_known_answer() {
        let xa: [u8; 20] = std::array::from_fn(|i| i as u8 + 1);
        let a = KeyPair::from_private(&xa);
        let b = KeyPair::from_private(&[0x80; 20]);
        assert_eq!(
            hex(&Sha1::digest(a.public)),
            "9fa4c50e31ec4635ddb9ef30405e9db341313ed2"
        );
        assert_eq!(
            hex(&Sha1::digest(b.public)),
            "7e6ff8955acc3faddcf8b6947f5fbce0e1a588ef"
        );
        let s = a.shared(&b.public).unwrap();
        assert_eq!(s, b.shared(&a.public).unwrap());
        assert_eq!(
            hex(&Sha1::digest(s)),
            "71255ec44ca50d9e0d4bc363f03c56e6562b332a"
        );
        assert_eq!(
            hex(&hash(b"req1", &[&s])),
            "1606bd51724734360197cd3fac086d9efb1ec79e"
        );
        let skey = [0xaa; 20];
        assert_eq!(
            hex(&hash(b"keyA", &[&s, &skey])),
            "9b8eec0ff143cd52edf49da4a678e5947bf8a3c3"
        );
        assert_eq!(
            hex(&hash(b"keyB", &[&s, &skey])),
            "1c74e477b3844d29f3e7d605bedb51268bb6e43e"
        );
        let (mut ka, mut kb) = keys(&s, &skey).unwrap();
        let (mut sa, mut sb) = ([0u8; 16], [0u8; 16]);
        ka.apply(&mut sa);
        kb.apply(&mut sb);
        assert_eq!(hex(&sa), "d4b39a3faadaf6973e879b107a2df607");
        assert_eq!(hex(&sb), "99814f27183fdf5b00b32f4d6b9a2e9d");
    }

    /// A hand-rolled initiator that sends `len(PadC)` and `len(IA)` as given
    /// (without the matching bytes) and returns the responder's result.
    async fn respond_to_crafted(pad_c: u16, ia_len: u16) -> Result<(), MseError> {
        let (mut a, b) = tokio::io::duplex(8192);
        let ih = [0x55; 20];
        let lookup = move |h: &[u8; 20]| (*h == super::super::req2_hash(&ih)).then_some(ih);
        let fake = async move {
            let me = KeyPair::generate();
            a.write_all(&me.public).await.unwrap();
            let mut yb = [0u8; DH_LEN];
            a.read_exact(&mut yb).await.unwrap();
            let s = me.shared(&yb).unwrap();
            let (mut enc, _) = keys(&s, &ih).unwrap();
            let mut msg = hash(b"req1", &[&s]).to_vec();
            msg.extend_from_slice(&xor20(&hash(b"req2", &[&ih]), &hash(b"req3", &[&s])));
            let mut tail = VC.to_vec();
            tail.extend_from_slice(&CryptoMethods::RC4.bits().to_be_bytes());
            tail.extend_from_slice(&pad_c.to_be_bytes());
            if pad_c == 0 {
                tail.extend_from_slice(&ia_len.to_be_bytes());
            }
            enc.apply(&mut tail);
            msg.extend_from_slice(&tail);
            a.write_all(&msg).await.unwrap();
            a
        };
        let (_a, r) = tokio::join!(fake, respond(b, Vec::new(), &lookup, CryptoMethods::all()));
        r.map(|_| ())
    }

    #[tokio::test]
    async fn oversized_pad_c_and_ia_rejected() {
        assert!(matches!(
            respond_to_crafted(513, 0).await,
            Err(MseError::BadLength("PadC"))
        ));
        assert!(matches!(
            respond_to_crafted(0, MSE_MAX_IA as u16 + 1).await,
            Err(MseError::BadLength("IA"))
        ));
    }

    /// A hand-rolled responder that answers step 3 with `select`.
    async fn initiate_against_select(select: u32, provide: CryptoMethods) -> Result<(), MseError> {
        let (a, mut b) = tokio::io::duplex(8192);
        let ih = [0x66; 20];
        let fake = async move {
            let mut ya = [0u8; DH_LEN];
            b.read_exact(&mut ya).await.unwrap();
            let me = KeyPair::generate();
            b.write_all(&me.public).await.unwrap();
            let s = me.shared(&ya).unwrap();
            let (_, mut enc) = keys(&s, &ih).unwrap();
            let mut msg = VC.to_vec();
            msg.extend_from_slice(&select.to_be_bytes());
            msg.extend_from_slice(&0u16.to_be_bytes());
            enc.apply(&mut msg);
            b.write_all(&msg).await.unwrap();
            b
        };
        let (r, _b) = tokio::join!(initiate(a, &ih, provide, b""), fake);
        r.map(|_| ())
    }

    #[tokio::test]
    async fn initiator_rejects_bad_select() {
        for select in [0, 3, 4, CryptoMethods::PLAINTEXT.bits()] {
            assert!(matches!(
                initiate_against_select(select, CryptoMethods::RC4).await,
                Err(MseError::NoCommonMethod)
            ));
        }
        assert!(initiate_against_select(2, CryptoMethods::RC4).await.is_ok());
    }

    #[tokio::test]
    async fn initiator_gives_up_without_vc() {
        // A responder that sends a valid Yb and then 1000 junk bytes.
        let (a, mut b) = tokio::io::duplex(8192);
        let fake = async move {
            let mut ya = [0u8; DH_LEN];
            b.read_exact(&mut ya).await.unwrap();
            b.write_all(&KeyPair::generate().public).await.unwrap();
            b.write_all(&[0x5c; 1000]).await.unwrap();
            b
        };
        let (r, _b) = tokio::join!(initiate(a, &[0x77; 20], CryptoMethods::RC4, b""), fake);
        assert!(matches!(r, Err(MseError::NoSync)));
    }

    #[tokio::test]
    async fn oversized_initial_payload_refused() {
        let (a, _b) = tokio::io::duplex(64);
        let r = initiate(a, &[0; 20], CryptoMethods::RC4, &[0; MSE_MAX_IA + 1]).await;
        assert!(matches!(r, Err(MseError::BadLength("IA"))));
    }
}
