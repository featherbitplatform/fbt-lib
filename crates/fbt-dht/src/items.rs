//! BEP 44 items: immutable values stored under their SHA-1, and mutable
//! values signed with an ed25519 key, stored under SHA-1(key ‖ salt).
//!
//! [`ItemStore`] holds the items other nodes `put` to us: bounded, each
//! item lives [`ITEM_TTL`] from its last put, and a full store evicts the
//! least recently put item. Puts are checked by the BEP 44 rules
//! ([`ItemStore::put`]); signatures are verified with
//! [`VerifyingKey::verify_strict`].

use std::collections::HashMap;
use std::time::{Duration, Instant};

use ed25519_dalek::{Signature, Signer as _, SigningKey, VerifyingKey};
use sha1::{Digest as _, Sha1};

use crate::krpc::{
    ERR_CAS_MISMATCH, ERR_INVALID_SIGNATURE, ERR_PROTOCOL, ERR_SALT_TOO_BIG, ERR_SEQ_TOO_LOW,
    ERR_VALUE_TOO_BIG, MAX_ITEM_VALUE, MAX_SALT, Response,
};

/// How long an item lives after its last put (BEP 44 suggests at least an
/// hour; 2 hours as libtorrent).
pub(crate) const ITEM_TTL: Duration = Duration::from_secs(2 * 60 * 60);

/// A BEP 44 mutable item, as [`Dht::get_mutable`](crate::Dht::get_mutable)
/// returns it: its signature verified against `public_key`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MutableItem {
    /// The ed25519 public key that signed it.
    pub public_key: [u8; 32],
    /// The salt (empty for none).
    pub salt: Vec<u8>,
    /// The sequence number.
    pub seq: i64,
    /// The value: one bencoded value.
    pub value: Vec<u8>,
}

/// The bytes a mutable item's signature covers:
/// `4:salt<n>:<salt>3:seqi<seq>e1:v<v>`, the salt part only with a salt.
pub(crate) fn signable(salt: &[u8], seq: i64, v: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(salt.len() + v.len() + 40);
    if !salt.is_empty() {
        out.extend_from_slice(format!("4:salt{}:", salt.len()).as_bytes());
        out.extend_from_slice(salt);
    }
    out.extend_from_slice(format!("3:seqi{seq}e1:v").as_bytes());
    out.extend_from_slice(v);
    out
}

/// Whether `sig` is `k`'s signature of [`signable`]`(salt, seq, v)`. Keys
/// that are no valid point, small-order keys and malleable signatures are
/// refused (`verify_strict`).
pub(crate) fn verify(k: &[u8; 32], sig: &[u8; 64], salt: &[u8], seq: i64, v: &[u8]) -> bool {
    let Ok(key) = VerifyingKey::from_bytes(k) else {
        return false;
    };
    key.verify_strict(&signable(salt, seq, v), &Signature::from_bytes(sig))
        .is_ok()
}

/// Signs a mutable item with the ed25519 key of `seed`: its public key and
/// the signature.
pub(crate) fn sign(seed: &[u8; 32], salt: &[u8], seq: i64, v: &[u8]) -> ([u8; 32], [u8; 64]) {
    // zeroized on drop
    let key = SigningKey::from_bytes(seed);
    let sig = key.sign(&signable(salt, seq, v));
    (key.verifying_key().to_bytes(), sig.to_bytes())
}

/// An immutable item's target: SHA-1 of its bencoded value.
pub(crate) fn immutable_target(v: &[u8]) -> [u8; 20] {
    Sha1::digest(v).into()
}

/// A mutable item's target: SHA-1 of its public key and salt.
pub(crate) fn mutable_target(k: &[u8; 32], salt: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(k);
    h.update(salt);
    h.finalize().into()
}

/// A `put`'s arguments, borrowed from [`Query::Put`](crate::Query::Put).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Put<'a> {
    pub(crate) v: &'a [u8],
    pub(crate) k: Option<&'a [u8; 32]>,
    pub(crate) sig: Option<&'a [u8; 64]>,
    pub(crate) seq: Option<i64>,
    /// Empty for none.
    pub(crate) salt: &'a [u8],
    pub(crate) cas: Option<i64>,
    pub(crate) reject: Option<i64>,
}

/// Why a put was refused: a KRPC error code and message.
pub(crate) type Refusal = (i64, &'static str);

/// The signed part of a mutable item (its salt is part of its target).
#[derive(Debug, Clone)]
pub(crate) struct Signed {
    pub(crate) k: [u8; 32],
    pub(crate) sig: [u8; 64],
    pub(crate) seq: i64,
}

/// One stored item.
#[derive(Debug, Clone)]
pub(crate) struct Item {
    pub(crate) v: Vec<u8>,
    /// `None` for an immutable item.
    pub(crate) signed: Option<Signed>,
    /// The last put.
    put_at: Instant,
}

impl Item {
    /// A mutable item's sequence number.
    pub(crate) fn seq(&self) -> Option<i64> {
        self.signed.as_ref().map(|s| s.seq)
    }
}

/// Items put to us, at most `max_items`.
#[derive(Debug)]
pub(crate) struct ItemStore {
    max_items: usize,
    items: HashMap<[u8; 20], Item>,
}

impl ItemStore {
    pub(crate) fn new(max_items: usize) -> Self {
        ItemStore {
            max_items,
            items: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    /// The live item at `target`.
    pub(crate) fn get(&self, target: &[u8; 20], now: Instant) -> Option<&Item> {
        self.items.get(target).filter(|i| alive(i, now))
    }

    /// Drops the items not put again for [`ITEM_TTL`].
    pub(crate) fn expire(&mut self, now: Instant) {
        self.items.retain(|_, i| alive(i, now));
    }

    /// Checks `p` by the BEP 44 rules and stores it: a value cut by the
    /// decoder gives its `reject` code (205, 207); a mutable item needs a
    /// key, signature and non-negative `seq` (else 203) and a signature
    /// that verifies (206); over a stored mutable item, `cas` must equal
    /// its `seq` (301) and `seq` must be higher, or equal with the same
    /// value (302). An immutable item is stored under the SHA-1 of its
    /// value, so it needs no other check.
    pub(crate) fn put(&mut self, p: &Put<'_>, now: Instant) -> Result<(), Refusal> {
        if let Some(code) = p.reject {
            return Err((code, "value or salt too big"));
        }
        if p.v.len() > MAX_ITEM_VALUE {
            return Err((ERR_VALUE_TOO_BIG, "value too big"));
        }
        if p.salt.len() > MAX_SALT {
            return Err((ERR_SALT_TOO_BIG, "salt too big"));
        }
        let (target, signed) = match (p.k, p.sig, p.seq) {
            (None, _, _) => (immutable_target(p.v), None),
            (Some(k), Some(sig), Some(seq)) => {
                if seq < 0 {
                    return Err((ERR_PROTOCOL, "negative sequence number"));
                }
                if !verify(k, sig, p.salt, seq, p.v) {
                    return Err((ERR_INVALID_SIGNATURE, "invalid signature"));
                }
                let signed = Signed {
                    k: *k,
                    sig: *sig,
                    seq,
                };
                (mutable_target(k, p.salt), Some(signed))
            }
            (Some(_), _, _) => return Err((ERR_PROTOCOL, "missing signature or seq")),
        };
        if let (Some(new), Some(old)) = (&signed, self.get(&target, now))
            && let Some(old_seq) = old.seq()
        {
            if p.cas.is_some_and(|cas| cas != old_seq) {
                return Err((ERR_CAS_MISMATCH, "cas mismatch"));
            }
            if new.seq < old_seq || (new.seq == old_seq && old.v != p.v) {
                return Err((ERR_SEQ_TOO_LOW, "sequence number less than current"));
            }
        }
        if self.max_items == 0 {
            return Ok(());
        }
        if !self.items.contains_key(&target) && self.items.len() >= self.max_items {
            self.expire(now);
            if self.items.len() >= self.max_items
                && let Some(oldest) = self
                    .items
                    .iter()
                    .min_by_key(|(_, i)| i.put_at)
                    .map(|(t, _)| *t)
            {
                self.items.remove(&oldest);
            }
        }
        self.items.insert(
            target,
            Item {
                v: p.v.to_vec(),
                signed,
                put_at: now,
            },
        );
        Ok(())
    }
}

fn alive(i: &Item, now: Instant) -> bool {
    now.saturating_duration_since(i.put_at) < ITEM_TTL
}

/// What a `get` lookup looks for.
#[derive(Debug, Clone)]
pub(crate) enum ItemWant {
    /// The immutable item at the lookup's target.
    Immutable,
    /// The mutable item of `key` and `salt`.
    Mutable { key: [u8; 32], salt: Vec<u8> },
}

/// A checked item from a `get` answer: its value and (mutable) `seq`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Found {
    pub(crate) v: Vec<u8>,
    pub(crate) seq: Option<i64>,
}

/// The item in a `get` answer `r` for `target`, if it is the one wanted:
/// an immutable value must hash to `target`, a mutable one must carry
/// `key` and verify. Anything else (including forged items) is `None`.
pub(crate) fn accept(want: &ItemWant, target: &[u8; 20], r: &Response) -> Option<Found> {
    let v = r.v.as_ref()?;
    match want {
        ItemWant::Immutable => (immutable_target(v) == *target).then(|| Found {
            v: v.clone(),
            seq: None,
        }),
        ItemWant::Mutable { key, salt } => {
            let (k, sig, seq) = (r.k.as_ref()?, r.sig.as_ref()?, r.seq?);
            (k == key && seq >= 0 && verify(k, sig, salt, seq, v)).then(|| Found {
                v: v.clone(),
                seq: Some(seq),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::krpc::{Body, Message, Query, QueryMsg};
    use std::time::{Duration, Instant};

    fn hex<const N: usize>(s: &str) -> [u8; N] {
        let mut out = [0u8; N];
        for (o, pair) in out.iter_mut().zip(s.as_bytes().chunks(2)) {
            *o = u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap();
        }
        out
    }

    /// BEP 44 "test vectors": the mutable item's public key and signatures.
    const BEP44_KEY: &str = "77ff84905a91936367c01360803104f92432fcd904a43511876df5cdf3e7e548";
    const BEP44_SIG: &str = "305ac8aeb6c9c151fa120f120ea2cfb923564e11552d06a5d856091e5e853cff1260d3f39e4999684aa92eb73ffd136e6f4f3ecbfda0ce53a1608ecd7ae21f01";
    const BEP44_SIG_SALT: &str = "6834284b6b24c3204eb2fea824d82f88883a3d95e8b4a21b8c0ded553d17d17ddf9a8a7104b1258f30bed3787e6cb896fca78c58f8e03b5f18f14951a87d9a08";
    const HELLO: &[u8] = b"12:Hello World!";

    #[test]
    fn bep44_test_vectors() {
        let k = hex::<32>(BEP44_KEY);
        // test 1: no salt
        assert_eq!(signable(b"", 1, HELLO), b"3:seqi1e1:v12:Hello World!");
        assert!(verify(&k, &hex(BEP44_SIG), b"", 1, HELLO));
        assert_eq!(
            mutable_target(&k, b""),
            hex::<20>("4a533d47ec9c7d95b1ad75f576cffc641853b750")
        );
        // test 2: salt "foobar"
        assert_eq!(
            signable(b"foobar", 1, HELLO),
            b"4:salt6:foobar3:seqi1e1:v12:Hello World!"
        );
        assert!(verify(&k, &hex(BEP44_SIG_SALT), b"foobar", 1, HELLO));
        assert_eq!(
            mutable_target(&k, b"foobar"),
            hex::<20>("411eba73b6f087ca51a3795d9c8c938d365e32c1")
        );
        // any other salt, seq or value fails
        assert!(!verify(&k, &hex(BEP44_SIG), b"foobar", 1, HELLO));
        assert!(!verify(&k, &hex(BEP44_SIG), b"", 2, HELLO));
        assert!(!verify(&k, &hex(BEP44_SIG), b"", 1, b"12:Hello World?"));
        // test 3 (immutable): the target is the SHA-1 of the bencoded value
        assert_eq!(
            immutable_target(HELLO),
            hex::<20>("e5f96f6f38320f0f33959cb4d3d656452117aadb")
        );
    }

    #[test]
    fn sign_then_verify_round_trip() {
        let seed: [u8; 32] = std::array::from_fn(|i| i as u8 * 7 + 3);
        for (salt, seq, v) in [
            (&b""[..], 0i64, &b"i42e"[..]),
            (b"foobar", 1, HELLO),
            (&[0xff; 64], i64::MAX, b"d1:al1:b1:cee"),
        ] {
            let (k, sig) = sign(&seed, salt, seq, v);
            assert!(verify(&k, &sig, salt, seq, v));
            assert!(!verify(&k, &sig, salt, seq.wrapping_add(1), v));
            let mut bad = sig;
            bad[0] ^= 1;
            assert!(!verify(&k, &bad, salt, seq, v));
        }
        // the key is the seed's ed25519 public key, fixed for a fixed seed
        let (k1, _) = sign(&seed, b"", 0, b"0:");
        let (k2, _) = sign(&seed, b"x", 9, b"1:a");
        assert_eq!(k1, k2);
        // a key that is no curve point verifies nothing, without panicking
        let (_, sig) = sign(&seed, b"", 0, b"0:");
        assert!(!verify(&[0xff; 32], &sig, b"", 0, b"0:"));
    }

    fn mutable_put<'a>(
        k: &'a [u8; 32],
        sig: &'a [u8; 64],
        seq: i64,
        v: &'a [u8],
        cas: Option<i64>,
    ) -> Put<'a> {
        Put {
            v,
            k: Some(k),
            sig: Some(sig),
            seq: Some(seq),
            salt: b"",
            cas,
            reject: None,
        }
    }

    #[test]
    fn seq_rules() {
        let seed = [9u8; 32];
        let now = Instant::now();
        let mut store = ItemStore::new(10);
        let put = |store: &mut ItemStore, seq: i64, v: &[u8], cas: Option<i64>| {
            let (k, sig) = sign(&seed, b"", seq, v);
            store.put(&mutable_put(&k, &sig, seq, v, cas), now)
        };
        assert_eq!(put(&mut store, 2, b"1:a", None), Ok(()));
        // a lower seq, or the same seq with another value: 302
        assert_eq!(put(&mut store, 1, b"1:a", None).map_err(|e| e.0), Err(302));
        assert_eq!(put(&mut store, 2, b"1:b", None).map_err(|e| e.0), Err(302));
        // the same seq and value refreshes
        assert_eq!(put(&mut store, 2, b"1:a", None), Ok(()));
        // cas must match the stored seq: 301
        assert_eq!(
            put(&mut store, 3, b"1:c", Some(1)).map_err(|e| e.0),
            Err(301)
        );
        assert_eq!(put(&mut store, 3, b"1:c", Some(2)), Ok(()));
        let (k, _) = sign(&seed, b"", 0, b"0:");
        let item = store.get(&mutable_target(&k, b""), now).unwrap();
        assert_eq!((item.v.as_slice(), item.seq()), (&b"1:c"[..], Some(3)));
        // a bad signature: 206, nothing changes
        let (k, mut sig) = sign(&seed, b"", 4, b"1:d");
        sig[5] ^= 0x40;
        assert_eq!(
            store
                .put(&mutable_put(&k, &sig, 4, b"1:d", None), now)
                .map_err(|e| e.0),
            Err(206)
        );
        assert_eq!(
            store.get(&mutable_target(&k, b""), now).unwrap().seq(),
            Some(3)
        );
        // a negative seq, or a key without a signature: 203
        assert_eq!(put(&mut store, -1, b"1:e", None).map_err(|e| e.0), Err(203));
        let p = Put {
            sig: None,
            ..mutable_put(&k, &sig, 5, b"1:e", None)
        };
        assert_eq!(store.put(&p, now).map_err(|e| e.0), Err(203));
    }

    #[test]
    fn store_is_bounded() {
        let t0 = Instant::now();
        let mut store = ItemStore::new(3);
        let values: Vec<Vec<u8>> = (0..5).map(|k| format!("i{k}e").into_bytes()).collect();
        for (k, v) in values.iter().enumerate() {
            let p = Put {
                v,
                k: None,
                sig: None,
                seq: None,
                salt: b"",
                cas: None,
                reject: None,
            };
            store.put(&p, t0 + Duration::from_secs(k as u64)).unwrap();
        }
        assert_eq!(store.len(), 3);
        // the least recently put went first
        let at = t0 + Duration::from_secs(10);
        for (k, v) in values.iter().enumerate() {
            assert_eq!(store.get(&immutable_target(v), at).is_some(), k >= 2, "{k}");
        }
        // items live 2 hours
        let late = t0 + ITEM_TTL + Duration::from_secs(5);
        assert!(store.get(&immutable_target(&values[4]), late).is_none());
        store.expire(late);
        assert_eq!(store.len(), 0);
        // a store of capacity 0 keeps nothing
        let mut none = ItemStore::new(0);
        let p = Put {
            v: b"i1e",
            k: None,
            sig: None,
            seq: None,
            salt: b"",
            cas: None,
            reject: None,
        };
        assert!(none.put(&p, t0).is_ok());
        assert_eq!(none.len(), 0);
    }

    fn put_datagram(v: &[u8], salt: Option<&[u8]>) -> Vec<u8> {
        let mut a = b"d2:id20:abcdefghij0123456789".to_vec();
        if let Some(salt) = salt {
            a.extend_from_slice(format!("4:salt{}:", salt.len()).as_bytes());
            a.extend_from_slice(salt);
        }
        a.extend_from_slice(b"5:token8:aoeusnth1:v");
        a.extend_from_slice(v);
        a.push(b'e');
        let mut out = b"d1:a".to_vec();
        out.extend_from_slice(&a);
        out.extend_from_slice(b"1:q3:put1:t2:aa1:y1:qe");
        out
    }

    #[test]
    fn oversized_value_is_205() {
        // a 1001-byte bencoded value: a 997-byte string
        let mut v = b"997:".to_vec();
        v.extend(std::iter::repeat_n(b'x', 997));
        assert_eq!(v.len(), MAX_ITEM_VALUE + 1);
        let m = Message::decode(&put_datagram(&v, None)).unwrap();
        let Body::Query(QueryMsg {
            query: Query::Put { reject, v: cut, .. },
            ..
        }) = m.body
        else {
            panic!("{m:?}")
        };
        assert_eq!(reject, Some(205));
        assert!(cut.is_empty());
        let mut store = ItemStore::new(10);
        let p = Put {
            v: &cut,
            k: None,
            sig: None,
            seq: None,
            salt: b"",
            cas: None,
            reject,
        };
        assert_eq!(store.put(&p, Instant::now()).map_err(|e| e.0), Err(205));
        assert_eq!(store.len(), 0);
        // exactly 1000 bytes is fine
        let mut ok = b"996:".to_vec();
        ok.extend(std::iter::repeat_n(b'x', 996));
        assert_eq!(ok.len(), MAX_ITEM_VALUE);
        let m = Message::decode(&put_datagram(&ok, None)).unwrap();
        assert!(matches!(
            m.body,
            Body::Query(QueryMsg {
                query: Query::Put { reject: None, .. },
                ..
            })
        ));
        // a 65-byte salt: 207
        let m = Message::decode(&put_datagram(b"i1e", Some(&[b's'; 65]))).unwrap();
        assert!(matches!(
            m.body,
            Body::Query(QueryMsg {
                query: Query::Put {
                    reject: Some(207),
                    salt: None,
                    ..
                },
                ..
            })
        ));
    }
}
