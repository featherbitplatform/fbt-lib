//! The MSE Diffie-Hellman exchange: 768-bit prime, generator 2, 160-bit
//! private keys, 96-byte big-endian public keys and secret.

use crypto_bigint::modular::{FixedMontyForm, FixedMontyParams};
use crypto_bigint::{Odd, U192, U768};
use rand::RngExt as _;

use super::MseError;

/// Length of a public key and of the shared secret `S` on the wire.
pub(crate) const DH_LEN: usize = 96;
/// Private key length: 160 random bits.
const PRIVATE_LEN: usize = 20;
const PRIVATE_BITS: u32 = 160;

/// The spec's 768-bit prime `P`.
const P: Odd<U768> = Odd::<U768>::from_be_hex(concat!(
    "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74",
    "020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F1437",
    "4FE1356D6D51C245E485B576625E7EC6F44C42E9A63A36210000000000090563",
));
const G: U768 = U768::from_u8(2);

fn params() -> FixedMontyParams<{ U768::LIMBS }> {
    // The modulus is public, so the variable-time setup leaks nothing.
    FixedMontyParams::new_vartime(P)
}

fn to_bytes(n: &U768) -> [u8; DH_LEN] {
    let mut out = [0u8; DH_LEN];
    for (d, s) in out.iter_mut().zip(n.to_be_bytes().iter()) {
        *d = *s;
    }
    out
}

/// One side's key pair.
pub(crate) struct KeyPair {
    private: U192,
    /// `G^private mod P`, 96 bytes big-endian.
    pub(crate) public: [u8; DH_LEN],
}

impl KeyPair {
    /// A fresh key pair with a random 160-bit private key.
    pub(crate) fn generate() -> KeyPair {
        let mut x = [0u8; PRIVATE_LEN];
        rand::rng().fill(&mut x[..]);
        KeyPair::from_private(&x)
    }

    /// The key pair for a given private key (fixed keys for tests).
    pub(crate) fn from_private(x: &[u8; PRIVATE_LEN]) -> KeyPair {
        let mut wide = [0u8; U192::BYTES];
        for (d, s) in wide.iter_mut().rev().zip(x.iter().rev()) {
            *d = *s;
        }
        let private = U192::from_be_slice(&wide);
        let public = FixedMontyForm::new(&G, &params())
            .pow_bounded_exp(&private, PRIVATE_BITS)
            .retrieve();
        KeyPair {
            private,
            public: to_bytes(&public),
        }
    }

    /// The shared secret `S = Y^private mod P`. A peer key of 0, 1, `P-1`
    /// or anything `>= P` is rejected: those pin `S` to a known value.
    pub(crate) fn shared(&self, peer: &[u8; DH_LEN]) -> Result<[u8; DH_LEN], MseError> {
        let y = U768::from_be_slice(peer);
        let p_minus_1 = P.get().wrapping_sub(&U768::ONE);
        if y <= U768::ONE || y >= p_minus_1 {
            return Err(MseError::BadPublicKey);
        }
        let s = FixedMontyForm::new(&y, &params())
            .pow_bounded_exp(&self.private, PRIVATE_BITS)
            .retrieve();
        Ok(to_bytes(&s))
    }
}

impl std::fmt::Debug for KeyPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyPair").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_secret_agrees() {
        let a = KeyPair::generate();
        let b = KeyPair::generate();
        assert_ne!(a.public, b.public);
        assert_eq!(a.shared(&b.public).unwrap(), b.shared(&a.public).unwrap());
    }

    #[test]
    fn rejects_degenerate_public_keys() {
        let me = KeyPair::generate();
        let mut one = [0u8; DH_LEN];
        one[DH_LEN - 1] = 1;
        let p = to_bytes(&P.get());
        let mut p_minus_1 = p;
        p_minus_1[DH_LEN - 1] -= 1;
        for bad in [[0u8; DH_LEN], one, p_minus_1, p, [0xff; DH_LEN]] {
            assert!(matches!(me.shared(&bad), Err(MseError::BadPublicKey)));
        }
        let mut two = [0u8; DH_LEN];
        two[DH_LEN - 1] = 2;
        assert!(me.shared(&two).is_ok());
    }

    #[test]
    fn public_key_of_one_is_generator() {
        let mut x = [0u8; PRIVATE_LEN];
        x[PRIVATE_LEN - 1] = 1;
        let k = KeyPair::from_private(&x);
        let mut two = [0u8; DH_LEN];
        two[DH_LEN - 1] = 2;
        assert_eq!(k.public, two);
    }
}
