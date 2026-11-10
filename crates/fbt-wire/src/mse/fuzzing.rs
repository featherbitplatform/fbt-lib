//! Fixed-key MSE entry points for the fuzz crate (re-exported, hidden, as
//! `fbt_wire::mse_fuzzing`).
//!
//! The handshake is random by design, so a mutated transcript could never
//! pass the `req1`/SKEY synchronisation search twice. With fixed private keys
//! and zero pads the transcript is reproducible: a captured one is a valid
//! fuzz seed. Never use these outside fuzzing and tests.

use tokio::io::{AsyncRead, AsyncWrite};

use super::dh::KeyPair;
use super::handshake::{initiate_with, respond_with};
use super::{CryptoMethods, MSE_MAX_PAD, MseError, MseStream};

/// [`initiate`](super::initiate) with a fixed private key and `pad_len`
/// zero bytes of PadA (capped at `MSE_MAX_PAD`).
pub async fn initiate_fixed<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    info_hash: &[u8; 20],
    provide: CryptoMethods,
    ia: &[u8],
    private: [u8; 20],
    pad_len: usize,
) -> Result<MseStream<S>, MseError> {
    initiate_with(stream, info_hash, provide, ia, || {
        (
            KeyPair::from_private(&private),
            vec![0; pad_len.min(MSE_MAX_PAD)],
        )
    })
    .await
}

/// [`respond`](super::respond) with a fixed private key and `pad_len` zero
/// bytes of PadB (capped at `MSE_MAX_PAD`).
pub async fn respond_fixed<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    prefix: Vec<u8>,
    lookup: &(dyn Fn(&[u8; 20]) -> Option<[u8; 20]> + Sync),
    allowed: CryptoMethods,
    private: [u8; 20],
    pad_len: usize,
) -> Result<(MseStream<S>, [u8; 20]), MseError> {
    respond_with(stream, prefix, lookup, allowed, || {
        (
            KeyPair::from_private(&private),
            vec![0; pad_len.min(MSE_MAX_PAD)],
        )
    })
    .await
}
