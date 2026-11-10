//! Message Stream Encryption / Protocol Encryption (MSE/PE).
//!
//! The obfuscation handshake from the "Message Stream Encryption" spec, as
//! libtorrent, Transmission and qBittorrent speak it: a 768-bit
//! Diffie-Hellman exchange, the info hash as shared secret `SKEY`, and RC4
//! (first 1024 keystream bytes discarded) or plaintext for the payload.
//!
//! Everything read here comes from an untrusted peer: every search window,
//! pad and initial payload is bounded by a named constant.

mod dh;
mod handshake;
mod stream;

use sha1::{Digest, Sha1};

pub use handshake::{initiate, respond};
pub use stream::MseStream;

bitflags::bitflags! {
    /// The `crypto_provide` / `crypto_select` bitfield.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct CryptoMethods: u32 {
        /// Payload in the clear after the obfuscated header.
        const PLAINTEXT = 0x01;
        /// Payload RC4-encrypted.
        const RC4 = 0x02;
    }
}

/// Largest PadA/PadB/PadC/PadD the spec allows (and we accept).
pub const MSE_MAX_PAD: usize = 512;
/// Our cap on `len(IA)`, the initiator's initial payload. Generous enough for
/// a BitTorrent handshake plus an extension handshake.
pub const MSE_MAX_IA: usize = 1024;

/// MSE handshake failures.
#[derive(Debug, thiserror::Error)]
pub enum MseError {
    /// Transport error or early EOF.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// The sync pattern (`HASH('req1', S)` or the encrypted VC) did not show
    /// up within its window, or the verification constant was wrong.
    #[error("MSE synchronisation pattern not found")]
    NoSync,
    /// No torrent matches the peer's `HASH('req2', SKEY)`.
    #[error("MSE handshake for an unknown torrent")]
    UnknownTorrent,
    /// The two sides share no crypto method, or the peer selected one we did
    /// not provide.
    #[error("no common MSE crypto method")]
    NoCommonMethod,
    /// The peer's Diffie-Hellman public key is degenerate or out of range.
    #[error("bad MSE public key")]
    BadPublicKey,
    /// A length field exceeds its cap; the payload names the field.
    #[error("MSE length out of range: {0}")]
    BadLength(&'static str),
}

/// `SHA1(tag ‖ parts...)`, the spec's `HASH()`.
pub(crate) fn hash(tag: &[u8], parts: &[&[u8]]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(tag);
    for p in parts {
        h.update(p);
    }
    h.finalize().into()
}

/// `SHA1("req2" ‖ info_hash)`: what a responder's `lookup` is keyed by.
pub fn req2_hash(info_hash: &[u8; 20]) -> [u8; 20] {
    hash(b"req2", &[info_hash])
}

const PLAINTEXT_HANDSHAKE: &[u8; 20] = b"\x13BitTorrent protocol";

/// True if `first` could start a plaintext BitTorrent handshake: it is a
/// prefix of `\x13BitTorrent protocol` (all 20 bytes are compared when
/// present; bytes past them are ignored).
pub fn is_plaintext_handshake_prefix(first: &[u8]) -> bool {
    first
        .iter()
        .zip(PLAINTEXT_HANDSHAKE.iter())
        .all(|(a, b)| a == b)
}
