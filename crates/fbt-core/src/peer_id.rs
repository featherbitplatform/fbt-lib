//! Our peer id (Azureus style).

use rand::RngExt as _;

const ALPHANUM: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// A 20-byte BitTorrent peer id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerId(pub [u8; 20]);

/// One version component as a single `0-9A-Z` character; saturates at `Z`.
fn version_char(n: u32) -> u8 {
    match n {
        0..=9 => b'0' + n as u8,
        10..=35 => b'A' + (n - 10) as u8,
        _ => b'Z',
    }
}

fn component(s: &str) -> u32 {
    s.parse().unwrap_or(0)
}

impl PeerId {
    /// Generates `-FB` + major + minor + patch + `0` + `-` followed by 12
    /// random alphanumerics.
    pub fn generate() -> PeerId {
        let mut id = [0u8; 20];
        let prefix = [
            b'-',
            b'F',
            b'B',
            version_char(component(env!("CARGO_PKG_VERSION_MAJOR"))),
            version_char(component(env!("CARGO_PKG_VERSION_MINOR"))),
            version_char(component(env!("CARGO_PKG_VERSION_PATCH"))),
            b'0',
            b'-',
        ];
        let mut rng = rand::rng();
        for (i, slot) in id.iter_mut().enumerate() {
            *slot = match prefix.get(i) {
                Some(&p) => p,
                None => ALPHANUM[rng.random_range(0..ALPHANUM.len())],
            };
        }
        PeerId(id)
    }

    /// The first 8 bytes (client and version tag).
    pub fn client_prefix(&self) -> [u8; 8] {
        let mut out = [0u8; 8];
        for (o, i) in out.iter_mut().zip(self.0.iter()) {
            *o = *i;
        }
        out
    }
}
