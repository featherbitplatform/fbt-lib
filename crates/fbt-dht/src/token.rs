//! `announce_peer` write tokens: `SHA1(secret ‖ ip)`, rotating secret.
//!
//! The node rotates the secret every 5 minutes and both the current and the
//! previous secret verify, so a token lives 5 to 10 minutes.

use std::net::IpAddr;

use rand::Rng;
use sha1::{Digest as _, Sha1};

/// Length of an issued token: the first bytes of the SHA-1 digest.
pub(crate) const TOKEN_LEN: usize = 8;
/// Length of a token secret.
const SECRET_LEN: usize = 16;

/// Issues and verifies write tokens bound to the requester's IP.
#[derive(Debug)]
pub(crate) struct Tokens {
    current: [u8; SECRET_LEN],
    previous: [u8; SECRET_LEN],
}

impl Tokens {
    pub(crate) fn new(rng: &mut impl Rng) -> Self {
        let current = secret(rng);
        Tokens {
            current,
            previous: current,
        }
    }

    /// The token for `ip` under the current secret.
    pub(crate) fn issue(&self, ip: IpAddr) -> Vec<u8> {
        token(&self.current, ip).to_vec()
    }

    /// Whether `token` was issued to `ip` under the current or previous secret.
    pub(crate) fn verify(&self, ip: IpAddr, token: &[u8]) -> bool {
        let cur = ct_eq(token, &self::token(&self.current, ip));
        let prev = ct_eq(token, &self::token(&self.previous, ip));
        cur | prev
    }

    /// Starts a new secret; the current one becomes the previous one.
    pub(crate) fn rotate(&mut self, rng: &mut impl Rng) {
        self.previous = self.current;
        self.current = secret(rng);
    }
}

/// Constant-time comparison of a candidate token against an issued one.
fn ct_eq(candidate: &[u8], issued: &[u8; TOKEN_LEN]) -> bool {
    if candidate.len() != TOKEN_LEN {
        return false;
    }
    candidate
        .iter()
        .zip(issued)
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

fn secret(rng: &mut impl Rng) -> [u8; SECRET_LEN] {
    let mut s = [0u8; SECRET_LEN];
    rng.fill_bytes(&mut s);
    s
}

fn token(secret: &[u8; SECRET_LEN], ip: IpAddr) -> [u8; TOKEN_LEN] {
    let mut h = Sha1::new();
    h.update(secret);
    match ip {
        IpAddr::V4(v4) => h.update(v4.octets()),
        IpAddr::V6(v6) => h.update(v6.octets()),
    }
    let digest: [u8; 20] = h.finalize().into();
    let mut out = [0u8; TOKEN_LEN];
    for (o, d) in out.iter_mut().zip(digest) {
        *o = d;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn token_valid_across_one_rotation_only() {
        let mut r = StdRng::seed_from_u64(1);
        let mut tokens = Tokens::new(&mut r);
        let ip: IpAddr = "1.2.3.4".parse().unwrap();
        let tok = tokens.issue(ip);
        assert_eq!(tok.len(), TOKEN_LEN);
        assert!(tokens.verify(ip, &tok));
        tokens.rotate(&mut r);
        assert!(tokens.verify(ip, &tok), "previous secret still verifies");
        let fresh = tokens.issue(ip);
        assert_ne!(fresh, tok);
        tokens.rotate(&mut r);
        assert!(!tokens.verify(ip, &tok), "two rotations: expired");
        assert!(tokens.verify(ip, &fresh));
    }

    #[test]
    fn token_bound_to_ip() {
        let mut r = StdRng::seed_from_u64(2);
        let tokens = Tokens::new(&mut r);
        let ip: IpAddr = "1.2.3.4".parse().unwrap();
        let tok = tokens.issue(ip);
        assert!(!tokens.verify("1.2.3.5".parse().unwrap(), &tok));
        assert!(!tokens.verify("::1".parse().unwrap(), &tok));
        assert!(!tokens.verify(ip, &tok[..7]));
        assert!(!tokens.verify(ip, &[]));
        let mut longer = tok.clone();
        longer.push(0);
        assert!(!tokens.verify(ip, &longer));
        let v6: IpAddr = "2001:db8::1".parse().unwrap();
        assert!(tokens.verify(v6, &tokens.issue(v6)));
    }
}
