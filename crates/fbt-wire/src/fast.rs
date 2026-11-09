//! BEP 6 allowed-fast set.

use std::net::Ipv4Addr;

use sha1::{Digest, Sha1};

/// Computes the allowed-fast set for a peer at `ip`, per BEP 6.
///
/// Returns at most `min(k, num_pieces)` distinct piece indices.
pub fn allowed_fast_set(info_hash: &[u8; 20], ip: Ipv4Addr, num_pieces: u32, k: usize) -> Vec<u32> {
    let mut set: Vec<u32> = Vec::new();
    if num_pieces == 0 {
        return set;
    }
    let k = k.min(num_pieces as usize);
    let masked = u32::from(ip) & 0xFFFF_FF00;
    let mut x: Vec<u8> = masked
        .to_be_bytes()
        .into_iter()
        .chain(info_hash.iter().copied())
        .collect();
    while set.len() < k {
        x = Sha1::digest(&x).to_vec();
        for chunk in x.as_chunks::<4>().0.iter().take(5) {
            if set.len() >= k {
                break;
            }
            let y = u32::from_be_bytes(*chunk);
            let index = y % num_pieces;
            if !set.contains(&index) {
                set.push(index);
            }
        }
    }
    set
}
