//! BEP 52 Merkle trees: SHA-256 over 16 KiB blocks, zero-hash padding,
//! span proofs and their verification.
//!
//! Heights count from the leaves (leaf layer 0). Missing leaves are the
//! all-zero hash, not the hash of zero bytes; a padded subtree of height `h`
//! therefore has root [`pad_hash`]`(h)`.
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::sync::OnceLock;

use sha2::{Digest, Sha256};

/// A SHA-256 digest.
pub type Hash256 = [u8; 32];

/// Size of one Merkle leaf block.
pub const MERKLE_BLOCK_LEN: u32 = 16_384;

/// Cap on every height input: enough for 2^48 leaves.
pub const MAX_TREE_HEIGHT: u32 = 48;

fn hash_pair(left: &Hash256, right: &Hash256) -> Hash256 {
    let mut h = Sha256::new();
    h.update(left);
    h.update(right);
    h.finalize().into()
}

/// Root of a perfect tree of `2^height` zero leaves (`pad_hash(0)` is all
/// zeroes). Heights above [`MAX_TREE_HEIGHT`] return the cap's value.
pub fn pad_hash(height: u32) -> Hash256 {
    static CACHE: OnceLock<[Hash256; MAX_TREE_HEIGHT as usize + 1]> = OnceLock::new();
    let table = CACHE.get_or_init(|| {
        let mut t = [[0u8; 32]; MAX_TREE_HEIGHT as usize + 1];
        let mut prev = [0u8; 32];
        for slot in t.iter_mut().skip(1) {
            prev = hash_pair(&prev, &prev);
            *slot = prev;
        }
        t
    });
    table
        .get(height.min(MAX_TREE_HEIGHT) as usize)
        .copied()
        .unwrap_or([0; 32])
}

/// SHA-256 of each 16 KiB chunk of `data` (the last may be short).
pub fn leaf_hashes(data: &[u8]) -> Vec<Hash256> {
    data.chunks(MERKLE_BLOCK_LEN as usize)
        .map(|c| Sha256::digest(c).into())
        .collect()
}

/// Root of `hashes` (a layer at `height`) padded with `pad_hash(height)` to
/// `width`, rounded up to a power of two and to at least `hashes.len()`. An
/// empty input gives `pad_hash(height + log2(width))`.
pub fn layer_root(hashes: &[Hash256], height: u32, width: usize) -> Hash256 {
    let width = width.max(hashes.len()).max(1).next_power_of_two();
    let mut level: Vec<Hash256> = hashes.to_vec();
    let mut h = height;
    let mut w = width;
    if level.is_empty() {
        return pad_hash(height.saturating_add(width.trailing_zeros()));
    }
    while w > 1 {
        level = next_level(&level, h);
        h = h.saturating_add(1);
        w /= 2;
    }
    level.first().copied().unwrap_or([0; 32])
}

/// Hash adjacent pairs of `level` (at `height`); an odd tail pairs with
/// `pad_hash(height)`.
fn next_level(level: &[Hash256], height: u32) -> Vec<Hash256> {
    let pad = pad_hash(height);
    level
        .chunks(2)
        .map(|c| match c {
            [l, r] => hash_pair(l, r),
            [l] => hash_pair(l, &pad),
            _ => [0; 32],
        })
        .collect()
}

/// Root of one piece's bytes: `layer_root(&leaf_hashes(data), 0, leaves_per_piece)`.
pub fn piece_root(data: &[u8], leaves_per_piece: usize) -> Hash256 {
    layer_root(&leaf_hashes(data), 0, leaves_per_piece)
}

/// Height of a file's tree: `log2(next_power_of_two(max(1, ceil(len / 16 KiB))))`.
pub fn file_tree_height(len: u64) -> u32 {
    len.div_ceil(u64::from(MERKLE_BLOCK_LEN))
        .max(1)
        .next_power_of_two()
        .trailing_zeros()
}

/// A Merkle tree built from one layer, holding that layer's real hashes and
/// everything above it up to the root. Holds at most
/// `2 * 2^(tree_height - height)` hashes.
#[derive(Debug, Clone)]
pub struct MerkleTree {
    /// Height of the layer the tree was built from.
    height: u32,
    tree_height: u32,
    /// `layers[i]` is the real (unpadded) layer at height `height + i`.
    layers: Vec<Vec<Hash256>>,
}

impl MerkleTree {
    /// Build from `hashes` at `height`. `None` if the layer is wider than
    /// `2^(tree_height - height)` or a height exceeds [`MAX_TREE_HEIGHT`].
    pub fn from_layer(hashes: &[Hash256], height: u32, tree_height: u32) -> Option<MerkleTree> {
        if tree_height > MAX_TREE_HEIGHT || height > tree_height {
            return None;
        }
        let max_width = 1usize.checked_shl(tree_height - height)?;
        if hashes.len() > max_width {
            return None;
        }
        let mut layers = vec![hashes.to_vec()];
        let mut h = height;
        while h < tree_height {
            let next = layers.last().map(|l| next_level(l, h)).unwrap_or_default();
            layers.push(next);
            h += 1;
        }
        Some(MerkleTree {
            height,
            tree_height,
            layers,
        })
    }

    /// The root hash.
    pub fn root(&self) -> Hash256 {
        self.node(self.tree_height, 0)
    }

    /// Hash at `(height, index)`; positions past the real width read as
    /// `pad_hash(height)`.
    fn node(&self, height: u32, index: usize) -> Hash256 {
        height
            .checked_sub(self.height)
            .and_then(|i| self.layers.get(i as usize))
            .and_then(|l| l.get(index))
            .copied()
            .unwrap_or_else(|| pad_hash(height))
    }

    /// `len` hashes at layer `base` starting at `index`, then one uncle per
    /// layer from `base + log2(len)` upward: `min(proof_layers, tree_height -
    /// base - log2(len))` of them, bottom-up. `None` when `base` is below the
    /// built layer or above the root, `len` is not a power of two, `index` is
    /// not a multiple of `len`, or the span leaves the layer's full width.
    pub fn hashes(
        &self,
        base: u32,
        index: usize,
        len: usize,
        proof_layers: u32,
    ) -> Option<(Vec<Hash256>, Vec<Hash256>)> {
        if base < self.height || base > self.tree_height || !len.is_power_of_two() {
            return None;
        }
        let span = len.trailing_zeros();
        let above = self.tree_height.checked_sub(base)?.checked_sub(span)?;
        if !index.is_multiple_of(len) {
            return None;
        }
        let node_index = index / len;
        if u128::from(node_index as u64) >= (1u128 << above) {
            return None;
        }
        let hashes: Vec<Hash256> = (index..index + len).map(|i| self.node(base, i)).collect();
        let mut uncles = Vec::new();
        let mut i = node_index;
        for h in (base + span..).take(proof_layers.min(above) as usize) {
            uncles.push(self.node(h, i ^ 1));
            i /= 2;
        }
        Some((hashes, uncles))
    }
}

/// Recompute the root of the span `hashes` (at layer `base`, starting at
/// `index`), climb through `proof` and compare with `root`. Needs exactly the
/// uncles that reach the root (`tree_height - base - log2(len)`). False on any
/// inconsistent input.
pub fn verify_hashes(
    root: &Hash256,
    tree_height: u32,
    base: u32,
    index: usize,
    hashes: &[Hash256],
    proof: &[Hash256],
) -> bool {
    if tree_height > MAX_TREE_HEIGHT || base > tree_height || !hashes.len().is_power_of_two() {
        return false;
    }
    let len = hashes.len();
    let span = len.trailing_zeros();
    let Some(above) = (tree_height - base).checked_sub(span) else {
        return false;
    };
    if proof.len() != above as usize || !index.is_multiple_of(len) {
        return false;
    }
    let mut node_index = index / len;
    if u128::from(node_index as u64) >= (1u128 << above) {
        return false;
    }
    let mut level: Vec<Hash256> = hashes.to_vec();
    while level.len() > 1 {
        level = level
            .chunks(2)
            .map(|c| match c {
                [l, r] => hash_pair(l, r),
                _ => [0; 32],
            })
            .collect();
    }
    let Some(mut cur) = level.first().copied() else {
        return false;
    };
    for uncle in proof {
        cur = if node_index.is_multiple_of(2) {
            hash_pair(&cur, uncle)
        } else {
            hash_pair(uncle, &cur)
        };
        node_index /= 2;
    }
    cur == *root
}
