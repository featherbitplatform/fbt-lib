//! MSB-first piece bitfield (BEP 3 `bitfield` message layout).

/// Error building a [`Bitfield`] from wire bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BitfieldError {
    /// The byte length does not equal `ceil(len / 8)`.
    #[error("bitfield has the wrong length")]
    WrongLength,
    /// Bits beyond `len` in the last byte are set.
    #[error("bitfield has spare bits set")]
    SpareBitsSet,
}

/// A fixed-length bit set; piece 0 is `0x80` of byte 0.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Bitfield {
    bytes: Vec<u8>,
    len: u32,
}

fn byte_len(len: u32) -> usize {
    (len as usize).div_ceil(8)
}

/// Mask of the valid bits in the last byte (`0xff` when the length is a multiple of 8).
fn last_mask(len: u32) -> u8 {
    match len % 8 {
        0 => 0xff,
        r => 0xffu8 << (8 - r),
    }
}

impl Bitfield {
    /// An all-zero bitfield of `len` bits.
    pub fn new(len: u32) -> Self {
        Self {
            bytes: vec![0; byte_len(len)],
            len,
        }
    }

    /// An all-ones bitfield of `len` bits (spare bits stay clear).
    pub fn full(len: u32) -> Self {
        let mut b = Self {
            bytes: vec![0xff; byte_len(len)],
            len,
        };
        if let Some(last) = b.bytes.last_mut() {
            *last &= last_mask(len);
        }
        b
    }

    /// Validates wire bytes: exact length and no spare bits set.
    pub fn from_bytes(bytes: &[u8], len: u32) -> Result<Self, BitfieldError> {
        if bytes.len() != byte_len(len) {
            return Err(BitfieldError::WrongLength);
        }
        if let Some(&last) = bytes.last()
            && last & !last_mask(len) != 0
        {
            return Err(BitfieldError::SpareBitsSet);
        }
        Ok(Self {
            bytes: bytes.to_vec(),
            len,
        })
    }

    /// Number of bits.
    pub fn len(&self) -> u32 {
        self.len
    }

    /// True when the bitfield has zero bits.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether bit `i` is set; out of range gives `false`.
    pub fn get(&self, i: u32) -> bool {
        if i >= self.len {
            return false;
        }
        self.bytes
            .get((i / 8) as usize)
            .is_some_and(|b| b & (0x80 >> (i % 8)) != 0)
    }

    /// Sets bit `i`; out of range is a no-op.
    pub fn set(&mut self, i: u32) {
        if i < self.len
            && let Some(b) = self.bytes.get_mut((i / 8) as usize)
        {
            *b |= 0x80 >> (i % 8);
        }
    }

    /// Clears bit `i`; out of range is a no-op.
    pub fn clear(&mut self, i: u32) {
        if i < self.len
            && let Some(b) = self.bytes.get_mut((i / 8) as usize)
        {
            *b &= !(0x80 >> (i % 8));
        }
    }

    /// Number of set bits.
    pub fn count_ones(&self) -> u32 {
        self.bytes.iter().map(|b| b.count_ones()).sum()
    }

    /// True when every bit is set (vacuously true when empty).
    pub fn is_full(&self) -> bool {
        self.count_ones() == self.len
    }

    /// The wire representation.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Indices of set bits in ascending order; zero bytes are skipped whole.
    pub fn iter_ones(&self) -> impl Iterator<Item = u32> + '_ {
        self.bytes
            .iter()
            .enumerate()
            .filter(|(_, b)| **b != 0)
            .flat_map(|(bi, b)| {
                let base = (bi as u32).saturating_mul(8);
                (0..8u32)
                    .filter(move |bit| b & (0x80 >> bit) != 0)
                    .map(move |bit| base + bit)
            })
    }
}
