//! Piece/block geometry and file-span mapping.

/// Standard request block size (16 KiB).
pub const BLOCK_SIZE: u32 = 16_384;

/// A block within a piece.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlockRef {
    /// Piece index.
    pub piece: u32,
    /// Byte offset within the piece.
    pub offset: u32,
    /// Length in bytes.
    pub len: u32,
}

/// Error building a [`PieceLayout`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LayoutError {
    /// Total length is zero.
    #[error("total length must be greater than zero")]
    EmptyTotal,
    /// Piece length is zero.
    #[error("piece length must be at least 1")]
    ZeroPieceLen,
    /// The piece count does not fit in `u32`.
    #[error("too many pieces")]
    TooManyPieces,
}

/// Piece geometry of a torrent's concatenated content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PieceLayout {
    total_len: u64,
    piece_len: u32,
    num_pieces: u32,
}

impl PieceLayout {
    /// Builds a layout; `total_len > 0`, `piece_len >= 1`, piece count fits `u32`.
    pub fn new(total_len: u64, piece_len: u32) -> Result<Self, LayoutError> {
        if total_len == 0 {
            return Err(LayoutError::EmptyTotal);
        }
        if piece_len == 0 {
            return Err(LayoutError::ZeroPieceLen);
        }
        let num_pieces = u32::try_from(total_len.div_ceil(u64::from(piece_len)))
            .map_err(|_| LayoutError::TooManyPieces)?;
        Ok(Self {
            total_len,
            piece_len,
            num_pieces,
        })
    }

    /// Total content length in bytes.
    pub fn total_len(&self) -> u64 {
        self.total_len
    }

    /// Nominal piece length.
    pub fn piece_len(&self) -> u32 {
        self.piece_len
    }

    /// Number of pieces.
    pub fn num_pieces(&self) -> u32 {
        self.num_pieces
    }

    /// Size of `piece` (the last one may be shorter); 0 if out of range.
    pub fn piece_size(&self, piece: u32) -> u32 {
        if piece >= self.num_pieces {
            return 0;
        }
        let remaining = self.total_len - self.piece_offset(piece);
        u32::try_from(remaining).map_or(self.piece_len, |r| r.min(self.piece_len))
    }

    /// Byte offset of `piece` in the content; 0 if out of range.
    pub fn piece_offset(&self, piece: u32) -> u64 {
        if piece >= self.num_pieces {
            return 0;
        }
        u64::from(piece) * u64::from(self.piece_len)
    }

    /// Number of blocks in `piece`; 0 if out of range.
    pub fn blocks_in_piece(&self, piece: u32) -> u32 {
        self.piece_size(piece).div_ceil(BLOCK_SIZE)
    }

    /// The `index`-th block of `piece`. Out-of-range piece or index gives an
    /// empty block (`offset: 0, len: 0`), which `is_valid_block` rejects.
    pub fn block(&self, piece: u32, index: u32) -> BlockRef {
        let size = u64::from(self.piece_size(piece));
        let offset = u64::from(index) * u64::from(BLOCK_SIZE);
        if offset >= size {
            return BlockRef {
                piece,
                offset: 0,
                len: 0,
            };
        }
        let len = (size - offset).min(u64::from(BLOCK_SIZE));
        BlockRef {
            piece,
            // Both fit u32: offset < size <= piece_len, len <= BLOCK_SIZE.
            offset: u32::try_from(offset).unwrap_or(0),
            len: u32::try_from(len).unwrap_or(0),
        }
    }

    /// Whether `b` is a block this layout can hold: valid piece, non-empty,
    /// at most `BLOCK_SIZE`, aligned to `BLOCK_SIZE`, and inside the piece.
    pub fn is_valid_block(&self, b: &BlockRef) -> bool {
        if b.piece >= self.num_pieces || b.len == 0 || b.len > BLOCK_SIZE {
            return false;
        }
        if !b.offset.is_multiple_of(BLOCK_SIZE) {
            return false;
        }
        b.offset
            .checked_add(b.len)
            .is_some_and(|end| end <= self.piece_size(b.piece))
    }
}

/// A span inside one file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileSlice {
    /// File index.
    pub file: usize,
    /// Offset within the file.
    pub file_offset: u64,
    /// Length of the span.
    pub len: u64,
}

/// Maps ranges of the concatenated content onto files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileLayout {
    /// Start offset of each file.
    starts: Vec<u64>,
    /// End offset (exclusive) of each file.
    ends: Vec<u64>,
    total_len: u64,
}

impl FileLayout {
    /// Builds a layout from file lengths in order (lengths saturate on overflow).
    pub fn new(lengths: impl IntoIterator<Item = u64>) -> Self {
        let mut starts = Vec::new();
        let mut ends = Vec::new();
        let mut acc = 0u64;
        for l in lengths {
            starts.push(acc);
            acc = acc.saturating_add(l);
            ends.push(acc);
        }
        Self {
            starts,
            ends,
            total_len: acc,
        }
    }

    /// Total content length.
    pub fn total_len(&self) -> u64 {
        self.total_len
    }

    /// Number of files (including empty ones).
    pub fn num_files(&self) -> usize {
        self.starts.len()
    }

    fn clamp_end(&self, offset: u64, len: u64) -> u64 {
        offset.saturating_add(len).min(self.total_len)
    }

    /// Splits `[offset, offset+len)` into per-file slices, skipping
    /// zero-length files and clamping at the end of the content.
    pub fn map(&self, offset: u64, len: u64) -> Vec<FileSlice> {
        let end = self.clamp_end(offset, len);
        let mut out = Vec::new();
        if offset >= end {
            return out;
        }
        let first = self.ends.partition_point(|&e| e <= offset);
        for (file, (&start, &fend)) in self
            .starts
            .iter()
            .zip(self.ends.iter())
            .enumerate()
            .skip(first)
        {
            if start >= end {
                break;
            }
            let lo = offset.max(start);
            let hi = end.min(fend);
            if hi > lo {
                out.push(FileSlice {
                    file,
                    file_offset: lo - start,
                    len: hi - lo,
                });
            }
        }
        out
    }

    /// Index range of files overlapping `[offset, offset+len)`, including
    /// zero-length files located inside the range.
    pub fn files_overlapping(&self, offset: u64, len: u64) -> std::ops::Range<usize> {
        let end = self.clamp_end(offset, len);
        let first = self.ends.partition_point(|&e| e <= offset);
        if offset >= end {
            return first..first;
        }
        let last = self.starts.partition_point(|&s| s < end);
        first..last.max(first)
    }
}
