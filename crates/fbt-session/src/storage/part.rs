//! The part file (libtorrent's `use_partfile`): the bytes of skipped files
//! that do not exist on disk, kept piece by piece so those files are never
//! created.
//!
//! Layout, all integers little-endian:
//!
//! | offset | size | field |
//! |---|---|---|
//! | 0 | 8 | magic `FBTPART1` |
//! | 8 | 4 | piece count |
//! | 12 | 4 | piece length |
//! | 16 | `count × 4` | slot of each piece (`u32::MAX` = none) |
//! | `16 + count × 4` | `piece length` each | the slots |
//!
//! The header comes from disk and is not trusted: a file whose header or
//! table does not match the torrent is set aside as `.parts.bad` and a
//! fresh one started. Nothing is ever allocated from a header value.

use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use super::fs::{read_exact_at, write_all_at};

/// The first eight bytes of every part file.
const MAGIC: &[u8; 8] = b"FBTPART1";
/// Magic, piece count and piece length.
const HEADER_LEN: u64 = 16;
/// A piece with no slot.
const NO_SLOT: u32 = u32::MAX;

/// One torrent's part file.
#[derive(Debug)]
pub(crate) struct PartFile {
    path: PathBuf,
    num_pieces: u32,
    piece_len: u32,
    /// Open once the file exists (read from disk or created by a write).
    file: Option<File>,
    /// The slot of each piece, `NO_SLOT` for none.
    slots: Vec<u32>,
    /// Whether each slot holds a piece.
    used: Vec<bool>,
    /// Slots in use.
    count: usize,
}

impl PartFile {
    /// The part file at `path` for a torrent of `num_pieces` pieces of
    /// `piece_len` bytes. A missing file is created by the first write; a
    /// corrupt one, or one written for another layout, is renamed to
    /// `.parts.bad` and replaced by an empty one.
    pub(crate) fn open(path: PathBuf, num_pieces: u32, piece_len: u32) -> io::Result<PartFile> {
        let mut pf = PartFile {
            path,
            num_pieces,
            piece_len,
            file: None,
            slots: vec![NO_SLOT; num_pieces as usize],
            used: vec![false; num_pieces as usize],
            count: 0,
        };
        let file = match OpenOptions::new().read(true).write(true).open(&pf.path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(pf),
            Err(e) => return Err(e),
        };
        match pf.load(&file) {
            Ok(()) => {
                pf.file = Some(file);
                Ok(pf)
            }
            Err(why) => {
                drop(file);
                let bad = bad_path(&pf.path);
                tracing::warn!(
                    path = %pf.path.display(),
                    reason = why,
                    "part file is corrupt, starting afresh"
                );
                if std::fs::rename(&pf.path, &bad).is_err() {
                    std::fs::remove_file(&pf.path)?;
                }
                pf.slots.fill(NO_SLOT);
                pf.used.fill(false);
                pf.count = 0;
                Ok(pf)
            }
        }
    }

    /// Reads the header and slot table of `file` into `self`; the reason
    /// it is unusable otherwise.
    fn load(&mut self, file: &File) -> Result<(), &'static str> {
        let mut header = [0u8; HEADER_LEN as usize];
        read_exact_at(file, &mut header, 0).map_err(|_| "short header")?;
        let (magic, rest) = header.split_at(8);
        let (count, plen) = rest.split_at(4);
        if magic != MAGIC {
            return Err("bad magic");
        }
        if le_u32(count) != Some(self.num_pieces) {
            return Err("piece count differs");
        }
        if le_u32(plen) != Some(self.piece_len) {
            return Err("piece length differs");
        }
        // Sized by the torrent's own piece count, never the header's.
        let mut table = vec![0u8; self.slots.len() * 4];
        read_exact_at(file, &mut table, HEADER_LEN).map_err(|_| "short slot table")?;
        for (piece, raw) in table.as_chunks::<4>().0.iter().enumerate() {
            let slot = u32::from_le_bytes(*raw);
            if slot == NO_SLOT {
                continue;
            }
            let used = self
                .used
                .get_mut(slot as usize)
                .ok_or("slot out of range")?;
            if *used {
                return Err("slot used twice");
            }
            *used = true;
            if let Some(s) = self.slots.get_mut(piece) {
                *s = slot;
            }
            self.count += 1;
        }
        Ok(())
    }

    /// Writes `data` at `offset` within `piece`, giving the piece the
    /// lowest free slot if it has none.
    pub(crate) fn write(&mut self, piece: u32, offset: u32, data: &[u8]) -> io::Result<()> {
        self.check(piece, offset, data.len())?;
        let (slot, new) = match self.slot(piece) {
            Some(s) => (s, false),
            None => {
                let s = self
                    .used
                    .iter()
                    .position(|u| !u)
                    .and_then(|s| u32::try_from(s).ok())
                    .ok_or_else(|| io::Error::other("part file has no free slot"))?;
                (s, true)
            }
        };
        if self.file.is_none() {
            self.create()?;
        }
        let at = self.slot_offset(slot) + u64::from(offset);
        if let Some(f) = &self.file {
            write_all_at(f, data, at)?;
        }
        if new {
            if let Some(s) = self.slots.get_mut(piece as usize) {
                *s = slot;
            }
            if let Some(u) = self.used.get_mut(slot as usize) {
                *u = true;
            }
            self.count += 1;
            self.write_table()?;
        }
        Ok(())
    }

    /// Fills `buf` from `offset` within `piece`; `NotFound` when the piece
    /// has no slot or its bytes are not all there.
    pub(crate) fn read(&self, piece: u32, offset: u32, buf: &mut [u8]) -> io::Result<()> {
        self.check(piece, offset, buf.len())?;
        let (Some(slot), Some(f)) = (self.slot(piece), self.file.as_ref()) else {
            return Err(io::ErrorKind::NotFound.into());
        };
        read_exact_at(f, buf, self.slot_offset(slot) + u64::from(offset)).map_err(|e| {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                io::Error::new(io::ErrorKind::NotFound, e)
            } else {
                e
            }
        })
    }

    /// Whether `piece` has a slot.
    pub(crate) fn has(&self, piece: u32) -> bool {
        self.slot(piece).is_some()
    }

    /// Gives back the slot of `piece`, if any.
    pub(crate) fn free(&mut self, piece: u32) -> io::Result<()> {
        let Some(slot) = self.slot(piece) else {
            return Ok(());
        };
        if let Some(s) = self.slots.get_mut(piece as usize) {
            *s = NO_SLOT;
        }
        if let Some(u) = self.used.get_mut(slot as usize) {
            *u = false;
        }
        self.count = self.count.saturating_sub(1);
        self.write_table()
    }

    /// No piece has a slot.
    pub(crate) fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Syncs the written data, if the file exists.
    pub(crate) fn sync(&self) -> io::Result<()> {
        self.file.as_ref().map_or(Ok(()), File::sync_data)
    }

    /// Closes and deletes the file; every slot is free afterwards.
    pub(crate) fn remove(&mut self) -> io::Result<()> {
        // Closed first: Windows cannot delete an open file.
        self.file = None;
        self.slots.fill(NO_SLOT);
        self.used.fill(false);
        self.count = 0;
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn slot(&self, piece: u32) -> Option<u32> {
        self.slots
            .get(piece as usize)
            .copied()
            .filter(|&s| s != NO_SLOT)
    }

    fn check(&self, piece: u32, offset: u32, len: usize) -> io::Result<()> {
        let end = u64::from(offset).checked_add(len as u64);
        if piece >= self.num_pieces || end.is_none_or(|e| e > u64::from(self.piece_len)) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        Ok(())
    }

    fn slot_offset(&self, slot: u32) -> u64 {
        HEADER_LEN + u64::from(self.num_pieces) * 4 + u64::from(slot) * u64::from(self.piece_len)
    }

    /// Creates the file with its header and an empty slot table.
    fn create(&mut self) -> io::Result<()> {
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;
        let mut header = Vec::with_capacity(HEADER_LEN as usize);
        header.extend_from_slice(MAGIC);
        header.extend_from_slice(&self.num_pieces.to_le_bytes());
        header.extend_from_slice(&self.piece_len.to_le_bytes());
        write_all_at(&f, &header, 0)?;
        self.file = Some(f);
        self.write_table()
    }

    /// Rewrites the whole slot table.
    fn write_table(&self) -> io::Result<()> {
        let Some(f) = &self.file else {
            return Ok(());
        };
        let table: Vec<u8> = self.slots.iter().flat_map(|s| s.to_le_bytes()).collect();
        write_all_at(f, &table, HEADER_LEN)
    }
}

fn le_u32(b: &[u8]) -> Option<u32> {
    Some(u32::from_le_bytes(b.try_into().ok()?))
}

/// `<path>.bad`.
fn bad_path(path: &Path) -> PathBuf {
    let mut s = OsString::from(path.as_os_str());
    s.push(".bad");
    PathBuf::from(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_round_trip_and_reuse() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".x.parts");
        let mut pf = PartFile::open(path.clone(), 4, 8).unwrap();
        assert!(pf.is_empty());
        assert_eq!(pf.path(), path.as_path());
        assert!(!path.exists(), "created by the first write only");
        let mut b = [0u8; 2];
        assert_eq!(
            pf.read(1, 0, &mut b).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        pf.write(3, 2, &[1, 2, 3]).unwrap();
        pf.write(1, 0, &[9; 8]).unwrap();
        assert!(path.exists());
        assert!(!pf.is_empty());
        // Lowest free slot first: piece 3 took slot 0, piece 1 slot 1.
        let data_start = HEADER_LEN + 4 * 4;
        let raw = std::fs::read(&path).unwrap();
        assert_eq!(
            &raw[data_start as usize + 2..data_start as usize + 5],
            &[1, 2, 3]
        );
        assert_eq!(
            &raw[data_start as usize + 8..data_start as usize + 16],
            &[9; 8]
        );
        let mut out = [0u8; 3];
        pf.read(3, 2, &mut out).unwrap();
        assert_eq!(out, [1, 2, 3]);
        // Out of the piece.
        assert_eq!(
            pf.write(3, 7, &[1, 2]).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            pf.write(4, 0, &[1]).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );

        // Reopened: the table round-trips.
        drop(pf);
        let mut pf = PartFile::open(path.clone(), 4, 8).unwrap();
        assert!(pf.has(3) && pf.has(1) && !pf.has(0));
        let mut out = [0u8; 8];
        pf.read(1, 0, &mut out).unwrap();
        assert_eq!(out, [9; 8]);
        // A freed slot is reused by the next piece.
        pf.free(3).unwrap();
        assert!(!pf.has(3));
        assert_eq!(
            pf.read(3, 0, &mut out).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        pf.write(0, 0, &[7; 8]).unwrap();
        let raw = std::fs::read(&path).unwrap();
        assert_eq!(&raw[data_start as usize..data_start as usize + 8], &[7; 8]);
        drop(pf);
        let mut pf = PartFile::open(path.clone(), 4, 8).unwrap();
        pf.read(0, 0, &mut out).unwrap();
        assert_eq!(out, [7; 8]);
        pf.free(0).unwrap();
        pf.free(1).unwrap();
        assert!(pf.is_empty());
        pf.remove().unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn corrupt_header_starts_afresh() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".x.parts");
        let bad = dir.path().join(".x.parts.bad");
        for junk in [
            b"garbage".to_vec(),
            b"NOTPART1\x04\0\0\0\x08\0\0\0".to_vec(),
            // A wrong piece length.
            b"FBTPART1\x04\0\0\0\x09\0\0\0".to_vec(),
            // A valid header with its table cut short.
            b"FBTPART1\x04\0\0\0\x08\0\0\0\0\0".to_vec(),
            // A slot past the piece count, then a slot used twice.
            [
                &b"FBTPART1\x04\0\0\0\x08\0\0\0"[..],
                &[9, 0, 0, 0],
                &[0xff; 12],
            ]
            .concat(),
            [
                &b"FBTPART1\x04\0\0\0\x08\0\0\0"[..],
                &[1, 0, 0, 0, 1, 0, 0, 0],
                &[0xff; 8],
            ]
            .concat(),
        ] {
            std::fs::write(&path, &junk).unwrap();
            let mut pf = PartFile::open(path.clone(), 4, 8).unwrap();
            assert!(pf.is_empty());
            assert!(!path.exists(), "set aside");
            assert_eq!(std::fs::read(&bad).unwrap(), junk);
            // Usable afresh.
            pf.write(2, 0, &[5; 8]).unwrap();
            let mut out = [0u8; 8];
            pf.read(2, 0, &mut out).unwrap();
            assert_eq!(out, [5; 8]);
            drop(pf);
            std::fs::remove_file(&path).unwrap();
        }
    }

    #[test]
    fn mismatched_piece_count_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".x.parts");
        // A header claiming u32::MAX pieces for a 4-piece torrent: never
        // trusted, never allocated from.
        let mut junk = b"FBTPART1".to_vec();
        junk.extend_from_slice(&u32::MAX.to_le_bytes());
        junk.extend_from_slice(&8u32.to_le_bytes());
        junk.extend_from_slice(&[0xff; 16]);
        std::fs::write(&path, &junk).unwrap();
        let pf = PartFile::open(path.clone(), 4, 8).unwrap();
        assert!(pf.is_empty());
        assert!(!path.exists());
        assert_eq!(
            std::fs::read(dir.path().join(".x.parts.bad")).unwrap(),
            junk
        );
    }
}
