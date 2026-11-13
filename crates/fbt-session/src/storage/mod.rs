//! Torrent data storage: the [`Storage`] trait, a filesystem implementation,
//! an in-memory one for tests, and piece checking.

pub(crate) mod cache;
mod check;
mod fs;
mod mem;
mod part;
mod stamp;

use std::io;
use std::path::Path;

use std::collections::HashSet;

use fbt_metainfo::{FileEntry, Metainfo, RelPath};

pub use cache::CacheStats;
pub(crate) use check::{PieceCheck, verify_piece};
pub use check::{check_pieces, sha1_piece};
pub use fs::{FsStorage, StorageError, error_path};
pub use mem::MemStorage;
pub use stamp::FileStamp;

/// The name of a torrent's part file inside its save path (see
/// [`FsStorage::part_path`]): `.<info hash>.parts`, with `.1`, `.2`, ...
/// appended while one of the torrent's own files (as its metainfo names
/// them) has that path or lies below it, so the part file never takes a
/// file's place. Renames are kept off it instead: [`is_part_path`].
pub(crate) fn part_file_name(metainfo: &Metainfo) -> String {
    free_part_name(
        format!(".{}.parts", metainfo.info_hashes),
        &metainfo.info.files,
    )
}

/// `base`, or `base` with the first `.N` suffix no non-pad file of `files`
/// starts with (compared as collision keys: ignoring case and Unicode
/// normalisation). Bounded: `files` takes at most `files.len()` names.
fn free_part_name(base: String, files: &[FileEntry]) -> String {
    let taken: HashSet<String> = files
        .iter()
        .filter(|f| !f.pad)
        .filter_map(|f| f.path.key().split('/').next().map(str::to_owned))
        .collect();
    let free = |name: &String| !taken.contains(&name.to_lowercase());
    if free(&base) {
        return base;
    }
    (1..=files.len())
        .map(|n| format!("{base}.{n}"))
        .find(free)
        .unwrap_or(base)
}

/// Whether `path` (relative to the save path) is the part file `part_name`,
/// or lies below it, ignoring case and Unicode normalisation as
/// [`fbt_metainfo::collides`] does.
pub(crate) fn is_part_path(path: &RelPath, part_name: &str) -> bool {
    path.key().split('/').next() == Some(part_name.to_lowercase().as_str())
}

/// What [`Storage::move_to`] does with files that already exist at the
/// target.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MoveMode {
    /// Checks every target first and moves nothing if any exists.
    #[default]
    FailIfExists,
    /// Overwrites files at the target.
    ReplaceExisting,
    /// Keeps the files at the target (ours stay at the old path); the
    /// session rechecks the torrent afterwards.
    KeepExisting,
}

/// Random-access storage for the concatenated torrent content.
///
/// Offsets are torrent-global. Reading data that was never written either
/// fails with [`io::ErrorKind::NotFound`] or yields zeros; both are allowed.
///
/// Implementors should expect methods to be added before 1.0 (with default
/// implementations where possible, but not guaranteed).
pub trait Storage: Send + Sync + std::fmt::Debug + 'static {
    /// Fills `buf` from `offset`.
    fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<()>;
    /// Writes `data` at `offset`.
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()>;
    /// Writes `bufs` back to back from `offset`, as one contiguous range
    /// (the write-back cache's coalesced runs). The default writes each
    /// slice at its running offset with [`write`](Self::write).
    fn write_vectored(&self, offset: u64, bufs: &[&[u8]]) -> io::Result<()> {
        let mut at = offset;
        for b in bufs {
            self.write(at, b)?;
            at = at
                .checked_add(b.len() as u64)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "offset overflow"))?;
        }
        Ok(())
    }
    /// Flushes buffered data to the medium.
    fn flush(&self) -> io::Result<()>;
    /// Removes the torrent's data.
    fn delete_files(&self) -> io::Result<()>;
    /// Which files are wanted: one flag per file (pad files included),
    /// `false` for a skipped file. The session calls it on a blocking
    /// thread right before every check reads the data (the check's own
    /// thread), and on the disk pool after every change of the file
    /// priorities, once the pieces written under the old ones are on disk.
    /// The default does nothing; [`FsStorage`] keeps skipped files that do
    /// not exist out of the filesystem (its part file).
    fn set_file_priorities(&self, wanted: &[bool]) -> io::Result<()> {
        let _ = wanted;
        Ok(())
    }
    /// Moves the torrent's data below `save_path`, per `mode`. The session
    /// calls it on the disk pool with no read or write of this storage
    /// running, every verified piece written first. A failure must leave
    /// the data usable where it was. The default fails with
    /// [`io::ErrorKind::Unsupported`].
    fn move_to(&self, save_path: &Path, mode: MoveMode) -> io::Result<()> {
        let _ = (save_path, mode);
        Err(io::ErrorKind::Unsupported.into())
    }
    /// Gives file `index` the path `path`, relative to the save path, with
    /// the same guarantees as [`move_to`](Self::move_to). The session has
    /// checked that `path` collides with no other file and is not
    /// [`FsStorage`]'s part file (nor below it). The default fails
    /// with [`io::ErrorKind::Unsupported`].
    fn rename_file(&self, index: usize, path: &RelPath) -> io::Result<()> {
        let _ = (index, path);
        Err(io::ErrorKind::Unsupported.into())
    }
    /// The stamp (length and modification time) of each file, pad files
    /// included, `None` for one that does not exist; then one more entry
    /// for the storage's own side file, if it has one (`None` otherwise).
    /// Resume data records them when it is saved, and the next add trusts
    /// its pieces without a check only if every file holding a saved piece
    /// has the same stamp again. The session calls it on the disk pool with
    /// every verified piece written and flushed.
    ///
    /// `Ok(None)` (the default) means the storage cannot tell: resume data
    /// is then trusted as saved. [`FsStorage`] closes its cached handles
    /// first (on Windows a file's modification time is final only then)
    /// and stamps its part file last.
    fn stamps(&self) -> io::Result<Option<Vec<Option<FileStamp>>>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbt_core::{Bitfield, PieceLayout};
    use fbt_metainfo::Metainfo;
    use std::io::ErrorKind;
    use std::ops::ControlFlow;
    use std::path::Path;

    fn bstr(out: &mut Vec<u8>, s: &[u8]) {
        out.extend_from_slice(format!("{}:", s.len()).as_bytes());
        out.extend_from_slice(s);
    }

    /// Builds a multi-file torrent: (path components, len, pad).
    pub(super) fn torrent(name: &str, files: &[(&[&str], u64, bool)], piece_len: u32) -> Metainfo {
        let total: u64 = files.iter().map(|f| f.1).sum();
        let n = total.div_ceil(u64::from(piece_len)) as usize;
        let mut b = b"d4:infod5:filesl".to_vec();
        for (path, len, pad) in files {
            b.extend_from_slice(b"d");
            if *pad {
                b.extend_from_slice(b"4:attr1:p");
            }
            b.extend_from_slice(format!("6:lengthi{len}e4:pathl").as_bytes());
            for c in *path {
                bstr(&mut b, c.as_bytes());
            }
            b.extend_from_slice(b"ee");
        }
        b.extend_from_slice(b"e4:name");
        bstr(&mut b, name.as_bytes());
        b.extend_from_slice(format!("12:piece lengthi{piece_len}e6:pieces{}:", n * 20).as_bytes());
        b.extend(std::iter::repeat_n(0u8, n * 20));
        b.extend_from_slice(b"ee");
        Metainfo::from_bytes(&b).expect("valid torrent")
    }

    fn three_files() -> Metainfo {
        torrent(
            "t",
            &[
                (&["a"], 5, false),
                (&["empty"], 0, false),
                (&["b"], 7, false),
            ],
            4,
        )
    }

    fn file_at(parts: &[&str]) -> fbt_metainfo::FileEntry {
        fbt_metainfo::FileEntry {
            path: fbt_metainfo::sanitize_path(parts.iter().map(|p| p.as_bytes())).unwrap(),
            len: 1,
            pad: false,
            pieces_root: None,
        }
    }

    /// The part file never takes a path the torrent's own files use (or a
    /// directory of theirs): `.1`, `.2`, ... are appended. A real torrent
    /// cannot name a file after its own info hash, so the files are made up.
    #[test]
    fn part_name_avoids_the_torrents_files() {
        let m = three_files();
        let base = format!(".{}.parts", m.info_hashes);
        assert_eq!(part_file_name(&m), base);
        assert_eq!(free_part_name(base.clone(), &m.info.files), base);
        let one = format!("{base}.1");
        let files = vec![
            file_at(&["a"]),
            file_at(&[base.to_uppercase().as_str()]),
            file_at(&[one.as_str(), "x"]),
        ];
        assert_eq!(free_part_name(base.clone(), &files), format!("{base}.2"));
        // A pad file never lands on disk: it takes nothing.
        let mut pad = file_at(&[base.as_str()]);
        pad.pad = true;
        assert_eq!(free_part_name(base.clone(), &[pad]), base);
        // The name and anything below it are the part file's.
        let part = fbt_metainfo::sanitize_path([base.as_bytes()]).unwrap();
        assert!(is_part_path(&part, &base));
        let upper = base.to_uppercase();
        let below = fbt_metainfo::sanitize_path([upper.as_bytes(), b"x".as_slice()]).unwrap();
        assert!(is_part_path(&below, &base));
        let other = fbt_metainfo::sanitize_path([b"x".as_slice(), base.as_bytes()]).unwrap();
        assert!(!is_part_path(&other, &base));
        let longer = format!("{base}x");
        let longer = fbt_metainfo::sanitize_path([longer.as_bytes()]).unwrap();
        assert!(!is_part_path(&longer, &base));
    }

    #[test]
    fn fs_storage_roundtrip_across_file_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let s = FsStorage::new(dir.path().to_path_buf(), &three_files());
        let data: Vec<u8> = (1..=12).collect();
        s.write(0, &data).unwrap();
        let mut out = [0u8; 12];
        s.read(0, &mut out).unwrap();
        assert_eq!(out.as_slice(), data.as_slice());
        assert!(dir.path().join("t/empty").exists());
        assert_eq!(std::fs::metadata(dir.path().join("t/a")).unwrap().len(), 5);
    }

    #[test]
    fn fs_storage_creates_empty_files() {
        let dir = tempfile::tempdir().unwrap();
        let s = FsStorage::new(dir.path().to_path_buf(), &three_files());
        assert!(!dir.path().join("t/empty").exists());
        // A first write that does not overlap the empty file still creates it.
        s.write(0, &[1, 2]).unwrap();
        assert!(dir.path().join("t/empty").exists());

        let dir2 = tempfile::tempdir().unwrap();
        let s2 = FsStorage::new(dir2.path().to_path_buf(), &three_files());
        s2.create_empty_files().unwrap();
        assert!(dir2.path().join("t/empty").exists());
        assert!(!dir2.path().join("t/a").exists());
        // Idempotent and never truncating.
        s2.create_empty_files().unwrap();
    }

    #[test]
    fn pad_file_never_created_and_reads_zero() {
        let dir = tempfile::tempdir().unwrap();
        let m = torrent(
            "t",
            &[
                (&["a"], 3, false),
                (&[".pad", "5"], 5, true),
                (&["b"], 4, false),
            ],
            4,
        );
        let s = FsStorage::new(dir.path().to_path_buf(), &m);
        s.write(0, &[1, 2, 3, 0, 0, 0, 0, 0, 9, 9, 9, 9]).unwrap();
        let mut out = [0xffu8; 12];
        s.read(0, &mut out).unwrap();
        assert_eq!(out, [1, 2, 3, 0, 0, 0, 0, 0, 9, 9, 9, 9]);
        assert!(!dir.path().join("t/.pad").exists());
        let e = s.write(3, &[1]).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::InvalidData);
        let mut z = [1u8; 5];
        s.read(3, &mut z).unwrap();
        assert_eq!(z, [0; 5]);
    }

    #[test]
    fn missing_file_reads_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let s = FsStorage::new(dir.path().to_path_buf(), &three_files());
        let mut b = [0u8; 2];
        assert_eq!(s.read(0, &mut b).unwrap_err().kind(), ErrorKind::NotFound);
        assert!(!dir.path().join("t").exists(), "read must not create files");
        s.write(0, &[1]).unwrap();
        // File `a` has set_len(5) so reads inside it work; file `b` is missing.
        assert_eq!(s.read(5, &mut b).unwrap_err().kind(), ErrorKind::NotFound);
    }

    #[test]
    fn read_past_eof_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let m = torrent("t", &[(&["a"], 8, false)], 4);
        let s = FsStorage::new(dir.path().to_path_buf(), &m);
        std::fs::create_dir_all(dir.path().join("t")).unwrap();
        std::fs::write(dir.path().join("t/a"), [1, 2]).unwrap();
        let mut b = [0u8; 4];
        assert_eq!(s.read(0, &mut b).unwrap_err().kind(), ErrorKind::NotFound);
        let mut ok = [0u8; 2];
        s.read(0, &mut ok).unwrap();
        // Writing never truncates existing data and grows to the full length.
        s.write(6, &[7, 7]).unwrap();
        assert_eq!(std::fs::metadata(dir.path().join("t/a")).unwrap().len(), 8);
        s.read(0, &mut ok).unwrap();
        assert_eq!(ok, [1, 2]);
    }

    fn piece_hashes(data: &[u8], piece_len: usize) -> Vec<[u8; 20]> {
        data.chunks(piece_len).map(sha1_piece).collect()
    }

    #[test]
    fn check_pieces_detects_partial_data() {
        let dir = tempfile::tempdir().unwrap();
        let m = torrent("t", &[(&["a"], 40, false), (&["b"], 24, false)], 16);
        let s = FsStorage::new(dir.path().to_path_buf(), &m);
        let data: Vec<u8> = (0..64u8)
            .map(|b| b.wrapping_mul(7).wrapping_add(1))
            .collect();
        let mut m = m;
        m.info.pieces = piece_hashes(&data, 16);
        let layout = PieceLayout::new(64, 16).unwrap();
        s.write(0, data.get(0..16).unwrap()).unwrap();
        s.write(32, data.get(32..48).unwrap()).unwrap();
        let mut seen = Vec::new();
        let (bf, roots): (Bitfield, _) = check_pieces(&s, &layout, &m, |i| {
            seen.push(i);
            ControlFlow::Continue(())
        })
        .unwrap();
        assert!(roots.is_empty(), "no v2 roots for a v1 torrent");
        assert_eq!(bf.iter_ones().collect::<Vec<_>>(), vec![0, 2]);
        assert_eq!(seen, vec![0, 1, 2, 3]);
    }

    #[test]
    fn check_pieces_on_mem_storage_and_errors() {
        let data: Vec<u8> = (0..10u8).collect();
        let mut m = torrent("t", &[(&["a"], 10, false)], 4);
        m.info.pieces = piece_hashes(&data, 4);
        let layout = PieceLayout::new(10, 4).unwrap();
        let s = MemStorage::from_bytes(data);
        let (bf, _) = check_pieces(&s, &layout, &m, |_| ControlFlow::Continue(())).unwrap();
        assert!(bf.is_full());
    }

    #[test]
    fn check_pieces_can_be_cancelled() {
        let data: Vec<u8> = (0..10u8).collect();
        let mut m = torrent("t", &[(&["a"], 10, false)], 4);
        m.info.pieces = piece_hashes(&data, 4);
        let layout = PieceLayout::new(10, 4).unwrap();
        let s = MemStorage::from_bytes(data);
        let mut seen = Vec::new();
        let e = check_pieces(&s, &layout, &m, |i| {
            seen.push(i);
            if i == 1 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .unwrap_err();
        assert_eq!(e.kind(), ErrorKind::Interrupted);
        assert_eq!(seen, vec![0, 1]);
    }

    #[test]
    fn delete_files_removes_tree() {
        let dir = tempfile::tempdir().unwrap();
        let m = torrent(
            "t",
            &[
                (&["d1", "d2", "a"], 3, false),
                (&["e"], 0, false),
                (&["b"], 3, false),
            ],
            4,
        );
        let keep = dir.path().join("keep.txt");
        std::fs::write(&keep, b"x").unwrap();
        let s = FsStorage::new(dir.path().to_path_buf(), &m);
        s.write(0, &[1, 2, 3, 4, 5, 6]).unwrap();
        assert!(dir.path().join("t/d1/d2/a").exists());
        // A foreign file inside the torrent dir keeps that directory alive.
        std::fs::write(dir.path().join("t/d1/foreign"), b"y").unwrap();
        s.delete_files().unwrap();
        assert!(!dir.path().join("t/d1/d2").exists());
        assert!(dir.path().join("t/d1/foreign").exists());
        assert!(!dir.path().join("t/b").exists());
        std::fs::remove_file(dir.path().join("t/d1/foreign")).unwrap();
        s.delete_files().unwrap();
        assert!(!dir.path().join("t").exists());
        assert!(dir.path().exists());
        assert!(keep.exists());
    }

    #[test]
    fn open_handle_cap() {
        let dir = tempfile::tempdir().unwrap();
        let names: Vec<String> = (0..100).map(|i| format!("f{i}")).collect();
        let files: Vec<(Vec<&str>, u64, bool)> =
            names.iter().map(|n| (vec![n.as_str()], 1, false)).collect();
        let refs: Vec<(&[&str], u64, bool)> = files
            .iter()
            .map(|(p, l, d)| (p.as_slice(), *l, *d))
            .collect();
        let m = torrent("t", &refs, 16);
        let s = FsStorage::new(dir.path().to_path_buf(), &m);
        for i in 0..100u64 {
            s.write(i, &[i as u8]).unwrap();
            assert!(s.open_handles() <= 64);
        }
        assert!(s.open_handles() <= 64);
        let mut out = [0u8; 100];
        s.read(0, &mut out).unwrap();
        assert!(s.open_handles() <= 64);
        let expect: Vec<u8> = (0..100).collect();
        assert_eq!(out.as_slice(), expect.as_slice());
        let _: &Path = dir.path();
    }

    #[test]
    fn mem_storage_basics() {
        let s = MemStorage::new(8);
        s.write(2, &[1, 2]).unwrap();
        let mut b = [9u8; 4];
        s.read(0, &mut b).unwrap();
        assert_eq!(b, [0, 0, 1, 2]);
        assert_eq!(
            s.read(6, &mut [0u8; 4]).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }

    #[test]
    fn flush_syncs_files_evicted_from_the_handle_cache() {
        let dir = tempfile::tempdir().unwrap();
        let names: Vec<String> = (0..100).map(|i| format!("f{i}")).collect();
        let files: Vec<(Vec<&str>, u64, bool)> =
            names.iter().map(|n| (vec![n.as_str()], 1, false)).collect();
        let refs: Vec<(&[&str], u64, bool)> = files
            .iter()
            .map(|(p, l, d)| (p.as_slice(), *l, *d))
            .collect();
        let s = FsStorage::new(dir.path().to_path_buf(), &torrent("t", &refs, 16));
        for i in 0..100u64 {
            s.write(i, &[1]).unwrap();
        }
        assert!(s.open_handles() <= 64);
        s.flush().unwrap();
        assert_eq!(s.synced_files(), 100);
    }
}
