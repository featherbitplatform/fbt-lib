//! File stamps: what resume data records of each file to tell, at the next
//! add, whether the file changed since the save.

use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// A file's length and last modification time, as
/// [`Storage::stamps`](super::Storage::stamps) reports it and resume data
/// records it. A file whose stamp differs from the saved one is checked
/// again. The time has a granularity of one second (as libtorrent's): an
/// edit within the same second as the save goes unnoticed.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileStamp {
    /// Length in bytes.
    pub len: u64,
    /// Last modification, in whole seconds since the Unix epoch (negative
    /// before it).
    pub mtime: i64,
}

impl FileStamp {
    /// A stamp of `len` bytes modified at `mtime` (seconds since the Unix
    /// epoch).
    pub fn new(len: u64, mtime: i64) -> FileStamp {
        FileStamp { len, mtime }
    }

    /// The stamp of the file at `path`; `None` if nothing is there (or it
    /// is not a regular file).
    pub(crate) fn of(path: &Path) -> io::Result<Option<FileStamp>> {
        let meta = match std::fs::metadata(path) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        if !meta.is_file() {
            return Ok(None);
        }
        Ok(Some(FileStamp {
            len: meta.len(),
            mtime: unix_seconds(meta.modified()?),
        }))
    }
}

/// `t` in whole seconds since the Unix epoch, saturating.
fn unix_seconds(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
        Err(e) => i64::try_from(e.duration().as_secs()).map_or(i64::MIN, |s| -s),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn stamp_follows_length_and_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("f");
        assert_eq!(FileStamp::of(&p).unwrap(), None);
        assert_eq!(FileStamp::of(dir.path()).unwrap(), None, "a directory");
        std::fs::write(&p, b"abc").unwrap();
        let t = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        std::fs::File::options()
            .write(true)
            .open(&p)
            .unwrap()
            .set_modified(t)
            .unwrap();
        assert_eq!(
            FileStamp::of(&p).unwrap(),
            Some(FileStamp::new(3, 1_700_000_000))
        );
    }

    #[test]
    fn times_before_the_epoch_are_negative() {
        assert_eq!(unix_seconds(UNIX_EPOCH - Duration::from_secs(5)), -5);
        assert_eq!(unix_seconds(UNIX_EPOCH + Duration::from_millis(1500)), 1);
    }
}
