use super::Storage;
use fbt_core::FileLayout;
use fbt_metainfo::Metainfo;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Maximum number of simultaneously open file handles.
const MAX_OPEN: usize = 64;

#[cfg(unix)]
fn read_exact_at(f: &File, buf: &mut [u8], off: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    f.read_exact_at(buf, off)
}

#[cfg(unix)]
fn write_all_at(f: &File, buf: &[u8], off: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    f.write_all_at(buf, off)
}

#[cfg(windows)]
fn read_exact_at(f: &File, mut buf: &mut [u8], mut off: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        let n = f.seek_read(buf, off)?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        buf = buf.split_at_mut(n).1;
        off += n as u64;
    }
    Ok(())
}

#[cfg(windows)]
fn write_all_at(f: &File, mut buf: &[u8], mut off: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        let n = f.seek_write(buf, off)?;
        if n == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        buf = buf.split_at(n).1;
        off += n as u64;
    }
    Ok(())
}

/// An I/O error on one file of a torrent.
///
/// [`FsStorage`] returns every I/O error as an [`io::Error`] of the
/// original kind wrapping one of these; [`error_path`] reads the path back.
/// Other [`Storage`] implementations may do the same to have the session
/// report the file in [`Event::FileError`](crate::Event::FileError).
#[derive(Debug, thiserror::Error)]
#[error("{}: {source}", .path.display())]
pub struct StorageError {
    /// The file the operation failed on.
    pub path: PathBuf,
    /// The underlying error.
    pub source: io::Error,
}

/// The file an I/O error names, when it wraps a [`StorageError`].
pub fn error_path(e: &io::Error) -> Option<&Path> {
    e.get_ref()
        .and_then(|inner| inner.downcast_ref::<StorageError>())
        .map(|s| s.path.as_path())
}

/// `e` naming `path`, keeping its kind; an error naming a file already is
/// returned as is.
fn at(path: &Path, e: io::Error) -> io::Error {
    if error_path(&e).is_some() {
        return e;
    }
    io::Error::new(
        e.kind(),
        StorageError {
            path: path.to_path_buf(),
            source: e,
        },
    )
}

#[derive(Debug)]
struct Entry {
    path: PathBuf,
    len: u64,
    pad: bool,
}

#[derive(Debug)]
struct Handle {
    /// Shared with the reads and writes using it, which run outside the
    /// state lock; an evicted handle closes once the last of them is done.
    file: Arc<File>,
    writable: bool,
    last_used: u64,
}

#[derive(Debug, Default)]
struct State {
    open: HashMap<usize, Handle>,
    tick: u64,
    /// Files whose length was already ensured by this instance.
    sized: HashSet<usize>,
    empties_created: bool,
    /// Files written through a handle evicted since the last flush, which
    /// reopens them to sync.
    unsynced: HashSet<usize>,
    /// Files synced to disk, recorded for the flush-coverage tests.
    #[cfg(test)]
    synced: HashSet<usize>,
}

/// Test hook run by every file read between taking the handle and reading.
#[cfg(test)]
struct ReadHook(Box<dyn Fn() + Send + Sync>);

#[cfg(test)]
impl std::fmt::Debug for ReadHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReadHook")
    }
}

/// Filesystem storage: each torrent file lives at `save_path/<entry path>`.
///
/// Reads and writes of different (or the same) files run in parallel: the
/// internal lock only guards the handle cache, never the I/O. Handles are
/// cached up to 64 open files; [`flush`](Storage::flush) syncs every file
/// written since the last flush, evicted ones included. I/O errors name
/// their file ([`StorageError`]).
#[derive(Debug)]
pub struct FsStorage {
    save_path: PathBuf,
    entries: Vec<Entry>,
    layout: FileLayout,
    state: Mutex<State>,
    #[cfg(test)]
    read_hook: std::sync::OnceLock<ReadHook>,
}

impl FsStorage {
    /// Storage for `metainfo`'s files below `save_path`.
    pub fn new(save_path: PathBuf, metainfo: &Metainfo) -> FsStorage {
        let entries: Vec<Entry> = metainfo
            .info
            .files
            .iter()
            .map(|f| Entry {
                path: save_path.join(f.path.to_path_buf()),
                len: f.len,
                pad: f.pad,
            })
            .collect();
        FsStorage {
            layout: FileLayout::new(entries.iter().map(|e| e.len)),
            save_path,
            entries,
            state: Mutex::new(State::default()),
            #[cfg(test)]
            read_hook: std::sync::OnceLock::new(),
        }
    }

    /// Creates every zero-length non-pad file (and its directories).
    pub fn create_empty_files(&self) -> io::Result<()> {
        let mut st = self.lock();
        self.create_empty_locked(&mut st)
    }

    #[cfg(test)]
    pub(super) fn synced_files(&self) -> usize {
        self.lock().synced.len()
    }

    #[cfg(test)]
    pub(super) fn open_handles(&self) -> usize {
        self.lock().open.len()
    }

    #[cfg(test)]
    fn set_read_hook(&self, hook: impl Fn() + Send + Sync + 'static) {
        let _ = self.read_hook.set(ReadHook(Box::new(hook)));
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn create_empty_locked(&self, st: &mut State) -> io::Result<()> {
        if st.empties_created {
            return Ok(());
        }
        for e in self.entries.iter().filter(|e| e.len == 0 && !e.pad) {
            create_parent(&e.path).map_err(|err| at(&e.path, err))?;
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&e.path)
                .map_err(|err| at(&e.path, err))?;
        }
        st.empties_created = true;
        Ok(())
    }

    /// The (cached) handle of file `idx`, opened for writing if `write`.
    /// Only the lookup and the opening hold the lock; the caller does its
    /// I/O on the returned handle after it is released.
    fn handle(&self, idx: usize, write: bool) -> io::Result<Arc<File>> {
        let entry = self
            .entries
            .get(idx)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        let mut st = self.lock();
        st.tick += 1;
        let tick = st.tick;
        if let Some(h) = st.open.get_mut(&idx)
            && (h.writable || !write)
        {
            h.last_used = tick;
            return Ok(Arc::clone(&h.file));
        }
        st.open.remove(&idx);
        let file = if write {
            Self::open_writable(&mut st, idx, entry).map_err(|e| at(&entry.path, e))?
        } else {
            File::open(&entry.path).map_err(|e| at(&entry.path, e))?
        };
        if st.open.len() >= MAX_OPEN {
            let lru = st
                .open
                .iter()
                .min_by_key(|(_, h)| h.last_used)
                .map(|(k, h)| (*k, h.writable));
            if let Some((k, writable)) = lru {
                // Its sync waits for `flush`, which reopens the file.
                if writable {
                    st.unsynced.insert(k);
                }
                st.open.remove(&k);
            }
        }
        let file = Arc::new(file);
        st.open.insert(
            idx,
            Handle {
                file: Arc::clone(&file),
                writable: write,
                last_used: tick,
            },
        );
        Ok(file)
    }

    fn open_writable(st: &mut State, idx: usize, entry: &Entry) -> io::Result<File> {
        create_parent(&entry.path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&entry.path)?;
        if !st.sized.contains(&idx) {
            if file.metadata()?.len() < entry.len {
                file.set_len(entry.len)?;
            }
            st.sized.insert(idx);
        }
        Ok(file)
    }

    fn check_range(&self, offset: u64, len: usize) -> io::Result<()> {
        match offset.checked_add(len as u64) {
            Some(end) if end <= self.layout.total_len() => Ok(()),
            _ => Err(io::ErrorKind::InvalidInput.into()),
        }
    }

    fn is_pad(&self, idx: usize) -> bool {
        self.entries.get(idx).is_some_and(|e| e.pad)
    }

    fn path_of(&self, idx: usize) -> &Path {
        self.entries
            .get(idx)
            .map_or(self.save_path.as_path(), |e| e.path.as_path())
    }
}

fn create_parent(path: &Path) -> io::Result<()> {
    match path.parent() {
        Some(p) => std::fs::create_dir_all(p),
        None => Ok(()),
    }
}

impl Storage for FsStorage {
    fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.check_range(offset, buf.len())?;
        let mut pos = 0usize;
        for s in self.layout.map(offset, buf.len() as u64) {
            let end = pos + s.len as usize;
            let chunk = buf
                .get_mut(pos..end)
                .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
            pos = end;
            if self.is_pad(s.file) {
                chunk.fill(0);
                continue;
            }
            let file = self.handle(s.file, false)?;
            #[cfg(test)]
            if let Some(hook) = self.read_hook.get() {
                (hook.0)();
            }
            read_exact_at(&file, chunk, s.file_offset).map_err(|e| {
                let e = if e.kind() == io::ErrorKind::UnexpectedEof {
                    io::Error::new(io::ErrorKind::NotFound, e)
                } else {
                    e
                };
                at(self.path_of(s.file), e)
            })?;
        }
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.check_range(offset, data.len())?;
        {
            let mut st = self.lock();
            self.create_empty_locked(&mut st)?;
        }
        let slices = self.layout.map(offset, data.len() as u64);
        let mut pos = 0usize;
        let mut parts = Vec::with_capacity(slices.len());
        for s in &slices {
            let end = pos + s.len as usize;
            let chunk = data
                .get(pos..end)
                .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
            pos = end;
            parts.push(chunk);
            if self.is_pad(s.file) && chunk.iter().any(|&b| b != 0) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "non-zero data written to a padding file",
                ));
            }
        }
        for (s, chunk) in slices.iter().zip(parts) {
            if self.is_pad(s.file) {
                continue;
            }
            let file = self.handle(s.file, true)?;
            write_all_at(&file, chunk, s.file_offset).map_err(|e| at(self.path_of(s.file), e))?;
        }
        Ok(())
    }

    fn flush(&self) -> io::Result<()> {
        let (open, reopen): (Vec<(usize, Arc<File>)>, Vec<usize>) = {
            let mut st = self.lock();
            let open: Vec<(usize, Arc<File>)> = st
                .open
                .iter()
                .filter(|(_, h)| h.writable)
                .map(|(k, h)| (*k, Arc::clone(&h.file)))
                .collect();
            let reopen = st
                .unsynced
                .drain()
                .filter(|k| !open.iter().any(|(o, _)| o == k))
                .collect();
            (open, reopen)
        };
        let mut first_err = None;
        let mut synced = Vec::with_capacity(open.len() + reopen.len());
        // An open handle that fails is tried again by the next flush.
        for (k, file) in open {
            match file.sync_data() {
                Ok(()) => synced.push(k),
                Err(e) => {
                    first_err.get_or_insert_with(|| at(self.path_of(k), e));
                }
            }
        }
        let mut retry = Vec::new();
        for k in reopen {
            let path = self.path_of(k);
            let r = OpenOptions::new()
                .write(true)
                .open(path)
                .and_then(|f| f.sync_data());
            match r {
                Ok(()) => synced.push(k),
                Err(e) => {
                    // A file gone from disk has nothing left to sync.
                    if e.kind() != io::ErrorKind::NotFound {
                        retry.push(k);
                    }
                    first_err.get_or_insert_with(|| at(path, e));
                }
            }
        }
        let mut st = self.lock();
        st.unsynced.extend(retry);
        #[cfg(test)]
        st.synced.extend(synced);
        #[cfg(not(test))]
        let _ = synced;
        first_err.map_or(Ok(()), Err)
    }

    fn delete_files(&self) -> io::Result<()> {
        let mut st = self.lock();
        st.open.clear();
        st.sized.clear();
        st.unsynced.clear();
        st.empties_created = false;
        for e in self.entries.iter().filter(|e| !e.pad) {
            match std::fs::remove_file(&e.path) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(at(&e.path, err)),
            }
        }
        // Ancestor directories strictly below save_path, deepest first.
        let mut dirs: HashSet<PathBuf> = HashSet::new();
        for e in self.entries.iter().filter(|e| !e.pad) {
            for a in e.path.ancestors().skip(1) {
                if a == self.save_path || !a.starts_with(&self.save_path) {
                    break;
                }
                dirs.insert(a.to_path_buf());
            }
        }
        let mut dirs: Vec<PathBuf> = dirs.into_iter().collect();
        dirs.sort_by_key(|d| std::cmp::Reverse(d.components().count()));
        for d in dirs {
            match std::fs::remove_dir(&d) {
                Ok(()) => {}
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::NotFound | io::ErrorKind::DirectoryNotEmpty
                    ) => {}
                Err(err) => return Err(at(&d, err)),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tests::torrent;
    use std::sync::{Arc, Barrier, mpsc};
    use std::time::Duration;

    /// `n` one-byte files named `f0`.. below `t/`.
    fn many_files(n: usize) -> Metainfo {
        let names: Vec<String> = (0..n).map(|i| format!("f{i}")).collect();
        let files: Vec<(Vec<&str>, u64, bool)> =
            names.iter().map(|n| (vec![n.as_str()], 1, false)).collect();
        let refs: Vec<(&[&str], u64, bool)> = files
            .iter()
            .map(|(p, l, d)| (p.as_slice(), *l, *d))
            .collect();
        torrent("t", &refs, 16)
    }

    #[test]
    fn eviction_defers_sync_to_flush() {
        let dir = tempfile::tempdir().unwrap();
        let s = FsStorage::new(dir.path().to_path_buf(), &many_files(MAX_OPEN + 36));
        for i in 0..(MAX_OPEN + 36) as u64 {
            s.write(i, &[1]).unwrap();
        }
        assert!(s.open_handles() <= MAX_OPEN);
        assert_eq!(s.synced_files(), 0, "eviction synced");
        s.flush().unwrap();
        assert_eq!(s.synced_files(), MAX_OPEN + 36);
        // Nothing is left to sync: a second flush only covers open handles.
        s.flush().unwrap();
        assert_eq!(s.synced_files(), MAX_OPEN + 36);
    }

    #[test]
    fn parallel_reads_do_not_serialize() {
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(FsStorage::new(dir.path().to_path_buf(), &many_files(4)));
        s.write(0, &[1, 2, 3, 4]).unwrap();
        // Every read waits inside the hook until all four are in it: only
        // reads that run outside the state lock get there together.
        let barrier = Arc::new(Barrier::new(4));
        let b = Arc::clone(&barrier);
        s.set_read_hook(move || {
            b.wait();
        });
        let (tx, rx) = mpsc::channel();
        for i in 0..4u64 {
            let s = Arc::clone(&s);
            let tx = tx.clone();
            std::thread::spawn(move || {
                let mut b = [0u8; 1];
                let r = s.read(i, &mut b).map(|()| b[0]);
                let _ = tx.send(r.ok());
            });
        }
        let mut got: Vec<u8> = (0..4)
            .map(|_| {
                rx.recv_timeout(Duration::from_secs(10))
                    .expect("reads serialized behind the lock")
                    .expect("read")
            })
            .collect();
        got.sort_unstable();
        assert_eq!(got, [1, 2, 3, 4]);
    }

    #[test]
    fn errors_name_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let m = torrent("t", &[(&["a"], 4, false), (&["b"], 4, false)], 4);
        let s = FsStorage::new(dir.path().to_path_buf(), &m);
        let e = s.read(4, &mut [0u8; 4]).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::NotFound);
        assert_eq!(
            error_path(&e),
            Some(dir.path().join("t").join("b").as_path())
        );
        assert!(e.to_string().contains('b'), "{e}");
        // A short file: the end-of-file error names it too.
        std::fs::create_dir_all(dir.path().join("t")).unwrap();
        std::fs::write(dir.path().join("t").join("a"), [1]).unwrap();
        let e = s.read(0, &mut [0u8; 4]).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::NotFound);
        assert_eq!(
            error_path(&e),
            Some(dir.path().join("t").join("a").as_path())
        );
        // Errors from elsewhere carry no path.
        assert_eq!(error_path(&io::Error::other("x")), None);
        let e = s.read(7, &mut [0u8; 4]).unwrap_err();
        assert_eq!(error_path(&e), None, "out of range is not a file error");
    }
}
