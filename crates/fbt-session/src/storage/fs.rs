use super::Storage;
use super::part::PartFile;
use fbt_core::{FileLayout, PieceLayout};
use fbt_metainfo::Metainfo;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Maximum number of simultaneously open file handles.
const MAX_OPEN: usize = 64;

#[cfg(unix)]
pub(super) fn read_exact_at(f: &File, buf: &mut [u8], off: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    f.read_exact_at(buf, off)
}

#[cfg(unix)]
pub(super) fn write_all_at(f: &File, buf: &[u8], off: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    f.write_all_at(buf, off)
}

#[cfg(windows)]
pub(super) fn read_exact_at(f: &File, mut buf: &mut [u8], mut off: u64) -> io::Result<()> {
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
pub(super) fn write_all_at(f: &File, mut buf: &[u8], mut off: u64) -> io::Result<()> {
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
///
/// Its `Display` is `<path>: <source>`; [`source`](std::error::Error::source)
/// is `None`, so an error-chain printer shows the cause once.
#[derive(Debug)]
pub struct StorageError {
    /// The file the operation failed on.
    pub path: PathBuf,
    /// The underlying error.
    pub source: io::Error,
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.path.display(), self.source)
    }
}

impl std::error::Error for StorageError {}

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
    /// Per file, from the last `set_file_priorities`: wanted. Empty (every
    /// file wanted) until then. Zero-length skipped files are not created.
    wanted: Vec<bool>,
    /// Files synced to disk, recorded for the flush-coverage tests.
    #[cfg(test)]
    synced: HashSet<usize>,
}

/// Which files live in the part file, and the part file itself.
#[derive(Debug, Default)]
struct Routing {
    /// Per file: its bytes go to the part file (a skipped file that did not
    /// exist on disk when the priorities were set). Empty until the first
    /// `set_file_priorities`.
    routed: Vec<bool>,
    /// Opened by the first `set_file_priorities`.
    part: Option<Mutex<PartFile>>,
}

impl Routing {
    fn is_routed(&self, idx: usize) -> bool {
        self.routed.get(idx).copied().unwrap_or(false)
    }
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
///
/// Skipped files are never created (as libtorrent's part file): once
/// [`set_file_priorities`](Storage::set_file_priorities) has run, a skipped
/// file that does not exist on disk has its bytes (those of the pieces it
/// shares with wanted files, or that a reader streams) kept in
/// [`part_path`](Self::part_path) instead, piece by piece. A skipped file
/// that exists (a seed adding with files skipped, or a file skipped after
/// it was downloaded) is read and written in place. Making a file wanted
/// again moves its bytes out of the part file into the file, which is
/// created sparse where the platform supports it; the part file is deleted
/// once it holds nothing, and by [`delete_files`](Storage::delete_files).
#[derive(Debug)]
pub struct FsStorage {
    save_path: PathBuf,
    entries: Vec<Entry>,
    layout: FileLayout,
    pieces: PieceLayout,
    part_path: PathBuf,
    /// Taken shared by every read and write for its whole I/O, and
    /// exclusively by `set_file_priorities` and `delete_files`: a file
    /// never changes place under a read. Always taken before `state`.
    routing: RwLock<Routing>,
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
            pieces: metainfo.piece_layout(),
            part_path: save_path.join(format!(".{}.parts", metainfo.info_hashes)),
            routing: RwLock::new(Routing::default()),
            save_path,
            entries,
            state: Mutex::new(State::default()),
            #[cfg(test)]
            read_hook: std::sync::OnceLock::new(),
        }
    }

    /// The part file: `<save_path>/.<info hash>.parts` (the v1 hash in hex
    /// for v1 and hybrid torrents). It exists only while it holds data.
    pub fn part_path(&self) -> PathBuf {
        self.part_path.clone()
    }

    /// Creates every zero-length non-pad file that is not skipped (and its
    /// directories).
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

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn read_routing(&self) -> RwLockReadGuard<'_, Routing> {
        self.routing.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write_routing(&self) -> RwLockWriteGuard<'_, Routing> {
        self.routing.write().unwrap_or_else(|e| e.into_inner())
    }

    fn create_empty_locked(&self, st: &mut State) -> io::Result<()> {
        if st.empties_created {
            return Ok(());
        }
        let wanted = |i: usize| st.wanted.get(i).copied().unwrap_or(true);
        for e in self
            .entries
            .iter()
            .enumerate()
            .filter(|&(i, e)| e.len == 0 && !e.pad && wanted(i))
            .map(|(_, e)| e)
        {
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

    /// The piece holding torrent offset `at`, the offset in it, and how
    /// many of `len` bytes from `at` lie in that piece.
    fn piece_span(&self, at: u64, len: usize) -> io::Result<(u32, u32, usize)> {
        let plen = u64::from(self.pieces.piece_len());
        let piece = at
            .checked_div(plen)
            .and_then(|p| u32::try_from(p).ok())
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        let off = at % plen;
        let n = (plen - off).min(len as u64) as usize;
        Ok((piece, off as u32, n))
    }

    /// Fills `buf` from torrent offset `at` out of the part file.
    fn part_read(&self, r: &Routing, mut at: u64, buf: &mut [u8]) -> io::Result<()> {
        let part = r
            .part
            .as_ref()
            .ok_or_else(|| at_part(self, io::ErrorKind::NotFound.into()))?;
        let part = part.lock().unwrap_or_else(|e| e.into_inner());
        let mut rest = buf;
        while !rest.is_empty() {
            let (piece, off, n) = self.piece_span(at, rest.len())?;
            let (head, tail) = rest.split_at_mut(n);
            part.read(piece, off, head).map_err(|e| at_part(self, e))?;
            rest = tail;
            at += n as u64;
        }
        Ok(())
    }

    /// Writes `data` at torrent offset `at` into the part file.
    fn part_write(&self, r: &Routing, mut at: u64, data: &[u8]) -> io::Result<()> {
        let part = r
            .part
            .as_ref()
            .ok_or_else(|| at_part(self, io::ErrorKind::NotFound.into()))?;
        let mut part = part.lock().unwrap_or_else(|e| e.into_inner());
        let mut rest = data;
        while !rest.is_empty() {
            let (piece, off, n) = self.piece_span(at, rest.len())?;
            let (head, tail) = rest.split_at(n);
            part.write(piece, off, head).map_err(|e| at_part(self, e))?;
            rest = tail;
            at += n as u64;
        }
        Ok(())
    }

    /// Copies the bytes of file `idx` that `part` holds into the file.
    fn export(&self, idx: usize, part: &PartFile) -> io::Result<()> {
        let Some(range) = self.layout.file_range(idx).filter(|r| !r.is_empty()) else {
            return Ok(());
        };
        let plen = u64::from(self.pieces.piece_len());
        let (first, last) = (range.start / plen, (range.end - 1) / plen);
        for p in first..=last {
            let Ok(piece) = u32::try_from(p) else {
                break;
            };
            if !part.has(piece) {
                continue;
            }
            let lo = range.start.max(p * plen);
            let hi = range.end.min((p + 1) * plen);
            let mut buf = vec![0u8; (hi - lo) as usize];
            match part.read(piece, (lo - p * plen) as u32, &mut buf) {
                Ok(()) => {}
                // Never fully written: nothing of it to keep.
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(at(part.path(), e)),
            }
            // Created (sparse where the platform supports it) and sized
            // by the first export, as by a first write.
            let file = self.handle(idx, true)?;
            write_all_at(&file, &buf, lo - range.start).map_err(|e| at(self.path_of(idx), e))?;
        }
        Ok(())
    }

    /// Whether a file of `piece` lives in the part file.
    fn piece_routed(&self, routed: &[bool], piece: u32) -> bool {
        let off = self.pieces.piece_offset(piece);
        let len = u64::from(self.pieces.piece_size(piece));
        self.layout
            .files_overlapping(off, len)
            .any(|i| routed.get(i).copied().unwrap_or(false))
    }
}

/// `e` naming the part file.
fn at_part(s: &FsStorage, e: io::Error) -> io::Error {
    at(&s.part_path, e)
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
        let routing = self.read_routing();
        let mut pos = 0usize;
        for s in self.layout.map(offset, buf.len() as u64) {
            let end = pos + s.len as usize;
            let chunk = buf
                .get_mut(pos..end)
                .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
            let at_torrent = offset + pos as u64;
            pos = end;
            if self.is_pad(s.file) {
                chunk.fill(0);
                continue;
            }
            if routing.is_routed(s.file) {
                self.part_read(&routing, at_torrent, chunk)?;
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
        let routing = self.read_routing();
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
        let mut at_torrent = offset;
        for (s, chunk) in slices.iter().zip(parts) {
            let here = at_torrent;
            at_torrent += s.len;
            if self.is_pad(s.file) {
                continue;
            }
            if routing.is_routed(s.file) {
                self.part_write(&routing, here, chunk)?;
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
        #[cfg(test)]
        let mut synced = Vec::new();
        // An open handle that fails is tried again by the next flush.
        for (k, file) in open {
            match file.sync_data() {
                Ok(()) => {
                    #[cfg(test)]
                    synced.push(k);
                }
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
                Ok(()) => {
                    #[cfg(test)]
                    synced.push(k);
                }
                // A file gone from disk has nothing left to sync.
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                // Made read-only behind our back: retrying would fail
                // forever, and its next write reports the problem.
                Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
                    tracing::warn!(path = %path.display(), error = %e, "cannot reopen to sync");
                }
                Err(e) => {
                    retry.push(k);
                    first_err.get_or_insert_with(|| at(path, e));
                }
            }
        }
        if let Some(part) = self.read_routing().part.as_ref() {
            let part = part.lock().unwrap_or_else(|e| e.into_inner());
            if let Err(e) = part.sync() {
                first_err.get_or_insert_with(|| at_part(self, e));
            }
        }
        let mut st = self.lock();
        st.unsynced.extend(retry);
        #[cfg(test)]
        st.synced.extend(synced);
        first_err.map_or(Ok(()), Err)
    }

    fn delete_files(&self) -> io::Result<()> {
        let mut routing = self.write_routing();
        match routing.part.as_mut() {
            Some(part) => part
                .get_mut()
                .unwrap_or_else(|e| e.into_inner())
                .remove()
                .map_err(|e| at_part(self, e))?,
            // Never opened by this instance: one may be left from before.
            None => match std::fs::remove_file(&self.part_path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(at_part(self, e)),
            },
        }
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

    fn set_file_priorities(&self, wanted: &[bool]) -> io::Result<()> {
        let want = |i: usize| wanted.get(i).copied().unwrap_or(true);
        let mut guard = self.write_routing();
        let Routing { routed, part } = &mut *guard;
        let part = match part {
            Some(p) => p,
            None => {
                let pf = PartFile::open(
                    self.part_path.clone(),
                    self.pieces.num_pieces(),
                    self.pieces.piece_len(),
                )
                .map_err(|e| at_part(self, e))?;
                part.insert(Mutex::new(pf))
            }
        };
        let part = part.get_mut().unwrap_or_else(|e| e.into_inner());
        routed.resize(self.entries.len(), false);
        {
            let mut st = self.lock();
            st.wanted = (0..self.entries.len()).map(want).collect();
            // Zero-length files wanted now are created at once if the
            // others were already (by a write), else with them.
            let created = std::mem::take(&mut st.empties_created);
            if created {
                self.create_empty_locked(&mut st)?;
            }
        }
        for (i, (e, r)) in self.entries.iter().zip(routed.iter_mut()).enumerate() {
            if e.pad || e.len == 0 {
                *r = false;
            } else if *r && want(i) {
                self.export(i, part)?;
                *r = false;
            } else if !*r && !want(i) && matches!(e.path.try_exists(), Ok(false)) {
                *r = true;
                let mut st = self.lock();
                st.open.remove(&i);
                st.sized.remove(&i);
            }
        }
        // Slots of pieces whose files all left the part file.
        for piece in 0..self.pieces.num_pieces() {
            if part.has(piece) && !self.piece_routed(routed, piece) {
                part.free(piece).map_err(|e| at_part(self, e))?;
            }
        }
        if part.is_empty() {
            part.remove().map_err(|e| at_part(self, e))?;
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
    fn flush_skips_evicted_files_gone_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let s = FsStorage::new(dir.path().to_path_buf(), &many_files(MAX_OPEN + 36));
        for i in 0..(MAX_OPEN + 36) as u64 {
            s.write(i, &[1]).unwrap();
        }
        // `f0` was the least recently used: evicted, waiting for its sync.
        assert!(s.lock().unsynced.contains(&0));
        std::fs::remove_file(dir.path().join("t").join("f0")).unwrap();
        s.flush().unwrap();
        assert_eq!(s.synced_files(), MAX_OPEN + 35);
        assert!(s.lock().unsynced.is_empty(), "a vanished file is retried");
        s.flush().unwrap();
        assert_eq!(s.synced_files(), MAX_OPEN + 35);
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
