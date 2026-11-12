use super::part::PartFile;
use super::{MoveMode, Storage};
use fbt_core::{FileLayout, PieceLayout};
use fbt_metainfo::{Metainfo, RelPath};
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
    len: u64,
    pad: bool,
}

/// Where the files live. Changed only by `move_to` and `rename_file`,
/// which hold the routing lock exclusively: never under a read or write.
#[derive(Debug)]
struct Paths {
    save_path: PathBuf,
    /// Per file, its path relative to `save_path`.
    rel: Vec<RelPath>,
    /// The part file's name inside `save_path`.
    part_name: String,
}

impl Paths {
    /// File `idx`'s path (the save path for an index out of range).
    fn file(&self, idx: usize) -> PathBuf {
        self.rel.get(idx).map_or_else(
            || self.save_path.clone(),
            |r| self.save_path.join(r.to_path_buf()),
        )
    }

    fn part(&self) -> PathBuf {
        self.save_path.join(&self.part_name)
    }

    /// The same files below `save_path`.
    fn at(&self, save_path: &Path) -> Paths {
        Paths {
            save_path: save_path.to_path_buf(),
            rel: self.rel.clone(),
            part_name: self.part_name.clone(),
        }
    }
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

/// Where the files are, which of them live in the part file, and the part
/// file itself.
#[derive(Debug)]
struct Routing {
    paths: Paths,
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
///
/// [`move_to`](Storage::move_to) and [`rename_file`](Storage::rename_file)
/// wait for running reads and writes, close every cached handle, and move
/// with `fs::rename`; across filesystems (and on Windows after any rename
/// error) a file is copied, synced and only then deleted. The part file
/// moves with the files. A move that fails moves back what it moved, so
/// the data stays usable at the old path.
#[derive(Debug)]
pub struct FsStorage {
    entries: Vec<Entry>,
    layout: FileLayout,
    pieces: PieceLayout,
    /// Taken shared by every read and write for its whole I/O, and
    /// exclusively by `set_file_priorities`, `delete_files`, `move_to` and
    /// `rename_file`: a file never changes place under a read. Always
    /// taken before `state`.
    routing: RwLock<Routing>,
    state: Mutex<State>,
    #[cfg(test)]
    read_hook: std::sync::OnceLock<ReadHook>,
    /// Tests: moves always copy, as across filesystems.
    #[cfg(test)]
    force_copy: std::sync::atomic::AtomicBool,
}

impl FsStorage {
    /// Storage for `metainfo`'s files below `save_path`.
    pub fn new(save_path: PathBuf, metainfo: &Metainfo) -> FsStorage {
        Self::with_paths(save_path, metainfo, &[])
    }

    /// Storage for `metainfo`'s files below `save_path`, the files listed
    /// in `renamed` at their given paths (relative to `save_path`, as
    /// [`rename_file`](Storage::rename_file) set them). An index out of
    /// range or naming a pad file is ignored.
    pub fn with_paths(
        save_path: PathBuf,
        metainfo: &Metainfo,
        renamed: &[(usize, RelPath)],
    ) -> FsStorage {
        let files = &metainfo.info.files;
        let entries: Vec<Entry> = files
            .iter()
            .map(|f| Entry {
                len: f.len,
                pad: f.pad,
            })
            .collect();
        let mut rel: Vec<RelPath> = files.iter().map(|f| f.path.clone()).collect();
        for (i, p) in renamed {
            if let Some(r) = rel
                .get_mut(*i)
                .filter(|_| files.get(*i).is_some_and(|f| !f.pad))
            {
                *r = p.clone();
            }
        }
        let paths = Paths {
            save_path,
            rel,
            part_name: format!(".{}.parts", metainfo.info_hashes),
        };
        FsStorage {
            layout: FileLayout::new(entries.iter().map(|e| e.len)),
            pieces: metainfo.piece_layout(),
            routing: RwLock::new(Routing {
                paths,
                routed: Vec::new(),
                part: None,
            }),
            entries,
            state: Mutex::new(State::default()),
            #[cfg(test)]
            read_hook: std::sync::OnceLock::new(),
            #[cfg(test)]
            force_copy: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// The part file: `<save_path>/.<info hash>.parts` (the v1 hash in hex
    /// for v1 and hybrid torrents). It exists only while it holds data.
    pub fn part_path(&self) -> PathBuf {
        self.read_routing().paths.part()
    }

    /// The directory the files are below (changed by
    /// [`move_to`](Storage::move_to)).
    pub fn save_path(&self) -> PathBuf {
        self.read_routing().paths.save_path.clone()
    }

    /// Creates every zero-length non-pad file that is not skipped (and its
    /// directories).
    pub fn create_empty_files(&self) -> io::Result<()> {
        let routing = self.read_routing();
        let mut st = self.lock();
        self.create_empty_locked(&routing.paths, &mut st)
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

    #[cfg(test)]
    fn set_force_copy(&self, on: bool) {
        self.force_copy
            .store(on, std::sync::atomic::Ordering::Relaxed);
    }

    /// Whether moves skip `fs::rename` (tests of the copy fallback).
    fn force_copy(&self) -> bool {
        #[cfg(test)]
        {
            self.force_copy.load(std::sync::atomic::Ordering::Relaxed)
        }
        #[cfg(not(test))]
        {
            false
        }
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

    fn create_empty_locked(&self, paths: &Paths, st: &mut State) -> io::Result<()> {
        if st.empties_created {
            return Ok(());
        }
        let wanted = |i: usize| st.wanted.get(i).copied().unwrap_or(true);
        for (i, _) in self
            .entries
            .iter()
            .enumerate()
            .filter(|&(i, e)| e.len == 0 && !e.pad && wanted(i))
        {
            let path = paths.file(i);
            create_parent(&path).map_err(|err| at(&path, err))?;
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .map_err(|err| at(&path, err))?;
        }
        st.empties_created = true;
        Ok(())
    }

    /// The (cached) handle of file `idx`, opened for writing if `write`.
    /// Only the lookup and the opening hold the lock; the caller does its
    /// I/O on the returned handle after it is released.
    fn handle(&self, paths: &Paths, idx: usize, write: bool) -> io::Result<Arc<File>> {
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
        let path = paths.file(idx);
        let file = if write {
            Self::open_writable(&mut st, idx, entry, &path).map_err(|e| at(&path, e))?
        } else {
            File::open(&path).map_err(|e| at(&path, e))?
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

    fn open_writable(st: &mut State, idx: usize, entry: &Entry, path: &Path) -> io::Result<File> {
        create_parent(path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
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
            .ok_or_else(|| at_part(r, io::ErrorKind::NotFound.into()))?;
        let part = part.lock().unwrap_or_else(|e| e.into_inner());
        let mut rest = buf;
        while !rest.is_empty() {
            let (piece, off, n) = self.piece_span(at, rest.len())?;
            let (head, tail) = rest.split_at_mut(n);
            part.read(piece, off, head).map_err(|e| at_part(r, e))?;
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
            .ok_or_else(|| at_part(r, io::ErrorKind::NotFound.into()))?;
        let mut part = part.lock().unwrap_or_else(|e| e.into_inner());
        let mut rest = data;
        while !rest.is_empty() {
            let (piece, off, n) = self.piece_span(at, rest.len())?;
            let (head, tail) = rest.split_at(n);
            part.write(piece, off, head).map_err(|e| at_part(r, e))?;
            rest = tail;
            at += n as u64;
        }
        Ok(())
    }

    /// Copies the bytes of file `idx` that `part` holds into the file.
    fn export(&self, paths: &Paths, idx: usize, part: &PartFile) -> io::Result<()> {
        let Some(range) = self.layout.file_range(idx).filter(|r| !r.is_empty()) else {
            return Ok(());
        };
        let plen = u64::from(self.pieces.piece_len());
        let (first, last) = (range.start / plen, (range.end - 1) / plen);
        let mut exported = None;
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
            let file = self.handle(paths, idx, true)?;
            write_all_at(&file, &buf, lo - range.start).map_err(|e| at(&paths.file(idx), e))?;
            exported = Some(file);
        }
        // On disk before the caller frees the slots it came from.
        if let Some(file) = exported {
            file.sync_data().map_err(|e| at(&paths.file(idx), e))?;
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

    /// The directories strictly below `paths.save_path` holding the
    /// torrent's files, deepest first.
    fn torrent_dirs(&self, paths: &Paths) -> Vec<PathBuf> {
        let mut dirs: HashSet<PathBuf> = HashSet::new();
        for (i, _) in self.entries.iter().enumerate().filter(|(_, e)| !e.pad) {
            dirs.extend(dirs_below(&paths.file(i), &paths.save_path));
        }
        let mut dirs: Vec<PathBuf> = dirs.into_iter().collect();
        dirs.sort_by_key(|d| std::cmp::Reverse(d.components().count()));
        dirs
    }

    /// Opens the part file at `paths`' location again after a move, or
    /// leaves none (reopened by the next `set_file_priorities`) if that
    /// fails.
    fn reopen_part(&self, paths: &Paths) -> Option<Mutex<PartFile>> {
        match PartFile::open(
            paths.part(),
            self.pieces.num_pieces(),
            self.pieces.piece_len(),
        ) {
            Ok(p) => Some(Mutex::new(p)),
            Err(e) => {
                tracing::warn!(path = %paths.part().display(), error = %e, "cannot reopen the part file");
                None
            }
        }
    }
}

/// `e` naming the part file.
fn at_part(r: &Routing, e: io::Error) -> io::Error {
    at(&r.paths.part(), e)
}

fn create_parent(path: &Path) -> io::Result<()> {
    match path.parent() {
        Some(p) => std::fs::create_dir_all(p),
        None => Ok(()),
    }
}

/// The ancestors of `path` strictly below `root`, deepest first.
fn dirs_below<'a>(path: &'a Path, root: &'a Path) -> impl Iterator<Item = PathBuf> + 'a {
    path.ancestors()
        .skip(1)
        .take_while(move |a| *a != root && a.starts_with(root))
        .map(Path::to_path_buf)
}

/// Removes each of `dirs` (deepest first) that is empty; best effort.
fn remove_empty_dirs(dirs: impl IntoIterator<Item = PathBuf>) {
    for d in dirs {
        // Not empty, gone, or not ours to remove: left alone.
        let _ = std::fs::remove_dir(&d);
    }
}

/// Whether `a` and `b` name the same directory.
fn same_dir(a: &Path, b: &Path) -> bool {
    if a == b {
        return true;
    }
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// Whether something exists at `path`; an error that hides the answer is
/// returned naming it.
fn exists(path: &Path) -> io::Result<bool> {
    path.try_exists().map_err(|e| at(path, e))
}

/// Moves file `from` to `to` (whose directory exists): `fs::rename`, else
/// (across filesystems, and on Windows after any rename error, as a target
/// on another volume reports several kinds there) a copy synced to disk
/// before `from` is deleted. `copy` skips the rename. On failure `from` is
/// intact; the error names the file it is about.
fn move_file(from: &Path, to: &Path, copy: bool) -> io::Result<()> {
    if !copy {
        match std::fs::rename(from, to) {
            Ok(()) => return Ok(()),
            Err(e) if cfg!(windows) || e.kind() == io::ErrorKind::CrossesDevices => {}
            Err(e) => return Err(at(to, e)),
        }
    }
    copy_then_delete(from, to)
}

fn copy_then_delete(from: &Path, to: &Path) -> io::Result<()> {
    let mut src = File::open(from).map_err(|e| at(from, e))?;
    let mut dst = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(to)
        .map_err(|e| at(to, e))?;
    let copied = io::copy(&mut src, &mut dst).and_then(|_| dst.sync_all());
    drop(dst);
    drop(src);
    if let Err(e) = copied {
        // Half a copy is no use to anyone; the source is intact.
        let _ = std::fs::remove_file(to);
        return Err(at(to, e));
    }
    if let Err(e) = std::fs::remove_file(from) {
        // Both would exist: the move did not happen.
        let _ = std::fs::remove_file(to);
        return Err(at(from, e));
    }
    Ok(())
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
            let file = self.handle(&routing.paths, s.file, false)?;
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
                at(&routing.paths.file(s.file), e)
            })?;
        }
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.check_range(offset, data.len())?;
        let routing = self.read_routing();
        {
            let mut st = self.lock();
            self.create_empty_locked(&routing.paths, &mut st)?;
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
            let file = self.handle(&routing.paths, s.file, true)?;
            write_all_at(&file, chunk, s.file_offset)
                .map_err(|e| at(&routing.paths.file(s.file), e))?;
        }
        Ok(())
    }

    fn flush(&self) -> io::Result<()> {
        // Held throughout: no move changes the paths under the flush.
        let routing = self.read_routing();
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
                    first_err.get_or_insert_with(|| at(&routing.paths.file(k), e));
                }
            }
        }
        let mut retry = Vec::new();
        for k in reopen {
            let path = routing.paths.file(k);
            let r = OpenOptions::new()
                .write(true)
                .open(&path)
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
                    first_err.get_or_insert_with(|| at(&path, e));
                }
            }
        }
        if let Some(part) = routing.part.as_ref() {
            let part = part.lock().unwrap_or_else(|e| e.into_inner());
            if let Err(e) = part.sync() {
                first_err.get_or_insert_with(|| at(part.path(), e));
            }
        }
        let mut st = self.lock();
        st.unsynced.extend(retry);
        #[cfg(test)]
        st.synced.extend(synced);
        first_err.map_or(Ok(()), Err)
    }

    fn delete_files(&self) -> io::Result<()> {
        let mut guard = self.write_routing();
        let Routing { paths, part, .. } = &mut *guard;
        match part.as_mut() {
            Some(part) => {
                let part = part.get_mut().unwrap_or_else(|e| e.into_inner());
                part.remove().map_err(|e| at(part.path(), e))?;
            }
            // Never opened by this instance: one may be left from before.
            None => match std::fs::remove_file(paths.part()) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(at(&paths.part(), e)),
            },
        }
        let mut st = self.lock();
        st.open.clear();
        st.sized.clear();
        st.unsynced.clear();
        st.empties_created = false;
        for (i, _) in self.entries.iter().enumerate().filter(|(_, e)| !e.pad) {
            let path = paths.file(i);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(at(&path, err)),
            }
        }
        // Ancestor directories strictly below save_path, deepest first.
        for d in self.torrent_dirs(paths) {
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
        let Routing {
            paths,
            routed,
            part,
        } = &mut *guard;
        let first = part.is_none();
        let part = match part {
            Some(p) => p,
            None => {
                let pf = PartFile::open(
                    paths.part(),
                    self.pieces.num_pieces(),
                    self.pieces.piece_len(),
                )
                .map_err(|e| at(&paths.part(), e))?;
                part.insert(Mutex::new(pf))
            }
        };
        let part = part.get_mut().unwrap_or_else(|e| e.into_inner());
        routed.resize(self.entries.len(), false);
        if first {
            // A part file left by an earlier instance (or one reopened by
            // a move) may hold the bytes of any file missing from disk,
            // whatever the priorities were then: every such file starts
            // routed, so the loop below exports the wanted ones instead of
            // freeing their slots.
            for (i, (e, r)) in self.entries.iter().zip(routed.iter_mut()).enumerate() {
                *r = !e.pad && e.len > 0 && matches!(paths.file(i).try_exists(), Ok(false));
            }
        }
        {
            let mut st = self.lock();
            st.wanted = (0..self.entries.len()).map(want).collect();
            // Zero-length files wanted now are created at once if the
            // others were already (by a write), else with them.
            let created = std::mem::take(&mut st.empties_created);
            if created {
                self.create_empty_locked(paths, &mut st)?;
            }
        }
        for (i, (e, r)) in self.entries.iter().zip(routed.iter_mut()).enumerate() {
            if e.pad || e.len == 0 {
                *r = false;
            } else if *r && want(i) {
                self.export(paths, i, part)?;
                *r = false;
            } else if !*r && !want(i) && matches!(paths.file(i).try_exists(), Ok(false)) {
                *r = true;
                let mut st = self.lock();
                st.open.remove(&i);
                st.sized.remove(&i);
            }
        }
        // Slots of pieces whose files all left the part file.
        for piece in 0..self.pieces.num_pieces() {
            if part.has(piece) && !self.piece_routed(routed, piece) {
                part.free(piece).map_err(|e| at(part.path(), e))?;
            }
        }
        if part.is_empty() {
            part.remove().map_err(|e| at(part.path(), e))?;
        }
        Ok(())
    }

    fn move_to(&self, save_path: &Path, mode: MoveMode) -> io::Result<()> {
        let mut guard = self.write_routing();
        let Routing { paths, part, .. } = &mut *guard;
        if same_dir(&paths.save_path, save_path) {
            return Ok(());
        }
        let target = paths.at(save_path);
        // Every non-pad file, then the part file: (here, there).
        let mut plan: Vec<(PathBuf, PathBuf)> = self
            .entries
            .iter()
            .enumerate()
            .filter(|(_, e)| !e.pad)
            .map(|(i, _)| (paths.file(i), target.file(i)))
            .collect();
        plan.push((paths.part(), target.part()));
        if mode == MoveMode::FailIfExists {
            for (_, to) in &plan {
                if exists(to)? {
                    return Err(at(to, io::ErrorKind::AlreadyExists.into()));
                }
            }
        }
        // No handle of the old place survives (Windows cannot delete an
        // open file); the part file is opened again where it ends up.
        self.lock().open.clear();
        let had_part = part.take().is_some();
        let copy = self.force_copy();
        let mut moved: Vec<(PathBuf, PathBuf)> = Vec::new();
        // Directories this move created, for a failure to take back.
        let mut created: Vec<PathBuf> = Vec::new();
        if !matches!(save_path.try_exists(), Ok(true)) {
            created.push(save_path.to_path_buf());
        }
        let mut failed = None;
        for (from, to) in plan {
            let step = (|| {
                if !exists(&from)? || (mode == MoveMode::KeepExisting && exists(&to)?) {
                    return Ok(false);
                }
                created.extend(
                    dirs_below(&to, save_path).filter(|d| matches!(d.try_exists(), Ok(false))),
                );
                create_parent(&to).map_err(|e| at(&to, e))?;
                move_file(&from, &to, copy).map(|()| true)
            })();
            match step {
                Ok(true) => moved.push((from, to)),
                Ok(false) => {}
                Err(e) => {
                    failed = Some(e);
                    break;
                }
            }
        }
        if let Some(e) = failed {
            for (from, to) in moved.iter().rev() {
                if let Err(back) = move_file(to, from, copy) {
                    tracing::warn!(from = %to.display(), to = %from.display(), error = %back, "cannot move a file back");
                }
            }
            // Directories the move created, deepest first.
            created.sort_by(|a, b| {
                b.components()
                    .count()
                    .cmp(&a.components().count())
                    .then_with(|| a.cmp(b))
            });
            created.dedup();
            remove_empty_dirs(created);
            if had_part {
                *part = self.reopen_part(paths);
            }
            return Err(e);
        }
        remove_empty_dirs(self.torrent_dirs(paths));
        *paths = target;
        if had_part {
            *part = self.reopen_part(paths);
        }
        Ok(())
    }

    fn rename_file(&self, index: usize, path: &RelPath) -> io::Result<()> {
        let mut guard = self.write_routing();
        let routed = guard.is_routed(index);
        let paths = &mut guard.paths;
        if self.entries.get(index).is_none_or(|e| e.pad) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let Some(old) = paths.rel.get(index).cloned() else {
            return Err(io::ErrorKind::InvalidInput.into());
        };
        if old == *path {
            return Ok(());
        }
        let from = paths.file(index);
        let to = paths.save_path.join(path.to_path_buf());
        self.lock().open.remove(&index);
        // A file in the part file, or not created yet, only changes name.
        if !routed && exists(&from)? {
            // The same file on a case-insensitive or normalising
            // filesystem when only the key matches.
            if old.key() != path.key() && exists(&to)? {
                return Err(at(&to, io::ErrorKind::AlreadyExists.into()));
            }
            // Directories created for it, deepest first, taken back on failure.
            let created: Vec<PathBuf> = dirs_below(&to, &paths.save_path)
                .filter(|d| matches!(d.try_exists(), Ok(false)))
                .collect();
            if let Err(e) = create_parent(&to)
                .map_err(|e| at(&to, e))
                .and_then(|()| move_file(&from, &to, self.force_copy()))
            {
                remove_empty_dirs(created);
                return Err(e);
            }
            remove_empty_dirs(dirs_below(&from, &paths.save_path));
        }
        if let Some(r) = paths.rel.get_mut(index) {
            *r = path.clone();
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

    /// Across filesystems a move copies, syncs and deletes: the part
    /// file goes with the files, and every byte reads back from the new
    /// place.
    #[test]
    fn copy_fallback_moves_the_part_file() {
        let old = tempfile::tempdir().unwrap();
        let new = tempfile::tempdir().unwrap();
        let m = torrent("t", &[(&["d", "a"], 10, false), (&["b"], 10, false)], 8);
        let s = FsStorage::new(old.path().to_path_buf(), &m);
        // `a` is skipped and missing: its bytes live in the part file.
        s.set_file_priorities(&[false, true]).unwrap();
        let data: Vec<u8> = (1..=20).collect();
        s.write(0, &data).unwrap();
        let old_part = s.part_path();
        assert!(old_part.exists());
        assert!(!old.path().join("t/d/a").exists());
        s.set_force_copy(true);
        s.move_to(new.path(), MoveMode::FailIfExists).unwrap();
        assert!(!old_part.exists(), "the part file stayed");
        assert!(!old.path().join("t").exists(), "the old directory stayed");
        assert_eq!(
            s.part_path(),
            new.path().join(old_part.file_name().unwrap())
        );
        assert!(s.part_path().exists(), "the part file did not move");
        assert_eq!(std::fs::read(new.path().join("t/b")).unwrap(), &data[10..]);
        assert!(
            !new.path().join("t/d/a").exists(),
            "a skipped file was created"
        );
        let mut out = [0u8; 20];
        s.read(0, &mut out).unwrap();
        assert_eq!(out.as_slice(), data.as_slice());
        // Still writable at the new place, the part file included.
        s.write(0, &[9; 20]).unwrap();
        s.read(0, &mut out).unwrap();
        assert_eq!(out, [9; 20]);
        assert_eq!(s.save_path(), new.path());
    }

    /// A move that cannot finish puts back what it moved.
    #[test]
    fn failed_copy_moves_back() {
        let old = tempfile::tempdir().unwrap();
        let new = tempfile::tempdir().unwrap();
        let m = torrent("t", &[(&["a"], 4, false), (&["b"], 4, false)], 4);
        let s = FsStorage::new(old.path().to_path_buf(), &m);
        s.write(0, &[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
        // `b`'s target is a directory: neither a rename nor a copy lands.
        std::fs::create_dir_all(new.path().join("t/b/x")).unwrap();
        s.set_force_copy(true);
        let e = s
            .move_to(new.path(), MoveMode::ReplaceExisting)
            .unwrap_err();
        assert!(error_path(&e).is_some(), "{e}");
        assert_eq!(std::fs::read(old.path().join("t/a")).unwrap(), [1, 2, 3, 4]);
        assert_eq!(std::fs::read(old.path().join("t/b")).unwrap(), [5, 6, 7, 8]);
        assert!(!new.path().join("t/a").exists());
        assert_eq!(s.save_path(), old.path());
        let mut out = [0u8; 8];
        s.read(0, &mut out).unwrap();
        assert_eq!(out, [1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn rename_moves_the_file_and_keeps_serving() {
        let dir = tempfile::tempdir().unwrap();
        let m = torrent("t", &[(&["a"], 4, false), (&["b"], 4, false)], 4);
        let s = FsStorage::new(dir.path().to_path_buf(), &m);
        s.write(0, &[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
        let to = fbt_metainfo::sanitize_path([&b"x"[..], b"y", b"b.bin"]).unwrap();
        s.rename_file(1, &to).unwrap();
        assert!(!dir.path().join("t/b").exists());
        assert_eq!(
            std::fs::read(dir.path().join("x/y/b.bin")).unwrap(),
            [5, 6, 7, 8]
        );
        let mut out = [0u8; 8];
        s.read(0, &mut out).unwrap();
        assert_eq!(out, [1, 2, 3, 4, 5, 6, 7, 8]);
        // A foreign file at the target is never overwritten.
        std::fs::write(dir.path().join("c"), b"theirs").unwrap();
        let c = fbt_metainfo::sanitize_path([&b"c"[..]]).unwrap();
        let e = s.rename_file(0, &c).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(dir.path().join("c")).unwrap(), b"theirs");
        // The renamed path survives a new instance through `with_paths`.
        let again = FsStorage::with_paths(dir.path().to_path_buf(), &m, &[(1, to)]);
        again.read(0, &mut out).unwrap();
        assert_eq!(out, [1, 2, 3, 4, 5, 6, 7, 8]);
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
