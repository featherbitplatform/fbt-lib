use super::Storage;
use fbt_core::FileLayout;
use fbt_metainfo::Metainfo;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

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

#[derive(Debug)]
struct Entry {
    path: PathBuf,
    len: u64,
    pad: bool,
}

#[derive(Debug)]
struct Handle {
    file: File,
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
    /// Files synced to disk, recorded for the flush-coverage test.
    #[cfg(test)]
    synced: HashSet<usize>,
}

/// Filesystem storage: each torrent file lives at `save_path/<entry path>`.
#[derive(Debug)]
pub struct FsStorage {
    save_path: PathBuf,
    entries: Vec<Entry>,
    layout: FileLayout,
    state: Mutex<State>,
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

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn create_empty_locked(&self, st: &mut State) -> io::Result<()> {
        if st.empties_created {
            return Ok(());
        }
        for e in self.entries.iter().filter(|e| e.len == 0 && !e.pad) {
            create_parent(&e.path)?;
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&e.path)?;
        }
        st.empties_created = true;
        Ok(())
    }

    /// Runs `f` on the (cached) handle of file `idx`.
    fn with_handle<R>(
        &self,
        st: &mut State,
        idx: usize,
        write: bool,
        f: impl FnOnce(&File) -> io::Result<R>,
    ) -> io::Result<R> {
        let entry = self
            .entries
            .get(idx)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        st.tick += 1;
        let tick = st.tick;
        let usable = st.open.get(&idx).is_some_and(|h| h.writable || !write);
        if !usable {
            st.open.remove(&idx);
            let file = if write {
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
                file
            } else {
                File::open(&entry.path)?
            };
            if st.open.len() >= MAX_OPEN {
                let lru = st
                    .open
                    .iter()
                    .min_by_key(|(_, h)| h.last_used)
                    .map(|(k, _)| *k);
                if let Some(k) = lru {
                    // Sync before dropping so `flush` covers evicted files too.
                    if let Some(h) = st.open.get(&k)
                        && h.writable
                    {
                        h.file.sync_data()?;
                        #[cfg(test)]
                        st.synced.insert(k);
                    }
                    st.open.remove(&k);
                }
            }
            st.open.insert(
                idx,
                Handle {
                    file,
                    writable: write,
                    last_used: tick,
                },
            );
        }
        let h = st
            .open
            .get_mut(&idx)
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
        h.last_used = tick;
        f(&h.file)
    }

    fn check_range(&self, offset: u64, len: usize) -> io::Result<()> {
        match offset.checked_add(len as u64) {
            Some(end) if end <= self.layout.total_len() => Ok(()),
            _ => Err(io::ErrorKind::InvalidInput.into()),
        }
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
        let mut st = self.lock();
        let mut pos = 0usize;
        for s in self.layout.map(offset, buf.len() as u64) {
            let end = pos + s.len as usize;
            let chunk = buf
                .get_mut(pos..end)
                .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
            pos = end;
            let pad = self.entries.get(s.file).is_some_and(|e| e.pad);
            if pad {
                chunk.fill(0);
                continue;
            }
            self.with_handle(&mut st, s.file, false, |f| {
                read_exact_at(f, chunk, s.file_offset).map_err(|e| {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        io::ErrorKind::NotFound.into()
                    } else {
                        e
                    }
                })
            })?;
        }
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.check_range(offset, data.len())?;
        let mut st = self.lock();
        self.create_empty_locked(&mut st)?;
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
            let pad = self.entries.get(s.file).is_some_and(|e| e.pad);
            if pad && chunk.iter().any(|&b| b != 0) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "non-zero data written to a padding file",
                ));
            }
        }
        for (s, chunk) in slices.iter().zip(parts) {
            if self.entries.get(s.file).is_some_and(|e| e.pad) {
                continue;
            }
            self.with_handle(&mut st, s.file, true, |f| {
                write_all_at(f, chunk, s.file_offset)
            })?;
        }
        Ok(())
    }

    fn flush(&self) -> io::Result<()> {
        #[cfg_attr(not(test), allow(unused_mut))]
        let mut st = self.lock();
        for (_k, h) in st.open.iter().filter(|(_, h)| h.writable) {
            h.file.sync_data()?;
        }
        #[cfg(test)]
        {
            let keys: Vec<usize> = st
                .open
                .iter()
                .filter(|(_, h)| h.writable)
                .map(|(k, _)| *k)
                .collect();
            st.synced.extend(keys);
        }
        Ok(())
    }

    fn delete_files(&self) -> io::Result<()> {
        let mut st = self.lock();
        st.open.clear();
        st.sized.clear();
        st.empties_created = false;
        for e in self.entries.iter().filter(|e| !e.pad) {
            match std::fs::remove_file(&e.path) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
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
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }
}
