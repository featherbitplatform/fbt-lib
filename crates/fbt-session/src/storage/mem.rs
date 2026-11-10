use super::Storage;
use std::io;
use std::sync::Mutex;

/// In-memory storage, for tests.
#[derive(Debug)]
pub struct MemStorage {
    data: Mutex<Vec<u8>>,
}

impl MemStorage {
    /// Zero-filled storage of `total_len` bytes.
    pub fn new(total_len: u64) -> Self {
        let len = usize::try_from(total_len).unwrap_or(usize::MAX);
        Self::from_bytes(vec![0; len])
    }

    /// Storage holding `data`.
    pub fn from_bytes(data: Vec<u8>) -> Self {
        Self {
            data: Mutex::new(data),
        }
    }

    fn range(len: usize, offset: u64, n: usize) -> Option<std::ops::Range<usize>> {
        let start = usize::try_from(offset).ok()?;
        let end = start.checked_add(n)?;
        (end <= len).then_some(start..end)
    }
}

impl Storage for MemStorage {
    fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let d = self.data.lock().unwrap_or_else(|e| e.into_inner());
        let r = Self::range(d.len(), offset, buf.len())
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
        buf.copy_from_slice(d.get(r).ok_or(io::ErrorKind::NotFound)?);
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        let mut d = self.data.lock().unwrap_or_else(|e| e.into_inner());
        let r = Self::range(d.len(), offset, data.len())
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        d.get_mut(r)
            .ok_or(io::ErrorKind::InvalidInput)?
            .copy_from_slice(data);
        Ok(())
    }

    fn flush(&self) -> io::Result<()> {
        Ok(())
    }

    fn delete_files(&self) -> io::Result<()> {
        self.data.lock().unwrap_or_else(|e| e.into_inner()).fill(0);
        Ok(())
    }
}
