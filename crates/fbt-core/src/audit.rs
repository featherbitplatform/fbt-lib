//! A process-wide audit of outbound operations, for the tests that prove
//! nothing leaves the host except through the proxy.
//!
//! Every outbound operation is reported through [`record`] at the point it
//! reaches the OS: a TCP connect is its destination, a UDP send its
//! destination (a relayed datagram is the relay), and a host name handed to
//! the system resolver is the name. Without a live [`capture`] a record is
//! dropped after one uncontended read lock.
//!
//! The module is hidden from the public API on purpose: it is an internal
//! test seam, not a feature.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};

/// One outbound operation, as the OS sees it: the destination of a socket
/// operation, or a host name handed to the system resolver.
///
/// What is recorded is what leaves the host, not what the application meant:
/// a proxied dial is the proxy's address, a relayed datagram the relay's.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outbound {
    /// A TCP connection opened to `addr`.
    TcpConnect(SocketAddr),
    /// A UDP datagram sent to `addr`.
    UdpSend(SocketAddr),
    /// A host name handed to the system resolver, including the name of a
    /// connect made by host name and port.
    Lookup(String),
}

/// The buffer of a live capture.
type Buffer = Arc<Mutex<Vec<Outbound>>>;

/// The installed buffer: `None` while nothing captures, in which case a
/// record is dropped.
static RECORDER: RwLock<Option<Buffer>> = RwLock::new(None);

/// Process-wide capture exclusion: while one capture is live, every other
/// [`capture`] waits for it to be dropped, so the records of two tests in
/// one binary never mix.
static CAPTURING: Mutex<()> = Mutex::new(());

/// A live capture: records are appended to it until it is dropped, which
/// uninstalls it. A capture belongs to the thread that made it (it holds a
/// lock guard for its life), so it is not `Send`.
pub struct Capture {
    buffer: Buffer,
    /// Held for the capture's life; the second capture waits on it.
    _exclusive: MutexGuard<'static, ()>,
}

impl Capture {
    /// The records so far, in the order made; the capture keeps collecting
    /// until it is dropped.
    pub fn take(&self) -> Vec<Outbound> {
        let mut buffer = self.buffer.lock().unwrap_or_else(PoisonError::into_inner);
        std::mem::take(&mut *buffer)
    }
}

impl std::fmt::Debug for Capture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Capture").finish_non_exhaustive()
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        // Uninstall before the exclusion releases, so a waiting capture
        // never finds a stale recorder. `_exclusive` drops last.
        let mut recorder = RECORDER.write().unwrap_or_else(PoisonError::into_inner);
        *recorder = None;
        drop(recorder);
    }
}

/// Records `o`: while nothing captures this is a no-op (one uncontended
/// read lock); with a live capture the record lands in it.
pub fn record(o: Outbound) {
    if let Some(buffer) = RECORDER
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .as_ref()
    {
        buffer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(o);
    }
}

/// Installs a recorder for the life of the returned [`Capture`]. Captures
/// are exclusive process-wide: a second call waits until the first capture
/// is dropped.
pub fn capture() -> Capture {
    let exclusive = CAPTURING.lock().unwrap_or_else(PoisonError::into_inner);
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let mut recorder = RECORDER.write().unwrap_or_else(PoisonError::into_inner);
    *recorder = Some(Arc::clone(&buffer));
    drop(recorder);
    Capture {
        buffer,
        _exclusive: exclusive,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_sees_records_and_stops_on_drop() {
        let c = capture();
        record(Outbound::Lookup("a.invalid".into()));
        assert_eq!(c.take(), vec![Outbound::Lookup("a.invalid".into())]);
        drop(c);
        record(Outbound::Lookup("b.invalid".into())); // no recorder: nothing kept, no panic
        assert!(capture().take().is_empty());
    }
}
