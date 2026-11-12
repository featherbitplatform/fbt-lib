//! An established uTP connection as a tokio byte stream.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Sleep;

use crate::UtpError;
use crate::conn::ConnState;
use crate::socket::{ConnSlot, Key, Shared, lock};

/// The `io::Error` a stream returns for a failed connection.
fn io_error(e: UtpError) -> io::Error {
    let kind = match e {
        UtpError::Reset => io::ErrorKind::ConnectionReset,
        UtpError::TimedOut | UtpError::ConnectTimeout => io::ErrorKind::TimedOut,
        UtpError::Closed => io::ErrorKind::ConnectionAborted,
        UtpError::TooManyConnections | UtpError::Io(_) => io::ErrorKind::Other,
    };
    io::Error::new(kind, e)
}

fn store(slot: &mut Option<Waker>, w: &Waker) {
    if !slot.as_ref().is_some_and(|old| old.will_wake(w)) {
        *slot = Some(w.clone());
    }
}

/// A reliable, ordered uTP byte stream. Dropping it sends FIN if the
/// connection is still up (and resets it if unread data is left or arrives
/// later). `poll_shutdown` sends FIN after the queued data and waits for
/// its ack; the whole wait, draining included, is bounded by `idle_timeout`.
pub struct UtpStream {
    shared: Arc<Shared>,
    key: Key,
    slot: Arc<Mutex<ConnSlot>>,
    peer: SocketAddr,
    /// Set once `poll_shutdown` queued the FIN: bounds the wait for its ack.
    shutdown_deadline: Option<Pin<Box<Sleep>>>,
}

impl std::fmt::Debug for UtpStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("UtpStream");
        d.field("peer", &self.peer).field("recv_id", &self.key.1);
        // try_lock: formatting must never block or deadlock.
        if let Ok(s) = self.slot.try_lock() {
            d.field("send_id", &s.conn.send_id())
                .field("state", &s.conn.state())
                .field("error", &s.error());
        }
        d.finish_non_exhaustive()
    }
}

impl UtpStream {
    pub(crate) fn new(
        shared: Arc<Shared>,
        key: Key,
        slot: Arc<Mutex<ConnSlot>>,
        peer: SocketAddr,
    ) -> UtpStream {
        UtpStream {
            shared,
            key,
            slot,
            peer,
            shutdown_deadline: None,
        }
    }

    /// The remote address.
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer
    }

    /// Settles the slot under `s` and sends after releasing it.
    fn flush_slot(&self, mut s: std::sync::MutexGuard<'_, ConnSlot>, now: Instant) {
        let st = self.shared.settle(&mut s, now, None);
        let peer = s.peer;
        drop(s);
        self.shared.finish(self.key, &self.slot, peer, st);
    }
}

impl AsyncRead for UtpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let mut s = lock(&this.slot);
        if let Some(e) = s.error() {
            return Poll::Ready(Err(io_error(e)));
        }
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        // Initialise only what this read can fill, not the whole buffer.
        let readable = s.conn.readable_bytes();
        let n = if readable == 0 {
            0
        } else {
            s.conn
                .read(buf.initialize_unfilled_to(buf.remaining().min(readable)))
        };
        if n > 0 {
            buf.advance(n);
            // Reading may reopen our window: let the peer know.
            this.flush_slot(s, Instant::now());
            return Poll::Ready(Ok(()));
        }
        if s.conn.is_eof() || s.conn.state() == ConnState::Closed {
            return Poll::Ready(Ok(()));
        }
        store(&mut s.read_waker, cx.waker());
        Poll::Pending
    }
}

impl AsyncWrite for UtpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let mut s = lock(&this.slot);
        if let Some(e) = s.error() {
            return Poll::Ready(Err(io_error(e)));
        }
        if this.shutdown_deadline.is_some() || s.conn.state() != ConnState::Connected {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let n = s.conn.write(buf);
        if n == 0 {
            store(&mut s.write_waker, cx.waker());
            return Poll::Pending;
        }
        this.flush_slot(s, Instant::now());
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Written bytes are already queued on the connection, which sends
        // them as the windows allow.
        match lock(&self.slot).error() {
            Some(e) => Poll::Ready(Err(io_error(e))),
            None => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let mut s = lock(&this.slot);
        if let Some(e) = s.error() {
            return Poll::Ready(Err(io_error(e)));
        }
        if s.conn.is_fin_acked() || s.conn.state() == ConnState::Closed {
            return Poll::Ready(Ok(()));
        }
        store(&mut s.write_waker, cx.waker());
        if this.shutdown_deadline.is_none() {
            s.conn.close();
            this.shutdown_deadline =
                Some(Box::pin(tokio::time::sleep(this.shared.cfg.idle_timeout)));
            this.flush_slot(s, Instant::now());
        } else {
            drop(s);
        }
        if let Some(d) = this.shutdown_deadline.as_mut()
            && d.as_mut().poll(cx).is_ready()
        {
            return Poll::Ready(Err(io::ErrorKind::TimedOut.into()));
        }
        Poll::Pending
    }
}

impl Drop for UtpStream {
    fn drop(&mut self) {
        let now = Instant::now();
        let mut s = lock(&self.slot);
        s.read_waker = None;
        s.write_waker = None;
        s.stream_dropped = true;
        if s.error().is_none()
            && s.conn.state() != ConnState::Closed
            && s.conn.readable_bytes() == 0
        {
            // FIN after the queued data; the slot lingers to deliver it.
            // Unread data left behind resets the connection instead
            // (`settle`).
            s.conn.close();
            s.reap_by(now.checked_add(self.shared.cfg.idle_timeout));
        }
        // Sending never blocks (DatagramSender::send_to is non-blocking).
        self.flush_slot(s, now);
    }
}
