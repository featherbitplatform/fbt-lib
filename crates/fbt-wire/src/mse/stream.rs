//! The post-handshake stream: RC4 or plaintext, with already-read bytes
//! served first.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use rc4::{KeyInit, Rc4, StreamCipher};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::MseError;

/// RC4 keystream bytes dropped after keying, per the MSE spec.
const RC4_DISCARD: usize = 1024;
/// Most plaintext bytes one `poll_write` accepts (and buffers encrypted).
const MSE_WRITE_CHUNK: usize = 16 * 1024;

/// RC4 keyed with a 20-byte MSE key, its first 1024 bytes discarded.
pub(crate) struct Cipher(Rc4);

impl Cipher {
    pub(crate) fn new(key: &[u8; 20]) -> Result<Cipher, MseError> {
        let mut rc4 = Rc4::new_from_slice(key).map_err(|_| MseError::BadLength("rc4 key"))?;
        rc4.apply_keystream(&mut [0u8; RC4_DISCARD]);
        Ok(Cipher(rc4))
    }

    pub(crate) fn apply(&mut self, buf: &mut [u8]) {
        self.0.apply_keystream(buf);
    }
}

impl std::fmt::Debug for Cipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Cipher(rc4)")
    }
}

/// A peer connection after the MSE handshake (or a plaintext one with some
/// bytes already read). Reads decrypt and writes encrypt when RC4 was
/// selected; otherwise bytes pass through.
#[derive(Debug)]
pub struct MseStream<S> {
    inner: S,
    /// Plaintext bytes to serve before reading from `inner`.
    prefix: Vec<u8>,
    prefix_pos: usize,
    /// Encrypts outgoing bytes; `None` for plaintext.
    enc: Option<Cipher>,
    /// Decrypts incoming bytes; `None` for plaintext.
    dec: Option<Cipher>,
    /// Encrypted bytes accepted by `poll_write` but not yet written.
    out: Vec<u8>,
    out_pos: usize,
    /// A transport error hit while draining `out`, not yet returned.
    pending_err: Option<io::Error>,
    /// The stored error was returned once; every later write-side call
    /// fails with `BrokenPipe`.
    broken: bool,
}

impl<S> MseStream<S> {
    /// A plaintext stream; `prefix` is served before the stream's own bytes.
    pub fn plaintext(stream: S, prefix: Vec<u8>) -> Self {
        Self::new(stream, prefix, None, None)
    }

    pub(crate) fn new(inner: S, prefix: Vec<u8>, enc: Option<Cipher>, dec: Option<Cipher>) -> Self {
        MseStream {
            inner,
            prefix,
            prefix_pos: 0,
            enc,
            dec,
            out: Vec::new(),
            out_pos: 0,
            pending_err: None,
            broken: false,
        }
    }

    /// True when the payload is RC4-encrypted.
    pub fn is_encrypted(&self) -> bool {
        self.enc.is_some()
    }

    /// The underlying transport.
    pub fn get_ref(&self) -> &S {
        &self.inner
    }
}

impl<S: AsyncWrite + Unpin> MseStream<S> {
    /// Writes out every buffered encrypted byte. A transport error is
    /// stored in `pending_err` (and the buffer dropped); the caller returns
    /// it through [`Self::check_err`].
    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        while let Some(pending) = self.out.get(self.out_pos..).filter(|p| !p.is_empty()) {
            let n = match ready!(Pin::new(&mut self.inner).poll_write(cx, pending)) {
                Ok(0) => {
                    self.fail(io::ErrorKind::WriteZero.into());
                    return Poll::Ready(());
                }
                Ok(n) => n,
                Err(e) => {
                    self.fail(e);
                    return Poll::Ready(());
                }
            };
            self.out_pos += n;
        }
        self.out.clear();
        self.out_pos = 0;
        Poll::Ready(())
    }

    fn fail(&mut self, e: io::Error) {
        self.out.clear();
        self.out_pos = 0;
        if self.pending_err.is_none() && !self.broken {
            self.pending_err = Some(e);
        }
    }

    /// The stored error once, then `BrokenPipe` forever.
    fn check_err(&mut self) -> io::Result<()> {
        if let Some(e) = self.pending_err.take() {
            self.broken = true;
            return Err(e);
        }
        if self.broken {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        Ok(())
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for MseStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Some(rest) = this.prefix.get(this.prefix_pos..).filter(|r| !r.is_empty()) {
            let n = rest.len().min(buf.remaining());
            if let Some(chunk) = rest.get(..n) {
                buf.put_slice(chunk);
            }
            this.prefix_pos += n;
            if this.prefix_pos >= this.prefix.len() {
                this.prefix = Vec::new();
                this.prefix_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }
        let before = buf.filled().len();
        ready!(Pin::new(&mut this.inner).poll_read(cx, buf))?;
        if let (Some(dec), Some(new)) = (this.dec.as_mut(), buf.filled_mut().get_mut(before..)) {
            dec.apply(new);
        }
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for MseStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.check_err()?;
        if this.enc.is_none() {
            return Pin::new(&mut this.inner).poll_write(cx, buf);
        }
        // Accept nothing new until the previous chunk is on the wire, so
        // every byte is encrypted exactly once.
        if this.out_pos < this.out.len() {
            ready!(this.poll_drain(cx));
            this.check_err()?;
        }
        let n = buf.len().min(MSE_WRITE_CHUNK);
        this.out.clear();
        this.out_pos = 0;
        this.out.extend_from_slice(buf.get(..n).unwrap_or_default());
        if let Some(enc) = this.enc.as_mut() {
            enc.apply(&mut this.out);
        }
        // Start writing now; whatever is left goes out on the next
        // write/flush. A drain error ends this call: nothing of it counts
        // as written. One that shows up later is returned by that call.
        if this.poll_drain(cx).is_ready() {
            this.check_err()?;
        }
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.check_err()?;
        ready!(this.poll_drain(cx));
        this.check_err()?;
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.check_err()?;
        ready!(this.poll_drain(cx));
        this.check_err()?;
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn rc4_discards_first_1024_bytes() {
        let key = [0x5a; 20];
        let mut ours = Cipher::new(&key).unwrap();
        let mut got = [0u8; 16];
        ours.apply(&mut got);
        let mut raw = Rc4::new_from_slice(&key).unwrap();
        let mut stream = [0u8; 1040];
        raw.apply_keystream(&mut stream);
        assert_eq!(&got[..], &stream[1024..1040]);
    }

    /// Accepts one byte per `poll_write` and returns `Pending` every other
    /// call, the worst case for partial writes.
    struct Trickle {
        got: Vec<u8>,
        stall: bool,
    }

    impl AsyncWrite for Trickle {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.stall = !self.stall;
            if self.stall {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            self.got.push(buf[0]);
            Poll::Ready(Ok(1))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn partial_writes_never_double_encrypt() {
        let key = [0x17; 20];
        let w = Trickle {
            got: Vec::new(),
            stall: false,
        };
        let mut s = MseStream::new(w, Vec::new(), Some(Cipher::new(&key).unwrap()), None);
        let msg: Vec<u8> = (0..300u32).map(|i| i as u8).collect();
        s.write_all(&msg[..100]).await.unwrap();
        s.write_all(&msg[100..]).await.unwrap();
        s.flush().await.unwrap();
        let mut wire = s.inner.got.clone();
        assert_eq!(wire.len(), msg.len());
        Cipher::new(&key).unwrap().apply(&mut wire);
        assert_eq!(wire, msg);
    }

    /// Accepts `ok` bytes in total, then (optionally after one `Pending`)
    /// fails with `ConnectionReset`.
    struct Failing {
        ok: usize,
        pend_first: bool,
        pended: bool,
    }

    impl AsyncWrite for Failing {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.pend_first && !self.pended {
                self.pended = true;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            if self.ok == 0 {
                return Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()));
            }
            let n = buf.len().min(self.ok);
            self.ok -= n;
            Poll::Ready(Ok(n))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn rc4_stream(w: Failing) -> MseStream<Failing> {
        MseStream::new(w, Vec::new(), Some(Cipher::new(&[3; 20]).unwrap()), None)
    }

    #[tokio::test]
    async fn drain_error_resurfaces_once_then_broken_pipe() {
        // The drain inside the write itself fails: that write reports it.
        let mut s = rc4_stream(Failing {
            ok: 3,
            pend_first: false,
            pended: false,
        });
        let e = s.write_all(b"hello").await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::ConnectionReset);
        let e = s.write(b"x").await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::BrokenPipe);

        // The drain is Pending at write time and fails later: the next
        // flush returns the error once, then BrokenPipe.
        let mut s = rc4_stream(Failing {
            ok: 0,
            pend_first: true,
            pended: false,
        });
        assert_eq!(s.write(b"hi").await.unwrap(), 2);
        let e = s.flush().await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::ConnectionReset);
        let e = s.flush().await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::BrokenPipe);
        let e = s.shutdown().await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::BrokenPipe);
    }
}
