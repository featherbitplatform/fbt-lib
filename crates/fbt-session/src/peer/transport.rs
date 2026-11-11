//! The transports a peer connection runs over: TCP or uTP (BEP 29).

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use fbt_utp::UtpStream;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// The transport of a peer connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Transport {
    /// TCP.
    Tcp,
    /// uTP (BEP 29) over the listen address's shared UDP socket.
    Utp,
}

impl Transport {
    /// The other transport (the fallback after a failed connect).
    pub(crate) fn other(self) -> Transport {
        match self {
            Transport::Tcp => Transport::Utp,
            Transport::Utp => Transport::Tcp,
        }
    }
}

/// A connected peer transport; reads and writes go to the stream inside.
#[derive(Debug)]
pub(crate) enum PeerTransport {
    Tcp(TcpStream),
    Utp(UtpStream),
}

impl PeerTransport {
    pub(crate) fn kind(&self) -> Transport {
        match self {
            PeerTransport::Tcp(_) => Transport::Tcp,
            PeerTransport::Utp(_) => Transport::Utp,
        }
    }
}

impl AsyncRead for PeerTransport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PeerTransport::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            PeerTransport::Utp(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for PeerTransport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            PeerTransport::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            PeerTransport::Utp(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            PeerTransport::Tcp(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            PeerTransport::Utp(s) => Pin::new(s).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            PeerTransport::Tcp(s) => s.is_write_vectored(),
            PeerTransport::Utp(s) => s.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PeerTransport::Tcp(s) => Pin::new(s).poll_flush(cx),
            PeerTransport::Utp(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PeerTransport::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            PeerTransport::Utp(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}
