//! Datagram traits shared by uTP, the DHT and UDP trackers, so each can run
//! on one shared UDP socket (implemented by `fbt-net`).

use std::io;
use std::net::SocketAddr;

/// Sends datagrams from a shared socket.
///
/// It never blocks: `WouldBlock` means the datagram was dropped, as UDP may.
pub trait DatagramSender: Send + Sync + std::fmt::Debug + 'static {
    /// The local address of the socket.
    fn local_addr(&self) -> SocketAddr;
    /// Sends one datagram to `to`; returns the bytes sent.
    fn send_to(&self, data: &[u8], to: SocketAddr) -> io::Result<usize>;
    /// Sends one datagram to the host name `host` and port, leaving the name
    /// for whoever carries it to resolve.
    ///
    /// The default refuses with [`io::ErrorKind::Unsupported`]: a socket that
    /// sends where it likes would have to resolve the name here, which a
    /// session under a proxy must never do. Only a sender that hands the name
    /// to a proxy (a SOCKS5 UDP relay) carries it.
    // audit: this trait only sends; the socket that implements it records where it reaches
    fn send_to_host(&self, _data: &[u8], _host: &str, _port: u16) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "this socket cannot send to a host name",
        ))
    }
}

/// Receives the datagrams of one packet kind.
///
/// It must not block or await (it runs on the receive task); handlers that
/// need async work `try_send` into their own bounded channel. It must not
/// panic either: a panic ends delivery for every kind.
pub trait DatagramHandler: Send + Sync + std::fmt::Debug + 'static {
    /// Called once per datagram received from `from`.
    fn handle(&self, from: SocketAddr, data: &[u8]);
}
