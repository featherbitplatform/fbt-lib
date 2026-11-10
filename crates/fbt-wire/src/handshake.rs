//! The 68-byte BitTorrent handshake.

use fbt_core::PeerId;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Total handshake length: 1 + 19 + 8 + 20 + 20.
pub const HANDSHAKE_LEN: usize = 68;
const PROTOCOL: &[u8; 19] = b"BitTorrent protocol";

/// The 8 reserved (extension) bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Reserved(pub [u8; 8]);

impl Reserved {
    /// BEP 6 fast extension.
    pub fn supports_fast(&self) -> bool {
        self.0[7] & 0x04 != 0
    }
    /// Advertises the fast extension.
    pub fn set_fast(&mut self) {
        self.0[7] |= 0x04;
    }
    /// BEP 10 extension protocol.
    pub fn supports_extended(&self) -> bool {
        self.0[5] & 0x10 != 0
    }
    /// Advertises the BEP 10 extension protocol.
    pub fn set_extended(&mut self) {
        self.0[5] |= 0x10;
    }
    /// BEP 5 DHT.
    pub fn supports_dht(&self) -> bool {
        self.0[7] & 0x01 != 0
    }
    /// Advertises the DHT.
    pub fn set_dht(&mut self) {
        self.0[7] |= 0x01;
    }
}

/// A decoded handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Handshake {
    /// Extension bits.
    pub reserved: Reserved,
    /// Torrent info hash.
    pub info_hash: [u8; 20],
    /// Sender's peer id.
    pub peer_id: PeerId,
}

/// Handshake failures.
#[derive(Debug, thiserror::Error)]
pub enum HandshakeError {
    /// Wrong `pstrlen` or protocol string.
    #[error("not the BitTorrent protocol")]
    BadProtocol,
    /// Transport error or early EOF.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl Handshake {
    /// Serialises to the 68-byte wire form.
    pub fn encode(&self) -> [u8; HANDSHAKE_LEN] {
        let mut out = [0u8; HANDSHAKE_LEN];
        let bytes = std::iter::once(19u8)
            .chain(PROTOCOL.iter().copied())
            .chain(self.reserved.0)
            .chain(self.info_hash)
            .chain(self.peer_id.0);
        for (d, s) in out.iter_mut().zip(bytes) {
            *d = s;
        }
        out
    }

    /// Parses the 68-byte wire form.
    pub fn decode(buf: &[u8; HANDSHAKE_LEN]) -> Result<Handshake, HandshakeError> {
        let bad = || HandshakeError::BadProtocol;
        let (&pstrlen, rest) = buf.split_first().ok_or_else(bad)?;
        let (proto, rest) = rest.split_first_chunk::<19>().ok_or_else(bad)?;
        if pstrlen != 19 || proto != PROTOCOL {
            return Err(HandshakeError::BadProtocol);
        }
        let (reserved, rest) = rest.split_first_chunk::<8>().ok_or_else(bad)?;
        let (info_hash, rest) = rest.split_first_chunk::<20>().ok_or_else(bad)?;
        let (peer_id, _) = rest.split_first_chunk::<20>().ok_or_else(bad)?;
        Ok(Handshake {
            reserved: Reserved(*reserved),
            info_hash: *info_hash,
            peer_id: PeerId(*peer_id),
        })
    }
}

/// Writes a handshake and flushes.
pub async fn write_handshake<W: AsyncWrite + Unpin>(
    w: &mut W,
    h: &Handshake,
) -> std::io::Result<()> {
    w.write_all(&h.encode()).await?;
    w.flush().await
}

/// Reads exactly one handshake.
pub async fn read_handshake<R: AsyncRead + Unpin>(r: &mut R) -> Result<Handshake, HandshakeError> {
    let mut buf = [0u8; HANDSHAKE_LEN];
    r.read_exact(&mut buf).await?;
    Handshake::decode(&buf)
}
