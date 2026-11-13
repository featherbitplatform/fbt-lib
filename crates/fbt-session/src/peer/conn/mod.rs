//! The per-peer connection task.
//!
//! One task per connection runs a single `select!` loop over the framed
//! socket, the command channel from the torrent and one timer (idle,
//! keepalive and request deadlines). Everything read from the socket is
//! hostile: protocol violations end the connection with
//! [`DisconnectReason::Protocol`], never a panic.
//!
//! M1 never sends `AllowedFast` (we do not generate our own allowed-fast set
//! yet), so requests from a remote we choke are always dropped or rejected.
//!
//! The BEP 10 extension layer lives in [`super::ext`]; this file only routes
//! extended messages. Without metadata (`PeerCtx::layout` is `None`) the
//! connection runs in no-metadata mode: the remote's piece announcements
//! are buffered unvalidated (bounded) and replayed on
//! [`PeerCommand::MetadataReady`].

mod inbound;
mod outbound;
mod state;
#[cfg(test)]
mod tests;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use fbt_core::{BLOCK_SIZE, Bitfield, BlockRef, PeerId, PieceLayout};
use fbt_net::proxy::{ProxyKind, ProxyTarget, connect_tcp};
use fbt_utp::{UtpError, UtpSocket};
use fbt_wire::{
    CryptoMethods, Handshake, HandshakeError, MseStream, Reserved, WireError, initiate,
    read_handshake, write_handshake,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout, timeout_at};

use super::ext::ExtConfig;
use super::messages::{DisconnectReason, PeerCommand, PeerEvent};
use super::transport::{PeerTransport, Transport};
use crate::bandwidth::{Bandwidth, BucketPair};
use crate::net::ExternalIp;
use crate::picker::PeerKey;
use crate::settings::Settings;
use state::Conn;

/// Bytes of a BitTorrent handshake.
const HANDSHAKE_LEN: u64 = 68;

/// Command queue room beyond `max_incoming_requests`: every queued remote
/// request may be answered by one command, plus control commands and the
/// answers to the torrent's 64 pending `hash request`s at most
/// (`MAX_PENDING_HASH_REQUESTS`).
const COMMAND_HEADROOM: usize = 128;

/// Capacity of a peer task's command channel. A full queue closes the peer
/// as stalled, so it must hold an answer for every request we accept.
pub(crate) fn command_queue(settings: &Settings) -> usize {
    settings
        .max_incoming_requests
        .saturating_add(COMMAND_HEADROOM)
}
/// Most allowed-fast pieces kept per connection; further `AllowedFast`
/// messages are ignored (BEP 6 sets are around ten pieces).
pub(crate) const MAX_ALLOWED_FAST: usize = 64;
/// Most `Have`s buffered before the metadata arrives; further ones are
/// ignored.
pub(crate) const MAX_PENDING_HAVES: usize = 4096;
/// Frame length limit before the metadata is known. It also bounds the one
/// bitfield buffered in no-metadata mode.
const NO_METADATA_MAX_FRAME: u32 = 1 << 20;

/// The codec's frame length limit: a block or the whole bitfield, at least
/// 1 MiB; [`NO_METADATA_MAX_FRAME`] without metadata.
fn max_frame(num_pieces: Option<u32>) -> u32 {
    num_pieces.map_or(NO_METADATA_MAX_FRAME, |n| {
        (BLOCK_SIZE + 13)
            .max(n.div_ceil(8).saturating_add(1))
            .max(1 << 20)
    })
}

/// Everything a peer task needs from its torrent.
#[derive(Debug, Clone)]
pub(crate) struct PeerCtx {
    pub key: PeerKey,
    pub info_hash: [u8; 20],
    pub our_id: PeerId,
    /// `None` while the metadata is pending.
    pub layout: Option<PieceLayout>,
    pub have: Bitfield,
    pub ext: ExtConfig,
    pub settings: Arc<Settings>,
    pub events: mpsc::Sender<PeerEvent>,
    /// The connection is RC4-encrypted (MSE), as `MseStream::is_encrypted`
    /// reports it after the handshake; copied into `PeerEvent::Connected`.
    pub encrypted: bool,
    /// The transport the connection runs over; copied into
    /// `PeerEvent::Connected`.
    pub transport: Transport,
    /// The session's rate limits.
    pub bandwidth: Arc<Bandwidth>,
    /// The torrent's own rate limit buckets.
    pub torrent_rate: BucketPair,
    /// The session's external address votes; the peer's `yourip` goes in.
    pub external_ip: Arc<ExternalIp>,
}

/// A peer connection's stream: MSE (RC4 or plaintext after the MSE
/// handshake) or a plain BitTorrent connection, over TCP or uTP.
pub(crate) type PeerStream = MseStream<PeerTransport>;

/// Connects to `addr` over `transport` and runs the peer task; returns its
/// command channel. `mse` is the MSE `crypto_provide` to open with, `None`
/// for a plain BitTorrent handshake; `utp` is the socket to dial uTP from
/// (required for [`Transport::Utp`]).
///
/// A failed or timed-out connect is reported as `Disconnected` with
/// [`DisconnectReason::Io`] and `connect_failed: Some(transport)`; a failed
/// MSE handshake (timeouts included) with [`DisconnectReason::Encryption`].
/// The MSE and BitTorrent handshakes share one `handshake_timeout`, which
/// starts once the connect succeeded.
pub(crate) fn spawn_outgoing(
    addr: SocketAddr,
    mut ctx: PeerCtx,
    mse: Option<CryptoMethods>,
    transport: Transport,
    utp: Option<UtpSocket>,
) -> mpsc::Sender<PeerCommand> {
    let (tx, rx) = mpsc::channel(command_queue(&ctx.settings));
    tokio::spawn(async move {
        let stream = match connect(addr, transport, utp, &ctx.settings).await {
            Ok(stream) => stream,
            Err((reason, failed)) => {
                let failed = failed.then_some(transport);
                return report(&ctx, reason, Vec::new(), failed).await;
            }
        };
        ctx.transport = transport;
        let deadline = after(Instant::now(), ctx.settings.handshake_timeout);
        let stream: PeerStream = match mse {
            None => MseStream::plaintext(stream, Vec::new()),
            Some(provide) => {
                match timeout_at(deadline, initiate(stream, &ctx.info_hash, provide, b"")).await {
                    Ok(Ok(s)) => s,
                    Ok(Err(e)) => {
                        let reason = DisconnectReason::Encryption(e.to_string());
                        return report(&ctx, reason, Vec::new(), None).await;
                    }
                    Err(_) => {
                        let reason = DisconnectReason::Encryption("MSE handshake timed out".into());
                        return report(&ctx, reason, Vec::new(), None).await;
                    }
                }
            }
        };
        ctx.encrypted = stream.is_encrypted();
        run_peer_until(stream, None, addr, ctx, rx, deadline).await;
    });
    tx
}

/// Peer TCP dials go through [`Settings::proxy`].
fn proxied(settings: &Settings) -> bool {
    settings.proxy.kind != ProxyKind::None && settings.proxy.proxy_peers
}

/// Opens the transport to `addr`. TCP waits up to `connect_timeout` (through
/// the proxy when [`Settings::proxy`] covers peers); uTP up
/// to the socket's connect timeout (`utp_connect_timeout`). Fails with
/// [`DisconnectReason::Io`] and whether it was a connect failure that the
/// other transport may get past: every TCP error and timeout, a uTP
/// `ConnectTimeout`, `Reset` or `TooManyConnections` (a full uTP table
/// says nothing about TCP). The other local uTP errors (no socket for the
/// address family, the socket closed) are not.
async fn connect(
    addr: SocketAddr,
    transport: Transport,
    utp: Option<UtpSocket>,
    settings: &Settings,
) -> Result<PeerTransport, (DisconnectReason, bool)> {
    let io = |msg: String| DisconnectReason::Io(msg);
    match transport {
        Transport::Tcp if proxied(settings) => {
            // The proxy dial, negotiation and reply share the timeout.
            match connect_tcp(
                &settings.proxy,
                ProxyTarget::Addr(addr),
                settings.connect_timeout,
            )
            .await
            {
                Ok(s) => Ok(PeerTransport::Tcp(s)),
                Err(e) => Err((io(e.to_string()), true)),
            }
        }
        Transport::Tcp => match timeout(settings.connect_timeout, TcpStream::connect(addr)).await {
            Ok(Ok(s)) => Ok(PeerTransport::Tcp(s)),
            Ok(Err(e)) => Err((io(e.to_string()), true)),
            Err(_) => Err((io("connect timed out".to_owned()), true)),
        },
        Transport::Utp => {
            let Some(socket) = utp else {
                return Err((
                    io("no uTP socket for this address family".to_owned()),
                    false,
                ));
            };
            // The socket times the connect out itself; this outer bound,
            // a second later, only guards against one that never resolves.
            let bound = settings
                .utp_connect_timeout
                .saturating_add(Duration::from_secs(1));
            // An IPv4-mapped peer is dialled from the IPv4 socket.
            let to = SocketAddr::new(addr.ip().to_canonical(), addr.port());
            match timeout(bound, socket.connect(to)).await {
                Ok(Ok(s)) => Ok(PeerTransport::Utp(s)),
                Ok(Err(e)) => {
                    let failed = matches!(
                        e,
                        UtpError::ConnectTimeout | UtpError::Reset | UtpError::TooManyConnections
                    );
                    Err((io(e.to_string()), failed))
                }
                Err(_) => Err((io(UtpError::ConnectTimeout.to_string()), true)),
            }
        }
    }
}

/// Runs the peer task on an accepted stream whose handshake the listener
/// already read (after the MSE handshake, if any); returns its command
/// channel. `ctx.encrypted` and `ctx.transport` must already describe
/// `stream`.
pub(crate) fn spawn_incoming(
    stream: PeerStream,
    remote: Handshake,
    addr: SocketAddr,
    ctx: PeerCtx,
) -> mpsc::Sender<PeerCommand> {
    let (tx, rx) = mpsc::channel(command_queue(&ctx.settings));
    tokio::spawn(run_peer(stream, Some(remote), addr, ctx, rx));
    tx
}

/// Runs one connection to completion. `remote` is the already-read handshake
/// of an incoming connection, or `None` for an outgoing one. Always ends with
/// exactly one `Disconnected` event.
/// The handshake must finish within `handshake_timeout`.
pub(crate) async fn run_peer<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    remote: Option<Handshake>,
    addr: SocketAddr,
    ctx: PeerCtx,
    cmds: mpsc::Receiver<PeerCommand>,
) {
    let deadline = after(Instant::now(), ctx.settings.handshake_timeout);
    run_peer_until(stream, remote, addr, ctx, cmds, deadline).await;
}

/// [`run_peer`] with the handshake due by `deadline`.
async fn run_peer_until<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    remote: Option<Handshake>,
    addr: SocketAddr,
    ctx: PeerCtx,
    mut cmds: mpsc::Receiver<PeerCommand>,
    deadline: Instant,
) {
    let (reason, outstanding) = match handshake(&mut stream, remote, &ctx, deadline).await {
        Err(reason) => (reason, Vec::new()),
        Ok(theirs) => {
            // Counted for mixed mode while connected (dropped with this
            // scope).
            let throttles = ctx
                .bandwidth
                .peer(ctx.transport, addr.ip(), &ctx.torrent_rate);
            // The handshakes are charged, never delayed.
            throttles.down.charge(HANDSHAKE_LEN);
            throttles.up.charge(HANDSHAKE_LEN);
            let mut conn = Conn::new(stream, &theirs, addr.ip(), &ctx, &throttles);
            let connected = PeerEvent::Connected {
                key: ctx.key,
                peer_id: theirs.peer_id,
                reserved: theirs.reserved,
                addr,
                encrypted: ctx.encrypted,
                transport: ctx.transport,
            };
            let reason = match conn.emit(connected).await {
                Err(reason) => reason,
                Ok(()) => conn.serve(&mut cmds, &ctx.have).await,
            };
            (reason, conn.outstanding.into_keys().collect())
        }
    };
    report(&ctx, reason, outstanding, None).await;
}

async fn report(
    ctx: &PeerCtx,
    reason: DisconnectReason,
    outstanding: Vec<BlockRef>,
    connect_failed: Option<Transport>,
) {
    let ev = PeerEvent::Disconnected {
        key: ctx.key,
        reason,
        outstanding,
        connect_failed,
    };
    // The torrent may already be gone; nothing is left to tell then.
    let _ = ctx.events.send(ev).await;
}

/// Exchanges handshakes by `deadline` and checks the remote's.
async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    remote: Option<Handshake>,
    ctx: &PeerCtx,
    deadline: Instant,
) -> Result<Handshake, DisconnectReason> {
    let mut reserved = Reserved::default();
    reserved.set_fast();
    reserved.set_extended();
    // BEP 52: we speak v2 on every connection, as libtorrent does.
    reserved.set_v2();
    if ctx.ext.dht_port.is_some() {
        reserved.set_dht();
    }
    let ours = Handshake {
        reserved,
        info_hash: ctx.info_hash,
        peer_id: ctx.our_id,
    };
    let io = |e: std::io::Error| DisconnectReason::Io(e.to_string());
    let exchange = async {
        match remote {
            Some(theirs) => {
                check_handshake(&theirs, ctx)?;
                write_handshake(stream, &ours).await.map_err(io)?;
                Ok(theirs)
            }
            None => {
                write_handshake(stream, &ours).await.map_err(io)?;
                let theirs = read_handshake(stream).await.map_err(|e| match e {
                    HandshakeError::Io(e) => io(e),
                    e => DisconnectReason::Protocol(e.to_string()),
                })?;
                check_handshake(&theirs, ctx)?;
                Ok(theirs)
            }
        }
    };
    timeout_at(deadline, exchange)
        .await
        .map_err(|_| DisconnectReason::HandshakeTimeout)?
}

fn check_handshake(theirs: &Handshake, ctx: &PeerCtx) -> Result<(), DisconnectReason> {
    if theirs.info_hash != ctx.info_hash {
        return Err(DisconnectReason::InfoHashMismatch);
    }
    if theirs.peer_id == ctx.our_id {
        return Err(DisconnectReason::SelfConnection);
    }
    Ok(())
}

fn wire_reason(e: WireError) -> DisconnectReason {
    match e {
        WireError::Io(e) => DisconnectReason::Io(e.to_string()),
        e => DisconnectReason::Protocol(e.to_string()),
    }
}

fn protocol(msg: &str) -> DisconnectReason {
    DisconnectReason::Protocol(msg.to_owned())
}

/// `t + d`, saturating far in the future instead of overflowing.
pub(crate) fn after(t: Instant, d: Duration) -> Instant {
    t.checked_add(d)
        .or_else(|| t.checked_add(Duration::from_secs(86_400 * 365)))
        .unwrap_or(t)
}

type Flow = Result<(), DisconnectReason>;

/// The bytes `m` takes on the wire (its frame, length prefix included):
/// what the rate limits count.
pub(crate) fn wire_len(m: &fbt_wire::Message) -> u64 {
    use fbt_wire::Message as M;
    let payload = match m {
        M::KeepAlive => return 4,
        M::Choke | M::Unchoke | M::Interested | M::NotInterested | M::HaveAll | M::HaveNone => 0,
        M::Have(_) | M::SuggestPiece(_) | M::AllowedFast(_) => 4,
        M::Port(_) => 2,
        M::Request(_) | M::Cancel(_) | M::RejectRequest(_) => 12,
        M::Bitfield(b) => b.len(),
        M::Piece { data, .. } => data.len().saturating_add(8),
        M::Extended { payload, .. } => payload.len().saturating_add(1),
        M::HashRequest(_) | M::HashReject(_) => 48,
        M::Hashes { hashes, .. } => hashes.len().saturating_add(48),
        M::Unknown { payload, .. } => payload.len(),
    };
    (payload as u64).saturating_add(5)
}
