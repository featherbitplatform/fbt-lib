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

use std::collections::{BTreeMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fbt_core::{BLOCK_SIZE, Bitfield, BlockRef, PeerId, PieceLayout};
use fbt_wire::{
    Handshake, HandshakeError, Message, MessageCodec, Reserved, WireError, read_handshake,
    write_handshake,
};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until, timeout};
use tokio_util::codec::Framed;

use super::ext::{self, ExtConfig, Incoming, RemoteIds};
use super::messages::{DisconnectReason, PeerCommand, PeerEvent, PeerPiecesOwned};
use crate::picker::PeerKey;
use crate::settings::Settings;

/// Command queue room beyond `max_incoming_requests`: every queued remote
/// request may be answered by one command, plus control commands.
const COMMAND_HEADROOM: usize = 64;

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
}

/// Connects to `addr` and runs the peer task; returns its command channel.
///
/// A failed or timed-out connect is reported as `Disconnected` with
/// [`DisconnectReason::Io`].
pub(crate) fn spawn_outgoing(addr: SocketAddr, ctx: PeerCtx) -> mpsc::Sender<PeerCommand> {
    let (tx, rx) = mpsc::channel(command_queue(&ctx.settings));
    tokio::spawn(async move {
        match timeout(ctx.settings.connect_timeout, TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => run_peer(stream, None, addr, ctx, rx).await,
            Ok(Err(e)) => report(&ctx, DisconnectReason::Io(e.to_string()), Vec::new()).await,
            Err(_) => {
                let reason = DisconnectReason::Io("connect timed out".to_owned());
                report(&ctx, reason, Vec::new()).await;
            }
        }
    });
    tx
}

/// Runs the peer task on an accepted stream whose handshake the listener
/// already read; returns its command channel.
pub(crate) fn spawn_incoming<S: AsyncRead + AsyncWrite + Send + Unpin + 'static>(
    stream: S,
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
pub(crate) async fn run_peer<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    remote: Option<Handshake>,
    addr: SocketAddr,
    ctx: PeerCtx,
    mut cmds: mpsc::Receiver<PeerCommand>,
) {
    let (reason, outstanding) = match handshake(&mut stream, remote, &ctx).await {
        Err(reason) => (reason, Vec::new()),
        Ok(theirs) => {
            let mut conn = Conn::new(stream, &theirs, addr.ip(), &ctx);
            let connected = PeerEvent::Connected {
                key: ctx.key,
                peer_id: theirs.peer_id,
                reserved: theirs.reserved,
                addr,
            };
            let reason = match conn.emit(connected).await {
                Err(reason) => reason,
                Ok(()) => conn.serve(&mut cmds, &ctx.have).await,
            };
            (reason, conn.outstanding.into_keys().collect())
        }
    };
    report(&ctx, reason, outstanding).await;
}

async fn report(ctx: &PeerCtx, reason: DisconnectReason, outstanding: Vec<BlockRef>) {
    let ev = PeerEvent::Disconnected {
        key: ctx.key,
        reason,
        outstanding,
    };
    // The torrent may already be gone; nothing is left to tell then.
    let _ = ctx.events.send(ev).await;
}

/// Exchanges handshakes within `handshake_timeout` and checks the remote's.
async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    remote: Option<Handshake>,
    ctx: &PeerCtx,
) -> Result<Handshake, DisconnectReason> {
    let mut reserved = Reserved::default();
    reserved.set_fast();
    reserved.set_extended();
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
    timeout(ctx.settings.handshake_timeout, exchange)
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
fn after(t: Instant, d: Duration) -> Instant {
    t.checked_add(d)
        .or_else(|| t.checked_add(Duration::from_secs(86_400 * 365)))
        .unwrap_or(t)
}

type Flow = Result<(), DisconnectReason>;

/// A piece set as the remote sent it, before validation.
#[derive(Debug)]
enum RawPieces {
    All,
    None,
    Bits(Bytes),
}

impl RawPieces {
    fn validate(self, num_pieces: u32) -> Result<PeerPiecesOwned, DisconnectReason> {
        Ok(match self {
            RawPieces::All => PeerPiecesOwned::All,
            RawPieces::None => PeerPiecesOwned::None,
            RawPieces::Bits(bytes) => PeerPiecesOwned::Bits(
                Bitfield::from_bytes(&bytes, num_pieces)
                    .map_err(|e| DisconnectReason::Protocol(e.to_string()))?,
            ),
        })
    }
}

/// Piece announcements received before the metadata: at most one piece set
/// (its size bounded by [`NO_METADATA_MAX_FRAME`]) and at most
/// [`MAX_PENDING_HAVES`] `Have`s.
#[derive(Debug, Default)]
struct Pending {
    pieces: Option<RawPieces>,
    haves: Vec<u32>,
}

/// State of one established connection.
struct Conn<S> {
    framed: Framed<S, MessageCodec>,
    key: PeerKey,
    events: mpsc::Sender<PeerEvent>,
    settings: Arc<Settings>,
    /// 0 while the metadata is pending.
    num_pieces: u32,
    /// `Some` in no-metadata mode.
    pending: Option<Pending>,
    ext: ExtConfig,
    remote_ip: IpAddr,
    remote_reserved: Reserved,
    /// The remote's ids for our extensions.
    remote_ids: RemoteIds,
    /// A `Port` was reported already.
    port_seen: bool,
    /// Both sides support BEP 6 (we always advertise it).
    fast: bool,
    /// The remote chokes us.
    choked: bool,
    /// We choke the remote.
    choking: bool,
    /// We are interested in the remote.
    interested: bool,
    /// The remote is interested in us.
    remote_interested: bool,
    /// A bitfield, have, `HaveAll` or `HaveNone` arrived.
    pieces_seen: bool,
    /// Pieces the remote lets us request while choked (at most
    /// [`MAX_ALLOWED_FAST`]).
    allowed_fast: HashSet<u32>,
    /// Our requests in flight, with their send time.
    outstanding: BTreeMap<BlockRef, Instant>,
    /// Remote requests forwarded to the torrent and not yet answered.
    incoming: HashSet<BlockRef>,
    last_recv: Instant,
    last_send: Instant,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Conn<S> {
    fn new(stream: S, theirs: &Handshake, remote_ip: IpAddr, ctx: &PeerCtx) -> Self {
        let num_pieces = ctx.layout.map(|l| l.num_pieces());
        let now = Instant::now();
        Conn {
            framed: Framed::new(stream, MessageCodec::new(max_frame(num_pieces))),
            key: ctx.key,
            events: ctx.events.clone(),
            settings: Arc::clone(&ctx.settings),
            num_pieces: num_pieces.unwrap_or(0),
            pending: num_pieces.is_none().then(Pending::default),
            ext: ctx.ext,
            remote_ip,
            remote_reserved: theirs.reserved,
            remote_ids: RemoteIds::default(),
            port_seen: false,
            fast: theirs.reserved.supports_fast(),
            choked: true,
            choking: true,
            interested: false,
            remote_interested: false,
            pieces_seen: false,
            allowed_fast: HashSet::new(),
            outstanding: BTreeMap::new(),
            incoming: HashSet::new(),
            last_recv: now,
            last_send: now,
        }
    }

    /// Sends our extension handshake, pieces and DHT port, then loops
    /// until the connection ends.
    async fn serve(
        &mut self,
        cmds: &mut mpsc::Receiver<PeerCommand>,
        have: &Bitfield,
    ) -> DisconnectReason {
        if let Err(reason) = self.greet(have).await {
            return reason;
        }
        loop {
            let deadline = self.next_deadline();
            let step = tokio::select! {
                msg = self.framed.next() => match msg {
                    Some(Ok(m)) => {
                        self.last_recv = Instant::now();
                        self.on_message(m).await
                    }
                    Some(Err(e)) => Err(wire_reason(e)),
                    None => Err(DisconnectReason::Closed),
                },
                cmd = cmds.recv() => match cmd {
                    Some(c) => self.on_command(c).await,
                    None => Err(DisconnectReason::Shutdown),
                },
                () = sleep_until(deadline) => self.on_timer().await,
            };
            if let Err(reason) = step {
                return reason;
            }
        }
    }

    /// The messages that follow the handshake: our extension handshake
    /// (if the remote speaks BEP 10), our pieces (only `HaveNone`, with
    /// fast, before the metadata) and `Port` (if both sides set the DHT bit).
    async fn greet(&mut self, have: &Bitfield) -> Flow {
        if self.remote_reserved.supports_extended() {
            self.feed(ext::handshake_message(&self.ext, self.remote_ip))
                .await?;
        }
        let pieces = if self.pending.is_some() {
            self.fast.then_some(Message::HaveNone)
        } else if self.fast && have.is_full() {
            Some(Message::HaveAll)
        } else if self.fast && have.count_ones() == 0 {
            Some(Message::HaveNone)
        } else {
            Some(Message::Bitfield(Bytes::copy_from_slice(have.as_bytes())))
        };
        if let Some(m) = pieces {
            self.feed(m).await?;
        }
        if let Some(port) = self.ext.dht_port
            && self.remote_reserved.supports_dht()
        {
            self.feed(Message::Port(port)).await?;
        }
        self.flush().await
    }

    fn next_deadline(&self) -> Instant {
        let s = &self.settings;
        let idle = after(self.last_recv, s.peer_idle_timeout);
        let keepalive = after(self.last_send, s.keepalive_interval);
        let request = self
            .outstanding
            .values()
            .min()
            .map(|&t| after(t, s.request_timeout));
        idle.min(keepalive).min(request.unwrap_or(idle))
    }

    async fn on_timer(&mut self) -> Flow {
        let now = Instant::now();
        if now >= after(self.last_recv, self.settings.peer_idle_timeout) {
            return Err(DisconnectReason::IdleTimeout);
        }
        let limit = self.settings.request_timeout;
        let expired: Vec<BlockRef> = self
            .outstanding
            .iter()
            .filter(|(_, t)| now >= after(**t, limit))
            .map(|(b, _)| *b)
            .collect();
        if !expired.is_empty() {
            for b in &expired {
                self.outstanding.remove(b);
                self.feed(Message::Cancel(*b)).await?;
            }
            self.flush().await?;
            for block in expired {
                self.emit(PeerEvent::Rejected {
                    key: self.key,
                    block,
                })
                .await?;
            }
            self.want_blocks().await?;
        }
        if now >= after(self.last_send, self.settings.keepalive_interval) {
            self.send(Message::KeepAlive).await?;
        }
        Ok(())
    }

    async fn on_message(&mut self, m: Message) -> Flow {
        let key = self.key;
        match m {
            Message::Choke => {
                if !self.choked {
                    self.choked = true;
                    self.emit(PeerEvent::Choked { key }).await?;
                    // The torrent drops our demand on `Choked`; allowed-fast
                    // pieces may still be requested.
                    if self.fast && !self.allowed_fast.is_empty() {
                        self.want_blocks().await?;
                    }
                }
                // Without fast, a choke silently drops every pending request.
                if !self.fast {
                    for block in std::mem::take(&mut self.outstanding).into_keys() {
                        self.emit(PeerEvent::Rejected { key, block }).await?;
                    }
                }
            }
            Message::Unchoke => {
                if self.choked {
                    self.choked = false;
                    self.emit(PeerEvent::Unchoked { key }).await?;
                    self.want_blocks().await?;
                }
            }
            Message::Interested => {
                if !self.remote_interested {
                    self.remote_interested = true;
                    self.emit(PeerEvent::Interested { key }).await?;
                }
            }
            Message::NotInterested => {
                if self.remote_interested {
                    self.remote_interested = false;
                    self.emit(PeerEvent::NotInterested { key }).await?;
                }
            }
            Message::Have(piece) => {
                if let Some(p) = self.pending.as_mut() {
                    self.pieces_seen = true;
                    if p.haves.len() < MAX_PENDING_HAVES {
                        p.haves.push(piece);
                    }
                    return Ok(());
                }
                if piece >= self.num_pieces {
                    return Err(protocol("have index out of range"));
                }
                self.pieces_seen = true;
                self.emit(PeerEvent::Have { key, piece }).await?;
            }
            Message::Bitfield(bytes) => self.initial_pieces(RawPieces::Bits(bytes)).await?,
            Message::HaveAll => {
                self.require_fast()?;
                self.initial_pieces(RawPieces::All).await?;
            }
            Message::HaveNone => {
                self.require_fast()?;
                self.initial_pieces(RawPieces::None).await?;
            }
            Message::Request(block) => self.on_remote_request(block).await?,
            Message::Piece {
                piece,
                offset,
                data,
            } => {
                let len = u32::try_from(data.len()).unwrap_or(u32::MAX);
                let block = BlockRef { piece, offset, len };
                if self.outstanding.remove(&block).is_some() {
                    self.emit(PeerEvent::Block { key, block, data }).await?;
                    self.want_blocks().await?;
                } else {
                    let bytes = data.len() as u64;
                    self.emit(PeerEvent::Wasted { key, bytes }).await?;
                }
            }
            Message::Cancel(block) => {
                if self.incoming.remove(&block) {
                    self.emit(PeerEvent::Cancel { key, block }).await?;
                }
            }
            Message::RejectRequest(block) => {
                self.require_fast()?;
                if self.outstanding.remove(&block).is_some() {
                    self.emit(PeerEvent::Rejected { key, block }).await?;
                    self.want_blocks().await?;
                }
            }
            Message::AllowedFast(piece) => {
                self.require_fast()?;
                // A remote may send any number of these: only the first
                // [`MAX_ALLOWED_FAST`] distinct pieces count, and only the
                // first one opens the pipeline (the torrent keeps the demand
                // and tops it up as more pieces become requestable).
                if piece < self.num_pieces
                    && self.allowed_fast.len() < MAX_ALLOWED_FAST
                    && self.allowed_fast.insert(piece)
                {
                    self.emit(PeerEvent::AllowedFast { key, piece }).await?;
                    if self.allowed_fast.len() == 1 {
                        self.want_blocks().await?;
                    }
                }
            }
            Message::SuggestPiece(_) => self.require_fast()?,
            Message::Port(port) => {
                if port != 0 && !self.port_seen {
                    self.port_seen = true;
                    self.emit(PeerEvent::DhtPort { key, port }).await?;
                }
            }
            Message::Extended { id, payload } => self.on_extended(id, &payload).await?,
            Message::KeepAlive | Message::Unknown { .. } => {}
        }
        Ok(())
    }

    /// Routes an extended message by the id we assigned.
    async fn on_extended(&mut self, id: u8, payload: &Bytes) -> Flow {
        let key = self.key;
        let msg = ext::decode(id, payload)
            .map_err(|e| DisconnectReason::Protocol(format!("extended message {id}: {e}")))?;
        match msg {
            Incoming::Handshake(hs) => {
                self.remote_ids.update(&hs);
                self.emit(PeerEvent::ExtHandshake { key, hs }).await
            }
            Incoming::Metadata(msg) => self.emit(PeerEvent::Metadata { key, msg }).await,
            Incoming::Pex(msg) => self.emit(PeerEvent::Pex { key, msg }).await,
            Incoming::Unknown => Ok(()),
        }
    }

    fn require_fast(&self) -> Flow {
        if self.fast {
            Ok(())
        } else {
            Err(protocol("fast extension message without fast support"))
        }
    }

    /// A bitfield, `HaveAll` or `HaveNone`: only before any other piece
    /// announcement. Without metadata it is buffered unvalidated.
    async fn initial_pieces(&mut self, raw: RawPieces) -> Flow {
        if self.pieces_seen {
            return Err(protocol("piece set after earlier piece announcements"));
        }
        self.pieces_seen = true;
        if let Some(p) = self.pending.as_mut() {
            p.pieces = Some(raw);
            return Ok(());
        }
        let pieces = raw.validate(self.num_pieces)?;
        self.emit(PeerEvent::Pieces {
            key: self.key,
            pieces,
        })
        .await
    }

    /// Leaves no-metadata mode: validates the buffered piece set against
    /// `layout` and replays it and the in-range `Have`s as if they had just
    /// arrived.
    async fn metadata_ready(&mut self, layout: PieceLayout) -> Flow {
        let Some(pending) = self.pending.take() else {
            return Ok(());
        };
        let n = layout.num_pieces();
        self.num_pieces = n;
        *self.framed.codec_mut() = MessageCodec::new(max_frame(Some(n)));
        let key = self.key;
        if let Some(raw) = pending.pieces {
            let pieces = raw.validate(n)?;
            self.emit(PeerEvent::Pieces { key, pieces }).await?;
        }
        for piece in pending.haves.into_iter().filter(|&p| p < n) {
            self.emit(PeerEvent::Have { key, piece }).await?;
        }
        Ok(())
    }

    async fn on_remote_request(&mut self, block: BlockRef) -> Flow {
        // Nothing can be served before the metadata.
        if self.pending.is_some()
            || self.choking
            || self.incoming.len() >= self.settings.max_incoming_requests
        {
            if self.fast {
                self.send(Message::RejectRequest(block)).await?;
            }
            return Ok(());
        }
        if self.incoming.insert(block) {
            self.emit(PeerEvent::Request {
                key: self.key,
                block,
            })
            .await?;
        }
        Ok(())
    }

    /// Asks the torrent for more blocks when we may request and the pipeline
    /// is below half full.
    async fn want_blocks(&mut self) -> Flow {
        let may_request = !self.choked || (self.fast && !self.allowed_fast.is_empty());
        let max = self.settings.max_outstanding_requests;
        let n = self.outstanding.len();
        if may_request && n.saturating_mul(2) < max {
            self.emit(PeerEvent::WantBlocks {
                key: self.key,
                count: max - n,
            })
            .await?;
        }
        Ok(())
    }

    async fn on_command(&mut self, c: PeerCommand) -> Flow {
        match c {
            PeerCommand::Request(blocks) => {
                let now = Instant::now();
                let mut rejected = Vec::new();
                let mut sent = false;
                for b in blocks {
                    if self.outstanding.contains_key(&b) {
                        continue;
                    }
                    let fast_ok = self.fast && self.allowed_fast.contains(&b.piece);
                    if self.choked && !fast_ok {
                        rejected.push(b);
                        continue;
                    }
                    self.feed(Message::Request(b)).await?;
                    self.outstanding.insert(b, now);
                    sent = true;
                }
                if sent {
                    self.flush().await?;
                }
                for block in rejected {
                    self.emit(PeerEvent::Rejected {
                        key: self.key,
                        block,
                    })
                    .await?;
                }
            }
            PeerCommand::Cancel(b) => {
                if self.outstanding.remove(&b).is_some() {
                    self.send(Message::Cancel(b)).await?;
                }
            }
            PeerCommand::Choke => {
                if !self.choking {
                    self.choking = true;
                    self.feed(Message::Choke).await?;
                    // Queued requests die with the choke; with fast they are
                    // rejected explicitly.
                    for b in std::mem::take(&mut self.incoming) {
                        if self.fast {
                            self.feed(Message::RejectRequest(b)).await?;
                        }
                    }
                    self.flush().await?;
                }
            }
            PeerCommand::Unchoke => {
                if self.choking {
                    self.choking = false;
                    self.send(Message::Unchoke).await?;
                }
            }
            PeerCommand::Interested(on) => {
                if on != self.interested {
                    self.interested = on;
                    let m = if on {
                        Message::Interested
                    } else {
                        Message::NotInterested
                    };
                    self.send(m).await?;
                }
            }
            PeerCommand::Have(piece) => self.send(Message::Have(piece)).await?,
            PeerCommand::Haves(pieces) => {
                if !pieces.is_empty() {
                    for piece in pieces {
                        self.feed(Message::Have(piece)).await?;
                    }
                    self.flush().await?;
                }
            }
            PeerCommand::SendBlock { block, data } => {
                // Only answer requests still queued (not cancelled or choked away).
                if self.incoming.remove(&block) {
                    self.send(Message::Piece {
                        piece: block.piece,
                        offset: block.offset,
                        data,
                    })
                    .await?;
                }
            }
            PeerCommand::Reject(b) => {
                if self.incoming.remove(&b) && self.fast {
                    self.send(Message::RejectRequest(b)).await?;
                }
            }
            PeerCommand::MetadataReady { layout } => self.metadata_ready(layout).await?,
            PeerCommand::SendMetadata(msg) => {
                if let Some(m) = self.remote_ids.metadata(&msg) {
                    self.send(m).await?;
                }
            }
            PeerCommand::SendPex(msg) => {
                if let Some(m) = self.remote_ids.pex(&msg) {
                    self.send(m).await?;
                }
            }
            PeerCommand::Close => return Err(DisconnectReason::Shutdown),
        }
        Ok(())
    }

    async fn emit(&mut self, ev: PeerEvent) -> Flow {
        self.events
            .send(ev)
            .await
            .map_err(|_| DisconnectReason::Shutdown)
    }

    /// A write blocked for the idle timeout means the remote stopped reading.
    fn write_done(
        &mut self,
        r: Result<Result<(), WireError>, tokio::time::error::Elapsed>,
    ) -> Flow {
        match r {
            Ok(Ok(())) => {
                self.last_send = Instant::now();
                Ok(())
            }
            Ok(Err(e)) => Err(wire_reason(e)),
            Err(_) => Err(DisconnectReason::Io("write stalled".to_owned())),
        }
    }

    async fn feed(&mut self, m: Message) -> Flow {
        let r = timeout(self.settings.peer_idle_timeout, self.framed.feed(m)).await;
        self.write_done(r)
    }

    async fn flush(&mut self) -> Flow {
        let r = timeout(self.settings.peer_idle_timeout, self.framed.flush()).await;
        self.write_done(r)
    }

    async fn send(&mut self, m: Message) -> Flow {
        self.feed(m).await?;
        self.flush().await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use fbt_core::BlockRef;
    use fbt_wire::{
        ExtHandshake, Message, MessageCodec, MetadataMsg, OUR_UT_METADATA_ID, OUR_UT_PEX_ID,
        PexFlags, PexMsg, Reserved, UT_METADATA, UT_PEX, read_handshake, write_handshake,
    };
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::{AsyncWriteExt, DuplexStream};
    use tokio::time::Instant;
    use tokio_util::codec::Framed;

    use super::super::messages::{DisconnectReason, PeerPiecesOwned};
    use super::*;

    const KEY: PeerKey = PeerKey(7);
    const HASH: [u8; 20] = [0xab; 20];
    const OUR_ID: PeerId = PeerId([1; 20]);
    const THEIR_ID: PeerId = PeerId([2; 20]);
    const PIECE: u32 = 32_768;
    const PIECES: u32 = 4;

    fn addr() -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 6881))
    }

    fn block(piece: u32, offset: u32) -> BlockRef {
        BlockRef {
            piece,
            offset,
            len: 16_384,
        }
    }

    struct Harness {
        remote: DuplexStream,
        events: mpsc::Receiver<PeerEvent>,
        cmds: mpsc::Sender<PeerCommand>,
    }

    /// Starts an outgoing-style peer task over a duplex pipe.
    fn start(have: Bitfield) -> Harness {
        start_with(have, PIECES)
    }

    /// [`start`] for a torrent of `pieces` pieces.
    fn start_with(have: Bitfield, pieces: u32) -> Harness {
        start_ctx(have, Some(layout(pieces)), ext_cfg())
    }

    fn layout(pieces: u32) -> PieceLayout {
        PieceLayout::new(u64::from(PIECE) * u64::from(pieces), PIECE).expect("layout")
    }

    const LISTEN_PORT: u16 = 6889;

    fn ext_cfg() -> ExtConfig {
        ExtConfig {
            listen_port: LISTEN_PORT,
            metadata_size: None,
            pex: true,
            dht_port: None,
            reqq: 250,
        }
    }

    /// Starts a peer task with an explicit layout (`None`: no metadata yet)
    /// and extension config.
    fn start_ctx(have: Bitfield, layout: Option<PieceLayout>, ext: ExtConfig) -> Harness {
        let (ours, remote) = tokio::io::duplex(1 << 20);
        let (etx, events) = mpsc::channel(1024);
        let (cmds, crx) = mpsc::channel(256);
        let ctx = PeerCtx {
            key: KEY,
            info_hash: HASH,
            our_id: OUR_ID,
            layout,
            have,
            ext,
            settings: Arc::new(Settings::default()),
            events: etx,
        };
        tokio::spawn(run_peer(ours, None, addr(), ctx, crx));
        Harness {
            remote,
            events,
            cmds,
        }
    }

    fn reserved(fast: bool, ext: bool, dht: bool) -> Reserved {
        let mut r = Reserved::default();
        if fast {
            r.set_fast();
        }
        if ext {
            r.set_extended();
        }
        if dht {
            r.set_dht();
        }
        r
    }

    impl Harness {
        /// Answers our handshake and returns the framed remote end.
        async fn handshake(
            self,
            info_hash: [u8; 20],
            peer_id: PeerId,
            fast: bool,
        ) -> (Framed<DuplexStream, MessageCodec>, Remote) {
            let (wire, r, _) = self
                .handshake_with(info_hash, peer_id, reserved(fast, false, false))
                .await;
            (wire, r)
        }

        /// Answers our handshake with `reserved`; also returns ours.
        async fn handshake_with(
            mut self,
            info_hash: [u8; 20],
            peer_id: PeerId,
            reserved: Reserved,
        ) -> (Framed<DuplexStream, MessageCodec>, Remote, Handshake) {
            let ours = read_handshake(&mut self.remote)
                .await
                .expect("our handshake");
            assert!(ours.reserved.supports_fast());
            assert!(ours.reserved.supports_extended());
            assert_eq!(ours.info_hash, HASH);
            assert_eq!(ours.peer_id, OUR_ID);
            let theirs = Handshake {
                reserved,
                info_hash,
                peer_id,
            };
            write_handshake(&mut self.remote, &theirs)
                .await
                .expect("their handshake");
            (
                Framed::new(self.remote, MessageCodec::new(1 << 20)),
                Remote {
                    events: self.events,
                    cmds: self.cmds,
                },
                ours,
            )
        }

        /// A fully connected remote; consumes `Connected` and our piece message.
        async fn connect(self, fast: bool) -> (Framed<DuplexStream, MessageCodec>, Remote) {
            let (mut wire, mut r) = self.handshake(HASH, THEIR_ID, fast).await;
            assert!(matches!(r.next().await, PeerEvent::Connected { .. }));
            wire.next().await.expect("piece message").expect("decode");
            (wire, r)
        }
    }

    struct Remote {
        events: mpsc::Receiver<PeerEvent>,
        cmds: mpsc::Sender<PeerCommand>,
    }

    impl Remote {
        async fn next(&mut self) -> PeerEvent {
            self.events.recv().await.expect("event")
        }

        async fn cmd(&self, c: PeerCommand) {
            self.cmds.send(c).await.expect("command");
        }

        async fn disconnected(&mut self) -> (DisconnectReason, Vec<BlockRef>) {
            loop {
                if let PeerEvent::Disconnected {
                    reason,
                    outstanding,
                    ..
                } = self.next().await
                {
                    return (reason, outstanding);
                }
            }
        }
    }

    async fn send(wire: &mut Framed<DuplexStream, MessageCodec>, m: Message) {
        wire.send(m).await.expect("send");
    }

    async fn recv(wire: &mut Framed<DuplexStream, MessageCodec>) -> Message {
        wire.next().await.expect("frame").expect("decode")
    }

    /// Unchokes us and requests `blocks`; returns once they are on the wire.
    async fn unchoke_and_request(
        wire: &mut Framed<DuplexStream, MessageCodec>,
        r: &mut Remote,
        blocks: &[BlockRef],
    ) {
        send(wire, Message::Unchoke).await;
        assert_eq!(r.next().await, PeerEvent::Unchoked { key: KEY });
        assert_eq!(
            r.next().await,
            PeerEvent::WantBlocks {
                key: KEY,
                count: 32
            }
        );
        r.cmd(PeerCommand::Request(blocks.to_vec())).await;
        for b in blocks {
            assert_eq!(recv(wire).await, Message::Request(*b));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn handshake_info_hash_mismatch_disconnects() {
        let h = start(Bitfield::new(PIECES));
        let (_wire, mut r) = h.handshake([0xcd; 20], THEIR_ID, true).await;
        assert_eq!(
            r.next().await,
            PeerEvent::Disconnected {
                key: KEY,
                reason: DisconnectReason::InfoHashMismatch,
                outstanding: vec![],
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn handshake_self_connection_detected() {
        let h = start(Bitfield::new(PIECES));
        let (_wire, mut r) = h.handshake(HASH, OUR_ID, true).await;
        assert_eq!(
            r.next().await,
            PeerEvent::Disconnected {
                key: KEY,
                reason: DisconnectReason::SelfConnection,
                outstanding: vec![],
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn handshake_timeout_disconnects() {
        let mut h = start(Bitfield::new(PIECES));
        read_handshake(&mut h.remote).await.expect("our handshake");
        let (reason, _) = Remote {
            events: h.events,
            cmds: h.cmds,
        }
        .disconnected()
        .await;
        assert_eq!(reason, DisconnectReason::HandshakeTimeout);
    }

    #[tokio::test(start_paused = true)]
    async fn sends_have_all_when_complete_and_fast() {
        let (mut wire, mut r) = start(Bitfield::full(PIECES))
            .handshake(HASH, THEIR_ID, true)
            .await;
        assert_eq!(
            r.next().await,
            PeerEvent::Connected {
                key: KEY,
                peer_id: THEIR_ID,
                reserved: reserved(true, false, false),
                addr: addr(),
            }
        );
        assert_eq!(recv(&mut wire).await, Message::HaveAll);

        // Empty and fast: HaveNone.
        let (mut wire, _r) = start(Bitfield::new(PIECES))
            .handshake(HASH, THEIR_ID, true)
            .await;
        assert_eq!(recv(&mut wire).await, Message::HaveNone);

        // Complete without fast on the remote: a bitfield.
        let (mut wire, _r) = start(Bitfield::full(PIECES))
            .handshake(HASH, THEIR_ID, false)
            .await;
        assert_eq!(
            recv(&mut wire).await,
            Message::Bitfield(Bytes::from_static(&[0xf0]))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn bad_bitfield_is_protocol_error() {
        // Spare bits set.
        let (mut wire, mut r) = start(Bitfield::new(PIECES)).connect(true).await;
        send(&mut wire, Message::Bitfield(Bytes::from_static(&[0xff]))).await;
        let (reason, _) = r.disconnected().await;
        assert!(
            matches!(reason, DisconnectReason::Protocol(_)),
            "{reason:?}"
        );

        // Wrong length.
        let (mut wire, mut r) = start(Bitfield::new(PIECES)).connect(true).await;
        send(&mut wire, Message::Bitfield(Bytes::from_static(&[0xf0, 0]))).await;
        let (reason, _) = r.disconnected().await;
        assert!(
            matches!(reason, DisconnectReason::Protocol(_)),
            "{reason:?}"
        );

        // A valid bitfield is accepted; a second one after it is not.
        let (mut wire, mut r) = start(Bitfield::new(PIECES)).connect(true).await;
        send(&mut wire, Message::Bitfield(Bytes::from_static(&[0xa0]))).await;
        let bits = Bitfield::from_bytes(&[0xa0], PIECES).expect("bits");
        assert_eq!(
            r.next().await,
            PeerEvent::Pieces {
                key: KEY,
                pieces: PeerPiecesOwned::Bits(bits),
            }
        );
        send(&mut wire, Message::Bitfield(Bytes::from_static(&[0xa0]))).await;
        let (reason, _) = r.disconnected().await;
        assert!(
            matches!(reason, DisconnectReason::Protocol(_)),
            "{reason:?}"
        );

        // Have beyond the piece count.
        let (mut wire, mut r) = start(Bitfield::new(PIECES)).connect(true).await;
        send(&mut wire, Message::Have(PIECES)).await;
        let (reason, _) = r.disconnected().await;
        assert!(
            matches!(reason, DisconnectReason::Protocol(_)),
            "{reason:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn oversized_frame_is_protocol_error() {
        let (mut wire, mut r) = start(Bitfield::new(PIECES)).connect(true).await;
        // A length prefix far beyond max_len, written raw.
        wire.get_mut()
            .write_all(&[0x7f, 0, 0, 0, 7])
            .await
            .expect("write");
        let (reason, _) = r.disconnected().await;
        assert!(
            matches!(reason, DisconnectReason::Protocol(_)),
            "{reason:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn requests_flow_when_unchoked() {
        let (mut wire, mut r) = start(Bitfield::new(PIECES)).connect(true).await;
        r.cmd(PeerCommand::Interested(true)).await;
        assert_eq!(recv(&mut wire).await, Message::Interested);
        let blocks = [block(0, 0), block(0, 16_384)];
        unchoke_and_request(&mut wire, &mut r, &blocks).await;

        // A delivered block is forwarded; outstanding is below half the
        // maximum, so the pipeline asks for more.
        send(
            &mut wire,
            Message::Piece {
                piece: 0,
                offset: 0,
                data: Bytes::from(vec![9; 16_384]),
            },
        )
        .await;
        match r.next().await {
            PeerEvent::Block { block: b, data, .. } => {
                assert_eq!(b, block(0, 0));
                assert_eq!(data.len(), 16_384);
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            r.next().await,
            PeerEvent::WantBlocks {
                key: KEY,
                count: 31
            }
        );

        // Closing reports what is still outstanding.
        r.cmd(PeerCommand::Close).await;
        let (reason, outstanding) = r.disconnected().await;
        assert_eq!(reason, DisconnectReason::Shutdown);
        assert_eq!(outstanding, vec![block(0, 16_384)]);
    }

    #[tokio::test(start_paused = true)]
    async fn request_while_choked_is_rejected_locally() {
        let (mut wire, mut r) = start(Bitfield::new(PIECES)).connect(false).await;
        r.cmd(PeerCommand::Request(vec![block(1, 0)])).await;
        assert_eq!(
            r.next().await,
            PeerEvent::Rejected {
                key: KEY,
                block: block(1, 0),
            }
        );
        // Nothing went out: the next frame is the keepalive.
        assert_eq!(recv(&mut wire).await, Message::KeepAlive);
    }

    #[tokio::test(start_paused = true)]
    async fn allowed_fast_requests_go_out_while_choked() {
        let (mut wire, mut r) = start(Bitfield::new(PIECES)).connect(true).await;
        send(&mut wire, Message::AllowedFast(2)).await;
        assert_eq!(
            r.next().await,
            PeerEvent::AllowedFast { key: KEY, piece: 2 }
        );
        assert_eq!(
            r.next().await,
            PeerEvent::WantBlocks {
                key: KEY,
                count: 32
            }
        );
        r.cmd(PeerCommand::Request(vec![block(2, 0), block(1, 0)]))
            .await;
        assert_eq!(recv(&mut wire).await, Message::Request(block(2, 0)));
        assert_eq!(
            r.next().await,
            PeerEvent::Rejected {
                key: KEY,
                block: block(1, 0),
            }
        );
    }

    #[test]
    fn command_queue_holds_every_accepted_request() {
        let mut s = Settings::default();
        assert_eq!(command_queue(&s), 250 + COMMAND_HEADROOM);
        s.max_incoming_requests = 4000;
        assert_eq!(command_queue(&s), 4000 + COMMAND_HEADROOM);
    }

    #[tokio::test(start_paused = true)]
    async fn allowed_fast_flood_is_capped() {
        const N: u32 = 10_000;
        let (mut wire, mut r) = start_with(Bitfield::new(N), N).connect(true).await;
        let writer = tokio::spawn(async move {
            for piece in 0..N {
                send(&mut wire, Message::AllowedFast(piece)).await;
            }
            // A marker after the flood.
            send(&mut wire, Message::Interested).await;
            wire
        });
        let mut fast = Vec::new();
        let mut want = 0;
        loop {
            match r.next().await {
                PeerEvent::AllowedFast { piece, .. } => fast.push(piece),
                PeerEvent::WantBlocks { .. } => want += 1,
                PeerEvent::Interested { .. } => break,
                other => panic!("{other:?}"),
            }
        }
        // The first 64 pieces count, and only the first opened the pipeline.
        assert_eq!(fast, (0..MAX_ALLOWED_FAST as u32).collect::<Vec<_>>());
        assert_eq!(want, 1);
        // The connection is still healthy.
        let _wire = writer.await.expect("writer");
        r.cmd(PeerCommand::Close).await;
        assert_eq!(r.disconnected().await.0, DisconnectReason::Shutdown);
    }

    #[tokio::test(start_paused = true)]
    async fn choke_keeps_requesting_allowed_fast_pieces() {
        let (mut wire, mut r) = start(Bitfield::new(PIECES)).connect(true).await;
        send(&mut wire, Message::AllowedFast(2)).await;
        assert!(matches!(r.next().await, PeerEvent::AllowedFast { .. }));
        assert!(matches!(r.next().await, PeerEvent::WantBlocks { .. }));
        send(&mut wire, Message::Unchoke).await;
        assert_eq!(r.next().await, PeerEvent::Unchoked { key: KEY });
        assert!(matches!(r.next().await, PeerEvent::WantBlocks { .. }));
        // Choked again with nothing outstanding: the torrent drops our
        // demand, so the pipeline asks again for the allowed-fast piece.
        send(&mut wire, Message::Choke).await;
        assert_eq!(r.next().await, PeerEvent::Choked { key: KEY });
        assert_eq!(
            r.next().await,
            PeerEvent::WantBlocks {
                key: KEY,
                count: 32
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn unrequested_or_wrong_length_piece_is_dropped() {
        let (mut wire, mut r) = start(Bitfield::new(PIECES)).connect(true).await;
        unchoke_and_request(&mut wire, &mut r, &[block(0, 0)]).await;

        // Right piece and offset, wrong length.
        send(
            &mut wire,
            Message::Piece {
                piece: 0,
                offset: 0,
                data: Bytes::from(vec![1; 100]),
            },
        )
        .await;
        assert_eq!(
            r.next().await,
            PeerEvent::Wasted {
                key: KEY,
                bytes: 100
            }
        );

        // Never requested.
        send(
            &mut wire,
            Message::Piece {
                piece: 3,
                offset: 0,
                data: Bytes::from(vec![1; 16_384]),
            },
        )
        .await;
        assert_eq!(
            r.next().await,
            PeerEvent::Wasted {
                key: KEY,
                bytes: 16_384,
            }
        );

        // The connection survives and the real block still lands.
        send(
            &mut wire,
            Message::Piece {
                piece: 0,
                offset: 0,
                data: Bytes::from(vec![1; 16_384]),
            },
        )
        .await;
        assert!(matches!(r.next().await, PeerEvent::Block { .. }));

        // A duplicate of a satisfied block is also wasted.
        send(
            &mut wire,
            Message::Piece {
                piece: 0,
                offset: 0,
                data: Bytes::from(vec![1; 16_384]),
            },
        )
        .await;
        assert_eq!(
            r.next().await,
            PeerEvent::WantBlocks {
                key: KEY,
                count: 32
            }
        );
        assert_eq!(
            r.next().await,
            PeerEvent::Wasted {
                key: KEY,
                bytes: 16_384,
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_block_arriving_late_is_wasted() {
        let (mut wire, mut r) = start(Bitfield::new(PIECES)).connect(true).await;
        unchoke_and_request(&mut wire, &mut r, &[block(0, 0)]).await;
        r.cmd(PeerCommand::Cancel(block(0, 0))).await;
        assert_eq!(recv(&mut wire).await, Message::Cancel(block(0, 0)));
        send(
            &mut wire,
            Message::Piece {
                piece: 0,
                offset: 0,
                data: Bytes::from(vec![1; 16_384]),
            },
        )
        .await;
        assert_eq!(
            r.next().await,
            PeerEvent::Wasted {
                key: KEY,
                bytes: 16_384,
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn choke_without_fast_returns_outstanding() {
        let (mut wire, mut r) = start(Bitfield::new(PIECES)).connect(false).await;
        let blocks = [block(0, 0), block(0, 16_384)];
        unchoke_and_request(&mut wire, &mut r, &blocks).await;
        send(&mut wire, Message::Choke).await;
        assert_eq!(r.next().await, PeerEvent::Choked { key: KEY });
        let mut rejected = vec![];
        for _ in 0..2 {
            match r.next().await {
                PeerEvent::Rejected { block, .. } => rejected.push(block),
                other => panic!("{other:?}"),
            }
        }
        rejected.sort();
        assert_eq!(rejected, blocks);
        r.cmd(PeerCommand::Close).await;
        let (_, outstanding) = r.disconnected().await;
        assert!(outstanding.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn choke_with_fast_keeps_outstanding_until_rejected() {
        let (mut wire, mut r) = start(Bitfield::new(PIECES)).connect(true).await;
        let blocks = [block(0, 0), block(0, 16_384)];
        unchoke_and_request(&mut wire, &mut r, &blocks).await;
        send(&mut wire, Message::Choke).await;
        assert_eq!(r.next().await, PeerEvent::Choked { key: KEY });
        send(&mut wire, Message::RejectRequest(block(0, 0))).await;
        assert_eq!(
            r.next().await,
            PeerEvent::Rejected {
                key: KEY,
                block: block(0, 0),
            }
        );
        r.cmd(PeerCommand::Close).await;
        let (_, outstanding) = r.disconnected().await;
        assert_eq!(outstanding, vec![block(0, 16_384)]);
    }

    #[tokio::test(start_paused = true)]
    async fn request_timeout_returns_block() {
        let (mut wire, mut r) = start(Bitfield::new(PIECES)).connect(true).await;
        let t0 = Instant::now();
        unchoke_and_request(&mut wire, &mut r, &[block(0, 0)]).await;
        // The remote keeps the connection alive but never answers.
        let mut keepalive = tokio::time::interval(Duration::from_secs(20));
        let ev = loop {
            tokio::select! {
                ev = r.next() => break ev,
                _ = keepalive.tick() => send(&mut wire, Message::KeepAlive).await,
            }
        };
        assert_eq!(
            ev,
            PeerEvent::Rejected {
                key: KEY,
                block: block(0, 0),
            }
        );
        assert!(t0.elapsed() >= Duration::from_secs(60));
        loop {
            match recv(&mut wire).await {
                Message::KeepAlive => continue,
                m => {
                    assert_eq!(m, Message::Cancel(block(0, 0)));
                    break;
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn idle_timeout_disconnects() {
        let (_wire, mut r) = start(Bitfield::new(PIECES)).connect(true).await;
        let t0 = Instant::now();
        let (reason, _) = r.disconnected().await;
        assert_eq!(reason, DisconnectReason::IdleTimeout);
        assert!(t0.elapsed() >= Duration::from_secs(120));
    }

    #[tokio::test(start_paused = true)]
    async fn keepalive_sent_when_quiet() {
        let (mut wire, _r) = start(Bitfield::new(PIECES)).connect(true).await;
        let t0 = Instant::now();
        assert_eq!(recv(&mut wire).await, Message::KeepAlive);
        assert!(t0.elapsed() >= Duration::from_secs(60));
    }

    #[tokio::test(start_paused = true)]
    async fn incoming_request_flood_is_capped() {
        for fast in [false, true] {
            let (mut wire, mut r) = start(Bitfield::full(PIECES)).connect(fast).await;
            r.cmd(PeerCommand::Unchoke).await;
            assert_eq!(recv(&mut wire).await, Message::Unchoke);
            for i in 0..300u32 {
                let b = BlockRef {
                    piece: i % PIECES,
                    offset: i,
                    len: 16_384,
                };
                send(&mut wire, Message::Request(b)).await;
            }
            for _ in 0..250 {
                assert!(matches!(r.next().await, PeerEvent::Request { .. }));
            }
            if fast {
                // The 50 excess requests are rejected on the wire.
                for i in 250..300u32 {
                    let b = BlockRef {
                        piece: i % PIECES,
                        offset: i,
                        len: 16_384,
                    };
                    assert_eq!(recv(&mut wire).await, Message::RejectRequest(b));
                }
            }
            // Nothing beyond the cap reached the torrent: the next event is
            // the marker sent after the flood.
            send(&mut wire, Message::Interested).await;
            assert_eq!(r.next().await, PeerEvent::Interested { key: KEY });
        }
    }

    #[tokio::test(start_paused = true)]
    async fn serves_queued_requests_only() {
        let (mut wire, mut r) = start(Bitfield::full(PIECES)).connect(true).await;
        r.cmd(PeerCommand::Unchoke).await;
        assert_eq!(recv(&mut wire).await, Message::Unchoke);
        send(&mut wire, Message::Request(block(1, 0))).await;
        assert_eq!(
            r.next().await,
            PeerEvent::Request {
                key: KEY,
                block: block(1, 0),
            }
        );
        let data = Bytes::from(vec![5; 16_384]);
        // Not queued: dropped.
        r.cmd(PeerCommand::SendBlock {
            block: block(2, 0),
            data: data.clone(),
        })
        .await;
        r.cmd(PeerCommand::SendBlock {
            block: block(1, 0),
            data: data.clone(),
        })
        .await;
        assert_eq!(
            recv(&mut wire).await,
            Message::Piece {
                piece: 1,
                offset: 0,
                data,
            }
        );
        // A remote cancel is forwarded.
        send(&mut wire, Message::Request(block(3, 0))).await;
        send(&mut wire, Message::Cancel(block(3, 0))).await;
        assert!(matches!(r.next().await, PeerEvent::Request { .. }));
        assert_eq!(
            r.next().await,
            PeerEvent::Cancel {
                key: KEY,
                block: block(3, 0),
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn remote_eof_is_closed() {
        let (wire, mut r) = start(Bitfield::new(PIECES)).connect(true).await;
        drop(wire);
        let (reason, _) = r.disconnected().await;
        assert_eq!(reason, DisconnectReason::Closed);
    }

    // ----- extension layer ------------------------------------------------

    /// Connects a remote with the given reserved bits; consumes `Connected`.
    async fn connect_ext(
        h: Harness,
        r: Reserved,
    ) -> (Framed<DuplexStream, MessageCodec>, Remote, Handshake) {
        let (wire, mut remote, ours) = h.handshake_with(HASH, THEIR_ID, r).await;
        assert!(matches!(remote.next().await, PeerEvent::Connected { .. }));
        (wire, remote, ours)
    }

    /// Reads our extension handshake off the wire.
    async fn our_ext(wire: &mut Framed<DuplexStream, MessageCodec>) -> ExtHandshake {
        match recv(wire).await {
            Message::Extended { id: 0, payload } => {
                ExtHandshake::decode(&payload).expect("our ext handshake")
            }
            other => panic!("expected the extension handshake, got {other:?}"),
        }
    }

    fn ext_msg(id: u8, payload: Bytes) -> Message {
        Message::Extended { id, payload }
    }

    fn their_ext(ut_metadata: Option<u8>, ut_pex: Option<u8>) -> ExtHandshake {
        let mut hs = ExtHandshake {
            listen_port: Some(7000),
            client: Some("remote/1.0".to_owned()),
            ..ExtHandshake::default()
        };
        if let Some(id) = ut_metadata {
            hs.m.insert(UT_METADATA.to_owned(), id);
        }
        if let Some(id) = ut_pex {
            hs.m.insert(UT_PEX.to_owned(), id);
        }
        hs
    }

    #[tokio::test(start_paused = true)]
    async fn extended_handshake_exchanged() {
        let h = start(Bitfield::new(PIECES));
        let (mut wire, mut r, ours) = connect_ext(h, reserved(true, true, false)).await;
        assert!(!ours.reserved.supports_dht());
        // The first message after the handshake is ours, before the pieces.
        let hs = our_ext(&mut wire).await;
        assert_eq!(hs.m.get(UT_METADATA), Some(&OUR_UT_METADATA_ID));
        assert_eq!(hs.m.get(UT_PEX), Some(&OUR_UT_PEX_ID));
        assert_eq!(hs.listen_port, Some(LISTEN_PORT));
        assert_eq!(hs.reqq, Some(250));
        assert_eq!(hs.your_ip, Some(addr().ip()));
        assert!(hs.client.as_deref().is_some_and(|v| v.starts_with("fbt/")));
        assert_eq!(recv(&mut wire).await, Message::HaveNone);

        let theirs = their_ext(Some(3), None);
        send(&mut wire, ext_msg(0, theirs.encode())).await;
        assert_eq!(
            r.next().await,
            PeerEvent::ExtHandshake {
                key: KEY,
                hs: Box::new(theirs.clone()),
            }
        );
        // A repeat is reported again.
        let again = their_ext(Some(4), Some(5));
        send(&mut wire, ext_msg(0, again.encode())).await;
        assert_eq!(
            r.next().await,
            PeerEvent::ExtHandshake {
                key: KEY,
                hs: Box::new(again),
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn no_extension_messages_without_the_bit() {
        let mut cfg = ext_cfg();
        cfg.dht_port = Some(6882);
        let h = start_ctx(Bitfield::new(PIECES), Some(layout(PIECES)), cfg);
        let (mut wire, _r, _) = connect_ext(h, reserved(true, false, false)).await;
        assert_eq!(recv(&mut wire).await, Message::HaveNone);
        // Nothing else until the keepalive: no extension handshake, no Port.
        assert_eq!(recv(&mut wire).await, Message::KeepAlive);
    }

    #[tokio::test(start_paused = true)]
    async fn pex_not_advertised_when_disabled() {
        let mut cfg = ext_cfg();
        cfg.pex = false;
        cfg.metadata_size = Some(4321);
        let h = start_ctx(Bitfield::new(PIECES), Some(layout(PIECES)), cfg);
        let (mut wire, _r, _) = connect_ext(h, reserved(true, true, false)).await;
        let hs = our_ext(&mut wire).await;
        assert_eq!(hs.m.get(UT_METADATA), Some(&OUR_UT_METADATA_ID));
        assert!(!hs.m.contains_key(UT_PEX));
        assert_eq!(hs.metadata_size, Some(4321));
    }

    #[tokio::test(start_paused = true)]
    async fn port_sent_and_received_once() {
        let mut cfg = ext_cfg();
        cfg.dht_port = Some(6882);
        let h = start_ctx(Bitfield::new(PIECES), Some(layout(PIECES)), cfg);
        let (mut wire, mut r, ours) = connect_ext(h, reserved(true, false, true)).await;
        assert!(ours.reserved.supports_dht());
        assert_eq!(recv(&mut wire).await, Message::HaveNone);
        assert_eq!(recv(&mut wire).await, Message::Port(6882));
        // Port 0 is ignored, the first real one reported, later ones ignored.
        for port in [0, 7000, 7001] {
            send(&mut wire, Message::Port(port)).await;
        }
        send(&mut wire, Message::Interested).await;
        assert_eq!(
            r.next().await,
            PeerEvent::DhtPort {
                key: KEY,
                port: 7000
            }
        );
        assert_eq!(r.next().await, PeerEvent::Interested { key: KEY });

        // Without the remote's DHT bit we send no Port.
        let mut cfg = ext_cfg();
        cfg.dht_port = Some(6882);
        let h = start_ctx(Bitfield::new(PIECES), Some(layout(PIECES)), cfg);
        let (mut wire, _r, _) = connect_ext(h, reserved(true, false, false)).await;
        assert_eq!(recv(&mut wire).await, Message::HaveNone);
        assert_eq!(recv(&mut wire).await, Message::KeepAlive);
    }

    #[tokio::test(start_paused = true)]
    async fn ext_messages_routed_by_our_ids() {
        let h = start(Bitfield::new(PIECES));
        let (mut wire, mut r, _) = connect_ext(h, reserved(true, true, false)).await;
        our_ext(&mut wire).await;
        let request = MetadataMsg::Request { piece: 1 };
        send(&mut wire, ext_msg(OUR_UT_METADATA_ID, request.encode())).await;
        assert_eq!(
            r.next().await,
            PeerEvent::Metadata {
                key: KEY,
                msg: request,
            }
        );
        let pex = PexMsg {
            added: vec![(SocketAddr::from(([10, 0, 0, 1], 6881)), PexFlags::SEED)],
            dropped: vec![],
        };
        send(&mut wire, ext_msg(OUR_UT_PEX_ID, pex.encode())).await;
        assert_eq!(r.next().await, PeerEvent::Pex { key: KEY, msg: pex });
        // An id we never assigned is ignored; the connection stays up.
        send(&mut wire, ext_msg(9, Bytes::from_static(b"junk"))).await;
        send(&mut wire, Message::Interested).await;
        assert_eq!(r.next().await, PeerEvent::Interested { key: KEY });
    }

    #[tokio::test(start_paused = true)]
    async fn malformed_ut_metadata_is_protocol_error() {
        for (id, payload) in [
            (OUR_UT_METADATA_ID, &b"garbage"[..]),
            (OUR_UT_METADATA_ID, b"d8:msg_typei7e5:piecei0ee"),
            (OUR_UT_PEX_ID, b"d5:added3:abce"),
            (0, b"not bencode"),
        ] {
            let h = start(Bitfield::new(PIECES));
            let (mut wire, mut r, _) = connect_ext(h, reserved(true, true, false)).await;
            send(&mut wire, ext_msg(id, Bytes::copy_from_slice(payload))).await;
            let (reason, _) = r.disconnected().await;
            assert!(
                matches!(reason, DisconnectReason::Protocol(_)),
                "{id}: {reason:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn send_uses_remote_ids_and_drops_unsupported() {
        let h = start(Bitfield::new(PIECES));
        let (mut wire, mut r, _) = connect_ext(h, reserved(true, true, false)).await;
        our_ext(&mut wire).await;
        assert_eq!(recv(&mut wire).await, Message::HaveNone);
        send(&mut wire, ext_msg(0, their_ext(Some(7), None).encode())).await;
        assert!(matches!(r.next().await, PeerEvent::ExtHandshake { .. }));
        r.cmd(PeerCommand::SendPex(PexMsg::default())).await;
        let msg = MetadataMsg::Reject { piece: 3 };
        r.cmd(PeerCommand::SendMetadata(msg.clone())).await;
        // The PEX message was dropped: the next frame is the metadata one.
        assert_eq!(recv(&mut wire).await, ext_msg(7, msg.encode()));
    }

    #[tokio::test(start_paused = true)]
    async fn availability_before_metadata_is_replayed() {
        let h = start_ctx(Bitfield::new(0), None, ext_cfg());
        let (mut wire, mut r, _) = connect_ext(h, reserved(true, false, false)).await;
        // No metadata, fast: we announce nothing but HaveNone.
        assert_eq!(recv(&mut wire).await, Message::HaveNone);
        send(
            &mut wire,
            Message::Bitfield(Bytes::from_static(&[0b1010_0000])),
        )
        .await;
        send(&mut wire, Message::Have(3)).await;
        // Requests cannot be served yet: rejected under fast.
        send(&mut wire, Message::Request(block(0, 0))).await;
        assert_eq!(recv(&mut wire).await, Message::RejectRequest(block(0, 0)));
        send(&mut wire, Message::Interested).await;
        // Nothing about pieces reached the torrent: the marker comes first.
        assert_eq!(r.next().await, PeerEvent::Interested { key: KEY });

        r.cmd(PeerCommand::MetadataReady { layout: layout(4) })
            .await;
        let bits = Bitfield::from_bytes(&[0b1010_0000], 4).expect("bits");
        assert_eq!(
            r.next().await,
            PeerEvent::Pieces {
                key: KEY,
                pieces: PeerPiecesOwned::Bits(bits),
            }
        );
        assert_eq!(r.next().await, PeerEvent::Have { key: KEY, piece: 3 });
        // Normal mode from now on: an out-of-range Have is a protocol error.
        send(&mut wire, Message::Have(4)).await;
        let (reason, _) = r.disconnected().await;
        assert!(
            matches!(reason, DisconnectReason::Protocol(_)),
            "{reason:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn invalid_buffered_bitfield_fails_at_metadata() {
        for bad in [&[0xffu8][..], &[0xf0, 0]] {
            let h = start_ctx(Bitfield::new(0), None, ext_cfg());
            let (mut wire, mut r, _) = connect_ext(h, reserved(true, false, false)).await;
            send(&mut wire, Message::Bitfield(Bytes::copy_from_slice(bad))).await;
            send(&mut wire, Message::Have(9)).await;
            send(&mut wire, Message::Interested).await;
            // Buffered unvalidated: the connection is still up.
            assert_eq!(r.next().await, PeerEvent::Interested { key: KEY });
            r.cmd(PeerCommand::MetadataReady { layout: layout(4) })
                .await;
            let (reason, _) = r.disconnected().await;
            assert!(
                matches!(reason, DisconnectReason::Protocol(_)),
                "{bad:?}: {reason:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn pending_haves_are_bounded() {
        const HAVES: u32 = 100_000;
        let h = start_ctx(Bitfield::new(0), None, ext_cfg());
        let (wire, mut r, _) = connect_ext(h, reserved(false, false, false)).await;
        let writer = tokio::spawn(async move {
            let mut wire = wire;
            for piece in 0..HAVES {
                wire.feed(Message::Have(piece)).await.expect("feed");
            }
            send(&mut wire, Message::Interested).await;
            wire
        });
        assert_eq!(r.next().await, PeerEvent::Interested { key: KEY });
        let mut wire = writer.await.expect("writer");
        r.cmd(PeerCommand::MetadataReady {
            layout: layout(200_000),
        })
        .await;
        // All replayed Haves are emitted while handling the command, so a
        // marker sent after the first one arrives after the last.
        let mut haves = match r.next().await {
            PeerEvent::Have { piece, .. } => vec![piece],
            other => panic!("{other:?}"),
        };
        send(&mut wire, Message::NotInterested).await;
        loop {
            match r.next().await {
                PeerEvent::Have { piece, .. } => haves.push(piece),
                PeerEvent::NotInterested { .. } => break,
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(haves.len(), MAX_PENDING_HAVES);
        assert_eq!(haves, (0..MAX_PENDING_HAVES as u32).collect::<Vec<_>>());
    }
}
