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

use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
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

use super::messages::{DisconnectReason, PeerCommand, PeerEvent, PeerPiecesOwned};
use crate::picker::PeerKey;
use crate::settings::Settings;

/// Capacity of each peer task's command channel.
const COMMAND_QUEUE: usize = 256;

/// Everything a peer task needs from its torrent.
#[derive(Debug, Clone)]
pub(crate) struct PeerCtx {
    pub key: PeerKey,
    pub info_hash: [u8; 20],
    pub our_id: PeerId,
    pub layout: PieceLayout,
    pub have: Bitfield,
    pub settings: Arc<Settings>,
    pub events: mpsc::Sender<PeerEvent>,
}

/// Connects to `addr` and runs the peer task; returns its command channel.
///
/// A failed or timed-out connect is reported as `Disconnected` with
/// [`DisconnectReason::Io`].
pub(crate) fn spawn_outgoing(addr: SocketAddr, ctx: PeerCtx) -> mpsc::Sender<PeerCommand> {
    let (tx, rx) = mpsc::channel(COMMAND_QUEUE);
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
    let (tx, rx) = mpsc::channel(COMMAND_QUEUE);
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
            let mut conn = Conn::new(stream, &theirs, &ctx);
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

/// State of one established connection.
struct Conn<S> {
    framed: Framed<S, MessageCodec>,
    key: PeerKey,
    events: mpsc::Sender<PeerEvent>,
    settings: Arc<Settings>,
    num_pieces: u32,
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
    /// Pieces the remote lets us request while choked (at most `num_pieces`).
    allowed_fast: HashSet<u32>,
    /// Our requests in flight, with their send time.
    outstanding: BTreeMap<BlockRef, Instant>,
    /// Remote requests forwarded to the torrent and not yet answered.
    incoming: HashSet<BlockRef>,
    last_recv: Instant,
    last_send: Instant,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Conn<S> {
    fn new(stream: S, theirs: &Handshake, ctx: &PeerCtx) -> Self {
        let num_pieces = ctx.layout.num_pieces();
        let max_len = (BLOCK_SIZE + 13)
            .max(num_pieces.div_ceil(8).saturating_add(1))
            .max(1 << 20);
        let now = Instant::now();
        Conn {
            framed: Framed::new(stream, MessageCodec::new(max_len)),
            key: ctx.key,
            events: ctx.events.clone(),
            settings: Arc::clone(&ctx.settings),
            num_pieces,
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

    /// Sends our pieces, then loops until the connection ends.
    async fn serve(
        &mut self,
        cmds: &mut mpsc::Receiver<PeerCommand>,
        have: &Bitfield,
    ) -> DisconnectReason {
        let pieces = if self.fast && have.is_full() {
            Message::HaveAll
        } else if self.fast && have.count_ones() == 0 {
            Message::HaveNone
        } else {
            Message::Bitfield(Bytes::copy_from_slice(have.as_bytes()))
        };
        if let Err(reason) = self.send(pieces).await {
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
                if piece >= self.num_pieces {
                    return Err(protocol("have index out of range"));
                }
                self.pieces_seen = true;
                self.emit(PeerEvent::Have { key, piece }).await?;
            }
            Message::Bitfield(bytes) => {
                let bits = Bitfield::from_bytes(&bytes, self.num_pieces)
                    .map_err(|e| DisconnectReason::Protocol(e.to_string()))?;
                self.initial_pieces(PeerPiecesOwned::Bits(bits)).await?;
            }
            Message::HaveAll => {
                self.require_fast()?;
                self.initial_pieces(PeerPiecesOwned::All).await?;
            }
            Message::HaveNone => {
                self.require_fast()?;
                self.initial_pieces(PeerPiecesOwned::None).await?;
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
                if piece < self.num_pieces && self.allowed_fast.insert(piece) {
                    self.emit(PeerEvent::AllowedFast { key, piece }).await?;
                    self.want_blocks().await?;
                }
            }
            Message::SuggestPiece(_) => self.require_fast()?,
            Message::KeepAlive
            | Message::Port(_)
            | Message::Extended { .. }
            | Message::Unknown { .. } => {}
        }
        Ok(())
    }

    fn require_fast(&self) -> Flow {
        if self.fast {
            Ok(())
        } else {
            Err(protocol("fast extension message without fast support"))
        }
    }

    /// A bitfield, `HaveAll` or `HaveNone`: only before any other piece
    /// announcement.
    async fn initial_pieces(&mut self, pieces: PeerPiecesOwned) -> Flow {
        if self.pieces_seen {
            return Err(protocol("piece set after earlier piece announcements"));
        }
        self.pieces_seen = true;
        self.emit(PeerEvent::Pieces {
            key: self.key,
            pieces,
        })
        .await
    }

    async fn on_remote_request(&mut self, block: BlockRef) -> Flow {
        if self.choking || self.incoming.len() >= self.settings.max_incoming_requests {
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
    use fbt_wire::{Message, MessageCodec, Reserved, read_handshake, write_handshake};
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
        let (ours, remote) = tokio::io::duplex(1 << 20);
        let (etx, events) = mpsc::channel(1024);
        let (cmds, crx) = mpsc::channel(256);
        let ctx = PeerCtx {
            key: KEY,
            info_hash: HASH,
            our_id: OUR_ID,
            layout: PieceLayout::new(u64::from(PIECE) * u64::from(PIECES), PIECE).expect("layout"),
            have,
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

    fn reserved(fast: bool) -> Reserved {
        let mut r = Reserved::default();
        if fast {
            r.set_fast();
        }
        r
    }

    impl Harness {
        /// Answers our handshake and returns the framed remote end.
        async fn handshake(
            mut self,
            info_hash: [u8; 20],
            peer_id: PeerId,
            fast: bool,
        ) -> (Framed<DuplexStream, MessageCodec>, Remote) {
            let ours = read_handshake(&mut self.remote)
                .await
                .expect("our handshake");
            assert!(ours.reserved.supports_fast());
            assert_eq!(ours.info_hash, HASH);
            assert_eq!(ours.peer_id, OUR_ID);
            let theirs = Handshake {
                reserved: reserved(fast),
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
                reserved: reserved(true),
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
}
