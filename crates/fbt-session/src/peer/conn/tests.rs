use std::time::Duration;

use bytes::Bytes;
use fbt_core::BlockRef;
use fbt_wire::{
    ExtHandshake, Message, MessageCodec, MetadataMsg, OUR_UT_METADATA_ID, OUR_UT_PEX_ID, PexFlags,
    PexMsg, Reserved, UT_METADATA, UT_PEX, read_handshake, write_handshake,
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
        encrypted: false,
        transport: crate::peer::transport::Transport::Tcp,
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
            connect_failed: None,
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
            connect_failed: None,
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
            encrypted: false,
            transport: crate::peer::transport::Transport::Tcp,
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

/// Without `ut_pex` advertised (a private torrent, PEX disabled) its id
/// is not ours: a PEX message, malformed or not, is ignored and the
/// connection stays up.
#[tokio::test(start_paused = true)]
async fn ut_pex_ignored_unless_advertised() {
    let mut cfg = ext_cfg();
    cfg.pex = false;
    let h = start_ctx(Bitfield::new(PIECES), Some(layout(PIECES)), cfg);
    let (mut wire, mut r, _) = connect_ext(h, reserved(true, true, false)).await;
    our_ext(&mut wire).await;
    let pex = PexMsg {
        added: vec![(SocketAddr::from(([10, 0, 0, 1], 6881)), PexFlags::SEED)],
        dropped: vec![],
    };
    send(&mut wire, ext_msg(OUR_UT_PEX_ID, pex.encode())).await;
    send(
        &mut wire,
        ext_msg(OUR_UT_PEX_ID, Bytes::from_static(b"d5:added3:abce")),
    )
    .await;
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

    r.cmd(PeerCommand::MetadataReady {
        layout: layout(4),
        ext: ext_cfg(),
    })
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
async fn metadata_ready_repeats_the_extension_handshake() {
    let mut cfg = ext_cfg();
    cfg.metadata_size = None;
    let h = start_ctx(Bitfield::new(0), None, cfg);
    let (mut wire, r, _) = connect_ext(h, reserved(true, true, false)).await;
    let first = our_ext(&mut wire).await;
    assert_eq!(first.metadata_size, None);
    assert_eq!(first.m.get(UT_PEX), Some(&OUR_UT_PEX_ID));
    assert_eq!(recv(&mut wire).await, Message::HaveNone);
    // Private after all: the repeat carries the size and turns PEX off.
    let mut now = ext_cfg();
    now.metadata_size = Some(4321);
    now.pex = false;
    r.cmd(PeerCommand::MetadataReady {
        layout: layout(4),
        ext: now,
    })
    .await;
    let again = our_ext(&mut wire).await;
    assert_eq!(again.metadata_size, Some(4321));
    assert_eq!(again.m.get(UT_PEX), Some(&0));
    assert_eq!(again.m.get(UT_METADATA), Some(&OUR_UT_METADATA_ID));
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
        r.cmd(PeerCommand::MetadataReady {
            layout: layout(4),
            ext: ext_cfg(),
        })
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
        ext: ext_cfg(),
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
