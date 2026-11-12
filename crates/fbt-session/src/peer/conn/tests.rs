use std::time::Duration;

use bytes::Bytes;
use fbt_core::BlockRef;
use fbt_wire::{
    ExtHandshake, LT_DONTHAVE, Message, MessageCodec, MetadataMsg, OUR_LT_DONTHAVE_ID,
    OUR_UPLOAD_ONLY_ID, OUR_UT_METADATA_ID, OUR_UT_PEX_ID, PexFlags, PexMsg, Reserved, UPLOAD_ONLY,
    UT_METADATA, UT_PEX, decode_donthave, decode_upload_only, encode_donthave, encode_upload_only,
    read_handshake, write_handshake,
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
        upload_only: false,
        holepunch: false,
    }
}

/// Starts a peer task with an explicit layout (`None`: no metadata yet)
/// and extension config.
fn start_ctx(have: Bitfield, layout: Option<PieceLayout>, ext: ExtConfig) -> Harness {
    start_full(have, layout, ext, Settings::default())
}

/// [`start_ctx`] with `settings`.
fn start_full(
    have: Bitfield,
    layout: Option<PieceLayout>,
    ext: ExtConfig,
    settings: Settings,
) -> Harness {
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
        settings: Arc::new(settings),
        events: etx,
        encrypted: false,
        transport: crate::peer::transport::Transport::Tcp,
        bandwidth: Arc::new(crate::bandwidth::Bandwidth::new(&Settings::default())),
        torrent_rate: crate::bandwidth::BucketPair::new(Default::default()),
        external_ip: Arc::new(crate::net::ExternalIp::new(
            tokio::sync::broadcast::channel(1).0,
        )),
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
    assert_eq!(r.next().await, PeerEvent::WantBlocks { key: KEY, count: 2 });
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

    // A delivered block is forwarded, and the freed request (at least a
    // quarter of the depth) is asked for again: the block just read warms
    // the rate meter to 3 blocks of depth, 2 of them free.
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
    assert_eq!(r.next().await, PeerEvent::WantBlocks { key: KEY, count: 2 });

    // Closing reports what is still outstanding.
    r.cmd(PeerCommand::Close).await;
    let PeerEvent::Disconnected {
        reason,
        outstanding,
        ..
    } = r.next().await
    else {
        panic!("Disconnected next");
    };
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
    assert_eq!(r.next().await, PeerEvent::WantBlocks { key: KEY, count: 2 });
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
    assert_eq!(r.next().await, PeerEvent::WantBlocks { key: KEY, count: 2 });
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
    assert_eq!(r.next().await, PeerEvent::WantBlocks { key: KEY, count: 3 });
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
    // The cancel frees pipeline room, asked for again at once (an end-game
    // duplicate cancelled this way must not leave the peer idle).
    assert_eq!(r.next().await, PeerEvent::WantBlocks { key: KEY, count: 2 });
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
    let mut events = Vec::new();
    while events.len() < 2 {
        tokio::select! {
            ev = r.next() => events.push(ev),
            _ = keepalive.tick() => send(&mut wire, Message::KeepAlive).await,
        }
    }
    // With the default timeouts (both 60 s) the snub comes first.
    assert_eq!(
        events,
        vec![
            PeerEvent::Snubbed {
                key: KEY,
                snubbed: true
            },
            PeerEvent::Rejected {
                key: KEY,
                block: block(0, 0),
            }
        ]
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
    // Reported sent once written, the only point the upload counts.
    assert_eq!(
        r.next().await,
        PeerEvent::BlockSent {
            key: KEY,
            block: block(1, 0),
            len: 16_384,
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

/// `lt_donthave` (BEP 54) is routed by our id and range-checked like
/// `Have`; the command sends it with the remote's id, or drops it when the
/// remote lacks the extension.
#[tokio::test(start_paused = true)]
async fn donthave_routed_and_range_checked() {
    let h = start(Bitfield::new(PIECES));
    let (mut wire, mut r, _) = connect_ext(h, reserved(true, true, false)).await;
    let hs = our_ext(&mut wire).await;
    assert_eq!(hs.m.get(LT_DONTHAVE), Some(&OUR_LT_DONTHAVE_ID));
    assert_eq!(recv(&mut wire).await, Message::HaveNone);
    send(&mut wire, ext_msg(OUR_LT_DONTHAVE_ID, encode_donthave(2))).await;
    assert_eq!(r.next().await, PeerEvent::DontHave { key: KEY, piece: 2 });

    // Not advertised by the remote yet: dropped, so the next frame is the
    // `Have` that follows it.
    r.cmd(PeerCommand::DontHave(1)).await;
    r.cmd(PeerCommand::Have(0)).await;
    assert_eq!(recv(&mut wire).await, Message::Have(0));
    let mut theirs = their_ext(None, None);
    theirs.m.insert(LT_DONTHAVE.to_owned(), 9);
    send(&mut wire, ext_msg(0, theirs.encode())).await;
    assert!(matches!(r.next().await, PeerEvent::ExtHandshake { .. }));
    r.cmd(PeerCommand::DontHave(1)).await;
    match recv(&mut wire).await {
        Message::Extended { id: 9, payload } => assert_eq!(decode_donthave(&payload).ok(), Some(1)),
        other => panic!("expected lt_donthave, got {other:?}"),
    }

    // Out of range is a protocol error; a short payload too.
    send(
        &mut wire,
        ext_msg(OUR_LT_DONTHAVE_ID, encode_donthave(PIECES)),
    )
    .await;
    let (reason, _) = r.disconnected().await;
    assert!(
        matches!(reason, DisconnectReason::Protocol(_)),
        "{reason:?}"
    );
    let h = start(Bitfield::new(PIECES));
    let (mut wire, mut r, _) = connect_ext(h, reserved(true, true, false)).await;
    send(
        &mut wire,
        ext_msg(OUR_LT_DONTHAVE_ID, Bytes::from_static(b"\x00\x01")),
    )
    .await;
    let (reason, _) = r.disconnected().await;
    assert!(
        matches!(reason, DisconnectReason::Protocol(_)),
        "{reason:?}"
    );
}

/// BEP 21: `upload_only` arrives as a handshake key or a message, and
/// `UploadOnly` sends it to a remote that speaks it; our own handshake
/// carries the key only once we are upload-only.
#[tokio::test(start_paused = true)]
async fn upload_only_from_handshake_and_message() {
    let h = start(Bitfield::new(PIECES));
    let (mut wire, mut r, _) = connect_ext(h, reserved(true, true, false)).await;
    let hs = our_ext(&mut wire).await;
    assert_eq!(hs.m.get(UPLOAD_ONLY), Some(&OUR_UPLOAD_ONLY_ID));
    assert_eq!(hs.upload_only, None);
    assert_eq!(recv(&mut wire).await, Message::HaveNone);

    let mut theirs = their_ext(None, None);
    theirs.m.insert(UPLOAD_ONLY.to_owned(), 5);
    theirs.upload_only = Some(true);
    send(&mut wire, ext_msg(0, theirs.encode())).await;
    assert!(matches!(r.next().await, PeerEvent::ExtHandshake { .. }));
    assert_eq!(r.next().await, PeerEvent::UploadOnly { key: KEY, on: true });
    send(
        &mut wire,
        ext_msg(OUR_UPLOAD_ONLY_ID, encode_upload_only(false)),
    )
    .await;
    assert_eq!(
        r.next().await,
        PeerEvent::UploadOnly {
            key: KEY,
            on: false
        }
    );
    send(
        &mut wire,
        ext_msg(OUR_UPLOAD_ONLY_ID, encode_upload_only(true)),
    )
    .await;
    assert_eq!(r.next().await, PeerEvent::UploadOnly { key: KEY, on: true });
    // A repeated handshake without the key turns it off again.
    let mut repeat = their_ext(None, None);
    repeat.m.insert(UPLOAD_ONLY.to_owned(), 5);
    send(&mut wire, ext_msg(0, repeat.encode())).await;
    assert!(matches!(r.next().await, PeerEvent::ExtHandshake { .. }));
    assert_eq!(
        r.next().await,
        PeerEvent::UploadOnly {
            key: KEY,
            on: false
        }
    );

    r.cmd(PeerCommand::UploadOnly(true)).await;
    match recv(&mut wire).await {
        Message::Extended { id: 5, payload } => {
            assert_eq!(decode_upload_only(&payload).ok(), Some(true))
        }
        other => panic!("expected upload_only, got {other:?}"),
    }

    // A finished torrent says so in its handshake.
    let mut cfg = ext_cfg();
    cfg.upload_only = true;
    let h = start_ctx(Bitfield::full(PIECES), Some(layout(PIECES)), cfg);
    let (mut wire, _r, _) = connect_ext(h, reserved(true, true, false)).await;
    assert_eq!(our_ext(&mut wire).await.upload_only, Some(true));
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

// ----- rate limits ------------------------------------------------------------

/// A connection built directly (no task), with fast, unchoking the
/// remote, whose TCP class limits are `down` and `up` bytes per second.
struct Direct {
    conn: state::Conn<DuplexStream>,
    _remote: DuplexStream,
    events: mpsc::Receiver<PeerEvent>,
    bandwidth: Arc<crate::bandwidth::Bandwidth>,
}

fn direct(settings: Settings, down: u64, up: u64) -> Direct {
    use crate::bandwidth::{Bandwidth, BucketPair, PeerClassLimits, RateLimit};
    let (ours, remote) = tokio::io::duplex(1 << 20);
    let (etx, events) = mpsc::channel(4096);
    let settings = Arc::new(settings);
    let bandwidth = Arc::new(Bandwidth::new(&settings));
    bandwidth.set_classes(PeerClassLimits {
        tcp: RateLimit::new(down, up),
        ..PeerClassLimits::default()
    });
    let ctx = PeerCtx {
        key: KEY,
        info_hash: HASH,
        our_id: OUR_ID,
        layout: Some(layout(PIECES)),
        have: Bitfield::full(PIECES),
        ext: ext_cfg(),
        settings,
        events: etx,
        encrypted: false,
        transport: Transport::Tcp,
        bandwidth: Arc::clone(&bandwidth),
        torrent_rate: BucketPair::new(RateLimit::default()),
        external_ip: Arc::new(crate::net::ExternalIp::new(
            tokio::sync::broadcast::channel(1).0,
        )),
    };
    let theirs = Handshake {
        reserved: reserved(true, false, false),
        info_hash: HASH,
        peer_id: THEIR_ID,
    };
    let throttles = bandwidth.peer(Transport::Tcp, addr().ip(), &ctx.torrent_rate);
    let mut conn = state::Conn::new(ours, &theirs, addr().ip(), &ctx, &throttles);
    conn.choking = false;
    Direct {
        conn,
        _remote: remote,
        events,
        bandwidth,
    }
}

impl Direct {
    /// Lifts the upload limit and writes whatever is queued.
    async fn unlimit_and_drain(&mut self) {
        self.bandwidth
            .set_classes(crate::bandwidth::PeerClassLimits::default());
        self.conn.drain_uploads().await.expect("drain");
    }

    /// The `BlockSent` events so far.
    fn sent(&mut self) -> Vec<BlockRef> {
        let mut out = Vec::new();
        while let Ok(ev) = self.events.try_recv() {
            if let PeerEvent::BlockSent { block, .. } = ev {
                out.push(block);
            }
        }
        out
    }
}

/// A remote looping Request, (answer), Cancel on one block while the
/// upload is throttled queues it once: the queue stays bounded and the
/// block goes out once.
#[tokio::test(start_paused = true)]
async fn requeued_block_is_queued_and_sent_once() {
    let s = Settings {
        max_incoming_requests: 4,
        ..Settings::default()
    };
    let mut d = direct(s, 0, 1);
    // In debt for a long time: nothing drains.
    d.conn.up.charge(1_000_000);
    let data = Bytes::from(vec![9; 16_384]);
    let x = block(1, 0);
    for _ in 0..1000 {
        d.conn
            .on_message(Message::Request(x))
            .await
            .expect("request");
        d.conn
            .on_command(PeerCommand::SendBlock {
                block: x,
                data: data.clone(),
            })
            .await
            .expect("send block");
        d.conn.on_message(Message::Cancel(x)).await.expect("cancel");
        d.conn.drain_uploads().await.expect("drain");
        assert!(d.conn.uploads.len() <= 4, "{}", d.conn.uploads.len());
    }
    assert_eq!(d.conn.uploads.len(), 1);
    // Requested once more: the queued entry answers it, once.
    d.conn
        .on_message(Message::Request(x))
        .await
        .expect("request");
    d.conn
        .on_command(PeerCommand::SendBlock { block: x, data })
        .await
        .expect("send block");
    assert_eq!(d.conn.uploads.len(), 1);
    d.unlimit_and_drain().await;
    assert_eq!(d.sent(), vec![x]);
    assert!(d.conn.uploads.is_empty() && d.conn.queued.is_empty());
}

/// Distinct blocks fill the queue up to `max_incoming_requests` at most.
#[tokio::test(start_paused = true)]
async fn upload_queue_is_bounded_by_incoming_requests() {
    let s = Settings {
        max_incoming_requests: 3,
        ..Settings::default()
    };
    let mut d = direct(s, 0, 1);
    d.conn.up.charge(1_000_000);
    let data = Bytes::from(vec![9; 16_384]);
    for i in 0..PIECES {
        for off in [0, 16_384] {
            let b = block(i, off);
            d.conn
                .on_message(Message::Request(b))
                .await
                .expect("request");
            d.conn
                .on_command(PeerCommand::SendBlock {
                    block: b,
                    data: data.clone(),
                })
                .await
                .expect("send block");
            assert!(d.conn.uploads.len() <= 3);
        }
    }
    assert_eq!(d.conn.uploads.len(), 3);
}

/// Blocks still queued when we choke are never written or reported sent,
/// so they never count as uploaded.
#[tokio::test(start_paused = true)]
async fn choke_drops_queued_blocks() {
    let mut d = direct(Settings::default(), 0, 1);
    d.conn.up.charge(1_000_000);
    let data = Bytes::from(vec![9; 16_384]);
    for b in [block(0, 0), block(0, 16_384)] {
        d.conn
            .on_message(Message::Request(b))
            .await
            .expect("request");
        d.conn
            .on_command(PeerCommand::SendBlock {
                block: b,
                data: data.clone(),
            })
            .await
            .expect("send block");
    }
    assert_eq!(d.conn.uploads.len(), 2);
    d.conn.on_command(PeerCommand::Choke).await.expect("choke");
    d.unlimit_and_drain().await;
    assert!(d.sent().is_empty());
    assert!(d.conn.uploads.is_empty() && d.conn.queued.is_empty());
}

/// A read hold pauses the idle timer and the request timeouts: when it
/// ends, both resume where they stood.
#[tokio::test(start_paused = true)]
async fn read_hold_pauses_idle_and_request_timers() {
    let s = Settings {
        request_timeout: Duration::from_secs(2),
        peer_idle_timeout: Duration::from_secs(3),
        ..Settings::default()
    };
    let mut d = direct(s, 0, 0);
    let t0 = Instant::now();
    d.conn.last_recv = t0;
    d.conn.outstanding.insert(block(0, 0), t0);
    assert_eq!(d.conn.next_deadline(), t0 + Duration::from_secs(2));
    d.conn.hold_reads(true, t0 + Duration::from_secs(1));
    // Neither the idle nor the request deadline counts while held: only
    // the keepalive.
    assert_eq!(
        d.conn.next_deadline(),
        d.conn.last_send + d.conn.settings.keepalive_interval
    );
    d.conn.hold_reads(false, t0 + Duration::from_secs(4));
    assert_eq!(d.conn.last_recv, t0 + Duration::from_secs(3));
    assert_eq!(
        d.conn.outstanding[&block(0, 0)],
        t0 + Duration::from_secs(3)
    );
    assert!(d.conn.held_since.is_none());
    // Shifted no later than now.
    d.conn.hold_reads(true, t0 + Duration::from_secs(4));
    d.conn.last_recv = t0 + Duration::from_secs(5);
    d.conn.hold_reads(false, t0 + Duration::from_secs(6));
    assert_eq!(d.conn.last_recv, t0 + Duration::from_secs(6));
}

/// A hold lasts at most `peer_idle_timeout`: then one frame may be read
/// whatever the debt, and the hold starts over after it.
#[tokio::test(start_paused = true)]
async fn read_hold_is_capped_at_the_idle_timeout() {
    let s = Settings::default();
    let idle = s.peer_idle_timeout;
    let mut d = direct(s, 1, 0);
    d.conn.down.charge(1_000_000);
    let t0 = Instant::now();
    assert!(d.conn.read_held(t0));
    assert!(d.conn.read_held(t0 + idle - Duration::from_secs(1)));
    assert!(!d.conn.read_held(t0 + idle), "one frame may be read");
    assert!(d.conn.floor_read && d.conn.held_since.is_none());
    // Until that frame arrives the idle timer runs again.
    assert!(!d.conn.read_held(t0 + idle + Duration::from_secs(5)));
    d.conn.floor_read = false;
    assert!(d.conn.read_held(t0 + idle + Duration::from_secs(6)));
}

// ----- adaptive pipeline and snubbing -------------------------------------------

/// What a scripted-peer loop saw next.
enum Step {
    Event(PeerEvent),
    Frame(Message),
    Tick,
    End,
}

/// A remote serving 2 MiB/s (a block every 1/128 s) as long as it has
/// requests: the pipeline deepens with the measured rate, well past the
/// old fixed 32 within 2 s.
#[tokio::test(start_paused = true)]
async fn pipeline_grows_with_rate() {
    const MANY: u32 = 4096;
    let (mut wire, mut r) = start_with(Bitfield::new(MANY), MANY).connect(true).await;
    send(&mut wire, Message::Unchoke).await;
    assert_eq!(r.next().await, PeerEvent::Unchoked { key: KEY });
    let t0 = Instant::now();
    let end = t0 + Duration::from_secs(2);
    let mut tick = tokio::time::interval(Duration::from_micros(7_812));
    let mut queue: std::collections::VecDeque<BlockRef> = Default::default();
    let (mut next, mut requested, mut received, mut most) = (0u32, 0usize, 0usize, 0usize);
    let mut first = None;
    while most <= 32 {
        let step = tokio::select! {
            ev = r.next() => Step::Event(ev),
            m = wire.next() => Step::Frame(m.expect("frame").expect("decode")),
            _ = tick.tick() => Step::Tick,
            () = tokio::time::sleep_until(end) => Step::End,
        };
        match step {
            Step::Event(PeerEvent::WantBlocks { count, .. }) => {
                first.get_or_insert(count);
                let blocks: Vec<BlockRef> = (next..next + count as u32)
                    .map(|i| block(i / 2, (i % 2) * 16_384))
                    .collect();
                next += count as u32;
                requested += blocks.len();
                r.cmd(PeerCommand::Request(blocks)).await;
            }
            Step::Event(PeerEvent::Block { .. }) => received += 1,
            Step::Event(other) => panic!("{other:?}"),
            Step::Frame(Message::Request(b)) => queue.push_back(b),
            Step::Frame(Message::KeepAlive) => {}
            Step::Frame(other) => panic!("{other:?}"),
            Step::Tick => {
                if let Some(b) = queue.pop_front() {
                    let data = Bytes::from(vec![7; b.len as usize]);
                    let piece = Message::Piece {
                        piece: b.piece,
                        offset: b.offset,
                        data,
                    };
                    send(&mut wire, piece).await;
                }
            }
            Step::End => break,
        }
        most = most.max(requested - received);
    }
    assert_eq!(first, Some(2), "a new peer starts at the minimum");
    assert!(most > 32, "at most {most} outstanding");
    assert!(t0.elapsed() <= Duration::from_secs(2));
}

#[test]
fn pipeline_minimum_is_two() {
    use super::outbound::pipeline_depth;
    let q = Duration::from_secs(3);
    assert_eq!(pipeline_depth(0, q, 500, None, false), 2);
    assert_eq!(pipeline_depth(5_000, q, 500, None, false), 2);
    // 1 MiB/s for 3 s is 192 blocks.
    assert_eq!(pipeline_depth(1 << 20, q, 500, None, false), 192);
    assert_eq!(
        pipeline_depth(u64::MAX, Duration::MAX, 500, None, false),
        500
    );
    assert_eq!(pipeline_depth(u64::MAX, Duration::MAX, 0, None, false), 0);
    assert_eq!(pipeline_depth(0, Duration::ZERO, 500, None, false), 2);
    // Never deeper than the remote's `reqq`, which may go below 2.
    assert_eq!(pipeline_depth(1 << 20, q, 500, Some(50), false), 50);
    assert_eq!(pipeline_depth(0, q, 500, Some(1), false), 1);
    assert_eq!(pipeline_depth(0, q, 500, Some(0), false), 1);
    // Snubbed: one request at a time.
    assert_eq!(pipeline_depth(1 << 20, q, 500, None, true), 1);
}

/// A remote advertising `reqq` 1 gets one request at a time.
#[tokio::test(start_paused = true)]
async fn pipeline_capped_by_remote_reqq() {
    let h = start(Bitfield::new(PIECES));
    let (mut wire, mut r, _) = connect_ext(h, reserved(true, true, false)).await;
    our_ext(&mut wire).await;
    assert_eq!(recv(&mut wire).await, Message::HaveNone);
    let mut theirs = their_ext(None, None);
    theirs.reqq = Some(1);
    send(&mut wire, ext_msg(0, theirs.encode())).await;
    assert!(matches!(r.next().await, PeerEvent::ExtHandshake { .. }));
    send(&mut wire, Message::Unchoke).await;
    assert_eq!(r.next().await, PeerEvent::Unchoked { key: KEY });
    assert_eq!(r.next().await, PeerEvent::WantBlocks { key: KEY, count: 1 });
}

/// No block for `snub_timeout` snubs the remote (one request at a time);
/// the next block clears it. Each flip is reported once.
#[tokio::test(start_paused = true)]
async fn snub_after_timeout_and_clear_on_block() {
    let settings = Settings {
        snub_timeout: Duration::from_millis(300),
        ..Settings::default()
    };
    let h = start_full(
        Bitfield::new(PIECES),
        Some(layout(PIECES)),
        ext_cfg(),
        settings,
    );
    let (mut wire, mut r) = h.connect(true).await;
    let t0 = Instant::now();
    unchoke_and_request(&mut wire, &mut r, &[block(0, 0), block(0, 16_384)]).await;
    assert_eq!(
        r.next().await,
        PeerEvent::Snubbed {
            key: KEY,
            snubbed: true
        }
    );
    assert!(t0.elapsed() >= Duration::from_millis(300));
    let piece = |offset| Message::Piece {
        piece: 0,
        offset,
        data: Bytes::from(vec![1; 16_384]),
    };
    send(&mut wire, piece(0)).await;
    assert_eq!(
        r.next().await,
        PeerEvent::Snubbed {
            key: KEY,
            snubbed: false
        }
    );
    assert!(matches!(r.next().await, PeerEvent::Block { .. }));
    assert_eq!(r.next().await, PeerEvent::WantBlocks { key: KEY, count: 2 });
    // Not snubbed again before another full timeout without a block.
    let t1 = Instant::now();
    assert_eq!(
        r.next().await,
        PeerEvent::Snubbed {
            key: KEY,
            snubbed: true
        }
    );
    assert!(t1.elapsed() >= Duration::from_millis(300));
    // The last block clears it again, and the emptied pipeline asks for
    // the depth the two blocks read so far warm the rate meter to.
    send(&mut wire, piece(16_384)).await;
    assert!(matches!(
        r.next().await,
        PeerEvent::Snubbed { snubbed: false, .. }
    ));
    assert!(matches!(r.next().await, PeerEvent::Block { .. }));
    assert_eq!(r.next().await, PeerEvent::WantBlocks { key: KEY, count: 6 });
}

/// A read hold pauses the snub timer as it does the request timeouts.
#[tokio::test(start_paused = true)]
async fn read_hold_pauses_the_snub_timer() {
    let s = Settings {
        snub_timeout: Duration::from_secs(2),
        ..Settings::default()
    };
    let mut d = direct(s, 0, 0);
    let t0 = Instant::now();
    d.conn.choked = false;
    d.conn.snub_since = t0;
    d.conn.outstanding.insert(block(0, 0), t0);
    assert_eq!(d.conn.snub_deadline(), Some(t0 + Duration::from_secs(2)));
    d.conn.hold_reads(true, t0 + Duration::from_secs(1));
    assert_eq!(d.conn.snub_deadline(), None);
    d.conn.hold_reads(false, t0 + Duration::from_secs(5));
    assert_eq!(d.conn.snub_deadline(), Some(t0 + Duration::from_secs(6)));
    // Never while choked, or once snubbed.
    d.conn.choked = true;
    assert_eq!(d.conn.snub_deadline(), None);
    d.conn.choked = false;
    d.conn.snubbed = true;
    assert_eq!(d.conn.snub_deadline(), None);
}

/// Settings with the given snub and request timeouts, in milliseconds.
fn snub_settings(snub: u64, request: u64) -> Settings {
    Settings {
        snub_timeout: Duration::from_millis(snub),
        request_timeout: Duration::from_millis(request),
        ..Settings::default()
    }
}

/// A remote that never answers is snubbed even when its requests time
/// out at the same moment, and when they time out earlier and are asked
/// again (the timer then runs from before the timeouts).
#[tokio::test(start_paused = true)]
async fn snub_fires_when_requests_time_out() {
    let h = start_full(
        Bitfield::new(PIECES),
        Some(layout(PIECES)),
        ext_cfg(),
        snub_settings(300, 300),
    );
    let (mut wire, mut r) = h.connect(true).await;
    unchoke_and_request(&mut wire, &mut r, &[block(0, 0), block(0, 16_384)]).await;
    assert_eq!(
        r.next().await,
        PeerEvent::Snubbed {
            key: KEY,
            snubbed: true
        }
    );
    for b in [block(0, 0), block(0, 16_384)] {
        assert_eq!(r.next().await, PeerEvent::Rejected { key: KEY, block: b });
    }
    // The refill asks for a snubbed peer's single request.
    assert_eq!(r.next().await, PeerEvent::WantBlocks { key: KEY, count: 1 });

    let h = start_full(
        Bitfield::new(PIECES),
        Some(layout(PIECES)),
        ext_cfg(),
        snub_settings(500, 300),
    );
    let (mut wire, mut r) = h.connect(true).await;
    let t0 = Instant::now();
    unchoke_and_request(&mut wire, &mut r, &[block(0, 0)]).await;
    assert_eq!(
        r.next().await,
        PeerEvent::Rejected {
            key: KEY,
            block: block(0, 0)
        }
    );
    assert!(matches!(r.next().await, PeerEvent::WantBlocks { .. }));
    // Asked again into a pipeline the timeout emptied: no fresh timer.
    r.cmd(PeerCommand::Request(vec![block(1, 0)])).await;
    assert_eq!(
        r.next().await,
        PeerEvent::Snubbed {
            key: KEY,
            snubbed: true
        }
    );
    let at = t0.elapsed();
    assert!(
        at >= Duration::from_millis(500) && at < Duration::from_millis(600),
        "{at:?}"
    );
}

/// A snubbed peer never has more than one request in flight: requests
/// beyond its depth go back to the torrent.
#[tokio::test(start_paused = true)]
async fn snubbed_peer_gets_one_request() {
    let h = start_full(
        Bitfield::new(PIECES),
        Some(layout(PIECES)),
        ext_cfg(),
        snub_settings(300, 600),
    );
    let (mut wire, mut r) = h.connect(true).await;
    unchoke_and_request(&mut wire, &mut r, &[block(0, 0), block(0, 16_384)]).await;
    assert_eq!(
        r.next().await,
        PeerEvent::Snubbed {
            key: KEY,
            snubbed: true
        }
    );
    // Already two in flight: a stale request is refused locally.
    r.cmd(PeerCommand::Request(vec![block(1, 0)])).await;
    assert_eq!(
        r.next().await,
        PeerEvent::Rejected {
            key: KEY,
            block: block(1, 0)
        }
    );
    // The two time out; the freed pipeline takes exactly one.
    for b in [block(0, 0), block(0, 16_384)] {
        assert_eq!(r.next().await, PeerEvent::Rejected { key: KEY, block: b });
    }
    assert_eq!(r.next().await, PeerEvent::WantBlocks { key: KEY, count: 1 });
    r.cmd(PeerCommand::Request(vec![block(2, 0), block(3, 0)]))
        .await;
    assert_eq!(
        r.next().await,
        PeerEvent::Rejected {
            key: KEY,
            block: block(3, 0)
        }
    );
    assert_eq!(recv(&mut wire).await, Message::Cancel(block(0, 0)));
    assert_eq!(recv(&mut wire).await, Message::Cancel(block(0, 16_384)));
    assert_eq!(recv(&mut wire).await, Message::Request(block(2, 0)));
}
