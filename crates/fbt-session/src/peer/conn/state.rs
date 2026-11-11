//! Connection state and its timers.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::net::IpAddr;
use std::sync::Arc;

use bytes::Bytes;
use fbt_core::{Bitfield, BlockRef};
use fbt_wire::{Handshake, Message, MessageCodec, Reserved};
use futures_util::StreamExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until};
use tokio_util::codec::Framed;

use super::{Flow, PeerCtx, after, max_frame, wire_len, wire_reason};
use crate::bandwidth::{PeerThrottles, Throttle};
use crate::peer::ext::{self, ExtConfig, RemoteIds};
use crate::peer::messages::{DisconnectReason, PeerCommand, PeerEvent, PeerPiecesOwned};
use crate::picker::PeerKey;
use crate::settings::Settings;

/// A piece set as the remote sent it, before validation.
#[derive(Debug)]
pub(super) enum RawPieces {
    All,
    None,
    Bits(Bytes),
}

impl RawPieces {
    pub(super) fn validate(self, num_pieces: u32) -> Result<PeerPiecesOwned, DisconnectReason> {
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
pub(super) struct Pending {
    pub(super) pieces: Option<RawPieces>,
    pub(super) haves: Vec<u32>,
}

/// State of one established connection.
pub(super) struct Conn<S> {
    pub(super) framed: Framed<S, MessageCodec>,
    pub(super) key: PeerKey,
    pub(super) events: mpsc::Sender<PeerEvent>,
    pub(super) settings: Arc<Settings>,
    /// 0 while the metadata is pending.
    pub(super) num_pieces: u32,
    /// `Some` in no-metadata mode.
    pub(super) pending: Option<Pending>,
    pub(super) ext: ExtConfig,
    pub(super) remote_ip: IpAddr,
    pub(super) remote_reserved: Reserved,
    /// The remote's ids for our extensions.
    pub(super) remote_ids: RemoteIds,
    /// A `Port` was reported already.
    pub(super) port_seen: bool,
    /// Both sides support BEP 6 (we always advertise it).
    pub(super) fast: bool,
    /// The remote chokes us.
    pub(super) choked: bool,
    /// We choke the remote.
    pub(super) choking: bool,
    /// We are interested in the remote.
    pub(super) interested: bool,
    /// The remote is interested in us.
    pub(super) remote_interested: bool,
    /// A bitfield, have, `HaveAll` or `HaveNone` arrived.
    pub(super) pieces_seen: bool,
    /// Pieces the remote lets us request while choked (at most
    /// [`MAX_ALLOWED_FAST`]).
    pub(super) allowed_fast: HashSet<u32>,
    /// Our requests in flight, with their send time.
    pub(super) outstanding: BTreeMap<BlockRef, Instant>,
    /// Remote requests forwarded to the torrent and not yet answered.
    pub(super) incoming: HashSet<BlockRef>,
    pub(super) last_recv: Instant,
    pub(super) last_send: Instant,
    /// What we read is charged here; reads wait while it is in debt.
    pub(super) down: Throttle,
    /// What we write is charged here; queued blocks wait while it is in
    /// debt (other messages never do).
    pub(super) up: Throttle,
    /// Blocks the torrent answered, waiting for the upload throttle. An
    /// entry whose block left `incoming` (cancelled, choked away) is
    /// skipped; at most `max_incoming_requests` live ones.
    pub(super) uploads: VecDeque<(BlockRef, Bytes)>,
    /// The blocks in `uploads`, each queued at most once.
    pub(super) queued: HashSet<BlockRef>,
    /// The read hold reached `peer_idle_timeout`: reads are allowed (and
    /// the idle and request timers run) until one frame arrives.
    pub(super) floor_read: bool,
    /// Since when the download throttle holds reads back: the idle timer
    /// is paused meanwhile, since unread messages are not silence.
    pub(super) held_since: Option<Instant>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Conn<S> {
    pub(super) fn new(
        stream: S,
        theirs: &Handshake,
        remote_ip: IpAddr,
        ctx: &PeerCtx,
        throttles: &PeerThrottles,
    ) -> Self {
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
            down: throttles.down.clone(),
            up: throttles.up.clone(),
            uploads: VecDeque::new(),
            queued: HashSet::new(),
            floor_read: false,
            held_since: None,
        }
    }

    /// Sends our extension handshake, pieces and DHT port, then loops
    /// until the connection ends.
    pub(super) async fn serve(
        &mut self,
        cmds: &mut mpsc::Receiver<PeerCommand>,
        have: &Bitfield,
    ) -> DisconnectReason {
        if let Err(reason) = self.greet(have).await {
            return reason;
        }
        loop {
            if let Err(reason) = self.drain_uploads().await {
                return reason;
            }
            let now = Instant::now();
            let read_held = self.read_held(now);
            let upload_held = !self.uploads.is_empty();
            let deadline = self.next_deadline();
            // Never blocks: a throttle only disables the read branch (the
            // transport's window closes and the sender slows) or delays
            // the upload queue, while commands and timers keep running.
            let step = tokio::select! {
                msg = self.framed.next(), if !read_held => match msg {
                    Some(Ok(m)) => {
                        self.last_recv = Instant::now();
                        self.floor_read = false;
                        self.down.charge(wire_len(&m));
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
                () = self.down.wait_ready(), if read_held => Ok(()),
                () = self.up.wait_ready(), if upload_held => Ok(()),
            };
            if let Err(reason) = step {
                return reason;
            }
        }
    }

    /// The messages that follow the handshake: our extension handshake
    /// (if the remote speaks BEP 10), our pieces (only `HaveNone`, with
    /// fast, before the metadata) and `Port` (if both sides set the DHT bit).
    pub(super) async fn greet(&mut self, have: &Bitfield) -> Flow {
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

    /// Whether reads wait for the download throttle now; starts and ends
    /// holds. A hold lasts at most `peer_idle_timeout`: then one frame is
    /// read whatever the debt (charged as usual), so a closed or dead
    /// connection is still noticed, and the hold starts over after it.
    pub(super) fn read_held(&mut self, now: Instant) -> bool {
        if self.floor_read {
            return false;
        }
        if let Some(since) = self.held_since
            && now >= after(since, self.settings.peer_idle_timeout)
        {
            self.hold_reads(false, now);
            self.floor_read = true;
            return false;
        }
        let held = self.down.ready_at(now).is_some();
        self.hold_reads(held, now);
        held
    }

    /// Starts or ends a read hold; at its end the idle timer and the
    /// request timeouts resume where they stood when the hold began.
    pub(super) fn hold_reads(&mut self, held: bool, now: Instant) {
        match (held, self.held_since) {
            (true, None) => self.held_since = Some(now),
            (false, Some(since)) => {
                let paused = now.saturating_duration_since(since);
                self.last_recv = after(self.last_recv, paused).min(now);
                for t in self.outstanding.values_mut() {
                    *t = after(*t, paused).min(now);
                }
                self.held_since = None;
            }
            _ => {}
        }
    }

    /// The idle deadline; none while reads are held.
    fn idle_deadline(&self) -> Option<Instant> {
        self.held_since
            .is_none()
            .then(|| after(self.last_recv, self.settings.peer_idle_timeout))
    }

    pub(super) fn next_deadline(&self) -> Instant {
        let s = &self.settings;
        let keepalive = after(self.last_send, s.keepalive_interval);
        let idle = self.idle_deadline().unwrap_or(keepalive);
        // Requests do not time out while we hold their answers unread.
        let request = self
            .outstanding
            .values()
            .min()
            .filter(|_| self.held_since.is_none())
            .map(|&t| after(t, s.request_timeout));
        idle.min(keepalive).min(request.unwrap_or(idle))
    }

    pub(super) async fn on_timer(&mut self) -> Flow {
        let now = Instant::now();
        if self.idle_deadline().is_some_and(|idle| now >= idle) {
            return Err(DisconnectReason::IdleTimeout);
        }
        let limit = self.settings.request_timeout;
        let expired: Vec<BlockRef> = self
            .outstanding
            .iter()
            .filter(|(_, t)| self.held_since.is_none() && now >= after(**t, limit))
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
}
