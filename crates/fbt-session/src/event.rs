//! Session events and filtered subscriptions.

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use fbt_core::InfoHashes;
use fbt_net::{MapMethod, MapProtocol};
use tokio::sync::broadcast::{self, error::RecvError};
use tokio_util::sync::ReusableBoxFuture;

use crate::bans::BlockReason;
use crate::peer::{DisconnectReason, Transport};
use crate::status::TorrentState;

/// Where a batch of peer addresses came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PeerSource {
    /// A tracker reply.
    Tracker,
    /// A DHT lookup.
    Dht,
    /// Peer exchange (BEP 11).
    Pex,
    /// A magnet link's `x.pe` hints.
    Magnet,
    /// Local service discovery (BEP 14).
    Lsd,
}

/// Where an external address estimate came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ExternalIpSource {
    /// The router, through UPnP, PCP or NAT-PMP.
    PortMapping,
    /// The DHT nodes' `ip` votes (BEP 42).
    Dht,
    /// Peers' `yourip` (BEP 10), once three of them agree.
    Peers,
}

/// Something that happened in the session.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Event {
    /// A torrent was added.
    TorrentAdded {
        /// The torrent.
        torrent: InfoHashes,
    },
    /// A torrent was removed.
    TorrentRemoved {
        /// The torrent.
        torrent: InfoHashes,
    },
    /// A torrent changed state.
    StateChanged {
        /// The torrent.
        torrent: InfoHashes,
        /// The new state.
        state: TorrentState,
    },
    /// A piece passed its hash check. It may not be on disk yet: the
    /// write-back cache serves it from memory and writes it later (see
    /// [`Settings::cache_size`](crate::Settings::cache_size)).
    PieceFinished {
        /// The torrent.
        torrent: InfoHashes,
        /// Piece index.
        piece: u32,
    },
    /// A downloaded piece failed its hash check.
    HashFailed {
        /// The torrent.
        torrent: InfoHashes,
        /// Piece index.
        piece: u32,
    },
    /// A magnet torrent received and verified its metadata.
    MetadataReceived {
        /// The torrent.
        torrent: InfoHashes,
    },
    /// Every wanted piece was downloaded, verified and written to storage.
    TorrentFinished {
        /// The torrent.
        torrent: InfoHashes,
    },
    /// An auto-managed seed reached
    /// [`Settings::seed_ratio_limit`](crate::Settings::seed_ratio_limit) or
    /// [`Settings::seed_time_limit`](crate::Settings::seed_time_limit): it
    /// is paused and no longer auto-managed.
    SeedLimitReached {
        /// The torrent.
        torrent: InfoHashes,
    },
    /// A tracker answered an announce.
    TrackerReply {
        /// The torrent.
        torrent: InfoHashes,
        /// Tracker URL.
        url: String,
        /// Peers in the reply.
        peers: usize,
    },
    /// An announce failed.
    TrackerError {
        /// The torrent.
        torrent: InfoHashes,
        /// Tracker URL.
        url: String,
        /// What went wrong.
        message: String,
    },
    /// A tracker answered a scrape
    /// ([`TorrentHandle::scrape_trackers`](crate::TorrentHandle::scrape_trackers)).
    ScrapeReply {
        /// The torrent.
        torrent: InfoHashes,
        /// Tracker URL (the announce URL).
        url: String,
        /// Seeds.
        complete: u32,
        /// Leechers.
        incomplete: u32,
        /// Completed downloads the tracker has seen.
        downloaded: u32,
    },
    /// A scrape failed, or the reply left the torrent out. The tracker
    /// stays in use for announces.
    ScrapeFailed {
        /// The torrent.
        torrent: InfoHashes,
        /// Tracker URL (the announce URL).
        url: String,
        /// What went wrong.
        message: String,
    },
    /// A batch of peer addresses arrived for a torrent.
    PeersFound {
        /// The torrent.
        torrent: InfoHashes,
        /// Where they came from.
        source: PeerSource,
        /// Dialable addresses in the batch.
        count: usize,
    },
    /// The session's DHT node filled its routing table for the first time.
    /// Emitted at most once per session.
    DhtBootstrapped {
        /// Nodes in the routing tables at that moment.
        nodes: usize,
    },
    /// A peer completed the handshake. Reported once per peer: when two
    /// connections to the same peer id meet (both sides dialled, as in a
    /// holepunch), the one the tie-break closes reports nothing, and if the
    /// newer one survives, it takes over the first one's report without a
    /// new event. `transport` and `encrypted` then describe the first
    /// connection, not the one still open.
    PeerConnected {
        /// The torrent.
        torrent: InfoHashes,
        /// Remote address.
        addr: SocketAddr,
        /// The transport the connection runs over.
        transport: Transport,
        /// The connection is RC4-encrypted (MSE); false for plaintext,
        /// MSE-negotiated or not.
        encrypted: bool,
    },
    /// A peer connection ended (or never got established). Exactly one
    /// follows each `PeerConnected`, with its `addr`; a connection closed as
    /// a duplicate by the tie-break reports nothing.
    PeerDisconnected {
        /// The torrent.
        torrent: InfoHashes,
        /// Remote address.
        addr: SocketAddr,
        /// Why.
        reason: DisconnectReason,
    },
    /// A peer was refused: not dialled, or its connection closed (an
    /// incoming one before its handshake is read when the session-wide
    /// checks refuse it). A connection that was reported connected also
    /// gets its [`PeerDisconnected`](Event::PeerDisconnected) with
    /// [`DisconnectReason::Blocked`].
    PeerBlocked {
        /// The torrent; `None` for an incoming connection refused before
        /// its handshake named one.
        torrent: Option<InfoHashes>,
        /// The peer's address.
        addr: SocketAddr,
        /// Why.
        reason: BlockReason,
    },
    /// Reading or writing torrent data failed; the torrent stopped, except
    /// for a piece found missing on disk while serving a peer: that piece
    /// is downloaded again and the torrent keeps running.
    /// That is reported once per file (until the torrent seeds again or is
    /// rechecked), and every piece overlapping the file goes with it. On
    /// Unix a file deleted while the torrent holds a read handle on it keeps
    /// being served from the unlinked inode: the loss shows only once the
    /// handle is closed or the file is truncated.
    FileError {
        /// The file involved, when known.
        path: Option<PathBuf>,
        /// The torrent.
        torrent: InfoHashes,
        /// The error.
        message: String,
    },
    /// [`TorrentHandle::move_storage`](crate::TorrentHandle::move_storage)
    /// finished: the torrent's files are below `path` now.
    StorageMoved {
        /// The torrent.
        torrent: InfoHashes,
        /// The new save path.
        path: PathBuf,
    },
    /// [`TorrentHandle::move_storage`](crate::TorrentHandle::move_storage)
    /// failed; whatever it moved was moved back, and the torrent keeps
    /// running from the old save path.
    StorageMoveFailed {
        /// The torrent.
        torrent: InfoHashes,
        /// The error.
        message: String,
    },
    /// [`TorrentHandle::rename_file`](crate::TorrentHandle::rename_file)
    /// finished.
    FileRenamed {
        /// The torrent.
        torrent: InfoHashes,
        /// The file's index.
        index: usize,
        /// Its new path, relative to the save path, `/`-separated.
        path: String,
    },
    /// [`TorrentHandle::rename_file`](crate::TorrentHandle::rename_file)
    /// failed; the file keeps its path.
    FileRenameFailed {
        /// The torrent.
        torrent: InfoHashes,
        /// The file's index.
        index: usize,
        /// The error.
        message: String,
    },
    /// The router mapped a listen port (UPnP, PCP or NAT-PMP), or the
    /// mapping's external address changed.
    #[non_exhaustive]
    PortMapped {
        /// Transport protocol.
        protocol: MapProtocol,
        /// The method that made it.
        method: MapMethod,
        /// The public address and port (`0.0.0.0` when UPnP could not tell
        /// the address).
        external: SocketAddr,
    },
    /// Mapping or renewing a listen port failed; it is retried with a
    /// backoff from 60 s up to 30 minutes.
    PortMappingFailed {
        /// Transport protocol.
        protocol: MapProtocol,
        /// The method that failed.
        method: MapMethod,
        /// Why.
        message: String,
    },
    /// A source learned our external address, or a new one. See
    /// [`Session::external_ip`](crate::Session::external_ip) for the best
    /// estimate across sources.
    ExternalIpChanged {
        /// The address.
        ip: IpAddr,
        /// Who reported it.
        source: ExternalIpSource,
    },
    /// The subscriber fell behind and `missed` events were dropped.
    Lagged {
        /// Number of events dropped.
        missed: u64,
    },
}

bitflags::bitflags! {
    /// Event categories a subscriber receives.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct EventMask: u32 {
        /// Torrent added/removed, state changes, metadata received, torrent
        /// finished, seed limit reached.
        const STATUS = 1;
        /// Piece finished, hash failed.
        const PIECE = 1 << 1;
        /// Tracker replies and errors, scrapes included.
        const TRACKER = 1 << 2;
        /// Peer connects and disconnects, peers found, peers blocked.
        const PEER = 1 << 3;
        /// Storage errors, storage moves and file renames.
        const STORAGE = 1 << 4;
        /// The session's DHT node.
        const DHT = 1 << 5;
        /// Port mapping and the external address.
        const NAT = 1 << 6;
        /// Every category.
        const ALL = Self::STATUS.bits()
            | Self::PIECE.bits()
            | Self::TRACKER.bits()
            | Self::PEER.bits()
            | Self::STORAGE.bits()
            | Self::DHT.bits()
            | Self::NAT.bits();
    }
}

impl Event {
    /// The category this event belongs to; [`Event::Lagged`] belongs to all.
    pub fn category(&self) -> EventMask {
        match self {
            Event::TorrentAdded { .. }
            | Event::TorrentRemoved { .. }
            | Event::StateChanged { .. }
            | Event::MetadataReceived { .. }
            | Event::TorrentFinished { .. }
            | Event::SeedLimitReached { .. } => EventMask::STATUS,
            Event::PieceFinished { .. } | Event::HashFailed { .. } => EventMask::PIECE,
            Event::TrackerReply { .. }
            | Event::TrackerError { .. }
            | Event::ScrapeReply { .. }
            | Event::ScrapeFailed { .. } => EventMask::TRACKER,
            Event::PeerConnected { .. }
            | Event::PeerDisconnected { .. }
            | Event::PeersFound { .. }
            | Event::PeerBlocked { .. } => EventMask::PEER,
            Event::DhtBootstrapped { .. } => EventMask::DHT,
            Event::FileError { .. }
            | Event::StorageMoved { .. }
            | Event::StorageMoveFailed { .. }
            | Event::FileRenamed { .. }
            | Event::FileRenameFailed { .. } => EventMask::STORAGE,
            Event::PortMapped { .. }
            | Event::PortMappingFailed { .. }
            | Event::ExternalIpChanged { .. } => EventMask::NAT,
            Event::Lagged { .. } => EventMask::ALL,
        }
    }
}

type RecvOutcome = (Result<Event, RecvError>, broadcast::Receiver<Event>);

async fn recv_owned(mut rx: broadcast::Receiver<Event>) -> RecvOutcome {
    let r = rx.recv().await;
    (r, rx)
}

/// A filtered subscription to session events.
///
/// Implements [`futures_core::Stream`]; [`EventStream::recv`] is the same as
/// awaiting the next item. A subscriber that falls behind gets
/// [`Event::Lagged`] with the number of events it missed, whatever its mask.
/// The stream ends when the session is gone.
pub struct EventStream {
    mask: EventMask,
    fut: ReusableBoxFuture<'static, RecvOutcome>,
    done: bool,
}

impl std::fmt::Debug for EventStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventStream")
            .field("mask", &self.mask)
            .finish_non_exhaustive()
    }
}

impl EventStream {
    pub(crate) fn new(rx: broadcast::Receiver<Event>, mask: EventMask) -> Self {
        EventStream {
            mask,
            fut: ReusableBoxFuture::new(recv_owned(rx)),
            done: false,
        }
    }

    /// The next matching event, or `None` once the session is gone.
    pub async fn recv(&mut self) -> Option<Event> {
        std::future::poll_fn(|cx| self.poll_event(cx)).await
    }

    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            let (r, rx) = std::task::ready!(self.fut.poll(cx));
            self.fut.set(recv_owned(rx));
            match r {
                Ok(ev) if self.mask.intersects(ev.category()) => return Poll::Ready(Some(ev)),
                Ok(_) => {}
                Err(RecvError::Lagged(missed)) => {
                    return Poll::Ready(Some(Event::Lagged { missed }));
                }
                Err(RecvError::Closed) => {
                    self.done = true;
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl futures_core::Stream for EventStream {
    type Item = Event;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        self.get_mut().poll_event(cx)
    }
}
