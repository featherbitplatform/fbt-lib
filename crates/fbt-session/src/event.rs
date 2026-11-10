//! Session events and filtered subscriptions.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use fbt_core::InfoHashes;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio_util::sync::ReusableBoxFuture;

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
    /// A piece passed its hash check and was written.
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
    /// Every wanted piece was downloaded.
    TorrentFinished {
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
    /// A peer completed the handshake.
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
    /// A peer connection ended (or never got established).
    PeerDisconnected {
        /// The torrent.
        torrent: InfoHashes,
        /// Remote address.
        addr: SocketAddr,
        /// Why.
        reason: DisconnectReason,
    },
    /// Reading or writing torrent data failed; the torrent stopped.
    FileError {
        /// The file involved, when known.
        path: Option<PathBuf>,
        /// The torrent.
        torrent: InfoHashes,
        /// The error.
        message: String,
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
        /// finished.
        const STATUS = 1;
        /// Piece finished, hash failed.
        const PIECE = 1 << 1;
        /// Tracker replies and errors.
        const TRACKER = 1 << 2;
        /// Peer connects and disconnects, peers found.
        const PEER = 1 << 3;
        /// Storage errors.
        const STORAGE = 1 << 4;
        /// The session's DHT node.
        const DHT = 1 << 5;
        /// Every category.
        const ALL = Self::STATUS.bits()
            | Self::PIECE.bits()
            | Self::TRACKER.bits()
            | Self::PEER.bits()
            | Self::STORAGE.bits()
            | Self::DHT.bits();
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
            | Event::TorrentFinished { .. } => EventMask::STATUS,
            Event::PieceFinished { .. } | Event::HashFailed { .. } => EventMask::PIECE,
            Event::TrackerReply { .. } | Event::TrackerError { .. } => EventMask::TRACKER,
            Event::PeerConnected { .. }
            | Event::PeerDisconnected { .. }
            | Event::PeersFound { .. } => EventMask::PEER,
            Event::DhtBootstrapped { .. } => EventMask::DHT,
            Event::FileError { .. } => EventMask::STORAGE,
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
