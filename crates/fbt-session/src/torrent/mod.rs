//! One torrent: its public handle and the actor task behind it.
//!
//! - `actor`: the event loop, commands, state and status.
//! - `swarm`: addresses to connect to, connections within the limits,
//!   sending to and closing peers.
//! - `peers`: peer events, interest, choking and block requests.
//! - `data`: block buffering, piece verification and writes.
//! - `serve`: answering block requests from peers.
//! - `metadata`: fetching a magnet's metadata from peers and serving ours
//!   (BEP 9).
//! - `announcer`, `tiers`: announcing to the trackers (BEP 12 tiers).
//! - `dht`: looking up and announcing on the session's DHT.
//! - `pex`: peer exchange (BEP 11) with connected peers.

mod actor;
mod announcer;
mod check;
mod commands;
mod data;
mod dht;
mod holepunch;
mod magnet;
mod metadata;
mod peers;
mod pex;
mod serve;
mod swarm;
mod tiers;

use std::net::SocketAddr;
use std::sync::{Arc, OnceLock, Weak};

use fbt_core::{Bitfield, InfoHashes};
use fbt_metainfo::Metainfo;
use fbt_wire::Handshake;
use tokio::sync::{mpsc, oneshot, watch};

use crate::bandwidth::RateLimit;
use crate::error::SessionError;
use crate::peer::conn::PeerStream;
use crate::session::{AddTorrent, Priority, RemoveMode, SessionInner, Shared, TorrentSource};
use crate::status::{TorrentState, TorrentStatus};
use crate::storage::Storage;

/// Capacity of a torrent's command channel.
const COMMAND_QUEUE: usize = 64;

/// Handle to the actor, from the public handle and the listener.
#[derive(Debug)]
pub(crate) enum Cmd {
    Pause(oneshot::Sender<()>),
    Resume(oneshot::Sender<()>),
    Remove(RemoveMode, oneshot::Sender<Result<(), SessionError>>),
    Recheck(oneshot::Sender<()>),
    Reannounce(oneshot::Sender<()>),
    SetPriorities(Vec<Priority>, oneshot::Sender<()>),
    AddPeer(SocketAddr, oneshot::Sender<()>),
    Pieces(oneshot::Sender<Bitfield>),
    SetRateLimit(RateLimit, oneshot::Sender<()>),
    /// An accepted connection whose handshake names this torrent.
    Incoming {
        /// Boxed: the MSE stream holds two RC4 states.
        stream: Box<PeerStream>,
        handshake: Handshake,
        addr: SocketAddr,
    },
    Shutdown(oneshot::Sender<()>),
}

#[derive(Debug)]
struct HandleInner {
    info_hashes: InfoHashes,
    /// Set by the actor once the metadata is known (at spawn for a
    /// `.torrent`).
    metainfo: Arc<OnceLock<Arc<Metainfo>>>,
    cmds: mpsc::Sender<Cmd>,
    status: watch::Receiver<TorrentStatus>,
    shared: Arc<Shared>,
}

/// A torrent in a [`Session`](crate::Session).
///
/// Cheap to clone. Commands go to the torrent's task and wait for it to
/// apply them; [`status`](Self::status) reads the latest snapshot without
/// waiting.
#[derive(Debug, Clone)]
pub struct TorrentHandle {
    inner: Arc<HandleInner>,
}

/// Starts the actor for `params` and returns its handle.
pub(crate) fn spawn(
    params: AddTorrent,
    storage: Option<Arc<dyn Storage>>,
    shared: Arc<Shared>,
    session: Weak<SessionInner>,
) -> TorrentHandle {
    let ih = params.source.info_hashes();
    let (cmd_tx, cmd_rx) = mpsc::channel(COMMAND_QUEUE);
    let initial = match &params.source {
        TorrentSource::Metainfo(m) => TorrentStatus::initial(ih, m.info.name.clone(), true),
        TorrentSource::Magnet(m) => {
            TorrentStatus::initial(ih, m.display_name.clone().unwrap_or_default(), false)
        }
    };
    let (status_tx, status_rx) = watch::channel(initial);
    let metainfo = Arc::new(OnceLock::new());
    let handle = TorrentHandle {
        inner: Arc::new(HandleInner {
            info_hashes: ih,
            metainfo: Arc::clone(&metainfo),
            cmds: cmd_tx,
            status: status_rx,
            shared: Arc::clone(&shared),
        }),
    };
    let actor = actor::Actor::new(
        params, storage, metainfo, shared, session, cmd_rx, status_tx,
    );
    tokio::spawn(actor.run());
    handle
}

/// Piece priorities from file priorities: each piece takes the highest
/// priority among the non-pad files it overlaps; pieces overlapping only pad
/// files get 0. Empty `files` means every file is `Normal`.
pub(crate) fn piece_priorities(meta: &Metainfo, files: &[Priority]) -> Vec<u8> {
    let layout = meta.piece_layout();
    let file_layout = meta.file_layout();
    (0..layout.num_pieces())
        .map(|piece| {
            let off = layout.piece_offset(piece);
            let len = u64::from(layout.piece_size(piece));
            file_layout
                .map(off, len)
                .iter()
                .filter(|s| meta.info.files.get(s.file).is_some_and(|f| !f.pad))
                .map(|s| {
                    files
                        .get(s.file)
                        .copied()
                        .unwrap_or_default()
                        .piece_priority()
                })
                .max()
                .unwrap_or(0)
        })
        .collect()
}

impl TorrentHandle {
    /// The torrent's info hashes.
    pub fn info_hashes(&self) -> InfoHashes {
        self.inner.info_hashes
    }

    /// The torrent's metadata; `None` for a magnet until
    /// [`Event::MetadataReceived`](crate::Event::MetadataReceived).
    pub fn metainfo(&self) -> Option<Arc<Metainfo>> {
        self.inner.metainfo.get().cloned()
    }

    /// The latest status snapshot. Still readable after removal or shutdown.
    pub fn status(&self) -> TorrentStatus {
        self.inner.status.borrow().clone()
    }

    /// Disconnects every peer and stops announcing.
    pub async fn pause(&self) -> Result<(), SessionError> {
        self.call(Cmd::Pause).await
    }

    /// Undoes [`pause`](Self::pause); also clears a storage error.
    pub async fn resume(&self) -> Result<(), SessionError> {
        self.call(Cmd::Resume).await
    }

    /// Removes the torrent from the session, optionally deleting its files.
    ///
    /// Once the command is sent the removal completes even if this future
    /// is dropped: the torrent's task unregisters itself when it is done.
    pub async fn remove(&self, mode: RemoveMode) -> Result<(), SessionError> {
        self.call(|tx| Cmd::Remove(mode, tx)).await?
    }

    /// Disconnects every peer and checks the data again.
    ///
    /// Fails with [`SessionError::NoMetadata`] for a magnet whose metadata
    /// has not arrived.
    pub async fn force_recheck(&self) -> Result<(), SessionError> {
        if self.metainfo().is_none() {
            return Err(SessionError::NoMetadata);
        }
        self.call(Cmd::Recheck).await
    }

    /// Announces to the trackers now.
    pub async fn force_reannounce(&self) -> Result<(), SessionError> {
        self.call(Cmd::Reannounce).await
    }

    /// Sets one priority per file (pad files included).
    ///
    /// Fails with [`SessionError::NoMetadata`] for a magnet whose metadata
    /// has not arrived.
    pub async fn set_file_priorities(&self, p: Vec<Priority>) -> Result<(), SessionError> {
        let num_files = self
            .metainfo()
            .ok_or(SessionError::NoMetadata)?
            .info
            .files
            .len();
        if p.len() != num_files {
            return Err(SessionError::InvalidPriorities {
                expected: num_files,
                got: p.len(),
            });
        }
        self.call(|tx| Cmd::SetPriorities(p, tx)).await
    }

    /// Connects to a peer.
    pub async fn add_peer(&self, addr: SocketAddr) -> Result<(), SessionError> {
        self.call(|tx| Cmd::AddPeer(addr, tx)).await
    }

    /// The pieces verified so far (empty while checking, and with no bits
    /// at all before a magnet's metadata).
    pub async fn pieces(&self) -> Result<Bitfield, SessionError> {
        self.call(Cmd::Pieces).await
    }

    /// Sets the torrent's own rate limit ([`AddTorrent::rate_limit`]); it
    /// applies at once, and to waiting connections within a second.
    pub async fn set_rate_limit(&self, limit: RateLimit) -> Result<(), SessionError> {
        self.call(|tx| Cmd::SetRateLimit(limit, tx)).await
    }

    fn gone(&self) -> SessionError {
        if self.inner.shared.is_shutdown() {
            SessionError::Shutdown
        } else {
            SessionError::TorrentNotFound
        }
    }

    async fn call<T>(
        &self,
        cmd: impl FnOnce(oneshot::Sender<T>) -> Cmd,
    ) -> Result<T, SessionError> {
        if self.inner.shared.is_shutdown() {
            return Err(SessionError::Shutdown);
        }
        let (tx, rx) = oneshot::channel();
        self.inner
            .cmds
            .send(cmd(tx))
            .await
            .map_err(|_| self.gone())?;
        rx.await.map_err(|_| self.gone())
    }

    /// Hands an accepted connection to the actor without waiting; false if
    /// its queue is full or it is gone.
    pub(crate) fn offer_incoming(
        &self,
        stream: PeerStream,
        handshake: Handshake,
        addr: SocketAddr,
    ) -> bool {
        self.inner
            .cmds
            .try_send(Cmd::Incoming {
                stream: Box::new(stream),
                handshake,
                addr,
            })
            .is_ok()
    }

    /// Asks the actor to stop; the receiver completes once it has.
    pub(crate) async fn send_shutdown(&self) -> Option<oneshot::Receiver<()>> {
        let (tx, rx) = oneshot::channel();
        self.inner.cmds.send(Cmd::Shutdown(tx)).await.ok()?;
        Some(rx)
    }
}

/// True if `a` and `b` are the same state, ignoring checking progress.
pub(crate) fn same_state(a: &TorrentState, b: &TorrentState) -> bool {
    std::mem::discriminant(a) == std::mem::discriminant(b)
}
