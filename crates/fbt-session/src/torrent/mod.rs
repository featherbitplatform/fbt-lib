//! One torrent: its public handle and the actor task behind it.
//!
//! - `actor`: the event loop, commands, state and status.
//! - `peers`: connections within the limits, peer events, interest and
//!   block requests.
//! - `data`: block buffering, piece verification and writes.
//! - `serve`: answering block requests from peers.
//! - `announcer`, `tiers`: announcing to the trackers (BEP 12 tiers).

mod actor;
mod announcer;
mod data;
mod peers;
mod serve;
mod tiers;

use std::net::SocketAddr;
use std::sync::{Arc, Weak};

use fbt_core::{Bitfield, InfoHashes};
use fbt_metainfo::Metainfo;
use fbt_wire::Handshake;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};

use crate::error::SessionError;
use crate::session::{AddTorrent, Priority, RemoveMode, SessionInner, Shared};
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
    /// An accepted connection whose handshake names this torrent.
    Incoming {
        stream: TcpStream,
        handshake: Handshake,
        addr: SocketAddr,
    },
    Shutdown(oneshot::Sender<()>),
}

#[derive(Debug)]
struct HandleInner {
    info_hashes: InfoHashes,
    num_files: usize,
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
    storage: Arc<dyn Storage>,
    shared: Arc<Shared>,
    session: Weak<SessionInner>,
) -> TorrentHandle {
    let ih = params.metainfo.info_hashes;
    let (cmd_tx, cmd_rx) = mpsc::channel(COMMAND_QUEUE);
    let initial = TorrentStatus::initial(ih, params.metainfo.info.name.clone());
    let (status_tx, status_rx) = watch::channel(initial);
    let handle = TorrentHandle {
        inner: Arc::new(HandleInner {
            info_hashes: ih,
            num_files: params.metainfo.info.files.len(),
            cmds: cmd_tx,
            status: status_rx,
            shared: Arc::clone(&shared),
        }),
    };
    let actor = actor::Actor::new(params, storage, shared, session, cmd_rx, status_tx);
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
    pub async fn force_recheck(&self) -> Result<(), SessionError> {
        self.call(Cmd::Recheck).await
    }

    /// Announces to the trackers now.
    pub async fn force_reannounce(&self) -> Result<(), SessionError> {
        self.call(Cmd::Reannounce).await
    }

    /// Sets one priority per file (pad files included).
    pub async fn set_file_priorities(&self, p: Vec<Priority>) -> Result<(), SessionError> {
        if p.len() != self.inner.num_files {
            return Err(SessionError::InvalidPriorities {
                expected: self.inner.num_files,
                got: p.len(),
            });
        }
        self.call(|tx| Cmd::SetPriorities(p, tx)).await
    }

    /// Connects to a peer.
    pub async fn add_peer(&self, addr: SocketAddr) -> Result<(), SessionError> {
        self.call(|tx| Cmd::AddPeer(addr, tx)).await
    }

    /// The pieces verified so far (empty while checking).
    pub async fn pieces(&self) -> Result<Bitfield, SessionError> {
        self.call(Cmd::Pieces).await
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
        stream: TcpStream,
        handshake: Handshake,
        addr: SocketAddr,
    ) -> bool {
        self.inner
            .cmds
            .try_send(Cmd::Incoming {
                stream,
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
