//! One torrent: its public handle and the actor task behind it.
//!
//! - `actor`: the event loop, commands, state and status.
//! - `swarm`: connections within the limits, sending to and closing peers.
//! - `known`, `retries`, `peer`: the addresses to connect to, their retries
//!   and fallbacks, and one connection's state.
//! - `peers`: peer events, interest, choking and block requests.
//! - `data`: block buffering, piece verification and writes.
//! - `smartban`: who sent the blocks of a piece that failed its hash
//!   check, and which senders to ban.
//! - `serve`: answering block requests from peers.
//! - `metadata`: fetching a magnet's metadata from peers and serving ours
//!   (BEP 9).
//! - `announcer`, `tiers`: announcing to the trackers (BEP 12 tiers).
//! - `dht`: looking up and announcing on the session's DHT.
//! - `pex`: peer exchange (BEP 11) with connected peers.
//! - `deadline`: the pieces open file readers wait for, and when.
//! - `readers`: open file readers, their reads and read-ahead.

mod actor;
mod announcer;
mod check;
mod commands;
mod data;
mod deadline;
mod dht;
mod holepunch;
mod known;
mod magnet;
mod metadata;
mod peer;
mod peers;
mod pex;
mod readers;
mod retries;
mod serve;
mod smartban;
mod swarm;
mod tiers;

pub(crate) use deadline::ReaderId;

use std::net::SocketAddr;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use bytes::Bytes;
use fbt_core::{Bitfield, InfoHashes};
use fbt_metainfo::Metainfo;
use fbt_wire::Handshake;
use tokio::sync::{mpsc, oneshot, watch};

use crate::bandwidth::RateLimit;
use crate::error::{SessionError, StreamError};
use crate::event::PeerSource;
use crate::peer::conn::PeerStream;
use crate::session::{AddTorrent, Priority, RemoveMode, SessionInner, Shared, TorrentSource};
use crate::status::{TorrentState, TorrentStatus};
use crate::storage::Storage;
use crate::stream::reader::ReaderLink;
use crate::stream::{FileReader, ReadAhead};

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
    Scrape(oneshot::Sender<()>),
    SetPriorities(Vec<Priority>, oneshot::Sender<()>),
    AddPeer(SocketAddr, oneshot::Sender<()>),
    Pieces(oneshot::Sender<Bitfield>),
    SetRateLimit(RateLimit, oneshot::Sender<()>),
    /// Peers discovered outside the torrent (local service discovery).
    FoundPeers(PeerSource, Vec<SocketAddr>),
    /// The session's IP filter changed: close the peers it blocks now.
    FilterChanged(oneshot::Sender<()>),
    /// Another torrent smart-banned a peer: close the peers the session's
    /// bans now block. Sent without waiting for an answer.
    BansChanged,
    /// Tests: the smart-ban trust points of the peer at this address.
    PeerTrust(SocketAddr, oneshot::Sender<i32>),
    /// An accepted connection whose handshake names this torrent.
    Incoming {
        /// Boxed: the MSE stream holds two RC4 states.
        stream: Box<PeerStream>,
        handshake: Handshake,
        addr: SocketAddr,
    },
    Shutdown(oneshot::Sender<()>),
    /// A [`FileReader`] opens on file `file`, sharing `link`.
    OpenReader {
        file: usize,
        link: Arc<ReaderLink>,
        tx: oneshot::Sender<Result<ReaderId, SessionError>>,
    },
    /// A reader's link moved (seek, new piece or new read-ahead).
    ReaderMoved(ReaderId),
    /// A reader was dropped.
    CloseReader(ReaderId),
    /// `len` bytes at the torrent-global `offset`, within one piece; at
    /// most one in flight per reader.
    Read {
        reader: ReaderId,
        offset: u64,
        len: u32,
        tx: oneshot::Sender<Result<Bytes, StreamError>>,
    },
    /// Tests: the merged deadlines.
    DebugDeadlines(oneshot::Sender<Vec<(u32, Duration)>>),
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

    /// Scrapes the first working tracker of each tier for this torrent's
    /// counts. Returns once the scrape is asked for; each tracker reports
    /// through [`Event::ScrapeReply`](crate::Event::ScrapeReply) or
    /// [`Event::ScrapeFailed`](crate::Event::ScrapeFailed)
    /// ([`EventMask::TRACKER`](crate::EventMask::TRACKER)). A torrent
    /// without trackers reports nothing. A scrape asked for while one is
    /// running replaces it.
    pub async fn scrape_trackers(&self) -> Result<(), SessionError> {
        self.call(Cmd::Scrape).await
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

    /// Opens file `index` for reading while the torrent downloads, with
    /// [`Settings::read_ahead`](crate::Settings::read_ahead).
    ///
    /// Fails with [`SessionError::NoMetadata`] for a magnet whose metadata
    /// has not arrived, [`SessionError::InvalidFileIndex`] for an index out
    /// of range or naming a pad file, and [`SessionError::TooManyReaders`]
    /// when the torrent has 16 open readers.
    pub async fn open_file(&self, index: usize) -> Result<FileReader, SessionError> {
        let read_ahead = self.inner.shared.settings.read_ahead;
        self.open_file_with(index, read_ahead).await
    }

    /// [`open_file`](Self::open_file) with its own read-ahead; fails with
    /// [`SessionError::InvalidSettings`] for a zero `time` or `bitrate`.
    pub async fn open_file_with(
        &self,
        index: usize,
        read_ahead: ReadAhead,
    ) -> Result<FileReader, SessionError> {
        let meta = self.metainfo().ok_or(SessionError::NoMetadata)?;
        let files = meta.info.files.len();
        let invalid = SessionError::InvalidFileIndex { index, files };
        if meta.info.files.get(index).is_none_or(|f| f.pad) {
            return Err(invalid);
        }
        read_ahead
            .validate()
            .map_err(SessionError::InvalidSettings)?;
        let file = meta.file_layout().file_range(index).ok_or(invalid)?;
        let link = Arc::new(ReaderLink::new(file.start, read_ahead));
        let shared = Arc::clone(&link);
        let id = self
            .call(|tx| Cmd::OpenReader {
                file: index,
                link: shared,
                tx,
            })
            .await??;
        let piece_len = meta.piece_layout().piece_len();
        Ok(FileReader::new(
            self.clone(),
            id,
            link,
            index,
            file,
            piece_len,
        ))
    }

    /// Tests: the pieces open readers wait for, earliest deadline first,
    /// with the time left until each is due; empty once the torrent is gone.
    #[doc(hidden)]
    pub async fn debug_deadlines(&self) -> Vec<(u32, Duration)> {
        self.call(Cmd::DebugDeadlines).await.unwrap_or_default()
    }

    /// The torrent's command channel, for its readers.
    pub(crate) fn cmd_sender(&self) -> mpsc::Sender<Cmd> {
        self.inner.cmds.clone()
    }

    /// The error for a torrent whose actor is gone.
    pub(crate) fn gone(&self) -> SessionError {
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

    /// Hands discovered peers to the actor without waiting; false if its
    /// queue is full or it is gone.
    pub(crate) fn offer_found_peers(&self, source: PeerSource, peers: Vec<SocketAddr>) -> bool {
        self.inner
            .cmds
            .try_send(Cmd::FoundPeers(source, peers))
            .is_ok()
    }

    /// Tells the actor the IP filter changed; returns once it closed the
    /// peers the new filter blocks.
    pub(crate) async fn filter_changed(&self) -> Result<(), SessionError> {
        self.call(Cmd::FilterChanged).await
    }

    /// Tells the actor, without waiting, that a ban was added elsewhere in
    /// the session. If its queue is full the command is sent from a task of
    /// its own, so the caller never waits and the ban still reaches it.
    pub(crate) fn bans_changed(&self) {
        if let Err(tokio::sync::mpsc::error::TrySendError::Full(cmd)) =
            self.inner.cmds.try_send(Cmd::BansChanged)
        {
            let cmds = self.inner.cmds.clone();
            tokio::spawn(async move {
                let _ = cmds.send(cmd).await;
            });
        }
    }

    /// The smart-ban trust points this torrent holds for the peer at
    /// `addr` (0 if it never sent a block). For tests.
    #[doc(hidden)]
    pub async fn peer_trust(&self, addr: SocketAddr) -> Result<i32, SessionError> {
        self.call(|tx| Cmd::PeerTrust(addr, tx)).await
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
