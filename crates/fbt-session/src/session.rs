//! The session: torrent registry, peer listener, events and limits.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use fbt_core::{InfoHashes, PeerId};
use fbt_metainfo::Metainfo;
use fbt_tracker::TrackerClient;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, oneshot};
use tokio::task::JoinHandle;

use crate::error::SessionError;
use crate::event::{Event, EventMask, EventStream};
use crate::settings::Settings;
use crate::status::TorrentStatus;
use crate::storage::{FsStorage, Storage};
use crate::torrent::{self, TorrentHandle};

/// Download priority of a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum Priority {
    /// Not downloaded.
    Skip,
    /// Downloaded after everything else.
    Low,
    /// The default.
    #[default]
    Normal,
    /// Downloaded first.
    High,
}

impl Priority {
    /// The piece priority this maps to (0, 1, 4 or 7).
    pub(crate) fn piece_priority(self) -> u8 {
        match self {
            Priority::Skip => 0,
            Priority::Low => 1,
            Priority::Normal => 4,
            Priority::High => 7,
        }
    }
}

/// What to do with a removed torrent's files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RemoveMode {
    /// Leave the files on disk.
    KeepData,
    /// Delete the torrent's files.
    DeleteData,
}

/// Parameters for [`Session::add_torrent`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct AddTorrent {
    /// The torrent.
    pub metainfo: Metainfo,
    /// Directory the torrent's files live below.
    pub save_path: PathBuf,
    /// Start paused (the existing data is still checked).
    pub paused: bool,
    /// Download pieces in index order instead of rarest first.
    pub sequential: bool,
    /// One per file (pad files included); empty means all `Normal`.
    pub file_priorities: Vec<Priority>,
    /// Custom storage; `None` uses the filesystem below `save_path`.
    pub storage: Option<Arc<dyn Storage>>,
}

impl AddTorrent {
    /// Adds `metainfo` with its files below `save_path`, every other field
    /// at its default.
    pub fn new(metainfo: Metainfo, save_path: PathBuf) -> Self {
        AddTorrent {
            metainfo,
            save_path,
            paused: false,
            sequential: false,
            file_priorities: Vec::new(),
            storage: None,
        }
    }
}

/// State shared by the session, its listener and every torrent actor.
#[derive(Debug)]
pub(crate) struct Shared {
    pub settings: Arc<Settings>,
    pub peer_id: PeerId,
    /// Port of the peer listener, as announced to trackers.
    pub listen_port: u16,
    /// Shared by every torrent's announcer.
    pub tracker: TrackerClient,
    pub events: broadcast::Sender<Event>,
    pub shutdown: AtomicBool,
    /// Peer tasks alive across the session (connecting or connected).
    pub global_peers: AtomicUsize,
    /// Outgoing connections still connecting or handshaking.
    pub half_open: AtomicUsize,
}

impl Shared {
    pub(crate) fn emit(&self, ev: Event) {
        // No subscribers is fine.
        let _ = self.events.send(ev);
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
}

/// Takes one unit of `counter` if it is below `max`.
pub(crate) fn try_reserve(counter: &AtomicUsize, max: usize) -> bool {
    counter
        .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
            (n < max).then_some(n + 1)
        })
        .is_ok()
}

/// Returns one unit of `counter`.
pub(crate) fn release(counter: &AtomicUsize) {
    let _ = counter.try_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
}

/// One registered torrent.
#[derive(Debug)]
pub(crate) struct TorrentEntry {
    pub handle: TorrentHandle,
    pub wire_hash: [u8; 20],
}

#[derive(Debug, Default)]
pub(crate) struct Registry {
    pub torrents: HashMap<InfoHashes, TorrentEntry>,
    pub by_wire: HashMap<[u8; 20], InfoHashes>,
}

#[derive(Debug)]
pub(crate) struct SessionInner {
    pub shared: Arc<Shared>,
    local_addr: SocketAddr,
    registry: Mutex<Registry>,
    listener: Mutex<Option<JoinHandle<()>>>,
}

impl SessionInner {
    pub(crate) fn registry(&self) -> MutexGuard<'_, Registry> {
        self.registry.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Unregisters a torrent; true if it was registered.
    pub(crate) fn unregister(&self, ih: &InfoHashes) -> bool {
        let mut reg = self.registry();
        match reg.torrents.remove(ih) {
            Some(e) => {
                reg.by_wire.remove(&e.wire_hash);
                true
            }
            None => false,
        }
    }
}

impl Drop for SessionInner {
    fn drop(&mut self) {
        if let Some(l) = self
            .listener
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            l.abort();
        }
    }
}

/// A BitTorrent session: a set of torrents sharing one listener, one peer
/// id, connection limits and an event stream.
///
/// Cheap to clone; every clone is the same session.
#[derive(Debug, Clone)]
pub struct Session {
    inner: Arc<SessionInner>,
}

impl Session {
    /// Starts a session and binds its peer listener on
    /// `settings.listen_addr`.
    ///
    /// Fails with [`SessionError::InvalidSettings`] for settings it cannot
    /// work with (see [`Settings`]).
    pub async fn new(settings: Settings) -> Result<Session, SessionError> {
        settings.validate().map_err(SessionError::InvalidSettings)?;
        let listener = TcpListener::bind(settings.listen_addr)
            .await
            .map_err(SessionError::Bind)?;
        let local_addr = listener.local_addr().map_err(SessionError::Bind)?;
        let tracker =
            TrackerClient::new(settings.tracker.clone()).map_err(SessionError::Tracker)?;
        let (events, _) = broadcast::channel(settings.event_queue);
        let shared = Arc::new(Shared {
            settings: Arc::new(settings),
            peer_id: PeerId::generate(),
            listen_port: local_addr.port(),
            tracker,
            events,
            shutdown: AtomicBool::new(false),
            global_peers: AtomicUsize::new(0),
            half_open: AtomicUsize::new(0),
        });
        let inner = Arc::new(SessionInner {
            shared: Arc::clone(&shared),
            local_addr,
            registry: Mutex::new(Registry::default()),
            listener: Mutex::new(None),
        });
        let task = crate::listener::spawn(listener, Arc::downgrade(&inner), shared);
        *inner.listener.lock().unwrap_or_else(|e| e.into_inner()) = Some(task);
        Ok(Session { inner })
    }

    /// The address the peer listener is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }

    /// Adds a torrent and starts checking its existing data.
    pub async fn add_torrent(&self, params: AddTorrent) -> Result<TorrentHandle, SessionError> {
        let shared = &self.inner.shared;
        if shared.is_shutdown() {
            return Err(SessionError::Shutdown);
        }
        let num_files = params.metainfo.info.files.len();
        if !params.file_priorities.is_empty() && params.file_priorities.len() != num_files {
            return Err(SessionError::InvalidPriorities {
                expected: num_files,
                got: params.file_priorities.len(),
            });
        }
        let ih = params.metainfo.info_hashes;
        let wire_hash = ih.wire_hash();
        let mut reg = self.inner.registry();
        if shared.is_shutdown() {
            return Err(SessionError::Shutdown);
        }
        if reg.torrents.contains_key(&ih) || reg.by_wire.contains_key(&wire_hash) {
            return Err(SessionError::DuplicateTorrent);
        }
        let storage: Arc<dyn Storage> = match params.storage.clone() {
            Some(s) => s,
            None => Arc::new(FsStorage::new(params.save_path.clone(), &params.metainfo)),
        };
        shared.emit(Event::TorrentAdded { torrent: ih });
        let handle = torrent::spawn(
            params,
            Some(storage),
            Arc::clone(shared),
            Arc::downgrade(&self.inner),
        );
        reg.by_wire.insert(wire_hash, ih);
        reg.torrents.insert(
            ih,
            TorrentEntry {
                handle: handle.clone(),
                wire_hash,
            },
        );
        Ok(handle)
    }

    /// The torrent with these info hashes.
    pub fn torrent(&self, ih: &InfoHashes) -> Option<TorrentHandle> {
        self.inner
            .registry()
            .torrents
            .get(ih)
            .map(|e| e.handle.clone())
    }

    /// Every torrent in the session.
    pub fn torrents(&self) -> Vec<TorrentHandle> {
        self.inner
            .registry()
            .torrents
            .values()
            .map(|e| e.handle.clone())
            .collect()
    }

    /// The status of every torrent.
    pub fn torrents_status(&self) -> Vec<TorrentStatus> {
        self.torrents().iter().map(TorrentHandle::status).collect()
    }

    /// Subscribes to the events in `mask` (plus [`Event::Lagged`]).
    pub fn subscribe(&self, mask: EventMask) -> EventStream {
        EventStream::new(self.inner.shared.events.subscribe(), mask)
    }

    /// Stops the listener and every torrent, waiting for pending writes and
    /// for up to 5 s for the `stopped` announces (sent in parallel).
    /// Idempotent; afterwards every command returns
    /// [`SessionError::Shutdown`].
    pub async fn shutdown(&self) {
        if self.inner.shared.shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(l) = self
            .inner
            .listener
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            l.abort();
        }
        let handles: Vec<TorrentHandle> = {
            let mut reg = self.inner.registry();
            reg.by_wire.clear();
            reg.torrents.drain().map(|(_, e)| e.handle).collect()
        };
        let mut waits: Vec<oneshot::Receiver<()>> = Vec::new();
        for h in &handles {
            if let Some(rx) = h.send_shutdown().await {
                waits.push(rx);
            }
        }
        for rx in waits {
            let _ = rx.await;
        }
    }
}
