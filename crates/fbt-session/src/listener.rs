//! Accepts incoming peer connections and routes them to torrents by the
//! info hash in their handshake.

use std::net::SocketAddr;
use std::sync::{Arc, Weak};
use std::time::Duration;

use fbt_wire::{read_handshake, write_handshake};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::debug;

use crate::session::{SessionInner, Shared};

/// Pause after a failed `accept` (for example out of file descriptors).
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// Runs the accept loop until the session goes away or the task is aborted.
pub(crate) fn spawn(
    listener: TcpListener,
    session: Weak<SessionInner>,
    shared: Arc<Shared>,
) -> JoinHandle<()> {
    // Connections still sending their handshake; more are dropped at once.
    let pending = Arc::new(Semaphore::new(shared.settings.max_peers_global.max(1)));
    tokio::spawn(async move {
        loop {
            let (stream, addr) = match listener.accept().await {
                Ok(c) => c,
                Err(e) => {
                    debug!(error = %e, "accept failed");
                    tokio::time::sleep(ACCEPT_BACKOFF).await;
                    continue;
                }
            };
            if session.strong_count() == 0 || shared.is_shutdown() {
                return;
            }
            let Ok(permit) = Arc::clone(&pending).try_acquire_owned() else {
                debug!(%addr, "too many pending handshakes; dropping connection");
                continue;
            };
            let session = session.clone();
            let shared = Arc::clone(&shared);
            tokio::spawn(async move {
                route(stream, addr, &session, &shared).await;
                drop(permit);
            });
        }
    })
}

async fn route(
    mut stream: TcpStream,
    addr: SocketAddr,
    session: &Weak<SessionInner>,
    shared: &Shared,
) {
    let hs = match timeout(
        shared.settings.handshake_timeout,
        read_handshake(&mut stream),
    )
    .await
    {
        Ok(Ok(hs)) => hs,
        Ok(Err(e)) => {
            debug!(%addr, error = %e, "bad incoming handshake");
            return;
        }
        Err(_) => {
            debug!(%addr, "incoming handshake timed out");
            return;
        }
    };
    if hs.peer_id == shared.peer_id {
        // Our own handshake goes back, so the outgoing side sees its own id
        // and reports `SelfConnection` (never retried) instead of an I/O
        // error; this side is dropped without reaching the torrent.
        debug!(%addr, "dropping incoming connection from ourselves");
        let _ = timeout(
            shared.settings.handshake_timeout,
            write_handshake(&mut stream, &hs),
        )
        .await;
        return;
    }
    let Some(session) = session.upgrade() else {
        return;
    };
    let handle = {
        let reg = session.registry();
        reg.by_wire
            .get(&hs.info_hash)
            .and_then(|ih| reg.torrents.get(ih))
            .map(|e| e.handle.clone())
    };
    match handle {
        Some(h) => {
            if !h.offer_incoming(stream, hs, addr) {
                debug!(%addr, "torrent busy; dropping incoming connection");
            }
        }
        None => debug!(%addr, "incoming connection for an unknown torrent"),
    }
}
