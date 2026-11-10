//! Accepts incoming peer connections and routes them to torrents by the
//! info hash in their handshake. The first 20 bytes tell a plaintext
//! handshake from MSE; MSE connections are answered as responder and
//! matched to a torrent by `HASH('req2', SKEY)`.

use std::net::SocketAddr;
use std::sync::{Arc, Weak};
use std::time::Duration;

use fbt_wire::{
    CryptoMethods, Handshake, MseStream, is_plaintext_handshake_prefix, read_handshake, respond,
    write_handshake,
};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};
use tracing::debug;

use crate::peer::conn::{PeerStream, after};
use crate::session::{SessionInner, Shared};
use crate::settings::EncryptionPolicy;

/// Pause after a failed `accept` (for example out of file descriptors).
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// Runs the accept loop until the session goes away or the task is aborted.
pub(crate) fn spawn(
    listener: TcpListener,
    session: Weak<SessionInner>,
    shared: Arc<Shared>,
) -> JoinHandle<()> {
    // Connections still sending their handshake; more are dropped at once.
    let permits = shared
        .settings
        .max_peers_global
        .clamp(1, Semaphore::MAX_PERMITS);
    let pending = Arc::new(Semaphore::new(permits));
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

/// Why an incoming connection never reached a torrent (logged at debug).
type Refusal = std::borrow::Cow<'static, str>;

/// Classifies, decrypts and routes one accepted connection. Everything up
/// to handing it to the torrent (MSE included) shares one
/// `handshake_timeout` deadline, so a stalled peer gives its pending slot
/// back in time.
async fn route(stream: TcpStream, addr: SocketAddr, session: &Weak<SessionInner>, shared: &Shared) {
    let deadline = after(Instant::now(), shared.settings.handshake_timeout);
    let (mut stream, hs) = match timeout_at(deadline, accept(stream, session, shared)).await {
        Ok(Ok(c)) => c,
        Ok(Err(why)) => {
            debug!(%addr, %why, "incoming connection refused");
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
        let _ = timeout_at(deadline, write_handshake(&mut stream, &hs)).await;
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

/// Tells plaintext from MSE by the first 20 bytes, applies the encryption
/// policy, runs the MSE responder if needed and reads the BitTorrent
/// handshake. Over MSE the handshake must name the torrent of the `SKEY`.
async fn accept(
    mut stream: TcpStream,
    session: &Weak<SessionInner>,
    shared: &Shared,
) -> Result<(PeerStream, Handshake), Refusal> {
    let policy = shared.settings.encryption;
    let mut first = [0u8; 20];
    // A short read is a closed peer, not a classification.
    stream
        .read_exact(&mut first)
        .await
        .map_err(|e| Refusal::Owned(format!("closed before its first bytes: {e}")))?;
    let (mut stream, skey) = if is_plaintext_handshake_prefix(&first) {
        if policy == EncryptionPolicy::Forced {
            return Err("plaintext handshake under the forced encryption policy".into());
        }
        (MseStream::plaintext(stream, first.to_vec()), None)
    } else {
        let allowed = match policy {
            EncryptionPolicy::Disabled => {
                return Err("MSE handshake under the disabled encryption policy".into());
            }
            EncryptionPolicy::Forced => CryptoMethods::RC4,
            EncryptionPolicy::Preferred => CryptoMethods::RC4 | CryptoMethods::PLAINTEXT,
        };
        // One map lookup per handshake, keyed by HASH('req2', SKEY).
        let lookup = |req2: &[u8; 20]| {
            session
                .upgrade()
                .and_then(|s| s.registry().by_req2.get(req2).copied())
        };
        let (stream, skey) = respond(stream, first.to_vec(), &lookup, allowed)
            .await
            .map_err(|e| Refusal::Owned(format!("MSE handshake failed: {e}")))?;
        (stream, Some(skey))
    };
    let hs = read_handshake(&mut stream)
        .await
        .map_err(|e| Refusal::Owned(format!("bad handshake: {e}")))?;
    if skey.is_some_and(|k| k != hs.info_hash) {
        return Err("handshake names another torrent than the MSE SKEY".into());
    }
    Ok((stream, hs))
}
