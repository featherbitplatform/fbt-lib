//! [`ApiServer`]: the API on its own listener.
//!
//! The server owns every connection task (a [`JoinSet`]), unlike
//! `axum::serve`, which detaches them: [`ApiServer::shutdown`] can then
//! promise that nothing of the API (no handler, no body, no
//! [`AppState`](crate::routes::AppState) clone) outlives it.

use std::fmt;
use std::net::SocketAddr;
use std::pin::pin;
use std::time::Duration;

use axum::Router;
use fbt::Session;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{ApiConfig, ApiError};

/// How long [`ApiServer::shutdown`] lets requests in flight finish before
/// it aborts their connections.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// How long the accept loop pauses after a failed `accept` (out of file
/// descriptors, say), so it does not spin.
const ACCEPT_ERROR_PAUSE: Duration = Duration::from_millis(50);

/// Longest wait for a request's head; a client that sends none is
/// disconnected (slowloris guard).
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// The API served on [`ApiConfig::bind`], for clients that do not mount
/// [`router`](crate::router) in a server of their own.
///
/// Stop it with [`shutdown`](Self::shutdown). Dropping it starts the same
/// shutdown without waiting for it. It never shuts the session down.
#[must_use = "dropping the server stops it"]
pub struct ApiServer {
    local_addr: SocketAddr,
    shutdown: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl fmt::Debug for ApiServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiServer")
            .field("local_addr", &self.local_addr)
            .finish_non_exhaustive()
    }
}

impl ApiServer {
    /// Validates `config` (as [`router`](crate::router) does), binds
    /// [`ApiConfig::bind`] and serves the API (HTTP/1.1) on a spawned task.
    ///
    /// # Errors
    ///
    /// The config errors of [`router`](crate::router), and
    /// [`ApiError::Bind`] when the address cannot be bound.
    pub async fn start(session: Session, config: ApiConfig) -> Result<ApiServer, ApiError> {
        let shutdown = CancellationToken::new();
        let router = crate::build_router(session, &config, shutdown.clone())?;
        let listener = TcpListener::bind(config.bind)
            .await
            .map_err(ApiError::Bind)?;
        ApiServer::serve(listener, router, shutdown)
    }

    /// Serves `router` on `listener` until `shutdown` fires.
    fn serve(
        listener: TcpListener,
        router: Router,
        shutdown: CancellationToken,
    ) -> Result<ApiServer, ApiError> {
        let local_addr = listener.local_addr().map_err(ApiError::Bind)?;
        let task = tokio::spawn(accept_loop(listener, router, shutdown.clone()));
        Ok(ApiServer {
            local_addr,
            shutdown,
            task: Some(task),
        })
    }

    /// The bound address (with the real port when
    /// [`ApiConfig::bind`] asked for port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops the server: closes the listener, ends every open event and
    /// file stream, closes idle connections, gives the requests in flight
    /// 5 s to finish, then aborts their connections. When it returns,
    /// every connection is closed and every handler and body of this
    /// server has been dropped.
    pub async fn shutdown(mut self) {
        self.shutdown.cancel();
        if let Some(task) = self.task.take()
            && let Err(e) = task.await
            && e.is_panic()
        {
            tracing::warn!("api server task panicked");
        }
    }
}

impl Drop for ApiServer {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// Accepts connections until `shutdown` fires, then drains them: idle
/// ones close at once, busy ones get [`SHUTDOWN_GRACE`], the rest are
/// aborted. Returns once every connection task is gone.
async fn accept_loop(listener: TcpListener, router: Router, shutdown: CancellationToken) {
    let mut conns = JoinSet::new();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    conns.spawn(connection(stream, router.clone(), shutdown.clone()));
                }
                Err(e) => {
                    tracing::debug!(error = %e, "api accept failed");
                    tokio::time::sleep(ACCEPT_ERROR_PAUSE).await;
                }
            },
            // Reap finished connections so the set does not grow.
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
        }
    }
    drop(listener);
    drop(router);
    let drained = tokio::time::timeout(SHUTDOWN_GRACE, async {
        while conns.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        conns.abort_all();
        while conns.join_next().await.is_some() {}
    }
}

/// Serves one connection; on `shutdown` it finishes the request in flight
/// and closes.
async fn connection(stream: TcpStream, router: Router, shutdown: CancellationToken) {
    let conn = http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .serve_connection(TokioIo::new(stream), TowerToHyperService::new(router));
    let mut conn = pin!(conn);
    tokio::select! {
        result = conn.as_mut() => {
            if let Err(e) = result {
                tracing::trace!(error = %e, "api connection ended");
            }
            return;
        }
        () = shutdown.cancelled() => {}
    }
    conn.as_mut().graceful_shutdown();
    if let Err(e) = conn.await {
        tracing::trace!(error = %e, "api connection ended");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Instant;

    use axum::routing::get;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn shutdown_bounds_stuck_handlers() {
        let probe = Arc::new(());
        let held = Arc::clone(&probe);
        let router = Router::new().route(
            "/stuck",
            get(move || {
                let held = Arc::clone(&held);
                async move {
                    let _held = held;
                    std::future::pending::<()>().await;
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = ApiServer::serve(listener, router, CancellationToken::new()).unwrap();

        let mut client = TcpStream::connect(server.local_addr()).await.unwrap();
        client
            .write_all(b"GET /stuck HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        // The test, the router and the running handler each hold one.
        let deadline = Instant::now() + Duration::from_secs(10);
        while Arc::strong_count(&probe) < 3 {
            assert!(Instant::now() < deadline, "the handler never ran");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let started = Instant::now();
        tokio::time::timeout(SHUTDOWN_GRACE + Duration::from_secs(1), server.shutdown())
            .await
            .expect("shutdown returns within the grace period plus 1 s");
        assert!(started.elapsed() >= SHUTDOWN_GRACE - Duration::from_millis(100));
        // Handler and router are gone, not detached.
        assert_eq!(Arc::strong_count(&probe), 1);
        // The client's connection is closed: EOF or a reset, never a hang.
        let mut buf = [0u8; 64];
        let read = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
            .await
            .expect("the connection is closed");
        assert!(matches!(read, Ok(0) | Err(_)), "{read:?}");
    }

    #[tokio::test]
    async fn shutdown_closes_idle_connections_at_once() {
        let router = Router::new().route("/", get(|| async { "ok" }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = ApiServer::serve(listener, router, CancellationToken::new()).unwrap();
        let mut client = TcpStream::connect(server.local_addr()).await.unwrap();
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 256];
        let n = client.read(&mut buf).await.unwrap();
        assert!(buf[..n].starts_with(b"HTTP/1.1 200"));
        // The keep-alive connection is idle now.
        tokio::time::timeout(Duration::from_secs(2), server.shutdown())
            .await
            .expect("an idle connection does not hold up shutdown");
    }
}
