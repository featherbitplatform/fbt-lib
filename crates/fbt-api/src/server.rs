//! [`ApiServer`]: the API on its own listener.

use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

use fbt::Session;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{ApiConfig, ApiError};

/// How long [`ApiServer::shutdown`] waits for open connections to finish
/// before it aborts the server.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// The API served on [`ApiConfig::bind`], for clients that do not mount
/// [`router`](crate::router) in a server of their own.
///
/// Stop it with [`shutdown`](Self::shutdown). Dropping it also stops it,
/// without waiting. It does not shut the session down.
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
    /// [`ApiConfig::bind`] and serves the API on a spawned task.
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
        let local_addr = listener.local_addr().map_err(ApiError::Bind)?;
        let signal = shutdown.clone().cancelled_owned();
        let task = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, router)
                .with_graceful_shutdown(signal)
                .await
            {
                tracing::warn!(error = %e, "api server stopped");
            }
        });
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
    /// file stream, lets the requests in flight finish, and aborts what is
    /// left after 5 s.
    pub async fn shutdown(mut self) {
        self.shutdown.cancel();
        if let Some(mut task) = self.task.take()
            && tokio::time::timeout(SHUTDOWN_GRACE, &mut task)
                .await
                .is_err()
        {
            task.abort();
        }
    }
}

impl Drop for ApiServer {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}
