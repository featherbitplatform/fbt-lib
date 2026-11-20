//! [`ApiServer`]: the API on its own listener.
//!
//! The server owns every connection task (a [`JoinSet`]), unlike
//! `axum::serve`, which detaches them: [`ApiServer::shutdown`] can then
//! promise that nothing of the API (no handler, no body, no
//! [`AppState`](crate::routes::AppState) clone) outlives it. Every request
//! carries its connection's peer address as axum's `ConnectInfo`, which the
//! [`Auth::Disabled`](crate::Auth::Disabled) guard reads, and every
//! connection is watched for write stalls: a client that stops reading gets
//! dropped after [`ApiConfig::stream_idle_timeout`](crate::ApiConfig::stream_idle_timeout)
//! of no progress, bodies and permits included. Request bodies are guarded
//! the other way around: a client that sends headers and then dribbles a
//! body (or never sends it) holds its connection and its
//! [`ApiConfig::max_connections`](crate::ApiConfig::max_connections) permit
//! until [`ApiConfig::body_read_timeout`](crate::ApiConfig::body_read_timeout)
//! runs out, extended by the bytes it keeps arriving at
//! [`ApiConfig::body_min_rate`](crate::ApiConfig::body_min_rate).

use std::fmt;
use std::io::{self, IoSlice};
use std::net::SocketAddr;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::Router;
use axum::extract::ConnectInfo;
use bytes::Bytes;
use fbt::Session;
use http_body::Frame;
use hyper::Request;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::Service;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant as TokioInstant, sleep};
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
        Self::start_with(session, config, Router::new()).await
    }

    /// [`start`](Self::start), with `extra` — routes of your own — served
    /// beside the API on this listener. They are merged ahead of fbt-api's
    /// layers ([`router_with`](crate::router_with) spells out what that gives
    /// them and what to avoid): the same [`Auth`](crate::Auth) decides who may
    /// reach them, they get [`ApiConfig::max_body_bytes`] as axum's
    /// `DefaultBodyLimit` — which the body extractors (`Bytes`, `String`,
    /// `Json`, `Form`, …) enforce, while a handler that consumes `Body` itself
    /// must cap it — and the same CORS layer. Being on this listener, they hold
    /// one of [`ApiConfig::max_connections`] slots, end with the stall guard
    /// ([`ApiConfig::stream_idle_timeout`](crate::ApiConfig::stream_idle_timeout))
    /// and close on [`shutdown`](Self::shutdown). Only CORS preflights are
    /// answered before auth, so a login route of yours has to be let through by
    /// your own [`Authenticator`](crate::Authenticator) — `Auth::Custom`, as
    /// `Auth::Token` wants the bearer token from everything else.
    ///
    /// ```no_run
    /// # async fn demo(session: fbt::Session) -> Result<(), Box<dyn std::error::Error>> {
    /// use axum::{Router, routing::get};
    /// use fbt_api::{ApiConfig, ApiServer};
    ///
    /// // The embedder's own routes, on paths fbt-api does not serve. Under
    /// // `Auth::Token` they need the bearer token like every other request here;
    /// // a login route that issues tokens must be let through by an
    /// // `Auth::Custom` authenticator instead.
    /// let mine = Router::new().route("/api/v1/fs/tree", get(|| async { "tree" }));
    /// let server = ApiServer::start_with(session, ApiConfig::new("secret-token"), mine).await?;
    /// println!("API on http://{}", server.local_addr());
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Panics
    ///
    /// When `extra` registers a route fbt-api already serves: axum panics when
    /// two routers answer the same method on the same path. Everything fbt-api
    /// serves is under `/api/v1/`, exactly the paths [`openapi`](crate::openapi)
    /// lists — so pick other ones (`/api/v1/auth/login` and `/api/v1/fs/tree`
    /// are free, `GET /api/v1/session` is not). Do not add a method to a path
    /// fbt-api serves either: a different one merges without panicking, but
    /// neither the document nor the auth rules would know it, and a `HEAD`
    /// there silently takes over what that path answers to `HEAD`.
    ///
    /// # Errors
    ///
    /// Those of [`start`](Self::start).
    pub async fn start_with(
        session: Session,
        config: ApiConfig,
        extra: Router,
    ) -> Result<ApiServer, ApiError> {
        let shutdown = CancellationToken::new();
        let router = crate::build_router(session, &config, shutdown.clone(), extra)?;
        let listener = TcpListener::bind(config.bind)
            .await
            .map_err(ApiError::Bind)?;
        ApiServer::serve(
            listener,
            router,
            shutdown,
            config.max_connections,
            config.stream_idle_timeout,
            config.body_read_timeout,
            config.body_min_rate,
        )
    }

    /// Serves `router` on `listener` until `shutdown` fires, ending a
    /// connection whose writes have made no progress for `stall_timeout`
    /// ([`Duration::ZERO`] disables that guard) and one whose request body
    /// takes longer than `body_read_timeout`, extended byte for byte at
    /// `body_min_rate` (also [`Duration::ZERO`]-off).
    fn serve(
        listener: TcpListener,
        router: Router,
        shutdown: CancellationToken,
        max_connections: usize,
        stall_timeout: Duration,
        body_read_timeout: Duration,
        body_min_rate: u32,
    ) -> Result<ApiServer, ApiError> {
        let local_addr = listener.local_addr().map_err(ApiError::Bind)?;
        let limit = Arc::new(Semaphore::new(
            max_connections.clamp(1, Semaphore::MAX_PERMITS),
        ));
        // The tower service is built once for the listener; each connection
        // only wraps it (see [`WithPeer`]) instead of re-layering the
        // router.
        let base = TowerToHyperService::new(router);
        let task = tokio::spawn(accept_loop(
            listener,
            base,
            shutdown.clone(),
            limit,
            stall_timeout,
            body_read_timeout,
            body_min_rate,
        ));
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
///
/// It takes a permit of `limit` before each `accept`, and the connection
/// holds it until it closes: with every permit taken, new clients wait in
/// the listener's backlog and cost no file descriptor.
async fn accept_loop(
    listener: TcpListener,
    base: TowerToHyperService<Router>,
    shutdown: CancellationToken,
    limit: Arc<Semaphore>,
    stall_timeout: Duration,
    body_read_timeout: Duration,
    body_min_rate: u32,
) {
    let mut conns = JoinSet::new();
    let mut permit: Option<OwnedSemaphorePermit> = None;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            acquired = Arc::clone(&limit).acquire_owned(), if permit.is_none() => {
                // The semaphore is never closed.
                permit = acquired.ok();
            }
            accepted = listener.accept(), if permit.is_some() => match accepted {
                Ok((stream, peer)) => {
                    conns.spawn(connection(
                        stream,
                        peer,
                        base.clone(),
                        shutdown.clone(),
                        permit.take(),
                        stall_timeout,
                        body_read_timeout,
                        body_min_rate,
                    ));
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
    drop(base);
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
/// and closes. `peer` (the accepted address) is attached to every request
/// as axum's [`ConnectInfo`] by [`WithPeer`], as `axum::serve` does for its
/// own listeners: the auth layer reads it under
/// [`Auth::Disabled`](crate::Auth::Disabled). `permit` (a slot of
/// [`ApiConfig::max_connections`]) is released when it returns.
///
/// The connection's socket is wrapped in [`StalledIo`], and every wake
/// re-arms a timer for what its [`StallWatch`] says is left before a write
/// has gone `stall_timeout` without the kernel accepting a byte. When that
/// timer fires with bytes still owed, the whole connection is aborted: the
/// body it was serving is dropped with its stream permit and its reader.
/// A client that keeps draining (however slowly), an idle keep-alive
/// connection and a body waiting for pieces to write all make or await
/// progress and are left alone — [`Duration::ZERO`] disables the guard.
async fn connection(
    stream: TcpStream,
    peer: SocketAddr,
    base: TowerToHyperService<Router>,
    shutdown: CancellationToken,
    permit: Option<OwnedSemaphorePermit>,
    stall_timeout: Duration,
    body_read_timeout: Duration,
    body_min_rate: u32,
) {
    let _permit = permit;
    let watch = Arc::new(StallWatch::new());
    let io = TokioIo::new(StalledIo {
        inner: stream,
        watch: Arc::clone(&watch),
    });
    // Cancelled by [`BodyDeadline`] when a request body takes too long to
    // arrive: dropping the connection future then ends the request the body
    // belongs to, exactly like the stall branch below.
    let body_stall = CancellationToken::new();
    let service = WithPeer {
        inner: base.clone(),
        peer,
        body_read_timeout,
        body_min_rate,
        body_stall: Arc::new(body_stall.clone()),
    };
    let mut conn = pin!(
        http1::Builder::new()
            .timer(TokioTimer::new())
            .header_read_timeout(HEADER_READ_TIMEOUT)
            .serve_connection(io, service)
    );
    // Re-armed from the watch below every round; only polled while the
    // guard is on, so a disabled timeout never registers with the timer.
    let mut stalled = pin!(sleep(stall_timeout));
    loop {
        stalled
            .as_mut()
            .reset(arm_deadline(watch.time_left(stall_timeout)));
        tokio::select! {
            biased;
            result = conn.as_mut() => {
                if let Err(e) = result {
                    tracing::trace!(error = %e, "api connection ended");
                }
                return;
            }
            () = body_stall.cancelled() => {
                // A request body took too long to arrive; the handler got
                // its error. Drop the connection: the request, its body and
                // this connection's permit go with it.
                return;
            }
            () = shutdown.cancelled() => {
                conn.as_mut().graceful_shutdown();
                break;
            }
            () = stalled.as_mut(), if !stall_timeout.is_zero() => {
                // Recomputed on every wake, so a firing timer means the
                // socket has taken no byte for the full timeout. Drop the
                // connection: its body, its permit and its reader go with
                // it. (If progress raced in, `stalled_out` says no and the
                // loop re-arms.)
                if watch.stalled_out(stall_timeout) {
                    // Dropping the connection future closes the socket;
                    // the body, its permit and its reader go with it.
                    tracing::debug!(%peer, "api connection stalled out");
                    return;
                }
            }
        }
    }
    if let Err(e) = conn.await {
        tracing::trace!(error = %e, "api connection ended");
    }
}

/// The stall timer's deadline `left` from now. Adding a huge remaining timeout
/// (an [`ApiConfig::stream_idle_timeout`](crate::ApiConfig::stream_idle_timeout)
/// of [`Duration::MAX`], say) overflows `Instant` and panics — every connection
/// task at once — so an overflowing sum falls back to a year out, the far
/// future tokio's own sleep clamp parks absurd durations at.
fn arm_deadline(left: Duration) -> TokioInstant {
    let now = TokioInstant::now();
    now.checked_add(left).unwrap_or_else(|| {
        now.checked_add(Duration::from_secs(365 * 24 * 60 * 60))
            .unwrap_or(now)
    })
}

/// One incoming request body with a deadline on how long it may take to
/// arrive: the client gets `timeout`, plus one second of reading per
/// `min_rate` bytes it has delivered (see
/// [`ApiConfig::body_min_rate`](crate::ApiConfig::body_min_rate) for the
/// `0` case). The clock starts at the first
/// [`poll_frame`](http_body::Body::poll_frame) (when a handler begins
/// reading the body, not when the request arrived), so a handler that
/// awaits something else first does not spend the client's allowance.
///
/// Every data frame pushes the deadline out by the bytes it carried, so a
/// client that keeps an average rate of at least `min_rate` bytes per
/// second is never cut off, however it chunks. A mere idle-gap timer would
/// not be enough: one byte every 29 seconds never trips one.
///
/// On time-out the body yields an error and `stall` is cancelled, so the
/// connection task ends the whole connection and its
/// [`ApiConfig::max_connections`](crate::ApiConfig::max_connections) permit
/// with it. The clock is tokio's, so a paused test clock advances it
/// exactly.
struct BodyDeadline<B> {
    inner: B,
    timeout: Duration,
    min_rate: u32,
    stall: Arc<CancellationToken>,
    /// When the first `poll_frame` ran; the deadline is measured from here.
    started: Option<TokioInstant>,
    /// Bytes of data received so far.
    received: u64,
    /// The armed sleep: due when the deadline is, re-armed whenever the
    /// deadline moves, polled only while the inner body is `Pending`.
    timer: Pin<Box<tokio::time::Sleep>>,
    /// The guard is finished: a time-out already ran, or the stream ended.
    /// `poll_frame` then never looks at the clock again.
    closed: bool,
}

impl<B> BodyDeadline<B>
where
    B: http_body::Body<Data = Bytes>,
{
    fn new(
        inner: B,
        timeout: Duration,
        min_rate: u32,
        stall: Arc<CancellationToken>,
    ) -> Self {
        Self {
            inner,
            timeout,
            min_rate,
            stall,
            started: None,
            received: 0,
            // Never polled before the first `poll_frame` arms it there.
            timer: Box::pin(sleep(Duration::from_secs(365 * 24 * 60 * 60))),
            closed: false,
        }
    }

    /// The deadline as a duration from `started`: the base allowance plus
    /// the bytes received at the minimum rate, rounded up to the next
    /// millisecond (a body must not be billed less than its bytes are
    /// worth). Saturation keeps a hostile combination (a `u64::MAX` frame
    /// over a huge `min_rate`) out of a panic; `min_rate` of 0 extends the
    /// deadline by nothing.
    fn deadline(&self) -> Duration {
        let min_rate = u64::from(self.min_rate).max(1);
        // `received / min_rate` seconds, in milliseconds, rounded up.
        let extra_ms = (self.received.saturating_mul(1000).saturating_add(min_rate - 1))
            / min_rate;
        let extra_ms = extra_ms.min(u64::MAX / 4);
        let total_ms = self
            .timeout
            .as_millis()
            .min(u64::MAX as u128 / 4)
            .saturating_add(extra_ms as u128)
            .min(u64::MAX as u128 / 4);
        Duration::from_millis(total_ms as u64)
    }

    /// True when the deadline has passed; runs the time-out on the first
    /// observation of it (logs once, cancels the connection token) and
    /// stays true after. The deadline is measured from `started`, not from
    /// the moment of this call: a body whose clock started long ago is
    /// still past its deadline now, even if this check runs late.
    fn stalled_out(&mut self) -> bool {
        if self.closed {
            return true;
        }
        if self.timeout.is_zero() {
            return false;
        }
        let Some(started) = self.started else {
            return false;
        };
        let deadline = self.deadline();
        // The deadline is `started + deadline`; it has passed when the
        // elapsed time since `started` reaches `deadline`. `checked_add`
        // keeps a huge `deadline` out of a panic (see [`arm_deadline`]).
        let due = started
            .checked_add(deadline)
            .unwrap_or_else(|| started.checked_add(Duration::MAX).unwrap_or(started));
        if TokioInstant::now() < due {
            return false;
        }
        self.closed = true;
        tracing::debug!("api request body too slow");
        self.stall.cancel();
        true
    }

    /// The time-out's error.
    fn timeout_error() -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(io::Error::new(io::ErrorKind::TimedOut, "request body too slow"))
    }
}

impl<B> http_body::Body for BodyDeadline<B>
where
    B: http_body::Body<Data = Bytes> + Unpin,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        // `B` is `Unpin`, so `Self` is too and every piece is a plain
        // projection; the inner body is polled through its own `Pin`.
        let this = self.get_mut();
        if this.closed {
            return Poll::Ready(Some(Err(Self::timeout_error())));
        }
        if this.timeout.is_zero() {
            // The guard is off: pass the inner body through.
            return match Pin::new(&mut this.inner).poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    if frame.data_ref().is_none() {
                        this.closed = true;
                    }
                    Poll::Ready(Some(Ok(frame)))
                }
                Poll::Ready(None) => {
                    this.closed = true;
                    Poll::Ready(None)
                }
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
                Poll::Pending => Poll::Pending,
            };
        }
        let started = match this.started {
            Some(started) => started,
            None => {
                let started = TokioInstant::now();
                this.started = Some(started);
                let deadline = this.deadline();
                let due = started
                    .checked_add(deadline)
                    .unwrap_or_else(|| started.checked_add(Duration::MAX).unwrap_or(started));
                this.timer.as_mut().reset(due);
                started
            }
        };
        if this.stalled_out() {
            return Poll::Ready(Some(Err(Self::timeout_error())));
        }
        let poll = Pin::new(&mut this.inner).poll_frame(cx);
        match poll {
            Poll::Ready(None) => {
                // End of stream: the guard is done with it.
                this.closed = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Ok(frame))) => {
                let received = frame
                    .data_ref()
                    .map(|d| u64::try_from(d.len()).unwrap_or(u64::MAX))
                    .unwrap_or(0);
                this.received = this.received.saturating_add(received);
                if frame.data_ref().is_none() {
                    // A trailers frame ends the stream too.
                    this.closed = true;
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
            Poll::Pending => {
                // The deadline may have moved since the last poll (a data
                // frame extended it), so re-arm at `started + deadline`
                // and wait on it: the timeout must fire even while no byte
                // arrives.
                let deadline = this.deadline();
                let due = started
                    .checked_add(deadline)
                    .unwrap_or_else(|| started.checked_add(Duration::MAX).unwrap_or(started));
                this.timer.as_mut().reset(due);
                match this.timer.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        if this.stalled_out() {
                            Poll::Ready(Some(Err(Self::timeout_error())))
                        } else {
                            // The deadline moved past the timer's due
                            // moment; the inner body still has what it
                            // had, and the caller polls again.
                            Poll::Pending
                        }
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

/// Write-progress watch of one connection, shared between [`StalledIo`]
/// (which records what the kernel accepted) and the connection task (which
/// decides when bytes have been owed too long). Only that one task polls
/// both halves, so nothing is ever truly contended; the `Arc` exists only
/// because hyper owns the IO inside the connection future. Progress and
/// staleness are milliseconds since `base`, the moment of accept.
#[derive(Debug)]
struct StallWatch {
    base: TokioInstant,
    /// Milliseconds since `base` of the last write progress — lifted to the
    /// moment a fresh debt starts ([`owed_now`](Self::owed_now)) when nothing
    /// was owed before it, so [`stalled_out`](Self::stalled_out) measures how
    /// long the current bytes have been stuck rather than how long since the
    /// last accepted byte.
    progress_ms: AtomicU64,
    /// Bytes this connection owes the peer that the kernel has not taken:
    /// a `poll_write` ended `Pending`, or took only part of what it was
    /// handed (the rest is queued above the socket, unflushed).
    pending: AtomicBool,
}

impl StallWatch {
    fn new() -> Self {
        Self {
            base: TokioInstant::now(),
            progress_ms: AtomicU64::new(0),
            pending: AtomicBool::new(false),
        }
    }

    /// Milliseconds since `base`, saturating.
    fn now_ms(&self) -> u64 {
        u64::try_from(self.base.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Records that the kernel took `n` bytes of a write of `len`: real
    /// progress, and nothing owed from that call if it was taken whole.
    fn recorded(&self, n: usize, len: usize) {
        self.progress_ms.store(self.now_ms(), Ordering::Relaxed);
        self.pending.store(n < len, Ordering::Relaxed);
    }

    /// Records that a write just went stuck. If nothing was owed before this
    /// moment, the owed clock starts here too: a body with nothing to send
    /// for a long stretch gets its full window once a write does stick,
    /// instead of dying at once against a long-past last accept.
    fn owed_now(&self) {
        if !self.pending.swap(true, Ordering::Relaxed) {
            self.progress_ms.fetch_max(self.now_ms(), Ordering::Relaxed);
        }
    }

    /// True when bytes are owed and the kernel has taken none for `timeout`.
    fn stalled_out(&self, timeout: Duration) -> bool {
        self.pending.load(Ordering::Relaxed)
            && self
                .now_ms()
                .saturating_sub(self.progress_ms.load(Ordering::Relaxed))
                >= u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)
    }

    /// How long until [`stalled_out`](Self::stalled_out) can next turn true:
    /// the full `timeout` while nothing is owed, the remainder of it once
    /// bytes are. The connection task re-arms its timer with this on every
    /// wake, so progress always pushes the deadline back.
    fn time_left(&self, timeout: Duration) -> Duration {
        if !self.pending.load(Ordering::Relaxed) {
            return timeout;
        }
        let owed_for = self
            .now_ms()
            .saturating_sub(self.progress_ms.load(Ordering::Relaxed));
        let total = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
        Duration::from_millis(total.saturating_sub(owed_for))
    }
}

/// The socket hyper sees: `inner` verbatim, plus notes to a [`StallWatch`]
/// about what the kernel accepted.
///
/// Progress is exactly what `poll_write` handed to the kernel. A nonblocking
/// TCP socket has no userspace buffer of its own — it takes bytes or blocks
/// — so a ready `poll_flush` carries nothing new, and counting flushes as
/// progress too would feed the watch on every wake instead of only real
/// movement ("written but unflushed" is what a partial `poll_write` left
/// behind, which [`recorded`](StallWatch::recorded) already marks pending;
/// a write that sticks outright starts the owed clock at that moment, see
/// [`owed_now`](StallWatch::owed_now)). `is_write_vectored` and
/// `poll_write_vectored` are forwarded to the socket so hyper keeps its
/// vectored write strategy through the wrapper; the run's iovecs count as
/// one write of their summed length.
struct StalledIo<T> {
    inner: T,
    watch: Arc<StallWatch>,
}

impl<T: AsyncRead + Unpin> AsyncRead for StalledIo<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for StalledIo<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) if n > 0 => {
                this.watch.recorded(n, buf.len());
                Poll::Ready(Ok(n))
            }
            Poll::Pending => {
                this.watch.owed_now();
                Poll::Pending
            }
            other => other,
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write_vectored(cx, bufs) {
            Poll::Ready(Ok(n)) if n > 0 => {
                let total = bufs.iter().map(|b| b.len()).sum::<usize>();
                this.watch.recorded(n, total);
                Poll::Ready(Ok(n))
            }
            Poll::Pending => {
                this.watch.owed_now();
                Poll::Pending
            }
            other => other,
        }
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

/// Attaches the connection's peer address to every request as axum's
/// [`ConnectInfo`] and bounds how long each request's body may take to
/// arrive ([`BodyDeadline`]). The router is built once for the listener;
/// each connection calls it as a tower service after replacing the request
/// body. `body_stall` is the connection's one [`CancellationToken`],
/// cancelled by [`BodyDeadline`] on time-out; the connection task ends the
/// connection on it.
/// Attaches the connection's peer address to every request as axum's
/// [`ConnectInfo`] and bounds how long each request's body may take to
/// arrive ([`BodyDeadline`]). The tower service is built once for the
/// listener; each connection only wraps it. `body_stall` is the
/// connection's one [`CancellationToken`], cancelled by [`BodyDeadline`]
/// on time-out; the connection task ends the connection on it.
#[derive(Clone)]
struct WithPeer<S> {
    inner: S,
    peer: SocketAddr,
    body_read_timeout: Duration,
    body_min_rate: u32,
    body_stall: Arc<CancellationToken>,
}

impl<S> Service<Request<Incoming>> for WithPeer<S>
where
    S: Service<Request<BodyDeadline<Incoming>>> + Clone,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let (mut parts, incoming) = req.into_parts();
        parts
            .extensions
            .insert(ConnectInfo(self.peer));
        let body = BodyDeadline::new(
            incoming,
            self.body_read_timeout,
            self.body_min_rate,
            Arc::clone(&self.body_stall),
        );
        self.inner.call(Request::from_parts(parts, body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    use axum::routing::get;
    use futures_util::task::noop_waker;
    use http_body::Body as _;
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
        let server = ApiServer::serve(
            listener,
            router,
            CancellationToken::new(),
            256,
            Duration::ZERO,
            Duration::ZERO,
            500,
        )
        .unwrap();

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

    /// Sends `GET /` on `client` and reads the response head.
    async fn get_ok(client: &mut TcpStream) -> bool {
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 256];
        let n = client.read(&mut buf).await.unwrap();
        buf[..n].starts_with(b"HTTP/1.1 200")
    }

    #[tokio::test]
    async fn connection_cap_holds_extra_clients_in_the_backlog() {
        let router = Router::new().route("/", get(|| async { "ok" }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = ApiServer::serve(
            listener,
            router,
            CancellationToken::new(),
            2,
            Duration::ZERO,
            Duration::ZERO,
            500,
        )
        .unwrap();
        let mut a = TcpStream::connect(server.local_addr()).await.unwrap();
        let mut b = TcpStream::connect(server.local_addr()).await.unwrap();
        assert!(get_ok(&mut a).await);
        assert!(get_ok(&mut b).await);

        // The kernel completes the handshake, but the server does not
        // accept the third connection while two are open.
        let mut c = TcpStream::connect(server.local_addr()).await.unwrap();
        c.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 256];
        let early = tokio::time::timeout(Duration::from_millis(500), c.read(&mut buf)).await;
        assert!(
            early.is_err(),
            "the third connection is not served: {early:?}"
        );

        drop(a);
        let n = tokio::time::timeout(Duration::from_secs(5), c.read(&mut buf))
            .await
            .expect("served once a slot frees")
            .unwrap();
        assert!(buf[..n].starts_with(b"HTTP/1.1 200"));
        drop(b);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_closes_idle_connections_at_once() {
        let router = Router::new().route("/", get(|| async { "ok" }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = ApiServer::serve(
            listener,
            router,
            CancellationToken::new(),
            256,
            Duration::ZERO,
            Duration::ZERO,
            500,
        )
        .unwrap();
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

    /// Every request carries its connection's peer address as axum's
    /// `ConnectInfo`, whatever the router mounted (the auth layer's
    /// `Auth::Disabled` peer check reads that extension).
    #[tokio::test]
    async fn connect_info_reaches_handlers() {
        let router = Router::new().route(
            "/peer",
            get(|ConnectInfo(peer): ConnectInfo<SocketAddr>| async move { peer.ip().to_string() }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = ApiServer::serve(
            listener,
            router,
            CancellationToken::new(),
            256,
            Duration::ZERO,
            Duration::ZERO,
            500,
        )
        .unwrap();

        let mut client = TcpStream::connect(server.local_addr()).await.unwrap();
        client
            .write_all(b"GET /peer HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut got = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut buf = [0u8; 256];
            while !got.ends_with(b"127.0.0.1") {
                match client.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => got.extend_from_slice(&buf[..n]),
                }
            }
        })
        .await
        .expect("the peer address reaches the handler");
        let text = String::from_utf8_lossy(&got);
        assert!(text.contains("200 OK"), "{text}");
        assert!(text.ends_with("127.0.0.1"), "{text}");
        server.shutdown().await;
    }

    /// A writer that takes `budget` bytes and then blocks forever, like a
    /// socket whose peer stopped reading.
    struct Sink {
        taken: usize,
        budget: usize,
    }

    impl AsyncWrite for Sink {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            if this.taken >= this.budget {
                return Poll::Pending;
            }
            let n = buf.len().min(this.budget - this.taken);
            this.taken += n;
            Poll::Ready(Ok(n))
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            if this.taken >= this.budget {
                return Poll::Pending;
            }
            let n = bufs
                .iter()
                .map(|b| b.len())
                .sum::<usize>()
                .min(this.budget - this.taken);
            this.taken += n;
            Poll::Ready(Ok(n))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// The watch records what `poll_write` handed over, marks a partial or
    /// blocked write pending, ignores flushes (which carry nothing new for
    /// this IO), and clears on resumed progress.
    #[tokio::test]
    async fn write_watch_tracks_the_socket() {
        let watch = Arc::new(StallWatch::new());
        let mut io = StalledIo {
            inner: Sink {
                taken: 0,
                budget: 5,
            },
            watch: Arc::clone(&watch),
        };
        let waker = noop_waker();
        let cx = &mut Context::from_waker(&waker);

        // A partial take is progress — and the rest of that write is owed.
        assert!(matches!(
            Pin::new(&mut io).poll_write(cx, b"0123456789"),
            Poll::Ready(Ok(5))
        ));
        assert!(watch.pending.load(Ordering::Relaxed), "the rest is owed");

        // A blocked write stays pending; time past the timeout turns it
        // into a stall...
        assert!(Pin::new(&mut io).poll_write(cx, b"more").is_pending());
        tokio::time::sleep(Duration::from_millis(60)).await;
        // ...a ready flush changes nothing (nothing new moved)...
        assert!(matches!(
            Pin::new(&mut io).poll_flush(cx),
            Poll::Ready(Ok(()))
        ));
        assert!(
            watch.stalled_out(Duration::from_millis(50)),
            "flush fed nothing"
        );
        assert!(
            watch.time_left(Duration::from_millis(50)).is_zero(),
            "the timer is due"
        );

        // ...and accepted bytes restart the watch.
        io.inner.taken = 0;
        assert!(matches!(
            Pin::new(&mut io).poll_write(cx, b"go"),
            Poll::Ready(Ok(2))
        ));
        assert!(!watch.pending.load(Ordering::Relaxed), "taken whole");
        assert!(!watch.stalled_out(Duration::from_millis(50)));
        assert!(watch.time_left(Duration::from_secs(1)) > Duration::from_millis(900));

        // A fresh watch owes nothing and cannot stall.
        let idle = StallWatch::new();
        assert!(!idle.pending.load(Ordering::Relaxed));
        assert!(!idle.stalled_out(Duration::ZERO));

        // Vectored writes count against the summed run, not the last slice.
        let vwatch = Arc::new(StallWatch::new());
        let mut vio = StalledIo {
            inner: Sink {
                taken: 0,
                budget: 3,
            },
            watch: Arc::clone(&vwatch),
        };
        assert!(vio.is_write_vectored(), "forwarded from the socket");
        let bufs = [IoSlice::new(b"12"), IoSlice::new(b"3456")];
        assert!(matches!(
            Pin::new(&mut vio).poll_write_vectored(cx, &bufs),
            Poll::Ready(Ok(3))
        ));
        assert!(
            vwatch.pending.load(Ordering::Relaxed),
            "the run's tail is owed"
        );
        vio.inner.budget = 10;
        assert!(matches!(
            Pin::new(&mut vio).poll_write_vectored(cx, &[IoSlice::new(b"abc")]),
            Poll::Ready(Ok(3))
        ));
        assert!(
            !vwatch.pending.load(Ordering::Relaxed),
            "a whole run clears the flag"
        );
    }

    /// A body that never arrives times out and cancels its token.
    #[tokio::test(start_paused = true)]
    async fn body_deadline_fires_on_a_pending_body() {
        struct Pending;
        impl http_body::Body for Pending {
            type Data = Bytes;
            type Error = Box<dyn std::error::Error + Send + Sync>;
            fn poll_frame(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
                Poll::Pending
            }
        }
        let token = Arc::new(CancellationToken::new());
        let mut body = BodyDeadline::new(
            Pending,
            Duration::from_millis(100),
            1000,
            Arc::clone(&token),
        );
        let waker = noop_waker();
        let cx = &mut Context::from_waker(&waker);

        // First poll starts the clock and goes pending.
        assert!(Pin::new(&mut body).poll_frame(cx).is_pending());
        // The deadline is 100 ms out; the clock is paused, so no time has
        // passed.
        assert!(!token.is_cancelled(), "the deadline has not passed yet");
        // Past the deadline the body times out and cancels the token.
        sleep(Duration::from_millis(150)).await;
        assert!(matches!(
            Pin::new(&mut body).poll_frame(cx),
            Poll::Ready(Some(Err(_)))
        ));
        assert!(token.is_cancelled(), "the timeout cancels the connection");
        // The guard stays latched.
        assert!(matches!(
            Pin::new(&mut body).poll_frame(cx),
            Poll::Ready(Some(Err(_)))
        ));
    }

    /// A body with nothing to send for a long stretch is not cut off at once
    /// when its first write sticks: the owed clock starts when the debt does.
    /// The watch reads tokio's clock, so a paused clock and the sleeps below
    /// advance it exactly — no real-timer slack around the 50 ms window.
    #[tokio::test(start_paused = true)]
    async fn owed_clock_starts_with_the_debt() {
        let watch = Arc::new(StallWatch::new());
        let mut io = StalledIo {
            inner: Sink {
                taken: 0,
                budget: 0,
            },
            watch: Arc::clone(&watch),
        };
        let waker = noop_waker();
        let cx = &mut Context::from_waker(&waker);

        // Quiet for longer than the timeout would allow...
        tokio::time::sleep(Duration::from_millis(60)).await;
        // ...the first stuck write starts a fresh debt, not an old one:
        assert!(Pin::new(&mut io).poll_write(cx, b"hi").is_pending());
        assert!(
            !watch.stalled_out(Duration::from_millis(50)),
            "the debt is new"
        );
        assert!(watch.time_left(Duration::from_millis(50)) > Duration::from_millis(40));

        // and it matures a full timeout after the write stuck.
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(watch.stalled_out(Duration::from_millis(50)));
    }

    /// A `Duration::MAX` stall timeout must not panic the connection task
    /// while arming its timer: an overflowing deadline falls back to a year
    /// out, and neither the parked timer nor the guard holds shutdown.
    #[tokio::test]
    async fn stall_timer_survives_the_largest_timeout() {
        let router = Router::new().route("/", get(|| async { "ok" }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = ApiServer::serve(
            listener,
            router,
            CancellationToken::new(),
            256,
            Duration::MAX,
            Duration::ZERO,
            500,
        )
        .unwrap();
        let mut client = TcpStream::connect(server.local_addr()).await.unwrap();
        assert!(get_ok(&mut client).await, "the task did not panic");
        tokio::time::timeout(Duration::from_secs(2), server.shutdown())
            .await
            .expect("the far-future timer does not hold shutdown");
    }
}
