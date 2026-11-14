//! `/api/v1/events`: the session's events as server-sent events.
//!
//! Each [`Event`](fbt::Event) becomes one message whose `event:` field is
//! its snake_case type and whose `data:` is its [`EventDto`] JSON. A
//! subscriber that falls behind gets `event: lagged` with the number of
//! events it missed. A comment every 15 s, sent as part of the stream so
//! hyper ships it as output, keeps proxies from timing the stream out and
//! counts as activity for the idle timeout — a live but quiet client stays
//! connected, a client that stopped reading does not.
//!
//! A stream holds one of [`AppState::events`]' permits until its body is
//! dropped (the client hung up, or hyper has not polled it for
//! [`stream_idle_timeout`](crate::ApiConfig::stream_idle_timeout)). It ends
//! on [`ApiServer::shutdown`](crate::ApiServer::shutdown) and within 1 s of
//! the session's shutdown — after delivering everything its subscriber had
//! already queued: the session's event sender lives as long as any
//! `Session` clone, this API's included, so the stream polls
//! [`Session::is_shut_down`] instead of waiting for the channel to close.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderName, HeaderValue};
use axum::response::sse::{self, Sse};
use axum::response::{IntoResponse, Response};
use fbt::{EventMask, EventStream, Session};
use futures_util::{Stream, StreamExt};
use serde::Deserialize;
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::{Instant, Interval, MissedTickBehavior, Sleep, sleep};
use tokio_util::sync::CancellationToken;
use utoipa::IntoParams;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::dto::{ErrorBody, EventDto};
use crate::error::{ApiErrorResponse, ApiQuery};
use crate::routes::AppState;
use crate::watchdog::Watchdog;

/// How often an idle stream sends a keep-alive comment.
const KEEP_ALIVE: Duration = Duration::from_secs(15);

/// How often a stream checks whether the session has shut down.
const SHUTDOWN_CHECK: Duration = Duration::from_secs(1);

/// The event routes.
pub(crate) fn router() -> OpenApiRouter<AppState> {
    OpenApiRouter::new().routes(routes!(events))
}

/// The query of `GET /api/v1/events`.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct EventsQuery {
    /// Comma-separated categories to receive: `status`, `piece`,
    /// `tracker`, `peer`, `dht`, `storage`, `nat` (default all).
    /// `lagged` comes whatever the categories.
    #[param(example = "status,tracker")]
    categories: Option<String>,
}

/// The mask for a `categories` value; `400 invalid_request` for an empty
/// or unknown name.
fn parse_categories(value: &str) -> Result<EventMask, ApiErrorResponse> {
    value.split(',').try_fold(EventMask::empty(), |mask, name| {
        let bit = match name {
            "status" => EventMask::STATUS,
            "piece" => EventMask::PIECE,
            "tracker" => EventMask::TRACKER,
            "peer" => EventMask::PEER,
            "dht" => EventMask::DHT,
            "storage" => EventMask::STORAGE,
            "nat" => EventMask::NAT,
            _ => {
                return Err(ApiErrorResponse::invalid_request(
                    "categories are status, piece, tracker, peer, dht, storage and nat, \
                     comma-separated",
                ));
            }
        };
        Ok(mask | bit)
    })
}

/// The session's events as `text/event-stream`. Browsers' `EventSource`
/// cannot send headers, so this route also takes `?token=`.
#[utoipa::path(
    get,
    path = "/api/v1/events",
    tag = "events",
    params(EventsQuery),
    responses(
        (status = 200, description = "Server-sent events: `event:` is the type, `data:` the JSON \
event. A comment every 15 s keeps the stream alive.", content_type = "text/event-stream", body = EventDto),
        (status = 400, description = "Unknown category (`invalid_request`).", body = ErrorBody),
        (status = 401, description = "No valid token (`unauthorized`).", body = ErrorBody),
        (status = 503, description = "Too many open event streams (`too_many_streams`).", body = ErrorBody),
    ),
)]
async fn events(
    State(state): State<AppState>,
    ApiQuery(query): ApiQuery<EventsQuery>,
) -> Result<Response, ApiErrorResponse> {
    let mask = match query.categories.as_deref() {
        Some(value) => parse_categories(value)?,
        None => EventMask::ALL,
    };
    let permit = state
        .events
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiErrorResponse::too_many_streams())?;
    let stream = sse_stream(
        state.session.subscribe(mask),
        state.session.clone(),
        Some(permit),
        state.shutdown.clone(),
        state.stream_idle_timeout,
    );
    let mut res = Sse::new(stream).into_response();
    res.headers_mut().insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    Ok(res)
}

/// `events` as SSE messages. It ends when `events` does, within
/// [`SHUTDOWN_CHECK`] of the session's shutdown (delivering everything the
/// subscriber had already queued first), when `shutdown` fires, and when
/// hyper stops polling it for `timeout` — see [`Watchdog`]. `permit` is
/// released when the stream is dropped.
///
/// [`Watchdog`]: crate::watchdog::Watchdog
pub(crate) fn sse_stream(
    events: EventStream,
    session: Session,
    permit: Option<OwnedSemaphorePermit>,
    shutdown: CancellationToken,
    timeout: Duration,
) -> impl Stream<Item = Result<sse::Event, Infallible>> + Send + 'static {
    let mut check = tokio::time::interval(SHUTDOWN_CHECK);
    check.set_missed_tick_behavior(MissedTickBehavior::Delay);
    Watchdog::new(
        KeepAlive::new(SessionEvents {
            events,
            session,
            check,
            _permit: permit,
        }),
        timeout,
    )
    .take_until(shutdown.cancelled_owned())
}

/// Emits a keep-alive comment when `inner` has been quiet for
/// [`KEEP_ALIVE`].
///
/// Axum's own `Sse::keep_alive` cannot be used here: its stream polls the
/// inner stream on every tick, and hyper repolls a body whenever its write
/// buffer has room, so those ticks would look like activity to the
/// [`Watchdog`] and no SSE stream could ever idle out. This wrapper runs
/// *below* the watchdog and emits the comment as an item of the stream:
/// hyper sends it as output (which is what keeps proxies from timing the
/// stream out), and the watchdog sees it as activity — an idle but live
/// SSE client is kept alive by its comments, and a client that has stopped
/// reading is caught when even the comments can no longer be written. The
/// comment bytes are axum's own [`KeepAlive`] default.
///
/// [`Watchdog`]: crate::watchdog::Watchdog
/// [`KeepAlive`]: axum::response::sse::KeepAlive
struct KeepAlive<S> {
    inner: S,
    /// When the next comment is due. Armed at construction, like axum's.
    sleep: Pin<Box<Sleep>>,
}

impl<S> KeepAlive<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            sleep: Box::pin(sleep(KEEP_ALIVE)),
        }
    }
}

impl<S, E> Stream for KeepAlive<S>
where
    S: Stream<Item = Result<sse::Event, E>> + Unpin,
{
    type Item = Result<sse::Event, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(item)) => {
                // Any output restarts the quiet period, including a comment.
                this.sleep.as_mut().reset(Instant::now() + KEEP_ALIVE);
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => {
                if this.sleep.as_mut().poll(cx).is_ready() {
                    this.sleep.as_mut().reset(Instant::now() + KEEP_ALIVE);
                    return Poll::Ready(Some(Ok(sse::Event::DEFAULT_KEEP_ALIVE)));
                }
                Poll::Pending
            }
        }
    }
}

/// [`EventStream`] mapped to SSE messages, ending with the session.
struct SessionEvents {
    events: EventStream,
    session: Session,
    /// Wakes the stream to check [`Session::is_shut_down`].
    check: Interval,
    /// The cap's permit, held while the stream lives.
    _permit: Option<OwnedSemaphorePermit>,
}

impl Stream for SessionEvents {
    type Item = Result<sse::Event, Infallible>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Registers the waker for the next check; drains ticks that are due.
        while this.check.poll_tick(cx).is_ready() {}
        match this.events.poll_next_unpin(cx) {
            Poll::Ready(Some(event)) => Poll::Ready(Some(Ok(to_sse(&EventDto::from(&event))))),
            Poll::Ready(None) => Poll::Ready(None),
            // The session's event sender lives as long as any `Session`
            // clone — this stream's own included — so the channel never
            // closes by itself. Once the session is shut down and this
            // subscriber's queue is drained there is nothing left to
            // deliver: end, keeping everything queued before the shutdown.
            Poll::Pending if this.session.is_shut_down() => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// One SSE message: `event:` the DTO's type, `data:` its JSON.
fn to_sse(dto: &EventDto) -> sse::Event {
    let data = serde_json::to_string(dto).unwrap_or_else(|e| {
        tracing::debug!(error = %e, "event serialisation failed");
        String::from("{}")
    });
    sse::Event::default().event(dto.kind()).data(data)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::ops::ControlFlow;

    use axum::body::Body;
    use fbt::{AddTorrent, CreateTorrent, Metainfo, Settings};
    use http_body_util::BodyExt;

    use super::*;

    /// The integration tests' `test_settings()`: loopback only, no DHT,
    /// LSD, UPnP or NAT-PMP, no queue limits.
    fn test_settings() -> Settings {
        let mut s = Settings::default();
        s.listen_addrs = vec![SocketAddr::from(([127, 0, 0, 1], 0))];
        s.status_interval = Duration::from_millis(50);
        s.dht_enabled = false;
        s.dht_bootstrap_nodes = Vec::new();
        s.enable_lsd = false;
        s.enable_upnp = false;
        s.enable_natpmp = false;
        s.queue.active_downloads = None;
        s.queue.active_seeds = None;
        s.queue.active_limit = None;
        s
    }

    #[test]
    fn categories() {
        assert_eq!(parse_categories("status").unwrap(), EventMask::STATUS);
        assert_eq!(
            parse_categories("tracker,nat").unwrap(),
            EventMask::TRACKER | EventMask::NAT
        );
        assert_eq!(
            parse_categories("status,piece,tracker,peer,dht,storage,nat").unwrap(),
            EventMask::ALL
        );
        for bad in ["", "status,", "Status", "bogus", "status,,peer"] {
            assert!(parse_categories(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn event_type_matches_serde_tag() {
        let dto = EventDto::from(&fbt::Event::Lagged { missed: 3 });
        let json = serde_json::to_value(&dto).unwrap();
        assert_eq!(json["type"], dto.kind());
        assert_eq!(json["missed"], 3);
    }

    #[tokio::test]
    async fn slow_subscriber_gets_lagged() {
        let mut settings = test_settings();
        settings.event_queue = 8;
        let session = Session::new(settings).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a.bin");
        std::fs::write(&file, vec![7u8; 40_000]).unwrap();
        let bytes = CreateTorrent::new(&file)
            .piece_len(16384)
            .build(&mut |_, _| ControlFlow::Continue(()))
            .unwrap();
        let metainfo = Metainfo::from_bytes(&bytes).unwrap();

        let handle = session
            .add_torrent(AddTorrent::new(metainfo, dir.path().to_path_buf()))
            .await
            .unwrap();
        // Seeding first: from there each pause and each resume is one
        // state change.
        tokio::time::timeout(Duration::from_secs(10), async {
            while !matches!(handle.status().state, fbt::TorrentState::Seeding) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();

        let stream = sse_stream(
            session.subscribe(EventMask::ALL),
            session.clone(),
            None,
            CancellationToken::new(),
            Duration::from_secs(3600),
        );
        for _ in 0..20 {
            tokio::time::timeout(Duration::from_secs(5), handle.pause())
                .await
                .unwrap()
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), handle.resume())
                .await
                .unwrap()
                .unwrap();
        }

        let mut body = Sse::new(stream).into_response().into_body();
        let frame = tokio::time::timeout(Duration::from_secs(5), next_data(&mut body))
            .await
            .unwrap();
        let text = String::from_utf8(frame).unwrap();
        assert!(text.starts_with("event: lagged\n"), "{text}");
        let data = text.lines().find_map(|l| l.strip_prefix("data: ")).unwrap();
        let json: serde_json::Value = serde_json::from_str(data).unwrap();
        assert_eq!(json["type"], "lagged");
        assert!(json["missed"].as_u64().unwrap() > 0, "{json}");
        session.shutdown().await;
    }

    /// The next data frame of `body`.
    async fn next_data(body: &mut Body) -> Vec<u8> {
        loop {
            let frame = body.frame().await.unwrap().unwrap();
            if let Ok(data) = frame.into_data() {
                return data.to_vec();
            }
        }
    }

    /// Every event type `body` carries, until it ends.
    async fn drain_types(body: &mut Body) -> Vec<String> {
        let mut out = Vec::new();
        while let Some(frame) = body.frame().await {
            let data = frame.unwrap().into_data().unwrap();
            for line in String::from_utf8_lossy(&data).lines() {
                if let Some(payload) = line.strip_prefix("data: ")
                    && let Ok(json) = serde_json::from_str::<serde_json::Value>(payload)
                {
                    out.push(json["type"].as_str().unwrap_or_default().to_owned());
                }
            }
        }
        out
    }

    /// Every event queued before the shutdown is delivered before the stream
    /// ends: `SessionEvents` drains what its subscriber still holds when the
    /// session goes instead of stopping on the flag. Nothing polls the stream
    /// between the pauses and the shutdown, so all six state changes are
    /// provably still queued at that point — manual polling, no hyper in
    /// front consuming them early.
    #[tokio::test]
    async fn sse_drains_before_ending() {
        let session = Session::new(test_settings()).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a.bin");
        std::fs::write(&file, vec![7u8; 40_000]).unwrap();
        let bytes = CreateTorrent::new(&file)
            .piece_len(16384)
            .build(&mut |_, _| ControlFlow::Continue(()))
            .unwrap();
        let handle = session
            .add_torrent(AddTorrent::new(
                Metainfo::from_bytes(&bytes).unwrap(),
                dir.path().to_path_buf(),
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !matches!(handle.status().state, fbt::TorrentState::Seeding) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();

        let stream = sse_stream(
            session.subscribe(EventMask::ALL),
            session.clone(),
            None,
            CancellationToken::new(),
            Duration::from_secs(3600),
        );
        // Each pause and each resume is one state change, sent before its
        // command replies, so all six are in the queue before `shutdown`.
        for _ in 0..3 {
            tokio::time::timeout(Duration::from_secs(5), handle.pause())
                .await
                .unwrap()
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), handle.resume())
                .await
                .unwrap()
                .unwrap();
        }
        session.shutdown().await;

        let mut body = Sse::new(stream).into_response().into_body();
        let types = tokio::time::timeout(Duration::from_secs(10), drain_types(&mut body))
            .await
            .expect("the stream ends after draining");
        let changes = types.iter().filter(|t| *t == "state_changed").count();
        assert!(
            changes >= 6,
            "all six queued state changes arrive before the end: {types:?}"
        );
    }
}
