//! [`Watchdog`]: ends a long-lived response body that has stopped producing
//! items.
//!
//! A file stream or event stream holds its cap permit until its body is
//! dropped, and normally only the client hanging up drops it. A client that
//! stops reading instead fills its socket: hyper takes no more of the body,
//! no item comes, and nothing else would ever end the stream or free the
//! permit. The watchdog ends it after a stretch of
//! [`ApiConfig::stream_idle_timeout`](crate::ApiConfig::stream_idle_timeout)
//! with no item — bytes handed to hyper, or an SSE keep-alive comment — so
//! those caps bound the slots even stalled clients can hold.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{Stream, StreamExt};
use tokio::time::{Instant, Sleep, sleep};

/// A body stream that ends when `inner` stops producing items for `timeout`.
///
/// The timer is re-armed whenever a poll yields an item — bytes for hyper to
/// write, or an SSE keep-alive comment emitted below this wrapper (see
/// [`crate::routes::events`]) — so a client that reads at all, however idle
/// the session is, keeps its stream open. Polls that yield nothing do not
/// re-arm it: the shutdown-check tick and the connection's own wake-ups poll
/// bodies constantly and prove nothing about the client. When the timeout
/// passes without an item the next poll yields `None`: the body ends early
/// and the client sees a short read. The sleep is polled from this stream's
/// own `poll_next` and stays armed between polls, so its wake-up makes a
/// parked connection poll again even when nothing else would — that is how a
/// stalled client or a reader whose pieces never arrive gets ended.
pub(crate) struct Watchdog<S> {
    inner: S,
    timeout: Duration,
    /// Boxed and pinned so `Watchdog` stays `Unpin` inside the body
    /// combinators (`take_until`, axum's SSE and hyper's bodies).
    sleep: Pin<Box<Sleep>>,
    ended: bool,
}

impl<S> Watchdog<S> {
    /// Wraps `inner`: it ends once `timeout` passes without an item from it.
    pub(crate) fn new(inner: S, timeout: Duration) -> Self {
        Watchdog {
            inner,
            timeout,
            sleep: Box::pin(sleep(timeout)),
            ended: false,
        }
    }
}

impl<S: Stream + Unpin> Stream for Watchdog<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        // The poll below registers this task's waker with the timer and the
        // `reset` after a yielded item moves that registration to the new
        // deadline, so between polls the armed sleep — not the inner stream,
        // which may have nothing to say — is what wakes us.
        if this.sleep.as_mut().poll(cx).is_ready() {
            this.ended = true;
            return Poll::Ready(None);
        }
        match this.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(item)) => {
                // An item is life on the other end: re-arm for the time
                // until the next one.
                this.sleep.as_mut().reset(Instant::now() + this.timeout);
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                this.ended = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;

    /// An item every poll yields, so a polled stream stays alive by items.
    fn ticks() -> impl Stream<Item = u8> + Unpin {
        stream::unfold(0u8, |n| std::future::ready(Some((n, n + 1))))
    }

    #[tokio::test]
    async fn an_unproducing_stream_ends_it() {
        let mut s = Watchdog::new(ticks(), Duration::from_millis(200));
        assert_eq!(s.next().await, Some(0));
        // 300 ms without a single item: the timeout passes...
        tokio::time::sleep(Duration::from_millis(300)).await;
        // ...and the next poll ends the stream, though `inner` still had
        // items ready — producing nothing is what the watchdog measures.
        assert_eq!(s.next().await, None);
    }

    #[tokio::test]
    async fn items_keep_it_alive() {
        let mut s = Watchdog::new(ticks(), Duration::from_secs(1));
        for n in 0..10u8 {
            // An item every 100 ms keeps a 1 s timeout from ever firing.
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(s.next().await, Some(n), "{n}");
        }
    }

    #[tokio::test]
    async fn pending_polls_do_not_extend_life() {
        let mut s = Watchdog::new(stream::pending::<u8>(), Duration::from_millis(300));
        for _ in 0..4 {
            // Each round polls (yielding nothing) and parks again: wakes and
            // polls that produce no item — a shutdown-check tick nudging
            // hyper, say — must not re-arm the timer.
            let early = tokio::time::timeout(Duration::from_millis(50), s.next()).await;
            assert!(early.is_err(), "a pending poll parks, as it should");
        }
        // Four rounds of polling did not push the 300 ms deadline: waiting
        // past it ends the stream.
        let ended = tokio::time::timeout(Duration::from_millis(400), s.next())
            .await
            .expect("the watchdog ends the stream on time");
        assert_eq!(ended, None);
    }

    #[tokio::test]
    async fn a_pending_inner_is_ended_by_the_timer() {
        // Nothing wakes `inner`; only the watchdog's own wake-up can end it,
        // which is the stalled-client case hyper parks the connection in.
        let mut s = Watchdog::new(stream::pending::<u8>(), Duration::from_millis(200));
        let started = Instant::now();
        let last = tokio::time::timeout(Duration::from_secs(5), s.next())
            .await
            .expect("the watchdog ends the stream");
        assert_eq!(last, None);
        assert!(started.elapsed() >= Duration::from_millis(190));
    }

    #[tokio::test]
    async fn a_naturally_ended_inner_passes_through() {
        let mut s = Watchdog::new(stream::iter([1u8, 2]), Duration::from_secs(60));
        assert_eq!(s.next().await, Some(1));
        assert_eq!(s.next().await, Some(2));
        assert_eq!(s.next().await, None);
    }
}
