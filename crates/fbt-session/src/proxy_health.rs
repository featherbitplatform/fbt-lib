//! The session's proxy health: the state the F2 classification leaves, the
//! task that re-probes an unreachable or login-refused proxy with a backoff
//! and rebuilds the stack when a relayed proxy comes back, and the way a peer
//! dial reports what it saw.
//!
//! [`ProxyHealth`] owns the state ([`ProxyState`]), a `watch` a reader
//! subscribes to before reading (as the network stack's generation is read),
//! and a [`Notify`] that wakes the health task to probe now. Setting the
//! state compares it to the current one under one lock, so a caller that sets
//! the same value twice emits only one `Event::ProxyStateChanged`.

use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use fbt_net::proxy::{
    ProxyConfig, ProxyError, ProxyKind, Socks5Udp, probe, udp_associate, udp_not_relayed,
};
use tokio::sync::{Notify, broadcast, watch};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::error::SessionError;
use crate::event::{Event, ProxyState};
use crate::session::SessionInner;
use crate::settings::Settings;

/// Whether `proxy` relays UDP: a SOCKS5 proxy that carries peers or trackers
/// does; every other proxy (HTTP, or a SOCKS5 one carrying neither) does not.
/// It decides whether the session opens a relayed UDP socket at all: a proxy
/// that does not relay UDP leaves the session with no UDP socket (no DHT, no
/// uTP, no UDP trackers).
pub(crate) fn relays_udp(proxy: &ProxyConfig) -> bool {
    proxy.kind == ProxyKind::Socks5 && (proxy.proxy_peers || proxy.proxy_trackers)
}

/// The state a proxy error leaves: a refused login is its own state;
/// anything else (a dial failure, a timeout, a broken protocol) is an
/// unreachable proxy carrying the error's display-safe text.
fn state_of(e: &ProxyError) -> ProxyState {
    match e {
        ProxyError::AuthFailed | ProxyError::HttpStatus(407) => ProxyState::LoginRefused,
        e => ProxyState::unreachable(&e.to_string()),
    }
}

/// Classifies `settings`'s proxy as the F2 ruling says, taking the UDP
/// association when one is needed: `NotSet` without a proxy; for a SOCKS5
/// proxy that relays UDP a UDP ASSOCIATE (its socket on success,
/// `UdpNotRelayed` when the proxy refuses one, otherwise the error's state),
/// and a probe (SOCKS5 greeting and login, HTTP TCP dial only) for anything
/// else.
///
/// Used by [`Session::new`](crate::Session::new) and every rebuild, so the
/// session never fails to start or to change because the proxy is down.
pub(crate) async fn classify(settings: &Settings) -> (ProxyState, Option<Socks5Udp>) {
    let proxy = &settings.proxy;
    if proxy.kind == ProxyKind::None {
        return (ProxyState::NotSet, None);
    }
    if relays_udp(proxy) {
        match udp_associate(proxy, settings.connect_timeout).await {
            Ok(assoc) => (ProxyState::Reachable, Some(assoc)),
            Err(e) if udp_not_relayed(&e) => (ProxyState::UdpNotRelayed, None),
            Err(e) => (state_of(&e), None),
        }
    } else {
        match probe(proxy, settings.connect_timeout).await {
            Ok(()) => (ProxyState::Reachable, None),
            Err(e) => (state_of(&e), None),
        }
    }
}

/// The wait before the `attempt`th re-probe (`attempt` counting from 0,
/// `base` doubling each time) capped at `max`. The shift saturates rather
/// than overflow, so no `attempt` panics.
pub(crate) fn retry_delay(attempt: u32, base: Duration, max: Duration) -> Duration {
    let factor = 1u32.checked_shl(attempt).unwrap_or(u32::MAX);
    base.saturating_mul(factor).min(max)
}

/// The session's proxy health: the state now, a `watch` of state changes,
/// and the channel that wakes the health task to probe now.
#[derive(Debug)]
pub(crate) struct ProxyHealth {
    /// The state now. The `watch` value and the event are updated under this
    /// same lock, so two racing setters cannot both emit the same state.
    state: Mutex<ProxyState>,
    /// Carries every change to the health task and to any other subscriber.
    changed: watch::Sender<ProxyState>,
    /// Wakes the health task for a probe now (a peer dial got through while
    /// the state was being retried).
    probe_now: Notify,
    /// The session's event stream, cloned so a change emits
    /// [`Event::ProxyStateChanged`] without reaching into `Shared`.
    events: broadcast::Sender<Event>,
}

impl ProxyHealth {
    /// A health for a session that starts in `state`, emitting into `events`.
    pub(crate) fn new(state: ProxyState, events: broadcast::Sender<Event>) -> Arc<ProxyHealth> {
        let (changed, _) = watch::channel(state.clone());
        Arc::new(ProxyHealth {
            state: Mutex::new(state),
            changed,
            probe_now: Notify::new(),
            events,
        })
    }

    /// The state now.
    pub(crate) fn state(&self) -> ProxyState {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Records `state`, waking subscribers and emitting
    /// [`Event::ProxyStateChanged`], but only when it differs from the state
    /// now: the comparison, the store and the emit happen under one lock, so
    /// a value set twice emits nothing and two setters racing on the same
    /// value emit once.
    pub(crate) fn set(&self, state: ProxyState) {
        let mut now = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if *now == state {
            return;
        }
        *now = state.clone();
        self.changed.send_replace(state.clone());
        // A broadcast send never awaits, so holding the lock here is safe,
        // and it keeps the event and the `watch` in step.
        let _ = self.events.send(Event::ProxyStateChanged { state });
    }

    /// A receiver of state changes; read the value after subscribing, so a
    /// change between the two is not missed.
    pub(crate) fn subscribe(&self) -> watch::Receiver<ProxyState> {
        self.changed.subscribe()
    }

    /// Wakes the health task to probe now.
    pub(crate) fn wake(&self) {
        self.probe_now.notify_one();
    }

    /// Awaits a "probe now" wake.
    async fn woken(&self) {
        self.probe_now.notified().await;
    }

    /// A peer dial through the proxy failed. Only a failure that says
    /// something about the proxy moves the state: an unreachable proxy, or a
    /// refused login. A status about the *target* (`Refused`, or an HTTP
    /// `502`/`503`/`504`) and any other error leave the state alone.
    pub(crate) fn dial_failed(&self, e: &ProxyError) {
        match e {
            ProxyError::Unreachable(_) => self.set(ProxyState::unreachable(&e.to_string())),
            ProxyError::AuthFailed | ProxyError::HttpStatus(407) => {
                self.set(ProxyState::LoginRefused)
            }
            _ => {}
        }
    }

    /// A peer dial through the proxy got through: a proxy that is only
    /// assumed down (a state being retried) is probed now, so the state and
    /// the DHT can come back without waiting out the backoff.
    pub(crate) fn dial_succeeded(&self) {
        if self.state().retried() {
            self.wake();
        }
    }
}

/// Spawns the session's proxy health task: it re-probes an unreachable or
/// login-refused proxy with a backoff from
/// [`Settings::proxy_retry_base`] to [`Settings::proxy_retry_max`] and, when
/// a proxy that relays UDP comes back, rebuilds the network stack with its
/// UDP association. Ends with the session (its handle is aborted with the
/// session's other tasks).
pub(crate) fn spawn(inner: Weak<SessionInner>, health: Arc<ProxyHealth>) -> JoinHandle<()> {
    tokio::spawn(run(inner, health))
}

async fn run(inner: Weak<SessionInner>, health: Arc<ProxyHealth>) {
    // Subscribe before reading, so a change between the two is not missed.
    let mut changed = health.subscribe();
    // Whether the backoff is running; `attempt` counts the waits so far.
    let mut retrying = false;
    let mut attempt: u32 = 0;
    loop {
        let state = changed.borrow_and_update().clone();
        if !state.retried() {
            // Nothing to retry until the state changes (or the session ends).
            retrying = false;
            if changed.changed().await.is_err() {
                return;
            }
            continue;
        }
        let Some(inner) = inner.upgrade() else {
            return;
        };
        if !retrying {
            // Entering a retried state from a non-retried one starts the
            // backoff over.
            retrying = true;
            attempt = 0;
        }
        let base = inner.shared.settings.proxy_retry_base;
        let max = inner.shared.settings.proxy_retry_max;
        let delay = retry_delay(attempt, base, max);
        let now = tokio::select! {
            _ = health.woken() => true,
            _ = tokio::time::sleep(delay) => false,
            r = changed.changed() => {
                if r.is_err() {
                    return;
                }
                // The state changed under us: re-evaluate it before probing.
                continue;
            }
        };
        if now {
            // A dial got through: probe at once, and reset the backoff.
            attempt = 0;
        } else {
            attempt = attempt.saturating_add(1);
        }
        probe_once(&inner, &health).await;
    }
}

/// Re-probes the proxy now (`probe` is already taken by the fbt-net dial
/// check): re-classifies the session's stored network
/// settings and moves the state. A SOCKS5 relay that has come back rebuilds
/// the stack with its UDP association (close_tcp is false: the proxy did not
/// change, so connected TCP peers stay); anything else only updates the
/// state. Nothing is set when the rebuild is dropped because a settings
/// change landed meanwhile: that change owns the state.
async fn probe_once(inner: &Arc<SessionInner>, health: &ProxyHealth) {
    let net = inner.network();
    let mut settings = (*inner.shared.settings).clone();
    settings.set_network(net.clone());
    let (state, relay) = classify(&settings).await;
    let Some(relay) = relay else {
        health.set(state);
        return;
    };
    match inner.rebuild_for_relay(net, relay).await {
        Ok(true) => health.set(ProxyState::Reachable),
        Ok(false) => {}
        Err(SessionError::Shutdown) => {}
        Err(e) => {
            warn!(error = %e, "cannot rebuild the network stack for the relayed proxy");
            health.set(ProxyState::unreachable(&e.to_string()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn retry_delay_doubles_to_max() {
        let (b, m) = (Duration::from_secs(1), Duration::from_secs(60));
        assert_eq!(retry_delay(0, b, m), b);
        assert_eq!(retry_delay(1, b, m), Duration::from_secs(2));
        assert_eq!(retry_delay(10, b, m), m);
        assert_eq!(retry_delay(u32::MAX, b, m), m);
    }

    #[test]
    fn unreachable_reason_is_display_safe() {
        let ProxyState::Unreachable { reason } = ProxyState::unreachable("\u{1b}[31mevil\u{202e}")
        else {
            panic!()
        };
        assert!(
            !reason.chars().any(|c| c.is_control() || c == '\u{202e}'),
            "{reason:?}"
        );
    }
}
