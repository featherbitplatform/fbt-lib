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
//!
//! The task probes the *stored* network settings
//! ([`SessionInner::network`]), and applies a result only while they still
//! equal the ones it probed ([`SessionInner::set_if_current`]): a settings
//! change that lands while a probe is in flight wins, and the stale result is
//! dropped. A change that fails in `NetStack::build` never stores its
//! settings, so the task keeps probing the *previous* proxy and may restore
//! that stack (see [`Session::set_network_settings`](crate::Session::set_network_settings)).

use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant};

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
    /// [`Event::ProxyStateChanged`], but only when the state's *variant*
    /// differs from the state now: the comparison, the store and the emit
    /// happen under one lock, so a value set twice emits nothing and two
    /// setters racing on the same state emit once.
    ///
    /// Two `Unreachable`s that differ only in their reason update the stored
    /// state (so [`state`](Self::state) reports the latest reason) without
    /// emitting: reaching the same state again is not a state change.
    pub(crate) fn set(&self, state: ProxyState) {
        let mut now = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if *now == state {
            return;
        }
        let emits = std::mem::discriminant(&*now) != std::mem::discriminant(&state);
        *now = state.clone();
        self.changed.send_replace(state.clone());
        if !emits {
            return;
        }
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
///
/// The retry bounds are read once, here, so the task never needs the session
/// alive to learn them: it holds only a [`Weak`] between wakes, so a session
/// dropped without [`shutdown`](crate::Session::shutdown) is released (its
/// listeners and ports) at once, as [`run_queue`](crate::session) does.
pub(crate) fn spawn(inner: Weak<SessionInner>, health: Arc<ProxyHealth>) -> JoinHandle<()> {
    let retry = inner.upgrade().map(|i| {
        let s = &i.shared.settings;
        (s.proxy_retry_base, s.proxy_retry_max)
    });
    tokio::spawn(async move {
        let Some((base, max)) = retry else {
            return;
        };
        run(inner, health, base, max).await
    })
}

/// The backoff schedule the health task follows. `attempt` only ever resets
/// on a *successful* dial through the proxy
/// ([`dial_succeeded`](ProxyHealth::dial_succeeded)) or after the proxy has
/// stayed healthy (non-retried) for at least `max`: a proxy that comes up
/// and drops again at once is not given the base delay every cycle, but the
/// doubling one, so a flapping proxy cannot drive a rebuild per
/// [`proxy_retry_base`](crate::Settings::proxy_retry_base).
async fn run(inner: Weak<SessionInner>, health: Arc<ProxyHealth>, base: Duration, max: Duration) {
    // Subscribe before reading, so a change between the two is not missed.
    let mut changed = health.subscribe();
    // Whether the backoff is running; `attempt` counts the waits so far.
    let mut retrying = false;
    let mut attempt: u32 = 0;
    // When the proxy last became healthy, so a long healthy stretch (at
    // least `max`) may reset the backoff. `None` while retrying, so a
    // too-short healthy window is remembered and the next retry keeps
    // `attempt`.
    let mut healthy_since: Option<Instant> = None;
    loop {
        let state = changed.borrow_and_update().clone();
        if !state.retried() {
            // Nothing to retry until the state changes (or the session ends).
            retrying = false;
            // Keep the first instant of this healthy stretch: a brief one
            // must not reset the backoff (see `attempt` above).
            let _ = healthy_since.get_or_insert_with(Instant::now);
            if changed.changed().await.is_err() {
                return;
            }
            continue;
        }
        if !retrying {
            // Entering a retried state: the backoff starts over only if the
            // proxy had been healthy long enough (or was never retried).
            retrying = true;
            if healthy_since.is_none_or(|t| t.elapsed() >= max) {
                attempt = 0;
            }
        }
        healthy_since = None;
        let delay = retry_delay(attempt, base, max);
        // Hold only the `Weak` while waiting: the session may be dropped
        // meanwhile and must be released at once (a probe is never run for a
        // session that is gone).
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
        let Some(inner) = inner.upgrade() else {
            return;
        };
        probe_once(&inner).await;
        // Drop the strong handle before the next wait (already at the end of
        // this iteration): the task never keeps a session alive.
        drop(inner);
    }
}

/// Re-probes the proxy now (`probe` is already taken by the fbt-net dial
/// check): re-classifies the session's stored network
/// settings and moves the state. A SOCKS5 relay that has come back and whose
/// relay the live stack has lost rebuilds the stack with its UDP association
/// (close_tcp is false: the proxy did not change, so connected TCP peers
/// stay); a relay the live stack still has is left alone, so a spurious dial
/// failure cannot tear down a healthy stack.
///
/// Nothing is set when the settings changed since the probe was taken: the
/// result is applied only while the stored settings still equal the ones
/// probed (`set_if_current`), so a probe of an *old* proxy cannot overwrite
/// the state of the one the user has just selected.
async fn probe_once(inner: &Arc<SessionInner>) {
    let net = inner.network();
    let mut settings = (*inner.shared.settings).clone();
    settings.set_network(net.clone());
    let (state, relay) = classify(&settings).await;
    match relay {
        // A reachable proxy that carries TCP only (or a failure): just the
        // state, and only while the settings are still the probed ones.
        None => inner.set_if_current(&net, state).await,
        // The proxy relays UDP and the live stack still has its relay: leave
        // the stack alone (a rebuild would churn uTP, the DHT and the
        // announces) and only make sure the state is `Reachable`.
        Some(_) if inner.relay_is_live() => inner.set_if_current(&net, ProxyState::Reachable).await,
        Some(relay) => match inner.rebuild_for_relay(net.clone(), relay).await {
            Ok(true) => inner.set_if_current(&net, ProxyState::Reachable).await,
            // A settings change landed since the probe: it owns the stack and
            // the state.
            Ok(false) => {}
            Err(SessionError::Shutdown) => {}
            Err(e) => {
                warn!(error = %e, "cannot rebuild the network stack for the relayed proxy");
                let state = ProxyState::unreachable(&e.to_string());
                inner.set_if_current(&net, state).await;
            }
        },
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
