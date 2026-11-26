//! The session's proxy health: the state the F2 classification leaves, the
//! task that re-probes an unreachable or login-refused proxy with a backoff
//! and rebuilds the stack when a relayed proxy comes back, and the way a peer
//! dial reports what it saw.
//!
//! [`ProxyHealth`] owns the state ([`ProxyState`]), a `watch` a reader
//! subscribes to before reading (as the network stack's generation is read),
//! and a wake counter plus a [`Notify`] that wakes the health task to probe
//! now. Setting the state compares it to the current one under one lock, so a
//! caller that sets the same value twice emits only one
//! `Event::ProxyStateChanged`; the `watch` is notified for every recorded
//! state, though, so a settings change that lands in the same state still
//! reaches the task (N1).
//!
//! A relay loss is pinned to the stack that lost it: a `Reachable` for that
//! stack is refused (the loss is reported once, so a dead relay reported
//! reachable would never be retried, P1) and the relay-loss cause outlives a
//! later failed probe of it (N2).
//!
//! The task probes the *stored* network settings
//! ([`SessionInner::network`]), and applies a result only while they still
//! equal the ones it probed ([`SessionInner::set_if_current`]): a settings
//! change that lands while a probe is in flight wins, and the stale result is
//! dropped. A change that fails in `NetStack::build` never stores its
//! settings, so the task keeps probing the *previous* proxy and may restore
//! that stack (see [`Session::set_network_settings`](crate::Session::set_network_settings)).
//! The change itself records that failure as a retried state
//! ([`build_failed_state`]), so the task probes the previous settings again
//! and rebuilds their stack instead of the session sitting on the closed one
//! with no network and no retry (I1).
//!
//! Two things keep the backoff honest. `attempt` is reset only by a long
//! healthy stretch or by a change of the proxy probed, never by a successful
//! dial: a proxy that relays TCP fine while dropping its UDP association
//! would otherwise be rebuilt once per peer dial. And a dial success only
//! wakes the task when the retried state came from a *dial or probe*
//! failure; when the relay itself was lost, a TCP success says nothing about
//! it, so the scheduled probe is left to run (see [`RetryCause`]).

use std::sync::atomic::{AtomicU64, Ordering};
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
use crate::settings::{NetworkSettings, Settings};

/// The reason a relayed stack's state is `Unreachable`: the proxy closed the
/// UDP association. Shared by the relay watcher and [`ProxyHealth::relay_lost`].
const RELAY_LOST_REASON: &str = "the proxy closed the UDP association";

/// Whether `proxy` relays UDP: a SOCKS5 proxy that carries peers or trackers
/// does; every other proxy (HTTP, or a SOCKS5 one carrying neither) does not.
/// It decides whether the session opens a relayed UDP socket at all: a proxy
/// that does not relay UDP leaves the session with no UDP socket (no DHT, no
/// uTP, no UDP trackers).
///
/// [`classify`] reads this on [`NetworkSettings::effective`]'s proxy, not the
/// stored one: a `proxy_only` value whose stored `proxy_peers`/`proxy_trackers`
/// are off has both turned on there (and documents the DHT and uTP as
/// relayed), so the association must be taken on the same proxy the stack is
/// built from, or the session would hold no UDP at all while reporting
/// `Reachable`.
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
///
/// The relaying decision is made on the proxy the session will actually run,
/// [`NetworkSettings::effective`]'s, so a `proxy_only` value whose stored
/// switches are off still takes the association the stack it is built with
/// expects (I2); without `proxy_only` the two are equal.
pub(crate) async fn classify(settings: &Settings) -> (ProxyState, Option<Socks5Udp>) {
    let effective = settings.network().effective();
    let proxy = &effective.proxy;
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

/// The state a change that could not build its stack leaves: `Unreachable`
/// with a display-safe reason naming the local failure, whatever the proxy
/// kind (a proxy that is down or rules UDP out is a state, not a build
/// error, so a build failure is always local: a listen address that cannot
/// be bound, or a relayed socket that cannot be opened).
///
/// It is a *retried* state on purpose: the health task probes the stored
/// settings again with its backoff and rebuilds the stack from them, so a
/// session whose change failed does not sit on the closed stack reporting
/// the proxy healthy with no network and no retry (I1).
pub(crate) fn build_failed_state(e: &SessionError) -> ProxyState {
    ProxyState::unreachable(&format!("the network stack could not be built: {e}"))
}

/// The wait before the `attempt`th re-probe (`attempt` counting from 0,
/// `base` doubling each time) capped at `max`. The shift saturates rather
/// than overflow, so no `attempt` panics.
pub(crate) fn retry_delay(attempt: u32, base: Duration, max: Duration) -> Duration {
    let factor = 1u32.checked_shl(attempt).unwrap_or(u32::MAX);
    base.saturating_mul(factor).min(max)
}

/// Why the session is in a retried proxy state. A successful TCP dial is
/// evidence the proxy is up only when a dial or a probe said it was down;
/// when the relay was lost, the TCP side is known good already, so a dial
/// success must not wake the task (F1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryCause {
    /// A probe or a peer dial found the proxy unreachable or refusing login.
    Dial,
    /// The proxy closed the UDP association of a live relay.
    RelayLost,
}

/// The state and why it is being retried, under one lock so a reader sees a
/// consistent pair.
#[derive(Debug)]
struct Inner {
    state: ProxyState,
    cause: RetryCause,
    /// The stack generation whose relay was reported lost, if any. It pins
    /// two things to that stack: a `Reachable` recorded for it is refused
    /// (a relay loss is reported once, so a dead relay reported reachable
    /// would never be retried: the DHT, uTP and UDP trackers would stay off
    /// for good, P1), and the relay-loss cause is kept while the state stays
    /// retried (a failed probe of the same stack says no more about the
    /// relay, so a TCP dial success must not re-arm the wake, N2). A newer
    /// stack (a rebuild's generation) is not pinned.
    relay_lost: Option<u64>,
}

/// The session's proxy health: the state now, a `watch` of state changes,
/// and the channel that wakes the health task to probe now.
#[derive(Debug)]
pub(crate) struct ProxyHealth {
    /// The state now and why it is retried. The `watch` value and the event
    /// are updated under this same lock, so two racing setters cannot both
    /// emit the same state.
    inner: Mutex<Inner>,
    /// Carries every change to the health task and to any other subscriber.
    changed: watch::Sender<ProxyState>,
    /// Wakes the health task for a probe now (a peer dial got through while
    /// the state was being retried).
    probe_now: Notify,
    /// Bumped on every [`wake`](Self::wake). The task remembers it when a
    /// wait starts and treats a wake as real only if the counter moved after
    /// that: a `Notify` permit stored while no one was waiting fires the wake
    /// branch at once, but must not end a later, unrelated wait (F2).
    wakes: AtomicU64,
    /// The generation of the current network stack. Read without blocking to
    /// drop a report (a failed dial, a lost relay) from a stack that is no
    /// longer the current one (I1-sub).
    net_gen: watch::Receiver<u64>,
    /// The session's event stream, cloned so a change emits
    /// [`Event::ProxyStateChanged`] without reaching into `Shared`.
    events: broadcast::Sender<Event>,
}

impl ProxyHealth {
    /// A health for a session that starts in `state`, emitting into `events`
    /// and reading the current stack generation from `net_gen`.
    pub(crate) fn new(
        state: ProxyState,
        events: broadcast::Sender<Event>,
        net_gen: watch::Receiver<u64>,
    ) -> Arc<ProxyHealth> {
        let (changed, _) = watch::channel(state.clone());
        Arc::new(ProxyHealth {
            inner: Mutex::new(Inner {
                state,
                cause: RetryCause::Dial,
                relay_lost: None,
            }),
            changed,
            probe_now: Notify::new(),
            wakes: AtomicU64::new(0),
            net_gen,
            events,
        })
    }

    /// The state now.
    pub(crate) fn state(&self) -> ProxyState {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .state
            .clone()
    }

    /// Records `state` (a probe's or a dial's result), emitting
    /// [`Event::ProxyStateChanged`] only when the state's *variant* differs
    /// from the state now: the comparison, the store and the emit happen
    /// under one lock, so a value set twice emits nothing and two setters
    /// racing on the same state emit once.
    ///
    /// The `watch` is notified on *every* recorded state, equal or not, so a
    /// settings change that lands in the same variant with an identical
    /// reason still reaches the health task instead of leaving it on the
    /// previous proxy's deadline (N1). Only the event stays variant-change
    /// only.
    ///
    /// Two `Unreachable`s that differ only in their reason update the stored
    /// state (so [`state`](Self::state) reports the latest reason) without
    /// emitting: reaching the same state again is not a state change. A
    /// `Reachable` for the stack whose relay was reported lost is refused
    /// (P1).
    pub(crate) fn set(&self, state: ProxyState) {
        self.record(state, RetryCause::Dial, None);
    }

    /// The relay of the stack built as `generation` was lost: moves the state
    /// to `Unreachable` with the relay cause, so a later successful TCP dial
    /// does not wake the task (a TCP dial says nothing about the relay, F1)
    /// and a `Reachable` for that same stack is refused (P1). The cause
    /// outlives a later failed probe of the same stack (N2). A report from a
    /// stack that is no longer the current one is dropped (I1-sub).
    pub(crate) fn relay_lost(&self, generation: u64) {
        if generation != self.generation() {
            return;
        }
        self.record(
            ProxyState::unreachable(RELAY_LOST_REASON),
            RetryCause::RelayLost,
            Some(generation),
        );
    }

    /// The common store: records `state`, why it is retried (`cause`) and,
    /// for a relay-loss report, the `lost` stack generation to pin, under one
    /// lock.
    fn record(&self, state: ProxyState, cause: RetryCause, lost: Option<u64>) {
        let current = self.generation();
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        // The stack's relay is gone and the loss is reported once: reporting
        // the proxy reachable for that same stack would leave the session
        // with the DHT, uTP and UDP trackers off and no retry ever (P1).
        if state == ProxyState::Reachable && inner.relay_lost == Some(current) {
            return;
        }
        // While the state stays retried for the same stack, the relay loss is
        // still why: a failed probe (or a proxy-unreachable dial report) about
        // the same stack says no more about the relay than the loss did, so a
        // TCP dial success must still not wake a probe (N2).
        inner.cause = if state.retried() && inner.relay_lost == Some(current) {
            RetryCause::RelayLost
        } else {
            cause
        };
        // The pin follows the stack: a relay-loss report pins the generation
        // it came from, and a state that leaves the retried variants
        // (reachable, or no proxy) releases it.
        inner.relay_lost = match lost {
            Some(generation) => Some(generation),
            None if state.retried() => inner.relay_lost,
            None => None,
        };
        let emits = std::mem::discriminant(&inner.state) != std::mem::discriminant(&state);
        inner.state = state.clone();
        // Notified even when the value is equal (as a settings change landing
        // in the same state is): the health task must never wait out the
        // previous proxy's deadline for a change it cannot see in the state
        // (N1).
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

    /// The generation of the current network stack.
    fn generation(&self) -> u64 {
        *self.net_gen.borrow()
    }

    /// Wakes the health task to probe now.
    pub(crate) fn wake(&self) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        self.probe_now.notify_one();
    }

    /// The wake counter now: a task records it when a wait begins and treats
    /// a wake as real only if it moved after that.
    fn wakes(&self) -> u64 {
        self.wakes.load(Ordering::SeqCst)
    }

    /// Awaits a "probe now" wake.
    async fn woken(&self) {
        self.probe_now.notified().await;
    }

    /// A peer dial through the proxy failed. Only a failure that says
    /// something about the proxy moves the state: an unreachable proxy, or a
    /// refused login. A status about the *target* (`Refused`, or an HTTP
    /// `502`/`503`/`504`) and any other error leave the state alone. A dial
    /// stamped with an older stack generation says nothing about the current
    /// proxy and is dropped (I1-sub).
    pub(crate) fn dial_failed(&self, generation: u64, e: &ProxyError) {
        if generation != self.generation() {
            return;
        }
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
    /// the DHT can come back without waiting out the backoff. A lost relay is
    /// *not* woken: the TCP dial succeeded while the UDP relay is broken, so
    /// it says nothing about the relay (F1). A dial stamped with an older
    /// stack generation is dropped (I1-sub).
    pub(crate) fn dial_succeeded(&self, generation: u64) {
        if generation != self.generation() {
            return;
        }
        let woken = {
            let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
            inner.state.retried() && inner.cause != RetryCause::RelayLost
        };
        if woken {
            self.wake();
        }
    }
}

/// What ended a wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    /// A real wake: the counter moved since the wait began.
    Woke,
    /// The timer fired.
    Slept,
    /// The state's `watch` changed: re-evaluate it.
    Changed,
    /// The state's `watch` ended (the session is going away).
    Ended,
}

/// Waits for a real wake, `deadline`, or a state change. A `Notify` permit
/// stored while no one was waiting fires the wake branch at once, but is
/// ignored: the counter has not moved since `snapshot`, so the wait goes on
/// to the same deadline (F2).
async fn wait_step(
    health: &ProxyHealth,
    changed: &mut watch::Receiver<ProxyState>,
    deadline: tokio::time::Instant,
    snapshot: u64,
) -> Step {
    loop {
        tokio::select! {
            _ = health.woken() => {
                if health.wakes() > snapshot {
                    return Step::Woke;
                }
            }
            // `sleep_until` with an absolute deadline: re-polling it after a
            // stale permit does not restart the wait.
            _ = tokio::time::sleep_until(deadline) => return Step::Slept,
            r = changed.changed() => {
                return if r.is_ok() { Step::Changed } else { Step::Ended };
            }
        }
    }
}

/// Whether a state update restarts the running retry wait: a change of the
/// state's *variant*, or of the proxy probed (the settings), does; a new
/// reason under the same variant does not, so alternating dial errors cannot
/// postpone an already-scheduled probe (F3, F4). `scheduled` is the variant
/// the running wait was started for, `None` when no wait is running.
fn restarts_wait(
    scheduled: Option<std::mem::Discriminant<ProxyState>>,
    variant: std::mem::Discriminant<ProxyState>,
    settings_changed: bool,
) -> bool {
    settings_changed || scheduled != Some(variant)
}

/// Spawns the session's proxy health task: it re-probes an unreachable or
/// login-refused proxy with a backoff from `Settings::proxy_retry_base` to
/// `Settings::proxy_retry_max` and, when a proxy that relays UDP comes back
/// (or a closed stack is left behind by a failed change), rebuilds the
/// network stack from the stored settings. Ends with the session (its handle
/// is aborted with the session's other tasks).
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

/// The backoff schedule the health task follows. `attempt` counts the waits
/// so far and is reset only when the proxy has stayed non-retried for at
/// least `max`, or when the settings being probed change: a successful dial
/// through the proxy never resets it (a proxy that relays TCP fine while
/// dropping its UDP association would otherwise be rebuilt once per dial).
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
    // The network settings the current schedule belongs to: a change (a new
    // proxy) starts a fresh schedule, so a switch to another down proxy is
    // probed after `base`, not after an inflated delay (F4). Kept across a
    // healthy stretch, so coming back to the same proxy keeps the schedule.
    let mut probed: Option<NetworkSettings> = None;
    // The state variant the running wait was started for: a reason-only
    // update must not restart it (F3). `None` while no wait is running.
    let mut scheduled: Option<std::mem::Discriminant<ProxyState>> = None;
    // The wake counter when the running wait began, and its deadline.
    let mut snapshot: u64 = health.wakes();
    let mut deadline: Option<tokio::time::Instant> = None;

    loop {
        let state = changed.borrow_and_update().clone();
        if !state.retried() {
            // Nothing to retry until the state changes (or the session ends).
            retrying = false;
            scheduled = None;
            deadline = None;
            // Keep the first instant of this healthy stretch: a brief one
            // must not reset the backoff (see `attempt` above).
            let _ = healthy_since.get_or_insert_with(Instant::now);
            if changed.changed().await.is_err() {
                return;
            }
            continue;
        }
        // The proxy about to be probed, read now (a short upgrade: the
        // session is never kept alive across the wait).
        let Some(alive) = inner.upgrade() else {
            return;
        };
        let settings = alive.network();
        drop(alive);
        let variant = std::mem::discriminant(&state);
        let settings_changed = probed.as_ref() != Some(&settings);
        if settings_changed {
            // A different proxy: its schedule starts over.
            probed = Some(settings);
            attempt = 0;
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
        if restarts_wait(scheduled, variant, settings_changed) {
            deadline = None;
        }
        scheduled = Some(variant);
        if deadline.is_none() {
            snapshot = health.wakes();
            deadline = Some(tokio::time::Instant::now() + retry_delay(attempt, base, max));
        }
        let step = wait_step(
            &health,
            &mut changed,
            deadline.expect("set just above"),
            snapshot,
        )
        .await;
        match step {
            // The state changed under us: re-evaluate it before probing,
            // keeping the running wait unless its variant or settings moved.
            Step::Changed => continue,
            Step::Ended => return,
            Step::Slept => {
                deadline = None;
                attempt = attempt.saturating_add(1);
            }
            // A dial got through: probe now. `attempt` is *not* reset: a
            // success says nothing about the backoff (F1).
            Step::Woke => deadline = None,
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
/// A closed stack is rebuilt whatever the proxy kind: a change that failed to
/// build its stack left the closed one installed and a retried state
/// ([`build_failed_state`]), and only applying the stored settings again gets
/// a network back (I1). The relay the classification took, when it took one,
/// goes with the rebuild; a proxy that needs none is applied as it is.
///
/// Nothing is set when the settings changed since the probe was taken: the
/// result is applied only while the stored settings still equal the ones
/// probed (`set_if_current`), so a probe of an *old* proxy cannot overwrite
/// the state of the one the user has just selected. A rebuild records
/// `Reachable` in [`apply_change`](SessionInner::apply_change) itself, before
/// it wires the new stack's relay watcher, so nothing here can report a lost
/// relay reachable (P1).
async fn probe_once(inner: &Arc<SessionInner>) {
    let net = inner.network();
    let mut settings = (*inner.shared.settings).clone();
    settings.set_network(net.clone());
    let (state, relay) = classify(&settings).await;
    // The stack installed now is the closed placeholder: a change that could
    // not build its own left it behind, and no state but a rebuilt stack
    // brings a network back (I1).
    let closed = inner.shared.net().closed;
    match relay {
        // A reachable proxy that carries TCP only (or a failure), on a live
        // stack: just the state, and only while the settings are still the
        // probed ones.
        None if !closed => inner.set_if_current(&net, state).await,
        // The proxy relays UDP and the live stack still has its relay: leave
        // the stack alone (a rebuild would churn uTP, the DHT and the
        // announces) and only make sure the state is `Reachable`.
        Some(_) if inner.relay_is_live() => inner.set_if_current(&net, ProxyState::Reachable).await,
        relay => match inner.rebuild_for_health(net.clone(), relay).await {
            // The rebuild records `Reachable` (or the classification's state,
            // for a proxy that relays nothing) itself, *before* it wires the
            // new stack's relay watcher: a relay that is lost at once moves
            // the state back to `Unreachable` and is retried, and nothing here
            // can write `Reachable` over it (P1). `Ok(false)` is a settings
            // change that landed since the probe: it owns the stack and the
            // state.
            Ok(_) => {}
            Err(SessionError::Shutdown) => {}
            Err(e) => {
                warn!(error = %e, "cannot rebuild the network stack for the probed settings");
                // The same state `apply_change` recorded: the stack could not
                // be built again, so the session keeps retrying.
                let state = build_failed_state(&e);
                inner.set_if_current(&net, state).await;
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::time::Duration;
    use tokio::sync::broadcast;

    fn events() -> broadcast::Sender<Event> {
        broadcast::channel(16).0
    }

    fn health(state: ProxyState, generation: u64) -> Arc<ProxyHealth> {
        health_with_gen(state, generation).0
    }

    /// A health that keeps the generation channel, so a test can install a
    /// newer stack (what a rebuild does) and see the pin move with it.
    fn health_with_gen(
        state: ProxyState,
        generation: u64,
    ) -> (Arc<ProxyHealth>, watch::Sender<u64>) {
        let (net_gen, _) = watch::channel(generation);
        (
            ProxyHealth::new(state, events(), net_gen.subscribe()),
            net_gen,
        )
    }

    /// A health that keeps the event channel, so a test can assert what it
    /// emits (and what it does not).
    fn health_with_events(
        state: ProxyState,
        generation: u64,
    ) -> (Arc<ProxyHealth>, broadcast::Sender<Event>) {
        let (net_gen, _) = watch::channel(generation);
        let events = events();
        (
            ProxyHealth::new(state, events.clone(), net_gen.subscribe()),
            events,
        )
    }

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

    /// A report stamped with an older stack generation says nothing about the
    /// current proxy and must not move its state: a dial through the proxy
    /// the user has just switched away from must not mark the new one
    /// unreachable (I1-sub).
    #[test]
    fn a_report_from_an_old_stack_is_dropped() {
        let down = |text: &str| ProxyError::Unreachable(io::Error::other(text.to_owned()));
        let h = health(ProxyState::Reachable, 7);
        // Generation 5 is not the current one (7): dropped.
        h.dial_failed(5, &down("refused"));
        h.relay_lost(5);
        assert_eq!(
            h.state(),
            ProxyState::Reachable,
            "an old stack moved the state"
        );
        // The current generation (7) is taken.
        h.dial_failed(7, &down("refused"));
        assert!(
            matches!(h.state(), ProxyState::Unreachable { .. }),
            "the current stack's failure was dropped"
        );
    }

    /// A successful TCP dial says nothing about a UDP relay that the proxy
    /// dropped, so it must not wake the task (F1); a state that a *dial or
    /// probe* failure put into retry may be woken.
    #[test]
    fn a_dial_success_does_not_wake_a_lost_relay() {
        let h = health(ProxyState::Reachable, 1);
        h.relay_lost(1);
        let before = h.wakes();
        h.dial_succeeded(1);
        assert_eq!(h.wakes(), before, "a TCP dial success woke a lost relay");

        let h = health(ProxyState::Reachable, 1);
        h.dial_failed(1, &ProxyError::Unreachable(io::Error::other("refused")));
        let before = h.wakes();
        h.dial_succeeded(1);
        assert!(
            h.wakes() > before,
            "a dial success did not wake a failed dial"
        );
    }

    /// A relay loss is reported once, so a `Reachable` that lands afterwards
    /// for the same stack would leave the session reporting a dead relay as
    /// reachable, with the DHT, uTP and UDP trackers off and no retry ever
    /// (P1). A newer stack (the rebuild a retry makes) is a new relay and may
    /// be reachable.
    #[test]
    fn a_lost_relay_is_not_reported_reachable() {
        let (h, net_gen) = health_with_gen(ProxyState::Reachable, 5);
        h.relay_lost(5);
        assert!(
            matches!(h.state(), ProxyState::Unreachable { .. }),
            "the relay loss did not move the state"
        );
        // The late `Reachable` the tail of a rebuild used to write over it.
        h.set(ProxyState::Reachable);
        assert!(
            matches!(h.state(), ProxyState::Unreachable { .. }),
            "a lost relay was reported reachable"
        );
        // The retry rebuilds the stack: its relay is not the lost one.
        net_gen.send_replace(7);
        h.set(ProxyState::Reachable);
        assert_eq!(
            h.state(),
            ProxyState::Reachable,
            "a newer stack's relay stayed pinned"
        );
    }

    /// The relay-loss cause outlives a later failed probe (or a
    /// proxy-unreachable dial report): while the state stays retried for the
    /// same stack, a TCP dial success still says nothing about the relay and
    /// must not wake a probe (N2). A stack that replaces it (a rebuild) drops
    /// the cause with the pin.
    #[test]
    fn a_failed_probe_does_not_re_arm_the_relay_loss_wake() {
        let (h, net_gen) = health_with_gen(ProxyState::Reachable, 1);
        h.relay_lost(1);
        // A failed probe of the same proxy records another reason under the
        // same variant.
        h.set(ProxyState::unreachable("proxy timed out"));
        assert!(
            matches!(h.state(), ProxyState::Unreachable { .. }),
            "a failed probe left the retried state"
        );
        let before = h.wakes();
        h.dial_succeeded(1);
        assert_eq!(
            h.wakes(),
            before,
            "a failed probe re-armed the dial-success wake"
        );

        // The retry rebuilt the stack: a fresh dial failure is a dial one.
        net_gen.send_replace(3);
        h.set(ProxyState::Reachable);
        h.dial_failed(3, &ProxyError::Unreachable(io::Error::other("refused")));
        let before = h.wakes();
        h.dial_succeeded(3);
        assert!(
            h.wakes() > before,
            "a new retry was not woken after the relay cause was dropped"
        );
    }

    /// The state store notifies the `wait` on every recorded state, even one
    /// equal to the stored one: a settings change that lands in the same
    /// variant with the same reason (two proxies failing identically) must
    /// not leave the task waiting out the old proxy's deadline (N1). The
    /// event stays variant-change only: no duplicate `ProxyStateChanged`.
    #[test]
    fn a_settings_change_in_the_same_state_still_notifies() {
        let state = ProxyState::unreachable("proxy timed out");
        let (h, events) = health_with_events(state.clone(), 1);
        let wait = h.subscribe();
        let mut emitted = events.subscribe();
        assert!(!wait.has_changed().unwrap(), "nothing was recorded yet");
        h.set(state);
        assert!(
            wait.has_changed().unwrap(),
            "an unchanged state did not notify the wait"
        );
        assert!(
            matches!(
                emitted.try_recv(),
                Err(broadcast::error::TryRecvError::Empty)
            ),
            "an unchanged state emitted a duplicate event"
        );
    }

    /// A `Notify` permit stored while no one was waiting must not end a later
    /// wait early; only a wake after the wait began does (F2).
    #[tokio::test]
    async fn a_stale_wake_does_not_end_the_wait() {
        let h = health(ProxyState::Reachable, 1);
        // Nobody is waiting: this stores a permit.
        h.wake();
        let mut changed = h.subscribe();
        let snapshot = h.wakes();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(150);
        let step = wait_step(&h, &mut changed, deadline, snapshot).await;
        assert_eq!(step, Step::Slept, "a stale permit ended the wait early");
    }

    /// A wake that arrives after the wait began is real and ends it at once.
    #[tokio::test]
    async fn a_real_wake_ends_the_wait() {
        let h = health(ProxyState::Reachable, 1);
        let mut changed = h.subscribe();
        let snapshot = h.wakes();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let waker = {
            let h = Arc::clone(&h);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(30)).await;
                h.wake();
            })
        };
        let step = wait_step(&h, &mut changed, deadline, snapshot).await;
        assert_eq!(step, Step::Woke);
        waker.await.unwrap();
    }

    /// Only a change of the state's *variant* (or of the proxy probed)
    /// restarts the wait: a new reason under the same variant must not, so
    /// alternating dial errors cannot postpone the probe (F3).
    #[test]
    fn a_reason_only_update_does_not_restart_the_wait() {
        let unreachable = ProxyState::unreachable("refused");
        let running = Some(std::mem::discriminant(&unreachable));
        let other_reason = ProxyState::unreachable("timed out");
        assert!(
            !restarts_wait(running, std::mem::discriminant(&other_reason), false),
            "a new reason restarted the wait"
        );
        assert!(
            restarts_wait(
                running,
                std::mem::discriminant(&ProxyState::Reachable),
                false
            ),
            "a new variant did not restart the wait"
        );
        assert!(
            restarts_wait(running, std::mem::discriminant(&other_reason), true),
            "a new proxy did not restart the wait"
        );
        assert!(
            restarts_wait(None, std::mem::discriminant(&other_reason), false),
            "a wait that was not running was kept"
        );
    }
}
