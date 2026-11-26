# Proxy at runtime, proxy health and no-bypass (0.13.0) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. The implementer step runs through the `parallel-implementers` skill (two lanes; Claude reviews every task).

**Goal:** a session whose proxy and proxy-related network switches change at runtime without a restart and without any direct connection in between, that starts and keeps running when the proxy is down or does not relay UDP, that reports the proxy's health as a state and an event, that never resolves a host name locally under a proxy, and a test that proves no socket other than the proxy's is opened (featherbittorrent's F1 to F5).

**Architecture:** everything a session sends with (UDP sockets, uTP, DHT, LSD, port mapping, tracker client, listeners) moves out of the immutable `Shared` fields into one swappable `NetStack` behind `Shared::net()`, stamped with a generation; `Session::set_network_settings` installs a closed stack first, tears down the old one, closes the connections of older generations, then builds and installs the new one. A proxy health task owns `ProxyState`, re-probes with bounded backoff and rebuilds the stack when the UDP association comes back. Outbound sockets and name lookups are reported to a process-wide audit hook in `fbt-core`, which the no-bypass test reads and a grep gate keeps complete.

**Tech Stack:** Rust 2024 (MSRV 1.99), tokio, the existing `fbt-net` proxy clients and the loopback fakes `crates/fbt-net/tests/support/fake_socks5.rs` and `fake_http_proxy.rs`.

**Spec:** `../featherbittorrent/docs/superpowers/specs/2026-11-25-settings-and-proxy-design.md` (worktree `featherbittorrent/.worktrees/settings-design`), sections 3.2 to 3.6 and 9 (F1 to F5, approved by the owner on 2026-11-25). The library's binding design stays `docs/superpowers/specs/2026-11-10-fbt-lib-design.md`.

## Global Constraints

- Gitflow: work on `feature/proxy-runtime` (off `develop` at the 0.12.0 merge); the two lanes below use `feature/proxy-runtime-a` and `feature/proxy-runtime-b` off it and merge back into it; release `0.13.0` via `release/0.13.0`, annotated tag `v0.13.0` on `main`, merged back to `develop`.
- Conventional Commits with the crate as scope; no attribution of any kind in commits or PRs (no `Co-Authored-By`, no `Claude-Session`, no session links, no "Generated with" footer).
- `#![forbid(unsafe_code)]` stays; no new dependency outside `[workspace.dependencies]`, and none is needed; `bash dev/check-pure-rust.sh` stays green.
- Remote input never panics, hangs or allocates without bound: proxy replies, relayed datagrams and tracker replies are hostile.
- Public enums and config structs of `fbt-session` and `fbt` are `#[non_exhaustive]`; config structs implement `Default`; every public item is documented (`cargo doc` with `-D warnings`).
- Tests never contact the internet, the public DHT, a real router or a real proxy: proxies are the loopback fakes, "remote" addresses are documentation ranges (`192.0.2.0/24`, `198.51.100.0/24`, `203.0.113.0/24`) or `.invalid` names that the fakes record and never dial, and every test `Settings` keeps LSD, UPnP and NAT-PMP off.
- Text that comes from a proxy or the network and lands in `ProxyState` or an event goes through `fbt_core::display_safe(s, fbt_core::MAX_DISPLAY_CHARS)`.
- Docs and changelog text: no emoji, no em-dashes.
- Breaking changes are allowed under 0.x; each one is listed in `CHANGELOG.md` under Changed with who is affected.
- Defaults keep 0.12.0 behaviour except where F2 changes it on purpose (`Session::new` no longer fails on a SOCKS5 UDP association failure).

## Rulings (where the spec leaves a choice or the code forces one)

- **F1 API.** `Session::set_network_settings(NetworkSettings)` plus `Session::network_settings()`; `NetworkSettings` holds the proxy and exactly the switches the spec's 3.3 turns off (DHT, LSD, UPnP, NAT-PMP, incoming TCP, incoming uTP, outgoing uTP). Round 3's toggles reuse it. `Settings` keeps its flat fields; `Settings::network()` and `Settings::set_network()` convert.
- **No fbt-api route.** The app saves through its own product routes so the setting persists (spec section 5); a second, non-persisting writer in fbt-api would recreate the gap section 5 closes. fbt-api only gains the new event's DTO.
- **Engine-side proxy-only switch.** The spec has featherbittorrent turn the bypasses off itself. The engine also gains `ProxyConfig::proxy_only` (default `false`): with it, every bypass is treated as off and `proxy_peers`, `proxy_trackers` and `proxy_hostnames` as on, whatever the switches say. The stored switches are untouched (`network_settings()` returns them as set), which is what the spec's "stored value comes back when the proxy is removed" needs. featherbittorrent sets it; the no-bypass test runs with it.
- **F4, UDP trackers: through the proxy.** SOCKS5 (RFC 1928, section 7) lets a UDP request header carry a domain name, so a UDP tracker named by host goes to the relay with `ATYP = 3` and is never looked up here. Replies are matched by transaction id from whatever source the relay reports.
- **F4, DHT bootstrap: not used by name under a proxy.** SOCKS5 would carry it, but the DHT matches every reply to the address it queried (its defence against forged replies), and a query sent to a name has no address to match. Under any proxy the DHT bootstraps from IP-literal entries, its saved state and the `PORT` messages of peers reached through the proxy; name entries are skipped with a debug log. A fresh install behind a proxy therefore gets its DHT after its first peers, not at once.
- **F2, how a start is classified.** At start (and after each settings change) the session probes the proxy within `connect_timeout`: a SOCKS5 proxy that should relay UDP gets a UDP ASSOCIATE, any other a probe (SOCKS5: greeting and login; HTTP: TCP dial only). Dial failure or dial timeout: `Unreachable`. Login rejected: `LoginRefused`. Greeting and login accepted but UDP ASSOCIATE refused (any `REP`) or answered with a relay named by domain: `UdpNotRelayed`. A proxy that does not answer at all still costs up to `connect_timeout` (10 s) in `Session::new`, as a SOCKS5 one does today.
- **HTTP proxy login.** HTTP has no login check without a `CONNECT`, so an HTTP proxy shows `Reachable` after its TCP probe until a real `CONNECT` is answered `407`, which moves it to `LoginRefused`.
- **Retry.** `Unreachable` and `LoginRefused` re-probe with backoff from `proxy_retry_base` (1 s) doubling to `proxy_retry_max` (60 s); a peer dial that gets through resets it. `UdpNotRelayed` is a property of the proxy and is not retried; it is checked again on every settings change and every return from `Unreachable`.
- **What a change closes.** Every change rebuilds the stack, so uTP connections end with their socket and reconnect. TCP peer connections, web seed and tracker requests are closed only when `proxy` changed.
- **`Event::ProxyError` stays** for 0.13.0, still emitted once when the relay is lost (message no longer says "until the session restarts"), documented as superseded by `Event::ProxyStateChanged`.
- **The proxy's own host name** is the one lookup a proxied session makes; everything else is given to the proxy by name.

## Review Focus

1. A proxy given by host name (`localhost`), not address: the only lookup the session makes is that name; nothing else resolves. Test in Task 6 (`proxy_host_name_is_the_only_lookup`).
2. Two `set_network_settings` calls racing: they apply one after the other and the session ends on the last one, with no direct dial between them. Test in Task 4 (`concurrent_changes_apply_in_order`).
3. A proxy that flaps (down, up, down): one event per real change, retries bounded by `proxy_retry_max`, the relay comes back each time. Tests in Task 5 (`state_events_only_on_change`, `relay_loss_then_recovery`, `retry_delay_doubles_to_max`).
4. `shutdown()` while a change is waiting on a silent proxy: both return within `connect_timeout` plus 2 s and no socket stays open. Test in Task 4 (`shutdown_during_change_completes`).
5. Removing the proxy: the session goes back to direct operation with the switches as stored, and `proxy_state()` is `NotSet`. Test in Task 4 (`removing_proxy_restores_direct`) and Task 5 (`removing_proxy_is_not_set`).

## Lanes and order

```
Lane A (fbt-core, fbt-net, fbt-tracker, fbt-dht, fbt-utp):  Task 1 -> Task 2 -> Task 3
Lane B (fbt-session):                                        Task 4
Join (after both lanes merge into feature/proxy-runtime):    Task 5 -> Task 6 -> Task 7
```

Lane A never edits `crates/fbt-session/`; lane B never edits the other crates (it consumes only what `fbt-net` 0.12.0 already exports). Task 5 needs Tasks 2 and 4, Task 6 needs Tasks 1, 3 and 5, Task 7 needs all.

---

### Task 1: Outbound audit hook (lane A)

**Files:**
- Create: `crates/fbt-core/src/audit.rs`; `dev/check-outbound.sh`
- Modify: `crates/fbt-core/src/lib.rs` (`#[doc(hidden)] pub mod audit`); every outbound call site in `crates/fbt-net/src/{proxy/mod.rs,demux.rs,lsd.rs,natpmp.rs,upnp/mod.rs,upnp/http.rs}`, `crates/fbt-tracker/src/{connector.rs,udp.rs}`, `crates/fbt-dht/src/node.rs`, `crates/fbt-utp/src/socket.rs`; `.github/workflows/ci.yml` (run the script in the lint job); `CLAUDE.md` (Commands block: the script)
- Test: unit tests in `audit.rs`; `crates/fbt-net/tests/proxy.rs`, `crates/fbt-net/tests/demux.rs`; unit tests in `fbt-tracker/src/udp.rs`, `fbt-dht` tests

**Interfaces:**
- Produces (in `fbt_core::audit`, `#[doc(hidden)]`, not part of the stable API):
  - `#[non_exhaustive] #[derive(Debug, Clone, PartialEq, Eq)] pub enum Outbound { TcpConnect(SocketAddr), UdpSend(SocketAddr), Lookup(String) }`: the OS-level destination (a relayed datagram is `UdpSend(relay)`, a proxied dial `TcpConnect(proxy)`), and every host name handed to the system resolver, `TcpStream::connect((name, port))` included.
  - `pub fn record(o: Outbound)`: a no-op (one uncontended read lock) without a capture.
  - `pub fn capture() -> Capture`: installs a recorder; a process-wide `Mutex` makes captures wait for each other, so tests in one binary never mix records. `impl Capture { pub fn take(&self) -> Vec<Outbound>; }`, uninstalled on drop.
- `dev/check-outbound.sh`: scans `crates/*/src` (not `fbt-api`, not `fbt-example`, stopping at a file's `#[cfg(test)]` module) for `TcpStream::connect`, `TcpSocket`'s `connect`, `lookup_host`, `.send_to(` and `send_to_host(`, and fails unless each hit has `audit::record` within the 3 lines above it or a `// audit: <reason>` comment on the line above. Prints `check-outbound: ok` or each offending `file:line`. Lane A covers its crates; Task 6 makes `fbt-session` pass.

- [ ] **Step 1: Write the failing tests**

```rust
// fbt-core audit.rs
#[test] fn capture_sees_records_and_stops_on_drop() {
    let c = capture();
    record(Outbound::Lookup("a.invalid".into()));
    assert_eq!(c.take(), vec![Outbound::Lookup("a.invalid".into())]);
    drop(c);
    record(Outbound::Lookup("b.invalid".into())); // no recorder: nothing kept, no panic
    assert!(capture().take().is_empty());
}
// fbt-net tests/proxy.rs
#[tokio::test] async fn audit_records_direct_and_proxied_dials() {
    // direct: ProxyKind::None to a loopback listener -> [TcpConnect(listener)]
    // SOCKS5 fake (Script::Ok), target ProxyTarget::Host("example.invalid", 80), proxy_hostnames
    //   -> exactly [TcpConnect(fake.addr())], no Lookup
}
// fbt-net tests/demux.rs
#[tokio::test] async fn relayed_send_records_the_relay_only() {
    // bind_relayed on a fake association; sender.send_to(b"x", 192.0.2.1:6881)
    // -> capture == [UdpSend(fake relay addr)]
}
// fbt-tracker udp.rs
#[tokio::test] async fn direct_udp_tracker_lookup_is_recorded() {
    // announce to udp://localhost:<fake tracker port> -> capture contains Lookup("localhost")
}
// fbt-dht
#[tokio::test] async fn bootstrap_lookup_is_recorded() {
    // DhtConfig.bootstrap = ["localhost:<loopback node port>"] -> capture contains Lookup("localhost")
}
```

- [ ] **Step 2: Run** `cargo test -p fbt-core audit && cargo test -p fbt-net audit relayed_send && cargo test -p fbt-tracker direct_udp_tracker_lookup && cargo test -p fbt-dht bootstrap_lookup`. Expected: FAIL (module and records missing).
- [ ] **Step 3: Implement** `audit.rs` (`RwLock<Option<Arc<dyn Fn(&Outbound) + Send + Sync>>>` plus the capture `Mutex`), then add `audit::record` before each call site listed under Files; a host name passed to `TcpStream::connect` records `Lookup(name)` then, once connected, nothing more (the address is the resolver's).
- [ ] **Step 4: Run** the tests of Step 2 and `bash dev/check-outbound.sh` with `fbt-session` excluded for now (`CHECK_OUTBOUND_SKIP=fbt-session`). Expected: PASS and `check-outbound: ok`.
- [ ] **Step 5: Commit** `test(core): outbound audit hook and check-outbound gate`.

### Task 2: Proxy client additions (lane A)

**Files:**
- Modify: `crates/fbt-net/src/proxy/mod.rs`, `crates/fbt-net/src/proxy/socks5.rs`, `crates/fbt-net/src/demux.rs`, `crates/fbt-core/src/udp.rs`, `crates/fbt-net/tests/support/fake_socks5.rs`, `crates/fbt-net/tests/support/fake_http_proxy.rs`, `fuzz/` (`socks5` target also feeds `encode_udp_host` round trips)
- Test: `crates/fbt-net/tests/proxy.rs`, `crates/fbt-net/tests/demux.rs`

**Interfaces:**
- Produces:
  - `ProxyConfig::proxy_only: bool` (default `false`, in `Debug`, `serde(default)`); documented as in Rulings; `validate` unchanged.
  - `ProxyError::Unreachable(io::Error)`: the TCP dial to the proxy failed, or the timeout ran out before it connected (`io::ErrorKind::TimedOut`); `Timeout` now means only "after the proxy accepted". Display: `"cannot reach the proxy: {0}"`.
  - `pub async fn probe(cfg: &ProxyConfig, timeout: Duration) -> Result<(), ProxyError>`: SOCKS5 dial, greeting and login, then close; HTTP dial only; `ProxyKind::None` is `Ok(())` without a socket.
  - `pub fn udp_not_relayed(e: &ProxyError) -> bool`: true for `Refused(_)` and `Unsupported` as returned by `udp_associate` (the greeting and login had succeeded).
  - `pub fn encode_udp_host(host: &str, port: u16, payload: &[u8], out: &mut Vec<u8>) -> Result<(), ProxyError>`: `ATYP = 3`; host 1 to 255 bytes of `[A-Za-z0-9._-]`, else `ProxyError::Protocol`.
  - `fbt_core::udp::DatagramSender::send_to_host(&self, data: &[u8], host: &str, port: u16) -> io::Result<usize>`: provided method returning `io::ErrorKind::Unsupported`; `SharedUdpSocket`'s relayed sender implements it with `encode_udp_host` (recording `UdpSend(relay)`), its direct sender keeps the default (never resolves).
  - Fakes: `fake_socks5::Script::NoUdp(u8)` (CONNECT relayed, UDP ASSOCIATE answered with that `REP`), `Script::RecordOnly` (CONNECT logged and answered `REP 5` without dialling; UDP datagrams logged, never forwarded), `FakeSocks5::start_on(addr: SocketAddr, script: Script)`, `FakeSocks5::relay_addr() -> Option<SocketAddr>`; `udp_targets()` logs domain targets as `name:port`. `fake_http_proxy::Script::RecordOnly` (CONNECT logged, answered `502`), `FakeHttpProxy::start_on(addr, script)`. A free port for `start_on` comes from binding `127.0.0.1:0` and dropping the listener (`support::closed_port()` in each fake).

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test] async fn dial_to_closed_port_is_unreachable() {
    let cfg = socks5(closed_port());
    assert!(matches!(connect_tcp(&cfg, addr_target(), T).await, Err(ProxyError::Unreachable(_))));
    assert!(matches!(probe(&cfg, T).await, Err(ProxyError::Unreachable(_))));
}
#[tokio::test] async fn probe_reports_login_refused() { // RequireAuth("u","p"), creds "u","wrong"
    assert!(matches!(probe(&cfg, T).await, Err(ProxyError::AuthFailed)));
}
#[tokio::test] async fn http_probe_dials_only() { // FakeHttpProxy Script::Ok
    probe(&cfg, T).await.unwrap();
    assert!(fake.connects().is_empty());
}
#[tokio::test] async fn udp_associate_refused_is_not_relayed() {
    let e = udp_associate(&socks5_fake(Script::NoUdp(7)), T).await.unwrap_err();
    assert!(udp_not_relayed(&e));
    let e = udp_associate(&socks5(closed_port()), T).await.unwrap_err();
    assert!(!udp_not_relayed(&e));
}
#[test] fn encode_udp_host_frames_domain() {
    let mut out = Vec::new();
    encode_udp_host("tracker.invalid", 6969, b"x", &mut out).unwrap();
    assert_eq!(out, [&[0, 0, 0, 3, 15][..], b"tracker.invalid", &[0x1b, 0x39], b"x"].concat());
    for bad in ["", "a b", &"a".repeat(256)] {
        assert!(matches!(encode_udp_host(bad, 1, b"", &mut out), Err(ProxyError::Protocol(_))));
    }
}
#[tokio::test] async fn relayed_socket_sends_to_host() { // fake RecordOnly
    relayed.sender().send_to_host(b"hi", "tracker.invalid", 6969).unwrap();
    // eventually: fake.udp_targets() == ["tracker.invalid:6969"]
    let e = direct.sender().send_to_host(b"hi", "tracker.invalid", 6969).unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::Unsupported);
}
#[cfg(feature = "serde")] #[test] fn proxy_only_defaults_off_and_round_trips() {
    assert!(!ProxyConfig::default().proxy_only);
    // serde_json round trip with proxy_only = true keeps it; a document without the key reads false
}
```

- [ ] **Step 2: Run** `cargo test -p fbt-net --all-features proxy demux`. Expected: FAIL.
- [ ] **Step 3: Implement** the items above; `connect_inner` and `udp_associate` time the dial phase so its errors and its share of the timeout become `Unreachable`.
- [ ] **Step 4: Run** `cargo test -p fbt-net --all-features && cargo test -p fbt-core && bash dev/check-outbound.sh` (with `CHECK_OUTBOUND_SKIP=fbt-session`). Expected: PASS.
- [ ] **Step 5: Commit** `feat(net): proxy probe, unreachable error, domain UDP relay, proxy_only`.

### Task 3: Names through the proxy (F4, lane A)

**Files:**
- Modify: `crates/fbt-tracker/src/udp.rs`, `crates/fbt-tracker/src/lib.rs` (docs of `new_with`), `crates/fbt-dht/src/node.rs`
- Test: unit tests in `crates/fbt-tracker/src/udp.rs`; `crates/fbt-dht/tests/` (the crate's bootstrap tests)

**Interfaces:**
- Consumes: Task 2 (`DatagramSender::send_to_host`), Task 1 (`audit::capture`).
- Produces:
  - UDP trackers on `Route::Relayed`: a URL host that is not an IP literal is never looked up; the connect and announce/scrape datagrams go through `send_to_host`, the waiter accepts the reply with its transaction id from any source the relay reports, and the connection-id cache is keyed by the tracker as written (`host:port`). IP literals and `Route::Direct` behave as in 0.12.0.
  - `DhtConfig::resolve_bootstrap_names: bool` (default `true`): with `false`, `bootstrap` entries whose host is not an IP literal are skipped with a `debug!` naming the entry; literals are used as before.

- [ ] **Step 1: Write the failing tests**

```rust
// fbt-tracker udp.rs (fake relayed DatagramSender records send_to_host calls; replies are fed to the handler)
#[tokio::test] async fn relayed_announce_by_name_never_resolves() {
    let cap = audit::capture();
    // announce("udp://tracker.invalid:6969/announce"); first datagram -> send_to_host("tracker.invalid", 6969)
    // feed a connect reply, then an announce reply (interval 1800), both from 192.0.2.9:6969
    assert_eq!(resp.interval, 1800);
    assert!(!cap.take().iter().any(|o| matches!(o, Outbound::Lookup(_))));
}
#[tokio::test] async fn relayed_reply_with_wrong_tid_is_ignored() {
    // a reply with another transaction id is dropped; the request times out with TrackerError::Timeout
}
#[tokio::test] async fn relayed_literal_still_uses_send_to() {
    // udp://192.0.2.9:6969 -> send_to(192.0.2.9:6969) on the relayed sender, no send_to_host
}
// fbt-dht
#[tokio::test] async fn bootstrap_skips_names_when_told() {
    // bootstrap = ["router.invalid:6881", "127.0.0.1:<loopback node>"], resolve_bootstrap_names = false
    // -> stats().bootstrapped or nodes_v4 >= 1 within 5 s, and capture has no Lookup
}
```

- [ ] **Step 2: Run** `cargo test -p fbt-tracker relayed && cargo test -p fbt-dht bootstrap_skips_names`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `cargo test -p fbt-tracker -p fbt-dht && cargo clippy -p fbt-tracker -p fbt-dht --all-targets --all-features -- -D warnings`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(tracker): UDP tracker names resolved by the SOCKS5 relay` and `feat(dht): option to skip bootstrap names` (two commits).

### Task 4: Runtime network settings (F1, lane B)

**Files:**
- Create: `crates/fbt-session/src/netstack.rs`; `crates/fbt-session/tests/network.rs`
- Modify: `crates/fbt-session/src/settings.rs` (`NetworkSettings`, `Settings::network`, `Settings::set_network`), `crates/fbt-session/src/session.rs` (`Shared` loses `tracker`, `dht`, `dht_addrs`, `utp`, `lsd`, `portmap`, listen ports, gains `net`; `Session::new`, `shutdown`, the setter and getter), `crates/fbt-session/src/net.rs`, `crates/fbt-session/src/listener.rs`, `crates/fbt-session/src/peer/conn/mod.rs`, `crates/fbt-session/src/peer/messages.rs` (`DisconnectReason::NetworkChanged`), `crates/fbt-session/src/torrent/{mod.rs,commands.rs,actor.rs,announcer.rs,peers.rs,swarm.rs,webseed.rs,pex.rs,layers.rs}` (read the stack through `shared.net()`), `crates/fbt-session/src/lib.rs`, `crates/fbt/src/lib.rs` (re-export `NetworkSettings`), `crates/fbt/tests/lib_surface.rs`, `crates/fbt-api/src/dto.rs` (`NetworkChanged` disconnect reason as `network_changed`)
- Test: `crates/fbt-session/tests/network.rs`

**Interfaces:**
- Consumes: `fbt-net` 0.12.0 as is.
- Produces:
  - `#[non_exhaustive] #[derive(Debug, Clone, PartialEq)] #[cfg_attr(feature = "serde", derive(Serialize, Deserialize), serde(default))] pub struct NetworkSettings { pub proxy: ProxyConfig, pub dht_enabled: bool, pub enable_lsd: bool, pub enable_upnp: bool, pub enable_natpmp: bool, pub enable_incoming_tcp: bool, pub enable_incoming_utp: bool, pub enable_outgoing_utp: bool }`; `Default` equals `Settings::default().network()`.
  - `impl Settings { pub fn network(&self) -> NetworkSettings; pub fn set_network(&mut self, net: NetworkSettings); }`
  - `impl Session { pub async fn set_network_settings(&self, net: NetworkSettings) -> Result<(), SessionError>; pub fn network_settings(&self) -> NetworkSettings; }`: errors `InvalidSettings` (the proxy's `validate`, or every transport off together with `Settings::enable_outgoing_tcp`), `Shutdown`. An equal value is a no-op.
  - `DisconnectReason::NetworkChanged`.
  - Crate-internal, used by Tasks 5 and 6: `pub(crate) struct NetStack` (fields: `generation: u64`, `net: NetworkSettings` as applied, `tracker`, `dht: Option<Dht>`, `dht_addrs`, `utp`, `udp`, `lsd`, `portmap`, `local_addrs`, listen ports, `listen_errors`, task handles); `Shared::net(&self) -> Arc<NetStack>`; `NetStack::build(settings: &Settings, net: &NetworkSettings, addrs: &[SocketAddr], dht_state: Option<Vec<u8>>, generation: u64, relay: Option<Socks5Udp>) -> Result<NetStack, SessionError>` (the caller does the UDP association, so Task 5 can classify it); `NetStack::closed(generation: u64) -> NetStack` (no socket, a tracker client whose every request fails with `TrackerError::Proxy("network settings are changing")`); `async fn NetStack::shutdown(&self) -> Option<Vec<u8>>` (returns the DHT state); `SessionInner::rebuild(&self, net: NetworkSettings, close_tcp: bool) -> Result<(), SessionError>` under a `tokio::sync::Mutex<()>` that serialises changes.
- The change algorithm (the guarantee is in its order):
  1. Validate; return if equal to the current value.
  2. Take the change mutex; `gen = current.generation + 1`; install `NetStack::closed(gen)`. From here every new dial, announce, web seed request and datagram reads the closed stack.
  3. Shut the old stack down (listeners, LSD, port mapping, DHT keeping its state, uTP, UDP), dropping its tracker client and its connection pool.
  4. If `proxy` changed: every torrent closes peers, web seed and tracker requests stamped with a generation below `gen` (handshaking and still-dialling ones included) with `NetworkChanged`, awaited as `set_ip_filter` awaits `filter_changed`.
  5. Build the stack for `gen` on the addresses the old one had bound (so the listen port stays), install it.
  6. Every torrent re-announces to its trackers and the DHT at once.
- Every outgoing dial records the generation of the stack it read; when the transport opens and `shared.net().generation` differs, the stream is dropped and the peer reported with `NetworkChanged`. Incoming connections are stamped at accept.

- [ ] **Step 1: Write the failing tests** (all in `tests/network.rs`, loopback only, `test_settings()`)

```rust
#[tokio::test] async fn set_network_settings_round_trips() {
    // network_settings() == settings.network(); set dht_enabled = true (no bootstrap) -> getter has it, dht_stats().is_some(); set false -> dht_stats().is_none()
}
#[tokio::test] async fn invalid_network_settings_are_refused() {
    // proxy kind Socks5 with empty host -> Err(InvalidSettings(msg)) && msg.contains("proxy"); network_settings() unchanged
}
#[tokio::test] async fn enabling_proxy_closes_direct_peers() {
    // leecher connected directly to a seeder; set proxy = FakeSocks5 Script::Ok
    // -> PeerDisconnected { reason: DisconnectReason::NetworkChanged, .. }; fake.connects() contains the seeder's addr; the torrent finishes
}
#[tokio::test] async fn half_open_direct_connection_closed_on_proxy_change() {
    // add_peer to a loopback listener that accepts and never sends a handshake; set a proxy
    // -> the accepted stream reads EOF within 2 s
}
#[tokio::test] async fn non_proxy_change_keeps_tcp_peers() {
    // TCP-only peer connected (dial_tcp settings); toggle enable_incoming_utp -> no PeerDisconnected within 1 s
}
#[tokio::test] async fn incoming_tcp_toggle_stops_and_restarts_accepting() {
    // enable_incoming_tcp = false -> TcpStream::connect(listen addr) refused; true again -> accepted; listen_port() unchanged
}
#[tokio::test] async fn concurrent_changes_apply_in_order() {
    // join!(set(A), set(B)) -> network_settings() is whichever call ran last (equals A or B, never a mix), and Session keeps working (a download completes)
}
#[tokio::test] async fn shutdown_during_change_completes() {
    // FakeSocks5 that accepts and never answers the greeting (Script::CloseAfterGreeting is not enough: add Script::Silent in the fake);
    // connect_timeout 2 s; spawn set_network_settings(proxy = it); shutdown() -> both done within 4 s; set returns Ok or Err(Shutdown)
}
#[tokio::test] async fn removing_proxy_restores_direct() {
    // proxy set, then NetworkSettings with ProxyKind::None -> a direct download completes; network_settings() has the stored switches
}
#[tokio::test] async fn dht_state_survives_rebuild() {
    // dht_enabled with a loopback node; dht_state() node id before == after a toggle of enable_incoming_utp
}
```

(`Script::Silent`: accepts, reads nothing, never answers; add it to `fake_socks5.rs` in this task. It is the only edit lane B makes outside `fbt-session`, in a test-support file lane A also touches: add it at the end of the `Script` enum to keep the merge trivial.)

- [ ] **Step 2: Run** `cargo test -p fbt-session --test network`. Expected: FAIL.
- [ ] **Step 3: Implement** `NetStack` and the algorithm above; `Session::new` builds generation 1 with `NetStack::build` (association failures still fail `new` in this task; Task 5 changes that). Keep every existing test green: `tests/proxy.rs`, `lsd.rs`, `portmap.rs`, `dht.rs`, `utp.rs` read the same public API.
- [ ] **Step 4: Run** `cargo test -p fbt-session && cargo test -p fbt && cargo test -p fbt-api && cargo clippy --workspace --all-targets --all-features -- -D warnings`. Expected: PASS. Then `bash dev/stress.sh fbt-session network enabling_proxy_closes_direct_peers 50`. Expected: `stress: 50/50 passed`.
- [ ] **Step 5: Commit** `feat(session): runtime network settings with a swappable network stack`.

### Task 5: Start without the proxy, proxy health (F2, F5; after Tasks 2 and 4)

**Files:**
- Create: `crates/fbt-session/src/proxy_health.rs`
- Modify: `crates/fbt-session/src/session.rs` (`Session::new`, `proxy_state`, relay watcher), `crates/fbt-session/src/netstack.rs`, `crates/fbt-session/src/event.rs` (`Event::ProxyStateChanged`, `ProxyState`; `ProxyError` docs), `crates/fbt-session/src/error.rs` (docs: `Proxy` now only for a relay socket that cannot bind), `crates/fbt-session/src/settings.rs` (hidden retry knobs), `crates/fbt-session/src/peer/conn/mod.rs` (report dial outcomes), `crates/fbt-session/src/lib.rs`, `crates/fbt-session/tests/support/mod.rs` (`test_settings`: retry 50 ms to 400 ms), `crates/fbt/src/lib.rs`, `crates/fbt/tests/lib_surface.rs`, `crates/fbt-api/src/dto.rs`, `crates/fbt-api/tests/events.rs`, `crates/fbt-api/tests/lib_surface.rs`
- Test: `crates/fbt-session/tests/proxy.rs`, unit tests in `proxy_health.rs`, `crates/fbt-api/tests/events.rs`

**Interfaces:**
- Consumes: Task 2 (`ProxyError::Unreachable`, `probe`, `udp_not_relayed`, fake `NoUdp`, `start_on`), Task 4 (`NetStack::build`, `SessionInner::rebuild(net, close_tcp: false)`).
- Produces:
  - `#[non_exhaustive] #[derive(Debug, Clone, PartialEq, Eq)] #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))] pub enum ProxyState { NotSet, Reachable, UdpNotRelayed, Unreachable { reason: String }, LoginRefused }`. `reason` is display-safe, built by `pub(crate) fn ProxyState::unreachable(text: &str) -> ProxyState`.
  - `Event::ProxyStateChanged { state: ProxyState }` in `EventMask::STATUS`, emitted only when the state differs from the last one.
  - `Session::proxy_state(&self) -> ProxyState`.
  - `#[doc(hidden)] pub proxy_retry_base: Duration` (1 s) and `#[doc(hidden)] pub proxy_retry_max: Duration` (60 s) on `Settings`, `serde(skip)`, validated non-zero and `base <= max`.
  - `pub(crate) fn retry_delay(attempt: u32, base: Duration, max: Duration) -> Duration`.
  - fbt-api: `EventDto::ProxyStateChanged { state: &'static str, reason: Option<String> }`, kind `proxy_state_changed`, states `not_set`, `reachable`, `udp_not_relayed`, `unreachable`, `login_refused`; in the OpenAPI document.
- Behaviour:
  - `Session::new` and every `rebuild` classify the proxy as in Rulings. Only `Reachable` with a SOCKS5 proxy that relays gets a relayed UDP socket; every other proxied state has no UDP socket at all (no DHT, no uTP, UDP trackers fail with `TrackerError::ProxyUnsupported`), and TCP still only dials through the proxy. `Session::new` no longer returns `SessionError::Proxy` for an association that fails.
  - The health task (one per session, ends with it) re-probes in `Unreachable` and `LoginRefused` at `retry_delay`; a probe that succeeds calls `rebuild(current, close_tcp: false)` when UDP should be relayed, else sets `Reachable`.
  - A lost relay sets `Unreachable { reason: "the proxy closed the UDP association" }`, emits the legacy `Event::ProxyError` once, and starts the retries.
  - Peer dials through the proxy report: `ProxyError::Unreachable` sets `Unreachable` (and wakes the task); `AuthFailed` or `HttpStatus(407)` sets `LoginRefused`; a dial that gets through while the state is `Unreachable` or `LoginRefused` wakes the task to probe now. Other errors (`Refused` by the target, `HttpStatus` 502, 503, 504) say nothing about the proxy and leave the state.
  - `ProxyKind::None` is `NotSet`, set at start and when a change removes the proxy.

- [ ] **Step 1: Write the failing tests**

```rust
// proxy_health.rs
#[test] fn retry_delay_doubles_to_max() {
    let (b, m) = (Duration::from_secs(1), Duration::from_secs(60));
    assert_eq!(retry_delay(0, b, m), b);
    assert_eq!(retry_delay(1, b, m), Duration::from_secs(2));
    assert_eq!(retry_delay(10, b, m), m);
    assert_eq!(retry_delay(u32::MAX, b, m), m);
}
#[test] fn unreachable_reason_is_display_safe() {
    let ProxyState::Unreachable { reason } = ProxyState::unreachable("\u{1b}[31mevil\u{202e}") else { panic!() };
    assert!(!reason.chars().any(|c| c.is_control() || c == '\u{202e}'));
}
// tests/proxy.rs (failed_association_fails_session_new is replaced)
#[tokio::test] async fn session_starts_when_udp_is_not_relayed() {
    // FakeSocks5 Script::NoUdp(7), dht_enabled = true with no bootstrap -> Session::new Ok;
    // proxy_state() == UdpNotRelayed; dht_stats().is_none(); demux_stats().is_empty(); a download from a seeder completes through the fake
}
#[tokio::test] async fn session_starts_with_proxy_unreachable() {
    // proxy at closed_port() -> Ok; matches!(proxy_state(), ProxyState::Unreachable { reason } if !reason.is_empty());
    // add_peer(seeder) -> no PeerConnected within 1 s and the seeder has no peer
}
#[tokio::test] async fn login_refused_is_its_own_state() {
    // RequireAuth("u","p"), creds ("u","wrong") -> proxy_state() == LoginRefused
}
#[tokio::test] async fn association_returns_after_proxy_comes_back() {
    // closed_port(); Session::new -> Unreachable; FakeSocks5::start_on(addr, Script::Ok)
    // -> ProxyStateChanged { state: Reachable } within 5 s; !demux_stats()[0].1.relay_lost; a UDP tracker announce through the relay is answered
}
#[tokio::test] async fn relay_loss_then_recovery() {
    // Script::Ok; drop fake -> ProxyStateChanged { Unreachable { .. } } and exactly one Event::ProxyError;
    // start_on(same addr) -> ProxyStateChanged { Reachable } within 5 s; TCP peers were not disconnected
}
#[tokio::test] async fn state_events_only_on_change() {
    // after Reachable, three successful proxied dials -> no further ProxyStateChanged within 500 ms
}
#[tokio::test] async fn removing_proxy_is_not_set() {
    // set_network_settings with ProxyKind::None -> ProxyStateChanged { NotSet }, proxy_state() == NotSet
}
// fbt-api tests/events.rs
#[test] fn proxy_state_event_dto() {
    // EventDto::from(&Event::ProxyStateChanged { state: Unreachable { reason: "x" } }) -> kind() == "proxy_state_changed",
    // JSON {"state":"unreachable","reason":"x"}; Reachable -> {"state":"reachable","reason":null}
}
```

- [ ] **Step 2: Run** `cargo test -p fbt-session --test proxy && cargo test -p fbt-session proxy_health && cargo test -p fbt-api proxy_state`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `cargo test --workspace && cargo clippy --workspace --all-targets --all-features -- -D warnings`, then `bash dev/stress.sh fbt-session proxy association_returns_after_proxy_comes_back 50`. Expected: PASS, `stress: 50/50 passed`.
- [ ] **Step 5: Commit** `feat(session): start without the proxy and report its health`.

### Task 6: Proxy-only and the no-bypass test (F3, F4 wiring; after Tasks 1, 3 and 5)

**Files:**
- Create: `crates/fbt-session/tests/no_bypass.rs`
- Modify: `crates/fbt-session/src/settings.rs` (`NetworkSettings::effective`, `proxy_bypasses` on it), `crates/fbt-session/src/netstack.rs` (build from `effective()`; `DhtConfig::resolve_bootstrap_names = false` under any proxy), the `fbt-session` outbound call sites (`peer/conn/mod.rs`, `net.rs`, `session.rs`, `torrent/webseed.rs`) with `audit::record`, `dev/check-outbound.sh` (drop the skip), `crates/fbt-session/src/settings.rs` docs of `Settings::proxy` (remove "There is no single proxy-only switch yet", describe `proxy_only`, F4)
- Test: `crates/fbt-session/tests/no_bypass.rs`, unit tests in `settings.rs`

**Interfaces:**
- Consumes: Task 1 (`audit::capture`, `Outbound`), Task 2 (`proxy_only`, `RecordOnly` fakes, `relay_addr`), Task 3 (`resolve_bootstrap_names`, relayed UDP tracker names), Task 5 (`proxy_state`).
- Produces: `impl NetworkSettings { pub fn effective(&self) -> NetworkSettings; pub fn proxy_bypasses(&self) -> Vec<&'static str>; }`. `effective()` with a proxy and `proxy_only`: `enable_incoming_tcp`, `enable_incoming_utp`, `enable_lsd`, `enable_upnp`, `enable_natpmp` false; `proxy_peers`, `proxy_trackers`, `proxy_hostnames` true; with an HTTP proxy also `dht_enabled` and `enable_outgoing_utp` false. Otherwise it returns the value unchanged except the 0.12.0 rule (HTTP proxy with `proxy_peers` turns outgoing uTP off). The stack is built from `effective()`; `network_settings()` still returns what was set. Validation runs on `effective()`, so a proxy-only session with `enable_outgoing_tcp` off is `InvalidSettings("a proxy-only session needs enable_outgoing_tcp")`.
- `no_bypass.rs` shares one torrent setup: trackers `http://tracker.invalid/announce`, `udp://tracker.invalid:6969/announce`, `udp://192.0.2.20:6969/announce`; web seed `http://seed.invalid/file`; `add_peer(198.51.100.7:6881)`; `dht_bootstrap_nodes = ["router.invalid:6881", "192.0.2.10:6881"]`; `enable_incoming_tcp` and `enable_incoming_utp` on (proxy-only must override them); LSD, UPnP, NAT-PMP off (the test rule; `effective()` is unit-tested for them); `proxy_hostnames = false` (proxy-only must override it). Each test waits until the fake has logged what it asserts, at most 10 s.

- [ ] **Step 1: Write the failing tests**

```rust
// settings.rs
#[test] fn proxy_only_turns_every_bypass_off() {
    // all switches on, Socks5 + proxy_only -> effective(): incoming tcp/utp, lsd, upnp, natpmp false; proxy_peers/trackers/hostnames true;
    // dht_enabled and enable_outgoing_utp unchanged; effective().proxy_bypasses().is_empty()
    // Http + proxy_only -> also dht_enabled and enable_outgoing_utp false
    // proxy_only with ProxyKind::None -> effective() == self
}
// tests/no_bypass.rs
fn only_proxy(cap: &[Outbound], proxy: SocketAddr, relay: Option<SocketAddr>) {
    for o in cap {
        match o {
            Outbound::TcpConnect(a) => assert_eq!(*a, proxy, "direct TCP: {o:?}"),
            Outbound::UdpSend(a) => assert_eq!(Some(*a), relay, "direct UDP: {o:?}"),
            Outbound::Lookup(h) => panic!("local lookup of {h}"),
            _ => panic!("unexpected {o:?}"),
        }
    }
}
#[tokio::test] async fn socks5_proxy_only_opens_nothing_else() {
    // fake Script::RecordOnly; only_proxy(cap, fake.addr(), fake.relay_addr());
    // fake.connects() ⊇ {"tracker.invalid:80", "seed.invalid:80", "198.51.100.7:6881"};
    // fake.udp_targets() ⊇ {"tracker.invalid:6969", "192.0.2.20:6969", "192.0.2.10:6881"} and none contains "router.invalid";
    // TcpStream::connect(session listen addr) is refused (incoming forced off)
}
#[tokio::test] async fn http_proxy_only_opens_nothing_else() {
    // FakeHttpProxy RecordOnly; only_proxy(cap, fake.addr(), None) (no UdpSend at all);
    // fake.connects() ⊇ {"tracker.invalid:80", "seed.invalid:80", "198.51.100.7:6881"}; dht_stats().is_none()
}
#[tokio::test] async fn unreachable_proxy_opens_nothing_else() {
    // closed_port(): only_proxy(cap, that addr, None) after 3 s with the torrent added
}
#[tokio::test] async fn udp_not_relayed_opens_nothing_else() {
    // Script::NoUdp(7) with RecordOnly-style CONNECT logging: only_proxy(cap, fake.addr(), None)
}
#[tokio::test] async fn switching_to_proxy_opens_nothing_direct() {
    // session with no proxy and the torrent added; capture starts, then set_network_settings(socks5 RecordOnly, proxy_only);
    // only_proxy(capture taken 3 s later, fake.addr(), fake.relay_addr())
}
#[tokio::test] async fn proxy_host_name_is_the_only_lookup() {
    // proxy host "localhost" (port of a RecordOnly fake): every Lookup in the capture is "localhost", nothing else resolves
}
```

- [ ] **Step 2: Run** `cargo test -p fbt-session --test no_bypass && cargo test -p fbt-session proxy_only_turns`. Expected: FAIL.
- [ ] **Step 3: Implement** `effective()`, build the stack from it, pass `resolve_bootstrap_names: false` under any proxy, and add the `fbt-session` audit records.
- [ ] **Step 4: Run** `bash dev/check-outbound.sh` (no skip) and the full gate: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo test --workspace`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features`, `cargo deny --all-features check`, `bash dev/check-pure-rust.sh`. Expected: `check-outbound: ok` and every gate green. Then `bash dev/stress.sh fbt-session no_bypass switching_to_proxy_opens_nothing_direct 50`. Expected: `stress: 50/50 passed`.
- [ ] **Step 5: Commit** `feat(session): proxy_only and the no-bypass test`.

### Task 7: Docs, changelog, release 0.13.0

**Files:**
- Modify: `CHANGELOG.md`, `crates/fbt/src/lib.rs` (crate docs: the proxy section), `README.md` (proxy paragraph, if it has one), `CLAUDE.md` (the test rule names `no_bypass.rs` and the audit hook; Commands: `dev/check-outbound.sh`), `Cargo.toml` (version on the release branch), `fuzz/seeds/socks5/` (a domain-framed datagram seed)

- [ ] **Step 1:** `CHANGELOG.md` under `[Unreleased]`, each entry ending in its PR link:
  - Added: `Session::set_network_settings` and `network_settings`, `NetworkSettings` (`effective`, `proxy_bypasses`), `Settings::network`/`set_network`, `ProxyConfig::proxy_only`, `ProxyState`, `Session::proxy_state`, `Event::ProxyStateChanged`, `DisconnectReason::NetworkChanged`, `ProxyError::Unreachable`, `fbt_net::proxy::probe`, `udp_not_relayed`, `encode_udp_host`, `DatagramSender::send_to_host`, `DhtConfig::resolve_bootstrap_names`, the `proxy_state_changed` API event.
  - Changed: `Session::new` starts when the SOCKS5 UDP association fails or the proxy is down (callers that matched `SessionError::Proxy` for that no longer get it); UDP tracker host names go to the SOCKS5 relay by name and DHT bootstrap names are skipped under a proxy; a failed dial to the proxy is `ProxyError::Unreachable`, no longer `Io` or `Timeout` (code matching those is affected); `Event::ProxyError` is superseded by `ProxyStateChanged` and kept for this release.
- [ ] **Step 2:** Run the Docker interop proxy scenario: `INTEROP_ONLY=proxy bash dev/interop/run.sh`. Expected: the scenario passes against Dante.
- [ ] **Step 3:** PR `feature/proxy-runtime` into `develop`, merge on green CI. Then `release/0.13.0` off `develop`: workspace version `0.13.0`, changelog section `## [0.13.0] - <date>`; `bash dev/test-release-version.sh` passes and `bash dev/release-version.sh --notes` prints the 0.13.0 section. PR into `main`, merge on green, annotated tag `v0.13.0` on `main`, then merge `main` back into `develop` through a `chore/merge-main-0.13.0` PR (as 0.12.0 did).
- [ ] **Step 4:** Tell the featherbittorrent controller the tag exists, so its round 1 moves the pin to `v0.13.0` and sets `proxy_only`.
