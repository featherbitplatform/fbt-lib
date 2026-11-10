# Milestone 3: Performance and Reach Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `fbt-lib` `v0.3.0`. Peers connect over uTP (BEP 29, LEDBAT) as well as TCP. uTP, the DHT and UDP trackers share one UDP socket per listen address, on IPv4 and IPv6. Ports are mapped through UPnP IGD and NAT-PMP/PCP. Peers are found on the LAN (BEP 14) and reached behind NATs through BEP 55 holepunch. Transfers obey global, per-torrent and per-peer-class rate limits. The choker gains configurable slots and seeding variants. Verified pieces go through a bounded write-back cache. All of it is proven against qBittorrent over uTP on the isolated Docker network.

**Architecture:** Two new protocol crates, each usable on its own:
- `fbt-utp` is uTP with a sans-IO core: a packet codec, a LEDBAT controller and a connection state machine that takes `now` as an argument. A thin tokio runtime (`UtpSocket`, `UtpStream`) sits on top. It never owns a socket unless asked to: it sends through an `Arc<dyn DatagramSender>` and is fed datagrams through `DatagramHandler`.
- `fbt-net` holds the shared UDP socket, which demultiplexes datagrams by their first bytes to the uTP, DHT and tracker handlers. It also holds the UPnP IGD client (SSDP, a small hand-written HTTP client, a bounded XML scanner, SOAP), NAT-PMP/PCP, LSD and default-gateway discovery.

The two traits live in `fbt_core::udp`, so `fbt-dht`, `fbt-tracker`, `fbt-utp` and `fbt-net` speak them without depending on each other. Both `fbt-dht` and `fbt-tracker` keep their standalone sockets and gain a mode that runs on the shared ones.

`fbt-session` changes:
- it binds a TCP listener and a shared UDP socket per listen address;
- peer connections become generic over a `PeerTransport` (TCP or uTP);
- rate limiting wraps every connection in a chain of debt-based token buckets;
- the write-back cache sits between the torrent actor and `Storage`, and its byte budget is shared session-wide with the partial-piece buffers.

The milestone starts with the file splits that milestone 2 deferred, so later tasks edit files of a workable size.

**Tech Stack:** as milestone 2. No new third-party crates: uTP, LEDBAT, SSDP, HTTP/1.1, XML, SOAP, NAT-PMP, PCP and LSD are all written here, on `tokio`, `socket2` (multicast, `SO_REUSEADDR`, IPv6-only sockets), `bytes`, `rand`, `tracing` and `thiserror`.

**Spec:** `docs/superpowers/specs/2026-11-10-fbt-lib-design.md` (binding). Milestone 3 is its section 8 item 3. Inputs:
- the section 4 rows for Transport (incl. mixed-mode balancing), Peers (BEP 14, 55), NAT, Peer management (choker, seeding variants), Rate limiting, Storage (write-back cache with size limit) and Core protocol (BEP 21, BEP 54);
- section 5.1 (`fbt-utp`, `fbt-net`), section 5.2 (shared UDP socket demultiplexed by packet shape) and section 7;
- the "Deferred" lists of PR #1 and PR #4 and the milestone 2 plan's deferred section. This plan folds in every item they give milestone 3.

## Global Constraints

- From scratch: uTP, LEDBAT, UPnP (SSDP, HTTP, XML, SOAP), NAT-PMP, PCP and LSD are written in this workspace. No BitTorrent, uTP, UPnP, NAT-PMP, HTTP-client or XML crates are added. General-purpose crates only, declared once in the root `[workspace.dependencies]`.
- `#![forbid(unsafe_code)]` in every crate, including `fbt-utp` and `fbt-net`. Both join the parser crates' clippy deny block (`unwrap_used`, `expect_used`, `indexing_slicing`, `panic` in non-test code).
- No panic, hang or unbounded allocation from remote input. Remote input here means uTP packets, LSD datagrams, SSDP replies, device descriptions, SOAP bodies, NAT-PMP/PCP replies and holepunch messages, as well as everything milestone 2 listed. Every remote list, map and buffer has a named cap constant. Every network wait has a timeout.
- `bash dev/check-pure-rust.sh` passes after every task that touches a `Cargo.toml`. No `*-sys` crate except `windows-sys`, no `cc`, no `ring` under `pure-rust`. Default-gateway discovery uses no FFI: it reads `/proc/net/route` on Linux and runs `route`/`netstat` elsewhere (Task 24).
- **No test and no CI job contacts the public internet, the public DHT or a real router.** `test_settings()` turns off the DHT, LSD, UPnP and NAT-PMP. NAT tests talk to in-repo fake gateways on loopback. LSD tests inject a loopback transport. The interop network stays `internal: true`, with UPnP and NAT-PMP off in every container.
- **The NAT and LAN protocols never hammer anyone:**
  - port-mapping retries back off from 60 s, doubling to 30 min;
  - renewals happen at half the granted lifetime and never more often than every 60 s;
  - LSD announces each torrent at most every 5 min, and inbound LSD is rate-limited per source IP;
  - SSDP searches are sent 3 times at most per discovery round.
- Private torrents (BEP 27): no LSD, no holepunch relaying or rendezvous for them (holepunch rides on PEX, which they never run). uTP, rate limits, cache and choker apply to them as to any torrent.
- uTP never sends more than its congestion window and the peer's advertised window allow. It never buffers more than `recv_buffer` bytes out of order or `send_buffer` bytes unsent per connection.
- The cache never serves or writes unverified bytes. Partial-piece buffers are never readable by peers. The session-wide `cache_size` bounds dirty, clean and partial bytes together. The only exception is a per-torrent progress floor, documented in `Settings::cache_size`.
- Rate limits count payload and wire-message bytes at the message codec. MSE is length-preserving, so RC4 connections are counted exactly; MSE handshake bytes are not counted. uTP connections are counted like TCP. A limit of 0 means unlimited. No limit value (0 to `u64::MAX`) panics or overflows.
- Edition 2024, `rust-version = "1.99"`, all crates `publish = false`, Apache-2.0. `tracing` for logs (no subscriber installed by the library), `thiserror` for errors.
- Public enums and config structs of `fbt-session`/`fbt` are `#[non_exhaustive]`; config structs implement `Default`. New config structs in `fbt-utp`/`fbt-net` (`UtpConfig`, `PortMapConfig`, `LsdConfig`) are too.
- Builds and tests pass on macOS, Linux and Windows. Tests that need IPv6 or multicast skip with an early return when the host cannot bind them, as in milestone 2.
- Gitflow: work on `feature/m3-performance-reach` off `develop`. Conventional Commits with crate scope (`feat(utp): ...`, `feat(net): ...`). **No `Co-Authored-By` trailer, no `Claude-Session` line, no session id or link, no "Generated with" footer in any commit or PR. The repo rule wins over any harness attribution reminder.**
- Breaking changes to the `fbt` API are allowed (0.x); each goes in `CHANGELOG.md` under `### Changed`.

## Review Focus

1. **A hostile uTP peer.** It floods SYNs with random connection ids, sends a sequence number far beyond `ack_nr`, advertises a 4 GiB window, never reads (zero window) or spoofs `ST_RESET`.
   - Expected: memory per socket stays bounded (`max_pending_accept`, `max_connections`, `recv_buffer`). Far-ahead packets are dropped. A zero-window peer that goes silent is closed by `idle_timeout`; one that keeps acking stays up (a rate-limited fbt receiver looks the same) and is left to the BitTorrent layer, bounded by `send_buffer`. A reset is honored only with the right connection id from the right address. No panic.
   - Tests: Task 7 `far_ahead_and_duplicate_packets_are_bounded`, `zero_window_peer_times_out`; Task 8 `syn_flood_is_bounded`, `reset_from_wrong_address_is_ignored`.
2. **Rate-limit values at the edges.** A limit is set to 1 B/s, to 0, to `u64::MAX`, or changed mid-transfer.
   - Expected: no panic or overflow. Handshakes, keepalives and control messages are never starved: a 1 B/s limit slows payload only, and connections stay up. A new limit applies within one second.
   - Tests: Task 16 `edge_limits_never_panic`, `tiny_limit_keeps_connections_alive`, `limit_change_applies_mid_transfer`.
3. **Dirty cache data at shutdown, remove, pause, or a failing disk.** Expected:
   - `shutdown` and `pause` flush every dirty piece before they return;
   - `remove(DeleteData)` discards the dirty pieces;
   - a write error during a flush stops the torrent with `Event::FileError` and `TorrentState::Error`, and the next resume rechecks;
   - a piece is readable by peers from the moment it passes its hash, whether written yet or not.

   Tests: Task 15 `shutdown_and_pause_flush_dirty_pieces`, `flush_failure_stops_the_torrent`, `cached_piece_is_served_before_it_is_written`.
4. **A listen address that only half binds.** UDP 6881 is taken while TCP 6881 is free, the host has no IPv6, or `listen_addrs` is empty.
   - Expected: the session starts. That address runs TCP-only, and the DHT falls back to its own ephemeral socket as in milestone 2. An IPv6 address that cannot bind is skipped and listed in `Session::listen_errors()`. An empty list is `InvalidSettings`; a list where nothing binds is `SessionError::Bind`.
   - Tests: Task 11 `dht_port_taken_falls_back` (extended with `udp_only`), `ipv6_bind_failure_is_reported_and_skipped`, `empty_listen_addrs_is_invalid`.
5. **A hostile LAN.** A rogue SSDP responder points `LOCATION` at another host, or serves a 10 MiB or byte-per-second description. NAT-PMP replies come from a non-gateway address. LSD is flooded.
   - Expected: `LOCATION` must name the responder's own IP; HTTP responses are capped at 64 KiB and 5 s; NAT-PMP/PCP replies from anyone but the gateway are ignored; LSD is rate-limited per IP and its decoded hashes are capped.
   - Tests: Task 23 `location_must_name_the_responder`, `oversized_or_slow_description_is_cut`; Task 22 `reply_from_wrong_source_is_ignored`; Task 20 `lsd_flood_is_rate_limited`.

---

## Starting point

`develop` holds milestone 2 (`v0.2.0`, merged back from `release/0.2.0`). The branch `feature/m3-performance-reach` already exists off `develop` and holds only this plan. Check first that the workspace version is `0.2.0` and `cargo test --workspace` is green.

## Decisions recorded in this plan

The owner made these in the milestone 3 brief:
- New crates `fbt-utp` (socket-agnostic: consumes and produces datagrams) and `fbt-net` (shared UDP demux, UPnP IGD, NAT-PMP/PCP, LSD). Both are from scratch and take no new third-party dependency. The UPnP HTTP is hand-written.
- uTP, DHT and UDP-tracker traffic share the listen port's UDP socket. The DHT and UDP tracker keep a standalone mode, so `fbt-dht` stays usable alone.
- Multiple listen addresses (IPv4 and IPv6) are in scope.
- The interop bar:
  - qBittorrent-nox serves a download over uTP on the `internal: true` network;
  - LSD discovery runs between two of our processes on that network;
  - UPnP and NAT-PMP are tested only against in-repo fake gateways;
  - holepunch is tested with three of our sessions in integration tests.
- Rate limits are token buckets: global, per torrent and per peer class (TCP, uTP, local network). They cover both directions, uTP included, with validated settings.
- The write-back cache is bounded by a byte limit, flushed on completion, pressure, pause, remove and shutdown. It never serves unverified data, and reads hit it.
- Choker tuning: configurable unchoke slots, rate-based tit-for-tat, optimistic unchoke rotation, and the seeding variants round-robin, fastest-upload and anti-leech.
- Version `0.3.0`; changelog entries go under `## [Unreleased]`.

The builder made these calls (they can be revisited by whoever plans a later milestone):
- **Flushing.** "Flush on piece completion" is read as "flush when the torrent completes" (`TorrentFinished`). Flushing every piece would make the cache write-through. Dirty pieces also flush after `cache_expiry` (30 s), under pressure, and before a recheck.
- **Shared traits in `fbt-core`.** `fbt_core::udp` holds the two datagram traits, so `fbt-utp` depends on `fbt-core` (the spec table says "none (tokio)"). The traits are std-only and tiny.
- **Outgoing transport.** It follows libtorrent: uTP first for every peer not known to lack it, then one TCP attempt after a uTP *connect* failure (and the reverse when uTP is off or the peer is known TCP-only). This is independent of the MSE plaintext retry, which follows an *encryption* failure.
- **Mixed mode** applies only while a global limit is set. `PeerProportional` (default, libtorrent's default) splits the global limit between the TCP and uTP classes by their connected-peer counts, re-evaluated every second. `PreferTcp` gives the uTP class 1/10 of the global limit while any TCP peer is connected. Without a global limit, LEDBAT's own yielding is the balance.
- **Local peers** (`fbt_dht::is_local` addresses) are exempt from the global and torrent limits unless `rate_limit_local_peers` is set (libtorrent's `ignore_limits_on_local_network`). Their transport class (`tcp`/`utp`) and the `local` class still apply.
- **Gateway discovery.** The UPnP device description is read with a hand-written bounded XML scanner. The default gateway comes from `/proc/net/route` (Linux), `route -n get default` (macOS/BSD) or `route print -4` (Windows), with `Settings::nat_gateway` as an override. Port mapping is IPv4-only (IPv6 is rarely NATed; PCP pinholes are backlog).
- **BEP 54 send side.** `lt_donthave` is sent when serving a block finds its piece gone from disk (`NotFound`). The piece is dropped from `have` and picked again, instead of stopping the whole torrent. Any other read error still stops it.
- **BEP 21 disconnect.** When we have every wanted piece and the remote is a seed or `upload_only`, the connection is closed with `DisconnectReason::UploadOnly`, as libtorrent does for upload-to-upload peers.
- **Holepunch transport.** BEP 55 is attempted over uTP only (TCP simultaneous open is unreliable through NATs). Two simultaneous uTP connections are resolved by milestone 2's peer-id tie-break.
- **Listen binding.** An address whose UDP port is taken while its TCP port is free runs TCP-only. Its DHT falls back to an ephemeral socket. Its UDP tracker traffic uses the tracker client's own sockets.

### Pre-flight rulings

Taken from the pre-flight conflict scan (`.superpowers/sdd/2026-11-10-milestone-3-performance-reach/preflight.md`) and applied in the tasks named:
- T1: the 700-line rule covers only the files Task 1 creates; unlisted items stay put; splitting `torrent/swarm.rs` is a follow-up.
- T2: reuse the existing `KeyPair::from_private(&[u8; 20])`; a drain error inside `poll_write` is returned by that call, stored only when the drain was `Pending`, then `BrokenPipe`.
- P6/T2/T26: the `mse` fuzz target uses Task 2's keys (initiator `[7; 20]`, responder `[9; 20]`) so captured seeds pass the sync search.
- T4: `ipv6_socket_is_v6_only` binds the unspecified addresses; `proptest` dev-dep.
- T5, T7, T11: `bash dev/check-pure-rust.sh` after their `Cargo.toml` changes; T5 adds the `proptest` dev-dep, T7 `sha1`.
- T6/T21/T23: `pub` items in private modules (re-exported `#[doc(hidden)]` where fuzzing needs them), never `pub(crate)` re-exported as `pub` (E0364); `DelayHistory::delay` added; the base-delay minimum compares with wrapping.
- T7/P21: `Connection` and its methods are `pub` in the hidden `conn` module, plus hidden `buffered_bytes()`; all `UtpConfig` fields `pub`; `std::time::Instant` in the core, the sim converts with `into_std()`.
- T7/P22: uTP closes only on "no packet received for `idle_timeout`"; a zero-window peer that keeps acking stays up and is left to the BitTorrent layer, so a rate-limited receiver is not dropped.
- T8: `syn_flood_is_bounded` shortens `idle_timeout`/`connect_timeout` to about 500 ms; dead pending connections leave the accept queue.
- T9: new `DhtError::DuplicateFamily` instead of reusing `InvalidState`.
- T10: the shared-socket tests live in `udp.rs`'s test module.
- T11: IPv6 TCP bound v6-only through `socket2`; minimal `fbt-example` port to `listen_addrs` here (P30); `data.rs` `Shared` literal and `tests/dht.rs` `dht_ipv6` in Files; the two duplicate DHT tests become extensions of the existing ones.
- P25: `DhtInput` is registered only on the sockets passed to `Dht::spawn_on`.
- P28: one session-wide `ListenerState` (pending-handshake semaphore and per-IP map) shared by every TCP and uTP accept loop.
- T12/P23: incoming uTP off installs a reject-all accept filter, so SYNs time out at the dialer and TCP fallback happens; `messages.rs` and `peers.rs` in Files.
- P24/T19: the uTP accept filter is one closure in `net.rs` composing the incoming-off and NAT-emulation checks.
- P33: `Known.set` becomes a map of per-address records.
- T13: `actor.rs` and `swarm.rs` in Files; `tests/pex.rs` dropped (the PEX tests are unit tests).
- T14/P36: `StorageError` reachable through `storage/mod.rs` and the `fbt_session` root.
- T15: the progress floor applies only while `dirty + in-flight writes <= cache_size`; `PieceFinished` doc fixed.
- T16: `repaid_at()` (uncapped) split from `ready_at()` (capped at 1 s); buckets start at balance 0; integration tests set `rate_limit_local_peers` except `local_limit_applies_when_enabled`; every peer chains its transport class, local peers add `local`; idle timer paused while reads are throttled; `Cancel` drops a queued block; the upload-count test sets an upload limit.
- P39/T25: the CLI sets `rate_limit_local_peers` whenever a limit flag is given.
- P37/T18: the lost-piece test runs B with `cache_size: 0`.
- T17: the rate-based test asserts {10k, 5k} plus one optimistic peer; `progress_ppm: u32` keeps `Eq`; `ChokerConfig` is `pub`, `#[non_exhaustive]`, `Default`; breaking changes go in CHANGELOG.
- T18: `PiecePicker::peer_lost(piece)` without a key; `HaveAll` materialized through `remove_peer_pieces(All)` + `add_peer_pieces(Bits)`; `FileError` doc updated.
- T19: A and C dial R; `net.rs` gets its own `MAX_RECENT_SENDS`; the not-connected requester is a scripted peer.
- T20/P16: hidden `lsd_bind` gives the session's LSD its own plain UDP socket through `Lsd::spawn_on`; last announce recorded per hash; `real_multicast_loopback` skips only on a failed join and uses TTL 0 on loopback; `IpRateLimiter` hoisted into `fbt-core`; missing files listed.
- P40/T20/T24: `fbt-example` turns LSD off in Task 20 and UPnP/NAT-PMP off in Task 24, so `cargo test --workspace` never touches the LAN before Task 25.
- T22/P43: `MapProtocol` and `MapMethod` `#[non_exhaustive]`; `Expect::{natpmp_external, natpmp_map, pcp_map}` listed.
- T23: `Igd`/`discover` use Task 21's inner request with a test-settable timeout; the test matches `UpnpError::Http(HttpError::{TooLarge, Timeout})`.
- T24: `tests/support/mod.rs` in Files; portmap tests assert on mappings and timing, not request counts; no dev-dependency for `#[path]` modules.
- P44/T25/T28: a hidden `lsd-probe` subcommand gives Task 28 its "found a peer, exit 0" probe.
- T27: the binary prints throughput and its pid; `bench.sh` samples CPU and RSS with `ps` and records RSS per idle torrent.
- T29: CHANGELOG `### Changed` also lists `ExtHandshake`, the choker API, `DhtError` and the earlier `PieceFinished`.
- Spec 5.3 (serde on config structs) stays unimplemented this milestone and is on the Deferred list.

---

## File Structure

```
Cargo.toml                                       + crates fbt-utp, fbt-net (members via crates/*); no new external deps
crates/fbt-core/src/udp.rs                       DatagramSender, DatagramHandler
crates/fbt-core/src/rate.rs                      IpRateLimiter (hoisted from fbt-dht; used by fbt-dht and fbt-net)
crates/fbt-wire/src/ext.rs                       upload_only (BEP 21), lt_donthave (BEP 54), ut_holepunch (BEP 55)
crates/fbt-wire/src/mse/{stream.rs,handshake.rs,fuzzing.rs}   sticky write error, fixed-key fuzz seam
crates/fbt-utp/                                  new crate
  src/{lib.rs,packet.rs,ledbat.rs,rtt.rs,conn.rs,socket.rs,stream.rs}
  tests/{sim.rs,socket.rs}
crates/fbt-net/                                  new crate
  src/{lib.rs,demux.rs,lsd.rs,natpmp.rs,gateway.rs,portmap.rs}
  src/upnp/{mod.rs,ssdp.rs,http.rs,xml.rs,soap.rs}
  tests/{demux.rs,lsd.rs,natpmp.rs,upnp.rs,support/{fake_igd.rs,fake_natpmp.rs}}
crates/fbt-dht/src/node.rs                       spawn_on (external sockets), DhtInput, bootstrap spacing; ExternalIpVotes public
crates/fbt-tracker/src/{lib.rs,udp.rs}           UDP announces through shared sockets
crates/fbt-session/src/
  settings.rs, event.rs, status.rs, error.rs, session.rs, listener.rs   (extended)
  net.rs                                         listen addresses, shared sockets, uTP sockets, LSD, port mapping wiring
  bandwidth.rs                                   token buckets, peer classes, mixed mode
  disk.rs                                        bounded disk pool and check semaphore
  storage/cache.rs                               write-back cache and the session cache budget
  storage/fs.rs                                  per-handle I/O outside the state lock, deferred sync
  choker.rs                                      algorithms and seeding variants
  peer/transport.rs                              PeerTransport (TCP | uTP), Transport
  peer/conn/{mod.rs,state.rs,inbound.rs,outbound.rs,tests.rs}   split of peer/conn.rs
  torrent/{check.rs,commands.rs,magnet.rs}       split of torrent/actor.rs
  torrent/holepunch.rs                           BEP 55 relay and rendezvous bookkeeping (pure)
crates/fbt-session/tests/{utp.rs,ratelimit.rs,cache.rs,holepunch.rs,lsd.rs,portmap.rs}
crates/fbt/src/lib.rs, crates/fbt/tests/lib_surface.rs
crates/fbt-example/src/main.rs, tests/cli.rs
fuzz/fuzz_targets/{utp_packet.rs,utp_conn.rs,lsd.rs,ssdp.rs,upnp_xml.rs,natpmp.rs}, fuzz/seeds/<new targets>/
.github/workflows/{fuzz.yml,ci.yml}
dev/bench.sh, docs/benchmarks.md
dev/interop/{compose.yaml,run.sh,README.md,qbittorrent/qBittorrent.conf}
```

---

### Task 1: split `peer/conn.rs` and `torrent/actor.rs`

Milestone 2 deferred this. The task moves code and changes no behavior.

**Files:**
- Create: `crates/fbt-session/src/peer/conn/{mod.rs,state.rs,inbound.rs,outbound.rs,tests.rs}` (replacing `peer/conn.rs`), `crates/fbt-session/src/torrent/{check.rs,commands.rs,magnet.rs}`.
- Modify: `crates/fbt-session/src/peer/mod.rs`, `crates/fbt-session/src/torrent/{actor.rs,mod.rs}`.

**Interfaces:**
- Produces (crate-internal; every path other modules import keeps working through re-exports from `peer::conn`):
  - `peer/conn/mod.rs`: `command_queue`, `MAX_ALLOWED_FAST`, `MAX_PENDING_HAVES`, `PeerCtx`, `PeerStream`, `spawn_outgoing`, `spawn_incoming`, `run_peer`, `run_peer_until`, `report`, `handshake`, `check_handshake`, `after`, `max_frame`.
  - `peer/conn/state.rs`: `Conn<S>`, `Pending`, `RawPieces`, `Conn::{new, serve, greet, next_deadline, on_timer}`.
  - `peer/conn/inbound.rs`: `Conn::{on_message, on_extended, require_fast, initial_pieces, metadata_ready, on_remote_request}`.
  - `peer/conn/outbound.rs`: `Conn::{want_blocks, on_command, emit, write_done, feed, flush, send}`.
  - `peer/conn/tests.rs`: every test now in `conn.rs`, unchanged.
  - `torrent/check.rs`: `start_check`, `cancel_check`, `on_checked`, `add_availability`.
  - `torrent/commands.rs`: `on_cmd`, `resume`, `remember_left`, `recheck`, `set_priorities`, `stop`, `selected_priorities`.
  - `torrent/magnet.rs`: `on_metadata_job`, `install_metadata`, `metadata_tick`, `MagnetState`.
  - `actor.rs` keeps the struct, `new`, `run`, status and state, announcing and errors.
  - Items not listed above stay where they are: `wire_reason`, `protocol`, `Flow`, `COMMAND_HEADROOM`, `NO_METADATA_MAX_FRAME` in `conn/mod.rs`; `recv_opt`, `wanted_bytes` in `actor.rs`.
- No file this task creates exceeds about 700 lines; `tests.rs` files are exempt. The rule covers only these files: `torrent/swarm.rs` is not split here (recorded as a follow-up in the Deferred list).

- [ ] **Step 1: Move the code.** Use `git mv peer/conn.rs peer/conn/mod.rs` first, so history follows, then cut. Change visibilities only as far as the split needs (`pub(super)` to `pub(in crate::peer)`, and so on).
- [ ] **Step 2: Run.** `cargo test -p fbt-session && cargo clippy -p fbt-session --all-targets -- -D warnings`: PASS, with the same test count as before (`cargo test -p fbt-session 2>&1 | grep -c '... ok'` before and after).
- [ ] **Step 3: Commit.** `refactor(session): split peer/conn.rs and torrent/actor.rs by responsibility` No trailers or attribution of any kind.

---

### Task 2: MSE sticky write error and a fixed-key fuzz seam

**Files:**
- Create: `crates/fbt-wire/src/mse/fuzzing.rs`.
- Modify: `crates/fbt-wire/src/mse/{stream.rs,handshake.rs,dh.rs,mod.rs}`, `crates/fbt-wire/src/lib.rs`.

**Interfaces:**
- Produces:
  - **The pending-write error.** `MseStream` keeps `pending_err: Option<io::Error>`. Today, `poll_write`'s opportunistic drain discards its transport error (`let _ = this.poll_drain(cx)`). Now:
    - when the drain fails inside a `poll_write`, that same call returns the error (nothing of this call is reported written);
    - when the drain is still `Pending` and its error surfaces later (in a background drain or `poll_flush`), it is stored in `pending_err`, and the next `poll_write`, `poll_flush` or `poll_shutdown` returns it;
    - after the error has been returned once, every call returns `io::ErrorKind::BrokenPipe`.

    No byte is reported written that will never be sent.
  - **The fuzz seam.** `#[doc(hidden)] pub mod mse_fuzzing` re-exports `fuzzing.rs`:
    - `pub async fn initiate_fixed<S>(stream: S, info_hash: &[u8; 20], provide: CryptoMethods, ia: &[u8], private: [u8; 20], pad_len: usize) -> Result<MseStream<S>, MseError>`;
    - `pub async fn respond_fixed<S>(stream: S, prefix: Vec<u8>, lookup: &(dyn Fn(&[u8; 20]) -> Option<[u8; 20]> + Sync), allowed: CryptoMethods, private: [u8; 20], pad_len: usize) -> Result<(MseStream<S>, [u8; 20]), MseError>`.

    `initiate`/`respond` call the same inner functions with a random key and pad. Pads are zero bytes, of length `pad_len.min(MSE_MAX_PAD)`. The fixed-key path reuses the existing `pub(crate) KeyPair::from_private(x: &[u8; PRIVATE_LEN])` in `dh.rs`; no new constructor is added.
  - **Fuzz-seed obligation (Task 26 consumes it).** The fixed keys are initiator `[7; 20]` and responder `[9; 20]`. Task 26's `mse` target uses exactly these two keys, so a transcript captured here passes the `req1`/SKEY sync search on the fuzzed side.

- [ ] **Step 1: Write failing tests.**
  - `stream.rs`: `drain_error_resurfaces_once_then_broken_pipe`. The writer accepts 3 bytes, then fails with `ConnectionReset`. `write_all(b"hello")` returns `ConnectionReset` (from the same `poll_write` whose drain failed); the next `write` returns `BrokenPipe`. A second case covers the stored path: the drain is `Pending`, then fails, and the next `poll_flush` returns `ConnectionReset`, then `BrokenPipe`.
  - `tests/mse.rs`: `fixed_keys_are_deterministic`. Two `initiate_fixed` runs with private `[7; 20]` and `pad_len` 0 against `respond_fixed` with `[9; 20]` produce byte-identical initiator transcripts (captured with a recording duplex wrapper). The handshake also succeeds with RC4. The test can write both transcripts out (behind an env var) for Task 26's seeds.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-wire`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test -p fbt-wire`: PASS.
- [ ] **Step 5: Commit.** `fix(wire): surface MSE drain errors; fixed-key MSE seam for fuzzing` No trailers or attribution of any kind.

---

### Task 3: extension messages for BEP 21, BEP 54 and BEP 55

**Files:**
- Modify: `crates/fbt-wire/src/ext.rs`, `crates/fbt-wire/src/lib.rs`, `crates/fbt-wire/tests/ext.rs`.

**Interfaces:**
- Produces (re-exported from `fbt_wire`):
  - Names and our ids (libtorrent's numbering):
    - `pub const UPLOAD_ONLY: &str = "upload_only";` and `pub const OUR_UPLOAD_ONLY_ID: u8 = 3;`
    - `pub const UT_HOLEPUNCH: &str = "ut_holepunch";` and `pub const OUR_UT_HOLEPUNCH_ID: u8 = 4;`
    - `pub const LT_DONTHAVE: &str = "lt_donthave";` and `pub const OUR_LT_DONTHAVE_ID: u8 = 7;`
  - `ExtHandshake` gains `pub upload_only: Option<bool>` (key `upload_only`, integer; non-zero means true; encoded only when `Some(true)`).
  - `pub fn encode_upload_only(on: bool) -> Bytes` (one byte, 0 or 1) and `pub fn decode_upload_only(payload: &[u8]) -> Result<bool, ExtError>`. An empty payload gives `InvalidField("upload_only")`; extra bytes are ignored.
  - `pub fn encode_donthave(piece: u32) -> Bytes` (4 bytes, big-endian) and `pub fn decode_donthave(payload: &[u8]) -> Result<u32, ExtError>`. Any length but 4 gives `InvalidField("donthave")`.
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum HolepunchMsg { Rendezvous(SocketAddr), Connect(SocketAddr), Error { addr: SocketAddr, code: u32 } }`, with `encode(&self) -> Bytes` and `decode(payload: &[u8]) -> Result<HolepunchMsg, ExtError>`.
    - Wire layout: `msg_type: u8` (0 rendezvous, 1 connect, 2 error), `addr_type: u8` (0 IPv4, 1 IPv6), the address (4 or 16 bytes), `port: u16` big-endian, then `err_code: u32` big-endian for errors only.
    - Decoding: an unknown type gives `InvalidField("msg_type")`, an unknown address type `InvalidField("addr_type")`, port 0 `InvalidField("port")`, a short payload `InvalidField("holepunch")`. Trailing bytes are ignored.
  - `pub const HP_NO_SUCH_PEER: u32 = 1; HP_NOT_CONNECTED: u32 = 2; HP_NO_SUPPORT: u32 = 3; HP_NO_SELF: u32 = 4;`

- [ ] **Step 1: Write failing tests** in `tests/ext.rs`

```rust
#[test] fn holepunch_bep55_layout() {
    let m = HolepunchMsg::Rendezvous("10.0.0.1:6881".parse().unwrap());
    assert_eq!(&m.encode()[..], &[0, 0, 10, 0, 0, 1, 0x1a, 0xe1]);
    let e = HolepunchMsg::Error { addr: "[::1]:80".parse().unwrap(), code: HP_NOT_CONNECTED };
    let wire = e.encode(); assert_eq!(wire.len(), 1 + 1 + 16 + 2 + 4); assert_eq!(HolepunchMsg::decode(&wire).unwrap(), e);
    assert!(matches!(HolepunchMsg::decode(&[9, 0, 1, 2, 3, 4, 0, 1]), Err(ExtError::InvalidField("msg_type"))));
    assert!(matches!(HolepunchMsg::decode(&[1, 2, 1, 2, 3, 4, 0, 1]), Err(ExtError::InvalidField("addr_type"))));
    assert!(matches!(HolepunchMsg::decode(&[1, 0, 1, 2, 3, 4, 0, 0]), Err(ExtError::InvalidField("port"))));
    assert!(matches!(HolepunchMsg::decode(&[2, 0, 1, 2, 3, 4, 0, 1]), Err(ExtError::InvalidField("holepunch"))));
}
#[test] fn donthave_and_upload_only() {
    assert_eq!(decode_donthave(&encode_donthave(0xdead_beef)).unwrap(), 0xdead_beef);
    assert!(decode_donthave(&[1, 2, 3]).is_err());
    assert!(decode_upload_only(&encode_upload_only(true)).unwrap());
    assert!(decode_upload_only(&[]).is_err());
    let h = ExtHandshake { upload_only: Some(true), ..Default::default() };
    assert_eq!(ExtHandshake::decode(&h.encode()).unwrap().upload_only, Some(true));
}
```

  Extend the existing `ext_decoders_never_panic` proptest to the three new decoders.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-wire --test ext`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test -p fbt-wire`: PASS.
- [ ] **Step 5: Commit.** `feat(wire): upload_only, lt_donthave and ut_holepunch extension messages` No trailers or attribution of any kind.

---

### Task 4: datagram traits and the `fbt-net` shared UDP socket

**Files:**
- Create: `crates/fbt-core/src/udp.rs`; `crates/fbt-net/{Cargo.toml,src/lib.rs,src/demux.rs,tests/demux.rs}`. The crate uses the parser-crate clippy deny block. Deps: `fbt-core`, `tokio` (`net`, `rt`, `time`, `sync`, `macros`), `socket2`, `bytes`, `rand`, `tracing`, `thiserror`. Dev-deps: `proptest.workspace = true`, `tokio` (`rt-multi-thread`, `macros`).
- Modify: `crates/fbt-core/src/lib.rs` (`pub mod udp;`).

**Interfaces:**
- Produces:
  - `fbt_core::udp`:
    - `pub trait DatagramSender: Send + Sync + std::fmt::Debug + 'static { fn local_addr(&self) -> SocketAddr; fn send_to(&self, data: &[u8], to: SocketAddr) -> io::Result<usize>; }`. It never blocks: `WouldBlock` means the datagram was dropped, as UDP may.
    - `pub trait DatagramHandler: Send + Sync + std::fmt::Debug + 'static { fn handle(&self, from: SocketAddr, data: &[u8]); }`. It must not block or await (it runs on the receive task); handlers that need async work `try_send` into their own bounded channel.
  - `fbt_net::demux`:
    - `#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)] pub enum PacketKind { Utp, Dht, Tracker }`
    - `pub fn classify(data: &[u8]) -> Option<PacketKind>`, by shape (section 5.2):
      - `Tracker` when `len >= 8` and the first 4 bytes are `00 00 00 0x` with `x <= 3` (the action);
      - `Dht` when the first byte is `b'd'`;
      - `Utp` when `len >= 20`, `data[0] & 0x0f == 1` and `data[0] >> 4 <= 4`;
      - otherwise `None`, and the datagram is dropped.

      The three shapes are disjoint, since a uTP first byte is never 0x00 or `b'd'`.
    - `pub const MAX_DATAGRAM: usize = 65_536;` is the receive buffer.
    - `#[derive(Debug, Clone)] pub struct SharedUdpSocket`, with:
      - `pub fn bind(addr: SocketAddr) -> io::Result<SharedUdpSocket>` (inside a tokio runtime; IPv6 addresses are IPv6-only through `socket2`, non-blocking);
      - `pub fn local_addr(&self) -> SocketAddr`;
      - `pub fn sender(&self) -> Arc<dyn DatagramSender>`;
      - `pub fn set_handler(&self, kind: PacketKind, h: Arc<dyn DatagramHandler>)` (replaces any previous one);
      - `pub fn stats(&self) -> DemuxStats`;
      - `pub fn close(&self)` (idempotent; the receive task also ends when the last clone drops).
    - `#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)] #[non_exhaustive] pub struct DemuxStats { pub utp: u64, pub dht: u64, pub tracker: u64, pub dropped: u64 }`. A datagram with no handler for its kind counts as dropped.
    - The receive loop treats `recv_from` errors (Windows `ConnectionReset` from ICMP) as transient. It logs at debug and continues, sleeping 50 ms after 16 consecutive errors so it never spins.

- [ ] **Step 1: Write failing tests**
  - Unit tests in `demux.rs`:
    - `classify_by_shape`: the uTP SYN header from Task 5's vector is `Utp`; `b"d1:ad1:id20:...e1:q4:ping1:t2:aa1:y1:qe"` is `Dht`; `[0,0,0,1, 0,0,0,7]` is `Tracker`; `[0,0,0,9,...]`, `b"x"`, an empty slice and a 19-byte uTP-looking buffer are `None`.
    - A proptest `classify_never_panics`.
  - Integration tests in `tests/demux.rs`, with a recording handler:
    - `routes_each_kind_to_its_handler`: three datagrams from a raw socket reach the three handlers, and `stats()` counts them.
    - `unknown_and_unhandled_are_dropped`
    - `sender_shares_the_port`: a reply through `sender()` arrives at the raw socket from `local_addr()`.
    - `ipv6_socket_is_v6_only`: skipped if `[::]:0` cannot bind. An IPv4 `SharedUdpSocket` on `0.0.0.0:p` and an IPv6 one on `[::]:p` bind the same port. Only the unspecified addresses prove v6-only (`127.0.0.1` and `[::1]` coexist without it); nothing is sent, so nothing leaves the host.
    - `close_stops_delivery`
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-net`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test -p fbt-core -p fbt-net && bash dev/check-pure-rust.sh`: PASS.
- [ ] **Step 5: Commit.** `feat(net): shared UDP socket demultiplexed by packet shape; datagram traits in fbt-core` No trailers or attribution of any kind.

---

### Task 5: `fbt-utp` crate and the BEP 29 packet codec

**Files:**
- Create: `crates/fbt-utp/{Cargo.toml,src/lib.rs,src/packet.rs}`. The crate uses the parser-crate deny block. Deps: `fbt-core`, `bytes`, `thiserror`, `tracing`; `tokio` and `rand` are added in later tasks. Dev-dep: `proptest.workspace = true`.

**Interfaces:**
- Produces (`#[doc(hidden)] pub mod packet` for the fuzz targets, plus re-exports):
  - `pub const HEADER_LEN: usize = 20;`, `pub const MAX_EXTENSIONS: usize = 8;` and `pub const MAX_SACK_LEN: usize = 32;` (bytes of selective-ack mask kept; longer masks are cut).
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum PacketType { Data = 0, Fin = 1, State = 2, Reset = 3, Syn = 4 }`
  - `#[derive(Debug, Clone, PartialEq, Eq)] pub struct Packet { pub ty: PacketType, pub conn_id: u16, pub timestamp_us: u32, pub timestamp_diff_us: u32, pub wnd_size: u32, pub seq_nr: u16, pub ack_nr: u16, pub sack: Option<Vec<u8>>, pub payload: Bytes }`, with `encode(&self, out: &mut Vec<u8>)` and `decode(datagram: &Bytes) -> Result<Packet, PacketError>` (the payload is a zero-copy slice).
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)] pub enum PacketError { TooShort, BadVersion, BadType, BadExtension }`
  - `pub fn sack_acks(sack: &[u8], ack_nr: u16) -> impl Iterator<Item = u16>`. Bit `j` (LSB first) of byte `k` acknowledges `ack_nr + 2 + 8k + j`, wrapping (BEP 29).
  - Helpers `pub fn seq_lt(a: u16, b: u16) -> bool` and `seq_le`, for wrapping comparison (`b.wrapping_sub(a)` in `1..0x8000`).

Decoding rules:
- **Header.** The header is 20 big-endian bytes: `type<<4 | version` (version must be 1), `extension`, `connection_id`, `timestamp_microseconds`, `timestamp_difference_microseconds`, `wnd_size`, `seq_nr`, `ack_nr`.
- **Extension chain.** Each extension is `next_type, len, len bytes`. Type 1 is SACK: its `len` must be a non-zero multiple of 4, else `BadExtension`. Unknown types are skipped.
- **Limits.** More than `MAX_EXTENSIONS` links, or a link running past the datagram, gives `BadExtension`. Everything after the chain is payload.

- [ ] **Step 1: Write failing tests** in `packet.rs`

```rust
#[test] fn syn_header_vector() {
    let p = Packet { ty: PacketType::Syn, conn_id: 0x1234, timestamp_us: 0x0102_0304, timestamp_diff_us: 0, wnd_size: 0x0010_0000, seq_nr: 1, ack_nr: 0, sack: None, payload: Bytes::new() };
    let mut out = Vec::new(); p.encode(&mut out);
    assert_eq!(out, [0x41, 0, 0x12, 0x34, 1, 2, 3, 4, 0, 0, 0, 0, 0, 0x10, 0, 0, 0, 1, 0, 0]);
    assert_eq!(Packet::decode(&Bytes::from(out)).unwrap(), p);
}
#[test] fn sack_roundtrip_and_bits() {
    let p = Packet { ty: PacketType::State, sack: Some(vec![0b0000_0101, 0, 0, 0x80]), ..state(10) };
    let mut out = Vec::new(); p.encode(&mut out); assert_eq!(out[1], 1); assert_eq!(&out[20..22], &[0, 4]);
    let back = Packet::decode(&Bytes::from(out)).unwrap();
    assert_eq!(sack_acks(back.sack.as_deref().unwrap(), 10).collect::<Vec<_>>(), vec![12, 14, 43]);
    assert_eq!(sack_acks(&[1], 0xffff).collect::<Vec<_>>(), vec![1]);
}
#[test] fn rejects_bad_packets() {
    assert_eq!(Packet::decode(&Bytes::from_static(&[0x41; 19])), Err(PacketError::TooShort));
    let mut v = syn_bytes(); v[0] = 0x42; assert_eq!(Packet::decode(&v.into()), Err(PacketError::BadVersion));
    let mut v = syn_bytes(); v[0] = 0x51; assert_eq!(Packet::decode(&v.into()), Err(PacketError::BadType));
    let mut v = syn_bytes(); v[1] = 1; v.extend([0, 3, 1, 2, 3]); assert_eq!(Packet::decode(&v.into()), Err(PacketError::BadExtension));
    let mut v = syn_bytes(); v[1] = 9; for _ in 0..20 { v.extend([9, 0]); } assert_eq!(Packet::decode(&v.into()), Err(PacketError::BadExtension));
}
#[test] fn wrapping_sequence_order() { assert!(seq_lt(0xfffe, 1)); assert!(!seq_lt(1, 0xfffe)); assert!(seq_le(5, 5)); }
```

  Plus a proptest `decode_never_panics_and_roundtrips`: arbitrary bytes into `decode`; on `Ok`, encode and decode again, and the result is equal.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-utp`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test -p fbt-utp && bash dev/check-pure-rust.sh`: PASS.
- [ ] **Step 5: Commit.** `feat(utp): BEP 29 packet codec with selective ack` No trailers or attribution of any kind.

---

### Task 6: LEDBAT, RTT and the delay history

**Files:**
- Create: `crates/fbt-utp/src/{ledbat.rs,rtt.rs}`.

**Interfaces:**
- Produces (pure, time passed in). The modules `ledbat` and `rtt` are private (`mod ledbat; mod rtt;`) and their items are `pub`, used crate-internally by Task 7. Nothing is re-exported: no fuzz target uses LEDBAT directly, and `pub(crate)` items re-exported as `pub` do not compile (E0364).
  - Constants:
    - `pub const TARGET_DELAY: Duration = Duration::from_millis(100);`
    - `pub const MAX_CWND_INCREASE_PER_RTT: f64 = 3000.0;` (bytes, BEP 29)
    - `pub const MIN_CWND_PACKETS: u32 = 2;`
    - `pub const MIN_RTO: Duration = Duration::from_millis(500);`, `INITIAL_RTO: Duration = Duration::from_secs(1);` and `MAX_RTO: Duration = Duration::from_secs(60);`
  - `pub struct DelayHistory`: the base delay is the minimum one-way delay sample over the last two one-minute buckets. Its methods are `add_sample(us: u32, now: Instant)`, `base() -> Option<u32>` and `delay(&self, sample_us: u32) -> u32` (`sample_us.wrapping_sub(base)`, 0 before any base). The bucket minimum also compares with wrapping order (`b.wrapping_sub(a) < 0x8000_0000` means `a` is earlier), so base `0xffff_fff0` survives a later sample `0x10`, and a wrapped clock never inflates the delay.
  - `pub struct Ledbat { cwnd: f64, mss: u32, history: DelayHistory }`, with:
    - `new(mss: u32) -> Ledbat`, starting at `cwnd = 2 * mss`;
    - `on_ack(&mut self, bytes_acked: u32, delay_sample_us: u32, flight: u32, now: Instant)`: `off_target = (TARGET − (sample − base)) / TARGET`, clamped to `[-1, 1]`; `cwnd += MAX_CWND_INCREASE_PER_RTT * off_target * bytes_acked / cwnd`; and `cwnd` does not grow while `flight < cwnd` (application-limited);
    - `on_loss(&mut self)`: halve the window, at least the floor, at most once per RTT (tracked by the caller);
    - `on_timeout(&mut self)`: `cwnd = mss`, the one place it may go below the floor;
    - `cwnd(&self) -> u32`, clamped to `[MIN_CWND_PACKETS * mss, u32::MAX]`.
  - `pub struct RttEstimator`: `on_sample(rtt: Duration)` (the first sample sets `rtt = s, var = s/2`; then `var += (|rtt − s| − var)/4` and `rtt += (s − rtt)/8`), `rto() -> Duration` = `max(rtt + 4·var, MIN_RTO)` capped at `MAX_RTO` (`INITIAL_RTO` before any sample), and `backoff()`, which doubles the RTO up to `MAX_RTO` until the next sample.

- [ ] **Step 1: Write failing tests** (unit, in each file)
  - `ledbat_grows_below_target_and_shrinks_above`: base 10 ms, samples at 10 ms; 100 full acks of 1400 bytes grow `cwnd` monotonically. Samples at 250 ms then shrink it, never below `2 * mss`.
  - `growth_is_capped_per_rtt`: acking exactly one window at zero delay grows `cwnd` by at most 3000 bytes.
  - `no_growth_when_application_limited`
  - `timeout_drops_to_one_packet_then_recovers`
  - `base_delay_forgets_after_two_minutes`: a 5 ms sample at t=0, then only 50 ms samples. At t=61 s the base is still 5 ms; at t=121 s it is 50 ms.
  - `wrapped_timestamps_do_not_inflate_delay`: after samples `0xffff_fff0` then `0x10`, `base()` is still `0xffff_fff0` and `delay(0x10)` is 32 µs.
  - `rto_follows_bep29_formula`: samples 100 ms ×8 put the RTO at 500 ms (the floor). One 2 s sample raises it. `backoff` twice from 1 s gives 4 s, and it never passes 60 s.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-utp`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `feat(utp): LEDBAT congestion control and RTT estimation` No trailers or attribution of any kind.

---

### Task 7: uTP connection state machine (sans-IO) and simulation

**Files:**
- Create: `crates/fbt-utp/src/conn.rs`, `crates/fbt-utp/tests/sim.rs`. Dev-deps: `tokio` (`rt`, `macros`, `time`, `test-util`), `rand`, `sha1`.

**Interfaces:**
- Consumes: Tasks 5 and 6.
- Produces (`#[doc(hidden)] pub mod conn`; `tests/sim.rs` and the fuzz crate are other crates, so everything they use is `pub` inside this hidden module):
  - **Clock.** The sans-IO core uses `std::time::Instant` (tokio is a dev-dependency only in this task). `tests/sim.rs` drives it from paused tokio time with `tokio::time::Instant::now().into_std()`.
  - `#[derive(Debug, Clone)] #[non_exhaustive] pub struct UtpConfig`, with these fields (all `pub`, so Task 8's tests and the session can set them) and defaults (the type is re-exported from the crate root and also used by Task 8):
    - `recv_buffer: usize = 1 MiB`, `send_buffer: usize = 1 MiB`
    - `max_payload_v4: usize = 1400`, `max_payload_v6: usize = 1200`
    - `connect_timeout: Duration = 7 s`: SYN at 0, then retries at 1 s and 3 s, then `ConnectTimeout` at 7 s
    - `idle_timeout: Duration = 60 s`, `keepalive: Duration = 29 s`
    - `max_retransmits: u32 = 6`
    - `max_connections: usize = 1000`, `max_pending_accept: usize = 64` (Task 8)
  - `pub struct Connection` (inside the hidden module, not re-exported from the root), with:
    - `pub fn connect(cfg: &UtpConfig, peer: SocketAddr, recv_id: u16, now: Instant) -> Connection` (`send_id = recv_id.wrapping_add(1)`; a SYN is queued);
    - `pub fn accept(cfg: &UtpConfig, peer: SocketAddr, syn: &Packet, now: Instant) -> Connection` (`recv_id = syn.conn_id + 1`, `send_id = syn.conn_id`; a STATE is queued);
    - `pub fn on_packet(&mut self, p: &Packet, now: Instant)`;
    - `pub fn on_timer(&mut self, now: Instant)`;
    - `pub fn poll_transmit(&mut self, now: Instant) -> Option<Vec<u8>>` (encoded datagrams, called until `None`);
    - `pub fn next_deadline(&self) -> Option<Instant>`;
    - `pub fn write(&mut self, data: &[u8]) -> usize` (accepts up to `send_buffer` minus what is buffered; 0 means full);
    - `pub fn read(&mut self, out: &mut [u8]) -> usize`;
    - `pub fn close(&mut self)` (FIN after the queued data);
    - `pub fn state(&self) -> ConnState`;
    - `pub fn error(&self) -> Option<UtpError>`;
    - `#[doc(hidden)] pub fn buffered_bytes(&self) -> usize` (out-of-order plus unread in-order receive bytes), for the sim and the `utp_conn` fuzz target (Task 26 consumes it).

    `ConnState` is `{ SynSent, Connected, FinSent, Closed }`.
  - `#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)] pub enum UtpError { ConnectTimeout, Reset, TimedOut, Closed, TooManyConnections, Io(String) }` (crate root).

Behavior:
- **Clock.** Timestamps are microseconds since the connection's creation, wrapping `u32`. `timestamp_diff_us` echoes our receive-time minus the remote timestamp, which is the delay sample the remote's LEDBAT uses.
- **Send path.** A packet is sent only while `in_flight + len <= min(cwnd, peer_wnd)`. Payload size is `max_payload_v4` or `max_payload_v6` by the peer's family. With nothing in flight and `peer_wnd == 0`, a window probe goes out every RTO.
- **Acks.** `ack_nr` plus SACK free acknowledged packets. RTT is sampled only from packets sent once (Karn's rule).
- **Loss.**
  - 3 duplicate acks, or a SACK showing at least 3 later packets, retransmit the oldest unacked packet and call `on_loss` (once per RTT).
  - RTO expiry retransmits it, calls `on_timeout` and `backoff`.
  - `max_retransmits` consecutive timeouts give `TimedOut`.
- **Receive.**
  - Packets with `seq_nr` up to `ack_nr + recv_buffer / min_payload` are buffered out of order (bounded by `recv_buffer` bytes in total); anything further is dropped.
  - Duplicates are acked and dropped. The advertised `wnd_size` is the free receive space.
  - Every data packet is acked, delayed at most 100 ms or every second packet, with a SACK when there are holes.
- **Close and errors.**
  - FIN: the remote's FIN `seq_nr` ends the stream once everything before it has arrived (reads then return 0). Our FIN is retransmitted like data.
  - `ST_RESET` gives `Reset`.
  - No packet *received* for `idle_timeout` gives `TimedOut`; a keepalive (STATE) goes out after `keepalive` of send silence.
  - **A zero-window peer that keeps acking is not closed here.** A peer that advertises `wnd_size = 0` but still sends packets (acks of our probes, keepalives) stays connected: an fbt receiver held back by its download rate limit (Task 16) looks exactly like that, and closing it would drop every throttled uTP connection after `idle_timeout`. A peer that never reads is left to the BitTorrent layer (request timeout, snubbing), and memory stays bounded by `send_buffer` × `max_connections`.

- [ ] **Step 1: Write failing tests**
  - Unit tests in `conn.rs`, driving two `Connection`s by hand:
    - `handshake_ids_follow_bep29`
    - `syn_retries_then_connect_timeout`: no answer gives SYNs at 0, 1 and 3 s and `ConnectTimeout` at 7 s.
    - `in_order_data_roundtrip`
    - `three_dup_acks_fast_retransmit`
    - `rto_retransmit_and_backoff`
    - `fin_after_data_reads_eof`
    - `reset_sets_error`
    - `far_ahead_and_duplicate_packets_are_bounded`: 10,000 packets with seq in `ack_nr+2..` and 1400-byte payloads keep `buffered_bytes()` ≤ `recv_buffer`. 1000 duplicates buffer nothing.
    - `zero_window_peer_times_out`: the peer advertises `wnd_size = 0`, acks our first probes, then stops sending anything. While it acks, we send at most one probe per RTO and stay connected; `write` stops accepting at `send_buffer`; once it goes silent, `TimedOut` comes by `idle_timeout` after its last packet.
    - `zero_window_peer_that_keeps_acking_stays_connected`: the peer acks every probe with `wnd_size = 0` for 3 × `idle_timeout`; the state stays `Connected`, and when the window reopens the buffered data flows.
    - `advertised_huge_window_is_capped_by_cwnd`
  - Simulation in `tests/sim.rs`, under `#[tokio::test(start_paused = true)]`:
    - `SimNet` links two connections with a seeded `StdRng`. Each datagram gets a delay of 20 ms ± 10 ms, a loss probability and a duplicate probability, and may be reordered.
    - `transfer_survives_loss_reorder_and_duplication`: 4 MiB each way at 10 % loss and 5 % duplication arrive intact (SHA-1 compared), for seeds 1..=10.
    - `ledbat_backs_off_under_queueing_delay`: a link with a 300 ms bottleneck queue ends with a smaller average `cwnd` than a link without one.
    - `bulk_transfer_completes_in_bounded_time`: 8 MiB at 0 % loss and 20 ms RTT completes in under 30 s of simulated time.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-utp`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test -p fbt-utp && bash dev/check-pure-rust.sh`: PASS, deterministic over 20 reruns (`for i in $(seq 20); do cargo test -p fbt-utp --test sim -q || break; done`).
- [ ] **Step 5: Commit.** `feat(utp): sans-IO connection with retransmission, SACK, FIN and LEDBAT` No trailers or attribution of any kind.

---

### Task 8: uTP socket runtime

**Files:**
- Create: `crates/fbt-utp/src/{socket.rs,stream.rs}`, `crates/fbt-utp/tests/socket.rs`. Deps: `tokio` (`net`, `rt`, `time`, `sync`, `io-util`), `rand`. Dev-dep: `fbt-net` (path; no cycle, fbt-net never depends on fbt-utp).

**Interfaces:**
- Consumes: `fbt_core::udp::{DatagramSender, DatagramHandler}` (Task 4); `Connection` and `UtpConfig` (Task 7).
- Produces (crate root):
  - `#[derive(Debug, Clone)] pub struct UtpSocket`, with:
    - `pub fn new(sender: Arc<dyn DatagramSender>, cfg: UtpConfig) -> UtpSocket`: shared mode; datagrams arrive through `handler()`;
    - `pub async fn bind(addr: SocketAddr, cfg: UtpConfig) -> io::Result<UtpSocket>`: standalone mode, which owns a tokio `UdpSocket` and its receive task;
    - `pub fn handler(&self) -> Arc<dyn DatagramHandler>`;
    - `pub fn local_addr(&self) -> SocketAddr`;
    - `pub async fn connect(&self, to: SocketAddr) -> Result<UtpStream, UtpError>`;
    - `pub async fn accept(&self) -> Result<(UtpStream, SocketAddr), UtpError>`;
    - `pub fn set_accept_filter(&self, f: Option<Arc<dyn Fn(SocketAddr) -> bool + Send + Sync>>)`: a SYN whose source fails the filter is dropped silently (never acked), so the dialer sees `ConnectTimeout`. One filter per socket; Task 12 (reject-all when incoming uTP is off) and Task 19 (NAT emulation) compose theirs into one closure in `net.rs`;
    - `pub fn num_connections(&self) -> usize`;
    - `pub async fn shutdown(&self)`: resets every connection; idempotent.
  - `#[derive(Debug)] pub struct UtpStream`: `AsyncRead + AsyncWrite + Send + Unpin`, plus `peer_addr(&self) -> SocketAddr`. `poll_shutdown` sends FIN and waits for its ack (bounded by `idle_timeout`). Dropping the stream sends FIN if still connected.

Runtime layout:
- **Table.** A `Mutex<HashMap<(SocketAddr, u16), Arc<Mutex<ConnSlot>>>>` keyed by (peer, `recv_id`). A `ConnSlot` holds the `Connection` and the read and write wakers.
- **Receive path.** `handle()` decodes the packet, looks up the slot, calls `on_packet` and flushes `poll_transmit` through the sender, all synchronously.
- **Incoming SYNs.**
  - An unknown SYN creates a connection only if `num_connections < max_connections`, the accept queue has fewer than `max_pending_accept` entries and the filter passes. Otherwise it is dropped (no reset, so a flood costs nothing).
  - A pending (not yet accepted) connection that dies (timeout, reset) is removed from the accept queue and the table, giving its place back.
  - A duplicate SYN for an existing slot is re-acked.
- **Unknown traffic.** A non-SYN packet for an unknown id gets one `ST_RESET` reply, rate-limited to 10 per second per socket.
- **Timers.** One timer task sleeps until the earliest `next_deadline` across connections and runs `on_timer`. It is woken through a `Notify` when a deadline moves earlier.
- **Ids.** `recv_id` is random and unique per peer.
- **Errors.** A connection in error wakes its stream; the stream's next poll returns `io::Error` (`ConnectionReset` for `Reset`, `TimedOut` for `TimedOut`), and the slot is removed.

- [ ] **Step 1: Write failing tests** in `tests/socket.rs` (real loopback UDP, standalone `bind`)
  - `connect_accept_echo`
  - `shared_mode_through_a_handler`: a `SharedUdpSocket` from `fbt-net` as a dev-dependency, with the uTP handler registered; echo works and the DHT kind stays unaffected.
  - `large_transfer_both_directions`: 16 MiB each way, hashes equal.
  - `connect_to_silent_port_times_out`: with `connect_timeout` 300 ms.
  - `syn_flood_is_bounded`: 10,000 forged SYNs from one raw socket with random ids, at `max_pending_accept: 64` and `idle_timeout`/`connect_timeout` about 500 ms. Afterwards `num_connections() <= 64`, and once the pending entries time out (and leave the accept queue) a real `connect` from a fresh socket succeeds.
  - `reset_from_wrong_address_is_ignored`: an `ST_RESET` with the right connection id from another socket leaves the stream working.
  - `accept_filter_drops_unsolicited_syns`
  - `drop_sends_fin_and_peer_reads_eof`
  - `shutdown_is_idempotent_and_wakes_streams`
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-utp`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test -p fbt-utp && bash dev/check-pure-rust.sh`: PASS on all three OSes.
- [ ] **Step 5: Commit.** `feat(utp): UtpSocket and UtpStream over a shared or owned UDP socket` No trailers or attribution of any kind.

---

### Task 9: DHT on shared sockets; bootstrap spacing

**Files:**
- Modify: `crates/fbt-dht/src/{node.rs,lib.rs}`, `crates/fbt-dht/Cargo.toml` (none new: `fbt-core` is already a dependency), `crates/fbt-dht/tests/swarm.rs`.

**Interfaces:**
- Consumes: `fbt_core::udp::{DatagramSender, DatagramHandler}`.
- Produces:
  - `pub async fn Dht::spawn_on(cfg: DhtConfig, sockets: Vec<Arc<dyn DatagramSender>>) -> Result<(Dht, DhtInput), DhtError>`.
    - It ignores `bind_v4`/`bind_v6` and takes one socket per family from `local_addr()`.
    - No sockets gives `NoSocket`; two of one family give a new `DhtError::DuplicateFamily` (breaking; `InvalidState` stays for unreadable saved state; noted in CHANGELOG by Task 29).
    - `local_addrs()` reports the senders' addresses.
  - `#[derive(Debug, Clone)] pub struct DhtInput`, which implements `DatagramHandler`. `handle` copies the datagram (cut at `MAX_DATAGRAM + 1`, so oversize is still detected and dropped) into a bounded channel of `pub const DHT_INPUT_QUEUE: usize = 1024` datagrams. A full channel drops the datagram. The actor's `select!` reads that channel in place of its own sockets. `spawn` (standalone) is unchanged and shares all the code past the socket.
  - **Bootstrap spacing** (PR #4's deferral): the next attempt is due `backoff(...)` after the *previous attempt finished*, not after it started. `start_bootstrap` no longer sets `boot_due`; the attempt's completion does.
  - `pub struct ExternalIpVotes` (renamed from the crate-internal `ExternalIp`, now public for the session's own votes in Task 24), with `new() -> Self` and `vote(&mut self, reporter: IpAddr, ip: IpAddr) -> Option<IpAddr>`, behavior unchanged.

- [ ] **Step 1: Write failing tests**
  - `node.rs`:
    - `spawn_on_answers_ping_through_the_shared_sender`: the test's `DatagramSender` wraps a loopback `std::net::UdpSocket`, and a pump task feeds received datagrams into `DhtInput`.
    - `full_input_queue_drops_instead_of_blocking`: 5000 datagrams pushed synchronously return at once, and the node keeps answering.
  - `tests/swarm.rs`:
    - `mixed_standalone_and_shared_nodes_converge`: 10 nodes, half on `spawn_on`.
    - `long_bootstrap_attempts_are_spaced_from_their_end`: a bootstrap node that answers each `find_node` after 300 ms, with base 50 ms and max 400 ms. The gap between the end of one attempt and the first datagram of the next is at least the backoff (within 20 ms).
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-dht`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test -p fbt-dht`: PASS, including every milestone 2 test.
- [ ] **Step 5: Commit.** `feat(dht): run on externally owned sockets; space bootstrap attempts from their end` No trailers or attribution of any kind.

---

### Task 10: UDP tracker announces through shared sockets

**Files:**
- Modify: `crates/fbt-tracker/src/{lib.rs,udp.rs}`.

**Interfaces:**
- Consumes: `fbt_core::udp::{DatagramSender, DatagramHandler}`.
- Produces:
  - `pub fn TrackerClient::with_udp_sockets(cfg: TrackerConfig, sockets: Vec<Arc<dyn DatagramSender>>) -> Result<TrackerClient, TrackerError>`. The UDP client sends through the socket of the tracker's family. A family without a shared socket falls back to today's lazily bound ephemeral `Sock`.
  - `pub fn TrackerClient::udp_handler(&self) -> Arc<dyn DatagramHandler>`. It does today's `recv_loop` matching, synchronously: the transaction id at offset 4 must be pending *and* the source must be the address the request went to. Otherwise the datagram is dropped.
  - `TrackerClient::new` is unchanged (standalone).

- [ ] **Step 1: Write failing tests** in `udp.rs`'s test module, next to the existing UDP fake (`async fn fake`); `tests.rs` holds only the HTTP `serve` helper
  - `udp_announce_through_shared_socket`: the announce succeeds, and the fake tracker saw the request come from the shared socket's port.
  - `shared_handler_ignores_wrong_source_and_unknown_tid`
  - `family_without_shared_socket_uses_its_own`: a shared IPv4 socket only, and an IPv6 tracker on `[::1]` (skipped without IPv6) still answers.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-tracker`
- [ ] **Step 3: Implement.** The pending map and `Waiter` are shared by both modes; only "where it is sent from" and "who calls the matcher" differ.
- [ ] **Step 4: Run.** `cargo test -p fbt-tracker`: PASS.
- [ ] **Step 5: Commit.** `feat(tracker): UDP announces through shared sockets` No trailers or attribution of any kind.

---

### Task 11: listen addresses and shared UDP sockets in the session

**Files:**
- Create: `crates/fbt-session/src/net.rs`.
- Modify: `crates/fbt-session/src/{settings.rs,session.rs,listener.rs,event.rs,error.rs,lib.rs}`, `crates/fbt-session/src/torrent/data.rs` (the `Shared { .. }` literal in `torrent_finished_sees_the_final_status` gains the new fields), `crates/fbt-session/Cargo.toml` (deps `fbt-net`, `fbt-utp`), `crates/fbt-session/tests/{support/mod.rs,lifecycle.rs,dht.rs,trackers.rs}` (`tests/dht.rs` also drops its `s.dht_ipv6 = false`), every test that sets `listen_addr`, and `crates/fbt-example/src/main.rs` (minimal port, see below).

**Interfaces:**
- Consumes: Tasks 4, 9 and 10.
- Produces:
  - Settings:
    - `Settings.listen_addr` is replaced by `pub listen_addrs: Vec<SocketAddr>`, default `[0.0.0.0:6881, [::]:6881]` (breaking).
    - `validate` refuses an empty list and duplicate entries.
    - `Settings::dht_ipv6` is removed: the DHT runs on each family that has a shared socket (breaking).
  - **Binding**, in `net.rs`: `pub(crate) async fn bind_all(settings: &Settings) -> Result<(Vec<Endpoint>, Vec<ListenError>), SessionError>` and `pub(crate) struct Endpoint { pub addr: SocketAddr, pub tcp: TcpListener, pub udp: Option<SharedUdpSocket> }`.
    - IPv6 TCP listeners are bound through `socket2` with `set_only_v6(true)` (then converted to a tokio `TcpListener`), so `[::]:6881` does not collide with `0.0.0.0:6881` on dual-stack Linux and macOS. Without it, the default would always report the IPv6 address in `listen_errors()` and serve no IPv6.
    - For each address it binds TCP first, then UDP on the TCP's actual port.
    - With port 0 and the UDP bind failing, it retries both with a fresh ephemeral port, up to `const BIND_ATTEMPTS: usize = 8`.
    - With a fixed port and the UDP bind failing, the address keeps TCP only (`udp: None`), with a warning and a `ListenError` with `udp_only: true`.
    - An address whose TCP bind fails is skipped with a `ListenError`. Endpoints that bind nothing give `SessionError::Bind` (the first error).
  - `#[derive(Debug, Clone, PartialEq, Eq)] #[non_exhaustive] pub struct ListenError { pub addr: SocketAddr, pub udp_only: bool, pub message: String }`. `udp_only` is true when TCP bound and only the UDP side failed. `Session::listen_errors(&self) -> Vec<ListenError>` returns them. They are not events, because binding happens inside `Session::new`, before anyone can subscribe.
  - `Session::local_addrs(&self) -> Vec<SocketAddr>` (TCP, in settings order). `Session::local_addr()` stays as the first of them. Every endpoint runs its own accept loop: `listener::spawn` is called once per listener.
  - **Shared listener state** (Tasks 12 and 13 consume it). `pub(crate) struct ListenerState` is created once per session (held in `Shared` or passed to every `spawn`), not inside `listener::spawn`. It holds the pending-handshake `Arc<Semaphore>` (`max_peers_global` permits) for every accept loop together, so N endpoints still allow only `max_peers_global` pending handshakes. `listener::spawn(listener, state: Arc<ListenerState>, session, shared)` takes it. Task 12's uTP accept loops take their permit from the same semaphore; Task 13 adds its per-IP map here.
  - Hidden `#[doc(hidden)] pub fn Session::demux_stats(&self) -> Vec<(SocketAddr, DemuxStats)>`, one per endpoint with a UDP socket, for tests.
  - **DHT on the shared sockets.**
    - The DHT runs through `Dht::spawn_on` with every endpoint's UDP sender, at most one per family (the first one wins). `DhtInput` is registered as the `PacketKind::Dht` handler only on those sockets, so replies leave from the address the query reached; a second endpoint of the same family has no DHT handler (its DHT datagrams count as dropped).
    - Without any shared socket, it falls back to standalone `Dht::spawn` on an ephemeral port per family. This keeps milestone 2's `dht_port_taken_falls_back` passing.
    - `Shared.dht_addrs` is the DHT's local addresses, as before, so `Port` messages keep carrying the right port.
  - **Trackers on the shared sockets.** `TrackerClient::with_udp_sockets` gets the endpoints' UDP senders. `PacketKind::Tracker` goes to `tracker.udp_handler()` on every UDP socket.
  - `test_settings()` sets `listen_addrs = vec![127.0.0.1:0]`.
  - **Minimal `fbt-example` port** (so `cargo test --workspace` compiles from here on; Task 25 adds the real flags). `main.rs`'s two `settings.listen_addr.set_port(port)` sites (download/seed and dht-node) become `settings.listen_addrs = vec![SocketAddr::from(([0, 0, 0, 0], port))]`.

- [ ] **Step 1: Write failing tests**
  - `tests/lifecycle.rs`:
    - `listens_on_every_address`: skipped without IPv6. `[127.0.0.1:0, [::1]:0]` gives two `local_addrs`, and a peer connects through each.
    - `ipv6_bind_failure_is_reported_and_skipped`: the second address is `[::1]:<port held by a std TcpListener>`. The session starts, `local_addrs().len() == 1` and `listen_errors()` names that address.
    - `unspecified_v4_and_v6_share_a_port`: skipped without IPv6. `[0.0.0.0:p, [::]:p]` (p found free on both) gives two `local_addrs` and empty `listen_errors()`.
    - `empty_listen_addrs_is_invalid`
    - `nothing_binds_is_a_bind_error`
  - `tests/dht.rs`, extending the existing tests rather than repeating them:
    - `dht_on_listener_port_when_free` additionally asserts that the DHT runs on the shared socket: after a ping from a loopback DHT node, `demux_stats()[0].1.dht > 0`.
    - `dht_port_taken_falls_back` (switched to `listen_addrs`) additionally asserts `listen_errors()` holds exactly one entry for that address with `udp_only: true`.
  - `tests/trackers.rs`: `udp_tracker_sees_the_listen_port_as_source`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace && bash dev/check-pure-rust.sh`: PASS, with every milestone 1 and 2 test updated only for `listen_addrs` and the removed `dht_ipv6`.
- [ ] **Step 5: Commit.** `feat(session): multiple listen addresses with shared UDP sockets for the DHT and UDP trackers` No trailers or attribution of any kind.

---

### Task 12: uTP peer connections and transport choice

**Files:**
- Create: `crates/fbt-session/src/peer/transport.rs`, `crates/fbt-session/tests/utp.rs`.
- Modify: `crates/fbt-session/src/{net.rs,settings.rs,event.rs,listener.rs,lib.rs}`, `crates/fbt-session/src/peer/conn/mod.rs`, `crates/fbt-session/src/peer/messages.rs` (`PeerEvent::Disconnected { connect_failed }`), `crates/fbt-session/src/torrent/{swarm.rs,pex.rs,peers.rs}` (`peers.rs` emits `Event::PeerConnected`).

**Interfaces:**
- Consumes: `UtpSocket`, `UtpStream`, `UtpError` and `set_accept_filter` (Task 8); the `Endpoint`s and the shared `ListenerState` of Task 11.
- Produces:
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)] #[non_exhaustive] pub enum Transport { Tcp, Utp }` (public).
  - `pub(crate) enum PeerTransport { Tcp(TcpStream), Utp(UtpStream) }`, implementing `AsyncRead + AsyncWrite + Unpin` by delegation, with `kind(&self) -> Transport`. `PeerStream` becomes `MseStream<PeerTransport>`.
  - **Endpoints.** Each endpoint with a UDP socket gets a `UtpSocket::new(udp.sender(), cfg)` registered as its `PacketKind::Utp` handler, and an accept loop. Accepted uTP streams go through the same `listener::route` as TCP, with the same handshake deadline and a permit from the session-wide `ListenerState` semaphore (Task 11), not a semaphore of their own.
  - Settings (libtorrent's names):
    - `enable_outgoing_tcp`, `enable_incoming_tcp`, `enable_outgoing_utp`, `enable_incoming_utp`: all `bool = true`. `validate` refuses all four off.
    - Hidden: `utp_connect_timeout: Duration = 7 s` (tests shorten it).
    - **Incoming uTP off** installs a reject-all accept filter on every `UtpSocket` (the socket still exists for outgoing uTP), and no accept loop runs. Unaccepted SYNs are never acked, so they really time out at the dialer as `ConnectTimeout`, which triggers its TCP fallback. Without the filter, Task 8's socket would ack the SYN while its accept queue has room, and the dialer would see a connected stream and a handshake timeout instead. The filter is built in `net.rs` as one closure, so Task 19 can compose its NAT-emulation check into it.
  - **Outgoing choice** in `swarm.rs`: `fn first_transport(&self, addr: &SocketAddr) -> Option<Transport>`.
    - uTP first when outgoing uTP is enabled, a UDP socket of the peer's family exists, and the address is not in `Retries::tcp_only`.
    - Otherwise TCP when it is enabled.
    - `spawn_outgoing` takes `transport: Transport` and `utp: Option<UtpSocket>`.
  - **Fallback.** A *connect* failure (TCP refused or timed out, `UtpError::ConnectTimeout`/`Reset` during connect) on the first transport is reported as `DisconnectReason::Io(..)` with `PeerEvent::Disconnected { connect_failed: Some(Transport) }`, a new field. The address is then retried once at once on the other transport, when that one is enabled.
    - `Retries::transport_failed(addr, t) -> bool` returns true the first time, bounded like the plaintext retry set.
    - A uTP connect failure followed by a TCP success puts the address into `Retries::tcp_only`, which is bounded by `MAX_NEVER`.
  - `Event::PeerConnected` gains `transport: Transport` and `encrypted: bool` (breaking; `encrypted` was already carried internally).
  - `PexFlags::UTP` is set on addresses connected over uTP, and an incoming PEX `UTP` flag is remembered for `first_transport`. `Known.set` becomes a map from address to a per-address record (here `utp: bool`), so Task 19 can add its PEX source and `HOLEPUNCH` flag to the same record.

- [ ] **Step 1: Write failing tests** in `tests/utp.rs`
  - `utp_only_download`: both sessions have TCP off in both directions. The download completes, and `PeerConnected.transport == Transport::Utp`.
  - `utp_preferred_by_default`: default transports give `Transport::Utp` on both ends.
  - `utp_connect_failure_falls_back_to_tcp`: the seed has `enable_incoming_utp: false`, and the leecher has `utp_connect_timeout` 300 ms. It completes over `Transport::Tcp`, and a second `add_peer` dials TCP first (one `PeerDisconnected` in total).
  - `tcp_only_peer_is_dialed_over_tcp_when_utp_disabled`
  - `encrypted_over_utp`: `Forced` on both ends gives `encrypted: true`, `Transport::Utp`.
  - `all_transports_off_is_invalid`
  - `mixed_swarm_tcp_and_utp`: three leechers (uTP-only, TCP-only, default) all finish from one default seed.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test utp`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS. Every existing swarm test now runs over uTP by default. A failure there is a uTP bug to fix in `fbt-utp` with a regression test, not a reason to change the default.
- [ ] **Step 5: Commit.** `feat(session): uTP peer connections, transport choice with fallback, transport in events` No trailers or attribution of any kind.

---

### Task 13: milestone 2 debts: per-IP MSE cap, RC4 negotiation, PEX tick cost, metadata pieces kept

**Files:**
- Modify: `crates/fbt-session/src/listener.rs`, `crates/fbt-session/src/torrent/{pex.rs,metadata.rs,magnet.rs,actor.rs,swarm.rs}` (`pex_live`, `peers_epoch` and `next_pex_due` are actor fields; `on_connected`/`on_disconnected` live in `swarm.rs`), `crates/fbt-session/tests/encryption.rs`. Every PEX test here is a unit test in `src/torrent/pex.rs`; `tests/pex.rs` is not touched.

**Interfaces:**
- Consumes: the session-wide `ListenerState` (Task 11), shared by the TCP and uTP accept loops of every endpoint (Task 12).
- Produces:
  - **Per-IP handshake cap.** `ListenerState` gains `Mutex<HashMap<IpAddr, usize>>` of handshakes in progress per source IP, capped at `const MAX_PENDING_PER_IP: usize = 8`. It is one map for every accept loop (TCP and uTP, all endpoints), so the cap is not split across loops. A further connection from that IP is dropped before reading anything. The count goes down on every exit path (an RAII guard). The map holds at most one entry per pending handshake, so it is bounded by the shared semaphore.
  - **PEX tick cost.** `pex_tick` builds the connectable list at most once per tick, and only when at least one PEX peer is due. The list is cached in the actor as `pex_live: Option<(u64, Vec<(SocketAddr, PexFlags)>)>`, keyed by a `peers_epoch: u64` that `on_connected`/`on_disconnected` bump. The actor also keeps `next_pex_due: Option<Instant>`, the earliest `last_sent + interval` (recomputed after each tick), and returns early before it.
  - **Metadata pieces kept on retire.** `MetadataFetch` keeps the delivered pieces of a retired (not banned) source in `assembly: Option<Assembly { size: u32, pieces: Vec<Option<Bytes>> }>`. The next source of the same `size` requests only the missing pieces.
    - If a buffer assembled from more than one source fails the hash, nobody is banned. The assembly is cleared, and the fetch goes on single-source, as today, so a later failure bans that one source.
    - A source with a different `size` never mixes with the assembly.

- [ ] **Step 1: Write failing tests**
  - `tests/encryption.rs`:
    - `preferred_to_preferred_negotiates_rc4`: `PeerConnected.encrypted == true` on both sides.
    - `pending_mse_handshakes_are_capped_per_ip`: 20 raw TCP sockets from 127.0.0.1 send 96 random bytes and stall. A 21st connection's real MSE handshake (a sockets-only client using `fbt_wire::initiate`) is refused until the stalled ones time out, while a connection from `127.0.0.2` (skipped where that does not route) is accepted at once.
  - `src/torrent/pex.rs` unit tests:
    - `tick_rebuilds_nothing_when_nobody_is_due`: with a counter behind `#[cfg(test)]`, 100 ticks with no peer due build the live list 0 times.
    - `tick_builds_once_for_many_due_peers`
  - `metadata.rs` unit tests:
    - `retired_source_pieces_are_kept`: source A delivers pieces 0 and 1 of 4 and times out. Source B is asked only for 2 and 3, and the assembled buffer verifies.
    - `mixed_assembly_failure_bans_nobody_then_goes_single_source`
    - `different_size_source_does_not_mix`
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `fix(session): per-IP pending handshake cap, cheaper PEX ticks, keep metadata pieces from retired sources` No trailers or attribution of any kind.

---

### Task 14: bounded disk pool, check semaphore and `FsStorage` contention

PR #1 deferred all three.

**Files:**
- Create: `crates/fbt-session/src/disk.rs`.
- Modify: `crates/fbt-session/src/{settings.rs,session.rs,status.rs,lib.rs}`, `crates/fbt-session/src/storage/{fs.rs,mod.rs}`, `crates/fbt-session/src/torrent/{check.rs,data.rs,serve.rs,actor.rs,magnet.rs,mod.rs}` (`mod.rs` if `torrent::same_state` needs the new state), `crates/fbt-session/tests/lifecycle.rs`.

**Interfaces:**
- Produces:
  - Settings:
    - `pub disk_threads: usize = 8`, 1 to `MAX_DISK_THREADS = 64`;
    - `pub max_active_checks: usize = 1`, 1 to 64 (libtorrent's `active_checking`).
  - `pub(crate) struct DiskPool`, in `Shared`:
    - `pub fn spawn<T: Send + 'static>(&self, jobs: &mut JoinSet<T>, f: impl FnOnce() -> T + Send + 'static)` runs `f` on `spawn_blocking` after taking a `disk_threads` semaphore permit inside an async wrapper. Every actor job that touches storage goes through it.
    - `pub fn check_permit(&self) -> Arc<Semaphore>` is the `max_active_checks` semaphore.
  - **Checks.** `start_check` enters a new `Phase::QueuedForCheck { cancel }`. A spawned task awaits a check permit, then runs the check (holding the permit) through the pool. Cancelling while queued drops the waiter.
  - `#[non_exhaustive] TorrentState::QueuedForChecking` (public); `state()` maps the queued phase to it.
  - **`FsStorage`.**
    - `State` holds `Arc<File>` handles, and the lock is held only to look up or open a handle; `read_exact_at`/`write_all_at` run outside it.
    - Evicting a writable handle no longer syncs. It records the file index in `unsynced: HashSet<usize>`, and `flush()` reopens and `sync_data`s those files, plus the open writable ones. The milestone 1 flush-coverage test keeps passing with the reopen.
    - **The file path on errors** (PR #1's `FileError.path` deferral; `fail()` today always sends `path: None`). `FsStorage` wraps every I/O error as `io::Error::new(e.kind(), StorageError { path, source })`, with `pub struct StorageError { pub path: PathBuf, pub source: io::Error }` and `pub fn error_path(e: &io::Error) -> Option<&Path>` (a downcast through `get_ref`). Both are made reachable: `storage/mod.rs` does `pub use fs::{StorageError, error_path}` (or defines them there), and `lib.rs` re-exports `StorageError` at the `fbt_session` root, which Task 25's `fbt` re-export consumes. `Actor::fail` fills `Event::FileError.path` from it. Test in `fs.rs`: `errors_name_the_file`.

- [ ] **Step 1: Write failing tests**
  - `tests/lifecycle.rs`:
    - `checks_run_one_at_a_time`: three torrents on a `CountingStorage` that records the peak number of concurrent `read` calls during checks, with `max_active_checks: 1`. The peak is 1, all three reach `Seeding`, and while one checks the others report `QueuedForChecking`.
    - `queued_check_can_be_removed`
  - `disk.rs` unit test: `pool_never_exceeds_disk_threads`.
  - `fs.rs` unit tests:
    - `eviction_defers_sync_to_flush`: more files than `MAX_OPEN` are written. `synced_files()` is 0 before `flush` and covers every written file after it.
    - `parallel_reads_do_not_serialize`: 4 threads read different files of one `FsStorage`, with a `Barrier` inside a test hook proving two reads overlap.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `perf(session): bounded disk pool, one check at a time, file I/O outside the storage lock` No trailers or attribution of any kind.

---

### Task 15: write-back cache

**Files:**
- Create: `crates/fbt-session/src/storage/cache.rs`, `crates/fbt-session/tests/cache.rs`.
- Modify: `crates/fbt-session/src/{settings.rs,session.rs,lib.rs,event.rs}` (`event.rs`: the `PieceFinished` doc), `crates/fbt-session/src/storage/mod.rs`, `crates/fbt-session/src/torrent/{actor.rs,data.rs,serve.rs,commands.rs,peers.rs}`.

**Interfaces:**
- Consumes: `DiskPool` (Task 14).
- Produces:
  - Settings:
    - `pub cache_size: u64 = 64 MiB`. 0 means write-through: every verified piece is written at once and there is no read cache.
    - `pub cache_expiry: Duration = 30 s`, non-zero.
    - The documented floor: each downloading torrent may hold `const PROGRESS_FLOOR_PIECES: usize = 4` partial buffers beyond the budget, but only while `dirty + in-flight write bytes <= cache_size`. Above that, no new partial buffer is created for any torrent (floor included) until a `WroteRun` lands, so a slow disk cannot grow memory without bound.
  - `pub(crate) struct CacheBudget`, session-wide and atomic. It counts `partial`, `dirty` and `clean` bytes, with `try_reserve(kind, n) -> bool` and `release(kind, n)`.
  - `#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)] #[non_exhaustive] pub struct CacheStats { pub dirty: u64, pub clean: u64, pub partial: u64, pub limit: u64 }`, from `Session::cache_stats()`.
  - `pub(crate) struct PieceCache`, one per torrent, owned by the actor. It is pure: it plans the I/O, and the actor runs it.
    - `insert_dirty(piece, data: Arc<[u8]>, now)`
    - `get(piece) -> Option<Arc<[u8]>>` (dirty or clean)
    - `insert_clean(piece, data, now)`: LRU, only within the budget
    - `take_flush(reason: FlushReason, now) -> Vec<WriteRun>`
    - `written(run)`
    - `discard_all()`
    - `pub(crate) struct WriteRun { first: u32, pieces: Vec<(u32, Arc<[u8]>)> }`: contiguous pieces written as one `Storage::write`.
    - `pub(crate) enum FlushReason { All, Pressure, Expired }`
  - Flow changes in the actor:
    - **Hash pass.** A passing piece is `insert_dirty` and then at once `piece_passed` plus `Have`. `Job::Written` gives way to `Job::WroteRun { generation, first: u32, count: u32, result: io::Result<()> }`; the existing `Job::Flushed` stays for `Storage::flush`. The piece is reported as ours before it is on disk, and is served from memory. `Event::PieceFinished`'s doc changes from "passed its hash check and was written" to "passed its hash check" (it may not be on disk yet).
    - **Flush triggers.**
      - Pressure: `dirty > cache_size / 2` flushes the oldest runs until `dirty <= cache_size / 4`.
      - Expiry: a dirty piece older than `cache_expiry`, checked on the status tick.
      - `All`: `TorrentFinished`, pause, `remove(KeepData)`, shutdown and before `recheck`.
      - `remove(DeleteData)` calls `discard_all`.
      - Pause and shutdown wait for their flush jobs before replying. The existing `STOPPED_TIMEOUT` does not bound them: data comes first.
    - **Flush failure.** It goes through `fail()`, which emits `Event::FileError` with the path when the error names one (Task 14's `StorageError`), and stops in `TorrentState::Error`. The failed pieces' data is dropped; the next resume rechecks, as in milestone 1.
    - **Serving.** `on_request` reads from `cache.get` first. On a miss, the whole piece is read through the pool (when `piece_len <= 4 MiB`, else the block only) and inserted clean when the budget allows.
    - **Partial buffers.** `Loaded.bufs` reserve `Partial` from the budget when a piece's buffer is created. When that fails, the torrent may still use its `PROGRESS_FLOOR_PIECES` floor, but only while `dirty + in-flight writes <= cache_size` (`CacheBudget` tracks in-flight write bytes until the `WroteRun`). Otherwise the picker is not given new pieces for this torrent until a buffer frees or a write lands. Reaching that limit is the existing partial-cap eviction path (`partial_cap` now reads the budget).

- [ ] **Step 1: Write failing tests**
  - Unit tests in `cache.rs`:
    - `pressure_flushes_oldest_contiguous_runs`: pieces 1, 2, 3 and 7 are dirty. The runs are `[1..=3]` and `[7]`, oldest first.
    - `expiry_flushes_only_old_pieces`
    - `clean_entries_evicted_before_dirty_are_flushed`
    - `zero_size_is_write_through`
    - `budget_counts_all_three_kinds`
  - Integration tests in `tests/cache.rs`, with a `RecordingStorage` that logs every `write` and can fail on demand:
    - `cached_piece_is_served_before_it_is_written`: the storage delays writes 2 s. A second leecher gets a piece from the first leecher while the first's storage has not written it yet.
    - `writes_are_coalesced`: a sequential download records fewer `write` calls than pieces.
    - `shutdown_and_pause_flush_dirty_pieces`: right after `pause().await` returns, the storage holds every verified piece. The same holds after `shutdown().await`.
    - `remove_with_delete_discards_dirty_pieces`: no `write` after `remove(DeleteData)`.
    - `flush_failure_stops_the_torrent`: the storage fails the first write with `StorageFull`. `Event::FileError` arrives and the state is `Error`; after resume, a recheck runs.
    - `memory_stays_within_budget`: `cache_size` 4 MiB, a 64 MiB torrent and a 2 s write delay. Sampling `cache_stats()` every 50 ms never shows `dirty + clean + partial` above `limit + PROGRESS_FLOOR_PIECES * piece_len`, for the whole download (slow writes included).
  - Unit test in `cache.rs`: `progress_floor_stops_while_writes_lag`: with dirty plus in-flight bytes above `cache_size`, no floor buffer is granted; after `written(run)` it is.
    - `no_unverified_bytes_reach_storage`: a peer sends a corrupt piece, and the storage never sees its bytes.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test cache`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS.
- [ ] **Step 5: Commit.** `feat(session): bounded write-back cache with coalesced writes and a session-wide budget` No trailers or attribution of any kind.

---

### Task 16: rate limiting: global, per torrent, per peer class, mixed mode

**Files:**
- Create: `crates/fbt-session/src/bandwidth.rs`, `crates/fbt-session/tests/ratelimit.rs`.
- Modify: `crates/fbt-session/src/{settings.rs,session.rs,lib.rs}`, `crates/fbt-session/src/peer/conn/{mod.rs,state.rs,outbound.rs}`, `crates/fbt-session/src/peer/messages.rs`, `crates/fbt-session/src/torrent/{mod.rs,serve.rs,actor.rs,commands.rs,data.rs}` (`commands.rs` handles the new `Cmd::SetRateLimit`; `data.rs` updates the `Shared { .. }` test literal).

**Interfaces:**
- Consumes: Task 7's uTP idle rule (a receiver that stops reading but keeps acking stays connected); `PeerTransport::kind()` (Task 12) for the class bucket.
- Produces (public):
  - `#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)] #[non_exhaustive] pub struct RateLimit { pub download: u64, pub upload: u64 }`: bytes per second, 0 meaning unlimited. `RateLimit::new(download, upload)`.
  - `#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)] #[non_exhaustive] pub struct PeerClassLimits { pub tcp: RateLimit, pub utp: RateLimit, pub local: RateLimit }`.
  - `#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)] #[non_exhaustive] pub enum MixedMode { #[default] PeerProportional, PreferTcp }`.
  - Settings: `rate_limit: RateLimit`, `peer_class_limits: PeerClassLimits`, `rate_limit_local_peers: bool = false` and `mixed_mode: MixedMode`.
  - `AddTorrent.rate_limit: RateLimit`.
  - Runtime setters that apply within one second: `Session::set_rate_limit(&self, RateLimit)`, `Session::set_peer_class_limits(&self, PeerClassLimits)` and `TorrentHandle::set_rate_limit(&self, RateLimit) -> Result<(), SessionError>` (a new `Cmd::SetRateLimit` handled in `torrent/commands.rs`).
  - **Local peers and the CLI** (Task 25 consumes this). The local-peer exemption below means a limit has no effect on loopback or LAN peers unless `rate_limit_local_peers` is set, so every integration test that measures a global or torrent limit over loopback sets it, and Task 25's CLI sets it whenever a limit flag is given.
  - Crate-internal:
    - `pub(crate) struct TokenBucket { rate: u64, balance: i64, capacity: i64, last: Instant }`:
      - a new bucket starts at balance 0 (no free initial burst);
      - `set_rate(rate, now)`, where capacity is `max(rate, 16 KiB)`, saturating;
      - `consume(n: u64, now)`, which lets the balance go negative down to `-(i64::MAX/2)`, saturating;
      - `repaid_at(now) -> Option<Instant>`: when the debt is repaid at the current rate, uncapped and saturating; `None` when the balance is ≥ 0 or the rate is 0;
      - `ready_at(now) -> Option<Instant>` = `min(repaid_at(now), now + 1 s)`: waiters sleep at most 1 s, then recheck, so a lowered limit can never strand a waiter for long.
    - `pub(crate) struct Throttle { chain: Vec<Arc<Mutex<TokenBucket>>> }` per peer and direction. Every peer chains its transport class bucket (`tcp` or `utp`); a local peer also chains `local`; the global and torrent buckets are chained for non-local peers, and for local ones only when `rate_limit_local_peers` is set. Methods: `charge(n)` and `async wait_ready()` (sleeps until every bucket is ready).
    - `pub(crate) struct Bandwidth`, in `Shared`: the global, class and per-torrent buckets, and a 1 s rebalance task for mixed mode. That task counts connected TCP and uTP peers (through `AtomicUsize`s updated on connect and disconnect) and sets the TCP and uTP class rates from the global limit as recorded in "Decisions". A class's own non-zero limit always caps it as well.
  - In the peer connection:
    - **Download.** After each received frame, `charge(frame_len)`. The `framed.next()` branch of `select!` is guarded by the download throttle being ready, so we stop reading, the transport's window closes, and the sender slows. Commands and timers keep running. While the throttle holds reads, the peer idle timer (`peer_idle_timeout`, measured from `last_recv`) is paused: unread keepalives are not the peer's silence. On a uTP transport the remote keeps acking our window probes, so Task 7's idle rule does not close it either.
    - **Upload.** `SendBlock` writes are queued, and the queue drains only while the upload throttle is ready. Each block is charged when it is written. A `Cancel` (or a choke) removes its block from the queue if it has not been written. Handshake, keepalive, `Have`, choke, interest, extension and other non-`Piece` messages bypass the throttle (charged but never delayed), so a tiny limit cannot starve control traffic.
    - **Upload accounting** (PR #1's deferred overcount). The connection emits `PeerEvent::BlockSent { key, block, len }` once a `Piece` message has been flushed. `counters.uploaded`, `ul` and `p.to_rate` count there instead of when `SendBlock` is queued, so cancelled and choked-away blocks are not counted.

- [ ] **Step 1: Write failing tests**
  - Unit tests in `bandwidth.rs`:
    - `bucket_refills_at_rate_and_caps_burst`
    - `debt_delays_until_repaid`: a fresh bucket (balance 0) at rate 1000, consume 3000: `repaid_at` = now + 3 s and `ready_at` = now + 1 s (both within 1 ms). After 1 s `ready_at` is again now + 1 s; after 3 s it is `None`.
    - `zero_rate_is_unlimited`
    - `edge_limits_never_panic`: rates `1`, `u64::MAX` and `i64::MAX as u64 + 1`, consumes of `u64::MAX`, `set_rate` back and forth, and `ready_at`/`repaid_at` at `Instant` + 100 years. All saturate, and no `ready_at` is more than 1 s away (`repaid_at` may be).
    - `peer_proportional_split`: global 900, 2 TCP and 1 uTP peer give TCP 600 and uTP 300.
    - `prefer_tcp_throttles_utp_while_tcp_present`: global 1000 gives uTP 100 while TCP peers exist, and 1000 when none do.
    - `local_peers_skip_global_by_default`: a local peer's chain is {its transport class, `local`}; with `rate_limit_local_peers` it also has global and torrent.
  - Integration tests in `tests/ratelimit.rs`, with an 8 MiB torrent on loopback measured by wall clock. Loopback peers are local, so every test sets `rate_limit_local_peers: true` except `local_limit_applies_when_enabled`, which toggles it:
    - `global_download_limit_is_honored`: limit 1 MiB/s takes between 6.5 s and 10 s.
    - `torrent_upload_limit_is_honored`
    - `class_limit_applies_to_utp_only`: uTP limited to 512 KiB/s and TCP unlimited, with two leechers. The uTP one is at least 3× slower (the transport class bucket applies to local peers too).
    - `local_limit_applies_when_enabled`: loopback peers are local. With `rate_limit_local_peers: false`, the global limit is ignored; with `true`, it is honored.
    - `tiny_limit_keeps_connections_alive`: 1 B/s both directions and `peer_idle_timeout` 2 s, over the default (uTP) transport. After 5 s the peers are still connected and keepalives flow.
    - `limit_change_applies_mid_transfer`: start at 256 KiB/s and switch to unlimited after 1 s through `set_rate_limit`. It finishes in under 3 s more.
    - `upload_counts_only_sent_blocks`: the seed has an upload limit of about 16 KiB/s, so blocks stay queued. A scripted peer requests 10 blocks and cancels 5 of the queued ones. `total_uploaded == 5 * 16384`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS.
- [ ] **Step 5: Commit.** `feat(session): global, per-torrent and per-peer-class rate limits with mixed-mode balancing` No trailers or attribution of any kind.

---

### Task 17: choker tuning, adaptive request pipeline and snubbing

**Files:**
- Modify: `crates/fbt-session/src/{choker.rs,settings.rs,lib.rs}`, `crates/fbt-session/src/torrent/{peers.rs,swarm.rs}`, `crates/fbt-session/src/peer/conn/{state.rs,outbound.rs,inbound.rs}`, `crates/fbt-session/src/peer/messages.rs`, `crates/fbt-session/tests/swarm.rs`.

**Interfaces:**
- Produces (public):
  - `#[non_exhaustive] pub enum ChokingAlgorithm { #[default] FixedSlots, RateBased }`
  - `#[non_exhaustive] pub enum SeedChokingAlgorithm { #[default] RoundRobin, FastestUpload, AntiLeech }`
  - Settings:
    - `choking_algorithm` and `seed_choking_algorithm`;
    - `optimistic_unchoke_slots: usize = 0`, where 0 means `max(1, unchoke_slots / 5)`;
    - `optimistic_unchoke_interval: Duration = 30 s`, which must be ≥ `rechoke_interval`, so rotation happens every `interval / rechoke_interval` rounds;
    - `seeding_piece_quota: u32 = 20`;
    - `snub_timeout: Duration = 60 s`;
    - `request_queue_time: Duration = 3 s`;
    - `max_outstanding_requests` default raised from 32 to 500 (libtorrent's `max_out_request_queue`).
  - `PeerChokeInfo` gains `snubbed: bool`, `progress_ppm: u32` (the peer's fraction of pieces in parts per million, 0 to 1,000,000, so the struct keeps `Eq` and `Copy`), `unchoked_since: Option<Instant>`, `uploaded_since_unchoke: u64` and `last_optimistic: Option<Instant>`.
  - `Choker::new(cfg: ChokerConfig)` (today `Choker::new(slots: usize)`) and `rechoke(&mut self, peers: &[PeerChokeInfo], seeding: bool, now: Instant, rng: &mut impl Rng) -> HashSet<PeerKey>`. `choker` stays a `pub mod`, so `ChokerConfig` is `pub`, `#[derive(Debug, Clone)] #[non_exhaustive]` with `Default`, and built from `Settings` by a crate-internal constructor: `{ slots, optimistic, rotate_rounds, algorithm, seed_algorithm, piece_quota_bytes }` (a crate-private type in a public signature fails `private_interfaces` under `-D warnings`). The `Choker::new`/`rechoke`/`PeerChokeInfo` changes are breaking and go in CHANGELOG `### Changed` (Task 29).
    - **Download (tit-for-tat).** Interested, non-snubbed peers are ranked by `rate_from_peer`.
      - `FixedSlots` gives the top `slots - optimistic` a slot.
      - `RateBased` unchokes in rank order while the peer's rate is ≥ a threshold that starts at 1024 B/s and grows by 2048 B/s per slot opened. At least one slot is opened, and the count is capped by the number of interested peers.
    - **Seeding.**
      - `FastestUpload` ranks by `rate_to_peer`.
      - `RoundRobin` keeps an unchoked peer until it has received `seeding_piece_quota` pieces' bytes or 3 rechoke rounds have passed, then moves it behind every peer by `unchoked_since`.
      - `AntiLeech` ranks by `|progress_ppm − 500_000|` descending, ties by `rate_to_peer`.
    - **Optimistic slots.** They go to the interested peers not already unchoked with the oldest `last_optimistic` (never counts as oldest), ties random. They rotate every `rotate_rounds`. Snubbed peers are eligible for optimistic slots only.
  - In the peer connection:
    - **Adaptive pipeline.** The target depth is `clamp(rate_from_peer * request_queue_time / BLOCK_SIZE, 2, max_outstanding_requests)`, from a `RateMeter` owned by the connection. It replaces the fixed `max_outstanding_requests` in `want_blocks`. A snubbed peer's depth is 1.
    - **Snubbing.** Outstanding requests with no block for `snub_timeout` set `snubbed`, and `PeerEvent::Snubbed { key, snubbed: bool }` is emitted on every flip. The next block clears it.

- [ ] **Step 1: Write failing tests**
  - Unit tests in `choker.rs`, updated from the milestone 1 tests to the new signature (all behavior kept):
    - `rate_based_opens_slots_by_threshold`: rates 10k, 5k, 2k and 1k B/s (thresholds 1024, 3072, 5120…). The regular unchoked set is exactly {10k, 5k}, plus exactly one optimistic peer (the auto slot) from {2k, 1k}.
    - `round_robin_rotates_after_quota`
    - `round_robin_rotates_after_three_rounds_without_quota`
    - `anti_leech_prefers_extremes`: progress 5 % and 95 % (50,000 and 950,000 ppm) beat 50 %.
    - `fastest_upload_seeding`
    - `optimistic_goes_to_longest_waiting`
    - `snubbed_only_optimistic`
    - `auto_optimistic_slots`: `unchoke_slots` 10 gives 2.
  - `peer/conn/tests.rs`:
    - `pipeline_grows_with_rate`: a scripted fast peer at about 2 MiB/s sees outstanding requests grow past 32 within 2 s.
    - `pipeline_minimum_is_two`
    - `snub_after_timeout_and_clear_on_block`: `snub_timeout` 300 ms.
  - `tests/swarm.rs`: `seeding_round_robin_serves_every_leecher`: 6 leechers, `unchoke_slots` 2 and `rechoke_interval` 200 ms. All finish.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS, and milestone 2's `rechoke_chokes_peer_that_lost_its_slot` still passes.
- [ ] **Step 5: Commit.** `feat(session): rate-based and seeding choker variants, adaptive request pipeline, snubbing` No trailers or attribution of any kind.

---

### Task 18: BEP 21 upload-only and BEP 54 `lt_donthave` in the session

**Files:**
- Modify: `crates/fbt-session/src/event.rs` (the `FileError` doc), `crates/fbt-session/src/peer/{ext.rs,messages.rs}`, `crates/fbt-session/src/peer/conn/{inbound.rs,outbound.rs}`, `crates/fbt-session/src/torrent/{peers.rs,serve.rs,data.rs,swarm.rs}`, `crates/fbt-session/src/picker.rs`, `crates/fbt-session/tests/swarm.rs`.

**Interfaces:**
- Consumes: Task 3's messages; Task 15's serve path (cache first, so tests that delete files disable the cache).
- Produces:
  - `ExtConfig` gains `upload_only: bool` and `holepunch: bool` (Task 19 sets the latter). `our_handshake` advertises `upload_only`, `lt_donthave` always, and the `upload_only` key when `upload_only` is true.
  - `PeerCommand::UploadOnly(bool)` sends the `upload_only` message when the remote advertised it. The actor sends it to every connected peer when "finished" (all wanted pieces) flips.
  - `PeerCommand::DontHave(u32)` sends `lt_donthave` when the remote advertised it.
  - `PeerEvent::UploadOnly { key, on: bool }` comes from the handshake key or the message. `PeerEvent::DontHave { key, piece }` is range-checked like `Have`; an out-of-range index is a protocol error.
  - In the actor:
    - **Remote `DontHave`.** It clears the piece in `p.bits` and calls `picker.peer_lost(piece)` to decrement availability, then re-evaluates interest. A `HaveAll` peer (counted in the picker's `seeds` counter) is first materialized through the existing pair: `remove_peer_pieces(PeerPieces::All)` then `add_peer_pieces(PeerPieces::Bits(full))`, and then the bit is cleared.
    - **Serve read `NotFound`** (we lost the piece on disk). `picker.piece_lost(piece)` clears `have` and makes the piece wanted again, `DontHave` goes to every peer, `Event::FileError` is emitted with `path` (the torrent keeps running), and the request is rejected. Other read errors keep stopping the torrent. `Event::FileError`'s doc changes from "the torrent stopped" to say it stops, except for a piece lost while serving.
    - **Upload-only peers.** When we are finished and a connected peer is a seed or `upload_only`, it is closed with `#[non_exhaustive] DisconnectReason::UploadOnly`.
  - `PiecePicker::{peer_lost(piece), piece_lost(piece)}`; `peer_lost` mirrors `peer_has(piece)` (the availability API carries no `PeerKey`). Both keep the picker invariants (`check_invariants` extended).

- [ ] **Step 1: Write failing tests**
  - `picker.rs`: `peer_lost_decrements_availability` and `piece_lost_is_wanted_again`, both with `check_invariants`.
  - `peer/conn/tests.rs`: `donthave_routed_and_range_checked` and `upload_only_from_handshake_and_message`.
  - `tests/swarm.rs`:
    - `seeds_disconnect_from_upload_only_peers`: two seeds of one torrent connect and close with `UploadOnly` within 2 s.
    - `lost_piece_sends_donthave_and_is_redownloaded`: seed A and partial seed B serve leecher C. B runs with `cache_size: 0`, so the piece is not held clean in B's cache (Task 15) and the deletion is seen. A file is deleted under B; C's request to B makes B send `DontHave` (a scripted observer peer sees it), B fetches the piece again from A, and C finishes.
    - `donthave_from_remote_lowers_availability`: a scripted peer sends `HaveAll` then `DontHave(3)`, and piece 3 is never requested from it.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `feat(session): BEP 21 upload-only and BEP 54 lt_donthave` No trailers or attribution of any kind.

---

### Task 19: BEP 55 holepunch

**Files:**
- Create: `crates/fbt-session/src/torrent/holepunch.rs`, `crates/fbt-session/tests/holepunch.rs`.
- Modify: `crates/fbt-session/src/{settings.rs,net.rs,listener.rs}`, `crates/fbt-session/src/peer/{ext.rs,messages.rs}`, `crates/fbt-session/src/peer/conn/{inbound.rs,outbound.rs}`, `crates/fbt-session/src/torrent/{swarm.rs,pex.rs,peers.rs}`.

**Interfaces:**
- Consumes: `HolepunchMsg` (Task 3), `UtpSocket::set_accept_filter` (Task 8), `first_transport`/`Retries` and the per-address `Known` record (Task 12), and the reject-all filter Task 12 builds in `net.rs`.
- Produces:
  - Settings:
    - `pub enable_holepunch: bool = true`. It takes effect only with outgoing and incoming uTP enabled, and never for private torrents.
    - Hidden `pub nat_emulation: bool = false` emulates an endpoint-dependent NAT for tests. Incoming TCP is refused, and uTP SYNs are accepted only from addresses we sent a datagram to in the last 30 s. `net.rs` keeps a bounded `recent_sends` map, capped by its own `const MAX_RECENT_SENDS: usize = 1024` (oldest evicted; `swarm.rs`'s private `MAX_NEVER` is not reused), behind the uTP accept filter, and uTP sends record into it. The filter is one closure composing Task 12's incoming-uTP-off check and this one.
  - `ExtConfig.holepunch` advertises `ut_holepunch`. `PeerCommand::SendHolepunch(HolepunchMsg)` and `PeerEvent::Holepunch { key, msg }`.
  - `torrent/holepunch.rs` (pure):
    - `pub(super) fn relay(target: SocketAddr, requester: SocketAddr, our_addrs: &[SocketAddr], target_peer: Option<(PeerKey, bool /*supports*/)>) -> RelayAction`, where `RelayAction` is one of:
      - `Error(HP_NO_SELF)` when the target is one of our listen addresses;
      - `Error(HP_NOT_CONNECTED)` when it is not connected;
      - `Error(HP_NO_SUPPORT)` when it lacks `ut_holepunch`;
      - `Connect { to_target: requester, to_requester: target }` otherwise.
    - `pub(super) struct RendezvousLimiter`: at most `const MAX_RENDEZVOUS_PER_MIN: u32 = 10` rendezvous accepted from each requesting peer; beyond that they are ignored.
    - `pub(super) struct Pending`: addresses we asked a relay about, capped at 64, each expiring after 30 s.
  - Rendezvous initiation:
    - `Known` records, per address learned from PEX, the PEX source's `PeerKey` and whether its `HOLEPUNCH` flag was set.
    - When an outgoing connection to such an address fails at connect (both transports, or uTP alone when TCP is off), and the source is still connected and supports `ut_holepunch`, the source gets `Rendezvous(addr)`, once per address.
    - On `Connect(addr)` from a relay, we dial `addr` over uTP at once. The dial is allowed only if `addr` is pending (we asked) or the message came from a connected relay, rate-limited by `RendezvousLimiter`.
    - Errors are logged at debug, and the address goes back to normal retries.
  - **PEX.** `PexFlags::HOLEPUNCH` is set on addresses whose connection advertised `ut_holepunch`.

- [ ] **Step 1: Write failing tests**
  - Unit tests in `holepunch.rs`: `relay_errors_follow_bep55` (each of the three error cases and the `Connect` case), `rendezvous_rate_limited` and `pending_expires_and_is_capped`.
  - Integration tests in `tests/holepunch.rs`. Three sessions on 127.0.0.1: relay R (a leecher with nothing), seed C and leecher A. A and C have `nat_emulation: true`, `enable_incoming_tcp: false` and `utp_connect_timeout` 300 ms; `pex_interval` is 200 ms.
    - `holepunch_connects_two_natted_peers`: A and C each get `add_peer` to R and dial it, as a real NAT'd peer would (R, not NAT-emulated, accepts; A and C let R's replies in because they sent to R first). A learns C through R's PEX (`HOLEPUNCH` set), its direct uTP SYN is dropped by C's emulated NAT, it sends `Rendezvous` to R, and R sends `Connect` to both. A and C then connect over `Transport::Utp`, and A finishes from C within 20 s. `PeerConnected { addr: C }` arrives exactly once at A after tie-breaking.
    - `relay_answers_not_connected`: a scripted peer connected to R (advertising `ut_holepunch`) sends `Rendezvous` for an address R does not hold, and receives `Error { code: HP_NOT_CONNECTED }`.
    - `private_torrent_never_relays`
    - `nat_emulation_blocks_unsolicited`: without holepunch (`enable_holepunch: false`), A never connects to C within 5 s.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test holepunch`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test -p fbt-session`: PASS, stable over 20 reruns of `--test holepunch`.
- [ ] **Step 5: Commit.** `feat(session): BEP 55 holepunch over uTP with relay, rendezvous and PEX flags` No trailers or attribution of any kind.

---

### Task 20: local service discovery (BEP 14)

**Files:**
- Create: `crates/fbt-core/src/rate.rs`, `crates/fbt-net/src/lsd.rs`, `crates/fbt-net/tests/lsd.rs`, `crates/fbt-session/tests/lsd.rs`.
- Modify: `crates/fbt-core/src/lib.rs`, `crates/fbt-dht/src/{rate.rs,lib.rs}` (its `RateLimiter` replaced by the fbt-core one; `rate.rs` removed or reduced to a re-export), `crates/fbt-net/src/lib.rs`, `crates/fbt-session/src/{settings.rs,net.rs,event.rs,session.rs}` (the session-owned `Lsd`, the wire-hash lookup and the announce tick), `crates/fbt-session/src/torrent/{actor.rs,swarm.rs,mod.rs,commands.rs}` (`Cmd::FoundPeers` in `mod.rs`, handled in `commands.rs`), `crates/fbt-session/tests/support/mod.rs` (`test_settings()` turns LSD off), `crates/fbt-example/src/main.rs` (LSD off, see below).

**Interfaces:**
- Produces (`fbt-core`):
  - `fbt_core::rate::IpRateLimiter` (std-only, like the `udp` traits), hoisted from `fbt-dht/src/rate.rs::RateLimiter`: `pub fn new(per_sec: u32, burst: u32, max_ips: usize) -> Self` and `pub fn allow(&mut self, ip: IpAddr, now: Instant) -> bool`; a per-IP token bucket in a capped map, the oldest evicted. fbt-dht keeps its behavior by passing its current burst; fbt-net (LSD, and NAT if needed) uses it too. No second copy.
- Produces (`fbt-net`):
  - Constants:
    - `pub const LSD_V4: SocketAddr` = `239.192.152.143:6771` and `pub const LSD_V6: SocketAddr` = `[ff15::efc0:988f]:6771`;
    - `pub const MAX_LSD_MESSAGE: usize = 1400;`
    - `pub const LSD_HASHES_PER_MESSAGE: usize = 16;` and `pub const MAX_LSD_DECODED_HASHES: usize = 64;`
  - `#[derive(Debug, Clone, PartialEq, Eq)] pub struct LsdMessage { pub port: u16, pub info_hashes: Vec<[u8; 20]>, pub cookie: Option<String> }`, with `encode(&self, host: SocketAddr) -> Vec<u8>` and `decode(data: &[u8]) -> Result<LsdMessage, LsdError>`.
    - The wire form is `BT-SEARCH * HTTP/1.1\r\nHost: <group>\r\nPort: <p>\r\nInfohash: <40 hex>\r\n…cookie: <c>\r\n\r\n\r\n`.
    - Header names are case-insensitive; unknown headers are ignored.
    - Decoding fails on a wrong request line, a port outside `1..=65535`, input over `MAX_LSD_MESSAGE`, or no valid info hash. Hashes beyond `MAX_LSD_DECODED_HASHES` are dropped, and so are the cookie and other values over 64 bytes.
  - LSD's inbound limiter is `fbt_core::rate::IpRateLimiter` with the map capped at `const LSD_MAX_IPS: usize = 1024`.
  - `#[derive(Debug, Clone)] #[non_exhaustive] pub struct LsdConfig`, with defaults:
    - `group_v4: Option<SocketAddr> = Some(LSD_V4)`, `group_v6: Option<SocketAddr> = Some(LSD_V6)`
    - `cookie: String` (random 8 hex characters)
    - `rate_per_ip: u32 = 1` per second, with a burst of 5
  - `pub struct Lsd` (Clone), with:
    - `pub fn spawn(cfg: LsdConfig, found: mpsc::Sender<LsdPeer>) -> io::Result<Lsd>`: binds `0.0.0.0:<group port>` with `SO_REUSEADDR` (and `SO_REUSEPORT` on Unix) through `socket2`, joins the group with multicast loopback on, and does the same for IPv6 when it can (skipped with a debug log when it cannot);
    - `pub fn spawn_on(cfg: LsdConfig, sockets: Vec<(Arc<dyn DatagramSender>, SocketAddr /*destination*/)>, found: mpsc::Sender<LsdPeer>) -> (Lsd, Arc<dyn DatagramHandler>)` for tests: announces go to each given destination (any address, unicast included), and the returned handler is fed by whoever owns the receiving socket;
    - `pub fn announce(&self, port: u16, info_hashes: &[[u8; 20]])`: batched by `LSD_HASHES_PER_MESSAGE`;
    - `pub async fn shutdown(&self)`.
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub struct LsdPeer { pub info_hash: [u8; 20], pub addr: SocketAddr }`. The address is the source IP with the message's port, and messages carrying our own cookie are ignored.
- Produces (`fbt-session`):
  - Settings:
    - `pub enable_lsd: bool = true`; `test_settings()` (in `tests/support/mod.rs`) sets it false.
    - Hidden `lsd_groups: Vec<SocketAddr> = [LSD_V4, LSD_V6]` and `lsd_announce_interval: Duration = 5 min`.
    - Hidden `lsd_bind: Option<SocketAddr> = None`. When set, the session binds a plain UDP socket there (not the shared listen socket: LSD never goes through the demux, whose `classify` drops `BT-SEARCH` datagrams) and runs `Lsd::spawn_on` with that socket's sender, one destination per `lsd_groups` entry, and a receive task feeding the returned handler. When `None`, it uses `Lsd::spawn` on the real groups.
  - The session owns one `Lsd`. Each public torrent (not private, not paused, metadata or magnet hash known) is announced at start or resume and then every `lsd_announce_interval`. The session records the last announce per info hash and skips a re-announce within `lsd_announce_interval`, so pause/resume cycles never exceed the global "at most every 5 min". Announces are batched across torrents on the session's tick.
  - **`fbt-example` turns LSD off** in this task (`settings.enable_lsd = false` in its download/seed/dht-node settings), so `cargo test --workspace` (including `fbt-example/tests/cli.rs`) never multicasts on the LAN before Task 25 adds the `--lsd` flag.
  - Found peers go to the torrent with that wire hash through a new `Cmd::FoundPeers(PeerSource, Vec<SocketAddr>)`, as `#[non_exhaustive] PeerSource::Lsd`.
  - A private torrent's hash is never announced, and LSD peers for it are dropped.

- [ ] **Step 1: Write failing tests**
  - Unit tests in `lsd.rs`:
    - `bep14_message_roundtrip`: the encoded bytes start with `b"BT-SEARCH * HTTP/1.1\r\nHost: 239.192.152.143:6771\r\nPort: 6881\r\n"`.
    - `decode_is_case_insensitive_and_bounded`: 100 `Infohash` headers keep 64.
    - `rejects_bad_messages`: wrong method, port 0, an oversize message, no hash.
    - `own_cookie_ignored`
    - Proptest `lsd_decode_never_panics`.
  - `tests/lsd.rs` (fbt-net):
    - `two_instances_find_each_other_via_injected_transport`
    - `lsd_flood_is_rate_limited`: 1000 messages from one IP in a burst give at most 6 `LsdPeer`s.
    - `real_multicast_loopback`: skipped (early return) only if joining the group fails; once joined, a missing message fails the test. It uses a random group in `239.255.x.y` and a random port, `IP_MULTICAST_TTL 0` and the loopback interface (`127.0.0.1`) as the multicast interface, so nothing reaches the LAN. If CI runners turn out to drop looped multicast, mark it `#[ignore]` with that reason rather than weakening it.
  - `fbt-session/tests/lsd.rs`:
    - `lsd_peer_found_and_download_completes`: two sessions with `enable_lsd`, each with `lsd_bind: Some(127.0.0.1:<own port>)` and `lsd_groups` set to the other's `lsd_bind` address. The leecher gets `PeersFound { source: PeerSource::Lsd }` and finishes.
    - `resume_does_not_reannounce_within_interval`: pause and resume a torrent three times; the peer's recording socket sees one announce for it.
    - `private_torrent_not_announced`
  - fbt-dht's existing rate-limit tests keep passing on the hoisted limiter; a unit test in `fbt-core/src/rate.rs` covers `burst`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-net -p fbt-session`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS (the example included, with LSD off).
- [ ] **Step 5: Commit.** `feat(net): BEP 14 local service discovery, wired into the session` No trailers or attribution of any kind.

---

### Task 21: hand-written HTTP/1.1 client and bounded XML scanner

**Files:**
- Create: `crates/fbt-net/src/upnp/{mod.rs,http.rs,xml.rs}`.
- Modify: `crates/fbt-net/src/lib.rs` (`pub mod upnp;`).

**Interfaces:**
- Produces (`fbt_net::upnp`). `http` and `xml` are private submodules of `upnp` whose items are `pub`; `upnp/mod.rs` re-exports the ones the fuzz targets need (`parse_response`, `find_services`, `element_text`, plus their types) under `#[doc(hidden)] pub use`. Re-exporting `pub(crate)` items as `pub` would not compile (E0364). Task 23 adds `ssdp`'s `parse_search_response` the same way.
  - `pub const MAX_HTTP_RESPONSE: usize = 65_536;` and `pub const HTTP_TIMEOUT: Duration = Duration::from_secs(5);`
  - `pub struct HttpRequest<'a> { pub method: &'a str, pub host: SocketAddr, pub path: &'a str, pub headers: &'a [(&'a str, &'a str)], pub body: &'a [u8] }` with `to_bytes(&self) -> Vec<u8>` (HTTP/1.1, `Host`, `Content-Length`, `Connection: close`).
  - `pub fn parse_response(bytes: &[u8]) -> Result<HttpResponse, HttpError>` (pure), where `HttpResponse { status: u16, headers: Vec<(String, String)>, body: Vec<u8> }`.
    - At most 64 headers, each line at most 4096 bytes.
    - `Content-Length`, `Transfer-Encoding: chunked` (chunk sizes bounded by what is left of the response cap), and read-to-EOF are all supported.
    - A body past `MAX_HTTP_RESPONSE` gives `TooLarge`.
  - `pub async fn request(req: &HttpRequest<'_>) -> Result<(HttpResponse, SocketAddr /*our local addr*/), HttpError>`: connect, write, read until EOF or a complete message, with the whole exchange bounded by `HTTP_TIMEOUT` and `MAX_HTTP_RESPONSE`. It calls `pub async fn request_with_timeout(req, timeout: Duration)`, which Task 23's `Igd`/`discover` also use so their tests can shorten the bound.
  - `#[derive(Debug, thiserror::Error)] pub enum HttpError { Io(#[from] io::Error), Timeout, TooLarge, Malformed(&'static str) }`
  - `xml.rs` (pure): a tag and text scanner over `&str`.
    - `pub fn find_services(xml: &str) -> Vec<Service>`, where `Service { service_type: String, control_url: String }`, from every `<service>` element at any depth. It keeps at most 32 services, and values are at most 256 bytes.
    - `pub fn element_text(xml: &str, local_name: &str) -> Option<String>`: the first element with that local name (namespace prefixes ignored), decoding only `&lt; &gt; &amp; &quot; &apos;`.
    - Depth is capped at 32. `<!DOCTYPE`, CDATA and processing instructions are skipped, and entities beyond those five stay literal.

- [ ] **Step 1: Write failing tests** (unit, in each file)
  - `http.rs`:
    - `parses_content_length_chunked_and_eof`
    - `rejects_oversize_and_bad_status_line`
    - `request_bytes_are_http11`
    - `slow_server_times_out`: a loopback server that writes 1 byte per second. `request_with_timeout` with 300 ms gives `HttpError::Timeout`.
  - `xml.rs`:
    - `finds_wanipconnection_in_miniupnpd_description`: a captured miniupnpd `rootDesc.xml` fixture, written by hand from the UPnP IGD examples, yields `urn:schemas-upnp-org:service:WANIPConnection:1` with `/ctl/IPConn`.
    - `element_text_ignores_prefix_and_decodes_entities`
    - `deep_nesting_is_cut`: 10,000 nested tags parse without stack growth (iterative) and give nothing.
    - Proptest `xml_scanner_never_panics`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-net`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `feat(net): bounded HTTP/1.1 client and XML scanner for UPnP` No trailers or attribution of any kind.

---

### Task 22: NAT-PMP and PCP

**Files:**
- Create: `crates/fbt-net/src/natpmp.rs`, `crates/fbt-net/tests/{natpmp.rs,support/fake_natpmp.rs}`.
- Modify: `crates/fbt-net/src/lib.rs` (`pub mod natpmp;` and the root re-exports).

**Interfaces:**
- Produces (`fbt_net::natpmp`):
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)] #[non_exhaustive] pub enum MapProtocol { Tcp, Udp }` (re-exported from the crate root; Tasks 23 and 24 use it, and `fbt-session`/`fbt` re-export it, hence non-exhaustive).
  - `pub const NATPMP_PORT: u16 = 5351;`
  - Codec (pure):
    - `pub fn natpmp_external_request() -> [u8; 2]` is `[0, 0]`.
    - `pub fn natpmp_map_request(p: MapProtocol, internal: u16, external: u16, lifetime: u32) -> [u8; 12]` is `[0, op (1 UDP, 2 TCP), 0, 0, internal BE, external BE, lifetime BE]`.
    - `pub fn pcp_map_request(nonce: [u8; 12], p: MapProtocol, internal: u16, external: u16, client: Ipv4Addr, lifetime: u32) -> [u8; 60]`:
      - the 24-byte header (version 2, opcode 1, lifetime, client as IPv4-mapped IPv6);
      - then the 36-byte MAP body (nonce, protocol 6 or 17, internal port, suggested external port, suggested external IP `::ffff:0.0.0.0`).
    - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub struct Expect` carries the opcode, protocol, internal port and, for PCP, the nonce. It is built only through `pub fn Expect::natpmp_external() -> Expect`, `pub fn Expect::natpmp_map(p: MapProtocol, internal: u16) -> Expect` and `pub fn Expect::pcp_map(p: MapProtocol, internal: u16, nonce: [u8; 12]) -> Expect` (Task 26's `natpmp` fuzz target consumes these three).
    - `pub fn parse_reply(data: &[u8], expect: &Expect) -> Result<Reply, NatPmpError>`, where `Reply` is one of:
      - `NatPmpExternal { epoch: u32, ip: Ipv4Addr }`;
      - `NatPmpMap { epoch: u32, internal: u16, external: u16, lifetime: u32 }`;
      - `PcpMap { lifetime: u32, external: SocketAddr }`;
      - `UnsupportedVersion`: a NAT-PMP reply with result code 1 to a PCP request, the signal to fall back.
    - A result code ≠ 0 (other than unsupported version) gives `NatPmpError::Result(code)`. A wrong length, op or nonce gives `Malformed`.
  - Client:
    - `pub async fn map(gateway: Ipv4Addr, port: u16 /*5351; tests vary it*/, p: MapProtocol, internal: u16, lifetime: u32) -> Result<Mapping, NatPmpError>`, where `Mapping { method: MapMethod, external: SocketAddr, lifetime: u32 }`.
    - It tries PCP first. On `UnsupportedVersion` it uses NAT-PMP: the external-address request, then the map.
    - Each request is retried at 250 ms, 500 ms, 1 s and 2 s (4 attempts, RFC 6886's schedule cut short), then gives `Timeout`.
    - It listens on an ephemeral socket and ignores datagrams whose source is not `gateway:port`.
    - `pub async fn unmap(...)` maps with lifetime 0.
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)] #[non_exhaustive] pub enum MapMethod { Upnp, NatPmp, Pcp }` (crate root; re-exported by `fbt-session` and `fbt`).

- [ ] **Step 1: Write failing tests**
  - Unit tests in `natpmp.rs`:

```rust
#[test] fn natpmp_request_layout() {
    assert_eq!(natpmp_map_request(MapProtocol::Tcp, 6881, 6881, 7200), [0, 2, 0, 0, 0x1a, 0xe1, 0x1a, 0xe1, 0, 0, 0x1c, 0x20]);
}
#[test] fn natpmp_map_reply_parsed() {
    let r = [0, 130, 0, 0, 0, 0, 0, 9, 0x1a, 0xe1, 0x1b, 0x00, 0, 0, 0x1c, 0x20];
    assert!(matches!(parse_reply(&r, &Expect::natpmp_map(MapProtocol::Tcp, 6881)), Ok(Reply::NatPmpMap { external: 6912, lifetime: 7200, .. })));
}
#[test] fn pcp_request_is_60_bytes_with_nonce() {
    let b = pcp_map_request([7; 12], MapProtocol::Udp, 6881, 6881, "192.168.1.5".parse().unwrap(), 7200);
    assert_eq!((b[0], b[1]), (2, 1)); assert_eq!(&b[24..36], &[7; 12]); assert_eq!(b[36], 17);
}
```

  Also `pcp_wrong_nonce_is_malformed`, `unsupported_version_detected` and the proptest `parse_reply_never_panics`.
  - `tests/natpmp.rs` uses `support/fake_natpmp.rs`, a loopback UDP server that speaks NAT-PMP only, or PCP, or neither, records requests, and can answer from a second socket:
    - `pcp_mapping`
    - `falls_back_to_natpmp`
    - `reply_from_wrong_source_is_ignored`: answers come only from the second socket, so `Timeout`.
    - `error_result_code_reported`
    - `silent_gateway_times_out_after_four_attempts`
    - `unmap_sends_zero_lifetime`
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-net`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `feat(net): NAT-PMP and PCP port mapping` No trailers or attribution of any kind.

---

### Task 23: UPnP IGD (SSDP and SOAP)

**Files:**
- Create: `crates/fbt-net/src/upnp/{ssdp.rs,soap.rs}`, `crates/fbt-net/tests/{upnp.rs,support/fake_igd.rs}`.
- Modify: `crates/fbt-net/src/upnp/mod.rs`.

**Interfaces:**
- Consumes: Task 21 (`request_with_timeout`, `parse_response`, `find_services`, `element_text`), `MapProtocol`.
- Produces (`ssdp` and `soap` are private submodules with `pub` items, like Task 21's; `upnp/mod.rs` re-exports `parse_search_response` and `Location` under `#[doc(hidden)] pub use` for Task 26):
  - `ssdp.rs`:
    - `pub const SSDP_V4: SocketAddr` = `239.255.255.250:1900`.
    - `pub fn search_request(st: &str) -> Vec<u8>`: `M-SEARCH * HTTP/1.1`, `HOST`, `ST`, `MAN: "ssdp:discover"`, `MX: 2`.
    - `pub fn parse_search_response(data: &[u8], from: SocketAddr) -> Result<Location, SsdpError>`, where `Location { addr: SocketAddr, path: String }`.
      - It needs `HTTP/1.1 200` and a `LOCATION: http://host:port/path` header.
      - The location host must be an IP literal equal to `from.ip()` (`SsdpError::Foreign` otherwise), and only `http` is accepted.
  - `soap.rs`: builders
    - `add_port_mapping(service_type, ext_port, p, int_port, client: Ipv4Addr, lease: u32, description) -> (String /*SOAPAction*/, Vec<u8>)`;
    - `delete_port_mapping(service_type, ext_port, p)`;
    - `get_external_ip(service_type)`.

    The parser `pub fn soap_result(resp: &HttpResponse) -> Result<Option<String /*NewExternalIPAddress if present*/>, UpnpError>`: a 200 is OK; a 500 with `<errorCode>` gives `UpnpError::Soap(code)`.
  - `mod.rs`:
    - `#[derive(Debug, Clone)] pub struct Igd { control: SocketAddr, path: String, service_type: String, local_ip: Ipv4Addr, http_timeout: Duration }`. All HTTP goes through Task 21's `request_with_timeout` with `http_timeout` (default `HTTP_TIMEOUT`; `#[doc(hidden)] pub fn Igd::with_http_timeout(self, d) -> Igd` for tests).
    - `pub async fn discover(ssdp: SocketAddr, timeout: Duration) -> Result<Igd, UpnpError>` and `#[doc(hidden)] pub async fn discover_with(ssdp, timeout, http_timeout: Duration)` (the description fetch uses `http_timeout`):
      - it sends the search for `InternetGatewayDevice:1` and `:2`, three times 500 ms apart, and takes the first good `LOCATION`;
      - it fetches the description and picks `WANIPConnection:2`, `WANIPConnection:1` or `WANPPPConnection:1`, in that order;
      - the control URL is resolved against the location (absolute paths only; an absolute URL must name the same host, else `Foreign`).

      `local_ip` is the local address of the description's TCP connection.
    - `pub async fn add_mapping(&self, p: MapProtocol, port: u16, lease: u32) -> Result<u16 /*external*/, UpnpError>`:
      - external port = internal first;
      - on error 718 (conflict), up to 4 random ports in `1025..=65535`;
      - on error 725 (only permanent leases), one retry with lease 0.
    - `pub async fn delete_mapping(&self, p: MapProtocol, external: u16) -> Result<(), UpnpError>`.
    - `pub async fn external_ip(&self) -> Result<Ipv4Addr, UpnpError>`.
    - `#[derive(Debug, thiserror::Error)] pub enum UpnpError { Http(#[from] HttpError), Ssdp(SsdpError), NoService, Foreign, Soap(u32), Timeout }`.

- [ ] **Step 1: Write failing tests**
  - Unit tests:
    - `ssdp.rs`: `parses_location`, `location_must_name_the_responder` (`LOCATION: http://10.9.9.9/…` from 127.0.0.1 gives `Foreign`), `non_200_rejected` and proptest `ssdp_never_panics`.
    - `soap.rs`: `add_port_mapping_body_matches_spec` (asserts `NewExternalPort`, `NewProtocol` `TCP`, `NewInternalClient`, `NewLeaseDuration` and the `SOAPAction` header value `"urn:schemas-upnp-org:service:WANIPConnection:1#AddPortMapping"`) and `soap_fault_code_extracted`.
  - `tests/upnp.rs` uses `support/fake_igd.rs`. It is a loopback UDP responder for SSDP on an ephemeral port, standing in for the multicast group, and a loopback HTTP server serving the description and SOAP. It records actions and can be scripted with: conflict on port X, permanent-only, a foreign `LOCATION`, an oversized body, a slow body.
    - `discover_and_map_tcp_and_udp`
    - `conflict_picks_another_port`
    - `permanent_only_retries_with_zero_lease`
    - `external_ip_reported`
    - `delete_on_request`
    - `oversized_or_slow_description_is_cut`: `discover_with(.., http_timeout: 300 ms)` gives `UpnpError::Http(HttpError::TooLarge)` for the oversized body and `UpnpError::Http(HttpError::Timeout)` for the slow one.
    - `foreign_location_is_ignored`: discovery times out, and the fake's HTTP port saw no connection.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-net`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `feat(net): UPnP IGD discovery and port mapping over SSDP and SOAP` No trailers or attribution of any kind.

---

### Task 24: port mapping in the session, gateway discovery and external IP

**Files:**
- Create: `crates/fbt-net/src/{portmap.rs,gateway.rs}`, `crates/fbt-session/tests/portmap.rs` (reusing `fbt-net`'s fakes from `crates/fbt-net/tests/support/` as `#[path = "..."]` modules; no dev-dependency is needed for that).
- Modify: `crates/fbt-net/src/lib.rs`, `crates/fbt-dht/src/node.rs` (`DhtStats.external_ip`), `crates/fbt-session/src/{settings.rs,net.rs,event.rs,session.rs,lib.rs}`, `crates/fbt-session/src/peer/conn/inbound.rs` (the `yourip` vote), `crates/fbt-session/tests/support/mod.rs` (`test_settings()` turns UPnP and NAT-PMP off), `crates/fbt-example/src/main.rs` (UPnP and NAT-PMP off, see below).

**Interfaces:**
- Consumes: Tasks 22 and 23; `fbt_dht::ExternalIpVotes` (Task 9).
- Produces (`fbt-net`):
  - `gateway.rs`:
    - Pure parsers, each unit-tested on captured output: `pub fn parse_proc_net_route(s: &str) -> Option<Ipv4Addr>` (the `00000000` destination line, little-endian hex gateway), `pub fn parse_route_get_default(s: &str) -> Option<Ipv4Addr>` (macOS/BSD `gateway:` line) and `pub fn parse_route_print(s: &str) -> Option<Ipv4Addr>` (Windows `0.0.0.0 0.0.0.0 <gw>` row).
    - `pub async fn default_gateway_v4() -> Option<Ipv4Addr>` dispatches by `cfg(target_os)`. It reads the file or runs the command (absolute path `/sbin/route`, `/usr/sbin/route` or `%SystemRoot%\System32\route.exe`) in `spawn_blocking`, with a 2 s timeout and output capped at 64 KiB.
  - `portmap.rs`:
    - `#[derive(Debug, Clone)] #[non_exhaustive] pub struct PortMapConfig`, with defaults:
      - `upnp: bool = true`, `natpmp: bool = true`
      - `gateway: Option<Ipv4Addr> = None` (discovered when `None`)
      - `ssdp: SocketAddr = SSDP_V4`, `natpmp_port: u16 = NATPMP_PORT`
      - `lease: u32 = 3600`
      - `retry_base: Duration = 60 s`, `retry_max: Duration = 30 min`
    - `#[derive(Debug, Clone, PartialEq, Eq)] pub enum MapEvent { Mapped { protocol: MapProtocol, method: MapMethod, internal: u16, external: SocketAddr }, Failed { protocol: MapProtocol, method: MapMethod, message: String }, ExternalIp { ip: Ipv4Addr, method: MapMethod } }`
    - `pub struct PortMapper` (Clone), with:
      - `pub fn spawn(cfg: PortMapConfig, events: mpsc::Sender<MapEvent>) -> PortMapper`;
      - `pub fn map(&self, p: MapProtocol, internal: u16)`;
      - `pub async fn shutdown(&self)`: removes every mapping it made, with a 2 s total budget.

      UPnP and PCP/NAT-PMP run independently, and each reports its own result. Renewal is due at `max(lifetime / 2, 60 s)`. Failures retry from `retry_base`, doubling to `retry_max`.
- Produces (`fbt-session`):
  - Settings: `enable_upnp: bool = true`, `enable_natpmp: bool = true` (covers PCP) and `nat_gateway: Option<Ipv4Addr> = None`. Hidden: `upnp_ssdp_addr: SocketAddr = 239.255.255.250:1900`, `natpmp_port: u16 = 5351` and `nat_retry_base: Duration = 60 s`. `test_settings()` turns both off.
  - **`fbt-example` turns UPnP and NAT-PMP off** in this task (`enable_upnp = false`, `enable_natpmp = false` next to Task 20's `enable_lsd = false`), so `cargo test --workspace` never sends SSDP or talks to the real default gateway before Task 25 adds the `--nat` flag.
  - For each IPv4 endpoint, the session maps TCP and UDP for its port. Loopback-only endpoints are not mapped, unless `nat_gateway` is a loopback address, which is the tests' hook.
  - Events (in a new `EventMask::NAT = 1 << 6`, part of `ALL`):
    - `#[non_exhaustive] Event::PortMapped { protocol: MapProtocol, method: MapMethod, external: SocketAddr }`
    - `Event::PortMappingFailed { protocol: MapProtocol, method: MapMethod, message: String }`
    - `Event::ExternalIpChanged { ip: IpAddr, source: ExternalIpSource }`, with `#[non_exhaustive] pub enum ExternalIpSource { PortMapping, Dht, Peers }`
  - `Session::external_ip(&self) -> Option<IpAddr>` is the best estimate: port mapping, then the DHT (`DhtStats` gains `external_ip: Option<IpAddr>`, which the DHT's existing votes fill), then peers. Peers' `yourip` values go into the session's own `ExternalIpVotes`, with the reporter IP being the peer's IP.
  - `shutdown()` calls `PortMapper::shutdown` after the torrents stop.
  - `MapProtocol` and `MapMethod` are re-exported from `fbt-session`.

- [ ] **Step 1: Write failing tests**
  - `gateway.rs` unit tests: `proc_net_route_sample`, `route_get_default_sample`, `route_print_sample`, and `garbage_yields_none` (proptest never panics).
  - `portmap.rs` tests against both fakes. With a paused clock, tokio auto-advances while a loopback reply is in flight, so the 250 ms retries fire early and request counts drift. The tests therefore assert on the mappings the fakes saw and on renewal/retry times (from the timestamps of the requests the fakes record), never on exact request counts:
    - `maps_with_both_methods_and_renews_at_half_lease`: lease 120 s; each fake records a map and then a renewal of the same mapping at 60 s.
    - `failure_backs_off`: the first request of each round arrives at 60, 120, 240 s.
    - `shutdown_unmaps`
  - `fbt-session/tests/portmap.rs`:
    - `session_maps_its_ports_and_reports_events`: `nat_gateway: Some(127.0.0.1)`, with the fake NAT-PMP and fake IGD on loopback ports through the hidden settings. `PortMapped` arrives for TCP and UDP from each method, and `external_ip()` equals the fakes' address.
    - `no_mapping_with_nat_disabled`: the fakes record nothing.
    - `yourip_votes_set_external_ip`: three scripted peers on 127.0.0.1, 127.0.0.2 and 127.0.0.3 (skipped where those do not route) report `yourip` 203.0.113.7, giving `ExternalIpChanged { source: Peers }`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-net -p fbt-session`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace && bash dev/check-pure-rust.sh`: PASS.
- [ ] **Step 5: Commit.** `feat(session): UPnP and NAT-PMP/PCP port mapping, gateway discovery and external IP` No trailers or attribution of any kind.

---

### Task 25: `fbt` facade and example CLI

**Files:**
- Modify: `crates/fbt/src/lib.rs`, `crates/fbt/tests/lib_surface.rs`, `crates/fbt-example/src/main.rs`, `crates/fbt-example/tests/cli.rs`.

**Interfaces:**
- Consumes: everything new and public from Tasks 11 to 24; `fbt_session::StorageError` at the crate root (Task 14); `rate_limit_local_peers` (Task 16); `fbt_net::Lsd` (Task 20). Tasks 11, 20 and 24 already ported `main.rs` to `listen_addrs` and turned LSD, UPnP and NAT-PMP off; this task replaces those fixed values with the flags.
- Produces:
  - New `fbt` re-exports, each named in `lib_surface.rs`: `Transport`, `ListenError`, `StorageError`, `RateLimit`, `PeerClassLimits`, `MixedMode`, `ChokingAlgorithm`, `SeedChokingAlgorithm`, `CacheStats`, `MapProtocol`, `MapMethod` and `ExternalIpSource`.
  - Crate docs gain sections on:
    - transports (uTP first, TCP fallback, the four `enable_*` switches);
    - listen addresses and the shared UDP port;
    - rate limits and peer classes;
    - the cache (`cache_size`, flush points, the progress floor);
    - LSD and port mapping (on by default; what goes on the LAN);
    - holepunch.

    A third `no_run` doctest sets limits and subscribes to `EventMask::NAT`.
  - `fbt-example` (flags on `download` and `seed`):
    - `--transport <both|tcp|utp>` (default `both`; `utp` and `tcp` turn the other off in both directions)
    - `--listen <ADDR>`, repeatable; it replaces `--port` when given, and `--port` keeps meaning `0.0.0.0:<port>`
    - `--download-limit <BYTES/S>` and `--upload-limit <BYTES/S>`, with `K`/`M` suffixes. Giving either flag also sets `rate_limit_local_peers: true`, so the limit applies to loopback and LAN peers too (otherwise it would have no visible effect there).
    - `--lsd` (LSD is off by default in the example, so the M1/M2 interop scenarios stay unchanged)
    - `--nat` (UPnP and NAT-PMP; off by default)
    - `--cache-mb <N>`
  - **`lsd-probe`, a hidden subcommand** (Task 28 consumes it): `fbt-example lsd-probe --port <P> [--timeout-secs <S>, default 3]`. It runs a standalone `fbt_net::Lsd` that announces a fixed, well-known probe info hash (a constant in `main.rs`) with port P, and exits 0 with `lsd probe: found <addr>` as soon as it receives an `LsdPeer` for that hash from another process (own cookie ignored), or exits 1 with `lsd probe: nothing within <S> s`. Run in two containers at once, both succeed when multicast passes between them.
  - Output lines used by the interop greps:
    - `peer connected <addr> via <tcp|utp>` (extends the M2 line; the old grep `peer connected <ip>:` still matches)
    - `found <n> peers via lsd`
    - `port mapped <tcp|udp> <external> via <upnp|natpmp|pcp>`
  - `dht-node` binds through `listen_addrs`.

- [ ] **Step 1: Write failing tests** in `tests/cli.rs`:
  - `download_over_utp_from_seed_process`: `seed --transport utp --no-dht --port 0`, then `download --transport utp --peer 127.0.0.1:<port>`. Exit 0, files identical, stdout contains `via utp`.
  - `download_limit_slows_transfer`: 4 MiB at `--download-limit 1M` over loopback takes ≥ 3 s (works because the flag sets `rate_limit_local_peers`).
  - `lsd-probe` gets no test here: it multicasts on the real group, which `cargo test` must not do. Task 28 exercises it.
  - `bad_limit_is_a_clean_error`: `--download-limit 12Q` gives exit 2 (clap) with an error message.
  - `listen_flag_accepts_two_addresses`: skipped without IPv6.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-example`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test -p fbt-example -p fbt && RUSTDOCFLAGS="-D warnings" cargo doc -p fbt --no-deps`: PASS.
- [ ] **Step 5: Commit.** `feat(fbt): uTP, limits, cache, LSD and NAT in the facade and example CLI` No trailers or attribution of any kind.

---

### Task 26: fuzz targets and workflow

**Files:**
- Create: `fuzz/fuzz_targets/{utp_packet.rs,utp_conn.rs,lsd.rs,ssdp.rs,upnp_xml.rs,natpmp.rs}`, `fuzz/seeds/{utp_packet,utp_conn,lsd,ssdp,upnp_xml,natpmp}/`.
- Modify: `fuzz/Cargo.toml` (deps `fbt-utp`, `fbt-net`, `fbt-core`; six `[[bin]]`s), `fuzz/fuzz_targets/{extension.rs,mse.rs}`, `.github/workflows/fuzz.yml`.

**Interfaces:**
- Consumes:
  - `fbt_utp::packet::Packet::decode` and `fbt_utp::conn::{Connection, UtpConfig}`, including the hidden `Connection::buffered_bytes()` (Task 7: `pub` inside the hidden `conn` module);
  - `fbt_net::{LsdMessage::decode, upnp::{parse_search_response, parse_response, find_services, element_text}, natpmp::{parse_reply, Expect}}`, with `Expect::{natpmp_external, natpmp_map, pcp_map}` (Task 22);
  - `fbt_wire::{HolepunchMsg::decode, decode_donthave, decode_upload_only}` and `fbt_wire::mse_fuzzing::{respond_fixed, initiate_fixed}` with Task 2's fixed keys (initiator `[7; 20]`, responder `[9; 20]`).

- [ ] **Step 1: Write the targets.**
  - `utp_packet`: decode; on `Ok`, re-encode and decode again and assert equality.
  - `utp_conn`: one `Connection::accept` from a synthetic SYN. The input is split into length-prefixed chunks; each chunk is decoded into a packet and fed with `on_packet` at a time advanced by its first byte (ms). Then `on_timer`, `poll_transmit` until `None` and `read`. Assert that `buffered_bytes()` stays ≤ `recv_buffer`, with no panic.
  - `lsd`, `ssdp` (with a fixed `from`), `upnp_xml` (both functions plus `parse_response`), `natpmp` (the first byte selects `Expect::natpmp_external()`, `natpmp_map(..)` or `pcp_map(..)`, with fixed arguments).
  - `extension` gains selector values for holepunch, donthave and upload_only.
  - `mse`: the fuzzed responder uses `respond_fixed` with the responder key `[9; 20]`, and the fuzzed initiator uses `initiate_fixed` with `[7; 20]`, both with `pad_len` 0: exactly Task 2's keys. The seeds are the initiator and responder transcripts captured from Task 2's deterministic test, so the SKEY/`req1` sync succeeds and mutations reach past the sync search (a different key would fail the sync on every seed).

  Seeds: the Task 5 vectors plus a captured data packet with SACK, the BEP 14 example, a miniupnpd SSDP reply and `rootDesc.xml`, a SOAP fault, and the NAT-PMP and PCP replies from Task 22.
- [ ] **Step 2: Run each new or changed target for 60 s.** `cargo +nightly fuzz run <target> fuzz/corpus/<target> fuzz/seeds/<target> -- -max_total_time=60` should report no crashes. Fix any finding in the owning crate, with a regression test, before continuing.
- [ ] **Step 3: Update `fuzz.yml`.** Add the six targets to the matrix, and `crates/fbt-utp/**` and `crates/fbt-net/**` to the PR paths.
- [ ] **Step 4: Commit.** `test(fuzz): uTP, LSD, SSDP, UPnP XML and NAT-PMP targets; fixed-key MSE seeds` No trailers or attribution of any kind.

---

### Task 27: benchmark script

Spec section 7: numbers are recorded per release, not gated.

**Files:**
- Create: `dev/bench.sh`, `docs/benchmarks.md`.
- Modify: `crates/fbt-example/src/main.rs` (hidden subcommand `bench`).

**Interfaces:**
- Produces:
  - `fbt-example bench --size <MiB> --transport <tcp|utp> [--torrents <N> --idle-secs <S>]`, a hidden subcommand. In one process it runs a seed session and a leech session on loopback with `test_settings`-like values (no DHT, LSD or NAT), and prints only what it can measure itself (no FFI):
    - `bench: pid <pid>` first, so the script can sample it;
    - `bench: <transport> <MiB> MiB in <s> s = <Mbit/s> Mbit/s`;
    - with `--torrents N`, it seeds N small torrents idle for S seconds and prints `bench: idle <N> torrents for <S> s`.
  - `dev/bench.sh` builds `--release` and runs TCP and uTP at 1024 MiB plus 1000 idle torrents for 60 s. It samples the binary's pid with `ps -o time=,rss= -p <pid>` (on Linux `/proc/<pid>/stat` and `/proc/<pid>/status` work too; all FFI-free) at start and end, and computes CPU seconds and CPU percentage for each run and RSS per idle torrent (spec section 7: memory per idle torrent bounded and documented). It then appends a dated row to `docs/benchmarks.md` (version, host OS and CPU from `uname -sm`, throughput, CPU, RSS per idle torrent).
- [ ] **Step 1: Write it, then run `bash dev/bench.sh` once.** The table in `docs/benchmarks.md` gets the first row. Record the targets from spec section 7 above the table (saturate 1 Gbit/s on loopback; a few percent of a core per 100 Mbit/s; 1000 idle seeding torrents under 1 % CPU; memory per idle torrent bounded, with the measured RSS per idle torrent recorded). A miss is recorded as such, not hidden.
- [ ] **Step 2: Commit.** `chore(dev): loopback benchmark script and first recorded numbers` No trailers or attribution of any kind.

---

### Task 28: interop: uTP with qBittorrent and LSD between our processes

**Files:**
- Modify: `dev/interop/{compose.yaml,run.sh,README.md}`, `dev/interop/qbittorrent/qBittorrent.conf`, `.github/workflows/ci.yml`.

**Interfaces:**
- Consumes: the `fbt-example` flags of Task 25, and its hidden `lsd-probe` subcommand.

Setup changes:
- **A second container of ours.** A new service `fbt-lsd` from `fbt-interop-fbt:local`, long-running like `fbt`, on the same `swarm` network.
- **Multicast.** The bridge passes multicast between containers. The README records the check `docker network inspect` plus a one-shot probe, which `run.sh` runs first: `fbt-example lsd-probe --port <p> --timeout-secs 3` started at the same time in `fbt` and `fbt-lsd`; both must exit 0. If it fails, the run fails with a clear message instead of reporting a bogus LSD failure.
- **NAT stays off.** UPnP and NAT-PMP are off everywhere: our CLI defaults them off, `PortForwardingEnabled=false` is kept for qBittorrent, and the README states that no router exists on the network.
- **qBittorrent's uTP switch.** The scenario switches qBittorrent to uTP-only through `app/setPreferences` with `json={"bittorrent_protocol":2}` (0 = both, 1 = TCP, 2 = uTP), and restores 0 afterwards. Check the preference against the pinned image's WebUI (Advanced, "Peer connection protocol") and record it in the README with the equivalent conf key, `Session\BTProtocol`.

New `run.sh` scenarios, after the milestone 2 ones:
1. **uTP download from qBittorrent.**
   - Switch qBittorrent to uTP-only and record its `uploaded` for fixture 1.
   - `fbt-example download /interop/fixture.torrent --out /interop/fbt-utp --port 6895 --transport utp --no-dht` must exit 0 within 300 s with matching hashes.
   - The log must contain `peer connected <qbittorrent ip>:6882 via utp`, and qBittorrent's `uploaded` must have grown.
   - Prints `interop: uTP download OK (qbittorrent)`.
2. **uTP seed to qBittorrent.**
   - Our `seed fixture2.torrent --transport utp --port 6896` runs while qBittorrent (still uTP-only) adds fixture 2 into a fresh save path, with `torrents/addPeers` pointing it at the `fbt` container's address and port 6896.
   - Wait until qBittorrent reports progress 1 and its hashes match.
   - Restore protocol 0. Prints `interop: uTP seed OK (qbittorrent)`.
3. **LSD.**
   - `make_trackerless_torrent` gives fixture 5.
   - `fbt-lsd` runs `seed fixture5.torrent --lsd --no-dht --no-pex --port 6897` in the background.
   - `fbt` runs `download fixture5.torrent --lsd --no-dht --no-pex --out /interop/fbt-lsd --port 6898`, with no `--peer`. It must exit 0 within 300 s with matching hashes, and the log must contain `found .* peers via lsd` and `peer connected <fbt-lsd ip>:6897`.
   - Prints `interop: LSD OK`.

- [ ] **Step 1: Write the files.** Run `bash dev/interop/run.sh` locally.
- [ ] **Step 2: Verify.** Output ends with the milestone 1 and 2 lines plus `interop: uTP download OK (qbittorrent)`, `interop: uTP seed OK (qbittorrent)` and `interop: LSD OK`, and the script exits 0. `docker network inspect fbt-interop_swarm --format '{{.Internal}}'` still prints `true`.
- [ ] **Step 3: Raise the CI `interop` timeout** to 55 minutes and add the new logs to the artifacts.
- [ ] **Step 4: Commit.** `ci: interop for uTP with qBittorrent and LSD between two fbt processes` No trailers or attribution of any kind.

---

### Task 29: docs, changelog and full gate

**Files:**
- Modify: `README.md`, `CHANGELOG.md`, `CLAUDE.md`, `deny.toml` (only if a new license or advisory appears, with its one-line reason).

- [ ] **Step 1: Update the docs.**
  - `README.md`:
    - the feature list (uTP, shared UDP port, IPv6 listening, LSD, UPnP and NAT-PMP/PCP, holepunch, rate limits, choker variants, write-back cache, BEP 21/54);
    - a rate-limit example matching the `fbt` doctest;
    - a "What goes on your LAN" note: SSDP, NAT-PMP/PCP to the gateway and LSD multicast, all on by default and each with its setting;
    - a link to `docs/benchmarks.md`.
  - `CHANGELOG.md` `## [Unreleased]`, every entry ending in `([#N](https://github.com/featherbitplatform/fbt-lib/pull/N))` with `N` the milestone PR's number, filled in when the PR is opened, as milestone 2 did with #4:
    - `### Added`, per crate:
      - `fbt-utp` and `fbt-net`, new;
      - the `fbt-core::udp` traits;
      - wire `upload_only`, `lt_donthave` and `ut_holepunch`;
      - the DHT and tracker on shared sockets;
      - `fbt-core::rate::IpRateLimiter` (hoisted from fbt-dht);
      - session: uTP, listen addresses, rate limits, choker settings, cache, check queue, LSD, port mapping, holepunch, external IP and the new events;
      - example flags, `bench` and `lsd-probe`;
      - fuzz targets;
      - interop scenarios.
    - `### Changed`:
      - `listen_addr` becomes `listen_addrs`, and `dht_ipv6` is removed;
      - `Event::PeerConnected` gains `transport` and `encrypted`;
      - `fbt-wire`: `ExtHandshake` gains `upload_only` (a pub struct that is not non-exhaustive);
      - `fbt-session` `choker`: `Choker::new` takes a `ChokerConfig`, `rechoke` takes `now`, and `PeerChokeInfo` gains fields;
      - `fbt-dht`: the new `DhtError::DuplicateFamily` variant;
      - `Event::PieceFinished` now fires once the piece is verified, before it is written;
      - new `TorrentState`, `Event`, `PeerSource` and `DisconnectReason` variants;
      - the `max_outstanding_requests` default rises to 500;
      - uTP is tried first for outgoing connections;
      - pieces count as ours once verified and cached, before they are written;
      - a piece missing on disk while seeding is re-downloaded instead of stopping the torrent.
    - `### Fixed`:
      - upload counted for cancelled blocks;
      - MSE drain errors swallowed;
      - handle eviction syncing on every eviction;
      - bootstrap attempts spaced from their start;
      - metadata pieces lost when a source is retired;
      - PEX rebuilding its list every tick.
  - `CLAUDE.md` (the only task that edits it):
    - the crate list with `fbt-utp` and `fbt-net`;
    - the clippy deny list naming them;
    - the rule "tests never contact the public internet or a real router: `test_settings()` disables DHT, LSD, UPnP and NAT-PMP; NAT tests use the fakes in `crates/fbt-net/tests/support/`";
    - the fuzz target list;
    - the interop scenarios line;
    - `bash dev/bench.sh` under Commands.
- [ ] **Step 2: Run the full gate.** `cargo fmt --all --check && cargo clippy --workspace --all-targets --all-features -- -D warnings && cargo test --workspace && cargo deny --all-features check && bash dev/check-pure-rust.sh && RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps && bash dev/sast.sh`: all pass.
- [ ] **Step 3: Commit.** `docs: milestone 3 changelog, README and CLAUDE.md` No trailers or attribution of any kind.

---

## After the plan: shipping v0.3.0

PR `feature/m3-performance-reach` → `develop`. Every CI job must be green, including `interop` on all three OSes' test jobs and the fuzz runs the PR triggers. The PR body lists the decisions made on the owner's behalf (the "Decisions recorded in this plan" section, plus any taken during execution) and a "Deferred to later milestones" section. Then follow the Gitflow release:

1. `release/0.3.0` off `develop`: bump the workspace version to `0.3.0` (the peer-id prefix becomes `-FB0300-` and the KRPC `v` becomes `FB\x00\x03` automatically). Move `Unreleased` to `## [0.3.0] - <date>`, and check `bash dev/test-release-version.sh`. Run `bash dev/bench.sh` on the release commit and keep its row.
2. PR into `main`, then the annotated tag `v0.3.0`, then merge `main` back into `develop`.

No attribution lines anywhere.

## Deferred to later milestones (recorded so nobody builds them now)

- **Milestone 4 (streaming):** deadline-driven requests may want per-peer uTP priority and a read cache tuned for sequential readers. The cache's `get` and clean entries are the hook.
- **Milestone 5:** these stay there, per PR #4:
  - rename scheme for case and Unicode collisions;
  - smart ban;
  - per-IP connection limits;
  - announce `stopped` semantics;
  - SOCKS5 including UDP associate for uTP, DHT and UDP trackers, plus HTTP CONNECT. The shared socket's `DatagramSender` is the seam for a proxied sender;
  - IP filter (it will also apply to LSD, holepunch `Connect` targets and uTP accept);
  - move storage (it must flush the cache first, through `FlushReason::All`).
- **Backlog:**
  - uTP path-MTU discovery (fixed 1400/1200-byte payloads for now);
  - IPv6 port mapping and PCP firewall pinholes;
  - UPnP IGD event subscriptions;
  - per-interface LSD on multi-homed hosts (we join on the default interface only);
  - serde support for all public config structs (spec 5.3): `Settings` shipped without it in milestones 1 and 2, and this milestone's `UtpConfig`, `PortMapConfig`, `LsdConfig`, `RateLimit`, `PeerClassLimits` and `ChokerConfig` do not add it either;
  - splitting `torrent/swarm.rs` (over 700 lines, and Tasks 12, 17 to 20 grow it), left out of Task 1;
  - documentation and test polish recorded in the per-task reviews.
