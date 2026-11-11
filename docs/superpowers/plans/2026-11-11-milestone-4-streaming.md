# Milestone 4: Streaming Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `fbt-lib` `v0.4.0`. `TorrentHandle::open_file(index)` returns a `FileReader` that implements `AsyncRead + AsyncSeek` over one file of a torrent while it downloads. Reads wait for missing pieces instead of failing. The reader's position sets deadlines on a read-ahead window (bytes, or a time budget at a bitrate), and a deadline picker requests those pieces from the fastest peers first, duplicating requests that would arrive late. uTP loses the hot-path costs the milestone 3 review found. All of it is proven by streaming a file from qBittorrent, with seeks, on the isolated Docker network.

**Architecture:**
- **`fbt-utp`** keeps its sans-IO core. The in-flight window gets an O(1) index of the packets waiting to be (re)sent, payload moves as `Bytes` chunks instead of a byte-wise `VecDeque<u8>`, and `UtpStream::poll_read` initialises only what it fills.
- **`fbt-session` streaming, reader side.** A new `stream` module holds the public `FileReader`, `ReadAhead` and `StreamError`, and the pure window function that turns a position into `(piece, deadline)` pairs. The reader talks to the torrent actor only through `Cmd`s and one shared `ReaderLink` per reader (an atomic position the actor reads; a coalescing nudge flag, so a seek storm queues one command).
- **`fbt-session` streaming, actor side.** `torrent/readers.rs` answers reads from the write-back cache, the disk pool or the hash pass, and keeps the waiters. `torrent/deadline.rs` holds the merged deadlines of every reader. `fill()` hands out deadline blocks before the normal pick, and `PiecePicker::pick_piece` gives the actor per-piece control (start a priority-0 piece, take a second owner).
- The read cache learns sequential readers: pieces just behind a reader are demoted to the front of the clean LRU, and a couple of pieces ahead are prefetched from disk.

The milestone starts with the uTP work, then the `torrent/swarm.rs` split the milestone 3 plan deferred, so the streaming tasks edit files of a workable size.

**Tech Stack:** as milestone 3. No new third-party crate. `bytes` (`Bytes::from_owner`, `BytesMut`), `tokio` (`AsyncRead`, `AsyncSeek`, `oneshot`, `ReadBuf::initialize_unfilled_to`), `tracing`, `thiserror`.

**Spec:** `docs/superpowers/specs/2026-11-10-fbt-lib-design.md` (binding). Milestone 4 is its section 8 item 4. Inputs:
- section 2 goal 3, the section 4 Streaming and Piece picking rows, section 5.3 (the `open_file` contract), section 5.5 (read-ahead, deadlines, fastest peers, duplicates near the deadline, seek cancels), section 7 (performance targets, simulation of streaming deadlines);
- the milestone 3 final review (`.superpowers/sdd/2026-11-10-milestone-3-performance-reach/final-review.md`, finding I2 and "Performance misses");
- the milestone 3 plan's Deferred list and the "Parked" lines of the milestone 3 ledger (`progress.md`), folded in where this milestone touches the code.

## Global Constraints

- From scratch: no BitTorrent, uTP or streaming crates. General-purpose crates only, declared once in the root `[workspace.dependencies]`; no new dependency is expected in this milestone.
- `#![forbid(unsafe_code)]` in every crate. `ReadBuf::uninit` and `initialize_unfilled_to` are the safe tokio APIs; nothing here needs `MaybeUninit` tricks.
- No panic, hang or unbounded allocation from remote input. `fbt-utp` stays in the clippy deny block (`unwrap_used`, `expect_used`, `indexing_slicing`, `panic` in non-test code). Reader-side values are local, but they are still bounded: every per-reader and per-torrent collection here has a named cap constant (`MAX_READERS`, `MAX_DEADLINE_PIECES`, `MAX_READ_CHUNK`, `READ_PREFETCH_PIECES`).
- `bash dev/check-pure-rust.sh` passes after every task that touches a `Cargo.toml` (none is planned).
- **No test and no CI job contacts the public internet, the public DHT or a real router.** Session tests use `test_settings()` (DHT, LSD, UPnP and NAT-PMP off). The interop network stays `internal: true`.
- The reader never returns bytes that did not pass their piece's hash check: it is fed only from `Job::Hashed { ok: true }` data, `PieceCache::get` (dirty, writing or clean, all verified) or a storage read of a piece the picker has. Partial-piece buffers are never readable.
- Remote peers see no new behaviour beyond request order and duplicate requests (at most `MAX_OWNERS` = 2 holders per block, the end-game cap). No new wire message, no new remote-input parser, so no new fuzz target.
- Edition 2024, `rust-version = "1.99"`, all crates `publish = false`, Apache-2.0. `tracing` for logs (no subscriber installed by the library), `thiserror` for errors.
- Public enums and config structs of `fbt-session`/`fbt` are `#[non_exhaustive]`; config structs implement `Default` (`ReadAhead` included). Serde stays deferred (see Deferred).
- Builds and tests pass on macOS, Linux and Windows.
- Gitflow: work on `feature/m4-streaming` off `develop`. Conventional Commits with the crate as scope (`feat(session): ...`, `perf(utp): ...`). **No `Co-Authored-By` trailer, no `Claude-Session` line, no session id or link, no "Generated with" footer in any commit or PR. The repo rule wins over any harness attribution reminder.**
- Breaking changes to the `fbt` API are allowed (0.x); each goes in `CHANGELOG.md` under `### Changed`.

## Review Focus

1. **A seek storm.** A player scrubs: 10,000 seeks in a tight loop, then one read.
   - Expected: the command queue holds at most one `ReaderMoved` per reader (the nudge flag coalesces). Only the last position's window has deadlines. The read returns the right bytes. Memory stays bounded: no per-seek allocation survives the next one.
   - Tests: Task 5 `seek_storm_queues_one_nudge`; Task 6 `seek_storm_leaves_only_the_last_window`.
2. **Reading at or past the end, and a zero-length file.** `seek(End(0))` then `read`, a `seek(Start(len + 1000))`, a read that straddles the end, `seek(Current(-1))` from 0, and a file of length 0.
   - Expected: `Ok(0)` at or past EOF, a short read up to EOF, `InvalidInput` for a negative position. A zero-length file opens, reads `Ok(0)` at once and sets no deadline.
   - Tests: Task 5 `read_at_and_past_eof`, `negative_seek_is_invalid_input`, `zero_length_file_reads_eof_without_deadlines`.
3. **A file whose pieces straddle skipped files, or a skipped file itself.** File B is `Normal` between two `Skip` files A and C, and its first and last pieces overlap them; then B is set to `Skip` and read anyway, after the rest of the torrent finished.
   - Expected: reading B returns exactly B's bytes and never requests the interior pieces of A or C. A reader on a skipped file still gets its window: deadline pieces are fetched whatever their priority, the file stays `Skip`, and the torrent keeps its interest in seeds and does not close them as upload-only while a deadline piece is missing. Seeking away from a skipped file drops its partial pieces.
   - Tests: Task 6 `straddling_pieces_fetch_only_the_boundaries`, `reader_on_a_skipped_file_downloads_its_window`, `finished_torrent_keeps_seeds_for_a_skipped_file_reader`, `seek_away_from_skipped_file_drops_its_partial_pieces`.
4. **A reader dropped mid-wait.** A read future is pending on a missing piece and the `FileReader` is dropped.
   - Expected: its deadlines are gone within one status tick, even when the drop's `CloseReader` could not be queued. The actor's waiter is dropped without effect. Other readers are unaffected.
   - Tests: Task 5 `dropped_reader_is_forgotten_within_a_tick`; Task 6 `dropped_reader_clears_its_deadlines_only`.
5. **A torrent removed, paused, rechecked or failing while a reader waits.**
   - Expected: remove fails the pending read with `StreamError::TorrentRemoved` (`ErrorKind::NotFound`), session shutdown with `StreamError::Shutdown`; a storage error fails it with `StreamError::TorrentError`. Pause serves pieces we have and leaves missing ones waiting until resume. A recheck keeps waiters and answers them after the check.
   - Tests: Task 5 `remove_while_waiting_fails_the_read`, `shutdown_while_waiting_fails_the_read`, `storage_error_fails_waiting_reads`, `pause_serves_have_pieces_and_waits_for_missing`, `recheck_keeps_waiters`.
6. **A magnet with no metadata yet.**
   - Expected: `open_file` returns `SessionError::NoMetadata` at once, like `set_file_priorities`. After `MetadataReceived` it works. A file index out of range or naming a pad file is `SessionError::InvalidFileIndex`.
   - Tests: Task 5 `open_file_on_magnet_without_metadata`, `open_file_index_checks`.
7. **A deadline piece whose only peer chokes us.** The one seed holding the window chokes us mid-piece (with and without the fast extension, and a fast peer that never sends its `Reject`s).
   - Expected: its outstanding deadline blocks become late at once and are duplicated to any other peer that has the piece. With no other peer the read keeps waiting (no error, no busy loop) and completes after the unchoke.
   - Tests: Task 6 `choked_deadline_blocks_go_to_another_peer`, `only_peer_chokes_then_unchokes`.
8. **Unverified data near a reader.** A peer sends a corrupt piece inside the window; the write-back cache holds the piece dirty while the storage delays writes.
   - Expected: the reader never sees the corrupt bytes, gets the verified piece from the second download, and gets a dirty piece from memory before it is written.
   - Tests: Task 5 `reader_never_sees_unverified_bytes`, `waiting_read_is_answered_from_the_hash_pass`.

---

## Starting point

`develop` holds milestone 3 (`v0.3.0`, merged back from `release/0.3.0`, `a4cc7a9`). The branch `feature/m4-streaming` exists off `develop` and holds only this plan. Check first that the workspace version is `0.3.0` and `cargo test --workspace` is green.

## Decisions recorded in this plan

The owner set the scope in the milestone 4 brief: uTP performance first (milestone 3 finding I2), then the seekable reader, read-ahead in bytes and as a time budget at a bitrate, the deadline picker (fastest peers first, duplicates near the deadline, seeking cancels old deadlines), several readers per torrent, the interactions with priorities, pause, remove, recheck, cache, end-game, choker and rate limits, the facade, an `fbt-example stream` subcommand, an interop scenario with seeks against qBittorrent, and version `0.4.0`.

The builder made these calls (they can be revisited by whoever plans a later milestone):
- **Name.** The reader is `FileReader` (`fbt::FileReader`). It reads one file; the HTTP byte-range server of milestone 7 wraps it.
- **Read-ahead defaults: `ReadAhead { bytes: 16 MiB, time: 10 s, bitrate: None }`.** libtorrent has no read-ahead default: the application calls `set_piece_deadline` per piece, and libtorrent's time-critical logic then requests those pieces from the peers with the shortest download queue and re-requests ones that run late. So the window size is ours to choose. The window is `max(bytes, bitrate × time)`. Without a bitrate the implied one is `bytes / time` = 1.6 MiB/s (about 13 Mbit/s), above a typical 1080p stream (5 to 10 Mbit/s) and below 4K; a client that knows the bitrate passes it. 16 MiB is a quarter of the default 64 MiB `cache_size`, so a full window of verified pieces fits in the cache. Each reader's window is capped at `MAX_DEADLINE_PIECES` = 512 pieces.
- **Deadlines slide with the reader.** They are recomputed, relative to now, on every reader move and every status tick: piece `i` of the window gets `now + (start(i) - pos) / rate`, the piece under the position gets `now`. A paused player therefore never makes the whole window overdue.
- **Fastest peers first, in the pull model.** Peers ask for blocks (`WantBlocks`); `fill(key)` hands out deadline blocks before the normal pick. A peer takes free blocks of a deadline piece only if its estimated arrival time for one more block, `(requested + 1) × 16 KiB / max(rate_from_peer, 16 KiB/s)`, is within the piece's time left, or it is the fastest live, unchoking peer that has the piece. `top_up_all` fills peers in ascending estimated arrival while any deadline is set.
- **Duplicates.** A deadline piece's outstanding block is *late* when its holder's estimated arrival exceeds the time left, or the holder chokes us or is snubbed. A late block may be given to a second peer whose estimated arrival is shorter, never to more than `MAX_OWNERS` = 2 holders (the end-game cap, shared). The first copy to arrive cancels the other, through the existing `BlockResult::cancel`.
- **Seeking.** A seek replaces that reader's deadlines. Pieces that leave every window continue at their normal priority with their requests in flight (spec 5.5). A priority-0 piece that leaves every window is abandoned: its outstanding requests are cancelled and its buffer freed, since nothing else would ever finish it.
- **Priorities.** Deadlines override piece priority 0, so a reader on a `Skip` file gets its window and nothing more; the file's priority is not changed. While a deadline piece is missing, the torrent stays interested in peers that have it and keeps seeds connected (no BEP 21 upload-only close), even when every wanted piece is done.
- **Magnets.** `open_file` before the metadata returns `SessionError::NoMetadata`, as `set_file_priorities` does: the file index means nothing before the file list exists.
- **Errors.** A read fails only when the torrent is removed (`StreamError::TorrentRemoved`, `ErrorKind::NotFound`), the session shuts down (`StreamError::Shutdown`, `ErrorKind::ConnectionAborted`) or the torrent is in `TorrentState::Error` (`StreamError::TorrentError(message)`, `ErrorKind::Other`). The reader is not poisoned: after `resume` the next read works. There is no read timeout; callers wrap reads in `tokio::time::timeout`.
- **Pause and recheck.** Pause serves pieces we have and leaves reads of missing pieces waiting until resume. A recheck keeps the waiters and answers them once the check says the piece is there (or once it is downloaded again).
- **Several readers.** At most `MAX_READERS` = 16 per torrent (`SessionError::TooManyReaders` beyond). Their deadlines merge by the earliest deadline per piece.
- **Read path and cache.** The reader asks for one chunk at a time, at most `MAX_READ_CHUNK` = 1 MiB and never across a piece boundary, answered as a zero-copy `Bytes` slice of the cached piece (`Bytes::from_owner`). A disk read for a reader reads the whole piece when it is at most 4 MiB (`WHOLE_PIECE_READ_MAX`) and keeps it clean when the budget allows; larger pieces are read chunk by chunk. After a reader moves on from a piece, that piece is demoted to the front of the clean LRU unless another reader's window holds it, and up to `READ_PREFETCH_PIECES` = 2 pieces we have ahead of it are read into the clean cache when the budget allows.
- **Choker and rate limits are unchanged.** The choker governs our uploads and streaming is a download concern; deadline blocks count against every rate limit like any other block, so a limit below the stream's bitrate makes reads wait. Both are documented on `FileReader`.
- **Per-peer uTP priority is not built.** LEDBAT is sender-side: as the receiver of deadline blocks we cannot make a remote's uTP sender yield less, and our receive side already acks every second packet and advertises its full window. The deadline picker reaches the intended effect by choosing peers by measured rate, whatever their transport. Recorded under Deferred with the condition that would revive it.
- **uTP targets.** On the host of the 0.3.0 row (Darwin arm64), `dev/bench.sh` must show uTP at **at least 1000 Mbit/s** and **at most 15 % of one core per 100 Mbit/s** (seed and leech in one process; 0.3.0: 603 Mbit/s and 39.9 %). The remaining gap to "a few percent" is the one syscall per 1.4 KiB datagram and the single receive task per socket; batching (`sendmmsg`/`recvmmsg`-style) is on the Deferred list.
- **Parked items folded in:** the `torrent/swarm.rs` split (Task 2), the rate meter's warm-up that starts the request pipeline shallow and the 5 s redial loop without backoff (Task 3), the uTP decode-failure trace and the half-open eviction order in `socket.rs` (Task 1), `INTEROP_ONLY` printing "all scenarios passed" and ignoring unknown names (Task 8). Everything else parked is on the Deferred list.
- **No fuzz target.** This milestone adds no parser of remote input; Task 1 re-runs `utp_packet` and `utp_conn` because it rewrites the code they reach.
- Version `0.4.0`; changelog entries go under `## [Unreleased]`.

---

## File Structure

```
crates/fbt-utp/src/conn.rs                        pending-send index, Bytes payload, check_invariants (hidden)
crates/fbt-utp/src/conn/tests.rs                  index and chunk tests
crates/fbt-utp/src/stream.rs                      poll_read initialises only what it fills
crates/fbt-utp/src/socket.rs                      decode-failure trace, half-open eviction after the re-check
crates/fbt-utp/tests/{sim.rs,socket.rs}           invariants in the simulation, uninit read test
crates/fbt-core/src/layout.rs                     FileLayout::file_range
crates/fbt-session/src/
  rate.rs                                         warm-up divisor
  settings.rs, error.rs, lib.rs                   read_ahead, InvalidFileIndex, TooManyReaders, exports
  picker.rs                                       pick_piece, progress, abandon
  storage/cache.rs                                demote, is_cached
  stream/{mod.rs,reader.rs,window.rs}             FileReader, ReadAhead, StreamError, ReaderLink, window()
  torrent/{known.rs,retries.rs,peer.rs}           split out of swarm.rs (Task 2); redial backoff in retries.rs
  torrent/swarm.rs                                what stays: connecting, sending, closing
  torrent/deadline.rs                             Deadlines (pure)
  torrent/readers.rs                              actor side: reader commands, waiters, prefetch, deadline fill
  torrent/{mod.rs,actor.rs,commands.rs,data.rs,serve.rs,peers.rs,check.rs}   wiring
crates/fbt-session/tests/stream.rs                integration tests
crates/fbt/src/lib.rs, crates/fbt/tests/lib_surface.rs
crates/fbt-example/src/main.rs, crates/fbt-example/tests/cli.rs   `stream`, `bench --stream`
dev/bench.sh, docs/benchmarks.md
dev/interop/{run.sh,README.md}
README.md, CHANGELOG.md, CLAUDE.md
```

---

### Task 1: uTP hot path: O(1) send index, chunked payload, no zero-fill

Milestone 3 final review, I2. The behaviour on the wire does not change.

**Files:**
- Modify: `crates/fbt-utp/src/conn.rs` (the `Connection` send and receive buffers, `has_pending_send` at :453, `transmit_outstanding` at :980, `packetize` at :1013, `push_outstanding` at :869, `process_ack` at :640, `read`/`write` at :392-418, `receive_data`/`advance_in_order` at :769-840), `crates/fbt-utp/src/conn/tests.rs`, `crates/fbt-utp/src/stream.rs:105`, `crates/fbt-utp/src/socket.rs` (`handle` at :501, `incoming_syn` at :583), `crates/fbt-utp/tests/{sim.rs,socket.rs}`, `fuzz/fuzz_targets/utp_conn.rs`, `docs/benchmarks.md` (the "Known defects" section).

**Interfaces:**
- Produces (crate-internal unless noted; the public `Connection` API keeps its signatures):
  - **Ordinals.** Every packet pushed to `outstanding` gets `ord: u64`, increasing by one per packet; `Connection` keeps `front_ord: u64`, the ordinal of `outstanding.front()`. The index of a packet is `ord - front_ord`.
  - **`to_send: BTreeSet<u64>`**: the ordinals of packets with `lost && !acked`. New packets enter it at `push_outstanding` (they keep `lost: true`), `transmit_outstanding` takes `to_send.first()`, an ack or SACK removes the ordinal, loss detection and the RTO insert it. `has_pending_send` and the guard in `packetize` read `!to_send.is_empty()`.
  - **`live_packets: usize`**: packets neither acked nor lost. `process_ack`'s RTO re-arm and `detect_loss`'s "nothing in flight" check read it instead of scanning.
  - **Payload.** `unsent: BytesMut` (`write` is `extend_from_slice`; `packetize` is `split_to(len).freeze()`). `inbox: VecDeque<Bytes>` plus `inbox_len: usize`; `read` copies with `copy_from_slice` chunk by chunk and drops or advances the front chunk. `ooo: BTreeMap<u16, Bytes>` stores `p.payload.clone()` (a slice of the datagram, no copy). `buffered_bytes` and `readable_bytes` read `inbox_len`.
  - `#[doc(hidden)] pub fn check_invariants(&self) -> Result<(), String>` on `Connection`: recomputes `to_send`, `live_packets`, `flight`, `unacked_bytes`, `inbox_len` and `ooo_bytes` with full scans and reports the first mismatch. Tests and fuzzing call it; production code never does.
  - `UtpStream::poll_read`: when `readable_bytes()` is 0 it reads nothing; otherwise it reads into `buf.initialize_unfilled_to(buf.remaining().min(readable))`.
  - `socket.rs` `handle`: a datagram that fails `Packet::decode` emits `tracing::trace!(%from, len, error = %e, "uTP datagram failed to decode")`, at most once per second per socket; the suppressed count goes in the next event's `suppressed` field.
  - `socket.rs` `incoming_syn`: the half-open victim is evicted only after the final re-check under the table lock passed, chosen again under that lock, so a SYN that loses the re-check evicts nobody.
- The one copy per datagram (`Bytes::copy_from_slice` in `handle`) stays: `DatagramHandler` hands out `&[u8]`, and that copy is now the only one on the receive path.

- [ ] **Step 1: Write failing tests.**
  - `conn/tests.rs`:
    - `pending_index_matches_a_full_scan`: two connections joined by a deterministic loop that drops every 7th datagram and reorders pairs. 2 MiB each way. `check_invariants()` is `Ok` on both sides after every `on_packet`, `on_timer` and `poll_transmit`; the bytes arrive intact.
    - `read_spans_chunks`: three DATA packets of 1000, 1, 1400 bytes arrive in order; `read` with buffers of 1, 999, 5000 and 4096 bytes returns the concatenation exactly, and `readable_bytes()` falls to 0.
    - `write_takes_at_most_send_room`: `write` of 2 × `send_buffer` returns `send_buffer`; after packetizing, the outstanding payloads concatenate to the written bytes.
  - `tests/sim.rs`: `transfer_survives_loss_reorder_and_duplication` calls `check_invariants()` on both connections after every event.
  - `tests/socket.rs`: `poll_read_initialises_only_what_it_fills`: a 100-byte message read into `ReadBuf::uninit` over a `[MaybeUninit<u8>; 16384]` leaves `buf.initialized().len() == 100` and `buf.filled() == message`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-utp`: `check_invariants` and the initialised-length assertion do not exist or fail.
- [ ] **Step 3: Implement** as in Interfaces.
- [ ] **Step 4: Run.** `cargo test -p fbt-utp && cargo clippy -p fbt-utp --all-targets -- -D warnings`: PASS. Then `cargo +nightly fuzz run utp_packet ... -- -max_total_time=60` and the same for `utp_conn` (the target gains a `check_invariants()` assertion after each step): no crash.
- [ ] **Step 5: Measure.** `BENCH_NO_RECORD=1 bash dev/bench.sh` on the 0.3.0 host: uTP ≥ 1000 Mbit/s and ≤ 15 % of a core per 100 Mbit/s, TCP no worse than 0.3.0 (5238 Mbit/s, 4.9 %). Rewrite the "Known defects" section of `docs/benchmarks.md` as "Fixed in 0.4.0" with the measured numbers and the remaining inherent costs; the release row is appended at release time, not here. If a target is missed, record the miss in that section with a profile summary and report the task `DONE_WITH_CONCERNS`; do not tune further inside this task.
- [ ] **Step 6: Interop check.** `INTEROP_ONLY=utp bash dev/interop/run.sh` passes (the uTP download and seed with qBittorrent).
- [ ] **Step 7: Commit.** `perf(utp): O(1) send index, chunked payload buffers and no zero-fill per read` No trailers or attribution of any kind.

---

### Task 2: split `torrent/swarm.rs`

Deferred by milestone 3 (1569 lines, 1174 of them non-test). The task moves code and changes no behaviour.

**Files:**
- Create: `crates/fbt-session/src/torrent/{known.rs,retries.rs,peer.rs}`.
- Modify: `crates/fbt-session/src/torrent/{swarm.rs,mod.rs}` and every `use super::swarm::...` that names a moved item (`actor.rs`, `peers.rs`, `data.rs`, `check.rs`, `pex.rs`, `holepunch.rs`, `magnet.rs`, `commands.rs`, `serve.rs`).

**Interfaces:**
- Produces (all `pub(super)` as today):
  - `known.rs`: `KnownPeer`, `Known` and its methods, `MAX_KNOWN`, `dialable`.
  - `retries.rs`: `Retries` and its methods, `AfterFailure`, `MAX_ATTEMPTS`, `FIRST_RETRY_DIVISOR`, `MAX_NEVER`.
  - `peer.rs`: `Peer` and its methods.
  - `swarm.rs` keeps `keep_new_connection`, `ConnectOutcome`, `BanTarget` and the `impl Actor` block (connecting, accepting, sending, closing, `on_connected`, `on_disconnected`).
  - Tests move with the code they test; the shared `tests::actor()` helper stays in `swarm.rs` at its current path `pub(in crate::torrent)`.
- No non-test part of a file this task touches exceeds about 700 lines.

- [ ] **Step 1: Move the code.** `git mv` is not possible for a split; move by cut and paste, keeping doc comments, and change visibilities only as the split needs.
- [ ] **Step 2: Run.** `cargo test -p fbt-session && cargo clippy -p fbt-session --all-targets -- -D warnings`: PASS, with the same test count before and after (`cargo test -p fbt-session 2>&1 | grep -c '\.\.\. ok'`).
- [ ] **Step 3: Commit.** `refactor(session): split torrent/swarm.rs into known, retries and peer` No trailers or attribution of any kind.

---

### Task 3: rate meter warm-up and redial backoff

Two parked items from the milestone 3 uTP stall report.

**Files:**
- Modify: `crates/fbt-session/src/rate.rs`, `crates/fbt-session/src/torrent/{retries.rs,swarm.rs}`.

**Interfaces:**
- Consumes: `Retries`, `Peer::connected_at` (Task 2 paths).
- Produces:
  - `RateMeter::rate(now)` divides by `min(seconds since the meter started + 1, 5)` instead of always 5, so a fresh meter does not under-report its first five seconds (and the request pipeline, sized from it, no longer starts shallow). After 5 s nothing changes.
  - `Retries::dropped(&mut self, addr: SocketAddr, now: Instant, connected_for: Duration)`: replaces the bare `failed()` call in `on_disconnected`'s "productive peer dropped" branch. It keeps `drops: HashMap<SocketAddr, u32>` (bounded by `MAX_NEVER`, oldest forgotten, as the other maps). A connection that lasted at least `STABLE_CONNECTION` = 5 min resets the count first. Drop `n` schedules the redial after `peer_retry_delay / FIRST_RETRY_DIVISOR` (5 s) for `n == 1`, then `peer_retry_delay × (n − 1)`, capped at `MAX_REDIAL_DELAY` = 30 min (libtorrent scales a 60 s minimum by the fail count). `connected()` does not reset `drops`.

- [ ] **Step 1: Write failing tests.**
  - `rate.rs` (paused time): `fresh_meter_is_not_diluted`: record 10,000 at t0; `rate` at t0 + 0.5 s is 10,000, at t0 + 1.5 s is 5,000; after 6 s idle it is 0. Update `rate_meter_window` to the new divisor (1000 at 0 s and 4000 at 1 s read 2,500 at 1 s; the values from 5 s on are unchanged).
  - `retries.rs`: `repeated_drops_back_off`: drops at 0, 10 s, 20 s, 30 s with `connected_for` 1 s give redials due after 5 s, 60 s, 120 s and 180 s (default `peer_retry_delay` 60 s); `drops_are_capped_at_thirty_minutes` (100 drops); `a_stable_connection_resets_the_count` (`connected_for` 5 min gives 5 s again); `drop_map_is_bounded` (`MAX_NEVER + 10` addresses keep `MAX_NEVER`).
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session rate:: retries::`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test -p fbt-session`: PASS, including milestone 3's `tests/utp.rs` redial tests.
- [ ] **Step 5: Commit.** `fix(session): rate meter warm-up and backoff for peers that keep dropping` No trailers or attribution of any kind.

---

### Task 4: picker and cache hooks, deadlines and the window function

Pure building blocks, each with unit tests; nothing calls them yet except tests.

**Files:**
- Create: `crates/fbt-session/src/torrent/deadline.rs`, `crates/fbt-session/src/stream/{mod.rs,window.rs}`.
- Modify: `crates/fbt-core/src/layout.rs`, `crates/fbt-session/src/{picker.rs,lib.rs,settings.rs}`, `crates/fbt-session/src/storage/cache.rs`, `crates/fbt-session/src/torrent/mod.rs`.

**Interfaces:**
- Produces:
  - `fbt_core::FileLayout::file_range(&self, index: usize) -> Option<Range<u64>>`: the file's byte range in the concatenated content (`None` out of range; an empty range for a zero-length file).
  - **Public config** (`fbt_session::stream`, re-exported at the crate root):
    ```rust
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[non_exhaustive]
    pub struct ReadAhead {
        /// Least window, in bytes (16 MiB).
        pub bytes: u64,
        /// Time budget at `bitrate` (10 s). Non-zero.
        pub time: Duration,
        /// The stream's bytes per second, when the client knows it; `None` uses `bytes / time`.
        pub bitrate: Option<u64>,
    }
    ```
    with `Default` as above and `ReadAhead::new(bytes, time, bitrate)`. `Settings::read_ahead: ReadAhead` (default `ReadAhead::default()`); `Settings::validate` rejects a zero `read_ahead.time` and a `Some(0)` bitrate (`"read_ahead.time must be greater than zero"`, `"read_ahead.bitrate must be greater than zero"`).
  - `pub(crate) const MAX_DEADLINE_PIECES: usize = 512;`
  - Every `Instant` in this task and Tasks 5 and 6 is `tokio::time::Instant`, as in the actor, so paused-time tests drive deadlines.
  - `stream/window.rs`: `pub(crate) fn window(file: Range<u64>, pos: u64, layout: &PieceLayout, ra: &ReadAhead, now: Instant) -> Vec<(u32, Instant)>`. `pos` is torrent-global. Empty when the file is empty or `pos >= file.end`. Otherwise the pieces covering `[pos, min(pos + W, file.end))` with `W = max(ra.bytes, bitrate × time)` saturating, at most `MAX_DEADLINE_PIECES`, in index order; the piece holding `pos` gets `now`, piece `i` after it gets `now + (piece_offset(i) - pos) / rate` with `rate = bitrate.unwrap_or(max(1, bytes / time.as_secs_f64()))`.
  - `torrent/deadline.rs`, `pub(crate) struct ReaderId(pub u64)` (`Copy`, `Eq`, `Hash`, `Ord`, `Debug`) and:
    ```rust
    pub(super) struct Deadlines { /* per reader: Vec<(u32, Instant)>; merged: HashMap<u32, Instant> */ }
    impl Deadlines {
        /// Replaces `reader`'s window; returns the pieces no reader holds any more.
        pub fn set(&mut self, reader: ReaderId, window: Vec<(u32, Instant)>) -> Vec<u32>;
        /// Forgets a reader; returns the pieces no reader holds any more.
        pub fn remove(&mut self, reader: ReaderId) -> Vec<u32>;
        pub fn deadline(&self, piece: u32) -> Option<Instant>;   // earliest over readers
        pub fn contains(&self, piece: u32) -> bool;
        /// Every piece with a deadline, earliest first (ties by index).
        pub fn ordered(&self) -> Vec<(u32, Instant)>;
        /// The piece passed: dropped from every window until the next `set`.
        pub fn piece_done(&mut self, piece: u32);
        pub fn holders(&self, piece: u32) -> usize;              // readers whose window has it
        pub fn is_empty(&self) -> bool;
    }
    ```
  - `PiecePicker` additions:
    - `pub fn pick_piece(&mut self, peer: PeerKey, piece: u32, max_blocks: usize, duplicate: bool) -> Vec<BlockRef>`: free blocks of `piece`, starting it if `Unrequested` whatever its priority (0 included); with `duplicate`, then blocks held by exactly one other peer and not by `peer` (never past `MAX_OWNERS`). Nothing for a piece we have, one pending its hash check, or out of range. A started priority-0 piece never enters `partial` or the buckets, so the normal pick never continues it.
    - `#[derive(Debug, Clone, PartialEq, Eq)] pub struct PieceProgress { pub free: usize, pub owners: Vec<PeerKey> }` and `pub fn progress(&self, piece: u32) -> Option<PieceProgress>`: `None` for a piece we have, pending its hash or out of range; `owners` are the peers holding an unreceived block, sorted, deduplicated.
    - `pub fn abandon(&mut self, piece: u32) -> Vec<(PeerKey, BlockRef)>`: drops the piece's download progress like `piece_failed` and returns the blocks that were outstanding, for the caller to cancel.
  - `PieceCache` additions: `pub(crate) fn demote(&mut self, piece: u32)` (a clean entry of this torrent becomes the session's next eviction; dirty or writing pieces are unaffected) and `pub(crate) fn is_cached(&self, piece: u32) -> bool` (dirty, writing or clean, without touching the LRU order). `CleanLru` starts `next_use` at `1 << 62` and keeps a `next_cold` counter going down from `(1 << 62) - 1`, so demoted entries sort before every used one, most recently demoted last among them.

- [ ] **Step 1: Write failing tests.**
  - `layout.rs`: `file_range_of_each_file` (lengths 10, 0, 5: `0..10`, `10..10`, `10..15`, `None` for 3).
  - `window.rs` (piece length 1 MiB, 64 MiB file starting at torrent offset 3 MiB + 100):
    - `default_window_covers_sixteen_mib`: at the file's start, 17 pieces (the first straddles), the first deadline `now`, the next one `now + (1 MiB − 100) / 1.6 MiB/s` within 1 ms;
    - `bitrate_extends_the_window`: bitrate 4 MiB/s × 10 s gives 40 MiB;
    - `window_stops_at_the_file_end` and `empty_at_or_past_the_end`;
    - `window_is_capped`: piece length 16 KiB, 64 MiB window gives `MAX_DEADLINE_PIECES`;
    - `huge_values_saturate`: `bytes: u64::MAX`, `bitrate: Some(u64::MAX)` neither panics nor exceeds the cap.
  - `deadline.rs`: `earliest_deadline_wins_across_readers`, `set_returns_pieces_left_by_everyone` (two readers overlap; moving one returns only pieces the other does not hold), `remove_returns_orphans`, `ordered_is_by_deadline_then_index`, `piece_done_drops_it_from_every_window`.
  - `picker.rs`:
    - `pick_piece_starts_a_skipped_piece`: priority 0 piece 3 is started by `pick_piece`, then `pick` from any peer never returns its blocks;
    - `pick_piece_duplicates_once`: peer A holds all blocks; `pick_piece(B, .., true)` returns them, `pick_piece(C, .., true)` returns none; `on_block` from B cancels A;
    - `progress_lists_owners_and_free`;
    - `abandon_returns_outstanding_and_requeues`: a normal-priority piece goes back to the buckets; a priority-0 one does not;
    - `random_operations_keep_invariants` gains `pick_piece` and `abandon` operations.
  - `cache.rs`: `demoted_entry_is_evicted_first`, `demote_leaves_dirty_pieces_alone`, `is_cached_does_not_touch_the_lru`.
  - `settings.rs`: `zero_read_ahead_time_is_invalid`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-core -p fbt-session`
- [ ] **Step 3: Implement.** Mark the not-yet-called items `#[cfg_attr(not(test), expect(dead_code, reason = "wired up in Task 5/6"))]` as milestone 3 did, and remove those attributes in the task that calls them.
- [ ] **Step 4: Run.** `cargo test -p fbt-core -p fbt-session && cargo clippy --workspace --all-targets -- -D warnings`: PASS.
- [ ] **Step 5: Commit.** `feat(session): deadline set, read-ahead window, picker and cache hooks for streaming` No trailers or attribution of any kind.

---

### Task 5: `FileReader`: open, read, seek and wait

Reads work end to end, waiting for the normal download; deadlines are recorded but not yet used for picking (Task 6).

**Files:**
- Create: `crates/fbt-session/src/stream/reader.rs`, `crates/fbt-session/src/torrent/readers.rs`, `crates/fbt-session/tests/stream.rs`.
- Modify: `crates/fbt-session/src/stream/mod.rs`, `crates/fbt-session/src/{error.rs,lib.rs}`, `crates/fbt-session/src/torrent/{mod.rs,actor.rs,commands.rs,data.rs,serve.rs,check.rs}`, `crates/fbt-session/tests/support/mod.rs` (`CountingStorage`, counting reads; `FailWritesStorage`, failing writes while a shared `AtomicBool` is set).

**Interfaces:**
- Consumes: Task 4's `window`, `Deadlines`, `ReaderId`, `ReadAhead`, `PieceCache::{demote, is_cached}`, `FileLayout::file_range`.
- Produces (public):
  - `SessionError::InvalidFileIndex { index: usize, files: usize }` (out of range or a pad file) and `SessionError::TooManyReaders`.
  - `#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)] #[non_exhaustive] pub enum StreamError { TorrentRemoved, Shutdown, TorrentError(String) }`, carried inside the `io::Error` a read returns, with the kinds listed under Decisions; `err.get_ref()` downcasts to it.
  - `TorrentHandle::open_file(&self, index: usize) -> Result<FileReader, SessionError>` (with `Settings::read_ahead`) and `open_file_with(&self, index: usize, read_ahead: ReadAhead) -> Result<FileReader, SessionError>`. Errors: `NoMetadata`, `InvalidFileIndex`, `TooManyReaders`, `InvalidSettings` for a zero `time` or `Some(0)` bitrate, `TorrentNotFound`/`Shutdown` as the other commands.
  - `pub struct FileReader` (`Send`, `Unpin`, `Debug`; not `Clone`): `impl AsyncRead`, `impl AsyncSeek`, and `len(&self) -> u64`, `position(&self) -> u64`, `index(&self) -> usize`, `set_read_ahead(&mut self, ReadAhead)` (applied with the next nudge). Seeks are local: `start_seek` computes the new position (`InvalidInput` below 0, saturating above), drops a pending fetch for another chunk, stores the position in the link and nudges; `poll_complete` is ready at once. A read at or past `len` returns `Ok(0)` without a command. Otherwise the reader asks for `[pos, pos + min(MAX_READ_CHUNK, piece end, file end))`, keeps the returned `Bytes` as its current chunk and serves later reads from it while they fall inside it.
- Produces (crate-internal):
  - `stream/reader.rs`: `pub(crate) struct ReaderLink { pos: AtomicU64 /* torrent-global */, seq: AtomicU64, nudged: AtomicBool, read_ahead: Mutex<ReadAhead> }` with:
    - `fn moved(&self, pos: u64, send: impl FnOnce() -> bool)`: stores `pos`, bumps `seq`, and calls `send` (the reader's `try_send` of `Cmd::ReaderMoved(id)`) only when `nudged` was false (swap); a `send` returning false clears `nudged` again;
    - `fn take(&self) -> (u64, u64)`: the actor clears `nudged` and reads `(pos, seq)`.
    The reader calls `moved` on every seek and when its position enters a new piece.
  - `#[doc(hidden)] pub async fn debug_deadlines(&self) -> Vec<(u32, Duration)>` on `TorrentHandle` (a `Cmd::DebugDeadlines(oneshot::Sender<..>)`): the merged deadlines as time left per piece, for tests.
  - New `Cmd`s:
    - `OpenReader { file: usize, link: Arc<ReaderLink>, tx: oneshot::Sender<Result<ReaderId, SessionError>> }`;
    - `ReaderMoved(ReaderId)`;
    - `CloseReader(ReaderId)` (sent by `Drop` with `try_send`);
    - `Read { reader: ReaderId, offset: u64, len: u32, tx: oneshot::Sender<Result<Bytes, StreamError>> }`, at most one in flight per reader.
  - `pub(crate) const MAX_READERS: usize = 16;`, `pub(crate) const MAX_READ_CHUNK: u32 = 1 << 20;`, `pub(crate) const READ_PREFETCH_PIECES: u32 = 2;`
  - `torrent/readers.rs`, owned by the actor as `readers: Readers`: per reader the file range, a `Weak<ReaderLink>`, the last applied `seq`, its current piece, and the window it last set in `actor.deadlines: Deadlines`. `waiting: HashMap<u32, Vec<(ReaderId, u64 /*offset*/, u32 /*len*/, oneshot::Sender<..>)>>`, at most one entry per reader.
  - Actor methods: `on_open_reader`, `on_reader_moved(id)`, `on_close_reader(id)`, `on_read(...)`, `readers_tick()` (on the status tick: drop readers whose `Weak` is dead or whose answer channel is closed, re-sync any whose `seq` moved, recompute every window relative to now), `answer_piece(piece, &Arc<[u8]>)` (answers that piece's waiters with `Bytes::from_owner(Arc::clone(data)).slice(..)`), `fail_readers(StreamError)`.
  - **Serving a `Read`:**
    1. `Phase::Error` answers `TorrentError` at once;
    2. a piece the picker has is served from `cache.get`, else read from disk through the pool: the whole piece when it is at most `WHOLE_PIECE_READ_MAX` (kept clean when `reserve_clean` succeeds, else not kept), the chunk alone otherwise. A whole-piece read shares `PieceReads` with peer requests: `PieceRead.waiting` becomes `Vec<Waiter>` with `enum Waiter { Peer(PeerKey, BlockRef), Reader(ReaderId, u64, u32, oneshot::Sender<Result<Bytes, StreamError>>) }`, and `MAX_PIECE_READS` still bounds them (a reader read beyond it uses a one-off chunk read);
    3. otherwise the request waits in `waiting`.
  - **Answers from the download.** `on_hashed` converts the verified data to `Arc<[u8]>` once, calls `answer_piece` and then `insert_dirty` with the same `Arc`, so a waiting read returns before the piece is written. `on_checked` answers the waiters whose pieces the check found and keeps the rest. A `NotFound` disk read for a reader goes through the existing `piece_lost` path and the read waits again for the re-download.
  - **Lifecycle.** `fail()` (storage error) calls `fail_readers(TorrentError(message))`; the actor dropping on remove or shutdown drops the senders, and the reader maps a closed channel to `TorrentRemoved` or `Shutdown` through `TorrentHandle`'s existing `gone()` rule. Pause and recheck keep waiters.
  - **Sequential read cache.** When a reader's current piece moves from `p` to `q`, the actor demotes `p` unless `Deadlines::holders(p) > 0` for another reader, then prefetches up to `READ_PREFETCH_PIECES` pieces after `q` in the file that the picker has, that are not `is_cached` and not in `PieceReads`, each only if `reserve_clean` succeeds.

- [ ] **Step 1: Write failing tests.** Unit tests in `stream/reader.rs` and integration tests in `tests/stream.rs` (a seed and a leech session on loopback with `test_settings()`, payload from `support::make_torrent`, files of 3 MiB, 0 B and 5 MiB, piece length 256 KiB):
  - `stream/reader.rs`: `seek_storm_queues_one_nudge`: 10,000 `moved` calls with a counting `send` that returns true call it once, and `take()` gives the last position and `seq` 10,000; the next `moved` calls it again; with a `send` returning false every call retries.
  - `tests/stream.rs`:
    - `reads_whole_file_while_downloading`: the leech opens file 2 before any piece arrives and `read_to_end` equals the file's bytes; the torrent finishes.
    - `seek_then_read_returns_the_right_bytes`: offsets 0, 1, 262,143, 262,144, `len − 1` and a chunk straddling a piece boundary, 4 KiB each.
    - `read_at_and_past_eof`: on a leech that has no peer, `read` at `len` and at `len + 1000` returns 0 at once; on the seed, a read straddling the end is short and ends there.
    - `negative_seek_is_invalid_input`: `seek(Current(-1))` from 0 and `seek(End(-(len + 1)))`; the position is unchanged afterwards.
    - `zero_length_file_reads_eof_without_deadlines`: file 1 (0 B) opens; `read` returns 0; `debug_deadlines()` is empty.
    - `open_file_index_checks`: index 3 gives `InvalidFileIndex { index: 3, files: 3 }`; a pad file's index (from a torrent built with a pad entry) gives the same; 17 readers give `TooManyReaders` on the 17th.
    - `open_file_on_magnet_without_metadata`: `NoMetadata`; after `MetadataReceived` it opens.
    - `waiting_read_is_answered_from_the_hash_pass`: the leech's storage is `SlowStorage` with a 2 s write delay; a read of piece 5 returns within 1 s of `PieceFinished { piece: 5 }`, before the write.
    - `reader_never_sees_unverified_bytes`: a `ScriptedPeer` sends a corrupt piece 2 inside the window, a real seed then serves it; the read returns the real bytes and `HashFailed { piece: 2 }` was emitted.
    - `have_pieces_are_read_from_disk_and_cached`: a complete torrent (seeding) reads its file twice; `cache_stats().clean` grows on the first pass and the second pass does no storage read (a counting `Storage` wrapper).
    - `sequential_reader_demotes_and_prefetches`: `cache_size` 2 MiB, piece 256 KiB: while one reader reads the seeding torrent front to back, the clean cache never holds more than 3 pieces behind the reader, and pieces `q+1`, `q+2` are cached before the reader reaches them.
    - `pause_serves_have_pieces_and_waits_for_missing`: half downloaded, paused: a read of a piece we have returns; one of a missing piece is still pending after 1 s and returns after `resume`.
    - `recheck_keeps_waiters`: a read waits on a missing piece, `force_recheck` runs, the read completes once the piece downloads.
    - `remove_while_waiting_fails_the_read`: the pending read fails with `ErrorKind::NotFound` wrapping `StreamError::TorrentRemoved`.
    - `shutdown_while_waiting_fails_the_read`: `StreamError::Shutdown`.
    - `storage_error_fails_waiting_reads`: `FailWritesStorage` with the flag set stops the torrent; the pending read fails with `StreamError::TorrentError`; with the flag cleared and after `resume`, a read works.
    - `dropped_reader_is_forgotten_within_a_tick`: the command queue is filled so `CloseReader` cannot be sent, the reader is dropped mid-read; within two status ticks `debug_deadlines()` is empty and the read slot is free (16 new readers open).
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test stream && cargo test -p fbt-session stream::`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS.
- [ ] **Step 5: Commit.** `feat(session): FileReader with seekable reads that wait for pieces` No trailers or attribution of any kind.

---

### Task 6: deadline picking

The readers' windows now drive requests (spec 5.5).

**Files:**
- Modify: `crates/fbt-session/src/torrent/{readers.rs,peers.rs,data.rs,commands.rs,actor.rs}`, `crates/fbt-session/tests/stream.rs`.

**Interfaces:**
- Consumes: `Deadlines`, `PiecePicker::{pick_piece, progress, abandon}` (Task 4); the reader plumbing and `debug_deadlines()` (Task 5); `Peer::{from_rate, requested, choked_us, snubbed, demand}`.
- Produces (crate-internal):
  - `const MIN_DEADLINE_RATE: u64 = 16 * 1024;` and `fn arrival(&self, key: PeerKey, now: Instant) -> Duration`: `(requested.len() + 1) × BLOCK_SIZE / max(from_rate.rate(now), MIN_DEADLINE_RATE)`.
  - `fn fill_deadlines(&mut self, key: PeerKey, room: usize, now: Instant) -> Vec<BlockRef>`, called first in `fill()` (its blocks count against `room`, the rest goes to the normal `pick_capped`). For each `(piece, deadline)` in `deadlines.ordered()` that the peer has (or that is in its `fast_has` while it chokes us), with `left = deadline.saturating_duration_since(now)`:
    1. free blocks, if `arrival(key) <= left` or `key` is the fastest live peer that has the piece and does not choke us: `pick_piece(key, piece, n, false)`;
    2. late blocks: if some owner in `progress(piece).owners` has `arrival(owner) > left`, chokes us or is snubbed, and `key` is not an owner and `arrival(key) < arrival(owner)` (a choking or snubbed owner counts as infinitely late): `pick_piece(key, piece, n, true)`.
    Buffers are reserved as in `fill`; a deadline piece that cannot reserve a buffer waits like any other.
  - `top_up_all` orders peers by ascending `arrival` while `deadlines` is not empty.
  - `wants_piece(piece)` is also true for a missing piece with a deadline, and every deadline change calls `update_interest` for the peers that have a newly added piece. `sync_upload_only` and `close_if_upload_only` treat "a deadline piece is missing" as not finished.
  - `on_hashed` (pass) calls `deadlines.piece_done(piece)`. `PeerEvent::Choked` calls `top_up_all` when the peer held deadline blocks.
  - Pieces returned by `Deadlines::set`/`remove` with priority 0 are `abandon`ed: their blocks are cancelled (`PeerCommand::Cancel`, removed from `Peer::requested`), their buffer dropped and `cache.release_partial` called. Other returned pieces are left alone.
  - Deadlines are reapplied after `on_checked` and on `resume`; while paused or checking they are kept but nothing is requested.

- [ ] **Step 1: Write failing tests.**
  - Unit tests in `readers.rs` on `swarm::tests::actor()` with fabricated peers (rates recorded into `from_rate` under paused time):
    - `fastest_peer_gets_the_first_deadline_piece`: peers at 1 MiB/s and 20 KiB/s with demand; the window's first piece goes to the fast one, and the slow one takes no block of a piece whose `left` is below its arrival.
    - `late_block_is_duplicated_to_a_faster_peer`: the slow peer holds the current piece's blocks; a fast peer's `fill` duplicates them; a third peer's does not (two owners).
    - `duplicate_never_goes_to_a_slower_peer`.
    - `endgame_and_deadline_never_exceed_two_owners`: in end-game, `fill` for four peers leaves no block with three owners (checked through `progress`).
    - `skipped_piece_with_deadline_is_requested_and_wanted`: priority 0 piece in a window; `wants_piece` is true and a peer that has only that piece becomes interesting.
    - `seek_storm_leaves_only_the_last_window`: 1,000 `on_reader_moved` with positions across the file, each followed by a `fill` of two peers; then `actor.deadlines.ordered()` holds exactly the last window, and `num_in_progress()` never exceeds the partial cap of `fill` (old windows' normal-priority pieces stay in progress, none is started twice).
  - Integration tests in `tests/stream.rs`:
    - `seek_reads_ahead_of_the_rest`: a 64 MiB torrent from one seed, `rate_limit_local_peers` and a 4 MiB/s download limit; the reader seeks to 48 MiB and reads 1 MiB within 3 s (the whole torrent takes 16 s, and rarest-first alone would reach those 4 pieces at a random point of it).
    - `two_readers_on_one_torrent`: readers on file 0 and file 2 read in parallel and both match; the merged deadlines hold both windows.
    - `dropped_reader_clears_its_deadlines_only`: of two readers, one is dropped; within a tick only the other's window remains.
    - `straddling_pieces_fetch_only_the_boundaries`: files A (3 MiB, `Skip`), B (2 MiB + 100 B, `Normal`), C (3 MiB, `Skip`), piece 256 KiB; reading B returns B exactly, and the seed (a `ScriptedPeer` that records requests) is never asked for a piece entirely inside A or C.
    - `reader_on_a_skipped_file_downloads_its_window`: file C is `Skip`; reading its first 1 MiB completes, and no piece after C's window is requested.
    - `finished_torrent_keeps_seeds_for_a_skipped_file_reader`: every wanted file done (state `Seeding`), the only peer a seed; a reader on a `Skip` file still completes, and the seed is not disconnected with `UploadOnly` while the window is missing.
    - `seek_away_from_skipped_file_drops_its_partial_pieces`: after a seek from the skipped file to a wanted one, `cache_stats().partial` returns to the new window's buffers only and the scripted peer receives `Cancel`s.
    - `choked_deadline_blocks_go_to_another_peer`: two scripted seeds; the one holding the window's blocks chokes us without rejecting (fast extension on, no `Reject`); the other receives requests for those blocks within one status tick, and the read completes.
    - `only_peer_chokes_then_unchokes`: one scripted seed chokes us mid-piece; the read is still pending after 2 s with no error, the seed received no request while choking us (no busy loop), and the read completes after `Unchoke`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test stream && cargo test -p fbt-session readers::`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS, including milestone 3's `tests/swarm.rs` end-game and choker tests unchanged.
- [ ] **Step 5: Commit.** `feat(session): deadline picker: fastest peers first, late duplicates, seeks replace windows` No trailers or attribution of any kind.

---

### Task 7: facade, example `stream` subcommand and streaming bench

**Files:**
- Modify: `crates/fbt/src/lib.rs`, `crates/fbt/tests/lib_surface.rs`, `crates/fbt-example/src/main.rs`, `crates/fbt-example/tests/cli.rs`, `dev/bench.sh`, `docs/benchmarks.md`.

**Interfaces:**
- Consumes: `FileReader`, `ReadAhead`, `StreamError`, `SessionError::{InvalidFileIndex, TooManyReaders}` (Tasks 4 to 6).
- Produces:
  - `fbt` re-exports `FileReader`, `ReadAhead` and `StreamError`, each named in `lib_surface.rs`. The crate docs gain a "Streaming" section (open, read-ahead and bitrate, waits instead of errors, what fails a read, the choker and rate-limit notes, several readers) and a fourth `no_run` doctest: open file 0, `seek(SeekFrom::Start(1 << 20))`, copy 1 MiB to `tokio::io::sink()`.
  - `fbt-example stream <TORRENT> --file <INDEX> --out <DIR> [--to <PATH>] [--seek <BYTES>] [--length <BYTES>] [--bitrate <BYTES/S>] [--peer <ADDR>]... [net flags]`. It adds the torrent (data under `--out`), opens the file, seeks, copies `--length` bytes (default: to the end) to `--to` or stdout, prints `stream: <n> bytes from <offset> in <s> s` to stderr, shuts down and exits 0. With stdout as the sink every other line the command prints (`listening on`, `peer connected ...`) goes to stderr. `--file` out of range is exit 2 with the `InvalidFileIndex` message. `--bitrate` takes the `K`/`M` suffixes of the limit flags.
  - `fbt-example bench --stream --size <MiB>` (hidden): seed and leech in one process on loopback over TCP, `MemStorage` on the leech, one reader reading the file front to back while it downloads, then ten seeks to random offsets (fixed seed) reading 64 KiB each. Prints `bench: stream <MiB> MiB in <s> s = <Mbit/s> Mbit/s` and `bench: stream seek p50 <ms> ms p90 <ms> ms`.
  - `dev/bench.sh` runs `bench --stream --size 256` and adds the "Stream Mbit/s" and "Seek p50 ms" columns to the table (earlier rows get `-`).

- [ ] **Step 1: Write failing tests** in `tests/cli.rs`:
  - `stream_to_a_path_matches_the_file`: `seed` a 3-file fixture, `stream fixture.torrent --file 1 --out <tmp> --to <tmp>/out.bin --peer 127.0.0.1:<port>`: exit 0 and `out.bin` equals file 1.
  - `stream_with_seek_and_length_to_stdout`: `--seek 300000 --length 70000` to stdout; stdout is exactly those bytes (nothing else on stdout).
  - `stream_bad_file_index_is_a_clean_error`: exit 2 and stderr names the index.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-example`
- [ ] **Step 3: Implement.** Then run `BENCH_NO_RECORD=1 BENCH_TORRENTS=10 BENCH_IDLE_SECS=5 bash dev/bench.sh` once to see the stream columns.
- [ ] **Step 4: Run.** `cargo test -p fbt-example -p fbt && RUSTDOCFLAGS="-D warnings" cargo doc -p fbt --no-deps`: PASS.
- [ ] **Step 5: Commit.** `feat(fbt): streaming in the facade, fbt-example stream and a streaming bench` No trailers or attribution of any kind.

---

### Task 8: interop: streaming from qBittorrent with seeks

**Files:**
- Modify: `dev/interop/run.sh`, `dev/interop/README.md`.

**Interfaces:**
- Consumes: `fbt-example stream` (Task 7), the existing fixture 1 (`alpha.bin` 30,000,000 B, `beta.bin` 25,000,000 B, `gamma.bin` 12,108,864 B, 256 KiB pieces, seeded by qBittorrent and Transmission), `qbt_field`, `check_download`.
- Produces:
  - A `stream` scenario (`want stream`), after the uTP scenarios, with qBittorrent back on protocol 0 and Transmission's copy of fixture 1 stopped (`transmission-remote -t <hash> -S`, started again at the end) so qBittorrent is the only source:
    1. The index of `beta.bin` comes from `fbt-example info /interop/fixture.torrent` (files print in torrent order). `beta.bin` starts at 30,000,000 (not a multiple of 262,144) and ends at 55,000,000, so both boundary pieces straddle `alpha.bin` and `gamma.bin`.
    2. `stream /interop/fixture.torrent --file <i> --out /interop/fbt-stream --port 6899 --no-dht --seek 12000000 --length 3000000 --to /interop/stream-mid.bin` (timeout 300 s).
    3. The same `--out` again with `--seek 0 --to /interop/stream-all.bin` (the whole file: what step 2 downloaded is read back from disk, the rest from qBittorrent).
    4. The same `--out` with `--seek 24999000 --to /interop/stream-tail.bin` (1,000 bytes to the end).
    5. Each output's SHA-1 equals the matching byte range of `/interop/payload/fixture/beta.bin` (`tail -c +$((offset + 1)) | head -c <len> | sha1sum`); every log shows `peer connected <qbittorrent ip>:6882`; qBittorrent's `uploaded` grew.
    6. Prints `interop: stream with seeks OK (qbittorrent)`.
  - `INTEROP_ONLY` accepts `stream`; an unknown name fails at once with `interop: unknown scenario '<name>' (known: download, seed, encryption, magnet, pex, utp, lsd, stream)`; a partial run ends with `interop: selected scenarios passed (<list>) ...` and only a full run prints `interop: all scenarios passed ...`.
  - The README documents the scenario and the `INTEROP_ONLY` names.

- [ ] **Step 1: Write the scenario.** Run `INTEROP_ONLY=stream bash dev/interop/run.sh`, then `INTEROP_ONLY=nope bash dev/interop/run.sh` (exits non-zero with the unknown-scenario message before the setup).
- [ ] **Step 2: Run the full suite.** `bash dev/interop/run.sh`: every milestone 1 to 3 line, then `interop: stream with seeks OK (qbittorrent)`, then `interop: all scenarios passed ...`, exit 0. `docker network inspect fbt-interop_swarm --format '{{.Internal}}'` prints `true`.
- [ ] **Step 3: Raise the CI `interop` timeout** by 10 minutes if the new scenario needs it, and add the stream logs to the artifacts.
- [ ] **Step 4: Commit.** `ci: interop streaming from qBittorrent with seeks; INTEROP_ONLY checks its names` No trailers or attribution of any kind.

---

### Task 9: docs, changelog and full gate

**Files:**
- Modify: `README.md`, `CHANGELOG.md`, `CLAUDE.md`, `docs/benchmarks.md`, `deny.toml` (only if a new license or advisory appears, with its one-line reason).

- [ ] **Step 1: Update the docs.**
  - `README.md`: streaming in the feature list; a streaming usage example matching the `fbt` doctest; `fbt-example stream` in the example CLI section; the interop paragraph names streaming with seeks; "Known limitations" drops the uTP performance note if Task 1 met its targets (keeps it, with the numbers, if not).
  - `CHANGELOG.md` gets a `## [Unreleased]` section. Every entry ends in `([#N](https://github.com/featherbitplatform/fbt-lib/pull/N))`, `N` being the milestone PR's number, filled in when the PR is opened:
    - `### Added`: `FileReader` (`TorrentHandle::open_file`, `open_file_with`), `ReadAhead` and `Settings::read_ahead`, `StreamError`, the deadline picker, the sequential read cache (demote and prefetch), `FileLayout::file_range`, `fbt-example stream` and `bench --stream`, the interop scenario.
    - `### Changed`: the new `SessionError` variants `InvalidFileIndex` and `TooManyReaders`; `RateMeter::rate` no longer dilutes its first five seconds (status rates and the request pipeline ramp up faster); repeated drops of a productive peer back off up to 30 minutes; uTP sends, acks and reads without per-byte copies or window scans (with the measured numbers).
    - `### Fixed`: `INTEROP_ONLY` reporting "all scenarios passed" for a partial run and ignoring unknown names; uTP decode failures now traced; the half-open eviction order on a lost re-check.
  - `CLAUDE.md` (the only task that edits it): the interop line names streaming with seeks; the bench line mentions the streaming row; nothing else changes.
- [ ] **Step 2: Run the full gate.** `cargo fmt --all --check && cargo clippy --workspace --all-targets --all-features -- -D warnings && cargo test --workspace && cargo deny --all-features check && bash dev/check-pure-rust.sh && RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps && bash dev/sast.sh`: all pass.
- [ ] **Step 3: Commit.** `docs: milestone 4 changelog, README and CLAUDE.md` No trailers or attribution of any kind.

---

## After the plan: shipping v0.4.0

PR `feature/m4-streaming` → `develop`. Every CI job must be green, including `interop` and the fuzz runs the PR triggers (`crates/fbt-utp/**` changed, so `utp_packet` and `utp_conn` run). The PR body lists the decisions made on the owner's behalf (the "Decisions recorded in this plan" section, plus any taken during execution) and a "Deferred to later milestones" section, and the CHANGELOG PR links are filled in with its number. Then follow the Gitflow release:

1. `release/0.4.0` off `develop`: bump the workspace version to `0.4.0` (the peer-id prefix becomes `-FB0400-` and the KRPC `v` becomes `FB\x00\x04` automatically). Move `Unreleased` to `## [0.4.0] - <date>`, and check `bash dev/test-release-version.sh`. Run `bash dev/bench.sh` on the release commit and keep its row.
2. PR into `main`, then the annotated tag `v0.4.0`, then merge `main` back into `develop`.

No attribution lines anywhere.

## Deferred to later milestones (recorded so nobody builds them now)

- **Milestone 5:** these stay there, per the earlier plans: resume data and session state, queueing and seed limits, move storage and rename (move must flush the cache first), proxies (SOCKS5 with UDP associate, HTTP CONNECT), IP filter, web seeds, smart ban, per-IP connection limits, `stopped` semantics, BEP 44/33/51, the rename scheme for case and Unicode collisions.
- **Milestone 7 (`fbt-api`):** HTTP `Range` serving on top of `FileReader` (206, `Content-Length`, `Accept-Ranges`, token in the query string).
- **Backlog:**
  - per-peer uTP priority: revisit if a measurement shows uTP deadline peers lagging TCP ones at the same rate (see Decisions);
  - batched UDP sends and receives (`sendmmsg`/`recvmmsg`-style, or GSO) and splitting a socket's uTP processing over more than one task, the inherent costs left after Task 1;
  - an adaptive status tick for idle torrents (1,000 idle torrents at 1.13 % CPU against the 1 % target, milestone 3 "Performance misses");
  - the part file for unwanted pieces that straddle wanted files (spec 5.4): skipped files that share a piece with a wanted or streamed file are still created;
  - the qBittorrent Docker-bridge ceiling of about 4 MiB/s (its 500 ms request batching), not investigated;
  - milestone 3 minors not touched here: the NAT-PMP gateway not looked up again after a network change, a double-NAT private address winning over the DHT's, shutdown releasing UDP ports asynchronously, port mapping ignoring which side bound, inconsistent `#[non_exhaustive]` on event variants, the rebalance task waking every second without a limit, a UPnP renewal deleting an old port by number after rediscovery, the gateway helper child not reaped on timeout, linger slots counting against `max_connections`, the keepalive STATE ambiguity, `send_lossy` dropping `DontHave`s on full queues, the stale snub timer after a timed-out pipeline;
  - serde support for all public config structs (spec 5.3), `ReadAhead` included;
  - documentation and test polish recorded in the per-task reviews.
