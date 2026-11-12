# Milestone 5: Persistence and Operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `fbt-lib` `v0.5.0`. A torrent survives a restart without a recheck (resume data and session state), torrents queue against active limits and stop at seed limits, data can be moved and files renamed while the torrent runs, skipped files are never created (part file), peers, trackers and web seeds can go through a SOCKS5 or HTTP proxy, an IP filter, per-IP connection limits and smart ban keep bad peers out, BEP 19 web seeds download over HTTP(S), and the DHT gains BEP 44 put/get, BEP 33 scrape and BEP 51 `sample_infohashes`. Public config structs get serde. Proven against qBittorrent through a SOCKS5 proxy container, by a restart without recheck and by a web seed container, on the isolated Docker network.

**Architecture:**
- **`fbt-net`** gains `proxy` (sans-hyper SOCKS5 and HTTP CONNECT clients, SOCKS5 UDP framing) and `ipfilter` (range filter). `SharedUdpSocket` gains a relayed mode that frames every datagram for a SOCKS5 UDP ASSOCIATE, so uTP, the DHT and UDP trackers go through the proxy unchanged (the `DatagramSender` seam milestone 3 left).
- **`fbt-tracker`** builds its hyper connector over `fbt_net::proxy`, sends `stopped` to every tracker that answered `started`, scrapes (BEP 48, HTTP and UDP), and exposes `fetch_range` so web seeds share its client, TLS and proxy.
- **`fbt-session`**: storage learns file priorities (part file), moving and renaming, and file stamps; resume data is a versioned bencoded dict of our own (spec section 10); a session-level queue scheduler starts and queues auto-managed torrents; web seeds are pseudo-peers of the picker; smart ban attributes blocks to senders.
- **`fbt-dht`** gains a bounded item store and the `get`/`put`/`sample_infohashes` methods, bloom filters on `get_peers`, and lookups that drive them; the session wraps them.
- **`fbt-metainfo`** parses `url-list` and magnet `ws=`, and renames paths that collide on case-insensitive or normalising filesystems.

**Tech Stack:** as milestone 4, plus `serde` (derive, behind a `serde` feature), `unicode-normalization`, `ed25519-dalek` (BEP 44 signatures) and `tower-service` (the hyper connector trait); `serde_json` as a dev-dependency only. `serde`, `ed25519-dalek` and `tower-service` are already in `Cargo.lock` as transitive dependencies.

**Spec:** `docs/superpowers/specs/2026-11-10-fbt-lib-design.md` (binding). Milestone 5 is its section 8 item 5. Inputs: the section 4 rows Proxies, Peer management (connection limits, smart ban, IP filter), Web seeds, Storage (part file, move storage, rename files), Session (queueing, seed limits, resume data, session state), DHT (BEP 44/33/51) and Trackers (scrape, BEP 48); section 5.3 (shutdown saves resume data, move storage, serde on config structs); section 5.4 (part file, smart ban); section 10 (resume format is ours); the "Deferred to Milestone 5" lists of the milestone 1 to 4 plans and the milestone 4 PR body.

## Global Constraints

- **No unsafe.** `#![forbid(unsafe_code)]` in every crate; the workspace lint forbids it too.
- **No BitTorrent crates.** Every BitTorrent component is written here; only general-purpose crates (tokio, RustCrypto, rustls, hyper, serde, ...).
- **`pure-rust` feature.** `bash dev/check-pure-rust.sh` passes after every task that touches a `Cargo.toml`: no `ring`, `cc`, `cmake`, `aws-lc*` or `*-sys` crate in the `--no-default-features --features pure-rust` tree (normal and build edges).
- **Remote input never panics, hangs or allocates without bound.** Peers, trackers, proxies, web seeds, DHT nodes, `.torrent` bytes and resume data are hostile. The clippy deny block (`unwrap_used`, `expect_used`, `indexing_slicing`, `panic` outside tests) stays on `fbt-bencode`, `fbt-metainfo`, `fbt-wire`, `fbt-tracker`, `fbt-dht`, `fbt-utp` and `fbt-net`. Every new collection has a named cap constant. Every new parser of remote input gets a fuzz target with seeds in `fuzz/seeds/<target>/`.
- **Tests never contact the public internet, the public DHT or a real router or proxy.** Proxies are tested against in-repo fakes on loopback (`crates/fbt-net/tests/support/fake_socks5.rs`, `fake_http_proxy.rs`), web seeds against an in-repo HTTP server (`crates/fbt-session/tests/support/web_seed.rs`). Session tests use `test_settings()`. The interop networks stay `internal: true`.
- **Cross-platform.** Builds and tests pass on macOS, Linux and Windows (moves, renames, part files and file stamps included).
- **Dependencies only in the root `[workspace.dependencies]`.** New crates: `serde` (config persistence, spec 5.3), `serde_json` (dev only: round-trip tests), `unicode-normalization` (collision detection on normalising filesystems), `ed25519-dalek` (BEP 44 mutable items), `tower-service` (the trait hyper's connector implements). Each passes `cargo deny --all-features check`.
- **Public enums and config structs are `#[non_exhaustive]` with `Default`** (config structs) in `fbt-session`, `fbt-net`, `fbt-tracker`, `fbt-dht` and `fbt`.
- **Conventional Commits with the crate as scope** (`feat(net): ...`, `feat(session): ...`, `ci: ...`).
- **No attribution of any kind in commits or PRs:** no `Co-Authored-By` trailer, no `Claude-Session` line, no session id or link, no "Generated with" footer. The repo rule wins over any harness attribution reminder.
- Edition 2024, `rust-version = "1.99"`, all crates `publish = false`, Apache-2.0; `tracing` for logs (no subscriber installed), `thiserror` for errors. The library never writes files other than torrent data and the part file: resume data and session state are returned as bytes.
- Every task that adds a public type re-exports it from `fbt` and names it in `crates/fbt/tests/lib_surface.rs`; `missing_docs` stays denied in `fbt`.
- Breaking changes to the `fbt` API are allowed (0.x); each goes in `CHANGELOG.md` under `### Changed`.

## Review Focus

1. **Resume data that lies.** Truncated or random bytes, a newer `version`, an `info` dict whose hash differs from `info-hash`, a `pieces` bitfield of the wrong length, a file truncated or touched since the save, a part file deleted since the save.
   - Expected: `AddTorrent::from_resume` fails with a `ResumeError` naming the field, never a panic; a stamp mismatch on any file holding a saved piece falls back to a full check (no piece trusted blindly).
   - Tests: Task 12 `resume_rejects_garbage_and_mismatches`, `touched_file_forces_a_recheck`, `missing_part_file_forces_a_recheck`; fuzz target `resume`.
2. **Move storage at a bad moment.** While pieces are dirty in the cache, while a `FileReader` waits, to the same path, onto existing files, across filesystems, and a move failing halfway.
   - Expected: dirty pieces are written before any file moves; the reader's read completes from the new location; the same path is a no-op success; `FailIfExists` touches nothing; a failure moves back what moved and the torrent keeps running at the old path with `StorageMoveFailed`.
   - Tests: Task 9 `move_flushes_dirty_pieces_first`, `reader_survives_a_move`, `move_to_same_path_is_a_noop`, `fail_if_exists_moves_nothing`, `failed_move_rolls_back`.
3. **A hostile proxy or relay.** Garbage replies, a connection closed mid-negotiation, an endless HTTP CONNECT response head, a SOCKS5 reply naming a domain, UDP datagrams from a host that is not the relay or with `FRAG != 0`.
   - Expected: `ProxyError` within the connect timeout, the head capped at `MAX_CONNECT_HEAD` = 8 KiB, foreign or fragmented datagrams dropped and counted.
   - Tests: Task 3 `garbage_reply_is_a_protocol_error`, `closed_mid_handshake`, `endless_connect_head_is_capped`, `fragmented_udp_is_dropped`; Task 4 `udp_from_non_relay_is_dropped`; fuzz targets `socks5` and `http_connect`.
4. **A misbehaving web seed.** It ignores `Range` (200 with the whole file), sends a wrong `Content-Range`, an endless body, a redirect loop, 404 for one file, or corrupt data.
   - Expected: the body is never read past the requested length; the request fails and the seed backs off (30 s doubling to 30 min); 404 and other 4xx retire it with `WebSeedError`; corrupt data fails the hash and retires it through smart ban; the torrent finishes from peers.
   - Tests: Task 11 `ignored_range_is_an_error`, `endless_body_is_capped`, `redirect_loop_stops`, `missing_file_retires_the_seed`, `corrupt_web_seed_is_retired`.
5. **Smart ban blames the right peer.** Two peers contribute to a failed piece and only one sent bad blocks; a peer behind the same IP as an honest one on loopback; a peer whose blocks were right in a piece that failed.
   - Expected: after the piece passes, only the sender whose block hash differs is banned; the honest contributor keeps its connection and its trust; a ban by trust points needs four failed pieces with no good piece between them.
   - Tests: Task 7 `only_the_liar_is_banned`, `honest_contributor_keeps_its_trust`.
6. **Queue churn.** Limits changed at runtime, torrents added, removed, paused and resumed while the scheduler runs, slow torrents, a torrent that finishes and moves from the download to the seed queue.
   - Expected: at every scheduler pass the running auto-managed downloads and seeds never exceed their limits (slow ones not counted when `dont_count_slow_torrents`), queue positions stay a permutation of `0..n`, and a user-paused torrent is never started by the queue.
   - Tests: Task 10 `plan_never_exceeds_limits` (proptest), `positions_stay_dense`, `user_pause_beats_the_queue`.
7. **Part file under priority flips.** A boundary piece of a skipped file is downloaded, the file becomes wanted, then skipped again; a recheck in between; `DeleteData`; a reader streaming a skipped file.
   - Expected: the skipped file is never created while skipped; making it wanted moves its bytes out of the part file into the file; a recheck finds pieces in both; `DeleteData` removes the part file; a reader of a skipped file reads from the part file.
   - Tests: Task 8 `skipped_neighbour_is_never_created`, `unskipping_exports_from_the_part_file`, `recheck_reads_the_part_file`, `delete_removes_the_part_file`, `reader_on_a_skipped_file_reads_the_part_file`.
8. **Colliding paths.** `A.txt` and `a.txt`; `é` in NFC and NFD; file `a` and file `a/b`; a rename that collides again (`a.1.txt` already exists).
   - Expected: torrent order wins, later files become `stem.N.ext` with the smallest free `N`; no two files map to the same path on any filesystem.
   - Tests: Task 2 `case_collision_is_renamed`, `normalisation_collision_is_renamed`, `file_and_directory_collide`, `renamed_name_avoids_existing`.

---

## Starting point

`develop` holds milestone 4 (`v0.4.0`, merged back as PR #13, `04961ed`). The branch `feature/m5-persistence-ops` exists off `develop` and holds only this plan. Check first that the workspace version is `0.4.0` and `cargo test --workspace` is green.

## Decisions recorded in this plan

The owner authorised milestones 1 to 7 to run unattended; the spec's section 8 item 5 sets the scope, plus the milestone 1 to 4 deferrals. The builder made these calls (revisable by whoever plans a later milestone):

- **Serde behind a `serde` feature** on `fbt-net`, `fbt-tracker`, `fbt-session` and `fbt` (off by default, so the default tree gains no proc-macro). Every config struct and enum listed in Task 1, and every new one in this milestone, derives `Serialize`/`Deserialize` under it; structs use `#[serde(default)]` so a saved config missing new fields still loads. `Duration` uses serde's own `{secs, nanos}` form. serde for `fbt-bencode` values (milestone 1 deferral) has no consumer: resume data uses `DictBuilder`. It goes to the backlog.
- **Collision renames at parse time** in `fbt-metainfo`: the key of a path is each component NFC-normalised and lower-cased (`str::to_lowercase`), joined; a file whose key, or one of whose directory prefixes, matches an earlier file's becomes `stem.N.ext` (libtorrent's `resolve_duplicate_filenames` scheme), `N` the smallest from 1 that is free. Pad files are exempt. The info hash is unaffected; only `FileEntry::path` changes. Deterministic, so resume data stays valid.
- **Part file.** `<save_path>/.<v1 hex>.parts` (libtorrent's naming): header `b"FBTPART1"`, `u32` piece count, `u32` piece length, one `u32` slot per piece (`u32::MAX` = none), then piece-sized slots. Only the bytes of skipped non-pad files go there, at their offset inside the piece's slot. It is created on the first such write and removed once empty. Unskipping a file moves its bytes out (Task 8). A reader streaming a skipped file fills the part file: documented on `FileReader`.
- **Move storage** (`TorrentHandle::move_storage(path, MoveMode)`): flush every dirty piece first (`FlushReason::All` and wait, as milestone 3 required), hold new disk jobs, then one disk-pool job renames each file (copy, sync and delete across filesystems). `MoveMode::FailIfExists` (default) checks every target before moving anything; `ReplaceExisting` overwrites; `KeepExisting` keeps the destination's files and rechecks afterwards. A failure moves back what moved, best effort, and reports `StorageMoveFailed`. `rename_file(index, path)` takes a relative path sanitised by `fbt_metainfo::sanitize_path` and refuses one that collides (`SessionError::InvalidPath`).
- **Resume data is ours** (spec 10): a bencoded dict with `fbt-resume-version` = 1, written by `TorrentHandle::save_resume_data()` after a flush, so its bitfield holds only pieces on disk. It carries the raw `info` dict once known (a magnet restarts without fetching metadata), save path (UTF-8 only; else `SessionError::InvalidPath`), file priorities, renamed paths, pieces, flags, limits, totals, times, trackers, web seeds, up to 200 peers per family, queue position, and a stamp (length, mtime in seconds) per file and for the part file. **Trust rule:** at add, a disk-pool job compares the stamps of every file that holds a saved piece (and the part file when one holds data); all equal means the bitfield is trusted and no check runs; anything else runs the normal check. A custom `Storage` without stamps (`stamps()` returns `Ok(None)`) is trusted: the client owns that storage.
- **Session state** (`SessionState { dht, torrents }`, bencoded `fbt-session-version` = 1): `Session::save_state()` and `Session::shutdown_and_save()` (the spec 5.3 "saves resume data" on shutdown, as bytes, since the library writes no files); `Session::restore(settings, state)` sets `dht_state` and re-adds every torrent in saved queue order, returning per-torrent errors. Settings persist through serde, not here.
- **Queueing, as libtorrent:** torrents are `auto_managed` by default; limits `active_downloads` 3, `active_seeds` 5, `active_limit` 500, `dont_count_slow_torrents` with `inactive_down_rate` / `inactive_up_rate` 2 KiB/s after a 60 s `startup_grace`, rescheduled every 30 s and on every add, remove, pause, resume, finish and limit change. Checking is not queued here (milestone 3's `max_active_checks` does that). A queued torrent is `TorrentState::Queued`: no peers, `stopped` announced. **User pause beats the queue:** a paused torrent stays paused whatever its flag; `resume` returns it to the queue, which may queue it at once.
- **Seed limits off by default** (`seed_ratio_limit: None`, `seed_time_limit: None`): libtorrent's 2.0 ratio default surprises users and clients override it anyway. Reaching one pauses the torrent, clears `auto_managed` and emits `SeedLimitReached`.
- **`stopped` semantics:** every tracker that answered `started` in the current run gets `stopped` (in parallel, bounded together by `STOPPED_TIMEOUT` = 5 s), not only the last one; trackers that never answered get nothing; queueing and seed limits stop the run like pause.
- **Per-IP connection limit:** `max_peers_per_ip` = 1 per torrent (libtorrent's `allow_multiple_connections_per_ip` = false), IPv4-mapped addresses canonicalised; loopback addresses are exempt so same-host clients and the test suites work.
- **IP filter and bans:** `fbt_net::ipfilter::IpFilter` (block/allow ranges, last rule wins, merged into sorted disjoint ranges per family). Applied to outgoing dials, TCP and uTP accepts, LSD, PEX, tracker, DHT and holepunch peers, and to live connections when set (`Session::set_ip_filter`). Not to trackers or DHT nodes. Smart-ban bans live in a separate session-wide `BanList` (bounded `MAX_BANNED_IPS` = 4096, oldest evicted), so replacing the filter never unbans. Both report `Event::PeerBlocked`.
- **Smart ban, as libtorrent's `smart_ban` plugin plus trust points:** blocks remember their sender; a failed piece keeps per-block SHA-1s (at most `MAX_SMART_BAN_PIECES` = 64 per torrent); when that piece later passes, every sender whose block differs is banned. Trust points per IP per torrent: −2 per failed piece it contributed to, +1 per passed piece (capped at 8), banned at −7 (four failures with no good piece in between).
- **Proxies:** SOCKS5 (RFC 1928, username/password RFC 1929) and HTTP CONNECT (Basic auth). `ProxyConfig { proxy_peers, proxy_trackers, proxy_hostnames }` default true, as libtorrent. Web seeds follow `proxy_peers`. With SOCKS5 and `proxy_peers`, the session's UDP traffic (outgoing uTP, DHT, UDP trackers) goes through one UDP ASSOCIATE, IPv4 only (the DHT runs IPv4-only then); incoming uTP arrives only as the relay forwards it. With an HTTP proxy, UDP cannot be proxied: outgoing uTP is off, UDP trackers fail with `TrackerError::ProxyUnsupported`, and the DHT runs direct unless `dht_enabled` is false (documented on `ProxyConfig`). UDP tracker hostnames are resolved locally even through SOCKS5 (Deferred). Listening is unchanged.
- **Web seeds:** a pseudo-peer per URL (`PeerKey` from the actor's counter, `HaveAll` in the picker), one request in flight, up to `WEB_SEED_REQUEST_MAX` = 4 MiB of whole blocks per request split by file, through `TrackerClient::fetch_range` (shared client, keep-alive, TLS, proxy, at most 3 redirects). Errors back off 30 s doubling to 30 min; 4xx other than 408 and 429 retires the seed. Bytes count toward the download rate meter, and a seed waits while the torrent's download bucket has no tokens. At most `MAX_WEB_SEEDS` = 16 per torrent.
- **Scrape (BEP 48, milestone 1 deferral):** `TrackerClient::scrape(url, hashes)`, HTTP by the `announce` → `scrape` path rule and UDP action 2, at most `MAX_SCRAPE_HASHES` = 74 per request; `TorrentHandle::scrape_trackers()` reports through `Event::ScrapeReply`/`ScrapeFailed`.
- **DHT:** BEP 33 bloom filters (2048 bits, two SHA-1-derived indices) on `get_peers` with `scrape=1` and a `seed` flag on `announce_peer`; BEP 51 `sample_infohashes` answered from the peer store (`interval` 6 h, at most 20 samples, as libtorrent) and one query from the client side (crawling is the client's job); BEP 44 items in a bounded store (`max_items` = 700 as libtorrent's `max_dht_items`, 2 h lifetime), `v` at most 1000 bytes, salt at most 64, error codes 205/206/207/301/302. Session wrappers return `SessionError::DhtDisabled` with the DHT off.
- **Size cut:** IP filter file parsers (eMule `.dat`, PeerGuardian `.p2p`) and bencode serde go to the backlog; every scope item of section 8 item 5 is in this plan.
- **New fuzz targets:** `resume`, `socks5`, `http_connect`, `scrape` (HTTP and UDP scrape responses), `web_seed_head` (Content-Range parsing). `krpc` gets seeds for the new methods. 
- Version `0.5.0`; changelog entries go under `## [Unreleased]`.

### Pre-flight rulings

None yet. The pre-flight scan, when it runs, records its findings in `.superpowers/sdd/2026-11-12-milestone-5-persistence-operations/preflight.md` and the controller indexes the rulings here, per task, as milestone 4 did.

---

## File Structure

```
Cargo.toml                                          serde, unicode-normalization, ed25519-dalek, tower-service, serde_json
crates/fbt-metainfo/src/{metainfo.rs,magnet.rs,collide.rs}   web_seeds, ws=, collision renames
crates/fbt-net/src/proxy/{mod.rs,socks5.rs,http.rs}         ProxyConfig, connect_tcp, udp_associate, framing
crates/fbt-net/src/ipfilter.rs                     IpFilter
crates/fbt-net/src/demux.rs                        SharedUdpSocket::bind_relayed
crates/fbt-net/tests/{proxy.rs,support/fake_socks5.rs,support/fake_http_proxy.rs}
crates/fbt-tracker/src/{connector.rs,scrape.rs,http.rs,udp.rs,lib.rs,types.rs}
crates/fbt-dht/src/{krpc.rs,server.rs,node.rs,lookup.rs,store.rs,items.rs,bloom.rs}
crates/fbt-session/src/
  settings.rs, error.rs, event.rs, status.rs, lib.rs, session.rs     new fields, variants, wrappers
  queue.rs                                         QueueSettings, plan(), scheduler task
  bans.rs                                          BanList
  resume.rs                                        ResumeData encode/decode, SessionState
  storage/{mod.rs,fs.rs,part.rs,stamp.rs}          trait additions, part file, moves, renames, stamps
  peer/conn/mod.rs, listener.rs, net.rs            proxied dials, filter at accept, relayed UDP
  torrent/{smartban.rs,webseed.rs,storage_ops.rs}  smart ban, web seeds, move/rename commands
  torrent/{actor.rs,commands.rs,data.rs,check.rs,swarm.rs,announcer.rs,mod.rs}   wiring
crates/fbt-session/tests/{proxy.rs,ipfilter.rs,smartban.rs,partfile.rs,move.rs,queue.rs,webseed.rs,resume.rs,dht_items.rs}
crates/fbt-session/tests/support/{mod.rs,web_seed.rs}
crates/fbt/src/lib.rs, crates/fbt/tests/lib_surface.rs
crates/fbt-example/src/main.rs, crates/fbt-example/tests/cli.rs
fuzz/fuzz_targets/{resume,socks5,http_connect,scrape,web_seed_head}.rs, fuzz/seeds/
dev/interop/{run.sh,README.md,compose.yaml,socks5/Dockerfile,socks5/danted.conf,webseed/nginx.conf}
README.md, CHANGELOG.md, CLAUDE.md
```

---

### Task 1: serde for public config structs

Spec 5.3; deferred since milestone 1.

**Files:**
- Modify: `Cargo.toml` (`serde = { version = "1", default-features = false, features = ["std", "derive"] }`, `serde_json = "1"`), `crates/{fbt-tracker,fbt-session,fbt}/Cargo.toml` (feature `serde = ["dep:serde", ...]`, chained `fbt/serde → fbt-session/serde → fbt-tracker/serde`), `crates/fbt-tracker/src/types.rs`, `crates/fbt-session/src/{settings.rs,bandwidth.rs,choker.rs,session.rs,stream/mod.rs}`, `crates/fbt/src/lib.rs` (feature doc).
- Test: `crates/fbt-session/tests/serde.rs` (`#![cfg(feature = "serde")]`).

**Interfaces:**
- Produces: under `cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))` and `#[serde(default)]` on structs: `Settings`, `TrackerConfig`, `ReadAhead`, `RateLimit`, `PeerClassLimits`, `MixedMode`, `ChokingAlgorithm`, `SeedChokingAlgorithm`, `EncryptionPolicy`, `Priority`, `RemoveMode`. Hidden (`#[doc(hidden)]`) fields serialise too. The pattern every later task follows for its own config types (`ProxyConfig`, `IpFilter`, `MoveMode`, `QueueSettings`).
- `fbt-net` gets the same `serde` feature in Task 3 (its first config type).

- [ ] **Step 1: Write failing tests** in `tests/serde.rs`: `settings_round_trip_through_json` (`Settings::default()` with every non-default field changed once → `serde_json::to_string` → `from_str` equals field by field; `Settings` has no `PartialEq`, so compare the `Debug` strings); `missing_fields_take_defaults` (`from_str("{}")` gives `Settings::default()`'s `Debug`); `read_ahead_and_priority_round_trip`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --features serde --test serde`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test -p fbt-session --features serde && cargo clippy --workspace --all-targets --all-features -- -D warnings && bash dev/check-pure-rust.sh && cargo deny --all-features check`: PASS.
- [ ] **Step 5: Commit.** `feat(session): serde for public config structs behind a serde feature` No trailers or attribution of any kind.

---

### Task 2: metainfo: web seed lists and collision renames

**Files:**
- Create: `crates/fbt-metainfo/src/collide.rs`.
- Modify: `Cargo.toml` (`unicode-normalization = "0.1"`), `crates/fbt-metainfo/Cargo.toml`, `crates/fbt-metainfo/src/{metainfo.rs,magnet.rs,lib.rs,path.rs}`, `crates/fbt-metainfo/tests/{metainfo.rs,magnet.rs}`, `fuzz/seeds/{metainfo,magnet}/`.

**Interfaces:**
- Produces:
  - `Metainfo::web_seeds: Vec<String>` from `url-list` (a string or a list of strings; `http`/`https` only, de-duplicated, at most `MAX_WEB_SEEDS` = 16, each at most `MAX_URL_LEN` = 2048 bytes; others dropped silently). `Magnet::web_seeds: Vec<String>` from `ws=` with the same rules; `Magnet::to_uri` writes them.
  - `pub(crate) fn resolve_collisions(files: &mut [FileEntry])` in `collide.rs`, called by `parse_info` after sanitisation: keys as in Decisions, an `O(files)` pass with a `HashSet<String>` of file keys and one of directory-prefix keys. A renamed component is `format!("{stem}.{n}{ext}")` (`ext` from the last `.`, none for dotfiles), cut on a char boundary to `MAX_COMPONENT_LEN` (the existing `path.rs` constant, made `pub(crate)`) by shortening the stem. `n` tries `1..=files.len() + 1`.
  - `pub fn sanitize_path` stays the one sanitiser; `RelPath::key(&self) -> String` (the collision key) is public for Task 9's rename check.

- [ ] **Step 1: Write failing tests.**
  - `tests/metainfo.rs`: `url_list_as_string_and_list`, `url_list_drops_non_http_and_caps` (20 URLs → 16, an `ftp://` and a 3000-byte URL dropped), `case_collision_is_renamed` (`A.txt`, `a.txt` → `A.txt`, `a.1.txt`), `normalisation_collision_is_renamed` (`caf\u{e9}` and `cafe\u{301}` → the second `cafe\u{301}.1`), `file_and_directory_collide` (`a` then `a/b`: `a/b` becomes `a.1/b`), `renamed_name_avoids_existing` (`x.txt`, `x.1.txt`, `X.txt` → third is `X.2.txt`), `pad_files_never_renamed`, `long_component_stays_within_the_cap`, `info_hash_unchanged_by_renames`.
  - `tests/magnet.rs`: `ws_round_trips`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-metainfo`
- [ ] **Step 3: Implement.** Add seeds with `url-list` and colliding paths to `fuzz/seeds/metainfo/`, one with `ws=` to `fuzz/seeds/magnet/`.
- [ ] **Step 4: Run.** `cargo test -p fbt-metainfo && cargo clippy -p fbt-metainfo --all-targets -- -D warnings && bash dev/check-pure-rust.sh`; then `cargo +nightly fuzz run metainfo ... -- -max_total_time=60` and the same for `magnet` and `path`: no crash.
- [ ] **Step 5: Commit.** `feat(metainfo): web seed lists and renames for colliding paths` No trailers or attribution of any kind.

---

### Task 3: proxy clients in `fbt-net`

**Files:**
- Create: `crates/fbt-net/src/proxy/{mod.rs,socks5.rs,http.rs}`, `crates/fbt-net/tests/proxy.rs`, `crates/fbt-net/tests/support/{fake_socks5.rs,fake_http_proxy.rs}`, `fuzz/fuzz_targets/{socks5.rs,http_connect.rs}`, `fuzz/seeds/{socks5,http_connect}/`.
- Modify: `crates/fbt-net/{Cargo.toml,src/lib.rs}` (`serde` feature, `pub mod proxy`), `fuzz/Cargo.toml`.

**Interfaces:**
- Produces (public, `fbt_net::proxy`):
  ```rust
  #[non_exhaustive] #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
  pub enum ProxyKind { #[default] None, Socks5, Http }
  #[non_exhaustive] #[derive(Debug, Clone, PartialEq, Eq)]   // Default: kind None, port 1080, flags true
  pub struct ProxyConfig {
      pub kind: ProxyKind, pub host: String, pub port: u16,
      pub username: Option<String>, pub password: Option<String>,
      pub proxy_peers: bool, pub proxy_trackers: bool, pub proxy_hostnames: bool,
  }
  pub enum ProxyTarget { Addr(SocketAddr), Host(String, u16) }
  #[derive(Debug, thiserror::Error)] #[non_exhaustive]
  pub enum ProxyError { Io(io::Error), Timeout, AuthFailed, Refused(u8), HttpStatus(u16), Protocol(&'static str), Unsupported }
  pub async fn connect_tcp(cfg: &ProxyConfig, target: ProxyTarget, timeout: Duration) -> Result<TcpStream, ProxyError>;
  pub struct Socks5Udp { /* control: TcpStream, relay: SocketAddr */ }
  pub async fn udp_associate(cfg: &ProxyConfig, timeout: Duration) -> Result<Socks5Udp, ProxyError>;
  impl Socks5Udp { pub fn relay(&self) -> SocketAddr; pub async fn closed(&mut self); }
  pub fn encode_udp(dst: SocketAddr, payload: &[u8], out: &mut Vec<u8>);
  pub fn decode_udp(datagram: &[u8]) -> Option<(SocketAddr, &[u8])>;
  #[doc(hidden)] pub fn parse_socks5_reply(buf: &[u8]) -> Result<Option<(SocketAddr, usize)>, ProxyError>;
  #[doc(hidden)] pub fn parse_connect_head(buf: &[u8]) -> Result<Option<(u16, usize)>, ProxyError>;
  pub const MAX_CONNECT_HEAD: usize = 8192;
  ```
  - SOCKS5 offers no-auth, plus user/pass when a username is set; usernames and passwords over 255 bytes are `Protocol`. A domain-typed bound address in a reply is read and ignored for CONNECT, and is `Unsupported` for UDP ASSOCIATE (the relay must be an address). A relay of `0.0.0.0` means the proxy's own address. `decode_udp` returns `None` for `FRAG != 0`, a domain `ATYP`, or a short datagram. `connect_tcp` reads the HTTP CONNECT head byte-wise until `\r\n\r\n`, so no tunnel byte is consumed; a 2xx is success.
  - `ProxyConfig::validate(&self) -> Result<(), String>` (host non-empty and port non-zero unless `None`).
- Fakes (`#[path]`-included by session tests, as the NAT fakes are): `FakeSocks5::start(Script) -> FakeSocks5` with `addr()`, `connects() -> Vec<String>` (targets as text), `udp_targets()`, and scripts `Ok`, `RequireAuth(user, pass)`, `Refuse(u8)`, `Garbage`, `CloseAfterGreeting`, `RelayFromStranger`, `Fragmented`; it relays CONNECT and UDP for real on loopback. `FakeHttpProxy::start(Script)` with `Ok`, `Status(407)`, `EndlessHead`, `RequireBasic(user, pass)`.

- [ ] **Step 1: Write failing tests** in `tests/proxy.rs`: `socks5_connect_by_addr_and_host` (echo server through the fake; `connects()` names `127.0.0.1:<p>` and `localhost:<p>`), `socks5_user_pass`, `socks5_wrong_password_is_auth_failed`, `socks5_refusal_carries_the_code` (`Refused(5)`), `garbage_reply_is_a_protocol_error`, `closed_mid_handshake` (an `Io` or `Protocol` error, within the timeout), `http_connect_tunnels_and_keeps_bytes` (the echo answer's first byte arrives after the head), `http_407_is_http_status`, `endless_connect_head_is_capped` (fails after reading at most `MAX_CONNECT_HEAD` bytes), `udp_associate_round_trip` (a datagram to a loopback echo comes back decoded with the echo's address), `fragmented_udp_is_dropped` (`decode_udp` of a `FRAG = 1` datagram, of a domain `ATYP` and of a 5-byte datagram is `None`; filtering by source is the socket's job, Task 4), `encode_decode_round_trip` (proptest over v4/v6 addresses and payloads).
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-net --test proxy`
- [ ] **Step 3: Implement.** Fuzz targets: `socks5` drives `parse_socks5_reply` and `decode_udp`; `http_connect` drives `parse_connect_head`. Seeds: a success reply per address type, a refusal, a 200 and a 407 head.
- [ ] **Step 4: Run.** `cargo test -p fbt-net && cargo clippy -p fbt-net --all-targets --all-features -- -D warnings`; both fuzz targets 60 s each: no crash.
- [ ] **Step 5: Commit.** `feat(net): SOCKS5 and HTTP CONNECT proxy clients` No trailers or attribution of any kind.

---

### Task 4: proxy wiring: peers, UDP and HTTP trackers

**Files:**
- Create: `crates/fbt-tracker/src/connector.rs`, `crates/fbt-session/tests/proxy.rs`.
- Modify: `Cargo.toml` (`tower-service = "0.3"`), `crates/fbt-tracker/{Cargo.toml,src/lib.rs,src/http.rs,src/tls.rs,src/types.rs}` (`fbt-net` dependency), `crates/fbt-net/src/demux.rs`, `crates/fbt-session/Cargo.toml` (its `serde` feature now also enables `fbt-net/serde`, for `Settings::proxy`), `crates/fbt-session/src/{settings.rs,session.rs,net.rs,peer/conn/mod.rs}`.

**Interfaces:**
- Consumes: Task 3's `ProxyConfig`, `connect_tcp`, `udp_associate`, `encode_udp`, `decode_udp`.
- Produces:
  - `connector.rs`: `pub(crate) struct ProxyConnector { proxy: Option<ProxyConfig>, timeout: Duration }` implementing `tower_service::Service<Uri>` with `Response = TokioIo<TcpStream>`: direct `TcpStream::connect` without a proxy; through `connect_tcp` with `ProxyTarget::Host` when `proxy_hostnames`, else resolved locally and `ProxyTarget::Addr`. `tls::build_connector(proxy)` wraps it with `HttpsConnectorBuilder::...wrap_connector`.
  - `TrackerClient::new_with(cfg: TrackerConfig, udp: Vec<Arc<dyn DatagramSender>>, proxy: Option<ProxyConfig>) -> Result<TrackerClient, TrackerError>` replaces `with_udp_sockets` (`new` keeps its signature). `TrackerError::Proxy(String)` and `TrackerError::ProxyUnsupported` (a UDP tracker with an HTTP proxy).
  - `SharedUdpSocket::bind_relayed(local: SocketAddr, assoc: Socks5Udp) -> io::Result<SharedUdpSocket>`: its sender frames with `encode_udp` to the relay; the receive loop keeps only datagrams from the relay that decode, dispatching them by `classify` with the decoded source; others count in `DemuxStats::dropped`. When the association's control connection closes, the socket stops and logs a warning; `DemuxStats` gains `relay_lost: bool`.
  - `Settings::proxy: ProxyConfig` (validated through `ProxyConfig::validate`).
  - `peer/conn/mod.rs` `connect()`: TCP dials through `connect_tcp(ProxyTarget::Addr)` with `connect_timeout` when `kind != None && proxy_peers`.
  - `Session::new`: with SOCKS5 and `proxy_peers`, one `udp_associate` per session replaces the first IPv4 endpoint's UDP socket with `bind_relayed`; other endpoints bind no UDP socket and the DHT runs IPv4-only on the relayed one. With HTTP and `proxy_peers`, `enable_outgoing_utp` is treated as off. A failed association fails `Session::new` with `SessionError::Proxy(String)` (new variant).
  - `fbt` re-exports `ProxyConfig` and `ProxyKind`.

- [ ] **Step 1: Write failing tests** in `crates/fbt-session/tests/proxy.rs` (fakes via `#[path]`):
  - `peer_dial_goes_through_socks5`: a leech with `proxy` aimed at the fake downloads from a loopback seed; the fake's `connects()` names the seed's address; the torrent finishes.
  - `peer_dial_goes_through_http_connect`: the same with `FakeHttpProxy`; uTP is never tried (the seed's `demux_stats()` shows no uTP datagram).
  - `http_tracker_through_proxy_by_hostname`: the fake HTTP tracker (from `tests/trackers.rs`, moved into `support`) at `http://localhost:<p>/announce`; `connects()` names `localhost:<p>` (no local resolution).
  - `utp_and_udp_tracker_go_through_the_relay`: SOCKS5 with `dial_tcp` off for the seed side: the leech reaches the seed over uTP and announces to the fake UDP tracker; `udp_targets()` lists both.
  - `udp_from_non_relay_is_dropped`: script `RelayFromStranger`; `demux_stats()[0].1.dropped > 0` and nothing is dispatched.
  - `failed_association_fails_session_new`: script `Refuse(1)` gives `SessionError::Proxy`.
  - `trackers.rs` keeps passing unchanged (direct path).
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test proxy`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace && cargo clippy --workspace --all-targets --all-features -- -D warnings && bash dev/check-pure-rust.sh && cargo deny --all-features check`: PASS.
- [ ] **Step 5: Commit.** `feat(session): peers, uTP, DHT and trackers through SOCKS5 or HTTP proxies` No trailers or attribution of any kind.

---

### Task 5: trackers: `stopped` to every tracker, and scrape

**Files:**
- Create: `crates/fbt-tracker/src/scrape.rs`, `fuzz/fuzz_targets/scrape.rs`, `fuzz/seeds/scrape/`.
- Modify: `crates/fbt-tracker/src/{lib.rs,types.rs,http.rs,udp.rs}`, `crates/fbt-session/src/torrent/{announcer.rs,commands.rs,mod.rs}`, `crates/fbt-session/src/event.rs`, `crates/fbt-session/tests/trackers.rs`, `fuzz/Cargo.toml`.

**Interfaces:**
- Produces:
  - `#[non_exhaustive] pub struct ScrapeInfo { pub info_hash: [u8; 20], pub complete: u32, pub downloaded: u32, pub incomplete: u32 }`; `pub const MAX_SCRAPE_HASHES: usize = 74;` `TrackerClient::scrape(&self, url: &str, hashes: &[[u8; 20]]) -> Result<Vec<ScrapeInfo>, TrackerError>`: HTTP when the last path segment starts with `announce` (replaced by `scrape`, query kept, one `info_hash` per hash), else `TrackerError::UnsupportedScheme("scrape")`; UDP action 2 over the same connection-id cache. Hashes absent from the reply are left out. `#[doc(hidden)] pub fn parse_http_scrape(&[u8]) -> Result<Vec<ScrapeInfo>, TrackerError>` and `parse_udp_scrape(&[u8], tid: u32, n: usize)`.
  - Announcer: `Run::answered: Vec<String>` (URLs that answered `started` in this run); `stopped()` announces to each in parallel under one `STOPPED_TIMEOUT`; `last_ok` goes. `Signals::scrapes: u64` (bumped by `Cmd::Scrape`) makes the task scrape the first working tracker of each tier.
  - `TorrentHandle::scrape_trackers(&self) -> Result<(), SessionError>`; `Event::ScrapeReply { torrent, url, complete, incomplete, downloaded }` and `Event::ScrapeFailed { torrent, url, message }`, both `EventMask::TRACKER`.

- [ ] **Step 1: Write failing tests.**
  - `fbt-tracker` (`src/tests.rs` and `udp.rs` tests with the existing fakes): `http_scrape_url_rule` (`/announce` → `/scrape`, `/x/announce.php?k=1` → `/x/scrape.php?k=1`, `/a` unsupported), `http_scrape_parses_files`, `udp_scrape_round_trip`, `scrape_caps_hashes` (75 hashes give `TrackerError::Failure("too many info hashes")` before any request).
  - `tests/trackers.rs`: `stopped_goes_to_every_started_tracker` (two tiers, both answer `started`; shutdown sends `stopped` to both), `stopped_skips_trackers_that_never_answered`, `scrape_trackers_reports_counts` (`ScrapeReply` with the fake's counts).
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-tracker && cargo test -p fbt-session --test trackers`
- [ ] **Step 3: Implement.** Fuzz target `scrape` drives both parsers.
- [ ] **Step 4: Run.** `cargo test --workspace`; `scrape` fuzz 60 s: no crash.
- [ ] **Step 5: Commit.** `feat(tracker): scrape and stopped to every tracker that answered` No trailers or attribution of any kind.

---

### Task 6: IP filter, per-IP connection limit and the ban list

**Files:**
- Create: `crates/fbt-net/src/ipfilter.rs`, `crates/fbt-session/src/bans.rs`, `crates/fbt-session/tests/ipfilter.rs`.
- Modify: `crates/fbt-net/src/lib.rs`, `crates/fbt-session/src/{settings.rs,session.rs,listener.rs,event.rs,lib.rs,peer/messages.rs}`, `crates/fbt-session/src/torrent/{actor.rs,swarm.rs,known.rs,holepunch.rs,mod.rs}`.

**Interfaces:**
- Produces:
  - `fbt_net::ipfilter::IpFilter` (`Clone`, `Default` = allow all, `PartialEq`, serde): `block(&mut self, range: RangeInclusive<IpAddr>)`, `allow(&mut self, range: RangeInclusive<IpAddr>)` (mixed families or `start > end` ignored), `is_blocked(&self, ip: IpAddr) -> bool` (IPv4-mapped canonicalised, `O(log n)` over `BTreeMap<u128, (u128, bool)>` per family), `len(&self) -> usize` (disjoint ranges) and `is_empty(&self) -> bool`, `MAX_RANGES` = 1,000,000 (rules beyond it are ignored and logged once).
  - `pub(crate) struct BanList` in `bans.rs`, keyed by `SocketAddr`: port 0 bans the whole IP (the normal mode), a non-zero port one endpoint (only under the hidden `Settings::ban_endpoints`, false by default, so loopback tests can ban one scripted peer and not its neighbours): `ban(&mut self, addr: SocketAddr) -> bool` (new), `is_banned(&self, addr: SocketAddr) -> bool`, `list(&self) -> Vec<SocketAddr>`, bounded `MAX_BANNED_IPS` = 4096 (oldest out). `Shared` gains `ip_filter: RwLock<Arc<IpFilter>>` and `bans: Mutex<BanList>`; `Shared::blocked(&self, ip) -> Option<BlockReason>` checks both.
  - `#[non_exhaustive] pub enum BlockReason { IpFilter, Banned, PerIpLimit }`; `Event::PeerBlocked { torrent: Option<InfoHashes>, addr: SocketAddr, reason: BlockReason }` (`EventMask::PEER`; `None` for an accept refused before the handshake named a torrent); `DisconnectReason::Blocked(BlockReason)`.
  - `Session::set_ip_filter(&self, f: IpFilter)` (every torrent receives `Cmd::FilterChanged` and closes peers now blocked), `Session::ip_filter(&self) -> IpFilter`, `Session::banned_peers(&self) -> Vec<SocketAddr>` (port 0 for a whole IP).
  - `Settings::max_peers_per_ip: usize` (1; non-zero) and `#[doc(hidden)] Settings::per_ip_exempt_loopback: bool` (true; tests turn it off, since every test peer is on `127.0.0.1`); checked in `on_connected`/`on_incoming` against the torrent's live peers by canonical IP.
  - Filter points: `ListenerState::admit` (TCP and uTP accepts, before reading anything), `try_connect`/`queue_addr` (dials from every source), `on_found_peers` (dropped before they enter `Known`), holepunch `Connect` targets.
  - `fbt` re-exports `IpFilter` and `BlockReason`.

- [ ] **Step 1: Write failing tests.**
  - `ipfilter.rs` unit: `last_rule_wins_and_ranges_merge`, `v4_mapped_is_canonical`, `families_are_separate`, `reversed_range_is_ignored`, plus a proptest `matches_a_naive_rule_list` (random rules and probes against a linear last-match scan).
  - `tests/ipfilter.rs`: `blocked_seed_is_never_dialled` (an `add_peer` of a blocked address emits `PeerBlocked` and no TCP connect reaches the seed's listener), `blocked_incoming_is_closed_before_handshake`, `setting_a_filter_closes_live_peers` (`PeerDisconnected` with `Blocked(IpFilter)`), `second_connection_from_one_ip_is_refused` (`per_ip_exempt_loopback = false`: of two scripted peers on `127.0.0.1`, the second is closed with `Blocked(PerIpLimit)` and `PeerBlocked`), `loopback_is_exempt_by_default` (the same two peers both stay connected).
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-net ipfilter && cargo test -p fbt-session --test ipfilter`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS (the swarm, pex, lsd and holepunch suites unchanged).
- [ ] **Step 5: Commit.** `feat(session): IP filter, per-IP connection limit and a session ban list` No trailers or attribution of any kind.

---

### Task 7: smart ban

**Files:**
- Create: `crates/fbt-session/src/torrent/smartban.rs`, `crates/fbt-session/tests/smartban.rs`.
- Modify: `crates/fbt-session/src/torrent/{data.rs,actor.rs,mod.rs,swarm.rs}`, `crates/fbt-session/tests/support/mod.rs` (`ScriptedPeer::serve_corrupt(payload, piece_len, bad_blocks: HashSet<BlockRef>)`).

**Interfaces:**
- Consumes: `BanList`, `BlockReason::Banned`, `DisconnectReason::Blocked` (Task 6).
- Produces (crate-internal):
  - `pub(super) enum BlockSource { Peer(SocketAddr), WebSeed(usize) }` (the peer's ban key: port 0 unless `ban_endpoints`; Task 11 uses `WebSeed`). Trust is kept per ban key.
  - `pub(super) struct SmartBan` owned by the actor: `record_block(piece, block_index, BlockSource)`, `piece_failed(piece, block_hashes: Vec<[u8; 20]>) -> Vec<BlockSource>` (records per-block hashes and senders, at most `MAX_SMART_BAN_PIECES`, oldest record dropped; returns senders whose trust fell to −7 or below), `piece_passed(piece, block_hashes: Option<Vec<[u8; 20]>>) -> Vec<BlockSource>` (returns senders whose recorded block differs, and credits +1 to each contributor up to 8), `wants_block_hashes(piece) -> bool`, `forget(piece)` (on abandon or skip). Trust map bounded by `MAX_TRUST_ENTRIES` = 4096.
  - `Job::Hashed` gains `block_hashes: Option<Vec<[u8; 20]>>`, computed in the hash job when the piece fails, or passes while `wants_block_hashes`.
  - `Actor::ban(&mut self, s: BlockSource)`: a peer's ban key goes into `Shared::bans`, every peer matching it is closed with `Blocked(Banned)` and `PeerBlocked` is emitted; a web seed is retired (Task 11 fills this arm; until then it is unreachable and logged).

- [ ] **Step 1: Write failing tests.**
  - `smartban.rs` unit: `trust_points_ban_after_four_failures`, `a_passed_piece_restores_trust`, `records_are_bounded`, `only_differing_blocks_are_blamed`.
  - `tests/smartban.rs` (`ban_endpoints = true`, scripted peers on `127.0.0.1`, a real seed):
    - `only_the_liar_is_banned`: peer A sends block 0 of piece 3 corrupt, peer B sends the rest honestly; the piece fails; the real seed then serves piece 3; `banned_peers()` holds A's address, a `PeerBlocked { reason: Banned }` names A, and none names B.
    - `honest_contributor_keeps_its_trust`: B's connection stays open and it is still asked for blocks.
    - `single_liar_banned_by_trust_points`: one peer corrupting every piece is banned after its fourth failed piece, with no passing piece needed.
    - `banned_peer_is_not_redialled`: `add_peer` of the banned address emits `PeerBlocked` again and no connection reaches it.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test smartban && cargo test -p fbt-session smartban::`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS (`HashFailed` tests in `tests/swarm.rs` unchanged).
- [ ] **Step 5: Commit.** `feat(session): smart ban of peers that send bad blocks` No trailers or attribution of any kind.

---

### Task 8: the part file

Spec 5.4; deferred from milestones 1 and 4.

**Files:**
- Create: `crates/fbt-session/src/storage/part.rs`, `crates/fbt-session/tests/partfile.rs`.
- Modify: `crates/fbt-session/src/storage/{mod.rs,fs.rs,mem.rs}`, `crates/fbt-session/src/stream/reader.rs` (doc: a reader of a skipped file fills the part file), `crates/fbt-session/src/torrent/{commands.rs,actor.rs,check.rs,serve.rs,data.rs}`, `crates/fbt-session/src/session.rs` (`FsStorage::new` call).

**Interfaces:**
- Produces:
  - `Storage::set_file_priorities(&self, wanted: &[bool]) -> io::Result<()>` with a default `Ok(())` body; called on the disk pool before the first check and after every `set_priorities` (after a flush of the affected pieces), never on an async worker.
  - `part.rs`: `pub(crate) struct PartFile` (`open(path, num_pieces, piece_len) -> io::Result<PartFile>` validating the header, a corrupt or mismatched one being renamed to `.parts.bad` and started afresh with a warning; `write(piece, offset_in_piece, data)`, `read(piece, offset_in_piece, buf) -> io::Result<()>` (`NotFound` for a piece without a slot), `free(piece)`, `is_empty()`, `path()`). Slots are allocated lowest-free first; the slot table is rewritten on every allocation or free (`num_pieces × 4` bytes, written at offset 16).
  - `FsStorage` routes each segment of a read or write that lies in a skipped non-pad file to the part file; `set_file_priorities` exports the newly wanted files' bytes from the part file into the files (created sparse) and frees slots whose pieces touch no skipped file any more, then deletes the part file if empty. `delete_files` deletes it too. `FsStorage::part_path(&self) -> PathBuf` (pub, for clients and tests).
  - Fix (milestone 4 deferral): `piece_lost` of a piece no wanted file overlaps leaves `finished_emitted` alone, so `TorrentFinished` is not emitted twice.

- [ ] **Step 1: Write failing tests.**
  - `part.rs` unit: `slots_round_trip_and_reuse`, `corrupt_header_starts_afresh`, `slot_table_is_bounded_by_piece_count` (a header claiming `u32::MAX` pieces for a 4-piece torrent is rejected as mismatched).
  - `tests/partfile.rs` (files A 300,000 B `Skip`, B 500,000 B `Normal`, C 300,000 B `Skip`, piece 256 KiB):
    - `skipped_neighbour_is_never_created`: after finishing, neither A nor C exists, `part_path()` exists, and reading B back is correct.
    - `unskipping_exports_from_the_part_file`: set A to `Normal`; after finishing, A matches and its boundary bytes came from the part file (the seed received no request for the boundary piece again).
    - `reskipping_keeps_data_readable`.
    - `recheck_reads_the_part_file`: `force_recheck` after finishing keeps every wanted piece.
    - `delete_removes_the_part_file`.
    - `reader_on_a_skipped_file_reads_the_part_file`: a `FileReader` on C reads its first 1 MiB window correctly while C does not exist on disk.
    - `lost_skipped_piece_does_not_refinish` (the milestone 4 deferral): exactly one `TorrentFinished`.
  - The milestone 1 test `skipped_file_is_not_downloaded` tightens to "the skipped file is absent".
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test partfile && cargo test -p fbt-session part::`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS.
- [ ] **Step 5: Commit.** `feat(session): part file so skipped files are never created` No trailers or attribution of any kind.

---

### Task 9: move storage and rename files

**Files:**
- Create: `crates/fbt-session/src/torrent/storage_ops.rs`, `crates/fbt-session/tests/move.rs`.
- Modify: `crates/fbt-session/src/storage/{mod.rs,fs.rs}`, `crates/fbt-session/src/torrent/{mod.rs,commands.rs,actor.rs,readers.rs,serve.rs}`, `crates/fbt-session/src/{event.rs,error.rs,status.rs}`.

**Interfaces:**
- Consumes: `FlushReason::All` and `flush_and_wait` (milestone 3); the part file (Task 8); `RelPath::key` (Task 2).
- Produces:
  - `#[non_exhaustive] #[derive(Default, ...)] pub enum MoveMode { #[default] FailIfExists, ReplaceExisting, KeepExisting }` (serde).
  - `Storage::move_to(&self, save_path: &Path, mode: MoveMode) -> io::Result<()>` and `Storage::rename_file(&self, index: usize, path: &RelPath) -> io::Result<()>`, both defaulting to `Err(io::ErrorKind::Unsupported)`. `FsStorage` keeps `save_path` and entry paths in its `State` (so they can change), closes cached handles first, moves with `fs::rename` and falls back on `CrossesDevices` (or any rename error on Windows) to copy, `sync_all`, delete; the part file moves too; empty directories the torrent created under the old path are removed; a failure moves back what moved.
  - `TorrentHandle::move_storage(&self, path: PathBuf, mode: MoveMode) -> Result<(), SessionError>` and `TorrentHandle::rename_file(&self, index: usize, path: &str) -> Result<(), SessionError>` (splits on `/`, `sanitize_path`, refuses a key equal to another file's with `SessionError::InvalidPath`). Both reply once the job ended; failure is also an event.
  - Actor: `storage_busy: bool`; while set, peer reads and reader reads wait in `PieceReads`/`readers.waiting` and no write is issued; dirty pieces arriving meanwhile stay in the cache. `KeepExisting` then calls `recheck()`.
  - `Event::StorageMoved { torrent, path }`, `Event::StorageMoveFailed { torrent, message }`, `Event::FileRenamed { torrent, index, path }`, `Event::FileRenameFailed { torrent, index, message }` (`EventMask::STORAGE`); `SessionError::InvalidPath(String)` and `SessionError::Unsupported`. `TorrentStatus::save_path: PathBuf`.
  - `fbt` re-exports `MoveMode`.

- [ ] **Step 1: Write failing tests** in `tests/move.rs` (tempdirs; the cross-filesystem path is exercised by a unit test that forces the copy fallback through a `#[cfg(test)]` switch on `FsStorage`):
  - `move_flushes_dirty_pieces_first`: large `cache_size` and `cache_expiry` so pieces stay dirty; `move_storage` while downloading; after `StorageMoved`, the old directory holds no torrent file and the torrent finishes at the new path with correct data.
  - `reader_survives_a_move`: a read pending on a missing piece completes after the move.
  - `move_to_same_path_is_a_noop`, `fail_if_exists_moves_nothing` (a file exists at the target: error, no file moved, torrent keeps seeding), `replace_existing_overwrites`, `keep_existing_rechecks` (state passes through `CheckingFiles`).
  - `failed_move_rolls_back` (a read-only target directory on Unix, a file locked open on Windows): `StorageMoveFailed`, data still at the old path.
  - `copy_fallback_moves_the_part_file` (unit, `fs.rs`).
  - `rename_file_then_serve`: rename file 1 to `sub/x.bin`; the seed serves it from the new path; `rename_file` onto file 0's path is `InvalidPath`; `../x` is `InvalidPath`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test move`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS.
- [ ] **Step 5: Commit.** `feat(session): move storage and rename files` No trailers or attribution of any kind.

---

### Task 10: queueing and seed limits

**Files:**
- Create: `crates/fbt-session/src/queue.rs`, `crates/fbt-session/tests/queue.rs`.
- Modify: `crates/fbt-session/src/{settings.rs,session.rs,status.rs,event.rs,lib.rs}`, `crates/fbt-session/src/torrent/{mod.rs,commands.rs,actor.rs,announcer.rs}`.

**Interfaces:**
- Produces:
  - ```rust
    #[non_exhaustive] pub struct QueueSettings {      // serde; Default as in Decisions
        pub active_downloads: Option<usize>,   // Some(3); None = unlimited
        pub active_seeds: Option<usize>,       // Some(5)
        pub active_limit: Option<usize>,       // Some(500)
        pub dont_count_slow_torrents: bool,    // true
        pub inactive_down_rate: u64,           // 2048
        pub inactive_up_rate: u64,             // 2048
        pub startup_grace: Duration,           // 60 s
        pub interval: Duration,                // 30 s, non-zero
    }
    ```
    `Settings::queue: QueueSettings`, `Settings::seed_ratio_limit: Option<f64>` (finite and ≥ 0 when set), `Settings::seed_time_limit: Option<Duration>`.
  - `Session::set_queue_settings(&self, q: QueueSettings) -> Result<(), SessionError>` (validated, then a pass at once): clients change limits at runtime.
  - Pure `pub(crate) fn plan(entries: &[QueueEntry], cfg: &QueueSettings, now: Instant) -> Vec<bool>` (`QueueEntry { position, seeding, running_since: Option<Instant>, down_rate, up_rate, user_paused, auto_managed }`; output `true` = run), in position order, downloads and seeds counted separately and against `active_limit` together; a slow torrent past its grace does not count when `dont_count_slow_torrents`.
  - Scheduler task in `session.rs`: woken by `Shared::queue_dirty: Notify` or the interval; reads statuses, calls `plan`, sends `Cmd::SetQueued(bool)` to changed torrents. Queue positions live in the registry (`Vec<InfoHashes>` order, dense).
  - `AddTorrent::auto_managed: bool` (true), `TorrentHandle::set_auto_managed(bool)`, `TorrentHandle::set_queue_position(usize)` (clamped; `usize::MAX` = bottom), `TorrentStatus::{queue_position: usize, auto_managed: bool, active_time: Duration, seeding_time: Duration}`, `TorrentState::Queued`, `Event::SeedLimitReached { torrent }` (`EventMask::STATUS`).
  - Actor `queued: bool`: `running()` and `connectable()` are false while queued (the announcer sends `stopped` through Task 5's path); state `Queued`. Seed limits are checked on each status tick while `Seeding` (ratio = uploaded / max(downloaded, total_wanted)).
  - `fbt` re-exports `QueueSettings`.

- [ ] **Step 1: Write failing tests.**
  - `queue.rs` unit: `plan_never_exceeds_limits` (proptest over 0 to 40 entries and limits: running downloads ≤ limit counting only non-slow ones, likewise seeds, total ≤ `active_limit`; user-paused and non-auto-managed entries keep `false`/their own state), `positions_order_wins`, `slow_torrents_do_not_count_after_grace`.
  - `tests/queue.rs` (paused time where possible): `fourth_download_is_queued` (`active_downloads` 3: the fourth is `Queued`, starts when one finishes), `positions_stay_dense` (remove the middle torrent: positions are `0..n`), `set_queue_position_moves_it_up`, `user_pause_beats_the_queue`, `seed_ratio_limit_pauses_and_emits`, `seed_time_limit_pauses`, `queued_torrent_announces_stopped` (the fake tracker records `stopped`), `limits_change_at_runtime` (raise `active_downloads` through `Session::set_queue_settings`; the queued one starts within one pass).
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test queue && cargo test -p fbt-session queue::`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS. `test_settings()` sets `queue.active_downloads`, `active_seeds` and `active_limit` to `None` so existing multi-torrent tests are unaffected.
- [ ] **Step 5: Commit.** `feat(session): auto-managed queueing and seed limits` No trailers or attribution of any kind.

---

### Task 11: web seeds (BEP 19)

**Files:**
- Create: `crates/fbt-session/src/torrent/webseed.rs`, `crates/fbt-session/tests/webseed.rs`, `crates/fbt-session/tests/support/web_seed.rs`, `fuzz/fuzz_targets/web_seed_head.rs`, `fuzz/seeds/web_seed_head/`.
- Modify: `crates/fbt-tracker/src/{http.rs,lib.rs}`, `crates/fbt-session/src/torrent/{actor.rs,data.rs,mod.rs,smartban.rs}`, `crates/fbt-session/src/{event.rs,status.rs,session.rs}`.

**Interfaces:**
- Consumes: `Metainfo::web_seeds`, `Magnet::web_seeds` (Task 2); `TrackerClient` with proxy (Task 4); `BlockSource::WebSeed`, `Actor::ban` (Task 7).
- Produces:
  - `TrackerClient::fetch_range(&self, url: &str, range: Range<u64>) -> Result<Bytes, TrackerError>`: `Range: bytes=a-b`; 206 with a matching `Content-Range` (`#[doc(hidden)] pub fn parse_content_range(&str) -> Option<(u64, u64, Option<u64>)>`), or 200 only when `a == 0` and `Content-Length == b - a + 1`; the body is read up to the range length and one byte more is an error; at most 3 redirects, an `https` → `http` redirect refused; `TrackerConfig::http_timeout` bounds it.
  - `webseed.rs`: `pub(super) struct WebSeeds` (per URL: `key: PeerKey`, `state: Idle | Busy | Backoff(Instant) | Retired`, `failures: u32`), `url_for(base, meta, file_index) -> String` (BEP 19: a base ending in `/` gets `name` and, for multi-file torrents, the percent-encoded path components appended; `fbt_tracker::url_encode_bytes` encodes), `segments(layout, blocks) -> Vec<(file_index, Range<u64>)>`. `const WEB_SEED_REQUEST_MAX: u64 = 4 << 20;`, `const MAX_WEB_SEEDS: usize = 16;`, backoff 30 s × 2^n to 30 min.
  - Actor: on the status tick, each idle seed with the torrent active and download tokens available picks blocks with `picker.pick(key, ..)` (the picker sees it as a `HaveAll` peer added at start), spawns the fetches, and the result returns as `Job::WebSeed { key, blocks, result: Result<Vec<Bytes>, String> }`, fed to `on_block` with `BlockSource::WebSeed(i)`; a failure releases the blocks (`picker.release`). `TorrentHandle::add_web_seed(&self, url: String) -> Result<(), SessionError>`, `remove_web_seed`, `web_seeds(&self) -> Result<Vec<String>, SessionError>`; `TorrentStatus::num_web_seeds: u32` (non-retired); `Event::WebSeedError { torrent, url, message, retired: bool }` (`EventMask::PEER`). Private torrents use web seeds too (BEP 27 does not forbid them).
- Test server `support/web_seed.rs`: `WebSeedServer::start(root: PathBuf, Behaviour) -> WebSeedServer` with `url()`, `requests() -> Vec<(String, Option<String>)>` (path, Range header), behaviours `Normal`, `IgnoreRange`, `WrongContentRange`, `EndlessBody`, `RedirectLoop`, `NotFound(path)`, `Corrupt(offset)`.

- [ ] **Step 1: Write failing tests.**
  - `fbt-tracker`: `content_range_parsing` (valid, `*`, malformed, overflow).
  - `tests/webseed.rs`: `downloads_from_a_web_seed_alone` (single- and multi-file torrents, no peers: finishes, data matches, every request carried a `Range`), `web_seed_and_peer_together` (both deliver bytes: the server logged requests and the seed's `total_uploaded > 0`), `ignored_range_is_an_error`, `wrong_content_range_is_an_error`, `endless_body_is_capped` (the request fails and the leech holds no buffer beyond `WEB_SEED_REQUEST_MAX`), `redirect_loop_stops`, `missing_file_retires_the_seed` (`WebSeedError { retired: true }`), `corrupt_web_seed_is_retired` (`HashFailed` then `retired: true` through smart ban; a peer finishes the torrent), `percent_encoding_of_paths` (a file named `a b#c.bin`), `web_seed_through_socks5` (the fake's `connects()` names the server).
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test webseed && cargo test -p fbt-tracker content_range`
- [ ] **Step 3: Implement.** Fuzz target `web_seed_head` drives `parse_content_range`.
- [ ] **Step 4: Run.** `cargo test --workspace`; `web_seed_head` fuzz 60 s: no crash.
- [ ] **Step 5: Commit.** `feat(session): BEP 19 web seeds` No trailers or attribution of any kind.

---

### Task 12: resume data and session state

**Files:**
- Create: `crates/fbt-session/src/resume.rs`, `crates/fbt-session/src/storage/stamp.rs`, `crates/fbt-session/tests/resume.rs`, `fuzz/fuzz_targets/resume.rs`, `fuzz/seeds/resume/`.
- Modify: `crates/fbt-session/src/{session.rs,error.rs,status.rs,lib.rs}`, `crates/fbt-session/src/storage/{mod.rs,fs.rs}`, `crates/fbt-session/src/torrent/{mod.rs,actor.rs,check.rs,commands.rs,data.rs}`, `fuzz/Cargo.toml` (the target depends on `fbt-session`).

**Interfaces:**
- Consumes: everything persisted: priorities, renamed paths and save path (Task 9), queue position and auto-managed (Task 10), web seeds (Task 11), the part file stamp (Task 8), `Session::dht_state` (milestone 2).
- Produces:
  - `#[non_exhaustive] pub struct FileStamp { pub len: u64, pub mtime: i64 }`; `Storage::stamps(&self) -> io::Result<Option<Vec<Option<FileStamp>>>>` (default `Ok(None)`; `FsStorage`: one entry per file, `None` for a missing one, then one for the part file).
  - `resume.rs`: `pub(crate) struct ResumeState { have: Bitfield, stamps: Vec<Option<FileStamp>>, uploaded, downloaded, active_time, seeding_time, added: i64, completed: Option<i64>, peers: Vec<SocketAddr> }`; `pub(crate) fn encode(...) -> Vec<u8>` and `pub fn decode_resume(bytes: &[u8]) -> Result<DecodedResume, ResumeError>` (`#[doc(hidden)]`, for the fuzz target), decoded with `fbt_bencode::decode_with` limits (depth 8, size 64 MiB, at most `MAX_RESUME_PEERS` = 200 per family, list lengths checked against the file count).
  - Keys (sorted, all optional except the first three): `fbt-resume-version` (1), `info-hash` (20 bytes), `save-path`, `info` (raw dict; its SHA-1 must equal `info-hash`), `magnet` (when no `info`), `file-priorities`, `renamed` (dict of index → path list), `pieces`, `paused`, `auto-managed`, `sequential`, `queue-position`, `download-limit`, `upload-limit`, `total-uploaded`, `total-downloaded`, `active-time`, `seeding-time`, `added-time`, `completed-time`, `trackers`, `url-list`, `peers`, `peers6`, `stamps` (list of `[len, mtime]` or empty list).
  - `#[derive(Debug, thiserror::Error)] #[non_exhaustive] pub enum ResumeError { Bencode(DecodeError), Version(i64), Missing(&'static str), Invalid(&'static str), InfoHashMismatch, Metainfo(MetainfoError), Magnet(MagnetError) }`.
  - `AddTorrent::from_resume(bytes: &[u8]) -> Result<AddTorrent, ResumeError>` fills `source`, `save_path`, flags, priorities, limits and a crate-private `resume: Option<Box<ResumeState>>`; the caller may still set `storage`.
  - `TorrentHandle::save_resume_data(&self) -> Result<Vec<u8>, SessionError>`: `flush_and_wait`, then encodes on the actor; while checking, `pieces` is the bitfield the torrent had before the check (kept as `last_have`), or the resume one, or empty. `TorrentStatus::need_save_resume: bool`, set by a written piece, priority, rename, move, pause, resume, limit or queue change, cleared by a save.
  - `start_check` fast path: with `resume` present, a disk-pool job reads `stamps()` and returns `Job::ResumeChecked { generation, trusted: Option<Bitfield> }`; `Some` goes to `on_checked(generation, Ok(have))`, `None` to the normal `start_check`. Trusted when stamps are `None` (custom storage) or every file holding a saved piece byte, and the part file when a saved piece has bytes in a skipped file, has an equal stamp.
  - `#[non_exhaustive] #[derive(Debug, Clone, Default, PartialEq, Eq)] pub struct SessionState { pub dht: Option<Vec<u8>>, pub torrents: Vec<Vec<u8>> }` with `to_bytes()`/`from_bytes()` (`fbt-session-version` 1, same limits); `Session::save_state(&self) -> Result<SessionState, SessionError>` (torrents in queue order), `Session::shutdown_and_save(&self) -> SessionState` (resume data captured after each torrent's final flush, then the usual shutdown), `Session::restore(settings: Settings, state: &SessionState) -> Result<(Session, Vec<(usize, SessionError)>), SessionError>`. `SessionError::Resume(ResumeError)`.
  - `fbt` re-exports `SessionState`, `ResumeError`, `FileStamp`.

- [ ] **Step 1: Write failing tests** in `tests/resume.rs` (and unit tests in `resume.rs`):
  - `resume_round_trip_without_recheck`: half-downloaded torrent, `save_resume_data`, shutdown; a new session adds `from_resume`: the state never shows `CheckingFiles`, `pieces()` equals the saved bitfield, the storage (`CountingStorage`) saw no piece read before the first peer request, and the torrent finishes.
  - `touched_file_forces_a_recheck` (rewrite one byte of a file and bump its mtime by 2 s: a full check runs and the changed piece is fetched again), `missing_part_file_forces_a_recheck`, `custom_storage_is_trusted`.
  - `magnet_resume_skips_metadata_fetch` (`info` restored: `MetadataReceived` is never emitted and no `ut_metadata` request is sent).
  - `resume_keeps_priorities_renames_queue_and_web_seeds`.
  - `resume_rejects_garbage_and_mismatches`: random bytes, version 2, `info` of another torrent (`InfoHashMismatch`), a `pieces` of the wrong length (`Invalid("pieces")`), 10,000 peers (truncated to 200, not an error).
  - `need_save_resume_follows_changes`.
  - `session_state_round_trip`: three torrents and DHT on (loopback nodes); `shutdown_and_save`, `restore`: same torrents, same queue order, no recheck, DHT state applied (`dht_stats().nodes_v4 > 0` before any bootstrap).
  - `dirty_pieces_at_shutdown_are_in_the_state`: pieces still dirty when `shutdown_and_save` runs are in the saved bitfield.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test resume && cargo test -p fbt-session resume::`
- [ ] **Step 3: Implement.** Fuzz target `resume` runs `decode_resume` and `SessionState::from_bytes`; seeds are a saved file and a session state from the tests.
- [ ] **Step 4: Run.** `cargo test --workspace`; `resume` fuzz 60 s: no crash.
- [ ] **Step 5: Commit.** `feat(session): resume data and session state` No trailers or attribution of any kind.

---

### Task 13: DHT scrape (BEP 33) and `sample_infohashes` (BEP 51)

**Files:**
- Create: `crates/fbt-dht/src/bloom.rs`.
- Modify: `crates/fbt-dht/src/{krpc.rs,server.rs,store.rs,lookup.rs,node.rs,lib.rs}`, `crates/fbt-dht/tests/swarm.rs`, `crates/fbt-session/src/{session.rs,error.rs,lib.rs}`, `crates/fbt-session/src/torrent/dht.rs`, `fuzz/seeds/krpc/`.

**Interfaces:**
- Produces:
  - `krpc`: `Query::GetPeers { info_hash, scrape: bool }`, `Query::AnnouncePeer { .., seed: bool }`, `Query::SampleInfohashes { target: NodeId }`; `Response` gains `bf_seeds: Option<Box<[u8; 256]>>`, `bf_peers: Option<Box<[u8; 256]>>`, `samples: Vec<[u8; 20]>` (cap `MAX_SAMPLES` = 20 when decoding), `num: Option<u32>`, `interval: Option<u32>`. A filter of the wrong length is ignored, not an error.
  - `bloom.rs`: `pub struct BloomFilter([u8; 256])` with `insert(ip: IpAddr)`, `union(&mut self, &BloomFilter)`, `estimate(&self) -> u32` (BEP 33 formula `ln(c/m)/(k·ln(1−1/m))`, `m` = 2048, `k` = 2, saturated at 6000 when full).
  - `PeerStore` keeps a `seed` flag per entry and answers scrape with both filters; `sample(rng, max) -> Vec<[u8; 20]>` and `len()`.
  - Server: `sample_infohashes` answers `interval` 21600, `num` = stored hashes, up to 20 samples, and the closest nodes.
  - `#[non_exhaustive] pub struct DhtScrape { pub seeds: u32, pub peers: u32 }`, `Dht::scrape(&self, info_hash: [u8; 20]) -> Result<DhtScrape, DhtError>` (a `get_peers` lookup with `scrape=1`, unioning the filters of the K closest answers); `#[non_exhaustive] pub struct DhtSamples { pub interval: Duration, pub num: u32, pub samples: Vec<[u8; 20]>, pub nodes: Vec<NodeInfo> }`, `Dht::sample_infohashes(&self, node: SocketAddr, target: NodeId) -> Result<DhtSamples, DhtError>`.
  - The torrent's DHT announcer sends `seed=1` while seeding.
  - `Session::dht_scrape(&self, info_hash: [u8; 20]) -> Result<DhtScrape, SessionError>`, `Session::dht_sample_infohashes(&self, node: SocketAddr, target: [u8; 20]) -> Result<DhtSamples, SessionError>`; `SessionError::DhtDisabled`; `fbt` re-exports `DhtScrape`, `DhtSamples`.

- [ ] **Step 1: Write failing tests.**
  - `bloom.rs`: `estimate_tracks_inserts` (100, 1000 distinct IPs estimate within 10 %), `union_is_or`, `bep33_test_vector` (BEP 33's example: IPv4 192.0.2.0 to 192.0.2.255 and 2001:DB8:: to 2001:DB8::3E7 inserted gives the published estimate within 1 %).
  - `krpc.rs`: `new_fields_round_trip`, `oversized_samples_are_cut`, `short_bloom_filter_is_ignored`.
  - `tests/swarm.rs` (loopback nodes): `scrape_counts_seeds_and_peers` (10 announces, 4 with `seed`: estimate seeds 4 ± 1, peers 6 ± 1), `sample_infohashes_returns_stored_hashes`.
  - `fbt-session/tests/dht.rs`: `session_dht_scrape` and `dht_methods_without_dht_are_disabled`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-dht && cargo test -p fbt-session --test dht`
- [ ] **Step 3: Implement.** Add `krpc` seeds for `sample_infohashes` and a scrape `get_peers` reply.
- [ ] **Step 4: Run.** `cargo test --workspace`; `krpc` fuzz 60 s: no crash.
- [ ] **Step 5: Commit.** `feat(dht): BEP 33 scrape and BEP 51 sample_infohashes` No trailers or attribution of any kind.

---

### Task 14: DHT items (BEP 44)

**Files:**
- Create: `crates/fbt-dht/src/items.rs`, `crates/fbt-session/tests/dht_items.rs`.
- Modify: `Cargo.toml` (`ed25519-dalek = { version = "3", default-features = false, features = ["std", "fast"] }`), `crates/fbt-dht/{Cargo.toml,src/krpc.rs,src/server.rs,src/lookup.rs,src/node.rs,src/lib.rs}`, `crates/fbt-session/src/{session.rs,lib.rs}`, `fuzz/seeds/krpc/`.

**Interfaces:**
- Consumes: Task 13's lookup extensions.
- Produces:
  - `krpc`: `Query::Get { target: NodeId, seq: Option<i64> }`, `Query::Put { token: Vec<u8>, v: Vec<u8> /* raw bencode, ≤ 1000 */, k: Option<[u8; 32]>, sig: Option<[u8; 64]>, seq: Option<i64>, salt: Option<Vec<u8>>, cas: Option<i64> }`; `Response` gains `v: Option<Vec<u8>>`, `k`, `sig`, `seq`. Over-long `v` or salt decode as `Query::Put` with a flag the server answers 205/207.
  - `items.rs`: `pub(crate) struct ItemStore` (bounded `max_items`, 2 h lifetime, least recently announced evicted), `signable(salt, seq, v) -> Vec<u8>` (`4:salt<n>:<salt>3:seqi<seq>e1:v<v>`, salt part omitted when empty), `verify(k, sig, salt, seq, v) -> bool` (`VerifyingKey::verify_strict`), the put rules (immutable target = SHA-1(v); mutable target = SHA-1(k ‖ salt); 206 bad signature, 301 cas mismatch, 302 seq less than stored, equal seq with different v is 302).
  - `DhtConfig::max_items: usize` (700).
  - `#[non_exhaustive] pub struct MutableItem { pub public_key: [u8; 32], pub salt: Vec<u8>, pub seq: i64, pub value: Vec<u8> }`; `Dht::get_immutable(&self, target: [u8; 20]) -> Result<Option<Vec<u8>>, DhtError>`, `Dht::put_immutable(&self, value: Vec<u8>) -> Result<([u8; 20], usize), DhtError>`, `Dht::get_mutable(&self, public_key: [u8; 32], salt: &[u8]) -> Result<Option<MutableItem>, DhtError>` (highest verified `seq`), `Dht::put_mutable(&self, secret_seed: [u8; 32], salt: Vec<u8>, value: Vec<u8>, seq: i64, cas: Option<i64>) -> Result<usize, DhtError>` (the count of nodes that stored it). Values must be valid bencode (`DhtError::InvalidValue`).
  - `Session::dht_get_immutable`, `dht_put_immutable`, `dht_get_mutable`, `dht_put_mutable` with the same shapes returning `SessionError`; `fbt` re-exports `MutableItem`.

- [ ] **Step 1: Write failing tests.**
  - `items.rs`: `bep44_test_vectors` (BEP 44's mutable example with salt `foobar`: the published signature verifies and our signing of the published seed reproduces it; the immutable example's target), `seq_rules` (lower seq 302, cas mismatch 301), `store_is_bounded`, `oversized_value_is_205`.
  - `krpc.rs`: `get_put_round_trip`.
  - `tests/swarm.rs`: `immutable_put_then_get`, `mutable_put_then_get_highest_seq` (two puts, seq 1 then 2), `forged_signature_is_refused` (a raw put with a bad `sig` gets error 206 and `get_mutable` returns `None`).
  - `fbt-session/tests/dht_items.rs`: `session_put_get_round_trip` on loopback nodes.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-dht && cargo test -p fbt-session --test dht_items`
- [ ] **Step 3: Implement.** Seeds for `get`, `put` and their replies in `fuzz/seeds/krpc/`.
- [ ] **Step 4: Run.** `cargo test --workspace && bash dev/check-pure-rust.sh && cargo deny --all-features check`; `krpc` fuzz 60 s: no crash. If `ed25519-dalek` brings `cc` into the pure-rust tree, stop and report `BLOCKED` rather than vendoring around it.
- [ ] **Step 5: Commit.** `feat(dht): BEP 44 immutable and mutable items` No trailers or attribution of any kind.

---

### Task 15: `fbt-example` flags and interop scenarios

"Done" for this milestone means these pass in CI.

**Files:**
- Create: `dev/interop/socks5/{Dockerfile,danted.conf}`, `dev/interop/webseed/nginx.conf`.
- Modify: `crates/fbt-example/src/main.rs`, `crates/fbt-example/tests/cli.rs`, `dev/interop/{compose.yaml,run.sh,README.md}`, `.github/workflows/ci.yml` (timeout only).

**Interfaces:**
- Consumes: `ProxyConfig` (Task 4), `AddTorrent::from_resume`, `save_resume_data` (Task 12), `add_web_seed` (Task 11).
- Produces:
  - `NetArgs` gains `--proxy <URL>` (`socks5://[user:pass@]host:port` or `http://[user:pass@]host:port`, parsed into `ProxyConfig`); `download` gains `--resume <PATH>` (load it when present, write `save_resume_data` there on exit, atomically through a temp file and rename), `--stop-at <PERCENT>` (exit 0 once progress reaches it, after saving resume data) and `--web-seed <URL>` (repeatable). With `--resume`, the first status line prints `resume: trusted (<n> pieces)` when no check ran and `resume: checking` otherwise.
  - Interop services: `socks5` (Dante from the pinned `debian:bookworm-slim` with `dante-server`, no auth, internal port 1080, on `swarm`) and `webseed` (`nginx` pinned by multi-arch index digest, serving `${INTEROP_DIR}/payload5` read-only, access log with the `Range` header to stdout).
  - Scenarios (`known_scenarios` gains `resume`, `webseed`, `proxy`):
    1. `resume`: fixture 1 from qBittorrent only (Transmission's copy stopped as in `stream`): `download --resume /interop/fbt-resume/r.dat --stop-at 50`, then the same command without `--stop-at`; the second log contains `resume: trusted` and no `checking files`, the data matches, and qBittorrent's `uploaded` grew in both runs. Prints `interop: resume without recheck OK (qbittorrent)`.
    2. `webseed`: `transmission-create -w http://webseed/ -s 256` over `payload5` (trackerless); `download --no-dht` with no peer: finishes, data matches, the nginx log shows `206` responses. Prints `interop: web seed OK (nginx)`.
    3. `proxy`: `download /interop/fixture.torrent --proxy socks5://socks5:1080` from qBittorrent: finishes, data matches; qBittorrent's peer list for the torrent (`/api/v2/sync/torrentPeers`) shows the `socks5` container's address and never `fbt`'s; opentracker received the announce from the `socks5` address (its `stats` page lists the proxy's IP). Prints `interop: SOCKS5 proxy OK (qbittorrent)`.

- [ ] **Step 1: Write failing tests** in `tests/cli.rs` (every invocation `--port 0 --no-dht`): `proxy_url_parsing` (both schemes, credentials, a missing port is exit 2), `resume_file_is_written_and_trusted` (download from a local `seed` with `--stop-at 50 --resume`, then again: the second run's stderr says `resume: trusted`), `web_seed_flag_downloads` (the test runs `WebSeedServer` from `crates/fbt-session/tests/support/web_seed.rs`, included with `#[path]`, and downloads with no peer).
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-example`
- [ ] **Step 3: Implement the flags,** then the scenarios. Run `INTEROP_ONLY=resume,webseed,proxy bash dev/interop/run.sh`.
- [ ] **Step 4: Run the full suite.** `bash dev/interop/run.sh`: every milestone 1 to 4 line, the three new OK lines, `interop: all scenarios passed ...`, exit 0; `docker network inspect fbt-interop_swarm --format '{{.Internal}}'` prints `true`. Raise the CI `interop` timeout by 10 minutes if needed.
- [ ] **Step 5: Commit.** `ci: interop resume without recheck, web seed and SOCKS5 proxy` No trailers or attribution of any kind.

---

### Task 16: docs, changelog and full gate

**Files:**
- Modify: `README.md`, `CHANGELOG.md`, `CLAUDE.md`, `crates/fbt/src/lib.rs` (crate docs), `deny.toml` (only if a new license or advisory appears, with its one-line reason).

- [ ] **Step 1: Update the docs.**
  - `crates/fbt/src/lib.rs`: sections "Resume data and session state" (with a `no_run` doctest: `save_resume_data`, `AddTorrent::from_resume`, `shutdown_and_save`, `restore`), "Queueing", "Proxies" (what is and is not proxied, per Decisions), "IP filter and bans", "Web seeds", "DHT items"; the `serde` feature in the feature list.
  - `README.md`: feature list (resume, queueing, seed limits, move and rename, part file, proxies, IP filter, smart ban, web seeds, BEP 44/33/51, serde); a resume example matching the doctest; the proxy caveats; `fbt-example` flags; the interop paragraph names the three scenarios.
  - `CHANGELOG.md` `## [Unreleased]`, every entry ending in `([#N](https://github.com/featherbitplatform/fbt-lib/pull/N))`, `N` the milestone PR's number:
    - `### Added`: everything produced in Tasks 1 to 15, by public name.
    - `### Changed`: `TrackerClient::new_with` replaces `with_udp_sockets`; new `SessionError`, `Event`, `TorrentState::Queued`, `DisconnectReason::Blocked` variants; `TorrentStatus` fields; `stopped` goes to every tracker that answered; one connection per IP per torrent by default (loopback exempt); skipped files are no longer created; colliding paths are renamed; torrents are auto-managed and queued by default (3 downloads, 5 seeds).
    - `### Fixed`: `TorrentFinished` emitted twice after a skipped-only piece loss.
  - `CLAUDE.md` (the only task that edits it): the fuzz target list gains `resume socks5 http_connect scrape web_seed_head`; the interop line names resume, web seed and SOCKS5; the test rule names the proxy and web seed fakes.
- [ ] **Step 2: Run the full gate.** `cargo fmt --all --check && cargo clippy --workspace --all-targets --all-features -- -D warnings && cargo test --workspace && cargo test --workspace --all-features && cargo deny --all-features check && bash dev/check-pure-rust.sh && RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features && bash dev/sast.sh`: all pass.
- [ ] **Step 3: Commit.** `docs: milestone 5 changelog, README and CLAUDE.md` No trailers or attribution of any kind.

---

## After the plan: shipping v0.5.0

PR `feature/m5-persistence-ops` → `develop`. Every CI job must be green, including `interop` and the fuzz runs the PR triggers (new targets `resume`, `socks5`, `http_connect`, `scrape`, `web_seed_head`; `krpc`, `metainfo`, `magnet` and `path` reached by changed code). The PR body lists the decisions made on the owner's behalf (this plan's Decisions section, plus any taken during execution) and a "Deferred to later milestones" section, and the CHANGELOG PR links carry its number. Then the Gitflow release:

1. `release/0.5.0` off `develop`: bump the workspace version to `0.5.0` (the peer-id prefix becomes `-FB0500-` and the KRPC `v` becomes `FB\x00\x05` automatically). Move `Unreleased` to `## [0.5.0] - <date>`, check `bash dev/test-release-version.sh`, run `bash dev/bench.sh` on the release commit and keep its row.
2. PR into `main`, then the annotated tag `v0.5.0`, then merge `main` back into `develop`.

No attribution lines anywhere.

## Deferred to later milestones (recorded so nobody builds them now)

- **Milestone 6:** v2 and hybrid torrents, Merkle verification, torrent creation; resume data gains v2 piece layers then (`fbt-resume-version` 2, version 1 still read).
- **Milestone 7 (`fbt-api`):** HTTP `Range` serving on `FileReader`; HTTP/JSON control of everything this milestone adds (resume, queue, move, filter, DHT items).
- **Backlog:**
  - IP filter file parsers (eMule `.dat`, PeerGuardian `.p2p`): featherbittorrent can parse them into `IpFilter` until a second client needs them here;
  - serde for `fbt-bencode` values (no consumer);
  - UDP tracker hostnames resolved through the SOCKS5 proxy (resolved locally today), IPv6 through a SOCKS5 UDP association, and proxying the DHT through an HTTP proxy (impossible: no UDP);
  - SOCKS4/4a and I2P (spec non-goal);
  - from milestone 4: batched UDP syscalls and multi-task uTP processing, an adaptive status tick for idle torrents, the qBittorrent Docker-bridge ceiling, a cached missing-piece count for readers, listener tests that retry a port in a Windows excluded range, and the review polish recorded in the per-task reviews.
