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
- Every new handle or session method that round-trips to an actor, the DHT or the disk pool is `async` (as `pause` and `set_file_priorities` are); only pure reads of shared state stay sync (`Session::ip_filter`, `banned_peers`, `set_queue_settings`).

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
7. **Part file under priority flips.** A boundary piece of a skipped file is downloaded, the file becomes wanted, then skipped again; a recheck in between; `DeleteData`; a reader streaming a skipped file; a skipped file already on disk at add.
   - Expected: the skipped file is never created while skipped; making it wanted moves its bytes out of the part file into the file; a recheck finds pieces in both; `DeleteData` removes the part file; a reader of a skipped file reads from the part file; a skipped file that exists on disk is used in place.
   - Tests: Task 8 `skipped_neighbour_is_never_created`, `unskipping_exports_from_the_part_file`, `recheck_reads_the_part_file`, `delete_removes_the_part_file`, `reader_on_a_skipped_file_reads_the_part_file`, `existing_skipped_file_is_used_in_place`.
8. **Colliding paths.** `A.txt` and `a.txt`; `é` in NFC and NFD; file `a` and files `a/b`, `a/c`; a rename that collides again (`a.1.txt` already exists); `rename_file` onto a directory prefix.
   - Expected: torrent order wins, later files become `stem.N.ext` with the smallest free `N`; no two files map to the same path on any filesystem.
   - Tests: Task 2 `case_collision_is_renamed`, `normalisation_collision_is_renamed`, `file_and_directory_collide`, `later_files_follow_a_renamed_directory`, `renamed_name_avoids_existing`; Task 9 `rename_file_then_serve`.

---

## Starting point

`develop` holds milestone 4 (`v0.4.0`, merged back as PR #13, `04961ed`). The branch `feature/m5-persistence-ops` exists off `develop` and holds only this plan. Check first that the workspace version is `0.4.0` and `cargo test --workspace` is green.

## Decisions recorded in this plan

The owner authorised milestones 1 to 7 to run unattended; the spec's section 8 item 5 sets the scope, plus the milestone 1 to 4 deferrals. The builder made these calls (revisable by whoever plans a later milestone):

- **Serde behind a `serde` feature** on `fbt-net`, `fbt-tracker`, `fbt-session` and `fbt` (off by default, so the default tree gains no proc-macro). Every config struct and enum `fbt` exports derives `Serialize`/`Deserialize` under it; structs use `#[serde(default)]` so a saved config missing new fields still loads; `Duration` uses serde's `{secs, nanos}`. `IpFilter` serialises as a rule list, never its internal map. The internal crates' configs (`UtpConfig`, `PortMapConfig`, `LsdConfig`, `ChokerConfig`, `DhtConfig`) are built by the session from `Settings` and are not in `fbt`'s API: no serde (backlog). serde for `fbt-bencode` values (milestone 1 deferral) has no consumer: backlog.
- **Collision renames at parse time** in `fbt-metainfo`, replacing today's `check_duplicates` (which rejects such torrents with `DuplicatePath`, now never produced): the key of a path is each component NFC-normalised and lower-cased, joined; a file whose key, or one of whose directory prefixes, matches an earlier file's becomes `stem.N.ext` (libtorrent's `resolve_duplicate_filenames`), `N` the smallest free from 1. A renamed directory carries every later file under it (`a`, `a/b`, `a/c` → `a`, `a.1/b`, `a.1/c`). Pad files are exempt; the info hash is unaffected; deterministic, so resume data stays valid.
- **Part file, as libtorrent's `use_partfile`.** `<save_path>/.<v1 hex>.parts`: header `b"FBTPART1"`, `u32` piece count, `u32` piece length, one `u32` slot per piece (`u32::MAX` = none), then piece-sized slots. It holds the bytes of skipped non-pad files *that do not exist on disk*; a skipped file already on disk (a seed adding with files skipped, or a file skipped after download) keeps being read and written in place. Zero-length skipped files are not created. Created on the first such write, removed once empty. Unskipping moves the bytes out. A reader streaming a skipped file fills the part file (documented on `FileReader`).
- **Move storage** (`TorrentHandle::move_storage(path, MoveMode)`): flush every dirty piece (`FlushReason::All` and wait, as milestone 3 required), hold new disk jobs and wait for running reads to return, then one disk-pool job moves each file (`fs::rename`, else copy, sync and delete). `MoveMode::FailIfExists` (default) checks every target first; `ReplaceExisting` overwrites; `KeepExisting` keeps the destination's files and rechecks. A failure moves back what moved, best effort, with `StorageMoveFailed`. `rename_file(index, path)` takes a path relative to the save path (as libtorrent), sanitised by `fbt_metainfo::sanitize_path`, refused with `SessionError::InvalidPath` when `fbt_metainfo::collides` says so.
- **Resume data is ours** (spec 10): a bencoded dict, `fbt-resume-version` = 1, written by `save_resume_data()` after a flush, so its bitfield holds only pieces on disk. It carries the raw `info` dict once known (a magnet restarts without fetching metadata), save path (UTF-8 only, else `InvalidPath`), priorities, renamed paths, pieces, flags, limits, totals, times, trackers, web seeds, up to 200 peers per family, queue position, and a stamp (length, mtime in seconds) per file and for the part file. **Trust rule:** at add, a disk-pool job compares the stamps of every file holding a saved piece (and the part file when one holds data); all equal means no check runs, anything else runs the normal check; a custom `Storage` without stamps is trusted. **Stamps hold only if nothing is written after the save:** a resume file is trusted on restart when saved after `pause` or through `shutdown_and_save` (documented on `save_resume_data`). Lengths prove nothing (files are preallocated); same-second edits go undetected (seconds granularity, as libtorrent). `TorrentStatus::resume_trusted` says which way an add went.
- **Session state** (`SessionState { dht, torrents }`, bencoded `fbt-session-version` = 1): `Session::save_state()` and `Session::shutdown_and_save()` (spec 5.3's "saves resume data" on shutdown, as bytes, since the library writes no files); `Session::restore(settings, state)` sets `dht_state` and re-adds every torrent in saved order, returning per-torrent errors. Settings persist through serde.
- **Queueing, as libtorrent:** torrents are `auto_managed` by default; `active_downloads` 3, `active_seeds` 5, `active_limit` 500, `dont_count_slow_torrents` with `inactive_down_rate`/`inactive_up_rate` 2 KiB/s after a 60 s `startup_grace`; a slow torrent is exempt from the download and seed limits only, never from `active_limit`. Rescheduled every 30 s and on every add, remove, pause, resume, finish and limit change; an auto-managed torrent is added queued and the first pass runs before its actor starts, so no `started`/`stopped` pair per add. Queued uses the pause path (close peers, flush, `stopped`), shown as `TorrentState::Queued`. Checking is not queued here (`max_active_checks` does that). **User pause beats the queue:** `resume` returns a torrent to the queue, which may queue it at once.
- **Seed limits off by default** (`seed_ratio_limit`, `seed_time_limit`: `None`): libtorrent's ratio default surprises users and clients override it. Reaching one pauses the torrent, clears `auto_managed` and emits `SeedLimitReached`.
- **`stopped` semantics, per tracker as libtorrent's `start_sent`:** a tracker that has not taken a `started` in the current run gets `started` whatever the run's next event; every tracker that answered one gets `stopped` (in parallel, under one `STOPPED_TIMEOUT` = 5 s); trackers that never answered get nothing; queueing and seed limits stop the run like pause.
- **Per-IP connection limit:** `max_peers_per_ip` = 1 per torrent (libtorrent's `allow_multiple_connections_per_ip` = false), IPv4-mapped canonicalised; loopback exempt so same-host clients and the test suites work.
- **IP filter and bans:** `fbt_net::ipfilter::IpFilter` (block/allow ranges, last rule wins). Applied to dials, TCP and uTP accepts, LSD, PEX, tracker, DHT and holepunch peers, and to live connections when set. Not to trackers or DHT nodes. Smart-ban bans live in a separate session `BanList` (`MAX_BANNED_IPS` = 4096), so replacing the filter never unbans. The milestone 2 metadata ban (`BanTarget`, per torrent) stays separate. Both new kinds report `Event::PeerBlocked`.
- **Smart ban, as libtorrent's `smart_ban` plugin plus trust points:** accepted blocks remember their sender (end-game duplicates are not accepted, so never blamed); a failed piece keeps per-block SHA-1s (at most `MAX_SMART_BAN_PIECES` = 64 per torrent); when it later passes, every sender whose block differs is banned. Trust per ban key per torrent: −2 per failed piece contributed to, +1 per passed piece (capped at 8), banned at −7.
- **Proxies:** SOCKS5 (RFC 1928, user/pass RFC 1929) and HTTP CONNECT (Basic). `ProxyConfig { proxy_peers, proxy_trackers, proxy_hostnames }` default true. With SOCKS5 and `proxy_peers || proxy_trackers`, *all* UDP (outgoing uTP, DHT, UDP trackers) goes through one UDP ASSOCIATE: one socket cannot be half proxied. Then the session binds TCP listeners only, no fallback UDP socket, and one relayed socket on an ephemeral port of the first IPv4 listen IP; the DHT runs IPv4-only; incoming uTP arrives only as the relay forwards it. With an HTTP proxy, UDP cannot be proxied: outgoing uTP is off with `proxy_peers`, UDP trackers fail with `ProxyUnsupported` with `proxy_trackers`, and the DHT runs direct unless `dht_enabled` is false. Many HTTP proxies refuse CONNECT to port 80, so plain-HTTP trackers may fail through them. All three caveats are documented on `ProxyConfig`. Web seeds follow `proxy_peers` (their own hyper client). UDP tracker hostnames are resolved locally (backlog). Listening is unchanged.
- **Web seeds:** a pseudo-peer per URL (`PeerKey` from the actor's counter, `HaveAll` in the picker), one request in flight, covering whole unstarted pieces in index order up to `WEB_SEED_REQUEST_MAX` = 4 MiB, split only at file boundaries and fetched sequentially through `TrackerClient::fetch_range` (keep-alive, TLS, proxy, at most 3 redirects, at most `len + 1` bytes read, a per-frame timeout so a slow but live seed is not cut). Errors back off 30 s doubling to 30 min; 4xx other than 408 and 429 retires the seed. Bytes count toward the download meter, and a seed waits while the download bucket has no tokens. At most `fbt_metainfo::MAX_WEB_SEEDS` = 16 per torrent.
- **Scrape (BEP 48, milestone 1 deferral):** `TrackerClient::scrape(url, hashes)`, HTTP by the `announce` → `scrape` path rule and UDP action 2, at most `MAX_SCRAPE_HASHES` = 74; `TorrentHandle::scrape_trackers()` reports through `Event::ScrapeReply`/`ScrapeFailed`.
- **DHT:** BEP 33 bloom filters (2048 bits, two SHA-1-derived indices) on `get_peers` with `scrape=1` and a `seed` flag on `announce_peer`; BEP 51 `sample_infohashes` from the peer store (`interval` 6 h, at most 20 samples) and one client-side query (crawling is the client's job); BEP 44 items in a bounded store (`max_items` = 700 as libtorrent, 2 h lifetime), `v` ≤ 1000 bytes, salt ≤ 64, write tokens as `announce_peer`, error codes 205/206/207/301/302. Session wrappers return `SessionError::DhtDisabled` with the DHT off and `SessionError::Dht(String)` for a DHT failure.
- **Async methods:** every new `TorrentHandle` method and `Session::{save_state, shutdown_and_save, restore, set_ip_filter}` and the DHT wrappers are `async`.
- **Size cut:** IP filter file parsers (eMule `.dat`, PeerGuardian `.p2p`), bencode serde, serde for internal crates' configs and absolute-form GET through HTTP proxies go to the backlog; every scope item of section 8 item 5 is in this plan.
- **New fuzz targets:** `resume`, `socks5`, `http_connect`, `scrape`, `web_seed_head`; each task that creates one adds it to the `.github/workflows/fuzz.yml` matrix (and its PR `paths`). `krpc` gets seeds for the new methods.
- Version `0.5.0`; changelog entries go under `## [Unreleased]`.

### Pre-flight rulings

From the pre-flight scan (`.superpowers/sdd/2026-11-12-milestone-5-persistence-operations/preflight.md`, findings F1 to F33); the controller adopted every recommended fix. Each is folded into its task's text below; this list is the index.

- **Task 1.** `serde_json` in `fbt-session`'s dev-dependencies; the configs left without serde recorded (F33).
- **Task 2.** `resolve_collisions` replaces `check_duplicates`; the two old rejection tests become rename tests; `DuplicatePath` kept, documented as unused; the constant is `MAX_COMPONENT_BYTES` (F1). Renamed directories carry later files; `pub fn collides` shared with Task 9 (F2). `MAX_WEB_SEEDS` is `pub` (F20).
- **Task 3.** `ProxyTarget` `#[non_exhaustive]` with `Debug`; `Socks5Udp` `Debug`; `ProxyConfig`'s `Debug` hides the password; `parse_socks5_reply` returns `Option<SocketAddr>`; `kind: None` dials direct (F3). Fuzz matrix (F31).
- **Task 4.** TCP-only listeners, no fallback UDP, one relayed socket on an ephemeral port (F4). Association on `proxy_peers || proxy_trackers`; two hyper clients (F5). Tests dial over TCP or turn TCP off as needed; the existing `FakeTracker` (F6). `udp.rs` and `support/mod.rs` in Files; `ProxyConnector` wraps `HttpConnector`; ports by scheme; CONNECT-to-80 caveat (F7).
- **Task 5.** Per-tracker `started_urls`; the test reaches tier 2 through a switchable fake reply (F8). `TooManyHashes`, `ScrapeUnsupported`; `FakeTracker` scrapes (F9). Fuzz matrix (F31).
- **Task 6.** `Shared::blocked(addr)`; `IpFilter` serialises as rules (F10). `set_ip_filter` async (F32).
- **Task 7.** `Actor::smart_ban` (the name `ban` is taken) (F11). Scripted peers are dialled; `serve_corrupt` corrupts once and logs; no real seed (F12).
- **Task 8.** Existing skipped files used in place; no empty skipped files; new test (F13). `mem.rs` dropped, `magnet.rs` added; the reader test reads all of C (F15).
- **Task 9.** Wait for running reads; real Windows lock (`share_mode(0)`); root-safe Unix test (F14). `collides` and save-path-relative renames (F2). Async (F32).
- **Task 10.** One `stopped()` predicate; queue uses the pause path; added queued (F16). Slow torrents count against `active_limit` (F17). `bench_settings` unlimited; `support/mod.rs` in Files (F18). `Cmd::SetQueuePosition` (F19). Async (F32).
- **Task 11.** `accept_block`; `PiecePicker::pick_run`; `BlockSource::WebSeed(PeerKey)`; one `MAX_WEB_SEEDS` (F20). `fetch_range` checks status itself, reads `len + 1` at most, per-frame timeout; the ignored-range test starts past 0 (F21). Fuzz matrix, `fuzz/Cargo.toml` (F31). Async (F32).
- **Task 12.** `ResumeState` gains `renamed`, `web_seeds`, `queue_position`; `FsStorage::with_paths` (F22). Save after pause; `stamps()` drops handles; the round trip uses `FsStorage` (F23). `MAX_RESUME_BYTES`, `MAX_RESUME_ITEMS`, `MAX_SESSION_TORRENTS` (F24). `TorrentStatus::resume_trusted` (F25). Fuzz matrix and `crates/fbt-session/**` in fuzz `paths` (F31). Async (F32).
- **Task 13.** Bloom estimates tested in `store.rs` with synthetic IPs; the swarm test checks one seed and one leecher (F28). `tests/dht.rs` modified; `Dht::announce(.., seed)`; `SessionError::Dht` (F29).
- **Task 14.** `ed25519-dalek` features `fast`, `zeroize` (F26). The BEP 44 vector is verified, signing round-tripped with our own seed (F27). `tests/swarm.rs`; `Query::Put { reject }`; write tokens (F29).
- **Task 15.** `--stop-at` pauses, saves, then exits (F23); the CLI reads `resume_trusted` (F25). The `webseed` scenario makes its own payload; the `proxy` scenario checks qBittorrent's peers and fbt's `TrackerReply` log; `fbt-example/Cargo.toml` dev-deps (F30).
- **Task 16.** `### Changed` names `DuplicatePath` no longer produced (F1) and the async methods (F32).

---

## File Structure

```
Cargo.toml                                          serde, unicode-normalization, ed25519-dalek, tower-service, serde_json
.github/workflows/fuzz.yml                          five new targets, crates/fbt-session/** in paths
crates/fbt-metainfo/src/{metainfo.rs,magnet.rs,collide.rs,path.rs}   web_seeds, ws=, collision renames
crates/fbt-net/src/proxy/{mod.rs,socks5.rs,http.rs}         ProxyConfig, connect_tcp, udp_associate, framing
crates/fbt-net/src/ipfilter.rs                     IpFilter
crates/fbt-net/src/demux.rs                        SharedUdpSocket::bind_relayed
crates/fbt-net/tests/{proxy.rs,support/fake_socks5.rs,support/fake_http_proxy.rs}
crates/fbt-tracker/src/{connector.rs,scrape.rs,http.rs,udp.rs,tls.rs,lib.rs,types.rs}
crates/fbt-dht/src/{krpc.rs,server.rs,node.rs,lookup.rs,store.rs,items.rs,bloom.rs}
crates/fbt-session/src/
  settings.rs, error.rs, event.rs, status.rs, lib.rs, session.rs, net.rs     new fields, variants, wrappers
  queue.rs                                         QueueSettings, plan(), scheduler
  bans.rs                                          BanList
  resume.rs                                        resume encode/decode, SessionState
  picker.rs                                        pick_run
  storage/{mod.rs,fs.rs,part.rs,stamp.rs}          trait additions, part file, moves, renames, stamps
  peer/conn/mod.rs, listener.rs                    proxied dials, filter at accept
  torrent/{smartban.rs,webseed.rs,storage_ops.rs}  smart ban, web seeds, move/rename commands
  torrent/{actor.rs,commands.rs,data.rs,check.rs,swarm.rs,peers.rs,announcer.rs,magnet.rs,mod.rs}   wiring
crates/fbt-session/tests/{serde.rs,proxy.rs,ipfilter.rs,smartban.rs,partfile.rs,move.rs,queue.rs,webseed.rs,resume.rs,dht_items.rs}
crates/fbt-session/tests/support/{mod.rs,web_seed.rs}
crates/fbt/src/lib.rs, crates/fbt/tests/lib_surface.rs
crates/fbt-example/{Cargo.toml,src/main.rs,tests/cli.rs}
fuzz/{Cargo.toml,fuzz_targets/{resume,socks5,http_connect,scrape,web_seed_head}.rs,seeds/}
dev/interop/{run.sh,README.md,compose.yaml,socks5/Dockerfile,socks5/danted.conf,webseed/nginx.conf}
README.md, CHANGELOG.md, CLAUDE.md
```

---

### Task 1: serde for public config structs

Spec 5.3; deferred since milestone 1.

**Files:**
- Modify: `Cargo.toml` (`serde = { version = "1", default-features = false, features = ["std", "derive"] }`, `serde_json = "1"`), `crates/{fbt-tracker,fbt-session,fbt}/Cargo.toml` (feature `serde = ["dep:serde", ...]`, chained `fbt/serde → fbt-session/serde → fbt-tracker/serde`; `serde_json` in `fbt-session`'s dev-dependencies), `crates/fbt-tracker/src/types.rs`, `crates/fbt-session/src/{settings.rs,bandwidth.rs,choker.rs,session.rs,stream/mod.rs}`, `crates/fbt/src/lib.rs` (feature doc).
- Test: `crates/fbt-session/tests/serde.rs` (`#![cfg(feature = "serde")]`).

**Interfaces:**
- Produces: under `cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))` and `#[serde(default)]` on structs: `Settings`, `TrackerConfig`, `ReadAhead`, `RateLimit`, `PeerClassLimits`, `MixedMode`, `ChokingAlgorithm`, `SeedChokingAlgorithm`, `EncryptionPolicy`, `Priority`, `RemoveMode`. Hidden fields serialise too. Later tasks follow the pattern for `ProxyConfig`, `IpFilter` (by hand), `MoveMode`, `QueueSettings`; `fbt-net` gets its `serde` feature in Task 3.

- [ ] **Step 1: Write failing tests** in `tests/serde.rs`: `settings_round_trip_through_json` (every non-default field changed once → `serde_json` → back; compare `Debug` strings, `Settings` has no `PartialEq`); `missing_fields_take_defaults` (`"{}"` gives `Settings::default()`'s `Debug`); `read_ahead_and_priority_round_trip`.
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
  - `Metainfo::web_seeds: Vec<String>` from `url-list` (a string or a list; `http`/`https` only, de-duplicated, at most `pub const MAX_WEB_SEEDS: usize = 16`, each at most `MAX_URL_LEN` = 2048 bytes; others dropped). `Magnet::web_seeds` from `ws=`, same rules; `to_uri` writes them.
  - `pub(crate) fn resolve_collisions(files: &mut [FileEntry])` replaces the `check_duplicates` call in `parse_info` (`metainfo.rs:271`), with the keys of Decisions: one `O(files)` pass over a `HashSet` of file keys, a `HashSet` of directory-prefix keys and a `HashMap` from an original directory-prefix key to its renamed prefix, through which later paths are rewritten before their own check; renamed keys enter both sets. A renamed component is `format!("{stem}.{n}{ext}")` (`ext` from the last `.`, none for dotfiles), cut on a char boundary to `MAX_COMPONENT_BYTES` (`path.rs`, made `pub(crate)`) by shortening the stem; `n` in `1..=files.len() + 1`. `MetainfoError::DuplicatePath` stays, documented as no longer produced.
  - `pub fn collides(files: &[FileEntry], index: usize, candidate: &RelPath) -> bool` (the candidate's key against every other file's key and directory prefixes, both directions) and `RelPath::key(&self) -> String`, for Task 9.

- [ ] **Step 1: Write failing tests.**
  - `tests/metainfo.rs`: `url_list_as_string_and_list`, `url_list_drops_non_http_and_caps` (20 URLs → 16; an `ftp://` and a 3000-byte URL dropped), `case_collision_is_renamed` (`A.txt`, `a.txt` → `A.txt`, `a.1.txt`), `normalisation_collision_is_renamed` (`caf\u{e9}`, `cafe\u{301}` → second `cafe\u{301}.1`), `file_and_directory_collide` (`a`, `a/b` → `a`, `a.1/b`), `later_files_follow_a_renamed_directory` (`a`, `a/b`, `a/c` → `a`, `a.1/b`, `a.1/c`), `renamed_name_avoids_existing` (`x.txt`, `x.1.txt`, `X.txt` → `X.2.txt`), `pad_files_never_renamed`, `long_component_stays_within_the_cap`, `info_hash_unchanged_by_renames`, `collides_sees_prefixes` (`collides` of `x` while another file is `x/y`, and of `x/y/z` while one is `x/y`). The old `rejects_duplicate_and_prefix_paths` and `rejects_case_insensitive_collisions` become rename assertions: `a/b` twice → second `a/b.1`; `A.bin`, `a.bin` → `a.1.bin`; `Dir`, `dir/x` → `dir.1/x`; two names that sanitise alike → the second renamed.
  - `tests/magnet.rs`: `ws_round_trips`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-metainfo`
- [ ] **Step 3: Implement.** Seeds with `url-list` and colliding paths in `fuzz/seeds/metainfo/`, one with `ws=` in `fuzz/seeds/magnet/`.
- [ ] **Step 4: Run.** `cargo test -p fbt-metainfo && cargo clippy -p fbt-metainfo --all-targets -- -D warnings && bash dev/check-pure-rust.sh`; then `metainfo`, `magnet` and `path` fuzz 60 s each: no crash.
- [ ] **Step 5: Commit.** `feat(metainfo): web seed lists and renames for colliding paths` No trailers or attribution of any kind.

---

### Task 3: proxy clients in `fbt-net`

**Files:**
- Create: `crates/fbt-net/src/proxy/{mod.rs,socks5.rs,http.rs}`, `crates/fbt-net/tests/proxy.rs`, `crates/fbt-net/tests/support/{fake_socks5.rs,fake_http_proxy.rs}`, `fuzz/fuzz_targets/{socks5.rs,http_connect.rs}`, `fuzz/seeds/{socks5,http_connect}/`.
- Modify: `crates/fbt-net/{Cargo.toml,src/lib.rs}` (`serde` feature, `pub mod proxy`), `fuzz/Cargo.toml`, `.github/workflows/fuzz.yml` (matrix: `socks5`, `http_connect`).

**Interfaces:**
- Produces (public, `fbt_net::proxy`):
  ```rust
  #[non_exhaustive] #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
  pub enum ProxyKind { #[default] None, Socks5, Http }
  #[non_exhaustive] #[derive(Clone, PartialEq, Eq)]   // Default: kind None, port 1080, flags true; Debug by hand: password: Some("***")
  pub struct ProxyConfig {
      pub kind: ProxyKind, pub host: String, pub port: u16,
      pub username: Option<String>, pub password: Option<String>,
      pub proxy_peers: bool, pub proxy_trackers: bool, pub proxy_hostnames: bool,
  }
  #[non_exhaustive] #[derive(Debug, Clone, PartialEq, Eq)]
  pub enum ProxyTarget { Addr(SocketAddr), Host(String, u16) }
  #[derive(Debug, thiserror::Error)] #[non_exhaustive]
  pub enum ProxyError { Io(io::Error), Timeout, AuthFailed, Refused(u8), HttpStatus(u16), Protocol(&'static str), Unsupported }
  pub async fn connect_tcp(cfg: &ProxyConfig, target: ProxyTarget, timeout: Duration) -> Result<TcpStream, ProxyError>;
  #[derive(Debug)] pub struct Socks5Udp { /* control: TcpStream, relay: SocketAddr */ }
  pub async fn udp_associate(cfg: &ProxyConfig, timeout: Duration) -> Result<Socks5Udp, ProxyError>;
  impl Socks5Udp { pub fn relay(&self) -> SocketAddr; pub async fn closed(&mut self); }
  pub fn encode_udp(dst: SocketAddr, payload: &[u8], out: &mut Vec<u8>);
  pub fn decode_udp(datagram: &[u8]) -> Option<(SocketAddr, &[u8])>;
  #[doc(hidden)] pub fn parse_socks5_reply(buf: &[u8]) -> Result<Option<(Option<SocketAddr>, usize)>, ProxyError>; // None addr = domain
  #[doc(hidden)] pub fn parse_connect_head(buf: &[u8]) -> Result<Option<(u16, usize)>, ProxyError>;
  pub const MAX_CONNECT_HEAD: usize = 8192;
  ```
  - `kind: None` makes `connect_tcp` a direct `TcpStream::connect` under the timeout. SOCKS5 offers no-auth, plus user/pass when a username is set (over 255 bytes is `Protocol`). A domain `BND.ADDR` is read and ignored for CONNECT and is `Unsupported` for UDP ASSOCIATE; a relay of `0.0.0.0` means the proxy's own address. `decode_udp` is `None` for `FRAG != 0`, a domain `ATYP` or a short datagram. The HTTP CONNECT head is read byte-wise to `\r\n\r\n`, so no tunnel byte is consumed; 2xx is success. `ProxyConfig::validate(&self) -> Result<(), String>` (host non-empty and port non-zero unless `None`). The `ProxyConfig` doc carries the caveats of Decisions.
- Fakes (`#[path]`-included by session tests, as the NAT fakes are): `FakeSocks5::start(Script) -> FakeSocks5` with `addr()`, `connects() -> Vec<String>` (CONNECT targets as text), `udp_targets()`, scripts `Ok`, `RequireAuth(user, pass)`, `Refuse(u8)`, `Garbage`, `CloseAfterGreeting`, `RelayFromStranger`, `Fragmented`; it relays CONNECT and UDP for real on loopback. `FakeHttpProxy::start(Script)` with `Ok`, `Status(407)`, `EndlessHead`, `RequireBasic(user, pass)`.

- [ ] **Step 1: Write failing tests** in `tests/proxy.rs`: `socks5_connect_by_addr_and_host` (an echo server through the fake; `connects()` names `127.0.0.1:<p>` and `localhost:<p>`), `socks5_user_pass`, `socks5_wrong_password_is_auth_failed`, `socks5_refusal_carries_the_code` (`Refused(5)`), `garbage_reply_is_a_protocol_error`, `closed_mid_handshake` (`Io` or `Protocol` within the timeout), `http_connect_tunnels_and_keeps_bytes`, `http_407_is_http_status`, `endless_connect_head_is_capped` (fails after at most `MAX_CONNECT_HEAD` bytes), `udp_associate_round_trip`, `fragmented_udp_is_dropped` (`decode_udp` of `FRAG = 1`, of a domain `ATYP` and of 5 bytes is `None`), `encode_decode_round_trip` (proptest), `none_kind_dials_direct`, `debug_hides_the_password`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-net --test proxy`
- [ ] **Step 3: Implement.** Fuzz `socks5` drives `parse_socks5_reply` and `decode_udp`; `http_connect` drives `parse_connect_head`. Seeds: a success reply per address type, a refusal, a 200 and a 407 head.
- [ ] **Step 4: Run.** `cargo test -p fbt-net && cargo clippy -p fbt-net --all-targets --all-features -- -D warnings`; both targets 60 s: no crash.
- [ ] **Step 5: Commit.** `feat(net): SOCKS5 and HTTP CONNECT proxy clients` No trailers or attribution of any kind.

---

### Task 4: proxy wiring: peers, UDP and HTTP trackers

**Files:**
- Create: `crates/fbt-tracker/src/connector.rs`, `crates/fbt-session/tests/proxy.rs`.
- Modify: `Cargo.toml` (`tower-service = "0.3"`), `crates/fbt-tracker/{Cargo.toml,src/lib.rs,src/http.rs,src/udp.rs,src/tls.rs,src/types.rs}` (`fbt-net` dependency; `udp.rs` tests use `new_with`), `crates/fbt-net/src/demux.rs`, `crates/fbt-session/Cargo.toml` (`serde` also enables `fbt-net/serde`), `crates/fbt-session/src/{settings.rs,session.rs,net.rs,error.rs,peer/conn/mod.rs}`, `crates/fbt-session/tests/support/mod.rs`.

**Interfaces:**
- Consumes: Task 3's `ProxyConfig`, `connect_tcp`, `udp_associate`, `encode_udp`, `decode_udp`.
- Produces:
  - `connector.rs`: `pub(crate) enum ProxyConnector { Direct(HttpConnector), Proxied { proxy: ProxyConfig, timeout: Duration } }` implementing `tower_service::Service<Uri>` with `Response = TokioIo<TcpStream>`: `Direct` delegates to `HttpConnector` (`enforce_http(false)`, so its connect timeout and address fallback stay); `Proxied` defaults the port by scheme (80/443) and calls `connect_tcp` with `ProxyTarget::Host` when `proxy_hostnames`, else resolves locally and uses `ProxyTarget::Addr`. `tls::build_connector(c: ProxyConnector)` wraps it with `HttpsConnectorBuilder::...enable_http1().wrap_connector(c)`.
  - `HttpClient` holds two hyper clients from one TLS config: `trackers` (proxied when `proxy_trackers`) and `web_seeds` (when `proxy_peers`; Task 11's `fetch_range` uses it).
  - `TrackerClient::new_with(cfg: TrackerConfig, udp: Vec<Arc<dyn DatagramSender>>, proxy: Option<ProxyConfig>) -> Result<TrackerClient, TrackerError>` replaces `with_udp_sockets` (`new` unchanged). `TrackerError::Proxy(String)` and `TrackerError::ProxyUnsupported` (a UDP tracker with an HTTP proxy and `proxy_trackers`).
  - `SharedUdpSocket::bind_relayed(local: SocketAddr, assoc: Socks5Udp) -> io::Result<SharedUdpSocket>`: the sender frames with `encode_udp` to the relay; the receive loop keeps only datagrams from the relay that decode, dispatching by `classify` with the decoded source; others count in `DemuxStats::dropped`. When the control connection closes, the socket stops with a warning and `DemuxStats::relay_lost` turns true.
  - `Settings::proxy: ProxyConfig` (through `ProxyConfig::validate`); `SessionError::Proxy(String)`.
  - `peer/conn/mod.rs` `connect()`: TCP dials through `connect_tcp(ProxyTarget::Addr)` under `connect_timeout` when `kind != None && proxy_peers`.
  - `Session::new` with SOCKS5 and `proxy_peers || proxy_trackers`: `bind_all` binds TCP only, `fallback_udp` is skipped, one `udp_associate` and `bind_relayed` on `SocketAddr::new(first IPv4 listen IP, 0)`; uTP, the DHT (IPv4 only) and UDP trackers use it and `demux_stats()` lists it. A failed association fails `Session::new` with `SessionError::Proxy`. With HTTP and `proxy_peers`, `enable_outgoing_utp` is treated as off.
  - `fbt` re-exports `ProxyConfig` and `ProxyKind`.

- [ ] **Step 1: Write failing tests** in `crates/fbt-session/tests/proxy.rs` (fakes via `#[path]`):
  - `peer_dial_goes_through_socks5`: the leech on `dial_tcp(test_settings())` with `proxy` aimed at the fake downloads from a loopback seed; `connects()` names the seed's address; the torrent finishes.
  - `peer_dial_goes_through_http_connect`: the same with `FakeHttpProxy`; the seed's `demux_stats()` shows no uTP datagram.
  - `http_tracker_through_proxy_by_hostname`: support's `FakeTracker::http` at `http://localhost:<p>/announce`; `connects()` names `localhost:<p>`.
  - `utp_and_udp_tracker_go_through_the_relay`: SOCKS5, the leech with `enable_outgoing_tcp = false`, reaches the seed over uTP and announces to a UDP `FakeTracker`; `udp_targets()` lists both and the leech has no other UDP socket.
  - `proxy_trackers_alone_still_relays_udp`: `proxy_peers = false`, `proxy_trackers = true`: the UDP announce is in `udp_targets()`.
  - `udp_from_non_relay_is_dropped`: script `RelayFromStranger`; `demux_stats()[0].1.dropped > 0`, nothing dispatched.
  - `failed_association_fails_session_new`: `Refuse(1)` gives `SessionError::Proxy`.
  - `tests/trackers.rs` passes unchanged (direct path).
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test proxy`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace && cargo clippy --workspace --all-targets --all-features -- -D warnings && bash dev/check-pure-rust.sh && cargo deny --all-features check`: PASS.
- [ ] **Step 5: Commit.** `feat(session): peers, uTP, DHT and trackers through SOCKS5 or HTTP proxies` No trailers or attribution of any kind.

---

### Task 5: trackers: `stopped` to every tracker, and scrape

**Files:**
- Create: `crates/fbt-tracker/src/scrape.rs`, `fuzz/fuzz_targets/scrape.rs`, `fuzz/seeds/scrape/`.
- Modify: `crates/fbt-tracker/src/{lib.rs,types.rs,http.rs,udp.rs}`, `crates/fbt-session/src/torrent/{announcer.rs,commands.rs,mod.rs}`, `crates/fbt-session/src/event.rs`, `crates/fbt-session/tests/{trackers.rs,support/mod.rs}`, `fuzz/Cargo.toml`, `.github/workflows/fuzz.yml` (matrix: `scrape`).

**Interfaces:**
- Produces:
  - `#[non_exhaustive] pub struct ScrapeInfo { pub info_hash: [u8; 20], pub complete: u32, pub downloaded: u32, pub incomplete: u32 }`; `pub const MAX_SCRAPE_HASHES: usize = 74;` `TrackerClient::scrape(&self, url: &str, hashes: &[[u8; 20]]) -> Result<Vec<ScrapeInfo>, TrackerError>`: HTTP when the last path segment starts with `announce` (replaced by `scrape`, query kept, one `info_hash` per hash), else `TrackerError::ScrapeUnsupported`; more than 74 hashes is `TrackerError::TooManyHashes` before any request; UDP action 2 over the same connection-id cache. Hashes absent from the reply are left out. `#[doc(hidden)] pub fn parse_http_scrape(&[u8]) -> Result<Vec<ScrapeInfo>, TrackerError>`, `parse_udp_scrape(&[u8], tid: u32, n: usize)`. `ScrapeUnsupported` never makes the announcer drop a tracker.
  - Announcer: `Run::started_urls: HashSet<String>` (bounded by the tracker cap): a URL without a `started` in this run gets `started` whatever the run's next event; `stopped()` announces to each in parallel under one `STOPPED_TIMEOUT`; `last_ok` goes. `Signals::scrapes: u64` (bumped by `Cmd::Scrape`) makes the task scrape the first working tracker of each tier.
  - `async fn TorrentHandle::scrape_trackers(&self) -> Result<(), SessionError>`; `Event::ScrapeReply { torrent, url, complete, incomplete, downloaded }`, `Event::ScrapeFailed { torrent, url, message }` (`EventMask::TRACKER`).
  - `support::FakeTracker` gains a switchable reply (`set_reply(Reply)`, `Reply::ServerError` included) and answers `/scrape` and UDP action 2 with its swarm counts.

- [ ] **Step 1: Write failing tests.**
  - `fbt-tracker`: `http_scrape_url_rule` (`/announce` → `/scrape`, `/x/announce.php?k=1` → `/x/scrape.php?k=1`, `/a` is `ScrapeUnsupported`), `http_scrape_parses_files`, `udp_scrape_round_trip`, `scrape_caps_hashes` (75 hashes: `TooManyHashes`, no request sent).
  - `tests/trackers.rs`: `stopped_goes_to_every_started_tracker` (tier 1 answers `started`, is switched to `ServerError`; a forced re-announce reaches tier 2, which records `started`; shutdown sends `stopped` to both), `stopped_skips_trackers_that_never_answered`, `scrape_trackers_reports_counts`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-tracker && cargo test -p fbt-session --test trackers`
- [ ] **Step 3: Implement.** Fuzz `scrape` drives both parsers.
- [ ] **Step 4: Run.** `cargo test --workspace`; `scrape` 60 s: no crash.
- [ ] **Step 5: Commit.** `feat(tracker): scrape and stopped to every tracker that answered` No trailers or attribution of any kind.

---

### Task 6: IP filter, per-IP connection limit and the ban list

**Files:**
- Create: `crates/fbt-net/src/ipfilter.rs`, `crates/fbt-session/src/bans.rs`, `crates/fbt-session/tests/ipfilter.rs`.
- Modify: `crates/fbt-net/src/lib.rs`, `crates/fbt-session/src/{settings.rs,session.rs,listener.rs,event.rs,lib.rs,peer/messages.rs}`, `crates/fbt-session/src/torrent/{actor.rs,swarm.rs,known.rs,holepunch.rs,mod.rs}`.

**Interfaces:**
- Produces:
  - `fbt_net::ipfilter::IpFilter` (`Clone`, `Default` = allow all, `PartialEq`): `block(&mut self, range: RangeInclusive<IpAddr>)`, `allow(&mut self, range)` (mixed families or `start > end` ignored), `is_blocked(&self, ip: IpAddr) -> bool` (IPv4-mapped canonicalised; `O(log n)` over a `BTreeMap<u128, (u128, bool)>` per family), `len()` (disjoint ranges), `is_empty()`, `rules(&self) -> Vec<(RangeInclusive<IpAddr>, bool)>`, `MAX_RANGES` = 1,000,000 (beyond it ignored, logged once). serde by hand: a list of `{ start, end, blocked }`, read back through `block`/`allow`.
  - `pub(crate) struct BanList` (`bans.rs`), keyed by `SocketAddr`: port 0 bans the whole IP (normal mode), a non-zero port one endpoint (only under the hidden `Settings::ban_endpoints`, false by default, so loopback tests can ban one scripted peer): `ban(&mut self, key: SocketAddr) -> bool`, `is_banned(&self, addr: SocketAddr) -> bool` (matches the endpoint or the port-0 key), `list() -> Vec<SocketAddr>`, `MAX_BANNED_IPS` = 4096 (oldest out). `Shared` gains `ip_filter: RwLock<Arc<IpFilter>>`, `bans: Mutex<BanList>`, and `blocked(&self, addr: SocketAddr) -> Option<BlockReason>`.
  - `#[non_exhaustive] pub enum BlockReason { IpFilter, Banned, PerIpLimit }`; `Event::PeerBlocked { torrent: Option<InfoHashes>, addr: SocketAddr, reason: BlockReason }` (`EventMask::PEER`; `None` before a handshake names a torrent); `DisconnectReason::Blocked(BlockReason)`.
  - `async fn Session::set_ip_filter(&self, f: IpFilter)` (every torrent gets `Cmd::FilterChanged` and closes peers now blocked), `Session::ip_filter(&self) -> IpFilter`, `Session::banned_peers(&self) -> Vec<SocketAddr>` (port 0 for a whole IP).
  - `Settings::max_peers_per_ip: usize` (1; non-zero) and `#[doc(hidden)] Settings::per_ip_exempt_loopback: bool` (true); checked in `on_connected`/`on_incoming` against the torrent's live peers by canonical IP.
  - Filter points: `ListenerState::admit` (TCP and uTP accepts, before reading), `try_connect`/`queue_addr`, `on_found_peers` (before `Known`), holepunch `Connect` targets.
  - `fbt` re-exports `IpFilter` and `BlockReason`.

- [ ] **Step 1: Write failing tests.**
  - `ipfilter.rs`: `last_rule_wins_and_ranges_merge`, `v4_mapped_is_canonical`, `families_are_separate`, `reversed_range_is_ignored`, `matches_a_naive_rule_list` (proptest against a linear last-match scan), `serde_is_a_rule_list` (`--features serde`).
  - `tests/ipfilter.rs`: `blocked_seed_is_never_dialled` (`PeerBlocked`, no connect reaches the listener), `blocked_incoming_is_closed_before_handshake`, `setting_a_filter_closes_live_peers` (`Blocked(IpFilter)`), `second_connection_from_one_ip_is_refused` (`per_ip_exempt_loopback = false`: of two scripted peers on `127.0.0.1` the second gets `Blocked(PerIpLimit)` and `PeerBlocked`), `loopback_is_exempt_by_default`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-net ipfilter && cargo test -p fbt-session --test ipfilter`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS (swarm, pex, lsd, holepunch unchanged).
- [ ] **Step 5: Commit.** `feat(session): IP filter, per-IP connection limit and a session ban list` No trailers or attribution of any kind.

---

### Task 7: smart ban

**Files:**
- Create: `crates/fbt-session/src/torrent/smartban.rs`, `crates/fbt-session/tests/smartban.rs`.
- Modify: `crates/fbt-session/src/torrent/{data.rs,actor.rs,peers.rs,mod.rs,swarm.rs}`, `crates/fbt-session/tests/support/mod.rs` (`ScriptedPeer::serve_corrupt(payload, piece_len, bad_blocks: HashSet<BlockRef>, log: Arc<Mutex<Vec<BlockRef>>>)`: corrupts each listed block the first time it serves it, logs every block served).

**Interfaces:**
- Consumes: `BanList`, `BlockReason::Banned`, `DisconnectReason::Blocked`, `ban_endpoints` (Task 6).
- Produces (crate-internal):
  - `pub(super) enum BlockSource { Peer(SocketAddr), WebSeed(PeerKey) }` (a peer's ban key: port 0 unless `ban_endpoints`; Task 11 uses `WebSeed`). Trust is kept per `BlockSource`.
  - `pub(super) struct SmartBan`: `record_block(piece, block_index, BlockSource)` (called after `picker.on_block` accepts), `piece_failed(piece, block_hashes: Vec<[u8; 20]>) -> Vec<BlockSource>` (records hashes and senders, at most `MAX_SMART_BAN_PIECES`, oldest dropped; returns sources whose trust fell to −7 or below), `piece_passed(piece, block_hashes: Option<Vec<[u8; 20]>>) -> Vec<BlockSource>` (sources whose recorded block differs; +1 to each contributor up to 8), `wants_block_hashes(piece) -> bool`, `forget(piece)` (on abandon, skip and recheck via `clear_bufs`). Trust map bounded by `MAX_TRUST_ENTRIES` = 4096.
  - `Job::Hashed` gains `block_hashes: Option<Vec<[u8; 20]>>`, computed in the hash job when the piece fails, or passes while `wants_block_hashes`.
  - `Actor::smart_ban(&mut self, s: BlockSource)` (the existing `Actor::ban(PeerKey)` is the metadata ban and stays): a peer key goes into `Shared::bans`, matching peers close with `Blocked(Banned)`, `PeerBlocked` is emitted; the `WebSeed` arm retires the seed (filled by Task 11; logged until then).

- [ ] **Step 1: Write failing tests.**
  - `smartban.rs`: `trust_points_ban_after_four_failures`, `a_passed_piece_restores_trust`, `records_are_bounded`, `only_differing_blocks_are_blamed`.
  - `tests/smartban.rs` (`ban_endpoints = true`; the leech on `dial_tcp(test_settings())` dials scripted peers built with `ScriptedPeer::accept_with`, so each ban key is a listen address):
    - `only_the_liar_is_banned`: A advertises only piece 3 and corrupts block 0 once; B advertises piece 3 and joins after A's first request arrived; the piece fails, is retried from the same two peers and passes. The banned set equals the peers whose log shows a bad block (A), a `PeerBlocked { reason: Banned }` names A only, and both logs show blocks of piece 3.
    - `honest_contributor_keeps_its_trust`: B stays connected and is asked for blocks again.
    - `single_liar_banned_by_trust_points`: a peer corrupting every piece is banned after its fourth failed piece.
    - `banned_peer_is_not_redialled`: `add_peer` of A's listen address emits `PeerBlocked` and no connection reaches it.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test smartban && cargo test -p fbt-session smartban::`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS.
- [ ] **Step 5: Commit.** `feat(session): smart ban of peers that send bad blocks` No trailers or attribution of any kind.

---

### Task 8: the part file

Spec 5.4; deferred from milestones 1 and 4.

**Files:**
- Create: `crates/fbt-session/src/storage/part.rs`, `crates/fbt-session/tests/partfile.rs`.
- Modify: `crates/fbt-session/src/storage/{mod.rs,fs.rs}`, `crates/fbt-session/src/stream/reader.rs` (doc), `crates/fbt-session/src/torrent/{commands.rs,actor.rs,check.rs,serve.rs,data.rs,magnet.rs}`, `crates/fbt-session/src/session.rs`.

**Interfaces:**
- Produces:
  - `Storage::set_file_priorities(&self, wanted: &[bool]) -> io::Result<()>`, default `Ok(())` (so `MemStorage` and custom storages are unchanged); run on the disk pool before the first check and after every `set_priorities` (after flushing the affected pieces).
  - `part.rs`: `pub(crate) struct PartFile` with `open(path, num_pieces, piece_len) -> io::Result<PartFile>` (a corrupt or mismatched header, e.g. a piece count other than the torrent's, is renamed to `.parts.bad` and started afresh with a warning), `write(piece, offset_in_piece, data)`, `read(piece, offset_in_piece, buf)` (`NotFound` without a slot), `free(piece)`, `is_empty()`, `path()`. Slots lowest-free first; the slot table (`num_pieces × 4` bytes at offset 16) is rewritten on every change.
  - `FsStorage`: a skipped non-pad file that does not exist on disk when `set_file_priorities` runs is routed to the part file (reads and writes of its segments); one that exists is used in place, skipped or not. Zero-length skipped files are not created (`create_empty_locked`). Unskipping exports the file's bytes from the part file (file created sparse) and frees slots whose pieces touch no routed file; an empty part file is deleted; `delete_files` deletes it. `pub fn part_path(&self) -> PathBuf`.
  - Fix (milestone 4 deferral): `piece_lost` of a piece no wanted file overlaps leaves `finished_emitted` alone.

- [ ] **Step 1: Write failing tests.**
  - `part.rs`: `slots_round_trip_and_reuse`, `corrupt_header_starts_afresh`, `mismatched_piece_count_is_rejected` (a header claiming `u32::MAX` pieces for a 4-piece torrent).
  - `tests/partfile.rs` (A 300,000 B `Skip`, B 500,000 B `Normal`, C 300,000 B `Skip`, piece 256 KiB: boundary pieces 1 and 3, wholly skipped 0 and 4):
    - `skipped_neighbour_is_never_created`: after finishing, neither A nor C exists, `part_path()` exists, B reads back correctly.
    - `unskipping_exports_from_the_part_file`: A set to `Normal`; after finishing, A matches and the seed got no second request for piece 1.
    - `reskipping_keeps_data_readable`: A back to `Skip` after it exists: it stays on disk and reads correctly.
    - `recheck_reads_the_part_file`, `delete_removes_the_part_file`.
    - `reader_on_a_skipped_file_reads_the_part_file`: a `FileReader` reads all of C correctly while C does not exist.
    - `existing_skipped_file_is_used_in_place`: all data present, A and C skipped at add: a recheck keeps every piece and no part file appears.
    - `lost_skipped_piece_does_not_refinish`: exactly one `TorrentFinished`.
  - The milestone 1 test `skipped_file_is_not_downloaded` tightens to "the skipped file is absent".
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test partfile && cargo test -p fbt-session part::`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS.
- [ ] **Step 5: Commit.** `feat(session): part file so skipped files are never created` No trailers or attribution of any kind.

---

### Task 9: move storage and rename files

**Files:**
- Create: `crates/fbt-session/src/torrent/storage_ops.rs`, `crates/fbt-session/tests/move.rs`.
- Modify: `crates/fbt-session/src/storage/{mod.rs,fs.rs}`, `crates/fbt-session/src/torrent/{mod.rs,commands.rs,actor.rs,readers.rs,serve.rs,data.rs}`, `crates/fbt-session/src/{event.rs,error.rs,status.rs}`.

**Interfaces:**
- Consumes: `flush_and_wait`, `FlushReason::All`; the part file (Task 8); `fbt_metainfo::collides` (Task 2).
- Produces:
  - `#[non_exhaustive] #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)] pub enum MoveMode { #[default] FailIfExists, ReplaceExisting, KeepExisting }` (serde).
  - `Storage::move_to(&self, save_path: &Path, mode: MoveMode) -> io::Result<()>` and `Storage::rename_file(&self, index: usize, path: &RelPath) -> io::Result<()>`, defaulting to `Err(ErrorKind::Unsupported)`. `FsStorage` keeps `save_path` and entry paths in its `State`, drops cached handles first, moves with `fs::rename` and falls back on `CrossesDevices` (or any rename error on Windows) to copy, `sync_all`, delete; the part file moves too; empty directories the torrent created under the old path are removed; a failure moves back what moved. `FsStorage::with_paths(save_path, &Metainfo, renamed: &[(usize, RelPath)]) -> FsStorage` (Task 12 uses it).
  - `async fn TorrentHandle::move_storage(&self, path: PathBuf, mode: MoveMode) -> Result<(), SessionError>` and `async fn TorrentHandle::rename_file(&self, index: usize, path: &str) -> Result<(), SessionError>` (relative to the save path; split on `/`, `sanitize_path`, `collides` → `SessionError::InvalidPath`). Both reply once the job ended; failure is also an event.
  - Actor: `storage_busy: bool` and `reads_in_flight: usize` (beside `hashing`, counting `Job::Read`, `ReadPiece`, `ReadChunk`). The move job is spawned once the flush is done and `reads_in_flight == 0`; while busy, peer and reader reads wait in `PieceReads`/`readers.waiting`, no write is issued, and new dirty pieces stay cached. `KeepExisting` then calls `recheck()`.
  - `Event::StorageMoved { torrent, path }`, `StorageMoveFailed { torrent, message }`, `FileRenamed { torrent, index, path }`, `FileRenameFailed { torrent, index, message }` (`EventMask::STORAGE`); `SessionError::InvalidPath(String)`, `SessionError::Unsupported`; `TorrentStatus::save_path: PathBuf`.
  - `fbt` re-exports `MoveMode`.

- [ ] **Step 1: Write failing tests** in `tests/move.rs` (tempdirs; the copy fallback through a `#[cfg(test)]` switch on `FsStorage`):
  - `move_flushes_dirty_pieces_first`: large `cache_size` and `cache_expiry`; `move_storage` while downloading; after `StorageMoved` the old directory holds no torrent file and the torrent finishes correctly at the new path.
  - `reader_survives_a_move`, `move_to_same_path_is_a_noop`, `fail_if_exists_moves_nothing` (error, nothing moved, still seeding), `replace_existing_overwrites`, `keep_existing_rechecks` (state passes through `CheckingFiles`).
  - `failed_move_rolls_back`: on Unix a read-only target directory, the test first creating a probe file there and skipping itself when that succeeds (root); on Windows a target file held open with `OpenOptionsExt::share_mode(0)`. `StorageMoveFailed`, data still at the old path.
  - `move_waits_for_running_reads` (unit, `storage_ops.rs` on `actor_with`: a `Job::ReadPiece` outstanding delays the move job until it returns).
  - `copy_fallback_moves_the_part_file` (unit, `fs.rs`).
  - `rename_file_then_serve`: rename file 1 to `t/sub/x.bin`; the seed serves it from there; a rename onto file 0's path, onto a directory prefix of another file, or to `../x` is `InvalidPath`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test move`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS.
- [ ] **Step 5: Commit.** `feat(session): move storage and rename files` No trailers or attribution of any kind.

---

### Task 10: queueing and seed limits

**Files:**
- Create: `crates/fbt-session/src/queue.rs`, `crates/fbt-session/tests/queue.rs`.
- Modify: `crates/fbt-session/src/{settings.rs,session.rs,status.rs,event.rs,lib.rs}`, `crates/fbt-session/src/torrent/{mod.rs,commands.rs,actor.rs,swarm.rs,announcer.rs}`, `crates/fbt-session/tests/support/mod.rs` (`test_settings`), `crates/fbt-example/src/main.rs` (`bench_settings`).

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
    `Settings::queue`, `Settings::seed_ratio_limit: Option<f64>` (finite, ≥ 0), `Settings::seed_time_limit: Option<Duration>`; `Session::set_queue_settings(&self, q: QueueSettings) -> Result<(), SessionError>` (validated, then a pass at once).
  - Pure `pub(crate) fn plan(entries: &[QueueEntry], cfg: &QueueSettings, now: Instant) -> Vec<bool>` (`QueueEntry { position, seeding, running_since: Option<Instant>, down_rate, up_rate, user_paused, auto_managed }`; `true` = run), in position order: a slow torrent past its grace is exempt from `active_downloads`/`active_seeds` only; every running auto-managed torrent counts against `active_limit`.
  - Scheduler in `session.rs`: woken by `Shared::queue_dirty: Notify` or the interval, it reads statuses, calls `plan`, and sends `Cmd::SetQueued(bool)` and `Cmd::SetQueuePosition(usize)` to changed torrents. Positions are the registry's dense order. `add_torrent` registers an auto-managed torrent queued and runs `plan` under the registry lock before the actor starts.
  - Actor: `queued: bool`, `queue_position: usize`, one predicate `stopped(&self) -> bool { self.paused || self.queued }` replacing `self.paused` in `active()`, `connectable()` and `running()`; `SetQueued(true)` runs the pause path (close, flush and wait), `false` the resume path; `state()` shows `Paused` before `Queued`. Seed limits are checked on each status tick while `Seeding` (ratio = uploaded / max(downloaded, total_wanted)).
  - `AddTorrent::auto_managed: bool` (true); `async fn set_auto_managed(&self, bool)`, `async fn set_queue_position(&self, usize)` (clamped; `usize::MAX` = bottom) on `TorrentHandle`; `TorrentStatus::{queue_position, auto_managed, active_time, seeding_time}`; `TorrentState::Queued`; `Event::SeedLimitReached { torrent }` (`EventMask::STATUS`).
  - `test_settings()` and `bench_settings` set the three limits to `None`, so existing multi-torrent tests and the 1,000-idle-seed bench row are unchanged.
  - `fbt` re-exports `QueueSettings`.

- [ ] **Step 1: Write failing tests.**
  - `queue.rs`: `plan_never_exceeds_limits` (proptest over 0 to 40 entries: non-slow running downloads ≤ their limit, likewise seeds, all running ≤ `active_limit`; user-paused and non-auto-managed entries never started by the plan), `positions_order_wins`, `slow_torrents_do_not_count_after_grace`.
  - `tests/queue.rs`: `fourth_download_is_queued` (it is `Queued` from its first status, never `started`, and starts when one finishes), `queued_torrent_has_no_peers`, `positions_stay_dense`, `set_queue_position_moves_it_up`, `user_pause_beats_the_queue`, `seed_ratio_limit_pauses_and_emits`, `seed_time_limit_pauses`, `queued_torrent_announces_stopped`, `limits_change_at_runtime` (`set_queue_settings`: the queued one starts within one pass).
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test queue && cargo test -p fbt-session queue::`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test --workspace`: PASS.
- [ ] **Step 5: Commit.** `feat(session): auto-managed queueing and seed limits` No trailers or attribution of any kind.

---

### Task 11: web seeds (BEP 19)

**Files:**
- Create: `crates/fbt-session/src/torrent/webseed.rs`, `crates/fbt-session/tests/webseed.rs`, `crates/fbt-session/tests/support/web_seed.rs`, `fuzz/fuzz_targets/web_seed_head.rs`, `fuzz/seeds/web_seed_head/`.
- Modify: `crates/fbt-tracker/src/{http.rs,lib.rs}`, `crates/fbt-session/src/picker.rs`, `crates/fbt-session/src/torrent/{actor.rs,data.rs,mod.rs,smartban.rs}`, `crates/fbt-session/src/{event.rs,status.rs,session.rs}`, `fuzz/Cargo.toml`, `.github/workflows/fuzz.yml` (matrix: `web_seed_head`).

**Interfaces:**
- Consumes: `Metainfo::web_seeds`, `Magnet::web_seeds`, `fbt_metainfo::MAX_WEB_SEEDS` (Task 2); `HttpClient`'s `web_seeds` client (Task 4); `BlockSource::WebSeed`, `Actor::smart_ban` (Task 7); `PiecePicker::pick_piece` (milestone 4).
- Produces:
  - `TrackerClient::fetch_range(&self, url: &str, range: Range<u64>) -> Result<Bytes, TrackerError>`: one redirect helper shared with announces returns the final response (at most 3 redirects, `https` → `http` refused); `Range: bytes=a-b`; 206 with a matching `Content-Range` (`#[doc(hidden)] pub fn parse_content_range(&str) -> Option<(u64, u64, Option<u64>)>`), or 200 only when `a == 0` and `Content-Length == b - a + 1`; reads at most `len + 1` bytes (one more is an error), whatever `max_response_bytes` says; `http_timeout` bounds each wait for a frame, not the whole request.
  - `PiecePicker::pick_run(&mut self, peer: PeerKey, first_free: u32, max_bytes: u64) -> Vec<BlockRef>`: consecutive unstarted pieces from the first one at or after `first_free`, through `pick_piece`, until `max_bytes`.
  - Data path: `on_block` splits into the peer checks and `pub(super) fn accept_block(&mut self, key: PeerKey, block: BlockRef, data: Bytes, source: BlockSource, generation: u64)`, which web seeds call with the generation their request was issued under.
  - `webseed.rs`: `pub(super) struct WebSeeds` (per URL: `key: PeerKey`, `state: Idle | Busy | Backoff(Instant) | Retired`, `failures: u32`), `url_for(base, meta, file_index) -> String` (BEP 19: a base ending in `/` gets `name` and, for multi-file torrents, the percent-encoded path components), `segments(layout, blocks) -> Vec<(file_index, Range<u64>)>`; `const WEB_SEED_REQUEST_MAX: u64 = 4 << 20;`, backoff 30 s × 2^n to 30 min.
  - Actor: on the status tick, each idle seed (torrent active, download tokens available) takes `pick_run`, fetches its segments one after the other, and returns `Job::WebSeed { key, generation, blocks, result: Result<Vec<Bytes>, String> }`, fed to `accept_block` with `BlockSource::WebSeed(key)`; a failure releases the blocks. A seed is a `HaveAll` peer added at start and removed (`remove_peer_pieces(PeerPieces::All)`) on retire or remove. `async fn add_web_seed(&self, url: String)`, `async fn remove_web_seed(&self, url: &str)`, `async fn web_seeds(&self) -> Result<Vec<String>, SessionError>` on `TorrentHandle`; `TorrentStatus::num_web_seeds: u32`; `Event::WebSeedError { torrent, url, message, retired: bool }` (`EventMask::PEER`). Private torrents use web seeds too.
- Test server `support/web_seed.rs`: `WebSeedServer::start(root: PathBuf, Behaviour) -> WebSeedServer` with `url()`, `requests() -> Vec<(String, Option<String>)>`, `set_rate(bytes_per_sec)`, behaviours `Normal`, `IgnoreRange`, `WrongContentRange`, `EndlessBody`, `RedirectLoop`, `NotFound(path)`, `Corrupt(offset)`. Uses `tokio` only.

- [ ] **Step 1: Write failing tests.**
  - `fbt-tracker`: `content_range_parsing` (valid, `*`, malformed, overflow).
  - `picker.rs`: `pick_run_takes_consecutive_unstarted_pieces` (a started piece ends the run).
  - `tests/webseed.rs`: `downloads_from_a_web_seed_alone` (single- and multi-file, no peers: finishes, every request carried a `Range`, each request is one contiguous range per file), `web_seed_and_peer_together` (a 16 MiB payload, the server rate-limited: both delivered bytes), `ignored_range_is_an_error` (a range starting past 0), `wrong_content_range_is_an_error`, `endless_body_is_capped`, `slow_live_seed_is_not_cut` (1 MiB at 64 KiB/s with `http_timeout` 2 s completes), `redirect_loop_stops`, `missing_file_retires_the_seed` (`retired: true`), `corrupt_web_seed_is_retired` (`HashFailed`, then `retired: true` through smart ban; a peer finishes the torrent), `percent_encoding_of_paths` (`a b#c.bin`), `web_seed_through_socks5` (`proxy_peers` only: `connects()` names the server).
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test webseed && cargo test -p fbt-tracker content_range`
- [ ] **Step 3: Implement.** Fuzz `web_seed_head` drives `parse_content_range`.
- [ ] **Step 4: Run.** `cargo test --workspace`; `web_seed_head` 60 s: no crash.
- [ ] **Step 5: Commit.** `feat(session): BEP 19 web seeds` No trailers or attribution of any kind.

---

### Task 12: resume data and session state

**Files:**
- Create: `crates/fbt-session/src/resume.rs`, `crates/fbt-session/src/storage/stamp.rs`, `crates/fbt-session/tests/resume.rs`, `fuzz/fuzz_targets/resume.rs`, `fuzz/seeds/resume/`.
- Modify: `crates/fbt-session/src/{session.rs,error.rs,status.rs,lib.rs}`, `crates/fbt-session/src/storage/{mod.rs,fs.rs}`, `crates/fbt-session/src/torrent/{mod.rs,actor.rs,check.rs,commands.rs,data.rs,magnet.rs}`, `fuzz/Cargo.toml` (depends on `fbt-session`), `.github/workflows/fuzz.yml` (matrix: `resume`; `crates/fbt-session/**` in the PR `paths`).

**Interfaces:**
- Consumes: priorities; `FsStorage::with_paths` and the save path (Task 9); queue position, `auto_managed`, `active_time`, `seeding_time` (Task 10); `add_web_seed` (Task 11); the part file stamp (Task 8); `Session::dht_state` (milestone 2).
- Produces:
  - `#[non_exhaustive] pub struct FileStamp { pub len: u64, pub mtime: i64 }`; `Storage::stamps(&self) -> io::Result<Option<Vec<Option<FileStamp>>>>` (default `Ok(None)`; `FsStorage` drops its cached handles first, then gives one entry per file, `None` if missing, then one for the part file).
  - `resume.rs`: `pub(crate) struct ResumeState { have: Bitfield, stamps: Vec<Option<FileStamp>>, renamed: Vec<(usize, RelPath)>, web_seeds: Vec<String>, queue_position: Option<usize>, uploaded: u64, downloaded: u64, active_time: Duration, seeding_time: Duration, added: i64, completed: Option<i64>, peers: Vec<SocketAddr> }`; `pub(crate) fn encode(..) -> Vec<u8>`; `#[doc(hidden)] pub fn decode_resume(bytes: &[u8]) -> Result<DecodedResume, ResumeError>` with `#[doc(hidden)] pub struct DecodedResume`. Input over `MAX_RESUME_BYTES` = 64 MiB is refused before decoding; `decode_with(Limits::new(8, MAX_RESUME_ITEMS))`, `MAX_RESUME_PEERS` = 200 per family, list lengths checked against the file count.
  - Keys (sorted; the first three required): `fbt-resume-version` (1), `info-hash` (20 bytes), `save-path`, `info` (raw; SHA-1 must equal `info-hash`), `magnet` (without `info`), `file-priorities`, `renamed` (index → path list), `pieces`, `paused`, `auto-managed`, `sequential`, `queue-position`, `download-limit`, `upload-limit`, `total-uploaded`, `total-downloaded`, `active-time`, `seeding-time`, `added-time`, `completed-time`, `trackers`, `url-list`, `peers`, `peers6`, `stamps` (list of `[len, mtime]` or empty list).
  - `#[derive(Debug, thiserror::Error)] #[non_exhaustive] pub enum ResumeError { Bencode(DecodeError), TooLarge, Version(i64), Missing(&'static str), Invalid(&'static str), InfoHashMismatch, Metainfo(MetainfoError), Magnet(MagnetError) }`.
  - `AddTorrent::from_resume(bytes: &[u8]) -> Result<AddTorrent, ResumeError>` fills `source`, `save_path`, flags, priorities, limits, `auto_managed` and a crate-private `resume: Option<Box<ResumeState>>`; the caller may still set `storage`. At add the storage is `FsStorage::with_paths(.., &resume.renamed)` (in `session.rs` and, for a magnet, `magnet.rs`); web seeds are added after `install`; the saved position is taken when it fits, else the bottom.
  - `async fn TorrentHandle::save_resume_data(&self) -> Result<Vec<u8>, SessionError>`: `flush_and_wait`, then encodes; while checking, `pieces` is the pre-check bitfield (`last_have`), or the resume one, or empty. Documented: trusted on restart only when nothing is written after it (save after `pause`, or use `shutdown_and_save`). `TorrentStatus::need_save_resume: bool` (set by a written piece, priority, rename, move, pause, resume, limit or queue change; cleared by a save) and `TorrentStatus::resume_trusted: Option<bool>` (`None` without resume data).
  - `start_check` fast path: with `resume`, a disk-pool job reads `stamps()` and returns `Job::ResumeChecked { generation, trusted: Option<Bitfield> }`; `Some` goes to `on_checked(generation, Ok(have))`, `None` to the normal check. Trusted when stamps are `None` or every file holding a saved piece byte (and the part file when a saved piece has bytes in a routed skipped file) has an equal stamp.
  - `#[non_exhaustive] #[derive(Debug, Clone, Default, PartialEq, Eq)] pub struct SessionState { pub dht: Option<Vec<u8>>, pub torrents: Vec<Vec<u8>> }`, `to_bytes()`/`from_bytes()` (`fbt-session-version` 1; zero-copy decode, at most `MAX_SESSION_TORRENTS` = 100,000 entries, no byte cap beyond the input; each entry checked by `decode_resume` at restore). `async fn Session::save_state(&self) -> Result<SessionState, SessionError>` (queue order), `async fn Session::shutdown_and_save(&self) -> SessionState` (each torrent's resume data after its final flush, then the usual shutdown), `async fn Session::restore(settings: Settings, state: &SessionState) -> Result<(Session, Vec<(usize, SessionError)>), SessionError>`. `SessionError::Resume(ResumeError)`.
  - `fbt` re-exports `SessionState`, `ResumeError`, `FileStamp`.

- [ ] **Step 1: Write failing tests** in `tests/resume.rs` (and `resume.rs` units):
  - `resume_round_trip_without_recheck`: `FsStorage`, half downloaded, `pause`, `save_resume_data`, shutdown; a new session adds `from_resume`: `resume_trusted == Some(true)`, the state never shows `CheckingFiles`, `pieces()` equals the saved bitfield, the torrent finishes.
  - `touched_file_forces_a_recheck` (one byte rewritten, mtime +2 s: `resume_trusted == Some(false)`, the piece is fetched again), `missing_part_file_forces_a_recheck`, `custom_storage_is_trusted` (`CountingStorage`: no piece read before the first peer request).
  - `magnet_resume_skips_metadata_fetch` (no `MetadataReceived`, no `ut_metadata` request).
  - `resume_keeps_priorities_renames_queue_and_web_seeds` (a renamed file is found at its new path without a recheck).
  - `resume_rejects_garbage_and_mismatches`: random bytes, version 2, another torrent's `info` (`InfoHashMismatch`), wrong-length `pieces` (`Invalid("pieces")`), 65 MiB of input (`TooLarge`), 10,000 peers (truncated to 200).
  - `need_save_resume_follows_changes`.
  - `session_state_round_trip`: three torrents, DHT on loopback nodes; `shutdown_and_save`, `restore`: same torrents and order, all `resume_trusted == Some(true)`, `dht_stats().nodes_v4 > 0` before any bootstrap.
  - `dirty_pieces_at_shutdown_are_in_the_state`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test resume && cargo test -p fbt-session resume::`
- [ ] **Step 3: Implement.** Fuzz `resume` runs `decode_resume` and `SessionState::from_bytes`; seeds from the tests.
- [ ] **Step 4: Run.** `cargo test --workspace`; `resume` 60 s: no crash.
- [ ] **Step 5: Commit.** `feat(session): resume data and session state` No trailers or attribution of any kind.

---

### Task 13: DHT scrape (BEP 33) and `sample_infohashes` (BEP 51)

**Files:**
- Create: `crates/fbt-dht/src/bloom.rs`.
- Modify: `crates/fbt-dht/src/{krpc.rs,server.rs,store.rs,lookup.rs,node.rs,lib.rs}`, `crates/fbt-dht/tests/swarm.rs`, `crates/fbt-session/src/{session.rs,error.rs,lib.rs}`, `crates/fbt-session/src/torrent/dht.rs`, `crates/fbt-session/tests/dht.rs`, `fuzz/seeds/krpc/`.

**Interfaces:**
- Produces:
  - `krpc`: `Query::GetPeers { info_hash, scrape: bool }`, `Query::AnnouncePeer { .., seed: bool }`, `Query::SampleInfohashes { target: NodeId }`; `Response` gains `bf_seeds: Option<Box<[u8; 256]>>`, `bf_peers: Option<Box<[u8; 256]>>`, `samples: Vec<[u8; 20]>` (cap `MAX_SAMPLES` = 20 on decode), `num: Option<u32>`, `interval: Option<u32>`. A filter of the wrong length is ignored.
  - `bloom.rs`: `pub struct BloomFilter([u8; 256])` with `insert(ip: IpAddr)`, `union(&mut self, &BloomFilter)`, `estimate(&self) -> u32` (BEP 33's formula, `m` = 2048, `k` = 2, 6000 when full).
  - `PeerStore` keeps a `seed` flag per entry and answers scrape with both filters; `sample(rng, max) -> Vec<[u8; 20]>`, `len()`. The server answers `sample_infohashes` with `interval` 21600, `num`, up to 20 samples and the closest nodes.
  - `Dht::announce(&self, info_hash: [u8; 20], port: Option<u16>, seed: bool) -> PeerLookup`; the torrent's DHT announcer passes `seed` while seeding.
  - `#[non_exhaustive] pub struct DhtScrape { pub seeds: u32, pub peers: u32 }`, `Dht::scrape(&self, info_hash) -> Result<DhtScrape, DhtError>` (a `get_peers` lookup with `scrape=1`, unioning the K closest answers' filters); `#[non_exhaustive] pub struct DhtSamples { pub interval: Duration, pub num: u32, pub samples: Vec<[u8; 20]>, pub nodes: Vec<NodeInfo> }`, `Dht::sample_infohashes(&self, node: SocketAddr, target: NodeId) -> Result<DhtSamples, DhtError>`.
  - `async fn Session::dht_scrape(&self, info_hash: [u8; 20]) -> Result<DhtScrape, SessionError>`, `async fn Session::dht_sample_infohashes(&self, node: SocketAddr, target: [u8; 20]) -> Result<DhtSamples, SessionError>`; `SessionError::DhtDisabled`, `SessionError::Dht(String)`; `fbt` re-exports `DhtScrape`, `DhtSamples`.

- [ ] **Step 1: Write failing tests.**
  - `bloom.rs`: `estimate_tracks_inserts` (100 and 1000 distinct IPs within 10 %), `union_is_or`, `bep33_test_vector` (BEP 33's example set gives the estimate it publishes, within 1 %).
  - `store.rs`: `scrape_filters_count_seeds_and_peers` (10 distinct synthetic IPs announced straight into `PeerStore`, 4 as seeds: estimates 4 ± 1 and 6 ± 1).
  - `krpc.rs`: `new_fields_round_trip`, `oversized_samples_are_cut`, `short_bloom_filter_is_ignored`.
  - `tests/swarm.rs` (loopback, one IP): `scrape_round_trip` (one seed and one leecher announce: both estimates 1), `sample_infohashes_returns_stored_hashes`.
  - `fbt-session/tests/dht.rs`: `session_dht_scrape`, `dht_methods_without_dht_are_disabled`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-dht && cargo test -p fbt-session --test dht`
- [ ] **Step 3: Implement.** `krpc` seeds for `sample_infohashes` and a scrape reply.
- [ ] **Step 4: Run.** `cargo test --workspace`; `krpc` 60 s: no crash.
- [ ] **Step 5: Commit.** `feat(dht): BEP 33 scrape and BEP 51 sample_infohashes` No trailers or attribution of any kind.

---

### Task 14: DHT items (BEP 44)

**Files:**
- Create: `crates/fbt-dht/src/items.rs`, `crates/fbt-session/tests/dht_items.rs`.
- Modify: `Cargo.toml` (`ed25519-dalek = { version = "3", default-features = false, features = ["fast", "zeroize"] }`: 3.0.0 has no `std` feature), `crates/fbt-dht/{Cargo.toml,src/krpc.rs,src/server.rs,src/lookup.rs,src/node.rs,src/token.rs,src/lib.rs}`, `crates/fbt-dht/tests/swarm.rs`, `crates/fbt-session/src/{session.rs,lib.rs}`, `fuzz/seeds/krpc/`.

**Interfaces:**
- Consumes: Task 13's lookup extensions, `SessionError::{DhtDisabled, Dht}`.
- Produces:
  - `krpc`: `Query::Get { target: NodeId, seq: Option<i64> }`, `Query::Put { token: Vec<u8>, v: Vec<u8> /* raw bencode */, k: Option<[u8; 32]>, sig: Option<[u8; 64]>, seq: Option<i64>, salt: Option<Vec<u8>>, cas: Option<i64>, reject: Option<i64> }` (`reject` = 205 or 207 when the decoder cut an over-long `v` or salt); `Response` gains `v: Option<Vec<u8>>`, `k`, `sig`, `seq`. `get` responses carry a write token (`token.rs`, as for `get_peers`) and `put` checks it (203 on a bad token).
  - `items.rs`: `pub(crate) struct ItemStore` (bounded `max_items`, 2 h lifetime, least recently put evicted), `signable(salt, seq, v) -> Vec<u8>` (`4:salt<n>:<salt>3:seqi<seq>e1:v<v>`, salt part omitted when empty), `verify(k, sig, salt, seq, v) -> bool` (`VerifyingKey::verify_strict`), put rules: immutable target = SHA-1(v); mutable target = SHA-1(k ‖ salt); 206 bad signature, 301 cas mismatch, 302 lower seq (or equal seq with a different `v`).
  - `DhtConfig::max_items: usize` (700).
  - `#[non_exhaustive] pub struct MutableItem { pub public_key: [u8; 32], pub salt: Vec<u8>, pub seq: i64, pub value: Vec<u8> }`; `Dht::get_immutable(&self, target: [u8; 20]) -> Result<Option<Vec<u8>>, DhtError>`, `Dht::put_immutable(&self, value: Vec<u8>) -> Result<([u8; 20], usize), DhtError>`, `Dht::get_mutable(&self, public_key: [u8; 32], salt: &[u8]) -> Result<Option<MutableItem>, DhtError>` (highest verified `seq`), `Dht::put_mutable(&self, secret_seed: [u8; 32], salt: Vec<u8>, value: Vec<u8>, seq: i64, cas: Option<i64>) -> Result<usize, DhtError>` (nodes that stored it). Values must be valid bencode (`DhtError::InvalidValue`).
  - `async fn Session::{dht_get_immutable, dht_put_immutable, dht_get_mutable, dht_put_mutable}` with the same shapes returning `SessionError`; `fbt` re-exports `MutableItem`.

- [ ] **Step 1: Write failing tests.**
  - `items.rs`: `bep44_test_vectors` (the published mutable signature, with and without salt `foobar`, verifies with `verify_strict` over our `signable`; the published targets match), `sign_then_verify_round_trip` (a fixed seed of ours), `seq_rules` (302, 301), `store_is_bounded`, `oversized_value_is_205`.
  - `krpc.rs`: `get_put_round_trip`.
  - `tests/swarm.rs`: `immutable_put_then_get`, `mutable_put_then_get_highest_seq`, `forged_signature_is_refused` (206; `get_mutable` returns `None`), `put_without_a_token_is_refused` (203).
  - `fbt-session/tests/dht_items.rs`: `session_put_get_round_trip` on loopback nodes.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-dht && cargo test -p fbt-session --test dht_items`
- [ ] **Step 3: Implement.** `krpc` seeds for `get`, `put` and their replies.
- [ ] **Step 4: Run.** `cargo test --workspace && bash dev/check-pure-rust.sh && cargo deny --all-features check`; `krpc` 60 s: no crash.
- [ ] **Step 5: Commit.** `feat(dht): BEP 44 immutable and mutable items` No trailers or attribution of any kind.

---

### Task 15: `fbt-example` flags and interop scenarios

"Done" for this milestone means these pass in CI.

**Files:**
- Create: `dev/interop/socks5/{Dockerfile,danted.conf}`, `dev/interop/webseed/nginx.conf`.
- Modify: `crates/fbt-example/{Cargo.toml,src/main.rs,tests/cli.rs}` (dev-deps: `tokio` for `WebSeedServer`), `dev/interop/{compose.yaml,run.sh,README.md}`, `.github/workflows/ci.yml` (timeout only).

**Interfaces:**
- Consumes: `ProxyConfig` (Task 4), `add_web_seed` (Task 11), `AddTorrent::from_resume`, `save_resume_data`, `resume_trusted` (Task 12).
- Produces:
  - `NetArgs` gains `--proxy <URL>` (`socks5://[user:pass@]host:port` or `http://[user:pass@]host:port` into `ProxyConfig`); `download` gains `--resume <PATH>` (loaded when present; on exit `save_resume_data` is written there through a temp file and rename), `--stop-at <PERCENT>` (at that progress: `pause`, save, shut down, exit 0) and `--web-seed <URL>` (repeatable). With `--resume` the CLI prints `resume: trusted (<n> pieces)` or `resume: checking` from `TorrentStatus::resume_trusted` once it is `Some`.
  - Services: `socks5` (Dante on the pinned `debian:bookworm-slim` with `dante-server`, no auth, UDP associate allowed, port 1080, on `swarm`), `webseed` (`nginx` pinned by multi-arch index digest, serving `${INTEROP_DIR}/payload6` read-only, access log with the status to stdout).
  - Scenarios (`known_scenarios` gains `resume`, `webseed`, `proxy`):
    1. `resume`: fixture 1 from qBittorrent only (Transmission's copy stopped as in `stream`): `download --resume /interop/fbt-resume/r.dat --stop-at 50`, then without `--stop-at`; the second log has `resume: trusted` and no `checking files`, the data matches, qBittorrent's `uploaded` grew in both runs. `interop: resume without recheck OK (qbittorrent)`.
    2. `webseed`: `make_payload /interop/payload6/fixture6` unless present; `transmission-create -w http://webseed/ -s 256` (trackerless); `download --no-dht` with no peer finishes, data matches, the nginx log shows `206`. `interop: web seed OK (nginx)`.
    3. `proxy`: `download /interop/fixture.torrent --proxy socks5://socks5:1080` from qBittorrent: finishes, data matches; `/api/v2/sync/torrentPeers` lists the `socks5` container's address and never `fbt`'s; fbt's log has a `TrackerReply` line. `interop: SOCKS5 proxy OK (qbittorrent)`.

- [ ] **Step 1: Write failing tests** in `tests/cli.rs` (every invocation `--port 0 --no-dht`): `proxy_url_parsing` (both schemes, credentials; a missing port exits 2), `resume_file_is_written_and_trusted` (`--stop-at 50 --resume` from a local `seed`, then again: stderr says `resume: trusted`), `web_seed_flag_downloads` (`WebSeedServer` via `#[path = "../../fbt-session/tests/support/web_seed.rs"]`, no peer).
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-example`
- [ ] **Step 3: Implement the flags, then the scenarios.** Run `INTEROP_ONLY=resume,webseed,proxy bash dev/interop/run.sh`.
- [ ] **Step 4: Run the full suite.** `bash dev/interop/run.sh`: every milestone 1 to 4 line, the three new OK lines, `interop: all scenarios passed ...`, exit 0; `docker network inspect fbt-interop_swarm --format '{{.Internal}}'` prints `true`. Raise the CI `interop` timeout by 10 minutes if needed.
- [ ] **Step 5: Commit.** `ci: interop resume without recheck, web seed and SOCKS5 proxy` No trailers or attribution of any kind.

---

### Task 16: docs, changelog and full gate

**Files:**
- Modify: `README.md`, `CHANGELOG.md`, `CLAUDE.md`, `crates/fbt/src/lib.rs` (crate docs), `deny.toml` (only if a new license or advisory appears, with its one-line reason).

- [ ] **Step 1: Update the docs.**
  - `crates/fbt/src/lib.rs`: sections "Resume data and session state" (a `no_run` doctest: `pause`, `save_resume_data`, `AddTorrent::from_resume`, `shutdown_and_save`, `restore`; when a resume file is trusted), "Queueing", "Proxies" (the `ProxyConfig` caveats), "IP filter and bans" (and the separate metadata ban), "Web seeds", "DHT items"; the `serde` feature.
  - `README.md`: feature list (resume, queueing, seed limits, move and rename, part file, proxies, IP filter, smart ban, web seeds, BEP 44/33/51, serde); a resume example matching the doctest; the proxy caveats; the `fbt-example` flags; the interop paragraph names the three scenarios.
  - `CHANGELOG.md` `## [Unreleased]`, every entry ending in `([#N](https://github.com/featherbitplatform/fbt-lib/pull/N))`:
    - `### Added`: everything produced in Tasks 1 to 15 by public name, the new handle and session methods named as `async`.
    - `### Changed`: `TrackerClient::new_with` replaces `with_udp_sockets`; `Dht::announce` takes `seed`; new `SessionError`, `TrackerError`, `Event`, `TorrentState::Queued`, `DisconnectReason::Blocked` variants; new `TorrentStatus` fields; `MetainfoError::DuplicatePath` is no longer produced (colliding paths are renamed instead); `stopped` goes to every tracker that answered; one connection per IP per torrent by default (loopback exempt); skipped files are no longer created; torrents are auto-managed and queued by default (3 downloads, 5 seeds).
    - `### Fixed`: `TorrentFinished` emitted twice after a skipped-only piece loss.
  - `CLAUDE.md` (the only task that edits it): the fuzz target list gains `resume socks5 http_connect scrape web_seed_head`; the interop line names resume, web seed and SOCKS5; the test rule names the proxy and web seed fakes.
- [ ] **Step 2: Run the full gate.** `cargo fmt --all --check && cargo clippy --workspace --all-targets --all-features -- -D warnings && cargo test --workspace && cargo test --workspace --all-features && cargo deny --all-features check && bash dev/check-pure-rust.sh && RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features && bash dev/sast.sh`: all pass.
- [ ] **Step 3: Commit.** `docs: milestone 5 changelog, README and CLAUDE.md` No trailers or attribution of any kind.

---

## After the plan: shipping v0.5.0

PR `feature/m5-persistence-ops` → `develop`. Every CI job must be green, including `interop` and the fuzz runs the PR triggers (the five new targets, plus `krpc`, `metainfo`, `magnet` and `path`). The PR body lists the decisions made on the owner's behalf (this plan's Decisions and Pre-flight rulings, plus any taken during execution) and a "Deferred to later milestones" section, and the CHANGELOG PR links carry its number. Then the Gitflow release:

1. `release/0.5.0` off `develop`: bump the workspace version to `0.5.0` (the peer-id prefix becomes `-FB0500-` and the KRPC `v` becomes `FB\x00\x05` automatically). Move `Unreleased` to `## [0.5.0] - <date>`, check `bash dev/test-release-version.sh`, run `bash dev/bench.sh` on the release commit and keep its row.
2. PR into `main`, then the annotated tag `v0.5.0`, then merge `main` back into `develop`.

No attribution lines anywhere.

## Deferred to later milestones (recorded so nobody builds them now)

- **Milestone 6:** v2 and hybrid torrents, Merkle verification, torrent creation; resume data gains v2 piece layers (`fbt-resume-version` 2, version 1 still read).
- **Milestone 7 (`fbt-api`):** HTTP `Range` serving on `FileReader`; HTTP/JSON control of everything this milestone adds.
- **Backlog:**
  - serde for the internal crates' configs (`UtpConfig`, `PortMapConfig`, `LsdConfig`, `ChokerConfig`, `DhtConfig`): not in `fbt`'s API, built by the session from `Settings` (pre-flight F33);
  - IP filter file parsers (eMule `.dat`, PeerGuardian `.p2p`): featherbittorrent parses them into `IpFilter` until a second client needs them;
  - serde for `fbt-bencode` values (no consumer);
  - UDP tracker hostnames resolved through the SOCKS5 proxy, IPv6 through a SOCKS5 UDP association, absolute-form GET for plain-HTTP trackers through HTTP proxies that refuse CONNECT to port 80, and the DHT through an HTTP proxy (impossible: no UDP);
  - SOCKS4/4a and I2P (spec non-goal);
  - from milestone 4: batched UDP syscalls and multi-task uTP processing, an adaptive status tick for idle torrents, the qBittorrent Docker-bridge ceiling, a cached missing-piece count for readers, listener tests that retry a port in a Windows excluded range, and the review polish recorded in the per-task reviews.
