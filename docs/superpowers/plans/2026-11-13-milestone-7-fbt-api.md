# Milestone 7: `fbt-api` (HTTP/JSON Control, Events, Streaming) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `fbt-lib` `v0.7.0` ships the optional crate `fbt-api`:
- a JSON control API over every `fbt` command and status call, with an OpenAPI document;
- a Server-Sent Events stream of session events;
- byte-range streaming URLs for every file of a torrent, which browsers, Tauri webviews, mpv and VLC can play and seek;
- bearer-token auth and a localhost default.

featherbittorrent mounts the router in its own server, or runs the ready server, and adds its own routes alongside. It is proven against an `fbt-example` session in Docker: `curl` streams ranges while the torrent downloads from qBittorrent, and the SSE stream reports progress.

**Architecture:**
- **`fbt-session`** gains the detail calls spec 5.3 names but no milestone built yet: `TorrentHandle::files`, `peers` and `trackers`.
- **`fbt-api`** (new crate) is built on axum 0.8 and hyper 1, the gateway's stack, and depends only on `fbt`.
  - **Config:** `ApiConfig`, holding bind, auth, CORS, allowed roots and caps.
  - **Entry points:** `router(Session, &ApiConfig)` returns an `axum::Router` the client mounts; `ApiServer` binds, serves and shuts down.
  - **Wire types:** handlers map `fbt` types to the crate's own serde/`utoipa::ToSchema` DTOs, so no core crate gains serde-on-status or an HTTP dependency (spec anti-goal).
  - **OpenAPI:** the document is generated with utoipa, as in the gateway.
- **Streaming** wraps `FileReader` (`AsyncRead + AsyncSeek`, deadline-driven) in an HTTP body. The `Range` header is parsed by a small pure function that has its own fuzz target.

**Tech Stack:** as milestone 6, plus the following, each declared once in root `[workspace.dependencies]` and used by `fbt-api` only:

| Crate | Version | Features |
|---|---|---|
| `axum` | 0.8 | `http1`, `tokio`, `json`, `query` |
| `utoipa` | 6 | `preserve_order` |
| `utoipa-axum` | 0.3 | |
| `tower` | 0.5 | `util` |
| `tower-http` | 0.6 | `cors`, `limit` |
| `futures-util` | 0.3 | |
| `hyper` | (existing) | add `server` |
| `tokio-util` | (existing) | add `io` |

`fbt-api` also builds its `Serialize`/`Deserialize` DTOs with `serde` and `serde_json`, which are already in the workspace.

**Spec:** `docs/superpowers/specs/2026-11-10-fbt-lib-design.md` (binding). Milestone 7 is its section 8 item 7. Inputs:
- section 2 goals 3 and 4;
- section 5.1 (`fbt-api` row);
- section 5.3 (detail calls for peers, files, pieces and trackers);
- section 5.5 (the `fbt-api` streaming bullet);
- section 5.6 (the whole `fbt-api` design);
- section 7 (`missing_docs` denied in `fbt-api`);
- the "Milestone 7" lines of the milestone 4, 5 and 6 plans' deferred lists: HTTP `Range` on `FileReader`, and HTTP/JSON control of everything milestones 5 and 6 added (resume, queueing, limits, IP filter, move/rename, web seeds, creation, v2).

## Global Constraints

- **No unsafe.** `#![forbid(unsafe_code)]` in every crate; the workspace lint forbids it too.
- **Core crates stay server-free.** Only `fbt-api` depends on axum, tower-http or hyper's `server` feature. `cargo tree -p fbt -e normal` must contain none of them (checked in Task 2).
- **`pure-rust` feature.**
  - `bash dev/check-pure-rust.sh` passes after every task that touches a `Cargo.toml`.
  - Task 2 extends the script to check `-p fbt-api` too: no `ring`, `cc`, `cmake`, `aws-lc*` or `*-sys` crate.
- **Remote input never panics, hangs or allocates without bound.** API clients are untrusted until authenticated, and the `Range` parser, query strings, JSON bodies and path parameters are hostile input.
  - The clippy deny block (`unwrap_used`, `expect_used`, `indexing_slicing`, `panic` outside tests) applies to `fbt-api`.
  - Every request body has a size limit, and every collection a named cap.
- **Tests never contact the public internet.** API tests run a session from `test_settings()` and the server on `127.0.0.1:0`.
- **Cross-platform.** Builds and tests pass on macOS, Linux and Windows.
- **Dependencies only in the root `[workspace.dependencies]`;** each passes `cargo deny --all-features check`.
- **Public enums and config structs are `#[non_exhaustive]`** in `fbt-session`, `fbt` and `fbt-api`. Config structs have `Default` too.
- **`missing_docs` is denied** in `fbt` and in `fbt-api` (spec 7). Every route has a utoipa `#[utoipa::path]` and appears in `/api/v1/openapi.json`.
- **No index loops.** CI's stable clippy flags `needless_range_loop`.
- **Commits.** Conventional Commits with the crate as scope (`feat(api): ...`, `feat(session): ...`).
- **No attribution of any kind in commits or PRs.** No `Co-Authored-By` trailer, no `Claude-Session` line, no session id or link, no "Generated with" footer.
- **Crate settings.** Edition 2024, `rust-version = "1.99"`, all crates `publish = false`, Apache-2.0.
- **Logging and errors.** `tracing` for logs (no subscriber installed), `thiserror` for errors.
- **Public types.** Every new public type of `fbt-session` is re-exported from `fbt` and named in `crates/fbt/tests/lib_surface.rs`. `fbt-api`'s public surface is pinned in `crates/fbt-api/tests/lib_surface.rs`.
- **Local builds use `CARGO_INCREMENTAL=0`.** Antivirus stalls incremental rustc on the dev machine.

## Review Focus

1. **Auth bypass.**
   - Inputs:
     - no `Authorization` header;
     - a wrong token, or one of a different length;
     - `Bearer` in another case, or extra spaces;
     - `?token=` on a non-streaming route;
     - an `OPTIONS` preflight with CORS on;
     - `Auth::Disabled` with a non-loopback bind.
   - Expected:
     - Wrong or missing credentials get `401` with `WWW-Authenticate: Bearer`.
     - The scheme matches case-insensitively and the token is compared in constant time.
     - `?token=` is honoured only on streaming URLs; anywhere else it counts as no token.
     - A preflight is answered by the CORS layer without a token, and only for configured origins.
     - `Auth::Disabled` with a non-loopback bind is refused by `router()` and `ApiServer::start` (`ApiError::InsecureBind`).
   - Tests: Task 2 `missing_token_is_401`, `wrong_token_is_401`, `token_query_only_on_streams`, `disabled_auth_needs_loopback`, `cors_preflight_without_token`.
2. **`Range` edge cases.**
   - Inputs:
     - `bytes=0-`, `bytes=-500`, a suffix longer than the file, `bytes=5-2`;
     - a start at or past the end;
     - several ranges, a malformed header, a unit other than `bytes`;
     - a zero-length file, `HEAD`;
     - the client hanging up mid-body;
     - a file of a magnet without metadata;
     - a skipped file;
     - a file index out of range.
   - Expected:
     - Satisfiable single range → `206` with exact `Content-Range` and `Content-Length`.
     - Unsatisfiable → `416` with `Content-Range: bytes */len`.
     - Several, malformed or non-bytes ranges → `200` with the whole file (RFC 9110 allows ignoring `Range`).
     - Zero-length file → `200` and an empty body.
     - `HEAD` → the same headers with no body.
     - A hang-up drops the reader, and its deadlines go with it.
     - No metadata → `409`; a bad index → `404`; a skipped file still streams (it fills the part file, milestone 5).
   - Tests: Task 7 `range_parse_table` (unit) plus the fuzz target `http_range`; `serves_partial_content`, `unsatisfiable_range_is_416`, `multi_range_serves_whole_file`, `head_has_no_body`, `hang_up_releases_the_reader`, `stream_without_metadata_is_409`.
3. **Hostile request bodies and parameters.**
   - Inputs:
     - a body over `max_body_bytes`;
     - malformed JSON, unknown fields, wrong types;
     - a priority list of the wrong length;
     - an id that is not 40 or 64 hex characters;
     - a `rename` path with `..`;
     - a save, move or create path outside `allowed_roots`;
     - a torrent removed between lookup and command.
   - Expected:
     - Oversized body → `413`; malformed JSON or bad types → `400` with the JSON error body.
     - Unknown fields are refused (`deny_unknown_fields`) → `400`.
     - Wrong priority count → `400` (`invalid_priorities`); bad id → `400`; unknown id → `404`.
     - A `..` rename → `400` (`invalid_path`); a path outside `allowed_roots` → `403` (`path_not_allowed`).
     - A torrent removed mid-request → `404`, never a panic.
   - Tests: Task 3 `bad_ids`; Task 4 `body_too_large_is_413`, `malformed_json_is_400`, `wrong_priority_count_is_400`, `rename_traversal_is_400`, `save_path_outside_roots_is_403`; Task 5 `create_outside_roots_is_403`.
4. **Slow and many event subscribers.**
   - Inputs: an SSE client that reads slowly; many SSE clients at once; a client that disconnects.
   - Expected:
     - A slow client gets a `lagged` event with the missed count and goes on; the session never waits for it.
     - Beyond `max_event_streams` → `503`.
     - A disconnect drops the subscription.
     - A comment keep-alive every 15 s holds idle connections through proxies.
   - Tests: Task 6 `slow_subscriber_gets_lagged`, `event_stream_cap_is_503`, `disconnect_frees_the_slot`.
5. **Many concurrent streams.**
   - Inputs: more than `max_streams` concurrent streaming requests; a stream left open while the torrent is removed.
   - Expected:
     - Beyond the cap → `503` with `Retry-After: 1`.
     - Removing the torrent ends open streams: the reader returns an error and the body ends early, so the client sees a short read.
     - No task or reader leaks.
   - Tests: Task 7 `stream_cap_is_503`, `remove_ends_open_streams`.

---

## Starting point

`develop` holds milestone 6 (`v0.6.0`, merged back as PR #19, `3973006`). The branch `feature/m7-fbt-api` exists off `develop` and holds only this plan. Check first that the workspace version is `0.6.0` and `cargo test --workspace` is green.

What exists:
- **Session API.** `Session` (add/remove/status/subscribe/limits/queue/IP filter/DHT wrappers/state save) and `TorrentHandle` (pause, resume, remove, recheck, reannounce, scrape, priorities, add peer, pieces, limits, `open_file`/`open_file_with` → `FileReader`, move, rename, web seeds, auto-managed, queue position, `save_resume_data`), all re-exported by `fbt`.
- **Events.** `Event`/`EventMask`/`EventStream`, with `Event::Lagged { missed }`.
- **Creation.** `fbt::CreateTorrent`.
- **Missing detail calls.** There are no per-torrent peer, file or tracker detail calls. `TorrentStatus` holds the aggregates.
- **Serde.** Config structs have serde behind the `serde` feature; status and event types have none.

## Decisions recorded in this plan

The owner authorised milestones 1 to 7 to run unattended and confirmed the design, `fbt-api` included, on 2026-11-13. Spec 5.6 sets the shape. The builder made these calls:

- **Crate surface.**
  - `fbt_api::{ApiConfig, Auth, ApiError, router, ApiServer, openapi}`.
  - `router(session: Session, config: &ApiConfig) -> Result<axum::Router, ApiError>` returns a router with every route under `/api/v1` and the auth, CORS and body-limit layers already applied, so a client merges it into its own router. The public API therefore names `axum::Router`, which ties `fbt-api`'s 0.x API to axum 0.8; this is acceptable for an optional crate.
  - `ApiServer::start(session, config) -> Result<ApiServer, ApiError>` binds `config.bind`. It has `local_addr()` and `async shutdown(self)`, a graceful shutdown that ends open SSE and stream bodies.
- **Config.** `#[non_exhaustive] #[derive(Debug, Clone)] pub struct ApiConfig` with these fields and defaults:

  | Field | Default |
  |---|---|
  | `bind: SocketAddr` | `127.0.0.1:8090` |
  | `auth: Auth` | `Auth::Token(String::new())`, refused: a token must be set |
  | `cors_origins: Vec<String>` | empty = CORS off |
  | `allowed_roots: Option<Vec<PathBuf>>` | `None` = any path |
  | `max_body_bytes: usize` | `MAX_BODY_BYTES` = 32 MiB, the largest `.torrent` or resume upload |
  | `max_streams: usize` | 32 |
  | `max_event_streams: usize` | 16 |

  - `Auth` is `#[non_exhaustive] enum Auth { Token(String), Disabled }`. Its `Debug` hides the token.
  - `ApiConfig::new(token: impl Into<String>) -> Self` is the normal constructor.
  - `router` and `ApiServer::start` validate the config:
    - an empty token → `ApiError::EmptyToken`;
    - `Disabled` with a bind that is not loopback → `ApiError::InsecureBind`;
    - a CORS origin that is not a valid `HeaderValue` → `ApiError::InvalidOrigin`.

    For the mounted router, the client promises to serve it on `config.bind`; this is documented on `router`.
- **Auth.**
  - **Header.** `Authorization: Bearer <token>`: scheme case-insensitive, one space, token compared in constant time (a local XOR-fold over equal lengths; a length mismatch fails after the same loop over the expected token).
  - **Query parameter.** Streaming URLs (`GET`/`HEAD /api/v1/torrents/{id}/files/{index}/stream`) also accept `?token=` (spec 5.6). Every other route ignores the query parameter.
  - **Exempt routes.** `GET /api/v1/openapi.json` and CORS preflights need no token.
  - **Failure.** `401`, `WWW-Authenticate: Bearer`, and the JSON error body.
- **Errors.** Every error is `{"error": {"code": "<snake_case>", "message": "<text>"}}` with these statuses:
  - `400`: `invalid_request`, `invalid_id`, `invalid_priorities`, `invalid_path`;
  - `401`: `unauthorized`;
  - `403`: `path_not_allowed`;
  - `404`: `not_found`;
  - `409`: `no_metadata`, `duplicate_torrent`, `busy`;
  - `413`: `body_too_large`;
  - `416`: `range_not_satisfiable`;
  - `503`: `too_many_streams`, `shutting_down`, `dht_disabled`;
  - `500`: `internal`.

  One `impl From<SessionError> for ApiErrorResponse` in `error.rs` holds the whole mapping. Variants added later fall to `500` with their message.
- **Ids.** A torrent is named by hex in the path:
  - 40 hex is its wire hash: v1, or the truncated v2, as qBittorrent does it;
  - 64 hex is its v2 hash.

  Either case is accepted and lookups go through `Session::torrent` (which tries every wire hash). Responses carry `id` (the 40-hex wire hash), `info_hash_v1` and `info_hash_v2` (null when absent).
- **DTOs.** They live in `fbt-api` (`dto.rs`), with serde and `ToSchema`, built by `From` impls from `fbt` types:
  - hashes are lowercase hex;
  - durations are seconds as `f64`;
  - rates are bytes per second;
  - states are snake_case strings;
  - priorities are `"skip" | "low" | "normal" | "high"`.

  Request DTOs use `#[serde(deny_unknown_fields)]`. No serde is added to `fbt-session` status or event types.
- **Adding torrents.**
  - `POST /api/v1/torrents` takes JSON `{ "magnet": "...", "save_path": ..., "paused": ..., "auto_managed": ..., "sequential": ..., "file_priorities": [...] }`.
  - The same route takes `Content-Type: application/x-bittorrent` with the raw `.torrent` bytes, and the same options as query parameters.
  - `POST /api/v1/torrents/resume` takes the raw resume bytes (`application/octet-stream`).

  No base64 dependency is needed.
- **`allowed_roots`.** When set, `save_path`, `move` targets and `create` sources must canonicalise inside one of the roots. The check canonicalises the existing ancestor plus the remaining components, and refuses `..` and symlink escapes. Otherwise the request gets `403 path_not_allowed`. A token holder is otherwise fully trusted: the API is the client's own control surface, and this is documented.
- **Streaming.**
  - **Responses.**
    - A request with no `Range` header, or a header that is malformed, multi-range or not in `bytes`, gets `200` with the whole file.
    - A single satisfiable range gets `206`.
    - An unsatisfiable range gets `416` with `Content-Range: bytes */<len>`.
  - **Headers.**
    - Always `Accept-Ranges: bytes` and `Content-Length`.
    - `Content-Type` from a built-in extension table: `mp4`, `m4v`, `mkv`, `webm`, `mov`, `avi`, `mp3`, `m4a`, `flac`, `ogg`, `opus`, `wav`, `srt`, `vtt`, `txt`, `jpg`, `png`, `pdf`; else `application/octet-stream`.
    - `Content-Disposition: inline; filename*=UTF-8''<percent-encoded>`.
    - `HEAD` gets the same headers.
  - **Reading.** The body reads the `FileReader` in 64 KiB chunks through `tokio_util::io::ReaderStream`. Reads wait for missing pieces, as the reader does, and the reader's position drives deadlines (spec 5.5). Each request opens its own reader with default `ReadAhead`. Dropping the body drops the reader.
  - **Cap.** At most `max_streams` bodies at once, counted by a semaphore; beyond that, `503 too_many_streams` with `Retry-After: 1`.
- **SSE.**
  - **Route.** `GET /api/v1/events?categories=status,piece,tracker,peer,dht,storage,nat`, where the names map to `EventMask` (default all). Each `Event` becomes one SSE message, with `event:` set to the snake_case variant name and `data:` to its JSON DTO. `Lagged { missed }` becomes `event: lagged`.
  - **Keep-alive.** A comment every 15 s.
  - **Cap.** At most `max_event_streams` at once, else `503`.
  - **Lifetime.** The stream ends on session shutdown.
- **OpenAPI.** It is built with `utoipa-axum`'s `OpenApiRouter` and served at `/api/v1/openapi.json`. `fbt_api::openapi() -> utoipa::openapi::OpenApi` returns the document for clients that generate their own clients from it. No Swagger UI is bundled: the client may add one.
- **Detail calls (spec 5.3).** `TorrentHandle::files() -> Result<Vec<FileStatus>, SessionError>`, `peers() -> Result<Vec<PeerStatus>, SessionError>` and `trackers() -> Result<Vec<TrackerStatus>, SessionError>` are actor round-trips. All three structs are `#[non_exhaustive]` and `Clone`.
  - **`FileStatus`.** Its fields:
    - `index: usize`
    - `path: PathBuf` (relative, renamed paths applied)
    - `len: u64`
    - `pad: bool`
    - `priority: Priority`
    - `downloaded: u64` (verified bytes)

    A magnet without metadata gets `Err(NoMetadata)`.
  - **`PeerStatus`.** Its fields:
    - `addr: SocketAddr`
    - `client: Option<String>` (from the BEP 10 `v`, else decoded from an Azureus-style peer id)
    - `transport: Transport`
    - `encrypted: bool`
    - `incoming: bool`
    - `download_rate: u64`
    - `upload_rate: u64`
    - `total_downloaded: u64`
    - `total_uploaded: u64`
    - `peer_choking: bool`
    - `peer_interested: bool`
    - `am_choking: bool`
    - `am_interested: bool`
    - `progress: f64`
    - `source: PeerSource`

    `PeerSource` is `#[non_exhaustive]` with variants `Tracker`, `Dht`, `Pex`, `Lsd`, `Incoming`, `Manual`, `Resume`, `WebSeed`, `Holepunch`, using the actor's existing provenance where it has one, else the closest variant.
  - **`TrackerStatus`.** Its fields:
    - `url: String`
    - `tier: usize`
    - `info_hash: [u8; 20]`
    - `last_error: Option<String>`
    - `last_reply: Option<SystemTime>`
    - `next_announce: Option<Duration>` (from now)
    - `seeders: Option<u32>`
    - `leechers: Option<u32>`
    - `peers_returned: Option<u32>`

    There is one entry per (tracker, hash), as milestone 6 announces.
- **Example and interop.**
  - **Example.** `fbt-example download|seed --api <ADDR> --api-token <TOKEN>` runs `ApiServer` next to the session.
  - **Interop scenario `api`.**
    1. fbt downloads the fixture from qBittorrent with `--api 0.0.0.0:8090` (inside the internal network, token set).
    2. `curl` lists torrents.
    3. `curl` streams three ranges of a file (start, middle, last 64 KiB) while the download runs, and compares them with `cmp` against the payload.
    4. `curl` checks that `401` comes without the token and that `416` comes past the end.
    5. `curl -N` reads `/events` until a `torrent_finished` event arrives.
- **New fuzz target:** `http_range` (`fbt_api::range::parse_range` on arbitrary header bytes and lengths).
- **Version and changelog.** Version `0.7.0`; changelog entries go under `## [Unreleased]`.

---

## File Structure

```
Cargo.toml                                    workspace deps: axum, utoipa, utoipa-axum, tower, tower-http, futures-util; hyper server; tokio-util io
dev/check-pure-rust.sh                        also checks -p fbt-api
crates/fbt-session/src/status.rs              FileStatus, PeerStatus, PeerSource, TrackerStatus
crates/fbt-session/src/torrent/{mod.rs,commands.rs,announcer.rs,...}   files/peers/trackers round-trips
crates/fbt-session/tests/details.rs           new
crates/fbt/src/lib.rs, crates/fbt/tests/lib_surface.rs
crates/fbt-api/                               new crate
  Cargo.toml
  src/lib.rs                                  docs, re-exports, router(), openapi()
  src/config.rs                               ApiConfig, Auth, validation
  src/server.rs                               ApiServer
  src/auth.rs                                 bearer/query-token middleware, constant-time compare
  src/error.rs                                ApiError, ApiErrorResponse, SessionError mapping
  src/dto.rs                                  response/request DTOs (ToSchema)
  src/ids.rs                                  hex id parsing and lookup
  src/paths.rs                                allowed_roots check
  src/routes/{mod.rs,session.rs,torrents.rs,control.rs,events.rs,stream.rs,create.rs}
  src/range.rs                                parse_range (pure)
  tests/{support/mod.rs,auth.rs,read.rs,control.rs,session.rs,events.rs,stream.rs,lib_surface.rs}
crates/fbt-example/src/main.rs, Cargo.toml    --api, --api-token
dev/interop/{run.sh,README.md}                api scenario
fuzz/fuzz_targets/http_range.rs, fuzz/Cargo.toml, fuzz/seeds/http_range/
.github/workflows/{ci.yml,fuzz.yml}           fbt-api in feature checks; http_range target
CHANGELOG.md, README.md, CLAUDE.md
```

---

### Task 1: Detail calls for files, peers and trackers

**Files:**
- Modify: `crates/fbt-session/src/status.rs`, `crates/fbt-session/src/torrent/mod.rs` (handle methods + `Cmd`s), `crates/fbt-session/src/torrent/commands.rs` (answers), `crates/fbt-session/src/torrent/announcer.rs` (shared per-tracker state the actor can read), `crates/fbt-session/src/lib.rs`, `crates/fbt/src/lib.rs`, `crates/fbt/tests/lib_surface.rs`
- Test: `crates/fbt-session/tests/details.rs`

**Interfaces:**
- Produces:
  - Types: `FileStatus`, `PeerStatus`, `PeerSource` and `TrackerStatus`, as in Decisions "Detail calls".
  - `TorrentHandle::{files, peers, trackers}`, async, each returning `Result<Vec<_>, SessionError>`.
  - Everything re-exported from `fbt`.
- Tracker state: the announcer tasks publish per-(url, hash) state into an `Arc<Mutex<..>>` or `watch` the actor reads. The implementer picks one; reading it must never block the announcer for longer than a clone.

- [ ] **Step 1: Write the failing tests** in `tests/details.rs` (seed + leech on loopback, as in `tests/swarm.rs`, with a `FakeTracker` from `tests/support`):
  - `files_report_progress`:
    - Before the download, every file's `downloaded == 0`.
    - After `TorrentFinished`, every non-pad file has `downloaded == len`.
    - Paths equal the metainfo paths; after `rename_file(0, "x/renamed.bin")`, `files()[0].path` ends with `renamed.bin`.
  - `files_without_metadata`: a magnet with no peers gives `Err(SessionError::NoMetadata)`.
  - `peers_report_the_seed`:
    - The leech's `peers()` contains the seed's address while connected.
    - `source == PeerSource::Manual` for an `add_peer` peer, and `transport == Transport::Tcp`.
    - `total_downloaded > 0` after the download.
  - `trackers_report_replies`:
    - A torrent with one `FakeTracker` URL has one `TrackerStatus` with that URL, tier 0, `last_reply.is_some()` and `last_error == None` after the first reply.
    - A hybrid has two entries, one per hash.
    - A dead tracker URL has `last_error.is_some()`.
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session --test details`. Expected: FAIL (methods missing).
- [ ] **Step 3: Implement the types, the round-trips and the shared tracker state.**
- [ ] **Step 4: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session && CARGO_INCREMENTAL=0 cargo test -p fbt`. Expected: PASS.
- [ ] **Step 5: Commit.** `feat(session): file, peer and tracker detail calls`

### Task 2: `fbt-api` crate, config, auth, errors and the server

**Files:**
- Create: `crates/fbt-api/{Cargo.toml,src/lib.rs,src/config.rs,src/server.rs,src/auth.rs,src/error.rs,src/routes/mod.rs,src/routes/session.rs,src/dto.rs,tests/support/mod.rs,tests/auth.rs,tests/lib_surface.rs}`
- Modify: `Cargo.toml` (workspace deps), `dev/check-pure-rust.sh`, `.github/workflows/ci.yml` (if the feature matrix lists crates explicitly), `deny.toml` (only if a new license needs allowing, with a reason)

**Interfaces:**
- Consumes: `fbt::{Session, Settings, SessionError}`; `fbt::Session::{local_addrs, external_ip, dht_stats, cache_stats, torrents_status}`.
- Produces:
  - Config and server: `ApiConfig`, `Auth`, `ApiError { EmptyToken, InsecureBind, InvalidOrigin, Bind(std::io::Error) }`, `router`, `ApiServer::{start, local_addr, shutdown}`, `openapi`.
  - Errors: `pub(crate) struct ApiErrorResponse { status: StatusCode, code: &'static str, message: String }` with `IntoResponse`, and `From<SessionError>`.
  - DTO: `SessionInfo { version: String, listen_addrs: Vec<String>, external_ip: Option<String>, dht_nodes: Option<u64>, download_rate: u64, upload_rate: u64, torrents: usize }`.
  - The `AppState { session: Session, config: Arc<ApiConfig>, streams: Arc<Semaphore>, events: Arc<Semaphore> }` that every later route uses.
  - The macro or helper later tasks use to add a route with its `#[utoipa::path]` to the `OpenApiRouter`.
- Test support: `tests/support/mod.rs` gives:
  - `async fn api() -> (Session, ApiServer, String /*base url*/, &'static str /*token*/)`;
  - `async fn get(url, token) -> (StatusCode, HeaderMap, Bytes)`, plus `post_json` and `send`.

  These use a raw hyper client from `hyper-util`, already in the workspace (enable its `client-legacy` + `http1`). There is no reqwest.

- [ ] **Step 1: Write the failing tests** in `tests/auth.rs`:
  - `missing_token_is_401`: `GET /api/v1/session` without a header gives 401, `www-authenticate: Bearer`, and body code `unauthorized`.
  - `wrong_token_is_401`: a wrong token, and a token of a different length, both give 401.
  - `bearer_scheme_is_case_insensitive`: `bearer <token>` gives 200, with the `SessionInfo` JSON fields present.
  - `token_query_only_on_streams`: `GET /api/v1/session?token=<token>` gives 401. The streaming route accepting it is tested in Task 7.
  - `openapi_needs_no_token`: `GET /api/v1/openapi.json` gives 200 with a JSON body whose `paths` contains `/api/v1/session`.
  - `disabled_auth_needs_loopback`:
    - `router` with `Auth::Disabled` and bind `0.0.0.0:8090` gives `Err(ApiError::InsecureBind)`.
    - With `127.0.0.1:0` it is `Ok`, and requests without a token get 200.
    - `[::1]:0` is also accepted.
  - `empty_token_is_refused`: `ApiConfig::new("")` gives `Err(EmptyToken)` from `router`.
  - `cors_preflight_without_token`:
    - With `cors_origins = ["http://ui.example"]`, an `OPTIONS` preflight from that origin gets 2xx with `access-control-allow-origin: http://ui.example`.
    - A preflight from another origin gets no allow-origin header.
    - With CORS off, there is no CORS header.
  - `shutdown_stops_serving`: after `server.shutdown().await`, connecting to the address fails.
  - `core_crates_stay_server_free`: run `cargo tree -p fbt -e normal --prefix none` through `std::process::Command` with `env!("CARGO")` and assert the output contains neither `axum` nor `tower-http`. Mark it `#[ignore]` on Windows only if `cargo tree` is unavailable in CI there; it should not be.
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-api`. Expected: FAIL (crate empty).
- [ ] **Step 3: Implement as Decisions "Crate surface", "Config", "Auth", "Errors" and "OpenAPI".**
  - The auth layer is an axum middleware that reads the matched path (`MatchedPath`) to decide whether a query token is allowed.
  - CORS is `tower_http::cors::CorsLayer` built from `cors_origins`, with headers `authorization`, `content-type` and `range`, methods `GET`, `POST`, `PUT`, `DELETE` and `HEAD`, and exposed headers `content-range`, `accept-ranges` and `content-length`.
  - The body limit is `tower_http::limit::RequestBodyLimitLayer::new(max_body_bytes)`; Task 4 tests its 413.
  - `#![deny(missing_docs)]` and the clippy deny block go in `lib.rs`.
- [ ] **Step 4: Extend `dev/check-pure-rust.sh` and pin the surface.** The script also checks `-p fbt-api` with `--no-default-features --features pure-rust`; `fbt-api` forwards a `pure-rust` feature and the default `tls` to `fbt`. `tests/lib_surface.rs` pins the public names.
- [ ] **Step 5: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-api && bash dev/check-pure-rust.sh && cargo deny --all-features check`. Expected: PASS.
- [ ] **Step 6: Commit.** `feat(api): fbt-api crate with config, bearer auth, errors and server`

### Task 3: Torrent read endpoints

**Files:**
- Create: `crates/fbt-api/src/ids.rs`, `crates/fbt-api/src/routes/torrents.rs`, `crates/fbt-api/tests/read.rs`
- Modify: `crates/fbt-api/src/{dto.rs,routes/mod.rs}`

**Interfaces:**
- Consumes: Task 1's `files`, `peers` and `trackers`; Task 2's `AppState`, error mapping and route helper; `TorrentHandle::{status, pieces, save_resume_data, metainfo}`; `Metainfo::magnet()`.
- Produces:
  - Routes:

    | Route | Returns |
    |---|---|
    | `GET /api/v1/torrents` | `[TorrentInfo]` |
    | `GET /api/v1/torrents/{id}` | `TorrentInfo` |
    | `GET .../{id}/files` | `[FileInfo]` |
    | `GET .../{id}/peers` | `[PeerInfo]` |
    | `GET .../{id}/trackers` | `[TrackerInfo]` |
    | `GET .../{id}/pieces` | `{ "num_pieces": n, "have": "<hex bitfield>" }` |
    | `GET .../{id}/resume-data` | `application/octet-stream` |
    | `GET .../{id}/magnet` | `{ "magnet": "..." }` |

  - `TorrentInfo` has every `TorrentStatus` field, plus `id`, `info_hash_v1`, `info_hash_v2` and `version` (`"v1" | "v2" | "hybrid" | null`).
  - `pub(crate) fn lookup(state: &AppState, id: &str) -> Result<TorrentHandle, ApiErrorResponse>`: `400 invalid_id` on a bad id, `404 not_found` when the torrent is unknown.

- [ ] **Step 1: Write the failing tests** in `tests/read.rs`, against a session with one torrent created with `CreateTorrent` (hybrid) and seeded from its source dir:
  - `lists_torrents`: one entry whose `id` is the 40-hex wire hash, `version == "hybrid"`, `state == "seeding"` and `progress == 1.0`.
  - `get_by_v1_and_v2_ids`: lookup by the 40-hex v1 (uppercase too) and by the 64-hex v2 gives the same `id`.
  - `bad_ids`: `abc` → 400 `invalid_id`; 39 hex → 400; 40 zeros → 404 `not_found`.
  - `files_peers_trackers_shapes`:
    - `files` lists the non-pad and pad files, with `priority == "normal"` and `downloaded == len` for the non-pad ones.
    - `peers` is `[]`.
    - `trackers` is `[]` (no trackers).
  - `pieces_hex`: `num_pieces` equals the layout, and `have` is all-`f`, except for the trailing bits of a last partial byte.
  - `resume_data_round_trips`: the bytes from `resume-data` load with `fbt::AddTorrent::from_resume`.
  - `magnet_has_both_hashes`: the magnet contains `xt=urn:btih:` and `xt=urn:btmh:`.
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-api --test read`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-api`. Expected: PASS.
- [ ] **Step 5: Commit.** `feat(api): torrent list, status and detail endpoints`

### Task 4: Torrent control endpoints

**Files:**
- Create: `crates/fbt-api/src/routes/control.rs`, `crates/fbt-api/src/paths.rs`, `crates/fbt-api/tests/control.rs`
- Modify: `crates/fbt-api/src/{dto.rs,routes/mod.rs,routes/torrents.rs}`

**Interfaces:**
- Consumes: Task 3 `lookup`; `Session::add_torrent`, `AddTorrent::{new, magnet, from_resume}`, the `TorrentHandle` command methods.
- Produces:
  - `pub(crate) fn check_path(config: &ApiConfig, path: &Path) -> Result<PathBuf, ApiErrorResponse>`, the `allowed_roots` check.
  - Routes: every body DTO uses `deny_unknown_fields`, and every success is `204`, except add, which returns `201` with a `TorrentInfo`.

    | Route | Body or query |
    |---|---|
    | `POST /api/v1/torrents` | JSON `AddTorrentRequest`, or `application/x-bittorrent` body with `AddOptions` as a query string |
    | `POST /api/v1/torrents/resume` | octet-stream resume bytes, `?save_path=` optional |
    | `DELETE /api/v1/torrents/{id}` | `?delete_files=true\|false`, default false |
    | `POST .../{id}/pause`, `/resume`, `/recheck`, `/reannounce`, `/scrape` | none |
    | `PUT .../{id}/file-priorities` | `{ "priorities": [...] }` |
    | `PUT .../{id}/limits` | `{ "download": u64\|null, "upload": u64\|null }`, null = unlimited |
    | `PUT .../{id}/queue-position` | `{ "position": n }` |
    | `PUT .../{id}/auto-managed` | `{ "auto_managed": bool }` |
    | `POST .../{id}/move` | `{ "path": "...", "mode": "fail_if_exists"\|"replace_existing"\|"keep_existing" }` |
    | `POST .../{id}/rename-file` | `{ "index": n, "path": "..." }` |
    | `POST .../{id}/web-seeds` | `{ "url": ... }` |
    | `DELETE .../{id}/web-seeds` | `?url=` |
    | `POST .../{id}/peers` | `{ "addr": "ip:port" }` |

  - `AddTorrentRequest`: `magnet: String`; `save_path: PathBuf` (required); `paused`, `auto_managed` and `sequential`, all `Option<bool>`; `file_priorities: Option<Vec<Priority>>`.

- [ ] **Step 1: Write the failing tests** in `tests/control.rs`:
  - `add_magnet_then_remove`: adding a magnet of a created torrent gives 201 with state `downloading_metadata`; a second add gives 409 `duplicate_torrent`; `DELETE` gives 204, and then `GET` gives 404.
  - `add_torrent_file`: posting `application/x-bittorrent` with `?save_path=<dir>&paused=true` gives 201 and state `paused`.
  - `pause_resume_recheck`: each gives 204, and the status follows (paused, then not paused).
  - `wrong_priority_count_is_400`: code `invalid_priorities`.
  - `limits_and_queue`: `PUT limits {download: 1000, upload: null}` gives 204; `PUT queue-position {position: 0}` gives 204.
  - `move_and_rename`:
    - Moving to a new dir gives 204, then `save_path` changes.
    - `rename-file` gives 204, then `files[0].path` changes.
    - `rename_traversal_is_400`: a `../x` rename gives 400 `invalid_path`.
  - `save_path_outside_roots_is_403`: with `allowed_roots = [dir_a]`, adding with `save_path` in `dir_b` gives 403 `path_not_allowed`. A move into `dir_a/../dir_b` also gives 403, as does a symlink in `dir_a` pointing to `dir_b` (Unix).
  - `body_too_large_is_413`: with `max_body_bytes = 1024`, a 2048-byte `.torrent` post gives 413 `body_too_large`.
  - `malformed_json_is_400`: invalid JSON, an unknown field and a wrong type each give 400 `invalid_request`, with the JSON error body (axum's rejection mapped through `ApiErrorResponse`).
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-api --test control`. Expected: FAIL.
- [ ] **Step 3: Implement.** Map axum's `JsonRejection` and the body-limit `413` into `ApiErrorResponse`, so every error has the JSON shape.
- [ ] **Step 4: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-api`. Expected: PASS.
- [ ] **Step 5: Commit.** `feat(api): torrent control endpoints with allowed roots`

### Task 5: Session settings and torrent creation endpoints

**Files:**
- Create: `crates/fbt-api/src/routes/create.rs`, `crates/fbt-api/tests/session.rs`
- Modify: `crates/fbt-api/src/{dto.rs,routes/mod.rs,routes/session.rs}`

**Interfaces:**
- Consumes: `Session::{set_rate_limit, set_peer_class_limits, set_queue_settings, set_ip_filter, ip_filter, banned_peers, save_state, dht_stats, cache_stats}`; `fbt::CreateTorrent`; Task 4 `check_path`.
- Produces:
  - Routes:

    | Route | Body or response |
    |---|---|
    | `PUT /api/v1/session/limits` | `{ download, upload }` |
    | `GET`/`PUT /api/v1/session/queue` | queue settings DTO mirroring `QueueSettings`' public fields |
    | `GET`/`PUT /api/v1/session/ip-filter` | `{ "rules": [{ "first": ip, "last": ip, "action": "block"\|"allow" }] }`, at most `MAX_IP_FILTER_RULES` = 65 536 rules in a body |
    | `GET /api/v1/session/banned-peers` | the banned peers |
    | `GET /api/v1/session/state` | `application/octet-stream` (`SessionState::to_bytes`) |
    | `POST /api/v1/create-torrent` | `CreateTorrentRequest` in, `application/x-bittorrent` bytes out |

  - `CreateTorrentRequest`:
    - `path` (required);
    - `version` (`"v1" | "v2" | "hybrid"`, default hybrid);
    - `piece_length`, `trackers`, `web_seeds`, `comment` and `private`, all optional.

    The handler runs `build` in `spawn_blocking`, with at most `MAX_CONCURRENT_CREATES` = 2 at once; beyond that, `503 busy`. Errors: `CreateError::Empty`/`InvalidName`/`InvalidPieceLen` → 400; `Io` → 404 when the path is missing, else 500.

- [ ] **Step 1: Write the failing tests** in `tests/session.rs`:
  - `session_limits_apply`: after `PUT limits`, the session's global limit is set. Read it back through `GET /api/v1/session`, whose `SessionInfo` gains `download_limit` and `upload_limit`.
  - `queue_round_trip`: `PUT` then `GET` returns the same values.
  - `ip_filter_round_trip`: a block range is set; `GET` returns it; `Session::ip_filter()` blocks an address in it.
  - `ip_filter_cap`: more than `MAX_IP_FILTER_RULES` rules gives 400.
  - `create_torrent_endpoint`: a temp dir with two files gives 200, and the bytes parse with `Metainfo::from_bytes` as a hybrid.
  - `create_outside_roots_is_403`.
  - `state_bytes_load`: `GET /session/state` bytes parse with `SessionState::from_bytes`.
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-api --test session`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-api`. Expected: PASS.
- [ ] **Step 5: Commit.** `feat(api): session settings and torrent creation endpoints`

### Task 6: Server-Sent Events

**Files:**
- Create: `crates/fbt-api/src/routes/events.rs`, `crates/fbt-api/tests/events.rs`
- Modify: `crates/fbt-api/src/{dto.rs,routes/mod.rs}`

**Interfaces:**
- Consumes: `Session::subscribe(EventMask) -> EventStream`; `Event` (`#[non_exhaustive]`); Task 2 `AppState.events`.
- Produces:
  - `GET /api/v1/events?categories=...` as Decisions "SSE", built on `axum::response::sse::{Sse, Event, KeepAlive}`.
  - `EventDto`: an internally tagged serde enum (`#[serde(tag = "type", rename_all = "snake_case")]`) with one variant per `Event` variant. Torrent ids are 40-hex, and addresses and paths are strings. An `Event` variant the DTO does not know becomes `{"type": "other", "debug": "<Debug>"}`.
  - The SSE `event:` field equals `type`.

- [ ] **Step 1: Write the failing tests** in `tests/events.rs`. They read the SSE body with the raw hyper client and parse `event:`/`data:` lines.
  - `events_report_add_and_finish`: subscribe, then add a torrent through the API whose data is complete. The stream yields `torrent_added`, then `torrent_finished` or a `state_changed` to `seeding`, for that id within 10 s.
  - `categories_filter`: with `?categories=tracker`, no `torrent_added` arrives while one is added; a wait of 1 s is enough.
  - `bad_category_is_400`.
  - `slow_subscriber_gets_lagged`: open a stream and do not read it while causing more events than the session's event buffer holds (pause/resume 2000 times through `TorrentHandle`). Reading then yields an `event: lagged` with `missed > 0`, and the session's commands never block: every call returns within 5 s.
  - `event_stream_cap_is_503`: with `max_event_streams = 2`, the third concurrent stream gives 503 `too_many_streams`.
  - `disconnect_frees_the_slot`: after dropping one of the two, a new stream gives 200 within 2 s.
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-api --test events`. Expected: FAIL.
- [ ] **Step 3: Implement.** The semaphore permit moves into the stream so it is released on drop. Keep-alive is `KeepAlive::new().interval(15 s)`.
- [ ] **Step 4: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-api`. Expected: PASS.
- [ ] **Step 5: Commit.** `feat(api): server-sent event stream`

### Task 7: Byte-range streaming

**Files:**
- Create: `crates/fbt-api/src/range.rs`, `crates/fbt-api/src/routes/stream.rs`, `crates/fbt-api/tests/stream.rs`, `fuzz/fuzz_targets/http_range.rs`, `fuzz/seeds/http_range/`
- Modify: `crates/fbt-api/src/{routes/mod.rs,auth.rs,lib.rs}`, `fuzz/Cargo.toml`, `.github/workflows/fuzz.yml` (matrix, and `crates/fbt-api/**` in its PR `paths`)

**Interfaces:**
- Consumes: `TorrentHandle::open_file(index) -> FileReader` (`AsyncRead + AsyncSeek`); `Metainfo` files; Task 2 `AppState.streams`.
- Produces:
  - `pub fn parse_range(header: &[u8], len: u64) -> RangeOutcome`, `#[doc(hidden)]` but `pub` for the fuzz target.
  - `#[derive(Debug, PartialEq, Eq)] pub enum RangeOutcome { Full, Partial { start: u64, end_inclusive: u64 }, Unsatisfiable }`. The rules:
    - Only `bytes=` with exactly one range-spec: `a-b`, `a-` or `-n`.
    - Whitespace around the spec is tolerated.
    - Numbers over 20 digits → `Full`.
    - `a > b` → `Full`, since the header is invalid and so ignored.
    - `a >= len` → `Unsatisfiable`.
    - `b` is clamped to `len - 1`.
    - `-0` → `Unsatisfiable`.
    - `-n` with `n >= len` → the whole file as `Partial { 0, len-1 }`.
    - With `len == 0`, every range is `Full`.
  - Routes `GET` and `HEAD /api/v1/torrents/{id}/files/{index}/stream` as Decisions "Streaming". The auth layer accepts `?token=` here only.

- [ ] **Step 1: Write the failing tests.**
  - Unit test `range_parse_table` in `range.rs`, with `len = 1000` unless noted:

    | Header | Outcome |
    |---|---|
    | `bytes=0-` | `Partial{0,999}` |
    | `bytes=0-0` | `Partial{0,0}` |
    | `bytes=100-199` | `Partial{100,199}` |
    | `bytes=900-5000` | `Partial{900,999}` |
    | `bytes=-500` | `Partial{500,999}` |
    | `bytes=-5000` | `Partial{0,999}` |
    | `bytes=1000-` | `Unsatisfiable` |
    | `bytes=-0` | `Unsatisfiable` |
    | `bytes=5-2` | `Full` |
    | `bytes=0-1,5-6` | `Full` |
    | `items=0-1` | `Full` |
    | `bytes=` | `Full` |
    | `bytes=abc` | `Full` |
    | `bytes=99999999999999999999999-` | `Full` |
    | ` bytes = 1-2 ` | `Full` (the unit itself must be exact) |
    | `bytes= 1-2` | `Partial{1,2}` |
    | `len = 0`, `bytes=0-` | `Full` |

  - In `tests/stream.rs`, against a session seeding a created v1 torrent of three files (one empty, one `video.mp4` of 300 000 bytes, one 70 000-byte `data.bin`):
    - `serves_whole_file`: `GET` with no `Range` gives 200, `content-length: 300000`, `accept-ranges: bytes`, `content-type: video/mp4`, and a body equal to the file.
    - `serves_partial_content`: `Range: bytes=1000-1999` gives 206, `content-range: bytes 1000-1999/300000`, `content-length: 1000` and the exact bytes. `bytes=-500` gives the last 500 bytes.
    - `unsatisfiable_range_is_416`: `bytes=300000-` gives 416 with `content-range: bytes */300000`.
    - `multi_range_serves_whole_file`: 200 with the whole body.
    - `head_has_no_body`: `HEAD` gives the same status and headers, with an empty body.
    - `empty_file_streams`: 200, `content-length: 0`.
    - `token_in_query_is_accepted`: `?token=<token>` with no header gives 200 (the counterpart of Task 2's test).
    - `bad_index_is_404`, and `stream_without_metadata_is_409` (a magnet with no peers).
    - `streams_while_downloading`: a leech session with the API downloads from a rate-limited seed (`upload` limit 200 KiB/s). A range at the end of `video.mp4` gives 206 with correct bytes before the torrent finishes: assert `status().progress < 1.0` at request time and correct bytes at the end. This proves the deadline-driven reader behind HTTP.
    - `hang_up_releases_the_reader`: start a `GET` of a file that cannot complete (a leech whose only seed is paused), read the headers, then drop the response. Within 2 s, `TorrentHandle::debug_deadlines()` is empty.
    - `stream_cap_is_503`: with `max_streams = 1`, a second concurrent stream gives 503 with `retry-after: 1`.
    - `remove_ends_open_streams`: during a stalled stream, `DELETE` the torrent; the body then ends with fewer bytes than `content-length` within 5 s.
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-api range && CARGO_INCREMENTAL=0 cargo test -p fbt-api --test stream`. Expected: FAIL.
- [ ] **Step 3: Implement `range.rs` and the routes.** The body is `Body::from_stream(ReaderStream::with_capacity(reader.take(len), 64 * 1024))` after `seek(start)`, and the semaphore permit is held inside the stream.
- [ ] **Step 4: Add the fuzz target `http_range`.** The first 8 bytes are `len` as `u64` big-endian and the rest is the header. It asserts `Partial` always satisfies `start <= end < len`. Seeds are the table's headers. Add it to `fuzz/Cargo.toml` and to the `fuzz.yml` matrix and paths.
- [ ] **Step 5: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-api`. Expected: PASS. Run `cargo +nightly fuzz build http_range` if nightly is installed.
- [ ] **Step 6: Commit.** `feat(api): byte-range streaming of torrent files`

### Task 8: `fbt-example --api` and the `api` interop scenario

**Files:**
- Modify: `crates/fbt-example/{Cargo.toml,src/main.rs}`, `crates/fbt-example/tests/cli.rs`, `dev/interop/{run.sh,README.md}`

**Interfaces:**
- Consumes: Task 2 `ApiServer`, `ApiConfig::new`.
- Produces:
  - `download` and `seed` take `--api <ADDR>` and `--api-token <TOKEN>`; `--api` without `--api-token` is a clap error.
  - With both, `ApiServer::start` runs next to the session, and `api: listening on <addr>` is printed.
  - The interop scenario `api` is as in Decisions "Example and interop" and is added to `known_scenarios`.

- [ ] **Step 1: Write the CLI test.** In `tests/cli.rs`, `api_flag_serves_the_api`: `seed` a created torrent with `--api 127.0.0.1:0 --api-token t`, parse the printed address, and check that `GET /api/v1/torrents` with the token returns one torrent (use `std::net::TcpStream` and a hand-written HTTP/1.1 request, so the example's tests add no client dependency). Also check that `--api` without `--api-token` exits non-zero with a usage error.
- [ ] **Step 2: Implement the flags and the scenario.**
- [ ] **Step 3: Run the scenario locally.** Run `INTEROP_ONLY=api bash dev/interop/run.sh`. Expected: `api OK`. If an image pull hangs over the VPN for more than ~10 minutes, stop and rely on CI.
- [ ] **Step 4: Commit.** `feat(example): serve fbt-api; test(interop): api scenario`

### Task 9: Docs and release notes

**Files:**
- Modify: `CHANGELOG.md`, `README.md`, `CLAUDE.md` (crate list, interop scenarios, fuzz targets), `crates/fbt-api/src/lib.rs` (crate docs: mounting example as a doctest, the standalone server example, the security model: token, loopback default, `allowed_roots`, CORS)

- [ ] **Step 1: Write the `CHANGELOG.md` `## [Unreleased]` entries.**
  - `### Added`: `fbt-api` (every route group), the detail calls, `fbt-example --api`, the `http_range` fuzz target and the `api` interop scenario.
  - `### Changed`: anything breaking (none expected beyond new `#[non_exhaustive]` types).
- [ ] **Step 2: Check the docs build.** Run `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features && CARGO_INCREMENTAL=0 cargo test --workspace --doc`. Expected: PASS.
- [ ] **Step 3: Commit.** `docs: milestone 7 changelog, README and fbt-api guide`

---

## Finishing

Before the PR, run the full gate:

```
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo deny --all-features check
bash dev/check-pure-rust.sh
```

Then open PR `feature/m7-fbt-api` → `develop`.
- **CI.** Every job must be green, including `interop` and the `http_range` fuzz run.
- **PR body.** It lists the decisions made on the owner's behalf and a "Deferred" section; the CHANGELOG entries link the PR number.

Then cut the release (Gitflow):
1. Branch `release/0.7.0` off `develop`.
   - Bump the workspace version to `0.7.0` and run `cargo update -w`.
   - Move `Unreleased` to `## [0.7.0] - <date>` and add the link line.
   - Run `bash dev/test-release-version.sh`, then run `bash dev/bench.sh` on the release commit and keep its row.
2. Open a PR into `main`, then push the annotated tag `v0.7.0`, then merge `main` back into `develop`.

No attribution lines anywhere.

## Deferred (recorded so nobody builds them now)

- A WebSocket endpoint (spec 5.6: "may be added if a client needs bidirectional push").
- Swagger UI, which the client can add.
- HTTP/2 and TLS on `ApiServer`: a client needing TLS terminates it in front, or mounts the router in its own TLS server.
- Multi-range (`multipart/byteranges`) responses and `If-Range`/`ETag`.
- Per-user tokens and scopes: the token is one shared secret.
- DHT item and sample endpoints: the session's DHT wrappers are not exposed until a client asks.
- The milestone 6 backlog, unchanged.
