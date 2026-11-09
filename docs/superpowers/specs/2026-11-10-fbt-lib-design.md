# fbt-lib: design

Status: written 2026-11-10 from the design conversation with the owner.

Decisions are tagged **[owner]** when the owner made them in the design conversation and
**[builder's call]** when they were made while writing this document; builder's calls may be
revisited by whoever plans a milestone, owner decisions may not without the owner.

## 1. Intent

`fbt-lib` is a pure-Rust BitTorrent library with the feature breadth of libtorrent
(rasterbar). It is the engine for **featherbittorrent** (`../featherbittorrent`), a future
client that will have:

- a ratatui TUI in the workspace "Graphite" design language with a full management UI and a
  minimal mode that only streams progress of downloading and seeding torrents; **[owner]**
- an optional API for a Graphite web UI, later reused for Tauri cross-platform desktop
  builds; **[owner]**
- streaming of media while it downloads (played by a browser `<video>` element, or by an
  external player such as mpv/VLC launched from the TUI). **[owner]**

The library has no UI. It must serve a TUI that redraws many times a second and a remote web
UI equally well, on macOS, Linux and Windows.

It is a reimplementation of libtorrent's *capabilities*, not of its C++ API: the public API is
idiomatic async Rust. **[builder's call]**

## 2. Goals

1. Interoperates with the real swarm: downloads from and seeds to libtorrent-based clients,
   Transmission and others, over TCP and uTP, encrypted or plain, v1, v2 and hybrid torrents.
2. Must-haves **[owner]**: DHT, magnet links, MSE/PE encryption.
3. Streaming while downloading **[owner]**: a seekable reader over any file of a torrent,
   deadline-based piece picking that chases the read position, and HTTP byte-range serving in
   `fbt-api`.
4. Optional `fbt-api` crate in this workspace **[owner]**: HTTP/JSON control API, a live event
   stream, byte-range streaming endpoints. The core crates never depend on it.
5. Safe against hostile peers, trackers and DHT nodes: no panics, unbounded allocations or
   path escapes from network input.
6. Same engineering standards as featherbit gateway and terminal (section 9) **[owner]**.

## 3. Non-goals and anti-goals

Non-goals (not built, may be reconsidered later):

- I2P, SSL torrents, share mode, super-seeding (BEP 16), BEP 17 (Hoffman HTTP seeding), BEP 38,
  BEP 46 (mutable torrents) **[builder's call, proposed to the owner without objection]**.
- Transcoding, subtitle handling or any media awareness: streaming serves bytes as they are.
- libtorrent C++ API/ABI compatibility, language bindings, reading libtorrent's or other
  clients' resume files.
- A daemon process, a CLI client, a docs website: they belong to featherbittorrent (the
  workspace ships one small example CLI for testing only).

Anti-goals (would make the work a failure even if it "works"):

- Any panic, hang or unbounded memory growth triggered by remote input.
- A torrent's file names writing outside its save path (path traversal, absolute paths, drive
  letters, Windows reserved names, symlinks pointing out).
- A core crate that pulls in an HTTP server stack, a UI toolkit or a specific client's
  concepts.
- A C toolchain requirement for the default build beyond what `ring` needs (section 6).
- Hammering trackers or the DHT (announce intervals, backoff and rate limits honored).

## 4. Feature scope

Implemented across milestones (section 8). BEP numbers for reference.

| Area | In scope |
|---|---|
| Core protocol | BEP 3, BEP 20 peer ids, BEP 6 fast extension, BEP 10 extension protocol, BEP 21 upload-only, BEP 54 `lt_donthave`, BEP 40 canonical peer priority |
| Metadata | `.torrent` v1, v2 and hybrid (BEP 52), BEP 47 pad files and attributes, magnet URIs incl. v2 `btmh` and BEP 53 `so=`, BEP 9 metadata exchange |
| Peers | BEP 11 PEX, BEP 14 local service discovery, BEP 55 holepunch, IPv4 and IPv6 everywhere (BEP 7) |
| Trackers | HTTP(S), UDP (BEP 15), multi-tracker tiers (BEP 12), compact responses (BEP 23), external IP (BEP 24), scrape (BEP 48), private torrents (BEP 27: no DHT/PEX/LSD) |
| DHT | BEP 5, IPv6 (BEP 32), security extension (BEP 42), read-only nodes (BEP 43), arbitrary data put/get (BEP 44), scrape (BEP 33), `sample_infohashes` (BEP 51); routing table persisted |
| Encryption | MSE/PE (RC4 + 768-bit DH), policy forced / preferred / disabled, plaintext fallback per policy |
| Transport | TCP, uTP (BEP 29) with LEDBAT congestion control, mixed-mode TCP/uTP bandwidth balancing |
| Web seeds | BEP 19 (GetRight style), over HTTP(S) |
| NAT | UPnP IGD and NAT-PMP/PCP port mapping |
| Proxies | SOCKS5 (incl. UDP associate for uTP/DHT/UDP trackers), HTTP CONNECT; proxy hostnames for trackers |
| Piece picking | rarest-first, sequential, per-file and per-piece priorities, end-game mode, time-critical deadlines for streaming |
| Peer management | choking algorithm (tit-for-tat with optimistic unchoke, seeding variants), connection limits, smart ban of peers sending bad data, IP filter |
| Rate limiting | global, per-torrent and per-peer-class upload/download limits |
| Storage | sparse files, part file for unwanted pieces, piece verification, write-back cache with size limit, move storage, rename files, force recheck |
| Session | queueing (active download/seed/total limits), seed ratio/time limits, resume data, session state save/restore, torrent creation (v1, v2, hybrid) |
| Streaming | seekable async reader per file, read-ahead window, deadline picker |

## 5. Architecture

### 5.1 Workspace

A Cargo workspace like the terminal's `crates/` layout. Crate names and boundaries are a
**[builder's call]** except `fbt-api` being an optional crate here **[owner]**.

| Crate | Responsibility | Depends on |
|---|---|---|
| `fbt-bencode` | bencode encode/decode, zero-copy decode, depth and size limits, serde support | none |
| `fbt-core` | shared types: info hashes (v1 SHA-1, v2 SHA-256), peer id, bitfield, piece/block math, Merkle trees | `fbt-bencode` |
| `fbt-metainfo` | `.torrent` parsing/creation (v1/v2/hybrid), magnet URIs, file-path sanitization | `fbt-core` |
| `fbt-wire` | peer wire protocol, handshake, MSE/PE, extension protocol messages (ut_metadata, ut_pex, holepunch, donthave) | `fbt-core` |
| `fbt-utp` | uTP sockets over a shared UDP socket, LEDBAT | none (tokio) |
| `fbt-dht` | Kademlia DHT node(s), BEP 42/43/44/33/51, routing table persistence | `fbt-core`, `fbt-bencode` |
| `fbt-tracker` | HTTP(S) and UDP tracker clients, scrape | `fbt-core` |
| `fbt-net` | UPnP, NAT-PMP/PCP, LSD, proxies, IP filter, shared UDP socket demultiplexing (uTP, DHT, UDP trackers on one port) | |
| `fbt-session` | session, torrents, peer connections, picker, choker, storage, rate limiting, queueing, resume, streaming reader | all of the above |
| `fbt` | facade: the stable public API, re-exporting what clients need; feature flags | `fbt-session` |
| `fbt-api` | optional: HTTP/JSON control API, SSE event stream, byte-range streaming | `fbt` |
| `fbt-example` | small CLI (`publish = false`) for manual and interop testing | `fbt` |

Protocol crates are usable on their own (someone can take just `fbt-dht`). Clients depend on
`fbt` (and optionally `fbt-api`), never on internals; the internals' APIs may change in any
release, `fbt`'s follows SemVer.

### 5.2 Runtime and concurrency

- tokio, multi-threaded runtime supplied by the caller (the library spawns onto the current
  runtime; it never creates its own). **[builder's call]**
- One shared UDP socket per listen address carries uTP, DHT and UDP-tracker traffic,
  demultiplexed by packet shape, as libtorrent does, so a single port works through NAT.
- Disk I/O runs on a bounded blocking pool (positional reads/writes), never on async worker
  threads. No mmap (SIGBUS on truncated files, poor Windows behavior). **[builder's call]**
- Hashing (SHA-1/SHA-256 of whole pieces) runs on the same or a separate bounded pool so large
  rechecks cannot starve network tasks.
- Internal structure (actor per torrent, per peer, or other) is left to the builder, with the
  constraint that status snapshots stay cheap (5.3) and 1,000 idle seeding torrents cost
  little CPU (section 7).

### 5.3 Public API shape **[builder's call]**

- `Session::new(Settings).await` returns a cloneable `Session` handle; dropping the last handle
  or calling `shutdown().await` stops it gracefully (announces `stopped`, flushes cache, saves
  resume data if configured).
- Commands are async methods: add torrent (file, bytes, magnet, resume data), remove (with or
  without data), pause/resume, set file/piece priorities, set limits, move storage, force
  recheck/reannounce, add peer, apply new settings at runtime.
- `Torrent` handles identify torrents by info hash (v1, v2 or both).
- Events: `session.subscribe()` returns a stream of a typed, `#[non_exhaustive]` `Event` enum
  (added, metadata received, state changed, piece finished, torrent finished, tracker reply or
  error, peer connected/disconnected, storage moved, file error, performance warning, ...),
  filterable by category like libtorrent's alert mask. Slow subscribers lag rather than block
  the session and are told how many events they missed.
- Status: `torrent.status()` and `session.torrents_status()` return plain-data snapshots
  (progress, rates, peers, seeds, ETA inputs, state, errors), cheap enough to call every TUI
  frame; detail calls for peers, files, pieces, trackers.
- Streaming: `torrent.open_file(index)` returns a reader implementing `AsyncRead + AsyncSeek`;
  reads wait for missing pieces instead of failing, and the reader's position drives the
  deadline picker (section 5.5).
- All public enums and config structs are `#[non_exhaustive]`; config structs have `Default`
  and serde support so clients can persist them.

### 5.4 Storage

- Files are created sparse where the OS supports it; unwanted pieces that straddle wanted
  files go to a per-torrent part file so skipped files are never created.
- Every downloaded piece is hash-checked (v1 piece hash, v2 Merkle leaf/piece layers) before
  being marked complete; failed pieces feed the smart-ban logic.
- Storage is behind a trait so tests can use in-memory storage and clients could plug in
  their own; the default is the filesystem.
- Path sanitization in `fbt-metainfo` is a single, heavily tested function: rejects or
  rewrites `..`, absolute paths, drive letters, NUL, Windows reserved names and characters,
  over-long components; BEP 47 symlink attributes may only point inside the torrent.

### 5.5 Streaming

- The reader converts its position into a byte range of pieces and sets deadlines on the next
  N pieces (read-ahead window, configurable in bytes and as a time budget at a given bitrate,
  default left to the builder). Seeking cancels old deadlines and sets new ones; the rest of
  the torrent continues with normal priority.
- Pieces with deadlines are requested from the fastest peers first and may be requested from
  several peers at once near the deadline (libtorrent's time-critical behavior).
- `fbt-api` exposes each file at a URL that supports `GET` with `Range` (206 partial
  content, correct `Content-Length`, `Accept-Ranges`), so browsers, Tauri webviews, mpv and
  VLC can play and seek.

### 5.6 fbt-api **[owner: optional crate here; details builder's call]**

- axum on hyper (same stack as the gateway), behind its own crate so `fbt` stays server-free.
- JSON control API mirroring the `fbt` commands and status calls, with an OpenAPI document
  generated with utoipa (as the gateway does).
- Event stream as Server-Sent Events (browser `EventSource`, works through proxies); a
  WebSocket endpoint may be added if a client needs bidirectional push.
- Binds to localhost by default; a bearer token is required for every route except when the
  embedding client explicitly disables auth for a localhost-only bind. Streaming URLs accept
  the token as a query parameter too, so they can be handed to an external player. CORS is
  off unless configured.
- The crate exposes a router the client mounts or a ready server; the client decides ports.

## 6. Dependencies and "pure Rust"

- **Every BitTorrent protocol component is written from scratch in this workspace [owner]**:
  bencode, metainfo, wire protocol, MSE/PE, uTP/LEDBAT, DHT, trackers, PEX, LSD, UPnP and
  NAT-PMP/PCP, picker, choker, storage. No dependency on existing BitTorrent crates (librqbit,
  `mainline`, uTP crates, bencode crates, ...). Only general-purpose crates are allowed (tokio,
  RustCrypto, rustls, hyper/axum, serde, tracing, thiserror, an XML parser, a bignum crate and
  the like). libtorrent's BSD-3 source may serve as a behavioral reference; wherever specific
  logic is ported from it, the file says so and `NOTICE` carries libtorrent's copyright and
  license. **[builder's call for the porting rule]**
- Every crate is Rust. Cryptography uses RustCrypto crates: `sha1`, `sha2`, `rc4`, a bignum
  crate for the MSE Diffie-Hellman exchange; no `asm` features that need a C compiler.
- TLS (HTTPS trackers and web seeds only) uses rustls. Its crypto provider is selected by
  Cargo feature **[owner]**: `ring` is the default; a `pure-rust` feature switches to
  `rustls-rustcrypto` for builds that must not contain C or assembly. HTTPS support as a whole
  sits behind a default-on `tls` feature so a build without it contains no TLS code.
- `#![forbid(unsafe_code)]` in every crate; an exception needs a comment explaining why and
  a review. **[builder's call]**
- Errors with `thiserror`, logging with `tracing` (no subscriber installed by the library),
  both like the gateway.
- Rust edition 2024 (like the terminal), stable toolchain; MSRV declared in `Cargo.toml` and
  tested in CI, set to the stable release current when milestone 1 starts. **[builder's call]**

## 7. Quality targets **[builder's call]**

- **Interop**: a Docker-based live job (like the terminal's `ssh live tests`) with
  qBittorrent-nox (libtorrent), Transmission and opentracker: we download from and seed to
  each, over TCP and uTP, with encryption forced and disabled, v1/v2/hybrid; a magnet
  download with trackers removed proves DHT and metadata exchange.
- **Fuzzing**: `cargo-fuzz` targets for every parser of remote input (bencode, metainfo,
  magnet, wire messages, MSE handshake, extension messages, DHT messages, tracker responses,
  uTP headers); a short run on every PR touching them, a long run on the weekly schedule.
  Property tests (`proptest`) for bencode round-trips, bitfields, piece math, path
  sanitization.
- **Simulation**: in-process swarms over loopback (or an in-memory network) for picker,
  choker, end-game and streaming deadline tests, deterministic where possible.
- **Performance** (release build, measured by a benchmark script under `dev/`): a single
  torrent saturates 1 Gbit/s on loopback with a few percent of one core per 100 Mbit/s;
  1,000 idle seeding torrents stay under 1% CPU; memory per idle torrent bounded and
  documented. Numbers are recorded per release, not CI-gated.
- **Docs**: every public item of `fbt` and `fbt-api` documented (`missing_docs` denied there);
  runnable examples in `fbt-example`.

## 8. Milestones **[owner: order approved]**

Each milestone gets its own implementation plan under `docs/superpowers/plans/` and ships as
a minor version (`0.N.0`). "Done" means its live interop tests pass in CI.

1. **Core download, `v0.1.0`.** Workspace, CI and security scaffolding; bencode, v1
   metainfo, HTTP and UDP trackers, TCP wire protocol with fast extension, piece picking
   (rarest-first, priorities, end-game), storage with verification, minimal session API
   (add/pause/remove, status, events), `fbt-example` downloads a real torrent from real peers.
2. **Must-haves.** DHT (BEP 5/32/42/43, persistence), magnet links with metadata exchange,
   MSE/PE, PEX.
3. **Performance and reach.** uTP with LEDBAT, shared UDP socket, UPnP and NAT-PMP/PCP, LSD,
   holepunch, rate limits, choker tuning, write-back cache.
4. **Streaming.** Seekable reader, deadline picker, read-ahead.
5. **Persistence and operations.** Resume data and session state, queueing and seed limits,
   move storage and rename, proxies, IP filter, web seeds, smart ban, BEP 44/33/51.
6. **BitTorrent v2 and hybrid,** Merkle verification, torrent creation (v1/v2/hybrid).
7. **`fbt-api`.** Control API with OpenAPI, SSE events, byte-range streaming, auth.

A `1.0.0` is cut when the owner decides the `fbt` API is stable; until then minor versions
may break the API (SemVer 0.x rules), noted in the changelog.

## 9. Engineering standards **[owner: same as gateway and terminal]**

- **Repo**: `github.com/featherbitplatform/fbt-lib`, Apache-2.0 (same as featherbittorrent,
  gateway, terminal). Its own `CLAUDE.md` written with the scaffold.
- **Gitflow**: `main` tagged releases only, `develop` integration, `feature/*`,
  `release/X.Y.Z`, `hotfix/*`; releases as in the terminal (bump on the release branch,
  merge to `main`, annotated `vX.Y.Z` tag, merge back to `develop`).
- **Conventional Commits** with optional scope (`feat(dht): ...`), **SemVer**,
  `CHANGELOG.md` in Keep a Changelog format with PR links.
- **No attribution of any kind** in commits or PRs: no `Co-Authored-By` trailer, no session
  links or ids, no "Generated with" footer **[owner]**.
- **CI (`ci.yml`)**: `cargo fmt --check`, `cargo clippy --workspace --all-targets
  --all-features -- -D warnings`, `cargo test --workspace` on macOS, Linux and Windows; a
  feature-matrix check (`--no-default-features`, `pure-rust`); MSRV build; docs build with
  warnings denied; the live interop job.
- **Security (`security.yml`)**, copied from the terminal's **[owner: security, static
  analysis and vulnerability jobs like the other projects]**: semgrep (`p/default`, `p/rust`,
  ERROR blocks, SARIF), cargo-deny (advisories, licenses, bans, sources via `deny.toml`), grype
  over the filesystem (high/critical fail, SARIF), gitleaks over the full history; weekly
  re-scan of `main`. npm audit is omitted until a JS package exists. Configs at the repo root
  (`deny.toml`, `.semgrepignore`, `.gitleaks.toml`, `.grype.yaml`), every ignore with a
  one-line reason, and `dev/sast.sh` / `dev/sast.ps1` running the same scans locally via
  Docker.
- **Fuzz (`fuzz.yml`)**: short runs on PRs touching parsers, long runs weekly. **[builder's
  call]**
- **Releases (`release.yml`)**: a `v*` tag creates the GitHub release with the changelog
  section and a CycloneDX SBOM (syft, pinned) of the workspace. Publishing to crates.io is
  not decided; until the owner decides, crates have `publish = false` and featherbittorrent
  depends on `fbt-lib` as a git dependency pinned by tag or `rev`, the same way terminal uses
  feathercritty. **[builder's call]**
- Naming other BitTorrent clients and libraries (libtorrent, qBittorrent, Transmission) is
  allowed in this repo, since interop with them is a goal and the library is described as a
  libtorrent reimplementation. **[builder's call]**

## 10. Left to the builder

- Internal task/actor structure, data structures of the picker and choker, cache policy.
- Exact default settings (connection limits, read-ahead size, cache size, announce
  behavior), taking libtorrent's defaults as the starting point.
- Resume-data format (bencoded, versioned, ours).
- Choice of general-purpose crates (bignum, XML, HTTP client, ...) within section 6.
- Making milestone-1 types and storage ready for v2 hashes so milestone 6 is not a rewrite.
- The contents of each milestone's plan, including test fixtures and the interop harness.
