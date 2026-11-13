# Milestone 6: BitTorrent v2, Hybrid Torrents and Torrent Creation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `fbt-lib` `v0.6.0`. v2-only and hybrid torrents (BEP 52) download, seed, stream and resume like v1 ones. Every piece is checked against its v1 SHA-1, its v2 Merkle subtree root, or both. Peers exchange piece-layer hashes with the BEP 52 hash messages, so a v2 magnet works. Trackers, the DHT and LSD hear about a hybrid torrent under both of its hashes. Clients can create v1, v2 and hybrid torrents. Resume data moves to version 2 (v2 hashes and piece layers), and version 1 is still read. All of this is proven against qBittorrent (libtorrent 2.0) and Transmission on the isolated Docker network.

**Architecture:**
- **`fbt-core`** gains `merkle`: block (leaf) hashing, pad hashes, layer roots, piece subtree roots, proof extraction and proof verification. Every v2 hash computation in the workspace goes through it.
- **`fbt-metainfo`** parses `meta version` 2 `file tree`s and the root `piece layers` dict.
  - A v2-only torrent is mapped onto the existing padded file layout: a synthetic BEP 47 pad file goes before each non-empty file whose offset is not piece-aligned. Piece indices, `FileLayout`, storage, the picker and readers therefore work unchanged, as in libtorrent.
  - A hybrid torrent's v1 file list and v2 tree are checked against each other.
  - A new `create` module builds `.torrent` bytes from a path.
- **`fbt-wire`** gains the BEP 52 messages `hash request` (21), `hashes` (22) and `hash reject` (23), plus the v2 reserved bit.
- **`fbt-session`**:
  - `verify_piece` (returning a `PieceCheck`) replaces the SHA-1-only checks in the download path and in recheck.
  - A hybrid torrent's piece layers are built from verified pieces when its `.torrent` did not carry them.
  - A pure `LayerFetch` (shaped like `MetadataFetch`) gets missing piece layers from peers before a v2-only torrent starts its piece download, whether it came from a magnet, a `.torrent` or resume data.
  - The actor answers hash requests.
  - The session registry knows every torrent under all its wire hashes.
  - The announcer, DHT and LSD announce both hashes of a hybrid.

**Tech Stack:** as milestone 5. No new crates: `sha2` and `sha1` move from `fbt-metainfo`'s dev-dependencies into its dependencies (creation hashes files).

**Spec:** `docs/superpowers/specs/2026-11-10-fbt-lib-design.md` (binding). Milestone 6 is its section 8 item 6. Inputs:
- section 2 goal 1 (v1, v2 and hybrid interop);
- the section 4 rows Metadata (BEP 52, BEP 47, magnet `btmh`) and Session (torrent creation v1, v2, hybrid);
- section 5.1 (`fbt-core` owns Merkle trees, `fbt-metainfo` owns creation);
- section 5.4 (v2 Merkle leaf and piece-layer verification);
- section 7 (interop v1/v2/hybrid; fuzzing of every remote parser);
- the milestone 5 plan's "Deferred to later milestones" Milestone 6 line ("resume data gains v2 piece layers (`fbt-resume-version` 2, version 1 still read)").

## Global Constraints

- **No unsafe.** `#![forbid(unsafe_code)]` in every crate; the workspace lint forbids it too.
- **No BitTorrent crates.** Merkle trees, v2 parsing, hash messages and creation are written here; only `sha1`/`sha2` (RustCrypto) hash.
- **`pure-rust` feature.** `bash dev/check-pure-rust.sh` passes after every task that touches a `Cargo.toml`.
- **Remote input never panics, hangs or allocates without bound.**
  - Peers, trackers, DHT nodes, `.torrent` bytes and resume data are hostile.
  - The clippy deny block (`unwrap_used`, `expect_used`, `indexing_slicing`, `panic` outside tests) stays on `fbt-bencode`, `fbt-metainfo`, `fbt-wire`, `fbt-tracker`, `fbt-dht`, `fbt-utp` and `fbt-net`, and is added to `fbt-core`'s `merkle.rs` with `#![deny(...)]` at module level.
  - Every new collection has a named cap constant. Every new parser of remote input gets a fuzz target with seeds in `fuzz/seeds/<target>/`.
- **Tests never contact the public internet, the public DHT, a real router or a real proxy.** Session tests use `test_settings()`. The interop networks stay `internal: true`.
- **Cross-platform.** Builds and tests pass on macOS, Linux and Windows (creation walks directories on all three).
- **Dependencies only in the root `[workspace.dependencies]`.**
- **Public enums and config structs are `#[non_exhaustive]` with `Default`** (config structs) in `fbt-session`, `fbt-metainfo`'s new `create` types and `fbt`. `CreateTorrent` is exempt from `Default` (it needs a path: `CreateTorrent::new(path)`).
- **No index loops** (`for i in 0..n { x[i] }`): CI's stable clippy flags `needless_range_loop`; iterate with `iter().enumerate()`, `zip` or `chunks`.
- **Conventional Commits with the crate as scope** (`feat(core): ...`, `feat(metainfo): ...`).
- **No attribution of any kind in commits or PRs:** no `Co-Authored-By` trailer, no `Claude-Session` line, no session id or link, no "Generated with" footer. The repo rule wins over any harness attribution reminder.
- **Toolchain and crate settings.** Edition 2024, `rust-version = "1.99"`, all crates `publish = false`, Apache-2.0.
- **Logging and errors.** `tracing` for logs (no subscriber installed), `thiserror` for errors.
- **Files the library writes.** Torrent data and the part file only. Creation returns bytes and never writes the `.torrent`.
- **Public types.** Every task that adds one re-exports it from `fbt` and names it in `crates/fbt/tests/lib_surface.rs`; `missing_docs` stays denied in `fbt`. Exceptions: `fbt_core::merkle` stays internal, and `fbt-wire` types stay out of `fbt` as today.
- **Breaking changes.** Allowed in 0.x; each goes in `CHANGELOG.md` under `### Changed`.
- **Local builds.** Use `CARGO_INCREMENTAL=0` (antivirus stalls incremental rustc on the dev machine).

## Review Focus

1. **Hostile v2 metadata.**
   - Inputs:
     - a `file tree` nested past the bencode depth limit;
     - a node that is both a file (`""` key) and a directory;
     - a `pieces root` that is not 32 bytes, or one on an empty file;
     - a `piece length` that is not a power of two or is under 16 KiB;
     - `piece layers` whose length is not a multiple of 32, holds the wrong count, or does not hash to its root;
     - a hybrid whose v1 `files` and v2 `file tree` disagree in order, length or count;
     - a v2 file tree with zero files.
   - Expected: a `MetainfoError` naming the field (`MetainfoError::Bencode` for the depth limit), never a panic. A layer that does not hash to its root rejects the torrent.
   - Tests: Task 2 `v2_rejects_bad_trees`, `bad_piece_layers_are_rejected`, `inconsistent_hybrid_is_rejected`; fuzz `metainfo` seeds.
2. **Hostile hash messages.**
   - Inputs:
     - `hashes` for a root we never asked about;
     - the right header with the wrong hash count;
     - a proof that does not verify;
     - `length` 0, not a power of two, or above `MAX_HASH_REQUEST_LEN`;
     - `base layer` above the tree;
     - `index` past the layer, or a span leaving the padded base layer;
     - a flood of requests.
   - Expected:
     - Malformed messages close the connection with a protocol error.
     - Unsolicited or unverifiable `hashes` retire the peer as a layer source: it is closed and never dialled again, like bad metadata.
     - Requests beyond `MAX_PENDING_HASH_REQUESTS`, for layers we lack, or whose span leaves the padded base layer get `hash reject`.
     - Nothing panics or allocates past the caps.
   - Tests: Task 4 `hash_messages_round_trip`, `bad_hash_messages_are_rejected`; Task 7 `requests_we_cannot_serve_are_rejected`; Task 8 `unsolicited_hashes_retire_the_source`, `bad_proof_retires_the_source`, `fetch_request_geometry`; fuzz targets `wire_codec` (seeds) and `hash_proof`.
3. **Torrent geometry edges.**
   - Inputs:
     - a file exactly one piece long;
     - a file one byte over a piece;
     - a file of one block (under 16 KiB);
     - empty files between non-empty ones (at an unaligned offset, as libtorrent places them);
     - the last file not padded, or followed by a tail pad (libtorrent);
     - `piece length` 16 KiB (piece layer height 0);
     - a file whose piece count is not a power of two;
     - a v2-only single-file torrent.
   - Expected: creation, parsing and verification agree. Every piece of a torrent created from a directory verifies against its own `.torrent` (v1 SHA-1, v2 root or both), and file `i`'s bytes sit at the same offsets in the v1 and v2 views.
   - Expected too: a v2 root hashes only the file's bytes of the piece, never pad bytes.
   - Tests: Task 1 `piece_roots_match_file_roots` (proptest); Task 2 `v2_piece_geometry`, `libtorrent_style_hybrid_parses`; Task 3 `created_torrents_verify_every_piece` (proptest over file-size lists).
4. **One torrent, two identities.**
   - Inputs:
     - a hybrid added by `.torrent` and then by its `btmh`-only magnet;
     - a `btih` magnet and then the `btmh` magnet of the same hybrid;
     - a `btmh` magnet whose metadata turns out hybrid and whose v1 hash belongs to a torrent already added.
   - Expected: a `btmh`-only magnet is accepted (`SessionError::V2OnlyUnsupported` is no longer produced). The second add fails with `DuplicateTorrent` when either hash is already registered. A magnet whose revealed v1 hash collides keeps running under its own key, with a warning logged and no alias added. A peer handshaking with either hash reaches the right torrent and gets that same hash back.
   - Tests: Task 6 `hybrid_is_one_torrent_under_both_hashes`, `v2_handshake_reaches_a_hybrid`, `tests/magnet.rs` `magnet_v2_only_is_accepted`.
5. **A hybrid whose halves disagree.** A hybrid whose v1 piece hashes are right but whose v2 tree is not (a broken creator), or the reverse.
   - Expected: a piece passes only if every hash it has passes. With layers in the `.torrent`, a v2 mismatch fails the piece (`HashFailed` when a peer sends the bytes). Without layers, v1 governs: a built layer that does not hash to its `pieces root` is discarded with a warning, and hash requests for that file are rejected.
   - Tests: Task 5 `hybrid_piece_must_pass_both`, `built_layer_that_disagrees_is_discarded`.
6. **Creating from awkward input.**
   - Inputs:
     - an empty directory;
     - a directory of only empty files;
     - a symlink (to a file, to a directory, dangling);
     - a non-UTF-8 file name (Unix);
     - a file that grows or shrinks while being hashed;
     - a single file;
     - cancellation from the progress callback.
   - Expected:
     - An empty directory or only empty files → `CreateError::Empty`.
     - Symlinks are skipped.
     - A non-UTF-8 name → `CreateError::InvalidName` naming the path.
     - A size change → `CreateError::FileChanged`.
     - Cancellation → `CreateError::Cancelled`.
     - A single file makes a single-file torrent.
   - Tests: Task 3 `empty_input_is_refused`, `symlinks_are_skipped`, `non_utf8_name_is_refused`, `file_changed_while_hashing`, `single_file_torrent`, `cancel_stops_hashing`.

---

## Starting point

`develop` holds milestone 5 (`v0.5.0`, merged back as PR #16, `b6deaa7`). The branch `feature/m6-v2-hybrid` exists off `develop` and holds only this plan. Check first that the workspace version is `0.5.0` and `cargo test --workspace` is green.

What exists:
- **Hashes.** `InfoHashV2`, `InfoHashes { v1, v2 }` and `InfoHashes::wire_hash()` (v1, else the first 20 bytes of v2) are in `fbt-core`.
- **Parsing.** `fbt-metainfo` parses hybrids through their v1 keys (pads included) and refuses v2-only ones with `MetainfoError::V2OnlyUnsupported`.
- **Magnets and metadata.** Magnets parse `btmh`; `from_info_bytes` checks SHA-256 against `expected.v2`.
- **Session registry.** It is keyed by the `InfoHashes` a torrent was added with, plus one `by_wire` and one `by_req2` entry per torrent.
- **SHA-1 only.** Pieces are checked only by SHA-1, in `torrent/data.rs` (`spawn_hash`) and `storage/check.rs` (`check_pieces`).
- **Fixture.** `crates/fbt-metainfo/tests/fixtures/hybrid.torrent` is synthetic, with no real piece layers.

## Decisions recorded in this plan

The owner authorised milestones 1 to 7 to run unattended. The spec's section 8 item 6 sets the scope. The builder made these calls (revisable by whoever plans a later milestone):

- **Tree conventions (BEP 52).**
  - Leaves are SHA-256 of 16 KiB blocks (the last one of a file may be short).
  - A file's tree has `next_power_of_two(ceil(len / 16 KiB))` leaves; missing leaves are all-zero hashes.
  - Heights count from the leaves: leaf layer 0, piece layer `log2(piece_len / 16 KiB)`.
  - A piece hash is the root of its subtree over the file's bytes in that piece only (never pad bytes). The last piece of a file is padded with zero leaves (`[0; 32]`, not hashes of zero bytes) to the full subtree width.
  - A file of at most one piece has no piece layer: its single piece is checked against `pieces root` over the file's own tree width.
  - A piece layer validates when the root of its hashes, padded with `pad_hash(piece_height)` to a power of two, equals `pieces root`.
- **v2-only geometry = libtorrent's.** A v2-only torrent gets a synthetic pad `FileEntry` (path `.pad/<len>`, `pad: true`) before every non-empty file whose offset is not a multiple of the piece length, as libtorrent's `canonicalize` places them (`[f0, f1 (empty), .pad/N, f2]`). No tail pad follows the last file. Piece indices on the wire and in storage are global over that padded layout (BEP 52 aligns files to pieces, and libtorrent numbers pieces this way).
  - File indices count the pads, as they already do for hybrids. Empty files take no pieces, get no pad and sit at whatever offset precedes them.
  - A single-file v2 torrent is one whose tree has exactly one entry, a file, at the top level. Its path is the sanitized `name`, as for v1.
- **v2 validation.** Rules:
  - `piece length` must be a power of two of at least 16 KiB.
  - Tree depth is capped by the bencode depth limits (each path component is one dict level): about 60 components in a `.torrent` (`MAX_TORRENT_DEPTH`) and about 29 in metadata fetched by magnet (`MAX_INFO_DEPTH`), past which parsing fails with `MetainfoError::Bencode`. `MAX_PATH_DEPTH` is never reached for v2. The file count is capped by the existing item caps.
  - `pieces root` is required for a non-empty file and forbidden for an empty one.
  - File-tree keys go through the existing component sanitizer and collision renames, exactly like v1 paths.
  - Hybrid consistency: the v1 non-pad files and the v2 files must match one to one in order and length, every non-empty, non-pad file must start on a piece boundary (empty files may sit anywhere; a trailing pad after the last file is accepted), and a mismatch gives `MetainfoError::InconsistentHybrid`.
  - `MetainfoError::V2OnlyUnsupported` and `SessionError::V2OnlyUnsupported` are deprecated and no longer produced (kept for compatibility, as `DuplicatePath` was in 0.5.0).
- **Piece layers.**
  - `Metainfo::piece_layers` holds only validated layers, keyed by `pieces root`.
  - In a `.torrent`, a layer that does not validate rejects the torrent (`InvalidField { field: "piece layers", .. }`).
  - A missing layer is allowed: it is fetched from peers (v2-only) or built from verified data (hybrid).
  - Layers for roots no file has are ignored.
  - `with_piece_layers` merges the given layers into those already held.
  - `MAX_PIECE_LAYER_HASHES` = 4_194_304 hashes in all, the same order as `MAX_INFO_ITEMS`.
- **Verification (`verify_piece`, returning a `PieceCheck`).**
  - v1: SHA-1.
  - v2-only: the subtree root against the layer entry, or against `pieces root` for a one-piece file.
  - Hybrid: SHA-1, and also the v2 root when the file's layer is known.
  - A piece passes only if every check it has passes.
  - v2 roots are `piece_root(&data[..v2.len], v2.leaves)`: the piece buffer is truncated to the file's bytes before hashing.
  - The hashing job returns the piece's v2 root alongside the verdict, so a hybrid without layers builds them for free. When a file's layer fills, it is validated against `pieces root` and added; one that disagrees is discarded with a `tracing::warn!`.
  - `LayerBuilder` batches completed layers; the actor swaps its `Loaded.meta` at most once per second, and once at completion. Only `Loaded.meta` is swapped: `metainfo_cell` is a `OnceLock`, so `TorrentHandle::metainfo()` keeps the layers known when it was set (documented).
  - The same verifier runs in recheck.
  - Hybrids hash twice (SHA-1 and SHA-256): accepted cost, recorded in the benchmark notes.
- **Layer fetch, v2-only only.** When a v2-only torrent's metadata (from a magnet, a `.torrent` or resume data without layers) lacks a layer, the torrent stays in `Phase::Metadata` (`TorrentState::DownloadingMetadata`) until every layer is fetched, whatever its source. Libtorrent fetches lazily; ours is simpler and costs 32 bytes per piece up front.
  - **Sources.** Peers that set the v2 reserved bit, at most `MAX_LAYER_SOURCES` = 4 at once.
  - **Request geometry.** For a file with piece-layer height `p = log2(piece_len / 16 KiB)` and tree height `H = file_tree_height(len)`, the padded layer width is `W = 2^(H - p)` (at least 2 for a multi-piece file). Requests have `length = min(LAYER_REQUEST_LEN, W)` (`LAYER_REQUEST_LEN` = 512), `index` = `0, length, 2*length, ... < W`, `base_layer = p` and `proof_layers = H - p - log2(length)` (exactly the uncles up to, excluding, the root). Every span lies inside the padded layer; `length` 1 is never sent.
  - **Failures.** Each request times out after 30 s (the source is retired, no ban). `hash reject` moves the span to another source. Hashes that fail their proof, or arrive unrequested, close the peer and add it to the metadata ban set.
  - **Hybrids.** A hybrid never fetches layers; v1 verifies its pieces.
- **Hash serving.**
  - **Requests answered.** `hash request`s for files whose layer we hold, at a base layer at or above the piece layer, are answered from layers computed on demand (`MerkleTree::from_layer`).
  - **Requests rejected.** Requests below the piece layer (we keep no block hashes), for unknown roots, or whose span leaves the padded base layer get `hash reject`.
  - **Request validity.** `length` must be a power of two in `1..=MAX_HASH_REQUEST_LEN` (= 8192; `length` 1 is accepted on receive), `index` a multiple of `length`, and `base layer` below the tree height. Otherwise the message is malformed: the connection closes.
  - **Pending cap.** At most `MAX_PENDING_HASH_REQUESTS` = 64 queued per peer; beyond that, reject.
- **Proof shape.** The answer carries `length` base-layer hashes, then one uncle hash per layer from `base + log2(length)` upward. There are `min(proof_layers, tree_height − base − log2(length))` uncles, bottom-up, where `tree_height` is the height of the file's root.
  - This is our reading of BEP 52 and of libtorrent's `merkle_tree::get_hashes`.
  - The interop `v2` scenario (a `btmh` magnet from qBittorrent, and qBittorrent's magnet from us) is the arbiter. If it shows a different shape, follow libtorrent and correct this paragraph in the same commit.
- **The v2 reserved bit** (`reserved[7] & 0x10`) is set on every handshake we send, as libtorrent does. Hash messages from a peer that did not set it close the connection.
- **Identity.**
  - The registry key stays the `InfoHashes` the torrent was added with (events and handles keep it).
  - `by_wire` (and `by_req2`) hold every wire hash of the torrent: the v1 hash and the truncated v2 hash.
  - When a magnet's metadata reveals a hash the key lacked, the actor registers the alias (in `torrent/magnet.rs::install_metadata`). If another torrent owns it, the alias is skipped with a warning. The alias is not announced (backlog).
  - `unregister` removes every wire hash of the torrent from `by_wire` and `by_req2`.
  - `Session::torrent(ih)` finds a torrent by exact key, else by any wire hash of `ih`.
  - An inbound handshake answers with the hash the peer sent (`ctx.info_hash = hs.info_hash` in `swarm.rs::on_incoming`). Outbound dials use `wire_hash()` (v1 for hybrids).
- **Announcing.** A hybrid announces under both hashes of its registry key, the v1 hash then the truncated v2, as libtorrent does, so peers that only know the `btmh` find it. A hash revealed only by metadata is registered but not announced (backlog).
  - **Trackers.** Each tracker keeps announce state per hash: `started`/`stopped` and intervals are tracked per (tracker, hash).
  - **DHT.** One `get_peers` + `announce_peer` per hash.
  - **LSD.** One message with two `Infohash` headers.
  - **Scrape.** Uses the wire hash only.
  - **Not affected.** PEX and holepunch carry no hash.
  - **Events.** `Event::TrackerReply` gains `info_hash: [u8; 20]`, saying which announce it was.
- **Torrent creation (`fbt_metainfo::CreateTorrent`), shaped like libtorrent's `create_torrent`.**
  - **Format.** Default `TorrentVersion::Hybrid` (libtorrent 2.0's default).
  - **Piece length.** `auto_piece_len(total)` is the smallest power of two ≥ 16 KiB giving at most 2048 pieces, capped at 16 MiB.
  - **Walking the input.** Recursive, `symlink_metadata`, symlinks skipped, hidden files kept. Names must be UTF-8 (`InvalidName`).
  - **File order.** Files sorted by their component lists bytewise (the v2 tree is a sorted dict, so the hybrid v1 list uses the same order).
  - **Pads.** Hybrids get `.pad/<n>` pads (`attr` `p`) before every non-empty file whose offset is not a multiple of the piece length, as libtorrent does; no tail pad. v1 gets no pads.
  - **Other keys.** `creation date` and `created by` are left out unless set, so output is deterministic.
  - **Changed files.** Lengths are re-checked once after all hashing, and `UnexpectedEof` while reading maps to `FileChanged`.
  - **Caps.** At most `MAX_CREATE_FILES` = 1_000_000 files.
  - **API.** `build` is synchronous and reports progress through a callback that may cancel. Async clients run it on `spawn_blocking`, as documented. `CreateTorrent` has no `Default`.
- **Resume data version 2.**
  - `fbt-resume-version` becomes 2.
  - `info-hash` holds the v1 hash and is absent for v2-only.
  - New `info-hash2` (32 bytes) for v2 and hybrid.
  - New `piece-layers`: a dict from `pieces root` to the concatenated layer hashes, every layer the torrent holds (built or fetched ones included). Each layer is validated on load, and a bad one is dropped (refetched or rebuilt), not fatal. Layers that would push the data over `MAX_RESUME_BYTES` are left out (refetched or rebuilt), never a save failure.
  - At least one of the two hashes is required, and the `info` is checked against each one present (`ResumeError::InfoHashMismatch`). Without `info`, the magnet's `btih` must equal `info-hash` when present and its `btmh` must equal `info-hash2` when present.
  - Version 1 is still read (v1 only, as before). Versions above 2 → `ResumeError::Version(v)`.
- **Interop.** Two new scenarios, `v2` and `hybrid`.
  - **qBittorrent's torrents.** qBittorrent 5.3 creates the v2 and hybrid torrents through its WebUI `torrentcreator` API (`addTask`, poll `status`, fetch `torrentFile`, `deleteTask`), so we parse libtorrent's output.
  - **Our torrents.** `fbt-example create` makes ours, and qBittorrent (both formats) and Transmission (hybrid only; it has no v2) download them from an fbt seed.
  - **v2 magnets.** fbt downloads a `btmh`-only magnet from a qBittorrent seed, which proves our layer fetch. qBittorrent downloads an fbt v2 seed by `btmh` magnet, which proves our hash serving.
  - **Cross-check.** fbt's and qBittorrent's v2 info hash for the same payload and piece size must be equal: a hard check, proving tree, ordering and encoding. The payload files are mode 0644 (libtorrent records `attr` `x`), and fbt creates from a directory with the same final name (it is the `name`). The hybrid hashes are compared and only logged, since libtorrent may name or attribute pads differently.
  - **Transmission fallback.** If Transmission 4.1 refuses a hybrid, that leg is dropped with a note in `dev/interop/README.md`.
- **New fuzz target:** `hash_proof` (`fbt_core::merkle::verify_hashes` on arbitrary input).
  - `metainfo`, `magnet`, `wire_codec` and `resume` get v2 seeds.
  - The target goes in `.github/workflows/fuzz.yml`'s matrix and its PR `paths` (`crates/fbt-core/**`).
- **Version and changelog.** Version `0.6.0`; changelog entries go under `## [Unreleased]`.

### Pre-flight rulings

From the pre-flight scan (`.superpowers/sdd/2026-11-13-milestone-6-v2-hybrid/preflight.md`, findings F1 to F24); the controller adopted every proposed fix, choosing libtorrent pad placement (F5), backlog for revealed-hash announcing (F18), an internal `merkle` (F20) and the task split (F23). Each is folded into its task's text; this list is the index.

- **Task 1.** One-piece files compared at their own tree width; `proofs_verify` separates layer size and span (F8). `proptest` dev-dependency (F13).
- **Task 2.** `V2Piece.len`; roots over the file's bytes only (F2). Pads before each unaligned non-empty file; empty files anywhere and a tail pad accepted; `libtorrent_style_hybrid_parses` (F5). Depth past the bencode limit is `MetainfoError::Bencode` (F14). `with_piece_layers` merges (F21).
- **Task 3.** Proptest hashes `&data[..len]` (F2). libtorrent pad placement, no tail pad (F5). Lengths re-checked after hashing, `UnexpectedEof` → `FileChanged` (F16). Generator: a non-empty file, fixed distinct names (F17). Re-exports `validate_layer` and `MAX_PIECE_LAYER_HASHES`; `merkle` internal; `CreateTorrent` without `Default` (F20).
- **Task 4.** The `inbound.rs` arm keeps `fbt-session` compiling (F3).
- **Task 5.** `verify_piece` truncates to `V2Piece.len` (F2). Session half of `hybrid_piece_must_pass_both` uses a `ScriptedPeer` (F7). Only `Loaded.meta` is swapped; `piece_layers_known` via a `Cmd` (F11). `lib_surface.rs` in Files (F12). Batched layer swaps; `check_pieces` breaking; no `PieceVerifier` (F21). Split from old Task 5 (F23).
- **Task 6.** `SessionError::V2OnlyUnsupported` no longer produced; `magnet_v2_only_is_accepted` (F1). `swarm.rs`, `magnet.rs`, `error.rs`, `tests/magnet.rs` in Files; `unregister` removes every hash (F12). Split from old Task 5 (F23).
- **Task 7.** Spans leaving the padded layer → `Reject` (F6). Split from old Task 6 (F23).
- **Task 8.** `pending_layers` and `Phase::Metadata` for every source (F4). Request geometry and `fetch_request_geometry` (F6). `seed_without_v2_peers_stays_in_metadata` setup (F19). Split from old Task 6 (F23).
- **Task 9.** Revealed hashes are not announced; backlog (F18).
- **Task 10.** `ResumeError::Version`, `ResumeError::InfoHashMismatch`, existing assertions, `resume-v1.bin` generator (F9). Magnet `btih`/`btmh` check and the `commands.rs` record (F10). Reads `Loaded.meta` (F11). Layers over `MAX_RESUME_BYTES` left out (F22).
- **Task 11.** `torrentcreator` call, file mode and directory name, `infohash_v2` (F15). `info` prints `hex(wire_hash())` and `num_pieces()` (F16).
- **Task 12.** `tempfile` dev-dependency in `crates/fbt`; the full `### Changed` list (F24, F1, F21).

---

## File Structure

```
crates/fbt-core/src/merkle.rs                      new: leaf/pad/root/proof functions, MerkleTree
crates/fbt-core/src/{lib.rs,tests.rs}              export, unit + proptests
crates/fbt-core/Cargo.toml                         proptest dev-dependency
crates/fbt-metainfo/Cargo.toml                     sha1, sha2 to [dependencies]
crates/fbt-metainfo/src/metainfo.rs                v2 file tree, piece layers, hybrid check, v2_piece, version()
crates/fbt-metainfo/src/v2.rs                      new: file-tree parse, layer validation (keeps metainfo.rs from growing)
crates/fbt-metainfo/src/create.rs                  new: CreateTorrent, TorrentVersion, CreateError, auto_piece_len
crates/fbt-metainfo/tests/{metainfo.rs,create.rs,support/mod.rs,fixtures/*}
crates/fbt-wire/src/{message.rs,codec.rs,handshake.rs,lib.rs}   hash request/hashes/reject, v2 bit
crates/fbt-wire/tests/wire.rs
crates/fbt-session/src/storage/check.rs            PieceCheck, verify_piece, check_pieces over it
crates/fbt-session/src/torrent/{data.rs,check.rs}  verifier in the hash job and recheck, layer building
crates/fbt-session/src/torrent/layers.rs           new: LayerFetch (pure), LayerBuilder, hash serving helpers
crates/fbt-session/src/torrent/{actor.rs,metadata.rs,peers.rs,magnet.rs,announcer.rs,dht.rs,swarm.rs,mod.rs}
crates/fbt-session/src/peer/conn/{inbound.rs,mod.rs}, peer/messages.rs, listener.rs   hash messages, echo hash
crates/fbt-session/src/{session.rs,resume.rs,event.rs,net.rs,error.rs}
crates/fbt-session/tests/{v2.rs,magnet.rs,resume.rs,trackers.rs,dht.rs,lsd.rs}, tests/support/mod.rs
crates/fbt-session/tests/fixtures/resume-v1.bin     new: version 1 resume blob
crates/fbt/src/lib.rs, crates/fbt/tests/lib_surface.rs, crates/fbt/Cargo.toml (tempfile dev-dep)
crates/fbt-example/src/main.rs                     create subcommand, info prints v2
dev/interop/{run.sh,README.md}                     v2 and hybrid scenarios
fuzz/fuzz_targets/hash_proof.rs, fuzz/Cargo.toml, fuzz/seeds/{hash_proof,metainfo,magnet,wire_codec,resume}/
.github/workflows/fuzz.yml
CHANGELOG.md, README.md, CLAUDE.md, crates/fbt-metainfo/tests/fixtures/README.md
```

---

### Task 1: Merkle trees in `fbt-core`

**Files:**
- Create: `crates/fbt-core/src/merkle.rs`
- Modify: `crates/fbt-core/src/lib.rs`, `crates/fbt-core/src/tests.rs`, `crates/fbt-core/Cargo.toml` (`[dev-dependencies] proptest.workspace = true`)

**Interfaces:**
- Produces (all in `fbt_core::merkle`, public in `fbt-core` but not re-exported from `fbt`):
  - `pub type Hash256 = [u8; 32];`
  - `pub const MERKLE_BLOCK_LEN: u32 = 16_384;`
  - `pub const MAX_TREE_HEIGHT: u32 = 48;` (a cap on every height input; enough for 2^48 leaves)
  - `pub fn pad_hash(height: u32) -> Hash256`: root of a perfect tree of `2^height` zero leaves (`pad_hash(0) == [0; 32]`). Cached up to `MAX_TREE_HEIGHT` with a `std::sync::OnceLock<[Hash256; 49]>`; above the cap it returns the cap's value.
  - `pub fn leaf_hashes(data: &[u8]) -> Vec<Hash256>`: SHA-256 of each 16 KiB chunk.
  - `pub fn layer_root(hashes: &[Hash256], height: u32, width: usize) -> Hash256`: the root of `hashes` at `height`, padded with `pad_hash(height)` to `width`. `width` is rounded up to a power of two and to at least `hashes.len()`; an empty input gives `pad_hash(height + log2(width))`.
  - `pub fn piece_root(data: &[u8], leaves_per_piece: usize) -> Hash256`: `layer_root(&leaf_hashes(data), 0, leaves_per_piece)`.
  - `pub fn file_tree_height(len: u64) -> u32`: `log2(next_power_of_two(max(1, ceil(len / 16 KiB))))`.
  - `pub struct MerkleTree` built from one layer: `pub fn from_layer(hashes: &[Hash256], height: u32, tree_height: u32) -> Option<MerkleTree>` (`None` if the layer is wider than `2^(tree_height - height)` or a height exceeds `MAX_TREE_HEIGHT`), `pub fn root(&self) -> Hash256`, `pub fn hashes(&self, base: u32, index: usize, len: usize, proof_layers: u32) -> Option<(Vec<Hash256>, Vec<Hash256>)>` (base-layer hashes, then uncles, as Decisions "Proof shape"; `None` when `base` is below the layer it was built from, the span leaves the layer, or `len` is not a power of two). Hashes past a layer's real width read as `pad_hash` of that layer.
  - `pub fn verify_hashes(root: &Hash256, tree_height: u32, base: u32, index: usize, hashes: &[Hash256], proof: &[Hash256]) -> bool`: recomputes the span root, climbs through the uncles, and compares with `root`. It needs exactly the uncles that reach the root; false on any inconsistent input, never a panic.
- `MerkleTree` holds at most `2 * 2^(tree_height - height)` hashes. Callers bound `tree_height` from the file length, and the session builds trees only from validated layers.

- [ ] **Step 1: Write the failing tests** in `crates/fbt-core/src/tests.rs`:
  - `pad_hash_chain`: `pad_hash(0) == [0; 32]`, and `pad_hash(1) == sha256([0;32] ++ [0;32])`.
  - `single_block_root_is_its_hash`: `piece_root(b"abc", 1) == sha256(b"abc")`.
  - `short_file_pads_with_zero_leaves`: a 20 000-byte buffer has root `sha256(leaf0 ++ leaf1)` with `leaf1 = sha256(last 3 616 bytes)`. A 40 000-byte buffer pads to 4 leaves, the fourth being `[0; 32]`.
  - `piece_roots_match_file_roots` (proptest; length `1..=40 * 16 KiB`, piece length 16/32/64 KiB):
    - Inputs: a file's piece roots (each `piece_root(chunk, piece_leaves)`) form a layer at height `log2(piece_leaves)`.
    - Assertion, files longer than one piece: `layer_root(layer, h, next_pow2(n_pieces))` equals the file root `layer_root(&leaf_hashes(file), 0, next_pow2(n_leaves))`.
    - Assertion, one-piece files: `piece_root(file, 2^file_tree_height(len))` equals the file root (the `V2Piece.leaves` rule).
  - `proofs_verify` (proptest):
    - Inputs: a random layer of `n` in `1..=300` hashes at height 2, tree height `2 + log2(next_pow2(n))`; spans `len` in powers of two up to `min(64, next_pow2(n))` at `index` a multiple of `len` below `next_pow2(n)`; `proof_layers` from 0 to 20.
    - Assertion: `verify_hashes(root, ..)` accepts exactly the `(hashes, proof)` that `MerkleTree::hashes` returns with enough proof layers to reach the root.
    - Assertion: flipping any byte of any hash or uncle makes it false.
  - `verify_rejects_garbage`: wrong counts, too few or too many uncles, `index` not a multiple of the span, `base > tree_height`, `tree_height > MAX_TREE_HEIGHT`, and an empty `hashes` all return false.
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-core`. Expected: FAIL (unresolved `merkle`).
- [ ] **Step 3: Implement `crates/fbt-core/src/merkle.rs`** with the signatures above. Add a `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::panic)]` module attribute, and export it with `pub mod merkle;` from `lib.rs`.
- [ ] **Step 4: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-core`, `cargo clippy -p fbt-core --all-targets -- -D warnings` and `bash dev/check-pure-rust.sh`. Expected: PASS, no warnings.
- [ ] **Step 5: Commit.** `feat(core): BEP 52 Merkle trees, proofs and verification`

### Task 2: v2 and hybrid parsing in `fbt-metainfo`

**Files:**
- Create: `crates/fbt-metainfo/src/v2.rs`
- Modify: `crates/fbt-metainfo/src/{metainfo.rs,lib.rs,magnet.rs}`, `crates/fbt-metainfo/Cargo.toml`
- Test: `crates/fbt-metainfo/tests/metainfo.rs`, `crates/fbt-metainfo/tests/support/mod.rs`, `crates/fbt-metainfo/tests/fixtures/{hybrid.torrent,v2.torrent,v2-single.torrent,README.md}`, `fuzz/seeds/metainfo/`, `fuzz/seeds/magnet/`

**Interfaces:**
- Consumes: Task 1 `fbt_core::merkle::{Hash256, layer_root, pad_hash, file_tree_height, MERKLE_BLOCK_LEN}`.
- Produces:
  - `FileEntry` gains `pub pieces_root: Option<[u8; 32]>`: set for non-empty files of v2 and hybrid torrents, `None` for v1, pads and empty files.
  - `Metainfo` gains `pub piece_layers: BTreeMap<[u8; 32], Vec<[u8; 32]>>`: validated layers only.
  - `#[non_exhaustive] pub enum TorrentVersion { V1, V2, #[default] Hybrid }`, with `Debug, Clone, Copy, PartialEq, Eq, Default`. It is defined in `v2.rs` and re-exported from the crate root; Task 3's creator uses it.
  - `impl Metainfo { pub fn version(&self) -> TorrentVersion; pub fn v2_piece(&self, piece: u32) -> Option<V2Piece>; pub fn with_piece_layers(self, layers: BTreeMap<[u8;32], Vec<[u8;32]>>) -> Metainfo }`. `with_piece_layers` merges into the layers already held, keeping only those `validate_layer` accepts for a file of this torrent.
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub struct V2Piece { pub file: usize, pub pieces_root: [u8; 32], pub index_in_file: u32, pub expected: Option<[u8; 32]>, pub leaves: u32, pub len: u32 }`:
    - `expected` is the layer entry, or `pieces_root` for a one-piece file, or `None` when the layer is missing.
    - `leaves` is the width `piece_root` pads to: `piece_len / 16 KiB` for files with a layer, the file's own `2^file_tree_height(len)` for one-piece files.
    - `len` is the bytes of the file in this piece. v2 roots are `piece_root(&data[..v2.len], v2.leaves)`: the piece's data is truncated to it before hashing.
  - `pub fn validate_layer(pieces_root: &[u8; 32], file_len: u64, piece_len: u32, layer: &[[u8; 32]]) -> bool`
  - `pub const MAX_PIECE_LAYER_HASHES: usize = 4_194_304;`
  - `MetainfoError::InconsistentHybrid` (new variant); `V2OnlyUnsupported` gets `#[deprecated]`, documented as no longer produced.
  - Synthetic pads go before each non-empty file at an unaligned offset (Decisions "v2-only geometry").
  - `Info.pieces` is empty for v2-only torrents.
  - `Metainfo::from_info_bytes` accepts v2-only `info` dicts (it has no piece layers: `piece_layers` is empty).

- [ ] **Step 1: Extend the fixture helper.** In `tests/support/mod.rs`, add a deterministic `build_v2(files: &[(&[&str], u64)], piece_len: u32, hybrid: bool, with_layers: bool) -> Vec<u8>` that builds real roots and layers with `fbt_core::merkle` from xorshift content. Regenerate `hybrid.torrent` (now with a real tree and layers), and add:
  - `v2.torrent`: three files, sizes 20 000, 0 and 70 000, 32 KiB pieces;
  - `v2-single.torrent`: one 50 000-byte file.

  Update the fixtures README.
- [ ] **Step 2: Write the failing tests** in `tests/metainfo.rs`:
  - `v2_only_parses_with_synthetic_pads`:
    - `v2.torrent` → `version() == V2`, `info_hashes.v1 == None`.
    - Files: `[f0 20000, f1 0, .pad/12768 pad, f2 70000]`.
    - `piece_layout().num_pieces() == 1 + 3`.
    - `piece_layers` has f2's layer (3 hashes) only (f0 is one piece: no layer).
  - `v2_single_file`: `v2-single.torrent` → one file, `single_file`, path is the name.
  - `hybrid_parses_both_halves`:
    - `version() == Hybrid`, and both hashes are set.
    - `pieces_root` is set on non-pad files.
    - `v2_piece(p).expected` equals the layer entry for multi-piece files.
  - `v2_piece_geometry`:
    - f0 (one piece; 20 000 bytes = 2 leaves) has `expected == pieces_root`, `leaves == 2` and `len == 20000`.
    - f2's pieces have `leaves == 2` (32 KiB / 16 KiB) and `index_in_file` 0, 1, 2; its last piece has `len == 70000 - 65536 = 4464`.
    - Every v2 piece maps to exactly one non-pad file.
  - `missing_layer_is_allowed`: `with_layers = false` parses, `v2_piece(p).expected == None` for multi-piece files.
  - `bad_piece_layers_are_rejected`: each of a layer whose length is not a multiple of 32, one with the wrong count, and one with a flipped byte gives `InvalidField { field: "piece layers", .. }`.
  - `v2_rejects_bad_trees`: each malformed tree in Review Focus 1 gives an error, never a panic:
    - a tree nested past the bencode depth limit → `MetainfoError::Bencode`;
    - a node with `""` plus children;
    - a 31-byte `pieces root`;
    - a `pieces root` on an empty file;
    - piece length 16 383 and 48 KiB;
    - an empty tree.
  - `inconsistent_hybrid_is_rejected`: a hybrid whose v1 list swaps two files, or changes a length, or drops a pad, gives `InconsistentHybrid`.
  - `libtorrent_style_hybrid_parses`: a hybrid with v1 list `[a 20000, empty 0, .pad/12768, b 70000, .pad/<tail>]` (empty file unaligned, tail pad) parses with `version() == Hybrid`.
  - `v2_info_from_peers`: `from_info_bytes(raw_info_of(v2.torrent), InfoHashes::from_v2(..), vec![])` parses with empty `piece_layers`.
  - `with_piece_layers_keeps_valid_ones`: a map holding f2's correct layer, plus a bad one and one for an unknown root → only f2's is kept. A second call with an empty map keeps it (merge).
  - Delete or replace the test that asserted `V2OnlyUnsupported`.
- [ ] **Step 3: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-metainfo`. Expected: FAIL.
- [ ] **Step 4: Implement the parsing.**
  - In `v2.rs`: `parse_file_tree(tree: &Dict, name: &str, piece_len: u32) -> Result<Vec<FileEntry>, MetainfoError>` (depth-first in key order, which bencode dicts already are; pads inserted for v2-only), `parse_piece_layers`, and `validate_layer`.
  - In `parse_info`:
    - A v2 dict without `pieces` is v2-only.
    - A v2 dict with `pieces` is hybrid: parse both halves and compare them.
    - Run `resolve_collisions` once over the final list, so v1 and v2 rename identically.
  - `piece layers` is read in `from_bytes` (root dict) and validated per file. Count the hashes against `MAX_PIECE_LAYER_HASHES` before allocating.
  - Move `sha1`/`sha2` to `[dependencies]` only if this task needs them (Task 3 surely does).
- [ ] **Step 5: Add fuzz seeds.** Copy the three new fixtures into `fuzz/seeds/metainfo/`, and add a `btmh`-only and a `btih`+`btmh` magnet to `fuzz/seeds/magnet/`.
- [ ] **Step 6: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-metainfo && cargo clippy -p fbt-metainfo --all-targets -- -D warnings`. Expected: PASS. Then run `CARGO_INCREMENTAL=0 cargo test --workspace` (FileEntry's new field may need `..` in other crates' patterns or constructors).
- [ ] **Step 7: Commit.** `feat(metainfo): parse v2 and hybrid torrents with piece layers`

### Task 3: Torrent creation

**Files:**
- Create: `crates/fbt-metainfo/src/create.rs`, `crates/fbt-metainfo/tests/create.rs`
- Modify: `crates/fbt-metainfo/src/lib.rs`, `crates/fbt-metainfo/Cargo.toml` (`sha1`, `sha2` in `[dependencies]`; `tempfile` dev-dep if not present: it is in the workspace), `crates/fbt/src/lib.rs`, `crates/fbt/tests/lib_surface.rs`

**Interfaces:**
- Consumes: Task 1 merkle; Task 2 `TorrentVersion`, `Metainfo::from_bytes`, `v2_piece`.
- Produces (in `fbt_metainfo`, re-exported from `fbt`):
  - `#[derive(Debug, Clone)] #[non_exhaustive] pub struct CreateTorrent` (no `Default`), built with `CreateTorrent::new(path: impl Into<PathBuf>) -> Self` and chained setters:
    - `version(TorrentVersion)` (default `Hybrid`);
    - `piece_len(u32)` (default auto);
    - `trackers(Vec<Vec<String>>)`;
    - `web_seeds(Vec<String>)`;
    - `comment(String)`;
    - `created_by(String)`;
    - `creation_date(i64)`;
    - `private(bool)`.

    Each takes `self` and returns `Self`.
  - `pub fn build(&self, progress: &mut dyn FnMut(u64, u64) -> ControlFlow<()>) -> Result<Vec<u8>, CreateError>`: `progress(done_bytes, total_bytes)` is called after each piece.
  - `pub fn auto_piece_len(total: u64) -> u32`
  - `pub const MAX_CREATE_FILES: usize = 1_000_000;`
  - `#[derive(Debug, thiserror::Error)] #[non_exhaustive] pub enum CreateError { Io { path: PathBuf, source: std::io::Error }, Empty, InvalidName(PathBuf), FileChanged(PathBuf), InvalidPieceLen(u32), TooManyFiles, Cancelled }`
  - `InvalidPieceLen` covers a piece length that is not a power of two, under 16 KiB, or over `MAX_PIECE_LEN`. v1 accepts any power of two ≥ 16 KiB too, for one rule.

- [ ] **Step 1: Write the failing tests** in `tests/create.rs`, with a `tempfile::TempDir`:
  - `created_torrents_verify_every_piece` (proptest, 16 cases):
    - Inputs: up to 5 files with sizes in `0..200_000`, at least one size ≥ 1; file names from a fixed set of distinct leaf names (`f0..f4`) under distinct directory names (`d0..d2`); piece length 16/32 KiB, each `TorrentVersion`.
    - Assertion: `build` → `Metainfo::from_bytes` succeeds and `version()` matches.
    - Assertion: for every piece, reading its bytes from the files (pads as zeros) gives `sha1 == info.pieces[p]` (v1, hybrid) and `piece_root(&data[..v2_piece.len], v2_piece.leaves) == v2_piece.expected` (v2, hybrid).
    - Assertion: every layer is present.
  - `hybrid_and_v2_hash_the_same_tree`: the same directory as V2 and as Hybrid gives the same v2 `file tree` bytes; this checks the tree is identical (decode both and compare the `file tree` values).
  - `deterministic_output`: two builds give equal bytes; no `creation date` unless set.
  - `auto_piece_len_table`:
    - `auto_piece_len(1) == 16384`;
    - `auto_piece_len(2048 * 16384) == 16384`;
    - `auto_piece_len(2048 * 16384 + 1) == 32768`;
    - `auto_piece_len(u64::MAX) == 16 << 20`.
  - `empty_input_is_refused`: an empty dir and a dir of empty files → `Empty`.
  - `symlinks_are_skipped` (`#[cfg(unix)]`): a symlink to a file, one to a dir and a dangling one are absent from `info.files`.
  - `non_utf8_name_is_refused` (`#[cfg(target_os = "linux")]`): a file named `b"\xff.bin"` gives `InvalidName`.
  - `file_changed_while_hashing`: a progress callback that appends to the last file in sort order on its first call gives `FileChanged`.
  - `single_file_torrent`: a path to one file gives `single_file` and a name equal to the file name.
  - `cancel_stops_hashing`: a callback returning `Break` gives `Cancelled`.
  - `private_trackers_and_seeds_round_trip`: the setters appear in the parsed `Metainfo`.
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-metainfo --test create`. Expected: FAIL.
- [ ] **Step 3: Implement `create.rs`** as Decisions "Torrent creation".
  - Hash in one pass over the files with one piece-sized buffer: SHA-1 over the padded v1 stream and per-file 16 KiB leaves for v2 (over the file's bytes only).
  - Re-check every length once after all hashing; map `UnexpectedEof` while reading to `FileChanged`.
  - Encode with `fbt_bencode`'s encoder. Info keys are in bencode order: `file tree`, `files` (hybrid and v1 multi), `length` (single v1 and hybrid), `meta version` 2, `name`, `piece length`, `pieces`, `private`.
  - The root dict holds `announce`, `announce-list`, `comment`, `created by`, `creation date`, `info`, `piece layers` and `url-list`.
- [ ] **Step 4: Re-export from `fbt`.** Re-export `CreateTorrent`, `CreateError`, `TorrentVersion`, `auto_piece_len`, `V2Piece`, `MAX_CREATE_FILES`, `validate_layer` and `MAX_PIECE_LAYER_HASHES` from `fbt` and add them to `lib_surface.rs`. `fbt_core::merkle` is not re-exported.
- [ ] **Step 5: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-metainfo -p fbt && bash dev/check-pure-rust.sh`. Expected: PASS.
- [ ] **Step 6: Commit.** `feat(metainfo): create v1, v2 and hybrid torrents`

### Task 4: BEP 52 hash messages in `fbt-wire`

**Files:**
- Modify: `crates/fbt-wire/src/{message.rs,codec.rs,handshake.rs,lib.rs}`, `crates/fbt-session/src/peer/conn/inbound.rs` (`Message` is not `#[non_exhaustive]` and its match there is exhaustive)
- Test: `crates/fbt-wire/tests/wire.rs`, `fuzz/seeds/wire_codec/`

**Interfaces:**
- Produces:
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)] pub struct HashRequest { pub pieces_root: [u8; 32], pub base_layer: u32, pub index: u32, pub length: u32, pub proof_layers: u32 }` (wire: root, then four big-endian `u32`s, 48 bytes).
  - `Message::HashRequest(HashRequest)` (id 21), `Message::Hashes { req: HashRequest, hashes: Bytes }` (id 22; `hashes` is the 32-byte hashes after the 48-byte header, base layer then uncles) and `Message::HashReject(HashRequest)` (id 23).
  - `pub const MAX_HASH_REQUEST_LEN: u32 = 8192;`
  - `Reserved::supports_v2(&self) -> bool` and `Reserved::set_v2(&mut self)` on `reserved[7] & 0x10`.
- Decoding errors (existing `WireError` kinds):
  - an id 21 or 23 body not exactly 48 bytes;
  - an id 22 body under 48 bytes, or whose hash part is not a multiple of 32.

  A semantically invalid header (`length` not a power of two, too large) decodes fine; the session judges it (Task 7). The existing max message size bounds `hashes`.
- `Message` stays exhaustive (no `#[non_exhaustive]`, which would force wildcard arms everywhere).

- [ ] **Step 1: Write the failing tests** in `tests/wire.rs`:
  - `hash_messages_round_trip`: encode/decode equality for all three, with ids 21, 22 and 23 on the wire.
  - `bad_hash_messages_are_rejected`: 47- and 49-byte request bodies, and a `hashes` body of 48 + 31 bytes, are errors.
  - `v2_reserved_bit`: `set_v2` sets exactly `reserved[7] |= 0x10` and leaves the other bits alone.
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-wire`. Expected: FAIL.
- [ ] **Step 3: Implement the messages and the reserved bit.** In `fbt-session`'s `peer/conn/inbound.rs`, add the arm `Message::HashRequest(_) | Message::Hashes { .. } | Message::HashReject(_) => {}` (ignored until Task 7 routes them).
- [ ] **Step 4: Add fuzz seeds.** Add one seed per message to `fuzz/seeds/wire_codec/`.
- [ ] **Step 5: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-wire && CARGO_INCREMENTAL=0 cargo test --workspace`. Expected: PASS.
- [ ] **Step 6: Commit.** `feat(wire): BEP 52 hash request, hashes and hash reject`

### Task 5: Piece verification for v2 and hybrid

**Files:**
- Modify: `crates/fbt-session/src/storage/check.rs`, `crates/fbt-session/src/torrent/{data.rs,check.rs,actor.rs,mod.rs}`, `crates/fbt/tests/lib_surface.rs` (the hidden `piece_layers_known` hook)
- Create: `crates/fbt-session/src/torrent/layers.rs` (the `LayerBuilder` part; Tasks 7 and 8 add the rest)
- Test: `crates/fbt-session/tests/v2.rs` (new), `crates/fbt-session/tests/support/mod.rs`

**Interfaces:**
- Consumes: Task 2 `Metainfo::{version, v2_piece, with_piece_layers}`, `V2Piece.len`, `validate_layer`; Task 1 `piece_root`; Task 3 `CreateTorrent` (tests make their torrents with it).
- Produces:
  - `pub(crate) struct PieceCheck { pub ok: bool, pub v2_root: Option<[u8; 32]> }` and `pub(crate) fn verify_piece(meta: &Metainfo, piece: u32, data: &[u8]) -> PieceCheck` in `storage/check.rs`:
    - v1: SHA-1.
    - v2: requires `expected`; `ok = false` when it is `None` (Task 8 makes sure v2-only torrents never pick such pieces). The root is `piece_root(&data[..v2.len], v2.leaves)`.
    - hybrid: SHA-1, plus v2 when `expected` is `Some`.
    - `v2_root` is the computed subtree root (v2 and hybrid).
  - `check_pieces(storage, layout, meta: &Metainfo, on_piece)` returns `(Bitfield, Vec<Option<[u8;32]>>)`: the `v2_root` of every passing piece. It is `pub` in `fbt_session::storage`, so the new signature is a breaking change (`### Changed`).
  - `pub(super) struct LayerBuilder` in `torrent/layers.rs`: `fn new(meta: &Metainfo) -> Option<Self>` (`None` unless hybrid with missing layers), `fn record(&mut self, piece: u32, root: [u8; 32]) -> Option<([u8;32], Vec<[u8;32]>)>` (returns a completed and validated layer; discards and warns on a mismatch), `fn forget(&mut self, piece: u32)` (a piece lost by recheck). Completed layers are batched.
  - The actor swaps `Loaded.meta` for `meta.with_piece_layers(..)` (a merge) at most once per second and once at completion. `metainfo_cell` is a `OnceLock` and is not swapped: `TorrentHandle::metainfo()` keeps the layers known when it was set (documented).
  - `TorrentHandle::piece_layers_known() -> usize`, a `#[doc(hidden)] pub` test hook answered by the actor from `Loaded.meta` through a `Cmd` (like `Cmd::PeerTrust`). Named in `lib_surface.rs` as hidden.

- [ ] **Step 1: Write the failing tests** in `tests/v2.rs` (two sessions on loopback, torrents from `CreateTorrent` over a `TempDir` payload of three files: 100 000, 0 and 300 000 bytes; 32 KiB pieces):
  - `v2_torrent_downloads_and_seeds`: seed and leech a V2 `.torrent` (layers included); data equal; the leech's `TorrentFinished`.
  - `hybrid_torrent_downloads`: the same for Hybrid.
  - `hybrid_piece_must_pass_both`:
    - Inputs: a hybrid `.torrent` whose `piece layers` entry for the 300 000-byte file is replaced by a valid-looking layer of a different file of the same size. Build it with `CreateTorrent` on altered data, then splice the layer and `pieces root` in with `fbt_bencode`. Run `verify_piece` on correct data for one of its pieces.
    - Assertion: `ok == false`.
    - Session half: the bytes come from a `ScriptedPeer` (`tests/support/mod.rs`) that handshakes with the hybrid's v1 hash, sends `HaveAll` (`connect_fast`) and answers block requests with the correct payload bytes. The leech emits `HashFailed` for that piece.
  - `built_layer_that_disagrees_is_discarded` (unit test in `layers.rs`): `LayerBuilder::record` fed roots that do not hash to `pieces_root` returns `None` once full, and the layer is absent.
  - `hybrid_without_layers_builds_them`: a hybrid `.torrent` stripped of `piece layers` downloads; afterwards `piece_layers_known()` on the leech equals the number of multi-piece files.
  - `recheck_verifies_v2`: a seeded V2 torrent with one byte flipped on disk rechecks to all pieces but one.
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session --test v2`. Expected: FAIL.
- [ ] **Step 3: Implement `verify_piece` and `check_pieces` over it.**
  - Use `verify_piece` in `spawn_hash` (`Job::Hashed` gains `v2_root`) and in `torrent/check.rs`.
  - Feed `LayerBuilder` from both; swap `Loaded.meta` as Interfaces.
  - Smart ban's per-block SHA-1 is unchanged.
- [ ] **Step 4: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session` (the whole crate: v1 paths must be unchanged) and `CARGO_INCREMENTAL=0 cargo test -p fbt --test lib_surface`. Expected: PASS.
- [ ] **Step 5: Commit.** `feat(session): verify v2 and hybrid pieces and build missing layers`

### Task 6: One torrent under every wire hash

**Files:**
- Modify: `crates/fbt-session/src/session.rs`, `crates/fbt-session/src/error.rs`, `crates/fbt-session/src/torrent/{swarm.rs,magnet.rs,actor.rs}`, `crates/fbt-session/src/peer/conn/mod.rs`, `crates/fbt-session/src/listener.rs`
- Test: `crates/fbt-session/tests/v2.rs`, `crates/fbt-session/tests/magnet.rs`

**Interfaces:**
- Consumes: Task 3 `CreateTorrent`; Task 4 `Reserved::set_v2`; Task 5 `tests/v2.rs` fixtures.
- Produces:
  - `TorrentEntry.wire_hash` becomes `wire_hashes: Vec<[u8; 20]>` (1 or 2); `by_wire` and `by_req2` hold one entry per wire hash, and `unregister` removes all of them. The listener lookup and the MSE `by_req2` lookup in `listener.rs` need no logic change.
  - `SessionInner::add_alias(&self, key: &InfoHashes, wire: [u8; 20]) -> bool`, called from `torrent/magnet.rs::install_metadata` when metadata reveals a new hash.
  - `Session::torrent(ih)` falls back to `by_wire` over `ih`'s hashes.
  - `pub(crate) fn wire_hashes(ih: &InfoHashes) -> Vec<[u8; 20]>`: the v1 hash, then the truncated v2, either one when only it exists. It goes in `session.rs` and is used by Task 9 too.
  - The inbound echo: `ctx.info_hash = hs.info_hash` in `swarm.rs::on_incoming` (`PeerCtx.info_hash` is built in `swarm.rs` and checked by `check_handshake`).
  - `SessionError::V2OnlyUnsupported` is no longer produced: the check in `Session::add_torrent` and its doc comment go, and the variant gets `#[deprecated(note = "no longer produced: v2 magnets are supported")]`, like `MetainfoError::DuplicatePath`. Task 10 handles the resumed-magnet branch.

- [ ] **Step 1: Write the failing tests.**
  - `tests/v2.rs` `hybrid_is_one_torrent_under_both_hashes`: add a hybrid by `.torrent`, then `add_torrent` of its `btmh`-only magnet, then its `btih`-only magnet → both `DuplicateTorrent`. `session.torrent(&InfoHashes::from_v2(v2))` finds it.
  - `tests/v2.rs` `v2_handshake_reaches_a_hybrid`: a raw TCP client handshakes with the truncated v2 hash of a hybrid the session seeds and receives a handshake carrying that same hash and the v2 reserved bit.
  - `tests/magnet.rs`: replace `magnet_v2_only_rejected` with `magnet_v2_only_is_accepted` (added, state `DownloadingMetadata`).
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session --test v2 --test magnet`. Expected: FAIL.
- [ ] **Step 3: Implement registry aliases and the handshake.**
  - Registry aliases: `by_wire`/`by_req2` entries for each wire hash, the duplicate check over all of them, `add_alias`, `unregister`, and the `torrent()` fallback.
  - Remove the `V2OnlyUnsupported` check and deprecate the variant.
  - Inbound connections echo the peer's hash.
  - Every handshake we send sets the v2 bit.
- [ ] **Step 4: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session`. Expected: PASS.
- [ ] **Step 5: Commit.** `feat(session): register a torrent under every wire hash and accept v2 magnets`

### Task 7: Hash messages in the session and hash serving

**Files:**
- Modify: `crates/fbt-session/src/peer/{messages.rs,conn/inbound.rs,conn/mod.rs}`, `crates/fbt-session/src/torrent/{layers.rs,actor.rs,peers.rs,mod.rs}`
- Create: `fuzz/fuzz_targets/hash_proof.rs`, `fuzz/seeds/hash_proof/`
- Modify: `fuzz/Cargo.toml`, `.github/workflows/fuzz.yml`
- Test: unit tests in `layers.rs`

**Interfaces:**
- Consumes: Task 1 `MerkleTree`, `verify_hashes`, `file_tree_height`; Task 4 messages, `MAX_HASH_REQUEST_LEN`, `supports_v2`; Task 5 `layers.rs` and the actor's `Loaded.meta`.
- Produces:
  - In `layers.rs`:
    - `pub(super) const MAX_PENDING_HASH_REQUESTS: usize = 64;`
    - `pub(super) fn serve_hash_request(meta: &Metainfo, req: &HashRequest) -> Result<Vec<u8>, ServeError>`, where `ServeError` is `Malformed` (close the peer) or `Reject` (send `hash reject`). It returns the hash bytes (base layer, then uncles).
    - `Malformed`: `length` not a power of two or above `MAX_HASH_REQUEST_LEN`, `index` not a multiple of `length`, `base_layer` at or above the tree height.
    - `Reject`: unknown root, a layer we lack, `base_layer` below the piece layer, or a span that leaves the padded base layer.
  - Peer plumbing: `HashRequest`, `Hashes` and `HashReject` reach the actor as peer messages. `Hashes` and `HashReject` are dropped until Task 8 consumes them.
  - Every torrent answers `HashRequest` through `serve_hash_request` from `Loaded.meta`, at most `MAX_PENDING_HASH_REQUESTS` per peer in flight (beyond → reject). Hash messages from a peer without the v2 bit, or on a v1-only torrent, close it.

- [ ] **Step 1: Write the failing unit tests** in `layers.rs`:
  - `serve_answers_piece_layer_spans`: the served bytes for `(root, piece_height, 0, 4, 10)` verify with `verify_hashes`.
  - `requests_we_cannot_serve_are_rejected`:
    - `base < piece_height` → `Reject`;
    - unknown root → `Reject`;
    - a span past the padded layer (`index` 16, `length` 16 on a 10-piece file) → `Reject`;
    - `length` 3 → `Malformed`;
    - `length` 16384 → `Malformed`;
    - `index` 5 with `length` 4 → `Malformed`;
    - `base` above the tree → `Malformed`.
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session layers`. Expected: FAIL.
- [ ] **Step 3: Implement serving, the pending cap and the peer-message plumbing.**
- [ ] **Step 4: Add the fuzz target `hash_proof`.** It parses arbitrary bytes into `(root, tree_height, base, index, hashes, proof)` and calls `verify_hashes`. Add seeds from the unit test's served spans, then add it to `fuzz/Cargo.toml`, the `fuzz.yml` matrix and the `paths` (`crates/fbt-core/**`).
- [ ] **Step 5: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session`. Expected: PASS. Run `cargo +nightly fuzz build hash_proof` if nightly is installed (else note it for CI).
- [ ] **Step 6: Commit.** `feat(session): route BEP 52 hash messages and serve piece layers`

### Task 8: Layer fetch for v2-only torrents

**Files:**
- Modify: `crates/fbt-session/src/torrent/{layers.rs,actor.rs,magnet.rs,metadata.rs,peers.rs,mod.rs}`
- Test: `crates/fbt-session/tests/v2.rs`, unit tests in `layers.rs`

**Interfaces:**
- Consumes: Task 1 `verify_hashes`, `file_tree_height`; Task 2 `with_piece_layers`; Task 4 `supports_v2`; Task 6 v2 magnets accepted; Task 7 the routed `Hashes`/`HashReject` and `serve_hash_request` (tests answer the fetch with it).
- Produces:
  - In `layers.rs`:
    - `pub(super) const LAYER_REQUEST_LEN: u32 = 512;`
    - `pub(super) const MAX_LAYER_SOURCES: usize = 4;`
    - `pub(super) const LAYER_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);`
    - `pub(super) struct LayerFetch` (pure, `now` passed in, shaped like `MetadataFetch`):
      - `fn new(meta: &Metainfo) -> Option<Self>` returns `None` when every multi-piece file has its layer or the torrent is not v2-only;
      - `fn add_source(&mut self, key: PeerKey, now) -> Vec<(PeerKey, HashRequest)>`;
      - `fn on_hashes(&mut self, key, req, hashes: &[u8], now) -> LayerStep`;
      - `fn on_reject(&mut self, key, req, now) -> Vec<(PeerKey, HashRequest)>`;
      - `fn on_disconnect(&mut self, key, now) -> Vec<..>`;
      - `fn tick(&mut self, now) -> Vec<..>`;
      - `fn is_complete(&self) -> bool`;
      - `fn into_layers(self) -> BTreeMap<[u8;32], Vec<[u8;32]>>`.
    - `enum LayerStep { Send(Vec<(PeerKey, HashRequest)>), Bad(PeerKey) }`.
  - Request geometry as Decisions "Layer fetch": `length = min(LAYER_REQUEST_LEN, W)`, `index` = `0, length, ... < W`, `base_layer = p`, `proof_layers = H - p - log2(length)`.
- Flow:
  1. A v2-only torrent whose `LayerFetch::new` is `Some` holds its metainfo in `Actor.pending_layers: Option<(Arc<Metainfo>, LayerFetch)>` with `loaded = None` and `Phase::Metadata`, whatever its source.
  2. For a `TorrentSource::Metainfo` (a `.torrent` or resume data) the actor does this in `Actor::new` instead of building `Loaded`. The storage passed from `add_torrent` is kept in a `MagnetState`-like holder. For a magnet, `install_metadata` diverts into the same holder.
  3. It feeds peers with `supports_v2` to `add_source`.
  4. On `is_complete()`, `install_metadata(meta.with_piece_layers(fetch.into_layers()))` runs as for a magnet (storage, check, download). `metainfo_cell` is set only then: `has_metadata`/`metainfo()` stay `None` until the layers are in, and `MetadataReceived` is emitted once, after them.
  5. `Bad` closes the peer with the metadata-ban reason.

- [ ] **Step 1: Write the failing unit tests** in `layers.rs`:
  - `fetch_request_geometry`: for the 10-piece file (300 000 bytes, 32 KiB pieces), exactly one request `(p, 0, 16, H - p - 4)`.
  - `fetch_completes_from_one_source`: a `LayerFetch` over a 3-file v2 meta with layers stripped, answered by `serve_hash_request` on the full meta, completes, and `into_layers` equals the full meta's.
  - `unsolicited_hashes_retire_the_source`: `on_hashes` for a span not requested → `Bad`.
  - `bad_proof_retires_the_source`: one flipped byte → `Bad`.
  - `reject_moves_the_span`: `on_reject` re-sends the span to the other source.
  - `timeout_retires_without_ban`: past `LAYER_REQUEST_TIMEOUT`, `tick` re-sends elsewhere and the first source is not `Bad`.
- [ ] **Step 2: Write the failing session tests** in `tests/v2.rs`:
  - `v2_magnet_downloads`: a seed with a V2 torrent (the 300 000-byte file gives it a layer); a leech adds the `btmh`-only magnet with the seed as a peer, gets metadata and layers, and finishes with data equal.
  - `v2_torrent_without_layers_fetches_them`: a V2 `.torrent` stripped of `piece layers` downloads the same way.
  - `hybrid_magnet_downloads_without_layer_fetch`: the `btmh` magnet of a hybrid finishes; `piece_layers_known` on the leech after finishing equals the number of multi-piece files (built from data, not fetched).
  - `seed_without_v2_peers_stays_in_metadata`: the leech adds a V2 `.torrent` stripped of `piece layers` (no metadata exchange needed) and connects to a `ScriptedPeer::accept_with` whose reserved bits lack 0x10. After 3 s its state is `DownloadingMetadata` and the peer has seen no `Request`.
- [ ] **Step 3: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session layers && CARGO_INCREMENTAL=0 cargo test -p fbt-session --test v2`. Expected: FAIL.
- [ ] **Step 4: Implement `LayerFetch` and the flow.**
- [ ] **Step 5: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session`. Expected: PASS.
- [ ] **Step 6: Commit.** `feat(session): fetch missing piece layers for v2-only torrents`

### Task 9: Announce both hashes of a hybrid

**Files:**
- Modify: `crates/fbt-session/src/torrent/{announcer.rs,tiers.rs,dht.rs,swarm.rs,commands.rs}`, `crates/fbt-session/src/net.rs`, `crates/fbt-session/src/event.rs`, `crates/fbt-session/src/torrent/actor.rs` (LSD `want`)
- Test: `crates/fbt-session/tests/{trackers.rs,dht.rs,lsd.rs}`

**Interfaces:**
- Consumes: Task 6 `wire_hashes(&InfoHashes) -> Vec<[u8; 20]>` and v2 magnets accepted.
- Produces:
  - `Event::TrackerReply` gains `info_hash: [u8; 20]` (breaking; `### Changed`).
  - The hashes announced are `wire_hashes` of the registry key, taken at spawn; a hash revealed later by metadata is not announced (backlog).
  - The announcer keeps per-(tracker, hash) `started`/`stopped`, next-announce and failure state. The `stopped` rules of milestone 5 apply per pair.
  - The DHT announce loop runs one `get_peers`/`announce_peer` per hash.
  - The LSD request carries every hash of every wanted torrent (`net.rs` `want` takes the hash list).
  - Scrape still uses `wire_hash()`.

- [ ] **Step 1: Write the failing tests.**
  - `trackers.rs` `hybrid_announces_both_hashes`: a `FakeTracker` sees `started` for the v1 hash and for the truncated v2 hash of a hybrid, and two `TrackerReply` events with those `info_hash`es. On pause it sees `stopped` for both.
  - `trackers.rs` `v1_announces_once`: a v1 torrent produces exactly one announce per interval (regression guard).
  - `dht.rs` `btmh_peer_finds_hybrid_seed_over_dht`: two sessions plus loopback DHT nodes as the existing DHT tests do. The seed adds a hybrid `.torrent`; the leech adds the `btmh`-only magnet with no peers and no trackers. It finishes.
  - `lsd.rs` `lsd_carries_both_hashes`: the multicast fake receives one message with two `Infohash` headers for a hybrid.
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session --test trackers --test dht --test lsd`. Expected: FAIL.
- [ ] **Step 3: Implement as Interfaces.**
- [ ] **Step 4: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session`. Expected: PASS.
- [ ] **Step 5: Commit.** `feat(session): announce hybrid torrents under both info hashes`

### Task 10: Resume data version 2

**Files:**
- Modify: `crates/fbt-session/src/resume.rs`, `crates/fbt-session/src/torrent/{commands.rs,actor.rs}`
- Create: `crates/fbt-session/tests/fixtures/resume-v1.bin`
- Test: `crates/fbt-session/tests/resume.rs`, unit tests in `resume.rs`, `fuzz/seeds/resume/`

**Interfaces:**
- Consumes: Task 2 `with_piece_layers`, `validate_layer`, `MAX_PIECE_LAYER_HASHES`; Task 5 the actor's `Loaded.meta` (layers included; not `metainfo_cell`); Task 8 the layer fetch for a `Metainfo` source.
- Produces:
  - `RESUME_VERSION` = 2, written always.
  - New keys `info-hash2` (32 bytes) and `piece-layers` (dict, root → concatenated hashes).
  - `info-hash` becomes optional (absent for v2-only). The record in `commands.rs` (today `info_hash: self.ih.wire_hash()`) writes `info-hash` from the v1 hash only, and `info-hash2` from `ih.v2` or the metadata's v2.
  - `decode_resume` accepts versions 1 and 2, refuses others with the existing `ResumeError::Version(v)`.
  - On load the `info` must hash to every hash present (`ResumeError::InfoHashMismatch`). Without `info`, the magnet's `btih` must equal `info-hash` when present and its `btmh` must equal `info-hash2` when present; at least one is required (replaces today's v1-only magnet check).
  - Without both `info-hash` and `info-hash2`, decoding fails with `Missing("info-hash")`.
  - Layers are applied through `with_piece_layers` (invalid ones dropped), and a v2-only torrent lacking one fetches it (Task 8).
  - Layers that would push the data over `MAX_RESUME_BYTES` are left out on save, never a save failure.
  - Update the module's key table doc.

- [ ] **Step 1: Write the failing tests.**
  - First, before the version bump, generate `crates/fbt-session/tests/fixtures/resume-v1.bin` with a committed `#[ignore]` generator test (as `fbt-metainfo`'s `regenerate_fixtures`) from the current encoder.
  - `resume.rs` unit `version_1_still_reads`: the fixture, loaded with `include_bytes!("../tests/fixtures/resume-v1.bin")`, decodes.
  - `version_3_is_refused`; change the two existing `Version(2)` assertions (`tests/resume.rs`, `src/resume.rs`) to version 3. The existing `Missing("info-hash")` assertion stays.
  - `v2_resume_requires_a_matching_hash`: `info-hash2` not matching `info` → `ResumeError::InfoHashMismatch`.
  - `v2_magnet_resume_without_info`: a magnet record whose `btmh` disagrees with `info-hash2` → `ResumeError::InfoHashMismatch`; a matching one decodes.
  - `oversized_layers_are_left_out`: layers past `MAX_RESUME_BYTES` are dropped from the saved data and the save succeeds.
  - Session `tests/resume.rs` `v2_magnet_resume_skips_layer_fetch`: a leech gets a V2 torrent by magnet (layers fetched), pauses, saves, shuts down, and restores in a session with no peers. Status shows `resume_trusted` and pieces intact, and the torrent is not in `DownloadingMetadata`.
  - `hybrid_resume_keeps_built_layers`: the layers built from data survive a save and restore (`piece_layers_known`).
  - `bad_layer_in_resume_is_dropped`: a corrupted `piece-layers` entry loads; the layer is absent; a v2-only torrent refetches it through Task 8's `pending_layers` path.
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session resume`. Expected: FAIL.
- [ ] **Step 3: Implement.** Keep `MAX_RESUME_BYTES` and `MAX_RESUME_ITEMS`. Count layer hashes against `MAX_PIECE_LAYER_HASHES` before allocating.
- [ ] **Step 4: Add fuzz seeds.** Add a v2 and a hybrid resume blob to `fuzz/seeds/resume/`.
- [ ] **Step 5: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session`. Expected: PASS.
- [ ] **Step 6: Commit.** `feat(session): resume data version 2 with v2 hashes and piece layers`

### Task 11: `fbt-example create` and the v2/hybrid interop scenarios

**Files:**
- Modify: `crates/fbt-example/src/main.rs`, `dev/interop/run.sh`, `dev/interop/README.md`, `crates/fbt-example/tests/cli.rs`

**Interfaces:**
- Consumes: Task 3 `CreateTorrent`.
- Produces:
  - `fbt-example create <PATH> --out <FILE> [--format v1|v2|hybrid (default hybrid)] [--piece-size BYTES] [--tracker URL]... [--web-seed URL]... [--private] [--comment TEXT]`. It prints `created: <file>` and the same hash lines as `info`.
  - `fbt-example info` prints:
    - `info hash: <wire hash hex>`: `hex(wire_hash())` explicitly (it printed `{info_hashes}`, the full v2 for v2-only), which is qBittorrent's torrent id;
    - the piece count from `piece_layout().num_pieces()` (`info.pieces` is empty for v2-only);
    - `info hash v2: <64 hex>` when present;
    - `version: v1|v2|hybrid`.
  - Interop scenarios `v2` and `hybrid` (added to `known_scenarios` and the header comment):
    - **v2.**
      1. qBittorrent creates a v2 torrent of a fresh payload (mode 0644 files) and fbt downloads it. WebUI `torrentcreator/addTask` (POST: `sourcePath=/interop/<dir>`, `format=v2`, `pieceSize=262144`, `trackers` = the opentracker URL, `startSeeding=true`) returns a `taskID`; poll `torrentcreator/status` until `Finished`, fetch the bytes with `torrentcreator/torrentFile`, then `deleteTask`. Check the `trackers` separator against the 5.3 source.
      2. `fbt-example create --format v2 --piece-size 262144` of a directory with the same final name must print the same `info hash v2` as qBittorrent's `infohash_v2` in `torrents/info` (hard check).
      3. fbt seeds its own v2 torrent and qBittorrent downloads it.
      4. fbt downloads a `btmh`-only magnet (`tr=` the opentracker) from the qBittorrent seed.
      5. qBittorrent downloads fbt's v2 seed by `btmh` magnet.
    - **hybrid.**
      1. qBittorrent creates a hybrid (as v2 step 1, `format=hybrid`) and seeds it, and fbt downloads it.
      2. `fbt-example create` of the same payload: both hashes are compared and logged.
      3. fbt seeds its own hybrid; qBittorrent and Transmission download it.
      4. Transmission fallback: see Decisions.

- [ ] **Step 1: Write the CLI tests.** Add to `crates/fbt-example/tests/cli.rs`: `create_then_info_round_trip` runs the binary with `assert_cmd`-free `std::process::Command` on `env!("CARGO_BIN_EXE_fbt-example")` for each format and checks `version:`, and that `info hash:` is 40 hex characters for V2.
- [ ] **Step 2: Implement** the subcommand and the scenario functions in `run.sh`, reusing `qbt_api`, `wait_for`, `same_data` and `other_client_data`.
- [ ] **Step 3: Run locally** if Docker is available: `INTEROP_ONLY=v2,hybrid bash dev/interop/run.sh`. Expected: `interop: OK`. Otherwise CI's `interop` job is the check; the PR must be green.
- [ ] **Step 4: Commit.** `feat(example): create torrents; test(interop): v2 and hybrid scenarios`

### Task 12: Docs and release notes

**Files:**
- Modify: `CHANGELOG.md`, `README.md`, `CLAUDE.md` (fuzz target list, interop scenario list), `crates/fbt/src/lib.rs` (crate docs: a creation example as a doctest that builds a torrent from a temp dir), `crates/fbt/Cargo.toml` (`tempfile.workspace = true` in dev-dependencies), `docs/benchmarks.md` (a note that hybrids hash twice)

- [ ] **Step 1: Write the `CHANGELOG.md` `## [Unreleased]` entries.**
  - `### Added`: v2/hybrid support, creation, hash messages, both-hash announcing, resume v2, the `hash_proof` fuzz target and the interop scenarios.
  - `### Changed`: `FileEntry::pieces_root` (breaking for struct literals), `Metainfo::piece_layers`, `Info.pieces` empty for v2-only, `Event::TrackerReply::info_hash`, `MetainfoError::V2OnlyUnsupported` and `SessionError::V2OnlyUnsupported` deprecated, `fbt_session::storage::check_pieces`'s new signature, `fbt_wire::Message`'s new variants, resume version 2 (version 1 read), the v2 reserved bit always set, and `fbt-example info` printing the wire hash.
  - `### Removed`: nothing unless a task removed something.
  - PR links use the PR number once known.
- [ ] **Step 2: Check the docs build.** Run `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features && CARGO_INCREMENTAL=0 cargo test --workspace --doc`. Expected: PASS.
- [ ] **Step 3: Commit.** `docs: milestone 6 changelog, README and creation example`

---

## Finishing

Before the PR: run the full gate (`cargo fmt --all --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo test --workspace`, `cargo deny --all-features check`, `bash dev/check-pure-rust.sh`).

Then open PR `feature/m6-v2-hybrid` → `develop`.
- **Checks.** Every CI job must be green, including `interop` and the fuzz runs the PR triggers (`hash_proof`, `metainfo`, `magnet`, `wire_codec`, `resume`).
- **PR body.** It lists the decisions made on the owner's behalf (this plan's Decisions and Pre-flight rulings, plus any taken during execution) and a "Deferred to later milestones" section, and the CHANGELOG PR links carry its number.

Then the Gitflow release:
1. `release/0.6.0` off `develop`.
   - Bump the workspace version to `0.6.0` (the peer-id prefix becomes `-FB0600-` automatically) and run `cargo update -w`.
   - Move `Unreleased` to `## [0.6.0] - <date>` and add the link line.
   - Check `bash dev/test-release-version.sh`, run `bash dev/bench.sh` on the release commit and keep its row.
2. PR into `main`, then the annotated tag `v0.6.0`, then merge `main` back into `develop`.

No attribution lines anywhere.

## Deferred to later milestones (recorded so nobody builds them now)

- **Milestone 7 (`fbt-api`):** HTTP/JSON control of creation and of everything v2 adds; HTTP `Range` serving on `FileReader`.
- **Backlog:**
  - lazy per-file layer fetch (libtorrent's), so a v2-only torrent can start on one-piece files before every layer arrives;
  - block-layer (leaf) hash requests and Merkle-based smart ban (pinpointing a bad block without a second download);
  - serving block-layer requests;
  - BEP 47 `attr` `x`/`h` on created files (executable, hidden) and symlink entries in creation;
  - multi-threaded hashing in `CreateTorrent`;
  - announcing a hash revealed only by metadata (a `btih` magnet of a hybrid is registered under its v2 hash but announces only the v1; pre-flight F18);
  - the milestone 5 backlog, unchanged.
