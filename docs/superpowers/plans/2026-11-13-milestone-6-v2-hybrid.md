# Milestone 6: BitTorrent v2, Hybrid Torrents and Torrent Creation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `fbt-lib` `v0.6.0`. v2-only and hybrid torrents (BEP 52) download, seed, stream and resume like v1 ones. Every piece is checked against its v1 SHA-1, its v2 Merkle subtree root, or both. Peers exchange piece-layer hashes with the BEP 52 hash messages, so a v2 magnet works. Trackers, the DHT and LSD hear about a hybrid torrent under both of its hashes. Clients can create v1, v2 and hybrid torrents. Resume data moves to version 2 (v2 hashes and piece layers), and version 1 is still read. All of this is proven against qBittorrent (libtorrent 2.0) and Transmission on the isolated Docker network.

**Architecture:**
- **`fbt-core`** gains `merkle`: block (leaf) hashing, pad hashes, layer roots, piece subtree roots, proof extraction and proof verification. Every v2 hash computation in the workspace goes through it.
- **`fbt-metainfo`** parses `meta version` 2 `file tree`s and the root `piece layers` dict.
  - A v2-only torrent is mapped onto the existing padded file layout: a synthetic BEP 47 pad file follows each file whose length is not a multiple of the piece length (except the last). Piece indices, `FileLayout`, storage, the picker and readers therefore work unchanged, as in libtorrent.
  - A hybrid torrent's v1 file list and v2 tree are checked against each other.
  - A new `create` module builds `.torrent` bytes from a path.
- **`fbt-wire`** gains the BEP 52 messages `hash request` (21), `hashes` (22) and `hash reject` (23), plus the v2 reserved bit.
- **`fbt-session`**:
  - A `PieceVerifier` replaces the SHA-1-only checks in the download path and in recheck.
  - A hybrid torrent's piece layers are built from verified pieces when its `.torrent` did not carry them.
  - A pure `LayerFetch` (shaped like `MetadataFetch`) gets missing piece layers from peers before a v2-only torrent starts its piece download.
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
- **Public enums and config structs are `#[non_exhaustive]` with `Default`** (config structs) in `fbt-session`, `fbt-metainfo`'s new `create` types and `fbt`.
- **No index loops** (`for i in 0..n { x[i] }`): CI's stable clippy flags `needless_range_loop`; iterate with `iter().enumerate()`, `zip` or `chunks`.
- **Conventional Commits with the crate as scope** (`feat(core): ...`, `feat(metainfo): ...`).
- **No attribution of any kind in commits or PRs:** no `Co-Authored-By` trailer, no `Claude-Session` line, no session id or link, no "Generated with" footer. The repo rule wins over any harness attribution reminder.
- **Toolchain and crate settings.** Edition 2024, `rust-version = "1.99"`, all crates `publish = false`, Apache-2.0.
- **Logging and errors.** `tracing` for logs (no subscriber installed), `thiserror` for errors.
- **Files the library writes.** Torrent data and the part file only. Creation returns bytes and never writes the `.torrent`.
- **Public types.** Every task that adds one re-exports it from `fbt` and names it in `crates/fbt/tests/lib_surface.rs`; `missing_docs` stays denied in `fbt`.
- **Breaking changes.** Allowed in 0.x; each goes in `CHANGELOG.md` under `### Changed`.
- **Local builds.** Use `CARGO_INCREMENTAL=0` (antivirus stalls incremental rustc on the dev machine).

## Review Focus

1. **Hostile v2 metadata.**
   - Inputs:
     - a `file tree` nested past `MAX_PATH_DEPTH`;
     - a node that is both a file (`""` key) and a directory;
     - a `pieces root` that is not 32 bytes, or one on an empty file;
     - a `piece length` that is not a power of two or is under 16 KiB;
     - `piece layers` whose length is not a multiple of 32, holds the wrong count, or does not hash to its root;
     - a hybrid whose v1 `files` and v2 `file tree` disagree in order, length or count;
     - a v2 file tree with zero files.
   - Expected: a `MetainfoError` naming the field, never a panic. A layer that does not hash to its root rejects the torrent.
   - Tests: Task 2 `v2_rejects_bad_trees`, `bad_piece_layers_are_rejected`, `inconsistent_hybrid_is_rejected`; fuzz `metainfo` seeds.
2. **Hostile hash messages.**
   - Inputs:
     - `hashes` for a root we never asked about;
     - the right header with the wrong hash count;
     - a proof that does not verify;
     - `length` 0, not a power of two, or above `MAX_HASH_REQUEST_LEN`;
     - `base layer` above the tree;
     - `index` past the layer;
     - a flood of requests.
   - Expected:
     - Malformed messages close the connection with a protocol error.
     - Unsolicited or unverifiable `hashes` retire the peer as a layer source: it is closed and never dialled again, like bad metadata.
     - Requests beyond `MAX_PENDING_HASH_REQUESTS` or for layers we lack get `hash reject`.
     - Nothing panics or allocates past the caps.
   - Tests: Task 4 `hash_messages_round_trip`, `bad_hash_messages_are_rejected`; Task 6 `unsolicited_hashes_retire_the_source`, `bad_proof_retires_the_source`, `requests_we_cannot_serve_are_rejected`; fuzz targets `wire_codec` (seeds) and `hash_proof`.
3. **Torrent geometry edges.**
   - Inputs:
     - a file exactly one piece long;
     - a file one byte over a piece;
     - a file of one block (under 16 KiB);
     - empty files between non-empty ones;
     - the last file not padded;
     - `piece length` 16 KiB (piece layer height 0);
     - a file whose piece count is not a power of two;
     - a v2-only single-file torrent.
   - Expected: creation, parsing and verification agree. Every piece of a torrent created from a directory verifies against its own `.torrent` (v1 SHA-1, v2 root or both), and file `i`'s bytes sit at the same offsets in the v1 and v2 views.
   - Tests: Task 1 `piece_roots_match_file_roots` (proptest); Task 3 `created_torrents_verify_every_piece` (proptest over file-size lists).
4. **One torrent, two identities.**
   - Inputs:
     - a hybrid added by `.torrent` and then by its `btmh`-only magnet;
     - a `btih` magnet and then the `btmh` magnet of the same hybrid;
     - a `btmh` magnet whose metadata turns out hybrid and whose v1 hash belongs to a torrent already added.
   - Expected: the second add fails with `DuplicateTorrent` when either hash is already registered. A magnet whose revealed v1 hash collides keeps running under its own key, with a warning logged and no alias added. A peer handshaking with either hash reaches the right torrent and gets that same hash back.
   - Tests: Task 5 `hybrid_is_one_torrent_under_both_hashes`, `v2_handshake_reaches_a_hybrid`.
5. **A hybrid whose halves disagree.** A hybrid whose v1 piece hashes are right but whose v2 tree is not (a broken creator), or the reverse.
   - Expected: a piece passes only if every hash it has passes. With layers in the `.torrent`, a v2 mismatch fails the piece (`HashFailed`). Without layers, v1 governs: a built layer that does not hash to its `pieces root` is discarded with a warning, and hash requests for that file are rejected.
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
  - A piece hash is the root of its subtree. The last piece of a file is padded with zero leaves to the full subtree width.
  - A file of at most one piece has no piece layer: its single piece is checked against `pieces root` over the file's own tree width.
  - A piece layer validates when the root of its hashes, padded with `pad_hash(piece_height)` to a power of two, equals `pieces root`.
- **v2-only geometry = libtorrent's.** A v2-only torrent gets a synthetic pad `FileEntry` (path `.pad/<len>`, `pad: true`) after every non-empty file except the last whose length is not a multiple of the piece length. Piece indices on the wire and in storage are global over that padded layout (BEP 52 aligns files to pieces, and libtorrent numbers pieces this way).
  - File indices count the pads, as they already do for hybrids. Empty files take no pieces and get no pad.
  - A single-file v2 torrent is one whose tree has exactly one entry, a file, at the top level. Its path is the sanitized `name`, as for v1.
- **v2 validation.** Rules:
  - `piece length` must be a power of two of at least 16 KiB.
  - Tree depth is capped by `MAX_PATH_DEPTH`, and the file count by the existing item caps.
  - `pieces root` is required for a non-empty file and forbidden for an empty one.
  - File-tree keys go through the existing component sanitizer and collision renames, exactly like v1 paths.
  - Hybrid consistency: the v1 non-pad files and the v2 files must match one to one in order and length, every non-pad file must start on a piece boundary, and a mismatch gives `MetainfoError::InconsistentHybrid`.
  - `V2OnlyUnsupported` is deprecated and no longer produced (kept for compatibility, as `DuplicatePath` was in 0.5.0).
- **Piece layers.**
  - `Metainfo::piece_layers` holds only validated layers, keyed by `pieces root`.
  - In a `.torrent`, a layer that does not validate rejects the torrent (`InvalidField { field: "piece layers", .. }`).
  - A missing layer is allowed: it is fetched from peers (v2-only) or built from verified data (hybrid).
  - Layers for roots no file has are ignored.
  - `MAX_PIECE_LAYER_HASHES` = 4_194_304 hashes in all, the same order as `MAX_INFO_ITEMS`.
- **Verification (`PieceVerifier`).**
  - v1: SHA-1.
  - v2-only: the subtree root against the layer entry, or against `pieces root` for a one-piece file.
  - Hybrid: SHA-1, and also the v2 root when the file's layer is known.
  - A piece passes only if every check it has passes.
  - The hashing job returns the piece's v2 root alongside the verdict, so a hybrid without layers builds them for free. When a file's layer fills, it is validated against `pieces root` and added; one that disagrees is discarded with a `tracing::warn!`.
  - The same verifier runs in recheck.
  - Hybrids hash twice (SHA-1 and SHA-256): accepted cost, recorded in the benchmark notes.
- **Layer fetch, v2-only only.** When a v2-only torrent's metadata (from a magnet or a `.torrent` without layers) lacks a layer, the torrent stays in `TorrentState::DownloadingMetadata` until every layer is fetched. Libtorrent fetches lazily; ours is simpler and costs 32 bytes per piece up front.
  - **Sources.** Peers that set the v2 reserved bit, at most `MAX_LAYER_SOURCES` = 4 at once.
  - **Request size.** Requests of `LAYER_REQUEST_LEN` = 512 hashes at the piece layer, with proof up to the root.
  - **Failures.** Each request times out after 30 s (the source is retired, no ban). `hash reject` moves the span to another source. Hashes that fail their proof, or arrive unrequested, close the peer and add it to the metadata ban set.
  - **Hybrids.** A hybrid never fetches layers; v1 verifies its pieces.
- **Hash serving.**
  - **Requests answered.** `hash request`s for files whose layer we hold, at a base layer at or above the piece layer, are answered from layers computed on demand (`MerkleTree::from_layer`).
  - **Requests rejected.** Requests below the piece layer (we keep no block hashes) or for unknown roots get `hash reject`.
  - **Request validity.** `length` must be a power of two in `1..=MAX_HASH_REQUEST_LEN` (= 8192), `index` a multiple of `length`, and `base layer` below the tree height. Otherwise the message is malformed: the connection closes.
  - **Pending cap.** At most `MAX_PENDING_HASH_REQUESTS` = 64 queued per peer; beyond that, reject.
- **Proof shape.** The answer carries `length` base-layer hashes, then one uncle hash per layer from `base + log2(length)` upward. There are `min(proof_layers, tree_height − base − log2(length))` uncles, bottom-up, where `tree_height` is the height of the file's root.
  - This is our reading of BEP 52 and of libtorrent's `merkle_tree::get_hashes`.
  - The interop `v2` scenario (a `btmh` magnet from qBittorrent, and qBittorrent's magnet from us) is the arbiter. If it shows a different shape, follow libtorrent and correct this paragraph in the same commit.
- **The v2 reserved bit** (`reserved[7] & 0x10`) is set on every handshake we send, as libtorrent does. Hash messages from a peer that did not set it close the connection.
- **Identity.**
  - The registry key stays the `InfoHashes` the torrent was added with (events and handles keep it).
  - `by_wire` (and `by_req2`) hold every wire hash of the torrent: the v1 hash and the truncated v2 hash.
  - When a magnet's metadata reveals a hash the key lacked, the actor registers the alias. If another torrent owns it, the alias is skipped with a warning.
  - `Session::torrent(ih)` finds a torrent by exact key, else by any wire hash of `ih`.
  - An inbound handshake answers with the hash the peer sent. Outbound dials use `wire_hash()` (v1 for hybrids).
- **Announcing.** A hybrid announces under both hashes, the v1 hash then the truncated v2, as libtorrent does, so peers that only know the `btmh` find it.
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
  - **Pads.** Hybrids get `.pad/<n>` pads (`attr` `p`) after every non-empty file except the last whose length is not a multiple of the piece length. v1 gets no pads.
  - **Other keys.** `creation date` and `created by` are left out unless set, so output is deterministic. Each file's length is re-checked after hashing (`FileChanged`).
  - **Caps.** At most `MAX_CREATE_FILES` = 1_000_000 files.
  - **API.** `build` is synchronous and reports progress through a callback that may cancel. Async clients run it on `spawn_blocking`, as documented.
- **Resume data version 2.**
  - `fbt-resume-version` becomes 2.
  - `info-hash` holds the v1 hash and is absent for v2-only.
  - New `info-hash2` (32 bytes) for v2 and hybrid.
  - New `piece-layers`: a dict from `pieces root` to the concatenated layer hashes, every layer the torrent holds (built or fetched ones included). Each layer is validated on load, and a bad one is dropped (refetched or rebuilt), not fatal.
  - At least one of the two hashes is required, and the `info` is checked against each one present.
  - Version 1 is still read (v1 only, as before). Versions above 2 → `ResumeError::UnsupportedVersion`.
- **Interop.** Two new scenarios, `v2` and `hybrid`.
  - **qBittorrent's torrents.** qBittorrent 5.3 creates the v2 and hybrid torrents through its WebUI `torrentcreator` API, so we parse libtorrent's output.
  - **Our torrents.** `fbt-example create` makes ours, and qBittorrent (both formats) and Transmission (hybrid only; it has no v2) download them from an fbt seed.
  - **v2 magnets.** fbt downloads a `btmh`-only magnet from a qBittorrent seed, which proves our layer fetch. qBittorrent downloads an fbt v2 seed by `btmh` magnet, which proves our hash serving.
  - **Cross-check.** fbt's and qBittorrent's v2 info hash for the same payload and piece size must be equal: a hard check, proving tree, ordering and encoding. The hybrid hashes are compared and only logged, since libtorrent may name or attribute pads differently.
  - **Transmission fallback.** If Transmission 4.1 refuses a hybrid, that leg is dropped with a note in `dev/interop/README.md`.
- **New fuzz target:** `hash_proof` (`fbt_core::merkle::verify_hashes` on arbitrary input).
  - `metainfo`, `magnet`, `wire_codec` and `resume` get v2 seeds.
  - The target goes in `.github/workflows/fuzz.yml`'s matrix and its PR `paths` (`crates/fbt-core/**`).
- **Version and changelog.** Version `0.6.0`; changelog entries go under `## [Unreleased]`.

---

## File Structure

```
crates/fbt-core/src/merkle.rs                      new: leaf/pad/root/proof functions, MerkleTree
crates/fbt-core/src/{lib.rs,tests.rs}              export, unit + proptests
crates/fbt-metainfo/Cargo.toml                     sha1, sha2 to [dependencies]
crates/fbt-metainfo/src/metainfo.rs                v2 file tree, piece layers, hybrid check, v2_piece, version()
crates/fbt-metainfo/src/v2.rs                      new: file-tree parse, layer validation (keeps metainfo.rs from growing)
crates/fbt-metainfo/src/create.rs                  new: CreateTorrent, TorrentVersion, CreateError, auto_piece_len
crates/fbt-metainfo/tests/{metainfo.rs,create.rs,support/mod.rs,fixtures/*}
crates/fbt-wire/src/{message.rs,codec.rs,handshake.rs,lib.rs}   hash request/hashes/reject, v2 bit
crates/fbt-wire/tests/wire.rs
crates/fbt-session/src/storage/check.rs            PieceVerifier, check_pieces over it
crates/fbt-session/src/torrent/{data.rs,check.rs}  verifier in the hash job and recheck, layer building
crates/fbt-session/src/torrent/layers.rs           new: LayerFetch (pure), LayerBuilder, hash serving helpers
crates/fbt-session/src/torrent/{actor.rs,metadata.rs,peers.rs,magnet.rs,announcer.rs,dht.rs,swarm.rs,mod.rs}
crates/fbt-session/src/peer/conn/{inbound.rs,mod.rs}, peer/messages.rs, listener.rs   hash messages, echo hash
crates/fbt-session/src/{session.rs,resume.rs,event.rs,net.rs}
crates/fbt-session/tests/{v2.rs,resume.rs,trackers.rs,dht.rs,lsd.rs}, tests/support/mod.rs
crates/fbt/src/lib.rs, crates/fbt/tests/lib_surface.rs
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
- Modify: `crates/fbt-core/src/lib.rs`, `crates/fbt-core/src/tests.rs`

**Interfaces:**
- Produces (all in `fbt_core::merkle`, re-exported as `fbt_core::merkle`):
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
    - Assertion: `layer_root(layer, h, next_pow2(n_pieces))` equals the file root `layer_root(&leaf_hashes(file), 0, next_pow2(n_leaves))`.
  - `proofs_verify` (proptest):
    - Inputs: a random layer of up to 300 hashes at height 2, tree height `2 + log2(next_pow2(len))`; every power-of-two `len` up to 64 at aligned `index`; `proof_layers` from 0 to 20.
    - Assertion: `verify_hashes(root, ..)` accepts exactly the `(hashes, proof)` that `MerkleTree::hashes` returns with enough proof layers to reach the root.
    - Assertion: flipping any byte of any hash or uncle makes it false.
  - `verify_rejects_garbage`: wrong counts, too few or too many uncles, `index` not a multiple of the span, `base > tree_height`, `tree_height > MAX_TREE_HEIGHT`, and an empty `hashes` all return false.
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-core`. Expected: FAIL (unresolved `merkle`).
- [ ] **Step 3: Implement `crates/fbt-core/src/merkle.rs`** with the signatures above. Add a `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::panic)]` module attribute, and export it with `pub mod merkle;` from `lib.rs`.
- [ ] **Step 4: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-core` and `cargo clippy -p fbt-core --all-targets -- -D warnings`. Expected: PASS, no warnings.
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
  - `impl Metainfo { pub fn version(&self) -> TorrentVersion; pub fn v2_piece(&self, piece: u32) -> Option<V2Piece>; pub fn with_piece_layers(self, layers: BTreeMap<[u8;32], Vec<[u8;32]>>) -> Metainfo }`. `with_piece_layers` keeps only the layers `validate_layer` accepts for a file of this torrent.
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub struct V2Piece { pub file: usize, pub pieces_root: [u8; 32], pub index_in_file: u32, pub expected: Option<[u8; 32]>, pub leaves: u32 }`:
    - `expected` is the layer entry, or `pieces_root` for a one-piece file, or `None` when the layer is missing.
    - `leaves` is the width `piece_root` pads to: `piece_len / 16 KiB` for files with a layer, the file's own `2^file_tree_height(len)` for one-piece files.
  - `pub fn validate_layer(pieces_root: &[u8; 32], file_len: u64, piece_len: u32, layer: &[[u8; 32]]) -> bool`
  - `pub const MAX_PIECE_LAYER_HASHES: usize = 4_194_304;`
  - `MetainfoError::InconsistentHybrid` (new variant); `V2OnlyUnsupported` gets `#[deprecated]`, documented as no longer produced.
  - `Info.pieces` is empty for v2-only torrents.
  - `Metainfo::from_info_bytes` accepts v2-only `info` dicts (it has no piece layers: `piece_layers` is empty).

- [ ] **Step 1: Extend the fixture helper.** In `tests/support/mod.rs`, add a deterministic `build_v2(files: &[(&[&str], u64)], piece_len: u32, hybrid: bool, with_layers: bool) -> Vec<u8>` that builds real roots and layers with `fbt_core::merkle` from xorshift content. Regenerate `hybrid.torrent` (now with a real tree and layers), and add:
  - `v2.torrent`: three files, sizes 20 000, 0 and 70 000, 32 KiB pieces;
  - `v2-single.torrent`: one 50 000-byte file.

  Update the fixtures README.
- [ ] **Step 2: Write the failing tests** in `tests/metainfo.rs`:
  - `v2_only_parses_with_synthetic_pads`:
    - `v2.torrent` → `version() == V2`, `info_hashes.v1 == None`.
    - Files: `[f0 20000, .pad/12768 pad, f1 0, f2 70000]`.
    - `piece_layout().num_pieces() == 1 + 3`.
    - `piece_layers` has f2's layer (3 hashes) only (f0 is one piece: no layer).
  - `v2_single_file`: `v2-single.torrent` → one file, `single_file`, path is the name.
  - `hybrid_parses_both_halves`:
    - `version() == Hybrid`, and both hashes are set.
    - `pieces_root` is set on non-pad files.
    - `v2_piece(p).expected` equals the layer entry for multi-piece files.
  - `v2_piece_geometry`:
    - f0 (one piece; 20 000 bytes = 2 leaves) has `expected == pieces_root` and `leaves == 2`.
    - f2's pieces have `leaves == 2` (32 KiB / 16 KiB) and `index_in_file` 0, 1, 2.
    - Every v2 piece maps to exactly one non-pad file.
  - `missing_layer_is_allowed`: `with_layers = false` parses, `v2_piece(p).expected == None` for multi-piece files.
  - `bad_piece_layers_are_rejected`: each of a layer whose length is not a multiple of 32, one with the wrong count, and one with a flipped byte gives `InvalidField { field: "piece layers", .. }`.
  - `v2_rejects_bad_trees`: each malformed tree in Review Focus 1 gives an error, never a panic:
    - depth over `MAX_PATH_DEPTH`;
    - a node with `""` plus children;
    - a 31-byte `pieces root`;
    - a `pieces root` on an empty file;
    - piece length 16 383 and 48 KiB;
    - an empty tree.
  - `inconsistent_hybrid_is_rejected`: a hybrid whose v1 list swaps two files, or changes a length, or drops a pad, gives `InconsistentHybrid`.
  - `v2_info_from_peers`: `from_info_bytes(raw_info_of(v2.torrent), InfoHashes::from_v2(..), vec![])` parses with empty `piece_layers`.
  - `with_piece_layers_keeps_valid_ones`: a map holding f2's correct layer, plus a bad one and one for an unknown root → only f2's is kept.
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
  - `#[derive(Debug, Clone)] #[non_exhaustive] pub struct CreateTorrent`, built with `CreateTorrent::new(path: impl Into<PathBuf>) -> Self` and chained setters:
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
    - Inputs: up to 5 files with sizes in `0..200_000` under nested dirs, piece length 16/32 KiB, each `TorrentVersion`.
    - Assertion: `build` → `Metainfo::from_bytes` succeeds and `version()` matches.
    - Assertion: for every piece, reading its bytes from the files (pads as zeros) gives `sha1 == info.pieces[p]` (v1, hybrid) and `piece_root(data, v2_piece.leaves) == v2_piece.expected` (v2, hybrid).
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
  - `file_changed_while_hashing`: a progress callback that appends to a file on its first call gives `FileChanged`.
  - `single_file_torrent`: a path to one file gives `single_file` and a name equal to the file name.
  - `cancel_stops_hashing`: a callback returning `Break` gives `Cancelled`.
  - `private_trackers_and_seeds_round_trip`: the setters appear in the parsed `Metainfo`.
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-metainfo --test create`. Expected: FAIL.
- [ ] **Step 3: Implement `create.rs`** as Decisions "Torrent creation".
  - Hash in one pass over the files with one piece-sized buffer: SHA-1 over the padded v1 stream and per-file 16 KiB leaves for v2.
  - Encode with `fbt_bencode`'s encoder. Info keys are in bencode order: `file tree`, `files` (hybrid and v1 multi), `length` (single v1 and hybrid), `meta version` 2, `name`, `piece length`, `pieces`, `private`.
  - The root dict holds `announce`, `announce-list`, `comment`, `created by`, `creation date`, `info`, `piece layers` and `url-list`.
- [ ] **Step 4: Re-export from `fbt`.** Re-export `CreateTorrent`, `CreateError`, `TorrentVersion`, `auto_piece_len`, `V2Piece` and `MAX_CREATE_FILES` from `fbt` and add them to `lib_surface.rs`.
- [ ] **Step 5: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-metainfo -p fbt && bash dev/check-pure-rust.sh`. Expected: PASS.
- [ ] **Step 6: Commit.** `feat(metainfo): create v1, v2 and hybrid torrents`

### Task 4: BEP 52 hash messages in `fbt-wire`

**Files:**
- Modify: `crates/fbt-wire/src/{message.rs,codec.rs,handshake.rs,lib.rs}`
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

  A semantically invalid header (`length` not a power of two, too large) decodes fine; the session judges it (Task 6). The existing max message size bounds `hashes`.

- [ ] **Step 1: Write the failing tests** in `tests/wire.rs`:
  - `hash_messages_round_trip`: encode/decode equality for all three, with ids 21, 22 and 23 on the wire.
  - `bad_hash_messages_are_rejected`: 47- and 49-byte request bodies, and a `hashes` body of 48 + 31 bytes, are errors.
  - `v2_reserved_bit`: `set_v2` sets exactly `reserved[7] |= 0x10` and leaves the other bits alone.
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-wire`. Expected: FAIL.
- [ ] **Step 3: Implement the messages and the reserved bit.**
- [ ] **Step 4: Add fuzz seeds.** Add one seed per message to `fuzz/seeds/wire_codec/`.
- [ ] **Step 5: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-wire && CARGO_INCREMENTAL=0 cargo test --workspace` (non-exhaustive matches elsewhere still compile). Expected: PASS.
- [ ] **Step 6: Commit.** `feat(wire): BEP 52 hash request, hashes and hash reject`

### Task 5: Session verification, identity and v2/hybrid swarms

**Files:**
- Modify: `crates/fbt-session/src/storage/check.rs`, `crates/fbt-session/src/torrent/{data.rs,check.rs,actor.rs,mod.rs}`, `crates/fbt-session/src/session.rs`, `crates/fbt-session/src/peer/conn/{mod.rs,inbound.rs}`, `crates/fbt-session/src/listener.rs`
- Create: `crates/fbt-session/src/torrent/layers.rs` (the `LayerBuilder` part; Task 6 adds the rest)
- Test: `crates/fbt-session/tests/v2.rs` (new), `crates/fbt-session/tests/support/mod.rs`

**Interfaces:**
- Consumes: Task 2 `Metainfo::{version, v2_piece, with_piece_layers}`, `validate_layer`; Task 1 `piece_root`; Task 3 `CreateTorrent` (tests make their torrents with it); Task 4 `Reserved::set_v2`.
- Produces:
  - `pub(crate) struct PieceCheck { pub ok: bool, pub v2_root: Option<[u8; 32]> }` and `pub(crate) fn verify_piece(meta: &Metainfo, piece: u32, data: &[u8]) -> PieceCheck` in `storage/check.rs`:
    - v1: SHA-1.
    - v2: requires `expected`; `ok = false` when it is `None` (Task 6 makes sure v2-only torrents never pick such pieces).
    - hybrid: SHA-1, plus v2 when `expected` is `Some`.
    - `v2_root` is the computed subtree root (v2 and hybrid).
  - `check_pieces(storage, layout, meta: &Metainfo, on_piece)` returns `(Bitfield, Vec<Option<[u8;32]>>)`: the `v2_root` of every passing piece.
  - `pub(super) struct LayerBuilder` in `torrent/layers.rs`: `fn new(meta: &Metainfo) -> Option<Self>` (`None` unless hybrid with missing layers), `fn record(&mut self, piece: u32, root: [u8; 32]) -> Option<([u8;32], Vec<[u8;32]>)>` (returns a completed and validated layer; discards and warns on a mismatch), `fn forget(&mut self, piece: u32)` (a piece lost by recheck).
  - The actor swaps `Arc<Metainfo>` for `meta.with_piece_layers(..)` when a layer completes.
  - `TorrentEntry.wire_hash` becomes `wire_hashes: Vec<[u8; 20]>` (1 or 2).
  - `SessionInner::add_alias(&self, key: &InfoHashes, wire: [u8; 20]) -> bool` is called by the actor when metadata reveals a new hash.
  - `Session::torrent(ih)` falls back to `by_wire` over `ih`'s hashes.
  - `pub(crate) fn wire_hashes(ih: &InfoHashes) -> Vec<[u8; 20]>`: the v1 hash, then the truncated v2, either one when only it exists. It goes in `session.rs` and is used by Task 7 too.

- [ ] **Step 1: Write the failing tests** in `tests/v2.rs` (two sessions on loopback, torrents from `CreateTorrent` over a `TempDir` payload of three files: 100 000, 0 and 300 000 bytes; 32 KiB pieces):
  - `v2_torrent_downloads_and_seeds`: seed and leech a V2 torrent; data equal; the leech's `TorrentFinished`.
  - `hybrid_torrent_downloads`: the same for Hybrid.
  - `hybrid_piece_must_pass_both`:
    - Inputs: a hybrid `.torrent` whose `piece layers` entry for the 300 000-byte file is replaced by a valid-looking layer of a different file of the same size. Build it with `CreateTorrent` on altered data, then splice the layer and `pieces root` in with `fbt_bencode`. Run `verify_piece` on correct data for one of its pieces.
    - Assertion: `ok == false`.
    - Run the same at the session level: the leech emits `HashFailed` and never finishes.
  - `built_layer_that_disagrees_is_discarded` (unit test in `layers.rs`): `LayerBuilder::record` fed roots that do not hash to `pieces_root` returns `None` once full, and the layer is absent.
  - `hybrid_without_layers_builds_them`: a hybrid `.torrent` stripped of `piece layers` downloads; afterwards, the leech's resume data (Task 8) or a test-only accessor shows the layer as present. Use `TorrentHandle::piece_layers_known() -> usize`, a `#[doc(hidden)] pub` test hook. Name it in `lib_surface.rs` as hidden.
  - `recheck_verifies_v2`: a seeded V2 torrent with one byte flipped on disk rechecks to all pieces but one.
  - `hybrid_is_one_torrent_under_both_hashes`: add a hybrid by `.torrent`, then `add_torrent` of its `btmh`-only magnet, then its `btih`-only magnet → both `DuplicateTorrent`. `session.torrent(&InfoHashes::from_v2(v2))` finds it.
  - `v2_handshake_reaches_a_hybrid`: a raw TCP client handshakes with the truncated v2 hash of a hybrid the session seeds and receives a handshake carrying that same hash and the v2 reserved bit.
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session --test v2`. Expected: FAIL.
- [ ] **Step 3: Implement `verify_piece` and `check_pieces` over it.**
  - Use `verify_piece` in `spawn_hash` (`Job::Hashed` gains `v2_root`) and in `torrent/check.rs`.
  - Feed `LayerBuilder` from both.
  - Smart ban's per-block SHA-1 is unchanged.
- [ ] **Step 4: Implement registry aliases and the handshake.**
  - Registry aliases: `by_wire`/`by_req2` entries for each wire hash, the duplicate check over all of them, `add_alias`, and the `torrent()` fallback.
  - Inbound connections echo the peer's hash.
  - Every handshake we send sets the v2 bit.
- [ ] **Step 5: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session` (the whole crate: v1 paths must be unchanged). Expected: PASS.
- [ ] **Step 6: Commit.** `feat(session): verify v2 and hybrid pieces and register every wire hash`

### Task 6: Hash serving and layer fetch (v2 magnets)

**Files:**
- Modify: `crates/fbt-session/src/torrent/{layers.rs,actor.rs,magnet.rs,metadata.rs,peers.rs,mod.rs}`, `crates/fbt-session/src/peer/{messages.rs,conn/inbound.rs,conn/mod.rs}`
- Create: `fuzz/fuzz_targets/hash_proof.rs`, `fuzz/seeds/hash_proof/`
- Modify: `fuzz/Cargo.toml`, `.github/workflows/fuzz.yml`
- Test: `crates/fbt-session/tests/v2.rs`, unit tests in `layers.rs`

**Interfaces:**
- Consumes: Task 1 `MerkleTree`, `verify_hashes`, `file_tree_height`; Task 4 messages and `MAX_HASH_REQUEST_LEN`, `supports_v2`; Task 5 `with_piece_layers`, the `DownloadingMetadata` state path of `torrent/magnet.rs`.
- Produces:
  - In `layers.rs`:
    - `pub(super) const LAYER_REQUEST_LEN: u32 = 512;`
    - `pub(super) const MAX_LAYER_SOURCES: usize = 4;`
    - `pub(super) const MAX_PENDING_HASH_REQUESTS: usize = 64;`
    - `pub(super) const LAYER_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);`
    - `pub(super) fn serve_hash_request(meta: &Metainfo, req: &HashRequest) -> Result<Vec<u8>, ServeError>`, where `ServeError` is `Malformed` (close the peer) or `Reject` (send `hash reject`). It returns the hash bytes (base layer, then uncles).
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
  - Requests are spans of `LAYER_REQUEST_LEN` (the last one rounded up to a power of two and clipped by the server's padding) at the piece layer. `proof_layers` reaches the root.
- Flow:
  1. A v2-only torrent whose metadata is complete but `LayerFetch::new` is `Some` stays in `DownloadingMetadata`.
  2. It feeds peers with `supports_v2` to `add_source`.
  3. On completion it swaps in `meta.with_piece_layers(fetch.into_layers())` and starts like a magnet that just got its metadata (storage built, check, download).
  4. `Bad` closes the peer with the metadata-ban reason.
  5. Every torrent answers `HashRequest` through `serve_hash_request`, at most `MAX_PENDING_HASH_REQUESTS` per peer in flight (beyond → reject). Hash messages from a peer without the v2 bit, or on a v1-only torrent, close it.

- [ ] **Step 1: Write the failing unit tests** in `layers.rs`:
  - `serve_answers_piece_layer_spans`: the served bytes for `(root, piece_height, 0, 4, 10)` verify with `verify_hashes`.
  - `requests_we_cannot_serve_are_rejected`:
    - `base < piece_height` → `Reject`;
    - unknown root → `Reject`;
    - `length` 3 → `Malformed`;
    - `length` 16384 → `Malformed`;
    - `index` 5 with `length` 4 → `Malformed`;
    - `base` above the tree → `Malformed`.
  - `fetch_completes_from_one_source`: a `LayerFetch` over a 3-file v2 meta with layers stripped, answered by `serve_hash_request` on the full meta, completes, and `into_layers` equals the full meta's.
  - `unsolicited_hashes_retire_the_source`: `on_hashes` for a span not requested → `Bad`.
  - `bad_proof_retires_the_source`: one flipped byte → `Bad`.
  - `reject_moves_the_span`: `on_reject` re-sends the span to the other source.
  - `timeout_retires_without_ban`: past `LAYER_REQUEST_TIMEOUT`, `tick` re-sends elsewhere and the first source is not `Bad`.
- [ ] **Step 2: Write the failing session tests** in `tests/v2.rs`:
  - `v2_magnet_downloads`: a seed with a V2 torrent (the 300 000-byte file gives it a layer); a leech adds the `btmh`-only magnet with the seed as a peer, gets metadata and layers, and finishes with data equal.
  - `v2_torrent_without_layers_fetches_them`: a V2 `.torrent` stripped of `piece layers` downloads the same way.
  - `hybrid_magnet_downloads_without_layer_fetch`: the btmh magnet of a hybrid finishes; the seed's log or a counter shows no hash request was served (use the `piece_layers_known` hook on the leech after finishing: equal to the number of multi-piece files, built from data).
  - `seed_without_v2_peers_stays_in_metadata`: the leech only connects to a peer that never sets the v2 bit (a scripted peer from `tests/support`) and stays `DownloadingMetadata`, with no panic and no piece requests.
- [ ] **Step 3: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session layers && CARGO_INCREMENTAL=0 cargo test -p fbt-session --test v2`. Expected: FAIL.
- [ ] **Step 4: Implement serving, the fetch, the flow and the peer-message plumbing.**
- [ ] **Step 5: Add the fuzz target `hash_proof`.** It parses arbitrary bytes into `(root, tree_height, base, index, hashes, proof)` and calls `verify_hashes`. Add seeds from the unit test's served spans, then add it to `fuzz/Cargo.toml`, the `fuzz.yml` matrix and the `paths` (`crates/fbt-core/**`).
- [ ] **Step 6: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session`. Expected: PASS. Run `cargo +nightly fuzz build hash_proof` if nightly is installed (else note it for CI).
- [ ] **Step 7: Commit.** `feat(session): serve and fetch BEP 52 piece layers`

### Task 7: Announce both hashes of a hybrid

**Files:**
- Modify: `crates/fbt-session/src/torrent/{announcer.rs,tiers.rs,dht.rs,swarm.rs,commands.rs}`, `crates/fbt-session/src/net.rs`, `crates/fbt-session/src/event.rs`, `crates/fbt-session/src/torrent/actor.rs` (LSD `want`)
- Test: `crates/fbt-session/tests/{trackers.rs,dht.rs,lsd.rs}`

**Interfaces:**
- Consumes: Task 5 `wire_hashes(&InfoHashes) -> Vec<[u8; 20]>`.
- Produces:
  - `Event::TrackerReply` gains `info_hash: [u8; 20]` (breaking; `### Changed`).
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

### Task 8: Resume data version 2

**Files:**
- Modify: `crates/fbt-session/src/resume.rs`, `crates/fbt-session/src/torrent/{commands.rs,actor.rs}`
- Test: `crates/fbt-session/tests/resume.rs`, unit tests in `resume.rs`, `fuzz/seeds/resume/`

**Interfaces:**
- Consumes: Task 2 `with_piece_layers`, `validate_layer`; Task 5/6 the actor's current `Arc<Metainfo>` (layers included).
- Produces:
  - `RESUME_VERSION` = 2, written always.
  - New keys `info-hash2` (32 bytes) and `piece-layers` (dict, root → concatenated hashes).
  - `info-hash` becomes optional (absent for v2-only).
  - `decode_resume` accepts versions 1 and 2, refuses others with the existing `UnsupportedVersion`.
  - On load the `info` must hash to every hash present. Layers are applied through `with_piece_layers` (invalid ones dropped), and a v2-only torrent lacking one fetches it (Task 6).
  - Update the module's key table doc.

- [ ] **Step 1: Write the failing tests.**
  - `resume.rs` unit `version_1_still_reads`: a version 1 blob (the current encoder's output with version 1, kept as a committed byte fixture under `crates/fbt-session/tests/fixtures/resume-v1.bin`) decodes.
  - `version_3_is_refused`.
  - `v2_resume_requires_a_matching_hash`: `info-hash2` not matching `info` → `ResumeError::Mismatch` (or the existing mismatch variant).
  - Session `tests/resume.rs` `v2_magnet_resume_skips_layer_fetch`: a leech gets a V2 torrent by magnet (layers fetched), pauses, saves, shuts down, and restores in a session with no peers. Status shows `resume_trusted` and pieces intact, and the torrent is not in `DownloadingMetadata`.
  - `hybrid_resume_keeps_built_layers`: the layers built from data survive a save and restore (`piece_layers_known`).
  - `bad_layer_in_resume_is_dropped`: a corrupted `piece-layers` entry loads; the layer is absent; a v2-only torrent refetches it.
- [ ] **Step 2: Run them to see them fail.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session resume`. Expected: FAIL.
- [ ] **Step 3: Implement.** Keep `MAX_RESUME_BYTES` and `MAX_RESUME_ITEMS`. Count layer hashes against `MAX_PIECE_LAYER_HASHES` before allocating.
- [ ] **Step 4: Add fuzz seeds.** Add a v2 and a hybrid resume blob to `fuzz/seeds/resume/`.
- [ ] **Step 5: Run the tests to see them pass.** Run `CARGO_INCREMENTAL=0 cargo test -p fbt-session`. Expected: PASS.
- [ ] **Step 6: Commit.** `feat(session): resume data version 2 with v2 hashes and piece layers`

### Task 9: `fbt-example create` and the v2/hybrid interop scenarios

**Files:**
- Modify: `crates/fbt-example/src/main.rs`, `dev/interop/run.sh`, `dev/interop/README.md`, `crates/fbt-example/tests/cli.rs`

**Interfaces:**
- Consumes: Task 3 `CreateTorrent`.
- Produces:
  - `fbt-example create <PATH> --out <FILE> [--format v1|v2|hybrid (default hybrid)] [--piece-size BYTES] [--tracker URL]... [--web-seed URL]... [--private] [--comment TEXT]`. It prints `created: <file>` and the same hash lines as `info`.
  - `fbt-example info` prints:
    - `info hash: <wire hash hex>` (unchanged meaning: v1, else truncated v2, which is qBittorrent's torrent id);
    - `info hash v2: <64 hex>` when present;
    - `version: v1|v2|hybrid`.
  - Interop scenarios `v2` and `hybrid` (added to `known_scenarios` and the header comment):
    - **v2.**
      1. qBittorrent creates a v2 torrent of a fresh payload (WebUI `torrentcreator/addTask`, `format=v2`, `pieceSize=262144`, the opentracker URL), adds it as a seed, and fbt downloads it.
      2. `fbt-example create --format v2 --piece-size 262144` of the same payload must print the same `info hash v2` as qBittorrent's torrent (hard check).
      3. fbt seeds its own v2 torrent and qBittorrent downloads it.
      4. fbt downloads a `btmh`-only magnet (`tr=` the opentracker) from the qBittorrent seed.
      5. qBittorrent downloads fbt's v2 seed by `btmh` magnet.
    - **hybrid.**
      1. qBittorrent creates a hybrid and seeds it, and fbt downloads it.
      2. `fbt-example create` of the same payload: both hashes are compared and logged.
      3. fbt seeds its own hybrid; qBittorrent and Transmission download it.
      4. Transmission fallback: see Decisions.

- [ ] **Step 1: Write the CLI tests.** Add to `crates/fbt-example/tests/cli.rs`: `create_then_info_round_trip` runs the binary with `assert_cmd`-free `std::process::Command` on `env!("CARGO_BIN_EXE_fbt-example")` for each format and checks `version:`.
- [ ] **Step 2: Implement** the subcommand and the scenario functions in `run.sh`, reusing `qbt_api`, `wait_for`, `same_data` and `other_client_data`.
- [ ] **Step 3: Run locally** if Docker is available: `INTEROP_ONLY=v2,hybrid bash dev/interop/run.sh`. Expected: `interop: OK`. Otherwise CI's `interop` job is the check; the PR must be green.
- [ ] **Step 4: Commit.** `feat(example): create torrents; test(interop): v2 and hybrid scenarios`

### Task 10: Docs and release notes

**Files:**
- Modify: `CHANGELOG.md`, `README.md`, `CLAUDE.md` (fuzz target list, interop scenario list), `crates/fbt/src/lib.rs` (crate docs: a creation example as a doctest that builds a torrent from a temp dir), `docs/benchmarks.md` (a note that hybrids hash twice)

- [ ] **Step 1: Write the `CHANGELOG.md` `## [Unreleased]` entries.**
  - `### Added`: v2/hybrid support, creation, hash messages, both-hash announcing, resume v2, the `hash_proof` fuzz target and the interop scenarios.
  - `### Changed`: `FileEntry::pieces_root`, `Metainfo::piece_layers`, `Event::TrackerReply::info_hash`, `V2OnlyUnsupported` deprecated, resume version 2 (version 1 read), and the v2 reserved bit always set.
  - `### Removed`: nothing unless a task removed something.
  - PR links use the PR number once known.
- [ ] **Step 2: Check the docs build.** Run `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features && CARGO_INCREMENTAL=0 cargo test --workspace --doc`. Expected: PASS.
- [ ] **Step 3: Commit.** `docs: milestone 6 changelog, README and creation example`

---

## Finishing

Before the PR: run the full gate (`cargo fmt --all --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo test --workspace`, `cargo deny --all-features check`, `bash dev/check-pure-rust.sh`).

Then open PR `feature/m6-v2-hybrid` → `develop`.
- **Checks.** Every CI job must be green, including `interop` and the fuzz runs the PR triggers (`hash_proof`, `metainfo`, `magnet`, `wire_codec`, `resume`).
- **PR body.** It lists the decisions made on the owner's behalf (this plan's Decisions, plus any taken during execution) and a "Deferred to later milestones" section, and the CHANGELOG PR links carry its number.

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
  - the milestone 5 backlog, unchanged.
