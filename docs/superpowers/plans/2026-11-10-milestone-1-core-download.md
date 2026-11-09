# Milestone 1: Core Download Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `fbt-lib` `v0.1.0`: a Cargo workspace that downloads a v1 torrent from real peers found through HTTP or UDP trackers over TCP, verifies it, seeds it back, and exposes a minimal async session API, with CI, security scans, fuzzing and a Docker interop job.

**Architecture:** Small from-scratch protocol crates (`fbt-bencode`, `fbt-core`, `fbt-metainfo`, `fbt-wire`, `fbt-tracker`) under a `fbt-session` engine (one actor task per torrent owning the picker and choker, one task per peer connection, disk I/O and hashing on tokio's blocking pool), re-exported by the facade crate `fbt`. The status of each torrent is published through a `watch` channel so `status()` is a synchronous, allocation-light read; events go out on a `broadcast` channel wrapped in a filtering stream that reports lag.

**Tech Stack:** Rust 2024 edition, tokio, tokio-util codecs, bytes, sha1 + sha2 (RustCrypto), hyper 1 + hyper-util + hyper-rustls (ring by default, rustls-rustcrypto with `pure-rust`), webpki-roots, thiserror, tracing, bitflags, rand, proptest, cargo-fuzz, clap (example CLI only).

**Spec:** `docs/superpowers/specs/2026-11-10-fbt-lib-design.md` (binding). Milestone 1 is its section 8 item 1.

## Global Constraints

- Every BitTorrent component is written from scratch; no BitTorrent crates (librqbit, `mainline`, uTP crates, bencode crates such as `serde_bencode`/`bendy`). General-purpose crates only.
- `#![forbid(unsafe_code)]` at the top of every crate's `lib.rs`/`main.rs`.
- No panic, hang or unbounded allocation from remote input (peers, trackers, `.torrent` bytes). `unwrap`/`expect`/indexing on remote-derived values is a review rejection; clippy lints `unwrap_used`, `expect_used`, `indexing_slicing`, `panic` set to `deny` in non-test code of `fbt-bencode`, `fbt-metainfo`, `fbt-wire`, `fbt-tracker`.
- Edition `2024`, `rust-version = "1.99"` (stable when milestone 1 started), workspace version `0.0.0` until the `release/0.1.0` branch bumps it.
- License Apache-2.0 for every crate; `NOTICE` at the root.
- Default build may need a C toolchain only for `ring`. `cargo tree -e normal --no-default-features --features pure-rust -p fbt` must contain neither `ring` nor `cc`.
- All crates `publish = false` until the owner decides on crates.io.
- `tracing` for logs; the library never installs a subscriber. `thiserror` for errors.
- Builds and tests pass on macOS, Linux and Windows.
- Gitflow: work on `feature/m1-core-download` off `develop`; Conventional Commits with crate scope (`feat(bencode): ...`); **no `Co-Authored-By` trailer, no session id or link, no "Generated with" footer in any commit or PR.**
- Naming libtorrent, qBittorrent, Transmission, opentracker is allowed in this repo.
- Public enums and config structs of `fbt-session`/`fbt` are `#[non_exhaustive]`; config structs implement `Default`.

## Review Focus

1. **A peer sends a `piece` we never requested, with the wrong length, or after we cancelled it.** Expected: the data is dropped, never written or counted as progress, and the connection survives (wasted bytes are counted). Test: Task 12 `unrequested_or_wrong_length_piece_is_dropped`.
2. **A tracker returns our own address, so we dial ourselves.** Expected: the handshake's peer id equals ours, the connection closes with `DisconnectReason::SelfConnection`, and the address is never retried. Test: Task 14 `self_connection_is_dropped_and_not_retried`.
3. **The save path is unwritable, or the disk fills mid-download.** Expected: that torrent moves to `TorrentState::Error`, emits `Event::FileError`, and the session and other torrents keep running. Test: Task 13 `unwritable_save_path_puts_torrent_in_error`.
4. **Zero-length files, or pieces spanning many tiny files.** Expected: the layout maps the bytes correctly, zero-length files get created, and verification passes. Tests: Task 4 `file_layout_spans_tiny_and_empty_files`, Task 10 `fs_storage_creates_empty_files`.
5. **A seeder disconnects halfway through a piece.** Expected: its outstanding blocks go back to the picker and another peer finishes the piece. Test: Task 14 `download_survives_seeder_leaving_mid_piece`.

---

## Repository bootstrap (done while writing this plan)

`main` holds one `chore: initial commit` with `LICENSE`; `develop` branches from it and carries `docs: add design and milestone 1 plan` (spec and this plan). Execution starts with `git -C fbt-lib switch -c feature/m1-core-download develop`.

## File Structure

```
Cargo.toml                    workspace, [workspace.package], [workspace.dependencies], [workspace.lints]
rustfmt.toml  deny.toml  .gitignore  .gitattributes  .gitleaks.toml  .grype.yaml  .semgrepignore
LICENSE  NOTICE  README.md  CHANGELOG.md  CLAUDE.md
.github/workflows/{ci.yml,security.yml,fuzz.yml,release.yml}
dev/{sast.sh,sast.ps1,check-pure-rust.sh,release-version.sh}
dev/interop/{compose.yaml,run.sh,README.md,qbittorrent/,transmission/}
crates/fbt-bencode/src/{lib.rs,decode.rs,encode.rs,value.rs}
crates/fbt-core/src/{lib.rs,info_hash.rs,peer_id.rs,bitfield.rs,layout.rs}
crates/fbt-metainfo/src/{lib.rs,path.rs,metainfo.rs}         tests/fixtures/*.torrent
crates/fbt-wire/src/{lib.rs,handshake.rs,message.rs,codec.rs,fast.rs}
crates/fbt-tracker/src/{lib.rs,types.rs,http.rs,udp.rs,urlencode.rs,tls.rs}
crates/fbt-session/src/{lib.rs,settings.rs,error.rs,event.rs,status.rs,rate.rs,picker.rs,choker.rs,
                        storage/{mod.rs,fs.rs,mem.rs,check.rs},peer/{mod.rs,conn.rs,messages.rs},
                        torrent/{mod.rs,actor.rs,announcer.rs,tiers.rs},session.rs,listener.rs}
crates/fbt-session/tests/{lifecycle.rs,swarm.rs,trackers.rs,support/mod.rs}
crates/fbt/src/lib.rs
crates/fbt-example/src/main.rs
fuzz/                          cargo-fuzz crate, excluded from the workspace
```

---

### Task 1: Workspace, CI and security scaffold

**Files:**
- Create: everything in the root list above except `fuzz.yml`, `release.yml`, `dev/check-pure-rust.sh`, `dev/release-version.sh`, `dev/interop/` (later tasks); `crates/*/Cargo.toml` and `crates/*/src/lib.rs` stubs for all eight crates (`fbt-example` with `src/main.rs`).
- Copy and adapt from `../terminal/`: `.github/workflows/security.yml` (drop the `npm-audit` job and every `website` mention; keep semgrep with `--config p/default --config p/rust` only), `dev/sast.sh` and `dev/sast.ps1` (drop the `npm-audit` target and the docs SBOM; SBOM name `fbt-lib-<version>.cdx.json` from `Cargo.lock`; keep `SYFT_IMAGE='anchore/syft:v1.54.1'`), `.gitleaks.toml` (allowlist `target/`, `sast-out/`, `fuzz/artifacts/`, `fuzz/corpus/`, `.superpowers/`), `.grype.yaml`, `.semgrepignore`, `rustfmt.toml` (`edition = "2024"`).
- Copy `LICENSE` from `../featherbittorrent/LICENSE`.

**Interfaces:**
- Produces: crate names `fbt-bencode`, `fbt-core`, `fbt-metainfo`, `fbt-wire`, `fbt-tracker`, `fbt-session`, `fbt`, `fbt-example`; `[workspace.dependencies]` entries every later task adds to (never per-crate versions).

- [ ] **Step 1: Write the workspace `Cargo.toml`**

`members = ["crates/*"]`, `exclude = ["fuzz"]`, `resolver = "3"`. `[workspace.package]`: `version = "0.0.0"`, `edition = "2024"`, `rust-version = "1.99"`, `license = "Apache-2.0"`, `repository = "https://github.com/featherbitplatform/fbt-lib"`, `publish = false`. `[workspace.lints.rust]`: `unsafe_code = "forbid"`, `missing_debug_implementations = "warn"`. `[workspace.lints.clippy]`: `all = "warn"`. Each crate sets `[lints] workspace = true`; the four parser crates add in `lib.rs`: `#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::panic))]`.

- [ ] **Step 2: Write stubs with one passing test each**

Each `lib.rs` has `#![forbid(unsafe_code)]`, a crate doc line, and `#[cfg(test)] mod tests { #[test] fn builds() {} }`.

- [ ] **Step 3: Write `deny.toml`**

Start from `../terminal/deny.toml`: same `[licenses].allow` list plus `"CDLA-Permissive-2.0"` (webpki-roots); `[advisories] version = 2, unmaintained = "workspace"`, empty `ignore`; `[bans] multiple-versions = "warn"`; `[sources] unknown-registry = "deny", unknown-git = "deny"`, no `allow-org`.

- [ ] **Step 4: Write `ci.yml`**

Jobs: `check` matrix `[macos-15, ubuntu-latest, windows-latest]` with fmt (Linux only), `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo test --workspace`; `msrv` (ubuntu, `dtolnay/rust-toolchain@1.99`, `cargo check --workspace`); `docs` (ubuntu, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`). Same action versions as the terminal's `ci.yml`. Task 8 adds the `features` and `pure-rust` jobs, Task 18 the `interop` job.

- [ ] **Step 5: Write `README.md`, `CHANGELOG.md`, `NOTICE`, `CLAUDE.md`, `.gitignore`, `.gitattributes`**

- README: one paragraph from the spec's intent, status "pre-release", build/test commands, link to the spec.
- CHANGELOG: Keep a Changelog header with an empty `## [Unreleased]`.
- NOTICE: `fbt-lib Copyright 2026 featherbitplatform`. Add libtorrent's BSD-3 notice only when ported code first lands.
- CLAUDE.md: shaped like `../terminal/CLAUDE.md`: what this is; binding spec path; the rules (from-scratch, no unsafe, remote input never panics, pure-rust feature, naming other clients allowed); conventions (Gitflow, Conventional Commits, SemVer, changelog, no attribution of any kind); the commands block.
- `.gitattributes`: `* text=auto eol=lf`, `*.torrent binary`.
- `.gitignore`: `/target`, `/sast-out`, `/fuzz/target`, `/fuzz/artifacts`, `/fuzz/corpus`, `.DS_Store`, `.superpowers/`.

- [ ] **Step 6: Verify**

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && cargo deny check`
Expected: all pass, 8 tests run.

- [ ] **Step 7: Commit**

`git add -A && git commit -m "chore: scaffold cargo workspace, CI and project docs"`

---

### Task 2: bencode decoding

**Files:**
- Create: `crates/fbt-bencode/src/{value.rs,decode.rs}`, tests in `decode.rs`.

**Interfaces:**
- Produces:
  - `pub enum Value<'a> { Int(i64), Bytes(&'a [u8]), List(Vec<Value<'a>>), Dict(Dict<'a>) }`, with `as_int() -> Option<i64>`, `as_bytes() -> Option<&'a [u8]>`, `as_str() -> Option<&'a str>` (UTF-8 only), `as_list() -> Option<&[Value<'a>]>`, `as_dict() -> Option<&Dict<'a>>`, and `raw(&self) -> &'a [u8]`, the exact input bytes of this value.
  - `pub struct Dict<'a>`, keeping entries in input order: `get(&self, key: &[u8]) -> Option<&Value<'a>>`, `iter()`, `len()`, `is_empty()`, `raw() -> &'a [u8]`.
  - `pub struct Limits { pub max_depth: usize }` with `Default` = 64.
  - `pub fn decode(input: &[u8]) -> Result<Value<'_>, DecodeError>`: default limits; the whole input must be consumed.
  - `pub fn decode_with(input: &[u8], limits: Limits) -> Result<Value<'_>, DecodeError>`.
  - `pub fn decode_prefix(input: &[u8], limits: Limits) -> Result<(Value<'_>, usize), DecodeError>`: returns the value and the bytes consumed (for ut_metadata in milestone 2).
  - `pub enum DecodeError` (thiserror), every variant carrying `pos: usize`: `UnexpectedEof`, `InvalidByte { byte }`, `LeadingZero`, `NegativeZero`, `IntOverflow`, `LengthOverflow`, `DepthExceeded`, `DuplicateKey`, `TrailingData`.

Rules: keys need not be sorted; duplicate keys are rejected. Duplicate detection must stay O(n log n) or better: compare with the previous key while keys arrive sorted, and fall back to a hash set once they don't. A string length greater than the remaining input fails before any allocation. Recursion depth is bounded by `max_depth`.

- [ ] **Step 1: Write failing tests**

```rust
#[test] fn decodes_scalars() {
    assert_eq!(decode(b"i42e").unwrap().as_int(), Some(42));
    assert_eq!(decode(b"i-7e").unwrap().as_int(), Some(-7));
    assert_eq!(decode(b"4:spam").unwrap().as_bytes(), Some(&b"spam"[..]));
    assert_eq!(decode(b"0:").unwrap().as_bytes(), Some(&b""[..]));
}
#[test] fn dict_get_and_raw_span() {
    let input = b"d4:infod6:lengthi3ee3:fooi1ee";
    let v = decode(input).unwrap();
    let info = v.as_dict().unwrap().get(b"info").unwrap();
    assert_eq!(info.raw(), b"d6:lengthi3ee");
}
#[test] fn rejects_malformed() {
    assert!(matches!(decode(b"i03e"), Err(DecodeError::LeadingZero { .. })));
    assert!(matches!(decode(b"i-0e"), Err(DecodeError::NegativeZero { .. })));
    assert!(matches!(decode(b"i9223372036854775808e"), Err(DecodeError::IntOverflow { .. })));
    assert!(matches!(decode(b"5:abc"), Err(DecodeError::UnexpectedEof { .. })));
    assert!(matches!(decode(b"99999999999999999999:x"), Err(DecodeError::LengthOverflow { .. })));
    assert!(matches!(decode(b"d1:ai1e1:ai2ee"), Err(DecodeError::DuplicateKey { .. })));
    assert!(matches!(decode(b"i1ei2e"), Err(DecodeError::TrailingData { pos: 3 })));
    assert!(matches!(decode(b"di1ei2ee"), Err(DecodeError::InvalidByte { .. })));
}
#[test] fn accepts_unsorted_keys() { assert!(decode(b"d1:bi1e1:ai2ee").is_ok()); }
#[test] fn depth_limit() {
    let deep = [b"l".repeat(65), b"e".repeat(65)].concat();
    assert!(matches!(decode(&deep), Err(DecodeError::DepthExceeded { .. })));
    let ok = [b"l".repeat(64), b"e".repeat(64)].concat();
    assert!(decode(&ok).is_ok());
}
#[test] fn many_unsorted_keys_is_fast() {
    // 100_000 keys in descending order; must decode in well under a second (debug build)
    let mut s = b"d".to_vec();
    for i in (0..100_000u32).rev() { let k = format!("{i:08}"); s.extend(format!("8:{k}i0e").as_bytes()); }
    s.push(b'e');
    let t = std::time::Instant::now();
    assert_eq!(decode(&s).unwrap().as_dict().unwrap().len(), 100_000);
    assert!(t.elapsed() < std::time::Duration::from_secs(2));
}
#[test] fn decode_prefix_reports_consumed() {
    let (v, n) = decode_prefix(b"d1:ai1ee<binary>", Limits::default()).unwrap();
    assert_eq!(n, 8); assert!(v.as_dict().is_some());
}
```

- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-bencode` fails to compile because the items don't exist yet.
- [ ] **Step 3: Implement** a recursive-descent parser over `&[u8]` with a position cursor, as specified in the Interfaces block.
- [ ] **Step 4: Run.** `cargo test -p fbt-bencode`: PASS.
- [ ] **Step 5: Commit.** `feat(bencode): zero-copy decoder with depth limit and raw spans`

---

### Task 3: bencode encoding and round-trip properties

**Files:**
- Create: `crates/fbt-bencode/src/encode.rs`; `proptest` dev-dependency.

**Interfaces:**
- Consumes: `Value`, `decode` (Task 2).
- Produces:
  - `pub enum OwnedValue { Int(i64), Bytes(Vec<u8>), List(Vec<OwnedValue>), Dict(BTreeMap<Vec<u8>, OwnedValue>) }` with `From` impls for `i64`, `&[u8]`, `Vec<u8>`, `&str`, `String`.
  - `pub fn encode(v: &OwnedValue) -> Vec<u8>` and `pub fn encode_into(v: &OwnedValue, out: &mut Vec<u8>)`.
  - `impl Value<'_> { pub fn to_owned_value(&self) -> OwnedValue }`.
  - `pub struct DictBuilder`, a sorted-key builder: `new()`, `insert(key: &[u8], v: impl Into<OwnedValue>) -> &mut Self`, `build() -> OwnedValue`.

- [ ] **Step 1: Write failing tests:** `encodes_canonical` asserts that `encode` of a dict built with keys `b"b"` and `b"a"` equals `b"d1:ai2e1:bi1ee"`. Add a proptest `roundtrip` over an arbitrary `OwnedValue` (depth ≤ 4, ≤ 8 children, byte strings ≤ 32 bytes) asserting `decode(&encode(&v)).unwrap().to_owned_value() == v`, and `canonical_reencode` asserting that re-encoding a decoded canonical input gives the same bytes.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-bencode`
- [ ] **Step 3: Implement** the encoder.
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `feat(bencode): canonical encoder and property tests`

---

### Task 4: core types (info hashes, peer id, bitfield, layouts)

**Files:**
- Create: `crates/fbt-core/src/{info_hash.rs,peer_id.rs,bitfield.rs,layout.rs}`; deps `sha1`, `sha2`, `rand`.

**Interfaces:**
- Produces:
  - `pub struct InfoHashV1(pub [u8; 20])` and `pub struct InfoHashV2(pub [u8; 32])`, both `Copy + Eq + Hash + Ord`, with `Display` (lowercase hex) and `FromStr` (40 or 64 hex digits). Plus `InfoHashV1::of(info_bytes: &[u8]) -> Self` (SHA-1) and `InfoHashV2::of(...)` (SHA-256).
  - `pub struct InfoHashes { pub v1: Option<InfoHashV1>, pub v2: Option<InfoHashV2> }`, `Copy + Eq + Hash`, with constructors `from_v1`, `from_v2` and `hybrid`, and `wire_hash(&self) -> [u8; 20]` (v1 if present, else the first 20 bytes of v2). `Display` prints v1, else v2.
  - `pub struct PeerId(pub [u8; 20])`. `PeerId::generate() -> PeerId` produces Azureus style `-FB` + major + minor + patch + `0` + `-` (each version digit one character of `0-9A-Z`) followed by 12 random alphanumerics. `client_prefix() -> [u8; 8]`.
  - `pub struct Bitfield`: `new(len: u32)`, `full(len: u32)`, `from_bytes(bytes: &[u8], len: u32) -> Result<Self, BitfieldError>`, `len()`, `is_empty()`, `get(i: u32) -> bool` (out of range gives false), `set(i)`, `clear(i)` (out of range is a no-op), `count_ones() -> u32`, `is_full()`, `as_bytes() -> &[u8]`, `iter_ones() -> impl Iterator<Item = u32>`. Bits are MSB-first: piece 0 is `0x80` of byte 0. `BitfieldError::{WrongLength, SpareBitsSet}`.
  - `pub const BLOCK_SIZE: u32 = 16_384;` and `pub struct BlockRef { pub piece: u32, pub offset: u32, pub len: u32 }` (`Copy + Eq + Hash + Ord`).
  - `pub struct PieceLayout`: `new(total_len: u64, piece_len: u32) -> Result<Self, LayoutError>` (`total_len > 0`, `1 ≤ piece_len`, piece count fits `u32`), `total_len()`, `piece_len()`, `num_pieces() -> u32`, `piece_size(piece: u32) -> u32`, `piece_offset(piece: u32) -> u64`, `blocks_in_piece(piece: u32) -> u32`, `block(piece: u32, index: u32) -> BlockRef`, `is_valid_block(b: &BlockRef) -> bool`.
  - `pub struct FileLayout`: `new(lengths: impl IntoIterator<Item = u64>) -> Self`, `total_len()`, `num_files()`, `map(offset: u64, len: u64) -> Vec<FileSlice>` (skips zero-length files, clamps at the end), `files_overlapping(offset: u64, len: u64) -> std::ops::Range<usize>` (zero-length files inside the range included). `pub struct FileSlice { pub file: usize, pub file_offset: u64, pub len: u64 }`.

- [ ] **Step 1: Write failing tests**

```rust
#[test] fn info_hash_hex_roundtrip() {
    let h: InfoHashV1 = "aabbccddeeff00112233445566778899aabbccdd".parse().unwrap();
    assert_eq!(h.to_string(), "aabbccddeeff00112233445566778899aabbccdd");
}
#[test] fn wire_hash_prefers_v1_else_truncates_v2() { /* hybrid → v1 bytes; v2-only → v2[..20] */ }
#[test] fn peer_id_prefix() { assert_eq!(&PeerId::generate().0[..8], b"-FB0000-"); } // workspace 0.0.0; tracks the version
#[test] fn bitfield_msb_first_and_spare_bits() {
    let mut b = Bitfield::new(10); b.set(0); b.set(9);
    assert_eq!(b.as_bytes(), &[0x80, 0x40]);
    assert!(matches!(Bitfield::from_bytes(&[0xff, 0xff], 10), Err(BitfieldError::SpareBitsSet)));
    assert!(matches!(Bitfield::from_bytes(&[0xff], 10), Err(BitfieldError::WrongLength)));
    assert!(!b.get(1000));
}
#[test] fn piece_layout_last_piece_and_blocks() {
    let l = PieceLayout::new(100_000, 32_768).unwrap();
    assert_eq!(l.num_pieces(), 4);
    assert_eq!(l.piece_size(3), 100_000 - 3 * 32_768);
    assert_eq!(l.blocks_in_piece(0), 2);
    assert_eq!(l.block(3, 0), BlockRef { piece: 3, offset: 0, len: 1_696 });
    assert!(!l.is_valid_block(&BlockRef { piece: 3, offset: 0, len: 16_384 }));
}
#[test] fn file_layout_spans_tiny_and_empty_files() {
    let f = FileLayout::new([3, 0, 1, 0, 10]);
    assert_eq!(f.map(2, 4), vec![
        FileSlice { file: 0, file_offset: 2, len: 1 },
        FileSlice { file: 2, file_offset: 0, len: 1 },
        FileSlice { file: 4, file_offset: 0, len: 2 },
    ]);
    assert_eq!(f.files_overlapping(2, 4), 0..5);
}
```

- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-core`
- [ ] **Step 3: Implement.** `FileLayout::map` uses binary search over cumulative offsets.
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `feat(core): info hashes, peer id, bitfield and piece/file layouts`

---

### Task 5: path sanitization

**Files:**
- Create: `crates/fbt-metainfo/src/path.rs`.

**Interfaces:**
- Produces:
  - `pub struct RelPath(Vec<String>)`: `components() -> &[String]`, `to_path_buf() -> PathBuf`, `Display` joined with `/`.
  - `pub fn sanitize_component(raw: &[u8]) -> Option<String>`.
  - `pub fn sanitize_path<'a>(raw: impl IntoIterator<Item = &'a [u8]>) -> Result<RelPath, PathError>`. `PathError::Empty` means nothing usable is left.

Rules, applied identically on every OS so a torrent saves the same everywhere:

- Invalid UTF-8 is decoded lossily.
- A component that contains `/` or `\` is split on them.
- Empty, `.` and `..` components are dropped.
- A leading drive prefix (`C:`) or root is dropped.
- Control characters and `< > : " | ? *` become `_`.
- Trailing dots and spaces are trimmed.
- A Windows reserved name gets `_` appended, compared case-insensitively against the stem before the first dot: `CON`, `PRN`, `AUX`, `NUL`, `COM1`-`COM9`, `LPT1`-`LPT9`. For example `con.txt` becomes `con_.txt`.
- A component longer than 255 bytes is truncated on a char boundary, keeping its extension if the extension is 16 bytes or less.

- [ ] **Step 1: Write failing tests**

```rust
fn p(parts: &[&str]) -> String { sanitize_path(parts.iter().map(|s| s.as_bytes())).unwrap().to_string() }
#[test] fn traversal_and_roots_dropped() {
    assert_eq!(p(&["..", "etc", "passwd"]), "etc/passwd");
    assert_eq!(p(&["/abs", "x"]), "abs/x");
    assert_eq!(p(&["a/../../b"]), "a/b");
    assert_eq!(p(&["C:\\Windows", "x"]), "Windows/x");
}
#[test] fn reserved_and_illegal() {
    assert_eq!(p(&["con.txt"]), "con_.txt");
    assert_eq!(p(&["LPT9"]), "LPT9_");
    assert_eq!(p(&["a<b>:c?.d "]), "a_b__c_.d");
    assert_eq!(p(&["x\u{0}y"]), "x_y");
}
#[test] fn long_component_keeps_extension() {
    let s = format!("{}.mkv", "é".repeat(200));
    let out = p(&[&s]);
    assert!(out.len() <= 255 && out.ends_with(".mkv"));
}
#[test] fn nothing_left_is_error() {
    assert!(matches!(sanitize_path([&b".."[..], b"."]), Err(PathError::Empty)));
}
```

Plus a proptest `never_escapes`: for arbitrary byte components, every resulting component is non-empty, is not `.` or `..`, and contains no `/`, `\`, `:` or NUL.

- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-metainfo path`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `feat(metainfo): portable path sanitization`

---

### Task 6: v1 metainfo parsing

**Files:**
- Create: `crates/fbt-metainfo/src/metainfo.rs`.
- Fixtures in `crates/fbt-metainfo/tests/fixtures/`:
  - `single.torrent`, `multi.torrent`, `hybrid.torrent`, produced once with `transmission-create` or `mktorrent` plus `python -m` scripts, committed together with the generating commands in `fixtures/README.md`;
  - the known-info-hash fixture `ubuntu-like.torrent`, built by a committed test helper so no third-party file is redistributed.

**Interfaces:**
- Consumes: `fbt_bencode::{decode, Value}`, `fbt_core::{InfoHashes, InfoHashV1, InfoHashV2, PieceLayout, FileLayout}`, `sanitize_path`.
- Produces:
  - `pub struct Metainfo { pub info_hashes: InfoHashes, pub info: Info, pub trackers: Vec<Vec<String>>, pub comment: Option<String>, pub created_by: Option<String>, pub creation_date: Option<i64> }`, with `from_bytes(bytes: &[u8]) -> Result<Metainfo, MetainfoError>`, `raw_info(&self) -> &[u8]`, `piece_layout(&self) -> PieceLayout`, `file_layout(&self) -> FileLayout`, `total_len(&self) -> u64`. `Clone + Debug`.
  - `pub struct Info { pub name: String, pub piece_len: u32, pub pieces: Vec<[u8; 20]>, pub files: Vec<FileEntry>, pub private: bool, pub single_file: bool }`.
  - `pub struct FileEntry { pub path: RelPath, pub len: u64, pub pad: bool }`. `path` already includes the torrent name as its first component for multi-file torrents; for single-file torrents it is just the name.
  - `pub enum MetainfoError`: `Bencode(DecodeError)`, `MissingField(&'static str)`, `InvalidField { field: &'static str, reason: &'static str }`, `DuplicatePath(String)`, `V2OnlyUnsupported`.

Rules:

- `info.raw()` gives the v1 hash (SHA-1).
- `info["meta version"] == 2` sets `v2` to the SHA-256 of the same bytes.
- A v2-only torrent (meta version 2 with no `pieces`) returns `V2OnlyUnsupported` until milestone 6.
- `pieces.len()` must equal `20 × num_pieces`.
- `piece_len` must be in `1..=268_435_456`.
- `attr` containing `p` sets `pad = true`. An entry whose `attr` contains `l` (symlink) is kept as a zero-length non-pad file, and no symlink is ever created (milestone 6 revisits this).
- `name` is sanitized with `sanitize_component`; if nothing is left, the v1 hex hash is used instead.
- Two files with the same sanitized path, or a file path that is also a directory prefix of another, give `DuplicatePath`.
- `trackers` comes from `announce-list` tiers when present, otherwise `[[announce]]`. Non-UTF-8 URLs are dropped, duplicates removed and empty tiers removed.

- [ ] **Step 1: Write failing tests**

```rust
#[test] fn single_file_parses()   { /* name, len, 1 file, path == [name], info_hash matches sha1(raw_info) */ }
#[test] fn multi_file_paths_rooted_at_name() { /* files[0].path.components()[0] == info.name */ }
#[test] fn hybrid_sets_both_hashes() { /* v1 and v2 Some, v2 == sha256(raw_info) */ }
#[test] fn announce_list_overrides_announce_and_dedupes() { /* built with DictBuilder */ }
#[test] fn rejects_bad_pieces_length() { /* pieces 39 bytes → InvalidField{field:"pieces",..} */ }
#[test] fn rejects_duplicate_and_prefix_paths() { /* ["a","b"] + ["a","b"]; ["a"] + ["a","b"] → DuplicatePath */ }
#[test] fn pad_files_flagged() { /* attr "p" → pad */ }
#[test] fn traversal_in_file_path_is_neutralised() { /* path ["..","..","x"] → name/x */ }
```

The tests build torrents with `fbt_bencode::DictBuilder` (a test helper `fn torrent(info: OwnedValue, extra: &[(&str, OwnedValue)]) -> Vec<u8>`), plus the fixtures for the three real files.

- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-metainfo`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `feat(metainfo): v1 and hybrid .torrent parsing`

---

### Task 7: peer wire protocol

**Files:**
- Create: `crates/fbt-wire/src/{handshake.rs,message.rs,codec.rs,fast.rs}`; deps `bytes`, `tokio-util` (codec), `tokio` (io-util), `sha1`.

**Interfaces:**
- Consumes: `fbt_core::{PeerId, BlockRef}`.
- Produces:
  - `pub struct Reserved(pub [u8; 8])`, with `supports_fast()` (byte 7 & 0x04), `set_fast()`, `supports_extended()` (byte 5 & 0x10), `supports_dht()` (byte 7 & 0x01). Milestone 1 sets only fast.
  - `pub struct Handshake { pub reserved: Reserved, pub info_hash: [u8; 20], pub peer_id: PeerId }`, with `pub const HANDSHAKE_LEN: usize = 68`, `encode(&self) -> [u8; 68]` and `decode(buf: &[u8; 68]) -> Result<Handshake, HandshakeError>`. The `pstrlen` must be 19 and the protocol string `BitTorrent protocol`. `HandshakeError::{BadProtocol, Io(std::io::Error)}`.
  - `pub async fn write_handshake<W: AsyncWrite + Unpin>(w: &mut W, h: &Handshake) -> io::Result<()>` and `pub async fn read_handshake<R: AsyncRead + Unpin>(r: &mut R) -> Result<Handshake, HandshakeError>`.
  - `pub enum Message { KeepAlive, Choke, Unchoke, Interested, NotInterested, Have(u32), Bitfield(Bytes), Request(BlockRef), Piece { piece: u32, offset: u32, data: Bytes }, Cancel(BlockRef), Port(u16), SuggestPiece(u32), HaveAll, HaveNone, RejectRequest(BlockRef), AllowedFast(u32), Extended { id: u8, payload: Bytes }, Unknown { id: u8, payload: Bytes } }`. Ids follow BEP 3/5/6/10: 0 to 9, 13 to 17, 20.
  - `pub struct MessageCodec { max_len: u32 }`, with `MessageCodec::new(max_len: u32)`, implementing `Decoder<Item = Message, Error = WireError>` and `Encoder<Message, Error = WireError>`. A length prefix above `max_len` returns `WireError::TooLong { len, max }` before buffering. A fixed-size message with the wrong length returns `WireError::BadLength { id, len }`.
  - `pub fn allowed_fast_set(info_hash: &[u8; 20], ip: Ipv4Addr, num_pieces: u32, k: usize) -> Vec<u32>`, per BEP 6, using its algorithm verbatim.

- [ ] **Step 1: Write failing tests**

```rust
#[test] fn handshake_roundtrip_and_bad_protocol() { /* encode/decode equal; pstrlen 18 → BadProtocol */ }
#[test] fn message_roundtrip_all_variants() { /* encode then decode each variant via BytesMut */ }
#[test] fn partial_frames_wait_for_more() { /* first 3 bytes of a frame → Ok(None) */ }
#[test] fn oversized_frame_rejected_without_buffering() {
    let mut c = MessageCodec::new(1 << 20);
    let mut b = BytesMut::from(&[0xff, 0xff, 0xff, 0xff, 7][..]);
    assert!(matches!(c.decode(&mut b), Err(WireError::TooLong { .. })));
}
#[test] fn fixed_length_violations() { /* Have with 3-byte payload → BadLength{id:4,len:4} */ }
#[test] fn bep6_allowed_fast_vectors() {
    let ih = [0xaa; 20]; let ip = "80.4.4.200".parse().unwrap();
    assert_eq!(allowed_fast_set(&ih, ip, 1313, 7), vec![1059, 431, 808, 1217, 287, 376, 1188]);
    assert_eq!(allowed_fast_set(&ih, ip, 1313, 9), vec![1059, 431, 808, 1217, 287, 376, 1188, 353, 508]);
}
```

Plus a proptest: any byte stream fed in random chunk sizes to `decode` either yields messages or errors and never panics.

- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-wire`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `feat(wire): handshake, message codec and BEP 6 allowed-fast set`

---

### Task 8: HTTP(S) tracker client and TLS features

**Files:**
- Create: `crates/fbt-tracker/src/{types.rs,http.rs,urlencode.rs,tls.rs,lib.rs}`, `dev/check-pure-rust.sh`.
- Modify: `crates/fbt-session/Cargo.toml` and `crates/fbt/Cargo.toml` (features), `.github/workflows/ci.yml` (add a `features` job running `cargo check -p fbt --no-default-features` and `cargo check -p fbt --no-default-features --features pure-rust`, and a `pure-rust` job running `bash dev/check-pure-rust.sh`).

**Interfaces:**
- Consumes: `fbt_bencode`, `fbt_core::PeerId`.
- Produces:
  - `pub enum AnnounceEvent { None, Started, Completed, Stopped }`.
  - `pub struct AnnounceRequest { pub info_hash: [u8; 20], pub peer_id: PeerId, pub port: u16, pub uploaded: u64, pub downloaded: u64, pub left: u64, pub event: AnnounceEvent, pub num_want: Option<u32>, pub key: u32, pub tracker_id: Option<Vec<u8>> }`.
  - `pub struct AnnounceResponse { pub interval: Duration, pub min_interval: Option<Duration>, pub peers: Vec<SocketAddr>, pub complete: Option<u32>, pub incomplete: Option<u32>, pub tracker_id: Option<Vec<u8>>, pub warning: Option<String>, pub external_ip: Option<IpAddr> }`.
  - `pub enum TrackerError`: `InvalidUrl(String)`, `UnsupportedScheme(String)`, `TlsDisabled`, `Failure(String)`, `HttpStatus(u16)`, `TooManyRedirects`, `ResponseTooLarge`, `InvalidResponse(&'static str)`, `Timeout`, `Io(std::io::Error)`.
  - `pub struct TrackerConfig { pub user_agent: String, pub http_timeout: Duration, pub udp_base_timeout: Duration, pub udp_max_retries: u32, pub max_response_bytes: usize }`. Defaults: `"fbt/<crate version>"`, 30 s, 15 s, 2, 2 MiB.
  - `pub struct TrackerClient`: `new(cfg: TrackerConfig) -> Result<TrackerClient, TrackerError>` and `async fn announce(&self, url: &str, req: &AnnounceRequest) -> Result<AnnounceResponse, TrackerError>`, which dispatches on scheme (`http`, `https`, `udp`). `Clone` (cheap, `Arc` inside).
  - `#[doc(hidden)] pub fn parse_http_announce(body: &[u8]) -> Result<AnnounceResponse, TrackerError>`, public for the fuzz targets.
  - `pub fn url_encode_bytes(bytes: &[u8]) -> String`: RFC 3986 unreserved bytes pass through, everything else becomes `%XX` in uppercase hex.

Rules:

- The query is appended with `&` if the URL already has one. It carries `info_hash`, `peer_id`, `port`, `uploaded`, `downloaded`, `left`, `compact=1`, `key` (8 hex digits), `numwant` and `trackerid`, plus `event` when it isn't `None`.
- The response body is capped at `max_response_bytes` while streaming.
- At most 3 redirects are followed; an `https → http` downgrade counts as an error (`InvalidResponse("insecure redirect")`).
- `failure reason` becomes `Failure`.
- `peers` is accepted as compact 6-byte entries or as a list of dicts (`ip`, `port`). `peers6` is compact 18-byte entries.
- `interval` is required; it is clamped to at least 60 s and `min interval` to at least 30 s.

TLS: `tls.rs` builds an `HttpsConnector` using `hyper_rustls::HttpsConnectorBuilder::new().with_provider_and_webpki_roots(provider)`. Check that this method exists in hyper-rustls 0.27.10; if it doesn't, build a `rustls::ClientConfig` from `webpki_roots::TLS_SERVER_ROOTS` with `builder_with_provider`. The provider is `rustls_rustcrypto::provider()` when `tls-rustcrypto` is enabled, otherwise `rustls::crypto::ring::default_provider()`. Without either feature, `https` URLs return `TlsDisabled`.

Features:

- `fbt-tracker`: `default = []`, `tls-ring = [dep:rustls, rustls/ring, dep:hyper-rustls, hyper-rustls/ring, dep:webpki-roots]`, `tls-rustcrypto = [dep:rustls, dep:hyper-rustls, dep:webpki-roots, dep:rustls-rustcrypto]`. hyper-rustls uses `default-features = false` and features `http1`, `tls12`, `webpki-roots`.
- `fbt`: `default = ["tls"]`, `tls = ["fbt-session/tls-ring"]`, `pure-rust = ["fbt-session/tls-rustcrypto"]`, with `fbt-session` forwarding both to `fbt-tracker`.
- Document in `fbt`'s crate docs: `default-features = false, features = ["pure-rust"]` gives a build with no C or assembly.

- [ ] **Step 1: Write failing tests**

```rust
#[test] fn urlencode_info_hash() {
    assert_eq!(url_encode_bytes(&[0x12, 0x34, b'a', b'~', b' ', 0xff]), "%124a~%20%FF");
}
#[test] fn parses_compact_and_dict_peers_and_peers6() { /* built with DictBuilder */ }
#[test] fn failure_reason_maps_to_failure() {}
#[test] fn interval_clamped_and_required() {}
#[tokio::test] async fn announce_against_local_http_server() {
    // tokio TcpListener on 127.0.0.1:0 answering one GET with a bencoded body;
    // assert request line contains "info_hash=%AA%AA" .. "&compact=1&event=started" and peers parsed
}
#[tokio::test] async fn oversized_body_rejected() { /* 3 MiB body → ResponseTooLarge */ }
#[tokio::test] async fn redirect_limit() { /* 4 chained 302s → TooManyRedirects */ }
#[cfg(not(any(feature = "tls-ring", feature = "tls-rustcrypto")))]
#[tokio::test] async fn https_without_tls_feature() { /* → TlsDisabled */ }
```

`dev/check-pure-rust.sh` runs `cargo tree -e normal -p fbt --no-default-features --features pure-rust --prefix none` and fails if any line starts with `ring ` or `cc `. It also runs `cargo test -p fbt-tracker --features tls-rustcrypto`.

- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-tracker`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test -p fbt-tracker && cargo test -p fbt-tracker --features tls-ring && bash dev/check-pure-rust.sh`: PASS.
- [ ] **Step 5: Commit.** `feat(tracker): HTTP(S) announce with selectable rustls provider`

---

### Task 9: UDP tracker client (BEP 15)

**Files:**
- Create: `crates/fbt-tracker/src/udp.rs`.

**Interfaces:**
- Consumes: `AnnounceRequest`, `AnnounceResponse`, `TrackerError`, `TrackerConfig` (Task 8).
- Produces: `udp://` handling inside `TrackerClient::announce`, plus `#[doc(hidden)] pub fn parse_udp_announce(buf: &[u8], transaction_id: u32, ipv6: bool) -> Result<AnnounceResponse, TrackerError>` for fuzzing.

Protocol:

- **Sockets.** One `UdpSocket` per address family, bound lazily to `0.0.0.0:0` and `[::]:0` and owned by the client. A receive task routes datagrams to waiters by transaction id; unknown ids are dropped.
- **Connect.** The request uses protocol id `0x41727101980` and action 0. The connection id is cached per resolved tracker address for 60 s.
- **Announce.** Action 1, 98-byte request. The `event` codes are none=0, completed=1, started=2, stopped=3. `num_want` defaults to `-1`.
- **Responses.** Action 3 is an error (message → `Failure`). Peers are 6 bytes over IPv4 and 18 bytes over IPv6.
- **Timeouts.** Each attempt waits `udp_base_timeout * 2^n`, for `n = 0..=udp_max_retries`, then fails with `Timeout`. A connection id rejected by the tracker (an error response to announce) clears the cache and reconnects once.

- [ ] **Step 1: Write failing tests** against a fake tracker task on `127.0.0.1:0`, using `TrackerConfig { udp_base_timeout: 100ms, .. }`:
  - `udp_connect_then_announce`: request fields at the right offsets, peers parsed.
  - `udp_connection_id_reused_within_60s`: two announces, one connect, using `tokio::time::pause` around a socket-free clock abstraction or by counting connects in the fake.
  - `udp_retransmits_then_times_out`: fake drops everything, so `Timeout` comes after the sum of the backoffs.
  - `udp_error_action_maps_to_failure`.
  - `udp_wrong_transaction_id_ignored`.
  - `parse_rejects_short_and_misaligned`: a peers section that isn't a multiple of 6 gives `InvalidResponse`.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-tracker udp`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `feat(tracker): UDP announce (BEP 15)`

---

### Task 10: storage and piece checking

**Files:**
- Create: `crates/fbt-session/src/storage/{mod.rs,fs.rs,mem.rs,check.rs}`, `crates/fbt-session/src/error.rs`.

**Interfaces:**
- Consumes: `fbt_core::{FileLayout, PieceLayout, Bitfield}`, `fbt_metainfo::{Metainfo, FileEntry}`.
- Produces:
  - `pub trait Storage: Send + Sync + std::fmt::Debug + 'static`, with `read(&self, offset: u64, buf: &mut [u8]) -> io::Result<()>`, `write(&self, offset: u64, data: &[u8]) -> io::Result<()>`, `flush(&self) -> io::Result<()>`, and `delete_files(&self) -> io::Result<()>`. Offsets are torrent-global. A read of data that was never written either returns `ErrorKind::NotFound` or reads zeros; both are allowed.
  - `pub struct FsStorage`, built with `FsStorage::new(save_path: PathBuf, metainfo: &Metainfo) -> FsStorage`.
    - Each file lives at `save_path.join(entry.path.to_path_buf())`.
    - Parent directories are created on first write, and the file gets `set_len(len)` (sparse) at that point.
    - Pad files are never created: reads return zeros and writes are ignored after checking that the data is all zeros.
    - Zero-length non-pad files are created when the first write to the torrent happens, and also by `create_empty_files()`.
    - At most 64 file handles are held open at once (least-recently-used ones are closed).
    - A read past the current end of a file, or from a missing file, gives `NotFound`.
    - `delete_files` removes every non-pad file, then any directories under the torrent root that become empty.
  - `pub struct MemStorage`, with `MemStorage::new(total_len: u64)` and `MemStorage::from_bytes(data: Vec<u8>)`, for tests.
  - `pub fn check_pieces(storage: &dyn Storage, layout: &PieceLayout, hashes: &[[u8; 20]], mut on_piece: impl FnMut(u32)) -> io::Result<Bitfield>`. A `NotFound` piece counts as missing; any other error propagates.
  - `pub fn sha1_piece(data: &[u8]) -> [u8; 20]`.

- [ ] **Step 1: Write failing tests** (with a `tempfile` dev-dep):
  - `fs_storage_roundtrip_across_file_boundary`: 3 files of 5, 0 and 7 bytes; write 12 bytes at 0, read them back; the 0-byte file exists.
  - `fs_storage_creates_empty_files`: covers Review Focus 4.
  - `pad_file_never_created_and_reads_zero`.
  - `missing_file_reads_not_found`.
  - `check_pieces_detects_partial_data`: write pieces 0 and 2 of 4, expect bitfield `{0, 2}`.
  - `delete_files_removes_tree`.
  - `open_handle_cap`: 100 one-byte files; writing them all leaves at most 64 open, checked through a `#[cfg(test)]` counter.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session storage`
- [ ] **Step 3: Implement.** Use positional I/O (`std::os::unix::fs::FileExt` / `std::os::windows::fs::FileExt`) behind a small `cfg` helper, holding the handles in a `Mutex`.
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `feat(session): filesystem and memory storage with piece checking`

---

### Task 11: piece picker and choker

**Files:**
- Create: `crates/fbt-session/src/{picker.rs,choker.rs}`.

**Interfaces:**
- Consumes: `fbt_core::{PieceLayout, Bitfield, BlockRef}`.
- Produces:
  - `#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug)] pub struct PeerKey(pub u64);`
  - `pub enum PeerPieces<'a> { All, Some(&'a Bitfield) }`.
  - `pub struct PiecePicker`, with:
    - `new(layout: PieceLayout, have: &Bitfield) -> Self`;
    - `set_piece_priorities(&mut self, prio: &[u8])`, where 0 = skip and 1..=7, default 4;
    - `set_sequential(&mut self, on: bool)`;
    - availability tracking: `add_peer_pieces(&mut self, p: PeerPieces)`, `remove_peer_pieces(&mut self, p: PeerPieces)`, `peer_has(&mut self, piece: u32)`;
    - `pick(&mut self, peer: PeerKey, has: PeerPieces, max_blocks: usize, rng: &mut impl rand::Rng) -> Vec<BlockRef>`;
    - `on_block(&mut self, peer: PeerKey, block: BlockRef) -> BlockResult`;
    - `release(&mut self, peer: PeerKey, block: BlockRef)`, which returns a block to the pool;
    - `release_peer(&mut self, peer: PeerKey)`;
    - `piece_passed(&mut self, piece: u32)` and `piece_failed(&mut self, piece: u32)`;
    - `have(&self) -> &Bitfield`, `is_endgame(&self) -> bool`, `wanted_left_bytes(&self) -> u64`, `is_finished(&self) -> bool` (every wanted piece passed).
  - `pub struct BlockResult { pub accepted: bool, pub piece_complete: bool, pub cancel: Vec<PeerKey> }`. `accepted` is false for blocks that were never requested or are duplicates. `cancel` lists the other peers holding the same block in end-game.
  - `pub struct PeerChokeInfo { pub key: PeerKey, pub interested: bool, pub rate_from_peer: u64, pub rate_to_peer: u64 }`.
  - `pub struct Choker`, with `new(slots: usize) -> Self` and `rechoke(&mut self, peers: &[PeerChokeInfo], seeding: bool, rng: &mut impl rand::Rng) -> HashSet<PeerKey>`.

Picker order:

1. Blocks of partially requested pieces that the peer has, highest priority first.
2. New pieces that the peer has, by priority descending, then lowest availability (or lowest index when sequential), with random tie-break among equal availability.
3. Once every block of every wanted, unfinished piece is requested (end-game), blocks already requested by other peers but not by this one, at most 2 owners per block.

Skipped pieces are never picked. Availability is kept in buckets so `pick` doesn't scan every piece (an `#[ignore]` perf test takes 100k pieces and 10k picks).

Choker rules:

- Only interested peers are considered.
- `slots - 1` regular unchoke slots, given to the highest `rate_from_peer`, or the highest `rate_to_peer` when seeding.
- One optimistic slot, given at random to a remaining interested peer and rotated every 3rd call.

- [ ] **Step 1: Write failing tests**
  - `picks_rarest_first`: availability 3, 1, 2 gives piece 1 first.
  - `finishes_partial_pieces_first`.
  - `respects_priorities_and_skip`.
  - `sequential_picks_lowest_index`.
  - `never_picks_what_peer_lacks`.
  - `release_peer_returns_blocks` (the Review Focus 5 logic).
  - `failed_piece_is_repicked`.
  - `endgame_duplicates_and_cancels`: two peers, last block, both get it; `on_block` from A gives `cancel == [B]`.
  - `unrequested_block_not_accepted`.
  - `choker_unchokes_fastest_plus_optimistic`.
  - `choker_rotates_optimistic_every_third_round`.
  - `choker_ignores_uninterested`.

  For example:

  ```rust
  #[test] fn picks_rarest_first() {
      let l = PieceLayout::new(3 * 16_384, 16_384).unwrap();
      let mut p = PiecePicker::new(l, &Bitfield::new(3));
      for bits in [[1,1,1],[1,0,1],[1,0,0]] { p.add_peer_pieces(PeerPieces::Some(&bf(&bits))); }
      let got = p.pick(PeerKey(9), PeerPieces::All, 1, &mut rng());
      assert_eq!(got, vec![BlockRef { piece: 1, offset: 0, len: 16_384 }]);
  }
  ```
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session picker choker`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `feat(session): rarest-first piece picker with end-game, and choker`

---

### Task 12: peer connection task

**Files:**
- Create: `crates/fbt-session/src/peer/{mod.rs,conn.rs,messages.rs}`, `crates/fbt-session/src/rate.rs`, `crates/fbt-session/src/settings.rs`.

**Interfaces:**
- Consumes: `fbt_wire::*`, `PeerKey`, `fbt_core::{Bitfield, BlockRef, PeerId, PieceLayout}`.
- Produces:
  - `pub struct Settings` (`#[non_exhaustive]`, `Default`, `Clone`, `Debug`). Fields and defaults:
    - `listen_addr: SocketAddr = 0.0.0.0:6881`
    - `max_peers_per_torrent: usize = 50`
    - `max_peers_global: usize = 200`
    - `max_half_open: usize = 10`
    - `unchoke_slots: usize = 4`
    - `max_outstanding_requests: usize = 32`
    - `max_incoming_requests: usize = 250`
    - `connect_timeout: Duration = 10s`
    - `handshake_timeout: Duration = 10s`
    - `peer_idle_timeout: Duration = 120s`
    - `keepalive_interval: Duration = 60s`
    - `request_timeout: Duration = 60s`
    - `rechoke_interval: Duration = 10s`
    - `status_interval: Duration = 500ms`
    - `event_queue: usize = 4096`
    - `num_want: u32 = 50`
    - `tracker: fbt_tracker::TrackerConfig = default`
  - `pub struct RateMeter`: a 5-second sliding window with 1-second buckets. Methods: `record(bytes: u64)`, `rate(&self, now: Instant) -> u64` (bytes per second), `total() -> u64`.
  - `pub(crate) enum PeerEvent` (peer → torrent), each variant carrying `key: PeerKey`:
    - `Connected { peer_id, reserved, addr }`
    - `Pieces(PeerPiecesOwned)`, where `PeerPiecesOwned::{All, None, Bits(Bitfield)}`
    - `Have(u32)`
    - `Choked`, `Unchoked`, `Interested`, `NotInterested`
    - `Block { block, data: Bytes }`
    - `Rejected(BlockRef)`
    - `AllowedFast(u32)`
    - `WantBlocks(usize)`
    - `Request(BlockRef)`, `Cancel(BlockRef)`
    - `Disconnected { reason: DisconnectReason, outstanding: Vec<BlockRef> }`
  - `pub(crate) enum PeerCommand` (torrent → peer): `Request(Vec<BlockRef>)`, `Cancel(BlockRef)`, `Choke`, `Unchoke`, `Interested(bool)`, `Have(u32)`, `SendBlock { block, data: Bytes }`, `Reject(BlockRef)`, `Close`.
  - `pub enum DisconnectReason`: `SelfConnection`, `InfoHashMismatch`, `HandshakeTimeout`, `IdleTimeout`, `Protocol(String)`, `Io(String)`, `Closed`, `Shutdown`.
  - `pub(crate) struct PeerCtx { key, info_hash: [u8; 20], our_id: PeerId, layout: PieceLayout, have: Bitfield, settings: Arc<Settings>, events: mpsc::Sender<PeerEvent> }`.
  - `pub(crate) fn spawn_outgoing(addr: SocketAddr, ctx: PeerCtx) -> mpsc::Sender<PeerCommand>` and `pub(crate) fn spawn_incoming<S: AsyncRead + AsyncWrite + Send + Unpin + 'static>(stream: S, remote: Handshake, addr: SocketAddr, ctx: PeerCtx) -> mpsc::Sender<PeerCommand>`. The incoming handshake has already been read by the listener.
  - `pub(crate) async fn run_peer<S: AsyncRead + AsyncWrite + Unpin>(stream: S, ...)`, generic over the stream so tests use `tokio::io::duplex`.

Behavior:

- **Handshake.** We send ours with the fast bit set. A remote info hash that differs from ours gives `InfoHashMismatch`; a remote peer id equal to ours gives `SelfConnection`.
- **Our pieces.** After the handshake we send `HaveAll`/`HaveNone` if both sides support fast and we are complete or empty, otherwise `Bitfield`.
- **Incoming bitfield.** A wrong length or spare bits set is a protocol error, which disconnects.
- **Codec limit.** `max_len = max(16_384 + 13, ceil(num_pieces / 8) + 1, 1 << 20)`.
- **Request pipeline.** When unchoked (or for allowed-fast pieces) the task emits `WantBlocks(max_outstanding - outstanding)` whenever outstanding drops below half the maximum. A block outstanding longer than `request_timeout` is reported through `Rejected` and cancelled.
- **Incoming `Piece` messages.** One that doesn't match an outstanding request by piece, offset and length is dropped and added to the wasted-bytes counter, without disconnecting.
- **Being choked.** Without fast, all outstanding requests come back as `Rejected` on `Choke`. With fast, only on an explicit `RejectRequest`.
- **Incoming requests.** They go to the torrent unvalidated (the torrent validates). The peer task itself drops any request beyond `max_incoming_requests` queued; with fast it answers `RejectRequest`.
- **Keepalives and idle.** A keepalive goes out after `keepalive_interval` of send silence. The connection closes after `peer_idle_timeout` of receive silence.

- [ ] **Step 1: Write failing tests** in `peer/conn.rs`, driving a scripted remote over `tokio::io::duplex` with `fbt_wire::MessageCodec` and `tokio::time::pause()`:
  - `handshake_info_hash_mismatch_disconnects`
  - `handshake_self_connection_detected`
  - `sends_have_all_when_complete_and_fast`
  - `bad_bitfield_is_protocol_error`
  - `requests_flow_when_unchoked`: Unchoke leads to `WantBlocks(32)`; `PeerCommand::Request` puts `Request` messages on the wire.
  - `unrequested_or_wrong_length_piece_is_dropped`: covers Review Focus 1.
  - `choke_without_fast_returns_outstanding`
  - `request_timeout_returns_block`
  - `idle_timeout_disconnects`
  - `keepalive_sent_when_quiet`
  - `incoming_request_flood_is_capped`
  - `rate_meter_window` (in `rate.rs`)
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session peer`
- [ ] **Step 3: Implement** a single `select!` loop over the framed read half, the command receiver and the timers.
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `feat(session): peer connection task with request pipeline and timeouts`

---

### Task 13: session, torrent actor, status and events

**Files:**
- Create: `crates/fbt-session/src/{session.rs,listener.rs,event.rs,status.rs,torrent/mod.rs,torrent/actor.rs}`, `crates/fbt-session/tests/{lifecycle.rs,support/mod.rs}`; deps `bitflags`, `tokio-stream` or a hand-written `Stream` (via `futures-core`).
- Modify: `crates/fbt-session/src/lib.rs` (public exports).

**Interfaces:**
- Consumes: Tasks 10–12.
- Produces (all public, `#[non_exhaustive]` where they're enums or structs with public fields):
  - `pub struct Session` (`Clone`), with:
    - `async fn new(settings: Settings) -> Result<Session, SessionError>`, which binds the TCP listener;
    - `fn local_addr(&self) -> SocketAddr`;
    - `async fn add_torrent(&self, params: AddTorrent) -> Result<TorrentHandle, SessionError>`;
    - `fn torrent(&self, ih: &InfoHashes) -> Option<TorrentHandle>`, `fn torrents(&self) -> Vec<TorrentHandle>`, `fn torrents_status(&self) -> Vec<TorrentStatus>`;
    - `fn subscribe(&self, mask: EventMask) -> EventStream`;
    - `async fn shutdown(&self)`, which is idempotent; later calls on any handle return `SessionError::Shutdown`.
  - `pub struct AddTorrent { pub metainfo: Metainfo, pub save_path: PathBuf, pub paused: bool, pub sequential: bool, pub file_priorities: Vec<Priority>, pub storage: Option<Arc<dyn Storage>> }`, with `AddTorrent::new(metainfo, save_path)`. Empty `file_priorities` means all `Normal`. `storage: None` means `FsStorage`.
  - `pub enum Priority { Skip, Low, Normal, High }`, which maps to piece priority 0/1/4/7. A piece takes the highest priority among the files it overlaps (pad files excluded).
  - `pub struct TorrentHandle` (`Clone`), with:
    - `fn info_hashes(&self) -> InfoHashes`, `fn status(&self) -> TorrentStatus` (synchronous, the latest `watch` value);
    - `async fn pause(&self)`, `async fn resume(&self)`, `async fn remove(&self, mode: RemoveMode)`, `async fn force_recheck(&self)`, `async fn force_reannounce(&self)`, all returning `Result<(), SessionError>`;
    - `async fn set_file_priorities(&self, p: Vec<Priority>) -> Result<(), SessionError>`;
    - `async fn add_peer(&self, addr: SocketAddr) -> Result<(), SessionError>`;
    - `async fn pieces(&self) -> Result<Bitfield, SessionError>`.
  - `pub enum RemoveMode { KeepData, DeleteData }`.
  - `pub struct TorrentStatus { pub info_hashes: InfoHashes, pub name: String, pub state: TorrentState, pub progress: f64, pub total_wanted: u64, pub wanted_done: u64, pub total_downloaded: u64, pub total_uploaded: u64, pub total_wasted: u64, pub download_rate: u64, pub upload_rate: u64, pub num_peers: u32, pub num_seeds: u32, pub error: Option<String> }`.
  - `pub enum TorrentState { CheckingFiles { progress: f64 }, Downloading, Seeding, Paused, Error }`.
  - `pub enum Event`, each variant carrying `torrent: InfoHashes` except `Lagged`:
    - `TorrentAdded`, `TorrentRemoved`
    - `StateChanged { state }`
    - `PieceFinished { piece }`, `HashFailed { piece }`
    - `TorrentFinished`
    - `TrackerReply { url, peers: usize }`, `TrackerError { url, message }`
    - `PeerConnected { addr }`, `PeerDisconnected { addr, reason: DisconnectReason }`
    - `FileError { path: Option<PathBuf>, message }`
    - `Lagged { missed: u64 }`
  - `bitflags! pub struct EventMask: u32 { STATUS, PIECE, TRACKER, PEER, STORAGE; ALL }`, with `Event::category(&self) -> EventMask`. `Lagged` always passes.
  - `pub struct EventStream`, implementing `futures_core::Stream<Item = Event>`, with an inherent `async fn recv(&mut self) -> Option<Event>`.
  - `pub enum SessionError`: `Bind(io::Error)`, `DuplicateTorrent`, `TorrentNotFound`, `InvalidPriorities { expected: usize, got: usize }`, `Shutdown`, `Storage(io::Error)`.

Behavior:

- **Adding a torrent** spawns one actor task. It goes to `CheckingFiles`: `check_pieces` runs in `spawn_blocking`, and status reports its progress. Then it moves to `Seeding` if every wanted piece is present, `Downloading` otherwise, or `Paused` if the torrent was added paused.
- **Status** is published at most every `status_interval` and immediately on a state change.
- **Writes.** Verified pieces are written in one `spawn_blocking` call; blocks are buffered in memory until the piece completes and its hash checks out. A write or read error puts the torrent in `TorrentState::Error`, emits `FileError`, disconnects its peers and stops its announces. The rest of the session is unaffected.
- **Serving.** The actor validates each incoming request: the piece is one we have, the block passes `is_valid_block` with `len ≤ 16 KiB`, and the peer is unchoked. Valid requests are read through `spawn_blocking` and sent with `SendBlock`.
- **Pausing** disconnects all peers and stops announces. Resuming restarts both.
- **Listener.** It reads the 68-byte handshake within `handshake_timeout` and routes the connection by `wire_hash` to the matching torrent. If there is none, or the torrent is full, the connection is dropped.
- **Connection limits.** The global and per-torrent peer limits and `max_half_open` are enforced in the actor and the session.

- [ ] **Step 1: Write failing tests** in `tests/lifecycle.rs`. `support/mod.rs` provides `fn make_torrent(files: &[(&str, usize)], piece_len: u32) -> (Metainfo, Vec<u8> /*payload*/)`, which generates random data and builds the metainfo through `DictBuilder`, and `async fn next_matching(stream, pred, timeout)`.
  - `add_then_status_downloading_with_no_data`
  - `add_with_complete_data_becomes_seeding`
  - `duplicate_add_rejected`
  - `pause_resume_state_events`
  - `remove_delete_data_removes_files`
  - `checking_reports_progress`: a large-ish 64-piece torrent; at least one `CheckingFiles` status observed via events.
  - `unwritable_save_path_puts_torrent_in_error`: covers Review Focus 3. On Unix the save path is a read-only temp dir; on Windows it is a path whose parent is a file. Feed one valid block via a scripted peer from `support`, then expect `FileError` and `TorrentState::Error`, while a second torrent in the same session keeps `Seeding`.
  - `event_mask_filters_and_lag_reported`: a subscriber with `event_queue = 4` that doesn't read gets `Lagged { missed > 0 }`.
  - `shutdown_is_idempotent_and_rejects_new_commands`
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test lifecycle`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `feat(session): session handle, torrent actor, status snapshots and events`

---

### Task 14: swarm download between sessions

**Files:**
- Modify: `crates/fbt-session/src/torrent/actor.rs` (wires picker, choker, peers and verification together, if Task 13 stubbed them).
- Create: `crates/fbt-session/tests/swarm.rs`.

**Interfaces:**
- Consumes: Task 13's API only (`add_peer` connects sessions on loopback, with no tracker).

Behavior added:

- **Rechoke.** Runs every `rechoke_interval` using `Choker`.
- **Interest.** We send `Interested(true)` when the peer has a wanted piece we lack, and `Interested(false)` once it doesn't.
- **Requests.** `WantBlocks` is answered from `PiecePicker::pick`.
- **Verification.** On `piece_complete` the piece is hashed with `sha1_piece` in `spawn_blocking`.
  - On a pass: write, `piece_passed`, send `Have` to all peers, emit `PieceFinished`.
  - On a fail: `piece_failed`, emit `HashFailed`.
- **Finishing.** When every wanted piece has passed, emit `TorrentFinished` and move to `Seeding`, if every piece is present; when some are skipped, the state is still `Seeding` per wanted pieces, as libtorrent's "finished" is.
- **Disconnect.** `release_peer` runs on `Disconnected`.
- **Retries.** A failed outgoing connection is retried after 60 s, at most 3 attempts. `SelfConnection` adds the address to a never-retry set.

- [ ] **Step 1: Write failing tests**
  - `two_sessions_download_multi_file`: 3 files (1 MiB, 0 B and 2.5 MiB), 64 KiB pieces. The seeder has the data, the leecher has none. After `add_peer(seeder.local_addr())`, the leecher reaches `TorrentFinished` within 30 s, the files are byte-identical, and the seeder's `total_uploaded >= payload`.
  - `download_from_two_seeds_uses_both`: both seeders have `total_uploaded > 0`.
  - `download_survives_seeder_leaving_mid_piece`: covers Review Focus 5. Use 1 MiB pieces and a seeder rate-throttled by a test `Storage` wrapper that sleeps on reads. Shut seeder A down once the leecher passes 30 %; the leecher still finishes from B.
  - `corrupt_seeder_causes_hash_failure_then_recovery`: seeder A has piece 2 corrupted in a `MemStorage`, and seeder B is good. The test sees `HashFailed { piece: 2 }` and still finishes with correct data.
  - `skipped_file_is_not_downloaded`: the middle file gets `Priority::Skip`; the leecher finishes and the skipped file is absent unless a boundary piece forced it to exist. Boundary pieces are allowed to create it, since part files come in milestone 5, so the assertion only covers non-boundary bytes.
  - `self_connection_is_dropped_and_not_retried`: covers Review Focus 2. `add_peer(own local_addr)` gives `PeerDisconnected { reason: SelfConnection }` exactly once in 3 s, even with a second `add_peer` of the same address.
  - `seeder_rejects_invalid_requests`: a scripted raw peer requests an out-of-range block and a 32 KiB block; the seeder drops or rejects them, and the connection stays up for the valid ones.
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session --test swarm`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS on all three OSes in CI.
- [ ] **Step 5: Commit.** `feat(session): swarm download, verification and seeding`

---

### Task 15: tracker announcing (tiers, events, intervals)

**Files:**
- Create: `crates/fbt-session/src/torrent/{announcer.rs,tiers.rs}`, `crates/fbt-session/tests/trackers.rs`.

**Interfaces:**
- Consumes: `fbt_tracker::{TrackerClient, AnnounceRequest, AnnounceEvent}`, `Metainfo::trackers`.
- Produces:
  - `pub(crate) struct Tiers`, with `new(tiers: Vec<Vec<String>>, rng) -> Self` (shuffles each tier once, per BEP 12), `current(&self) -> Option<&str>`, `report_success(&mut self)` (moves the URL to the front of its tier), `report_failure(&mut self)` (advances to the next URL, then the next tier, wrapping around), and `all_failed_once(&self) -> bool`.
  - The announcer task per torrent:
    - `started` on start or resume;
    - `none` after `interval`, or `min_interval` on a forced re-announce;
    - `completed` once when the torrent finishes, if it was downloading in this session;
    - `stopped` on pause, remove or shutdown, with a 5 s timeout and never retried.
  - After a failure the announcer backs off for 60 s × 2^n, capped at 30 min. Peers from replies feed the actor's candidate list. Each reply emits `TrackerReply` or `TrackerError`. `key` is random per torrent. Trackers with an unsupported scheme are skipped with one `TrackerError`.
  - `Session::shutdown` waits up to 5 s for all `stopped` announces.

- [ ] **Step 1: Write failing tests**
  - Unit tests in `tiers.rs`: `tier_advances_on_failure`, `success_moves_to_front`, `wraps_to_first_tier`.
  - Integration tests in `tests/trackers.rs`, using a fake HTTP tracker in `support` (a tokio TCP server keeping an in-memory swarm per info hash and recording each request's `event`):
    - `seeder_and_leecher_meet_via_http_tracker`: no `add_peer`, finishes.
    - `seeder_and_leecher_meet_via_udp_tracker`: a fake UDP tracker in `support`.
    - `announce_events_sequence`: the leecher's recorded events are `started`, `completed`, then `stopped` on shutdown.
    - `failing_first_tracker_falls_back_to_second_tier`
    - `tracker_failure_reason_emits_event`
- [ ] **Step 2: Run them and confirm they fail.** `cargo test -p fbt-session tiers` and `cargo test -p fbt-session --test trackers`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** PASS.
- [ ] **Step 5: Commit.** `feat(session): multi-tier tracker announcing`

---

### Task 16: fbt facade and example CLI

**Files:**
- Modify: `crates/fbt/src/lib.rs`, `crates/fbt-example/src/main.rs`.

**Interfaces:**
- Consumes: everything public from `fbt-session`, `fbt-metainfo` and `fbt-core`.
- Produces:
  - `fbt` re-exports: `Session`, `Settings`, `AddTorrent`, `TorrentHandle`, `TorrentStatus`, `TorrentState`, `Priority`, `RemoveMode`, `Event`, `EventMask`, `EventStream`, `DisconnectReason`, `SessionError`, `Storage`, `FsStorage`, `MemStorage`, `Metainfo`, `MetainfoError`, `InfoHashes`, `InfoHashV1`, `InfoHashV2`, `PeerId`, `Bitfield`, `TrackerConfig`. Crate-level `#![deny(missing_docs)]` and a crate doc with a complete download example, compiled as a doctest with `no_run`.
  - `fbt-example`, built with `clap` derive:
    - `download <TORRENT> --out <DIR> [--port <N>] [--seed-for <SECS>] [--peer <ADDR>]...`: prints one progress line per second (`{pct:5.1}% {down}/s down {up}/s up {peers} peers {state}`). It exits 0 after finishing, plus `--seed-for` if given; exits 1 on `TorrentState::Error`; and on Ctrl-C calls `shutdown`.
    - `seed <TORRENT> --data <DIR> [--port <N>] [--seed-for <SECS>]`: same output.
    - `info <TORRENT>`: name, info hash, piece count and size, files, trackers.
    - Logs go through `tracing-subscriber` with `RUST_LOG`.

- [ ] **Step 1: Write a failing test.** `crates/fbt-example/tests/cli.rs` runs the built binary (`env!("CARGO_BIN_EXE_fbt-example")`) with `info` on the metainfo fixture and asserts that the info hash appears in stdout. A second test, `download_from_seed_process`, spawns `seed`, then `download --peer 127.0.0.1:<port>`, waits for exit status 0 within 60 s, and compares the files.
- [ ] **Step 2: Run it and confirm it fails.** `cargo test -p fbt-example`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** `cargo test -p fbt-example && RUSTDOCFLAGS="-D warnings" cargo doc -p fbt --no-deps`: PASS.
- [ ] **Step 5: Commit.** `feat(fbt): public facade and example CLI`

---

### Task 17: fuzz targets

**Files:**
- Create: `fuzz/Cargo.toml` (`cargo fuzz init` layout, `[workspace] members = ["."]`), with `fuzz/fuzz_targets/{bencode.rs,metainfo.rs,wire_codec.rs,handshake.rs,http_announce.rs,udp_announce.rs,path.rs}` and `.github/workflows/fuzz.yml`.

**Interfaces:**
- Consumes: `fbt_bencode::decode`, `Metainfo::from_bytes`, `MessageCodec::decode` (input split into chunks by its first byte), `Handshake::decode`, `parse_http_announce`, `parse_udp_announce`, `sanitize_path` (asserting the `never_escapes` property).

- [ ] **Step 1: Write the targets.** Each target asserts no panic. `bencode` additionally checks that `encode(decode(x).to_owned_value())` decodes again.
- [ ] **Step 2: Run each locally for 60 s.** `cargo +nightly fuzz run <target> -- -max_total_time=60` should report no crashes. Seed corpora from the fixtures, committed under `fuzz/seeds/<target>/`; the generated corpus stays git-ignored.
- [ ] **Step 3: Write `fuzz.yml`.**
  - On pull requests touching `crates/fbt-{bencode,metainfo,wire,tracker}/**` or `fuzz/**`, every target runs for 60 s.
  - The weekly schedule (Mondays 05:30, as in `security.yml`) runs every target for 20 min, uploading crash artifacts on failure.
  - Use `dtolnay/rust-toolchain@nightly` and `cargo install cargo-fuzz --locked`.
- [ ] **Step 4: Commit.** `test(fuzz): fuzz targets for every remote-input parser`

---

### Task 18: Docker interop job

**Files:**
- Create: `dev/interop/{compose.yaml,run.sh,README.md}`, `dev/interop/qbittorrent/qBittorrent.conf`, `dev/interop/transmission/settings.json`.
- Modify: `.github/workflows/ci.yml` (job `interop`, `ubuntu-latest`, 20-minute timeout, uploading container logs on failure).

**Interfaces:**
- Consumes: the `fbt-example` binary (release build).

Setup (Linux only, everything `network_mode: host` on 127.0.0.1):

- **opentracker** on port 6969.
- **qBittorrent-nox** (libtorrent): peer port 6882, WebUI 8080 with `WebUI\LocalHostAuth=false`; DHT, PeX, LSD and UPnP off.
- **Transmission**: peer port 51413, RPC 9091 with auth disabled; DHT, PEX, LSD and port-forwarding off.
- Image tags are pinned by digest. Candidates are `qbittorrentofficial/qbittorrent-nox`, `lscr.io/linuxserver/transmission`, and an opentracker image; check each exists and record why it was chosen in the README.

`run.sh` steps:

1. `docker compose up -d` and wait for the ports.
2. Generate a 64 MiB random payload in 3 files under `target/interop/payload/`, and create `fixture.torrent` with `transmission-create -t http://127.0.0.1:6969/announce -s 256` inside the transmission container.
3. Add the torrent to both clients as already-complete seeds. qBittorrent uses `/api/v2/torrents/add` with `savepath` and `skip_checking=false`; Transmission uses `transmission-remote -a ... -w`.
4. **Download test:** `fbt-example download fixture.torrent --out target/interop/fbt --port 6890` must exit 0 within 5 minutes, `sha256sum` must match, and a log check must show both seeds supplied data. The latter is asserted from qBittorrent's `/api/v2/torrents/info` `uploaded > 0` and from `transmission-remote -t 1 -i` "Uploaded".
5. **Seed test:** create a second payload, `fixture2.torrent`. `fbt-example seed fixture2.torrent --data ... --port 6891 --seed-for 300` runs in the background. Add it to Transmission without data, wait until it reports 100 %, and compare the hashes.
6. `docker compose down` (in a trap).

- [ ] **Step 1: Write the files.** Run `bash dev/interop/run.sh` locally (Linux or a Linux VM; Docker Desktop on macOS doesn't support host networking the same way, so the README says Linux only).
- [ ] **Step 2: Verify.** The script prints `interop: download OK (qbittorrent, transmission)`, `interop: seed OK (transmission)` and exits 0.
- [ ] **Step 3: Add the CI job and push the branch.** The `interop` job passes.
- [ ] **Step 4: Commit.** `ci: live interop tests against qBittorrent and Transmission`

---

### Task 19: release workflow and docs

**Files:**
- Create: `.github/workflows/release.yml`, `dev/release-version.sh`, `dev/test-release-version.sh` (adapted from `../terminal/dev/`).
- Modify: `README.md` (usage example from `fbt`'s crate docs, the feature flags table, interop instructions), `CHANGELOG.md` (`## [Unreleased]` Added entries for milestone 1), `CLAUDE.md` commands block (interop, fuzz, check-pure-rust).

**Interfaces:**
- Produces: on a `v*` tag, `release.yml` runs these jobs:
  - `verify`: `dev/release-version.sh` refuses a tag that isn't `v<workspace version>`.
  - `sbom`: syft `v1.54.1` via `anchore/sbom-action/download-syft@v0`, writing `fbt-lib-<version>.cdx.json` from `Cargo.lock`.
  - `release`: a GitHub release with the tag's CHANGELOG section as the body (extracted by a small `awk` in the script) and the SBOM attached.
  - Nothing is published to crates.io.

- [ ] **Step 1: Write a test.** `bash dev/test-release-version.sh` covers the version resolver (accepts `v0.1.0` when the workspace is `0.1.0`, refuses `v0.1.1`), as in the terminal.
- [ ] **Step 2: Run it.** PASS.
- [ ] **Step 3: Write the workflow and docs.**
- [ ] **Step 4: Run the full gate.** `cargo fmt --all --check && cargo clippy --workspace --all-targets --all-features -- -D warnings && cargo test --workspace && cargo deny --all-features check && bash dev/check-pure-rust.sh && bash dev/sast.sh` all pass.
- [ ] **Step 5: Commit.** `ci: release workflow with CycloneDX SBOM`

---

## After the plan: shipping v0.1.0

PR `feature/m1-core-download` → `develop`; every CI job must be green, including `interop`. Then follow the Gitflow release:

1. `release/0.1.0` off `develop`, bumping the workspace version to `0.1.0` and moving the `Unreleased` changelog section to `## [0.1.0] - <date>` (the `-FB0100-` peer id prefix follows automatically).
2. PR into `main`, then the annotated tag `v0.1.0`, then merge `main` back into `develop`.

No attribution lines anywhere.

## Deferred to later milestones (recorded so nobody builds them now)

- **Milestone 2:** extension protocol handshake (BEP 10), DHT, magnets, MSE, PEX.
- **Milestone 3:** uTP, shared UDP socket, port mapping, LSD, rate limits, write-back cache, multiple listen addresses.
- **Milestone 4:** streaming reader.
- **Milestone 5:**
  - part files (skipped files never created), resume data, queueing, proxies, IP filter, web seeds, smart ban;
  - tracker scrape (BEP 48);
  - serde support in `fbt-bencode`.
- **Milestone 6:** v2-only torrents, symlink/executable attributes, torrent creation.
