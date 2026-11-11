# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

**fbt-lib**: a from-scratch, pure-Rust BitTorrent library with the feature breadth of libtorrent and an idiomatic async API. It is the engine for featherbittorrent (`../featherbittorrent`). Cargo workspace under `crates/`: `fbt-bencode`, `fbt-core`, `fbt-metainfo`, `fbt-wire`, `fbt-tracker`, `fbt-dht`, `fbt-session`, `fbt` (public facade) and `fbt-example`.

Binding design: `docs/superpowers/specs/2026-11-10-fbt-lib-design.md`. Read it before any feature work; it records which decisions are the owner's and which are the builder's calls. Plans live next to it under `docs/superpowers/plans/`.

## Rules specific to this repo

- **From scratch.** Every BitTorrent component (bencode, metainfo, wire protocol, trackers, DHT, uTP, ...) is written here. No BitTorrent crates (librqbit, `mainline`, uTP crates, bencode crates such as `serde_bencode` or `bendy`). General-purpose crates (tokio, tracing, thiserror, sha1, ...) are fine.
- **No unsafe.** `#![forbid(unsafe_code)]` at the top of every crate; the workspace lint forbids it too.
- **Remote input never panics, hangs or allocates without bound.** Peers, trackers and `.torrent` bytes are hostile. `unwrap`, `expect`, indexing and `panic!` on remote-derived values are rejected; clippy denies them in non-test code of `fbt-bencode`, `fbt-metainfo`, `fbt-wire`, `fbt-tracker` and `fbt-dht`.
- **`pure-rust` feature.** `cargo tree -e normal,build --no-default-features --features pure-rust -p fbt` (normal and build edges) must contain neither `ring` nor `cc` (nor `cmake`, `aws-lc*` or `*-sys` crates; `windows-sys` is bindings only); the default build may need a C toolchain only for `ring`.
- **Naming other clients is allowed.** libtorrent, qBittorrent, Transmission and opentracker may be named in code, docs and commits.
- **Logging and errors:** `tracing` for logs (the library never installs a subscriber), `thiserror` for errors. Public enums and config structs of `fbt-session` and `fbt` are `#[non_exhaustive]`; config structs implement `Default`.
- **Dependencies** are declared once in the root `[workspace.dependencies]`; crates inherit them, never pin their own versions. All crates are `publish = false` for now.
- **Tests never contact public DHT nodes.** `test_settings()` disables the DHT; tests that need one run their own nodes on loopback. `Settings::default()` bootstraps from the public routers, so never build a session from it in a test.
- **Cross-platform.** Builds and tests must pass on macOS, Linux and Windows.

## Conventions

- **Gitflow:** `main` holds tagged releases (`vX.Y.Z`) only; `develop` is the integration branch; work on `feature/<name>` off `develop`; `release/X.Y.Z` and `hotfix/<name>` as in the other featherbit repos.
- **Conventional Commits** with the crate as scope (`feat(bencode): ...`, `ci: ...`, `chore: ...`).
- **No attribution of any kind:** no `Co-Authored-By` trailer, no `Claude-Session` line or session link in commits, no "Generated with" footer or session link in PR bodies.
- **Semantic versioning.** The workspace version is bumped on the `release/X.Y.Z` branch of each release (it is `0.1.0` since that release).
- **Changelog:** `CHANGELOG.md` at the root (Keep a Changelog); user-visible changes go under `## [Unreleased]`.
- **CI gates:** `cargo fmt --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo test --workspace`, an MSRV check, `cargo doc` with warnings denied, plus the security scans in `.github/workflows/security.yml` (semgrep, cargo-deny, grype, gitleaks over the full history; also weekly on `main`). Their configs live at the repo root (`.semgrepignore`, `.gitleaks.toml`, `.grype.yaml`, `deny.toml`); every ignore carries a one-line reason.

## Commands

Run `source "$HOME/.cargo/env"` first if cargo is not on PATH.

```
# Full gate, as CI runs it (fmt is Linux-only in CI; deny is cargo-deny-action)
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo deny --all-features check
bash dev/check-pure-rust.sh   # pure-rust build has no ring, cc or other C-building crate

# Release plumbing: tag check + changelog extraction (tested with fake refs)
bash dev/test-release-version.sh
bash dev/release-version.sh --notes   # the current version's CHANGELOG section

# Local security scans via Docker (semgrep, deny, grype, gitleaks, sbom)
bash dev/sast.sh              # everything
bash dev/sast.sh semgrep      # one tool

# Live interop vs qBittorrent-nox and Transmission via opentracker, plus magnet via DHT, forced
# encryption, PEX, uTP with qBittorrent and LSD between two fbt-example containers, on isolated
# networks with our own dht-node (Docker; see dev/interop/README.md)
bash dev/interop/run.sh

# Fuzzing (nightly + `cargo install cargo-fuzz --locked`); targets: bencode metainfo wire_codec
# handshake http_announce udp_announce path krpc extension mse magnet utp_packet utp_conn lsd ssdp
# upnp_xml natpmp. Seeds live in fuzz/seeds/<target>/.
mkdir -p fuzz/corpus/<target>
cargo +nightly fuzz run <target> fuzz/corpus/<target> fuzz/seeds/<target> -- -max_total_time=60
```
