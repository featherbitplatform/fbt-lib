# fbt-lib

A from-scratch, pure-Rust BitTorrent library with the feature breadth of libtorrent, and an idiomatic async Rust API. It is the engine for featherbittorrent, a future client with a terminal UI, a web UI and media streaming while downloading. It has no UI of its own and runs on macOS, Linux and Windows.

Status: pre-release. Milestone 1 (core download) is in progress; nothing is published and the API will change.

## Build and test

```
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

## Design

The binding design is [docs/superpowers/specs/2026-11-10-fbt-lib-design.md](docs/superpowers/specs/2026-11-10-fbt-lib-design.md).

## License

Apache-2.0, see [LICENSE](LICENSE) and [NOTICE](NOTICE).
