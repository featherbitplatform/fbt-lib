# fbt-lib

A from-scratch, pure-Rust BitTorrent library with the feature breadth of libtorrent, and an idiomatic async Rust API. It is the engine for featherbittorrent, a future client with a terminal UI, a web UI and media streaming while downloading. It has no UI of its own and runs on macOS, Linux and Windows.

Status: pre-release. Milestone 1 (core download: metainfo, peer wire, HTTP and UDP trackers, a tokio session, verified storage) is complete and v0.1.0 is next; nothing is published to crates.io and the API will change.

## Usage

```rust
use fbt::{AddTorrent, Event, EventMask, Metainfo, Session, Settings, TorrentState};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bytes = std::fs::read("example.torrent")?;
    let metainfo = Metainfo::from_bytes(&bytes)?;

    let session = Session::new(Settings::default()).await?;
    let mut events = session.subscribe(EventMask::STATUS);
    let torrent = session
        .add_torrent(AddTorrent::new(metainfo, "downloads".into()))
        .await?;

    // Already complete data goes straight to Seeding without
    // TorrentFinished; a storage error ends in Error.
    while let Some(event) = events.recv().await {
        match event {
            Event::TorrentFinished { .. }
            | Event::StateChanged {
                state: TorrentState::Seeding | TorrentState::Error,
                ..
            } => break,
            _ => {}
        }
    }
    println!("{:.1}% done", torrent.status().progress * 100.0);
    session.shutdown().await;
    Ok(())
}
```

`fbt-example` is a small command-line downloader and seeder built on the same API.

## Feature flags

Set on the `fbt` crate.

| Feature | Default | What it does |
|---|---|---|
| `tls` | yes | HTTPS trackers over rustls with the `ring` provider (needs a C toolchain to build) |
| `pure-rust` | no | HTTPS trackers over rustls with the pure-Rust RustCrypto provider: no C or assembly. Depend on `fbt` with `default-features = false, features = ["pure-rust"]`. The provider is a pre-release crate (`rustls-rustcrypto`), so expect churn |

`bash dev/check-pure-rust.sh` verifies that the `pure-rust` build pulls in neither `ring` nor `cc` (nor any other C-building crate), through normal and build dependencies.

## Build and test

```
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all --check
cargo deny --all-features check
```

## Interop and fuzzing

Live interop against qBittorrent-nox and Transmission through opentracker needs Docker (details in [dev/interop/README.md](dev/interop/README.md)):

```
bash dev/interop/run.sh
```

Fuzzing needs nightly and `cargo install cargo-fuzz --locked`. Targets: `bencode`, `metainfo`, `wire_codec`, `handshake`, `http_announce`, `udp_announce`, `path`; seeds live in `fuzz/seeds/<target>/`.

```
mkdir -p fuzz/corpus/<target>
cargo +nightly fuzz run <target> fuzz/corpus/<target> fuzz/seeds/<target> -- -max_total_time=60
```

## Security scans

`bash dev/sast.sh` runs semgrep, cargo-deny, grype, gitleaks and a CycloneDX SBOM via Docker (one tool: `bash dev/sast.sh semgrep`). The same scans run in CI, and weekly on `main`. Tagged releases publish a CycloneDX SBOM with the GitHub release.

## Design

The binding design is [docs/superpowers/specs/2026-11-10-fbt-lib-design.md](docs/superpowers/specs/2026-11-10-fbt-lib-design.md).

## License

Apache-2.0, see [LICENSE](LICENSE) and [NOTICE](NOTICE).
