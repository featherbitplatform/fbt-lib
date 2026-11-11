# fbt-lib

A from-scratch, pure-Rust BitTorrent library with the feature breadth of libtorrent, and an idiomatic async Rust API. It is the engine for featherbittorrent, a future client with a terminal UI, a web UI and media streaming while downloading. It has no UI of its own and runs on macOS, Linux and Windows.

Status: pre-release. Milestone 1 (core download) shipped as v0.1.0. Milestone 2 added the Mainline DHT, magnet links with metadata exchange, MSE/PE encryption and peer exchange (v0.2.0). Milestone 3 adds uTP, local and NAT reach, rate limits and a write-back cache, and is heading for v0.3.0. Nothing is published to crates.io and the API will change.

## Features

- Metainfo v1 and hybrid, HTTP(S) and UDP trackers, verified storage, piece picking and a tokio session with an event stream
- Mainline DHT (BEP 5, BEP 42 node ids) in its own crate, `fbt-dht`
- Magnet links with metadata exchange from peers (BEP 9, BEP 10)
- MSE/PE message stream encryption: forced, preferred (the default) or disabled
- Peer exchange (BEP 11), off for private torrents
- uTP (BEP 29) with LEDBAT, tried first for outgoing connections with TCP as the fallback, on the same port number as TCP and shared with the DHT and UDP trackers; IPv4 and IPv6 listening
- Local service discovery (BEP 14), UPnP and NAT-PMP/PCP port mapping, and BEP 55 holepunch over uTP
- Global, per-torrent and per-peer-class rate limits; rate-based and seeding choker variants; BEP 21 upload-only and BEP 54 `lt_donthave`
- Bounded write-back cache that coalesces writes, and a queue for file checks
- Pure Rust, no `unsafe`; hostile peer, tracker and `.torrent` input is bounded

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

### Magnet links

```rust
use fbt::{AddTorrent, Event, EventMask, Magnet, Session, Settings};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let magnet = Magnet::parse(
        "magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335aa7c1367a88a&dn=example",
    )?;
    let session = Session::new(Settings::default()).await?;
    let mut events = session.subscribe(EventMask::STATUS);
    let torrent = session
        .add_torrent(AddTorrent::magnet(magnet, "downloads".into()))
        .await?;
    while let Some(event) = events.recv().await {
        if let Event::MetadataReceived { .. } = event {
            break;
        }
    }
    println!("got metadata for {}", torrent.status().name);
    session.shutdown().await;
    Ok(())
}
```

### Rate limits and the cache

```rust
use fbt::{RateLimit, Session, Settings};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut settings = Settings::default();
    settings.rate_limit = RateLimit::new(2_000_000, 500_000);
    settings.cache_size = 128 * 1024 * 1024;
    let session = Session::new(settings).await?;
    // Limits can change while running; 0 means unlimited.
    session.set_rate_limit(RateLimit::new(0, 0));
    session.shutdown().await;
    Ok(())
}
```

`RateLimit::new(download, upload)` takes bytes per second. `Settings::cache_size` bounds the memory spent on dirty, clean and partial-piece bytes together; only verified bytes are served or written.

### What goes on your LAN

`Settings::default()` is not quiet on the local network. Each feature has its own switch:

| Traffic | Setting |
|---|---|
| Local service discovery (BEP 14): announces of public torrents to a multicast group, at most every 5 minutes per torrent | `enable_lsd` |
| UPnP: SSDP `M-SEARCH` datagrams to 239.255.255.250:1900 (two search targets, three rounds per discovery), then HTTP and SOAP to the router that answered | `enable_upnp` |
| NAT-PMP and PCP datagrams to the default gateway only | `enable_natpmp` |

All three are on by default; set them to `false` to stay off the LAN. Private torrents are never announced by LSD.

### DHT and encryption defaults

`Settings::default()` turns the DHT on and bootstraps from the public routers in `DEFAULT_DHT_BOOTSTRAP` (`router.bittorrent.com`, `dht.transmissionbt.com`, `router.utorrent.com`). Override them with `dht_bootstrap_nodes` (`host:port` entries), or turn the DHT off with `dht_enabled = false`. The library keeps no state of its own: a client saves `Session::dht_state()` (an opaque blob) on shutdown and passes it back as `Settings::dht_state` next run for a fast start. Private torrents never use the DHT or PEX (`pex_enabled` switches PEX off for the rest).

`Settings::encryption` defaults to `EncryptionPolicy::Preferred`: outgoing connections open with an MSE handshake and may fall back to plain text. `Forced` refuses plain text, `Disabled` never encrypts.

`fbt-example` is a small command-line downloader and seeder built on the same API. `fbt-example download <torrent-or-magnet> --out DIR` takes a `.torrent` file or a `magnet:` link, with `--encryption forced|preferred|disabled`, `--dht-bootstrap HOST:PORT`, `--no-dht` and `--no-pex`, plus `--transport tcp|utp|both`, `--listen ADDR`, `--download-limit` and `--upload-limit`, `--cache-mb`, and `--lsd` and `--nat` (both off in the CLI); `fbt-example dht-node` runs a bare DHT node.

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

## Benchmarks

`bash dev/bench.sh` runs a loopback throughput and idle-torrent benchmark and appends a row to [docs/benchmarks.md](docs/benchmarks.md), which also records the targets and the method. It is not gated in CI.

## Known limitations

- uTP throughput and CPU, and the CPU of 1,000 idle torrents, miss their targets on the recorded benchmark (see [docs/benchmarks.md](docs/benchmarks.md)); they are to be revisited.
- LSD joins the multicast group on the default interface only, and on a host with no default route it fails to start: the session then runs without LSD.

## Interop and fuzzing

Live interop against qBittorrent-nox and Transmission through opentracker, plus magnet via DHT, forced encryption, PEX, uTP with qBittorrent and LSD between two fbt containers on isolated networks with their own DHT node (no public node and no router is contacted), needs Docker (details in [dev/interop/README.md](dev/interop/README.md)):

```
bash dev/interop/run.sh
```

Fuzzing needs nightly and `cargo install cargo-fuzz --locked`. Targets: `bencode`, `metainfo`, `wire_codec`, `handshake`, `http_announce`, `udp_announce`, `path`, `krpc`, `extension`, `mse`, `magnet`, `utp_packet`, `utp_conn`, `lsd`, `ssdp`, `upnp_xml`, `natpmp`; seeds live in `fuzz/seeds/<target>/`.

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
