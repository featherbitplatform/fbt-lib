# fbt-lib

A from-scratch, pure-Rust BitTorrent library with the feature breadth of libtorrent, and an idiomatic async Rust API. It is the engine for featherbittorrent, a future client with a terminal UI, a web UI and media streaming while downloading. It has no UI of its own and runs on macOS, Linux and Windows.

Status: pre-release. Milestone 1 (core download) shipped as v0.1.0. Milestone 2 added the Mainline DHT, magnet links with metadata exchange, MSE/PE encryption and peer exchange (v0.2.0). Milestone 3 added uTP, local and NAT reach, rate limits and a write-back cache (v0.3.0). Milestone 4 added streaming while downloading, with a seekable reader over a torrent that is still downloading (v0.4.0). Milestone 5 adds persistence and operations (resume data, queueing, proxies, IP filtering, web seeds and more DHT) and is heading for v0.5.0. Nothing is published to crates.io and the API will change.

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
- Streaming while downloading: a seekable `FileReader` (tokio `AsyncRead` and `AsyncSeek`) whose position sets deadlines on a read-ahead window, fetched first from the fastest peers; seeks replace the window, and several readers can share a torrent
- Resume data and session state as bytes (the library writes no state files), trusted on restart without a recheck when the files are unchanged
- Queueing of auto-managed torrents (3 downloads and 5 seeds at once by default) and optional seed ratio and seed time limits
- Moving a torrent's storage and renaming its files; skipped files are never created (a part file holds their pieces' shared bytes); colliding paths are renamed
- SOCKS5 and HTTP `CONNECT` proxies for peers, web seeds, trackers and, through SOCKS5, the DHT and uTP
- IP filter, one connection per IP per torrent, and smart ban of peers that send bad blocks
- HTTP and HTTPS web seeds (BEP 19)
- DHT scrape (BEP 33), `sample_infohashes` (BEP 51) and immutable and mutable items (BEP 44); tracker scrape
- `serde` for the public config structs (the `serde` feature)
- Pure Rust, no `unsafe`; hostile peer, tracker, proxy, web seed, resume and `.torrent` input is bounded

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

### Streaming while downloading

```rust
use std::io::SeekFrom;
use fbt::{AddTorrent, Metainfo, Session, Settings};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let metainfo = Metainfo::from_bytes(&std::fs::read("example.torrent")?)?;
    let session = Session::new(Settings::default()).await?;
    let torrent = session
        .add_torrent(AddTorrent::new(metainfo, "downloads".into()))
        .await?;

    // A reader on file 0 of the torrent, which is still downloading.
    let mut file = torrent.open_file(0).await?;
    file.seek(SeekFrom::Start(1 << 20)).await?;
    tokio::io::copy(&mut file.take(1 << 20), &mut tokio::io::sink()).await?;
    session.shutdown().await;
    Ok(())
}
```

`TorrentHandle::open_file` returns a `FileReader`. A read of a piece that is not there yet waits, and returns only bytes that passed their piece's hash check. The reader's position sets deadlines on the pieces of its read-ahead window (`ReadAhead`: at least 16 MiB, or 10 s at the stream's bitrate when that is more; `Settings::read_ahead`, `TorrentHandle::open_file_with` and `FileReader::set_read_ahead` change it), and those pieces are fetched first from the fastest peers, whatever the file's priority. Seeking replaces the deadlines. A read fails only with a `StreamError` (torrent removed, session shut down, or torrent in error); there is no read timeout, so wrap reads in `tokio::time::timeout`.

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

### Resume data and session state

```rust
use fbt::{AddTorrent, Metainfo, Session, SessionState, Settings};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let metainfo = Metainfo::from_bytes(&std::fs::read("example.torrent")?)?;
    let session = Session::new(Settings::default()).await?;
    let torrent = session
        .add_torrent(AddTorrent::new(metainfo, "downloads".into()))
        .await?;

    // One torrent: pause first, so nothing is written after the save.
    torrent.pause().await?;
    std::fs::write("example.resume", torrent.save_resume_data().await?)?;
    session.shutdown().await;

    // Next run: the saved pieces are trusted if the files are unchanged.
    let session = Session::new(Settings::default()).await?;
    let mut params = AddTorrent::from_resume(&std::fs::read("example.resume")?)?;
    params.paused = false; // it was saved paused
    let torrent = session.add_torrent(params).await?;
    println!("{} added", torrent.status().name);

    // The whole session: every torrent and the DHT state at shutdown.
    let (state, failed) = session.shutdown_and_save().await;
    for (info_hash, error) in failed {
        eprintln!("{info_hash} not saved: {error}");
    }
    std::fs::write("session.state", state.to_bytes())?;

    // Next run: one call restores them all.
    let state = SessionState::from_bytes(&std::fs::read("session.state")?)?;
    let (session, failed) = Session::restore(Settings::default(), &state).await?;
    for (index, error) in failed {
        eprintln!("torrent {index} not restored: {error}");
    }
    session.shutdown().await;
    Ok(())
}
```

The library writes no state files: resume data and the session state are bytes for the client to store. A resume file is trusted, and its pieces taken without a check, when every file holding a saved piece (and the part file, when it holds one) has the length and modification time, in whole seconds, it had at the save; otherwise the add runs a full check. `TorrentStatus::resume_trusted` says which happened. A file written after the save no longer matches, so save after `pause` or through `Session::shutdown_and_save`. Resume data is hostile input: bad data fails with a `ResumeError` naming the field.

### Queueing and seed limits

Torrents are auto-managed by default: in queue order, at most 3 downloads and 5 seeds run at once (500 in all), and the others wait as `TorrentState::Queued`. `Settings::queue` (`QueueSettings`) and `Session::set_queue_settings` change the limits; `TorrentHandle::set_queue_position` and `set_auto_managed` change one torrent. A torrent the user paused is never started by the queue. Seed limits (`Settings::seed_ratio_limit`, `seed_time_limit`) are off by default; reaching one pauses the torrent and emits `Event::SeedLimitReached`.

### Proxies

`Settings::proxy` (`ProxyConfig`) sends peer connections, web seeds and trackers through a SOCKS5 or HTTP `CONNECT` proxy; `proxy_peers`, `proxy_trackers` and `proxy_hostnames` (all on) choose what goes through it, and listening is unchanged. The caveats:

- SOCKS5 proxies all UDP or none: with `proxy_peers` or `proxy_trackers`, outgoing uTP, the DHT and UDP trackers all go through one UDP ASSOCIATE. The session then listens on TCP only, the DHT runs over the relay's address family only (IPv4 in practice), and incoming uTP arrives only as the relay forwards it.
- An HTTP proxy cannot carry UDP: with `proxy_peers` outgoing uTP is off, with `proxy_trackers` UDP trackers fail, and the DHT runs directly unless `dht_enabled` is off.
- Many HTTP proxies refuse `CONNECT` to port 80, so plain-HTTP trackers may fail through them.
- UDP tracker and DHT bootstrap host names are resolved locally whatever `proxy_hostnames` says, and LSD, UPnP and NAT-PMP stay direct on the local network.

### IP filter, bans and web seeds

`Session::set_ip_filter` installs an `IpFilter` (block and allow ranges, the last rule wins) for peers from every source, never for trackers, DHT nodes or the proxy. A torrent accepts one connection per IP address by default (`Settings::max_peers_per_ip`; loopback is exempt). Smart ban bans, for the rest of the session, the peer whose blocks made a piece fail its hash check, and never an honest co-contributor; bans live apart from the filter (`Session::banned_peers`). Blocked peers are reported as `Event::PeerBlocked`.

Web seeds (BEP 19) come from a torrent's `url-list`, a magnet's `ws` or `TorrentHandle::add_web_seed`, and are fetched with ranged HTTP(S) requests under the download limits and through the proxy. A failing seed backs off from 30 s to 30 minutes; a 4xx answer other than 408 and 429, or corrupt data, retires it (`Event::WebSeedError`).

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

`fbt-example` is a small command-line downloader and seeder built on the same API. `fbt-example download <torrent-or-magnet> --out DIR` takes a `.torrent` file or a `magnet:` link, with `--encryption forced|preferred|disabled`, `--dht-bootstrap HOST:PORT`, `--no-dht` and `--no-pex`, plus `--transport tcp|utp|both`, `--listen ADDR`, `--download-limit` and `--upload-limit`, `--cache-mb`, and `--lsd` and `--nat` (both off in the CLI); `fbt-example stream <torrent> --file N --out DIR` streams file `N` to stdout or a file while it downloads, fetching only what is read plus the read-ahead window, with `--seek`, `--length`, `--bitrate` and `--peer`; `fbt-example dht-node` runs a bare DHT node. `download` also takes `--resume PATH` (loads the resume data when the file exists, and writes it on exit after pausing; it prints `resume: trusted (<n> pieces)` or `resume: checking`), `--stop-at PERCENT` (pause, save, shut down and exit once that much is done) and `--web-seed URL` (repeatable); `download`, `seed` and `stream` take `--proxy socks5://[user:pass@]host:port` or `http://[user:pass@]host:port`. The CLI prints tracker replies and errors.

## Feature flags

Set on the `fbt` crate.

| Feature | Default | What it does |
|---|---|---|
| `tls` | yes | HTTPS trackers over rustls with the `ring` provider (needs a C toolchain to build) |
| `pure-rust` | no | HTTPS trackers over rustls with the pure-Rust RustCrypto provider: no C or assembly. Depend on `fbt` with `default-features = false, features = ["pure-rust"]`. The provider is a pre-release crate (`rustls-rustcrypto`), so expect churn |
| `serde` | no | `Serialize` and `Deserialize` for the public config structs and enums (`Settings`, `QueueSettings`, `ProxyConfig`, `TrackerConfig`, `RateLimit`, ...) and `IpFilter` (a list of `{ start, end, blocked }` rules); structs use `#[serde(default)]`, so a saved config missing newer fields still loads |

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

- uTP throughput and CPU, and the CPU of 1,000 idle torrents, miss their targets on the recorded benchmark (see [docs/benchmarks.md](docs/benchmarks.md)). Milestone 4 removed the in-flight window scans, the byte-wise payload queue and the zero-fill per read from the uTP hot path, but the measured numbers did not move: about 579 Mbit/s at 42.7 % of a core per 100 Mbit/s, against targets of 1000 Mbit/s and 15 %, with TCP at about 5100 Mbit/s. About 90 % of the on-CPU time is system calls (one `sendto` and one `recvfrom` per 1.4 KiB datagram, plus the reactor wakeups), and batching them (`sendmmsg`, `recvmmsg`, GSO) needs platform FFI, which the no-unsafe rule rules out. The idle CPU is to be revisited.
- Reading a skipped file of a finished torrent switches its state between `Seeding` and `Downloading` once per piece until the window is written. It is parked for a later fix.
- LSD joins the multicast group on the default interface only, and on a host with no default route it fails to start: the session then runs without LSD.

## Interop and fuzzing

Live interop against qBittorrent-nox and Transmission through opentracker, plus magnet via DHT, forced encryption, PEX, uTP with qBittorrent, streaming with seeks from qBittorrent, LSD between two fbt containers, resume without recheck against qBittorrent, a web seed served by nginx, and a download through a SOCKS5 proxy (Dante) on isolated networks with their own DHT node (no public node and no router is contacted), needs Docker (details in [dev/interop/README.md](dev/interop/README.md)):

```
bash dev/interop/run.sh
```

Fuzzing needs nightly and `cargo install cargo-fuzz --locked`. Targets: `bencode`, `metainfo`, `wire_codec`, `handshake`, `http_announce`, `udp_announce`, `path`, `krpc`, `extension`, `mse`, `magnet`, `utp_packet`, `utp_conn`, `lsd`, `ssdp`, `upnp_xml`, `natpmp`, `resume`, `socks5`, `http_connect`, `scrape`, `web_seed_head`; seeds live in `fuzz/seeds/<target>/`.

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
