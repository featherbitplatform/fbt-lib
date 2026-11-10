# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-11-10

### Added

- `fbt-dht`: new crate with the Mainline DHT (BEP 5): node ids with BEP 42 restrictions, KRPC messages, routing table, announce tokens, a bounded peer store, rate limiting, iterative lookups, bootstrap with backoff, announce, external-IP votes and a persistable state blob; it bootstraps from nothing unless told to ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt-bencode`: `Limits::new(depth, items)`, the `MAX_DEPTH_CAP` hard depth cap and `DecodeError::TooManyItems` ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt-wire`: the extension protocol (BEP 10) with `ut_metadata` (BEP 9) and `ut_pex` (BEP 11) messages, `Reserved::set_dht` and `Reserved::set_extended`, and MSE/PE (message stream encryption) handshake and encrypted stream ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt-metainfo`: magnet URIs (`btih`, `btmh`, `so`), `Metainfo::from_info_bytes` to build a `Metainfo` from verified info bytes, and `Metainfo::magnet()` ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt-session`: magnet links with metadata exchange: `TorrentSource`, `AddTorrent::magnet`, `Settings::max_metadata_size`, `Event::MetadataReceived`, `TorrentState::DownloadingMetadata` and `DisconnectReason::BadMetadata` (peers that send bad metadata are banned for that torrent) ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt-session`: a session DHT with `Settings::dht_enabled` and `Settings::dht_bootstrap_nodes` (public routers by default, `DEFAULT_DHT_BOOTSTRAP`), `Settings::dht_state` and `Session::dht_state()` to persist the routing table across runs, `Event::DhtBootstrapped`, and `EventMask::DHT`, which is now part of `EventMask::ALL`; private torrents never use the DHT ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt-session`: `Settings::encryption` (`EncryptionPolicy`: `Forced`, `Preferred`, `Disabled`) for MSE/PE on outgoing and incoming connections, and `DisconnectReason::Encryption` for a failed MSE handshake ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt-session`: `Session::dht_stats()` returning `DhtStats`, and `Settings::dht_ipv6`, `Settings::dht_read_only` and `Settings::dht_announce_interval` ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt-session`: `Event::PeersFound` (with its `PeerSource`), `TorrentStatus::has_metadata` and `TorrentHandle::metainfo()` ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt-session`: `SessionError::NoMetadata` (a command that needs the metadata of a magnet still fetching it) and `SessionError::V2OnlyUnsupported` ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt-session`: peer exchange with `Settings::pex_enabled` and `PeerSource` on discovered peers; off for private torrents ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt`: facade re-exports for magnets, encryption and the DHT, with a magnet doctest ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt-example`: magnet download (`fbt-example download <magnet:...>`), `--encryption forced|preferred|disabled`, `--dht-bootstrap HOST:PORT`, `--no-dht`, `--no-pex`, and a `dht-node` subcommand that runs a bare DHT node ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- Fuzz targets `krpc`, `extension`, `mse` and `magnet`, with seed corpora, a persisted corpus and manual runs ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- Interop scenarios on an isolated network that never reaches the public DHT: magnet via DHT, forced encryption and PEX, against qBittorrent-nox and Transmission ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))

### Changed

- `AddTorrent.metainfo` is now `AddTorrent.source: TorrentSource`; `AddTorrent::new` still takes a `Metainfo`. Breaking under 0.x ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt-bencode`: `Limits` gained a field (`max_items`), so struct-literal construction no longer compiles; use `Limits::new` ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `Settings::default()` now has encryption `Preferred`: outgoing connections open with an MSE handshake and fall back to plain text only where the policy allows it ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `TorrentState` and `Event` gained variants (`DownloadingMetadata`, `MetadataReceived`, `DhtBootstrapped`); match with a wildcard arm ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- When two peers dial each other at once, the connection initiated by the lower peer id is kept ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `Settings::default()` now starts a DHT node and contacts the public routers (`DEFAULT_DHT_BOOTSTRAP`); set `dht_enabled = false` to stay off the DHT ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `TorrentHandle::force_recheck` and `TorrentHandle::set_file_priorities` can return `SessionError::NoMetadata` for a magnet without its metadata ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `EventMask::PEER` also carries `Event::PeersFound` ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))

### Fixed

- `fbt-session`: the `completed` tracker announce is sent at most once, and is retried after a failed attempt ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt-session`: the `left` value of a `stopped` announce after a recheck is the last known one ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt-session`: a magnet answers incoming peers during the check that follows its metadata instead of dropping them ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt-session`: a torrent whose check after the metadata fails stops like any errored torrent: it announces `stopped`, refuses incoming peers, and a resume starts over cleanly ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt-session`: a slow metadata source can no longer stall a magnet: each request times out on its own, and a source that has not delivered the whole metadata within its deadline gives its slot to the next one ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt-session`: the status read on `TorrentFinished` already has the final counters and the `Seeding` state, instead of the last periodic snapshot ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))
- `fbt-dht`: a node that queries us enters the routing table only after answering our ping, so spoofed sources cannot fill it ([#4](https://github.com/featherbitplatform/fbt-lib/pull/4))

## [0.1.0] - 2026-11-10

### Added

- `fbt-bencode`: bencode decoder and encoder with strict canonical-form checks, bounded depth and exact-span access for info-hash computation ([#1](https://github.com/featherbitplatform/fbt-lib/pull/1))
- `fbt-core`: info hashes (v1, v2 and hybrid), peer ids (`-FB0100-` prefix), bitfields, piece and file layouts, block references ([#1](https://github.com/featherbitplatform/fbt-lib/pull/1))
- `fbt-metainfo`: `.torrent` parsing for v1 and hybrid torrents (v2-only from milestone 6), with hostile-input limits (at most 64 tracker URLs) and path sanitising (Windows device names included) ([#1](https://github.com/featherbitplatform/fbt-lib/pull/1))
- `fbt-wire`: peer wire protocol codec (handshake and messages) with bounded frame sizes ([#1](https://github.com/featherbitplatform/fbt-lib/pull/1))
- `fbt-tracker`: HTTP(S) and UDP tracker announces over rustls, with tracker-supplied text cut to 1 KiB, time-bounded UDP host lookups and an IPv4/IPv6 fallback; `TrackerConfig` is `#[non_exhaustive]` ([#1](https://github.com/featherbitplatform/fbt-lib/pull/1))
- `fbt-session`: async session on tokio with torrent management, piece picking, verified storage (`FsStorage`, `MemStorage`), peer connections, tracker announces and an event stream; `Session::new` validates its `Settings` (`SessionError::InvalidSettings`), duplicate peer ids are refused (`DisconnectReason::Duplicate`), and remove and shutdown cancel a running piece check ([#1](https://github.com/featherbitplatform/fbt-lib/pull/1))
- `fbt`: public facade crate re-exporting the stable API ([#1](https://github.com/featherbitplatform/fbt-lib/pull/1))
- `fbt-example`: command-line downloader and seeder built on the facade ([#1](https://github.com/featherbitplatform/fbt-lib/pull/1))
- Features `tls` (default, rustls with `ring`) and `pure-rust` (rustls with the RustCrypto provider, no C or assembly), with a `dev/check-pure-rust.sh` guard ([#1](https://github.com/featherbitplatform/fbt-lib/pull/1))
- Fuzz targets for bencode, metainfo, the wire codec, handshake, HTTP and UDP announce parsing and path handling, with seed corpora and a fuzz workflow ([#1](https://github.com/featherbitplatform/fbt-lib/pull/1))
- Interop tests against qBittorrent-nox and Transmission through opentracker (`dev/interop/run.sh`, run in CI) ([#1](https://github.com/featherbitplatform/fbt-lib/pull/1))
- Security CI: semgrep, cargo-deny, grype and gitleaks (`.github/workflows/security.yml`, `dev/sast.sh`) ([#1](https://github.com/featherbitplatform/fbt-lib/pull/1))
- Release workflow: tag check against the workspace version, CycloneDX SBOM and a GitHub release with the changelog section (`.github/workflows/release.yml`) ([#1](https://github.com/featherbitplatform/fbt-lib/pull/1))

[0.2.0]: https://github.com/featherbitplatform/fbt-lib/releases/tag/v0.2.0
[0.1.0]: https://github.com/featherbitplatform/fbt-lib/releases/tag/v0.1.0
