# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-11-11

### Added

- `fbt-utp`: new crate with uTP (BEP 29) written from scratch: packet codec with selective ack, LEDBAT congestion control, RTT estimation, a sans-IO connection with retransmission, and `UtpSocket`/`UtpStream` over a shared or owned UDP socket; it never sends beyond the congestion and peer windows and buffers a bounded amount per connection ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-net`: new crate with a shared UDP socket demultiplexed by packet shape (uTP, DHT, UDP trackers), BEP 14 local service discovery, a bounded HTTP/1.1 client and XML scanner, UPnP IGD discovery and port mapping over SSDP and SOAP, and NAT-PMP and PCP port mapping with default-gateway discovery that needs no FFI ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-core`: the `udp` module with the `DatagramSender` and `DatagramHandler` traits that let the DHT, trackers and uTP run on sockets they do not own ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-core`: `rate::IpRateLimiter`, hoisted from `fbt-dht` ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-wire`: the `upload_only` (BEP 21), `lt_donthave` (BEP 54) and `ut_holepunch` (BEP 55) extension messages ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-dht` and `fbt-tracker`: the DHT and UDP trackers run on shared sockets, so they use the listen port; `AnnounceRequest` carries a port per family ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-session`: uTP peer connections, tried first for outgoing connections with TCP as the fallback, and `Settings::enable_outgoing_tcp`, `enable_incoming_tcp`, `enable_outgoing_utp` and `enable_incoming_utp`; `Transport` on connection events ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-session`: `Settings::listen_addrs` (IPv4 and IPv6, TCP and uTP on the same port number), `Session::local_addrs` and `Session::listen_errors` ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-session`: global, per-torrent and per-peer-class rate limits (`RateLimit`, `PeerClassLimits`, `MixedMode`, `Settings::rate_limit`, `peer_class_limits`, `rate_limit_local_peers`, `mixed_mode`), changed at runtime with `Session::set_rate_limit` and `Session::set_peer_class_limits`; a torrent's own limit with `AddTorrent::rate_limit`, changed at runtime with `TorrentHandle::set_rate_limit` ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-session`: choker settings (`ChokingAlgorithm` with a rate-based variant, `SeedChokingAlgorithm`, `Settings::unchoke_slots`, `optimistic_unchoke_slots`, `optimistic_unchoke_interval`, `seeding_piece_quota`), an adaptive request pipeline (`request_queue_time`) and snubbing (`snub_timeout`) ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-session`: a bounded write-back cache with coalesced writes and one session-wide budget (`Settings::cache_size`, `cache_expiry`, `Session::cache_stats`, `CacheStats`), a bounded disk pool (`disk_threads`) and a check queue (`max_active_checks`, `TorrentState::QueuedForChecking`) ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-session`: BEP 14 local service discovery (`enable_lsd`, `lsd_groups`, `lsd_announce_interval`, `lsd_bind`, `PeerSource::Lsd`); announces each public torrent at most every 5 minutes and rate-limits inbound messages per source IP ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-session`: UPnP and NAT-PMP/PCP port mapping with renewals and backoff (`enable_upnp`, `enable_natpmp`, `nat_gateway`), `Event::PortMapped`, `Event::PortMappingFailed`, `Event::ExternalIpChanged`, `EventMask::NAT`, `Session::external_ip` and `ExternalIpSource` ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-session`: BEP 55 holepunch over uTP (`enable_holepunch`), BEP 21 upload-only and BEP 54 `lt_donthave`, and `DisconnectReason::UploadOnly` for two peers that have nothing to trade ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-session`: `Storage::write_vectored`, a new trait method with a default implementation ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-dht`: `DhtStats::external_ip` ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt`: facade re-exports for `StorageError`, `RateLimit`, `PeerClassLimits`, `MixedMode`, `ChokingAlgorithm`, `SeedChokingAlgorithm`, `MapProtocol`, `MapMethod` and `ExternalIpSource`, with a rate-limit and NAT-events doctest ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-example`: `--transport tcp|utp|both`, `--listen ADDR`, `--download-limit`, `--upload-limit`, `--lsd`, `--nat` and `--cache-mb`; hidden `bench` and `lsd-probe` subcommands; the output line `peer connected <addr> via <tcp|utp>` ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `dev/bench.sh` and `docs/benchmarks.md`: loopback throughput and idle-torrent benchmark, recorded per release and not gated in CI ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- Fuzz targets `utp_packet`, `utp_conn`, `lsd`, `ssdp`, `upnp_xml` and `natpmp`, with seed corpora, and fixed-key MSE seeds ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- Interop scenarios: uTP download and seed with qBittorrent-nox, and LSD between two `fbt-example` containers, on isolated networks with no router ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))

### Changed

- `Settings::listen_addr` is now `Settings::listen_addrs`, and `Settings::dht_ipv6` is removed (the DHT follows the listen addresses). Breaking under 0.x ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `Event::PeerConnected` gains `transport` and `encrypted` ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-wire`: `ExtHandshake` gains `upload_only`; it is a pub struct that is not `#[non_exhaustive]`, so struct-literal construction no longer compiles ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-session` `choker`: `Choker::new` takes a `ChokerConfig`, `rechoke` takes `now`, and `PeerChokeInfo` gains fields ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-dht`: the new `DhtError::DuplicateFamily` variant ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- A torrent added with metadata now starts in `TorrentState::QueuedForChecking` instead of `CheckingFiles { progress: 0.0 }`, and moves to `CheckingFiles` once it gets a check slot; code that waited for `CheckingFiles` as the first state must accept `QueuedForChecking` before it ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `TorrentState`, `Event`, `PeerSource` and `DisconnectReason` gained variants (`QueuedForChecking`, `PortMapped`, `PortMappingFailed`, `ExternalIpChanged`, `Lsd`, `UploadOnly`); match with a wildcard arm. `EventMask::ALL` includes the new `NAT` bit ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `Event::PieceFinished` now fires once the piece is verified, before it is written ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `Seeding` and `TorrentFinished` fire only after the final flush, so the data is on disk when they are seen ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- Pieces count as ours (announced, served) once verified and cached, before they are written ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- A piece missing on disk while seeding is downloaded again instead of stopping the torrent ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `TorrentHandle::remove` and shutdown return `SessionError::Storage` when verified pieces could not be written (a `FileError` event is emitted as well) ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `Settings::max_outstanding_requests` defaults to 500, and the request pipeline adapts to each peer's rate ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- An outgoing uTP connect gives up after 3 s, as libtorrent does, and falls back to TCP; so does one refused locally because the uTP connection table is full ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `Settings::default()` now enables LSD, UPnP and NAT-PMP/PCP (`enable_lsd`, `enable_upnp`, `enable_natpmp`; `nat_gateway` overrides the discovered gateway), which sends multicast, SSDP and gateway datagrams on the LAN; switch them off to stay quiet ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- With incoming TCP disabled, the TCP port is released instead of being held bound but not listening ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-tracker`: `AnnounceRequest` gains `port_v4` and `port_v6`; the DHT `announce_peer` uses the first listener port of each family ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- Upload is counted once a block is written to the socket, not when it is queued ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- Choker: `optimistic_unchoke_interval` must be at least `rechoke_interval` (validated); automatic optimistic slots are `max(1, unchoke_slots / 5)`, at most `unchoke_slots - 1`, and reserved within `unchoke_slots`; the rate-based algorithm may exceed `unchoke_slots` ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- A torrent that reaches `Seeding` sends `UploadOnly(true)` to its peers ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- Incoming handshakes in progress are capped per source IP, and a duplicate connection is suppressed by a deterministic tie-break ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-net` `natpmp`: `Mapping` carries the PCP nonce and the gateway epoch, and `renew` and `unmap` take the previous mapping ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))

### Fixed

- `fbt-session`: the first retry of a failed outgoing connection now comes after 5 s (a twelfth of the retry delay) instead of a full minute, so a peer that was not ready yet (starting, checking its files, briefly full) no longer stalls a download that has no other peer ([#7](https://github.com/featherbitplatform/fbt-lib/pull/7))
- `fbt-session`: upload is no longer counted for blocks the peer cancelled ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-wire`: MSE drain errors are surfaced instead of swallowed ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-session`: file-handle eviction no longer syncs the file on every eviction ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-dht`: bootstrap attempts are spaced from their start, and a bootstrap whose attempt dies is rescheduled ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-session`: metadata pieces already received are kept when their source is retired ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-session`: PEX no longer rebuilds its peer list on every tick ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-utp`: an incoming connection reaches `UtpSocket::accept` only once the dialler acks our handshake, and at most `UtpConfig::max_pending_accept` connections are half-open (the oldest is dropped for a newer SYN), so a flood of forged SYNs no longer takes the session's pending-handshake slots or fills the uTP connection table ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-net`: a UPnP renewal asks for the external port it holds, so a mapping moved to a random port after a conflict is no longer leaked on the router at each renewal ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-utp`: a selective ack of any non-empty length is accepted. libtorrent (qBittorrent) sends only the bytes its bits need rather than a multiple of four, and such packets, data included, were dropped whole: transfers from libtorrent stalled for an RTO whenever it had a hole in our stream, and deadlocked once a lost packet of ours left every one of its packets carrying a SACK ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))
- `fbt-session`: an outgoing peer that traded data and then went away (closed, timed out or idle) is dialled again after the first retry delay (5 s by default) instead of only when a tracker or PEX names it again ([#8](https://github.com/featherbitplatform/fbt-lib/pull/8))

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

[0.3.0]: https://github.com/featherbitplatform/fbt-lib/releases/tag/v0.3.0
[0.2.0]: https://github.com/featherbitplatform/fbt-lib/releases/tag/v0.2.0
[0.1.0]: https://github.com/featherbitplatform/fbt-lib/releases/tag/v0.1.0
