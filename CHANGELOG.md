# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `fbt-api`: a new crate, an HTTP/JSON API over an `fbt::Session` built on axum (only this crate depends on a server stack; the core crates stay server-free). `router(session, &ApiConfig)` returns an `axum::Router` to mount in your own server and `ApiServer::start` runs a standalone one. Route groups under `/api/v1`:
  - torrents: list, status, `files`, `peers`, `trackers`, `pieces`, `resume-data` and `magnet`
  - control: add (`.torrent`, magnet or URL), add from resume data, remove, pause, resume, recheck, reannounce, scrape, file priorities, per-torrent limits, queue position, auto-managed, move, rename file, web seeds and manual peers
  - session: settings and state, rate limits, queue settings, IP filter and banned peers
  - `create-torrent`: torrent creation with at most 2 creations at once
  - events: a server-sent event stream with category filters, a bounded subscriber queue and a `lagged` notice for a slow client
  - streaming: `GET` and `HEAD` of a torrent file with single `Range` requests (`206`, `416`, a `Range` the parser cannot serve falls back to a full `200`), fetching only what is read
  - `GET /api/v1/openapi.json`: the OpenAPI 3.1 document, served without a token and also available as `fbt_api::openapi()`
- `fbt-api` auth model: `Auth::Token` (bearer token on every request; `?token=` is accepted on the stream and event URLs only, for media elements and `EventSource`) or `Auth::Disabled`, which needs a loopback `bind` and answers `403` to a `Host` that is not `localhost`, `127.0.0.1` or `[::1]` and to an `Origin` not in `cors_origins` (the CSRF and DNS-rebinding guard); optional CORS origins
- `fbt-api` limits: `allowed_roots` confines every path a request names (save paths, move targets, creation sources) to directories, with a lexical check before any filesystem access, symlinks resolved and refused when they leave the roots, `403 path_not_allowed` otherwise; request bodies capped (`MAX_BODY_BYTES`, 32 MiB by default, `413`), at most `max_streams` (32) file streams and `max_event_streams` (16) event streams at once (`503`), and caps on every collection a request carries
- `ApiServer::shutdown` is graceful and bounded: it stops accepting, ends event and file streams and aborts the remaining connections after a deadline
- `fbt-session`: `TorrentHandle::files`, `peers` and `trackers` with the new `FileStatus`, `PeerStatus` and `TrackerStatus` types (re-exported by `fbt`)
- `fbt-session`: `Session::rate_limit`, `peer_class_limits`, `queue_settings` and `is_shut_down`, the readers for what the session was configured with
- `fbt-example`: `--api ADDR --api-token TOKEN` on `download` and `seed` serve the API next to the transfer (either flag alone is a usage error)
- Fuzz target `http_range` (the `Range` header parser) with seeds
- Interop scenario `api`: `fbt-example download --api` serves the API while it downloads, and curl lists the torrent, streams byte ranges, checks 401 and 416 and follows the event stream

### Changed

- `PeerSource` gains the variants `Incoming`, `Manual`, `Resume` and `Holepunch`; match it with a wildcard arm. Redials of remembered peers keep the source they were learnt from, and the first source that learnt an address wins
- Peer client names are sanitised: control and Unicode format characters are stripped and a name is at most 64 characters. The libtorrent Azureus ids are corrected (`LT` is "libtorrent", `lt` is "libTorrent")

## [0.6.0] - 2026-11-13

### Added

- BitTorrent v2 and hybrid torrents (BEP 52): `fbt-core` gains Merkle trees (`merkle` module: `MerkleTree`, `piece_root`, `layer_root`, `leaf_hashes`, `file_tree_height`, `verify_hashes`); `fbt-metainfo` parses v2-only and hybrid `.torrent` files with their piece layers (`Metainfo::version`, `TorrentVersion`, `Metainfo::v2_piece`, `V2Piece`, `Metainfo::with_piece_layers`, `validate_layer`, `MetainfoError::InconsistentHybrid`, `MAX_PIECE_LAYER_HASHES`, `MAX_V2_PIECES`); a hybrid is checked for consistency between its halves; `fbt-session` verifies pieces by SHA-1, by Merkle root or by both, and builds the piece layers a hybrid lacks while it downloads ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- `fbt-metainfo`: torrent creation: `CreateTorrent` (`new`, `version`, `piece_len`, `trackers`, `web_seeds`, `comment`, `created_by`, `creation_date`, `private`, `build` with a progress callback that can cancel), `CreateError`, `auto_piece_len` and `MAX_CREATE_FILES`, for v1, v2 and hybrid torrents; re-exported by `fbt`, with a creation doctest ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- `fbt-wire`: the BEP 52 messages `HashRequest`, `Hashes` and `HashReject` (`MAX_HASH_REQUEST_LEN`) and `Reserved::supports_v2` and `Reserved::set_v2` ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- `fbt-session`: a torrent is registered under every wire hash (a hybrid under both its v1 and truncated v2 hash) and a `btmh`-only (v2) magnet is accepted; `Session::torrent` also finds a torrent by any of its wire hashes ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- `fbt-session`: hash serving: a seed answers hash requests from its piece layers (Merkle trees cached, serving jobs and pending requests capped); a v2-only torrent whose `.torrent` or magnet metadata lacks piece layers fetches them from v2 peers over hash requests, verified against each file's `pieces root` ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- `fbt-session`: a hybrid torrent announces under both its info hashes: every tracker, the DHT and LSD run once per hash ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- `fbt-session`: resume data version 2 stores v2 info hashes and piece layers (see Changed) ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- `fbt-example`: `create PATH --out FILE [--format v1|v2|hybrid] [--piece-size BYTES] [--tracker URL] [--web-seed URL] [--private] [--comment TEXT]` ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- Fuzz target `hash_proof` (Merkle proof verification and hash messages), with seed corpora for the new wire messages ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- Interop scenarios `v2` and `hybrid` against qBittorrent-nox and Transmission: created torrents give the same hashes as qBittorrent's creator, v2 downloads and seeds both ways, a `btmh` magnet fetches its layers over hash requests ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))

### Changed

- `SessionError::V2OnlyUnsupported` is deprecated and no longer produced: v2 (`btmh`-only) magnets are accepted, `Session::torrent()` also finds a torrent by any of its wire hashes and a hybrid is registered under both hashes ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- `MetainfoError::V2OnlyUnsupported` is deprecated and no longer produced ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- `FileEntry` gains `pieces_root` (breaking for struct literals), `Metainfo::piece_layers` is new, and `Info.pieces` is empty for a v2-only torrent ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- The v2 reserved bit is set on the handshakes of torrents that have a v2 hash, and a connection made under a truncated v2 hash counts as v2 ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- A v2-only `.torrent` without piece layers starts in `DownloadingMetadata` (`has_metadata` false, `metainfo()` `None`) until its layers are fetched ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- `Event::TrackerReply` gains `info_hash`, the hash the announce was made under (breaking for struct patterns without `..`); hybrids announce under both hashes ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- `fbt_session::storage::check_pieces` has a new signature: it takes the `Metainfo` and also returns the v2 roots of the pieces that passed ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- Resume data is version 2, which older builds cannot read: `info-hash` is optional, and the new keys are `info-hash2` and `piece-layers`; version 1 is still read ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- `fbt_wire::Message` gains three variants (`HashRequest`, `Hashes`, `HashReject`); match with a wildcard arm ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- `fbt-example info` prints the wire hash (40 hex), `info hash v2:` when there is one, and `version:` ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- Created multi-file hybrids carry a tail pad after their last file, so their hashes match libtorrent 2.0 and qBittorrent 5.3 exactly (a single-file hybrid has no file list to hold one) ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- `ResumeError::InfoHashMismatch` reads "the resume data's torrent does not match its info hash": it now covers `info-hash2` too ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- A v2 file tree is single-file only when its one top-level entry is a file whose key equals `name`; any other tree is a directory named `name` ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))
- `fbt-metainfo`: a torrent without v1 `pieces` (v2-only) is refused with `MetainfoError::InvalidField` (`length`, "too many pieces") when it has more than `MAX_V2_PIECES` (4,194,304, equal to `MAX_PIECE_LAYER_HASHES`) pieces, both from a `.torrent` and from metadata fetched for a `btmh` magnet; `CreateTorrent::build` refuses such a v2-only torrent with the new `CreateError::TooManyPieces`. Before, a 154-byte v2 `.torrent` could claim 2^32 - 1 pieces and abort the process when the session sized its per-piece state ([#17](https://github.com/featherbitplatform/fbt-lib/pull/17))

## [0.5.0] - 2026-11-13

### Added

- `fbt-session`: resume data: the async `TorrentHandle::save_resume_data` returns a torrent's state as bytes (its `info` dictionary, save path, priorities, renamed files, verified pieces, web seeds, queue position, counters, times, up to `MAX_RESUME_PEERS` peers per family and a `FileStamp` per file and for the part file), and `AddTorrent::from_resume` turns it back into add parameters; the saved pieces are trusted without a check when every file holding one has the length and modification time it had at the save, else the torrent is checked in full; hostile resume data fails with `ResumeError` (`MAX_RESUME_BYTES`, `MAX_RESUME_ITEMS`) ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-session`: session state: `SessionState` (`to_bytes`, `from_bytes`, at most `MAX_SESSION_TORRENTS` torrents) with the async `Session::save_state` and `Session::shutdown_and_save`, which return the DHT state and every torrent's resume data in queue order plus the torrents they could not save with their error, and the async `Session::restore`, which starts a session from it and returns the torrents it could not add ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-session`: `Storage::stamps` and `FileStamp` (with `FileStamp::new` for custom storages); a custom `Storage` without stamps is trusted ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-session`: auto-managed queueing as libtorrent's: `QueueSettings` (`active_downloads` 3, `active_seeds` 5, `active_limit` 500, `dont_count_slow_torrents` with `inactive_down_rate`, `inactive_up_rate` and `startup_grace`, and `interval`), `Settings::queue`, `Session::set_queue_settings`, `AddTorrent::auto_managed`, and the async `TorrentHandle::set_auto_managed` and `TorrentHandle::set_queue_position`; a torrent the user paused is never started by the queue ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-session`: seed limits, off by default: `Settings::seed_ratio_limit` and `Settings::seed_time_limit` pause an auto-managed seed, clear its `auto_managed` and emit `Event::SeedLimitReached` ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-session`: the async `TorrentHandle::move_storage` with `MoveMode` (`FailIfExists`, the default, `ReplaceExisting`, `KeepExisting`), which writes every cached piece first and moves back what moved on a failure, and the async `TorrentHandle::rename_file`; `Event::StorageMoved`, `StorageMoveFailed`, `FileRenamed` and `FileRenameFailed`; `Storage::move_to` and `Storage::rename_file`, and `FsStorage::with_paths` and `FsStorage::save_path` ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-session`: a part file, as libtorrent's: the bytes of skipped files that a wanted file's pieces share go to `<save_path>/.<info hash>.parts` (`FsStorage::part_path`), and move into the file when it becomes wanted; `Storage::set_file_priorities` ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-net`: the `proxy` module with SOCKS5 (RFC 1928, username and password per RFC 1929) and HTTP `CONNECT` (Basic) clients: `ProxyConfig`, `ProxyKind`, `ProxyTarget`, `ProxyError`, `connect_tcp`, `udp_associate` and `Socks5Udp`, with replies bounded (`MAX_CONNECT_HEAD` = 8 KiB, interim `1xx` CONNECT replies skipped) and the password hidden from `Debug`; `ProxyConfig::validate`; `SharedUdpSocket::bind_relayed`, `SharedUdpSocket::relay_lost` and `DemuxStats::relay_lost`; for the fuzz targets (hidden from the docs) `parse_connect_head` and `parse_socks5_reply`, and the SOCKS5 UDP framing `encode_udp` and `decode_udp` ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-session`: `Settings::proxy`: peers, web seeds and trackers through a SOCKS5 or HTTP proxy, and, through SOCKS5, outgoing uTP, the DHT and UDP trackers over one UDP ASSOCIATE; `SessionError::Proxy` carrying the `ProxyError`; `Event::ProxyError` (under `EventMask::STATUS`) when the SOCKS5 proxy closes the UDP association, which stops the DHT, uTP and UDP trackers until the session restarts; a warning at `Session::new` for each enabled feature that bypasses the proxy (with an HTTP proxy the DHT and uTP, with any proxy LSD, UPnP and NAT-PMP) ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-net`: `ipfilter::IpFilter` (block and allow ranges, the last rule wins, at most `MAX_RANGES`), and in `fbt-session` the async `Session::set_ip_filter`, `Session::ip_filter`, `Session::banned_peers`, `Settings::max_peers_per_ip`, `BlockReason` and `Event::PeerBlocked` ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-session`: smart ban, as libtorrent's plugin: the sender of a block that made a piece fail its hash check is banned for the session once the piece passes, and peers lose trust for failed pieces until they are banned; bans live apart from the IP filter ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-session`: BEP 19 web seeds: from `url-list`, `ws` or the async `TorrentHandle::add_web_seed`, `remove_web_seed` and `web_seeds`; ranged requests of whole pieces, under the download limits and through the proxy, with backoff and retirement reported as `Event::WebSeedError`; a web seed also finishes pieces a peer left partial (it fetches the piece's range and keeps the blocks still missing); `TorrentStatus::num_web_seeds` ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-metainfo`: `Metainfo::web_seeds` and `Magnet::web_seeds` (`url-list` and `ws`, at most `MAX_WEB_SEEDS` = 16), `collides` and `RelPath::key` ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-tracker`: scrape (BEP 48 over HTTP, action 2 over UDP): `TrackerClient::scrape`, `ScrapeInfo` and `MAX_SCRAPE_HASHES` = 74; `TrackerClient::fetch_range` for web seeds; hidden from the docs, for the fuzz targets and the session, `parse_http_scrape`, `parse_udp_scrape`, `parse_content_range`, `TrackerClient::fetch_range_paced` and `RangePace` ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-session`: the async `TorrentHandle::scrape_trackers`, reporting through `Event::ScrapeReply` and `Event::ScrapeFailed` ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-dht`: BEP 33 scrape and BEP 51 `sample_infohashes`: `Dht::scrape`, `Dht::sample_infohashes`, `DhtScrape` and `DhtSamples`; a seeding torrent announces itself as a seed ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-dht`: BEP 44 immutable and mutable items: `Dht::get_immutable`, `put_immutable`, `get_mutable` and `put_mutable`, `MutableItem`, `DhtConfig::max_items` (700), `MAX_ITEM_VALUE`, `MAX_SALT` and the BEP 44 error codes ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-session`: the async `Session::dht_scrape`, `dht_sample_infohashes`, `dht_get_immutable`, `dht_put_immutable`, `dht_get_mutable` and `dht_put_mutable` ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- The `serde` feature on `fbt`, `fbt-session`, `fbt-tracker` and `fbt-net`: `Serialize` and `Deserialize` for the public config structs and enums (`Settings`, `QueueSettings`, `ProxyConfig`, `ProxyKind`, `TrackerConfig`, `ReadAhead`, `RateLimit`, `PeerClassLimits`, `MixedMode`, `ChokingAlgorithm`, `SeedChokingAlgorithm`, `EncryptionPolicy`, `Priority`, `RemoveMode`, `MoveMode`) and `IpFilter` as a list of rules ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt`: facade re-exports for `SessionState`, `ResumeError`, `FileStamp`, `QueueSettings`, `MoveMode`, `ProxyConfig`, `ProxyKind`, `ProxyError`, `IpFilter`, `BlockReason`, `DhtError`, `DhtScrape`, `DhtSamples`, `MutableItem`, `NodeId` and `NodeInfo`, and the limits `MAX_RESUME_BYTES`, `MAX_RESUME_ITEMS`, `MAX_RESUME_PEERS`, `MAX_SESSION_TORRENTS`, `MAX_IP_FILTER_RANGES` (`fbt_net::ipfilter::MAX_RANGES`, also re-exported by `fbt-session`) and `MAX_WEB_SEEDS`, with a resume data doctest ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-example`: `download --resume PATH`, `--stop-at PERCENT` and `--web-seed URL`, and `--proxy URL` (`socks5://` or `http://`) ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- Fuzz targets `resume`, `socks5`, `http_connect`, `scrape` and `web_seed_head`, with seed corpora, and `krpc` seeds for the new DHT methods ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- Interop scenarios: resume without recheck against qBittorrent-nox, a web seed served by nginx, and a download through a SOCKS5 proxy (Dante) ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))

### Changed

- `fbt-tracker`: `TrackerClient::new_with`, which also takes an optional `ProxyConfig`, replaces `with_udp_sockets` ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-tracker`: new `TrackerError` variants `ScrapeUnsupported`, `TooManyHashes`, `ProxyUnsupported`, `Proxy` and `TooSlow` ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-metainfo`: `MAX_TRACKERS` is now public, alongside the new `MAX_TORRENT_DEPTH` and `MAX_TORRENT_ITEMS` ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-metainfo`: colliding paths (`A.txt` and `a.txt`, one name in NFC and NFD, a file and a directory of the same name) are renamed `stem.N.ext` at parse time, as libtorrent does, so `MetainfoError::DuplicatePath` is no longer produced; it is deprecated and will be removed in a later breaking release ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-dht`: `Dht::announce` takes `seed`; `DhtError` is `#[non_exhaustive]` with the new variants `NoNodes`, `Timeout`, `Busy`, `Remote` and `InvalidValue`; `NodeInfo` is `#[non_exhaustive]`, built with `NodeInfo::new`; the KRPC `Query` gains `SampleInfohashes`, `Get` and `Put` and the `scrape` and `seed` flags, and `Response` gains fields ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- New `SessionError` variants `Proxy(ProxyError)`, `InvalidPath`, `Unsupported`, `Busy`, `InvalidUrl`, `TooManyWebSeeds`, `Resume(ResumeError)`, `DhtDisabled` and `Dht(DhtError)`; the typed errors keep their codes reachable (`DhtError::Remote { code }`: `204`, `301`, `302`) ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `Event`, `TorrentState` and `DisconnectReason` gained variants (`SeedLimitReached`, `ScrapeReply`, `ScrapeFailed`, `WebSeedError`, `PeerBlocked`, `StorageMoved`, `StorageMoveFailed`, `FileRenamed`, `FileRenameFailed`, `ProxyError`; `TorrentState::Queued`; `DisconnectReason::Blocked`); match with a wildcard arm ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- New `TorrentStatus` fields `is_seed`, `need_save_resume`, `resume_trusted`, `save_path`, `num_web_seeds`, `queue_position`, `auto_managed`, `active_time` and `seeding_time` ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- Torrents are auto-managed and queued by default: at most 3 downloads and 5 seeds run at once, and an auto-managed torrent beyond the limits is added `Queued`; set `AddTorrent::auto_managed` to false, or lift the limits in `Settings::queue`, for the old behaviour. `Session::torrents` lists torrents in queue order ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- A torrent accepts one connection per IP address by default (`Settings::max_peers_per_ip`, as libtorrent's `allow_multiple_connections_per_ip` off); loopback is exempt ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- Skipped files are no longer created: the bytes of their pieces that wanted files share live in the part file ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `stopped` goes to every tracker that answered a `started` in the run, in parallel, instead of only the last one; a tracker that never took a `started` gets `started` first ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `Settings::validate` (and so `Session::new`) refuses more values with `InvalidSettings`: a `proxy` without a host or with port 0, SOCKS5 credentials outside 1 to 255 bytes each (a username without a password included) or an HTTP proxy username with `:`, a zero `queue.interval`, a zero `max_peers_per_ip`, and a negative or non-finite `seed_ratio_limit` ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `TorrentHandle::move_storage` and `rename_file` fail at once with `SessionError::Busy` while the torrent checks its data or waits to, instead of holding every storage command (pause, remove, save, the session's shutdown) until the check ends; `rename_file` and resume data refuse a path onto the part file (`InvalidPath`, `ResumeError::Invalid("renamed")`), and the part file takes a `.N` suffix should one of the torrent's own files have its name ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- With the `serde` feature, the hidden test settings `per_ip_exempt_loopback` and `ban_endpoints` are no longer serialised (they load as their defaults); the proxy password is serialised in clear text ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-example` prints tracker replies and errors ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))

### Fixed

- `fbt-session`: `TorrentFinished` is no longer emitted twice after a lost piece that only skipped files overlap ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-session`: the upload throttle serves waiting connections in turn, so one connection can no longer starve the others ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-session`: smart ban parole mode, as libtorrent's: a contributor of a failed piece downloads only whole pieces of its own until one passes, so an honest co-contributor is no longer banned along with the peer that sent the bad block ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))
- `fbt-session`: pause, resume and priority changes publish the torrent's status before they reply, so a status read right after them is current ([#14](https://github.com/featherbitplatform/fbt-lib/pull/14))

## [0.4.0] - 2026-11-12

### Added

- `fbt-session`: `FileReader`, a seekable tokio `AsyncRead` and `AsyncSeek` over one file of a torrent that is still downloading, opened with the async `TorrentHandle::open_file` and `open_file_with`; a read of a missing piece waits and returns only hash-verified bytes ([#11](https://github.com/featherbitplatform/fbt-lib/pull/11))
- `fbt-session`: `ReadAhead` (bytes, time and optional bitrate) and `Settings::read_ahead`, and `StreamError` for failed reads ([#11](https://github.com/featherbitplatform/fbt-lib/pull/11))
- `fbt-session`: the deadline picker: pieces in a reader's window are fetched first, from the fastest peers, with late blocks duplicated to a second peer, and a seek replaces the window ([#11](https://github.com/featherbitplatform/fbt-lib/pull/11))
- `fbt-session`: `PiecePicker::{pick_piece, progress, abandon}` and `PieceProgress` ([#11](https://github.com/featherbitplatform/fbt-lib/pull/11))
- `fbt-session`: a sequential read cache: a piece a reader has left is demoted to the front of the clean LRU, and the next pieces are prefetched ([#11](https://github.com/featherbitplatform/fbt-lib/pull/11))
- `fbt-core`: `FileLayout::file_range` ([#11](https://github.com/featherbitplatform/fbt-lib/pull/11))
- `fbt-example`: the `stream` subcommand and `bench --stream` ([#11](https://github.com/featherbitplatform/fbt-lib/pull/11))
- Interop scenario: streaming with seeks from qBittorrent-nox ([#11](https://github.com/featherbitplatform/fbt-lib/pull/11))
- `dev/bench.sh` and `docs/benchmarks.md`: "Stream Mbit/s" and "Seek p50 ms" columns ([#11](https://github.com/featherbitplatform/fbt-lib/pull/11))

### Changed

- `fbt-session`: new `SessionError` variants `InvalidFileIndex` and `TooManyReaders` ([#11](https://github.com/featherbitplatform/fbt-lib/pull/11))
- `fbt-session`: `RateMeter::rate` no longer dilutes its first five seconds, so status rates and the request pipeline ramp up faster ([#11](https://github.com/featherbitplatform/fbt-lib/pull/11))
- `fbt-session`: repeated drops of a productive peer back off up to 30 minutes ([#11](https://github.com/featherbitplatform/fbt-lib/pull/11))
- `fbt-utp`: sends, acks and reads without per-byte copies or window scans (an O(1) send index, chunked payload buffers, no zero-fill per read); measured on Darwin arm64, uTP stays at about 579 Mbit/s and 42.7 % of a core per 100 Mbit/s because about 90 % of the time is system calls ([#11](https://github.com/featherbitplatform/fbt-lib/pull/11))
- `fbt-session`: while a reader is open on a file with missing pieces, a finished torrent stays interested in seeds and does not announce upload-only ([#11](https://github.com/featherbitplatform/fbt-lib/pull/11))

### Fixed

- `fbt-session`: skipping the files still missing while a piece of one of them was being hashed no longer leaves that piece in the write-back cache until `cache_expiry`, which held `Seeding` back as long (or reported `TorrentFinished` before it) ([#11](https://github.com/featherbitplatform/fbt-lib/pull/11))
- Interop: `INTEROP_ONLY` no longer reports "all scenarios passed" for a partial run and rejects unknown names ([#11](https://github.com/featherbitplatform/fbt-lib/pull/11))
- `fbt-utp`: datagrams that fail to decode are traced, and the half-open eviction order on a lost re-check is fixed ([#11](https://github.com/featherbitplatform/fbt-lib/pull/11))

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

[0.6.0]: https://github.com/featherbitplatform/fbt-lib/releases/tag/v0.6.0
[0.5.0]: https://github.com/featherbitplatform/fbt-lib/releases/tag/v0.5.0
[0.4.0]: https://github.com/featherbitplatform/fbt-lib/releases/tag/v0.4.0
[0.3.0]: https://github.com/featherbitplatform/fbt-lib/releases/tag/v0.3.0
[0.2.0]: https://github.com/featherbitplatform/fbt-lib/releases/tag/v0.2.0
[0.1.0]: https://github.com/featherbitplatform/fbt-lib/releases/tag/v0.1.0
