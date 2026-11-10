# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.0]: https://github.com/featherbitplatform/fbt-lib/releases/tag/v0.1.0
