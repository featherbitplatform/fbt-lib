# Live interop harness

`bash dev/interop/run.sh` checks `fbt-example` against two real clients:

- **Download test:** qBittorrent-nox (libtorrent) and Transmission both seed a
  64 MiB, 3-file torrent; `fbt-example download` must finish within 5 minutes,
  the data must hash identically, and both seeds must report uploaded data.
- **Seed test:** `fbt-example seed` serves a second 64 MiB torrent; Transmission
  must download it to 100 % with matching hashes.

On success it prints `interop: download OK (qbittorrent, transmission)` and
`interop: seed OK (transmission)` and exits 0. CI runs it as the `interop` job.

## Layout

Everything runs under Docker Compose on one bridge network (`swarm`); services
reach each other by name, so it works the same on Linux and on Docker Desktop
for macOS or Windows (no host networking). Only Docker with Compose v2 is
needed on the host.

| Service | What | Ports (inside the network) |
|---|---|---|
| `tracker` | opentracker | 6969 HTTP and UDP |
| `qbittorrent` | qBittorrent-nox, `qbittorrent/qBittorrent.conf` | peers 6882, WebUI 8080 |
| `transmission` | Transmission daemon, `transmission/settings.json` | peers 51413, RPC 9091 |
| `fbt` | `fbt-example` built from this checkout by `Dockerfile`, plus `curl`/`jq` | 6890 (download), 6891 (seed) |

DHT, PeX, LSD and UPnP/NAT-PMP are off in both clients so peers come only from
the tracker. Both seeds cap their upload at 4096 KiB/s (qBittorrent with
`IgnoreLimitsOnLAN=false`, since the bridge is a LAN): unthrottled, the whole
payload crosses the bridge in a fraction of a second from whichever seed
connects first, and the test could not show that both supplied data. qBittorrent's WebUI accepts unauthenticated requests from any
address (subnet whitelist `0.0.0.0/0`, host-header validation and CSRF checks
off); Transmission's RPC has authentication and whitelists off. Neither port
is published to the host. Torrents are made with `transmission-create -s 256`
(256 KiB pieces) and announce to `http://tracker:6969/announce`.

`run.sh` keeps all state under `target/interop/` (payloads, torrents, client
configs, downloaded data, `logs/`). Containers run as the calling user's
uid/gid, so the directory stays removable. On failure it writes
`logs/containers.log` and prints the tail of every log. Set `RUST_LOG` (for
example `RUST_LOG=debug`) to raise `fbt-example`'s log level.

## Images

All pinned by the digest of the multi-arch index (manifest list), so the same
reference resolves to the native image on amd64 CI runners and arm64 Macs.

| Image | Tag | Index digest | Why |
|---|---|---|---|
| `qbittorrentofficial/qbittorrent-nox` | `5.3.0-2` | `sha256:43b2d352cf76330a7ca34c7320b9b74227d6f31e662e0e123d0457206b09a7ea` | The qBittorrent project's own image; current release on libtorrent 2.x; amd64 and arm64. |
| `lscr.io/linuxserver/transmission` | `4.1.4-r0-ls368` | `sha256:fb8fe625941cf15eb69aba4e5c0a678e035ddb11605fd85af4516a4e641f2687` | Transmission upstream ships no image; LinuxServer.io's is the maintained, widely used one, tracks the current 4.1 release, honours `PUID`/`PGID` and includes `transmission-create`/`transmission-remote`; amd64 and arm64. |
| `alpine` (opentracker) | `3.22` | `sha256:49340b346798875b5592e5d420241f2f9cfba5f47e62ebb1446a1b34734e898f` | No maintained multi-arch opentracker image exists (the common ones are amd64-only and years old), so `tracker/Dockerfile` installs Alpine's packaged `opentracker`. |
| `rust` (builder) | `1.99-bookworm` | `sha256:e6f2cff35d0f309acf8433b4f05878ed77092602c5065cc81801177da0bb3a9c` | The workspace `rust-version`; has the C toolchain `ring` needs. |
| `debian` (runtime) | `bookworm-slim` | `sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc` | Same glibc as the builder; small. |

To bump an image, take the new index digest from
`docker buildx imagetools inspect <image>:<tag>` and update `compose.yaml` or the
Dockerfile and this table.
