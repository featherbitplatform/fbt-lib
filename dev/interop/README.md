# Live interop harness

`bash dev/interop/run.sh` checks `fbt-example` against two real clients:

- **Download test:** qBittorrent-nox (libtorrent) and Transmission both seed a
  64 MiB, 3-file torrent; `fbt-example download` must finish within 5 minutes,
  the data must hash identically, and both seeds must report uploaded data.
  This run uses `--encryption disabled`, so plaintext keeps its coverage.
- **Seed test:** `fbt-example seed` serves a second 64 MiB torrent; Transmission
  must download it to 100 % with matching hashes.
- **Forced encryption:** both clients switch to encryption required
  (qBittorrent `app/setPreferences` `{"encryption":1}`, Transmission
  `--encryption-required`; both settings are read back).
  `fbt-example download --encryption forced` fetches the first torrent again
  (both seeds' upload counters must move). Then, with the first seed's process
  confirmed gone, `fbt-example seed --encryption forced` serves the second
  torrent to Transmission in a fresh directory, and its log must show
  Transmission connecting. Both clients go back to preferred.
- **Magnet through DHT:** a third 64 MiB torrent with no tracker, seeded by
  both clients. `run.sh` waits (up to 240 s) until Transmission has announced
  it: every node of the private DHT prints its stored info-hash count
  (`[<service>] dht nodes: ... torrents=N`), and with nothing else
  announcing, a qBittorrent announce alone grows 3 of those stores and the
  two announces together 7 (measured), so 5 or more grown stores is the
  signal. Then
  `fbt-example download "magnet:?xt=urn:btih:<hash>&dn=fixture3" --no-pex`
  must find peers via the DHT, fetch the metadata (BEP 9) and finish with
  matching hashes. With no tracker, no `x.pe` and no PEX, its outgoing
  connections to qBittorrent's port 6882 and Transmission's port 51413 (both
  required) can only come from addresses the DHT returned. Once a magnet has
  its metadata, our session looks the DHT up again only every
  `dht_announce_interval` (15 min; every 60 s while the metadata is missing),
  so if Transmission is missing the download is run again, at most 3
  attempts in all, and the script prints which attempt passed.
- **PEX:** a fourth trackerless torrent seeded by Transmission only.
  qBittorrent is pointed at Transmission (`torrents/addPeers`);
  `fbt-example download --no-dht --peer <qbittorrent>:6882` must learn
  Transmission's address via PEX (BEP 11) from qBittorrent and connect to its
  listen port, `<transmission>:51413`.

Every `fbt-example` run passes `--dht-bootstrap dht-node:6881`. On success it
prints `interop: download OK (qbittorrent, transmission)`,
`interop: seed OK (transmission)`,
`interop: encryption OK (qbittorrent, transmission)`,
`interop: magnet via DHT OK` and `interop: PEX OK`, and exits 0. It also prints
how long the DHT took to join and the total time. CI runs it as the `interop`
job (40-minute timeout).

## Layout

Everything runs under Docker Compose on one bridge network (`swarm`); services
reach each other by name, so it works the same on Linux and on Docker Desktop
for macOS or Windows (no host networking). Only Docker with Compose v2 is
needed on the host.

**No public DHT node is ever contacted.** `swarm` is `internal: true`: no
container has a route out of it (`run.sh` checks
`docker network inspect fbt-interop_swarm --format '{{.Internal}}'` prints
`true`), and every image is pulled or built before `up`. Transmission still
tries to resolve its built-in `dht.transmissionbt.com` and logs
`Couldn't look up 'dht.transmissionbt.com:6881'`; that is the expected
outcome.

| Service | What | Ports (inside the network) |
|---|---|---|
| `tracker` | opentracker | 6969 HTTP and UDP |
| `qbittorrent` | qBittorrent-nox, `qbittorrent/qBittorrent.conf` | peers 6882, WebUI 8080 |
| `transmission` | Transmission daemon, `transmission/settings.json` | peers 51413, RPC 9091 |
| `dht-node` | `fbt-example dht-node` (same image as `fbt`): the private DHT's bootstrap node | DHT 6881 UDP |
| `dht-helper-1` to `dht-helper-11` | the same, bootstrapped from `dht-node:6881`; one container (and address) each | DHT 6881 UDP |
| `fbt` | `fbt-example` built from this checkout by `Dockerfile`, plus `curl`/`jq` | 6890 (download), 6891 (seed), 6892 (magnet), 6893 (encrypted download), 6894 (PEX), 6895 (encrypted seed) |

### DHT and PEX

DHT and PEX are on in both clients; LSD and UPnP/NAT-PMP stay off. Our own
`fbt-example dht-node` is the only DHT bootstrap for everyone:

- **qBittorrent:** `Session\DHTBootstrapNodes=dht-node:6881` in the
  `[BitTorrent]` section of `qBittorrent.conf` (checked against the pinned
  5.3.0 image: the key is in the binary, and `app/preferences` reports
  `"dht_bootstrap_nodes":"dht-node:6881"`; it replaces libtorrent's default
  `dht.libtorrent.org:25401` and the other routers).
- **Transmission:** settings.json has no bootstrap setting; libtransmission
  reads `dht.bootstrap` (one `host port` per line) from its config directory,
  so `run.sh` copies `transmission/dht.bootstrap` next to `settings.json`. It
  lists `dht-node 6881` and the helper nodes below (`dht-helper-1 6881` to
  `dht-helper-11 6881`).
- **Transmission's announce threshold.** libtransmission announces torrents
  to the DHT only once its IPv4 routing table is at least "poor": 4 good nodes
  and more than 8 nodes in all (`tr-dht.cc`, `swarm_status`). With only
  `dht-node` and qBittorrent in the network it never announces, so eleven
  helper services (`dht-helper-1` to `dht-helper-11`, the same image and
  command, bootstrapped from `dht-node:6881`) join it. Transmission pings
  every `dht.bootstrap` entry (one every 2 s), so it reaches that threshold
  within about half a minute of starting. With only `dht-node 6881` listed it
  had to learn the helpers through `find_node` replies and, in a local run,
  had still not announced the magnet torrent two minutes after starting.
- **One address per DHT node.** Every DHT node is its own compose service
  (a YAML anchor, `x-dht-node`, holds the shared definition), so each has
  its own address on `swarm`, as real DHT nodes do. They used to run as
  twelve processes in one container: our DHT verifies a new node with a ping
  before inserting it and rate-limits inbound queries per IP (5/s, burst
  10), so nodes sharing one address throttled each other's join, which took
  about 40 s locally and more than 120 s on a GitHub runner. Now `up` starts
  `dht-node` first and waits until it is healthy (its `fbt-example` runs),
  then the helpers; Transmission waits for all twelve. `run.sh` waits until
  all twelve are listening, every helper knows a node and `dht-node` knows at
  least eleven, and prints the time from `up`: about 14 s locally, most of it
  the nodes' 10 s stats interval (they print `dht nodes:` every 10 s).
- **libtorrent's /24 rule.** libtorrent keeps at most one routing-table node
  per /24 by default, and every container shares one bridge subnet, so
  qBittorrent's table holds a single node, and its announces reach only 3 of
  the twelve DHT nodes. The scenarios only need each client to reach
  `dht-node`. Transmission announces to the 8 closest nodes it knows
  (qBittorrent's among them), and a `get_peers` lookup from `dht-node` finds
  both clients.

The first download and seed tests run `fbt-example` with `--no-dht`, so their
peers come only from the tracker (and PEX). Both seeds cap their upload at
4096 KiB/s (qBittorrent with
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
