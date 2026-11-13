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
- **uTP download from qBittorrent:** qBittorrent switches to uTP only
  (`app/setPreferences` `{"bittorrent_protocol":2}`, read back).
  `fbt-example download --transport utp --no-dht` fetches the first torrent
  again; it must finish with matching hashes, its log must show
  `peer connected <qbittorrent>:6882 via utp`, and qBittorrent's `uploaded`
  must grow.
- **uTP seed to qBittorrent:** Transmission drops the second torrent, so
  `fbt-example seed --transport utp` (port 6896) is the only source;
  qBittorrent, still uTP only, adds it into a fresh save path and is pointed
  at the `fbt` container (`torrents/addPeers`). It must reach progress 1 with
  matching hashes, and the seed's log must show qBittorrent connecting
  `via utp`. qBittorrent then goes back to `bittorrent_protocol` 0.
- **Streaming with seeks:** Transmission stops its copy of the first
  torrent, so qBittorrent (back on protocol 0) is the only source.
  `fbt-example stream --file <beta.bin's index> --out /interop/fbt-stream
  --port 6899 --no-dht` runs three times over the same `--out`: `--seek
  12000000 --length 3000000` (a range from the middle; `beta.bin` starts at
  30,000,000, not on a piece boundary, so both boundary pieces straddle its
  neighbours), then `--seek 0` (the whole file: the first range is read back
  from disk, the rest comes from qBittorrent), then `--seek 24999000` (the
  last 1,000 bytes, all on disk by then). Each output's SHA-1 must equal the
  matching range of `beta.bin`, the logs of the first two must show
  `peer connected <qbittorrent>:6882`, and qBittorrent's `uploaded` must
  grow. The logs are `logs/fbt-stream-{mid,all,tail}.log`.
- **LSD:** a fifth trackerless torrent moves between two of our own
  processes: `fbt-lsd` seeds it (`--lsd --no-dht --no-pex --port 6897`) and
  `fbt` downloads it (`--lsd --no-dht --no-pex --port 6898`, no `--peer`).
  The download must finish with matching hashes, and its log must show
  `found <n> peers via lsd` and `peer connected <fbt-lsd>:6897`. A session
  announces each torrent only once per 5 minutes and nothing answers an
  announce, so the download starts first and waits until its own announce
  is out (three progress lines in the downloading state); the seed's
  announce, sent once it has verified its data, is then the only way the
  download can learn `fbt-lsd`'s address.
- **Resume without recheck:** Transmission stops its copy of the first
  torrent, so qBittorrent is the only source. `fbt-example download
  --resume /interop/fbt-resume/r.dat --stop-at 50 --no-dht` (port 6900)
  stops once half is done: it pauses the torrent, saves its resume data to
  `r.dat` (through a temporary file and a rename) and exits 0; its log
  (`fbt-resume-stop.log`) must show `stop at 50%: <p>% done` with `<p>`
  below 100. The same command without `--stop-at` then loads `r.dat`; its
  log (`fbt-resume-again.log`) must show `resume: trusted (<n> pieces)` (the
  CLI prints it from `TorrentStatus::resume_trusted`) and neither
  `resume: checking` nor a progress line in the checking state; the data
  must match and qBittorrent's `uploaded` must grow in both runs.
- **Web seed:** a sixth trackerless torrent, `transmission-create -w
  http://webseed/ -s 256` over its own payload (`payload6`, made unless
  present), whose only source is its url-list: the `webseed` service
  (nginx) serves `payload6` read-only. `fbt-example download --no-dht`
  (port 6901, no `--peer`) must finish with matching hashes and no peer
  connection, and nginx's access log (`logs/webseed-nginx.log`) must show
  `206` range responses.
- **SOCKS5 proxy:** qBittorrent is the only source again. `fbt-example
  download --proxy socks5://socks5:1080` (port 6902, DHT on) sends its
  tracker announce, peer connections and UDP (DHT, uTP) through the
  `socks5` service (Dante, no authentication). It must finish with matching
  hashes; while it runs, qBittorrent's `/api/v2/sync/torrentPeers` is
  sampled every second and must list the `socks5` container's address and
  never `fbt`'s; its log must show a `tracker reply
  http://tracker:6969/announce: ...` line (the CLI prints every
  `Event::TrackerReply`). opentracker is not consulted: its `/stats` modes
  report counts, not peer addresses.
- **BitTorrent v2:** qBittorrent creates a v2 torrent of a fresh 3-file
  payload (`payload-v2/v2data`, files chmod 0644) with its WebUI torrent
  creator: `POST torrentcreator/addTask` with `sourcePath`, `format=v2`,
  `pieceSize=262144`, `trackers` (split on `|` by the WebUI; one URL here)
  and `startSeeding=true` returns a `taskID`; `run.sh` polls
  `torrentcreator/status?taskID=` until `Finished`, saves
  `torrentcreator/torrentFile?taskID=` and calls `deleteTask`. qBittorrent
  seeds it from the payload's parent directory. Then:
  1. `fbt-example download` of qBittorrent's `.torrent` (port 6903) must
     match the payload and connect to qBittorrent.
  2. `fbt-example create --format v2 --piece-size 262144` of the same
     directory must print the `info hash v2` qBittorrent reports as
     `infohash_v2` in `torrents/info` (a hard check: file tree, ordering and
     encoding). The payload is mode 0644 and fbt creates from the same
     directory (its final name is the `name`), because libtorrent records
     executable and hidden files as `attr` `x`/`h`, which our creator never
     writes.
  3. `fbt-example download "magnet:?xt=urn:btmh:1220<infohash_v2>&tr=..."`
     (port 6904, a `btmh`-only magnet) from qBittorrent's seed: metadata,
     then the piece layers over BEP 52 hash requests.
  4. fbt seeds its own v2 torrent of a 12-file payload (`payload-v2own`,
     port 6905) and qBittorrent downloads it (`torrents/add` plus
     `addPeers`).
  5. qBittorrent drops fbt's torrent (keeping the files) and adds it again
     by `btmh` magnet into a fresh save path; fbt's seed is its only source,
     so it must fetch the twelve files' piece layers from our hash serving.
     The seed runs with `RUST_LOG=info,fbt_session::torrent::layers=debug`
     and `run.sh` logs how many hash requests it rejected over its serving
     cap (8 per torrent, `hash request over the serving caps`). Measured: 0.
     libtorrent keeps at most 2 hash requests outstanding per peer, so a
     single qBittorrent never reaches the cap.

  The magnet download (3) runs before fbt's seed starts, and that seed and
  the hybrid one run for 1200 s (`--seed-for`; `stop_seed` ends them), more
  than all the waits of the legs they serve, so a slow runner cannot outlast
  them.
- **Hybrid:** qBittorrent creates (`format=hybrid`) and seeds a hybrid of a
  fresh payload; `fbt-example download` (port 6906) fetches it.
  `fbt-example create --format hybrid` of the same directory prints both
  hashes, which must equal qBittorrent's `infohash_v1` and `infohash_v2`
  (a hard check: the v1 file list with libtorrent's pads, `.pad/<n>` with
  `attr` `p` before each unaligned file and after the last one, and the v2
  tree). fbt then seeds its own hybrid (port 6907); qBittorrent and
  Transmission both download it, and the seed's log must show each
  connecting. Transmission 4.1 takes the hybrid (as v1) and writes its pad
  files to disk under `.pad/`; the data comparison leaves `.pad/` out.
  qBittorrent names a hybrid by its truncated v2 hash (its `hash` in
  `torrents/info`), Transmission by the v1 hash, so `run.sh` looks each
  up by its own (`qbt_id`, `info_hash`). Both drop fbt's hybrid at the
  end.
- **HTTP/JSON API:** `fbt-example download --api 0.0.0.0:8090 --api-token
  <token> --download-limit 4M --seed-for 30` (port 6908) fetches the first
  torrent and serves `fbt-api` next to its session. The limit and the 30 s
  of seeding keep the process, and so the API, up while `curl` (in the `fbt`
  container, on `127.0.0.1:8090`) drives it; a plain download would exit
  once done and take the API with it. As soon as the log shows `api:
  listening on`, `curl -sN --max-time 120` follows `GET /api/v1/events` in
  the background (`logs/fbt-api-events.log`). Then, while the download
  runs: `GET /api/v1/torrents` must list the torrent alone, by its 40-hex
  wire hash; `GET /api/v1/torrents/<id>/files` gives `beta.bin`'s index;
  three `Range` requests on `/api/v1/torrents/<id>/files/<index>/stream`
  (the first, a middle and the last 64 KiB) must answer `206` and equal
  those bytes of `beta.bin` (`cmp`); a request without the token must get
  `401`, and `Range: bytes=25000000-` (past the end) `416`. The download
  must exit 0 with matching data, and the event stream, which the API's
  shutdown ends, must hold an `event: torrent_finished` line.

Every `fbt-example` run passes `--dht-bootstrap dht-node:6881`. On success it
prints `interop: download OK (qbittorrent, transmission)`,
`interop: seed OK (transmission)`,
`interop: encryption OK (qbittorrent, transmission)`,
`interop: magnet via DHT OK`, `interop: PEX OK`,
`interop: uTP download OK (qbittorrent)`, `interop: uTP seed OK (qbittorrent)`,
`interop: stream with seeks OK (qbittorrent)`, `interop: LSD OK`,
`interop: resume without recheck OK (qbittorrent)`,
`interop: web seed OK (nginx)`, `interop: SOCKS5 proxy OK (qbittorrent)`,
`interop: v2 OK (qbittorrent)`, `interop: hybrid OK (qbittorrent, transmission)` and
`interop: api OK (list, ranges, 401, 416, events)`,
and exits 0. It also prints how long the DHT took to
join and the total time. CI runs it as the `interop` job (65-minute timeout).

## Layout

Everything runs under Docker Compose on a bridge network (`swarm`) that every
service joins (the two fbt containers that run LSD also join `lan`, below);
services reach each other by name, so it works the same on Linux and on Docker Desktop
for macOS or Windows (no host networking). Only Docker with Compose v2 is
needed on the host.

**No public DHT node is ever contacted.** `swarm` is `internal: true`: no
container has a route out of it (`run.sh` checks
`docker network inspect fbt-interop_swarm --format '{{.Internal}}'` prints
`true`, and the same for `fbt-interop_lan`), and every image is pulled or
built before `up`. **There is no router on either network**, so no UPnP or
NAT-PMP answer is possible, and both stay off everywhere anyway: our CLI
leaves them off unless `--nat` is given (no run passes it), and qBittorrent
keeps `PortForwardingEnabled=false`. Transmission still
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
| `fbt` | `fbt-example` built from this checkout by `Dockerfile`, plus `curl`/`jq`; on `swarm` and `lan` | 6890 (download), 6891 (seed), 6892 (magnet), 6893 (encrypted download), 6894 (PEX), 6895 (encrypted seed, then uTP download), 6896 (uTP seed), 6898 (LSD download), 6899 (LSD probe, then the stream scenario), 6900 (resume), 6901 (web seed), 6902 (SOCKS5 proxy), 6903 (v2 download), 6904 (btmh magnet), 6905 (v2 seed), 6906 (hybrid download), 6907 (hybrid seed), 6908 (API download; its API on 8090) |
| `fbt-lsd` | the same image, idle like `fbt`; on `swarm` and `lan` | 6897 (LSD seed), 6899 (LSD probe) |
| `fbt-mcast-route`, `fbt-lsd-mcast-route` | one-shot, in the network namespace of `fbt` / `fbt-lsd`: routes 224.0.0.0/4 out of the `lan` interface | none |
| `socks5` | Dante (`danted`) built by `socks5/Dockerfile`, `socks5/danted.conf`: no authentication, CONNECT and UDP ASSOCIATE | SOCKS5 1080 |
| `webseed` | nginx, `webseed/nginx.conf`: serves `target/interop/payload6` read-only, logs each request's status and `Range` | HTTP 80 |

### Multicast and LSD

LSD (BEP 14) multicasts to 239.192.152.143:6771, and the `swarm` bridge does
not carry that: Docker filters an internal bridge network with iptables and
drops every packet whose destination lies outside the network's subnet,
multicast included (measured on Docker 29.8: two `lsd-probe` runs on an
internal bridge never hear each other, on a non-internal one they do at
once). So `fbt` and `fbt-lsd` are also on a second network, `lan`: an
internal macvlan network (`10.143.152.0/24`) on a dummy parent interface,
with no uplink, no host address and no iptables in the path, which passes
multicast between its members. Nothing else is on it.

An internal network gives its containers no default route either, and
joining a multicast group on the default interface then fails with
`No such device`. The one-shot services `fbt-mcast-route` and
`fbt-lsd-mcast-route` (the tracker's Alpine image, root with `NET_ADMIN`,
sharing the target's network namespace) add `224.0.0.0/4 dev <lan interface>`
in `fbt` and `fbt-lsd`; those two stay unprivileged. Multicast leaves with
TTL 1 and only ever reaches `lan`.

Before any scenario, `run.sh` checks both networks are internal, waits until
the multicast route shows in `/proc/net/route` of both containers, and runs
`fbt-example lsd-probe --port 6899 --timeout-secs 3` in `fbt` and `fbt-lsd`
at the same time (the hidden subcommand announces a fixed probe hash every
500 ms and exits 0 with `lsd probe: found <addr>` once it hears the other,
announcing once more first: the probe that started first hears the other's
very first announce, and without that answer the later one, which joined
after the earlier one's last announce, timed out in about one run in two).
`docker compose exec` can take seconds while containers start, so both
first wait for the same second on their shared clock, which keeps the two
3 s windows overlapping. Both must exit 0, or
the run fails with "multicast does not reach ..." instead of a misleading
LSD failure later. By hand, after `up`:

```
docker network inspect fbt-interop_swarm fbt-interop_lan --format '{{.Name}} {{.Driver}} {{.Internal}}'
docker compose -f dev/interop/compose.yaml exec -T fbt fbt-example lsd-probe --port 6899 --timeout-secs 3 &
docker compose -f dev/interop/compose.yaml exec -T fbt-lsd fbt-example lsd-probe --port 6899 --timeout-secs 3
```

### qBittorrent's transport switch

The uTP scenarios use the WebUI preference `bittorrent_protocol` (0 = TCP
and uTP, 1 = TCP, 2 = uTP), the WebUI's Advanced setting "Peer connection
protocol". Checked against the pinned 5.3.0 image: `app/preferences`
reports `"bittorrent_protocol":0` by default and reads back 2 and 0 after
`app/setPreferences`, and qBittorrent writes it to `qBittorrent.conf` as
`Session\BTProtocol` in `[BitTorrent]`, by name: `Both`, `TCP` or `UTP`. The
committed conf leaves it at the default (`Both`); `run.sh` switches it at
run time and restores 0.

### DHT and PEX

DHT and PEX are on in both clients; LSD and UPnP/NAT-PMP stay off in both
(LSD runs only between our own two processes, on `lan`). Our own
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
  least 8 (one full bucket, K), and prints the time from `up`: about 14 s
  locally, most of it the nodes' 10 s stats interval (they print `dht nodes:`
  every 10 s). It used to require eleven, one per helper, which a Kademlia
  table cannot promise: only the bucket holding the node's own id splits, so
  the far half of the id space keeps at most 8 nodes, and with random ids
  more than 8 helpers land there in a few percent of runs (a local run sat
  at `v4=10` for the whole 120 s wait).
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

To chase one scenario, `INTEROP_ONLY` takes a comma-separated list of
`download`, `seed`, `encryption`, `magnet`, `pex`, `utp`, `lsd`, `stream`,
`resume`, `webseed`, `proxy`, `v2`, `hybrid` and `api`;
an unknown name fails at once with `interop: unknown scenario '<name>' (known:
...)`, before the setup. The shared setup and the uTP scenarios always run,
the others only when named. A partial run ends with `interop: selected
scenarios passed (<list>) ...`; only a full run prints `interop: all
scenarios passed ...`.
`FBT_RUST_LOG` sets `RUST_LOG` for the uTP download alone (its tracing goes
to `fbt-download-utp.log`), for example
`INTEROP_ONLY=utp FBT_RUST_LOG=info,fbt_utp=debug,fbt_session=debug`. The
script prints the setup time (image builds and pulls, container start, DHT
join) apart from the scenario time, and the uTP download's own duration.

## Images

All pinned by the digest of the multi-arch index (manifest list), so the same
reference resolves to the native image on amd64 CI runners and arm64 Macs.

| Image | Tag | Index digest | Why |
|---|---|---|---|
| `qbittorrentofficial/qbittorrent-nox` | `5.3.0-2` | `sha256:43b2d352cf76330a7ca34c7320b9b74227d6f31e662e0e123d0457206b09a7ea` | The qBittorrent project's own image; current release on libtorrent 2.x; amd64 and arm64. |
| `lscr.io/linuxserver/transmission` | `4.1.4-r0-ls368` | `sha256:fb8fe625941cf15eb69aba4e5c0a678e035ddb11605fd85af4516a4e641f2687` | Transmission upstream ships no image; LinuxServer.io's is the maintained, widely used one, tracks the current 4.1 release, honours `PUID`/`PGID` and includes `transmission-create`/`transmission-remote`; amd64 and arm64. |
| `alpine` (opentracker) | `3.22` | `sha256:49340b346798875b5592e5d420241f2f9cfba5f47e62ebb1446a1b34734e898f` | No maintained multi-arch opentracker image exists (the common ones are amd64-only and years old), so `tracker/Dockerfile` installs Alpine's packaged `opentracker`. |
| `rust` (builder) | `1.99-bookworm` | `sha256:e6f2cff35d0f309acf8433b4f05878ed77092602c5065cc81801177da0bb3a9c` | The workspace `rust-version`; has the C toolchain `ring` needs. |
| `debian` (runtime) | `bookworm-slim` | `sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc` | Same glibc as the builder; small. Also the base of `socks5/Dockerfile`, which installs Debian's `dante-server` (Dante 1.4). |
| `nginx` (web seed) | `1.30.6-alpine` | `sha256:583b7f2fce1cdb8da5ad5c39ebd3c33b98b87d493190e548bdee1fa8c619c311` | The official image; serves byte ranges (`206`) from static files out of the box; amd64 and arm64. |

To bump an image, take the new index digest from
`docker buildx imagetools inspect <image>:<tag>` and update `compose.yaml` or the
Dockerfile and this table.
