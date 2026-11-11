# Benchmarks

Numbers are recorded per release by `bash dev/bench.sh` (release build, one row
appended per run). They are not gated in CI.

## Targets (design spec, section 7)

- A single torrent saturates 1 Gbit/s on loopback, with a few percent of one
  core per 100 Mbit/s.
- 1,000 idle seeding torrents stay under 1 % CPU.
- Memory per idle torrent is bounded and documented: the measured RSS per idle
  torrent is recorded below.

## Method

`fbt-example bench` runs a seed session and a leech session in one process on
loopback. DHT, LSD, UPnP, NAT-PMP, PEX and holepunching are off and no
transfer leaves the machine. The payload is generated on demand and the
leech discards what it writes (every piece is verified against the torrent's
hashes first), so a 1 GiB run uses no disk and no payload memory. Rate limits
do not apply to local peers.

The script samples the process with `ps -o time=,rss=` at the start and end of
each run. CPU is the whole process, so seed and leech together. The idle run
seeds 1,000 one-piece torrents with no peers; RSS per idle torrent is the
difference against a 10-torrent run, divided by 990. TCP and uTP rows use only
that transport on both sides.

## Status

First row (0.2.0): TCP saturates 1 Gbit/s on loopback (about 4.2 Gbit/s) at
about 5 % of a core per 100 Mbit/s, with seed and leech both counted. Three
targets are missed and recorded as such: uTP reaches about 0.6 Gbit/s, not
1 Gbit/s, at about 39 % of a core per 100 Mbit/s; and 1,000 idle seeding
torrents use about 1.2 % CPU, over the 1 % target. RSS per idle torrent is
about 18 KiB.

| Date | Version | Host | TCP Mbit/s | TCP CPU % per 100 Mbit/s | uTP Mbit/s | uTP CPU % per 100 Mbit/s | Idle torrents | Idle CPU % | RSS per idle torrent | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| 2026-11-11 | 0.2.0 | Darwin arm64 | 4249 | 5.0 | 607 | 39.3 | 1000 | 1.18 | 18.3 KiB | seed and leech in one process |
