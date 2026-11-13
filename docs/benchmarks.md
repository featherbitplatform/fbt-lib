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
that transport on both sides. CPU time comes from `ps` on macOS (centisecond
resolution) and from `/proc/<pid>/stat` on Linux (clock-tick resolution).

Hybrid torrents hash every piece twice, SHA-1 for v1 and the SHA-256 Merkle
root for v2, so checking, downloading and creating one costs more CPU than the
same data as v1 or v2 alone. The recorded rows use v1 torrents and do not
include this cost.

## Status

First row (0.2.0), against each target:

- TCP throughput: met, about 4.2 Gbit/s against 1 Gbit/s.
- TCP CPU per 100 Mbit/s: 5.0 % of a core, seed and leech together;
  borderline against "a few percent".
- uTP throughput: missed, about 0.6 Gbit/s against 1 Gbit/s.
- uTP CPU per 100 Mbit/s: missed, 39 % of a core.
- 1,000 idle torrents under 1 % CPU: missed, 1.2 %.
- Memory per idle torrent: recorded, about 18 KiB RSS.

### Fixed in 0.4.0: uTP hot-path defects (targets still missed)

The three implementation costs recorded against 0.3.0 are gone from
`fbt-utp`'s hot path:

- **No in-flight window scan to find the next packet to send.** Every
  outstanding packet has an ordinal; the lost and not-yet-sent ones are kept
  in an ordered set and a count of live packets is kept, so picking the next
  packet, `has_pending_send` and re-arming the retransmission timer are
  O(log n) or O(1). Only loss handling still walks the window (the SACK loss
  loop, bounded by `MAX_IN_FLIGHT_PACKETS` and stopping early, and the RTO).
- **No per-byte copies.** Send data sits in a `BytesMut` and is packetized
  with `split_to(..).freeze()`; full-size received payloads (1024 bytes or
  more) are normally kept as slices of their datagram and read with
  `copy_from_slice`. Shorter payloads are copied and merged so a tiny payload
  never pins a whole datagram; a full-size payload that arrives while such
  merged bytes are still short of 1024 is appended to them (copied), so the
  buffer's chunk count stays bounded. For
  full-size traffic the only copy before `read` (which copies into the
  reader's buffer) is the datagram copy in the socket's receive handler.
- **No zero-fill per read.** `UtpStream::poll_read` initialises only the
  bytes it is about to fill.

Measured on the 0.3.0 host (Darwin arm64, `BENCH_NO_RECORD=1`, same day, same
build settings), uTP moved from 582 Mbit/s at 41.9 % of a core per
100 Mbit/s to 579 Mbit/s at 42.7 %: no change, within run-to-run noise. TCP
measured 5107 and 5108 Mbit/s at 4.9 and 5.0 %. The 0.4.0 targets (uTP at
least 1000 Mbit/s and at most 15 % of a core per 100 Mbit/s) are **missed**.

Profile (`sample`, 10 s of a 2 GiB uTP run, on-CPU samples only): the
system calls dominate both before and after the fix. `sendto` is 47 %,
`kevent` 25 % and `recvfrom` 19 % of on-CPU samples; MSE's RC4 is 4 %. The
uTP connection code itself (`transmit_outstanding`, `poll_transmit` and the
`VecDeque<u8>` drains) was 2.4 % of on-CPU samples before the fix and is
0.1 % after. The removed costs were real but small on this host; the
remaining cost is inherent to the current design: one `sendto` per
1.4 KiB datagram, one `recvfrom` per datagram, and a reactor wakeup
(`kevent`) for most of them, with all of a socket's uTP processing on its
single receive task. Closing the gap means batching datagrams (`sendmmsg`,
`recvmmsg`, GSO/GRO), which needs platform FFI that the no-unsafe rule
currently rules out. That work is deferred and is not a defect in the
connection code.

The transfer timer polls the torrent state every 20 ms, so a very fast run
(TCP finishes in about 2 s) is accurate to roughly 1 %.

The stream column is one reader front to back over a 256 MiB file while it
downloads over loopback TCP (`fbt-example bench --stream`). The seek column is
the median of ten 64 KiB reads at fixed pseudo-random offsets, taken after the
download completes, so it measures the cached and local read path, not a fetch
from peers.

| Date | Version | Host | TCP Mbit/s | TCP CPU % per 100 Mbit/s | uTP Mbit/s | uTP CPU % per 100 Mbit/s | Stream Mbit/s | Seek p50 ms | Idle torrents | Idle CPU % | RSS per idle torrent | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-11-11 | 0.2.0 | Darwin arm64 | 4249 | 5.0 | 607 | 39.3 | - | - | 1000 | 1.18 | 18.3 KiB | seed and leech in one process |
| 2026-11-11 | 0.3.0 | Darwin arm64 | 5238 | 4.9 | 603 | 39.9 | - | - | 1000 | 1.13 | 18.5 KiB | seed and leech in one process |
| 2026-11-12 | 0.4.0 | Darwin arm64 | 4247 | 4.9 | 615 | 40.4 | 4139 | 0.05 | 1000 | 1.15 | 19.4 KiB | seed and leech in one process |
| 2026-11-13 | 0.5.0 | Darwin arm64 | 4920 | 5.0 | 630 | 39.2 | 4032 | 0.05 | 1000 | 1.55 | 23.9 KiB | seed and leech in one process |
| 2026-11-13 | 0.6.0 | Darwin arm64 | 5105 | 5.0 | 621 | 40.4 | 4118 | 0.05 | 1000 | 0.82 | 25.6 KiB | seed and leech in one process |
