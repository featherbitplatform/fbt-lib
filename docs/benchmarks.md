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

## Status

First row (0.2.0), against each target:

- TCP throughput: met, about 4.2 Gbit/s against 1 Gbit/s.
- TCP CPU per 100 Mbit/s: 5.0 % of a core, seed and leech together;
  borderline against "a few percent".
- uTP throughput: missed, about 0.6 Gbit/s against 1 Gbit/s.
- uTP CPU per 100 Mbit/s: missed, 39 % of a core.
- 1,000 idle torrents under 1 % CPU: missed, 1.2 %.
- Memory per idle torrent: recorded, about 18 KiB RSS.

### Known defects: uTP throughput and CPU

The uTP misses are known implementation costs in `fbt-utp`'s hot path, not
open tuning targets:

- **In-flight window scans per send.** Every packet sent, and every ack
  processed, scans the whole in-flight window for packets to retransmit
  (`transmit_outstanding`, `packetize`, `has_pending_send`). With a 1 MiB
  send buffer that is about 750 packets, so CPU per packet grows with the
  window and the faster the link, the worse it gets.
- **Byte-wise `VecDeque` payload.** Send and receive payload sits in
  `VecDeque<u8>` buffers and moves through byte iterators
  (`extend(iter().take(n))`, `drain(..n)`), about three non-`memcpy` copies
  per payload byte on each side.
- **Zero-fill per read.** `UtpStream::poll_read` zero-fills the reader's
  whole spare capacity (`initialize_unfilled`) on every call, often 8 to
  16 KiB to deliver 1.4 to 3 KiB.

They are scheduled for milestone 4: an O(1) path to the next unsent and
lost packets, chunked or slice-copied payload buffers, and a read that
initialises only what it fills. What remains after that is inherent to the
design for now (one syscall per datagram, and one receive task per socket
running all of its uTP processing) and is tuning work.

The transfer timer polls the torrent state every 20 ms, so a very fast run
(TCP finishes in about 2 s) is accurate to roughly 1 %.

| Date | Version | Host | TCP Mbit/s | TCP CPU % per 100 Mbit/s | uTP Mbit/s | uTP CPU % per 100 Mbit/s | Idle torrents | Idle CPU % | RSS per idle torrent | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| 2026-11-11 | 0.2.0 | Darwin arm64 | 4249 | 5.0 | 607 | 39.3 | 1000 | 1.18 | 18.3 KiB | seed and leech in one process |
| 2026-11-11 | 0.3.0 | Darwin arm64 | 5238 | 4.9 | 603 | 39.9 | 1000 | 1.13 | 18.5 KiB | seed and leech in one process |
