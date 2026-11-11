#!/usr/bin/env bash
# Loopback benchmark: builds the release binary, runs the hidden `bench`
# subcommand (TCP and uTP throughput, then idle seeding torrents), samples the
# process with `ps` (no FFI, works on macOS and Linux) and appends one dated
# row to docs/benchmarks.md. Numbers are recorded per release, not gated.
#
# Everything stays on loopback: no DHT, LSD, UPnP, NAT-PMP or PEX, and the
# payload is generated in memory-free storage (nothing is written to disk).
#
# Overrides (environment), for hosts too slow or small for the defaults:
#   BENCH_SIZE_MIB     payload per transport run (default 1024)
#   BENCH_TORRENTS     idle seeding torrents (default 1000)
#   BENCH_IDLE_SECS    how long they idle (default 60)
#   BENCH_BASELINE     torrents in the RSS baseline run (default 10)
#   BENCH_NO_RECORD=1  print the row instead of appending it
#
# The row holds only the date, the workspace version, `uname -sm` and the
# measured numbers; nothing else from the environment is written.
set -euo pipefail

cd "$(dirname "$0")/.."

SIZE=${BENCH_SIZE_MIB:-1024}
TORRENTS=${BENCH_TORRENTS:-1000}
IDLE_SECS=${BENCH_IDLE_SECS:-60}
BASELINE=${BENCH_BASELINE:-10}
DOC=docs/benchmarks.md
BIN="${CARGO_TARGET_DIR:-target}/release/fbt-example"

if [ -f "$HOME/.cargo/env" ] && ! command -v cargo >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
fi

for v in "$SIZE" "$TORRENTS" "$IDLE_SECS" "$BASELINE"; do
    case $v in '' | *[!0-9]*) echo "bench: overrides must be whole numbers" >&2; exit 2 ;; esac
done

echo "bench: building the release binary" >&2
cargo build --release -p fbt-example >&2

WORK=$(mktemp -d)
BENCH_PID=
cleanup() {
    [ -n "$BENCH_PID" ] && kill "$BENCH_PID" 2>/dev/null || true
    rm -rf "$WORK"
}
trap cleanup EXIT

# cpu_secs "[[dd-]hh:]mm:ss[.cc]" -> seconds
cpu_secs() {
    awk -v t="$1" 'BEGIN {
        days = 0
        if (index(t, "-")) { split(t, a, "-"); days = a[1]; t = a[2] }
        n = split(t, p, ":")
        s = 0
        for (i = 1; i <= n; i++) s = s * 60 + p[i]
        printf "%.2f", days * 86400 + s
    }'
}

# sample PID -> "<cpu seconds> <rss KiB>". CPU time comes from /proc/PID/stat
# (utime + stime, in clock ticks) where it exists, for better than the 1 s
# resolution of Linux `ps`; macOS `ps` already reports centiseconds.
sample() {
    local out t r stat rest
    out=$(ps -o time=,rss= -p "$1") || return 1
    read -r t r <<<"$out"
    if [ -r "/proc/$1/stat" ] && stat=$(cat "/proc/$1/stat" 2>/dev/null); then
        # Fields after the ")" that ends the command name start at field 3,
        # so utime and stime (fields 14 and 15) are words 12 and 13.
        rest=${stat##*) }
        # shellcheck disable=SC2086
        set -- $1 $rest
        shift
        echo "$(awk -v u="${12}" -v s="${13}" -v h="$(getconf CLK_TCK)" \
            'BEGIN { printf "%.2f", (u + s) / h }') $r"
        return 0
    fi
    echo "$(cpu_secs "$t") $r"
}

# take_sample PID CPU_VAR RSS_VAR: sample, or stop with a clear error.
take_sample() {
    local out
    if ! out=$(sample "$1") || [ -z "$out" ]; then
        echo "bench: could not sample process $1 (ps or /proc failed)" >&2
        exit 1
    fi
    read -r "$2" "$3" <<<"$out"
}

# wait_line FILE PATTERN: block until a line matching PATTERN appears.
wait_line() {
    local waited=0
    until grep -q "$2" "$1" 2>/dev/null; do
        if ! kill -0 "$BENCH_PID" 2>/dev/null; then
            echo "bench: the benchmark process ended early:" >&2
            cat "$1" "${1%.out}.err" >&2
            exit 1
        fi
        sleep 0.05
        waited=$((waited + 1))
        if [ "$waited" -gt 72000 ]; then
            echo "bench: timed out waiting for '$2'" >&2
            exit 1
        fi
    done
}

# run_bench NAME START_PATTERN END_PATTERN ARGS...
# Sets CPU0 RSS0 CPU1 RSS1 and OUT (the output file) for the finished run.
run_bench() {
    local name=$1 start=$2 end=$3
    shift 3
    OUT="$WORK/$name.out"
    : >"$OUT"
    "$BIN" bench "$@" >"$OUT" 2>"$WORK/$name.err" &
    BENCH_PID=$!
    wait_line "$OUT" "$start"
    take_sample "$BENCH_PID" CPU0 RSS0
    wait_line "$OUT" "$end"
    take_sample "$BENCH_PID" CPU1 RSS1
    wait "$BENCH_PID"
    BENCH_PID=
}

# transfer TRANSPORT: sets MBIT and CPUPCT100 (CPU % of a core per 100 Mbit/s,
# seed and leech together, since both run in the one process).
transfer() {
    echo "bench: $1, $SIZE MiB" >&2
    run_bench "$1" '^bench: start' '^bench: [a-z]* [0-9]* MiB in' \
        --size "$SIZE" --transport "$1"
    local line secs
    line=$(grep '^bench: [a-z]* [0-9]* MiB in' "$OUT")
    secs=$(awk '{print $6}' <<<"$line")
    MBIT=$(awk '{print $9}' <<<"$line")
    CPUSECS=$(awk -v a="$CPU0" -v b="$CPU1" 'BEGIN { printf "%.1f", b - a }')
    CPUPCT100=$(awk -v c="$CPUSECS" -v s="$secs" -v m="$MBIT" \
        'BEGIN { if (s > 0 && m > 0) printf "%.1f", c / s * 100 / (m / 100); else print "n/a" }')
    echo "$line (cpu $CPUSECS s, $CPUPCT100 % of a core per 100 Mbit/s)" >&2
}

transfer tcp
TCP_MBIT=$MBIT TCP_CPU=$CPUPCT100
transfer utp
UTP_MBIT=$MBIT UTP_CPU=$CPUPCT100

echo "bench: $TORRENTS idle torrents for $IDLE_SECS s" >&2
run_bench idle '^bench: idle start' '^bench: idle [0-9]* torrents' \
    --torrents "$TORRENTS" --idle-secs "$IDLE_SECS"
IDLE_CPU=$(awk -v a="$CPU0" -v b="$CPU1" -v s="$IDLE_SECS" \
    'BEGIN { if (s > 0) printf "%.2f", (b - a) / s * 100; else print "n/a" }')
IDLE_RSS=$RSS1

if [ "$TORRENTS" -gt "$BASELINE" ] && [ "$BASELINE" -gt 0 ]; then
    echo "bench: RSS baseline with $BASELINE torrents" >&2
    run_bench base '^bench: idle start' '^bench: idle [0-9]* torrents' \
        --torrents "$BASELINE" --idle-secs 2
    RSS_PER=$(awk -v n="$IDLE_RSS" -v b="$RSS1" -v t="$TORRENTS" -v u="$BASELINE" \
        'BEGIN { printf "%.1f KiB", (n - b) / (t - u) }')
else
    RSS_PER="n/a"
fi

DATE=$(date -u +%Y-%m-%d)
VERSION=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n 1)
HOST=$(uname -sm)
NOTE="seed and leech in one process"
[ "$SIZE" -ne 1024 ] && NOTE="$NOTE; override: $SIZE MiB instead of 1024"
[ "$TORRENTS" -ne 1000 ] && NOTE="$NOTE; override: $TORRENTS idle torrents instead of 1000"
[ "$IDLE_SECS" -ne 60 ] && NOTE="$NOTE; override: $IDLE_SECS s idle instead of 60"

ROW="| $DATE | $VERSION | $HOST | $TCP_MBIT | $TCP_CPU | $UTP_MBIT | $UTP_CPU | $TORRENTS | $IDLE_CPU | $RSS_PER | $NOTE |"
echo "$ROW"
if [ -z "${BENCH_NO_RECORD:-}" ]; then
    printf '%s\n' "$ROW" >>"$DOC"
    echo "bench: row appended to $DOC" >&2
fi
