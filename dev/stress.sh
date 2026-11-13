#!/usr/bin/env bash
# Reproduce and deflake intermittent tests.
#
#   dev/stress.sh <package> <test-binary-or---lib> <test-name> <runs>
#
# Builds the test target once, then runs `<runs>` repetitions of `<test-name>`,
# 4 at a time in parallel (override with STRESS_PARALLEL). Prints `stress: N/N
# passed` and exits non-zero if any repetition fails. The output of every
# failing repetition is kept under target/stress/ so the backtrace survives.
#
# Examples:
#   dev/stress.sh fbt-session swarm rechoke_chokes_peer_that_lost_its_slot 100
#   dev/stress.sh fbt-net peer_io some_flaky_test 300
set -euo pipefail

usage() {
  sed -n '2,14p' "$0"
  exit 2
}

[ "$#" -eq 4 ] || usage

pkg=$1
target=$2
name=$3
runs=$4

case $runs in
  '' | *[!0-9]*) usage ;;
esac

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

out_dir=target/stress
mkdir -p "$out_dir"
# A fresh run starts from a clean slate: stale logs from an earlier run are how
# a green-looking directory ends up describing a different test.
rm -rf "$out_dir/logs"
mkdir -p "$out_dir/logs"

# Build once and take the executable path from cargo itself. Guessing by name
# under target/debug picks up stale binaries left behind by renames, which is
# how a filter that matches nothing ends up "passing".
if [ "$target" = "--lib" ]; then
  build_json=$(cargo test -q -p "$pkg" --lib --no-run --message-format=json)
  list_cmd="cargo test -p $pkg --lib -- --list"
  # A lib target builds `<pkg_with_underscores>-<hash>`; package names use dashes.
  want=$(printf '%s' "$pkg" | tr '-' '_')
else
  build_json=$(cargo test -q -p "$pkg" --test "$target" --no-run --message-format=json)
  list_cmd="cargo test -p $pkg --test $target -- --list"
  want=$target
fi

bin=$(printf '%s\n' "$build_json" \
  | grep -o '"executable":"[^"]*"' \
  | sed 's/"executable":"//; s/"$//' \
  | grep -v '^null$' \
  | grep -- "/${want}-" \
  | head -1)

if [ -z "$bin" ] || [ ! -x "$bin" ]; then
  printf 'stress: could not locate the test binary for %s/%s\n' "$pkg" "$target" >&2
  printf 'stress: build output:\n%s\n' "$build_json" >&2
  exit 1
fi

parallel=${STRESS_PARALLEL:-4}
pass_file=$(mktemp)
fail_file=$(mktemp)
trap 'rm -f "$pass_file" "$fail_file"' EXIT

# shellcheck disable=SC2329 # invoked through xargs below
run_one() {
  idx=$1
  log="$out_dir/logs/$name-$idx.log"
  if "$bin" "$name" --exact --test-threads=1 --nocapture >"$log" 2>&1 \
     && grep -q 'test result: ok' "$log"; then
    # A filter that matches no test also exits 0. That is not a pass, and it is
    # the difference between a green stress run and a meaningless one.
    if grep -q 'running 0 tests' "$log"; then
      printf 'stress: %s does not exist in %s\nstress: list the real names with: %s\n' \
        "$name" "$bin" "$list_cmd" >&2
      return 1
    fi
    printf pass
  else
    printf fail
    return 1
  fi
}

export bin out_dir name pkg target list_cmd pass_file
export -f run_one

printf 'stress: %s/%s :: %s x %s (%s parallel)\n' "$pkg" "$target" "$name" "$runs" "$parallel"

# `grep -c` counts lines, and a count of 0 is exit status 1. Under set -e that
# would abort the run on the first failure, so the totals are read separately.
printf pass >"$pass_file" &
printf fail >"$fail_file" &
seq 1 "$runs" | xargs -P "$parallel" -n 1 -I{} bash -c 'run_one "$@"' _ {} \
  >"$out_dir/logs/summary.txt" 2>"$out_dir/logs/errors.txt" || true

# A failure that never even reached the harness (a missing binary, a filter that
# matched nothing) is reported through stderr, not in the pass/fail stream.
if [ -s "$out_dir/logs/errors.txt" ]; then
  cat "$out_dir/logs/errors.txt" >&2
fi

pass=$(tr -cd 'p' <"$out_dir/logs/summary.txt" | wc -c | tr -d ' ')
fail=$(tr -cd 'f' <"$out_dir/logs/summary.txt" | wc -c | tr -d ' ')
if [ "$pass" = 0 ]; then pass=0; fi
if [ "$fail" = 0 ]; then fail=0; fi

printf 'stress: %s/%s passed\n' "$pass" "$runs"

if [ "$fail" -gt 0 ] || [ $((pass + fail)) -ne "$runs" ]; then
  printf 'stress: failing output kept in %s/logs/\n' "$out_dir"
  exit 1
fi

exit 0
