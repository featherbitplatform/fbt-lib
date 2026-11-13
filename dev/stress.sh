#!/usr/bin/env bash
# Reproduce and deflake intermittent tests. Unix only (macOS and Linux).
#
#   dev/stress.sh <package> <test-binary-or---lib> <test-name> <runs>
#
# Builds the test target once, then runs `<runs>` repetitions of `<test-name>`,
# 4 at a time in parallel (override with STRESS_PARALLEL). Prints `stress: N/N
# passed` and exits non-zero if any repetition fails.
#
# Every repetition is watched: one that outlives STRESS_TIMEOUT seconds
# (default 600) is killed — the test binary *and* everything it forked, which
# for the two-process CLI tests means its seed too — and counts as a failure
# with its log kept. Killing this script (INT, TERM, a CI timeout) kills the
# whole run the same way, so no test process is left behind. There is no
# `timeout` command on macOS and job control needs a terminal, so both are done
# here by walking the process tree with pgrep.
#
# Logs land in target/stress/<package>-<target>-<test-name>/logs/, so one
# test's evidence survives another test's run.
#
# Examples:
#   dev/stress.sh fbt-session swarm rechoke_chokes_peer_that_lost_its_slot 100
#   STRESS_PARALLEL=36 STRESS_TIMEOUT=120 dev/stress.sh fbt-dht swarm twenty_nodes_bootstrap_and_fill_tables 50
set -euo pipefail

usage() {
  sed -n '2,23p' "$0" >&2
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
if [ "$runs" -lt 1 ]; then
  printf 'stress: runs must be at least 1, got %s\n' "$runs" >&2
  exit 2
fi

parallel=${STRESS_PARALLEL:-4}
case $parallel in
  '' | *[!0-9]*) usage ;;
esac
if [ "$parallel" -lt 1 ]; then
  printf 'stress: STRESS_PARALLEL must be at least 1, got %s\n' "$parallel" >&2
  exit 2
fi

stress_timeout=${STRESS_TIMEOUT:-600}
case $stress_timeout in
  '' | *[!0-9]*) usage ;;
esac
if [ "$stress_timeout" -lt 1 ]; then
  printf 'stress: STRESS_TIMEOUT must be at least 1, got %s\n' "$stress_timeout" >&2
  exit 2
fi

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

if [ "$target" = "--lib" ]; then
  target_slug=lib
else
  target_slug=$target
fi
out_dir=target/stress/${pkg}-${target_slug}-${name}
# A fresh run starts from a clean slate, but only its own: stale logs from an
# earlier run are how a green-looking directory ends up describing a different
# test, and wiping a shared directory throws away the other tests' evidence.
rm -rf "$out_dir"
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

# Children are listed before their parent is signalled, so a process cannot
# escape by forking after its parent died. Depth-first, parents first: nothing
# below is left running, and a process that already exited is not an error.
# shellcheck disable=SC2329 # invoked from traps and through xargs below
kill_tree() {
  pid=$1
  sig=$2
  kids=$(pgrep -P "$pid" 2>/dev/null || true)
  kill "-$sig" "$pid" 2>/dev/null || true
  for kid in $kids; do
    kill_tree "$kid" "$sig"
  done
}

work_pid=''

# shellcheck disable=SC2329 # invoked from the traps below
cleanup() {
  # Only while the run is actually alive: after a normal `wait` its pid is
  # reaped, and signalling a recycled pid would hit someone else's process.
  if [ -n "$work_pid" ] && kill -0 "$work_pid" 2>/dev/null; then
    kill_tree "$work_pid" TERM
    sleep 1
    kill_tree "$work_pid" KILL
    return 0
  fi
  # Interrupted before or during the build: no repetitions are running, but a
  # cargo process belongs to this script all the same.
  kids=$(pgrep -P "$$" 2>/dev/null || true)
  for kid in $kids; do
    kill_tree "$kid" TERM
  done
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# shellcheck disable=SC2329 # invoked through xargs below
run_one() {
  idx=$1
  log=$out_dir/logs/$idx.log
  status_file=$out_dir/logs/status/$idx
  timeout_marker=$log.timeout
  mkdir -p "$out_dir/logs/status"
  rm -f "$status_file" "$timeout_marker"

  # The wrapper writes the exit code itself, with errexit off, so a failing
  # test still records one, and so a repetition that was killed is visible as
  # a missing code rather than a passing run. Its pid is what both the watchdog
  # and the traps above kill, tree first.
  (
    set +e
    "$bin" "$name" --exact --test-threads=1 --nocapture >"$log" 2>&1 &
    test_pid=$!
    (
      sleep "$stress_timeout"
      if [ ! -e "$status_file" ]; then
        # Tree first, while the test binary is still there to be asked: a CLI
        # test's seed process is its child and would otherwise outlive it.
        : >"$timeout_marker"
        kill_tree "$test_pid" TERM
        sleep 1
        kill_tree "$test_pid" KILL
      fi
    ) &
    wait "$test_pid"
    printf '%s' "$?" >"$status_file"
  ) &
  job_pid=$!

  set +e
  wait "$job_pid"
  code=$(cat "$status_file" 2>/dev/null)
  set -e

  if [ -e "$timeout_marker" ]; then
    printf 'stress: repetition %s timed out after %ss; killed pid %s and its children\n' \
      "$idx" "$stress_timeout" "$job_pid" >&2
    kill_tree "$job_pid" KILL
    printf fail
    return 1
  fi

  if [ -z "$code" ]; then
    printf 'stress: repetition %s left no exit code (pid %s died oddly)\n' \
      "$idx" "$job_pid" >&2
    kill_tree "$job_pid" KILL
    printf fail
    return 1
  fi

  if [ "$code" != 0 ]; then
    printf fail
    return 1
  fi

  # A filter that matches no test also exits 0. That is not a pass, and it is
  # the difference between a green stress run and a meaningless one.
  if ! grep -q 'test result: ok' "$log" || grep -q 'running 0 tests' "$log"; then
    printf 'stress: %s does not exist in %s\nstress: list the real names with: %s\n' \
      "$name" "$bin" "$list_cmd" >&2
    printf fail
    return 1
  fi

  printf pass
}

export bin out_dir name pkg target list_cmd stress_timeout
export -f run_one kill_tree

printf 'stress: %s/%s :: %s x %s (%s parallel, %ss per repetition)\n' \
  "$pkg" "$target" "$name" "$runs" "$parallel" "$stress_timeout"

# One background subshell rather than a bare pipeline: `$!` of a pipeline is
# its last command, and the traps above need the pid of the whole run.
{
  seq 1 "$runs" | xargs -P "$parallel" -I{} bash -c 'run_one "$@"' _ {} \
    >"$out_dir/logs/summary.txt" 2>"$out_dir/logs/errors.txt"
} &
work_pid=$!

set +e
wait "$work_pid"
set -e

# A failure that never even reached the harness (a missing binary, a filter that
# matched nothing) is reported through stderr, not in the pass/fail stream.
if [ -s "$out_dir/logs/errors.txt" ]; then
  cat "$out_dir/logs/errors.txt" >&2
fi

pass=$(tr -cd 'p' <"$out_dir/logs/summary.txt" | wc -c | tr -d ' ')
fail=$(tr -cd 'f' <"$out_dir/logs/summary.txt" | wc -c | tr -d ' ')

printf 'stress: %s/%s passed\n' "$pass" "$runs"

if [ "$fail" -gt 0 ] || [ $((pass + fail)) -ne "$runs" ]; then
  printf 'stress: failing output kept in %s/logs/\n' "$out_dir"
  exit 1
fi

exit 0
