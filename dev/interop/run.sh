#!/usr/bin/env bash
# Live interop test: fbt-example downloads from qBittorrent-nox (libtorrent)
# and Transmission, then seeds to Transmission, all through opentracker.
# Needs Docker with Compose v2. Everything lands in target/interop/.
#
#   bash dev/interop/run.sh
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
export INTEROP_DIR="$root/target/interop"
HOST_UID=$(id -u)
HOST_GID=$(id -g)
export HOST_UID HOST_GID
logs="$INTEROP_DIR/logs"
tracker_url=http://tracker:6969/announce

dc() { docker compose -f "$here/compose.yaml" "$@"; }
# Commands inside the long-running containers.
fbt() { dc exec -T fbt "$@"; }
trc() { dc exec -T -u abc transmission "$@"; }

fail() {
    echo "interop: FAIL: $*" >&2
    exit 1
}

cleanup() {
    local status=$?
    trap - EXIT
    if [ "$status" -ne 0 ]; then
        echo "interop: failed (exit $status); container logs in $logs/containers.log" >&2
        mkdir -p "$logs"
        dc logs --no-color --timestamps >"$logs/containers.log" 2>&1 || true
        cp "$INTEROP_DIR"/qbt-config/qBittorrent/data/logs/qbittorrent.log "$logs/" 2>/dev/null || true
        for f in "$logs"/*.log; do
            [ -f "$f" ] || continue
            echo "----- tail of $f" >&2
            tail -n 40 "$f" >&2 || true
        done
    fi
    dc down -v --remove-orphans >/dev/null 2>&1 || true
    exit "$status"
}

# wait_for <description> <timeout secs> <command...>: retry once a second.
wait_for() {
    local what=$1 timeout=$2
    shift 2
    local deadline=$((SECONDS + timeout))
    until "$@" >/dev/null 2>&1; do
        if [ "$SECONDS" -ge "$deadline" ]; then
            fail "timed out after ${timeout}s waiting for $what"
        fi
        sleep 1
    done
}

# make_payload <dir>: 64 MiB of random data in 3 files with sizes that do not
# line up with the 256 KiB piece size.
make_payload() {
    fbt sh -c "mkdir -p '$1' \
        && head -c 30000000 /dev/urandom >'$1/alpha.bin' \
        && head -c 25000000 /dev/urandom >'$1/beta.bin' \
        && head -c 12108864 /dev/urandom >'$1/gamma.bin'"
}

# make_torrent <data dir> <torrent path>
make_torrent() {
    trc transmission-create -o "$2" -t "$tracker_url" -s 256 "$1" >/dev/null
}

# hashes <parent dir> <torrent name>: sha256 of every file, relative paths.
hashes() {
    fbt sh -c "cd '$1' && find '$2' -type f | sort | xargs sha256sum"
}

info_hash() {
    fbt fbt-example info "$1" | sed -n 's/^info hash: //p'
}

qbt_api() { fbt curl -fsS "http://qbittorrent:8080/api/v2/$1" "${@:2}"; }

qbt_field() { # qbt_field <hash> <jq expr>
    qbt_api "torrents/info?hashes=$1" | fbt jq -r ".[0] | $2"
}

qbt_seeding() {
    [ "$(qbt_field "$1" '(.progress == 1) and (.state | test("UP$"))')" = true ]
}

qbt_uploaded() {
    [ "$(qbt_field "$1" '.uploaded')" -gt 0 ]
}

tr_info() { trc transmission-remote -t "$1" -i; }

tr_field() { # tr_field <hash> <label>
    tr_info "$1" | sed -n "s/^ *$2: *//p" | head -n 1
}

tr_complete() {
    [ "$(tr_field "$1" 'Percent Done')" = "100%" ] && tr_field "$1" State | grep -Eq '^(Seeding|Idle)$'
}

tr_uploaded() {
    local up
    up=$(tr_field "$1" Uploaded)
    [ -n "$up" ] && [ "$up" != None ] && ! echo "$up" | grep -Eq '^0([.,]0+)? '
}

trap cleanup EXIT

echo "interop: preparing $INTEROP_DIR"
dc down -v --remove-orphans >/dev/null 2>&1 || true
rm -rf "$INTEROP_DIR"
mkdir -p "$logs" "$INTEROP_DIR/qbt-config/qBittorrent/config" "$INTEROP_DIR/tr-config" \
    "$INTEROP_DIR/tr-dl" "$INTEROP_DIR/fbt"
cp "$here/qbittorrent/qBittorrent.conf" "$INTEROP_DIR/qbt-config/qBittorrent/config/"
cp "$here/transmission/settings.json" "$INTEROP_DIR/tr-config/"

echo "interop: building and starting containers"
dc build --quiet
dc up -d --quiet-pull
wait_for "opentracker" 60 fbt curl -fsS http://tracker:6969/stats
wait_for "qBittorrent WebUI" 120 qbt_api app/version
wait_for "Transmission RPC" 120 trc transmission-remote -l

# ---------------------------------------------------------------- download --
echo "interop: creating payload and fixture.torrent"
make_payload /interop/payload/fixture
make_torrent /interop/payload/fixture /interop/fixture.torrent
hash1=$(info_hash /interop/fixture.torrent)
[ -n "$hash1" ] || fail "could not read the info hash of fixture.torrent"

echo "interop: seeding fixture from qBittorrent and Transmission"
qbt_api torrents/add -F torrents=@/interop/fixture.torrent -F savepath=/interop/payload \
    -F skip_checking=false >/dev/null
trc transmission-remote -a /interop/fixture.torrent -w /interop/payload >/dev/null
wait_for "qBittorrent to verify and seed fixture" 120 qbt_seeding "$hash1"
wait_for "Transmission to verify and seed fixture" 120 tr_complete "$hash1"

echo "interop: fbt-example download (timeout 300s)"
set +e
fbt timeout 300 fbt-example download /interop/fixture.torrent --out /interop/fbt --port 6890 \
    >"$logs/fbt-download.log" 2>&1
status=$?
set -e
case $status in
0) ;;
124) fail "fbt-example download did not finish within 300s (see $logs/fbt-download.log)" ;;
*) fail "fbt-example download exited $status (see $logs/fbt-download.log)" ;;
esac
[ "$(hashes /interop/payload fixture)" = "$(hashes /interop/fbt fixture)" ] ||
    fail "downloaded data differs from the payload"
wait_for "qBittorrent to report uploaded > 0" 30 qbt_uploaded "$hash1"
wait_for "Transmission to report uploaded data" 30 tr_uploaded "$hash1"
echo "interop: qBittorrent uploaded $(qbt_field "$hash1" .uploaded) bytes, Transmission uploaded $(tr_field "$hash1" Uploaded)"
echo "interop: download OK (qbittorrent, transmission)"

# -------------------------------------------------------------------- seed --
echo "interop: creating payload and fixture2.torrent"
make_payload /interop/payload2/fixture2
make_torrent /interop/payload2/fixture2 /interop/fixture2.torrent
hash2=$(info_hash /interop/fixture2.torrent)
[ -n "$hash2" ] || fail "could not read the info hash of fixture2.torrent"

echo "interop: fbt-example seed (300s)"
fbt fbt-example seed /interop/fixture2.torrent --data /interop/payload2 --port 6891 \
    --seed-for 300 >"$logs/fbt-seed.log" 2>&1 &
seed_pid=$!
seed_ready() {
    kill -0 "$seed_pid" 2>/dev/null || fail "fbt-example seed exited early (see $logs/fbt-seed.log)"
    grep -q ' seeding$' "$logs/fbt-seed.log"
}
wait_for "fbt-example to verify and seed fixture2" 120 seed_ready

echo "interop: Transmission downloads fixture2 from fbt-example"
trc transmission-remote -a /interop/fixture2.torrent -w /interop/tr-dl >/dev/null
wait_for "Transmission to complete fixture2" 300 tr_complete "$hash2"
[ "$(hashes /interop/payload2 fixture2)" = "$(hashes /interop/tr-dl fixture2)" ] ||
    fail "data Transmission downloaded differs from the payload"
echo "interop: seed OK (transmission)"
