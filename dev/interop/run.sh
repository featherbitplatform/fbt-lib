#!/usr/bin/env bash
# Live interop test: fbt-example downloads from qBittorrent-nox (libtorrent)
# and Transmission, then seeds to Transmission, all through opentracker; then
# repeats both with encryption forced everywhere, downloads a trackerless
# magnet through the private DHT (bootstrapped from the `dht-node` service
# only) with BEP 9 metadata exchange, and finds a peer through PEX alone.
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
# The private DHT's bootstrap node; every fbt-example run uses only this one.
dht_bootstrap=dht-node:6881
# Seconds both clients get to announce the magnet torrent to the DHT.
dht_announce_wait=150

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
    until "$@" >/dev/null; do
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

# make_trackerless_torrent <data dir> <torrent path>: DHT and PEX only.
make_trackerless_torrent() {
    trc transmission-create -o "$2" -s 256 "$1" >/dev/null
}

# check_download <log> <status> <what>: fail on a timeout or non-zero exit.
check_download() {
    case $2 in
    0) ;;
    124) fail "$3 did not finish within 300s (see $1)" ;;
    *) fail "$3 exited $2 (see $1)" ;;
    esac
}

# container_ip <service>: its address on the swarm network.
container_ip() {
    fbt getent hosts "$1" | awk '{print $1; exit}'
}

# hashes <parent dir> <torrent name>: sha256 of every file, relative paths.
hashes() {
    fbt sh -c "cd '$1' && find '$2' -type f | sort | xargs sha256sum"
}

info_hash() {
    fbt fbt-example info "$1" | sed -n 's/^info hash: //p'
}

qbt_api() { fbt curl -fsS --max-time 10 "http://qbittorrent:8080/api/v2/$1" "${@:2}"; }

qbt_field() { # qbt_field <hash> <jq expr>
    qbt_api "torrents/info?hashes=$1" | fbt jq -r ".[0] | $2"
}

qbt_seeding() {
    [ "$(qbt_field "$1" '(.progress == 1) and (.state | test("^(uploading|stalledUP|forcedUP)$"))')" = true ]
}

qbt_uploaded() {
    [ "$(qbt_field "$1" '.uploaded')" -gt 0 ]
}

qbt_uploaded_more_than() { # qbt_uploaded_more_than <hash> <bytes>
    [ "$(qbt_field "$1" '.uploaded')" -gt "$2" ]
}

qbt_has() {
    [ "$(qbt_field "$1" '.hash')" = "$1" ]
}

qbt_started() {
    [ "$(qbt_field "$1" '.progress > 0')" = true ]
}

tr_info() { trc timeout 15 transmission-remote -t "$1" -i; }

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

tr_uploaded_changed() { # tr_uploaded_changed <hash> <previous Uploaded>
    local up
    up=$(tr_field "$1" Uploaded)
    [ -n "$up" ] && [ "$up" != "$2" ]
}

dht_node_ready() {
    dc logs dht-node | grep -q 'dht node listening on'
}

# set_encryption required|preferred: both clients at once.
set_encryption() {
    local qbt_level tr_flag
    case $1 in
    required) qbt_level=1 tr_flag=--encryption-required ;;
    preferred) qbt_level=0 tr_flag=--encryption-preferred ;;
    esac
    qbt_api app/setPreferences --data-urlencode "json={\"encryption\":$qbt_level}" >/dev/null
    trc transmission-remote "$tr_flag" >/dev/null
    [ "$(qbt_api app/preferences | fbt jq -r .encryption)" = "$qbt_level" ] ||
        fail "qBittorrent did not take encryption=$qbt_level"
}

# start_seed <torrent> <data dir> <port> <log> [extra args...]: background
# fbt-example seed (420 s); sets seed_pid and waits until it is seeding.
start_seed() {
    local torrent=$1 data=$2 port=$3 log=$4
    shift 4
    fbt fbt-example seed "$torrent" --data "$data" --port "$port" \
        --dht-bootstrap "$dht_bootstrap" --seed-for 420 "$@" >"$log" 2>&1 &
    seed_pid=$!
    seed_log=$log
    wait_for "fbt-example to verify and seed $torrent" 120 seed_ready
}

seed_ready() {
    kill -0 "$seed_pid" 2>/dev/null || fail "fbt-example seed exited early (see $seed_log)"
    grep -q ' seeding$' "$seed_log"
}

# stop_seed: ends the background fbt-example seed (the only fbt-example
# process in the fbt container while it runs).
stop_seed() {
    fbt sh -c 'pids=$(pidof fbt-example) && kill $pids' >/dev/null 2>&1 || true
    wait "$seed_pid" 2>/dev/null || true
}

trap cleanup EXIT

echo "interop: preparing $INTEROP_DIR"
dc down -v --remove-orphans >/dev/null 2>&1 || true
rm -rf "$INTEROP_DIR"
mkdir -p "$logs" "$INTEROP_DIR/qbt-config/qBittorrent/config" "$INTEROP_DIR/tr-config" \
    "$INTEROP_DIR/tr-dl" "$INTEROP_DIR/fbt"
cp "$here/qbittorrent/qBittorrent.conf" "$INTEROP_DIR/qbt-config/qBittorrent/config/"
cp "$here/transmission/settings.json" "$here/transmission/dht.bootstrap" "$INTEROP_DIR/tr-config/"

# The swarm network is internal (no route out), so every image is pulled or
# built before anything starts.
echo "interop: building and starting containers"
dc build --quiet
dc --progress quiet pull --ignore-buildable --policy missing
dc up -d --quiet-pull
[ "$(docker network inspect fbt-interop_swarm --format '{{.Internal}}')" = true ] ||
    fail "the swarm network is not internal"
wait_for "dht-node" 60 dht_node_ready
wait_for "opentracker" 60 fbt curl -fsS http://tracker:6969/stats
wait_for "qBittorrent WebUI" 120 qbt_api app/version
wait_for "Transmission RPC" 120 trc timeout 15 transmission-remote -l

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
    --no-dht --dht-bootstrap "$dht_bootstrap" --encryption disabled >"$logs/fbt-download.log" 2>&1
status=$?
set -e
check_download "$logs/fbt-download.log" "$status" "fbt-example download"
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

echo "interop: fbt-example seed (420s)"
start_seed /interop/fixture2.torrent /interop/payload2 6891 "$logs/fbt-seed.log" --no-dht

echo "interop: Transmission downloads fixture2 from fbt-example"
trc transmission-remote -a /interop/fixture2.torrent -w /interop/tr-dl >/dev/null
wait_for "Transmission to complete fixture2" 300 tr_complete "$hash2"
[ "$(hashes /interop/payload2 fixture2)" = "$(hashes /interop/tr-dl fixture2)" ] ||
    fail "data Transmission downloaded differs from the payload"
stop_seed
echo "interop: seed OK (transmission)"

# ------------------------------------------------- magnet through DHT, setup --
# Seeded now, a few minutes before the magnet test runs: Transmission's first
# DHT announce of a new torrent took up to a minute in local runs once
# fbt-example sessions (DHT nodes too) had come and gone, and our download
# looks the torrent up once at its start.
echo "interop: creating payload and trackerless fixture3.torrent"
make_payload /interop/payload3/fixture3
make_trackerless_torrent /interop/payload3/fixture3 /interop/fixture3.torrent
hash3=$(info_hash /interop/fixture3.torrent)
[ -n "$hash3" ] || fail "could not read the info hash of fixture3.torrent"

echo "interop: seeding fixture3 from qBittorrent and Transmission (DHT only)"
qbt_api torrents/add -F torrents=@/interop/fixture3.torrent -F savepath=/interop/payload3 \
    -F skip_checking=false >/dev/null
trc transmission-remote -a /interop/fixture3.torrent -w /interop/payload3 >/dev/null
wait_for "qBittorrent to verify and seed fixture3" 120 qbt_seeding "$hash3"
wait_for "Transmission to verify and seed fixture3" 120 tr_complete "$hash3"
fixture3_seeding_since=$SECONDS

# -------------------------------------------------------- forced encryption --
echo "interop: switching qBittorrent and Transmission to encryption required"
set_encryption required
qbt_up_before=$(qbt_field "$hash1" .uploaded)
tr_up_before=$(tr_field "$hash1" Uploaded)

echo "interop: fbt-example download --encryption forced (timeout 300s)"
set +e
fbt timeout 300 fbt-example download /interop/fixture.torrent --out /interop/fbt-enc --port 6893 \
    --dht-bootstrap "$dht_bootstrap" --encryption forced >"$logs/fbt-download-enc.log" 2>&1
status=$?
set -e
check_download "$logs/fbt-download-enc.log" "$status" "fbt-example download --encryption forced"
[ "$(hashes /interop/payload fixture)" = "$(hashes /interop/fbt-enc fixture)" ] ||
    fail "data downloaded with forced encryption differs from the payload"
wait_for "qBittorrent's uploaded to grow" 30 qbt_uploaded_more_than "$hash1" "$qbt_up_before"
wait_for "Transmission's Uploaded to change" 30 tr_uploaded_changed "$hash1" "$tr_up_before"
echo "interop: encrypted: qBittorrent uploaded $qbt_up_before -> $(qbt_field "$hash1" .uploaded) bytes, Transmission $tr_up_before -> $(tr_field "$hash1" Uploaded)"

echo "interop: fbt-example seed --encryption forced; Transmission downloads fixture2 again"
trc transmission-remote -t "$hash2" -r >/dev/null
mkdir -p "$INTEROP_DIR/tr-dl-enc"
start_seed /interop/fixture2.torrent /interop/payload2 6895 "$logs/fbt-seed-enc.log" --encryption forced
trc transmission-remote -a /interop/fixture2.torrent -w /interop/tr-dl-enc >/dev/null
wait_for "Transmission to complete fixture2 from the encrypted seed" 300 tr_complete "$hash2"
[ "$(hashes /interop/payload2 fixture2)" = "$(hashes /interop/tr-dl-enc fixture2)" ] ||
    fail "data Transmission downloaded from the encrypted seed differs from the payload"
stop_seed
set_encryption preferred
echo "interop: encryption OK (qbittorrent, transmission)"

# ------------------------------------------------------- magnet through DHT --
# fixture3 was seeded before the encryption test (see above); give the
# clients at least 30 s more, and dht_announce_wait in all, to announce it.
wait=$((dht_announce_wait - (SECONDS - fixture3_seeding_since)))
[ "$wait" -ge 30 ] || wait=30
echo "interop: waiting ${wait}s for both clients to announce fixture3 to the DHT"
sleep "$wait"

# --no-pex: with no tracker and no x.pe either, an outgoing connection to a
# client's listen port can only come from an address the DHT handed out.
echo "interop: fbt-example download magnet (timeout 300s)"
set +e
fbt timeout 300 fbt-example download "magnet:?xt=urn:btih:$hash3&dn=fixture3" --out /interop/fbt-magnet \
    --port 6892 --dht-bootstrap "$dht_bootstrap" --no-pex >"$logs/fbt-magnet.log" 2>&1
status=$?
set -e
check_download "$logs/fbt-magnet.log" "$status" "fbt-example download magnet"
[ "$(hashes /interop/payload3 fixture3)" = "$(hashes /interop/fbt-magnet fixture3)" ] ||
    fail "data downloaded from the magnet differs from the payload"
grep -Eq 'found .* peers via dht' "$logs/fbt-magnet.log" ||
    fail "no peers came from the DHT (see $logs/fbt-magnet.log)"
grep -q 'metadata received: fixture3' "$logs/fbt-magnet.log" ||
    fail "no metadata received for the magnet (see $logs/fbt-magnet.log)"
for peer in "$(container_ip qbittorrent):6882" "$(container_ip transmission):51413"; do
    grep -q "^peer connected $peer$" "$logs/fbt-magnet.log" ||
        fail "the magnet download never connected to $peer found via the DHT (see $logs/fbt-magnet.log)"
done
echo "interop: magnet via DHT OK"

# --------------------------------------------------------------------- PEX --
echo "interop: creating payload and trackerless fixture4.torrent"
make_payload /interop/payload4/fixture4
make_trackerless_torrent /interop/payload4/fixture4 /interop/fixture4.torrent
hash4=$(info_hash /interop/fixture4.torrent)
[ -n "$hash4" ] || fail "could not read the info hash of fixture4.torrent"
tr_ip=$(container_ip transmission)
qbt_ip=$(container_ip qbittorrent)
[ -n "$tr_ip" ] && [ -n "$qbt_ip" ] || fail "could not resolve the client addresses"

echo "interop: Transmission seeds fixture4; qBittorrent fetches it from $tr_ip:51413"
trc transmission-remote -a /interop/fixture4.torrent -w /interop/payload4 >/dev/null
wait_for "Transmission to verify and seed fixture4" 120 tr_complete "$hash4"
qbt_api torrents/add -F torrents=@/interop/fixture4.torrent -F savepath=/interop/qbt-pex >/dev/null
wait_for "qBittorrent to add fixture4" 30 qbt_has "$hash4"
qbt_api torrents/addPeers --data-urlencode "hashes=$hash4" --data-urlencode "peers=$tr_ip:51413" >/dev/null
wait_for "qBittorrent to report progress on fixture4" 120 qbt_started "$hash4"

echo "interop: fbt-example download from qBittorrent only, no DHT (timeout 300s)"
set +e
fbt timeout 300 fbt-example download /interop/fixture4.torrent --no-dht --dht-bootstrap "$dht_bootstrap" \
    --peer "$qbt_ip:6882" --out /interop/fbt-pex --port 6894 >"$logs/fbt-pex.log" 2>&1
status=$?
set -e
check_download "$logs/fbt-pex.log" "$status" "fbt-example download (PEX)"
[ "$(hashes /interop/payload4 fixture4)" = "$(hashes /interop/fbt-pex fixture4)" ] ||
    fail "data downloaded in the PEX scenario differs from the payload"
grep -Eq 'found .* peers via pex' "$logs/fbt-pex.log" ||
    fail "no peers came from PEX (see $logs/fbt-pex.log)"
grep -q "^peer connected $tr_ip:" "$logs/fbt-pex.log" ||
    fail "never connected to Transmission, which only PEX could name (see $logs/fbt-pex.log)"
echo "interop: PEX OK"
