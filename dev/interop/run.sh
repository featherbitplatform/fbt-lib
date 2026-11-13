#!/usr/bin/env bash
# Live interop test: fbt-example downloads from qBittorrent-nox (libtorrent)
# and Transmission, then seeds to Transmission, all through opentracker; then
# repeats both with encryption forced everywhere, downloads a trackerless
# magnet through the private DHT (bootstrapped from the `dht-node` service
# only) with BEP 9 metadata exchange, finds a peer through PEX alone, moves
# a torrent each way with qBittorrent over uTP only, streams a file from
# qBittorrent with seeks, finds a second fbt-example (the `fbt-lsd`
# service) through LSD alone, resumes a half-done download from its resume
# file without a recheck, downloads from an nginx web seed alone and through
# a Dante SOCKS5 proxy. Then BitTorrent v2: qBittorrent creates a v2 and a
# hybrid torrent (its WebUI torrent creator) that fbt-example downloads, by
# .torrent and by `btmh`-only magnet; `fbt-example create` must reproduce
# qBittorrent's v2 info hash for the same payload; and qBittorrent (v2, by
# .torrent and by magnet) and Transmission (hybrid) download torrents
# fbt-example created from an fbt-example seed. Last, `fbt-example download
# --api` serves fbt-api while it downloads: curl lists the torrent, streams
# byte ranges, checks 401 and 416 and follows its event stream.
# Needs Docker with Compose v2. Everything lands in target/interop/.
#
#   bash dev/interop/run.sh
#
# INTEROP_ONLY=utp (a comma-separated list of download, seed, encryption,
# magnet, pex, utp, lsd, stream, resume, webseed, proxy, v2, hybrid, api) runs only those scenarios, after the shared
# setup; uTP always runs, and an unknown name fails before anything starts. FBT_RUST_LOG sets RUST_LOG for the uTP download (its
# tracing lands in fbt-download-utp.log), e.g. FBT_RUST_LOG=fbt_utp=debug.
set -euo pipefail

known_scenarios=(download seed encryption magnet pex utp lsd stream resume webseed proxy v2 hybrid api)
# An unknown name fails at once, before the setup, rather than selecting nothing.
if [ -n "${INTEROP_ONLY:-}" ]; then
    IFS=, read -ra only_names <<<"$INTEROP_ONLY"
    for name in "${only_names[@]}"; do
        [[ " ${known_scenarios[*]} " == *" $name "* ]] || {
            echo "interop: unknown scenario '$name' (known: $(IFS=,; echo "${known_scenarios[*]}" | sed 's/,/, /g'))" >&2
            exit 1
        }
    done
fi
# want <scenario>: INTEROP_ONLY is unset or names it.
want() { [ -z "${INTEROP_ONLY:-}" ] || [[ ",$INTEROP_ONLY," == *",$1,"* ]]; }
utp_env=()
[ -z "${FBT_RUST_LOG:-}" ] || utp_env=(env "RUST_LOG=$FBT_RUST_LOG")

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
# The private DHT's nodes, one container (and address) each, all on UDP 6881.
dht_services=(dht-node dht-helper-{1..11})
dht_nodes=${#dht_services[@]}
# Nodes among them whose store must grow before the magnet test: measured, a
# qBittorrent announce alone grows 3 of them (libtorrent searches through one
# node per /24), and once Transmission's lands too (on its closest K, which
# include qBittorrent's own node) 7 have grown.
announced_nodes=5

# The tracker as a magnet `tr=` value.
tracker_param=http%3A%2F%2Ftracker%3A6969%2Fannounce
# Piece size of every v2 and hybrid torrent, ours and qBittorrent's.
v2_piece_size=262144

dc() { docker compose -f "$here/compose.yaml" "$@"; }
# Commands inside the long-running containers.
fbt() { dc exec -T fbt "$@"; }
lsd_host() { dc exec -T fbt-lsd "$@"; }
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

# make_v2_payload <dir>: make_payload with mode 0644 files (libtorrent would
# record an executable file as `attr` `x`, which fbt-example create never
# writes, and the v2 info hashes would differ for that alone).
make_v2_payload() {
    make_payload "$1"
    fbt chmod 0644 "$1/alpha.bin" "$1/beta.bin" "$1/gamma.bin"
}

# make_many_payload <dir>: 12 files of 3 000 000 bytes (mode 0644), each a
# few pieces long, so a v2 downloader fetching by magnet needs 12 piece
# layers. libtorrent keeps at most 2 hash requests outstanding per peer, so
# one qBittorrent never reaches the seed's serving cap (8 per torrent); the
# scenario logs how many requests were rejected over it (0 so far).
make_many_payload() {
    fbt sh -c "mkdir -p '$1' && for i in \$(seq -w 1 12); do
        head -c 3000000 /dev/urandom >'$1/part'\$i.bin && chmod 0644 '$1/part'\$i.bin; done"
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

# network_ip <service> <network>: its address on one compose network (fbt
# and fbt-lsd are on two, so a name lookup could answer with either).
network_ip() {
    docker inspect --format "{{(index .NetworkSettings.Networks \"fbt-interop_$2\").IPAddress}}" \
        "$(dc ps -q "$1")"
}

# has_mcast_route <service>: 224.0.0.0/4 is routed (fbt-mcast-route ran).
has_mcast_route() {
    dc exec -T "$1" cat /proc/net/route | awk '$2 == "000000E0" && $8 == "000000F0" {f = 1} END {exit !f}'
}

# hashes <parent dir> <torrent name>: sha256 of every file, relative paths.
# BEP 47 pad files (`.pad/<n>`, which Transmission writes to disk for a
# hybrid) are not payload and are left out.
hashes() {
    fbt sh -c "cd '$1' && find '$2' -type f -not -path '*/.pad/*' | sort | xargs sha256sum"
}

# same_data <dir_a> <dir_b> <name>: both copies hash the same.
same_data() {
    [ "$(hashes "$1" "$3")" = "$(hashes "$2" "$3")" ]
}

# other_client_data <payload_dir> <dir> <name> <what>: another client may
# report a torrent complete before its disk cache has written every piece, so
# give its copy in <dir> up to 120 s to match the payload.
other_client_data() {
    wait_for "$4 to write data matching the payload" 120 same_data "$1" "$2" "$3"
}

info_hash() {
    fbt fbt-example info "$1" | sed -n 's/^info hash: //p'
}

# info_hash_v2 <torrent>: its full v2 info hash (64 hex), if it has one.
info_hash_v2() {
    fbt fbt-example info "$1" | sed -n 's/^info hash v2: //p'
}

# qbt_id <torrent>: qBittorrent's id for it (its `hash` in torrents/info):
# the truncated v2 hash when there is one (hybrids included), else the v1.
qbt_id() {
    local v2
    v2=$(info_hash_v2 "$1")
    if [ -n "$v2" ]; then echo "${v2:0:40}"; else info_hash "$1"; fi
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

qbt_complete() {
    [ "$(qbt_field "$1" '.progress == 1')" = true ]
}

qbt_gone() {
    [ "$(qbt_api "torrents/info?hashes=$1" | fbt jq length)" = 0 ]
}

# qbt_remove <hash>: drop a torrent from qBittorrent, keeping its files.
qbt_remove() {
    qbt_api torrents/delete --data-urlencode "hashes=$1" --data-urlencode deleteFiles=false >/dev/null
    wait_for "qBittorrent to remove $1" 30 qbt_gone "$1"
}

qbt_task_status() {
    qbt_api "torrentcreator/status?taskID=$1" | fbt jq -r '.[0].status'
}

qbt_task_done() {
    case $(qbt_task_status "$1") in
    Finished | Failed) ;;
    *) false ;;
    esac
}

# qbt_create <source dir> <v2|hybrid> <torrent path>: qBittorrent (libtorrent)
# creates a torrent of <source dir> with its WebUI torrent creator, at
# v2_piece_size, announcing to our tracker, and starts seeding it from the
# directory's parent; the .torrent is saved at <torrent path> (under
# /interop). The WebUI splits `trackers` on `|`; there is one here.
qbt_create() {
    local task status
    task=$(qbt_api torrentcreator/addTask --data-urlencode "sourcePath=$1" --data-urlencode "format=$2" \
        --data-urlencode "pieceSize=$v2_piece_size" --data-urlencode "trackers=$tracker_url" \
        --data-urlencode startSeeding=true | fbt jq -r .taskID)
    [ -n "$task" ] && [ "$task" != null ] || fail "qBittorrent did not take the $2 torrent creator task"
    wait_for "qBittorrent to create the $2 torrent of $1" 300 qbt_task_done "$task"
    status=$(qbt_task_status "$task")
    [ "$status" = Finished ] ||
        fail "qBittorrent's $2 torrent creator task ended $status: $(qbt_api "torrentcreator/status?taskID=$task")"
    qbt_api "torrentcreator/torrentFile?taskID=$task" >"$INTEROP_DIR/${3#/interop/}"
    qbt_api torrentcreator/deleteTask --data-urlencode "taskID=$task" >/dev/null
}

# fbt_create <log> <args...>: fbt-example create, its output in <log>.
fbt_create() {
    local log=$1
    shift
    fbt fbt-example create "$@" >"$log" 2>&1 || fail "fbt-example create failed (see $log)"
}

# fbt_download <log> <what> <args...>: a foreground fbt-example download
# (timeout 300 s, no DHT), failing on a timeout or an error.
fbt_download() {
    local log=$1 what=$2 status
    shift 2
    set +e
    fbt timeout 300 fbt-example download "$@" --no-dht --dht-bootstrap "$dht_bootstrap" >"$log" 2>&1
    status=$?
    set -e
    check_download "$log" "$status" "$what"
}

# set_bt_protocol <0|1|2>: qBittorrent's peer connection protocol (0 both,
# 1 TCP, 2 uTP; Advanced, "Peer connection protocol"), read back.
set_bt_protocol() {
    qbt_api app/setPreferences --data-urlencode "json={\"bittorrent_protocol\":$1}" >/dev/null
    [ "$(qbt_api app/preferences | fbt jq -r .bittorrent_protocol)" = "$1" ] ||
        fail "qBittorrent did not take bittorrent_protocol=$1"
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

# dht_log: every DHT node's output, each line prefixed `[<service>] `.
dht_log() { dc logs --no-color --no-log-prefix "${dht_services[@]}"; }

# dht_nodes_up: every DHT node is listening, every helper knows a node, and
# the bootstrap node dht-node knows at least dht_bucket nodes. Not one per
# helper: a Kademlia table splits only the bucket holding its own id, so the
# far half of the id space holds at most K = 8 nodes, and with random ids
# more than 8 of the 11 helpers land there in a few percent of runs (seen:
# dht-node stuck at v4=10 for the whole 120 s).
dht_bucket=8
dht_nodes_up() {
    local log listening joined v4
    log=$(dht_log)
    listening=$(echo "$log" | grep -c '^\[[a-z0-9-]*\] dht node listening on ' || true)
    joined=$(echo "$log" | sed -n 's/^\[\(dht-helper-[0-9]*\)\] dht nodes: v4=\([0-9]*\) .*/\1 \2/p' |
        awk '{c[$1] = $2} END {for (p in c) if (c[p] > 0) n++; print n + 0}')
    v4=$(echo "$log" | sed -n 's/^\[dht-node\] dht nodes: v4=\([0-9]*\) .*/\1/p' | tail -n 1)
    [ "$listening" -eq "$dht_nodes" ] && [ "$joined" -eq $((dht_nodes - 1)) ] &&
        [ "${v4:-0}" -ge "$dht_bucket" ]
}

# dht_store_counts: "<service> <stored info hashes>" per node, latest report.
dht_store_counts() {
    dht_log | sed -n 's/^\[\([a-z0-9-]*\)\] dht nodes: .* torrents=\([0-9]*\)$/\1 \2/p' |
        awk '{c[$1] = $2} END {for (p in c) print p, c[p]}' | sort
}

# dht_new_stores <baseline>: nodes whose store grew past the baseline counts.
dht_new_stores() {
    join <(echo "$1") <(dht_store_counts) | awk '$3 > $2 {n++} END {print n + 0}'
}

dht_announced() { [ "$(dht_new_stores "$1")" -ge "$announced_nodes" ]; }

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
    trc timeout 15 transmission-remote -si | grep -Eq "^ *Encryption: $1$" ||
        fail "Transmission did not take encryption $1"
}

# The container start_seed and stop_seed run in.
seed_service=fbt
# Extra `docker compose exec` options for start_seed (e.g. `-e RUST_LOG=...`).
seed_exec_env=()
# How long start_seed's seed runs (--seed-for); a seed must outlive every
# wait of every leg that uses it.
seed_secs=420

# start_seed <torrent> <data dir> <port> <log> [extra args...]: background
# fbt-example seed ($seed_secs s) in $seed_service; sets seed_pid and waits
# until it is seeding.
start_seed() {
    local torrent=$1 data=$2 port=$3 log=$4
    shift 4
    dc exec -T ${seed_exec_env[@]+"${seed_exec_env[@]}"} "$seed_service" fbt-example seed "$torrent" --data "$data" --port "$port" \
        --dht-bootstrap "$dht_bootstrap" --seed-for "$seed_secs" "$@" >"$log" 2>&1 &
    seed_pid=$!
    seed_log=$log
    wait_for "fbt-example to verify and seed $torrent" 120 seed_ready
}

seed_ready() {
    kill -0 "$seed_pid" 2>/dev/null || fail "fbt-example seed exited early (see $seed_log)"
    grep -q ' seeding$' "$seed_log"
}

fbt_example_gone() { ! dc exec -T "$seed_service" pidof fbt-example >/dev/null; }

# stop_seed: ends the background fbt-example seed (the only fbt-example
# process in its container while it runs) and waits until it is gone.
stop_seed() {
    dc exec -T "$seed_service" sh -c 'kill $(pidof fbt-example)' ||
        fail "no fbt-example seed was running to stop (see $seed_log)"
    wait_for "fbt-example seed to exit" 30 fbt_example_gone
    wait "$seed_pid" 2>/dev/null || true
}

trap cleanup EXIT

echo "interop: preparing $INTEROP_DIR"
dc down -v --remove-orphans >/dev/null 2>&1 || true
rm -rf "$INTEROP_DIR"
mkdir -p "$logs" "$INTEROP_DIR/qbt-config/qBittorrent/config" "$INTEROP_DIR/tr-config" \
    "$INTEROP_DIR/tr-dl" "$INTEROP_DIR/fbt" "$INTEROP_DIR/payload6"
cp "$here/qbittorrent/qBittorrent.conf" "$INTEROP_DIR/qbt-config/qBittorrent/config/"
cp "$here/transmission/settings.json" "$here/transmission/dht.bootstrap" "$INTEROP_DIR/tr-config/"

# The swarm network is internal (no route out), so every image is pulled or
# built before anything starts.
echo "interop: building and starting containers"
dc build --quiet
dc --progress quiet pull --ignore-buildable --policy missing
# Timed from here: `up` itself waits for dht-node (and Transmission for every
# helper) to be healthy.
dht_started=$SECONDS
dc up -d --quiet-pull
for net in swarm lan; do
    [ "$(docker network inspect "fbt-interop_$net" --format '{{.Internal}}')" = true ] ||
        fail "the $net network is not internal"
done

# LSD needs multicast between fbt and fbt-lsd (over `lan`, see compose.yaml).
# Both announce a probe hash at once and must hear each other, or the run
# stops here instead of reporting a bogus LSD failure later.
wait_for "the multicast route in fbt" 30 has_mcast_route fbt
wait_for "the multicast route in fbt-lsd" 30 has_mcast_route fbt-lsd
# `docker compose exec` alone can take seconds while the other containers
# start, enough to slide one 3 s probe past the other's end, so both wait
# for the same second on the clock they share (one kernel) and then start.
probe_at=$(($(fbt date +%s) + 3))
probe() { # probe <service>: lsd-probe from second probe_at on
    dc exec -T "$1" sh -c "while [ \$(date +%s) -lt $probe_at ]; do sleep 0.05; done
        exec fbt-example lsd-probe --port 6899 --timeout-secs 3"
}
probe fbt >"$logs/lsd-probe-fbt.log" 2>&1 &
probe_pid=$!
probe fbt-lsd >"$logs/lsd-probe-fbt-lsd.log" 2>&1 ||
    fail "multicast does not reach fbt-lsd from fbt: LSD cannot work on this Docker network (see $logs/lsd-probe-fbt-lsd.log)"
wait "$probe_pid" ||
    fail "multicast does not reach fbt from fbt-lsd: LSD cannot work on this Docker network (see $logs/lsd-probe-fbt.log)"
echo "interop: multicast between fbt and fbt-lsd OK ($(cat "$logs/lsd-probe-fbt.log"))"
wait_for "all $dht_nodes DHT nodes to listen and join" 120 dht_nodes_up
echo "interop: all $dht_nodes DHT nodes joined $((SECONDS - dht_started))s after starting the containers"
wait_for "opentracker" 60 fbt curl -fsS http://tracker:6969/stats
wait_for "qBittorrent WebUI" 120 qbt_api app/version
wait_for "Transmission RPC" 120 trc timeout 15 transmission-remote -l
tr_ip=$(container_ip transmission)
qbt_ip=$(container_ip qbittorrent)
[ -n "$tr_ip" ] && [ -n "$qbt_ip" ] || fail "could not resolve the client addresses"
# Setup (image builds and pulls, container start, DHT join) is timed apart
# from the scenarios, which alone say anything about our code.
setup_secs=$SECONDS
echo "interop: setup took ${setup_secs}s"

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

if want download; then
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
fi

# -------------------------------------------------------------------- seed --
echo "interop: creating payload and fixture2.torrent"
make_payload /interop/payload2/fixture2
make_torrent /interop/payload2/fixture2 /interop/fixture2.torrent
hash2=$(info_hash /interop/fixture2.torrent)
[ -n "$hash2" ] || fail "could not read the info hash of fixture2.torrent"

if want seed; then
echo "interop: fbt-example seed (420s)"
start_seed /interop/fixture2.torrent /interop/payload2 6891 "$logs/fbt-seed.log" --no-dht

echo "interop: Transmission downloads fixture2 from fbt-example"
trc transmission-remote -a /interop/fixture2.torrent -w /interop/tr-dl >/dev/null
wait_for "Transmission to complete fixture2" 300 tr_complete "$hash2"
other_client_data /interop/payload2 /interop/tr-dl fixture2 Transmission
stop_seed
echo "interop: seed OK (transmission)"
fi

if want encryption; then
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
other_client_data /interop/payload2 /interop/tr-dl-enc fixture2 Transmission
grep -q "^peer connected $tr_ip:" "$logs/fbt-seed-enc.log" ||
    fail "Transmission never connected to the encrypted seed (see $logs/fbt-seed-enc.log)"
stop_seed
set_encryption preferred
echo "interop: encryption OK (qbittorrent, transmission)"
fi

if want magnet; then
# ------------------------------------------------------- magnet through DHT --
echo "interop: creating payload and trackerless fixture3.torrent"
make_payload /interop/payload3/fixture3
make_trackerless_torrent /interop/payload3/fixture3 /interop/fixture3.torrent
hash3=$(info_hash /interop/fixture3.torrent)
[ -n "$hash3" ] || fail "could not read the info hash of fixture3.torrent"

# Nothing else is announcing now, so a node whose store count grows from here
# on stores fixture3. qBittorrent's announce alone grows 3 of the DHT nodes'
# stores; announced_nodes or more mean Transmission's has landed too.
dht_baseline=$(dht_store_counts)
echo "interop: seeding fixture3 from qBittorrent and Transmission (DHT only)"
qbt_api torrents/add -F torrents=@/interop/fixture3.torrent -F savepath=/interop/payload3 \
    -F skip_checking=false >/dev/null
trc transmission-remote -a /interop/fixture3.torrent -w /interop/payload3 >/dev/null
wait_for "qBittorrent to verify and seed fixture3" 120 qbt_seeding "$hash3"
wait_for "Transmission to verify and seed fixture3" 120 tr_complete "$hash3"
announce_started=$SECONDS
wait_for "Transmission to announce fixture3 ($announced_nodes or more DHT node stores grown)" 240 \
    dht_announced "$dht_baseline"
echo "interop: fixture3 stored on $(dht_new_stores "$dht_baseline") of $dht_nodes DHT nodes after $((SECONDS - announce_started))s"

# --no-pex: with no tracker and no x.pe either, an outgoing connection to a
# client's listen port can only come from an address the DHT handed out. Our
# download looks the DHT up once metadata arrives only every
# dht_announce_interval (15 min), so a Transmission announce that lands just
# after its first lookup is caught by a fresh attempt (at most 3 in all).
attempt=1
while :; do
    log="$logs/fbt-magnet-$attempt.log"
    echo "interop: fbt-example download magnet, attempt $attempt (timeout 300s)"
    set +e
    fbt timeout 300 fbt-example download "magnet:?xt=urn:btih:$hash3&dn=fixture3" \
        --out "/interop/fbt-magnet-$attempt" --port 6892 --dht-bootstrap "$dht_bootstrap" --no-pex \
        >"$log" 2>&1
    status=$?
    set -e
    check_download "$log" "$status" "fbt-example download magnet"
    [ "$(hashes /interop/payload3 fixture3)" = "$(hashes "/interop/fbt-magnet-$attempt" fixture3)" ] ||
        fail "data downloaded from the magnet differs from the payload"
    grep -Eq 'found .* peers via dht' "$log" || fail "no peers came from the DHT (see $log)"
    grep -q 'metadata received: fixture3' "$log" || fail "no metadata received for the magnet (see $log)"
    grep -Eq "^peer connected $qbt_ip:6882 via (tcp|utp)$" "$log" ||
        fail "the magnet download never connected to qBittorrent ($qbt_ip:6882) found via the DHT (see $log)"
    grep -Eq "^peer connected $tr_ip:51413 via (tcp|utp)$" "$log" && break
    [ "$attempt" -lt 3 ] ||
        fail "no attempt connected to Transmission ($tr_ip:51413) found via the DHT (see $logs/fbt-magnet-*.log)"
    echo "interop: attempt $attempt did not reach Transmission through the DHT; retrying in 30s"
    attempt=$((attempt + 1))
    sleep 30
done
echo "interop: magnet attempt $attempt passed"
echo "interop: magnet via DHT OK"
fi

if want pex; then
# --------------------------------------------------------------------- PEX --
echo "interop: creating payload and trackerless fixture4.torrent"
make_payload /interop/payload4/fixture4
make_trackerless_torrent /interop/payload4/fixture4 /interop/fixture4.torrent
hash4=$(info_hash /interop/fixture4.torrent)
[ -n "$hash4" ] || fail "could not read the info hash of fixture4.torrent"

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
grep -Eq "^peer connected $tr_ip:51413 via (tcp|utp)$" "$logs/fbt-pex.log" ||
    fail "never connected to Transmission, which only PEX could name (see $logs/fbt-pex.log)"
echo "interop: PEX OK"
fi

# ------------------------------------------------------ uTP with qBittorrent --
fbt_ip=$(network_ip fbt swarm)
[ -n "$fbt_ip" ] || fail "could not resolve fbt's address on swarm"
echo "interop: switching qBittorrent to uTP only"
set_bt_protocol 2
qbt_up_before=$(qbt_field "$hash1" .uploaded)

echo "interop: fbt-example download --transport utp (timeout 300s)"
utp_started=$SECONDS
set +e
fbt ${utp_env[@]+"${utp_env[@]}"} timeout 300 fbt-example download /interop/fixture.torrent --out /interop/fbt-utp --port 6895 \
    --transport utp --no-dht --dht-bootstrap "$dht_bootstrap" >"$logs/fbt-download-utp.log" 2>&1
status=$?
set -e
check_download "$logs/fbt-download-utp.log" "$status" "fbt-example download --transport utp"
[ "$(hashes /interop/payload fixture)" = "$(hashes /interop/fbt-utp fixture)" ] ||
    fail "data downloaded over uTP differs from the payload"
grep -Eq "^peer connected $qbt_ip:6882 via utp$" "$logs/fbt-download-utp.log" ||
    fail "never connected to qBittorrent ($qbt_ip:6882) over uTP (see $logs/fbt-download-utp.log)"
wait_for "qBittorrent's uploaded to grow over uTP" 30 qbt_uploaded_more_than "$hash1" "$qbt_up_before"
echo "interop: uTP: qBittorrent uploaded $qbt_up_before -> $(qbt_field "$hash1" .uploaded) bytes"
echo "interop: uTP download OK (qbittorrent) in $((SECONDS - utp_started))s"

echo "interop: fbt-example seed --transport utp; qBittorrent (uTP only) downloads fixture2"
# Transmission still seeds fixture2 (and would be on the tracker's list):
# drop it, so the uTP seed is qBittorrent's only source.
if want seed; then
trc transmission-remote -t "$hash2" -r >/dev/null
fi
start_seed /interop/fixture2.torrent /interop/payload2 6896 "$logs/fbt-seed-utp.log" --no-dht --transport utp
qbt_api torrents/add -F torrents=@/interop/fixture2.torrent -F savepath=/interop/qbt-utp >/dev/null
wait_for "qBittorrent to add fixture2" 30 qbt_has "$hash2"
qbt_api torrents/addPeers --data-urlencode "hashes=$hash2" --data-urlencode "peers=$fbt_ip:6896" >/dev/null
wait_for "qBittorrent to complete fixture2 from the uTP seed" 300 qbt_complete "$hash2"
other_client_data /interop/payload2 /interop/qbt-utp fixture2 qBittorrent
grep -Eq "^peer connected $qbt_ip:[0-9]+ via utp$" "$logs/fbt-seed-utp.log" ||
    fail "qBittorrent never connected to the uTP seed (see $logs/fbt-seed-utp.log)"
stop_seed
set_bt_protocol 0
echo "interop: uTP seed OK (qbittorrent)"

if want stream; then
# ------------------------------------------------------ streaming, with seeks --
# qBittorrent is the only source: Transmission's copy of fixture 1 stops.
echo "interop: fbt-example stream from qBittorrent, with seeks"
trc transmission-remote -t "$hash1" -S >/dev/null
stream_started=$SECONDS
# beta.bin's index in the torrent (files print in torrent order; no pad files).
beta_index=$(fbt fbt-example info /interop/fixture.torrent |
    awk '/^files:/ {n = 0; on = 1; next} /^trackers:/ {on = 0} on { if ($1 ~ /(^|\/)beta\.bin$/) { print n; exit } n++ }')
[ -n "$beta_index" ] || fail "could not find beta.bin in fixture.torrent"
qbt_up_before=$(qbt_field "$hash1" .uploaded)
beta=/interop/payload/fixture/beta.bin

# stream_step <step> <offset> <length|-> <out file>: one fbt-example stream run.
stream_step() {
    local log="$logs/fbt-stream-$1.log" len=()
    [ "$3" = - ] || len=(--length "$3")
    set +e
    fbt timeout 300 fbt-example stream /interop/fixture.torrent --file "$beta_index" \
        --out /interop/fbt-stream --port 6899 --no-dht --seek "$2" ${len[@]+"${len[@]}"} \
        --to "$4" >"$log" 2>&1
    status=$?
    set -e
    check_download "$log" "$status" "fbt-example stream ($1)"
}
# stream_matches <offset> <length|-> <out file>: the output equals that range of beta.bin.
stream_matches() {
    local want_sum got_sum
    if [ "$2" = - ]; then
        want_sum=$(fbt sh -c "tail -c +$(($1 + 1)) '$beta' | sha1sum")
    else
        want_sum=$(fbt sh -c "tail -c +$(($1 + 1)) '$beta' | head -c $2 | sha1sum")
    fi
    got_sum=$(fbt sh -c "sha1sum <'$3'")
    [ "$want_sum" = "$got_sum" ]
}

stream_step mid 12000000 3000000 /interop/stream-mid.bin
stream_matches 12000000 3000000 /interop/stream-mid.bin ||
    fail "the mid-file stream differs from beta.bin (see $logs/fbt-stream-mid.log)"
stream_step all 0 - /interop/stream-all.bin
stream_matches 0 - /interop/stream-all.bin ||
    fail "the whole-file stream differs from beta.bin (see $logs/fbt-stream-all.log)"
stream_step tail 24999000 - /interop/stream-tail.bin
stream_matches 24999000 - /interop/stream-tail.bin ||
    fail "the tail stream differs from beta.bin (see $logs/fbt-stream-tail.log)"
for step in mid all; do
    grep -Eq "^peer connected $qbt_ip:6882( |$)" "$logs/fbt-stream-$step.log" ||
        fail "the $step stream never connected to qBittorrent ($qbt_ip:6882) (see $logs/fbt-stream-$step.log)"
done
wait_for "qBittorrent's uploaded to grow while streaming" 30 qbt_uploaded_more_than "$hash1" "$qbt_up_before"
echo "interop: stream: qBittorrent uploaded $qbt_up_before -> $(qbt_field "$hash1" .uploaded) bytes"
trc transmission-remote -t "$hash1" -s >/dev/null
echo "interop: stream with seeks OK (qbittorrent) in $((SECONDS - stream_started))s"
fi

if want lsd; then
# --------------------------------------------------------------------- LSD --
echo "interop: creating payload and trackerless fixture5.torrent"
make_payload /interop/payload5/fixture5
make_trackerless_torrent /interop/payload5/fixture5 /interop/fixture5.torrent
hash5=$(info_hash /interop/fixture5.torrent)
[ -n "$hash5" ] || fail "could not read the info hash of fixture5.torrent"
lsd_ip=$(network_ip fbt-lsd lan)
[ -n "$lsd_ip" ] || fail "could not resolve fbt-lsd's address on lan"

# A session announces each torrent on LSD once per 5 minutes, at its start,
# and nothing answers an announce. So the download (no tracker, no DHT, no
# PEX, no --peer) runs first and has sent its own announce before the seed
# starts; the seed's announce, once it has verified its data, is then the
# only way the download can learn fbt-lsd's address, and the download dials
# it (fbt-lsd:6897) rather than being dialled from an ephemeral port.
echo "interop: fbt-example download --lsd in fbt (timeout 300s)"
lsd_log="$logs/fbt-lsd-download.log"
fbt timeout 300 fbt-example download /interop/fixture5.torrent --lsd --no-dht --no-pex \
    --dht-bootstrap "$dht_bootstrap" --out /interop/fbt-lsd --port 6898 >"$lsd_log" 2>&1 &
lsd_dl_pid=$!
# Progress lines come once a second; three in the downloading state put the
# download's first announce (sent within 750 ms of it starting) behind us.
lsd_download_announced() {
    kill -0 "$lsd_dl_pid" 2>/dev/null || fail "the LSD download exited early (see $lsd_log)"
    [ "$(grep -c ' downloading$' "$lsd_log")" -ge 3 ]
}
wait_for "the LSD download to start" 60 lsd_download_announced

echo "interop: fbt-example seed --lsd in fbt-lsd ($lsd_ip:6897)"
seed_service=fbt-lsd
start_seed /interop/fixture5.torrent /interop/payload5 6897 "$logs/fbt-lsd-seed.log" --lsd --no-dht --no-pex
set +e
wait "$lsd_dl_pid"
status=$?
set -e
check_download "$lsd_log" "$status" "fbt-example download --lsd"
[ "$(hashes /interop/payload5 fixture5)" = "$(hashes /interop/fbt-lsd fixture5)" ] ||
    fail "data downloaded in the LSD scenario differs from the payload"
grep -Eq 'found .* peers via lsd' "$lsd_log" || fail "no peers came from LSD (see $lsd_log)"
grep -Eq "^peer connected $lsd_ip:6897 via (tcp|utp)$" "$lsd_log" ||
    fail "never connected to fbt-lsd ($lsd_ip:6897), which only LSD could name (see $lsd_log)"
stop_seed
seed_service=fbt
echo "interop: LSD OK"
fi
if want resume; then
# ------------------------------------------------- resume without recheck --
# qBittorrent is the only source: Transmission's copy of fixture 1 stops. The
# first run stops at 50 % (pause, save the resume file, exit 0); the second
# loads it, must trust its pieces without checking the files, and finishes.
echo "interop: fbt-example download --resume --stop-at 50, then again from the resume file"
trc transmission-remote -t "$hash1" -S >/dev/null
resume_file=/interop/fbt-resume/r.dat
mkdir -p "$INTEROP_DIR/fbt-resume"
qbt_up_before=$(qbt_field "$hash1" .uploaded)
# resume_run <step> [extra args...]: one download into /interop/fbt-resume.
resume_run() {
    local log="$logs/fbt-resume-$1.log"
    shift
    set +e
    fbt timeout 300 fbt-example download /interop/fixture.torrent --out /interop/fbt-resume --port 6900 \
        --no-dht --dht-bootstrap "$dht_bootstrap" --resume "$resume_file" "$@" >"$log" 2>&1
    status=$?
    set -e
    check_download "$log" "$status" "fbt-example download --resume ($(basename "$log"))"
}
resume_run stop --stop-at 50
fbt test -s "$resume_file" || fail "the first run wrote no resume file (see $logs/fbt-resume-stop.log)"
stopped_at=$(sed -n 's/^stop at 50%: \([0-9.]*\)% done$/\1/p' "$logs/fbt-resume-stop.log")
[ -n "$stopped_at" ] && awk -v p="$stopped_at" 'BEGIN {exit !(p >= 50 && p < 100)}' ||
    fail "the first run did not stop part-way (at '${stopped_at}'%; see $logs/fbt-resume-stop.log)"
wait_for "qBittorrent's uploaded to grow in the first run" 30 qbt_uploaded_more_than "$hash1" "$qbt_up_before"
qbt_up_mid=$(qbt_field "$hash1" .uploaded)
resume_run again
[ "$(hashes /interop/payload fixture)" = "$(hashes /interop/fbt-resume fixture)" ] ||
    fail "data downloaded in the resume scenario differs from the payload"
grep -Eq '^resume: trusted \([0-9]+ pieces\)$' "$logs/fbt-resume-again.log" ||
    fail "the second run did not trust its resume data (see $logs/fbt-resume-again.log)"
# Neither a full check (`resume: checking`) nor a progress line in the
# checking state.
! grep -Eq '^resume: checking$| checking$' "$logs/fbt-resume-again.log" ||
    fail "the second run checked its files (see $logs/fbt-resume-again.log)"
wait_for "qBittorrent's uploaded to grow in the second run" 30 qbt_uploaded_more_than "$hash1" "$qbt_up_mid"
echo "interop: resume: stopped at ${stopped_at}%, qBittorrent uploaded $qbt_up_before -> $qbt_up_mid -> $(qbt_field "$hash1" .uploaded) bytes"
trc transmission-remote -t "$hash1" -s >/dev/null
echo "interop: resume without recheck OK (qbittorrent)"
fi

if want webseed; then
# ---------------------------------------------------------------- web seed --
# A trackerless torrent whose only source is its url-list: the `webseed`
# service (nginx) serving payload6 read-only.
echo "interop: creating payload and web-seeded fixture6.torrent"
fbt test -d /interop/payload6/fixture6 || make_payload /interop/payload6/fixture6
trc transmission-create -o /interop/fixture6.torrent -w http://webseed/ -s 256 /interop/payload6/fixture6 >/dev/null
hash6=$(info_hash /interop/fixture6.torrent)
[ -n "$hash6" ] || fail "could not read the info hash of fixture6.torrent"
echo "interop: fbt-example download from the web seed alone (timeout 300s)"
webseed_log="$logs/fbt-webseed.log"
set +e
fbt timeout 300 fbt-example download /interop/fixture6.torrent --no-dht --dht-bootstrap "$dht_bootstrap" \
    --out /interop/fbt-webseed --port 6901 >"$webseed_log" 2>&1
status=$?
set -e
check_download "$webseed_log" "$status" "fbt-example download (web seed)"
[ "$(hashes /interop/payload6 fixture6)" = "$(hashes /interop/fbt-webseed fixture6)" ] ||
    fail "data downloaded from the web seed differs from the payload"
! grep -q '^peer connected ' "$webseed_log" || fail "the web seed download connected to a peer (see $webseed_log)"
dc logs --no-color --no-log-prefix webseed >"$logs/webseed-nginx.log" 2>&1
grep -Eq '"GET /fixture6/[^ ]+ HTTP/1\.1" 206 ' "$logs/webseed-nginx.log" ||
    fail "nginx served no 206 range response (see $logs/webseed-nginx.log)"
echo "interop: web seed: nginx answered $(grep -Ec '"GET /fixture6/[^ ]+ HTTP/1\.1" 206 ' "$logs/webseed-nginx.log") range requests"
echo "interop: web seed OK (nginx)"
fi

if want proxy; then
# ------------------------------------------------------------ SOCKS5 proxy --
# qBittorrent is the only source again. Every connection of ours goes through
# Dante, so qBittorrent sees the socks5 container's address, never fbt's.
echo "interop: fbt-example download through the SOCKS5 proxy (timeout 300s)"
trc transmission-remote -t "$hash1" -S >/dev/null
socks5_ip=$(container_ip socks5)
[ -n "$socks5_ip" ] || fail "could not resolve the socks5 address"
qbt_up_before=$(qbt_field "$hash1" .uploaded)
proxy_log="$logs/fbt-proxy.log"
fbt timeout 300 fbt-example download /interop/fixture.torrent --out /interop/fbt-proxy --port 6902 \
    --dht-bootstrap "$dht_bootstrap" --proxy socks5://socks5:1080 >"$proxy_log" 2>&1 &
proxy_pid=$!
# qBittorrent lists only connected peers, so sample them while it runs.
seen_socks5=false
seen_fbt=false
while kill -0 "$proxy_pid" 2>/dev/null; do
    peer_ips=$(qbt_api "sync/torrentPeers?hash=$hash1" | fbt jq -r '(.peers // {})[] | .ip' 2>/dev/null || true)
    if echo "$peer_ips" | grep -qxF "$socks5_ip"; then seen_socks5=true; fi
    if echo "$peer_ips" | grep -qxF "$fbt_ip"; then seen_fbt=true; fi
    sleep 1
done
set +e
wait "$proxy_pid"
status=$?
set -e
check_download "$proxy_log" "$status" "fbt-example download --proxy"
[ "$(hashes /interop/payload fixture)" = "$(hashes /interop/fbt-proxy fixture)" ] ||
    fail "data downloaded through the proxy differs from the payload"
[ "$seen_fbt" = false ] || fail "qBittorrent saw fbt's own address $fbt_ip: a connection bypassed the proxy"
[ "$seen_socks5" = true ] || fail "qBittorrent never listed the proxy's address $socks5_ip as a peer"
grep -q '^tracker reply http://tracker:6969/announce: ' "$proxy_log" ||
    fail "no tracker reply came through the proxy (see $proxy_log)"
wait_for "qBittorrent's uploaded to grow through the proxy" 30 qbt_uploaded_more_than "$hash1" "$qbt_up_before"
echo "interop: proxy: qBittorrent saw $socks5_ip (socks5), uploaded $qbt_up_before -> $(qbt_field "$hash1" .uploaded) bytes"
trc transmission-remote -t "$hash1" -s >/dev/null
echo "interop: SOCKS5 proxy OK (qbittorrent)"
fi

if want v2; then
# ------------------------------------------------------------- BitTorrent v2 --
# Nothing else seeds these payloads: each transfer has one source.
echo "interop: qBittorrent creates and seeds a v2 torrent"
make_v2_payload /interop/payload-v2/v2data
qbt_create /interop/payload-v2/v2data v2 /interop/qbt-v2.torrent
qbt_v2=$(qbt_id /interop/qbt-v2.torrent)
[ -n "$qbt_v2" ] || fail "could not read qBittorrent's v2 torrent"
fbt fbt-example info /interop/qbt-v2.torrent | grep -qx 'version: v2' ||
    fail "qBittorrent's v2 torrent does not read as v2"
wait_for "qBittorrent to seed its v2 torrent" 120 qbt_seeding "$qbt_v2"

echo "interop: fbt-example downloads qBittorrent's v2 torrent (timeout 300s)"
fbt_download "$logs/fbt-v2-download.log" "fbt-example download (v2)" /interop/qbt-v2.torrent \
    --out /interop/fbt-v2 --port 6903
same_data /interop/payload-v2 /interop/fbt-v2 v2data || fail "the v2 download differs from the payload"
grep -Eq "^peer connected $qbt_ip:6882 via (tcp|utp)$" "$logs/fbt-v2-download.log" ||
    fail "the v2 download never connected to qBittorrent (see $logs/fbt-v2-download.log)"

echo "interop: fbt-example create of the same payload must give qBittorrent's v2 info hash"
fbt_create "$logs/fbt-create-v2-same.log" /interop/payload-v2/v2data --out /interop/fbt-v2-same.torrent \
    --format v2 --piece-size "$v2_piece_size" --tracker "$tracker_url"
ours_v2=$(sed -n 's/^info hash v2: //p' "$logs/fbt-create-v2-same.log")
qbt_infohash_v2=$(qbt_field "$qbt_v2" .infohash_v2)
[ -n "$ours_v2" ] && [ "$ours_v2" = "$qbt_infohash_v2" ] ||
    fail "fbt-example create gave v2 info hash '$ours_v2', qBittorrent '$qbt_infohash_v2'"
echo "interop: v2 info hash matches qBittorrent's: $ours_v2"

echo "interop: fbt-example downloads a btmh-only magnet from qBittorrent (timeout 300s)"
fbt_download "$logs/fbt-v2-magnet.log" "fbt-example download (btmh magnet)" \
    "magnet:?xt=urn:btmh:1220$qbt_infohash_v2&dn=v2data&tr=$tracker_param" --out /interop/fbt-v2-magnet --port 6904
same_data /interop/payload-v2 /interop/fbt-v2-magnet v2data ||
    fail "the btmh magnet download differs from the payload"
grep -q 'metadata received: v2data' "$logs/fbt-v2-magnet.log" ||
    fail "no metadata received for the btmh magnet (see $logs/fbt-v2-magnet.log)"

# fbt's v2 seed serves two legs (qBittorrent by .torrent, then by magnet):
# up to 930 s of waits in all, so it runs for 1200 s (stop_seed ends it).
echo "interop: fbt-example seeds its own v2 torrent; qBittorrent downloads it"
make_many_payload /interop/payload-v2own/v2own
fbt_create "$logs/fbt-create-v2.log" /interop/payload-v2own/v2own --out /interop/fbt-v2.torrent \
    --format v2 --piece-size "$v2_piece_size" --tracker "$tracker_url"
fbt_v2=$(qbt_id /interop/fbt-v2.torrent)
fbt_v2_full=$(info_hash_v2 /interop/fbt-v2.torrent)
[ -n "$fbt_v2" ] && [ -n "$fbt_v2_full" ] || fail "could not read fbt-v2.torrent"
seed_exec_env=(-e "RUST_LOG=info,fbt_session::torrent::layers=debug")
seed_secs=1200
start_seed /interop/fbt-v2.torrent /interop/payload-v2own 6905 "$logs/fbt-v2-seed.log" --no-dht
seed_secs=420
seed_exec_env=()
qbt_api torrents/add -F torrents=@/interop/fbt-v2.torrent -F savepath=/interop/qbt-v2dl >/dev/null
wait_for "qBittorrent to add fbt's v2 torrent" 30 qbt_has "$fbt_v2"
qbt_api torrents/addPeers --data-urlencode "hashes=$fbt_v2" --data-urlencode "peers=$fbt_ip:6905" >/dev/null
wait_for "qBittorrent to complete fbt's v2 torrent" 300 qbt_complete "$fbt_v2"
other_client_data /interop/payload-v2own /interop/qbt-v2dl v2own qBittorrent
grep -Eq "^peer connected $qbt_ip:[0-9]+ via (tcp|utp)$" "$logs/fbt-v2-seed.log" ||
    fail "qBittorrent never connected to the v2 seed (see $logs/fbt-v2-seed.log)"

echo "interop: qBittorrent downloads fbt's v2 seed by btmh magnet"
qbt_remove "$fbt_v2"
qbt_api torrents/add --data-urlencode "urls=magnet:?xt=urn:btmh:1220$fbt_v2_full&dn=v2own&tr=$tracker_param" \
    --data-urlencode savepath=/interop/qbt-v2mag >/dev/null
wait_for "qBittorrent to add the btmh magnet" 30 qbt_has "$fbt_v2"
qbt_api torrents/addPeers --data-urlencode "hashes=$fbt_v2" --data-urlencode "peers=$fbt_ip:6905" >/dev/null
wait_for "qBittorrent to complete fbt's v2 seed by magnet" 300 qbt_complete "$fbt_v2"
other_client_data /interop/payload-v2own /interop/qbt-v2mag v2own qBittorrent
stop_seed
echo "interop: v2 seed rejected $(grep -c 'hash request over the serving caps' "$logs/fbt-v2-seed.log" || true) hash requests over its caps"
qbt_remove "$fbt_v2"
qbt_remove "$qbt_v2"
echo "interop: v2 OK (qbittorrent)"
fi

if want hybrid; then
# ------------------------------------------------------------- hybrid torrents --
echo "interop: qBittorrent creates and seeds a hybrid torrent"
make_v2_payload /interop/payload-hyb/hybdata
qbt_create /interop/payload-hyb/hybdata hybrid /interop/qbt-hybrid.torrent
qbt_hyb=$(qbt_id /interop/qbt-hybrid.torrent)
[ -n "$qbt_hyb" ] || fail "could not read qBittorrent's hybrid torrent"
fbt fbt-example info /interop/qbt-hybrid.torrent | grep -qx 'version: hybrid' ||
    fail "qBittorrent's hybrid torrent does not read as hybrid"
wait_for "qBittorrent to seed its hybrid torrent" 120 qbt_seeding "$qbt_hyb"

echo "interop: fbt-example downloads qBittorrent's hybrid torrent (timeout 300s)"
fbt_download "$logs/fbt-hybrid-download.log" "fbt-example download (hybrid)" /interop/qbt-hybrid.torrent \
    --out /interop/fbt-hybrid --port 6906
same_data /interop/payload-hyb /interop/fbt-hybrid hybdata || fail "the hybrid download differs from the payload"
grep -Eq "^peer connected $qbt_ip:6882 via (tcp|utp)$" "$logs/fbt-hybrid-download.log" ||
    fail "the hybrid download never connected to qBittorrent (see $logs/fbt-hybrid-download.log)"

# The same payload and piece size give qBittorrent's two hashes: the v1
# file list, pads included (libtorrent's `.pad/<n>`, `attr` `p`, before
# each unaligned file and after the last), and the v2 tree.
fbt_create "$logs/fbt-create-hybrid-same.log" /interop/payload-hyb/hybdata \
    --out /interop/fbt-hybrid-same.torrent --format hybrid --piece-size "$v2_piece_size" --tracker "$tracker_url"
ours_v1=$(sed -n 's/^info hash: //p' "$logs/fbt-create-hybrid-same.log")
ours_v2=$(sed -n 's/^info hash v2: //p' "$logs/fbt-create-hybrid-same.log")
theirs_v1=$(qbt_field "$qbt_hyb" .infohash_v1)
theirs_v2=$(qbt_field "$qbt_hyb" .infohash_v2)
[ -n "$ours_v1" ] && [ "$ours_v1" = "$theirs_v1" ] ||
    fail "fbt-example create gave hybrid v1 info hash '$ours_v1', qBittorrent '$theirs_v1'"
[ -n "$ours_v2" ] && [ "$ours_v2" = "$theirs_v2" ] ||
    fail "fbt-example create gave hybrid v2 info hash '$ours_v2', qBittorrent '$theirs_v2'"
echo "interop: hybrid info hashes match qBittorrent's: v1 $ours_v1, v2 $ours_v2"
qbt_remove "$qbt_hyb"

echo "interop: fbt-example seeds its own hybrid; qBittorrent and Transmission download it"
make_v2_payload /interop/payload-hybown/hybown
fbt_create "$logs/fbt-create-hybrid.log" /interop/payload-hybown/hybown --out /interop/fbt-hybrid.torrent \
    --format hybrid --piece-size "$v2_piece_size" --tracker "$tracker_url"
# qBittorrent names a hybrid by its truncated v2 hash, Transmission by v1.
fbt_hyb=$(qbt_id /interop/fbt-hybrid.torrent)
fbt_hyb_v1=$(info_hash /interop/fbt-hybrid.torrent)
[ -n "$fbt_hyb" ] && [ -n "$fbt_hyb_v1" ] || fail "could not read fbt-hybrid.torrent"
# Up to 870 s of waits below: the seed runs for 1200 s (stop_seed ends it).
seed_secs=1200
start_seed /interop/fbt-hybrid.torrent /interop/payload-hybown 6907 "$logs/fbt-hybrid-seed.log" --no-dht
seed_secs=420
mkdir -p "$INTEROP_DIR/tr-hybrid"
qbt_api torrents/add -F torrents=@/interop/fbt-hybrid.torrent -F savepath=/interop/qbt-hybrid >/dev/null
wait_for "qBittorrent to add fbt's hybrid" 30 qbt_has "$fbt_hyb"
qbt_api torrents/addPeers --data-urlencode "hashes=$fbt_hyb" --data-urlencode "peers=$fbt_ip:6907" >/dev/null
trc transmission-remote -a /interop/fbt-hybrid.torrent -w /interop/tr-hybrid >/dev/null ||
    fail "Transmission refused fbt's hybrid torrent"
wait_for "qBittorrent to complete fbt's hybrid" 300 qbt_complete "$fbt_hyb"
wait_for "Transmission to complete fbt's hybrid" 300 tr_complete "$fbt_hyb_v1"
other_client_data /interop/payload-hybown /interop/qbt-hybrid hybown qBittorrent
other_client_data /interop/payload-hybown /interop/tr-hybrid hybown Transmission
grep -Eq "^peer connected $qbt_ip:[0-9]+ via (tcp|utp)$" "$logs/fbt-hybrid-seed.log" ||
    fail "qBittorrent never connected to the hybrid seed (see $logs/fbt-hybrid-seed.log)"
grep -Eq "^peer connected $tr_ip:[0-9]+ via (tcp|utp)$" "$logs/fbt-hybrid-seed.log" ||
    fail "Transmission never connected to the hybrid seed (see $logs/fbt-hybrid-seed.log)"
stop_seed
qbt_remove "$fbt_hyb"
trc transmission-remote -t "$fbt_hyb_v1" -r >/dev/null
echo "interop: hybrid OK (qbittorrent, transmission)"
fi

if want api; then
# ------------------------------------------------------------ HTTP/JSON API --
# fbt-example download --api serves fbt-api next to its session. It is held
# to 2 MiB/s and seeds 30 s after the finish, so the API is up while curl
# (in the same container) lists the torrent, streams three byte ranges of
# beta.bin and checks 401 and 416; the event stream starts before the
# download ends and must carry its torrent_finished.
echo "interop: fbt-example download --api (port 6908, API 8090), driven over HTTP"
api_started=$SECONDS
api_token=interop-api-token
api_url=http://127.0.0.1:8090/api/v1
api_auth="Authorization: Bearer $api_token"
api_log=$logs/fbt-api-download.log
fbt timeout 300 fbt-example download /interop/fixture.torrent --out /interop/fbt-api --port 6908 \
    --no-dht --dht-bootstrap "$dht_bootstrap" --api 0.0.0.0:8090 --api-token "$api_token" \
    --download-limit 2M --seed-for 30 >"$api_log" 2>&1 &
api_pid=$!
api_listening() {
    kill -0 "$api_pid" 2>/dev/null || fail "fbt-example download --api exited early (see $api_log)"
    grep -q '^api: listening on ' "$api_log"
}
wait_for "fbt-example to serve the API" 60 api_listening
fbt curl -sN --max-time 120 -H "$api_auth" "$api_url/events" >"$logs/fbt-api-events.log" 2>&1 &
events_pid=$!
# api_get <path> [curl args...]: an authenticated GET, failing on an HTTP error.
api_get() { fbt curl -fsS --max-time 120 -H "$api_auth" "$api_url/$1" "${@:2}"; }
# api_status <path> [curl args...]: the HTTP status alone.
api_status() { fbt curl -sS --max-time 30 -o /dev/null -w '%{http_code}' "$api_url/$1" "${@:2}"; }

api_torrents=$(api_get torrents)
[ "$(echo "$api_torrents" | fbt jq length)" = 1 ] && [ "$(echo "$api_torrents" | fbt jq -r '.[0].id')" = "$hash1" ] ||
    fail "GET /torrents did not list fixture ($hash1) alone: $api_torrents"
api_beta=$(api_get "torrents/$hash1/files" | fbt jq -r '.[] | select(.path | endswith("beta.bin")) | "\(.index) \(.len)"')
read -r api_beta_index api_beta_len <<<"$api_beta"
[ -n "$api_beta_index" ] && [ "$api_beta_len" = 25000000 ] ||
    fail "GET /torrents/$hash1/files has no 25000000-byte beta.bin: '$api_beta'"
api_stream="torrents/$hash1/files/$api_beta_index/stream"
beta=/interop/payload/fixture/beta.bin
# api_range <name> <first byte> <length>: stream that range and compare it with beta.bin.
api_range() {
    local out=/interop/api-range-$1.bin code
    code=$(api_get "$api_stream" -H "Range: bytes=$2-$(($2 + $3 - 1))" -o "$out" -w '%{http_code}')
    [ "$code" = 206 ] || fail "the $1 range answered $code, not 206"
    fbt sh -c "tail -c +$(($2 + 1)) '$beta' | head -c $3 | cmp - '$out'" ||
        fail "the $1 range ($2, $3 bytes) differs from beta.bin"
}
api_range start 0 65536
api_range middle 12000000 65536
api_range end $((api_beta_len - 65536)) 65536
code=$(api_status torrents)
[ "$code" = 401 ] || fail "GET /torrents without the token answered $code, not 401"
code=$(api_status "$api_stream" -H "$api_auth" -H "Range: bytes=$api_beta_len-")
[ "$code" = 416 ] || fail "a range past the end of beta.bin answered $code, not 416"
# Still downloading once every check is done: 64 MiB at 2 MiB/s takes about
# 32 s, the checks about 7 (measured). A missing or non-numeric progress
# fails too (jq orders null below every number).
api_progress=$(api_get "torrents/$hash1" | fbt jq -c .progress)
fbt jq -en --argjson p "$api_progress" '($p | type) == "number" and $p < 1' >/dev/null ||
    fail "the download had finished (progress $api_progress) before the API checks ended (see $api_log)"
echo "interop: api: listed, streamed 3 ranges, 401 and 416 in $((SECONDS - api_started))s, at progress $api_progress"

set +e
wait "$api_pid"
status=$?
set -e
check_download "$api_log" "$status" "fbt-example download --api"
# The API's shutdown ends the event stream, so curl exits with it.
wait "$events_pid" || true
[ "$(hashes /interop/payload fixture)" = "$(hashes /interop/fbt-api fixture)" ] ||
    fail "data downloaded in the api scenario differs from the payload"
grep -q '^event: torrent_finished' "$logs/fbt-api-events.log" ||
    fail "the event stream has no torrent_finished (see $logs/fbt-api-events.log)"
echo "interop: api OK (list, ranges, 401, 416, events) in $((SECONDS - api_started))s"
fi

if [ -n "${INTEROP_ONLY:-}" ]; then
    echo "interop: selected scenarios passed ($INTEROP_ONLY) in $((SECONDS - setup_secs))s (setup ${setup_secs}s, total ${SECONDS}s)"
else
    echo "interop: all scenarios passed in $((SECONDS - setup_secs))s (setup ${setup_secs}s, total ${SECONDS}s)"
fi
