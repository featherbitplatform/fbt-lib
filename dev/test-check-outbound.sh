#!/usr/bin/env bash
# test-check-outbound.sh: self-test for check-outbound.sh.
#
# Runs the gate on fixture trees in a temp dir and checks that it reports
# what it should report and passes what it should pass:
#   1. an unrecorded dial is reported, with file:line
#   2. an empty `// audit:` reason does not pass
#   3. an out-of-line `#[cfg(test)] mod tests;` does not hide the code below
#   4. a file with a record, real reasons and an inline test module passes
#   5. a crate named in CHECK_OUTBOUND_SKIP is not scanned
#
# Prints `test-check-outbound: ok` and exits 0, or the failed case and exits 1.
set -u

here="$(cd "$(dirname "$0")" && pwd)"
gate="$here/check-outbound.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

fail=0
pass() { echo "test-check-outbound: case $1: ok"; }
failcase() { echo "test-check-outbound: case $1 FAILED: $2" >&2; fail=1; }

reset() {
    rm -rf "$tmp/crates"
    mkdir -p "$tmp/crates"
}

# Case 1: an unrecorded dial is reported.
reset
mkdir -p "$tmp/crates/t1/src"
cat > "$tmp/crates/t1/src/a.rs" <<'EOF'
use tokio::net::TcpStream;

async fn dial() {
    TcpStream::connect("192.0.2.1:80").await;
}
EOF
out="$(CHECK_OUTBOUND_SKIP= bash "$gate" "$tmp/crates" 2>&1)"; rc=$?
if [ "$rc" -eq 0 ]; then
    failcase 1 "expected failure, got ok"
elif ! printf '%s\n' "$out" | grep -q 't1/src/a.rs:4'; then
    failcase 1 "expected t1/src/a.rs:4, got: $out"
else
    pass 1
fi

# Case 2: an empty `// audit:` reason does not pass.
reset
mkdir -p "$tmp/crates/t2/src"
cat > "$tmp/crates/t2/src/b.rs" <<'EOF'
async fn dial() {
    // audit:
    TcpStream::connect("192.0.2.1:80").await;
}
EOF
out="$(CHECK_OUTBOUND_SKIP= bash "$gate" "$tmp/crates" 2>&1)"; rc=$?
if [ "$rc" -eq 0 ]; then
    failcase 2 "an empty reason must not pass"
elif ! printf '%s\n' "$out" | grep -q 't2/src/b.rs:3'; then
    failcase 2 "expected t2/src/b.rs:3, got: $out"
else
    pass 2
fi

# Case 3: an out-of-line test module does not hide the code below it.
reset
mkdir -p "$tmp/crates/t3/src"
cat > "$tmp/crates/t3/src/c.rs" <<'EOF'
#[cfg(test)]
mod tests;

async fn dial() {
    TcpStream::connect("192.0.2.1:80").await;
}
EOF
out="$(CHECK_OUTBOUND_SKIP= bash "$gate" "$tmp/crates" 2>&1)"; rc=$?
if [ "$rc" -eq 0 ]; then
    failcase 3 "an out-of-line mod tests; must not hide code below"
elif ! printf '%s\n' "$out" | grep -q 't3/src/c.rs:5'; then
    failcase 3 "expected t3/src/c.rs:5, got: $out"
else
    pass 3
fi

# Case 4: a record, real reasons and an inline test module all pass.
reset
mkdir -p "$tmp/crates/t4/src"
cat > "$tmp/crates/t4/src/d.rs" <<'EOF'
async fn dial() {
    audit::record(Outbound::TcpConnect(addr));
    TcpStream::connect("192.0.2.1:80").await;
    // audit: a UDP connect only pins the destination; no packet is sent
    sock.connect(addr).await;
}

#[cfg(test)]
mod tests {
    async fn hidden() {
        TcpStream::connect("192.0.2.1:80").await;
    }
}
EOF
out="$(CHECK_OUTBOUND_SKIP= bash "$gate" "$tmp/crates" 2>&1)"; rc=$?
if [ "$rc" -ne 0 ]; then
    failcase 4 "expected ok, got: $out"
else
    pass 4
fi

# Case 5: a skipped crate is not scanned.
reset
mkdir -p "$tmp/crates/t5/src"
cat > "$tmp/crates/t5/src/e.rs" <<'EOF'
async fn dial() {
    TcpStream::connect("192.0.2.1:80").await;
}
EOF
out="$(CHECK_OUTBOUND_SKIP=t5 bash "$gate" "$tmp/crates" 2>&1)"; rc=$?
if [ "$rc" -ne 0 ]; then
    failcase 5 "a skipped crate must not be scanned: $out"
else
    pass 5
fi

if [ "$fail" -ne 0 ]; then
    echo "test-check-outbound: FAILED" >&2
    exit 1
fi
echo "test-check-outbound: ok"
