#!/usr/bin/env bash
# The pure-rust build must contain no C or assembly: no `ring`, no `cc`, no
# other C-building crate (`cmake`, `aws-lc*`, `*-sys`), through normal or
# build-dependency edges, on any target.
set -euo pipefail
cd "$(dirname "$0")/.."

for pkg in fbt fbt-api; do
    tree=$(cargo tree -e normal,build -p "$pkg" --no-default-features --features pure-rust \
        --target all --prefix none)
    # windows-sys only declares bindings to system DLLs; it compiles no C.
    if echo "$tree" | grep -v '^windows-sys ' \
        | grep -E '^(ring|cc|cmake|aws-lc[a-z-]*|openssl-sys|[a-z0-9_-]+-sys) '; then
        echo "error: $pkg pure-rust dependency tree contains a C-building crate" >&2
        exit 1
    fi
    echo "$pkg pure-rust tree is free of ring, cc and other C-building crates"
done

# The core crates stay server-free: only fbt-api pulls in the HTTP server
# stack, and no core crate turns on hyper's `server` feature.
core=$(cargo tree -p fbt -e normal --prefix none)
if echo "$core" | grep -E '^(axum|tower-http|utoipa) '; then
    echo "error: fbt depends on the HTTP server stack" >&2
    exit 1
fi
# Captured first: inside `if`, a failing `cargo tree` would read as "no
# match" and pass; as an assignment, `set -e` stops the script instead.
hyper_features=$(cargo tree -p fbt -e features -i hyper)
if echo "$hyper_features" | grep -F 'hyper feature "server"'; then
    echo "error: fbt turns on hyper's server feature" >&2
    exit 1
fi
echo "fbt is server-free"
cargo test -p fbt-tracker --features tls-rustcrypto
