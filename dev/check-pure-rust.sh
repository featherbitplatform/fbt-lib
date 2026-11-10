#!/usr/bin/env bash
# The pure-rust build must contain no C or assembly: no `ring`, no `cc`, no
# other C-building crate (`cmake`, `aws-lc*`, `*-sys`), through normal or
# build-dependency edges, on any target.
set -euo pipefail
cd "$(dirname "$0")/.."

tree=$(cargo tree -e normal,build -p fbt --no-default-features --features pure-rust \
    --target all --prefix none)
# windows-sys only declares bindings to system DLLs; it compiles no C.
if echo "$tree" | grep -v '^windows-sys ' \
    | grep -E '^(ring|cc|cmake|aws-lc[a-z-]*|openssl-sys|[a-z0-9_-]+-sys) '; then
    echo "error: pure-rust dependency tree contains a C-building crate" >&2
    exit 1
fi
echo "pure-rust tree is free of ring, cc and other C-building crates"
cargo test -p fbt-tracker --features tls-rustcrypto
