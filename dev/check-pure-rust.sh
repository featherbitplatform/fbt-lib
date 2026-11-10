#!/usr/bin/env bash
# The pure-rust build must contain no C or assembly: no `ring`, no `cc`.
set -euo pipefail
cd "$(dirname "$0")/.."

tree=$(cargo tree -e normal -p fbt --no-default-features --features pure-rust --prefix none)
if echo "$tree" | grep -E '^(ring|cc) '; then
    echo "error: pure-rust dependency tree contains ring or cc" >&2
    exit 1
fi
echo "pure-rust tree is free of ring and cc"
cargo test -p fbt-tracker --features tls-rustcrypto
