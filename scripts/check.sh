#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
export CARGO_TARGET_DIR="$ROOT/target"
mkdir -p benchmarks/ingest/bin
for script in scripts/*.sh; do
    sh -n "$script"
done

cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace

(
    cd benchmarks/ingest/go
    unformatted=$(gofmt -l .)
    if [ -n "$unformatted" ]; then
        printf 'Go files require gofmt:\n%s\n' "$unformatted" >&2
        exit 1
    fi
    go vet ./...
    go test ./...
    go build -buildvcs=false -o ../bin/ingest-bench-go .
)
python3 -m py_compile benchmarks/ingest/harness.py benchmarks/ingest/tests/test_conformance.py
python3 -m unittest discover -s benchmarks/ingest/tests -v
