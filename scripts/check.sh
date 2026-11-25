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
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
cargo build --locked --workspace
python3 backend/headless/tests/test_headless.py -v AuthorityTests ControlledStore
python3 backend/headless/tests/test_traces.py -v TraceFailures

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
(
    cd benchmarks/ingest/otlp/go
    unformatted=$(gofmt -l .)
    if [ -n "$unformatted" ]; then
        printf 'Go files require gofmt:\n%s\n' "$unformatted" >&2
        exit 1
    fi
    go vet -mod=readonly ./...
    go build -mod=readonly -buildvcs=false -o ../../bin/otlp-bench-go .
    go test -mod=readonly -count=1 -v ./...
)
python3 -m py_compile benchmarks/ingest/otlp/*.py
python3 -m py_compile benchmarks/ingest/harness.py benchmarks/ingest/tests/test_conformance.py
python3 -m unittest discover -s benchmarks/ingest/tests -v
