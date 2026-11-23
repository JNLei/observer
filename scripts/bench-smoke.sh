#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
export CARGO_TARGET_DIR="$ROOT/target"
mkdir -p benchmarks/ingest/bin benchmarks/ingest/results
result_dir=$(mktemp -d "$ROOT/benchmarks/ingest/results/smoke.XXXXXX")

cargo build --release -p ingest-bench-rust
(
    cd benchmarks/ingest/go
    go build -buildvcs=false -o ../bin/ingest-bench-go .
)
{
    printf 'Functional smoke only; not a scored comparison.\n\nRust compiler:\n'
    rustc --version --verbose
    printf '\nCargo:\n'
    cargo --version
    printf '\nGo compiler:\n'
    go version
    printf '\nHost:\n'
    uname -a
    printf '\nBuild: cargo release profile; go build defaults, -buildvcs=false.\n'
    printf 'GOMAXPROCS=%s\n' "${GOMAXPROCS:-not set}"
    if git -C "$ROOT" rev-parse --verify HEAD >/dev/null 2>&1; then
        printf '\nGit commit:\n'
        git -C "$ROOT" rev-parse HEAD
        printf '\nWorktree status:\n'
        git -C "$ROOT" status --short
    else
        printf '\nGit revision unavailable.\n'
    fi
} > "$result_dir/metadata.txt"
python3 benchmarks/ingest/harness.py --server "$ROOT/target/release/ingest-bench-rust" \
    --scenario benchmarks/ingest/scenarios/smoke.json --output "$result_dir/rust.json"
python3 benchmarks/ingest/harness.py --server "$ROOT/benchmarks/ingest/bin/ingest-bench-go" \
    --scenario benchmarks/ingest/scenarios/smoke.json --output "$result_dir/go.json"
printf 'Functional smoke results: %s\n' "$result_dir"
