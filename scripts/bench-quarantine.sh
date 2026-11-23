#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
export CARGO_TARGET_DIR="$ROOT/target"
mkdir -p benchmarks/ingest/bin benchmarks/ingest/results
if [ ! -x benchmarks/ingest/bin/loki ]; then
    python3 benchmarks/ingest/otlp/fetch_tools.py
fi
cargo build --release --locked -p otlp-bench-rust
go -C benchmarks/ingest/otlp/go build -mod=readonly -buildvcs=false -o ../../bin/otlp-bench-go .
result_dir=$(mktemp -d "$ROOT/benchmarks/ingest/results/quarantine.XXXXXX")
printf 'Quarantine fault evidence in %s/result.json\n' "$result_dir"
python3 benchmarks/ingest/otlp/quarantine.py --output "$result_dir/result.json"
