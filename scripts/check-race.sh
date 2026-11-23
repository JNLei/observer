#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
export CARGO_TARGET_DIR="$ROOT/target"
cargo build --workspace --locked
mkdir -p benchmarks/ingest/bin benchmarks/ingest/results
report_dir=$(mktemp -d "$ROOT/benchmarks/ingest/results/race.XXXXXX")
export GORACE="halt_on_error=1 log_path=$report_dir/report"
export OBSERVE_BENCH_GO="$ROOT/benchmarks/ingest/bin/ingest-bench-go-race"
export OBS_OTLP_GO="$ROOT/benchmarks/ingest/bin/otlp-bench-go-race"
go -C benchmarks/ingest/go build -race -buildvcs=false -o "$OBSERVE_BENCH_GO" .
go -C benchmarks/ingest/otlp/go build -race -mod=readonly -buildvcs=false -o "$OBS_OTLP_GO" .
go -C benchmarks/ingest/otlp/go test -race -mod=readonly -count=1 -v ./...
python3 -m unittest discover -s benchmarks/ingest/tests -v
for report in "$report_dir"/report.*; do
    if [ -e "$report" ]; then
        printf 'Go race report saved at %s\n' "$report" >&2
        exit 1
    fi
done
