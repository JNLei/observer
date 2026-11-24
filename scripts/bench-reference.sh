#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
if [ "$(uname -s)" != Linux ]; then
    echo "This profile requires a Linux cgroup-v2 host or VM with systemd and sudo." >&2
    exit 1
fi
export CARGO_TARGET_DIR="$ROOT/target"
bin="$ROOT/benchmarks/ingest/bin"
mkdir -p "$bin" benchmarks/ingest/results
if [ ! -x "$bin/loki" ] || [ ! -x "$bin/otelcol-contrib" ]; then
    python3 benchmarks/ingest/otlp/fetch_tools.py
fi
cargo build --release --locked -p otlp-bench-rust
cp "$CARGO_TARGET_DIR/release/otlp-bench-rust" "$bin/otlp-bench-rust"
go -C benchmarks/ingest/otlp/go build -mod=readonly -buildvcs=false -o "$bin/otlp-bench-go" .
python3 - "$bin/reference-build.json" <<'PY'
import json
from pathlib import Path
import subprocess
import sys
def version(*command):
    return subprocess.check_output(command, text=True).strip()
Path(sys.argv[1]).write_text(json.dumps({"rustc": version("rustc", "-vV"), "go": version("go", "-C", "benchmarks/ingest/otlp/go", "version"),
    "rust": "cargo build --release --locked", "go_flags": "-mod=readonly -buildvcs=false", "python": sys.version}, indent=2) + "\n")
PY
folder=$(mktemp -d "$ROOT/benchmarks/ingest/results/reference.XXXXXX")
echo "Raw evidence in $folder/run"
python3 benchmarks/ingest/otlp/reference.py --output "$folder/run" "$@"
python3 benchmarks/ingest/otlp/verify_reference.py "$folder/run"
