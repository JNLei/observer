#!/usr/bin/env python3
"""Run the single-node trace store with its sole listener inside the private state directory."""
import os
from pathlib import Path
import stat
import sys

ROOT = Path(__file__).resolve().parents[1]

if len(sys.argv) != 2:
    raise SystemExit("usage: run-local-traces.py ABSOLUTE_STATE_DIR")
state = Path(sys.argv[1])
info = state.lstat()
if not state.is_absolute() or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
    raise SystemExit("state must be a private directory owned by this Unix user")
binary = Path(os.environ.get("OBSERVE_TRACES_BINARY", ROOT / "benchmarks/ingest/bin/victoria-traces")).resolve()
if not binary.is_file():
    raise SystemExit("install VictoriaTraces with python3 benchmarks/ingest/otlp/fetch_tools.py --traces-only")
retention_days = int(os.environ.get("OBSERVE_TRACE_RETENTION_DAYS", "7"))
if retention_days < 3:
    raise SystemExit("retention must cover the 48-hour trace write window with at least three days")
os.umask(0o077)
os.chdir(state)
os.execv(binary, [str(binary), "-httpListenAddr=unix:traces.sock", "-storageDataPath=trace-data",
                  f"-retentionPeriod={retention_days}d",
                  "-search.maxConcurrentRequests=4", "-search.maxQueryDuration=2s",
                  "-memory.allowedBytes=128MiB", "-loggerLevel=WARN"])
