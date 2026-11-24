#!/usr/bin/env python3
"""Repeat paired 16-worker Linux trials. A missing kernel control fails the trial."""
import argparse
import json
import os
from pathlib import Path
import platform
import random
import subprocess
import sys
import tempfile
import uuid

from recovery import BIN, HERE, ROOT, digest, require
from reference_load import PHASES, QUICK_PHASES


def order():
    rng = random.Random(1)
    firsts = ["rust", "go", "rust", "go", rng.choice(["rust", "go"])]
    rng.shuffle(firsts)
    return [[name, "rust" if name == "go" else "go"] for name in firsts]


def stdout(*cmd):
    return subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="new evidence directory")
    parser.add_argument("--pair", type=int, choices=range(1, 6), help="run one of the five predeclared pairs")
    parser.add_argument("--quick", action="store_true", help="short engineering check")
    parser.add_argument("--bin-dir", type=Path, default=BIN)
    args = parser.parse_args()
    require(platform.system() == "Linux" and os.getuid() != 0, "use a non-root account on a cgroup-v2 Linux host with sudo")
    folder, binaries = args.output.resolve(), args.bin_dir.resolve()
    folder.mkdir(parents=True, exist_ok=False)
    versions = {"collector": stdout(binaries/"otelcol-contrib", "--version"), "loki": stdout(binaries/"loki", "-version")}
    require(versions["collector"].endswith(" 0.165.0") and "version 3.7.10 " in versions["loki"], "tool version mismatch")
    manifest = {"schema": 1, "profile": "quick" if args.quick else "reference", "phases": QUICK_PHASES if args.quick else PHASES,
                "seed": 1, "order": order(), "git_head": stdout("git", "-C", ROOT, "rev-parse", "HEAD"),
                "git_status": stdout("git", "-C", ROOT, "status", "--porcelain"), "versions": versions,
                "sha256": {p.name: digest(p) for p in (binaries/"loki", binaries/"otelcol-contrib", binaries/"otlp-bench-rust",
                          binaries/"otlp-bench-go", HERE/"collector.yaml", HERE/"loki.yaml")},
                "build": json.loads((binaries/"reference-build.json").read_text()), "pairs": {}}
    failed = False
    for i in [args.pair] if args.pair else range(1, 6):
        results = manifest["pairs"][str(i)] = {}
        for variant in order()[i-1]:
            out = folder/f"pair-{i}"/variant
            out.mkdir(parents=True)
            manifest_path = folder/"manifest.json"
            results[variant] = "running"
            manifest_path.write_text(json.dumps(manifest, indent=2)+"\n")
            print(f"Pair {i}, {variant}, {manifest['profile']} workload", flush=True)
            with tempfile.TemporaryDirectory(prefix="m1-ref-", dir="/tmp") as scratch:
                command = ["sudo", "-n", "systemd-run", "--quiet", "--scope", "-p", "Delegate=yes",
                           "--unit=m1-logs-"+uuid.uuid4().hex, "unshare", "--net", "--mount", "--propagation", "private",
                           "/usr/bin/python3", HERE/"linux_budget.py", scratch, out/"controls.json", str(os.getuid()),
                           str(os.getgid()), "/usr/bin/python3", HERE/"reference_trial.py", scratch, out,
                           binaries/("otlp-bench-"+variant), binaries, manifest["profile"]]
                status = subprocess.run(command).returncode
            results[variant] = "failed" if status else "passed"
            manifest_path.write_text(json.dumps(manifest, indent=2)+"\n")
            failed |= bool(status)
    paths = sorted(folder.rglob("*"))
    with (folder/"SHA256SUMS").open("x") as f:
        for path in paths:
            if path.is_file():
                print(digest(path), " ", path.relative_to(folder), sep="", file=f)
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
