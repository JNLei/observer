"""Run the private bbolt custody fault matrix and two independent real-Loki trials."""

import argparse
import contextlib
import json
import os
from pathlib import Path
import platform
import secrets
import shutil
import subprocess
import tempfile
import time

from recovery import BIN, HERE, ROOT, Child, digest, free_ports, http, require, wait_for

MOD = HERE / "go"
PACKAGE = "./quarantine"


def test_binary(directory):
    """Instrument pinned bbolt's published failpoint locations in a private test build."""
    module = json.loads(subprocess.check_output(
        ["go", "list", "-mod=readonly", "-m", "-json", "go.etcd.io/bbolt"], cwd=MOD))
    require(module["Version"] == "v1.5.0", "unreviewed bbolt version")
    source = Path(module["Dir"]) / "tx.go"
    code = source.read_text()
    require(code.count('import (') == 1, "unrecognized bbolt imports")
    code = code.replace('import (', 'import (\n "syscall"')
    for marker, name in (("beforeSyncDataPages", "syncDataEIO"),
                         ("beforeSyncMetaPage", "syncMetaEIO"),
                         ("beforeWriteMetaError", "writeMetaEIO")):
        point = "// gofail: var " + marker + (" string" if name == "writeMetaEIO" else " struct{}")
        require(code.count(point) == 1, "missing pinned bbolt fault point")
        injection = (f'if os.Getenv("QUARANTINE_BOLT_POINT") == "{marker}" {{ '
                     'if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil { panic(err) }; select {} }\n'
                     f'if os.Getenv("QUARANTINE_BOLT_POINT") == "{name}" {{ return syscall.EIO }}\n')
        code = code.replace(point, injection + point)
    # Go forbids overlays into the module cache. Build against a private
    # instrumented copy using a temporary modfile and the original checksums.
    local = directory / "bbolt"
    shutil.copytree(source.parent, local)
    replacement = local / "tx.go"
    replacement.chmod(0o600)
    replacement.write_text(code)
    modfile = directory / "fault.mod"
    shutil.copyfile(MOD / "go.mod", modfile)
    shutil.copyfile(MOD / "go.sum", directory / "fault.sum")
    subprocess.run(["go", "mod", "edit", "-modfile", modfile, "-replace=go.etcd.io/bbolt=" + str(local)],
                   cwd=MOD, check=True)
    binary = directory / "owner.test"
    subprocess.run(["go", "test", "-c", "-race", "-mod=readonly", "-modfile", modfile,
                    "-o", binary, PACKAGE], cwd=MOD, check=True)
    return binary, {"bbolt": module["Version"], "module_sum": module["Sum"],
                    "tx_sha256": digest(source), "instrumented_tx_sha256": digest(replacement),
                    "instrumented_test_binary_sha256": digest(binary)}


def run_test(binary, pattern, env):
    run = subprocess.run([binary, "-test.v", "-test.timeout=150s", "-test.run=" + pattern],
                         env=env, cwd=MOD, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    print(run.stdout, end="", flush=True)
    return {"exit_code": run.returncode, "transcript": run.stdout.splitlines()}


def downstream(binary, server, epoch, env, trial):
    with tempfile.TemporaryDirectory(prefix="dq-", dir="/tmp") as temp, contextlib.ExitStack() as cleanup:
        directory = Path(temp)
        grpc, port = free_ports(2)
        sock = directory / "loki-http:0"
        base = {k: env[k] for k in ("PATH", "HOME", "TMPDIR", "LANG", "GOMAXPROCS") if k in env}
        loki = Child("loki", [BIN / "loki", "-config.expand-env=true", "-config.file=" + str(HERE / "loki.yaml")],
                     {**base, "TRIAL_DIR": temp, "LOKI_GRPC_PORT": str(grpc)}, directory, trial["process_usage"], cleanup)
        wait_for(lambda: loki.alive() and http(sock, "/ready")[0] == 200, "Loki readiness", 75)
        tokens = {"OBS_BENCH_TOKEN_A": secrets.token_hex(32), "OBS_BENCH_TOKEN_B": secrets.token_hex(32)}
        addr = f"127.0.0.1:{port}"
        gateway = Child("receiver", [server], {**base, **tokens, "OBS_BENCH_LOKI_SOCKET": str(sock),
                        "OBS_BENCH_LISTEN": addr}, directory, trial["process_usage"], cleanup)
        wait_for(lambda: gateway.alive() and http(port, "/bench/v1/counts", token=tokens["OBS_BENCH_TOKEN_A"])[0] == 200,
                 "receiver readiness")
        report = directory / "observations.json"
        trial["test"] = run_test(binary, "^TestLiveReceiver$", {**env, **tokens, "QUARANTINE_RECEIVER": "http://" + addr,
                                 "QUARANTINE_LOKI_SOCKET": str(sock), "QUARANTINE_EPOCH": str(epoch),
                                 "QUARANTINE_REPORT": str(report)})
        require(trial["test"]["exit_code"] == 0 and report.is_file(), "real-Loki assertions failed")
        trial["observations"] = json.loads(report.read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    epoch = time.time_ns() // 60_000_000_000 * 60_000_000_000
    env = {**os.environ, "QUARANTINE_INSTRUMENTED": "1", "GOMAXPROCS": "2"}
    for key in ("QUARANTINE_BOLT_POINT", "QUARANTINE_CHILD", "QUARANTINE_RECEIVER"):
        env.pop(key, None)
    version = subprocess.check_output([BIN / "loki", "-version"], text=True, stderr=subprocess.STDOUT)
    require("version 3.7.10 " in version, "unexpected Loki version")
    result = {"epoch_ns": epoch, "host": platform.platform(), "logical_cpus": os.cpu_count(), "gomaxprocs": 2,
              "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "git_status": subprocess.check_output(["git", "status", "--short"], cwd=ROOT, text=True),
              "go_version": subprocess.check_output(["go", "version"], cwd=MOD, text=True).strip(),
              "loki_version": version.strip(), "loki_sha256": digest(BIN / "loki"),
              "loki_config_sha256": digest(HERE / "loki.yaml"), "receivers": {}}
    with tempfile.TemporaryDirectory(prefix="custody-build-") as temp:
        binary, result["instrumentation"] = test_binary(Path(temp))
        cuts = Path(temp) / "cuts"
        cuts.mkdir(mode=0o700)
        result["queue_owner"] = run_test(binary, "^TestScenario", {**env, "QUARANTINE_CUT_EVIDENCE": str(cuts)})
        result["queue_owner"]["restart_inventories"] = [json.loads(p.read_text()) for p in sorted(cuts.glob("*.json"))]
        failed = result["queue_owner"]["exit_code"] != 0 or len(result["queue_owner"]["restart_inventories"]) != 7
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        for name, server in (("rust", ROOT / "target/release/otlp-bench-rust"), ("go", BIN / "otlp-bench-go")):
            trial = result["receivers"][name] = {"binary_sha256": digest(server), "process_usage": []}
            try:
                downstream(binary, server, epoch, env, trial)
                trial["result"] = "passed"
            except (OSError, RuntimeError, subprocess.SubprocessError) as err:
                trial.update(result="failed", error=str(err))
                failed = True
            print(name, trial["result"], trial.get("error", ""), flush=True)
            args.output.write_text(json.dumps(result, indent=2) + "\n")
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
