#!/usr/bin/env python3
"""Launch the local Loki candidate with private HTTP and mutually authenticated gRPC."""
import fcntl
import os
from pathlib import Path
import stat
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: run-local-loki.py ABSOLUTE_STATE_DIR")
    state = Path(sys.argv[1])
    info = state.lstat()
    if not state.is_absolute() or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise SystemExit("state must be a private directory owned by this Unix user")
    binary = Path(os.environ.get("OBSERVE_LOKI_BINARY", ROOT / "benchmarks/ingest/bin/loki")).resolve()
    if not binary.is_file():
        raise SystemExit("install Loki with python3 benchmarks/ingest/otlp/fetch_tools.py --loki-only")
    os.umask(0o077)
    os.chdir(state)
    lock = os.open("loki.lock", os.O_CREAT | os.O_WRONLY, 0o600)
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    os.set_inheritable(lock, True)
    if Path("loki.sock:0").is_socket():
        Path("loki.sock:0").unlink()
    # All internal gRPC callers in this single-node deployment share one local principal.
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "30",
                    "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
                    "-addext", "extendedKeyUsage=serverAuth,clientAuth", "-keyout", "loki.key",
                    "-out", "loki.crt"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    port = int(os.environ.get("OBSERVE_LOKI_GRPC_PORT", "9096"))
    if not 0 < port < 65536:
        raise SystemExit("invalid gRPC port")
    os.environ["OBSERVE_LOKI_GRPC_PORT"] = str(port)
    os.environ.setdefault("OBSERVE_LOKI_RETENTION", "168h")
    os.execv(binary, [str(binary), f"-config.file={ROOT}/config/headless-loki.yaml", "-config.expand-env=true"])


if __name__ == "__main__":
    main()
