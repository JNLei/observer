#!/usr/bin/env python3
"""Install checksum-pinned local OSS storage and Collector binaries."""

import argparse
import hashlib
from pathlib import Path
import platform
import shutil
import tarfile
import tempfile
import urllib.request
import zipfile

BIN = Path(__file__).resolve().parents[1] / "bin"
# SHA-256 values from the projects' release asset manifests, in Loki/Collector order.
ARCHIVES = {
    ("darwin", "arm64"): ("e991ddbd16ad7d85c00372ad28529ea30c2a09b71d5d23ef02a018c0a6865059",
                          "f9a0ed310e9beeb0fabca9d9c24bd0d3467f6339e3207710dbd66a6adf1298f6"),
    ("darwin", "amd64"): ("cfd44c7d93cd7eefa85c6eadc6f9e6cc56418958fdcfb95cbb38ec035a942c30",
                          "7d14e5c14bcb930d55f2bd5d3fbb3734be5fd2889698488e65e488efda5473c9"),
    ("linux", "arm64"): ("13c3f926c8bb020b225b2802fa8e2d4e969186d6d2a8fd15e26f323a71beff83",
                         "376420729c862230807bc89811d07d10031eaeca3f6481fe371a2bdcefd2c8fa"),
    ("linux", "amd64"): ("200e74f7e631969bd0f2a066a2e1d33cdea9b3c0801aaaa7d78947c4ac29bddb",
                         "bd6e73d94bf3359ad6fac25529a8ee308adb9b0b8a0e8af034ed3891fbe5aa33"),
}


TRACE_ARCHIVES = {
    ("darwin", "arm64"): "b0553e27fa77d979ab14032a3463dab5464776883cfb0bdc61fd071db93b8650",
    ("darwin", "amd64"): "2847b3a8bafdd6755784fa1d3d5b5e08372587da08bce7098d882670f30b265e",
    ("linux", "arm64"): "210bc863666865ba9a4ff0a19901318042da0c474d248fec69ca71c07b1e5f7a",
    ("linux", "amd64"): "789e006d076f3c8b99de7bb1d0277024a7db776db0294d1c27fcc5422d98c4bc",
}

def install(name, url, member, expected):
    with tempfile.TemporaryDirectory(dir=BIN) as d:
        archive = Path(d) / "archive"
        sha = hashlib.sha256()
        with urllib.request.urlopen(url, timeout=60) as response, archive.open("wb") as output:
            for chunk in iter(lambda: response.read(1 << 20), b""):
                sha.update(chunk)
                output.write(chunk)
        if sha.hexdigest() != expected:
            raise ValueError(f"{name} release archive digest mismatch")
        binary = Path(d) / name
        with (zipfile.ZipFile(archive) if name == "loki" else tarfile.open(archive)) as bundle:
            source = bundle.open(member) if name == "loki" else bundle.extractfile(member)
            with source, binary.open("wb") as output:
                shutil.copyfileobj(source, output)
        binary.chmod(0o755)
        binary.replace(BIN / name)
        print(f"Installed {name} from verified archive {expected}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--traces-only", action="store_true", help="install the VictoriaTraces candidate")
    group.add_argument("--loki-only", action="store_true", help="install just the pinned Loki candidate")
    args = parser.parse_args()
    system = platform.system().lower()
    machine = {"x86_64": "amd64", "aarch64": "arm64"}.get(platform.machine(), platform.machine())
    if (system, machine) not in ARCHIVES:
        raise SystemExit("Supported trial hosts are macOS and Linux on amd64 and arm64")
    loki, collector = ARCHIVES[system, machine]
    BIN.mkdir(parents=True, exist_ok=True)
    base = "https://github.com"
    if args.traces_only:
        asset = f"victoria-traces-{system}-{machine}-v0.13.0.tar.gz"
        install("victoria-traces", f"{base}/VictoriaMetrics/VictoriaTraces/releases/download/v0.13.0/{asset}",
                "victoria-traces-prod", TRACE_ARCHIVES[system, machine])
        raise SystemExit(0)
    member = f"loki-{system}-{machine}"
    install("loki", f"{base}/grafana/loki/releases/download/v3.7.10/{member}.zip", member, loki)
    if args.loki_only:
        raise SystemExit(0)
    asset = f"otelcol-contrib_0.165.0_{system}_{machine}.tar.gz"
    install("otelcol-contrib", f"{base}/open-telemetry/opentelemetry-collector-releases/releases/download/v0.165.0/{asset}",
            "otelcol-contrib", collector)
