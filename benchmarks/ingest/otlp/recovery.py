#!/usr/bin/env python3
"""Matched native Collector/Loki log-recovery trial. All inputs are generated."""

import argparse
import collections
import concurrent.futures
import contextlib
import hashlib
from http import client as http_client
import json
import math
import os
from pathlib import Path
import platform
import re
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
BIN = HERE.parent / "bin"
HOUR = 3_600_000_000_000


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


class UnixHTTP(http_client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=3)
        self.path = str(path)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


def http(endpoint, path, *, token=None, payload=None):
    conn = UnixHTTP(endpoint) if isinstance(endpoint, Path) else http_client.HTTPConnection(
        "127.0.0.1", endpoint, timeout=4)
    headers = {"Authorization": "Bearer " + token} if token else {}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    try:
        conn.request("GET" if payload is None else "POST", path, payload, headers)
        reply = conn.getresponse()
        data = reply.read(2_097_153)
        require(len(data) <= 2_097_152, "probe response exceeded cap")
        return reply.status, data
    finally:
        conn.close()


def wait_for(fn, description, seconds=40):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result:
                return result
        except (OSError, http_client.HTTPException):
            pass
        time.sleep(.05)
    raise RuntimeError("timed out waiting for " + description)


class Child:
    def __init__(self, name, cmd, env, directory, usage, cleanup):
        self.name, self.usage = name, usage
        self.log = directory / (name + ".log")
        with self.log.open("ab") as output:
            self.proc = subprocess.Popen(cmd, cwd=directory, env=env, stdout=output, stderr=output)
        cleanup.callback(self.stop)

    def alive(self):
        if self.proc.poll() is not None:
            with self.log.open("rb") as stream:
                stream.seek(max(0, self.log.stat().st_size - 1800))
                tail = re.sub(r"[0-9a-fA-F]{64,}", "[redacted]", stream.read().decode(errors="replace"))
            raise RuntimeError(f"{self.name} exited {self.proc.returncode}: {tail}")
        return True

    def stop(self):
        if self.proc.returncode is not None:
            return
        self.proc.kill()
        _, status, usage = os.wait4(self.proc.pid, 0)
        self.proc.returncode = os.waitstatus_to_exitcode(status)
        self.usage.append(dict(process=self.name, exit_code=self.proc.returncode,
                               user_cpu_s=usage.ru_utime, system_cpu_s=usage.ru_stime,
                               peak_rss_bytes=usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024),
                               input_blocks=usage.ru_inblock, output_blocks=usage.ru_oublock))
        require(self.proc.returncode == -signal.SIGKILL, self.name + " did not reach the injected kill")


def free_ports(count):
    with contextlib.ExitStack() as stack:
        socks = [stack.enter_context(socket.socket()) for _ in range(count)]
        for sock in socks:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in socks]


def record(seq, event_ns):
    return {"timeUnixNano": str(event_ns), "observedTimeUnixNano": str(event_ns + 11),
            "severityNumber": 9, "traceId": f"{seq:032x}", "spanId": f"{seq:016x}",
            "body": {"stringValue": "x" * (32, 256, 1024)[(seq - 1) % 3]},
            "attributes": [{"key": "bench.seq", "value": {"intValue": str(seq)}}]}


def export(port, token, records, journal):
    body = json.dumps({"resourceLogs": [{"scopeLogs": [{"logRecords": records}]}]},
                      separators=(",", ":")).encode()
    item = {"records": len(records), "first_seq": int(records[0]["attributes"][0]["value"]["intValue"]),
            "json_bytes": len(body), "outcome": "unknown"}
    journal.append(item)
    status, data = http(port, "/v1/logs", token=token, payload=body)
    item["http_status"] = status
    if status == 200 and not json.loads(data or b"{}").get("partialSuccess"):
        item["outcome"] = "source_enqueue_ack"
    elif status in (400, 401, 403, 413, 415):
        item["outcome"] = "source_rejected"
    require(item["outcome"] == "source_enqueue_ack", f"source did not confirm full enqueue (HTTP {status})")
    return len(body)


def rows(data):
    doc = json.loads(data)
    require(doc["status"] == "success" and doc["data"]["resultType"] == "streams", "unexpected Loki query result")
    found = []
    for stream in doc["data"]["result"]:
        labels = stream["stream"]
        for row in stream["values"]:
            stamp, line = int(row[0]), row[1]
            match = re.fullmatch(r"bench-([1-9][0-9]*)", line)
            require(match, "stored log line failed the capture policy")
            seq = int(match[1])
            require(labels.get("service_name") == "m1-bench" and labels.get("severity_number") == "9" and
                    labels.get("trace_id") == f"{seq:032x}" and labels.get("span_id") == f"{seq:016x}" and
                    labels.get("observed_timestamp") == str(stamp + 11), "Loki correlation/metadata mismatch")
            found.append((stamp, line))
    return found


def metrics(port, prefixes):
    status, data = http(port, "/metrics")
    require(status == 200, "metrics read failed")
    result = {}
    for line in data.decode().splitlines():
        if line.startswith(prefixes):
            key, value = line.rsplit(" ", 1)
            result[key] = float(value)
    return result


def distribution(seconds):
    ordered = sorted(seconds)
    return {"count": len(ordered), **{f"p{p}_ms": round(1000 * ordered[math.ceil(len(ordered)*p/100)-1], 3)
            if ordered else None for p in (50, 95, 99)}}


def digest(path):
    sha = hashlib.sha256()
    with path.open("rb") as stream:
        for piece in iter(lambda: stream.read(1 << 20), b""):
            sha.update(piece)
    return sha.hexdigest()


def trial(server, epoch, directory, result):
    history, live, receiver, monitor, grpc = free_ports(5)
    token_a, token_b, source_token = (secrets.token_hex(32) for _ in range(3))
    store_socket = directory / "loki-http:0"
    runtime = {k: os.environ[k] for k in ("PATH", "HOME", "TMPDIR", "LANG") if k in os.environ}
    runtime.update(GOMAXPROCS="2", GOGC="100", GOMEMLIMIT="off")
    env = {**runtime, "TRIAL_DIR": str(directory), "OBS_BENCH_LISTEN": f"127.0.0.1:{receiver}",
           "OBS_BENCH_LOKI_SOCKET": str(store_socket), "OBS_BENCH_TOKEN_A": token_a,
           "OBS_BENCH_TOKEN_B": token_b, "SOURCE_TOKEN": source_token,
           "LOKI_GRPC_PORT": str(grpc), "HISTORY_PORT": str(history), "LIVE_PORT": str(live),
           "METRICS_PORT": str(monitor), "RECEIVER_PORT": str(receiver)}
    path = f"/bench/v1/logs?start={epoch - 24*HOUR - 1}&end={epoch + 5_000_000_000}"
    history_batches = [[record(4*h+j+1, epoch-(24-h)*HOUR+j) for j in range(4)] for h in range(24)]
    fresh = [record(97+i, epoch+i*100_000_000) for i in range(24)]
    historical = {(int(r["timeUnixNano"]), "bench-"+r["attributes"][0]["value"]["intValue"])
                  for batch in history_batches for r in batch}
    live_keys = [(int(r["timeUnixNano"]), f"bench-{97+i}") for i, r in enumerate(fresh)]
    expected = historical | set(live_keys)
    usage = result["process_usage"] = []
    journal = result["source_batches"] = []
    result.update(scheduled_history=96, scheduled_live=24, query_progress=[])
    source_metrics = ("otelcol_exporter_queue_size", "otelcol_exporter_enqueue_failed_log_records",
                      "otelcol_exporter_sent_log_records", "otelcol_receiver_accepted_log_records")
    with contextlib.ExitStack() as cleanup:
        def start_loki():
            if store_socket.is_socket():
                store_socket.unlink()
            child = Child("loki", [BIN/"loki", "-config.expand-env=true", "-config.file="+str(HERE/"loki.yaml")],
                          {**runtime, "TRIAL_DIR": str(directory), "LOKI_GRPC_PORT": str(grpc)}, directory, usage, cleanup)
            wait_for(lambda: child.alive() and http(store_socket, "/ready")[0] == 200, "Loki readiness", 75)
            return child

        def start_source():
            child = Child("collector", [BIN/"otelcol-contrib", "--config", HERE/"collector.yaml"],
                          env, directory, usage, cleanup)
            wait_for(lambda: child.alive() and http(monitor, "/metrics")[0] == 200, "Collector readiness")
            for port in (history, live):
                wait_for(lambda: child.alive() and http(port, "/v1/logs", payload=b"{}")[0] in (401, 403),
                         "authenticated Collector receiver")
            return child

        def start_receiver():
            child = Child("receiver", [server], env, directory, usage, cleanup)
            wait_for(lambda: child.alive() and http(receiver, "/bench/v1/counts", token=token_a)[0] == 200,
                     "receiver readiness")
            return child

        source = start_source()
        result["history_source_json_bytes"] = sum(export(history, source_token, b, journal) for b in history_batches)
        result["source_before_kill"] = metrics(monitor, source_metrics)
        require(any("queue_size" in k and v > 0 for k, v in result["source_before_kill"].items()),
                "no persisted queue backlog was observed before source kill")
        source.stop()
        source = start_source()
        result["source_after_restart"] = metrics(monitor, source_metrics)
        require(any("queue_size" in k and v > 0 for k, v in result["source_after_restart"].items()),
                "no restored queue backlog was observed")

        # Reach the receiver while the store socket is absent. A source enqueue or a
        # receiver admission cannot release this export as a successful store write.
        unavailable = start_receiver()
        def failed_write():
            unavailable.alive()
            status, data = http(receiver, "/bench/v1/counts", token=token_a)
            if status == 200:
                counts = json.loads(data)
                if counts["store_unknown_records"] > 0 and counts["store_ack_records"] == 0:
                    return counts
        result["receiver_without_store"] = wait_for(failed_write, "failed downstream write")
        held = result["source_without_store"] = metrics(monitor, source_metrics)
        require(all(v == 0 for k,v in held.items() if k.startswith("otelcol_exporter_sent_log_records")),
                "source reported a successful export without a store acknowledgement")
        require(any("queue_size" in k and v > 0 for k,v in held.items()), "source backlog disappeared")
        unavailable.stop()
        loki = start_loki()

        # All intended arrivals exist before the first send. Four senders, one bounded finite corpus.
        start = time.monotonic()
        intended = [start+i/10 for i in range(len(fresh))]
        sent = []
        def offer(i):
            attempt = time.monotonic()
            size = export(live, source_token, [fresh[i]], journal)
            return attempt, time.monotonic(), size

        def send_live():
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as workers:
                for i, when in enumerate(intended):
                    time.sleep(max(0, when-time.monotonic()))
                    sent.append(workers.submit(offer, i))
                return [job.result() for job in sent]

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as generator:
            arrivals = generator.submit(send_live)
            reconnect = time.monotonic()
            gateway = start_receiver()
            connected = time.monotonic()
            first_seen = {}
            probes, probe_errors = [], collections.Counter()
            last_rows = []
            def probe():
                nonlocal last_rows
                for child in (gateway, source, loki):
                    child.alive()
                before = time.monotonic()
                try:
                    status, body = http(receiver, path, token=token_a)
                except (OSError, http_client.HTTPException):
                    probe_errors["transport_unknown"] += 1
                    return False
                now = time.monotonic()
                if status != 200:
                    probe_errors[str(status)] += 1
                    return False
                probes.append(now-before)
                last_rows = rows(body)
                seen = set(last_rows)
                require(not (seen - expected), "query returned unexpected identity or event time")
                result["query_progress"].append({"elapsed_ms": round(1000*(now-reconnect), 3),
                    "service_ms": round(1000*(now-before), 3), "history_unique": len(seen & historical),
                    "live_unique": len(seen-historical), "rows": len(last_rows)})
                for item in last_rows:
                    first_seen.setdefault(item, now)
                if arrivals.done():
                    arrivals.result()
                return set(last_rows) == expected
            wait_for(probe, "all 120 unique rows at original event times", 45)
            offers = arrivals.result()
        def acknowledged():
            status, data = http(receiver, "/bench/v1/counts", token=token_a)
            if status == 200:
                counts = json.loads(data)
                result["receiver"] = counts
                if counts["store_ack_records"] >= len(expected) and counts["outstanding_records"] == 0:
                    return counts
        counts = wait_for(acknowledged, "store ACK accounting after query visibility")
        visible_seconds = max(first_seen.values()) - reconnect
        history_complete = max(first_seen[k] for k in historical)
        first_live = min(first_seen[k] for k in live_keys)
        result.update(source_confirmed_records=sum(b["records"] for b in journal if b["outcome"] == "source_enqueue_ack"),
                      live_unsent=24-len(offers), receiver=counts, query_rows=len(last_rows),
                      visible_unique=len(set(last_rows)), visible_duplicate_rows=len(last_rows)-len(set(last_rows)),
                      missing=len(expected-set(last_rows)), unexpected=len(set(last_rows)-expected),
                      queryable_unique_per_second=round(len(expected)/visible_seconds, 3),
                      store_ack_protobuf_bytes_per_visibility_second=round(counts["store_ack_bytes"]/visible_seconds, 3),
                      live_source_json_bytes=sum(x[2] for x in offers),
                      reconnect_ms=round(1000*(connected-reconnect),3),
                      first_fresh_visible_ms=round(1000*(first_live-reconnect),3),
                      full_history_visible_ms=round(1000*(history_complete-reconnect),3),
                      fresh_observed_before_history_complete=first_live < history_complete,
                      query_result_sha256=hashlib.sha256(json.dumps(sorted(last_rows)).encode()).hexdigest(),
                      live_visible_from_intended=distribution(first_seen[k]-s for k,s in zip(live_keys,intended)),
                      live_source_ack_from_intended=distribution(x[1]-s for x,s in zip(offers,intended)),
                      live_source_ack_from_attempt=distribution(x[1]-x[0] for x in offers),
                      live_generator_wait=distribution(x[0]-s for x,s in zip(offers,intended)),
                      query_service=distribution(probes), query_http_errors=dict(probe_errors))
        def source_released():
            values = metrics(monitor, source_metrics)
            result["source_after_replay"] = values
            for lane, logical in (("history", 96), ("live", 24)):
                sent = sum(v for k,v in values.items() if k.startswith("otelcol_exporter_sent_log_records")
                           and f'exporter="otlp_http/{lane}"' in k)
                if sent != logical:
                    return False
            return all(v == 0 for k,v in values.items() if k.startswith("otelcol_exporter_queue_size"))
        wait_for(source_released, "Collector per-lane sent counts and queue drain")
        wal = metrics(store_socket, ("loki_ingester_wal_disk_full_failures_total",
                                    "loki_ingester_wal_corruptions_total","loki_ingester_wal_records_logged_total"))
        require(any("records_logged" in k and v > 0 for k,v in wal.items()), "WAL append evidence missing")
        require(all(v == 0 for k,v in wal.items() if "records_logged" not in k), "WAL health exception")
        result["wal_before_kill"] = wal
        status, other = http(receiver, path, token=token_b)
        require(status == 200 and not rows(other), "tenant isolation failed")
        loki.stop()
        loki = start_loki()
        def restored():
            gateway.alive()
            loki.alive()
            status, body = http(receiver, path, token=token_a)
            return status == 200 and collections.Counter(rows(body)) == collections.Counter(last_rows)
        wait_for(restored, "same event-time rows after Loki process kill", 40)
        result["post_loki_restart_unique"] = len(expected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--rust", type=Path, default=ROOT/"target/release/otlp-bench-rust")
    parser.add_argument("--go", type=Path, default=BIN/"otlp-bench-go")
    args = parser.parse_args()
    epoch = time.time_ns() // 60_000_000_000 * 60_000_000_000
    result = {"profile": "24 event-time hours, native unscored logs trial", "epoch_ns": epoch,
              "git_head": subprocess.check_output(["git","-C",ROOT,"rev-parse","HEAD"],text=True).strip(),
              "git_status": subprocess.check_output(["git","-C",ROOT,"status","--short"],text=True),
              "host": platform.platform(), "logical_cpu": os.cpu_count(), "GOMAXPROCS": 2, "GOGC": 100,
              "GOMEMLIMIT": "off", "rust_runtime_threads": 2, "rust_max_blocking_threads": 2,
              "build_profiles": {"rust": "cargo release --locked", "go": "default optimization -buildvcs=false"},
              "memory_bytes": os.sysconf("SC_PHYS_PAGES")*os.sysconf("SC_PAGE_SIZE"),
              "cpu_memory_disk_network_quotas": None, "variants": {}}
    for name, path, arg in (("loki",BIN/"loki","-version"),("collector",BIN/"otelcol-contrib","--version"),
                            ("rustc","rustc","--version"),("go","go","version")):
        result[name+"_version"] = subprocess.check_output([path,arg],text=True,stderr=subprocess.STDOUT).strip()
    result["common_sha256"] = {p.name:digest(p) for p in (BIN/"loki",BIN/"otelcol-contrib",HERE/"loki.yaml",HERE/"collector.yaml")}
    require("version 3.7.10 " in result["loki_version"] and result["collector_version"].endswith(" 0.165.0"),
            "unexpected trial tool versions; run fetch_tools.py to install the pinned builds")
    failed = False
    for name, binary in (("rust", args.rust.resolve()), ("go", args.go.resolve())):
        run = result["variants"][name] = {"binary_sha256": digest(binary)}
        with tempfile.TemporaryDirectory(prefix="m1-", dir="/tmp") as directory:
            try:
                trial(binary, epoch, Path(directory), run)
                run["result"] = "passed"
            except Exception as e:
                failed = True
                run.update(result="failed", error=str(e))
                print(name, str(e), file=sys.stderr)
        args.output.write_text(json.dumps(result, indent=2, sort_keys=True)+"\n")
        print(name, run["result"], flush=True)
    if not failed:
        hashes = {v["query_result_sha256"] for v in result["variants"].values()}
        result["same_visible_rows"] = len(hashes) == 1
        args.output.write_text(json.dumps(result, indent=2, sort_keys=True)+"\n")
        failed = len(hashes) != 1
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
