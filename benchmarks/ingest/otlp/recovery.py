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
import select
import signal
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
from typing import NamedTuple

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


class Row(NamedTuple):
    event_ns: int
    line: str
    observed_ns: str
    service: str
    severity: str
    trace_id: str
    span_id: str


def expected_row(r):
    seq = r["attributes"][0]["value"]["intValue"]
    return Row(int(r["timeUnixNano"]), "bench-" + seq, r["observedTimeUnixNano"],
               "m1-bench", str(r["severityNumber"]), r["traceId"], r["spanId"])


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
        for value in stream["values"]:
            stamp, line = value[:2]
            require(re.fullmatch(r"bench-[1-9][0-9]*", line), "stored log line failed the capture policy")
            found.append(Row(int(stamp), line, labels["observed_timestamp"], labels["service_name"],
                             labels["severity_number"], labels["trace_id"], labels["span_id"]))
    return found


def check_rows(actual, expected, phase, *, complete=True):
    """Allow missing rows only while watching an in-progress read converge."""
    seen = collections.Counter(actual)
    surplus = seen - expected
    unexpected = sum(n for row, n in surplus.items() if row not in expected)
    duplicates = surplus.total() - unexpected
    missing = (expected - seen).total()
    require(not (unexpected or duplicates or (complete and missing)),
            f"{phase}: missing={missing} duplicate={duplicates} unexpected={unexpected}")
    return seen


def overlap_witness(samples, history_total, live_total):
    first = None
    for sample in samples:
        h, live = sample["history_unique"], sample["live_unique"]
        if 0 < h < history_total and 0 < live < live_total:
            if first is None:
                first = sample
            elif (sample["elapsed_ms"] > first["elapsed_ms"] and
                  h > first["history_unique"] and live > first["live_unique"]):
                return [first, sample]
    raise RuntimeError("controlled overlap: both lanes must advance between two partial visibility probes")


@contextlib.contextmanager
def paced_history(receiver, delay):
    """Pace only the history consumer. Both real receivers close after one HTTP exchange."""
    class Exchange(socketserver.BaseRequestHandler):
        def handle(self):
            try:
                with socket.create_connection(("127.0.0.1", receiver), timeout=2) as upstream:
                    self.request.settimeout(2)
                    deadline = time.monotonic() + 2
                    time.sleep(delay)
                    peers = {self.request: upstream, upstream: self.request}
                    while time.monotonic() < deadline:
                        readable, _, _ = select.select(list(peers), [], [], max(0, deadline-time.monotonic()))
                        if not readable:
                            return
                        for peer in readable:
                            chunk = peer.recv(65536)
                            if not chunk:
                                return
                            peers[peer].sendall(chunk)
            except OSError:
                # An offline receiver closes this attempt. The Collector owns its retry.
                return

    with socketserver.TCPServer(("127.0.0.1", 0), Exchange) as relay:
        worker = threading.Thread(target=relay.serve_forever, kwargs={"poll_interval": .05})
        worker.start()
        try:
            yield relay.server_address[1]
        finally:
            relay.shutdown()
            worker.join()


def metrics(port, prefixes):
    status, data = http(port, "/metrics")
    require(status == 200, "metrics read failed")
    result = {}
    for line in data.decode().splitlines():
        if line.startswith(prefixes):
            key, value = line.rsplit(" ", 1)
            result[key] = float(value)
    return result


SOURCE_METRICS = {"queue_requests": "otelcol_exporter_queue_size",
                  "sent_records": "otelcol_exporter_sent_log_records",
                  "accepted_records": "otelcol_receiver_accepted_log_records",
                  "enqueue_failed_records": "otelcol_exporter_enqueue_failed_log_records"}


def source_snapshot(port):
    raw = metrics(port, tuple(SOURCE_METRICS.values()))
    lanes = {}
    for lane in ("history", "live"):
        lanes[lane] = {}
        for field, metric in SOURCE_METRICS.items():
            label = f'receiver="otlp/{lane}"' if field == "accepted_records" else f'exporter="otlp_http/{lane}"'
            values = [v for k, v in raw.items() if k.startswith(metric + "{") and label in k]
            lanes[lane][field] = sum(values) if values else None
    return {"series": raw, **lanes}


def check_outage(snapshot, batches, accepted_history):
    for lane, queued in (("history", batches), ("live", 0)):
        observed = snapshot[lane]
        require(observed["queue_requests"] == queued, f"{lane}: expected {queued} queued requests, got {observed}")
        require(observed["sent_records"] in (None, 0), f"{lane}: unexpected completed exporter records")
        require(observed["enqueue_failed_records"] in (None, 0), f"{lane}: source enqueue failure")
    require(snapshot["live"]["accepted_records"] in (None, 0), "unexpected live submissions during outage")
    if accepted_history is not None:
        require(snapshot["history"]["accepted_records"] == accepted_history,
                "Collector history acceptance disagrees with the submitted workload")
    else:
        require(snapshot["history"]["accepted_records"] in (None, 0), "restored Collector received new history")


def drained(snapshot, history_records, live_records):
    return all(snapshot[lane]["queue_requests"] == 0 and snapshot[lane]["sent_records"] == count
               for lane, count in (("history", history_records), ("live", live_records)))


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


def rejection_trial(history, live, receiver, monitor, store_socket, token, source_token, epoch, result):
    """Exercise a real Loki mixed-age 400 after the clean recovery measurements."""
    stamp = epoch + 10_000_000_000
    path = f"/bench/v1/logs?start={stamp-1}&end={stamp+10}"
    partial, mixed, later, control = [record(201+i, stamp+i) for i in range(4)]
    expired = record(205, epoch-72*HOUR)
    journal = result["source_batches"] = []

    def counts():
        status, body = http(receiver, "/bench/v1/counts", token=token)
        require(status == 200, "failed to read receiver disposition counts")
        return json.loads(body)

    def query():
        status, body = http(receiver, path, token=token)
        require(status == 200, "failed to read rejection fixture")
        return rows(body)

    before = counts()
    source_before = source_snapshot(monitor)
    # This invalid fixture fails local validation. OTLP partial success is terminal.
    export(history, source_token, [partial, record(0, stamp)], journal)

    def partial_completed():
        snapshot = source_snapshot(monitor)
        if snapshot["history"]["sent_records"] == source_before["history"]["sent_records"] + 2:
            return snapshot
    partial_source = wait_for(partial_completed, "partial success counted by the Collector")
    partial_counts = counts()
    partial_rows = collections.Counter([expected_row(partial)])
    partial_seen = wait_for(lambda: check_rows(query(), partial_rows, "partial success visibility", complete=False),
                            "query visibility after OTLP partial success")
    require(partial_counts["invalid_records"] - before["invalid_records"] == 1 and
            partial_counts["store_ack_records"] - before["store_ack_records"] == 1,
            "OTLP partial success did not account for the accepted and rejected records")
    result["partial_success"] = {
        "collector_sent_delta": partial_source["history"]["sent_records"] - source_before["history"]["sent_records"],
        "receiver_invalid_delta": partial_counts["invalid_records"] - before["invalid_records"],
        "store_ack_delta": partial_counts["store_ack_records"] - before["store_ack_records"],
        "visible_rows": partial_seen.total()}

    export(history, source_token, [expired, mixed], journal)

    def bad_attempt():
        c = counts()
        if c["store_http_400_attempts"] > partial_counts["store_http_400_attempts"]:
            return c
    wait_for(bad_attempt, "Loki 400 on the mixed-age export")
    export(history, source_token, [later], journal)
    export(live, source_token, [control], journal)
    # Count retries after the follower was enqueued. A stuck lane must not be
    # confused with a slow first attempt or a store that is unavailable to both lanes.
    at_enqueue = counts()

    def retried():
        c = counts()
        if (c["store_http_400_attempts"] >= at_enqueue["store_http_400_attempts"] + 2 and
                c["store_ack_records"] == partial_counts["store_ack_records"] + 1):
            return c
    last = wait_for(retried, "repeated mixed-batch 400s and an independent live ACK", 25)
    def mixed_visible():
        seen = collections.Counter(query())
        # Retries of the eligible member can create visible copies. Count all of them.
        others = seen.copy()
        copies = others.pop(expected_row(mixed), 0)
        expected_others = partial_rows + collections.Counter([expected_row(control)])
        check_rows(others.elements(), expected_others, "mixed commit and blocked follower", complete=False)
        if copies and others == expected_others:
            return seen
    seen = wait_for(mixed_visible, "mixed valid member and independent live visibility")
    status, body = http(receiver, f"/bench/v1/logs?start={int(expired['timeUnixNano'])-1}"
                        f"&end={int(expired['timeUnixNano'])+1}", token=token)
    require(status == 200, "expired-time query failed")
    expired_seen = check_rows(rows(body), collections.Counter(), "expired event-time read")
    held = result["source_while_blocked"] = source_snapshot(monitor)
    require(held["history"]["queue_requests"] == 2 and held["live"]["queue_requests"] == 0
            and held["history"]["sent_records"] == partial_source["history"]["sent_records"],
            "poisoned history request and its follower were not retained on the single-consumer lane")
    bad = last["store_http_400_attempts"] - partial_counts["store_http_400_attempts"]
    unknown = last["store_unknown_records"] - partial_counts["store_unknown_records"]
    require(unknown == 2 * bad, "mixed-batch attempts were given a false per-record disposition")
    discards = metrics(store_socket, ("loki_discarded_samples_total",))
    age_rejects = sum(v for k, v in discards.items()
                      if 'reason="greater_than_max_sample_age"' in k and 'tenant="a"' in k)
    require(age_rejects >= bad, "Loki age-rejection evidence missing")
    result.update(outcome="same_lane_blocked_during_retries", store_http_400_attempts=bad,
                  retries_after_follower_enqueue=last["store_http_400_attempts"]-at_enqueue["store_http_400_attempts"],
                  store_unknown_record_attempts=unknown, loki_age_rejected_attempts=age_rejects,
                  expired_event_ns=int(expired["timeUnixNano"]), expired_query_rows=expired_seen.total(),
                  mixed_valid_query_rows=seen[expected_row(mixed)], later_history_query_rows=seen[expected_row(later)],
                  independent_live_query_rows=seen[expected_row(control)], quarantine_enabled=False, quarantined_records=0)


def trial(server, epoch, directory, result, *, controlled=False):
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
    historical = collections.Counter(expected_row(r) for batch in history_batches for r in batch)
    live_keys = [expected_row(r) for r in fresh]
    expected = historical + collections.Counter(live_keys)
    usage = result["process_usage"] = []
    journal = result["source_batches"] = []
    result.update(scheduled_history=96, scheduled_live=24, query_progress=[])
    with contextlib.ExitStack() as cleanup:
        # The control run changes only history transport pacing and the live start barrier.
        delay_ms = result["history_delay_per_exchange_ms"] = 150 if controlled else 0
        source_cmd = [BIN/"otelcol-contrib", "--config", HERE/"collector.yaml"]
        if controlled:
            relay = cleanup.enter_context(paced_history(receiver, delay_ms / 1000))
            source_cmd.extend(["--set", f"exporters.otlp_http/history.endpoint=http://127.0.0.1:{relay}"])

        def start_loki():
            if store_socket.is_socket():
                store_socket.unlink()
            child = Child("loki", [BIN/"loki", "-config.expand-env=true", "-config.file="+str(HERE/"loki.yaml")],
                          {**runtime, "TRIAL_DIR": str(directory), "LOKI_GRPC_PORT": str(grpc)}, directory, usage, cleanup)
            wait_for(lambda: child.alive() and http(store_socket, "/ready")[0] == 200, "Loki readiness", 75)
            return child

        def start_source():
            child = Child("collector", source_cmd, env, directory, usage, cleanup)
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
        result["source_before_kill"] = source_snapshot(monitor)
        check_outage(result["source_before_kill"], len(history_batches), historical.total())
        source.stop()
        source = start_source()
        result["source_after_restart"] = source_snapshot(monitor)
        check_outage(result["source_after_restart"], len(history_batches), None)

        # Reach the receiver while the store socket is absent. A source enqueue or a
        # receiver admission cannot release this export as a successful store write.
        unavailable = start_receiver()

        def failed_write():
            unavailable.alive()
            status, data = http(receiver, "/bench/v1/counts", token=token_a)
            if status == 200:
                counts = json.loads(data)
                if (counts["store_attempts"] > 0 and counts["outstanding_records"] == 0 and
                        counts["store_unknown_records"] == counts["store_attempts"] * len(history_batches[0]) and
                        counts["store_ack_records"] == counts["invalid_records"] == 0):
                    return counts
        result["receiver_without_store"] = wait_for(failed_write, "failed downstream write")
        result["source_without_store"] = source_snapshot(monitor)
        check_outage(result["source_without_store"], len(history_batches), None)
        unavailable.stop()
        loki = start_loki()

        def offer(i):
            attempt = time.monotonic()
            size = export(live, source_token, [fresh[i]], journal)
            return attempt, time.monotonic(), size

        def send_live(intended):
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as workers:
                sent = []
                for i, when in enumerate(intended):
                    time.sleep(max(0, when-time.monotonic()))
                    sent.append(workers.submit(offer, i))
                return [job.result() for job in sent]

        first_seen = {}
        probes, probe_errors = [], collections.Counter()

        def probe():
            for child in (gateway, source, loki):
                child.alive()
            before = time.monotonic()
            try:
                status, body = http(receiver, path, token=token_a)
            except (OSError, http_client.HTTPException):
                probe_errors["transport_unknown"] += 1
                return None
            now = time.monotonic()
            if status != 200:
                probe_errors[str(status)] += 1
                return None
            probes.append(now-before)
            seen = check_rows(rows(body), expected, "replay probe", complete=False)
            result["query_progress"].append({"elapsed_ms": round(1000*(now-reconnect), 3),
                "service_ms": round(1000*(now-before), 3), "history_unique": len(seen & historical),
                "live_unique": len(seen-historical), "rows": seen.total()})
            for item in seen:
                first_seen.setdefault(item, now)
            return seen

        def start_live(generator):
            start = time.monotonic()
            intended = [start+i/10 for i in range(len(fresh))]
            return intended, generator.submit(send_live, intended)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as generator:
            if not controlled:
                intended, arrivals = start_live(generator)
            reconnect = time.monotonic()
            gateway = start_receiver()
            connected = time.monotonic()
            if controlled:
                prefix = wait_for(probe, "first visible historical batch before offering live traffic")
                require(0 < prefix.total() < historical.total(), "history completed before overlap control was armed")
                intended, arrivals = start_live(generator)

            def complete_probe():
                if arrivals.done():
                    arrivals.result()
                return probe() == expected

            wait_for(complete_probe, "the exact 120 source rows at original event times", 45)
            offers = arrivals.result()
        if controlled:
            result["overlap_witness"] = overlap_witness(result["query_progress"], len(historical), len(live_keys))

        def acknowledged():
            status, data = http(receiver, "/bench/v1/counts", token=token_a)
            if status == 200:
                counts = json.loads(data)
                result["receiver"] = counts
                if counts["store_ack_records"] == expected.total() and counts["outstanding_records"] == 0:
                    return counts
        counts = wait_for(acknowledged, "store ACK accounting after query visibility")
        visible_seconds = max(first_seen.values()) - reconnect
        history_complete = max(first_seen[k] for k in historical)
        first_live = min(first_seen[k] for k in live_keys)
        require(counts["admitted_records"] == expected.total() and counts["store_unknown_records"] == 0
                and counts["invalid_records"] == 0, "healthy receiver accounting disagrees with the workload")
        result.update(source_confirmed_records=sum(b["records"] for b in journal if b["outcome"] == "source_enqueue_ack"),
                      live_unsent=len(fresh)-len(offers), receiver=counts,
                      queryable_unique_per_second=round(len(expected)/visible_seconds, 3),
                      store_ack_protobuf_bytes_per_visibility_second=round(counts["store_ack_bytes"]/visible_seconds, 3),
                      live_source_json_bytes=sum(x[2] for x in offers),
                      reconnect_ms=round(1000*(connected-reconnect),3),
                      first_fresh_visible_ms=round(1000*(first_live-reconnect),3),
                      full_history_visible_ms=round(1000*(history_complete-reconnect),3),
                      fresh_observed_before_history_complete=first_live < history_complete,
                      live_visible_from_intended=distribution(first_seen[k]-s for k,s in zip(live_keys,intended)),
                      live_source_ack_from_intended=distribution(x[1]-s for x,s in zip(offers,intended)),
                      live_source_ack_from_attempt=distribution(x[1]-x[0] for x in offers),
                      live_generator_wait=distribution(x[0]-s for x,s in zip(offers,intended)),
                      query_service=distribution(probes), query_http_errors=dict(probe_errors))

        def source_released():
            snapshot = result["source_after_replay"] = source_snapshot(monitor)
            return drained(snapshot, historical.total(), len(fresh)) and snapshot["live"]["accepted_records"] == len(fresh)
        wait_for(source_released, "Collector per-lane sent counts and queue drain")
        require(result["source_confirmed_records"] == expected.total(), "source enqueue journal did not reconcile")

        def exact_read(phase):
            def read():
                gateway.alive()
                loki.alive()
                status, body = http(receiver, path, token=token_a)
                if status == 200:
                    return check_rows(rows(body), expected, phase)
            return wait_for(read, phase)

        settled = exact_read("settled read before Loki restart")
        result.update(query_rows=settled.total(), visible_unique=len(settled),
                      visible_duplicate_rows=(settled-expected).total(), missing=(expected-settled).total(),
                      unexpected=len(settled.keys()-expected.keys()),
                      query_result_sha256=hashlib.sha256(json.dumps(sorted(settled.elements())).encode()).hexdigest())
        wal = metrics(store_socket, ("loki_ingester_wal_disk_full_failures_total",
                                    "loki_ingester_wal_corruptions_total","loki_ingester_wal_records_logged_total"))
        require(wal.get("loki_ingester_wal_records_logged_total", 0) > 0, "WAL append evidence missing")
        require(wal.get("loki_ingester_wal_disk_full_failures_total") == 0, "expected observed zero WAL disk-full failures")
        require(all(v == 0 for k,v in wal.items() if "records_logged" not in k), "WAL health exception")
        result["wal_before_kill"] = wal
        status, other = http(receiver, path, token=token_b)
        require(status == 200 and not rows(other), "tenant isolation failed")
        loki.stop()
        loki = start_loki()
        restored = exact_read("restored read after Loki restart")
        result.update(post_loki_restart_unique=len(restored), post_loki_restart_rows=restored.total())
        if not controlled:
            result["rejection"] = {}
            rejection_trial(history, live, receiver, monitor, store_socket, token_a, source_token,
                            epoch, result["rejection"])


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
        run["controlled_overlap"] = {}
        for controlled, scenario in ((False, run), (True, run["controlled_overlap"])):
            with tempfile.TemporaryDirectory(prefix="m1-", dir="/tmp") as directory:
                try:
                    trial(binary, epoch, Path(directory), scenario, controlled=controlled)
                    scenario["result"] = "passed"
                except Exception as e:
                    failed = True
                    scenario.update(result="failed", error=str(e))
                    print(name, str(e), file=sys.stderr)
            if controlled and scenario["result"] == "failed":
                run["result"] = "failed"
            args.output.write_text(json.dumps(result, indent=2, sort_keys=True)+"\n")
            print(name, "controlled overlap" if controlled else "recovery and rejection", scenario["result"], flush=True)
    if not failed:
        hashes = {s["query_result_sha256"] for v in result["variants"].values() for s in (v, v["controlled_overlap"])}
        result["same_visible_rows"] = len(hashes) == 1
        args.output.write_text(json.dumps(result, indent=2, sort_keys=True)+"\n")
        failed = len(hashes) != 1
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
