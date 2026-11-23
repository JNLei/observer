#!/usr/bin/env python3
"""Bounded, open-loop smoke harness for the synthetic forwarding prototypes.

This is not an OTLP workload, durability test, or language performance gate.
"""

import argparse
import collections
import hashlib
import json
import math
import os
import platform
import queue
import select
import signal
import socket
import socketserver
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path

MAX_EVENTS = 1_000_000
MAX_PAYLOAD = 1_048_576
MAX_DIAGNOSTIC_BYTES = 8192
MAX_CONTROL_LINE = 8192
OUTCOMES = {
    0: "acknowledged",
    1: "overloaded",
    2: "downstream_failure_or_unknown",
    3: "invalid",
}


def integer(value, name, minimum, maximum):
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer in [{minimum}, {maximum}]")
    return value


def validate_scenario(scenario):
    if not isinstance(scenario, dict):
        raise ValueError("scenario must be an object")
    for key, default, low, high in (
        ("workers", 4, 1, 256),
        ("queue_capacity", 8, 1, 4096),
        ("generator_workers", 8, 1, 256),
        ("generator_queue_capacity", 64, 1, 4096),
        ("server_timeout_ms", 500, 1, 60000),
        ("client_timeout_ms", 1200, 1, 60000),
        ("payload_bytes", 128, 0, MAX_PAYLOAD),
        ("max_payload_bytes", 65536, 1, MAX_PAYLOAD),
        ("drain_timeout_ms", 5000, 1, 60000),
        ("sink_drain_timeout_ms", 5000, 1, 60000),
    ):
        scenario[key] = integer(scenario.get(key, default), key, low, high)
    if scenario["payload_bytes"] > scenario["max_payload_bytes"]:
        raise ValueError("payload_bytes exceeds max_payload_bytes")
    phases = scenario.get("phases")
    if not isinstance(phases, list) or not 1 <= len(phases) <= 100:
        raise ValueError("phases must contain 1 to 100 phases")
    total = 0
    names = set()
    for phase in phases:
        if not isinstance(phase, dict) or not isinstance(phase.get("name"), str) or not phase["name"]:
            raise ValueError("each phase needs a nonempty name")
        if phase["name"] in names:
            raise ValueError("phase names must be unique")
        names.add(phase["name"])
        for key in ("duration_seconds", "rate_per_second"):
            value = phase.get(key)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{key} must be a positive finite number")
        if phase["duration_seconds"] > 86400 or phase["rate_per_second"] > MAX_EVENTS:
            raise ValueError("phase duration or rate exceeds harness bounds")
        phase["delay_ms"] = integer(phase.get("delay_ms", 0), "delay_ms", 0, 60000)
        if phase.get("sink_mode", "healthy") not in ("healthy", "disconnect"):
            raise ValueError("sink_mode must be healthy or disconnect")
        phase["sink_mode"] = phase.get("sink_mode", "healthy")
        count = int(phase["duration_seconds"] * phase["rate_per_second"])
        if not count:
            raise ValueError("each phase must schedule at least one request")
        phase["scheduled_count"] = count
        total += count
    if total > MAX_EVENTS:
        raise ValueError(f"scenario exceeds the {MAX_EVENTS} event in-memory accounting limit")
    return scenario


def recv_exact(conn, size):
    chunks = bytearray()
    while len(chunks) < size:
        data = conn.recv(min(size - len(chunks), 65536))
        if not data:
            raise EOFError("short frame")
        chunks.extend(data)
    return bytes(chunks)


class Sink(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 512

    def __init__(self, workers, max_payload, timeout):
        self.permits = threading.BoundedSemaphore(workers)
        self.lock = threading.Lock()
        self.idle = threading.Condition(self.lock)
        self.active = 0
        self.mode = "healthy"
        self.delay_ms = 0
        self.max_payload = max_payload
        self.timeout = timeout
        self.stats = collections.Counter()
        self.ids = set()
        super().__init__(("127.0.0.1", 0), SinkHandler)

    def set_mode(self, mode, delay_ms):
        with self.lock:
            self.mode = mode
            self.delay_ms = delay_ms

    def process_request(self, request, client_address):
        if not self.permits.acquire(blocking=False):
            with self.lock:
                self.stats["capacity_rejections"] += 1
            self.shutdown_request(request)
            return
        with self.lock:
            self.active += 1
        try:
            super().process_request(request, client_address)
        except Exception:
            with self.lock:
                self.active -= 1
                self.idle.notify_all()
            self.permits.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            with self.lock:
                self.active -= 1
                self.idle.notify_all()
            self.permits.release()

    def snapshot_after_drain(self, timeout):
        deadline = time.monotonic() + timeout
        with self.idle:
            while self.active:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self.idle.wait(remaining)
            return {**self.stats, "unique_ids": len(self.ids), "active_handlers_at_snapshot": self.active}


class SinkHandler(socketserver.BaseRequestHandler):
    def handle(self):
        sink = self.server
        with sink.lock:
            mode, delay = sink.mode, sink.delay_ms
            sink.stats["connections"] += 1
        if mode == "disconnect":
            with sink.lock:
                sink.stats["simulated_disconnects"] += 1
            return
        self.request.settimeout(sink.timeout)
        try:
            header = recv_exact(self.request, 12)
            record_id, length = struct.unpack("!QI", header)
            if length > sink.max_payload:
                with sink.lock:
                    sink.stats["oversize_frames"] += 1
                return
            payload = recv_exact(self.request, length)
            if payload != bytes([record_id % 251]) * length:
                with sink.lock:
                    sink.stats["payload_mismatches"] += 1
                return
            with sink.lock:
                sink.stats["valid_frames"] += 1
                if record_id in sink.ids:
                    sink.stats["duplicate_ids"] += 1
                elif len(sink.ids) < MAX_EVENTS:
                    sink.ids.add(record_id)
                else:
                    sink.stats["ids_not_tracked"] += 1
            if delay:
                time.sleep(delay / 1000)
            with sink.lock:
                sink.stats["ack_write_attempts"] += 1
            try:
                self.request.sendall(header[:8])
            except OSError:
                with sink.lock:
                    sink.stats["ack_write_failures"] += 1
                raise
            with sink.lock:
                sink.stats["ack_writes"] += 1
        except (OSError, EOFError):
            with sink.lock:
                sink.stats["connection_errors"] += 1


def percentile(values, p):
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[max(0, math.ceil(len(ordered) * p) - 1)] * 1000, 3)


def distribution(values):
    return {"count": len(values), "p50": percentile(values, 0.50), "p95": percentile(values, 0.95), "p99": percentile(values, 0.99)}


class Forwarder:
    """Manage the local child and its bounded control/diagnostic streams."""

    def __init__(self, command):
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, bufsize=0)
        self.output = bytearray()
        self.stderr = bytearray()
        self.stderr_truncated = False
        self.stderr_lock = threading.Lock()
        self.stderr_thread = threading.Thread(target=self._read_stderr, daemon=True)
        self.stderr_thread.start()

    def _read_stderr(self):
        while True:
            try:
                chunk = os.read(self.process.stderr.fileno(), 4096)
            except OSError:
                return
            if not chunk:
                return
            with self.stderr_lock:
                self.stderr.extend(chunk)
                if len(self.stderr) > MAX_DIAGNOSTIC_BYTES:
                    self.stderr_truncated = True
                    del self.stderr[:-MAX_DIAGNOSTIC_BYTES]

    def diagnostics(self):
        with self.stderr_lock:
            return {"tail": bytes(self.stderr).decode("utf-8", errors="replace"),
                    "truncated": self.stderr_truncated}

    def unexpected_exit(self):
        self.stderr_thread.join(timeout=0.2)
        info = self.diagnostics()
        return RuntimeError(f"forwarder exited unexpectedly (exit {self.process.poll()}); "
                            f"stderr tail (truncated={info['truncated']}): {info['tail']}")

    def check_alive(self):
        if self.process.poll() is not None:
            raise self.unexpected_exit()

    def send(self, command):
        self.check_alive()
        try:
            data = (command + "\n").encode("ascii")
            if os.write(self.process.stdin.fileno(), data) != len(data):
                raise RuntimeError("short forwarder control write")
        except (BrokenPipeError, OSError) as error:
            self.process.poll()
            if self.process.returncode is not None:
                raise self.unexpected_exit() from error
            raise RuntimeError(f"forwarder control write failed: {error}") from error

    def read_line(self, deadline):
        while True:
            if b"\n" in self.output:
                line, _, rest = self.output.partition(b"\n")
                self.output = bytearray(rest)
                if len(line) > MAX_CONTROL_LINE:
                    raise RuntimeError("forwarder control line exceeded limit")
                return line.decode("utf-8", errors="replace")
            if len(self.output) > MAX_CONTROL_LINE:
                raise RuntimeError("forwarder control line exceeded limit")
            remaining = deadline - time.monotonic()
            ready, _, _ = select.select([self.process.stdout], [], [], max(0, min(remaining, 0.05)))
            if ready:
                chunk = os.read(self.process.stdout.fileno(), 4096)
                if not chunk:
                    if self.process.poll() is None:
                        try:
                            self.process.wait(timeout=0.2)
                        except subprocess.TimeoutExpired as error:
                            raise RuntimeError("forwarder closed its control output unexpectedly") from error
                    raise self.unexpected_exit()
                self.output.extend(chunk)
            else:
                if self.process.poll() is not None:
                    if select.select([self.process.stdout], [], [], 0)[0]:
                        continue
                    raise self.unexpected_exit()
                if remaining <= 0:
                    raise TimeoutError("forwarder control response timed out")

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=2)
        self.stderr_thread.join(timeout=0.5)
        for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
            if stream:
                stream.close()


def parse_counters(line, kind):
    prefix = kind + " "
    if not line.startswith(prefix):
        raise RuntimeError(f"unexpected forwarder control response: {line!r}")
    try:
        counts = json.loads(line[len(prefix):])
        keys = ("accepted", "admitted", "overloaded", "overload_writing", "overload_write_ok",
                "overload_write_failed", "queued", "in_flight", "completed", "ready_write_failed",
                "status_ok", "status_downstream_unknown", "status_invalid", "response_write_ok",
                "response_write_failed")
        if not isinstance(counts, dict) or set(counts) != set(keys):
            raise ValueError("unexpected counter fields")
        if any(isinstance(counts[key], bool) or not isinstance(counts[key], int) or counts[key] < 0
               for key in keys):
            raise ValueError("invalid counter value")
        if (counts["accepted"] != counts["admitted"] + counts["overloaded"]
                or counts["admitted"] != counts["queued"] + counts["in_flight"] + counts["completed"]
                or counts["overloaded"] != counts["overload_writing"] + counts["overload_write_ok"] + counts["overload_write_failed"]
                or counts["completed"] != counts["ready_write_failed"] + counts["status_ok"] + counts["status_downstream_unknown"] + counts["status_invalid"]
                or counts["completed"] != counts["ready_write_failed"] + counts["response_write_ok"] + counts["response_write_failed"]):
            raise ValueError("counter conservation failed")
        return counts
    except (json.JSONDecodeError, ValueError) as error:
        raise RuntimeError(f"invalid forwarder {kind.lower()} counters: {error}") from error


def snapshot(forwarder):
    forwarder.send("SNAPSHOT")
    result = parse_counters(forwarder.read_line(time.monotonic() + 2), "SNAPSHOT")
    forwarder.check_alive()
    return result


def drain_forwarder(forwarder, timeout):
    """Drain accepted work or preserve a conservative pre-termination snapshot."""
    before = snapshot(forwarder)
    forwarder.send("DRAIN")
    try:
        line = forwarder.read_line(time.monotonic() + timeout)
    except TimeoutError:
        # A snapshot can race with normal completion; either response is valid.
        if forwarder.process.poll() is None:
            try:
                forwarder.send("SNAPSHOT")
            except RuntimeError:
                if forwarder.process.poll() != 0:
                    raise
        line = forwarder.read_line(time.monotonic() + 2)
        if line.startswith("SNAPSHOT "):
            last = parse_counters(line, "SNAPSHOT")
            termination_signal = None
            if forwarder.process.poll() is None:
                forwarder.process.terminate()
                termination_signal = signal.SIGTERM
            try:
                code = forwarder.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                forwarder.process.kill()
                termination_signal = signal.SIGKILL
                code = forwarder.process.wait(timeout=2)
            if code == 0:
                line = forwarder.read_line(time.monotonic() + 2)
            elif termination_signal is not None and code == -termination_signal:
                return {"state": "forced_after_timeout", "before_drain": before,
                        "last_snapshot_before_termination": last,
                        "accepted_connection_accounting_complete": False,
                        "process_exit_code": code, "termination_signal": termination_signal.name,
                        "aborted_after_snapshot": "unknown; work may have finished before termination"}
            else:
                raise forwarder.unexpected_exit()
    final = parse_counters(line, "FINAL")
    try:
        code = forwarder.process.wait(timeout=2)
    except subprocess.TimeoutExpired as error:
        raise RuntimeError("forwarder emitted final counters but did not exit") from error
    if code != 0:
        raise forwarder.unexpected_exit()
    if final["queued"] or final["in_flight"] or final["overload_writing"]:
        raise RuntimeError("forwarder reported a final snapshot with outstanding accepted work")
    return {"state": "drained", "before_drain": before, "final": final,
            "accepted_connection_accounting_complete": True, "process_exit_code": code,
            "termination_signal": None, "aborted_after_snapshot": 0}


def run_scenario(server_path, scenario):
    scenario = validate_scenario(scenario)
    server_path = Path(server_path).resolve(strict=True)
    sink = Sink(scenario["workers"], scenario["max_payload_bytes"], scenario["server_timeout_ms"] / 1000)
    sink_thread = threading.Thread(target=sink.serve_forever, daemon=True)
    sink_thread.start()
    command = [str(server_path), "--listen", "127.0.0.1:0", "--downstream", f"127.0.0.1:{sink.server_address[1]}",
               "--workers", str(scenario["workers"]), "--queue", str(scenario["queue_capacity"]),
               "--timeout-ms", str(scenario["server_timeout_ms"]), "--max-payload", str(scenario["max_payload_bytes"])]
    forwarder = None
    workers = []
    abort = threading.Event()
    try:
        forwarder = Forwarder(command)
        line = forwarder.read_line(time.monotonic() + 5).strip()
        if not line.startswith("LISTENING 127.0.0.1:"):
            raise RuntimeError(f"forwarder failed to start: {line!r}")
        address = ("127.0.0.1", int(line.rsplit(":", 1)[1]))
        tasks = queue.Queue(maxsize=scenario["generator_queue_capacity"])
        lock = threading.Lock()
        pending_condition = threading.Condition()
        pending = [0]
        worker_errors = queue.Queue(maxsize=1)
        phase_stats = [{"name": phase["name"], "counts": collections.Counter(), "intended": [], "service": [], "to_attempt": [], "queue": [], "scheduler_lag": []}
                       for phase in scenario["phases"]]

        def worker():
            while not abort.is_set():
                try:
                    task = tasks.get(timeout=0.1)
                except queue.Empty:
                    continue
                try:
                    if task is None:
                        return
                    if abort.is_set():
                        continue
                    index, record_id, intended, enqueued = task
                    started = time.monotonic()
                    stats = phase_stats[index]
                    with lock:
                        stats["counts"]["attempted"] += 1
                        stats["to_attempt"].append(started - intended)
                        stats["queue"].append(started - enqueued)
                        stats["scheduler_lag"].append(enqueued - intended)
                    frame = struct.pack("!QI", record_id, scenario["payload_bytes"]) + bytes([record_id % 251]) * scenario["payload_bytes"]
                    outcome = "transport_error_or_unknown"
                    try:
                        with socket.create_connection(address, timeout=scenario["client_timeout_ms"] / 1000) as conn:
                            conn.settimeout(scenario["client_timeout_ms"] / 1000)
                            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                            with lock:
                                stats["counts"]["connected"] += 1
                            admission = conn.recv(1)
                            if admission == b"\x01":
                                outcome = "overloaded"
                            elif admission == b"\x04":
                                conn.sendall(frame)
                                with lock:
                                    stats["counts"]["sent"] += 1
                                reply = conn.recv(1)
                                if reply:
                                    outcome = OUTCOMES.get(reply[0], "protocol_error_or_unknown")
                            elif admission:
                                outcome = "protocol_error_or_unknown"
                    except socket.timeout:
                        outcome = "timeout_or_unknown"
                    except OSError:
                        pass
                    finished = time.monotonic()
                    with lock:
                        stats["counts"][outcome] += 1
                        if outcome == "acknowledged":
                            stats["intended"].append(finished - intended)
                            stats["service"].append(finished - started)
                except Exception as error:
                    try:
                        worker_errors.put_nowait(repr(error))
                    except queue.Full:
                        pass
                finally:
                    tasks.task_done()
                    if task is not None:
                        with pending_condition:
                            pending[0] -= 1
                            pending_condition.notify_all()

        workers = [threading.Thread(target=worker, daemon=True) for _ in range(scenario["generator_workers"])]
        for thread in workers:
            thread.start()

        def check_running():
            forwarder.check_alive()
            try:
                error = worker_errors.get_nowait()
            except queue.Empty:
                return
            raise RuntimeError(f"load generator worker failed: {error}")

        def wait_until(deadline):
            while True:
                check_running()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                time.sleep(min(remaining, 0.05))

        start = time.monotonic()
        phase_start = start
        record_id = 0
        for index, phase in enumerate(scenario["phases"]):
            wait_until(phase_start)
            sink.set_mode(phase["sink_mode"], phase["delay_ms"])
            for event in range(phase["scheduled_count"]):
                intended = phase_start + event / phase["rate_per_second"]
                wait_until(intended)
                record_id += 1
                with lock:
                    phase_stats[index]["counts"]["scheduled"] += 1
                try:
                    enqueued = time.monotonic()
                    with pending_condition:
                        tasks.put_nowait((index, record_id, intended, enqueued))
                        pending[0] += 1
                except queue.Full:
                    with lock:
                        phase_stats[index]["counts"]["unsent_generator_full"] += 1
                else:
                    with lock:
                        phase_stats[index]["counts"]["queued"] += 1
            phase_start += phase["duration_seconds"]
        wait_until(phase_start)
        with pending_condition:
            while pending[0]:
                pending_condition.wait(timeout=0.05)
                check_running()
        check_running()
        for _ in workers:
            tasks.put(None)
        for thread in workers:
            thread.join(timeout=2)
            if thread.is_alive():
                raise RuntimeError("load generator worker did not stop")
        check_running()
        generator_elapsed = time.monotonic() - start
        drain_start = time.monotonic()
        forwarder_snapshot = drain_forwarder(forwarder, scenario["drain_timeout_ms"] / 1000)
        drain_elapsed = time.monotonic() - drain_start
        sink_snapshot = sink.snapshot_after_drain(scenario["sink_drain_timeout_ms"] / 1000)
        forwarder.stderr_thread.join(timeout=0.5)
        phases = []
        for stats in phase_stats:
            phases.append({"name": stats["name"], "counts": dict(stats["counts"]),
                           "intended_arrival_to_ack_ms": distribution(stats["intended"]),
                           "attempt_to_ack_ms": distribution(stats["service"]),
                           "intended_arrival_to_attempt_ms": distribution(stats["to_attempt"]),
                           "generator_queue_wait_ms": distribution(stats["queue"]),
                           "scheduler_lag_ms": distribution(stats["scheduler_lag"])})
        totals = collections.Counter()
        for stats in phase_stats:
            totals.update(stats["counts"])
        if forwarder_snapshot["state"] == "drained":
            final = forwarder_snapshot["final"]
            for client_name, server_name in (("acknowledged", "status_ok"),
                                             ("overloaded", "overloaded"),
                                             ("invalid", "status_invalid"),
                                             ("downstream_failure_or_unknown", "status_downstream_unknown")):
                if totals[client_name] > final[server_name]:
                    raise RuntimeError(f"client {client_name} exceeds forwarder {server_name}")
            reconciliation = {
                "client_connected_minus_server_accepted": totals["connected"] - final["accepted"],
                "server_overloaded_minus_client_observed": final["overloaded"] - totals["overloaded"],
                "comparison": "aggregate counts only; differences do not identify individual dispositions",
            }
        else:
            reconciliation = {"comparison": "unavailable after forced termination; last snapshot is not final"}
        return {
            "purpose": "functional smoke only; not a scored language comparison",
            "arrival_model": "open-loop, fixed scheduled rate per phase; bounded generator queue",
            "scenario": scenario,
            "binary": {"path": str(server_path), "sha256": hashlib.sha256(server_path.read_bytes()).hexdigest()},
            "environment": {"os": platform.platform(), "machine": platform.machine(), "logical_cpus": os.cpu_count(),
                            "python": sys.version.split()[0], "GOMAXPROCS": os.environ.get("GOMAXPROCS"),
                            "cpu_and_memory_quotas": "not measured or enforced"},
            "generator": {"state": "completed", "elapsed_seconds": round(generator_elapsed, 3)},
            "forwarder": {**forwarder_snapshot, "drain_elapsed_seconds": round(drain_elapsed, 3),
                          "stderr": forwarder.diagnostics()},
            "reconciliation": reconciliation,
            "elapsed_seconds": round(time.monotonic() - start, 3),
            "phases": phases,
            "sink": sink_snapshot,
            "limits": ["loopback synthetic protocol; no OTLP, auth, redaction, durable buffer, replay, query or projection",
                       "ack means only that the volatile sink echoed the identifier; no durability or visibility claim",
                       "timeout, transport and downstream failures may have completed at the sink",
                       "server-side response writes do not prove the client received them",
                       "kernel listen backlogs and connections not observed by the forwarder are not accounted",
                       "after forced termination the last snapshot is not a final disposition or an exact abort count",
                       "latencies exclude non-acknowledged attempts; counts expose censored and unsent work",
                       "sink ack_writes do not prove the client received acknowledgements",
                       "CPU, memory, deployment pressure, cost and residual remote work are not measured"],
        }
    finally:
        abort.set()
        if forwarder is not None:
            forwarder.close()
        until = time.monotonic() + 2
        for thread in workers:
            thread.join(timeout=max(0, until - time.monotonic()))
        sink.shutdown()
        sink.server_close()
        sink_thread.join()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True, help="path to one compiled forwarder")
    parser.add_argument("--scenario", required=True, help="JSON workload definition")
    parser.add_argument("--output", help="write results here as JSON instead of stdout")
    args = parser.parse_args()
    try:
        with open(args.scenario, encoding="utf-8") as source:
            scenario = json.load(source)
        result = run_scenario(args.server, scenario)
        encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
        if args.output:
            Path(args.output).write_text(encoded, encoding="utf-8")
        else:
            print(encoded, end="")
    except (OSError, ValueError, RuntimeError) as error:
        print(f"harness: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
