import copy
import importlib.util
import json
import os
from pathlib import Path
import select
import shutil
import signal
import socket
import struct
import subprocess
import tempfile
import threading
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
HARNESS_PATH = ROOT / "benchmarks/ingest/harness.py"
spec = importlib.util.spec_from_file_location("ingest_harness", HARNESS_PATH)
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)
SERVERS = {
    "rust": Path(os.environ.get("OBSERVE_BENCH_RUST", ROOT / "target/debug/ingest-bench-rust")),
    "go": Path(os.environ.get("OBSERVE_BENCH_GO", ROOT / "benchmarks/ingest/bin/ingest-bench-go")),
}


def phase(name, duration, rate, **extra):
    return {"name": name, "duration_seconds": duration, "rate_per_second": rate, **extra}


def scenario(phases, **extra):
    return {
        "workers": 2, "queue_capacity": 2, "generator_workers": 4,
        "generator_queue_capacity": 16, "server_timeout_ms": 300,
        "client_timeout_ms": 700, "payload_bytes": 32, "max_payload_bytes": 1024,
        "phases": phases, **extra,
    }


def assert_accounting(test, result, allow_outstanding=False):
    test.assertEqual(result["generator"]["state"], "completed")
    total_acknowledged = 0
    total_overloaded = 0
    for phase_result in result["phases"]:
        counts = phase_result["counts"]
        get = lambda key: counts.get(key, 0)
        test.assertEqual(get("scheduled"), get("queued") + get("unsent_generator_full"), phase_result)
        test.assertEqual(get("queued"), get("attempted"), phase_result)
        outcomes = ["acknowledged", "overloaded", "downstream_failure_or_unknown", "invalid",
                    "timeout_or_unknown", "transport_error_or_unknown", "protocol_error_or_unknown"]
        test.assertEqual(get("attempted"), sum(get(key) for key in outcomes), phase_result)
        test.assertLessEqual(get("sent"), get("connected"), phase_result)
        test.assertLessEqual(get("connected"), get("attempted"), phase_result)
        total_acknowledged += get("acknowledged")
        total_overloaded += get("overloaded")
    test.assertEqual(result["sink"].get("payload_mismatches", 0), 0)
    forwarder = result["forwarder"]
    if not allow_outstanding:
        test.assertEqual(forwarder["state"], "drained")
        test.assertTrue(forwarder["accepted_connection_accounting_complete"])
        test.assertEqual(forwarder["process_exit_code"], 0)
        test.assertEqual(result["sink"]["active_handlers_at_snapshot"], 0)
        final = forwarder["final"]
        test.assertEqual(final["queued"] + final["in_flight"] + final["overload_writing"], 0)
        test.assertGreaterEqual(final["status_ok"], total_acknowledged)
        test.assertGreaterEqual(final["overloaded"], total_overloaded)


class ForwarderConformance(unittest.TestCase):
    def test_health_disconnect_reconnect_and_accounting(self):
        workload = scenario([
            phase("healthy", 0.35, 20),
            phase("disconnected", 0.35, 20, sink_mode="disconnect"),
            phase("reconnected", 0.35, 20),
        ])
        for language, binary in SERVERS.items():
            with self.subTest(language=language):
                result = harness.run_scenario(binary, copy.deepcopy(workload))
                assert_accounting(self, result)
                healthy, disconnected, reconnected = result["phases"]
                self.assertGreater(healthy["counts"].get("acknowledged", 0), 0)
                self.assertGreater(reconnected["counts"].get("acknowledged", 0), 0)
                self.assertGreater(disconnected["counts"].get("downstream_failure_or_unknown", 0), 0)
                self.assertEqual(disconnected["counts"].get("acknowledged", 0), 0)
                self.assertGreater(result["sink"].get("simulated_disconnects", 0), 0)

    def test_stall_exposes_unsent_timeouts_and_latency(self):
        workload = scenario([
            phase("healthy", 0.3, 10),
            phase("stall", 0.45, 100, delay_ms=150),
            phase("settle", 0.8, 1.25),
            phase("timeout", 0.45, 100, delay_ms=500),
        ], workers=1, queue_capacity=1, generator_workers=1, generator_queue_capacity=1,
           server_timeout_ms=350, client_timeout_ms=250)
        for language, binary in SERVERS.items():
            with self.subTest(language=language):
                result = harness.run_scenario(binary, copy.deepcopy(workload))
                assert_accounting(self, result)
                healthy, stalled, _, timeout = result["phases"]
                self.assertGreater(healthy["counts"].get("acknowledged", 0), 0)
                self.assertGreater(stalled["counts"].get("acknowledged", 0), 0)
                self.assertGreater(stalled["counts"].get("unsent_generator_full", 0), 0)
                self.assertGreater(stalled["intended_arrival_to_ack_ms"]["p95"], 100)
                self.assertGreater(timeout["counts"].get("timeout_or_unknown", 0), 0)
                self.assertGreater(timeout["counts"].get("unsent_generator_full", 0), 0)

    def test_client_timeout_can_follow_sink_receipt(self):
        workload = scenario(
            [phase("ambiguous", 0.5, 2, delay_ms=400)],
            workers=1, queue_capacity=1, generator_workers=1,
            generator_queue_capacity=1, server_timeout_ms=1000, client_timeout_ms=100,
        )
        for language, binary in SERVERS.items():
            with self.subTest(language=language):
                result = harness.run_scenario(binary, copy.deepcopy(workload))
                assert_accounting(self, result)
                counts = result["phases"][0]["counts"]
                self.assertEqual(counts.get("timeout_or_unknown", 0), 1)
                self.assertEqual(counts.get("acknowledged", 0), 0)
                self.assertEqual(result["sink"].get("valid_frames", 0), 1)
                self.assertEqual(result["forwarder"]["final"]["status_ok"], 1)

    def test_two_timed_out_clients_do_not_hide_queued_work(self):
        workload = scenario(
            [phase("two_timeouts", 0.1, 20, delay_ms=500)],
            workers=1, queue_capacity=1, generator_workers=2,
            generator_queue_capacity=2, server_timeout_ms=2500, client_timeout_ms=100,
        )
        for language, binary in SERVERS.items():
            with self.subTest(language=language):
                result = harness.run_scenario(binary, copy.deepcopy(workload))
                assert_accounting(self, result)
                counts = result["phases"][0]["counts"]
                self.assertEqual(counts.get("timeout_or_unknown", 0), 2)
                self.assertEqual(counts.get("sent", 0), 1)
                before = result["forwarder"]["before_drain"]
                self.assertEqual((before["queued"], before["in_flight"]), (1, 1))
                final = result["forwarder"]["final"]
                self.assertEqual((final["admitted"], final["completed"]), (2, 2))
                self.assertEqual(final["status_ok"], 1)
                self.assertEqual(final["ready_write_failed"] + final["status_invalid"], 1)
                self.assertEqual(result["sink"].get("valid_frames", 0), 1)

    def test_bounded_drain_reports_unresolved_work_without_inventing_aborts(self):
        workload = scenario(
            [phase("two_timeouts", 0.1, 20, delay_ms=700)],
            workers=1, queue_capacity=1, generator_workers=2,
            generator_queue_capacity=2, server_timeout_ms=2500, client_timeout_ms=100,
            drain_timeout_ms=30, sink_drain_timeout_ms=10,
        )
        for language, binary in SERVERS.items():
            with self.subTest(language=language):
                result = harness.run_scenario(binary, copy.deepcopy(workload))
                assert_accounting(self, result, allow_outstanding=True)
                self.assertEqual(result["phases"][0]["counts"].get("timeout_or_unknown", 0), 2)
                forwarder = result["forwarder"]
                self.assertEqual(forwarder["state"], "forced_after_timeout")
                self.assertFalse(forwarder["accepted_connection_accounting_complete"])
                self.assertIn(forwarder["process_exit_code"], (-signal.SIGTERM, -signal.SIGKILL))
                self.assertIn(forwarder["termination_signal"], ("SIGTERM", "SIGKILL"))
                last = forwarder["last_snapshot_before_termination"]
                self.assertEqual((last["queued"], last["in_flight"]), (1, 1))
                self.assertIn("unknown", forwarder["aborted_after_snapshot"])
                self.assertEqual(result["sink"]["active_handlers_at_snapshot"], 1)

    def test_bounded_receiver_reports_overload(self):
        workload = scenario(
            [phase("burst", 0.4, 100, delay_ms=150)],
            workers=1, queue_capacity=1, generator_workers=16,
            generator_queue_capacity=128, server_timeout_ms=600, client_timeout_ms=1000,
        )
        for language, binary in SERVERS.items():
            with self.subTest(language=language):
                result = harness.run_scenario(binary, copy.deepcopy(workload))
                assert_accounting(self, result)
                counts = result["phases"][0]["counts"]
                self.assertGreater(counts.get("overloaded", 0), 0)
                self.assertGreater(counts.get("acknowledged", 0), 0)

    def test_maximum_frames_under_overload_have_server_side_accounting(self):
        workload = scenario(
            [phase("large_burst", 0.4, 100, delay_ms=150)],
            workers=1, queue_capacity=1, generator_workers=16,
            generator_queue_capacity=128, server_timeout_ms=1200, client_timeout_ms=1500,
            payload_bytes=harness.MAX_PAYLOAD, max_payload_bytes=harness.MAX_PAYLOAD,
        )
        for language, binary in SERVERS.items():
            with self.subTest(language=language):
                result = harness.run_scenario(binary, copy.deepcopy(workload))
                assert_accounting(self, result)
                counts = result["phases"][0]["counts"]
                final = result["forwarder"]["final"]
                self.assertGreater(counts.get("acknowledged", 0), 0)
                self.assertGreater(counts.get("overloaded", 0), 0)
                self.assertGreaterEqual(final["overloaded"], counts["overloaded"])
                self.assertEqual(final["overloaded"], final["overload_write_ok"] + final["overload_write_failed"])
                self.assertGreater(result["sink"].get("valid_frames", 0), 0)

    def test_wrong_ack_and_oversized_frame_are_not_success(self):
        for language, binary in SERVERS.items():
            with self.subTest(language=language):
                listener = socket.socket()
                listener.bind(("127.0.0.1", 0))
                listener.listen()
                listener.settimeout(3)

                def wrong_ack():
                    try:
                        conn, _ = listener.accept()
                        with conn:
                            harness.recv_exact(conn, 12 + 4)
                            conn.sendall(struct.pack("!Q", 999))
                    finally:
                        listener.close()

                thread = threading.Thread(target=wrong_ack, daemon=True)
                thread.start()
                process = subprocess.Popen([
                    str(binary), "--listen", "127.0.0.1:0", "--downstream",
                    f"127.0.0.1:{listener.getsockname()[1]}", "--workers", "1", "--queue", "1",
                    "--timeout-ms", "500", "--max-payload", "16",
                ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    self.assertTrue(select.select([process.stdout], [], [], 5)[0], "no listener announcement")
                    line = process.stdout.readline().strip()
                    self.assertTrue(line.startswith("LISTENING 127.0.0.1:"), line)
                    address = ("127.0.0.1", int(line.rsplit(":", 1)[1]))
                    with socket.create_connection(address, timeout=2) as conn:
                        self.assertEqual(conn.recv(1), b"\x04")
                        conn.sendall(struct.pack("!QI", 1, 17))
                        self.assertEqual(conn.recv(1), b"\x03")
                    with socket.create_connection(address, timeout=2) as conn:
                        self.assertEqual(conn.recv(1), b"\x04")
                        conn.sendall(struct.pack("!QI", 2, 4) + b"data")
                        self.assertEqual(conn.recv(1), b"\x02")
                    thread.join(3)
                    self.assertFalse(thread.is_alive())
                finally:
                    process.terminate()
                    process.wait(timeout=5)
                    process.stdout.close()
                    process.stderr.close()
                    listener.close()

    def test_non_loopback_configuration_is_rejected(self):
        for language, binary in SERVERS.items():
            with self.subTest(language=language):
                result = subprocess.run([str(binary), "--listen", "0.0.0.0:19091"],
                                        capture_output=True, text=True, timeout=5, check=False)
                self.assertNotEqual(result.returncode, 0)

    def test_killed_forwarder_fails_the_scenario(self):
        workload = scenario([phase("crash", 1, 20)])
        real_popen = subprocess.Popen
        real_sink_handler = harness.SinkHandler.handle
        for language, binary in SERVERS.items():
            with self.subTest(language=language):
                timers = []
                killed = threading.Event()
                sink_reached = threading.Event()

                def observe_sink(handler):
                    sink_reached.set()
                    return real_sink_handler(handler)

                def launch(*args, **kwargs):
                    process = real_popen(*args, **kwargs)
                    def kill():
                        if sink_reached.wait(timeout=3) and process.poll() is None:
                            process.kill()
                            killed.set()
                    timer = threading.Thread(target=kill, daemon=True)
                    timers.append(timer)
                    timer.start()
                    return process

                try:
                    with mock.patch.object(harness.SinkHandler, "handle", new=observe_sink):
                        with mock.patch.object(harness.subprocess, "Popen", side_effect=launch):
                            with self.assertRaisesRegex(RuntimeError, "forwarder exited unexpectedly") as caught:
                                harness.run_scenario(binary, copy.deepcopy(workload))
                finally:
                    for timer in timers:
                        timer.join()
                self.assertTrue(killed.is_set(), "test did not kill the forwarder")
                self.assertTrue(sink_reached.is_set(), "test did not start downstream work")
                self.assertIn(f"exit {-signal.SIGKILL}", str(caught.exception))


class HarnessValidation(unittest.TestCase):
    def test_sink_ack_attempts_include_failed_writes(self):
        for fails in (False, True):
            with self.subTest(fails=fails), harness.Sink(1, 16, 1) as sink:
                conn = mock.Mock()
                conn.recv.side_effect = [struct.pack("!QI", 1, 1), b"\x01"]
                if fails:
                    conn.sendall.side_effect = BrokenPipeError()
                harness.SinkHandler(conn, ("127.0.0.1", 0), sink)
                self.assertEqual(sink.stats["ack_write_attempts"], 1)
                self.assertEqual(sink.stats["ack_writes"], int(not fails))
                self.assertEqual(sink.stats["ack_write_failures"], int(fails))

    def test_inconsistent_forwarder_counters_are_rejected(self):
        keys = ("accepted", "admitted", "overloaded", "overload_writing", "overload_write_ok",
                "overload_write_failed", "queued", "in_flight", "completed", "ready_write_failed",
                "status_ok", "status_downstream_unknown", "status_invalid", "response_write_ok",
                "response_write_failed")
        counts = dict.fromkeys(keys, 0)
        counts["admitted"] = 1
        with self.assertRaisesRegex(RuntimeError, "counter conservation failed"):
            harness.parse_counters("SNAPSHOT " + json.dumps(counts), "SNAPSHOT")

    def test_forwarder_exit_during_drain_fails_the_scenario(self):
        with tempfile.TemporaryDirectory() as directory:
            fake = Path(directory) / "drain-crash"
            keys = ("accepted", "admitted", "overloaded", "overload_writing", "overload_write_ok",
                    "overload_write_failed", "queued", "in_flight", "completed", "ready_write_failed",
                    "status_ok", "status_downstream_unknown", "status_invalid", "response_write_ok",
                    "response_write_failed")
            response = "SNAPSHOT " + json.dumps(dict.fromkeys(keys, 0))
            with socket.socket() as unused:
                unused.bind(("127.0.0.1", 0))
                port = unused.getsockname()[1]
            fake.write_text("#!/usr/bin/env python3\nimport sys\n"
                            f"print('LISTENING 127.0.0.1:{port}', flush=True)\n"
                            "for line in sys.stdin:\n"
                            f"    if line.strip() == 'SNAPSHOT': print({response!r}, flush=True)\n"
                            "    if line.strip() == 'DRAIN':\n"
                            "        print('drain crashed', file=sys.stderr, flush=True)\n"
                            "        sys.exit(42)\n", encoding="utf-8")
            fake.chmod(0o700)
            with self.assertRaises(RuntimeError) as caught:
                harness.run_scenario(fake, scenario([phase("drain_crash", 0.1, 10)]))
            self.assertIn("exit 42", str(caught.exception))
            self.assertIn("drain crashed", str(caught.exception))

    def test_stderr_diagnostics_are_bounded_and_include_crash_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            fake = Path(directory) / "crashing-forwarder"
            fake.write_text("#!/usr/bin/env python3\nimport os, sys, time\n"
                            "print('LISTENING 127.0.0.1:1', flush=True)\n"
                            "os.write(2, b'x' * 20000 + b'CRASH_TAIL')\n"
                            "time.sleep(0.2)\nsys.exit(42)\n", encoding="utf-8")
            fake.chmod(0o700)
            with self.assertRaises(RuntimeError) as caught:
                harness.run_scenario(fake, scenario([phase("crash", 1, 20)]))
            message = str(caught.exception)
            self.assertIn("exit 42", message)
            self.assertIn("truncated=True", message)
            self.assertIn("CRASH_TAIL", message)
            self.assertLess(len(message), harness.MAX_DIAGNOSTIC_BYTES + 256)

    def test_shell_check_validates_each_script(self):
        with tempfile.TemporaryDirectory() as directory:
            scripts = Path(directory) / "scripts"
            scripts.mkdir()
            shutil.copy2(ROOT / "scripts/check.sh", scripts / "check.sh")
            (scripts / "later-invalid.sh").write_text("#!/bin/sh\nif then\n", encoding="utf-8")
            result = subprocess.run(["sh", str(scripts / "check.sh")], capture_output=True,
                                    text=True, timeout=5, check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("later-invalid.sh", result.stderr)

    def test_invalid_and_unbounded_scenarios_are_rejected(self):
        for bad in (
            scenario([phase("duplicate", 1, 1), phase("duplicate", 1, 1)]),
            scenario([phase("zero", 1, 0)]),
            scenario([phase("too many", 2, 1_000_000)]),
            scenario([phase("bad mode", 1, 1, sink_mode="unknown")]),
            scenario([phase("payload", 1, 1)], payload_bytes=1025, max_payload_bytes=1024),
        ):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    harness.validate_scenario(bad)


if __name__ == "__main__":
    unittest.main()
