#!/usr/bin/env python3
"""Functional tests over real HTTP. No SDK, UI, agent or hosted account needed."""
import concurrent.futures
import gzip
import http.client
from http.server import BaseHTTPRequestHandler
import json
import os
from pathlib import Path
import selectors
import socket
import socketserver
import ssl
import struct
import subprocess
import tempfile
import threading
import time
import unittest

ROOT = Path(__file__).resolve().parents[3]
BIN = ROOT / "target/debug/observe-backend"
TRACE = "00112233445566778899aabbccddeeff"
SPAN = "1122334455667788"
PRIVATE = "synthetic-private-input"
EMPTY_PAGE = {"status": "success", "data": {"resultType": "streams", "result": []}}


def stop(p):
    if p.poll() is None:
        p.terminate()
        try:
            p.wait(timeout=3)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
    if p.stdout:
        p.stdout.close()


def envelope(records):
    attr = lambda k, v: {"key": k, "value": {"stringValue": v}}
    return {"resourceLogs": [{"resource": {"attributes": [attr("service.name", "claimed-foreign-service"),
                                                          attr("tenant.id", "bravo")]},
                               "scopeLogs": [{"scope": {"name": PRIVATE}, "logRecords": records}]}]}


def log(t, body=PRIVATE):
    return {"timeUnixNano": str(t), "observedTimeUnixNano": str(t + 500), "severityNumber": 17,
            "traceId": TRACE, "spanId": SPAN, "flags": 257, "body": {"stringValue": body},
            "attributes": [{"key": "private", "value": {"stringValue": PRIVATE}}]}


def wire_field(tag, data):
    # All fixture messages here are below 16 KiB.
    n = len(data)
    size = bytes([n]) if n < 128 else bytes([(n & 127) | 128, n >> 7])
    return bytes([tag << 3 | 2]) + size + data


def protobuf_log(t):
    r = (b'\x09' + struct.pack('<Q', t) + b'\x59' + struct.pack('<Q', t + 500) + b'\x10\x11'
         + wire_field(5, wire_field(1, b"visible log line")) + wire_field(9, bytes.fromhex(TRACE))
         + wire_field(10, bytes.fromhex(SPAN)))
    return wire_field(1, wire_field(2, wire_field(2, r)))


class Local(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="observe-", dir="/tmp")
        self.addCleanup(temp.cleanup)
        self.state = Path(temp.name)
        self.cli("init")
        self.start_backend()

    def cli(self, op):
        subprocess.run([BIN, op, self.state], check=True, stdout=subprocess.DEVNULL)

    def start_backend(self):
        self.backend = subprocess.Popen([BIN, "serve", self.state], stdout=subprocess.PIPE,
                                        env=dict(os.environ, OBSERVE_LISTEN="127.0.0.1:0"))
        self.addCleanup(stop, self.backend)
        with selectors.DefaultSelector() as sel:
            sel.register(self.backend.stdout, selectors.EVENT_READ)
            self.assertTrue(sel.select(5), "backend startup timeout")
        line = self.backend.stdout.readline().decode()
        self.assertTrue(line.startswith("LISTENING 127.0.0.1:"), "backend failed to bind privately")
        self.port = int(line.split(":")[-1])

    def call(self, path, token=None, payload=None, method="POST", headers=None):
        h = {"Content-Type": "application/json"}
        if token is not None:
            h["Authorization"] = "Bearer " + token
        if headers:
            h.update(headers)
        if payload is not None and not isinstance(payload, bytes):
            payload = json.dumps(payload).encode()
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=8)
        try:
            c.request(method, path, payload, h)
            resp = c.getresponse()
            data = resp.read()
            if data and resp.getheader("Content-Type") == "application/json":
                data = json.loads(data)
            return resp.status, data, resp.headers
        finally:
            c.close()

    def boot(self):
        self.ticket = (self.state / "bootstrap.token").read_text().strip()
        code, owner, _ = self.call("/api/v1/bootstrap", self.ticket)
        self.assertEqual(code, 201)
        self.owner = owner["token"]

    def tenant(self, name, capture="metadata_only"):
        self.assertEqual(self.call("/api/v1/tenants", self.owner, {"tenant": name, "capture": capture})[0], 201)

    def credential(self, name, *permissions):
        code, grant, _ = self.call(f"/api/v1/tenants/{name}/credentials", self.owner,
                                    {"service": "gateway", "permissions": permissions})
        self.assertEqual(code, 201)
        return grant

    def revoke(self, name, cred):
        self.assertEqual(self.call(f"/api/v1/tenants/{name}/credentials/{cred['id']}",
                                   self.owner, method="DELETE")[0], 204)

    def read(self, tenant, token, q, evidence=False):
        family = "evidence" if evidence else "logs"
        return self.call(f"/api/v1/tenants/{tenant}/{family}/query", token, q)


class AuthorityTests(Local):
    def test_private_concurrent_init_bootstrap_restart_and_recovery(self):
        self.assertEqual((self.state / "bootstrap.token").stat().st_mode & 0o777, 0o600)
        self.assertEqual((self.state / "authority.sqlite").stat().st_mode & 0o777, 0o600)
        empty = self.state / "fresh"
        with concurrent.futures.ThreadPoolExecutor(4) as pool:
            statuses = list(pool.map(lambda _: subprocess.run([BIN, "init", empty], capture_output=True).returncode, range(4)))
        self.assertEqual(statuses, [0] * 4)
        first = (empty / "bootstrap.token").read_text()
        subprocess.run([BIN, "init", empty], check=True)
        self.assertTrue(first == (empty / "bootstrap.token").read_text())
        empty.chmod(0o755)
        self.assertNotEqual(subprocess.run([BIN, "init", empty], capture_output=True).returncode, 0)
        empty.chmod(0o700)

        ticket = (self.state / "bootstrap.token").read_text().strip()
        with concurrent.futures.ThreadPoolExecutor(5) as pool:
            results = list(pool.map(lambda k: self.call("/api/v1/bootstrap", k), [ticket] * 4 + ["1" * 64]))
        self.assertEqual(sorted(r[0] for r in results), [201, 401, 401, 401, 401])
        self.owner = next(r[1]["token"] for r in results if r[0] == 201)
        self.tenant("alpha")
        reader = self.credential("alpha", "query")
        self.revoke("alpha", reader)
        stop(self.backend)
        self.cli("init")
        self.start_backend()
        self.assertEqual(self.call("/api/v1/bootstrap", ticket)[0], 401)
        self.assertEqual(self.read("alpha", reader["token"], {})[0], 401)
        old = self.owner
        self.cli("recover-owner")
        self.assertEqual(self.call("/api/v1/tenants", old, {"tenant": "r", "capture": "text_body"})[0], 403)
        self.boot()
        self.tenant("recovered")
        self.assertEqual(self.call("/api/v1/bootstrap", self.ticket)[0], 401)
        db = (self.state / "authority.sqlite").read_bytes()
        self.assertTrue(all(secret.encode() not in db for secret in (ticket, self.ticket, old, self.owner, reader["token"])))


class Sink(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True


class ControlledStore(Local):
    def setUp(self):
        super().setUp()
        self.boot()
        self.tenant("alpha")
        self.writer = self.credential("alpha", "ingest")["token"]
        self.reader = self.credential("alpha", "query")["token"]
        self.q = {"start_ns": "1000000000", "end_ns": "1000000001"}
        self.captured = []
        self.entered = threading.Event()
        self.release = threading.Event()
        self.hold = False
        self.drop_reply = False
        self.status = 204
        self.page = EMPTY_PAGE
        test = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                wire = self.rfile.read(int(self.headers["Content-Length"]))
                test.captured.append((self.headers["X-Scope-OrgID"], wire))
                test.entered.set()
                if test.hold:
                    test.release.wait(7)
                if not test.drop_reply:
                    self.send_response(test.status)
                    self.end_headers()

            def do_GET(self):
                test.captured.append((self.headers["X-Scope-OrgID"], self.headers["X-Loki-Query-Limits"]))
                test.entered.set()
                if test.hold:
                    test.release.wait(7)
                if not test.drop_reply:
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(json.dumps(test.page).encode())

        self.sink = Sink(str(self.state / "loki.sock:0"), Handler)
        thread = threading.Thread(target=self.sink.serve_forever, kwargs={"poll_interval": .02}, daemon=True)
        thread.start()
        self.addCleanup(self.sink.server_close)
        self.addCleanup(self.sink.shutdown)
        self.addCleanup(self.release.set)

    def test_delivery_counts_capture_at_store_and_failures(self):
        t = time.time_ns()
        batch = envelope([log(t), dict(log(t), traceId="aa")])
        code, data, h = self.call("/v1/logs", self.writer, batch)
        self.assertEqual((code, data["partialSuccess"]["rejectedLogRecords"]), (200, "1"))
        self.assertEqual([h["x-observe-" + k + "-records"] for k in ("accepted", "rejected", "unknown")], ["1", "1", "0"])
        self.assertEqual(self.captured[0][0], "alpha")
        self.assertNotIn(PRIVATE.encode(), self.captured[0][1])
        self.assertNotIn(b"claimed-foreign-service", self.captured[0][1])
        self.status = 400  # A Loki 400 may follow a partial commit.
        code, _, h = self.call("/v1/logs", self.writer, batch)
        self.assertEqual((code, h["x-observe-accepted-records"], h["x-observe-unknown-records"], h["x-observe-rejected-records"]),
                         (503, "0", "1", "1"))
        self.drop_reply = True
        code, _, h = self.call("/v1/logs", self.writer, envelope([log(t)]))
        self.assertEqual((code, h["x-observe-unknown-records"]), (503, "1"))
        self.drop_reply = False
        self.assertEqual(self.call("/v1/logs", self.writer, envelope([dict(log(t), severityNumber=99)]))[0], 400)
        self.assertEqual(self.call("/v1/logs", self.writer, {})[0], 200)
        self.assertEqual(len(self.captured), 3)

    def test_protojson_nulls_nonfinite_values_and_otlp_field_rules(self):
        t = time.time_ns()
        span = "aabbccdd55667788"
        records = [dict(log(t), timeUnixNano=None, observedTimeUnixNano=str(t), traceId=TRACE.upper(),
                        spanId=span.upper(), flags=None, body={"doubleValue": value})
                   for value in ("NaN", "Infinity", "-Infinity")]
        records.append({"timeUnixNano": f"{t}0e-1", "severityNumber": 9, "attributes": None,
                        "body": {"arrayValue": {"values": [{"doubleValue": "NaN"},
                                  {"kvlistValue": {"values": [{"key": "k", "value": {"doubleValue": "-Infinity"}}]}}]}}})
        records.append({"timeUnixNano": t, "severity_number": 17, "unknownNewField": {"traceId": []}})
        code, _, headers = self.call("/v1/logs", self.writer, envelope(records))
        self.assertEqual((code, headers.get("x-observe-accepted-records")), (200, "5"))
        values = json.loads(self.captured[-1][1])["streams"][0]["values"]
        decoded = [json.loads(line) for _, line in values]
        for row in decoded[:3]:
            self.assertEqual((row["event_ns"], row["observed_ns"], row["timestamp_ns"]), (None, str(t), str(t)))
            self.assertEqual((row["trace_id"], row["span_id"], row["trace_flags"]), (TRACE, span, 0))
            self.assertEqual((row["body_capture"], row["body"]), ("withheld_by_policy", None))
        self.assertEqual(decoded[3]["event_ns"], str(t))
        self.assertEqual(decoded[4]["severity_number"], 0)
        for fields in ({"severityNumber": "SEVERITY_NUMBER_INFO"}, {"attributes": [None]},
                       {"traceId": "not hex"}, {"timeUnixNano": "1.25"}, {"body": {"doubleValue": "unknown"}}):
            with self.subTest(fields=fields):
                self.assertEqual(self.call("/v1/logs", self.writer, envelope([dict(log(t), **fields)]))[0], 400)
        self.assertEqual(len(self.captured), 1)

    def test_invalid_source_fields_fail_closed_across_both_read_routes(self):
        t = time.time_ns()
        self.assertEqual(self.call("/v1/logs", self.writer, envelope([log(t)]))[0], 200)
        timestamp, line = json.loads(self.captured[-1][1])["streams"][0]["values"][0]
        row = json.loads(line)
        q = {"start_ns": str(t), "end_ns": str(t + 1)}

        def serve(record):
            self.page = {"status": "success", "data": {"resultType": "streams",
                         "result": [{"values": [[timestamp, json.dumps(record)]]}]}}

        for change in ({"event_ns": "bad-time"}, {"observed_ns": "0"}, {"received_ns": "-1"},
                       {"timestamp_ns": "0" + timestamp}, {"event_ns": str(t + 1)},
                       {"event_ns": None, "observed_ns": None, "received_ns": str(t + 1)},
                       {"trace_id": "invalid-id"}, {"trace_id": "0" * 32}, {"span_id": "0" * 16},
                       {"span_id": "a"}, {"trace_id": None}, {"span_id": "ABCDEF0123456789"},
                       {"trace_id": TRACE.upper()},
                       {"severity_number": 25}, {"service": ""}, {"tenant": "bravo"},
                       {"body_capture": "text", "body": PRIVATE}):
            serve(dict(row, **change))
            for evidence in (False, True):
                with self.subTest(fields=list(change), evidence=evidence):
                    code, data, _ = self.read("alpha", self.reader, q, evidence)
                    self.assertEqual((code, data["outcome"], data["effective_range"]), (503, "unavailable", None))
                    self.assertTrue({"records", "references", "provenance"}.isdisjoint(data))
                    self.assertNotIn(PRIVATE, json.dumps(data))
        serve(row)
        self.assertEqual(self.read("alpha", self.reader, q, True)[1]["records"], [row])
        self.page["data"]["result"][0]["values"].append([timestamp, json.dumps(dict(row, event_ns="invalid"))])
        code, data, _ = self.read("alpha", self.reader, dict(q, limit=1), True)
        self.assertEqual((code, data["outcome"]), (503, "unavailable"))
        self.assertTrue({"records", "references", "provenance"}.isdisjoint(data))

    def test_timestamp_dispositions_at_dispatch_and_ambiguous_ack(self):
        t = time.time_ns()
        expired = t - 72 * 3600 * 10**9
        outside = [{"observedTimeUnixNano": str(expired)}, log(t + 12 * 60 * 10**9)]
        code, _, h = self.call("/v1/logs", self.writer, envelope(outside))
        self.assertEqual((code, h.get("x-observe-rejected-records")), (400, "2"))
        self.assertFalse(self.captured)
        self.drop_reply = True
        code, _, h = self.call("/v1/logs", self.writer, envelope([log(t)] + outside))
        self.assertEqual((code, h.get("x-observe-rejected-records"), h.get("x-observe-unknown-records"),
                          h.get("x-observe-accepted-records")), (503, "2", "1", "0"))
        values = json.loads(self.captured[0][1])["streams"][0]["values"]
        self.assertEqual([timestamp for timestamp, _ in values], [str(t)])

    def test_wire_limits_filters_and_denials(self):
        t = time.time_ns()
        for token, expected in [(None, 401), ("wrong", 401), (self.reader, 403)]:
            self.assertEqual(self.call("/v1/logs", token, envelope([log(t)]))[0], expected)
        self.assertEqual(self.call("/v1/logs", self.writer, {}, headers={"X-Scope-OrgID": "bravo"})[0], 400)
        self.assertEqual(self.call("/v1/logs", self.writer, b"x" * 65537)[0], 413)
        self.assertEqual(self.call("/v1/logs", self.writer, gzip.compress(b" " * 65537), headers={"Content-Encoding": "gzip"})[0], 413)
        self.assertEqual(self.call("/v1/logs", self.writer, b"bad gzip", headers={"Content-Encoding": "gzip"})[0], 400)
        self.assertEqual(self.call("/v1/logs", self.writer, b"[", headers={"Content-Type": "text/plain"})[0], 415)
        self.assertEqual(self.call("/v1/logs", self.writer, b"[" * 40 + b"]" * 40)[0], 413)
        self.assertEqual(self.call("/v1/logs", self.writer, envelope([{}] * 257))[0], 413)
        many = wire_field(1, wire_field(2, wire_field(2, b"") * 257))
        self.assertEqual(self.call("/v1/logs", self.writer, many, headers={"Content-Type": "application/x-protobuf"})[0], 413)
        self.assertEqual(self.call("/v1/logs", self.writer, b"\x0a\xff", headers={"Content-Type": "application/x-protobuf"})[0], 400)
        for fields in ({"limit": 0}, {"limit": 101}, {"end_ns": "99999999999999"}, {"end_ns": self.q["start_ns"]},
                       {"trace_id": "wrong"}, {"native_query": "{}"}, {"service": 'a"b'}, {"deadline_ms": 3000}):
            self.assertEqual(self.read("alpha", self.reader, dict(self.q, **fields))[0], 400)
        self.assertEqual(len(self.captured), 0)

    def test_deadline_charges_until_completion_and_indeterminate_disconnect(self):
        self.hold = True
        code, timeout_result, _ = self.read("alpha", self.reader, dict(self.q, deadline_ms=75), True)
        self.assertEqual((code, timeout_result["outcome"], timeout_result["effective_range"]), (504, "unavailable", None))
        self.assertTrue(self.entered.is_set())
        self.assertIn("maxQueryTime", json.loads(self.captured[0][1]))
        for _ in range(3):
            self.assertEqual(self.read("alpha", self.reader, self.q)[0], 429)
        self.assertEqual(len(self.captured), 1)
        self.hold = False
        self.assertEqual(self.call("/v1/logs", self.writer, envelope([log(time.time_ns())]))[0], 200)
        self.release.set()
        for _ in range(100):
            result = self.read("alpha", self.reader, self.q)
            if result[0] == 200:
                break
            time.sleep(.01)
        self.assertEqual(result[0], 200)
        self.drop_reply = True
        self.assertEqual(self.read("alpha", self.reader, self.q)[0], 503)
        self.assertEqual(self.read("alpha", self.reader, self.q)[0], 429)

    def test_stalled_ingestion_reserves_capacity_for_peer_and_reads(self):
        self.tenant("bravo")
        other = self.credential("bravo", "ingest")["token"]
        self.hold = True
        batch = envelope([log(time.time_ns())])
        with concurrent.futures.ThreadPoolExecutor() as pool:
            pending = [pool.submit(self.call, "/v1/logs", self.writer, batch) for _ in range(2)]
            try:
                for _ in range(200):
                    if len(self.captured) == 2:
                        break
                    time.sleep(.005)
                self.assertEqual(len(self.captured), 2)
                self.assertEqual(self.call("/v1/logs", self.writer, batch)[0], 429)
                self.hold = False
                self.assertEqual(self.call("/v1/logs", other, batch)[0], 200)
                self.assertEqual(self.read("alpha", self.reader, self.q)[0], 200)
            finally:
                self.release.set()
            self.assertEqual([task.result()[0] for task in pending], [200, 200])

    def test_revoke_during_request_body_and_slow_body_deadline(self):
        grant = self.credential("alpha", "ingest")
        payload = json.dumps(envelope([log(time.time_ns())])).encode()
        for revoke in (True, False):
            token = grant["token"] if revoke else self.writer
            with socket.create_connection(("127.0.0.1", self.port), timeout=4) as conn:
                head = (f"POST /v1/logs HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\n"
                        f"Content-Type: application/json\r\nContent-Length: {len(payload)}\r\nExpect: 100-continue\r\n\r\n")
                conn.sendall(head.encode())
                response = b""
                while b"\r\n\r\n" not in response:
                    chunk = conn.recv(256)
                    self.assertTrue(bool(chunk))
                    response += chunk
                self.assertTrue(response.startswith(b"HTTP/1.1 100 "))
                if revoke:
                    self.revoke("alpha", grant)
                    conn.sendall(payload)
                final = http.client.HTTPResponse(conn)
                final.begin()
                self.assertEqual(final.status, 401 if revoke else 408)
                final.close()
        self.assertEqual(len(self.captured), 0)

    def test_source_warning_malformed_reply_and_inflight_revocation(self):
        self.page = dict(EMPTY_PAGE, warnings=["private adapter diagnostic"])
        _, data, _ = self.read("alpha", self.reader, self.q, True)
        self.assertEqual((data["outcome"], data["effective_range"]), ("partial", None))
        self.assertNotIn("private adapter diagnostic", json.dumps(data))
        self.page = {"status": "success", "data": {}}
        self.assertEqual(self.read("alpha", self.reader, self.q, True)[0], 503)
        self.hold = True
        self.entered.clear()
        reader = self.credential("alpha", "query")
        with concurrent.futures.ThreadPoolExecutor() as pool:
            pending = pool.submit(self.read, "alpha", reader["token"], self.q, True)
            self.assertTrue(self.entered.wait(2))
            self.revoke("alpha", reader)
            self.release.set()
            self.assertEqual(pending.result()[0:2], (401, {"outcome": "denied"}))


class RealLoki(Local):
    def visible(self, tenant, token, query, count):
        # Acknowledgement and visibility are separate observations.
        deadline = time.monotonic() + 6
        while True:
            status, data, _ = self.read(tenant, token, query)
            if status == 200 and len(data["records"]) == count:
                return data
            if time.monotonic() >= deadline or status not in (200, 429, 503, 504):
                self.fail(f"expected {count} visible rows in {tenant}, last HTTP status {status}")
            time.sleep(.05)

    def start_loki(self):
        with socket.socket() as port:
            port.bind(("127.0.0.1", 0))
            grpc_port = port.getsockname()[1]
        diagnostics = (self.state / "loki-test.log").open("wb")
        self.addCleanup(diagnostics.close)
        loki = subprocess.Popen(["python3", ROOT / "scripts/run-local-loki.py", self.state],
                                env=dict(os.environ, OBSERVE_LOKI_GRPC_PORT=str(grpc_port)),
                                stdout=diagnostics, stderr=subprocess.STDOUT)
        self.addCleanup(stop, loki)
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            self.assertIsNone(loki.poll(), "private Loki failed to start")
            try:
                with socket.socket(socket.AF_UNIX) as s:
                    s.settimeout(1)
                    s.connect(str(self.state / "loki.sock:0"))
                    s.sendall(b"GET /ready HTTP/1.0\r\nHost: localhost\r\n\r\n")
                    if b" 200 " in s.recv(500):
                        break
            except OSError:
                pass
            time.sleep(.1)
        else:
            self.fail("Loki readiness timeout")
        tls = ssl.create_default_context(cafile=str(self.state / "loki.crt"))
        tls.maximum_version = ssl.TLSVersion.TLSv1_2
        with socket.create_connection(("127.0.0.1", grpc_port), timeout=2) as conn:
            with self.assertRaises(ssl.SSLError):
                tls.wrap_socket(conn, server_hostname="localhost")
        return loki

    def test_byte_budget_partial_page_and_subsequent_read(self):
        self.boot()
        self.tenant("alpha", "text_body")
        token = self.credential("alpha", "ingest", "query")["token"]
        self.start_loki()
        t = time.time_ns() - 10**9
        # Ordinary maximum-length text records. The wire budget reserves space
        # for every permitted encoding independently of this fixture's contents.
        body = "application log ".ljust(4096, ".")
        for start in range(0, 100, 10):
            records = [log(t + n, body) for n in range(start, start + 10)]
            code, _, h = self.call("/v1/logs", token, envelope(records))
            self.assertEqual((code, h.get("x-observe-accepted-records")), (200, "10"))
        self.visible("alpha", token, {"start_ns": str(t + 99), "end_ns": str(t + 100)}, 1)
        q = {"start_ns": str(t), "end_ns": str(t + 100), "limit": 100}
        for evidence in (False, True):
            code, page, _ = self.read("alpha", token, q, evidence)
            self.assertEqual((code, page["outcome"], page["coverage"]), (200, "partial", "unknown"))
            self.assertIn("response_byte_limit", page["limitations"])
            count = len(page["records"])
            self.assertTrue(0 < count < 100)
            self.assertEqual([r["event_ns"] for r in page["records"]], [str(t + n) for n in range(count)])
            self.assertEqual(page["effective_range"]["end_ns"], str(t + count))
            if evidence:
                self.assertEqual(len(page["references"]), count)
        code, one, _ = self.read("alpha", token, dict(q, start_ns=str(t + 99), limit=1))
        self.assertEqual((code, one["outcome"], one["records"][0]["body"]), (200, "matches", body))
        # Filling the budget exactly, with no lookahead row, is a successful full page.
        exact = self.read("alpha", token, dict(q, end_ns=str(t + count)))[1]
        self.assertEqual((exact["outcome"], len(exact["records"])), ("matches", count))

    def test_timestamp_rejection_and_visible_eligible_subset(self):
        self.boot()
        self.tenant("alpha", "text_body")
        token = self.credential("alpha", "ingest", "query")["token"]
        self.start_loki()
        t = time.time_ns()
        minute = 60 * 10**9
        expired = t - 72 * 60 * minute
        code, _, h = self.call("/v1/logs", token, protobuf_log(expired),
                                headers={"Content-Type": "application/x-protobuf"})
        self.assertEqual((code, h.get("x-observe-rejected-records"), h.get("x-observe-unknown-records")), (400, "1", "0"))
        code, data, h = self.call("/v1/logs", token, envelope([log(expired), log(t, "eligible")]))
        self.assertEqual((code, data.get("partialSuccess", {}).get("rejectedLogRecords")), (200, "1"))
        self.assertEqual((h.get("x-observe-accepted-records"), h.get("x-observe-unknown-records")), ("1", "0"))
        page = self.visible("alpha", token, {"start_ns": str(t), "end_ns": str(t + 1)}, 1)
        self.assertEqual([r["body"] for r in page["records"]], ["eligible"])
        # A recently written future-eligible row may precede in-window history on the same stream.
        late = t - (48 * 60 - 1) * minute
        for stamp in (t + 9 * minute, late):
            self.assertEqual(self.call("/v1/logs", token, envelope([log(stamp)]))[0], 200)
        self.visible("alpha", token, {"start_ns": str(late), "end_ns": str(late + 1)}, 1)

    def test_two_tenants_otlp_raw_evidence_and_offline_store(self):
        self.boot()
        self.tenant("alpha", "text_body")
        self.tenant("bravo")
        writer = self.credential("alpha", "ingest")
        reader = self.credential("alpha", "query")
        other = self.credential("bravo", "ingest", "query")
        loki = self.start_loki()

        t = time.time_ns() - 1_000_000_000
        q = {"start_ns": str(t), "end_ns": str(t + 100), "trace_id": TRACE, "service": "gateway"}
        wire = gzip.compress(protobuf_log(t))
        code, protobuf, h = self.call("/v1/logs", writer["token"], wire,
                                      headers={"Content-Type": "application/x-protobuf", "Content-Encoding": "gzip"})
        self.assertEqual((code, protobuf, h["x-observe-accepted-records"]), (200, b"", "1"))
        # Two different tenants present the same submitted correlation and resource claims.
        alpha = envelope([log(t + 2, "second log"), dict(log(t), traceId="aa")])
        code, partial, _ = self.call("/v1/logs", writer["token"], alpha)
        self.assertEqual((code, partial.get("partialSuccess", {}).get("rejectedLogRecords")), (200, "1"))
        self.assertEqual(self.call("/v1/logs", other["token"], envelope([log(t)]))[0], 200)
        self.assertEqual(self.call("/v1/logs", reader["token"], alpha)[0], 403)
        self.assertEqual(self.read("alpha", writer["token"], q)[0], 403)

        raw = self.visible("alpha", reader["token"], q, 2)
        self.assertEqual(raw["outcome"], "matches")
        records = raw["records"]
        self.assertEqual([r["body"] for r in records], ["visible log line", "second log"])
        self.assertEqual([r["timestamp_ns"] for r in records], [str(t), str(t + 2)])
        self.assertEqual((records[0]["event_ns"], records[0]["observed_ns"]), (str(t), str(t + 500)))
        self.assertEqual((records[0]["trace_id"], records[0]["span_id"], records[1]["trace_flags"]), (TRACE, SPAN, 1))
        self.assertTrue(all(r["producer_id"] == writer["id"] and r["service"] == "gateway" for r in records))
        scoped = self.visible("bravo", other["token"], q, 1)
        self.assertEqual(len(scoped["records"]), 1)
        r = scoped["records"][0]
        self.assertEqual((r["body"], r["body_capture"], r["producer_id"]), (None, "withheld_by_policy", other["id"]))
        self.assertNotIn(PRIVATE, json.dumps(scoped))
        for evidence in (False, True):
            known = self.read("alpha", other["token"], q, evidence)[:2]
            absent = self.read("missing", other["token"], q, evidence)[:2]
            self.assertEqual(known, absent)
            self.assertEqual(known, (403, {"outcome": "denied"}))

        _, ev, _ = self.read("alpha", reader["token"], q, True)
        self.assertEqual(ev["records"], records)
        self.assertEqual((ev["coverage"], ev["outcome"]), ("unknown", "matches"))
        self.assertEqual(ev["requested_range"], {"start_ns": str(t), "end_ns": str(t + 100)})
        self.assertEqual(ev["source_query_range"], ev["requested_range"])
        self.assertEqual(ev["effective_range"], {"start_ns": str(t), "end_ns": str(t + 3)})
        self.assertEqual((ev["provenance"]["tenant"], ev["provenance"]["source"]), ("alpha", "local-loki-v1"))
        self.assertGreaterEqual(int(ev["provenance"]["retrieved_at_ns"]), t)
        self.assertEqual(len(ev["references"]), 2)
        ref = ev["references"][0]
        resolved = self.call(ref["path"], reader["token"], ref["query"])
        self.assertEqual((resolved[0], resolved[1]["records"]), (200, records[:1]))
        self.assertEqual(self.call(ref["path"], other["token"], ref["query"])[:2], (403, {"outcome": "denied"}))
        self.assertEqual(self.read("bravo", other["token"], ref["query"])[1]["outcome"], "no_matches")
        limited = self.read("alpha", reader["token"], dict(q, limit=1), True)[1]
        self.assertEqual((limited["outcome"], len(limited["references"])), ("partial", 1))
        self.assertIn("row_limit", limited["limitations"])
        empty = self.read("alpha", reader["token"], dict(q, start_ns=str(t + 1), end_ns=str(t + 2)), True)[1]
        self.assertEqual((empty["outcome"], empty["coverage"], empty["effective_range"]), ("no_matches", "unknown", None))

        # Missing event time remains explicitly absent with observed/receive fallback.
        fallbacks = [{"observedTimeUnixNano": str(t + 20)}, {}]
        self.assertEqual(self.call("/v1/logs", writer["token"], envelope(fallbacks))[0], 200)
        timed = self.visible("alpha", reader["token"],
                             {"start_ns": str(t + 20), "end_ns": str(time.time_ns() + 1)}, 2)
        self.assertEqual(len(timed["records"]), 2)
        observed, received = timed["records"]
        self.assertEqual((observed["event_ns"], observed["observed_ns"], observed["timestamp_ns"]),
                         (None, str(t + 20), str(t + 20)))
        self.assertEqual((received["event_ns"], received["observed_ns"], received["timestamp_ns"]),
                         (None, None, received["received_ns"]))

        # Unavailable and empty searches have different results on exactly the same request.
        stop(loki)
        status, unavailable, _ = self.read("alpha", reader["token"], q, True)
        self.assertEqual((status, unavailable["outcome"], unavailable["effective_range"]), (503, "unavailable", None))
        self.assertEqual(unavailable["requested_range"], ev["requested_range"])
        status, _, h = self.call("/v1/logs", writer["token"], envelope([log(t)]))
        self.assertEqual((status, h["x-observe-unsubmitted-records"], h["x-observe-unknown-records"]), (503, "1", "0"))
        self.revoke("alpha", reader)
        self.assertEqual(self.call(ref["path"], reader["token"], ref["query"])[:2], (401, {"outcome": "denied"}))
        self.assertNotIn(PRIVATE.encode(), (self.state / "loki-test.log").read_bytes())


if __name__ == "__main__":
    unittest.main()
