#!/usr/bin/env python3
"""v0.3 product workflow against real OSS stores, plus deterministic failure injection."""
import base64
import concurrent.futures
import gzip
from http.server import BaseHTTPRequestHandler
import json
import os
import socket
import sqlite3
import struct
import subprocess
import threading
import time
import unittest

import test_headless
from test_headless import Local, ROOT, Sink, SPAN, TRACE, envelope, log, stop, wire_field

PARENT = "abababababababab"
OTHER = "12" * 16
SECRET = "synthetic-trace-content-canary"
OTLP = {"Content-Type": "application/x-protobuf"}


def attr(key, value):
    return {"key": key, "value": {"stringValue": value}}


def span(t, ident=SPAN, parent=PARENT):
    return {"traceId": TRACE.upper(), "spanId": ident, "parentSpanId": parent, "name": SECRET,
            "startTimeUnixNano": str(t), "endTimeUnixNano": str(t + 123),
            "kind": 3, "flags": 257, "traceState": "vendor=abc", "attributes": [attr("future.agent.field", SECRET)],
            "status": {"code": 2, "message": SECRET},
            "events": [{"timeUnixNano": str(t + 1), "name": SECRET, "attributes": [attr("model", SECRET)]}],
            "links": [{"traceId": OTHER, "spanId": PARENT, "attributes": [attr("context", SECRET)]}]}


def export(*spans):
    return {"resourceSpans": [{"resource": {"attributes": [attr("service.name", "claimed-service"),
                                                          attr("tenant.id", "bravo"), attr("observe.producer", SECRET)]},
                               "schemaUrl": "urn:resource", "scopeSpans": [
                                   {"scope": {"name": SECRET, "version": "1.0"}, "schemaUrl": "urn:scope",
                                    "spans": list(spans)}]}]}


def protobuf_parent(t):
    # ExportTraceServiceRequest -> ResourceSpans -> ScopeSpans -> Span.
    fields = (wire_field(1, bytes.fromhex(TRACE)) + wire_field(2, bytes.fromhex(PARENT))
              + wire_field(5, b"checkout") + b'\x39' + struct.pack('<Q', t)
              + b'\x41' + struct.pack('<Q', t + 150))
    return wire_field(1, wire_field(2, wire_field(2, fields)))


def stored_payload(wire):
    # Read the adapter's KeyValue.value -> AnyValue.string_value. Protobuf
    # length bytes can themselves be valid base64 characters.
    def read_field(data, tag):
        assert data[0] == (tag << 3 | 2)
        size = 0
        for i, byte in enumerate(data[1:6]):
            size |= (byte & 127) << (7 * i)
            if byte < 128:
                start = i + 2
                assert start + size <= len(data)
                return data[start:start + size]
        raise AssertionError("invalid fixture field length")

    key = wire_field(1, b"observe.unit")
    _, marker, value = wire.partition(key)
    assert marker
    return read_field(read_field(value, 2), 1)


class TraceHarness(Local):
    def setUp(self):
        super().setUp()
        self.boot()
        self.assertEqual(self.call("/api/v1/tenants", self.owner,
                                   {"tenant": "alpha", "capture": "text_body", "trace_capture": "full"})[0], 201)
        self.tenant("bravo")
        self.writer = self.issue("alpha", "payments", "ingest_traces")
        self.reader = self.issue("alpha", "analyst", "query", "query_traces")
        self.bravo = self.issue("bravo", "warehouse", "ingest_traces", "query_traces", "query")
        self.t = time.time_ns() - 5_000_000_000
        self.q = {"start_ns": str(self.t), "end_ns": str(self.t + 1000)}

    def issue(self, tenant, service, *permissions):
        status, result, _ = self.call(f"/api/v1/tenants/{tenant}/credentials", self.owner,
                                      {"service": service, "permissions": permissions})
        self.assertEqual(status, 201)
        return result

    def trace(self, tenant="alpha", reader=None, q=None, evidence=False, trace_id=TRACE):
        family = "evidence/traces" if evidence else "traces"
        return self.call(f"/api/v1/tenants/{tenant}/{family}/{trace_id}",
                         (reader or self.reader)["token"], self.q if q is None else q)

    def visible(self, count, **kwargs):
        deadline = time.monotonic() + 12
        while True:
            code, data, _ = self.trace(**kwargs)
            if code == 200 and len(data["spans"]) == count:
                return data
            if time.monotonic() >= deadline:
                self.fail(f"expected {count} spans; last outcome {data.get('outcome')}, reason {data.get('reason')}")
            time.sleep(.1)

    def send(self, body, credential=None, headers=None):
        return self.call("/v1/traces", (credential or self.writer)["token"], body, headers=headers)


class RealStores(TraceHarness):
    def start_traces(self):
        diagnostics = (self.state / "trace-store.log").open("wb")
        self.addCleanup(diagnostics.close)
        store = subprocess.Popen(["python3", ROOT / "scripts/run-local-traces.py", self.state],
                                 stdout=diagnostics, stderr=subprocess.STDOUT)
        self.addCleanup(stop, store)
        deadline = time.monotonic() + 8
        while not (self.state / "traces.sock").is_socket():
            if store.poll() is not None or time.monotonic() >= deadline:
                self.fail((self.state / "trace-store.log").read_text()[:600])
            time.sleep(.05)
        return store

    def test_resource_and_scope_boundaries_with_typed_attributes(self):
        self.start_traces()
        resources = []
        expected = {}
        for r in range(2):
            scopes = []
            for n in range(2):
                ident = f"{1+r*2+n:016x}"
                name = f"scope-{n}"
                submitted = span(self.t + r*2 + n, ident, "")
                submitted["attributes"] = [{"key": "agent.future", "value": {"kvlistValue": {"values": [
                    {"key": "typed", "value": {"arrayValue": {"values": [{"intValue": "9007199254740993"},
                    {"doubleValue": "NaN"}, {"bytesValue": "AAE="}]}}}]}}}]
                scopes.append({"scope": {"name": name}, "spans": [submitted]})
                expected[ident] = (f"resource-{r}", name, submitted["attributes"])
            resources.append({"resource": {"attributes": [attr("service.name", f"resource-{r}")]},
                              "scopeSpans": scopes})
        self.assertEqual(self.send({"resourceSpans": resources})[2]["x-observe-accepted-records"], "4")
        for fact in self.visible(4)["spans"]:
            unit = fact["otlp"]
            scope = unit["scopeSpans"][0]
            self.assertEqual((unit["resource"]["attributes"][0]["value"]["stringValue"], scope["scope"]["name"],
                              scope["spans"][0]["attributes"]), expected[fact["identity"]["span_id"]])

    def test_two_tenant_distributed_trace_and_evidence(self):
        trace_store = self.start_traces()
        loki = test_headless.RealLoki.start_loki(self)
        t = self.t
        code, partial, h = self.send(export(span(t + 10), span(t, "00" * 8)))
        self.assertEqual((code, partial["partialSuccess"]["rejectedSpans"], h["x-observe-accepted-records"],
                          h["x-observe-durable-records"]), (200, "1", "1", "0"))
        before = self.visible(1)
        child = before["spans"][0]
        self.assertEqual(child["parent_resolution"], "not_in_returned_page")
        self.assertEqual(child["identity"]["producer_id"], self.writer["id"])
        self.assertEqual(child["identity"]["service"], "payments")
        self.assertEqual(child["identity"]["tenant"], "alpha")
        unit = child["otlp"]
        self.assertEqual(unit["resource"]["attributes"], export()["resourceSpans"][0]["resource"]["attributes"])
        self.assertEqual(unit["scopeSpans"][0]["scope"], {"name": SECRET, "version": "1.0"})
        returned = unit["scopeSpans"][0]["spans"][0]
        self.assertEqual(returned, dict(span(t + 10), traceId=TRACE))

        checkout = self.issue("alpha", "checkout", "ingest_traces", "ingest")
        self.assertEqual(self.send(gzip.compress(protobuf_parent(t)), checkout,
                                   dict(OTLP, **{"Content-Encoding": "gzip"}))[:2], (200, b""))
        self.assertEqual(self.send(export(span(t + 10)), self.bravo)[0], 200)
        self.assertEqual(self.call("/v1/logs", checkout["token"], envelope([log(t + 12, "checkout log")]))[0], 200)
        log_page = test_headless.RealLoki.visible(self, "alpha", self.reader["token"], self.q, 1)
        trace_id = log_page["records"][0]["trace_id"]
        self.assertEqual(trace_id, TRACE)
        whole = self.visible(2, trace_id=trace_id)
        self.assertEqual([s["parent_resolution"] for s in whole["spans"]],
                         ["no_parent_reported", "one_observed_candidate"])
        self.assertEqual([s["identity"]["service"] for s in whole["spans"]], ["checkout", "payments"])
        self.assertEqual(whole["coverage"], "unknown")
        bravo = self.visible(1, tenant="bravo", reader=self.bravo)
        self.assertNotIn(SECRET, json.dumps(bravo))
        self.assertEqual(bravo["spans"][0]["identity"]["service"], "warehouse")
        self.assertEqual(self.trace(reader=self.bravo)[0], 403)
        self.assertEqual(self.trace(tenant="bravo")[0], 403)

        code, evidence, _ = self.trace(evidence=True, trace_id=trace_id)
        self.assertEqual((code, evidence["outcome"]), (200, "matches"))
        self.assertEqual(evidence["relationships"], [
            {"method": "exact_trace_id", "basis": "producer_supplied_ids", "log_record_id": log_page["records"][0]["id"],
             "trace_id": TRACE, "matching_span_record_ids": [child["record_id"]]}])
        self.assertEqual(evidence["traces"]["effective_range"], {"start_ns": str(t), "end_ns": str(t + 11)})
        self.assertEqual(evidence["traces"]["requested_range"], self.q)
        for source, key in (("logs", "records"), ("traces", "spans")):
            for ref in evidence[source]["references"]:
                code, resolved, _ = self.call(ref["path"], self.reader["token"], ref["query"])
                self.assertEqual((code, len(resolved[key])), (200, 1))
                self.assertEqual(resolved[key][0].get("id", resolved[key][0].get("record_id")), ref["record_id"])
                self.assertEqual(self.call(ref["path"], self.bravo["token"], ref["query"])[0], 403)

        # Conflicting IDs remain distinct submitted records, even across producer credentials.
        duplicate = dict(span(t, PARENT, ""), kind=0, name="separate-submission")
        self.assertEqual(self.send(export(duplicate), self.writer)[0], 200)
        conflict = self.visible(3)
        self.assertIn("repeated_span_id", conflict["limitations"])
        self.assertEqual(conflict["spans"][-1]["parent_resolution"], "ambiguous")
        # Resolve both conflicting parents *after* the collision. Compare with
        # their submitted data and issued credentials independently of the page.
        parents = {s["record_id"] for s in conflict["spans"] if s["identity"]["span_id"] == PARENT}
        self.assertEqual(len(parents), 2)
        resolved_parents = set()
        for ref in conflict["references"]:
            if ref["record_id"] in parents:
                code, result, _ = self.call(ref["path"], self.reader["token"], ref["query"])
                self.assertEqual((code, len(result["spans"])), (200, 1))
                fact = result["spans"][0]
                otlp = fact["otlp"]["scopeSpans"][0]["spans"][0]
                self.assertEqual((fact["record_id"], otlp["traceId"], otlp["spanId"], otlp.get("kind", 0),
                                  otlp["startTimeUnixNano"]), (ref["record_id"], TRACE, PARENT, 0, str(t)))
                resolved_parents.add((fact["identity"]["producer_id"], fact["identity"]["service"], otlp["name"]))
        self.assertEqual(resolved_parents, {(checkout["id"], "checkout", "checkout"),
                                            (self.writer["id"], "payments", "separate-submission")})
        self.assertEqual(self.trace(q=dict(self.q, limit=1))[1]["outcome"], "partial")
        self.assertEqual(self.trace(trace_id=OTHER)[1]["outcome"], "no_matches")
        # Shared scope/resource context must survive batching and output byte limits.
        large = span(t + 100, "01" * 8, "")
        large["attributes"] = [attr("future.large", "x" * 8000)]
        for i in range(16):
            large.update(traceId=OTHER, spanId=f"{i + 1:016x}")
            self.assertEqual(self.send(export(large))[0], 200)
        deadline = time.monotonic() + 12
        while True:
            bounded = self.trace(trace_id=OTHER)[1]
            if "capture_byte_limit" in bounded["limitations"]:
                break
            self.assertLess(time.monotonic(), deadline, "capture budget was never reached")
            time.sleep(.1)
        self.assertEqual(bounded["outcome"], "partial")
        self.assertTrue(0 < len(bounded["spans"]) < 16)
        span_only = self.issue("alpha", "reader", "query_traces")
        self.assertEqual(self.trace(reader=span_only)[0], 200)
        self.assertEqual(self.trace(reader=span_only, evidence=True)[0], 403)
        stop(trace_store)
        degraded = self.trace(evidence=True)[1]
        self.assertEqual((degraded["outcome"], degraded["traces"]["outcome"], len(degraded["logs"]["records"])),
                         ("partial", "unavailable", 1))
        self.start_traces()
        self.visible(3)
        stop(loki)
        degraded = self.trace(evidence=True)[1]
        self.assertEqual((degraded["outcome"], degraded["logs"]["outcome"], len(degraded["traces"]["spans"])),
                         ("partial", "unavailable", 3))
        ref = whole["references"][0]
        self.revoke("alpha", self.reader)
        self.assertEqual(self.call(ref["path"], self.reader["token"], ref["query"])[0], 401)

    def test_self_parent_remains_an_attributed_claim(self):
        self.start_traces()
        self.assertEqual(self.send(export(span(self.t, SPAN, SPAN)))[0], 200)
        raw = self.visible(1)
        code, evidence, _ = self.trace(evidence=True)
        self.assertEqual(code, 200)
        for page in (raw, evidence["traces"]):
            fact, = page["spans"]
            self.assertEqual(fact["parent_resolution"], "self_reference")
            self.assertEqual(fact["identity"]["parent_span_id"], SPAN)
            self.assertEqual(fact["otlp"]["scopeSpans"][0]["spans"][0]["parentSpanId"], SPAN)
            self.assertEqual(fact["identity"]["producer_id"], self.writer["id"])

    def test_same_tenant_log_becomes_visible_during_trace_dispatch(self):
        test_headless.RealLoki.start_loki(self)
        writer = self.issue("alpha", "checkout", "ingest")
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sink:
            sink.bind(str(self.state / "traces.sock"))
            sink.listen(1)
            sink.settimeout(3)
            with concurrent.futures.ThreadPoolExecutor(1) as clients:
                trace_write = clients.submit(self.send, export(span(self.t)))
                peer, _ = sink.accept()
                with peer:
                    peer.settimeout(2)
                    with peer.makefile("rb") as stream:
                        self.assertEqual(stream.readline(256).split()[:2], [b"POST", b"/insert/opentelemetry/v1/traces"])
                    code, _, _ = self.call("/v1/logs", writer["token"], envelope([log(self.t, "during trace write")]))
                    self.assertEqual(code, 200)
                    page = test_headless.RealLoki.visible(self, "alpha", self.reader["token"], self.q, 1)
                    self.assertEqual(page["records"][0]["producer_id"], writer["id"])
                    self.assertFalse(trace_write.done())
                code, _, headers = trace_write.result(timeout=6)
                self.assertEqual((code, headers["x-observe-unknown-records"]), (503, "1"))


class TraceFailures(TraceHarness):
    def setUp(self):
        super().setUp()
        self.requests = []
        self.pause = threading.Event()
        self.arrived = threading.Event()
        self.drop = False
        self.status = 200
        self.answer = b""
        test = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def handle_source(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                test.requests.append((self.headers.get("AccountID"), self.path, body))
                test.arrived.set()
                test.pause.wait(6)
                if test.drop:
                    self.connection.close()
                    return
                self.send_response(test.status)
                self.send_header("Content-Length", str(len(test.answer)))
                self.end_headers()
                self.wfile.write(test.answer)

            do_POST = do_GET = handle_source

        self.pause.set()
        self.sink = Sink(str(self.state / "traces.sock"), Handler)
        threading.Thread(target=self.sink.serve_forever, daemon=True).start()
        self.addCleanup(self.sink.server_close)
        self.addCleanup(self.sink.shutdown)
        self.addCleanup(self.pause.set)

    def test_redaction_before_storage_and_authority(self):
        self.assertEqual(self.send(export(span(self.t)), self.bravo)[0], 200)
        account, path, raw = self.requests[-1]
        self.assertEqual(path, "/insert/opentelemetry/v1/traces")
        self.assertNotIn(SECRET.encode(), raw)
        # Also inspect the adapter's base64 copy of the captured OTLP unit.
        decoded = base64.b64decode(stored_payload(raw), validate=True)
        self.assertIn(bytes.fromhex(TRACE), decoded)
        self.assertNotIn(SECRET.encode(), decoded)
        self.assertNotIn(b"claimed-service", decoded)
        self.assertEqual(self.send(export(span(self.t)))[0], 200)
        self.assertNotEqual(self.requests[-1][0], account)
        before = len(self.requests)
        self.assertEqual(self.send({}, {"token": "0"*64})[0], 401)
        log_only = self.credential("alpha", "query", "ingest")
        self.assertEqual(self.send(export(span(self.t)), log_only)[0], 403)
        self.assertEqual(self.trace(reader=log_only)[0], 403)
        self.revoke("alpha", self.writer)
        self.assertEqual(self.send(export(span(self.t)))[0], 401)
        self.assertEqual(len(self.requests), before)
        with sqlite3.connect(self.state / "log-spool.sqlite") as db:
            self.assertEqual(db.execute("select count(*) from entries").fetchone()[0], 0)

    def test_existing_log_tenant_upgrades_without_inherited_trace_access(self):
        legacy = self.credential("alpha", "query", "ingest")
        stop(self.backend)
        with sqlite3.connect(self.state / "authority.sqlite") as db:
            db.execute("drop table trace_permissions")
            db.execute("drop table trace_tenants")
        self.start_backend()
        self.assertEqual(self.send(export(span(self.t)), legacy)[0], 403)
        self.assertEqual(self.trace(reader=legacy)[0], 403)
        upgraded = self.issue("alpha", "writer", "ingest_traces")
        self.assertEqual(self.send(export(span(self.t)), upgraded)[0], 200)
        self.assertNotIn(SECRET.encode(), base64.b64decode(stored_payload(self.requests[-1][2]), validate=True))

    def storage_row(self, submitted):
        self.assertEqual(self.send(export(submitted))[0], 200)
        payload = stored_payload(self.requests[-1][2])
        return {"trace_id": TRACE, "span_id": submitted["spanId"], "start_time_unix_nano": submitted["startTimeUnixNano"],
                "end_time_unix_nano": submitted["endTimeUnixNano"], "span_attr:observe.tenant": "alpha",
                "span_attr:observe.unit": payload.decode(), "span_attr:observe.id": "22"*16,
                "span_attr:observe.producer": self.writer["id"], "span_attr:observe.service": "payments"}

    def test_malformed_and_mismatched_storage_rows_fail_closed(self):
        row = self.storage_row(span(self.t))
        self.answer = json.dumps(row).encode()
        self.assertEqual(len(self.trace()[1]["spans"]), 1)
        for field, wrong in (("span_attr:observe.tenant", "bravo"), ("trace_id", OTHER), ("span_id", PARENT),
                             ("end_time_unix_nano", "1"), ("span_attr:observe.unit", "bad-protobuf")):
            self.answer = json.dumps(dict(row, **{field: wrong})).encode()
            code, data, _ = self.trace()
            self.assertEqual((code, data["outcome"]), (503, "unavailable"))
            self.assertNotIn(SECRET, json.dumps(data))
        self.answer = json.dumps(dict(row, **{"span_attr:observe.tenant": "bravo"})).encode()
        self.assertEqual(self.trace(tenant="bravo", reader=self.bravo)[0], 503)
        self.answer = b"x" * (2 * 1024 * 1024 + 1)
        self.assertEqual(self.trace()[1]["reason"], "source_response_byte_limit")
        self.answer = b""
        self.assertEqual(self.trace()[0], 200)

    def test_cutoff_requires_verified_rows(self):
        large = dict(span(self.t), attributes=[attr("padding", "x" * 8000)])
        valid = self.storage_row(large)
        outside = self.storage_row(dict(large, startTimeUnixNano=str(self.t - 1)))
        foreign = dict(valid, **{"span_attr:observe.tenant": "bravo"})
        size = len(valid["span_attr:observe.unit"])
        for prefix, limit, reason in ((1, 1, "row_limit"), (128 * 1024 // size, 100, "capture_byte_limit")):
            rows = [dict(valid, **{"span_attr:observe.id": f"{i + 1:032x}"}) for i in range(prefix)]
            q = dict(self.q, limit=limit)
            for sentinel in (valid, outside, foreign):
                with self.subTest(cutoff=reason, tenant=sentinel["span_attr:observe.tenant"],
                                  start=sentinel["start_time_unix_nano"]):
                    self.answer = b"\n".join(json.dumps(r).encode() for r in [*rows, sentinel])
                    code, data, _ = self.trace(q=q)
                    if sentinel is valid:
                        self.assertEqual((code, data["outcome"], len(data["spans"])), (200, "partial", prefix))
                        self.assertIn(reason, data["limitations"])
                    else:
                        self.assertEqual((code, data["outcome"]), (503, "unavailable"))
                        code, evidence, _ = self.trace(q=q, evidence=True)
                        self.assertEqual((code, evidence["traces"]["outcome"]), (503, "unavailable"))
                        self.assertNotIn(reason, json.dumps(evidence))

    def test_invalid_ids_and_bounded_decode(self):
        for field, value in (("spanId", "aabb"), ("traceId", "0"*32), ("parentSpanId", "00"*8),
                             ("links", [{"traceId": OTHER, "spanId": "11"}]), ("endTimeUnixNano", "1")):
            s = span(self.t)
            s[field] = value
            code, _, h = self.send(export(s))
            self.assertEqual((code, h["x-observe-rejected-records"]), (400, "1"))
        self.assertEqual(self.send({"resourceSpans": "bad"})[0], 400)
        self.assertEqual(self.send(b'\xff', headers=OTLP)[0], 400)
        self.assertEqual(self.send(b"x"*65537)[0], 413)
        self.assertEqual(self.send(gzip.compress(b" "*65537), headers={"Content-Encoding": "gzip"})[0], 413)
        self.assertEqual(self.send(wire_field(1, wire_field(2, wire_field(2, b"")*257)), headers=OTLP)[0], 413)
        self.assertEqual(self.send({}, headers={"X-Scope-OrgID": "bravo"})[0], 400)
        self.assertEqual(self.trace(q=dict(self.q, limit=101))[0], 400)
        self.assertEqual(self.trace(q=dict(self.q, deadline_ms=2001))[0], 400)
        self.assertEqual(self.trace(q=dict(self.q, record_id="invalid"))[0], 400)
        self.assertEqual(len(self.requests), 0)

    def test_offline_ambiguous_and_peer_trace_capacity(self):
        raw = export(span(self.t))
        sock = self.state / "traces.sock"
        os.rename(sock, self.state / "moved.sock")
        status, _, h = self.send(raw)
        self.assertEqual((status, h["x-observe-unsubmitted-records"], h["x-observe-unknown-records"]), (503, "1", "0"))
        os.rename(self.state / "moved.sock", sock)
        self.status = 500
        self.assertEqual(self.send(raw)[2]["x-observe-unknown-records"], "1")
        self.drop = True
        for _ in range(2):
            status, _, h = self.send(raw)
            self.assertEqual((status, h["x-observe-unknown-records"], h["x-observe-durable-records"]), (503, "1", "0"))
        self.assertEqual(self.send(raw)[0], 429)
        self.assertEqual(self.send(raw, self.bravo)[0], 503)
        self.assertEqual(len(self.requests), 4)

    def test_deadline_residual_and_revocation_during_read(self):
        self.pause.clear()
        short = dict(self.q, deadline_ms=100)
        self.assertEqual(self.trace(q=short)[0], 504)
        self.assertTrue(self.arrived.is_set())
        self.assertEqual(self.trace()[0], 429)
        self.assertEqual(len(self.requests), 1)
        self.pause.set()
        for _ in range(100):
            if self.trace()[0] == 200:
                break
            time.sleep(.01)
        else:
            self.fail("completed work did not release its lane")
        self.pause.clear()
        self.arrived.clear()
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            future = pool.submit(self.trace)
            self.assertTrue(self.arrived.wait(1))
            self.revoke("alpha", self.reader)
            self.pause.set()
            self.assertEqual(future.result()[0], 401)

    def test_shared_evidence_deadline_expires_with_source_work_outstanding(self):
        self.pause.clear()
        code, data, _ = self.trace(q=dict(self.q, deadline_ms=100), evidence=True)
        self.assertEqual((code, data["outcome"], data["reason"]), (504, "unavailable", "deadline"))
        self.assertNotIn("relationships", data)
        self.assertTrue(self.arrived.is_set())
        self.assertEqual(self.trace()[0], 429)


if __name__ == "__main__":
    unittest.main()
