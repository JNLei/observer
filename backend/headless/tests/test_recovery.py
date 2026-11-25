#!/usr/bin/env python3
"""Observe log custody tests. All producers and readers use the real HTTP backend."""

from collections import Counter
import concurrent.futures
from contextlib import closing
import gzip
import http.client
from http.server import BaseHTTPRequestHandler
import json
import multiprocessing
import socket
import socketserver
import sqlite3
import threading
import time
import unittest

from test_headless import Local, RealLoki, Sink, TRACE, SPAN, PRIVATE, envelope, log, protobuf_log


def withhold_loki_reply(front, back, control):
    """One local HTTP hop. Report the real Loki result and withhold it from Observe."""

    class Proxy(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            wire = self.rfile.read(int(self.headers["Content-Length"]))
            with closing(http.client.HTTPConnection("localhost", timeout=4)) as client:
                client.sock = socket.socket(socket.AF_UNIX)
                client.sock.connect(back)
                client.request(
                    "POST",
                    self.path,
                    wire,
                    {"Content-Type": "application/json", "X-Scope-OrgID": self.headers["X-Scope-OrgID"]},
                )
                reply = client.getresponse()
                reply.read()
                control.send((reply.status, json.loads(json.loads(wire)["streams"][0]["values"][0][1])["id"]))
            control.recv()

    with socketserver.UnixStreamServer(front, Proxy) as server:
        control.send("ready")
        server.handle_request()


class Custody(Local):
    start_loki = RealLoki.start_loki
    visible = RealLoki.visible

    def setUp(self):
        super().setUp()
        self.boot()
        self.tenant("alpha", "text_body")
        self.alpha = self.credential("alpha", "ingest", "query")
        self.tenant("bravo")
        self.bravo = self.credential("bravo", "ingest", "query")
        self.db = self.state / "log-spool.sqlite"

    def status(self, tenant="alpha"):
        grant = getattr(self, tenant)
        code, data, _ = self.call(f"/api/v1/tenants/{tenant}/logs/delivery", grant["token"], method="GET")
        self.assertEqual(code, 200)
        return data

    def wait_status(self, tenant, predicate, seconds=16):
        end = time.monotonic() + seconds
        while True:
            s = self.status(tenant)
            if predicate(s):
                return s
            if time.monotonic() >= end:
                self.fail(f"unexpected {tenant} custody counts {s}")
            time.sleep(0.025)

    def post(self, tenant, records):
        return self.call("/v1/logs", getattr(self, tenant)["token"], envelope(records))

    def kill_backend(self):
        self.backend.kill()
        self.backend.wait(timeout=3)

    def scripted_store(self, responses, waiting=None):
        attempts = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                data = self.rfile.read(int(self.headers["Content-Length"]))
                rows = [json.loads(line) for _, line in json.loads(data)["streams"][0]["values"]]
                attempts.append((time.monotonic(), self.headers["X-Scope-OrgID"], rows))
                code, delay = responses[min(len(attempts) - 1, len(responses) - 1)]
                if waiting is not None:
                    entered, release = waiting
                    entered.set()
                    release.wait(4)
                self.send_response(code)
                if delay is not None:
                    self.send_header("Retry-After", str(delay))
                self.end_headers()

        server = Sink(str(self.state / "loki.sock:0"), Handler)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return attempts

    def test_disconnected_client_keeps_committed_work(self):
        entered, release = threading.Event(), threading.Event()
        attempts = self.scripted_store([(204, None)], (entered, release))
        self.addCleanup(release.set)
        body = json.dumps(envelope([log(time.time_ns())]))

        def producer():
            with closing(http.client.HTTPConnection("127.0.0.1", self.port, timeout=1)) as client:
                client.request(
                    "POST",
                    "/v1/logs",
                    body,
                    {"Content-Type": "application/json", "Authorization": "Bearer " + self.alpha["token"]},
                )
                with self.assertRaises(socket.timeout):
                    client.getresponse()

        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            request = pool.submit(producer)
            self.assertTrue(entered.wait(2))
            request.result()
            status = self.status()
            self.assertEqual((status["pending_records"], status["unsettled_dispatch_records"]), (1, 1))
            release.set()
        self.wait_status("alpha", lambda s: s["store_acknowledged_records"] == 1)
        self.assertEqual(len(attempts), 1)

    def test_unknown_batch_retry_after_and_stable_members(self):
        attempts = self.scripted_store([(400, None), (429, 2), (204, None)])
        t = time.time_ns()
        code, _, h = self.post("alpha", [log(t), log(t + 1), dict(log(t), traceId="aa")])
        self.assertEqual(
            (
                code,
                h["x-observe-rejected-records"],
                h["x-observe-unknown-records"],
                h["x-observe-durable-records"],
            ),
            (503, "1", "2", "2"),
        )
        s = self.wait_status("alpha", lambda s: s["store_acknowledged_records"] == 2)
        self.assertEqual((s["durable_records"], s["unknown_record_attempts"], s["pending_records"]), (2, 4, 0))
        self.assertEqual(len(attempts), 3)
        self.assertGreaterEqual(attempts[2][0] - attempts[1][0], 1.98)
        self.assertTrue(all(a[1:] == attempts[0][1:] for a in attempts))
        self.assertEqual(len({r["id"] for r in attempts[0][2]}), 2)

    def test_failed_ack_transaction_preserves_custody(self):
        attempts = self.scripted_store([(204, None)])
        with closing(sqlite3.connect(self.db)) as db:
            db.execute("""CREATE TRIGGER fail_release BEFORE DELETE ON entries
                          BEGIN SELECT RAISE(ABORT,'fixture abort'); END""")
        code, _, h = self.post("alpha", [log(time.time_ns())])
        self.assertEqual((code, h["x-observe-unknown-records"], h["x-observe-durable-records"]), (503, "1", "1"))
        s = self.status()
        self.assertEqual((s["storage_fault"], s["pending_records"], s["store_acknowledged_records"]), (True, 1, 0))
        self.assertEqual(self.post("bravo", [log(time.time_ns())])[0], 503)
        with closing(sqlite3.connect(self.db)) as db:
            db.execute("DROP TRIGGER fail_release")
        self.kill_backend()
        self.start_backend()
        s = self.wait_status("alpha", lambda s: s["pending_records"] == 0)
        self.assertEqual(
            (s["unknown_record_attempts"], s["store_acknowledged_records"], s["residual_remote_attempt_records"]),
            (1, 1, 1),
        )
        self.assertEqual(len(attempts), 2)
        self.assertEqual(attempts[0][1:], attempts[1][1:])

    def test_sqlite_page_exhaustion(self):
        self.kill_backend()
        self.start_backend(OBSERVE_SPOOL_MAX_PAGES="8")
        t = time.time_ns()
        self.assertEqual(self.post("alpha", [log(t, "prior")])[2]["x-observe-durable-records"], "1")
        code, _, h = self.post("alpha", [log(t + n, "x" * 4096) for n in range(12)])
        self.assertEqual(code, 503)
        self.assertIsNone(h.get("x-observe-durable-records"))
        s = self.status()
        self.assertEqual((s["storage_fault"], s["durable_records"], s["pending_records"]), (True, 1, 1))
        self.kill_backend()
        sent = self.scripted_store([(204, None)])
        self.start_backend()
        self.wait_status("alpha", lambda s: s["store_acknowledged_records"] == 1)
        self.assertEqual([[r["body"] for r in rows] for _, _, rows in sent], [["prior"]])

    def test_serialized_byte_limit_and_global_capacity(self):
        # These bodies fit the OTLP request limit but fill the sanitized byte budget first.
        t = time.time_ns()
        batch = [log(t + i, "s" * 4096) for i in range(14)]
        for i in range(64):
            code, _, h = self.post("alpha", batch)
            if code == 429:
                break
            self.assertEqual((code, h["x-observe-durable-records"]), (503, "14"))
        self.assertTrue(0 < i < 63)
        self.assertEqual((code, h["x-observe-durable-records"]), (429, "0"))
        s = self.status()
        self.assertLessEqual(s["pending_bytes"], 2 * 1024 * 1024)
        self.assertEqual(s["pending_records"], 14 * i)
        # Narrow entries can still consume the tenant's remaining slots.
        for _ in range(64 - i):
            self.assertEqual(self.post("alpha", [log(t, "x")])[0], 503)
        for tenant in ("bravo", "charlie", "delta"):
            if tenant != "bravo":
                self.tenant(tenant)
                setattr(self, tenant, self.credential(tenant, "ingest", "query"))
            for _ in range(64):
                self.assertEqual(self.post(tenant, [log(t)])[2]["x-observe-durable-records"], "1")
        self.tenant("echo")
        self.echo = self.credential("echo", "ingest", "query")
        code, _, h = self.post("echo", [log(t)])
        self.assertEqual(
            (code, h["x-observe-durable-records"], self.status("echo")["pending_records"]), (429, "0", 0)
        )

    def test_two_tenant_crash_replay_and_fresh_visibility(self):
        t = time.time_ns() - 1800 * 10**9
        count = 20
        inventory = {}
        for name in ("alpha", "bravo"):
            for n in range(count):
                if n == 0 and name == "alpha":
                    code, _, h = self.call(
                        "/v1/logs",
                        self.alpha["token"],
                        gzip.compress(protobuf_log(t)),
                        headers={"Content-Type": "application/x-protobuf", "Content-Encoding": "gzip"},
                    )
                else:
                    code, _, h = self.post(name, [log(t + n, "history" if name == "alpha" else PRIVATE)])
                self.assertEqual(
                    (
                        code,
                        h["x-observe-durable-records"],
                        h["x-observe-accepted-records"],
                        h["x-observe-unsubmitted-records"],
                    ),
                    (503, "1", "0", "1"),
                )
            s = self.status(name)
            self.assertEqual(
                (s["durable_records"], s["pending_records"], s["store_acknowledged_records"]), (count, count, 0)
            )
        self.assertEqual(
            self.call("/api/v1/tenants/alpha/logs/delivery", self.bravo["token"], method="GET")[0], 403
        )
        self.assertEqual(self.db.stat().st_mode & 0o777, 0o600)
        with closing(sqlite3.connect(self.db)) as db:
            for name in ("alpha", "bravo"):
                rows = [
                    r
                    for (blob,) in db.execute("SELECT payload FROM entries WHERE tenant=?", (name,))
                    for r in json.loads(blob)
                ]
                self.assertEqual(Counter(int(r["event_ns"]) for r in rows), Counter(range(t, t + count)))
                self.assertTrue(
                    all(
                        r["tenant"] == name
                        and r["producer_id"] == getattr(self, name)["id"]
                        and r["service"] == "gateway"
                        and r["trace_id"] == TRACE
                        and r["span_id"] == SPAN
                        and int(r["observed_ns"]) == int(r["event_ns"]) + 500
                        for r in rows
                    )
                )
                self.assertEqual(len({r["id"] for r in rows}), count)
                inventory[name] = {r["id"]: r for r in rows}
        disk = self.db.read_bytes()
        for secret in (PRIVATE, "claimed-foreign-service", self.alpha["token"], self.bravo["token"]):
            self.assertFalse(secret.encode() in disk, "restricted field present in spool")
        self.kill_backend()
        self.start_loki()
        self.start_backend()
        self.wait_status("alpha", lambda s: 0 < s["store_acknowledged_records"] < count)
        fresh = time.time_ns()
        qfresh = {"start_ns": str(fresh), "end_ns": str(fresh + 1)}
        # Two independently submitted identical exports must remain separate instances.
        for _ in range(2):
            self.assertEqual(self.post("alpha", [log(fresh, "fresh")])[0], 200)
        seen = self.visible("alpha", self.alpha["token"], qfresh, 2)["records"]
        self.assertEqual(len({r["id"] for r in seen}), 2)
        self.assertGreater(self.status("alpha")["pending_records"], 0)
        # A single authorized query sees both fresh instances and only part of history.
        joint = {"start_ns": str(t), "end_ns": str(fresh + 1), "limit": 100}
        joint_rows = self.read("alpha", self.alpha["token"], joint)[1]["records"]
        self.assertTrue(2 < len(joint_rows) < count + 2)
        self.assertEqual(joint_rows[-2:], seen)
        q = {"start_ns": str(t), "end_ns": str(t + count), "limit": 100}
        page = self.read("alpha", self.alpha["token"], q)[1]
        self.assertTrue(0 < len(page["records"]) < count)
        for name in ("alpha", "bravo"):
            token = getattr(self, name)["token"]
            s = self.wait_status(name, lambda s: s["pending_records"] == 0)
            expected = count + (2 if name == "alpha" else 0)
            self.assertEqual(
                (s["store_acknowledged_records"], s["durable_records"], s["unknown_record_attempts"]),
                (expected, expected, 0),
            )
            page = self.visible(name, token, q, count)
            self.assertEqual(Counter(r["id"] for r in page["records"]), Counter(inventory[name].keys()))
            self.assertTrue(all(r == inventory[name][r["id"]] for r in page["records"]))
            ev = self.read(name, token, q, True)[1]
            self.assertEqual(ev["records"], page["records"])
            ref = ev["references"][0]
            self.assertEqual(self.call(ref["path"], token, ref["query"])[1]["records"], page["records"][:1])
        self.assertFalse(set(inventory["alpha"]) & set(inventory["bravo"]))

    def test_crash_after_loki_commit_before_backend_receives_reply(self):
        store = self.state / "store"
        store.mkdir(mode=0o700)
        self.start_loki(store)
        front, back = self.state / "loki.sock:0", store / "loki.sock:0"
        ctx = multiprocessing.get_context("spawn")
        parent, child = ctx.Pipe()
        self.addCleanup(parent.close)
        proxy = ctx.Process(target=withhold_loki_reply, args=(str(front), str(back), child))
        proxy.start()
        child.close()
        self.addCleanup(proxy.join, 3)
        self.addCleanup(proxy.terminate)
        self.assertTrue(parent.poll(5))
        self.assertEqual(parent.recv(), "ready")
        t = time.time_ns()
        q = {"start_ns": str(t), "end_ns": str(t + 1)}
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            response = pool.submit(self.post, "alpha", [log(t, "ambiguous")])
            self.assertTrue(parent.poll(4))
            status, record_id = parent.recv()
            self.assertEqual(status, 204)
            front.unlink()
            front.symlink_to(back)
            page = self.visible("alpha", self.alpha["token"], q, 1)
            self.assertEqual(page["records"][0]["id"], record_id)
            self.assertEqual(self.status()["store_acknowledged_records"], 0)
            self.kill_backend()
            with self.assertRaises((http.client.RemoteDisconnected, ConnectionResetError)):
                response.result()
        front.unlink()
        for restart in range(2):
            self.start_backend()
            s = self.status()
            self.assertEqual(
                (
                    s["pending_records"],
                    s["residual_remote_attempt_records"],
                    s["unknown_record_attempts"],
                    s["store_acknowledged_records"],
                ),
                (1, 1, 1, 0),
            )
            if restart == 0:
                self.kill_backend()
        front.symlink_to(back)
        s = self.wait_status("alpha", lambda s: s["pending_records"] == 0)
        self.assertEqual(s["residual_remote_attempt_records"], 1)
        parent.send("exit")
        proxy.join(3)
        self.assertEqual(proxy.exitcode, 0)
        s = self.wait_status("alpha", lambda s: s["residual_remote_attempt_records"] == 0)
        self.assertEqual(
            (s["durable_records"], s["store_acknowledged_records"], s["unknown_record_attempts"]), (1, 1, 1)
        )
        rows = self.read("alpha", self.alpha["token"], q, True)[1]["records"]
        self.assertIn(len(rows), (1, 2))
        self.assertTrue(all(r == page["records"][0] for r in rows))
        print(f"lost-reply fixture: 2 Loki submissions, 1 logical ID, {len(rows)} visible row(s)", flush=True)

    def test_admission_rollback_and_quota_race(self):
        t = time.time_ns()
        with closing(sqlite3.connect(self.db)) as db:
            db.execute("""CREATE TRIGGER fail_admit BEFORE UPDATE OF durable ON totals
                          BEGIN SELECT RAISE(ABORT,'fixture abort'); END""")
        code, _, h = self.post("alpha", [log(t, "rollback-marker")])
        self.assertEqual(code, 503)
        self.assertIsNone(h.get("x-observe-durable-records"))
        self.assertEqual(self.status()["durable_records"], 0)
        with closing(sqlite3.connect(self.db)) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM entries").fetchone()[0], 0)
            db.execute("DROP TRIGGER fail_admit")
        self.kill_backend()
        self.start_backend()
        for n in range(63):
            self.assertEqual(self.post("alpha", [log(t + n)])[2]["x-observe-durable-records"], "1")
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            last = list(pool.map(lambda n: self.post("alpha", [log(t + n)]), [63, 64]))
        self.assertEqual(
            sorted((code, h["x-observe-durable-records"]) for code, _, h in last), [(429, "0"), (503, "1")]
        )
        self.assertEqual(self.post("bravo", [log(t)])[2]["x-observe-durable-records"], "1")
        self.kill_backend()
        self.start_backend()
        s = self.status()
        self.assertEqual((s["durable_records"], s["pending_records"], s["capacity_refused_records"]), (64, 64, 1))
        self.assertEqual(self.status("bravo")["pending_records"], 1)

    def test_replay_rechecks_authority_and_window(self):
        now = time.time_ns()
        expired = now - 49 * 3600 * 10**9
        self.assertEqual(self.post("alpha", [log(expired, "refused-marker")])[0], 400)
        self.assertEqual(
            self.post("alpha", [log(now - 48 * 3600 * 10**9 + 2 * 10**9)])[2]["x-observe-durable-records"], "1"
        )
        self.assertEqual(self.post("bravo", [log(now)])[2]["x-observe-durable-records"], "1")
        self.revoke("bravo", self.bravo)
        self.bravo = self.credential("bravo", "query")
        self.kill_backend()
        self.start_loki()
        self.start_backend()
        self.wait_status("bravo", lambda s: s["held_authority_records"] == 1)
        self.wait_status("alpha", lambda s: s["held_time_window_records"] == 1)
        self.assertNotIn(b"refused-marker", self.db.read_bytes())
        for name, t in (("bravo", now), ("alpha", now - 48 * 3600 * 10**9 + 2 * 10**9)):
            s = self.status(name)
            self.assertEqual(
                (s["durable_records"], s["pending_records"], s["store_acknowledged_records"]), (1, 1, 0)
            )
            self.assertEqual(
                self.read(name, getattr(self, name)["token"], {"start_ns": str(t), "end_ns": str(t + 1)})[1][
                    "outcome"
                ],
                "no_matches",
            )


if __name__ == "__main__":
    unittest.main()
