from collections import Counter
from dataclasses import asdict
import gzip
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"otlp"))
from reference import order
from reference_load import Batch, NS, PHASES, load_plan, offer, partition, recovery_plan, summarize
from verify_reference import verify_case


class ReferenceProfile(unittest.TestCase):
    def test_reference_demand_and_query_bounds(self):
        plan = load_plan(2*10**18, PHASES)
        self.assertEqual(len(plan), 21250)
        self.assertEqual(plan[-1].first+3, 85000)
        self.assertEqual(plan[15000].offset_ns, 600*NS)
        self.assertEqual(plan[21000].offset_ns, 660*NS)
        windows = list(partition(plan))
        for part, low, high in windows:
            self.assertLessEqual(sum(b.count for b in part), 256)
            self.assertTrue(all(low < b.event_ns <= b.event_ns+b.count-1 < high for b in part))
        self.assertEqual([hi for _, _, hi in windows[:-1]], [lo for _, lo, _ in windows[1:]])
        self.assertEqual(sorted(Counter(p[0] for p in order()).values()), [2, 3])
        self.assertTrue(all(sorted(p) == ["go", "rust"] for p in order()))

    def test_three_second_stall_exposes_unsent_and_intended_latency(self):
        release = threading.Event()
        started = threading.Event()

        def slow(port, token, batch, intended):
            now = time.monotonic_ns()
            started.set()
            release.wait(5)
            return dict(first=batch.first, intended_ns=intended, start_ns=now, end_ns=time.monotonic_ns(),
                        json_bytes=1, status=200, outcome="source_ack")

        def stall():
            if started.wait(5):
                time.sleep(3)
            release.set()
        timer = threading.Thread(target=stall)
        timer.start()
        plan = [Batch("stall", i+1, 1, i+1, i*5_000_000) for i in range(12)]
        start = time.monotonic_ns()
        journal = offer(plan, None, None, start, lambda _: None, send=slow, workers=1, capacity=1)
        timer.join()
        s = summarize(plan, journal, {}, start, 1)
        self.assertEqual(s["scheduled"], 12)
        self.assertEqual(s["sent"], 2)
        self.assertEqual(s["outcome_records"]["unsent"], 10)
        self.assertGreater(s["generator_wait"]["p99_ms"], 2800)
        self.assertGreater(s["source_ack_from_intended"]["p99_ms"], 2800)
        self.assertEqual(s["visible_from_intended"]["count"], 0)

    def test_archive_oracle_at_each_boundary(self):
        epoch = 2*10**18
        history, overflow, fresh = recovery_plan(epoch, 1)
        batches = history + overflow + fresh
        report = {"status": "passed", "recovery_epoch_ns": epoch, "source_outcome_records": {"source_ack": 120, "queue_full": 8}}
        rows = [list(row) for b in history+fresh for row in b.keys()]
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)/"paced"
            folder.mkdir()
            for fault in ("none", "duplicate", "missing", "span", "refusal"):
                with gzip.open(folder/"events.jsonl.gz", "wt") as f:
                    def add(kind, **data):
                        print(json.dumps({"kind": kind, **data}), file=f)
                    for b in batches:
                        add("plan", **asdict(b))
                        add("source", first=b.first, outcome="queue_full" if b in overflow and fault != "refusal" else "source_ack")
                    add("query", purpose="settled", status=200, start_ns=1, end_ns=2, rows=rows)
                    restored = [list(row) for row in rows]
                    if fault == "duplicate":
                        restored.append(rows[0])
                    elif fault == "missing":
                        restored.pop()
                    elif fault == "span":
                        restored[0][-1] = "0"*16
                    add("query", purpose="restored", status=200, start_ns=3, end_ns=4, rows=restored)
                    add("query", purpose="tenant_b", status=200, start_ns=5, end_ns=6, rows=[])
                with self.subTest(fault=fault):
                    if fault == "none":
                        self.assertEqual(verify_case(folder, report, PHASES)["exact_rows_at_each_read"], 120)
                    else:
                        with self.assertRaises(RuntimeError):
                            verify_case(folder, report, PHASES)
