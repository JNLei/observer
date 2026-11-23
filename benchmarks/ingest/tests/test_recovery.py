import collections
import importlib.util
import json
from pathlib import Path
import unittest
from unittest import mock

path = Path(__file__).resolve().parents[1] / "otlp" / "recovery.py"
spec = importlib.util.spec_from_file_location("recovery", path)
recovery = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recovery)


class RecoveryRows(unittest.TestCase):
    def setUp(self):
        self.rows = [recovery.expected_row(recovery.record(i, i * recovery.HOUR)) for i in range(1, 121)]
        self.expected = collections.Counter(self.rows)

    def test_reviewers_121_rows_fail_during_replay_and_at_both_restart_boundaries(self):
        corrupt = self.rows + self.rows[:1]
        for phase, complete in (("replay", False), ("settled", True), ("restored", True)):
            with self.subTest(phase=phase), self.assertRaisesRegex(RuntimeError, "duplicate=1"):
                recovery.check_rows(corrupt, self.expected, phase, complete=complete)

    def test_missing_unexpected_and_substitution_fail(self):
        extra = recovery.expected_row(recovery.record(999, recovery.HOUR))
        for phase in ("settled", "restored"):
            for actual, error in ((self.rows[:-1], "missing=1"), (self.rows + [extra], "unexpected=1"),
                                  (self.rows[:-1] + self.rows[:1], "missing=1 duplicate=1")):
                with self.subTest(phase=phase, error=error), self.assertRaisesRegex(RuntimeError, error):
                    recovery.check_rows(actual, self.expected, phase)

    def test_expected_multiplicity_and_order(self):
        values = self.rows + self.rows[:1]
        expected = collections.Counter(values)
        self.assertEqual(recovery.check_rows(reversed(values), expected, "restored"), expected)
        with self.assertRaisesRegex(RuntimeError, "missing=1"):
            recovery.check_rows(self.rows, expected, "restored")

    def test_query_fields_against_original_event(self):
        first = self.rows[0]
        labels = dict(observed_timestamp=first.observed_ns, service_name=first.service, severity_number=first.severity,
                      trace_id=first.trace_id, span_id=first.span_id)
        def read(stamp=first.event_ns, line=first.line, **overrides):
            stream = {"stream": {**labels, **overrides}, "values": [[str(stamp), line]]}
            return recovery.rows(json.dumps({"status": "success", "data": {"resultType": "streams", "result": [stream]}}))
        self.assertEqual(read(), [first])
        for changes in ({"stamp": first.event_ns+1, "observed_timestamp": str(first.event_ns+12)},
                        {"line": "bench-2"}, {"service_name": "other"}, {"observed_timestamp": "1"},
                        {"severity_number": "10"}, {"trace_id": "a"*32}, {"span_id": "b"*16}):
            with self.subTest(changes=changes), self.assertRaisesRegex(RuntimeError, "unexpected=1"):
                recovery.check_rows(read(**changes), collections.Counter([first]), "probe", complete=False)


class RecoveryOverlap(unittest.TestCase):
    def witness(self, counts):
        samples = [dict(history_unique=h, live_unique=l, elapsed_ms=i) for i, (h, l) in enumerate(counts)]
        return recovery.overlap_witness(samples, 96, 24)

    def test_fresh_withheld_until_history_finishes_fails(self):
        for counts in ([(4, 0), (92, 0), (96, 0), (96, 24)], [(4, 0), (92, 0), (96, 24)]):
            with self.subTest(counts=counts), self.assertRaisesRegex(RuntimeError, "controlled overlap"):
                self.witness(counts)

    def test_a_single_fresh_row_during_stalled_history_is_insufficient(self):
        with self.assertRaisesRegex(RuntimeError, "controlled overlap"):
            self.witness([(4, 0), (4, 1), (4, 23), (96, 24)])

    def test_both_classes_advance_during_replay(self):
        a, b = self.witness([(4, 0), (4, 2), (8, 3), (96, 24)])
        self.assertLess(a["elapsed_ms"], b["elapsed_ms"])


class CollectorAccounting(unittest.TestCase):
    @staticmethod
    def scrape(history_queue=24):
        return {
            'otelcol_exporter_queue_size{data_type="logs",exporter="otlp_http/history"}': history_queue,
            'otelcol_exporter_queue_size{data_type="logs",exporter="otlp_http/live"}': 0,
            'otelcol_receiver_accepted_log_records{receiver="otlp/history",transport="http"}': 96,
            'otelcol_exporter_sent_log_records{exporter="otlp_http/live"}': 0}

    def snapshot(self, raw):
        with mock.patch.object(recovery, "metrics", return_value=raw):
            return recovery.source_snapshot(0)

    def test_missing_series_and_observed_zero_remain_distinct(self):
        snapshot = self.snapshot(self.scrape())
        self.assertIsNone(snapshot["history"]["sent_records"])
        self.assertEqual(snapshot["live"]["sent_records"], 0)
        recovery.check_outage(snapshot, 24, 96)

    def test_outage_requires_full_backlog_and_admissions(self):
        for snapshot in (self.snapshot(self.scrape(23)), self.snapshot({})):
            with self.assertRaisesRegex(RuntimeError, "expected 24 queued"):
                recovery.check_outage(snapshot, 24, 96)
        with self.assertRaisesRegex(RuntimeError, "acceptance"):
            recovery.check_outage(self.snapshot(self.scrape()), 24, 100)

    def test_missing_final_queue_or_sent_series_cannot_certify_drain(self):
        raw = self.scrape(0)
        raw['otelcol_exporter_sent_log_records{exporter="otlp_http/history"}'] = 96
        raw['otelcol_exporter_sent_log_records{exporter="otlp_http/live"}'] = 24
        self.assertTrue(recovery.drained(self.snapshot(raw), 96, 24))
        for series in raw:
            if "exporter" in series:
                with self.subTest(missing=series):
                    self.assertFalse(recovery.drained(self.snapshot({k:v for k,v in raw.items() if k != series}), 96, 24))
