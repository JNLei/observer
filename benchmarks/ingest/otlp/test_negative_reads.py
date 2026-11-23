"""Run the real recovery driver with faults injected only into successful query replies."""

import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import recovery


class NegativeReads(unittest.TestCase):
    def run_fault(self, binary, fault, expected_error):
        real_http = recovery.http
        starts = 0
        altered = 0

        def read(endpoint, path, **kwargs):
            nonlocal starts, altered
            status, body = real_http(endpoint, path, **kwargs)
            if isinstance(endpoint, Path) and path == "/ready" and status == 200:
                starts += 1
            if status != 200 or not path.startswith("/bench/v1/logs?"):
                return status, body
            response = json.loads(body)
            streams = response["data"]["result"]
            values = [value for stream in streams for value in stream["values"]]
            if fault == "withhold fresh":
                def historical(value):
                    return int(value[1].removeprefix("bench-")) <= 96
                history = sum(historical(v) for v in values)
                if history < 96 and len(values) > history:
                    for stream in streams:
                        stream["values"] = [v for v in stream["values"] if historical(v)]
                    altered += 1
            elif len(values) == 120 and (fault == "duplicate" or starts == 2):
                streams[0]["values"].append(streams[0]["values"][0])
                altered += 1
            return status, json.dumps(response).encode()

        with tempfile.TemporaryDirectory(prefix="m1-negative-", dir="/tmp") as directory:
            with patch.object(recovery, "http", read), self.assertRaisesRegex(RuntimeError, expected_error):
                recovery.trial(binary, time.time_ns(), Path(directory), {}, controlled=fault == "withhold fresh")
        self.assertGreater(altered, 0, "the fault must have reached a real query response")

    def test_121_rows_at_full_replay(self):
        self.run_fault(recovery.ROOT/"target/release/otlp-bench-rust", "duplicate", "replay probe: missing=0 duplicate=1")

    def test_121_rows_only_after_loki_restart(self):
        self.run_fault(recovery.BIN/"otlp-bench-go", "duplicate after restart", "restored read.*duplicate=1")

    def test_fresh_invisible_until_all_history_is_visible(self):
        self.run_fault(recovery.BIN/"otlp-bench-go", "withhold fresh", "controlled overlap:")


if __name__ == "__main__":
    unittest.main()
