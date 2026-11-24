"""One quota-verified receiver trial. Invoked by reference.py inside the private scope."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import contextlib
from dataclasses import asdict
import gzip
from http import client
import json
import os
from pathlib import Path
import secrets
import sys
import threading
import time

from linux_budget import resource_sample
from recovery import (Child, HERE, distribution, free_ports, http, metrics, overlap_witness, paced_history,
                      require, rows, source_snapshot, wait_for)
from reference_load import (NS, OUTCOMES, PHASES, QUICK_PHASES, load_plan, offer, partition, reconcile, recovery_plan,
                            summarize)


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


class Journal:
    def __init__(self, directory, stack):
        self.file = stack.enter_context(gzip.open(directory / "events.jsonl.gz", "wt", compresslevel=1))
        self.lock = threading.Lock()

    def add(self, kind, **data):
        with self.lock:
            self.file.write(json.dumps(dict(kind=kind, **data), separators=(",", ":")) + "\n")


class Case:
    def __init__(self, name, scratch, output, binary, tools, controls, stack, report):
        self.groups = {k: Path(v["cgroup"]) for k, v in controls["roles"].items()}
        self.disks = {role: scratch/role for role in ("collector", "loki")}
        self.directory = scratch/name
        self.directory.mkdir()
        for role, link in (("loki", "loki"), ("collector", "source")):
            data = self.disks[role]/name
            data.mkdir()
            (self.directory/link).symlink_to(data)
        self.bin, self.tools, self.stack, self.report = binary, tools, stack, report
        self.journal = Journal(output, stack)
        self.history, self.live, self.port, self.monitor, grpc = free_ports(5)
        self.a, self.b, self.token = (secrets.token_hex(32) for _ in range(3))
        self.socket = self.directory/"loki-http:0"
        self.env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": str(self.directory),
                    "GOMAXPROCS": "2", "GOGC": "100", "GOMEMLIMIT": "off", "TRIAL_DIR": str(self.directory),
                    "OBS_BENCH_LISTEN": f"127.0.0.1:{self.port}", "OBS_BENCH_LOKI_SOCKET": str(self.socket),
                    "OBS_BENCH_TOKEN_A": self.a, "OBS_BENCH_TOKEN_B": self.b, "SOURCE_TOKEN": self.token,
                    "LOKI_GRPC_PORT": str(grpc), "HISTORY_PORT": str(self.history), "LIVE_PORT": str(self.live),
                    "METRICS_PORT": str(self.monitor), "RECEIVER_PORT": str(self.port)}
        self.receiver = None
        self.phase = "startup"
        self.report.update(process_usage=[], peak_queue={}, peak_workers_observed=0)
        self.start_loki()
        cmd = [tools/"otelcol-contrib", "--config", HERE/"collector.yaml", "--set",
               "exporters.otlp_http/history.sending_queue.queue_size=24", "--set",
               "exporters.otlp_http/live.sending_queue.num_consumers=16"]
        if name == "paced":
            relay = stack.enter_context(paced_history(self.port, .15))
            cmd.extend(["--set", f"exporters.otlp_http/history.endpoint=http://127.0.0.1:{relay}"])
        self.source = self.child("collector", cmd)
        wait_for(lambda: self.source.alive() and http(self.monitor, "/metrics")[0] == 200, "Collector readiness")
        for port in (self.history, self.live):
            wait_for(lambda: self.source.alive() and http(port, "/v1/logs", payload=b"{}")[0] in (401, 403),
                     "authenticated source listener")
        self.verify_membership("collector", self.source)
        self.done = threading.Event()
        pool = stack.enter_context(ThreadPoolExecutor(1))
        self.monitoring = pool.submit(self.sample_loop)
        stack.callback(self.stop_sampling)

    def child(self, role, command, **env):
        # Join before exec so initial allocations and runtime threads inherit the budget.
        launch = ["/bin/sh", "-ec", 'echo $$ > "$1/cgroup.procs"; shift; exec "$@"', "join-cgroup",
                  self.groups[role], *command]
        cwd = self.disks[role]/self.directory.name if role in self.disks else self.directory
        return Child(role, launch, self.env | env, cwd, self.report["process_usage"], self.stack)

    def verify_membership(self, role, child):
        require(str(child.proc.pid) in (self.groups[role]/"cgroup.procs").read_text().split(),
                f"{role} running outside assigned cgroup")

    def start_loki(self):
        self.socket.unlink(missing_ok=True)
        self.loki = self.child("loki", [self.tools/"loki", "-config.expand-env=true", "-config.file="+str(HERE/"loki.yaml")])
        wait_for(lambda: self.loki.alive() and http(self.socket, "/ready")[0] == 200, "Loki readiness", 80)
        self.verify_membership("loki", self.loki)

    def start_receiver(self):
        child = self.child("receiver", [self.bin], GOMAXPROCS="4", OBS_BENCH_PROFILE="reference16")
        wait_for(lambda: child.alive() and self.counts().get("worker_limit") == 16, "reference receiver readiness")
        self.verify_membership("receiver", child)
        self.receiver = child

    def counts(self):
        status, data = http(self.port, "/bench/v1/counts", token=self.a)
        return json.loads(data) if status == 200 else {}

    def stop_sampling(self):
        self.done.set()
        self.monitoring.result()

    def sample_loop(self):
        while not self.done.is_set():
            now = time.monotonic_ns()
            self.source.alive()
            source = source_snapshot(self.monitor)
            try:
                counts = self.counts() if self.receiver else None
            except (OSError, client.HTTPException):
                counts = None
            resources = resource_sample(self.groups, self.disks)
            self.journal.add("sample", start_ns=now, end_ns=time.monotonic_ns(), phase=self.phase, source=source,
                             receiver=counts, resources=resources)
            for lane in ("history", "live"):
                q = source[lane]["queue_requests"]
                if q is not None:
                    peaks = self.report["peak_queue"]
                    peaks[lane] = max(q, peaks.get(lane, 0))
            if counts:
                self.report["peak_workers_observed"] = max(self.report["peak_workers_observed"], counts["workers_busy"])
            self.done.wait(.5)

    def query(self, low, high, purpose, *, other=False):
        start = time.monotonic_ns()
        try:
            status, data = http(self.port, f"/bench/v1/logs?start={low}&end={high}", token=self.b if other else self.a)
        except (OSError, client.HTTPException):
            status, data = None, None
        end = time.monotonic_ns()
        found = rows(data) if status == 200 else None
        entry = dict(start_ns=start, end_ns=end, low=low, high=high, purpose=purpose, status=status)
        if found is not None:
            if purpose in ("settled", "restored", "tenant_b"):
                entry["rows"] = found
            else:
                entry["ids"] = [int(r.line[6:]) for r in found]
        self.journal.add("query", **entry)
        return found, end

    def offer(self, plan, lane, start, completed=None):
        def record(item):
            self.journal.add("source", **item)
            if completed is not None:
                completed[item["first"]] = item
        return offer(plan, lane, self.token, start, record)

    def drain(self, known_history, known_live):
        def check():
            snapshot = source_snapshot(self.monitor)
            self.report["last_source"] = snapshot
            return all(snapshot[lane]["queue_requests"] == 0 and
                       (snapshot[lane]["sent_records"] == count or (not count and snapshot[lane]["sent_records"] is None))
                       for lane, count in (("history", known_history), ("live", known_live)))
        wait_for(check, "source queue drain and exporter counts", 35)

    def scan(self, windows, purpose, offers):
        observed, batches = [], []
        for part, low, high in windows:
            found, _ = self.query(low, high, purpose)
            require(found is not None, purpose + " query unavailable")
            require(len(found) < 512, purpose + " query reached the result cap")
            observed.extend(found)
            batches.extend(part)
        result = reconcile(observed, batches, offers)
        self.report.setdefault("integrity", {})[purpose] = result
        require(not any(result[k] for k in ("missing_accepted", "unexpected_rows", "duplicate_rows",
                                             "visible_without_source_ack")), f"{purpose} integrity {result}")


def observe(case, part_keys, low, high, purpose, first_seen):
    found, end = case.query(low, high, purpose)
    if found is None:
        return set()
    seen = Counter(found)
    extra = seen-part_keys
    if extra:
        case.journal.add("invalid_rows", rows=list(extra.elements()))
        raise RuntimeError("unexpected or duplicated rows during " + purpose)
    ids = {int(r.line[6:]) for r in seen}
    for i in ids:
        first_seen.setdefault(i, end)
    return ids


def run_load(case, plan, phases):
    start = time.monotonic_ns()
    report = case.report["load"] = {"start_ns": start}
    durations = {name: seconds for name, seconds, _ in phases}
    # Sequential fixed windows keep queries bounded while measuring first observed visibility.
    # A stalled window is censored at the drain deadline; the independent settled scan covers every window.
    first_seen, completed = {}, {}
    with ThreadPoolExecutor(1) as pool:
        job = pool.submit(case.offer, plan, case.live, start, completed)
        deadline = start + (sum(durations.values())+25)*NS
        for part, low, high in partition(plan):
            keys = Counter(k for b in part for k in b.keys())
            while time.monotonic_ns() < deadline:
                elapsed = (time.monotonic_ns()-start)/NS
                phase_end = 0
                for name, seconds, _ in phases:
                    phase_end += seconds
                    if elapsed < phase_end:
                        case.phase = name
                        break
                else:
                    case.phase = "settling"
                seen = observe(case, keys, low, high, "load_progress", first_seen)
                if len(seen) == len(keys):
                    break
                if all(b.first in completed for b in part):
                    eligible = {i for b in part if completed[b.first]["outcome"] == "source_ack"
                                for i in range(b.first, b.first+b.count)}
                    if eligible <= seen:
                        break
                if job.done():
                    job.result()
                time.sleep(.20)
        journal = job.result()
    for name, seconds, _ in phases:
        batches = [b for b in plan if b.phase == name]
        report[name] = summarize(batches, journal, first_seen, start+batches[0].offset_ns, seconds)
    live_ack = sum(b.count for b in plan if journal[b.first]["outcome"] == "source_ack")
    report["full_demand_served"] = live_ack == sum(b.count for b in plan)
    report["settling_started_ns"] = time.monotonic_ns()
    case.drain(0, live_ack)
    counts = report["receiver_before_disconnect"] = case.counts()
    require(counts["store_ack_records"] == live_ack and counts["outstanding_records"] == 0,
            "load receiver acknowledgements disagree with source")
    case.receiver.stop()
    case.receiver = None
    return journal, live_ack


def recover(case, history, overflow, fresh, previous_live):
    result = case.report["recovery"] = {}
    case.phase = "offline"
    offline = time.monotonic_ns()
    journal = case.offer(history, case.history, offline)
    full = result["before_overflow"] = source_snapshot(case.monitor)
    require(full["history"]["queue_requests"] == 24 and full["history"]["accepted_records"] == 96,
            "offline history admission differs from plan")
    journal.update(case.offer(overflow, case.history, time.monotonic_ns()))
    pressure = result["after_overflow"] = source_snapshot(case.monitor)
    result["overflow_outcomes"] = dict(Counter(journal[b.first]["outcome"] for b in overflow))
    require(pressure["history"]["enqueue_failed_records"] == 8 and
            result["overflow_outcomes"] == {"queue_full": 8}, "queue-full responses and source counters disagree")
    time.sleep(max(0, 3-(time.monotonic_ns()-offline)/NS))
    case.phase = "replay"
    start = result["reconnect_ns"] = time.monotonic_ns()
    result["offline_ms"] = (start-offline)/1e6
    case.start_receiver()
    result["receiver_ready_ms"] = (time.monotonic_ns()-start)/1e6
    keys = Counter(k for b in history+fresh for k in b.keys())
    hids = {i for b in history for i in range(b.first, b.first+b.count)}
    first_seen, progress = {}, []
    low, high = history[0].event_ns-1, fresh[-1].event_ns+1
    history_keys = Counter(k for b in history for k in b.keys())
    fresh_keys = keys-history_keys

    def probe():
        if previous_live:
            # The sustained phase occupied the intervening event-time range.
            seen = observe(case, history_keys, low, overflow[-1].event_ns+1, "history_progress", first_seen)
            seen |= observe(case, fresh_keys, fresh[0].event_ns-1, high, "fresh_progress", first_seen)
        else:
            seen = observe(case, keys, low, high, "recovery_progress", first_seen)
        if seen:
            now = time.monotonic_ns()
            sample = {"history_unique": len(seen & hids), "live_unique": len(seen-hids),
                      "elapsed_ms": (now-start)/1e6}
            if not progress or any(sample[k] != progress[-1][k] for k in ("history_unique", "live_unique")):
                progress.append(sample)
        return seen
    if case.directory.name == "paced":
        prefix = wait_for(probe, "historical prefix before paced overlap")
        require(0 < len(prefix) < len(hids), "no partial history prefix")
    live_start = time.monotonic_ns()
    with ThreadPoolExecutor(1) as pool:
        job = pool.submit(case.offer, fresh, case.live, live_start)
        wait_for(lambda: len(probe()) == len(keys), "both classes queryable", 30)
        journal.update(job.result())
    result["progress"] = progress
    if case.directory.name == "paced":
        result["overlap_witness"] = overlap_witness(progress, 96, 24)
    for lane, batches, eligible in (("history", history, start), ("fresh", fresh, live_start)):
        observed = sorted({first_seen[i] for b in batches for i in range(b.first, b.first+b.count)})
        result[lane] = {"first_visible_ms": (observed[0]-start)/1e6,
                        "complete_ms": (observed[-1]-start)/1e6,
                        "longest_observed_progress_gap_ms": max(b-a for a, b in zip([eligible]+observed, observed))/1e6}
    result["fresh_visible_from_intended"] = distribution((first_seen[b.first]-journal[b.first]["intended_ns"])/NS
                                                          for b in fresh)
    result["fresh_seen_before_full_history"] = result["fresh"]["first_visible_ms"] < result["history"]["complete_ms"]
    case.drain(96, previous_live+24)
    result["drained_ms"] = (time.monotonic_ns()-start)/1e6
    counts = result["receiver_after_replay"] = case.counts()
    require(counts["store_ack_records"] == 120 and counts["store_unknown_records"] == 0 and
            counts["outstanding_records"] == 0 and counts["invalid_records"] == 0, "recovery write accounting")
    return journal


def run_case(case, phases):
    load = []
    if case.directory.name == "natural":
        case.start_receiver()
        case.report["load_epoch_ns"] = time.time_ns()
        load = load_plan(case.report["load_epoch_ns"], phases)
    for batch in load:
        case.journal.add("plan", **asdict(batch))
    submissions, live_ack = run_load(case, load, phases) if load else ({}, 0)
    epoch = case.report["recovery_epoch_ns"] = time.time_ns() + 3*NS
    history, overflow, fresh = recovery_plan(epoch, (load[-1].first+4) if load else 1)
    for batch in history+overflow+fresh:
        case.journal.add("plan", **asdict(batch))
    all_batches = load+history+overflow+fresh
    replay = recover(case, history, overflow, fresh, live_ack)
    submissions.update(replay)
    counts = Counter()
    for b in all_batches:
        counts[submissions[b.first]["outcome"]] += b.count
    case.report["source_outcome_records"] = {k: counts[k] for k in OUTCOMES}
    windows = list(partition(sorted(all_batches, key=lambda b: b.event_ns)))
    case.phase = "settled"
    case.scan(windows, "settled", submissions)
    require(case.query(windows[0][1], windows[-1][2], "tenant_b", other=True)[0] == [], "tenant B isolation")
    case.report["wal"] = metrics(case.socket, ("loki_ingester_wal_disk_full_failures_total",
                                                "loki_ingester_wal_corruptions_total", "loki_ingester_wal_records_logged_total"))
    require(case.report["wal"].get("loki_ingester_wal_records_logged_total", 0) > 0 and
            case.report["wal"].get("loki_ingester_wal_disk_full_failures_total") == 0, "missing healthy WAL evidence")
    case.phase = "store_restart"
    case.loki.stop()
    case.start_loki()
    case.scan(windows, "restored", submissions)
    require(not any(counts[k] for k in ("source_rejected", "timeout_unknown", "source_unknown", "outstanding")),
            "unresolved or invalid source outcome; see dispositions")
    case.report["resources_at_end"] = resource_sample(case.groups, case.disks)


def main():
    scratch, output, binary, tools = map(Path, sys.argv[1:5])
    quick = sys.argv[5] == "quick"
    controls = json.loads((output/"controls.json").read_text())
    driver = Path(controls["roles"]["driver"]["cgroup"])/"cgroup.procs"
    require(str(os.getpid()) in driver.read_text().split(), "driver cgroup mismatch")
    report = {}
    for name in ("natural", "paced"):
        folder = output/name
        folder.mkdir()
        case_report = report[name] = {"status": "incomplete"}
        try:
            with contextlib.ExitStack() as stack:
                case = Case(name, scratch, folder, binary, tools, controls, stack, case_report)
                run_case(case, QUICK_PHASES if quick else PHASES)
            case_report["status"] = "passed"
        except Exception as exc:
            case_report.update(status="failed", error=str(exc))
            print(name, str(exc), file=sys.stderr, flush=True)
        save(output/"result.json", report)
    return int(any(r["status"] != "passed" for r in report.values()))


if __name__ == "__main__":
    raise SystemExit(main())
