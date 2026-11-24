"""Deterministic logs, open-loop source offers and multiset reconciliation."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from http import client
import json
import time

from recovery import distribution, expected_row, HOUR, http, record

NS = 1_000_000_000
PHASES = (("steady", 600, 100), ("burst", 60, 400), ("return", 10, 100))
QUICK_PHASES = (("steady", 5, 100), ("burst", 2, 400), ("return", 2, 100))
OUTCOMES = ("source_ack", "queue_full", "source_rejected", "timeout_unknown", "source_unknown", "unsent", "outstanding")


@dataclass(frozen=True)
class Batch:
    phase: str
    first: int
    count: int
    event_ns: int
    offset_ns: int

    def records(self):
        return [record(self.first + i, self.event_ns + i) for i in range(self.count)]

    def keys(self):
        return [expected_row(r) for r in self.records()]


def load_plan(epoch, phases):
    batches, elapsed, seq = [], 0, 1
    for phase, duration, rate in phases:
        for i in range(duration * rate // 4):
            offset = elapsed + i * 4 * NS // rate
            batches.append(Batch(phase, seq, 4, epoch + offset, offset))
            seq += 4
        elapsed += duration * NS
    return batches


def recovery_plan(epoch, first):
    history = [Batch("history", first + i*4, 4, epoch - (24-i)*HOUR, i*100_000_000) for i in range(24)]
    overflow = [Batch("overflow", first + 96+i, 1, epoch-30*60*NS+i, i*50_000_000) for i in range(8)]
    fresh = [Batch("fresh", first + 104+i, 1, epoch+i*100_000_000, i*100_000_000) for i in range(24)]
    return history, overflow, fresh


def submit(port, token, batch, intended):
    body = json.dumps({"resourceLogs": [{"scopeLogs": [{"logRecords": batch.records()}]}]},
                      separators=(",", ":")).encode()
    started = time.monotonic_ns()
    status, outcome = None, "source_unknown"
    try:
        status, data = http(port, "/v1/logs", token=token, payload=body)
        if status == 200 and not json.loads(data or b"{}").get("partialSuccess"):
            outcome = "source_ack"
        elif status in (429, 500, 503) and b"queue is full" in data.lower():
            outcome = "queue_full"
        elif status in (400, 401, 403, 413, 415):
            outcome = "source_rejected"
    except TimeoutError:
        outcome = "timeout_unknown"
    except (OSError, client.HTTPException, ValueError):
        pass
    return dict(first=batch.first, intended_ns=intended, start_ns=started, end_ns=time.monotonic_ns(),
                json_bytes=len(body), status=status, outcome=outcome)


def offer(batches, port, token, start, emit, *, send=submit, workers=8, capacity=32):
    """One bounded generator queue. The clock advances even while every sender is busy."""
    results, pending = {}, set()

    def finish(future):
        item = future.result()
        results[item["first"]] = item

    def worker(batch, intended):
        item = send(port, token, batch, intended)
        emit(item)
        return item

    with ThreadPoolExecutor(workers) as pool:
        for batch in batches:
            intended = start + batch.offset_ns
            time.sleep(max(0, (intended-time.monotonic_ns())/NS))
            completed = {f for f in pending if f.done()}
            for f in completed:
                finish(f)
            pending -= completed
            if len(pending) < workers + capacity:
                pending.add(pool.submit(worker, batch, intended))
            else:
                item = dict(first=batch.first, intended_ns=intended, outcome="unsent", start_ns=None,
                            end_ns=None, json_bytes=0, status=None)
                emit(item)
                results[batch.first] = item
        for f in pending:
            finish(f)
    return results


def partition(batches, width=64):
    """Nonoverlapping fixed query windows, at most 256 expected records under the 512 row cap."""
    for offset in range(0, len(batches), width):
        part = batches[offset:offset+width]
        low = batches[offset-1].event_ns + batches[offset-1].count if offset else part[0].event_ns-1
        high = part[-1].event_ns + part[-1].count
        yield part, low, high


def reconcile(actual, batches, journal):
    scheduled = Counter()
    accepted = Counter()
    for b in batches:
        keys = b.keys()
        scheduled.update(keys)
        if journal.get(b.first, {}).get("outcome") == "source_ack":
            accepted.update(keys)
    seen = Counter(actual)
    excess = seen - scheduled
    return {"scheduled": scheduled.total(), "source_accepted": accepted.total(), "visible_rows": seen.total(),
            "visible_unique": len(seen), "scheduled_absent": (scheduled-seen).total(),
            "missing_accepted": (accepted-seen).total(),
            "unexpected_rows": sum(n for k, n in excess.items() if k not in scheduled),
            "duplicate_rows": sum(n for k, n in excess.items() if k in scheduled),
            "visible_without_source_ack": (seen & (scheduled-accepted)).total()}


def summarize(batches, journal, first_seen, start, seconds):
    outcomes = Counter()
    for b in batches:
        outcomes[journal.get(b.first, {}).get("outcome", "outstanding")] += b.count
    acks = [journal[b.first] for b in batches if journal.get(b.first, {}).get("outcome") == "source_ack"]
    sent = [journal[b.first] for b in batches if journal.get(b.first, {}).get("start_ns") is not None]
    lag = [(first_seen[i] - journal[b.first]["intended_ns"])/NS for b in batches if b.first in journal
           for i in range(b.first, b.first+b.count) if i in first_seen]
    visibility_end = max((first_seen[i] for b in batches for i in range(b.first, b.first+b.count)
                          if i in first_seen), default=start)
    ack_end = max((a["end_ns"] for a in acks), default=start)
    ack_bytes = sum(a["json_bytes"] for a in acks)
    elapsed = max(seconds, (ack_end-start)/NS)
    return {"scheduled": sum(b.count for b in batches), "sent": sum(b.count for b in batches
            if journal.get(b.first, {}).get("start_ns") is not None), "outcome_records": {k: outcomes[k] for k in OUTCOMES},
            "offered_records_s": sum(b.count for b in batches)/seconds, "source_ack_records_s": outcomes["source_ack"]/elapsed,
            "source_ack_json_bytes_s": ack_bytes/elapsed, "queryable_observed_records_s":
            len(lag)/max(seconds, (visibility_end-start)/NS), "visible_from_intended": distribution(lag),
            "source_ack_from_intended": distribution((x["end_ns"]-x["intended_ns"])/NS for x in acks),
            "source_ack_from_attempt": distribution((x["end_ns"]-x["start_ns"])/NS for x in acks),
            "generator_wait": distribution((x["start_ns"]-x["intended_ns"])/NS for x in sent)}
