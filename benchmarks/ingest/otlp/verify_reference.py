#!/usr/bin/env python3
"""Verify archived evidence offline, including every returned row and the applied limits."""
from collections import Counter, defaultdict
from dataclasses import asdict
import gzip
import json
import lzma
from pathlib import Path
import sys

from linux_budget import BANDWIDTH, BUDGETS, GIB
from recovery import digest, distribution, overlap_witness, require
from reference_load import NS, PHASES, QUICK_PHASES, load_plan, recovery_plan, summarize
from reference import order


def verify_controls(controls, resources):
    require(any(q["kind"] == "tbf" and q["options"]["rate"] == 12_500_000 for q in controls["qdisc"]),
            "network limit mismatch")
    for role, (cores, memory) in BUDGETS.items():
        cfg = controls["roles"][role]
        require(cfg["cpu.max"] == f"{cores * 100000} 100000" and cfg["memory.max"] == str(memory*GIB) and
                cfg["memory.swap.max"] == "0", role + " CPU/memory control mismatch")
        observed = resources[role]
        require(observed["cpu"]["usage_usec"] > 0 and observed["memory_peak"] > 0 and
                observed["events"]["oom_kill"] == 0, role + " resource accounting failed")
        if role in ("collector", "loki"):
            dev = cfg["device"]
            require(cfg["disk_bytes"] == GIB and {dev, f"rbps={BANDWIDTH}", f"wbps={BANDWIDTH}"} <=
                    set(cfg["io_max"].split()), role + " disk control mismatch")
            lines = [line.split()[1:] for line in observed["io"].splitlines() if line.split()[0] == dev]
            require(lines and int(dict(kv.split("=") for kv in lines[0])["wbytes"]) > 0,
                    role + " has no writes charged to its limited device")


def verify_case(directory, result, phases):
    require(result["status"] == "passed", f"{directory}: incomplete trial")
    load = load_plan(result["load_epoch_ns"], phases) if directory.name == "natural" else []
    history, overflow, fresh = recovery_plan(result["recovery_epoch_ns"], load[-1].first+4 if load else 1)
    batches = load+history+overflow+fresh
    plan = {b.first: b for b in batches}
    # Construct expected fields directly from the charter, independently of both receivers and the query parser.
    ids = {b.first+i: b.event_ns+i for b in batches for i in range(b.count)}
    planned, source = set(), {}
    visible = {k: Counter() for k in ("settled", "restored")}
    latencies, query_errors = defaultdict(list), Counter()
    receiver_samples = defaultdict(list)
    first_seen, progress = {}, []
    history_ids = {i for b in history for i in range(b.first, b.first+b.count)}
    fresh_ids = {b.first for b in fresh}
    # The long-lived review archive losslessly recompresses the hosted JSONL streams.
    raw = directory/"events.jsonl.gz"
    stream = gzip.open(raw, "rt") if raw.exists() else lzma.open(directory/"events.jsonl.xz", "rt")
    with stream as f:
        for line in f:
            e = json.loads(line)
            kind = e.pop("kind")
            if kind == "plan":
                first = e["first"]
                require(first not in planned and first in plan and e == asdict(plan[first]), "fixture plan mismatch")
                planned.add(first)
            elif kind == "source":
                first = e["first"]
                require(first not in source and first in plan, "duplicate or unscheduled source outcome")
                source[first] = e
            elif kind == "sample" and e["receiver"]:
                receiver_samples[e["phase"]].append((e["end_ns"], e["receiver"]))
            elif kind == "query":
                purpose = e["purpose"]
                if e["status"] != 200:
                    query_errors[purpose] += 1
                    continue
                latencies[purpose].append((e["end_ns"]-e["start_ns"])/NS)
                if purpose in visible:
                    require(len(e["rows"]) < 512, "saturated query result")
                    for row in e["rows"]:
                        seq = int(row[1].removeprefix("bench-"))
                        stamp = ids.get(seq)
                        require(stamp is not None and row == [stamp, f"bench-{seq}", str(stamp+11), "m1-bench",
                                "9", f"{seq:032x}", f"{seq:016x}"], "row field or identity mismatch")
                        visible[purpose][seq] += 1
                elif purpose == "tenant_b":
                    require(e["rows"] == [], "cross-tenant read")
                else:
                    observed = set(e["ids"])
                    require(len(observed) == len(e["ids"]) and observed <= ids.keys(), "invalid progress IDs")
                    for i in observed:
                        first_seen.setdefault(i, e["end_ns"])
                    if purpose == "recovery_progress":
                        progress.append({"elapsed_ms": e["end_ns"]/1e6, "history_unique": len(observed & history_ids),
                                         "live_unique": len(observed & fresh_ids)})
    require(planned == plan.keys() == source.keys(), "incomplete demand accounting")
    accepted = Counter(i for b in batches if source[b.first]["outcome"] == "source_ack"
                       for i in range(b.first, b.first+b.count))
    require(visible["settled"] == visible["restored"] == accepted, "independent read or multiplicity mismatch")
    require(len(latencies["tenant_b"]) == 1, "missing tenant B read")
    require(all(source[b.first]["outcome"] == "queue_full" for b in overflow), "missing explicit queue refusals")
    totals = Counter()
    for b in batches:
        totals[source[b.first]["outcome"]] += b.count
    require(totals == Counter(result["source_outcome_records"]) and
            not (set(totals)-{"source_ack", "queue_full", "unsent"}), "source accounting mismatch")
    require(all(source[b.first]["outcome"] == "source_ack" for b in history+fresh), "incomplete recovery corpus")
    if directory.name == "paced":
        overlap_witness(progress, len(history_ids), len(fresh_ids))
    rates = {}
    for phase, seconds, _ in (phases if load else ()):
        part = [b for b in load if b.phase == phase]
        start = source[part[0].first]["intended_ns"]
        require(summarize(part, source, first_seen, start, seconds) == result["load"][phase],
                phase + " raw demand, latency or rate disagrees with result")
        samples = receiver_samples[phase]
        if len(samples) >= 2:
            t0, c0 = samples[0]
            t1, c1 = samples[-1]
            dt = (t1-t0)/NS
            rates[phase] = {"sample_seconds": dt, "store_ack_records_s":
                            (c1["store_ack_records"]-c0["store_ack_records"])/dt,
                            "store_ack_protobuf_bytes_s": (c1["store_ack_bytes"]-c0["store_ack_bytes"])/dt}
    return {"scheduled": len(ids), "source_outcomes": totals, "exact_rows_at_each_read": accepted.total(),
            "query_latency": {p: distribution(times) for p, times in latencies.items()},
            "query_errors": query_errors, "sampled_write_rates": rates}


def verify(folder):
    manifest = json.loads((folder/"manifest.json").read_text())
    require(manifest["profile"] in ("reference", "quick") and manifest["order"] == order(), "pair plan mismatch")
    phases = PHASES if manifest["profile"] == "reference" else QUICK_PHASES
    require(manifest["phases"] == [list(p) for p in phases], "phase plan mismatch")
    for entry in (folder/"SHA256SUMS").read_text().splitlines():
        expected, name = entry.split(maxsplit=1)
        path = (folder/name).resolve()
        require(path.is_relative_to(folder.resolve()) and digest(path) == expected, "artifact checksum mismatch")
    audit = {}
    for pair, variants in manifest["pairs"].items():
        require(list(variants) == manifest["order"][int(pair)-1] and set(variants.values()) == {"passed"},
                "pair incomplete or out of order")
        for variant in variants:
            directory = folder/f"pair-{pair}"/variant
            result = json.loads((directory/"result.json").read_text())
            controls = json.loads((directory/"controls.json").read_text())
            verify_controls(controls, result["natural"]["resources_at_end"])
            audit[f"{pair}-{variant}"] = {n: verify_case(directory/n, result[n], phases) for n in ("natural", "paced")}
    require(audit, "empty evidence")
    return audit


if __name__ == "__main__":
    print(json.dumps(verify(Path(sys.argv[1])), indent=2))
