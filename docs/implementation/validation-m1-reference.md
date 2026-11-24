# M1 controlled 16-worker log ingestion

Five hosted pairs observed median sustained queryable rates of 99.97 records/s
for Rust and 99.99 for Go. During the 400 records/s burst they observed
399.09 and 399.08 records/s. Every source-confirmed record matched the expected
fields and multiplicity on both sides of Loki restart. One Rust run left 248
records unsent at the generator. The other nine hosted runs served the full
healthy demand. The evidence supports independent review of this logs slice.
Milestone 1 and the language decision remain open.

## Workload and measurement boundaries

Each pair ran the two release builds sequentially on the same machine. Seed 1
placed Rust first in pairs 1, 4 and 5 and Go first in pairs 2 and 3. A new
receiver, stock Collector Contrib 0.165.0 and single-node Loki 3.7.10 used
fresh private state for each variant. The data path verified authentication,
normalized OTLP logs, a fsync-backed source queue, Loki HTTP 204 responses
and authorized event-time reads. The capture policy retained the generated
line, event and observed timestamps, fixed service, severity, trace ID and
span ID. [Workload charter](workload-charter.md#controlled-16-worker-logs-profile)

| Work performed per receiver trial | Configuration |
| --- | --- |
| Sustained, burst, return | Open-loop 100 records/s for 600 s, 400/s for 60 s, then 100/s for 10 s. Four records per request, with 32, 256 and 1,024 B bodies cycled by sequence. Eight generator senders with 32 further exchanges allowed. |
| Natural recovery | Take the receiver offline for at least 3 s. Fill the historical lane with 24 four-record exports spanning 24 event-time hours, observe eight explicit one-record refusals, reconnect without artificial pacing and offer 24 fresh records at 10/s. |
| Paced overlap control | Repeat the 96 historical, eight overflow and 24 fresh records on fresh data directories. Pace history exchanges by 150 ms and arm fresh arrivals on the first visible historical prefix. |
| Capacity | 16 concurrent receiver write workers globally, 32 connections, 4 decoders and one reader per tenant. The Linux quotas are 4 CPU / 1 GiB for the receiver, 2 CPU / 2 GiB each for Collector and Loki, and 1 CPU / 1 GiB for the driver. Zero swap. Each downstream service has a 1 GiB ext4 device capped at 50 MiB/s in each direction. TCP shares a 100 Mbit/s loopback limit. The store uses a Unix socket. |
| Query oracle | Check exact multiplicities and original fields in disjoint time windows with at most 256 expected rows. Check tenant B, kill Loki, reopen the same store and repeat the scan. Each actual settled/restored row and each source disposition is archived. |

A processing worker is a request holding a receiver reservation from write
admission through the downstream outcome. The shared sink test held all 16
workers inside real store exchanges, observed 16 busy, and received HTTP 429
on a request from the other tenant. Both receivers released all 16 after the
sink acknowledged them. The benchmark records OS thread counts separately.

The arrival-to-source-ACK clock includes generator wait. The attempt clock
starts after JSON preparation, immediately before HTTP submission. Visibility
latency runs from intended arrival through the end of the first successful
probe containing that exact record. Probe intervals, query duration and
observer scheduling contribute to this upper bound. Sampled receiver and
Collector counters, phase durations and byte rates can be recovered from the
raw journals. Reconnect starts immediately before starting the receiver.

## Hosted measurements

Pairs ran on separate four-logical-CPU Ubuntu runners. Rates below use the fixed offered load. Queryable throughput counts first-observed identities through the later of the end of the offering interval and the last visibility probe. The source latency ends at Collector enqueue. Each value in the latency columns is the median of five trial percentiles.

| Phase | Receiver | Queryable records/s | Store ACK records/s¹ | Source ACK P50 / P95 / P99 (ms) | Queryable P50 / P95 / P99 (ms) |
| --- | --- | ---: | ---: | ---: | ---: |
| Sustained | Rust | 99.97 | 100.00 | 1.29 / 1.78 / 2.08 | 110.7 / 204.8 / 216.4 |
| Sustained | Go | 99.99 | 100.00 | 1.29 / 1.89 / 2.20 | 110.6 / 204.8 / 216.7 |
| Burst | Rust | 399.09 | 400.01 | 0.90 / 1.21 / 2.52 | 114.4 / 210.4 / 222.2 |
| Burst | Go | 399.08 | 400.03 | 0.87 / 1.14 / 2.53 | 114.1 / 210.2 / 221.6 |
| Return | Rust | 100.00 | 99.71 | 1.18 / 1.55 / 2.06 | 109.0 / 205.1 / 217.3 |
| Return | Go | 100.00 | 99.71 | 1.47 / 1.80 / 2.16 | 110.9 / 207.7 / 221.8 |

¹ Difference between receiver 204 counters at the first and last sampler completions in the phase, divided by sampled elapsed time. The raw journal also gives input gzip bytes, acknowledged protobuf bytes, generator wait, attempt-to-ACK latency and successful query service times.

| Pair and order | Burst queryable records/s, Rust / Go | Burst queryable P99 ms, Rust / Go | Natural full history ms, Rust / Go | Receiver CPU s, Rust / Go |
| --- | ---: | ---: | ---: | ---: |
| 1, Rust then Go | 399.08 / 399.05 | 331.3 / 277.4 | 483 / 181 | 11.42 / 19.05 |
| 2, Go then Rust | 399.21 / 399.12 | 221.5 / 221.8 | 1294 / 1427 | 10.63 / 16.49 |
| 3, Go then Rust | 399.13 / 399.08 | 222.3 / 221.5 | 719 / 640 | 10.02 / 17.67 |
| 4, Rust then Go | 399.08 / 399.07 | 222.2 / 221.6 | 416 / 873 | 9.96 / 15.96 |
| 5, Rust then Go | 399.09 / 399.13 | 220.7 / 221.3 | 327 / 402 | 9.59 / 16.51 |

## Recovery and resource use

Recovery times are measured from the reconnect request. A progress gap runs from lane eligibility to the first observed advance, or between subsequent advances up to completion. Each cell shows the median and full range over five trials.

| Observation | Rust | Go |
| --- | ---: | ---: |
| First fresh visibility, natural (ms) | 97 (92–101) | 98 (94–107) |
| Full history visibility, natural (ms) | 483 (327–1294) | 640 (181–1427) |
| Longest historical progress gap, natural (ms) | 483 (327–1294) | 640 (104–1340) |
| Longest fresh progress gap, natural (ms) | 161 (159–164) | 165 (162–320) |
| Both source queues drained and sent counts reconciled, natural (ms) | 2400 (2389–2433) | 2393 (2366–2402) |
| Full history visibility, paced (ms) | 4310 (3719–4669) | 4006 (3792–4948) |
| Longest fresh progress gap, paced (ms) | 153 (151–154) | 155 (154–158) |
| Receiver RSS high-water after exec (MiB) | 5.0 (4.9–5.6) | 20.0 (19.9–20.2) |
| Sampled busy application workers, natural peak | 1 (1–1) | 1 (1–2) |
| Receiver OS threads, natural sampled peak | 6 (6–9) | 8 (8–8) |
| Live source queue, natural sampled peak (requests) | 1 (1–15) | 1 (1–11) |

The following deployment measurements cover the natural case. Each value is a five-trial median. CPU is cumulative cgroup time. Memory is the kernel charged peak. Disk growth is the highest sampled allocated filesystem space above the empty ext4 baseline. Write volume uses the limited loop device's counter once.

| Role | CPU s, Rust / Go | Peak memory MiB, Rust / Go | Peak disk growth MiB, Rust / Go | Charged writes MiB, Rust / Go |
| --- | ---: | ---: | ---: | ---: |
| Receiver | 10.02 / 16.51 | 2.69 / 8.68 | — | — |
| Collector | 35.01 / 35.15 | 67.09 / 66.99 | 0.17 / 0.17 | 666.32 / 666.32 |
| Loki | 65.89 / 65.75 | 165.97 / 171.38 | 13.35 / 13.35 | 18.04 / 18.04 |
| Driver | 19.04 / 19.51 | 133.90 / 134.47 | — | — |

## Delivery and fairness

These totals count record instances across the five hosted trials per receiver.
Both settled and restored reads must match the source-confirmed multiset
separately. The 80 refusals per receiver comprise eight overflow records in
each of ten outage cases. The Collector reported the corresponding failed
enqueues, and none of those identities appeared in a read.

| Hosted record accounting | Rust | Go |
| --- | ---: | ---: |
| Scheduled | 426,280 | 426,280 |
| Sent to the source | 426,032 | 426,280 |
| Generator unsent | 248 | 0 |
| Explicit source queue-full refusals | 80 | 80 |
| Source-confirmed and visible, each side of restart | 425,952 | 426,200 |
| Missing from source-confirmed records | 0 | 0 |
| Visible excess copies / unexpected rows | 0 / 0 | 0 / 0 |
| Receiver validation rejections / unknown store record attempts | 0 / 0 | 0 / 0 |
| Source timeouts / other unknown source outcomes | 0 / 0 | 0 / 0 |
| Outstanding receiver records / final queued source requests | 0 / 0 | 0 / 0 |

Pair 1 ran on AMD EPYC 9V74 under Linux 7.0.0-1017-azure. Pairs 2–5 ran on
Intel Xeon 6975P-C under Linux 7.0.0-1014-aws. Within each pair both variants
used the same runner. Both receiver binary hashes, both dependency binary
hashes and the configuration hashes matched across hosted pairs for their
respective builds.

In pair 1 the Rust run filled the generator's bounded queue during steady
traffic. It recorded 62 unsent four-record batches over a 2.68 s interval
of intended arrivals. The longest successful intended-to-source-ACK latency
in that phase was 4.073 s. Every one of the 59,752 source-confirmed steady
records was visible. The other nine hosted runs sent all 85,000 healthy-phase
records. The tables retain pair 1. Restricting the comparison to pairs 2–5,
which delivered the full healthy corpus in both variants, gives median burst
visibility P99 of 221.8 ms for Rust and 221.5 ms for Go.

Fresh data became visible before history finished in all ten natural runs.
The paced controls each saw the partial historical/fresh counts advance from
4/1 to 8/2 between successful reads. Natural fresh visibility from intended
arrival had median trial P50/P95/P99 of 44.4/78.6/79.6 ms for Rust and
42.9/78.7/86.4 ms for Go. The probe cadence and the stock retry backoff limit
the precision of these observations.

No hosted receiver, Collector or Loki cgroup reported an OOM kill or CPU
throttling in the natural case. Driver cgroups recorded 13 throttled periods
across ten trials. The largest sampled receiver CPU rate was 0.104 cores
against a four-core quota. The generator and source showed observable
backpressure. This workload leaves the maximum sustainable receiver rate
undetermined. The RSS row uses the executable's sampled Linux VmHWM.
The raw wait4 high-water figure also covers the child launcher lifecycle.

## ARM64 replication

The additional full pair ran Go then Rust in a four-vCPU, 6 GiB Ubuntu VM on an Apple M1 Pro. Rust used a cross-compiled musl release build and Go used a Linux/arm64 build with CGO disabled. The same budgets and workload applied.

| Receiver | Burst queryable records/s | Burst queryable P50 / P95 / P99 (ms) | Natural full history ms | Receiver CPU s | Peak charged MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Rust | 399.11 | 116.9 / 211.9 / 223.3 | 931 | 16.62 | 1.52 |
| Go | 399.12 | 117.1 / 211.6 / 222.7 | 998 | 21.02 | 8.62 |

## Reproduction and checks

The measured source commit is `000203dd7f354be0dd2a310bb70b30058afee342`.
The five-pair hosted workflow passed all six jobs, including the original
foundation checks. The quick push workflow passed both jobs on the same
commit. [Full paired CI](https://github.com/JNLei/observer/actions/runs/45304295878)
and [quick push CI](https://github.com/JNLei/observer/actions/runs/45304295714).

- `./scripts/check.sh` passed locally, including Rust formatting, Clippy, workspace tests, Go formatting/vet/build/tests, the shared 16-write hold/refusal case, and 29 Python tests.
- `./scripts/check-race.sh`, `./scripts/bench-smoke.sh` and `./scripts/bench-recovery.sh` passed locally. Recovery included all three negative controls against actual read responses. Hosted CI also passed the existing synthetic custody regressions.
- `./scripts/bench-reference.sh --pair N` ran for N=1–5 on the hosted matrix. The ARM64 VM ran the same Linux runner and verifier with the separately hashed cross-compiled binaries. The generator stall test preserved unsent work and queue wait. Offline control checks rejected a removed CPU limit, enabled swap, missing device-write charges and a missing qdisc.
- An early short VM run exposed a metrics-before-listener readiness race and generator saturation. The driver now checks both authenticated source listeners before offering data. A checksum delimiter error found during a local offline replay was corrected before the full runs. The retained full observations use the frozen workload.

The permanent archive contains the five hosted pairs and the ARM64 pair.
Its SHA-256 is `234a37f887fe7bda1a10d6f8fcf37b81462e0c1c9650d8b4fbfbb7e1afa401b1`.
Every raw JSONL stream was losslessly recompressed from gzip to XZ and checked
against the decoded original. The archive includes the original file
inventories, the decoded-stream checksums, a separate archive inventory for
each pair, and the original manifests, controls and results.
[Raw trial archive](evidence/reference-000203d.tar) and
[per-trial measurements](evidence/reference-000203d-summary.json).

From the repository root, verify all six pairs without running services or
obtaining elevated privileges.

```sh
evidence_dir=$(mktemp -d)
tar -xf docs/implementation/evidence/reference-000203d.tar -C "$evidence_dir"
for pair in "$evidence_dir"/hosted/pair-* "$evidence_dir"/arm64-vm/pair-*; do
    python3 benchmarks/ingest/otlp/verify_reference.py "$pair" || exit 1
done
```

## Interpretation and remaining gates

The quotas bound consumption on a shared VM. They reserve neither host cores
nor physical disk bandwidth. The 1 GiB device ceiling replaces the charter's
proposed 20 GiB candidate for this bounded corpus. The block limit covers
I/O charged to each private loop device. The host page cache, shared libraries,
hypervisor and backing storage can affect a pair. Whole-process RSS and
cgroup-charged memory have different accounting scopes. The natural-case
resource totals include startup, ingestion, recovery, final reads and the Loki
restart. The paced case reuses the variant's cgroups, so its reported cgroup
peaks are cumulative. Exact per-allocation costs, per-record durable timestamps,
background compaction completion and financial cost were outside the measurements.

Each 204 and query establishes a specific observation. A visible duplicate
row is measurable, while Loki can coalesce identical physical writes. An
observed queue drain establishes source progress. This trial restarts the
receiver and Loki with intact disks. The stock Collector's startup
delete/reinsert window remains unresolved. The earlier synthetic custody
experiment exercised a different owner. [Collector recovery concern](../adr/PROPOSED-loki-poisoned-exports.md#scenario-8-and-production-gates)

REQ-005/006, 019, 026/027, 029 and 035–039 apply. The slice exercises
ADR-I1/I2/I4/I5, ADR-REC-01–06, ADR-LANG-01 and ADR-BENCH-01. It contributes
to TEST-029/030, 032–038, 054 and 055. S03/TEST-044 and S10/TEST-051 still
need the broader residual-work and adversarial pressure tests. Wall-clock
REC-REF-24, noisy-tenant fairness, adaptive admission, scaled envelopes,
multi-signal workloads and maintained-system baselines remain open. The
architecture specification, preferred language status and deferred storage
decision are preserved. This slice proposes no ADR, SLO or cost target.
