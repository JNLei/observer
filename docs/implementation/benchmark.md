# Rust versus Go: benchmark design and current scope

This is a **functional test foundation, not the production-shaped language
comparison** in architecture §24.2. It does not establish a performance
winner, storage choice, language commitment, recovery guarantee, SLO or
security claim. M1 is partial; TEST-038 is not passed. No numeric release
threshold is selected. Use [roadmap M2](roadmap.md#m2--production-shaped-language-and-recovery-gate)
for the required next gate.

## What is runnable now

`benchmarks/ingest/{rust,go}` implement the same private synthetic protocol
and downstream behavior using only each language's standard library. Both are
loopback-only, use fixed workers and a finite queue of accepted connections,
cap frame length before allocation, apply an absolute forwarding deadline and
send an explicit overload response. The deadline starts when a worker dequeues
a connection; queue wait is outside it and response writes have a separate
timeout. Client timeouts apply per socket operation. The OS listen backlog,
Go runtime threads, memory and CPU are **not** quota-enforced or measured by
this local test. Both paths perform one TCP connection and one synthetic frame per attempt. No compression,
batching, reuse, or retry is present. These are prototype-only details, not a
platform API, transport decision, collector, OTLP adapter or production receiver.

On connection, a worker sends `4` (ready) before the client transmits a frame;
the acceptor sends `1` (busy) if the bounded queue is full. The frame is a
big-endian 64-bit logical test ID, a big-endian 32-bit payload length and that
many bytes. After a ready response the client sends the frame and waits for a
final byte: `0` after the mock validates the payload and echoes the ID, `2` for
a failed or ambiguous downstream exchange, or `3` for an invalid/short/oversize
frame. Waiting for admission prevents a normal large-frame client from sending
an unread payload while a busy response is being closed. A disconnect, timeout,
or lost response can still make an outcome unknown; neither a successful server
write nor an overload counter proves that the client received the response.
An acknowledgement is **only a volatile sink echo**, with no durable,
queryable or processed claim.
Status `2`, client timeout, and transport error can all hide a completed
sink-side operation; retries are deliberately absent and no exactly-once claim
is made. The sink's `ack_writes` do not prove client receipt.

The harness runs the same Python sink and open-loop, fixed-rate arrival schedule
against either binary. Each phase can choose rate, duration and sink behavior:
healthy, accept-and-disconnect, or a delayed reply. The disconnect mode closes
new sink connections; it is a deterministic application-level substitute,
**not** a network partition or fault-injection proof. Sink behavior is selected
when each connection begins, while outcomes are grouped by scheduled phase, so
a queued request can encounter a later phase. The next healthy phase exercises
new connections. Delayed sink handlers may outlive a client timeout;
the harness reports sink handlers still active at its snapshot. Fixed forwarder
and sink worker limits bound simultaneous work, but the harness does not measure
actual remote residual work or prove TEST-044. The sink and generator use
bounded thread counts and queues, and the harness caps scheduled records at one
million because it keeps IDs and latency observations in memory. Payload size,
queue, worker and deadline limits are validated before a run.

## Accounting and scenario interpretation

For each phase, `scheduled = queued + unsent_generator_full`, and after the
queue drains `queued = attempted = sum(outcome counts)`. `connected` counts
successful client connects and `sent` counts completed local frame writes after
ready; neither proves a durable or queryable record. Outcomes include
acknowledged, overloaded, downstream failure/unknown, invalid, timeout/unknown, transport error/unknown,
and protocol error/unknown. A generator that falls behind does not silently
lower scheduled demand. The sink independently counts valid payloads, unique
IDs, duplicate IDs, capacity rejections, disconnects, ack attempts
(`ack_write_attempts`), completed writes (`ack_writes`) and failed writes
(`ack_write_failures`). An active handler can still have an outstanding write;
this is not a durable reconciliation ledger.

When generator work finishes, the parent requests a counter snapshot and a
bounded drain over the child's standard-input control channel. The forwarder
finishes accepting connections already visible to its listener, closes that
listener and drains its bounded worker queue. Its final counters distinguish
accepted, admitted, overloaded, queued, in-flight and completed connections;
they also distinguish ready-write failures, final status choices, and response
write successes/failures. `accepted = admitted + overloaded`,
`admitted = queued + in_flight + completed`, and the reported terminal status
and write counters reconcile to `completed`. The harness rejects inconsistent
snapshots and checks observed client ACK/rejection/status counts against the
completed server counts. These are aggregate checks, not per-record proof of
delivery; a server response write can succeed without client receipt.

If `drain_timeout_ms` expires, the harness takes a final **pre-termination**
snapshot, terminates the child and reports `forced_after_timeout`. Queued,
in-flight and overload-response writes in that snapshot may finish before
termination, so the exact number aborted and their final dispositions are
unknown. A separate bounded `sink_drain_timeout_ms` reports sink handlers still
active at its snapshot; they can outlive the child. Both default to 5,000 ms
and can be set from 1 to 60,000 ms. The OS listen backlogs, connections the
forwarder did not accept, and asynchronous remote work are not fully
accounted. A successful forwarder drain describes accepted forwarder work,
not source durability or sink/query visibility. Unexpected child exit fails the
scenario; captured stderr is limited to its last 8 KiB.

Results include P50/P95/P99 for acknowledgements from intended arrival and from
attempt start, plus generator queue wait, scheduling lag, and intended arrival
to attempt for attempted work. Failed and unsent demand has counts rather than
invented latency values; successful-only distributions are explicitly censored.
The generator's scheduling jitter, phase transitions and shared process
contention can affect results. Tests inject a known stall and check visible
unsent demand, timeout/unknown accounting and increased completed latency. This
is preliminary coverage of TEST-054, not the multi-second, production-scored
acceptance procedure. It does not provide capacity or fairness evidence.

The included `smoke.json` contains short steady, burst, disconnect and reconnect
phases for functional verification. Run both with the same scenario:

```sh
./scripts/bench-smoke.sh
```

The script saves JSON and build/environment metadata under the ignored
`benchmarks/ingest/results/` directory. Individual runs can use
`python3 benchmarks/ingest/harness.py --server PATH --scenario FILE --output FILE`.
The scenario and binary hash are embedded in each result. The default script
runs variants sequentially and does not control machine load, quotas, warmup,
trial ordering, CPU affinity or deployment-level memory. Its numbers are not
suitable for scoring.

## Workload charter and full comparison still required

Before a scored run, the owner must approve a profile specifying workload and
payload distributions, candidate baseline(s), arrival model, machine and
resource quotas, cost ceiling, durability/replication and failure scope,
allowed loss/duplicates, retention/late windows, concurrency/batching,
security/redaction, query/alert/background mix and actual freshness/backlog
acceptance thresholds. Pin compiler, dependencies, build flags and runtime/GC
settings; record source revisions and optimization effort. Use repeated,
randomized warm/cold trials and verify generator capacity. Report scheduled,
sent, admitted, durable, queryable, processed, rejected, timed-out, unsent,
missing and duplicate work at their actual boundaries; report intended-arrival
and completed-service tails without inventing censored latencies.

The required full slice includes the same real OTLP decoding/validation,
shared-auth enforcement, capture/redaction, batching/compression, bounded
asynchronous I/O, durable retry/replay, identical candidate signal-store
adapter, query/evidence serialization and async projection handoff on both
sides. Include realistic metrics, logs, traces and deep/wide agent traces;
run mixed ingestion, queries, compaction, alerting and evaluation. Validate
security, correctness, failure recovery and actual residual work before
comparing throughput or latency. Disclose whether this is a language comparison
or a stack comparison when libraries/semantics differ. Capture CPU quota
versus OS threads, Go runtime/GC and Rust runtime/blocking limits; measure
process and deployment memory/pressure with platform-appropriate tools,
including cgroup metrics for Linux containers. Include the defined 16-thread
reference and a resource-scaled profile. TEST-035–038 and TEST-044/051/054/055
remain open until their full applicable gates pass.

### REC-REF-24

The required reference exercise is a real 24-hour source-to-backend disconnect
with configured source durability, followed by replay while fresh traffic,
queries and relevant background work continue. Measure actual reconnect, fresh
and backlog visibility, original event-time correctness, duplicates, loss and
resources all the way to the declared queryable boundary; distinguish optional
projection/evaluation lag. Model capacity as `B / (μ − λ)` only when the
constant-rate assumptions hold and `μ > λ`. Clearing 24 hours of backlog in one
hour while normal traffic continues illustrates roughly 25× steady capacity; it
is **not** an adopted one-hour SLO. A scaled simulation is useful during
development but cannot replace the actual endurance test. This repository does
not yet have a durable source queue, history replay, fresh/backlog scheduler,
query visibility, or an executable REC-REF-24 run. The million-event harness cap
also prevents many realistic 24-hour profiles. REC-REF-24 is **unrun**.

No pass/fail or release-readiness claim can be made without the approved charter,
production-shaped implementations, end-to-end reconciliation, full recovery
suite and an architecture-reviewed language decision record.
