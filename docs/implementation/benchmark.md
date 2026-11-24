# Rust versus Go benchmark design

The [workload charter](workload-charter.md) defines the paired workloads and
unresolved scoring criteria. The original synthetic baseline and the OTLP log
trial have different acknowledgement boundaries. TEST-038 remains open.

## OTLP logs and restart trial

Run `./scripts/check.sh` for protocol conformance, `./scripts/check-race.sh`
for Go race instrumentation and `./scripts/bench-recovery.sh` for the real
source-to-query trial. Each recovery run creates an ignored JSON report with
its revision, dirty status, dependency/binary hashes, source dispositions,
receiver counters, visibility samples, latency distributions and per-process
resource usage. Both variants use the same absolute fixture epoch. The
checked-in YAML files contain environment references for ephemeral credentials.

| Boundary | Observable behavior |
| --- | --- |
| Source | The stock Collector receives generated OTLP/HTTP JSON, persists an export request in a finite source-owned queue and replies. Each history/live queue holds 64 requests with one consumer and fsync enabled. The exporter sends gzip binary protobuf, backs off on retry, and records terminal dispositions. The source can retain the original synthetic body. |
| Admission | Both receivers decode the official log export schema and accept only the fixture profile. Each credential selects its own tenant with the same read/write policy. The single-producer fixture assigns the literal `m1-bench` to both tenants. Receivers strip submitted resource, scope, arbitrary attributes and free text. Each accepted record retains original event/observed timestamps, severity, trace/span correlation, the low 8 flag bits and a generated line derived from a positive integer `bench.seq`. Record order within the export is preserved. S02 / TEST-043 remains open for general producer attribution. |
| Write | Each sanitized nonempty batch is sent once per upstream attempt to Loki on a private Unix HTTP socket. Only the pinned Loki handler's HTTP 204 confirms a write. A different status, timeout, disconnect or redirect yields 503 with an unknown write outcome. No receiver-side retry. The source owns retries. |
| OTLP response | Full success is HTTP 200 with an empty protobuf response. Mixed batches report a protobuf partial success after the eligible subset receives 204. Do not retry the entire partial batch. Empty exports succeed without a store write. Wholly invalid records or malformed protobuf return 400, missing/invalid credentials 401, oversize 413, unsupported format/encoding 415 and full tenant admission 429. Error bodies use fixed google.rpc.Status messages. |
| Visibility | The private authenticated read accepts only a start/end pair, with a fixed service selector, credential-derived tenant, 26-hour maximum window, 512 returned rows and 512 KiB response cap. Loki allows 48-hour-old samples and a same-stream reordering window of 26 hours. Polling gives an upper bound on first visibility. The probe verifies actual timestamp, sanitized line, observed timestamp, severity and trace/span IDs in range-query results. |
| Counters | Per-tenant admitted, invalid, acknowledged, unknown and outstanding **record attempts** are distinct from unique visible records. The synchronized snapshot conserves admitted = acknowledged + unknown + outstanding. `store_http_400_attempts` counts downstream batch responses, including mixed commits. The forwarded records on each such attempt contribute to unknown. Input bytes count validated request bodies on the wire. ACK bytes count sanitized protobuf payloads receiving 204. Pre-decode rejection has no inferred record count. |

Collector Contrib 0.165.0 counts all records in an export when its OTLP
exporter returns normally, including a decoded partial-success response.
The two-record partial-success control records two sent, one local rejection,
one store ACK and one queryable row. Each snapshot preserves the raw series
and reports a missing per-lane series as null. After replay, both queue gauges
must explicitly report zero and both sent counters must match the input to
their exporter. Storage evidence comes from the receiver's 204 boundary, WAL
observations and independent reads after a process kill. [Exporter accounting](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.165.0/exporter/exporterhelper/internal/obs_report_sender.go)

The unpaced and controlled runs each compare returned multiplicities, original
event and observed timestamps, line, service, severity and trace/span IDs with
the generated corpus on both sides of restart. Monotonic query-completion
times measure first observed visibility. Only the independently paced run
certifies observed overlap. A separate rejection scenario captures a
permanently stuck history lane, unknown mixed-batch commits and independent
live progress. The JSON retains the pending queue snapshot before the generated
fixture is torn down. [Workload controls](workload-charter.md#controls-added-for-independent-review)
and [proposed custody policy](../adr/PROPOSED-loki-poisoned-exports.md).

The HTTP listeners cap active connections at 12. Each tenant has three
write reservations, one downstream writer and one independent reader. A
maximum of two decoders run concurrently, with 64 KiB wire/inflation limits
and a 256-record request cap. The OS listen backlog, library buffers and
decoded object overhead are additional. Header/body/store deadlines bound
local work. A caller timeout can leave a queued or active transaction. Its
reservation stays occupied through local completion or downstream timeout.
Loki's query timeout and query concurrency limits apply independently.
The conformance sink can confirm residual work while a client has timed out.
Full S03/S10 pressure and residual-work validation remains pending.

The harness uses a trusted single-user host, private scratch directory,
loopback-only source, gateway and internal Loki gRPC listeners, and a private
Loki HTTP socket. Credential checks protect source submission, receiver
admission and the benchmark read API. The operator controls the binaries,
local disk and internal service endpoints. This trust assumption limits the
evidence for production shared-host isolation, revocation and service identity.

Loki's ingester acknowledges a write under its configured WAL behavior. A
successful write alone cannot establish disk/node/power-loss survival. The
trial reads WAL metrics, kills the store process and checks original-time
visibility again using its existing disk. Source queues live outside the
receiver and resume after a Collector process kill. The runner also brings up
the receiver with the store unavailable, observes failed writes and retained
source demand, then kills and restarts the receiver before healthy replay.
Identical physical writes
may be deduplicated on query. Optional processing stages have no measured
watermark in this trial. [OTLP responses](https://opentelemetry.io/docs/specs/otlp/),
[Collector resilience](https://opentelemetry.io/docs/collector/resiliency/),
[Loki write handler at v3.7.10](https://github.com/grafana/loki/blob/v3.7.10/pkg/distributor/http.go)
and [Loki WAL failure scope](https://grafana.com/docs/loki/latest/operations/storage/wal/).

Rust uses Hyper, Prost and generated opentelemetry-proto log messages. Go uses
net/http and generated OpenTelemetry protobuf messages. Each forwards an
uncompressed sanitized batch over a new Unix HTTP connection. Both builds
pin their dependency graphs. Parser internals, HTTP header parsing slack,
runtime scheduling, allocations and GC differ. The native trial has no CPU,
RSS, network or disk throttling. Fixed order and a single pair permit
functional comparison only. [Rust generated bindings](https://docs.rs/opentelemetry-proto/0.33.1/opentelemetry_proto/)
and [Go generated bindings](https://pkg.go.dev/go.opentelemetry.io/proto/otlp@v1.11.1/collector/logs/v1).

## Original synthetic baseline

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

## Conditions for a scored comparison

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
development but cannot replace the actual endurance test. M1 implements a
finite simulated history, persistent queues and query probes for logs. The
million-event synthetic harness also has a separate accounting ceiling.
The wall-clock REC-REF-24 endurance run is **unrun**.

No pass/fail or release-readiness claim can be made without the approved charter,
production-shaped implementations, end-to-end reconciliation, full recovery
suite and an architecture-reviewed language decision record.

## Reproducing the controlled log pairs

Run `./scripts/bench-reference.sh` on a disposable Linux host with cgroup v2,
systemd, passwordless sudo, four available logical CPUs, iproute2, util-linux,
e2fsprogs, and the pinned build tools. Each variant gets a new delegated scope,
private mount/network namespaces and two disposable ext4 loop devices. Only
namespace and resource setup uses root. The driver and services run as the
invoking non-root account. Missing controls or incorrect process membership
fail the run. The kernel can temporarily burst above an I/O rate limit.
[Linux cgroup v2](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html)

Use `--pair 1` through `--pair 5` to distribute whole pairs. Add `--quick`
for the short engineering check. Push CI runs a quick pair. Dispatch the
Foundation checks workflow with `full_reference=true` to run all five full
pairs. The runner prints its evidence directory. Run
`python3 benchmarks/ingest/otlp/verify_reference.py <run-directory>` on an
unpacked artifact to recompute the input plan, check the SHA-256 inventory,
verify applied quotas and charged disk writes, and compare every archived
settled/restored row with its independent field and multiplicity oracle.

The manifest records execution order, build, commit and hashes. Under each
pair and language, `controls.json` records kernel readbacks and
`result.json` records dispositions, timings, progress and resource usage.
The natural and paced gzip JSONL journals contain the exact plan, source HTTP
outcomes, monotonic nanoseconds, query windows and answers, raw Collector
series, receiver counter snapshots and cgroup samples. Missing series are
null. Query and source tail distributions use nearest rank for successful
observations. The source-ACK boundary is successful Collector enqueue.
Receiver ACK bytes count sanitized protobuf with a Loki 204. Visibility uses
authorized reads. The archive can establish visible multiplicity only.
[Controlled workload](workload-charter.md#controlled-16-worker-logs-profile)

The [validation record](validation-m1-reference.md) includes the full
measurements and a permanent archive of the JSONL streams in XZ format.
The verifier reads either encoding and reconstructs the reported phase rates
and latency percentiles from individual requests and visibility probes.
