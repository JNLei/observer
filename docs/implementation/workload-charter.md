# M1 Rust/Go workload charter, revision 1 with review corrections

This charter precedes the first OTLP trial. The owner approved an OpenTelemetry
Collector persistent source sending queue and a single-node Grafana Loki trial.
An eligible export succeeds at the receiver only after Loki acknowledges the
write. A separate authorized time-range query establishes visibility. These
are benchmark dependencies. Product store, replication, language selection,
capture defaults and numerical acceptance criteria remain open.

## First executable profile

Use generated OTLP/HTTP binary protobuf log messages, identity or gzip, on
`POST /v1/logs`. Use a real Collector and the same Loki build, Unix HTTP socket,
schema, single ingester, enabled WAL and filesystem data directory for both
variants. Start each run with empty private data directories on a trusted,
single-user host. Bind development TCP endpoints to numeric loopback. Issue
fresh credentials for each run. Each credential selects a tenant. Both
receivers assign every export the single synthetic producer `m1-bench`.
Submitted service names and resource claims are stripped. General verified
producer attribution remains open under S02 / TEST-043.

| Dimension | Provisional functional workload |
| --- | --- |
| Record shape | One deterministic integer `bench.seq` attribute per record. Cycle string body sizes of 32, 256 and 1,024 bytes by sequence number. Preserve event and observed timestamps, severity and valid trace/span IDs. Produce the stored line from the sequence number. Assign the fixed fixture service and discard free text and other attributes before forwarding. |
| Batches | Exercise empty, single, mixed valid/invalid and multirecord requests. Cap wire and inflated payloads at 65,536 bytes each and batches at 256 records. No per-record test ID becomes a metric label. |
| Admission | Maximum 12 active HTTP connections, 3 admitted writes per tenant including 1 active downstream write, and 1 independent read per tenant. Use two principals. Close client connections after each response. Bound body-read and store-call time. Return explicit overloads. |
| Historical load | Model 24 hourly batches of 4 records, with original timestamps covering the previous 24 hours. Bring the receiver offline before submitting. Confirm queue acceptance, kill and restart the source Collector on the same disk, then reconnect. |
| Live load | A separate persistent source queue and exporter consumer feed the same tenant/stream during replay. Offer 24 one-record exports at 10/s, from scheduled wall-clock offsets using a monotonic timer. Probe visibility while both classes are eligible. |
| Queries | Fixed service selector, authenticated tenant, bounded event-time range and result limit. Compare expected (event time, line, correlation) and multiplicity against the independent read, including a second tenant. Run reads during recovery and after a Loki process restart. |
| Fault scope | Local process kill/restart on intact disks. The single-node trial excludes node loss, power loss, replication, full disk and retry expiry. An independent deterministic sink supplies slow, failed and ambiguous downstream responses for conformance. |

Use a finite Collector file storage queue per lane, one consumer per lane,
fsync enabled, retry with backoff and jitter, and no retry expiry for this
small restart trial. Separate queue identities survive source restart. Queue
overflow can reject new submissions. Source admission can precede store
acknowledgement. Terminal rejection/partial acceptance is an explicit
disposition. A timeout or unexpected downstream response can hide a write.
Retry can create physical duplicates. No write or queue-empty counter proves
query visibility. Loki documents a full-disk exception to WAL durability.
[Collector queue](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.165.0/exporter/exporterhelper/README.md)
and [Loki WAL](https://grafana.com/docs/loki/latest/operations/storage/wal/).

### Controls added for independent review

Keep the original 96 historical and 24 live records and the unpaced run.
Compare the full multiset built from the generated inputs with authorized
reads after drain and again after Loki restart. Excess copies and unexpected
fields fail even an intermediate probe. During the outage both queue gauges
must report their expected request counts, including the empty live lane.
The first source process must report 96 history records accepted. Counters
restart with the process. Each absent series is recorded as null.

Repeat the 120-record trial on fresh disks for the overlap assertion. All
history is queued before reconnect. A loopback relay delays each history
exchange by 150 ms, independently of the live lane and the read path. Start
the live schedule after an independent query sees the first historical prefix.
Require two time-ordered successful probes, each showing both classes partially
visible, with both counts increasing between probes. Record the two samples.
These controlled timings serve only the overlap test.

After the unpaced restart checks, use a disjoint time window for the rejection
regression. Send one locally mixed-validity export, then a mixed-age export
with one record 72 hours old, a later same-lane export and a live control.
Observe at least two retries after enqueuing the follower. Compare source
metrics, receiver dispositions, Loki age discards and authenticated reads.
The test ends with two synthetic history requests pending. The owner approved
quarantine in principle. A separate approved synthetic experiment exercises the
source-custody contract. The stock Collector keeps this blocked-lane baseline. [Loki custody ADR](../adr/PROPOSED-loki-poisoned-exports.md)

## Subsequent representative profiles

The following values are **proposed fixture parameters**, pending owner input
on anonymized production distributions, SLOs and cost. Keep the original
[synthetic forwarder smoke](benchmark.md) as its own historical baseline.

| Dimension | Reproducible proposal for a scored charter |
| --- | --- |
| Signal corpus | Run logs, metrics and traces separately, then interleave one batch per signal in rotation. Use binary OTLP, both identity and gzip. Cycle batches of 1, 16 and 64 records, payload padding of 32 B, 256 B, 1 KiB and 8 KiB, and record measured serialized/compressed size histograms. Include conventional spans and depth-8/width-16 agent trees with stable parents and links. Keep metric fixtures as gauge, counter delta/cumulative and histogram streams with golden writer/start/reset timelines. |
| Arrivals | Open-loop fixed schedule per signal for 10 minutes at 100 records/s, 60 seconds at 4×, then return to steady load. Record intended arrivals even when the generator is saturated. Precompute identities with seed 1. Also inject a 3-second stall. Calibrate against a reference sink before scoring. |
| Outage | Short disconnect, prolonged disconnect, independent source/receiver/store restart, receipt loss, full spool and retry expiry. Save produced IDs, class, tenant, event times, expected values and checksums. Exercise duplicate, conflicting, unordered and late inputs. Preserve timestamps and label unsupported dispositions. |
| REC-REF-24 | After the simulated fixture, run a full 24 **wall-clock** hours disconnected with continuing production at the declared live rate. Reconnect with live, queries, alerts, compaction and projections enabled. Define 16 as application processing workers for the reference run and record actual OS/runtime threads and CPU quota separately. Report a second resource-scaled profile. The owner's approximate one-hour catch-up is contextual. |
| Scheduling and isolation | Compare fixed bounded per-tenant/per-lane admission against a bounded adaptive controller. Record fresh and backlog progress by tenant and class, including a noisy tenant. Preserve per-writer metric ordering across retries and resets. Use fixed event-time query windows with narrow and broad selectivity. Exercise read pressure and protected alert/ingest capacity. |
| Resource envelope | Proposed isolated Linux profile per receiver: 4 vCPU, 1 GiB RAM. Give source and store separate explicit cgroups, each initially 2 vCPU/2 GiB, 20 GiB disk, 50 MiB/s disk and 100 Mbit/s network. Scale CPU/memory together for a second profile. Preflight spool capacity against the corpus and report any overflow. Freeze machine, filesystem, kernel, quotas, topology, sidecar apportionment and prices before scoring. Native functional runs have no enforced host resource quotas. |

## Accounting and comparison procedure

Reconcile scheduled against sent, unsent and pending source demand. Count
transport attempts, source enqueue successes, accepted records, durable boundary
observations, store acknowledgements, queryable unique IDs, visible duplicate
rows, conflicting values, policy rejects, known drops and unknown outcomes
separately. A Loki query can hide identical physical duplicates. Record that
limit. Confirm original event-time ranges and values, not just row counts.
Keep optional projection/evaluation processed counts and lag separate.

For each phase, tenant and lane report offered/admitted/durable/queryable
records and bytes per second, P50/P95/P99 from intended arrival and attempt
start, generator wait, successful query latency, and lost/censored observations.
Time reconnect, first fresh visibility, full eligible backlog visibility and
post-recovery settling separately with monotonic clocks. Polling yields upper
bounds. Report CPU time, process RSS/peak, allocations and Go GC, storage/network
I/O, backlog bytes/oldest age, storage amplification and cost. For Linux use
cgroup current/peak, anon/file/kernel memory, pressure, writeback and OOMs.

Freeze the corpus, versions, release flags, auth/redaction, durability, query
mix, source retry settings and quotas for both languages. Record build hashes,
seed, revision, dirty tree, OS, CPU, memory, GOMAXPROCS/GC and Rust worker limits.
Propose five trials per variant/profile with seeded randomized paired order,
separate cold/warm data/cache starts and equal tuning time. Publish raw results,
failures, unknowns and variability along with implementation and maintenance
cost. Run security, integrity and resource gates before scoring. Release
thresholds, tolerable loss/duplicates, fairness, maximum lag and cost require
owner approval. Full TEST-038 also requires a reviewed decision record.

Traceability includes REQ-008, 025–029, 035–039, ADR-I1/I2/I4/I5,
ADR-REC-01–06, ADR-LANG-01, ADR-BENCH-01, TEST-029–030, 032–038, 043–045,
051, 054–055 and architecture §§23–26.

## Controlled 16-worker logs profile

This profile fixes experimental inputs for five paired observations. Numerical
release SLOs, cost ceilings and language selection remain deferred. Each pair
uses one Linux machine, one receiver at a time, and new processes, credentials
and empty filesystems for each variant. Seed 1 shuffles a counterbalanced
starting order. A matrix can place different pairs on different machines.
Comparisons therefore use the within-pair observations. Host caches and VM
scheduling remain shared. The short engineering check uses the same topology.

| Component | Enforced budget for each variant |
| --- | --- |
| Receiver | 4 CPU quota, 1 GiB memory, zero swap. Up to 16 application workers globally from authenticated write admission through downstream disposition, including client disconnect. Both tenants share the ceiling. Each allows up to 16 concurrent writes and one independent read. 32 connections, 4 concurrent decodes, 64 KiB wire/inflated and 256 records per request. Tokio has 4 runtime and up to 4 blocking threads. Go uses GOMAXPROCS=4 and GOGC=100. |
| Stock Collector Contrib 0.165.0 | 2 CPU, 2 GiB, zero swap. Separate 1 GiB ext4 image, 50 MiB/s reads and 50 MiB/s writes on its loop block device. Source-owned fsync queues retain 24 history requests with one consumer and 64 live requests with 16 consumers. Retries use the existing backoff configuration. |
| Single-node Loki 3.7.10 | 2 CPU, 2 GiB, zero swap. Its own 1 GiB ext4 image and the same block limits. The existing schema, WAL, store ACK, Unix socket, fixed query selector, two query slots and 512-row read bound apply. Both Go dependencies use GOMAXPROCS=2, GOGC=100. |
| Driver, probes and sampler | 1 CPU, 1 GiB, zero swap. Eight source senders and at most 32 further submitted exchanges. All TCP traffic shares a private 100 Mbit/s loopback qdisc. The local Unix store transport bypasses that qdisc. CPU quotas are ceilings on the shared machine. The OS/runtime thread count is sampled separately. |

Use monotonic open-loop arrivals. Generate the established synthetic body,
severity, identity and trace/span fields. The source accepts OTLP JSON and
sends gzip OTLP protobuf. A healthy run offers four-record batches at 100
records/s for 600 seconds, 400/s for 60 seconds, then 100/s for 10 seconds.
This gives 85,000 records in 21,250 requests. The quick check uses 5, 2 and 2
seconds respectively. The schedule continues under pressure. Record unsent
demand, source refusals, timeouts, ambiguous attempts and outstanding work.
Each case anchors event times near its own wall clock so live events remain
fresh across sequential trials. Archived anchors and offsets reproduce each
exact corpus. IDs, sizes, cadence and relative ages match across variants.

After healthy traffic drains, take the receiver offline for at least three
seconds. Offer 24 four-record historical exports, one per simulated hour of
the preceding day, at 10 requests/s. With all 24 queued, offer eight disjoint
single-record overflow probes and correlate their explicit refusals with the
Collector enqueue-failure counter. Start the receiver and send 24 fresh
single-record exports at 10/s. Observe natural catch-up with the ordinary
transport. Repeat just the outage/replay on fresh data directories with the
existing 150 ms history-only relay. Start fresh offers after the first history
prefix and require two partial reads showing both classes advance. Report
natural and paced timings separately.

Sample resource and queue counters every 0.5 seconds plus scrape time.
Probe the active load window every 0.2 seconds plus query time and record
individual first observations. Record the natural and paced progress curves,
first fresh, history completion, longest observed gap for both classes, and
queue/counter drain. Visibility times are upper bounds. Run a separate full
multiset scan before and after a Loki SIGKILL on intact storage. Shard by
event time with at most 256 expected records per read. A capped result fails
verification. Check the other credential across the entire event window.
Retain every returned settled/restored row, offered identity, disposition,
timing, failed query, resource sample, build hash and limit readback.

Integrity checks require one visible copy of each source-confirmed record,
the original fields, no cross-tenant rows, and agreement with the explicit
pressure dispositions. Report generator or queue saturation as measured
outcomes and flag any phase with incomplete demand. This fixture uses
24 event-time hours. Wall-clock REC-REF-24, adaptive admission, cold/warm cache
experiments and scored language acceptance remain open. The stock Collector
restart concern and the synthetic custody review retain their separate gates.
