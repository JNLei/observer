# Durable quarantine experiment results

Scenarios 1–7 passed for the synthetic logs queue owner. Both receivers exposed
a queryable member of a mixed Loki 400 before parking, and a queryable
same-lane follower after parking. The queue owner retained the original batch
and its uncertain delivery history. [Approved experimental ADR](../adr/PROPOSED-loki-poisoned-exports.md)

**Recommendation.** Choose another queue owner for any future integration
review. Require an interface that preserves a source-minted identity and
controls completion and restart recovery. The pinned Collector can retire a
terminal error, and its startup path deletes old dispatched entries before
reinserting them under new indices. A crash between those writes can lose the
only source copy. An exporter has no control over that window. Scenario 8
requires independent review and separate authorization. [Collector 0.165.0 completion and recovery](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.165.0/exporter/exporterhelper/internal/queue/persistent_queue.go#L360-L503)

## Source queue-owner evidence

The source generated an ID and independently encoded the expected OTLP
protobuf from the input fixture before enqueue. It persisted the ID, SHA-256,
count and length outside bbolt. After restart the audit compared this inventory
with both payload buckets, the per-entry receipt and tenant/global charges.
The checked-in run includes the before-retry and after-retry inventories for
each crash point. [Machine-readable local evidence](evidence/quarantine-f9570ec.json)

| Fault or decision | Observed result |
| --- | --- |
| SIGKILL before transfer, while staging the transfer, before commit, or before syncing dirty data pages | The reopened owner had the original active bytes and no quarantine charge. |
| SIGKILL after the metadata page write and before its sync, after commit, or before receipt delivery | The reopened owner had one parked copy and one charge. Repeating the command returned its original receipt. The metadata-page result applies to a process death with the host page cache intact. |
| Second SIGKILL during recovery | All seven cases reconciled two distinct source IDs carrying identical bytes. Stale send completions could not release an entry. A kill while recovering an in-flight replay preserved one unknown attempt and required an explicit new retry command. |
| Tenant reservation, last-slot race and storage faults | The eligible peer used its reserved capacity. One contender won the last entry or byte allocation. Staged rollback preserved active custody. Injected write/data-sync errors recovered active state. A reported metadata-sync error recovered parked state. Uncertain results fenced dispatch until reconciliation. |
| Actual bbolt file growth refusal | With a 512 KiB file ceiling, 11 admitted entries survived reopening. The refused submission received no admission identity. The separate logical active-store test also refused new work at its cap. |
| Authorization and replay | Cross-tenant and conflicting commands failed. Metadata readers could not retrieve or export payloads. The owner rechecked revoked grants before dispatch. Concurrent workers shared one replay intent. A new command had to name the prior uncertain replay to authorize another send. |
| Review, disposal and restore | A due review retained custody. A hold blocked disposal. Authorized disposal revoked a pending replay and retained its audit and prior uncertainty. An older backup stayed fenced with missing, corrupt or stale evidence. A second kill during restore left it fenced. Reconciliation applied the tombstone and surviving hold. Missing bytes for a live entry kept the entire restore fenced. |

The instrumented test binary uses a temporary copy of bbolt 1.5.0. It injects
SIGKILL and reported EIO at the library's published dirty-page, metadata-write
and metadata-sync fault locations. The ordinary test build also runs against
the unmodified dependency. The physical-capacity case reaches bbolt's actual
file growth limit. [Test driver](../../benchmarks/ingest/otlp/quarantine.py) · [Pinned bbolt commit path](https://github.com/etcd-io/bbolt/blob/v1.5.0/tx.go#L517-L620)

## Rust and Go downstream evidence

Each column uses a fresh single-node Loki 3.7.10, a fresh synthetic source and
the existing receiver binary. Authenticated event-time queries verified line,
service, severity, trace ID, span ID and observed time. A receipt records batch
custody. The receiver counters describe individual send attempts.

| Observation | Rust receiver | Go receiver |
| --- | ---: | ---: |
| Offline Loki attempts before repair / recovered query rows | 4 / 1 | 4 / 1 |
| Injected retry responses / known credential refusal | 429, 503 / 401 | 429, 503 / 401 |
| All-expired batch, Loki 400s / unknown record attempts / query rows | 1 / 2 / 0 | 1 / 2 / 0 |
| Mixed batch, receiver 503s / Loki 400s / unknown record attempts | 2 / 2 / 4 | 2 / 2 / 4 |
| Mixed eligible member / follower visible before parking | 1 / 0 | 1 / 0 |
| Mixed eligible member / follower visible after parking and sending the follower | 1 / 1 | 1 / 1 |
| Store acknowledgements from parking / other tenant's query rows | 0 / 0 | 0 / 0 |
| Lost-reply fixture, receiver store ACKs / source unknown attempts / query rows | 3 / 2 / 1 | 3 / 2 / 1 |

The mixed receipt retained both members and four unknown record attempts.
The all-expired read returned no rows. The lost-reply cut occurred after real
write success. Multiplicity in a Loki query cannot establish the number of
physical writes.

## Existing recovery and CI

The original Collector Contrib 0.165.0 recovery driver ran unchanged. Each
case compared the complete 96-history/24-live multiset before and after a Loki
SIGKILL. The controlled witnesses below give historical/live counts at two
successive partial reads. The local recovery JSON has SHA-256
`9c92f475b2b0d18840e807aaeb55653a3329b50bdb59e39d40dcdbbb8e21bae7`.

| Existing regression | Rust receiver | Go receiver |
| --- | --- | --- |
| Unpaced rows before / after restart | 120 / 120 | 120 / 120 |
| Controlled rows before / after restart | 120 / 120 | 120 / 120 |
| Controlled overlap witness | 4/1 → 8/2 | 4/1 → 8/2 |
| Stock Collector mixed-batch follower visible while retrying | 0 | 0 |

All four reads had the same result digest, `df0b24fca91765522a17cc814352d3245faca773b79775c9e56b9021d84c04f5`. All three
live negative controls detected the injected duplicate or missing overlap.
Formatting, Clippy, Rust tests/builds, Go vet/tests/builds, 26 Python checks,
the race-instrumented runs and both smoke variants passed. The quarantine
driver also used the Go race detector for its process fault matrix and both
real-Loki cases. [Implementation commit](https://github.com/JNLei/observer/commit/f9570eccc7bf582e239b22f0050c5228fb804a0c) · [Hosted implementation checks and raw artifacts](https://github.com/JNLei/observer/actions/runs/45114964755)

The first hosted run stopped at the existing streamed-size conformance test.
Its HTTP client saw a broken pipe while flushing the final chunk after Rust
refused the oversized upload. The test now flushes one complete chunked frame
sequence and reads the receiver's HTTP response directly. The 413 assertion
and separate declared-size and gzip cases remain. The corrected case passed
30 race-instrumented repetitions against each receiver locally. [Initial hosted failure](https://github.com/JNLei/observer/actions/runs/45114641887) · [Test-only correction](https://github.com/JNLei/observer/commit/f9570eccc7bf582e239b22f0050c5228fb804a0c)

From the repository root, run the following with the pinned Rust and Go
toolchains. The scripts write independent JSON evidence under
`benchmarks/ingest/results/`. The workflow retains the full recovery and
quarantine files for each tested SHA for 30 days.

```sh
./scripts/check.sh
./scripts/check-race.sh
./scripts/bench-smoke.sh
./scripts/bench-recovery.sh
./scripts/bench-quarantine.sh
```

## Policy and limits of the result

The fixture admits up to 256 generated sequence/time pairs per entry and
derives the trace and span IDs. It discards submitted body, tenant and service
claims before persistence. The 96-hour content window, 48-hour replay age and
one-hour review interval are experimental values. Access expires even when a
hold requires keeping the bytes. Disposal is logical. Free bbolt pages,
historical backups and previously accepted downstream copies have separate
physical erasure requirements.

By default, the active and parked pools each allow 12 entries and
256 KiB globally, 10 entries and 224 KiB per tenant, and a reservation of
two entries and 32 KiB for each tenant. The file cap is 16 MiB. Tests tighten
individual limits. Quotas charge admitted payload bytes. The file cap also
covers bbolt pages and metadata. The source retains at most 256 entry
identities and 1,024 commands per generation. Synthetic data directories are
private and removed at trial teardown.

This was a functional run on macOS ARM64, 10 logical CPUs and 16 GiB of
shared RAM. The native trial uses two Go runtime processors and one active
FIFO lane per tenant. Source retry time advances under a fixture clock. Production throughput, host power loss, device exhaustion,
node loss, replication and secure erasure remain untested.
The external disposition authority is modeled by a private,
independently retained current digest, metadata catalogue and exclusive file
lock. Rolling that authority back with the database is outside the experiment.
A production authority, distributed fencing, identity, erasure and resource
budgets remain open.

The experiment contributes logs-only evidence to REQ-008/016/025/029/035/036,
ADR-I4/I5, ADR-REC-01/02/03/05/06 and TEST-030/032–034/036–037. The fixture
covers selected S01/TEST-042 and S02/TEST-043 confidentiality and attribution
cases. Full Milestone 1,
TEST-038 and production integration remain open. The revised experimental ADR
is the only decision record changed.
