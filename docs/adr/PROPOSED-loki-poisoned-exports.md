# Proposed ADR — custody of permanently failing Loki log exports

**Status** Proposed for owner review. The choice changes when a source can
release data and who must retain it. It applies to the Loki logs adapter.
The current benchmark still retries a downstream 400 and has no quarantine.

## Evidence and current operator signal

Loki 3.7.10 validates individual entries. In the trial a single export held a
72-hour-old record and an eligible record. Loki rejected the expired member,
wrote the eligible one and returned HTTP 400. Both receivers returned OTLP
503. Each retry of that batch added one to the tenant's
`store_http_400_attempts` and two to `store_unknown_records`. These count
attempts. Only a separate authorized query established which eligible member
was visible. Loki's age-discard counter corroborated the expired member in
this isolated fixture. The same 400 is also possible for a wholly rejected
export. [Pinned Loki distributor](https://github.com/grafana/loki/blob/v3.7.10/pkg/distributor/distributor.go)

With one consumer and unlimited retry time, both the original export and a
later valid export remained in the history queue across repeated attempts.
The other lane advanced. Operators can correlate the authenticated tenant
counts with persistent queue depth, lack of same-lane progress and Loki's
discard reason. A missing sent series is unobserved. A valid OTLP partial
success can increment it by the full request size. The stock exporter logs
the partial rejection and completes the request. [Collector OTLP handler](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.165.0/exporter/otlphttpexporter/otlp.go)

## Recommended decision

Give the source queue an atomic, durable transfer to a bounded, tenant-scoped
quarantine for permanent or persistently ambiguous **log** exports. Park the
entire mixed export with its uncertainty intact. A source-aware exporter
must own both the queue disposition and a stable identifier for this queue
entry across restarts. The existing OTLP endpoint and stock Collector lack
this handoff. Implementing it requires owner approval of the custody, capacity,
retention and replay contract.

Classify a failure as permanent only with an authenticated adapter disposition
and a known validation rule. A bare 400 carries insufficient information for
that classification. Repeated unclassified failures can reach the separately
approved ambiguous-attempt budget and enter quarantine with unknown status.

| Evidence at the failing boundary | Proposed disposition |
| --- | --- |
| Local validation definitively rejects a record before forwarding | Record a known rejection and its bounded reason. Return the exact rejected count on OTLP partial success after Loki acknowledges eligible members. An authenticated, authoritative age policy could do this for expired records before transmission. Preserve event timestamps. |
| Loki acknowledges the entire eligible export with 204 | Count store-acknowledged records. The source can release the export. Source enqueue and receiver admission are earlier accepted states. Query visibility requires a read. |
| Throttle, temporary unavailability, disconnect, lost receipt or timeout | Keep the entry pending. Retry with bounded concurrency, jittered backoff and applicable Retry-After. Record uncertainty about commits. At an approved retry budget, transfer to durable quarantine or retain and backpressure. |
| Downstream validation error, including an expired member in a mixed 400 | Mark the request as failed at the adapter and the forwarded members' commit outcomes unknown. Preserve any independent positive evidence of acceptance. Treat unclassified errors conservatively. Authentication/configuration faults need operator repair, backoff and a separate alert. |
| Quarantine transfer commits | Record **quarantined** as a custody disposition. The source releases the active queue entry and can advance later valid batches in that lane. Return a distinct internal disposition to the source. OTLP write success continues to require the approved Loki acknowledgement for eligible records. |
| Quarantine is full, inaccessible or fails to commit | Retain the source entry, apply backpressure and alert. A crash before transfer leaves the active entry recoverable. A crash after transfer resumes from the durable disposition. |

The transfer must atomically retain the recoverable contents or a durable
reference under the same ownership and record the release. An idempotent
transfer keyed by source entry prevents duplicate custody changes after a lost
receipt. Retransmission to Loki may still duplicate writes. A missing query
row cannot establish rejection. Re-drive only after an authorized operator
reconciles available evidence, repairs the cause, and accepts the documented
duplicate risk. Record lineage, attempt and policy version on re-drive.

Keep original source data within its authorized source boundary. Enforce
capture filtering before any platform-owned persistence. Protect quarantine
inspection, retention, deletion and replay by tenant and current authorization.
Use safe reason codes, counts and opaque references. Keep bodies, credentials
and downstream error text out of logs and metric labels. Alert on retained age,
repeated failure, occupancy, transfer failure and replay dispositions.
Expose pending and quarantined gaps in completeness accounting. Disposal
requires operator approval and a durable, attributable loss count and reason.

## Alternatives and approval evidence

| Option | Trade-off |
| --- | --- |
| Continue indefinite retry | Keeps source custody within queue capacity. One poison export blocks the lane and repeats eligible writes. This is the measured baseline. |
| Return permanent OTLP failure or partial success based only on Loki 400 | Frees the lane but can release unaccounted records or report a false rejection count. The response lacks a per-record commit receipt. |
| Split and resubmit ambiguous batches, or add consumers | Can advance some work. Splitting can duplicate already committed members. Additional consumers can all become blocked and can change ordering. Neither provides a durable disposition. |
| Durable quarantine at the source | Allows later progress while retaining uncertain records. Requires source integration, protected capacity, recovery testing and an operator workflow. |

Before adoption, test lost receipts and crashes on both sides of transfer,
full/unavailable quarantine, mixed and wholly expired batches, continued
same-lane progress, replay duplicates, authorization changes and operator
accounting. The architecture leaves the acknowledgement, overflow and
late-data policy per adapter to review. This proposal preserves REQ-025,
029, 035–036, ADR-I4/I5 and ADR-REC-01/02/03/05. It supplies partial
investigation evidence for TEST-030 and TEST-032–036.
[Architecture §§23 and 25](../architecture/AI_Native_Observability_Architecture_Spec_v0.5.1.md#23-recovery-architecture-and-benchmark-suite--rec-01rec-06)
