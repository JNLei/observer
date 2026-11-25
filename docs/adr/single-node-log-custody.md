# Single-node log custody before Loki acknowledgement

**Accepted for Headless Backend v0.2.** The owner approved Observe retaining a sanitized, tenant-bound SQLite copy after an unsuccessful or disconnected OTLP request and owning replay on restart. Producer release still requires OTLP success backed by Loki 204 for forwarded rows. This decision applies to a single Observe process, a private local disk and a directly connected, single-process Loki candidate.

## Decision and consequences

In reviewed v0.1 (`0ad17da`), a producer could abandon an unsubmitted export after a connection failure and Observe would have nothing to send later. Under v0.2, a committed copy can reach Loki independently after that 503. A producer retry is a new export with new instance IDs. A backend retry reuses the original IDs, canonical rows, attribution and timestamps. A lost reply can produce physical duplicates. Content equality cannot identify duplicate submissions.

| Boundary | Evidence and responsibility |
| --- | --- |
| Before local commit | The producer owns retry. Observe authenticates, validates, applies capture and time policy, and reserves tenant and global capacity. A refusal cannot evict earlier entries. |
| Local commit | A synchronous SQLite transaction records the sanitized batch and increments the tenant's durable total. Observe owns recovery on the same intact disk. A lost HTTP response can hide this commit from the producer. |
| Dispatch | The backend rechecks the current writer, canonical fields and time window, then commits a reservation identifying the connected Unix peer before sending. Neither a bearer nor submitted resource claims authorize replay. |
| Loki 204 | Observe commits removal of the payload and increments the acknowledged total together. Only then can a live request return success for those rows. A crash between Loki and SQLite leaves a possible prior write. |
| Lost reply or crash during dispatch | Preserve the batch, count the uncertain record attempts and retain the old remote-work charge. A retry can use a separately available reservation. Exhausted reservations reopen when the corresponding peer process has exited. |
| Query | A current tenant-authorized read at the original timestamp establishes positive visibility. Historical attempt counts survive a later 204. An empty query cannot settle an ambiguous write. |

A completed Loki 400 can follow a partial commit. Retry the whole eligible batch with its existing IDs. A local validation failure before enqueue gets an attributable rejection. A previously admitted batch that crosses the age window, loses its writer or fails a canonical check remains held and charged in the active store. This slice provides no disposal or quarantine operation. The producer-facing dispositions, retry schedule and physical limits are in the [operations guide](../implementation/headless-v0.2.md).

## Choice of persistence component

SQLite already ships with the Rust authority. Its rollback journal supplies transaction recovery for the payload, membership and counters. A private, separate database gives the spool a page ceiling independent of the credential database. Observe owns admission, scheduling and network outcomes. No application-defined journal format or new daemon is necessary. Disk and filesystem behavior bound what a successful sync can establish. [SQLite atomic commit](https://www.sqlite.org/atomiccommit.html) and [synchronous settings](https://sqlite.org/pragma.html#pragma_synchronous).

Collector Contrib 0.165.0 can delete a dispatched item's old key before reinserting it during recovery. Its completion callback cannot atomically settle Observe's tenant-bound entry. The existing review also evaluated bbolt, Kafka transactions, RabbitMQ quorum queues and Fluent Bit. bbolt would require a second storage binding or a separate process here. Broker-backed ownership adds another deployment and acknowledgement boundary. The pinned Collector, synthetic quarantine owner and distributed brokers are outside this decision. [Versioned component evaluation and crash window](PROPOSED-loki-poisoned-exports.md#existing-oss-capabilities).

## Decision scope

Confirmed requirements are REQ-025/029/035/036, ADR-I4/I5 and ADR-REC-01–06. Rust is preferred for this owned service. The queue limits and SQLite choice apply to this local logs slice. Final storage, public Evidence schemas, production retention/disposal, multi-node custody, release SLOs and the broader language gate remain deferred. Relevant evidence contributes to TEST-030, 032–036, 043 and 051. [Architecture delivery and recovery contract](../architecture/AI_Native_Observability_Architecture_Spec_v0.5.1.md#23-recovery-architecture-and-benchmark-suite--rec-01rec-06).
