# Proposed ADR — custody of permanently failing Loki log exports

**Approved in principle.** The architecture owner approved durable, tenant-scoped
quarantine as a transfer of source custody. A poisoned entry may leave active
replay only after a crash-safe, idempotent handoff. A full quarantine retains
source custody and applies backpressure. Ordinary outages follow durable retry.
Downstream acknowledgement, source acceptance and query visibility each require
their own evidence. A bare Loki 400 leaves individual commit outcomes unknown.
Tenant authorization and provenance apply through inspection, replay and disposal.

**For architecture review.** The contracts and acceptance tests below are
proposed. The recommended first experiment uses one source-owned database
transaction on an intact local disk. Production technology, capacity and
retention values, a wire protocol and implementation authorization remain open.
The current single-consumer lane can still stall. Full M1 and TEST-038 remain open.

## Evidence and current operator signal

At commit `421071c`, Loki 3.7.10 received an export containing a 72-hour-old
record and an eligible record. Each receiver observed three Loki 400s and
returned OTLP 503. The tenant counters recorded three failed batch attempts
and six unknown record attempts. An independent query found the eligible
member. Loki's age-discard counter corroborated rejection of the expired
member in this fixture. Two history requests stayed queued while the separate
live lane advanced. Loki can also return 400 for a wholly rejected export.
[Hosted regression](https://github.com/JNLei/observer/actions/runs/45108587649)
and [pinned Loki distributor](https://github.com/grafana/loki/blob/v3.7.10/pkg/distributor/distributor.go#L780-L930).

Operators can correlate the stalled lane with the tenant's 400-attempt
counter. Neither that aggregate nor the generic upstream 503 identifies an
entry to park. The Collector sent counter can also include locally rejected
members of an OTLP partial success. [Collector OTLP handler](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.165.0/exporter/otlphttpexporter/otlp.go#L416-L444)

## Custody and acknowledgements

| Observation | What the proposed contract permits |
| --- | --- |
| Source accepts an export | Acknowledge source enqueue after the entry and its identity are durable within the declared failure scope. The Collector's source-facing response describes this boundary. |
| Receiver admits or rejects a record | Record admission and attributable validation failures separately. For an OTLP partial success, count only definitively rejected members and require the approved Loki acknowledgement for the eligible subset. |
| Loki returns 204 for the eligible export | Record downstream acknowledgement. The receiver may return OTLP write success. A lost reply can still leave the source uncertain. |
| Quarantine owner commits a transfer | The source records a private custody receipt bound to the verified tenant, source entry, digest and disposition revision. Release active dispatch for that entry. This operation produces no Loki 204, OTLP write success or OTLP partial success. |
| Authorized event-time query returns a member | Record positive visibility and observation time for that member. An empty or partial read leaves unobserved members unresolved. A parked batch can contain queryable records. |

The source mints an opaque entry ID at durable enqueue. Scope it by verified
tenant and source queue generation. Preserve it through retry, restart and
recovery of an in-flight entry. Two legitimate submissions of identical bytes
receive distinct IDs. A protected content digest detects conflicting reuse.
Keep the verified principal or admission channel separate from submitted
service and trace claims. Persist the authorized destination, event and
observed timestamps, source acceptance time, record count, policy version
and bounded attempt evidence. Keep credentials outside the entry.
The benchmark's fixed `m1-bench` producer still leaves S02 / TEST-043 open.

The queue owner serializes transfer against dispatch, replay and deletion. In
one durable transaction it checks current ownership and capacity, keeps the
recoverable payload or pins its immutable reference, records the receipt,
transfer time, reason, unknown members and quota charge, and removes active
membership. Keep Loki network calls outside this transaction.
Before commit the source can recover the entry. After commit one logical
quarantine entry owns it and the next valid log batch may progress. Physical
reclamation follows commit. On timeout or restart, look up the entry before
sending to Loki again. The same key and digest return the original receipt
even when capacity is now full. A conflicting digest, stale owner or unverifiable
receipt stops release and raises an alert. A remote write already in flight
can still complete. Custody idempotence applies only to the transfer.

## Failure classification

| Evidence | Action on already accepted work |
| --- | --- |
| Throttling, ordinary outage, network failure, timeout or lost acknowledgement | Keep durable source custody. Retry with bounded concurrency, jittered backoff and applicable Retry-After. Track uncertainty for attempts that could have reached Loki. Duration and retry count alone never cause parking, disposal or a permanent classification. |
| Authentication, authorization or configuration fault | Hold the affected work and alert. Resume under current authorization after repair. A broad dependency fault must not fill quarantine with healthy records. |
| Definitive local validation before forwarding | Record the rule, version and affected members. An expired record can be known rejected for this attempt when an authoritative tenant policy establishes that result. Preserve original timestamps and any uncertainty from earlier attempts. Use the custody handoff for already accepted records designated for preservation. |
| Bare Loki 400, ambiguous mixed batch or unclassified response | Keep the entire forwarded unit active under durable retry, with its unknown commit outcomes. Aggregate discard counters, error strings and missing query rows cannot settle individual outcomes. Parking needs the evidence or authorization below. |
| Entry-specific permanent failure, or an operator elects to park an unresolved entry | Require a trusted, versioned adapter disposition bound to the entry and failed rule, or an authorized operator decision bound to its ID and digest. Park the whole eligible unit with its evidence. An operator can change custody without asserting rejection. The first experiment uses this manual path. |

## Capacity, retention and replay

- **Capacity.** Enforce per-tenant and global limits on bytes and entries.
  Reserve the charge in the transfer transaction. Account for pinned data
  and leave physical headroom for the journal and recovery. On full quota or
  a confirmed storage abort, retain the active entry and apply backpressure.
  On a lost commit result, failed sync or inaccessible store, hold dispatch
  and reconcile against the durable receipt before any release or retry.
  If the active queue also fills, refuse new submissions before acknowledging
  them. Alert on the affected scope and preserve other tenants' reserved capacity.
- **Retention.** Start the quarantine review clock at durable transfer.
  Keep original event time for Loki eligibility. A review deadline raises
  an alert. An authorized disposal or binding deletion policy records actor
  or policy, reason, counts and prior uncertainty, revokes replay and removes
  payloads and references under the applicable retention and hold policy.
  A full store never evicts an entry to admit its successor. Keep the minimal
  permitted receipt or tombstone through the source retry and restore horizon.
  Fence older queue generations before retiring receipts. A snapshot restore
  starts with dispatch disabled until reconciled against surviving deletion
  and disposition evidence. If that evidence is unavailable, keep it fenced.
  Disposal ends custody of the source copy. A previously ambiguous Loki
  write keeps its recorded uncertainty. Tenant limits, deadlines and audit
  retention need owner-selected values.
- **Replay.** Authorize inspect, park, replay and dispose separately. Keep
  original source data inside its authorized boundary and apply capture policy
  before platform persistence. An operator records the repaired cause, chosen
  scope and possible duplicate writes. Persist one replay intent per command
  ID with parent entry, actor and policy version. Concurrent requests or a
  lost reply reuse that intent. Recheck current authorization, deletion,
  redaction, destination and age before dispatch. Keep the parked record
  recoverable through the attempt. A timeout or mixed error leaves the entry
  parked with the replay outcome unknown. A 204 resolves that replay as
  store-acknowledged. Payload disposal follows retention policy, and any
  later query evidence is recorded separately.
- **Operator evidence.** Expose tenant-authorized active and parked counts,
  bytes, oldest age, failed transfers, replay outcomes and explicit disposal.
  Reconcile custody by stable entry and report downstream attempts and
  visibility separately. Keep payloads, tokens, raw Loki error text and
  per-entry identifiers out of metric labels and ordinary logs.

## Existing OSS capabilities

The following findings come from published interfaces and versioned source
inspection. The local benchmark uses Collector Contrib 0.165.0 with fsync,
one consumer per lane and unlimited retry time. [Trial configuration](../../benchmarks/ingest/otlp/collector.yaml)

| Candidate | Where the guarantee ends |
| --- | --- |
| Collector persistent queue and OTLP/HTTP exporter, 0.165.0 | The internal completion callback preserves shutdown work and retires other completed sends, including terminal errors. Recovery can re-enqueue a dispatched item under a new index. The OTLP exporter handles downstream status and partial success separately from queue deletion. A finite retry limit or forwarding a permanent 400 can therefore end source custody without a parking receipt. Auth-extension context also does not survive the persistent queue. [Queue completion and recovery](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.165.0/exporter/exporterhelper/internal/queue/persistent_queue.go#L393-L490) · [Retry sender](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.165.0/exporter/exporterhelper/internal/retry_sender.go#L94-L126) · [Context propagation](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.165.0/exporter/exporterhelper/README.md#persistent-queue) |
| Collector failover connector and a file exporter, 0.165.0 | Failover selects a healthy pipeline level, optionally by matching error text. A later valid entry may follow the same alternate route. File export writes a separate output. Neither configuration binds a particular source entry's release to an idempotent archive receipt. [Failover routing](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.165.0/connector/failoverconnector/README.md#configuration) · [File output](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.165.0/exporter/fileexporter/README.md#configuration-options) |
| Collector file storage and bbolt | One client's batch runs in one bbolt write transaction. File storage can cap database-file growth. One owner could use that transaction for active and parked membership, tenant quota and receipt. The stock callback owns its private queue cleanup. A second exporter using the same directory cannot join its transaction. [Storage transaction](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.165.0/extension/storage/filestorage/client.go#L117-L131) · [File size limit](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.165.0/extension/storage/filestorage/README.md#L30-L37) · [Component storage files](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.165.0/extension/storage/filestorage/README.md#L129-L143) · [bbolt transactions](https://github.com/etcd-io/bbolt/blob/v1.5.0/README.md#transactions) |
| Fluent Bit filesystem dead-letter queue, 5.2.0 | The output-error handler invokes a chunk copy and discards its return value. A full DLQ can skip that copy. This fails the required source-custody rule on exhaustion. [Engine error path](https://github.com/fluent/fluent-bit/blob/v5.2.0/src/flb_engine.c#L520-L565) · [DLQ capacity check](https://github.com/fluent/fluent-bit/blob/v5.2.0/src/flb_storage.c#L895-L908) |
| Apache Kafka transaction, 4.2 API | When Kafka owns the input, a transactional producer can publish the parked envelope and enlist the group's next contiguous offset with `sendOffsetsToTransaction`. Disable independent commits, fence stale owners and use `read_committed`. Topic retention can delete unread segments, so source preservation and hard tenant capacity need separate enforcement. This alternative introduces a broker and a transactional consumer. [Producer transaction contract](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html) · [Consumer isolation](https://kafka.apache.org/42/configuration/consumer-configs/#isolation.level) · [Topic retention](https://kafka.apache.org/42/configuration/topic-configs/#retention.bytes) |
| RabbitMQ 4.2 quorum queues | Opt-in at-least-once dead lettering with `reject-publish` retains the source message until the target confirms. A full target can delay that confirmation. Target retries can produce copies, so a source using this broker would still need stable entry IDs, an idempotent logical receipt and tenant-bound classification. [Quorum queue dead lettering](https://www.rabbitmq.com/docs/4.2/quorum-queues#dead-lettering) |

## Narrow implementation recommendation

**Review a source-local proof first.** If authorized, give a synthetic source
one private database with synchronous bbolt commits. Use one `DB.Update` for
the conditional transfer. Drive the existing receivers and real Loki with one
history consumer, an operator-selected mixed batch and a valid follower. Use
a second tenant for isolation. Exercise durable enqueue, transfer, restart
and receipt lookup. This isolates the queue-owner work without introducing
a new classifier, broker or custom Collector exporter.

Promotion to the real trial requires a supported queue-owner integration that
persists the ID at enqueue, restores it, and controls the release transaction.
The reviewed stock Collector configuration exposes no such hook. Keep private
Collector queue files outside the proof. If a supported upstream integration
cannot provide these boundaries, return for a choice between a narrowly
scoped upstream queue change and a broker-owned source trial. An exporter
alone cannot make a separate queue deletion atomic.

## Acceptance tests proposed for review

The bbolt 1.5.0 library checks `TestDB_Update`, `TestTx_Rollback` and
`TestDB_MaxSizeNotExceeded` passed locally with Go 1.27.4. They exercise
database commit, rollback and refused file growth. The eight quarantine
scenarios below are proposed and unrun. Use deterministic fault points,
a durable custody ledger and independent reads. The baseline regression
continues to expect a blocked history lane until the integrated gate passes.

| Test | Required observation |
| --- | --- |
| Crash and lost receipt | Kill before transfer, during commit, after commit before receipt delivery, and again during recovery. On intact-disk restart each accepted ID has exactly one logical custodian. Repeating a committed handoff returns the same receipt and consumes no extra quota. A corrupt or unreadable receipt holds dispatch. |
| Identity and contention | Race two owners and repeat a command after restart. One transition wins. Identical payloads with different entry IDs remain separate. A receipt for another tenant or entry, a conflicting digest or a stale lease cannot release the entry. |
| Ordinary outage and configuration fault | Hold Loki offline beyond the diagnostic retry threshold, inject 429/503 and lost replies, and restart the source. No automatic parking or disposal occurs. Restore service and recover eligible records at their original event times. A credential fault holds work until authorization is restored. |
| Mixed and wholly expired exports | Use real Loki with an expired and an eligible member and a separate all-expired batch. A bare 400 establishes no per-record rejection. Keep positive visibility where observed. On authorized parking, the whole mixed unit gets one custody receipt, zero store-success credit, and its later valid same-lane follower becomes queryable. |
| Capacity and storage failure | Fill a one-entry tenant quota, then the global byte limit. Race the last slot and inject write/sync failure. A refused transfer keeps its active payload and applies backpressure. An indeterminate commit blocks until reconciled. New demand receives no false acceptance. A tenant with reserved room can progress until the shared limit binds. Free capacity and retry without a second charge. |
| Replay, isolation and confidentiality | Try cross-tenant lookup and replay, forged tenant/service claims, a revoked operator, duplicate replay commands and a lost Loki ACK. No protected data escapes. At most one replay intent exists per command. Recheck policy at dispatch, preserve timestamps and lineage, and measure possible duplicates and unknown attempts independently. |
| Retention and disposal | Expire a review deadline, apply a hold, then exercise authorized deletion and restore an older snapshot. Verify review/hold behavior, disposal audit and absence of payload access or replay after deletion. Disposed unknown writes retain their historical uncertainty. |
| Integrated exit gate | On both Rust and Go paths, separate enqueue acknowledgements, custody receipts, local rejects, store 204s and query results. Repeat the 120-record unpaced and controlled overlap runs, exact multiset reads before/after restart, and the same-lane poison/follower case. Queue depth or Collector sent counts alone cannot pass this gate. |

Traceability covers REQ-008/016/025/029/035/036, ADR-I4/I5 and
ADR-REC-01/02/03/05/06. The proposed gates contribute to TEST-030,
032–034, 036–037 and the applicable S01/S02 security cases. The trial
addresses process restart on intact disk. Node loss, replication, a
production identity system and release thresholds remain separate decisions.
[Architecture recovery and delivery contracts](../architecture/AI_Native_Observability_Architecture_Spec_v0.5.1.md#23-recovery-architecture-and-benchmark-suite--rec-01rec-06)
