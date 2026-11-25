# Headless Backend v0.3

Observe accepts distributed spans over OTLP/HTTP and exposes tenant-authorized trace and log evidence. The local profile runs the Rust service, VictoriaTraces 0.13.0 and Loki 3.7.10. Clients need only Observe HTTP and their own credential. A trace-store outage leaves the log spool and Loki path available.

## Reproduce the two-tenant workflow

Run from the repository root on Linux or macOS with the [repository toolchain](../../README.md#local-requirements).

```sh
cargo build --locked -p observe-backend
python3 benchmarks/ingest/otlp/fetch_tools.py --loki-only
python3 benchmarks/ingest/otlp/fetch_tools.py --traces-only
python3 backend/headless/tests/test_traces.py -v RealStores.test_two_tenant_distributed_trace_and_evidence
```

This test provisions a private temporary installation. Payments sends a JSON child before checkout sends its protobuf parent. The client reads a checkout log, takes its trace ID into the raw API, inspects the reported parent, and follows the trace Evidence operation back to the related log. It resolves both kinds of references. A second tenant writes the same trace and span IDs under metadata-only capture. The test checks its separate results, conflicting submissions by two credentials, partial responses during each store outage and loss of reference access after revocation.

For an interactive installation, build and fetch as above. Use a trusted local filesystem. The trace store's only listener is a Unix socket under the operator's mode-0700 state directory. Keep the authority database and candidate store together for the lifetime of the installation. The operator account is trusted. Local programs running as that account can access these files.

```sh
umask 077
state=$(mktemp -d /tmp/observe.XXXXXX)
target/debug/observe-backend init "$state"
python3 scripts/run-local-traces.py "$state" >"$state/traces.log" 2>&1 &
trace_pid=$!
python3 scripts/run-local-loki.py "$state" >"$state/loki.log" 2>&1 &
loki_pid=$!
target/debug/observe-backend serve "$state" >"$state/backend.log" 2>&1 &
api_pid=$!
base=http://127.0.0.1:4318
```

Use the [v0.1 bootstrap and private header-file helpers](headless-v0.1.md#bootstrap-and-provision-two-tenants) to redeem the owner ticket. Before creating the example tenants, use the following tenant and credential bodies. The trace capture setting is immutable for a tenant in this slice.

```sh
call owner "$base/api/v1/tenants" \
  -d '{"tenant":"alpha","capture":"text_body","trace_capture":"full"}'
call owner "$base/api/v1/tenants" \
  -d '{"tenant":"bravo","capture":"metadata_only"}'
call owner "$base/api/v1/tenants/alpha/credentials" \
  -d '{"service":"checkout","permissions":["ingest","ingest_traces"]}' | save_header writer
call owner "$base/api/v1/tenants/alpha/credentials" \
  -d '{"service":"analyst","permissions":["query","query_traces"]}' | save_header reader
```

The trace capture default is `metadata_only`, including for v0.2 tenants. Existing log grants keep their previous scope. The owner can issue trace grants to an existing tenant. Each trace tenant has a persistent, unique numeric VictoriaTraces account in the authority database. The adapter supplies that account on each read and write through the private socket. Restore coordination and stale-authority fencing remain open.

## Provisional APIs

| Operation | Authorization and inputs | Result |
| --- | --- | --- |
| `POST /v1/traces` | `ingest_traces`, standard ExportTraceServiceRequest in OTLP JSON or protobuf, optional gzip | OTLP response in the request encoding with attributable dispositions |
| `POST /api/v1/tenants/{tenant}/traces/{trace_id}` | `query_traces`, bounded JSON query | Individual submissions, authenticated identities, captured OTLP, parent candidates and per-record query references |
| `POST /api/v1/tenants/{tenant}/evidence/traces/{trace_id}` | Both `query_traces` and log `query` | Trace and log source results with separate coverage, effective ranges, limitations, provenance and references, plus relationships based on reported IDs |
| `POST /api/v1/tenants/{tenant}/logs/query` or `.../evidence/query` | Log `query`, optional `trace_id` filter | Existing log operations. Log evidence references also carry common identity and source fields. |

Trace queries require a 32-character nonzero lowercase hexadecimal trace ID in the path and decimal string `start_ns` and `end_ns` in the body. The interval is half-open, up to one hour. It selects spans by original start time and logs by their canonical log timestamp. `service` filters the provisioned service. The raw trace operation also accepts an exact `record_id`. Each source accepts `limit` from 1 through 100 (default 50) and a shared `deadline_ms` from 1 through 2,000 (default 2,000). Each page reports any truncation and supplies references for its returned rows. Narrowing the time window or filtering by service can reduce the result. v0.3 has no continuation cursor. If a single service exceeds a limit at one timestamp, even a one-nanosecond window can omit submissions that the client cannot enumerate through this API.

For example, after ingesting a correlated log and trace, set `start_ns` and `end_ns` to a window containing them. The following commands use the provisioned reader. A reference is a new authenticated query and may stop resolving after retention or credential changes.

```sh
q=$(jq -nc --arg start "$start_ns" --arg end "$end_ns" '{start_ns:$start,end_ns:$end}')
tenant_api="$base/api/v1/tenants/alpha"
trace=$(call reader "$tenant_api/logs/query" -d "$q" | jq -er '[.records[]|select(.trace_id != null)][0].trace_id')
call reader "$tenant_api/traces/$trace" -d "$q" | jq '.spans[]|{identity,parent_resolution}'
call reader "$tenant_api/evidence/traces/$trace" -d "$q" >"$state/evidence.json"
jq '{relationships,trace_coverage:.traces.coverage,log_coverage:.logs.coverage}' "$state/evidence.json"
ref=$(jq -cer '.traces.references[0]' "$state/evidence.json")
call reader "$base$(printf '%s' "$ref" | jq -r .path)" -d "$(printf '%s' "$ref" | jq -c .query)"
kill "$api_pid" "$loki_pid" "$trace_pid"
```

## Capture, identity and visibility

- `metadata_only` preserves ID bytes, parent and link contexts, kind, flags, start/end and event timestamps, status code and span dropped counts. It removes free text, trace state, attributes, resource and scope descriptions, and schema URLs before the adapter receives the batch. The read path checks this representation again. `full` explicitly opts in to the submitted OTLP fields, including free text, binary/nested attributes, events and links. Treat all such content as tenant-readable. This profile supplies no selective content classifier.
- Tenant, credential ID and provisioned service come from the authority. The resource and instrumentation scope in the OTLP unit are submitted claims. A credential attests the submitting producer, which can be a collector. Exact trace/span ID joins are correlation hints. A reported parent can have zero, one or several candidates in the returned page. When a producer sets the parent ID equal to the span's own ID, both raw and Evidence reads preserve that ID and mark its parent resolution as `self_reference`. A matching root supplies no collection or sampling guarantee.
- Each accepted submission gets a random record ID. Native trace ID, span ID, parent and timestamps remain intact. The adapter carries a sanitized protobuf resource/scope/span unit as a bounded attribute alongside its native indexing fields. This preserves the types that VictoriaTraces flattens, including future instrumentation attribute keys. Lookups use one fixed query with validated scalar filters and check each returned record against its tenant, time, capture policy and filters. External clients cannot submit a native query.
- The trace and log sources are read concurrently under one deadline. A successful source can contribute facts when the other is unavailable. If both sources fail before the shared deadline, the operation returns HTTP 503. Expiry of the overall query deadline yields HTTP 504. Each source records its requested window and observed extent. For spans that extent runs from the first returned start to one nanosecond beyond the last start. Collection, sampling, later arrivals and retention remain unknown, including after `no_matches`. There is no cross-store snapshot or inferred causal explanation.

## Admission, custody and limits

The trace receiver accepts at most 64 KiB compressed and 64 KiB inflated, and up to 256 spans per request. JSON preflight limits nesting to depth 32 and counts up to 4,096 container openings and commas outside strings. Protobuf preflight limits the OTLP message nesting it traverses to depth 32 and counts up to 4,096 visited field headers. Prost skips unknown protobuf groups using its own recursion accounting. Their inner fields and nesting fall outside the preflight counters. Both encodings share the byte and decoder caps. It limits each captured protobuf unit to 12 KiB and expanded batch content to 256 KiB. Shared resource and scope data count toward the expanded limit. Two trace decoders and four trace write reservations (two per tenant) bound work alongside the separate log lanes. Trace starts must be within 48 hours in the past and 10 minutes in the future. The sample store retains seven days. `OBSERVE_TRACE_RETENTION_DAYS` sets retention in whole days with a minimum of three for this write window.

| Observed boundary | Producer disposition |
| --- | --- |
| Authentication, invalid wire, compressed/decode or capacity limit before dispatch | HTTP 4xx. The receiver submits no batch. A wholly invalid decoded batch returns 400 and the rejected count. |
| Accepted empty export | HTTP 200 with zero counts |
| Store acknowledges the sanitized export with its expected empty HTTP 200 | HTTP 200, with OTLP partial success and the local rejection count if some submitted spans failed validation or the per-span cap |
| Socket unavailable before dispatch | HTTP 503, eligible records counted as unsubmitted |
| Non-success, unexpected reply, or lost transport after dispatch | HTTP 503, forwarded records counted as unknown. Retrying can create separately attributed duplicate records. |

The `X-Observe-{Accepted,Rejected,Unknown,Unsubmitted}-Records` headers count decoded dispositions. `X-Observe-Durable-Records` is zero for trace batches. A success establishes store admission only. Query visibility requires a subsequent authorized read. Observe keeps no durable trace copy and does no automatic trace replay. Producers that require outage buffering must retain their export across failure. A lost client reply can hide a completed write. Single-node durable log custody, Loki 204 settlement and recovery remain under the [accepted v0.2 decision](../adr/single-node-log-custody.md).

The trace read lane has four global reservations and one per tenant. Reads propagate remaining time to the store, bound its response to 2 MiB and bound retained base64 capture payloads to 128 KiB per page before rendering. The adapter verifies each source row, including omitted and lookahead rows, before reporting truncation. A row outside the tenant, filter, time or capture scope makes that source unavailable. Each successful source keeps its tenant and global reservations through local assembly and JSON serialization. Observe rechecks permissions and the deadline before handing the response to HTTP. A timeout or disconnect during blocking work leaves the worker charged until it finishes. Network transmission and client scheduling can add latency after handoff. Lost transport after dispatch retains the slot in memory for the process lifetime. Coordinated Observe and store restart clears residual work in this local profile. Backend-only restart cannot establish that a previous remote request ended.

## Verification and decision scope

```sh
./scripts/check.sh
python3 backend/headless/tests/test_traces.py -v TraceFailures RealStores
python3 backend/headless/tests/test_headless.py -v RealLoki
python3 backend/headless/tests/test_recovery.py -v Custody
```

The real-store test checks JSON and protobuf ingress, original nanoseconds, resource/scope fidelity, events/links, missing and late parents, duplicate IDs, two-tenant isolation, query limits, partial source failures, and authorized reference resolution. Controlled HTTP peers cover pre-persistence redaction, malformed input, response limits, upgrade permissions, revocation during lookup, slow queries, and definite versus ambiguous dispatch.

The conflict test submits the same trace ID, span ID, kind and start time under two credentials. After the collision it resolves both references and compares each producer, provisioned service and span name with the submitted fixtures. A separate real-store test reads a self-parent claim through both APIs. Another test holds a trace write open at a controlled peer and observes a same-tenant log become queryable in real Loki before that trace write finishes. Controlled peers place foreign and out-of-window rows at both cutoff boundaries. A synchronized Rust test stalls response construction through the deadline and checks that the timed-out worker retains both source reservations.

The source review of Tempo 3.1.1 found a tenant-scoped trace-by-ID API. Its combiner can collapse different submissions sharing ID and kind. VictoriaTraces' OTLP receiver and row query preserve both records in the tested workflow. The adapter remains replaceable. Physical storage selection and the public Evidence schema are deferred. [Tempo combiner](https://github.com/grafana/tempo/blob/v3.1.1/pkg/model/trace/combine.go), [VictoriaTraces ingestion](https://docs.victoriametrics.com/victoriatraces/data-ingestion/) and [query operations](https://docs.victoriametrics.com/victoriatraces/querying/).

This slice contributes evidence for REQ-001/005/007/008/009/013/015/016/018/019/025/026/027/029/030/032/034/036. Confirmed decisions include ADR-003/005/007/010–012, A1/A7/A8, D1/D2, I1/I2/I4/I5, EVID-01 and REC-03/04/05. Rust remains preferred under LANG-01. The tests exercise portions of TEST-001/003/005–008/014/024–027/029/030/034–036/042–044/050/051. Broader workloads, distributed recovery and release gates require additional evidence.

Full M3/M4/M5/M6 and TEST-038 remain open. Prior ambiguous log attempts can retain capacity, revoked or expired batches lack a disposal lifecycle, and power loss, physical disk failure and stale restore remain unverified. The [proposed trace custody decision](../adr/PROPOSED-single-node-trace-custody.md) records the separate future approval gate.
