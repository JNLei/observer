# Headless Backend v0.1

This slice takes application logs from an authenticated OTLP producer to a
tenant-scoped raw query and a reproducible evidence reference. It runs two
local processes, the Rust backend and the Loki 3.7.10 storage candidate.
The Rust binary contains the local management authority, telemetry gateway
and evidence operation. An external client needs only HTTP.

## Run locally

Use macOS or Linux, the repository's Rust toolchain, Python 3.10+, OpenSSL
with `req -addext`, curl and jq. The first build and binary download need
network access. Run these commands from the repository root in one POSIX
shell. The private state directory contains credentials, identity metadata
and Loki data. Keep it on a trusted local filesystem under operator control.

```sh
umask 077
state=$(mktemp -d /tmp/observe.XXXXXX)
cargo build --locked -p observe-backend
python3 benchmarks/ingest/otlp/fetch_tools.py --loki-only

target/debug/observe-backend init "$state"
python3 scripts/run-local-loki.py "$state" >"$state/loki.log" 2>&1 &
loki_pid=$!
target/debug/observe-backend serve "$state" >"$state/backend.log" 2>&1 &
api_pid=$!
base=http://127.0.0.1:4318
curl --silent --show-error --fail --retry 40 --retry-all-errors --retry-delay 1 \
  --unix-socket "$state/loki.sock:0" http://localhost/ready
```

Set `OBSERVE_LISTEN` to another numeric loopback address and port if needed.
`OBSERVE_LOKI_GRPC_PORT` defaults to 9096. Loki exposes HTTP only on the
private Unix socket and requires a client certificate on loopback gRPC.
The launcher generates a fresh 30-day local certificate on startup. Restart
the local store before it expires. Use `OBSERVE_LOKI_BINARY` to supply an
existing pinned native binary. The sample Loki compactor has a seven-day
retention setting. Set `OBSERVE_LOKI_RETENTION` to a supported Loki duration
to change it. Actual coverage depends on collection, admission and compaction.

## Bootstrap and provision two tenants

The Unix account that owns the mode-0700 directory is the installation trust
root. `init` atomically establishes one bootstrap grant. Concurrent or repeated
initialization preserves the original ticket. Its one-time secret is written
to `bootstrap.token` with mode 0600. Only high-entropy token digests enter
SQLite. Redeeming the ticket atomically replaces it with the owner's digest.
The owner can manage tenants and credentials. Data access requires a credential
with an explicit tenant permission.

The following commands read authorization headers from private files and keep
bearer values out of process arguments and terminal output.

```sh
{ printf 'Authorization: Bearer '; cat "$state/bootstrap.token"; } >"$state/bootstrap.header"
call() {
  header_file=$1
  shift
  curl --silent --show-error --fail-with-body -H "@$state/$header_file.header" \
    -H 'Content-Type: application/json' "$@"
}
save_header() { jq -er '"Authorization: Bearer " + .token' >"$state/$1.header"; }

call bootstrap -X POST "$base/api/v1/bootstrap" | save_header owner
call owner "$base/api/v1/tenants" -d '{"tenant":"alpha","capture":"text_body"}'
call owner "$base/api/v1/tenants" -d '{"tenant":"bravo","capture":"metadata_only"}'

call owner "$base/api/v1/tenants/alpha/credentials" \
  -d '{"service":"checkout","permissions":["ingest"]}' >"$state/writer.json"
save_header writer <"$state/writer.json"
call owner "$base/api/v1/tenants/alpha/credentials" \
  -d '{"service":"analyst","permissions":["query"]}' | save_header reader
call owner "$base/api/v1/tenants/bravo/credentials" \
  -d '{"service":"analyst","permissions":["query"]}' | save_header bravo
```

Tenant and provisioned service names start with a lowercase ASCII letter,
followed by up to 47 lowercase letters, digits, underscores or hyphens.
Credential creation returns an opaque ID and a one-time disclosure of its
32-byte random bearer token. One credential can hold both actions. A token
has a single tenant. A query grant can read that tenant’s captured logs across
services. The local authority checks the digest on each request.

## Ingest, query and resolve a reference

This example sends a standard OTLP/HTTP JSON log, searches a two-nanosecond
window, then resolves the first evidence reference with an ordinary HTTP
client. Protobuf exporters use the same `POST /v1/logs` endpoint with
`application/x-protobuf`. Both media types support gzip.

```sh
t=$(python3 -c 'import time; print(time.time_ns())')
trace=00112233445566778899aabbccddeeff
jq -n --arg t "$t" --arg trace "$trace" \
  '{resourceLogs:[{scopeLogs:[{logRecords:[{timeUnixNano:$t,severityNumber:9,
    body:{stringValue:"checkout completed"},traceId:$trace,spanId:"1122334455667788"}]}]}]}' \
  >"$state/export.json"
call writer -i "$base/v1/logs" --data-binary "@$state/export.json"

jq -n --arg start "$((t-1))" --arg end "$((t+1))" --arg trace "$trace" \
  '{start_ns:$start,end_ns:$end,trace_id:$trace,service:"checkout",limit:10,deadline_ms:1500}' \
  >"$state/query.json"
# Observe query visibility separately from the write acknowledgement.
for attempt in 1 2 3 4 5; do
  call reader "$base/api/v1/tenants/alpha/logs/query" --data-binary "@$state/query.json" >"$state/raw.json"
  if jq -e '.records | length > 0' "$state/raw.json" >/dev/null; then break; fi
  sleep 1
done
jq -e '.records[0]' "$state/raw.json"
call reader "$base/api/v1/tenants/alpha/evidence/query" --data-binary "@$state/query.json" >"$state/evidence.json"
jq '{outcome,coverage,effective_range,provenance,references}' "$state/evidence.json"
ref_path=$(jq -er '.references[0].path' "$state/evidence.json")
jq -e '.references[0].query' "$state/evidence.json" >"$state/reference.json"
call reader "$base$ref_path" --data-binary "@$state/reference.json" | jq .records

call bravo "$base/api/v1/tenants/bravo/logs/query" --data-binary "@$state/query.json" | jq .outcome
```

The last call returns `no_matches` on a fresh installation. Using Bravo's
credential against Alpha's reference path returns HTTP 403 with
`{"outcome":"denied"}`. The same credential gets the identical response for
an unknown tenant.

Revoke the example writer by ID. Stop only the two processes started above.

```sh
writer_id=$(jq -er .id "$state/writer.json")
call owner -X DELETE "$base/api/v1/tenants/alpha/credentials/$writer_id"
kill "$api_pid" "$loki_pid"
```

To restart, use the same state directory. If the owner token or bootstrap
response is lost, run `target/debug/observe-backend recover-owner "$state"`
as the directory owner. This immediately invalidates the previous owner and
any pending ticket. Repeat the bootstrap header and redemption commands with
the replacement ticket. Existing tenant credentials and logs survive owner
rotation. Service credentials are revoked through the management API.

## Capture and delivery contract

Tenant creation requires an explicit capture mode. Both modes keep the
authenticated producer credential ID and its operator-assigned service,
event and observed timestamps, local receive time, numeric severity, trace
flags and valid trace/span IDs. Submitted tenant/service claims, resource
and scope metadata, arbitrary attributes and severity text are discarded
in memory. Incoming `X-Scope-OrgID` is rejected. Cross-service correlation
IDs remain source claims and carry no authority.

`metadata_only` withholds every body. `text_body` retains a string body of at
most 4,096 UTF-8 bytes verbatim. A structured or larger body is withheld.
`body_capture` describes the disposition on every record. Text bodies can
contain secrets, so this mode requires deliberate tenant opt-in. The local
policy is fixed at tenant creation. Each admitted instance gets a new record
ID. Retries can create separate records.

| OTLP response | What the caller knows |
| --- | --- |
| 200, empty success | The candidate acknowledged every eligible row with HTTP 204. An empty export is also successful. |
| 200, partial success | The eligible rows received that acknowledgement. The protocol's rejected count covers records that failed local validation. |
| 400 or 413 before dispatch | Malformed input, all-invalid records or a request limit prevented submission. A request-level limit rejects the entire export. |
| 401, 403, 415, 408 or 429 before dispatch | Authentication, permission, media, body timeout or available capacity prevented submission. |
| 503 after a store attempt | The response distinguishes a connection failure before submission from an ambiguous submitted batch. Loki can commit a subset before returning 400. A lost response is also ambiguous. |

For a classified batch the response includes `X-Observe-Accepted-Records`,
`X-Observe-Rejected-Records`, `X-Observe-Unknown-Records` and
`X-Observe-Unsubmitted-Records`. Counts apply to this exchange. Failures
before classification omit counts. Full and partial successes and errors use
the OTLP response media type. The caller can verify visibility with a fresh
authorized query. Arbitrary-crash durability and downstream processing have
no acknowledgement in this slice. [OTLP response semantics](https://opentelemetry.io/docs/specs/otlp/#otlphttp-response)

The input caps are 64 KiB each on wire and after decompression, 256 records,
32 nested levels and 4,096 structural units in the supported message fields.
Two decoder slots bound aggregate decode work. The listener admits up to 32
connections with an 8 KiB HTTP buffer and 32-header limit. Header and body
reads have separate two-second deadlines. Ingestion has eight slots, at most
two per tenant. Query has four separate slots, at most one per tenant. These
are local safety limits. Workload performance and fairness remain unscored.

## Provisional query and evidence semantics

Both read routes accept a JSON request of at most 4 KiB. Use positive decimal
strings for Unix nanoseconds and a half-open interval `[start_ns,end_ns)`
of at most one hour. Filters are exact matches on `service`, `trace_id` and
`record_id`. IDs use 32 lowercase hex digits. Unknown parameters fail validation.
The row limit is 1–100 (default 50), and the deadline is 1–2,000 ms (default
2,000). The client supplies structured parameters. Only the adapter creates
the store query. It scopes the actual read to the authority-issued tenant,
bounds the response to 2 MiB and requests one extra row to detect truncation.
The query deadline starts at HTTP handler entry. The adapter forwards the
remaining budget using Loki's per-request limit. [Loki query API](https://grafana.com/docs/loki/latest/reference/loki-http-api/#query-logs-within-a-range-of-time)

The timestamp used for ordering is the original event time, then the observed
time if event time is absent, then local receive time if both are absent.
Missing source times remain null. `requested_range` is the client's interval.
`source_query_range` is the interval used in a successful source query.
`effective_range` is the half-open extent from the first returned timestamp
through one nanosecond past the last. Empty and unavailable results have no
observed extent. This extent says nothing about gaps between its endpoints.

| Outcome | Interpretation |
| --- | --- |
| `matches` | The source returned visible records. Collection, late arrivals and retained coverage remain unknown. |
| `no_matches` | The authorized source search succeeded with zero visible rows. |
| `partial` | The extra row proved truncation or the source signaled a warning. The response lists the reason. |
| `unavailable` | The source failed, the response was unusable, or the deadline expired. HTTP 503 or 504. |
| `denied` | Invalid/revoked credentials or insufficient scope. HTTP 401 or 403. The reply contains no target, source or reference. |

HTTP 429 means local capacity is occupied. HTTP 400 identifies a malformed
request. Successful evidence adds a retrieval timestamp, sanitized query,
source and transform identity, and per-record references. Re-run a reference
using its method, tenant-scoped path and query. Each resolution checks the
current credential. Retention or late data can change a repeated query. The
service stores no separate evidence copy or result cache.

## Security and recovery boundaries

- The deployment assumes a trusted OS account, private state and loopback
  clients. The operator has direct access to the files and storage. Bearer
  credentials last until revocation. Production TLS for external clients,
  distributed policy propagation, delegation, audits, backup handling and
  production deletion/retention policy remain open.
- The local authority rechecks writes following decode and authorizes queries
  again at dispatch and before release of results. Revocation blocks new
  dispatches. A storage write already in flight may finish.
- An expired or disconnected caller leaves dispatched work charged to its
  original lane. A completed source reply releases the slot. Loss of the
  post-dispatch transport leaves the slot charged for the life of the
  process, including after its five-second exchange timeout. To restore
  that capacity, stop the backend, stop the local Loki instance and confirm
  it has exited, then restart both. Restarting just the API cannot establish
  termination of old remote work.
- The direct path has no durable ingress queue. Loki's WAL and a 204 response
  do not prove arbitrary-crash custody. The earlier stock Collector 0.165.0
  experiment identified a startup delete/reinsert window for dispatched
  entries and an unresolved permanent-rejection handoff. No queue or quarantine
  substitutes for that missing guarantee in v0.1. [Collector custody limitation](../adr/PROPOSED-loki-poisoned-exports.md#scenario-8-and-production-gates)
- Freezing the public contract requires architecture-owner approval of the
  tenant/resource model, version compatibility, capture options, time and
  completeness fields, and reference lifecycle. Cross-adapter behavior still
  needs verification. Loki, SQLite metadata and the local grant representation
  are candidate implementation choices.

## Acceptance and traceability

Run the external-client suite after the build and pinned Loki installation.
It creates disposable private state, starts its own processes and stops them
on exit.

```sh
python3 backend/headless/tests/test_headless.py -v
./scripts/check.sh
```

| Exercised slice | Applicable architecture identifiers | Functional evidence |
| --- | --- | --- |
| Local authority and external clients | REQ-001, 003, 008–010, 013–014, 016, 037. ADR-002–005, ADR-A6, ADR-LANG-01. Parts of TEST-001–006, 041, 053. | Empty and concurrent initialization, single redemption, restart, owner recovery, scopes, revocation, known/absent foreign tenants. |
| OTLP and data boundary | REQ-005, 019, 025–027, 029. ADR-A1/A7/A8, ADR-I1/I2/I4/I5. Parts of TEST-007, 024–026, 029–030, 036, 043, 051. | Gzipped protobuf and JSON through real Loki, timestamp fallbacks, copied IDs, pre-store capture, partial rejection, refused/ambiguous writes, byte/decode/connection admission fixtures. |
| Raw retrieval and evidence | REQ-007, 015–016, 018, 032. ADR-010–012, ADR-EVID-01. Parts of TEST-003, 006–008, 014, 034, 042, 044, 050. | External reference resolution, original timestamps, bounded filters, truncation, warnings, source outage, malformed replies, deadline and residual work, revocation during a read. |

M3, M4 and M6 each have additional acceptance criteria. Full TEST-038,
REC-REF-24, broader recovery/fairness, policy and multi-signal gates remain
open. The first product slice is ready for independent review.
