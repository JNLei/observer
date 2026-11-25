# Headless Backend v0.2 — durable local logs

Observe keeps each admitted log batch on private local disk until Loki acknowledges it. During an outage a producer can receive HTTP 503 and later find its rows through the raw or Evidence API, even if it never resubmits them. Producers retain their retry obligation until OTLP success. A local durable commit, a Loki 204 and an authorized query are three separately observable events. [Owner decision](../adr/single-node-log-custody.md).

## Run and recover locally

Use the v0.1 [build, local Loki and bootstrap instructions](headless-v0.1.md#run-locally) and its [example producer and reader](headless-v0.1.md#bootstrap-and-provision-two-tenants). The `serve` command creates `log-spool.sqlite` with mode 0600 under the existing operator-owned state directory. Keep that directory and its rollback journals together on a private local filesystem. A v0.1 installation can reuse its authority and Loki data.

To exercise recovery with the shell variables from that guide, first provision Alpha, save its writer/reader headers, and create a recent `export.json` and matching `query.json`. Use a new timestamp if the example has already been sent. The command below stops the local Loki process before submitting, kills Observe after its local commit, and restarts both processes. Run it only against the disposable example installation.

```sh
kill "$loki_pid"
wait "$loki_pid" || true
curl -sS -i -H "@$state/writer.header" -H 'Content-Type: application/json' \
  --data-binary "@$state/export.json" "$base/v1/logs"
call reader "$base/api/v1/tenants/alpha/logs/delivery" | jq .
kill -KILL "$api_pid"
wait "$api_pid" || true
target/debug/observe-backend serve "$state" >"$state/backend.log" 2>&1 &
api_pid=$!
python3 scripts/run-local-loki.py "$state" >"$state/loki.log" 2>&1 &
loki_pid=$!
```

After Loki is ready, repeat the following reads. A 503 with `X-Observe-Durable-Records: 1` during the outage indicates a committed local copy. Recovery should advance the acknowledged total and make that instance observable within its event-time query window while authorization, adapter eligibility and working storage persist.

```sh
call reader "$base/api/v1/tenants/alpha/logs/delivery" | jq .
call reader "$base/api/v1/tenants/alpha/logs/query" --data-binary "@$state/query.json" | jq .records
call reader "$base/api/v1/tenants/alpha/evidence/query" --data-binary "@$state/query.json" | jq .references
```

## Reading delivery evidence

| Observation | Interpretation |
| --- | --- |
| 200, full or partial | Every forwarded member received a Loki 204 and local settlement committed. Partial success reports only the pre-admission rejections. An empty export retains v0.1 empty-success semantics. |
| 429 with durable count zero | The classified export exceeded the tenant/global spool budget. Eligible members remain unsubmitted. Earlier HTTP/decoder admission limits can return 429 before classification. |
| 503 with positive durable count | Local custody was committed. Accepted, rejected, unknown and unsubmitted headers describe the **original exchange**. Unsubmitted rows may be forwarded later. A response lost after dispatch leaves the producer uncertain. |
| Storage fault or missing response | The producer has no additional durability evidence from an absent header. A failed acknowledgement transaction can leave a previously committed payload pending even after Loki wrote it. Check delivery status after restoring storage. |
| Tenant delivery totals | `durable_records = store_acknowledged_records + pending_records` in a healthy snapshot. Totals count record instances. Pending includes retryable, active and held batches. Refused quota attempts and unknown record attempts are separate counters. |
| Remote and visibility evidence | `unsettled_dispatch_records` is the local dispatch journal. `residual_remote_attempt_records` counts work potentially still running, including after successful replay. `pending_with_prior_unknown` counts pending instances with a prior ambiguous attempt. Query results return the specific observed instances and times. Repeated reads can observe the same instance. |

The tenant delivery endpoint requires a current query grant. It returns aggregate counts and current payload bytes. Authorized queries and references retain the v0.1 limits, capture checks, provenance and unknown coverage semantics.

## Limits and dispositions

- At most 64 batches and 2 MiB of serialized canonical payload per tenant, 256 batches and 8 MiB globally. Insertion, quota checks and the durable increment share one transaction. Each export has the existing 64 KiB wire/decoded and 256-record limits. Another tenant can use remaining global capacity when a single tenant fills its allowance. All tenants receive backpressure at global exhaustion.
- SQLite uses a DELETE rollback journal, synchronous FULL and a default ceiling of 8,192 pages (32 MiB with 4 KiB pages). Set `OBSERVE_SPOOL_MAX_PAGES` to an integer from 8 through 8,192 to lower the main-file ceiling. Startup rejects a cap below the allocated file size. Leave headroom for the rollback journal, local authority and Loki. Logical pending bytes exclude this overhead. A spool transaction fault stops new spool work, returns a generic failure and leaves raw reads independent. Repair storage and restart Observe to reconcile the journal. Keep the original files.
- Fresh ingestion retains eight HTTP write reservations with at most two per tenant. A separate recovery worker selects one due batch at a time, rotates across eligible tenants and yields 50 ms between selections. There are separate durable remote-work budgets for fresh sends (8 global, 2 per tenant) and replay (2 global, 1 per tenant). Prior ambiguous work continues to consume those budgets across backend restarts.
- Completed failures and offline connects back off from 0.5 to 8 seconds plus a bounded batch-ID offset. For Loki 429/503, a valid longer Retry-After takes precedence. A bare 400 retries the whole batch and counts the forwarded members as unknown on each attempted write. A Loki 401/403 holds the batch and its uncertainty until operator repair and backend restart.
- A lost post-dispatch transport or interrupted send preserves the old charge against the OS-reported PID of the connected peer. Replay can use free capacity while that peer is alive. When the same directly connected, single-process store exits, the worker can reclaim its charges. PID reuse can delay reclamation. A forwarding proxy with asynchronous downstream work would require its own termination evidence. The deployed topology uses a direct private Unix socket.
- Replay checks the original credential ID, tenant, service, capture representation and full batch time window before sending. A revoked writer, a decodable record that fails canonical validation, or a newly expired member holds the entire admitted batch against capacity. Other eligible batches can advance. The adapter keeps the 48-hour past / 10-minute future window and original timestamps. Held content has no automatic expiry, replay override, or administrative disposal path in this slice. Plan for accumulated holds before using it with sensitive production data.

## Verification and limits

Build the backend, fetch the pinned Loki binary and run the suites from the repository root.

```sh
cargo build --locked -p observe-backend
python3 benchmarks/ingest/otlp/fetch_tools.py --loki-only
./scripts/check.sh
python3 backend/headless/tests/test_headless.py -v RealLoki
python3 backend/headless/tests/test_recovery.py -v Custody
```

| Injected condition | Oracle |
| --- | --- |
| Loki socket unavailable, 20 historical batches per tenant, then SIGKILL Observe | Compare independently expected timestamps, identity, correlation, redaction and multiplicities with the private spool. Restore real Loki 3.7.10, restart Observe with no producer resubmission, and resolve raw and Evidence rows against that inventory. |
| Live traffic during recovery | Two identical fresh exports produce distinct IDs. One authorized event-time query observes both fresh rows and a proper subset of history. Both tenants' full eligible histories subsequently reconcile. |
| Real Loki 204 withheld by a test Unix proxy, then Observe killed | Observe query visibility while local acknowledged count is zero. Kill and restart the backend twice, retain the uncertain attempt, replay the same ID, keep the prior peer charged until it exits, and check returned multiplicity. |
| Client abandons its HTTP request after a one-second timeout | Hold the downstream reply, observe a committed active dispatch, then release the 204 and verify one submission and local settlement. |
| Record rejection, mixed error and throttling | A controlled peer returns 400, 429 with Retry-After, then 204. Verify validated rejection counts, exact retry rows, retry delay, and cumulative uncertain attempts. |
| Logical limits | Race the last tenant slot, admit a peer, fill serialized bytes and the global entry limit. Inspect explicit refusals and previous committed counts. |
| Local transaction and file-growth failure | SQLite triggers abort admission after insertion and release after Loki's simulated 204. A separate eight-page main-file limit forces SQLite allocation failure. Verify rollback, withheld success, restart reconciliation and survival of the earlier committed row. |
| Revocation and age | Remove a writer during an outage and let a second tenant's row age out. Verify both holds, tenant-specific counts and absence from real Loki. |

These are bounded process-restart tests on an intact local filesystem. They do not exercise sudden power removal, a torn filesystem, physical device ENOSPC/fsync failure, malicious database modification, stale backup restore, node loss, replication, prolonged load or a 24-hour outage. Visibility depends on Loki retention and availability. After a local settlement removes the payload, Loki owns its own failure scope. Production deletion, high availability, multi-signal recovery, performance thresholds, full M3/M4/M5/M6 and TEST-038 remain open.


Local verification on macOS ARM64 passed the repository check script, all 13 v0.1 headless tests and the v0.2 custody suite. The controlled lost-reply case produced two Loki submissions of one ID and one visible row in the local query. That multiplicity is an observation for this fixture. The push workflow repeats these checks on Linux. Independent review of v0.2 is pending.
