# M1 log ingestion and restart evidence

The first production-shaped slice is ready for independent review. The
production language decision and full M1 acceptance remain open. The tested
implementation is [06794c1](https://github.com/JNLei/observer/commit/06794c1dfb17ca1a586b9bd1c9c429809ef6d980).
The working tree was clean for the paired local recovery run.

## What the recovery run established

On a fresh private disk directory per variant, the Collector accepted 24
historical batches of four records while the receiver was offline. Its queue
reported 24 requests both before and after a process kill. Bringing up the
receiver without Loki produced one four-record attempt with an unknown store
outcome, zero store acknowledgements and zero successful source exports in
each run. All 24 queue entries remained observable. The harness then killed
the receiver, started Loki and reconnected the receiver during live generation.

Each healthy receiver handled 48 batches, including the 24 fresh single-record
exports. Both recorded 7,083 gzip input bytes and 9,132 bytes of acknowledged
sanitized protobuf. Independent queries retrieved all 120 expected event-time
and line identities, together with the expected trace ID, span ID, observed
timestamp and severity. The second tenant saw zero rows. Both source queues
drained and the source exporter recorded 96 historical and 24 live records.
After a Loki SIGKILL and restart on the same disk, each query returned the
same rows and multiplicities. Each run observed 49 WAL records logged and zero
WAL disk-full failures before the kill.

| Local functional measurement | Rust | Go |
| --- | ---: | ---: |
| Unique visible before / after Loki restart | 120 / 120 | 120 / 120 |
| Missing / extra / visible duplicate rows | 0 / 0 / 0 | 0 / 0 / 0 |
| First fresh visibility from reconnect start | 96 ms | 244 ms |
| All 96 historical records visible from reconnect start | 1.134 s | 1.065 s |
| Fresh visibility P50 / P95 / P99 from intended arrival | 63 / 102 / 113 ms | 81 / 145 / 245 ms |
| Successful query P50 / P95 / P99, 31 probes each | 20 / 24 / 34 ms | 19 / 25 / 33 ms |
| Effective unique records/s through final visibility | 51.1 | 51.0 |
| Healthy receiver process peak RSS | 4.02 MiB | 20.23 MiB |

These are polling upper bounds from one unscored pair. The 24 hours are
represented in event timestamps over 24 batches. Live arrivals were scheduled
at 10/s for 24 records. Each run observed a fresh row before the entire
history became visible. Production volumes, sustained capacity, physical
duplicate writes, fairness under sustained overload, and a wall-clock
24-hour disconnect require additional evidence.

The host was an Apple M1 Pro with 10 logical CPUs, 16 GiB RAM and macOS 27.2.
Rust 1.100.0 used a release build with two async runtime threads and up to two
blocking decoders. Go 1.27.4 used default optimization, GOMAXPROCS=2 and
GOGC=100. Both variants used Collector Contrib 0.165.0 and Loki 3.7.10.
The generator, receiver and dependencies shared a host. There were no CPU,
disk, memory or network quotas. Rust ran first. Process RSS comes from
`wait4` on each child. The JSON records each process lifetime separately.
This sample supports functional parity for the covered log profile.

The local raw file is `benchmarks/ingest/results/recovery.umSRmw/result.json`
with SHA-256 `5e79c1932e6e4e456becbe4d4b02ffd0907cb3a384989c9388ea7b5909d4dc33`.
Its matched query-result digest is
`246291863052891ec9aaa1d47c919c96922f37398f5cac78c04a9f53931f957a`.
Fresh runs produce their own ignored directory, absolute fixture epoch,
binary hashes, queue snapshots, counters and measurements.

## Verification and traceability

| Command or component | Evidence |
| --- | --- |
| `./scripts/check.sh` | Rust formatting, Clippy with warnings as errors, workspace tests/builds, Go formatting/vet, Python compilation, 7 health tests, 16 synthetic harness tests and 6 OTLP conformance cases against each real receiver passed locally. |
| `./scripts/check-race.sh` | All 12 receiver/scenario combinations and all 16 synthetic tests passed with race-instrumented Go binaries. The report directory contained no race report. |
| `go -C benchmarks/ingest/otlp/go test -run '^TestFullSizeGzipAndRecordLimit$' -count=10` | The exact 64 KiB inflated request and 256-record boundary passed ten repetitions per receiver. The sink consumes and decodes the entire write before acknowledging. |
| `./scripts/bench-recovery.sh` | Both real restart trials passed locally and produced matching query digests. |
| `./scripts/bench-smoke.sh` | Each synthetic run recorded 10 steady, 20 burst and 10 reconnect ACKs, 10 disconnected failure/unknown outcomes, 40 successful sink writes in 40 attempts and zero active handlers at the final snapshot. |
| Components | The M0 commit closes both P3s. The charter precedes receiver implementation in Git. OTLP receivers, shared tests, source/store configs, recovery driver, build/check scripts and the Linux workflow are the M1 changes. |
| Requirements | REQ-008, 025–029 and 035–039. ADR-005/007, ADR-I1/I2/I4/I5, ADR-REC-01–06, ADR-LANG-01 and ADR-BENCH-01. Partial evidence for TEST-029/030, 032–038, 043/044, 051 and 054. |

GitHub Actions records the Linux build, shared conformance and native
Collector/Loki restart steps for the tested code. [Implementation workflow run](https://github.com/JNLei/observer/actions/runs/45102057737).

The source queue and log store are owner-approved trial choices. The path
runs headlessly, applies tenant authorization and capture filtering before
platform writes, and queries the store independently. No architecture change
or new ADR is proposed. Production identity, durable replication, disk-full
and node-loss behavior, metrics writer/temporality and reset semantics
(TEST-045), traces, adaptive admission, mixed alerts/evaluation, deployment
pressure (TEST-055), REC-REF-24 and quantitative SLO/cost criteria remain
open. TEST-038 requires the full paired workload and a reviewed decision ADR.
