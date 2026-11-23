# Foundation validation — 2026-11-22

This records local evidence for the initial foundation. It is not a release,
security, durability, language-performance or architecture acceptance report.
No Git metadata was present in the supplied directory, so no source revision or
Git diff could be recorded. The hosted CI workflow was created but was not run.

The owner subsequently reported local acceptance after independent re-review.
See the [roadmap](roadmap.md#m0--repository-foundation) for closeout of the
two non-blocking P3 findings. The following entries describe earlier runs.

## Independent-review corrections — before re-review

The independent reviewer did not approve the initial M0 sign-off. The six
reported findings have been corrected and the checks below passed locally. M0
is **ready for independent re-review**, not independently approved. The work
was limited to those corrections; later milestone implementation remains paused.

- **P2 request syntax:** valid CRLF requests still succeed; LF-only and
  extra-CR request-line terminators return 400 in regression tests.
- **P2 outstanding work:** both forwarders expose synchronized accepted,
  queued, in-flight, completed and rejection counters. The harness snapshots,
  requests a bounded drain, checks counter conservation and separately reports
  active sink handlers. On a forced stop it records the last snapshot and the
  confirmed process exit, but the exact number aborted after that snapshot is
  unknown.
- **P2 unexpected exits:** the harness monitors process status throughout the
  run and drain; it retains only the last 8 KiB of stderr. Tests kill each real
  forwarder after downstream work begins and exercise a process that exits 42
  during drain, plus a
  verbose crashing process with a truncated diagnostic tail.
- **P2 large frames under overload:** the same ready/busy handshake in both
  languages precedes payload transmission. Server overload and response-write
  counters are reported, and observed client statuses are checked against server
  decisions; lost responses remain unknown. Tests send valid 1 MiB payloads while the receiver is saturated.
- **P3 concurrent health:** a four-worker, eight-connection bounded queue lets
  a healthy request complete while another client stalls; a regression test
  exercises the actual binary.
- **P3 shell validation:** `scripts/check.sh` validates every `scripts/*.sh`
  separately. A temporary second invalid script verifies that the loop fails.

### Commands and results

The following were run on macOS 27.2 / arm64 with Rust 1.100.0, Go 1.27.4 and
Python 3.10.1:

```sh
export PATH="/Users/jasonlei/.local/opt/go1.27.4/go/bin:/Users/jasonlei/.cargo/bin:$PATH"
./scripts/check.sh
```

Passed: Rust formatting and Clippy, six health integration tests, workspace
builds, Go formatting and vet, and all 15 shared harness tests. The Go package
has no separate Go-only test files. The shell script runs `sh -n` separately
for each script. The hosted CI workflow was not run.

The review regressions were also run directly with:

```sh
PYTHONPATH=benchmarks/ingest/tests python3 -m unittest -v \
  test_conformance.ForwarderConformance.test_two_timed_out_clients_do_not_hide_queued_work \
  test_conformance.ForwarderConformance.test_bounded_drain_reports_unresolved_work_without_inventing_aborts \
  test_conformance.ForwarderConformance.test_maximum_frames_under_overload_have_server_side_accounting \
  test_conformance.ForwarderConformance.test_killed_forwarder_fails_the_scenario \
  test_conformance.HarnessValidation.test_stderr_diagnostics_are_bounded_and_include_crash_exit \
  test_conformance.HarnessValidation.test_shell_check_validates_each_script
```

All six passed for the applicable Rust and Go variants. The later complete
check also includes the added drain-crash and counter-conservation tests.

```sh
go -C benchmarks/ingest/go build -race -buildvcs=false -o ../bin/ingest-bench-go-race .
export OBSERVE_BENCH_GO="$PWD/benchmarks/ingest/bin/ingest-bench-go-race"
export GORACE="halt_on_error=1 log_path=/tmp/observe-m0-final-race"
python3 -m unittest discover -s benchmarks/ingest/tests -v
```

All 15 shared tests passed with the Go race-instrumented binary, and no
`/tmp/observe-m0-final-race*` report was produced. This covers only exercised
paths and is not proof of the absence of races.

### Reproduced accounting cases

Before the correction, the two-request reproduction on each variant reported
two client timeouts and two frame sends, but only one sink record and no
forwarder queue or in-flight state. After the correction, the equivalent
workload reported two client timeouts, one frame sent after ready, and a
pre-drain snapshot of one queued plus one in-flight connection. Both variants
then drained two accepted connections: one sink echo and one incomplete frame.
The client timeout did not imply that the first downstream operation stopped.

With a 30 ms drain and a delayed sink, both variants reported one queued and one
in-flight connection in their last snapshot, a confirmed forced child exit, one
active sink handler, and an unknown exact abort count. In one 40-attempt test
with 1 MiB payloads, each variant recorded four sink ACKs and 36 server and
client overloads. This is a correctness example, not a guarantee that overload
responses will always be observed or a performance comparison.

No source revision or Git diff is available because the supplied directory has
no Git metadata. The architecture Markdown was not edited; both its root path
and documented symlink resolve to SHA-256
`52a377771e79ce511b0977d867e5a781860815281c367ea214568a75e0d6fda2`.
TEST-001, TEST-026, TEST-030, TEST-035–038, TEST-044 and TEST-054 remain open
at their full architecture scope. No ADR or deferred technology choice is made.

## Original local checks — before independent review

`./scripts/check.sh` passed locally with Rust 1.100.0, Go 1.27.4 and Python
3.10.1 on macOS 27.2 / arm64. It ran Rust formatting, Clippy with warnings
as errors, all workspace tests/builds, Go formatting, `go vet`, Go package tests
(the package has no Go-only test files), Python compilation, and shared
integration tests that launch and exercise both built binaries. Rust's four
health tests passed, including an actual binary invocation, loopback guard,
request-line handling, header limit and absolute read deadline. The shared
suite covers matching responses, saturation, disconnect/reconnect, ambiguous
outcomes, delayed sink
and generator accounting. All seven shared integration tests passed against
both forwarders. The historical `sh -n scripts/check.sh scripts/bench-smoke.sh`
invocation returned success, but checked only the first script; the current
per-script loop corrects that gap.

The Go forwarder was additionally built with `go build -race -buildvcs=false`
and the same seven tests ran with `OBSERVE_BENCH_GO` pointing to that binary
and `GORACE=halt_on_error=1`. They passed with no race report for the exercised
paths. This does not prove the absence of all races.

The earlier report described the then-local M0 criteria as met; the subsequent
independent review found the issues above. The health-only process is a small
headless component and does **not** satisfy TEST-001, which additionally
requires real ingest, query, alerts and management. No protected data access is exposed.
TEST-026 and production authorization, first-owner bootstrap and other
system-level acceptance tests remain open. M1 is partial; no production
language, durability, outage or resource acceptance gate has passed.

## Original functional smoke — before corrections

This historical run predates the review corrections and did not report
forwarder queued or in-flight work; it cannot establish their final dispositions.

`./scripts/bench-smoke.sh` ran once per variant with the same included
[scenario](../../benchmarks/ingest/scenarios/smoke.json): four forwarder workers,
an eight-connection queue, eight generator workers, 32 queued generator
requests, 128-byte payloads, 250 ms server and 1,000 ms client timeouts. The
four phases scheduled 10 steady, 20 burst, 10 disconnected and 10 reconnected
records; the disconnect closes newly accepted sink connections. Results:

| Variant | Steady + burst ack | Disconnect failure/unknown | Reconnected ack | Unsent |
| --- | ---: | ---: | ---: | ---: |
| Rust | 30 | 10 | 10 | 0 |
| Go | 30 | 10 | 10 | 0 |

Each volatile sink observed 40 unique valid IDs and 10 simulated disconnects.
These small counts verify the intended paths, not relative throughput or latency.
The forwarder ACK means only a mock sink ID echo; failures can be ambiguous and
neither acknowledgement nor queue drain implies durability or queryability.
The actual 24-hour outage and replay scenario was **not run**.

The machine was an Apple M1 Pro with 10 logical CPUs and 16 GiB RAM; CPU/memory
quotas were not set or measured, `GOMAXPROCS` was unset, and generator, sink and
forwarder shared the host. The Rust prototype used the Cargo release profile;
the Go build used default optimization with `-buildvcs=false`. One trial each,
no affinity, warmup, randomized ordering or background-load controls, and no
RSS/GC/cgroup measurements mean the results cannot support a performance
comparison. These are functional observations, not an approved workload or SLO.

The local, ignored artifacts at `benchmarks/ingest/results/smoke.blPxsS/`
contain each complete JSON result and build metadata. SHA-256 hashes of the
binaries in this run were:

- Rust: `5c70090dc5a7b18a0b3628213fbae651a47d31474f7e51a9441058d0e4258e40`
- Go: `888f513d2ac3e6e473f045cebc100346f4e8c3f6fe3329f47c89993459659827`

See [benchmark design](benchmark.md) for the missing real protocol, policy,
persistence, replay, resource accounting and fairness work and the complete
TEST-038 gate.
