# AI-native observability platform

Headless Backend v0.2 accepts authenticated OTLP/HTTP logs, applies tenant capture policy and commits them to a bounded local buffer. The backend replays eligible logs after a process restart or Loki outage. Producer success requires Loki 204. External clients can inspect tenant delivery counts, search raw logs and resolve evidence references. [Local setup and recovery tests](docs/implementation/headless-v0.2.md) and [API examples](docs/implementation/headless-v0.1.md#bootstrap-and-provision-two-tenants).

The [v0.5.1 architecture](docs/architecture/AI_Native_Observability_Architecture_Spec_v0.5.1.md)
remains authoritative. The [roadmap](docs/implementation/roadmap.md) records
the owner’s scoped Rust-first decision and the open language and recovery
gates. The repository also includes the earlier liveness endpoint and
the Rust/Go prototypes.

The [16-worker logs benchmark](docs/implementation/benchmark.md#reproducing-the-controlled-log-pairs)
has a Linux runner, a five-pair workload and an offline evidence verifier.
The [validation record](docs/implementation/validation-m1-reference.md)
reports sustained ingestion, queue pressure and replay observations.

## Local requirements

- Rust **1.100.0**, including `rustfmt` and `clippy`; `rust-toolchain.toml`
  lets [rustup](https://rustup.rs/) install the pinned toolchain.
- Go **1.27.4** for the reference build; the Go module sets this minimum.
- Python **3.10+**, a POSIX shell and `curl`. Cargo.lock and the OTLP Go
  module pin the protocol dependencies. The first build requires registry
  access. The recovery script fetches checksum-verified native releases of
  Collector Contrib **0.165.0** and Loki **3.7.10** for macOS/Linux on
  arm64/amd64. Recovery uses disposable private directories on local disk.

The checks have been designed for macOS/Linux. On a system with multiple
compilers, put the intended Rust and Go binaries on `PATH` and verify
`rustc --version`, `cargo --version`, `go version` and `python3 --version`.
If a newly installed rustup toolchain is not yet on your shell `PATH`, run
`. "$HOME/.cargo/env"` or open a new shell after installing rustup.

## Run the process liveness endpoint

```sh
cargo run -p observe-health
# In another terminal:
curl -i http://127.0.0.1:8080/healthz
```

`OBSERVE_HEALTH_ADDR` can select another **numeric loopback** address and port;
see [the example](config/health.env.example). The server has four workers and
an eight-connection queue, a 4 KiB header cap and a two-second absolute read
deadline. HTTP/1.0 and HTTP/1.1 request lines must end in exactly CRLF. When
the queue is full it attempts a 503 response. Stop it
with Ctrl-C. `{"status":"ok"}` means only that this process can reply; it does
not imply platform readiness, durable storage, or any other component's health.
This endpoint contains no sensitive data or privileged operations and is not an
authenticated management API. Non-loopback listeners are rejected.

## Check and test

```sh
./scripts/check.sh
```

The script checks formatting, Clippy, Go vet, builds, health integration,
headless authorization and failure behavior, synthetic accounting and
cross-language OTLP conformance. Push CI also runs the product test against
pinned Loki and keeps the existing functional recovery and custody checks.
The timed reference comparison is available by manual workflow dispatch. The local Go race pass also exercises
the instrumented binaries through their network endpoints.

```sh
./scripts/check-race.sh
./scripts/bench-recovery.sh
```

The recovery script writes per-variant results and environment metadata to a
new ignored results directory. It models 24 hours of **event time** and tests
real process failure with ongoing live writes and reads. See the
[predeclared workload charter](docs/implementation/workload-charter.md) and
[OTLP trial contract](docs/implementation/benchmark.md#otlp-logs-and-restart-trial).
The [M1 validation record](docs/implementation/validation-m1-logs.md) includes
the measured restart and visibility results for both receivers.

Run the reproducible **functional smoke**, without comparing performance:

```sh
./scripts/bench-smoke.sh
```

Each run writes a new ignored results directory with its configuration, binary
hashes and tool versions. It does not implement a durable queue or 24-hour
recovery scenario; see [benchmark limitations](docs/implementation/benchmark.md).

## Repository map

- `backend/headless/`: Rust authority, OTLP log intake, storage adapter, raw and Evidence APIs, and external-client tests.
- `backend/health/`: small loopback-only Rust process liveness component.
- `benchmarks/ingest/`: matched Rust/Go synthetic forwarders, bounded workload
  generator, mock downstream, scenarios and conformance tests.
- `benchmarks/ingest/otlp/`: matched OTLP receivers, shared conformance suite,
  Collector/Loki configurations, fetch utility and recovery runner.
- `agents/`, `evaluation/`: reserved for independently deployable Python
  runtimes, not implementations or chosen frameworks.
- `ui/`: reserved for the optional presentation client; no UI is implemented.
- `docs/architecture/`, `docs/adr/`, `docs/implementation/`: authority,
  decision-record location, roadmap and benchmark design.
- `config/`, `scripts/`, `.github/workflows/`: example local configuration,
  development checks and CI foundation.

The exact open-source license, engines, queue, policy engine, stable public API
and deployment packaging remain open decisions. No production data or secrets
are needed by these examples.
