# AI-native observability platform: foundation

This repository contains an early repository foundation, not a working
observability platform. The intended architecture is described by the
[authoritative v0.5.1 specification](docs/architecture/AI_Native_Observability_Architecture_Spec_v0.5.1.md).
The current code provides a local process-liveness endpoint and a separate,
synthetic Rust/Go forwarding test. It does not ingest telemetry, provide
management, authorization, storage, evidence, alerts or durable recovery.

The four architectural planes are logical boundaries. The eventual self-hosted
backend must be usable without the optional UI, built-in agent, evaluation
workers or a hosted service. Existing decision statuses and implementation
gates are summarized in the [roadmap](docs/implementation/roadmap.md). The
[benchmark design](docs/implementation/benchmark.md) describes the limited
prototype, measurement accounting and unfinished production-shaped gate. The
[initial validation record](docs/implementation/validation-2026-11-22.md)
records the local checks and smoke observations.

## Local requirements

- Rust **1.100.0**, including `rustfmt` and `clippy`; `rust-toolchain.toml`
  lets [rustup](https://rustup.rs/) install the pinned toolchain.
- Go **1.27.4** for the reference build; the Go module sets this minimum.
- Python **3.10+**, a POSIX shell and `curl`. There are no third-party runtime
  dependencies in the current Rust, Go or Python code.

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

The script checks Rust formatting and Clippy, runs Rust tests, checks Go
formatting and `go vet`, builds both prototypes, and runs the same Python
protocol/failure/accounting tests against both binaries. The health integration
test starts the actual Rust process. CI runs this same script on Linux. These
checks do not constitute the architecture's complete TEST-001 headless or
TEST-038 language acceptance procedures.

Run the reproducible **functional smoke**, without comparing performance:

```sh
./scripts/bench-smoke.sh
```

Each run writes a new ignored results directory with its configuration, binary
hashes and tool versions. It does not implement a durable queue or 24-hour
recovery scenario; see [benchmark limitations](docs/implementation/benchmark.md).

## Repository map

- `backend/health/`: small loopback-only Rust process liveness component.
- `benchmarks/ingest/`: matched Rust/Go synthetic forwarders, bounded workload
  generator, mock downstream, scenarios and conformance tests.
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
