# Implementation roadmap

This is an implementation plan, not an architectural decision record or a
release commitment. The [v0.5.1 architecture](../architecture/AI_Native_Observability_Architecture_Spec_v0.5.1.md)
remains authoritative. Milestones are independently verifiable increments;
the listed dependencies must be available before integrating a later slice.
The complete acceptance tests apply to the finished capabilities, not merely
to a directory, a health check, or a mock.

## Current priority — Headless Backend v0.1

The architecture owner authorized Rust-first product development after the
independent review of `f2594f9361dfcd12b82ccf2d889c1efa378d4a18`. The
controlled, fixed-load logs comparison is accepted within its tested scope.
The first product delivery connects the M3 local authority, M4 OTLP logs and
raw reads, and one M6 evidence operation. Full TEST-038, multi-signal work,
recovery qualification and REC-REF-24 remain open. Standalone timed
comparisons are paused. [Owner decision](../adr/rust-first-headless-slice.md)
and [v0.1 contract and functional checks](headless-v0.1.md).

The subsequent product review rejected `1176a5e`. The corrected logs slice
now budgets read pages before dispatch, classifies timestamp rejections per
record, validates stored fields before issuing evidence, and decodes OTLP
JSON nulls and special doubles. Independent re-review is pending.
[Product checks and scope](headless-v0.1.md#acceptance-and-traceability)

The earlier benchmark review identified two reporting corrections.

1. Natural overlap is demonstrated in nine of ten hosted trials. Separate
   probes in pair 1 Go leave completion order unresolved. The report now
   distinguishes those observations from the joint paced probes.
2. The offline verifier can miss changed pre-disconnect unknown counters and
   reported recovery times, and omits reconciliation with the published
   summary. Target an offline check against raw counters, probe timestamps
   and the summary, with negative fixtures for both changes. The reviewer
   independently confirmed the original counts and timings. This verifier
   follow-up is open. [Reviewed measurements](validation-m1-reference.md)

## Decision and boundary rules

- **CONFIRMED:** four logical planes, complete self-hosted headless core, local
  authority, shared authorization at actual data access, Evidence Contract
  responsibilities and six principles, A1–A8, D1–D6, I1–I6 and REC-01–06.
  Logical planes do not require four binaries or a fixed deployment topology.
- **PREFERRED:** Rust for owned backend, Python for optional agents/evaluation,
  TypeScript for an optional UI. Rust remains subject to the production-shaped
  Rust/Go gate; reuse mature OSS written in other languages.
- **PROPOSED:** example evidence fields, tool names and JSON, metadata-first
  capture, and suggested lifecycle/handoff mechanisms are starting points only.
- **DEFERRED:** engine/build-vs-integrate scope, storage/queue products, public
  schemas/transports, authorization engine/policy syntax, capture defaults,
  deployment topology, license, workload thresholds and SLOs, and hybrid access.
  A milestone requiring one must evaluate options and obtain the owner's decision
  at the specified gate; the synthetic test protocol makes none of these choices.

The Evidence Contract comprises identity/correlation, evidence query and evidence
result responsibilities; the principles are common identity, precise time,
provenance, completeness, consistent authorization and backend independence.
External agents must be able to use it without the built-in runtime. Evidence,
hypotheses and verified conclusions remain distinct. No public evidence schema
is frozen by this plan. S01–S14 and TEST-042–055 apply when their respective
features are enabled; their tests are not waived by the milestone sequence.

## M0 — Repository foundation

- **Objective / deliverables:** small Rust workspace, loopback-only process
  liveness executable, setup/check scripts, CI, optional Python agent/evaluation
  and UI locations, architecture/ADR/implementation documentation.
- **Components / dependencies:** Rust standard library; no storage, control
  plane, worker, UI, hosted service or third-party runtime library.
- **Traceability:** REQ-001, REQ-009, REQ-014, REQ-026, REQ-037;
  ADR-002, ADR-003, ADR-A8, ADR-LANG-01. The health smoke supports development
  toward TEST-001 and TEST-026 but is not either test's complete pass condition.
- **Acceptance / validation:** clean setup builds the binary; GET `/healthz`
  succeeds locally with optional components absent; non-loopback binding and
  malformed request lines and oversized headers are rejected; formatting, lint
  and tests pass locally through the check script. CI is configured to run the
  same checks.
- **Blocking decisions:** none. The exact license remains open before public
  release; no license is inferred from the OSS direction.
- **Status:** locally accepted after independent re-review, as reported by the
  owner. Both non-blocking P3 findings are closed. The health endpoint now
  checks header field syntax and the synthetic sink distinguishes attempted,
  failed and successful acknowledgement writes. The first
  [hosted foundation run](https://github.com/JNLei/observer/actions/runs/45100149504)
  passed after Git initialization.
  The [original validation record](validation-2026-11-22.md) describes the
  earlier review and its local regression checks.

## M1 — Workload charter and first production-shaped comparison

- **Objective / deliverables:** retain the synthetic baseline, predeclare a
  [workload charter](workload-charter.md) and exercise real log ingestion,
  restart, replay with live traffic and independent reads in Rust and Go.
- **Components / dependencies:** an authenticated isolated benchmark receiver,
  generated OTLP protobuf models, Collector persistent sending queues and a
  single-node Loki WAL trial. The same harness drives both release binaries.
- **Traceability:** REQ-008, 025–029, 035–039, ADR-I1/I2/I4/I5,
  ADR-REC-01–06, ADR-LANG-01 and ADR-BENCH-01. Partial coverage of
  TEST-029–030, 032–038, 043–044, 051 and 054. S03, S04, S10, S13 and S14
  apply at the full comparison gate. See the [benchmark design](benchmark.md).
- **Acceptance / validation:** both variants pass identical protocol/error,
  overflow, stall, interruption, reconnection and accounting checks. Record
  any gaps without presenting a microbenchmark as language evidence.
- **Blocking decisions:** the owner selected the Collector and Loki for the
  process-restart trial. Scoring requires representative workload evidence,
  approved SLO/cost/loss criteria and additional signal candidates.
- **Status:** the reviewed single-node logs slice has corrected integrity,
  overlap and accounting checks. The permanent-rejection regression demonstrates
  a blocked history lane. The owner approved durable quarantine in principle.
  Its handoff contract and implementation still require review. The full
  scored recovery and language gate remains open. [Review evidence](validation-m1-logs.md)
  and [proposed custody policy](../adr/PROPOSED-loki-poisoned-exports.md).
  The [controlled logs comparison](validation-m1-reference.md) adds repeated
  paired evidence for the independently accepted 16-worker profile, with
  per-service Linux budgets.

## M2 — Production-shaped language and recovery gate

- **Objective / deliverables:** equivalent real protocol decode/validation,
  shared authorization, capture/redaction, batching/compression, bounded I/O,
  durable retry/replay, identical candidate signal-store adapters, query/evidence
  serialization and asynchronous projection handoff in Rust and Go. Measure
  correctness, recovery, resource use and maintainability before broad language
  commitment; record the reviewed outcome in an ADR.
- **Components / dependencies:** M1 plus M3's local auth design and isolated
  candidate adapters/queues, explicitly identified as trials. This is a
  validation slice, not a choice to build a storage engine or deploy services.
- **Traceability:** REQ-008, 016, 025–029, 035–039; ADR-005, ADR-007,
  ADR-I1–I6, ADR-REC-01–06, ADR-LANG-01, ADR-BENCH-01; TEST-030, 032–038,
  043–045, 051, 054–055 where applicable.
- **Acceptance / validation:** equal semantics, credentials, policies,
  downstream, resource limits and failure scope; crash, replay, visibility,
  fresh/backlog fairness, deadline/residual-work and resource-pressure tests;
  repeated performance trials only after security/correctness gates. Include
  REC-REF-24 as an actual endurance run or explicitly unrun, never infer it
  from a scaled test.
- **Blocking decisions:** approved workload/SLO/cost charter; auth and capture
  trial policy; candidate store/queue and durability/lateness scopes. Record
  candidate substitutions and do not silently promote them to product choices.
- **Status:** the M1 log trial contributes protocol, replay and visibility
  evidence. Product work proceeds on the owner's scoped Rust-first decision.
  Full multi-signal, isolation, endurance and language acceptance remain open.

## M3 — Local identity, authorization and headless management slice

- **Objective / deliverables:** minimal local identity authority and management
  API with secure first-owner bootstrap, one shared subject/action/resource
  model, authenticated service/agent/human access and tenant isolation.
- **Components / dependencies:** M0; supplies a small trial security boundary
  for M2 without requiring a separate binary, UI or hosted control plane.
- **Traceability:** REQ-001, 008–010, 012, 014, 016, 024, 029; ADR-003–005,
  ADR-A6, ADR-I5; TEST-001–006, 022–024, 030, 041–043, 053.
- **Acceptance / validation:** clean bootstrap and concurrent initialization,
  token one-time use and recovery; direct resource/data-access authorization,
  cross-tenant existence isolation, delegation, revocation/fail-closed tests and
  equivalent policy semantics across subject types; documented API-only flows.
- **Blocking decisions:** auth model/engine, tenant identity, policy propagation,
  offline freshness and bootstrap trust root before production exposure.
- **Status:** v0.1 implements local operator bootstrap and recovery, tenant provisioning, scoped service credentials and revocation. Distributed policy and delegated identities remain open.

## M4 — Authorized telemetry admission and raw retrieval

- **Objective / deliverables:** first end-to-end standards-compatible OTLP
  signal slice plus authorized raw retrieval and explicit success, partial,
  rejected, durable and queryable meanings. Grow independent metrics/logs/traces
  paths rather than a single mandatory physical bottleneck.
- **Components / dependencies:** M2 evidence and M3 local authority; candidate
  signal adapter(s), bounded decode/queues, privacy and retention enforcement.
- **Traceability:** REQ-005, 008, 013, 016–019, 025–030; ADR-005–007,
  ADR-A1, ADR-A7–A8, ADR-I1–I5; TEST-004–008, 015, 024, 026, 029–030,
  043, 050–051.
- **Acceptance / validation:** representative OTLP and signal-specific fixtures;
  authentication, source attribution, tenant isolation, limits before decode,
  redaction before platform persistence, correct protocol retry/partial-success
  behavior, direct authorized queries and worker/UI/LLM independence.
- **Blocking decisions:** signal/adapters and backend candidate selection,
  retention/durability, capture/redaction policies and external API contract.
- **Status:** v0.1 implements bounded OTLP/HTTP logs, pre-storage capture policy, a candidate Loki adapter and tenant-authorized raw retrieval. Other signals and recovery qualification remain open.

## M5 — Recovery, ordering and isolation

- **Objective / deliverables:** finite durable buffering and replay under the
  declared failure scope, fair fresh/backlog and tenant scheduling, explicit
  late-arrival semantics and bounded/adaptive admission, with visibility probes.
- **Components / dependencies:** M4 and selected source/store integrations.
- **Traceability:** REQ-029, 035–036, 039; ADR-REC-01–06; TEST-032–037,
  041, 044–045, 051, 054–055 as applicable.
- **Acceptance / validation:** restart, partition, disk/queue full, duplicate,
  reorder, timeout/ambiguous ack, mixed live/backlog/query/alert workloads,
  cancellation/residual-work and original-event-time queries; reconcile unique
  eligible records, pending, rejected and declared loss through query visibility.
  Run the actual 24-hour reference only with an approved and documented profile.
- **Blocking decisions:** buffer failure scope, overflow/retry policies,
  late windows, fairness and resource/SLO charter per adapter.
- **Status:** not started.

## M6 — Evidence and operational context

- **Objective / deliverables:** authorized, bounded raw/semantic evidence and
  versioned context access usable by an external agent; provenance, effective
  coverage and explicit no-match/partial/unavailable distinctions.
- **Components / dependencies:** M3–M5, at least one real metrics and one real
  log/trace adapter; replaceable agent is a client, not an authority.
- **Traceability:** REQ-002–004, 007–008, 015–016, 018, 032–034;
  ADR-010–012, ADR-EVID-01, ADR-CTX-01; TEST-003, 006–008, 011, 014, 039,
  042, 044, 050.
- **Acceptance / validation:** contract fixtures across adapters, budgets and
  cancellation, reauthorization of references and derived artifacts, source
  lineage, stale/contradictory desired-versus-observed context and injection
  boundaries; no evidence or resource-existence leaks across tenants.
- **Blocking decisions:** reviewed public schemas/transport, resource identities,
  evidence retention/declassification and permitted backend query capabilities.
- **Status:** v0.1 implements a provisional external log evidence query with reauthorized record references, time coverage and source limitations. Multi-source semantics and stable contracts remain open.

## M7 — Canonical AI execution and projections

- **Objective / deliverables:** preserve trace/span relationships, explicit run
  and session mappings, immutable finalized observations and asynchronous,
  rebuildable AI projections over canonical telemetry.
- **Components / dependencies:** M4–M6; projection logic may be packaged without
  creating a new service. Built-in investigation workers remain optional.
- **Traceability:** REQ-017, 019–020, 025, 028, 030; ADR-A1–A2, ADR-D1–D3,
  ADR-I3/I6; TEST-007, 015–016, 021, 025, 027, 029, 031, 043, 046.
- **Acceptance / validation:** source-provenance and distributed relationship
  fixtures, late/orphan/retry cases, GenAI convention capability matrix, rebuild
  after crash without resurrecting deleted data, disabled-projection isolation.
- **Blocking decisions:** supported convention versions, mapping/schema,
  completion, attribution and retention rules.
- **Status:** not started.

## M8 — Evaluation and optional investigation workers

- **Objective / deliverables:** separate evaluation, feedback, dataset and
  experiment records; independently deployable asynchronous workers and optional
  investigation runtime, consuming authorized evidence and respecting budgets.
- **Components / dependencies:** M3, M6–M7; Python worker locations exist but
  no framework, model provider, worker queue or execution sandbox is selected.
- **Traceability:** REQ-002–004, 020–025, 031–034; ADR-A3–A7, ADR-D4–D6,
  ADR-AGENT-01; TEST-009, 017–025, 028, 040, 042, 047–049.
- **Acceptance / validation:** retries, revocation and worker outages cannot
  block telemetry; execution/evaluation/feedback histories remain distinct;
  derived content and exports inherit authorization; winning attempt/input
  lineage, correction-safe aggregates and evaluator validity gates are tested.
- **Blocking decisions:** evaluation schema/reproducibility, worker isolation,
  capture/egress and declassification policy, scheduler and persistence.
- **Status:** not started.

## M9 — Declarative operations and release hardening

- **Objective / deliverables:** documented self-hosted deployment and
  declarative management, compatible upgrades, secret-safe configuration and
  operational/benchmark evidence for an agreed release profile.
- **Components / dependencies:** enabled M3–M8 features; integration with
  Terraform/OpenTofu and Kubernetes as chosen for the supported packaging matrix.
- **Traceability:** REQ-001, 006, 009–013, 026, 039; ADR-003–004, ADR-008–009,
  ADR-BENCH-01; TEST-001–004, 010, 012–013, 026, 037, 041, 052–055.
- **Acceptance / validation:** clean install/upgrade/restore without UI or hosted
  dependencies, credential canaries in state/plan/logs, workload-specific
  baseline benchmarks and fault matrix under declared resources and policies.
- **Blocking decisions:** license, packaging/upgrade matrix, workload/SLO/cost
  charter, secret lifecycle and release feature scope.
- **Status:** not started.

## M10 — Optional presentation

- **Objective / deliverables:** optional independently deployed UI using only
  documented, authorized backend APIs; external clients retain equal access.
- **Components / dependencies:** relevant headless APIs from M3–M9. No UI code
  or framework is chosen by M0.
- **Traceability:** REQ-009–010, 014; ADR-002–003; TEST-002, 013, 041.
- **Acceptance / validation:** stop, replace and independently upgrade UI while
  backend operations continue; compare supported operations via external client;
  verify UI never becomes an authorization authority.
- **Blocking decisions:** UI framework and supported workflows when reached.
- **Status:** not started.
