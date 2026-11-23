# AI-Native Observability Platform

### Architecture Requirements Boundaries Evidence and Unified Observability v0.3

Document status: Living implementation specification v0.3
Last updated: 2026-11-22
Project stage: Architecture definition; no storage engine, framework, or final API schema selected
Intended readers: Project owner, implementation sessions, maintainers, reviewers, contributors

**v0.3 update:** The earlier Evidence Contract responsibilities and principles are now CONFIRMED; detailed schemas remain PROPOSED or DEFERRED. Approved A1–A8 extend the architecture in Section 17. The next proposed decision is the execution and evaluation lifecycle contract in Section 19. Section 20 records all status changes.

**Navigation:** [Requirements](#2-requirements-register) · [Decision register](#11-architecture-decisions-register) · [Acceptance tests](#13-implementation-acceptance-tests-and-anti-drift-guardrails) · [Approved A1–A8](#17-unified-traditional-and-ai-agent-observability) · [Added open decisions](#182-added-deferred-decisions) · [Next decision](#19-next-recommended-architecture-decision) · [Change record](#20-version-history-and-preservation-record)

> Authority and change control
> This document is the project-level source of truth for agreed architecture and scope. It separates CONFIRMED decisions from PROPOSED / NOT YET APPROVED designs and DEFERRED implementation choices. An implementation session MUST NOT silently promote a proposed or deferred item to a confirmed requirement. Resolve only the decisions that block the task being implemented; ask the project owner at that point. If an implementation conflicts with a confirmed decision, stop and propose an Architecture Decision Record (ADR) instead of making the change implicitly.

## 0. How to use this document in implementation sessions

1. Read Sections 1–5 before starting architecture-affecting work. Read the relevant subsystem section and the acceptance tests in Section 13. For agent observability/evaluation, also read Sections 17–20.

1. Follow requirements marked CONFIRMED and with IDs REQ-* or decisions ADR-*.

1. Treat items marked PROPOSED as design starting points, not settled external APIs or mandatory technologies.

1. Treat DEFERRED choices as deliberately unresolved. When a specific implementation task truly needs the choice, offer a small number of options, trade-offs, and a recommended default; obtain the owner's decision before locking it in.

1. Do not ask the owner to resolve every deferred item in advance. Do not invent decisions to avoid asking later.

1. Add new architectural decisions to the decision register, with rationale and affected requirements. Keep the document versioned.

1. Preserve API-only operation and authorization invariants even in a local prototype.

1. Do not turn an illustrative code snippet, JSON object, technology example, benchmark number, or proposed service name into a hard requirement without an explicit decision.

Requirement language: MUST = required for architectural compliance; SHOULD = expected unless a documented, justified exception is approved; MAY = optional. Status takes precedence: a PROPOSED MUST describes the proposed design, not an approved binding requirement.

Decision statuses: CONFIRMED (explicit user agreement or directly established project constraint); PROPOSED (architecture recommendation under discussion); DEFERRED (not yet decided); REFERENCE (example from another project, not a platform requirement).

## 1. Product vision and scope

### 1.1 Vision — CONFIRMED

Create a pure open-source, AI-agent-native observability platform. AI agents are first-class consumers of observability and operational data; the platform should help them investigate what happened, understand service and infrastructure context, test hypotheses, and provide prompt, accurate, evidence-backed insights. The platform must support extensive agent customization and company/project-specific context. It must also retain strong conventional observability operations and deployment/IaC support.

Concise position: Machine-first investigation over reliable, modular observability infrastructure; human interfaces are optional clients, not the backend's reason to exist.

### 1.2 Success criteria — CONFIRMED direction; quantitative thresholds DEFERRED

- An organization can run the complete core backend without deploying the project's default UI.

- Organizations can build their own UI or integrate their own agent using versioned, documented APIs.

- Observability ingestion, storage, raw queries, and conventional alerting remain useful when all LLM/agent components are unavailable.

- Agents can retrieve telemetry and operational context through a coherent contract without being coupled to a single storage engine.

- Every high-level conclusion can be linked to the evidence used, with explicit uncertainty and completeness limits.

- The product supports self-hosted deployment and standard IaC/DevOps workflows.

- Traditional metrics/logs/traces performance is comparable with established products for representative target workloads; it is not required to outperform all competitors.

- All core OSS capabilities remain available without an externally hosted control plane.

### 1.3 Explicit non-goals for the initial architecture — CONFIRMED direction

- Do not select a final metrics, logs, or traces backend prematurely.

- Do not require one physical database for all telemetry types.

- Do not require one particular LLM, agent framework, UI, or language.

- Do not make the AI runtime responsible for ordinary telemetry ingestion or routine alert evaluation.

- Do not require a commercial or hosted service; possible future enterprise offerings are optional and should not compromise OSS independence.

- Do not treat architectural examples from OpenObserve, Grafana, VictoriaMetrics, Braintrust, and other projects as prescribed implementations or exclusively as competitors. They are reference designs.

## 2. Requirements register

REQ-001–REQ-018 are retained from v0.2. Only the explicitly documented status updates below change their approval state. REQ-019–REQ-026 add approved A1–A8.

The REQ IDs are stable references for future tasks. If a requirement changes, change its status in an ADR rather than silently reinterpreting it.

| ID | Requirement | Status | Acceptance implication |
| --- | --- | --- | --- |
| REQ-001 | Pure-OSS-first architecture; complete self-hostable core | CONFIRMED | No hosted service needed for core operation |
| REQ-002 | Agent-native investigation rather than AI bolted onto a dashboard | CONFIRMED | Agent access/context/evidence are first-class APIs |
| REQ-003 | Customizable agents, including organization-owned runtimes | CONFIRMED | Evidence APIs remain usable without built-in agent |
| REQ-004 | Company-specific operational context (services, runbooks, deployments, IaC, changes) | CONFIRMED | Context has defined retrieval and update boundaries |
| REQ-005 | High-quality traditional metrics, logs, traces foundation | CONFIRMED | Separate workload-specific performance evaluation |
| REQ-006 | Comparable, not universally superior, performance versus mature systems | CONFIRMED | Controlled workload benchmarks determine fitness |
| REQ-007 | Acceptable cost/retention/completeness trade-off, but agent must not overclaim missing data | CONFIRMED | Preserve limitations and sampling/retention signals |
| REQ-008 | Unified authorization model for humans, service accounts, and AI agents | CONFIRMED | Same resource/action model; backend enforcement |
| REQ-009 | UI and backend independently deployable; UI optional | CONFIRMED | Backend-only conformance tests |
| REQ-010 | Custom company UIs via documented backend API | CONFIRMED | No essential UI-only operations |
| REQ-011 | Conventional Terraform/OpenTofu, IaC, Kubernetes, and deployment workflows retained | CONFIRMED | Declarative management and deployment supported |
| REQ-012 | Data plane may reside in customer infrastructure; future hybrid option should remain possible | CONFIRMED as design flexibility; not a release feature | No mandatory external control-plane dependency |
| REQ-013 | Core telemetry and alerting independent of LLM availability | CONFIRMED | Outage isolation tests |
| REQ-014 | Logical separation between Telemetry, Intelligence, Management/Security and Presentation | CONFIRMED as working boundary | Well-defined ownership and interfaces |
| REQ-015 | Agent can access underlying evidence, not only opaque generated summaries | CONFIRMED direction | Source query, references, completeness available |
| REQ-016 | Authorization must be enforced on actual data access, not only a UI or tool list | CONFIRMED | Query-scope and backend access checks |
| REQ-017 | No high-cardinality IDs as ordinary metric labels | CONFIRMED direction in v0.3 under A5; exact limits DEFERRED | Trace/run IDs through traces, logs, exemplars |
| REQ-018 | Canonical evidence envelope and semantic queries | CONFIRMED contract responsibilities and six principles in v0.3; schemas and specific semantic operations PROPOSED/DEFERRED | Sections 6–9; review exact interfaces before public API freeze |
| REQ-019 | Unified traditional and AI-agent telemetry with canonical correlation | CONFIRMED in v0.3 (A1) | Correlate application, agent, model and tool operations without a separate incompatible trace tree |
| REQ-020 | First-class agent observations, runs and sessions over canonical telemetry | CONFIRMED in v0.3 (A2) | Preserve source identity/provenance and relationships in AI projections |
| REQ-021 | First-class evaluation, feedback, datasets and experiment records | CONFIRMED in v0.3 (A3) | Attach later/versioned judgments without rewriting execution history |
| REQ-022 | Independent asynchronous evaluation service and workers | CONFIRMED in v0.3 (A4) | Evaluation outages/backlogs do not block production responses or telemetry |
| REQ-023 | Separate operational metrics from evaluation-derived quality metrics | CONFIRMED in v0.3 (A5) | Retain canonical evaluation records and bounded metric cardinality |
| REQ-024 | Shared authorization and complete headless evaluation APIs | CONFIRMED in v0.3 (A6) | Use evaluation with external agents and without default UI or hosted control |
| REQ-025 | Policy-controlled content capture, privacy and provenance | CONFIRMED in v0.3 (A7) | Protect payload access/egress and surface unavailable evidence |
| REQ-026 | Physical storage selection remains deferred | CONFIRMED in v0.3 (A8) | Select engines/layouts only after workload and operational evaluation |

## 3. Architecture boundaries

### 3.1 Four logical planes — CONFIRMED working architecture

```
                   OPTIONAL CLIENTS / PRESENTATION PLANE
     OSS UI       Company UI        CLI/SDK/Terraform     External agents
          \              |               |                  /
                  Versioned, authenticated APIs
                                |
                 MANAGEMENT / SECURITY PLANE
       local management API | tenant & config authority | policy
                                |
                 INTELLIGENCE / INVESTIGATION PLANE
     evidence gateway | context | optional agent workers | evaluation
                                |
                   TELEMETRY DATA PLANE
         ingest/route | metrics | logs | traces | raw query | alerts
                                ^
                        collectors / sources
```

The boundaries are logical service responsibilities, not necessarily four containers or clusters. Services can be combined for local installation if the required APIs, security, and UI independence remain intact.

#### Plane 1: Telemetry Data Plane — CONFIRMED

Owns: receivers, write path, buffering/WAL if applicable, indexing, storage, retention, raw query execution, backend-specific execution, deterministic alert/SLO evaluation, metrics/logs/traces availability and health.

Does not own: autonomous reasoning, organization-specific diagnosis policy, prompt management logic, evaluation of agent conclusions, UI session state.

Always-on principle: telemetry ingest and routine monitoring must not depend on LLM, agent worker, or presentation uptime.

Performance principle: metrics, logs, and traces may use different internal engines, physical formats, indexing strategies and scaling methods.

#### Plane 2: Intelligence & Investigation Plane — CONFIRMED ownership; interface details PROPOSED

Owns: agent-facing Evidence Gateway, semantic retrieval/correlation, operational-context retrieval, investigation orchestration, hypothesis/evidence tracking, persisted investigation results, agent evaluations, optional built-in agent workers.

Does not own: raw telemetry databases or privileged backend credentials exposed to users/agents, identity authority, underlying deployment orchestration.

Deployability: Evidence Gateway must be usable without running the project's own agent. Built-in agent workers must be independently scalable and disable-able. The exact number of services is deferred.

Important distinction: context source configuration and authorization are management concerns; context facts and their retrieval, versions, evidence and freshness are intelligence/data concerns.

#### Plane 3: Management & Security Plane — CONFIRMED

Owns: management API, tenant and resource configuration, identity integration, service accounts, policy definitions, agent permissions and budgets, configuration lifecycle, data-source registrations and credentials, audit policy, declarative configuration APIs.

Security authority: an independently usable customer-hosted local management/policy authority must be available. A future remote management UI is optional and may be delegated bounded management responsibilities, but must not be necessary for core OSS operation.

Distributed enforcement: the shared policy model is enforced at every sensitive service boundary (API, data query gateway, tool gateway, action executor, ingestion interfaces), not merely by a front-end permission check.

#### Plane 4: Presentation Plane — CONFIRMED

Owns: optional default OSS UI, presentation, custom UI adapters, CLI interaction and customer/hosted web interfaces. A CLI or Terraform provider calls management APIs; it is not the source of authorization truth.

Does not own: business-critical state or privileged permission decisions. UI deployment, upgrades, outage, or deletion must not interrupt core backend operation.

### 3.2 Trust boundary distinct from logical boundary — CONFIRMED

By default the customer-controlled trust boundary contains sensitive telemetry, company/project context, investigation artifacts, authorization enforcement, production credentials, and privileged agent execution. Separate deployment units inside that boundary are permitted.

A future hosted UI or management experience may communicate through explicitly authenticated and authorized APIs; it must not require a remote vendor service to approve every read or ingest. External model calls are data egress and need explicit administrator configuration, redaction, and permitted destination policy. A hosted UI alone cannot reach a private VPC endpoint without an approved connectivity design.

### 3.3 Failure-boundary expectations — CONFIRMED direction

| Unavailable component | MUST continue (given healthy dependencies) | Degraded functions |
| --- | --- | --- |
| Default UI | Ingestion, raw queries, alerts, backend admin APIs, enabled agent jobs | Default web experience |
| External hosted UI/control | Complete self-hosted OSS operation | Remote convenience features |
| Agent workers or LLM | Telemetry ingest/query, ordinary alerts, management | New AI investigations |
| Evidence Gateway | Raw data read/write and ordinary alerts | Semantic evidence and agent tool APIs |
| Management API transient outage | Existing telemetry pipelines and valid locally authorized operations within explicit freshness policy | Configuration changes; certain new permissions |
| Individual metrics/logs/traces backend | Unaffected signal backends | Queries requiring failed signal; report partiality |
| Query compute exhaustion | Ingestion SHOULD remain protected through workload isolation | Some queries may be throttled |

Caveat: These are design targets. Credential expiry, revocation and policy-cache rules must be specified before production security claims are made.

## 4. Deployment and API boundaries

### 4.1 Supported deployment modes — CONFIRMED direction

| Mode | Required components | UI | Notes |
| --- | --- | --- | --- |
| A. Local all-in-one OSS | Local backend + management + optional packaged intelligence | Included for convenience | Packaging convenience, not architectural coupling |
| B. Headless OSS | All necessary backend services and local management/API | Absent | Fully supported, complete functional surface |
| C. Split self-hosted OSS | Backend and default/custom UI independently deployed | Separate | Independent upgrades within compatibility policy |
| D. Future optional hybrid | Customer-hosted data and execution; remote UI/optional management | Potentially hosted | Not an initial-release requirement |

The built-in agent runtime is optional in headless mode if external agents can use the evidence APIs. The minimum services needed to support each feature are to be documented, not guessed.

### 4.2 API families — CONFIRMED conceptual boundary; versions/transport DEFERRED

- Telemetry API: standards-compatible ingestion and authorized raw query/retrieval for metrics, logs, traces.

- Intelligence API: semantic evidence queries, context access, investigation execution/status/history, evaluations and evidence references.

- Management API: tenants, permissions, service accounts, data sources, collection/retention policies, agent configurations, alert rules, service definitions, budgets and audit.

Invariants:

- All default UI functionality MUST be realizable through documented APIs.

- Public APIs MUST enforce authorization at the actual resource and data scope.

- APIs MUST be versioned with compatibility/deprecation rules before stable release.

- No default UI-only privileged or undocumented path is permitted.

- Custom UIs SHOULD not need direct access to database ports.

- Streaming, pagination, cancellation, async job state and rate-limit semantics are required where relevant; exact wire transport is DEFERRED.

- Raw telemetry and semantic evidence APIs must not leak service names, resource existence, or query metadata across unauthorized tenants.

### 4.3 IaC and operations — CONFIRMED

Two distinct forms of declarative support:

1. Deploy platform infrastructure: Kubernetes manifests/Helm, Terraform/OpenTofu, local install and repeatable upgrades; exact maintained packaging matrix is DEFERRED.

1. Manage platform resources as code: users, roles/policies, agents, data sources, alerts, service context, collection/retention and query budgets through versioned management APIs and declarative providers.

A third investigative capability, read-only interpretation of customer IaC/deployments, is part of the operational context goal. Actual privileged remediation is not presumed and must use explicit task-based authorization and approvals.

## 5. Traditional observability performance — SEPARATE WORKSTREAM

Confirmed principle: storage/backends are not selected because of agent architecture alone. The telemetry foundation must stand on its own.

### 5.1 Reference architectures (not mandatory choices)

- VictoriaMetrics: specialized time-series indexing and ingestion; vminsert, vmstorage, vmselect in cluster deployments.

- OpenObserve: mixed telemetry ingestion, WAL, Parquet/object-store oriented analytics and distributed query.

- Grafana ecosystem: Mimir (metrics), Loki (logs), Tempo (traces), Grafana presentation/application server; independently deployed data engines.

- Other useful references: ClickHouse/ClickStack, Jaeger, Prometheus, OpenTelemetry Collector.

No decision is made yet that VictoriaMetrics is universally faster than OpenObserve, or OpenObserve is universally best for logs/traces. Architecture and benchmark results are workload-dependent. Compare actual sustained throughput under simultaneous ingestion and query, not only vendor-reported isolated query latency.

### 5.2 Benchmark requirements — CONFIRMED direction

Separate metrics, logs and traces tests before mixed-workload tests. Measure at least:

| Signal | Ingestion | Query/read | Efficiency/reliability |
| --- | --- | --- | --- |
| Metrics | samples/sec, active series, series creation/churn | latency by query type, concurrent query load | CPU/sample, memory, durability, lag, cardinality impact |
| Logs | records/sec, bytes/sec, indexing cost | text search, structured filtering, grouping/patterns | storage footprint, compression, compaction, tail latency |
| Traces | spans/sec, trace completeness | ID lookup, attribute search, critical path | storage, sampling effects, latency, ingest impact |
| All | simultaneous ingestion + representative queries | P50/P95/P99 | recovery, resource isolation, cost, tenant isolation |

Benchmark acceptance target: Comparable to established engines on explicitly chosen representative scenarios. Quantitative SLOs, hardware, datasets, cost ceiling, test methodology and selected baselines are DEFERRED until benchmark planning. This is NOT a requirement to dominate every benchmark.

### 5.3 Known trade-offs — CONFIRMED as acceptable

- Indexing work versus flexible arbitrary search: accepted; benchmark after implementation.

- Sampling, aggregation, finite retention versus perfect completeness: accepted; expose the resulting uncertainty.

- Agent flexibility versus security: not a trade-off permitting weakened authorization; enforce the shared policy model.

- Additional agent query demand versus ingest stability: must be mitigated by budgets, priorities and resource isolation.

## 6. Canonical data and evidence contract

**Status: CONFIRMED responsibilities and principles in v0.3 following the owner's explicit agreement with the Evidence Contract.** The approved responsibilities are Identity and Correlation, Evidence Query, and Evidence Result. The six approved principles are common identity, precise time semantics, evidence provenance, data completeness, consistent authorization, and backend independence. Detailed field definitions, JSON structures, exact operations, enum names and wire schemas below remain PROPOSED or DEFERRED and are not API-stable.

### 6.1 Why it exists

- Different backends use PromQL/MetricsQL, SQL/log search and trace search APIs; they should not dictate agent reasoning contracts.

- Agents need bounded, relevant, provenance-rich evidence, not unbounded raw telemetry.

- Custom agents should call the same stable operations as the built-in agent.

- A finding must distinguish no match from unavailable, expired, sampled, truncated, or not authorized.

- The contract must preserve sufficient information to reproduce or audit queries.

### 6.2 Separate three contracts — CONFIRMED responsibilities with draft representations

1. Signal/Identity Normalization Contract: identity/time/correlation mapping from native OTLP/Prometheus/backends into the platform's canonical context.

1. Evidence Query Contract: authorized query requests, budgets, filters, consistency and execution metadata.

1. Evidence Result Contract: results, references, source provenance, completeness, query costs and structured machine-readable errors.

Do not force all logs, metric samples and spans into one identical physical record. The canonical format is an envelope around typed signal payloads and evidence references, not a replacement for OTLP or backend-native storage.

## 7. Identity time and correlation contracts

**CONFIRMED principles; PROPOSED detailed representations.** Identity, timing, correlation and provenance responsibilities are approved. The field names, mandatory/optional constraints, alias rules and serialization details below remain draft. The metric-cardinality direction is confirmed by A5.

### 7.1 Canonical identity

Required concepts (proposed): tenant_ref (authorization boundary), service_ref (durable logical identifier), environment_ref, source_ref, and optional deployment_ref, cluster_ref, incident_ref, agent_run_ref, trace_id, span_id.

Map OTel resource attributes such as service.name, service.namespace, and deployment.environment.name into these identities without discarding their original names. Do not assume service.name alone is globally unique, or that the OTel service.instance.id is a durable logical service ID. Maintain aliases and a reconciliation policy for renamed or duplicate services.

Metric cardinality direction (CONFIRMED by A5; exact label rules/limits DEFERRED): Do not insert unique trace, user, request, incident or agent-run IDs as normal metrics labels. Correlate by bounded service/environment labels, trace/log metadata, exemplars where supported, and evidence references.

Trace context: Preserve W3C trace context across service/agent/tool boundaries where instrumented. OTel trace IDs are 16-byte identifiers, span IDs 8-byte; keep exact canonical lowercase hex for cross-backend referencing. Do not infer common ancestry solely from similar timestamps.

### 7.2 Time semantics

Canonical time ranges are in UTC, with explicit units and well-defined inclusivity (recommended [start, end)), but exact wire serialization is DEFERRED pending API implementation.

Distinguish:

- event_time: source event occurrence time; for traces the relevant span start/end or events.

- observed_time: when a collector first observed the record if available.

- ingested_time: when the backend accepted/persisted it if available.

- retrieved_at: when the Evidence Gateway performed the retrieval.

- effective_time_range: the time interval actually covered by returned data, which may differ from the request.

Preserve metric resolution, aggregation window, rollup/downsampling and counter semantics. Do not infer causality from coincident peaks alone; clock skew and delayed ingestion must be representable.

### 7.3 Correlation

Correlation must support, where available:

- trace_id/span_id for trace-to-log and distributed-trace linkage;

- bounded logical service/environment/cluster identities for metrics-to-service joins;

- exemplars, if supported, for metric-point-to-trace links;

- deployment revision, release, Git commit, IaC change and incident IDs for operational context;

- agent run/session IDs for linking tool execution to evidence and evaluations.

All correlations should report how the link was established (exact_trace_id, exemplar, service_time_window, deployment_mapping, inferred) and whether it is authoritative or heuristic. Never silently imply an exact join where only a temporal or name-based association exists.

## 8. Evidence Query and Result Contract

**CONFIRMED responsibilities; PROPOSED detailed API design.** Authorized bounded retrieval, provenance, completeness and backend independence are approved. Tool groupings/names, field lists, result enums, numeric limits and example JSON below remain proposals or deferred choices.

### 8.1 Query API model

Two levels, both with identical backend authorization semantics:

Raw access tools: query_metrics, search_logs, get_trace, search_traces, get_service_context, get_deployments (actual names DEFERRED). Provide expert access with validated filters/parameters and backend-specific escape hatches, subject to policy and cost limits.

Semantic tools: service_health, compare_before_after_change, find_new_error_patterns, find_representative_failed_traces, investigate_dependency, retrieve_incident_evidence (actual names DEFERRED). These may call multiple raw tools and must return underlying source references and aggregation logic.

Execution limits: Each query request must support a server-enforced tenant scope, time range, bounded result size, timeout/deadline and (as needed) rate/concurrency budgets. Concrete numeric defaults and quota algorithms are DEFERRED. LLM-supplied filters are not authority; the server derives tenant and resource scope from verified identity and policy.

### 8.2 Proposed minimum Evidence Envelope fields

| Group | Candidate fields | Why |
| --- | --- | --- |
| Envelope | schema_version, evidence_id, kind, created_at | Evolution and durable addressing |
| Authority | tenant_ref, subject_ref, policy_version or opaque decision reference | Audit without disclosing privileged internals |
| Entity | service_ref, environment_ref, deployment_ref, trace_id (optional) | Cross-system correlation |
| Time | requested and effective ranges, retrieved_at | Distinguish stale/partial data |
| Query | operation, sanitized parameters, fingerprint, backend adapter ID | Reproducibility and troubleshooting |
| Payload | typed metric/log/trace/context result or summary | Avoid one-size-fits-all raw schema |
| Provenance | source system, reference/locator, query or transform lineage | Verify claims against source |
| Completeness | coverage, status, limitations, sampling/retention/truncation metadata | Calibrate conclusions |
| Diagnostics | execution duration, limits, warnings, continuation cursor where relevant | Efficient investigation and safe pagination |

Sensitive fields: authorization scopes and backend credentials must never be returned as secrets. Query fingerprints/logging must redact credentials and confidential input. Source references must be tenant-scoped and reauthorized when dereferenced.

### 8.3 Completeness semantics

Proposed top-level result outcomes (exact enum DEFERRED):

- OK: query completed; result may contain matching data.

- NO_MATCH: query completed successfully and no matching records were found within verified coverage; not proof that the real-world event never happened.

- PARTIAL: some records/sources/time ranges missing, sampling/truncation/timeout or aggregation may materially limit inference.

- UNAVAILABLE: source or required query unavailable.

- DENIED: caller not authorized; response must not reveal unauthorized resource existence.

- ERROR: failure not safely categorizable as a successful negative result.

Coverage reasons may include sampled, retention_expired, not_collected, late_arrival, truncated, backend_timeout, adapter_limitation, source_unavailable, unknown, as available. The system must not invent a sampling rate or a completeness guarantee it cannot verify.

Required reasoning behavior: A consumer must be able to tell the difference between "no matching records in available, retained and accessible data" and "there was no failure." Unknown completeness stays unknown. An evidence result can be syntactically valid yet inappropriate to support a causal conclusion.

### 8.4 Example envelope — ILLUSTRATIVE ONLY, NOT AN API SPECIFICATION

```
{
  "schema_version": "draft-0.1",
  "evidence_id": "ev_01EXAMPLE",
  "kind": "metric_comparison",
  "subject": {
    "tenant_ref": "tenant-example",
    "service_ref": "checkout-api",
    "environment_ref": "production"
  },
  "time": {
    "requested_start": "2026-11-22T10:00:00Z",
    "requested_end": "2026-11-22T10:20:00Z",
    "retrieved_at": "2026-11-22T10:21:00Z"
  },
  "query": {
    "operation": "compare_before_after_change",
    "backend_refs": ["metrics-primary"],
    "fingerprint": "opaque-normalized-query-hash"
  },
  "result": {
    "summary": "p95 latency increased after the deployment",
    "measurements": [
      {"name": "p95_latency", "before_ms": 180, "after_ms": 510}
    ]
  },
  "provenance": [{
    "source_ref": "src_metric_query_01",
    "reference": "tenant-scoped-opaque-evidence-handle",
    "correlation_method": "service_time_window"
  }],
  "completeness": {
    "outcome": "PARTIAL",
    "limitations": ["trace_sampling_unknown"],
    "coverage_known": false
  }
}
```

All values are fictional. The numbers, IDs, field names and structure are a design exploration. Do not copy this directly into a stable public API without explicit review.

### 8.5 Evidence references and reproduction

Each source reference should identify an authorized method to retrieve or verify the evidence, without embedding raw sensitive data by default. Evidence may be immutable snapshots or reproducible queries; these have different guarantees:

- Snapshot: retained excerpt or aggregate tied to a content hash/version and collection time. Better reproducibility, additional storage/custody.

- Query reference: sanitized query and time window executed against a backend. Lower duplication, but results may change due to late data, retention, reindexing, and rollups.

Proposal: support both concepts and record which type was used; decide exact persistence policy, hashing, TTL and reference URI scheme when implementing evidence storage.

No agent output should claim "verified" solely because the source was queried; verification requires checking the claim-to-evidence relationship.

## 9. Authorization, security and privacy invariants

### 9.1 One identity/authorization model — CONFIRMED

- Subject types: human users, service accounts, standalone agents, delegated agents. Agent executions have their own auditable subject identity.

- Permission concepts: subject, action, resource, tenant, environment and optional task conditions. Exact policy DSL/model DEFERRED.

- Delegated agent rights must be bounded by user rights ∩ agent rights ∩ task scope; autonomous agents use their own service identity and task scopes.

- Policy definitions are managed centrally in the customer-hosted management authority; enforcement is distributed to gateway and services.

- Backend query adapters must scope actual underlying queries, not only approve the generic name of the tool.

- Default deny where valid permissions cannot be established; policy caching, offline allowances and revocation windows require explicit definition before release.

- Never trust client-provided tenant IDs as authority; backend credentials are never delegated directly to untrusted agents or custom UIs.

- Destructive changes/remediation require explicit, independently enforced permissions and (when configured) approval gates.

### 9.2 Trust-boundary egress — CONFIRMED principle, implementation DEFERRED

- The customer owns sensitive telemetry and company context in self-hosted mode.

- LLM prompts, retrieved logs, tool inputs/outputs, runbooks and incident history can all contain sensitive information.

- External model use must be explicitly configured; redaction/classification and destination policies must execute before egress.

- Local/on-prem model integration must remain possible; no exact inference server or vendor selected.

- Hosted presentation/management is optional; private endpoint access must be explicitly designed, not assumed from CORS.

- All sensitive retrieval and privileged tool operations should be audit logged, with retention/redaction policies.

### 9.3 Prompt-injection boundary — PROPOSED mandatory safeguard

Logs, incident tickets, runbooks, Git files, and tool outputs are untrusted data for an LLM. These contents must never grant permissions or override system/operator policies. The execution gateway must enforce capability rules independently of model reasoning.

## 10. Agent-native system behavior

### 10.1 Principle — CONFIRMED

An agent is a first-class platform client, not a hidden privileged component or a model hardcoded into the UI.

Proposed investigation lifecycle: trigger → resolve service/context → form hypotheses → query evidence → test alternatives → qualify missing evidence → produce findings with references → request human approval where appropriate → record feedback/outcome.

The platform should let users substitute their own agent framework/runtime without changing telemetry storage or permission semantics. MCP is an optional integration interface, not the platform's entire internal design.

### 10.2 Agent state and evaluation — CONFIRMED direction; schemas DEFERRED

Preserve, subject to configured retention and permissions:

- investigation ID and task scope;

- model/agent/instruction/tool versions;

- relevant company-context versions;

- tool calls, bounded query details and result references;

- hypotheses and alternative explanations;

- evidence-to-claim relationships and known missing evidence;

- final report, user feedback, eventual confirmed outcome if available;

- cost/latency/query counts for self-observability and evaluation.

The platform should not equate fluent model output with correctness. Evidence provenance supports auditing, not automatic proof of root cause. Human feedback must not be blindly promoted into universally applicable rules.

## 11. Architecture decisions register

| ADR | Decision | Status | Why / consequences |
| --- | --- | --- | --- |
| ADR-001 | Agent-native, OSS-first product; AI agents first-class clients | CONFIRMED | Differentiates primary workflows, not dependent on commercial edition |
| ADR-002 | Four logical planes: Telemetry, Intelligence, Management/Security, Presentation | CONFIRMED working design | Clear ownership and independence; does not force four binaries |
| ADR-003 | Backend-only, UI-optional and custom-UI-capable deployments | CONFIRMED | Complete documented APIs; UI not on critical path |
| ADR-004 | Customer-controlled data and local authority; future hosted UI optional | CONFIRMED | No hosted core dependency; opt-in egress and connectivity |
| ADR-005 | One authZ model for users, services, agents with enforcement everywhere | CONFIRMED | Strong multi-tenant isolation and delegated task scopes |
| ADR-006 | Conventional telemetry and alerting not LLM-dependent | CONFIRMED | Outage isolation and deterministic operations |
| ADR-007 | Storage optimized per metrics/logs/traces workload, no engine preselected | CONFIRMED | Backend choice by benchmark, not platform narrative |
| ADR-008 | Comparable performance target; acceptable indexing/retention trade-offs | CONFIRMED | Workload-based validation and visible partial data |
| ADR-009 | Terraform/OpenTofu and modern deployment/IaC workflows remain | CONFIRMED | Two kinds of IaC: platform deploy + configuration |
| ADR-010 | Canonical identity/time/correlation and Evidence Envelope | CONFIRMED responsibilities and six principles in v0.3 | Sections 6–8; exact schema/interface still requires review |
| ADR-011 | Semantic Evidence Gateway independent from agent worker | CONFIRMED independence; detailed interface PROPOSED | Enables BYO agents, bounded source queries; consistent with A6 |
| ADR-012 | Typed raw + semantic query APIs; references, completeness and budgets | CONFIRMED bounded retrieval/result principles; raw/semantic tool design PROPOSED | Preserve source references, authorization and completeness; exact operations and limits DEFERRED |
| ADR-013 | Exact storage engines, programming languages, framework | DEFERRED | Choose per measured requirements |
| ADR-014 | Exact authZ engine (OPA/OpenFGA/custom) and policy syntax | DEFERRED | Shared model/invariants already decided |
| ADR-015 | Exact OSS license | DEFERRED | Remain fully OSS; review dependencies |
| ADR-016 | Exact hybrid hosting/security connectivity | DEFERRED | Future capability, not v1 requirement |
| ADR-A1 | Unified traditional and AI-agent telemetry with canonical correlation | CONFIRMED in v0.3 | REQ-019; Section 17 |
| ADR-A2 | First-class agent observations, runs and sessions over canonical telemetry | CONFIRMED in v0.3 | REQ-020; Section 17 |
| ADR-A3 | First-class evaluation, feedback, datasets and experiment records | CONFIRMED in v0.3 | REQ-021; Section 17 |
| ADR-A4 | Independent asynchronous evaluation service and workers | CONFIRMED in v0.3 | REQ-022; Section 17 |
| ADR-A5 | Separate operational metrics from evaluation-derived quality metrics | CONFIRMED in v0.3 | REQ-023; Section 17 |
| ADR-A6 | Shared authorization and complete headless evaluation APIs | CONFIRMED in v0.3 | REQ-024; Section 17 |
| ADR-A7 | Policy-controlled content capture, privacy and provenance | CONFIRMED in v0.3 | REQ-025; Section 17 |
| ADR-A8 | Physical storage selection remains deferred | CONFIRMED in v0.3 | REQ-026; Section 17 |

## 12. Deferred decisions — ask only when implementation reaches them

All original deferred topics below remain unresolved. Section 18.2 adds the observation/evaluation topics introduced in v0.3; no engine, transport, schema, policy syntax or numeric setting is selected by approving the conceptual architecture.

| Topic | Specifically unresolved | Trigger for asking owner |
| --- | --- | --- |
| Storage | Engine(s), retention, indexes, compression, object-store/local tiers | Building data plane / running comparative benchmarks |
| Performance | Target workload profile, SLOs, comparable baselines and thresholds | Benchmark harness and release performance gate |
| API transport | REST vs gRPC, GraphQL, streaming transport, exact public resource paths | API server and first external client |
| Evidence schema | Field names, required/optional constraints, versioning, source handle format, data model | Before committing public API / persistence migrations |
| Tenant/service identity | ID format, namespace semantics, alias resolution, environments and cluster scopes | Tenant creation and context resolver |
| Time and consistency | Time precision, inclusive/exclusive bounds, cache, late data semantics | Query gateway schema and test fixtures |
| Evidence retention | Query vs snapshot, evidence TTL, hashes, exports, redaction | Persisted investigation records |
| AuthZ engine | OPA vs OpenFGA vs others, policy language and propagation | Security plane implementation |
| AuthZ operations | Cache freshness, revocation, emergencies, cross-region access, auditing detail | Production multi-tenant/security design |
| Agent execution | Agent SDK/framework, model SDKs, worker queue, state/checkpoint store | Built-in worker implementation |
| Egress | Model-provider redaction, LLM hosting, confidentiality classes, key management | External provider integration |
| Deployment | Compose/Helm/operator/Terraform module matrix, packaging and upgrade policy | Deployment pipeline |
| UI | Framework, default workflows, design system | Optional UI implementation |
| OSS | License, source contribution process, code and dependency obligations | Public repo release |
| Hybrid | Remote management model, proxy/tunnel, network/auth handshakes | Only if future hosted edition is pursued |

Rule: Do not pre-emptively resolve a deferred item in another implementation session without documenting the user's authorization and a new ADR.

## 13. Implementation acceptance tests and anti-drift guardrails

### 13.1 Cross-cutting acceptance tests — CONFIRMED expectations

| ID | Test | Pass criterion |
| --- | --- | --- |
| TEST-001 | Headless install | Backend ingest, query, alerts, management operations without default UI |
| TEST-002 | Custom UI | Independent client can perform supported user-facing operations through published APIs |
| TEST-003 | Own agent | External agent retrieves authorized context/evidence without built-in agent runtime |
| TEST-004 | LLM outage | Metrics/logs/traces ingestion, query, routine alerting remain available |
| TEST-005 | Policy equivalence | Human, service account and agent actions use same resource/action authorization semantics |
| TEST-006 | Tenant isolation | Unauthorized principal cannot query, infer existence, or fetch references for other tenant data |
| TEST-007 | Context/trace correlation | Correct links where IDs exist; heuristic links explicitly labeled |
| TEST-008 | Sampling and retention | Missing data cannot be reported as complete coverage without proof |
| TEST-009 | Model/tool audit | Investigation record preserves source evidence, versions and permitted actions |
| TEST-010 | IaC | Deployment and management configuration doable declaratively, without UI |
| TEST-011 | Vendor independence | At least one backend adapter can be substituted without modifying agent orchestration core |
| TEST-012 | Performance | Run agreed representative metrics/logs/traces benchmarks before claiming parity |
| TEST-013 | UI independent upgrades | Backend functions continue through UI restart, replacement and removal |
| TEST-014 | Backend failure partiality | Intelligence API reports missing sources; never fabricates their data |
| TEST-015 | Unified application and agent trace (A1) | Traditional service, agent, model and tool operations correlate using canonical source identity and relationships |
| TEST-016 | Observation projection integrity (A2) | AI projection preserves trace/span identity, source provenance and explicit session/run mappings |
| TEST-017 | Late and conflicting evaluations (A3) | Multiple evaluations/corrections retain attribution and history without rewriting execution evidence |
| TEST-018 | Evaluation worker isolation (A4) | Disabled or failed evaluation workers leave production responses, ingestion, ordinary queries and deterministic alerts independent |
| TEST-019 | Evaluator self-observability (A4) | Judge/tool errors, latency, cost and versions are inspectable separately from target quality judgments |
| TEST-020 | Quality metric lineage (A5) | Aggregates resolve to canonical evaluations and evaluator/agent versions; individual records remain available under retention policy |
| TEST-021 | Metric cardinality (A5) | Unique execution/user/request IDs do not become ordinary metric labels; exact allowlists/limits require separate decisions |
| TEST-022 | Headless external-agent evaluation (A6) | Enabled evaluation APIs work without default UI, hosted control plane or built-in investigation agent |
| TEST-023 | Evaluation authorization (A6) | Cross-tenant target/content/result access is denied under the shared policy model; workers do not bypass it |
| TEST-024 | Content policy enforcement (A7) | Capture, content reads, evaluator use and external egress obey configured authorization/redaction/destination policy |
| TEST-025 | Missing evidence and retention (A7) | Withheld, sampled, expired or unavailable content is surfaced as a limitation, not fabricated evidence or an automatic failing score |
| TEST-026 | Storage independence (A8) | Logical records/APIs do not require an unapproved engine, object store, queue or mandatory database count |

Additional candidate Evidence Contract test procedures (PROPOSED; exact fixtures/procedures not frozen). The underlying approved responsibilities and invariants remain mandatory:

- NO_MATCH differs from PARTIAL/UNAVAILABLE/DENIED.

- Source evidence references require reauthorization and cannot cross tenant boundaries.

- A query budget cannot be increased by editing LLM-generated tool arguments.

- An agent citation resolves to the same original source or reports expiration/invalidation.

- Time-window normalization and service identity aliases work across two real backend adapters.

- High-cardinality request IDs are not silently inserted into metric labels.

TEST-015–TEST-026 are added conformance expectations derived from approved A1–A8. They are future implementation acceptance criteria, not claims that software has been implemented or these tests have already passed. Lifecycle-specific pass criteria await Section 19's decision.

### 13.2 Explicit architectural anti-patterns

Implementation sessions MUST NOT:

1. Require the default UI to provision the backend, manage accounts, query evidence, or run alerts.

1. Make externally hosted control-plane availability a dependency for basic self-hosted operations.

1. Use a private database connection from custom UI/agent clients instead of authorized APIs.

1. Build a second, weaker agent-only permissions system.

1. Allow an LLM to decide its own permissions, execute unreviewed destructive actions, or treat log text as trusted instructions.

1. Choose a telemetry database solely because it is easy to access from an LLM.

1. Rewrite proven metrics/logs/traces storage functionality before performance requirements demonstrate the need.

1. Convert inferred/aggregated evidence into exact causal claims without stating limits.

1. Pretend missing, sampled, expired, unauthorized, or failed-source telemetry is a complete negative result.

1. Fix all implementation TBDs at once or claim a suggested technology is already approved.

## 14. Suggested implementation order — PROPOSED, not an approved schedule

1. Establish repository structure, decision register, public API design process and testing strategy.

1. Build a minimal independently running local management/auth layer and backend API with headless smoke tests.

1. Define and validate a draft evidence schema with one real metrics backend and one log/trace backend.

1. Add canonical service identity, operational context ingestion, and provenance/completeness semantics.

1. Add independent evidence gateway and a simple bring-your-own-agent reference example.

1. Add optional agent worker, investigation records, evaluation and self-observability.

1. Add default UI and declarative management/deployment integrations (some may be developed in parallel).

1. Run representative storage-performance and agent-access benchmarks; tune/revisit storage choices based on data.

Do not read this as a binding milestone schedule or as a decision on the implementation language, storage product or agent framework.

## 15. Primary reference materials retained from v0.2

The original specification recorded these references as verified on 2026-11-22. This revision retains that list; Section 20.2 identifies the additional sources freshly checked for v0.3.

These sources motivate designs; they do not convert external conventions into mandatory choices unless noted above.

1. OpenTelemetry architecture — https://opentelemetry.io/docs/collector/architecture/

1. OTel service resource conventions — https://opentelemetry.io/docs/specs/semconv/resource/service/

1. OTel context propagation — https://opentelemetry.io/docs/concepts/context-propagation/

1. OTel logs data model — https://opentelemetry.io/docs/specs/otel/logs/data-model/

1. OTel metrics data model — https://opentelemetry.io/docs/specs/otel/metrics/data-model/

1. OTel trace API — https://opentelemetry.io/docs/specs/otel/trace/api/

1. OTel GenAI semantic conventions — https://opentelemetry.io/docs/specs/semconv/gen-ai/ (note the separate evolving repository)

1. Prometheus metric-label practices — https://prometheus.io/docs/practices/naming/

1. Prometheus/OpenMetrics exemplar compatibility — https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_openmetrics/

1. VictoriaMetrics cluster architecture — https://docs.victoriametrics.com/victoriametrics/cluster-victoriametrics/

1. OpenObserve architecture — https://openobserve.ai/docs/architecture/

1. Grafana Tempo HTTP API / LLM-optimized responses — https://grafana.com/docs/tempo/latest/api_docs/

1. Grafana Tempo AI integrations — https://grafana.com/docs/tempo/latest/introduction/tempo-and-ai/

1. Open Policy Agent deployment / PDP-PEP separation — https://www.openpolicyagent.org/docs/deploy

1. Open Policy Agent tool-call authorization — https://www.openpolicyagent.org/docs/agent-tool-calling

1. OpenFGA authorization — https://openfga.dev/

1. Braintrust BYOC separation — https://www.braintrust.dev/blog/security-data-control

1. OTel sampling — https://opentelemetry.io/docs/concepts/sampling/

1. OTel semantic convention stability — https://opentelemetry.io/docs/specs/otel/versioning-and-stability/

## 16. Implementation handoff template

Use this block at the start of any substantial implementation session:

> Project: AI-Native Observability Platform
> Architecture reference: AI_Native_Observability_Architecture_Spec_v0.3.md
> Task: [fill with concrete task]
> Relevant confirmed requirements: [REQ IDs]
> Relevant ADRs: [ADR IDs including ADR-A1 through ADR-A8 where applicable]
> Known proposed or deferred decisions involved: [topics]
> Requested implementation boundaries: [components and public API]
> Acceptance tests to satisfy: [TEST IDs]
> Stop-and-ask conditions: Only when a DEFERRED choice blocks the task or a confirmed requirement would need changing.
> Before coding: Summarize which confirmed contracts the implementation must preserve and distinguish approved choices from assumed defaults.
> After coding: List tests passed, security/performance evidence, any remaining TBDs, and whether an ADR update is required.

## 17. Unified traditional and AI agent observability

### 17.1 Approval and scope

**Status: CONFIRMED in v0.3.** The owner explicitly approved the unified observability findings and recommendations in the referenced conversation, then requested incorporation of decisions A1–A8. The records below express those approvals as implementation requirements. They extend the existing four-plane architecture; they do not introduce a fifth plane or replace earlier confirmed requirements.

The architecture combines a unified telemetry foundation with specialized AI observation and evaluation models and independently deployable evaluation execution. This corresponds to the prior research's Option C for data architecture and Option D for deployment. Separate logical records and service responsibilities do not mandate separate physical databases.

### A1 Unified telemetry foundation

**CONFIRMED — REQ-019 / ADR-A1.** Traditional applications, infrastructure, customer-owned agents, and the platform's own agents MUST share a common telemetry and correlation foundation. OpenTelemetry/OTLP is the canonical tracing and telemetry interoperability foundation. GenAI semantic conventions and adapters for OpenInference or framework-specific instrumentation enrich that foundation without creating an incompatible trace system.

Preserve original trace IDs, span IDs, parent relationships, span links, timestamps, resource identity, and source provenance. Agents, model generations, tool calls, retrievals, and handoffs must be correlatable with the ordinary application and infrastructure operations that caused or supported them. Parallel and asynchronous workflows must not be reduced to a fictitious strictly nested execution tree.

Exact supported convention versions, normalization mappings, collector topology, and compatibility behavior remain DEFERRED. GenAI conventions are still marked Development in the official repository; implementation must select and document versions rather than treating the moving upstream branch as a frozen platform schema [R20].

Unified identity and telemetry semantics do not require a single wire path or physical ingestion pipeline. Existing Prometheus-compatible and backend-specific interfaces remain compatible with the earlier architecture where selected; trace correlation and shared authorization must remain consistent across them.

### A2 First class agent observations and sessions

**CONFIRMED — REQ-020 / ADR-A2.** Agent observations and sessions are first-class logical concepts. An AI observation projection provides efficient AI-oriented retrieval over canonical execution telemetry; it MUST preserve links to the original execution records and MUST NOT become an unrelated trace tree with independently invented trace ancestry.

The conceptual structure is session → trace or agent run → observation or span. A session may span multiple runs and traces; run identity and trace identity need an explicit mapping rather than an assumption that they always mean the same thing. Observations may describe agent invocation, model generation, tool use, retrieval, or handoff. Normalized attributes may include agent/model/prompt versions, provider, operation, usage, latency, cost, service/environment, and authorized content references when available.

Only identity, provenance, timing, and correlation needs should determine the mandatory minimum. Missing optional fields must remain distinguishable from known values. The precise field set, identifier formats, projection update rules, and session completion policy are DEFERRED. Content capture is governed by A7.

### A3 First class evaluation and improvement records

**CONFIRMED — REQ-021 / ADR-A3.** Evaluation results, human feedback and annotations, evaluator/rubric versions, datasets, and experiments are first-class logical records with their own lifecycle. They reference execution evidence rather than overwriting it.

Support evaluations targeting an observation/span, agent run/trace, session, or experiment result. Evaluation sources include deterministic code, LLM judges, human reviewers, and external systems. Multiple or conflicting judgments and later corrections must be representable with attribution and history. A thumbs-down event is feedback; it does not automatically establish a verified failure or root cause.

The capability direction includes asynchronous production evaluation, on-demand historical evaluation, session/task evaluation, authorized annotation, dataset curation, offline experiments, and regression comparison. Exact release sequencing, score types/ranges, evaluator interfaces, correction semantics, and dataset/experiment schemas remain DEFERRED. Langfuse's current concepts provide a reference for linking scores and experiments; its product-specific schemas are not adopted requirements [R21].

### A4 Independent asynchronous evaluation execution

**CONFIRMED — REQ-022 / ADR-A4.** The Intelligence Plane contains an independently deployable Agent Evaluation Service and asynchronous workers, separate from the built-in Investigation Runtime. Evaluation workers can scale or be disabled without making production agent responses, telemetry ingestion, ordinary queries, or deterministic alerting depend on evaluation completion.

External agents and external evaluation systems can use the supported APIs without running the default investigation agent. Evaluation may use code, local/customer-hosted models, explicitly permitted external models, or human workflows. Tenant permissions, budgets, and trust-boundary rules apply in all cases.

Evaluators themselves must be observable: retain permitted execution status, errors, latency, usage/cost, evaluator/model/rubric versions, and links to evaluation targets and results. Evaluator execution telemetry and the quality judgment of the target are distinct logical records. An evaluator error is not a failing quality score for the target. Langfuse documents separate asynchronous worker execution and evaluator debugging traces as useful references [R22, R23].

Queue technology, delivery guarantees, retry/backoff limits, idempotency keys, scheduling, resource quotas, code-evaluator isolation, and failure recovery mechanisms remain DEFERRED. Section 19 recommends the next decision needed to settle their behavioral contract.

### A5 Separate operational and quality metrics

**CONFIRMED — REQ-023 / ADR-A5.** Operational metrics describe execution health and efficiency: latency, errors, tool reliability, token usage, resource consumption, and model cost. Quality/outcome metrics describe evaluated behavior: task success, diagnosis accuracy, groundedness, evaluation pass rates, human corrections, and regressions.

Canonical evaluation records are the source for evaluation-derived quality aggregates. Aggregates may be exported to the ordinary metrics engine, but MUST NOT replace individual evaluation records, evaluator attribution, evidence references, or version history. An agent's self-reported success must remain identifiable as such and cannot silently become independent ground truth.

Avoid unique run, session, trace, request, incident, and user IDs as ordinary metric labels. Use traces/logs, evidence references, and exemplars where supported for individual-execution correlation. Exact aggregate formulas, eligible populations, denominators, missing-score treatment, label allowlists, and cardinality limits are DEFERRED. Operational latency and task-quality scores must not be conflated even when a dashboard presents them together.

### A6 Shared authorization and complete headless operation

**CONFIRMED — REQ-024 / ADR-A6.** Observations, protected content, evaluation targets/results, annotations, datasets, experiments, and evaluation jobs MUST use the existing authorization model for users, service accounts, standalone agents, and delegated agents. There is no separate weaker evaluation permission system.

Actual retrieval, target dereferencing, content access, job execution, and result publication must respect authorized tenant/resource/task scope. Agent delegation remains bounded by user rights ∩ agent rights ∩ task scope, with its own audit identity. Workers do not gain permission merely because they are internal services.

All enabled evaluation capabilities must be operable through documented APIs without the default UI, a vendor-hosted control plane, or the built-in investigation agent. Telemetry-only installations can omit evaluation workers. Customer-hosted operation and local authorization remain complete. Exact authorization actions, sharing policies, worker credential lifetimes, and revocation behavior remain DEFERRED under the earlier security invariants.

### A7 Content capture privacy and provenance

**CONFIRMED — REQ-025 / ADR-A7.** Capturing prompts, outputs, tool arguments/results, retrieved documents, and other sensitive payloads is policy-controlled and optional. Basic operational observability must remain useful without full content capture. Do not require unavailable internal model reasoning; only deliberately emitted, permitted execution data can be observed.

Capture, processing, storage, retrieval, evaluator use, export, and external model egress must follow customer configuration, authorization, redaction/classification, and retention policies. Large or sensitive payloads may be held in an optional protected payload store and referenced from observations. A separate physical store is not mandatory. Access to a trace does not automatically imply access to every protected payload.

Preserve provenance and visibility limitations: source execution, transformations/redaction, permitted content references, relevant configuration/evaluator versions, and missing/sampled/expired/unavailable data must remain distinguishable. An evaluation that lacks required evidence must report that limitation rather than fabricate a judgment. Corrections must not rewrite the original execution history.

Precise default capture mode, opt-in granularity, field classifications, payload limits, encryption/key management, redaction implementation, retention durations, deletion propagation, and reproducibility policies remain DEFERRED. Metadata-first capture with explicit content opt-in is a PROPOSED default, not a silently selected deployment policy.

### A8 Physical storage selection remains deferred

**CONFIRMED CONSTRAINT — REQ-026 / ADR-A8.** No physical storage engine, queue product, index layout, object store, or mandatory database count is selected by A1–A7. Canonical execution telemetry, observation projections, evaluation records, operational metrics, quality aggregates, and protected content are logical responsibilities and may share infrastructure or use specialized stores where justified.

Select physical technologies only after representative traditional and AI workload evaluation. Include deep traces, large optional payloads, late evaluations, analytical score queries, ingestion/query contention, recovery, retention, privacy, and operational cost. Langfuse's separation of transactional, analytical, queue, and blob workloads illustrates one implementation, not a required stack [R22]. ADR-007 and ADR-013 remain in force.

### 17.2 Approved ownership and dependency boundaries

| Plane | Added responsibility | Boundary retained |
| --- | --- | --- |
| Telemetry Data | Unified operational/agent telemetry and correlated observation projections | No evaluator rubric or autonomous investigation business logic; routine alerts remain deterministic |
| Intelligence | Observation/evidence APIs, evaluation APIs and records, datasets/experiments, independent evaluation workers | No mandatory built-in agent or UI; accesses telemetry through authorized interfaces |
| Management and Security | Capture/egress policies, evaluator configuration, permissions, budgets and retention policy | Local customer authority and distributed enforcement remain required |
| Presentation | Optional tracing, evaluation review, and experiment interfaces | No authoritative state, hidden privilege, or mandatory UI dependency |

Primary logical flow: sources → unified ingestion/normalization → canonical telemetry and AI observation projection → authorized evidence and evaluation APIs → asynchronous evaluation → evaluation records → optional quality aggregates. The Investigation Runtime is a separate consumer of evidence. Every sensitive operation, including ingestion and worker access, remains subject to the shared authorization boundary.

The arrows describe dependencies, not a mandatory synchronous call chain. Observation/evaluation lag, loss, and incomplete evidence must be visible; their precise consistency guarantees are the next proposed decision.

## 18. Additional implementation guidance and open decisions

### 18.1 Contract extensions remain at conceptual level

**CONFIRMED responsibilities; PROPOSED representations.** Extend the three approved Evidence Contract responsibilities rather than replacing them. Identity/correlation associates observations, runs, sessions, evaluator executions and evaluation targets. Queries retrieve bounded authorized data. Results return typed records, source references, provenance and completeness information.

Candidate observation groups are identity, operation, timing/status, model/tool metadata, usage/cost, content references, and lineage. Candidate evaluation groups are target reference/version, evaluator identity/version/type, result/type, supporting evidence, execution provenance, and correction history. These are conceptual groups, not required JSON keys or a frozen API. Optional unavailable model, usage, content, and cost fields must not be invented.

Preserve distinct source execution, evaluation execution, evaluation result, and feedback records. A result may arrive long after the target finishes. Re-evaluation with another evaluator/version is not an edit to the execution trace. Score interpretation and the treatment of conflicting or superseded evaluations require explicit schema and aggregation decisions.

### 18.2 Added deferred decisions

These entries extend Section 12. Every earlier deferred topic remains open unless the v0.3 change record explicitly changes its status.

| Topic | Still unresolved | Ask when |
| --- | --- | --- |
| Instrumentation compatibility | Supported GenAI/OpenInference versions, mappings, extension namespaces and migration rules | Implementing ingestion normalization or SDK compatibility |
| Observation projection | Required fields, identity mappings, lag indicators, rebuild/reconciliation and duplicate handling | Implementing projection persistence or serving APIs |
| Execution and evaluation lifecycle | Completion/checkpoints, late data, target snapshots, job states, retries, idempotency, cancellation and replay | Before connecting ingestion/projections to asynchronous evaluation |
| Evaluation models | Target schemas, score ranges, rubrics, corrections, feedback distinctions, dataset/experiment versioning and API resources | Implementing evaluation persistence and public APIs |
| Worker isolation and scale | Queue/store technology, tenant fairness, budgets, code sandboxing, model routing and evaluator recursion protection | Implementing evaluation execution |
| Content policy | Default capture mode, opt-in scope, redaction, payload placement, authorization, deletion and retention propagation | Before capturing or evaluating sensitive content |
| Quality analytics | Selection bias, population/denominator, unevaluated/failed/skipped cases, version comparisons and label limits | Implementing aggregate quality metrics and alerts |
| Feature delivery | Minimum initial evaluation feature set and sequencing of online, session, dataset and experiment workflows | Planning the first implementation release |

Metadata-first capture, at-least-once job delivery, idempotent result writes, a protected object store, an outbox, or a particular worker framework remain proposals until selected at the relevant decision point. No technology or default becomes mandatory solely because it appears in this document.

## 19. Next recommended architecture decision

### 19.1 Canonical execution and evaluation lifecycle contract

**Status: PROPOSED NEXT DECISION — not yet approved.** Decide what makes an observation, run, or session ready for evaluation and what guarantees connect canonical execution data, projections, workers, and results.

This is the highest-priority next decision because A2–A4 introduce asynchronous views and later judgments over execution evidence. Without a lifecycle contract, different implementation sessions could silently disagree about completion, evaluate partial sessions as final, duplicate scores after retries, or overwrite judgments when late data arrives. These behavior choices affect APIs, authorization, retention, storage, and operational reliability before any particular database is chosen.

The recommendation follows three verified reference findings. OpenTelemetry documents loss boundaries from queue capacity, retry limits, and persistent-storage failures; buffering alone is not an unlimited delivery guarantee [R24]. Langfuse's architecture decouples intake from processing with queued references and workers [R22]. Its session-evaluation guide states that the system does not inherently know when a conversation ends and describes explicit final-trace tagging as one approach [R25]. These are reference facts; selecting the lifecycle contract below is our architectural recommendation, not a requirement imposed by those products.

### 19.2 Recommended direction for discussion

Prefer a durable asynchronous handoff with at-least-once delivery, idempotent logical result publication, and versioned evaluation targets. Support an explicit run/session completion or checkpoint signal plus a configurable fallback for missing completion signals. Represent incomplete/provisional evaluations and define how late-arriving evidence triggers a new evaluation revision. Keep physical queue/store products and exact timing values DEFERRED.

| Option | Benefit | Trade-off |
| --- | --- | --- |
| Best-effort evaluation on arrival | Small initial implementation | Lost triggers and incomplete targets can go unnoticed unless explicitly surfaced |
| Durable handoff with idempotency and target versions | Recoverable work and traceable judgments under retries/late data | Requires identity, reconciliation, replay and revision rules; recommended |
| End-to-end exactly-once processing across all stores and external evaluators | Strong idealized uniqueness guarantee | Difficult cross-system coordination and external side effects; requires proof before any such claim |

Idempotent result publication does not guarantee that an external judge is invoked or billed only once after an ambiguous failure. Retry and cost behavior must be explicit. The recommendation is about observable behavior; it does not choose Kafka, Redis, a transactional outbox, or another implementation.

### 19.3 Questions this decision must settle

1. What identifies a source event, observation, run, session, evaluation target version, job, attempt, and logical evaluation result?
2. Which completion/checkpoint signals exist, and how are open sessions, timeouts, late spans, clock skew and projection lag represented?
3. At what durability boundary is ingestion or evaluation submission acknowledged, and how are missed triggers reconciled?
4. What are retry, deduplication, cancellation, replay, backpressure, fairness and budget semantics?
5. How do provisional results, later evidence, evaluator changes, corrections, conflicts and supersession affect records and aggregate quality metrics?
6. Which evidence is snapshotted or referenced, and what happens after expiry, redaction, deletion or permission revocation, including before a queued job executes?
7. How are evaluator failures distinguished from target failure, missing evidence, skipped work and successful evaluations?

Approve the lifecycle responsibilities and guarantees first. Validate proposed states and transitions against failure/recovery examples before freezing names, schemas, thresholds or technologies. This does not reopen the already approved Evidence Contract responsibilities or A1–A8.

## 20. Version history and preservation record

### 20.1 Changes from v0.2 to v0.3

- Preserved the complete v0.2 specification supplied as DOCX, including REQ-001–REQ-018, ADR-001–ADR-016, TEST-001–TEST-014, earlier architecture boundaries, benchmark requirements, proposed designs, illustrative JSON and the entire deferred-decisions register. Markdown is the maintained deliverable for this revision.
- Recorded the owner's subsequent Evidence Contract approval at the conceptual level. REQ-018 and ADR-010 now confirm the three contract responsibilities and six principles: common identity, precise time semantics, provenance, completeness, consistent authorization, and backend independence. Exact schema, enum, field constraints, transport and API names remain proposed/deferred.
- Confirmed the Evidence Gateway's independence from the built-in agent in ADR-011. ADR-012 distinguishes approved bounded retrieval/result principles from proposed raw/semantic tool design.
- Promoted the metric-cardinality direction in REQ-017 under A5; exact label policies and limits remain deferred.
- Added A1–A8 as ADR-A1–ADR-A8, REQ-019–REQ-026, and TEST-015–TEST-026, with detailed boundaries in Sections 17–18. These extend, rather than replace, earlier requirements and tests.
- Added the next lifecycle-contract recommendation in Section 19, explicitly PROPOSED; no storage product, worker framework, retention duration, capture default, numeric threshold, or final public schema was selected.
- Updated version references and the implementation handoff. All other earlier PROPOSED and DEFERRED choices retain their status. Earlier proposed implementation order is retained as guidance and must be reconciled with approved A1–A8 when planning actual work.

### 20.2 Additional primary references verified 2026-11-22

- **R20 — OpenTelemetry GenAI semantic conventions.** The official repository lists inference, tool execution, retrieval, agent, events and metrics conventions and marks the specification Development. [Official GenAI conventions](https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/README.md).
- **R21 — Langfuse evaluation core concepts.** Scores attach to execution-related targets, multiple evaluator methods are supported, and production evaluation connects with datasets and experiments. [Evaluation core concepts](https://langfuse.com/docs/evaluation/core-concepts).
- **R22 — Langfuse architecture.** Separate web/API and worker containers, asynchronous processing, analytical/transactional/blob storage roles, and queued event references illustrate separation of responsibilities. [Platform architecture](https://langfuse.com/handbook/product-engineering/architecture).
- **R23 — Langfuse LLM as a judge.** Evaluator execution traces, statuses and retry behavior illustrate why the evaluator requires its own operational telemetry. [LLM as a judge](https://langfuse.com/docs/evaluation/evaluation-methods/llm-as-a-judge).
- **R24 — OpenTelemetry Collector resiliency.** Sending queues, persistent WAL-backed queues and message queues provide different recovery behavior and retain explicit loss limits. [Collector resiliency](https://opentelemetry.io/docs/collector/resiliency/).
- **R25 — Langfuse session evaluation.** Sessions do not inherently provide an ending signal; explicit final-trace tagging is one documented approach. [Evaluating sessions and conversations](https://langfuse.com/resources/engineering/evaluating-sessions-conversations).

These references support the architecture rationale. They do not import vendors' deployment dependencies, license terms, performance claims, or schemas as platform requirements. Section 15 retains the original v0.2 reference list; the sources newly checked for this revision are listed here.

End of specification v0.3.
