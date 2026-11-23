
# AGENTS.md — AI-Native Observability Platform

## 1. Project Overview

This repository implements an open-source, AI-native observability
platform supporting both traditional systems and AI agents.

The architecture is designed around:
- High-performance metrics, logs, and traces.
- AI agent tracing, evaluation, feedback, and experiments.
- Context-aware, evidence-backed AI investigations.
- Headless backend operation and optional UI deployment.
- Unified authorization for humans, services, and AI agents.
- Independent scalability and reliable outage recovery.

## 2. Source of Truth

The authoritative architecture specification is:

docs/architecture/AI_Native_Observability_Architecture_Spec_v0.5.1.md

Before architecture-affecting implementation:
1. Read the relevant specification sections.
2. Identify applicable REQ, ADR, and TEST identifiers.
3. Distinguish CONFIRMED, PREFERRED, PROPOSED, and DEFERRED decisions.
4. Follow confirmed decisions and required acceptance tests.
5. Do not silently resolve consequential deferred decisions.

The architecture specification takes precedence over
implementation convenience or suggestions in this file.

If an implementation conflicts with an approved architectural
decision, stop and propose an Architecture Decision Record (ADR).
Do not silently change the architecture.

## 3. Architectural Invariants

The following are mandatory:

- Preserve the four logical planes: Telemetry Data,
  Intelligence, Management/Security, and optional Presentation.
- Backend APIs must work without the default UI or any hosted service.
- Telemetry ingestion, raw queries, and routine alerting must
  operate without LLM or agent services.
- Traditional and AI-agent telemetry share canonical correlation
  and compatible OpenTelemetry-based ingestion.
- AI observations and evaluations are first-class resources.
- Evaluation workers must not block production telemetry ingestion.
- The Evidence API must be usable by external agents.
- Humans, service accounts, and agents share one authorization model.
- Enforce authorization at trusted backend and data-access boundaries.
- Protect sensitive information in derived reports, caches,
  evaluation outputs, datasets, and exports.
- Keep evidence, agent hypotheses, and verified conclusions distinct.
- Preserve source attribution, provenance, and data completeness.
- Support independently deployable services and configurable retention.
- Preserve Terraform/OpenTofu, Kubernetes, and declarative management.

Do not tightly couple agents, UI, or storage engines.

## 4. Language Strategy

Preferred implementation languages:
- Rust: owned core backend services.
- Python: built-in AI agent runtime and evaluation workers.
- TypeScript: optional default UI.

Rust-first remains subject to the required production-shaped
Rust-versus-Go ingestion prototype.

Reuse mature OSS components even when written in other languages.
Do not rewrite dependencies solely for language consistency.

All cross-language public interfaces must be versioned and
language-independent.

## 5. Telemetry and Reliability Requirements

Ingestion and query implementations must consider:

- Bounded concurrency, buffering, memory, and resource usage.
- Explicit backpressure and failure behavior.
- Durable buffering and outage recovery where configured.
- Replay of historical backlogs alongside fresh traffic.
- Correct late-arrival, ordering, and metric temporality semantics.
- Cancellation accounting, including work still running after timeout.
- Distinct accepted, durable, queryable, and processed states.
- Retry and duplicate handling without assuming exactly-once delivery.
- Signal-specific resource isolation and query limits.
- Recovery correctness, not only ingestion throughput.

Do not claim performance, durability, or zero data loss without
appropriate testing evidence.

## 6. Security Requirements

- Never trust tenant IDs, service identities, or permissions
  merely because they appear in incoming telemetry.
- Authenticate callers and enforce tenant/resource authorization.
- Do not expose unrestricted storage credentials to AI agents.
- Protect sensitive source-derived information across transformations.
- Apply appropriate authorization to asynchronous work and exports.
- Prevent secrets from leaking through logs, traces, test output,
  fixtures, or configuration files.
- Respect configured content capture, redaction, and retention policies.
- Avoid committing credentials or sensitive production data.

Security-sensitive decisions must follow the architecture
specification and its applicable implementation gates.

## 7. Implementation Workflow

For each significant implementation task:

1. Identify scope and relevant architecture requirements.
2. Inspect existing code and interfaces before designing changes.
3. Develop a minimal implementation plan.
4. Implement the smallest complete, testable change.
5. Add or update relevant tests.
6. Run applicable tests, formatting, and static checks.
7. Review the diff for architecture compliance.
8. Summarize changes, evidence, and remaining limitations.

Prefer vertical slices that work end to end.

Do not introduce unnecessary frameworks, services, or abstractions.

Do not redesign unrelated components while implementing a task.

## 8. Decision Policy

Codex may independently make reversible implementation choices
that comply with the architecture.

For consequential unresolved decisions:
- Research relevant technical options.
- Compare trade-offs against the specification.
- Recommend a default with supporting evidence.
- Request owner approval before committing to a direction that
  materially affects architecture, security, compatibility,
  product scope, or expensive-to-reverse infrastructure choices.

Do not repeatedly ask for approval on routine implementation details.

Record accepted architecture changes in docs/adr/.

## 9. Testing and Verification

Use the applicable acceptance tests from the architecture
specification.

Testing must cover relevant cases including:
- Headless backend operation.
- Authorization and tenant isolation.
- Correct telemetry and agent trace correlation.
- Replay, retries, outage recovery, and late-arriving data.
- Resource exhaustion and failure isolation.
- AI evaluation lifecycle and derived-data confidentiality.
- Performance under declared workloads.

Benchmark results must disclose workload, resources, methodology,
failures, and limitations.

Never claim tests passed unless they were executed successfully.
If a test could not run, state why.

## 10. Required Completion Report

At the end of each significant task, report:

1. Implemented functionality.
2. Relevant REQ/ADR/TEST identifiers.
3. Files and components changed.
4. Tests executed and their results.
5. Architecture compliance assessment.
6. Remaining risks and unresolved decisions.
7. Any proposed ADR.

Do not mark a milestone complete solely because code compiles.
