# Architecture decision records

The authoritative existing decision register is in the [architecture
specification](../architecture/AI_Native_Observability_Architecture_Spec_v0.5.1.md#11-architecture-decisions-register).
Add new reviewed ADRs here when a consequential architectural decision is made.
The language preference, engines, policy engine, license, and deployment topology
remain at the statuses recorded in that specification.

The owner approved tenant-scoped quarantine in principle. The reviewed contract authorizes
scenarios 1–7 with a synthetic source. Scenario 8 and production adoption
require separate approval.
[Custody of permanently failing Loki log exports](PROPOSED-loki-poisoned-exports.md)

The owner authorized Rust-first development of the first headless logs slice
after independent review of the controlled comparison. Full TEST-038 and
the remaining recovery gates stay open.
[Rust-first headless product development](rust-first-headless-slice.md)
