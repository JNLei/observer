# Owner decision — start Rust-first headless product development

**Accepted scope.** The architecture owner accepted the independent review
of the controlled logs comparison at `f2594f9361dfcd12b82ccf2d889c1efa378d4a18`.
The measured harness was `000203dd7f354be0dd2a310bb70b30058afee342`.
Proceed with owned Rust backend development for Headless Backend v0.1.
Pause standalone Rust/Go benchmark expansion and build the first functional
M3/M4/M6 slice.

Five hosted pairs reconciled acknowledged records across the tested Loki
restart at fixed offered loads. Rust used less receiver CPU and charged
memory in those trials. The largest sustainable rate, broader trade-offs
and arbitrary-crash recovery remain unqualified. Full REQ-038 / TEST-038
and ADR-LANG-01's broader evaluation remain open. ADR-REC-01–06 and
ADR-BENCH-01 keep their existing acceptance procedures.
[Reviewed measurements and report qualifications](../implementation/validation-m1-reference.md)

Loki is a replaceable candidate for this slice. Local credential metadata,
capture modes and provisional HTTP operations are reversible implementation
choices. The stable evidence schema, storage architecture, authorization
engine and production custody mechanism still require their respective gates.
