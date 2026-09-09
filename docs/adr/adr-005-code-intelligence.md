# ADR-005: AST-Based Code Intelligence (Phase 4)

## Status: Accepted (Phase 4)

## Context
Phase 2/3 validated pipeline shape and data profiles but could not assess
code quality. Phase 4 adds structural Python/PySpark analysis without
runtime metrics.

## Decision

### AST-Based, Not Regex
- Parse Python source with `ast` module into structured `CodeAnalysis`:
  functions, imports, dataframe variables, operations (with line/column),
  assignments, secrets/paths, driver loops, complexity.
- Detectors run over `CodeAnalysis.operations`, not raw text.

### Structured Operations
Each `Operation` carries:
- `OperationType` (READ/WRITE/JOIN/.../COLLECT/TO_PANDAS/BROADCAST/…)
- line/column, receiving dataframe, code snippet, typed arguments, context
- Flow graph via `flow_root` (variable rename chains resolved to root)
- Complexity metrics: functions, nesting, size, loops, secrets, paths

### Context-Aware Evaluators
10 rules (CODE-PYSPARK-006…015) reference evaluators by name
(`large_collect`, `topandas_risk`, …). Each evaluator receives:
- `CodeAnalysis` (already built by the engine)
- Merged rule context (volumes, formats, schemas, JDBC/streaming metadata)
- Per-rule `params` (no hard-coded thresholds anywhere)

Evaluator returns per-operation dicts → aggregated into one `Finding`
with evidence (locations + data context), recommendation, confidence,
blocking flag, and assumptions.

### Cap-Status for WARN-Only Rules
Rules that cannot be proved from static analysis (e.g., shuffle risk)
declare `cap_status: WARN`; the engine demotes FAIL → WARN for them.

### Evidence Coverage Tiers
`EvidenceCoverage` tallies total/evaluated/unknown + tier breakdown
(static / context / runtime). Reported alongside the score, never
converted to FAIL.

### Evidence Coverage Tiers
- `static` = syntax/structure only
- `context` = code + data/source evidence combined
- `runtime` = measured runs (empty in offline phases)

## Consequences
- 10 rules (CODE-PYSPARK-006…015) with 5 tests each (positive, negative,
  edge, false-positive, context) + cross-data tests
- 14 code fixtures covering all detector classes
- 227 tests green, ruff + mypy clean
- Good pipeline: PASS on CODE; Bad pipeline: FAIL on 10 rules (006-015)
- EvidenceCoverage tier breakdown in CLI output
- No fake metrics; UNKNOWN never becomes PASS; no secret values logged

## Related
- ADR-002: Checkpoint dependency propagation
- ADR-003: Pydantic v2 model fix
- ADR-004: Source/Data Intelligence (Phase 3)