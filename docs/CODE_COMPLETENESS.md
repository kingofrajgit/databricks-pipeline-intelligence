# DPIF — Code Completeness Intelligence (Phase 4)

**Status:** Implemented (minimal approved scope).
**Basis:** `docs/PHASE_4_CODE_COMPLETENESS_AUDIT.md`.
**Rule:** distinguish CODE completeness (markers/stubs present) from STRUCTURAL
completeness (graph reaches target) from BUSINESS completeness (never claimed
from static analysis).

---

## 1. What was built

```
source text ──▶ M5E implementation_completeness (existing dimension, extended)
   ├── lexical: NotImplementedError / TODO-FIXME-XXX-HACK / pass-... stubs /
   │            placeholder returns (all allowlisted, all WARN/LOW or MEDIUM)
   ├── parse failure ──▶ IMP-COMP-000 UNKNOWN (never PASS)
   ├── flow_graph (shared offline/online) ──▶ structural signals (WARN)
   └── sql_analysis ──▶ unused-CTE findings (WARN/LOW)
```

No new checkpoint, dimension, analyzer, scoring rule, or readiness gate. No
M5E rewrite; the other 8 dimensions are byte-for-byte untouched.

## 2. Structural completeness (`src/dpif/flow/completeness.py`)

Pure shared consumer over the Phase 1 graph — no new model:

- `target_reachability(graph)` → `TARGET_REACHABLE` (every target reachable
  from a source) / `TARGET_NOT_REACHABLE` (some target unreachable) / `UNKNOWN`
  (no sources or no targets — never invented).
- `analyze_structural_completeness(graph)` → verdict + `CompletenessSignal`s:
  `DEAD_END_OPERATION` (no outgoing edge, action/cache/persist/checkpoint and
  WRITE terminals exempt), `UNUSED_DATASET` (assigned, never consumed),
  `DISCONNECTED_JOIN` (fewer than two resolved inputs),
  `UNREACHABLE_TARGET` (from existing `structural_issues()`).
- M5E maps signals to `IMP-COMP-010..013` (WARN/LOW, unreachable MEDIUM).
  Reachability answers "does the implementation structurally reach a target" —
  never "does the target hold correct business data".

## 3. Parse-failure honesty

`IMP-COMP-000`: any `CodeAnalysis.parse_error` yields exactly one UNKNOWN
finding and skips further completeness analysis. A broken file can no longer
report completeness PASS. (Previously all three M5E re-parses returned `[]`;
only the completeness dimension is fixed here — the others are out of scope.)

## 4. Lexical extensions + allowlists

- Markers: `TODO/FIXME/HACK` case-insensitive, `XXX` uppercase-only, word
  boundaries, `#`-comments only, **docstring ranges excluded** (AST-derived).
  Same rule ID `IMP-COMP-002`, same WARN/LOW.
- `raise NotImplementedError` incl. `builtins.X` / bare-attribute forms and the
  `NotImplemented` singleton (`IMP-COMP-001`, WARN/MEDIUM).
- Stubs: `def` bodies of only `pass` / `...` → `IMP-COMP-003`; sole-statement
  explicit `return None` → `IMP-COMP-004` (both WARN/LOW, confidence 0.6).
- Allowlist: `@abstract*` / `@overload` decorators, `except`-handler bodies
  (never function bodies), empty `class` bodies. Abstract interfaces, overloads,
  handlers, and namespace classes stay silent.
- Production wiring fix (required for any of the above to fire): M5E now falls
  back to `context["code_snippet"]` because `CodeAnalysis._raw_source` is never
  populated by the parser; both offline (`cli.py`) and online (`online.py`)
  `rule_context`s now carry `code_snippet` + `flow_graph` (minimal wiring).

## 5. SQL unused CTE

`SQLParser._extract_ctes` populates `referenced_by` (other-CTE names,
`"__main__"`, `"__recursive__"` for legitimate `WITH RECURSIVE` self-reference)
via identity-based subtree analysis. M5E flags `referenced_by == []` as
`IMP-COMP-020` (WARN/LOW), skipping when parse errors exist (UNKNOWN covers
that). A CTE referenced only inside its own definition is unused; `SELECT`
without `FROM`-style edge cases follow the same name rule.

## 6. UNKNOWN semantics

Missing code/graph → no findings (existing behavior); parse failure → UNKNOWN;
ambiguous structure → DERIVED/LOW heuristic findings, never FAIL; confirmed
markers/stubs → WARN findings; valid complete code → existing PASS. Lack of
evidence is never PASS-by-absence for broken inputs and never FAIL for
uncertain ones. Business completeness is never asserted.

## 7. Reporting

Findings flow through the existing M5E `implementation_forensics` payload
(JSON `validation.json`, markdown findings) with rule IDs `IMP-COMP-000..004`,
`-010..013`, `-020`, each carrying evidence/provenance/location/reason. No
report-system changes; scoring and readiness untouched.
