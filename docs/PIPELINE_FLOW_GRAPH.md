# DPIF — Pipeline Flow Graph (Phase 1, GAP-001)

**Status:** Implemented (Phase 1 of the Pipeline Data-Flow & Optimization Intelligence plan).
**Scope:** One unified data-flow graph model + one builder, populated identically by offline
and online validation.
**Audit basis:** `docs/PIPELINE_DATA_FLOW_INTELLIGENCE_AUDIT.md` — gap (1) "no unified flow
graph model".

---

## 1. What was built

A single graph model, `dpif.flow.PipelineFlowGraph`, and a single builder,
`dpif.flow.builder.build_pipeline_flow_graph()`, shared by **both** validation paths:

```
SOURCE ──▶ OPERATION ──▶ DATASET ──▶ OPERATION ──▶ SHUFFLE ──▶ OPERATION ──▶ TARGET
```

Design rules (from the audit):

- **One builder, both paths.** Offline (`cli.py::_run_offline_validation`) and online
  (`OnlineValidationOrchestrator.validate`) call the same builder with the same arguments
  `(code_analysis, contract, pipeline_name, raw_code)`. Identical evidence ⇒ identical graph
  (enforced by a parity test).
- **Evidence-driven only.** Every node/edge carries provenance. Nothing is fabricated:
  dataset identity stays `UNKNOWN` when no evidence determines it.
- **No new provenance vocabulary.** `FlowProvenance.kind` reuses the M5E
  `EvidenceProvenanceKind` enum (STATIC_CODE, CONTRACT, RUNTIME, FIXTURE, ...).
  `FlowProvenance.state` distinguishes KNOWN / DERIVED / UNKNOWN.
- **Structural validation only.** `PipelineFlowGraph.structural_issues()` reports
  DUPLICATE_NODE, INVALID_EDGE, CYCLE, ORPHAN_NODE, UNREACHABLE_TARGET. No business or
  optimization rules live in the graph layer.
- **Never breaks validation.** The builder catches all exceptions and returns an empty
  (honest) graph for evidence shapes it does not understand.

Out of scope for Phase 1 (future graph consumers): per-operation volumes, runtime
observations, partition states, cache nodes, cross-task file flows.

---

## 2. Architecture position

```
                        ┌─ offline: fixtures + contract file ─┐
evidence acquisition ──▶│                                     │──▶ CodeAnalysis
                        └─ online:  Databricks APIs (M5B..M5D) ┘    (unchanged)
                                                                      │
                                          build_pipeline_flow_graph() │  ← NEW
                                                                      ▼
                                                       PipelineFlowGraph
                                                                      │
        context["flow_graph"] / result.flow_graph ────────────────────┤
                                                                      ▼
        CP-001..CP-024 → M5E..M5I  (existing analyzers, unchanged)
                                                                      │
                                              validation.json / .md   ▼  ← NEW section
                                              payload["pipeline_flow_graph"]
```

No second checkpoint engine, no second decision path, no orchestration changes beyond the
builder call — exactly the seam the audit prescribed.

---

## 3. Node & edge model (`src/dpif/flow/models.py`)

| Node kind  | Carries | Provenance |
|------------|---------|------------|
| `SOURCE`   | `dataset` identity of the read entry (table/path/sql), `location` | STATIC_CODE |
| `OPERATION`| `operation_type` (READ/WRITE/FILTER/JOIN/GROUP_BY/...), `location`, variable name | STATIC_CODE |
| `DATASET`  | DataFrame variable identity (DERIVED), stable node per variable | STATIC_CODE (DERIVED) |
| `SHUFFLE`  | `shuffle_cause` — the operation type that induces it (JOIN, GROUP_BY, DISTINCT, ORDER_BY, REPARTITION, WINDOW, DROP_DUPLICATES per `SHUFFLE_OPS`) | STATIC_CODE (DERIVED) |
| `TARGET`   | `dataset` identity of the sink (saveAsTable/save path), `location` | STATIC_CODE |

`FlowEdge` links `from_node → to_node` with an optional carrying-dataset name and provenance.

Key API on `PipelineFlowGraph`:

- `node() / node_ids() / edges_from() / edges_to() / nodes_of_kind() / operations_of_type()`
- `ancestors_of() / descendants_of() / reaches_target()` (cycle-safe traversal;
  `reaches_target` returns `None` when the graph declares no targets — "no evidence"
  is never conflated with "disconnected")
- `flow_chain() / summary()` — compact `Source → Filter → Join → Target` rendering
- `to_dict()` — JSON-safe serialization for reports
- `structural_issues()` — the five structural checks above

`DatasetIdentity` states:

- **KNOWN** — stated directly by evidence (`spark.read.table('db.tbl')`, `saveAsTable`,
  SQL `FROM db.tbl`, DLT `dlt.read`/`@dlt.table`).
- **DERIVED** — deterministic inference (DataFrame variable nodes; contract-fallback
  identity filling).
- **UNKNOWN** — evidence unavailable (`spark.readStream.load()` with no path; path reads
  whose terminal argument is not a literal). Never guessed.

---

## 4. Builder evidence mapping (`src/dpif/flow/builder.py`)

| Evidence | Mapped to |
|----------|-----------|
| `Operation(READ, via='table'|'sql')` | SOURCE node; identity from recorded `query` argument |
| `Operation(READ, via='read'|'parquet'|...)` | SOURCE node; identity parsed from the **terminal** read call (`.load(...)` / `.parquet(...)`), format from `.format(...)` — the format name is never mistaken for a location |
| `Operation(WRITE)` | OPERATION(WRITE) → TARGET node; identity from `.saveAsTable('t')` / `.save('path')` |
| Other `Operation`s | OPERATION nodes chained via producer tracking; shuffle-causing ops additionally emit a SHUFFLE node so downstream ops chain OPERATION → SHUFFLE → OPERATION |
| JOIN arguments `other=` | second input edge from the other branch's producer (handles `df2`, `F.broadcast(df2)`, `broadcast(df2)`, `df2.alias('r')`); only used when that variable has known producers |
| `VarAssignment` | DATASET nodes (`a = df.filter(...)` → `op:filter → ds:a`); fan-out is preserved — `b = df.filter(...)` branches from the **same** `ds:df` ancestor, not from branch `a`'s tail |
| `CodeAnalysis.sql_analysis` | SOURCE per declared table (qualified `catalog.schema.name` when available); JOIN operation + SHUFFLE per join, both inputs wired; parser quirks (empty join sides, `line=0`) handled deterministically — unresolved join sides are filled from declared-but-unassigned tables, marked DERIVED |
| DLT vocabulary in raw code (`dlt.read`/`dlt.read_stream`, `dlt.create_streaming_table`, `@dlt.table(name=...)` / `@dlt.table()` function) | SOURCE / TARGET nodes (`metadata.dlt = True`); deduplicated against code-derived names |
| `contract.source` / `contract.target` | provenance upgraded to CONTRACT on matching code nodes; synthesized SOURCE/TARGET nodes when the contract declares datasets the code does not show (e.g. contract-only offline runs); UNKNOWN code identities filled from contract (DERIVED) |

Ordering: Python path processes operations line-by-line (line, column sorted), applies the
line's assignments afterwards, then DLT fallback, then contract enrichment. SQL path runs
when `analysis.language == "sql"`, or as fallback when Python analysis yields no
sources/targets (SQL-in-string content).

---

## 5. Integration points

| Location | Change |
|----------|--------|
| `src/dpif/flow/models.py` | graph model (new) |
| `src/dpif/flow/builder.py` | builder (new) |
| `src/dpif/flow/__init__.py` | public exports (new) |
| `src/dpif/cli.py` | offline path builds graph → `context["flow_graph"]`, `payload["pipeline_flow_graph"]`, passes to `persist_offline_validation_report` |
| `src/dpif/orchestration/online.py` | online path builds the same graph → `rule_context["flow_graph"]`, `OnlineValidationResult.flow_graph`, serialized as `payload["pipeline_flow_graph"]` |
| `src/dpif/reporting/validation_reports.py` | `persist_offline_validation_report(..., flow_graph=None)`; both offline/online markdown reports gain a `## Pipeline Flow Graph (GAP-001)` section (summary line, node/edge counts, structural issues) |

All payload additions are **additive**; `to_dict()` only emits `pipeline_flow_graph` when a
graph exists, and existing payload keys are unchanged.

---

## 6. Parity guarantee

Both paths call the same builder with the same evidence tuple. The test
`tests/unit/test_pipeline_flow_graph.py::test_offline_online_parity_same_builder_same_graph`
asserts identical `(analysis, contract, pipeline_name, raw_code)` produce byte-identical
`to_dict()` output regardless of which path constructs the graph.

---

## 7. Testing

`tests/unit/test_pipeline_flow_graph.py` (29 tests) covers:

- model semantics: node labels, provenance defaults, JSON-safety, evidence states
- PySpark builder: read→write chain, identity recording, path reads (format vs location),
  shuffle nodes, **multi-input joins** (two incoming edges), **branching fan-out**
  (two outgoing edges from one dataset), UNKNOWN preservation
- SQL builder: join wiring with two sources, multi-join uniqueness
- DLT builder: `dlt.read_stream` sources, `@dlt.table` / `create_streaming_table` targets
- structural validation: all five issue codes, cycle-safety, empty-graph honesty
- contract enrichment: contract-only graphs, provenance upgrade
- offline CLI integration: `--json` payload and persisted `validation.json`/`validation.md`
- online orchestrator integration: graph from live-discovered code, markdown section
- offline/online parity and malformed-evidence robustness

Existing suites (`test_report_persistence.py`, `test_online_orchestration.py`,
`test_online_discovery_parity.py`, ...) all pass unchanged.

> Note on local runs: the repository's 100-pipeline performance tests
> (`test_cli_batch.py`, `test_multi_pipeline_batch.py`, `test_multi_pipeline_orchestration.py`,
> `test_fleet_orchestration.py`) take several minutes each **on the unmodified tree**;
> they are slow, not hung. All other suites run in ~75 s.

---

## 8. Future phases (consumers of the graph)

The graph deliberately carries no volumes/runtime yet. Planned consumers:

- **Phase 2 — completeness:** unreachable OPERATION nodes, created-but-never-consumed
  DATASET nodes (dead code), disconnected branches.
- **Phase 3 — shuffle intelligence:** repeated shuffle across a chain, shuffle-boundary
  queries replacing M5E flat-list heuristics.
- **Phase 4 — runtime correlation:** map OPERATION nodes to `RuntimeStage`s (the runtime
  model already carries the needed stage evidence), attach RUNTIME-provenance observations
  at node level.
- **Phase 5 — volume propagation:** per-node volume when evidence exists (UNKNOWN otherwise).
