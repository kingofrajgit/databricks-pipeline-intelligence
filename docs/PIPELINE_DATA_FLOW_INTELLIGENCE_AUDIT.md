# DPIF — Pipeline Data-Flow & Optimization Intelligence Audit

**Status:** AUDIT ONLY — no framework behavior was modified.
**Scope:** Static repository inspection of `src/dpif/**`, `rules/**`, `tests/unit/**`, `docs/**`.
**Date:** 2026-09-30
**Evidence basis:** Actual source files read during this audit (file references are given inline).

---

## 1. Executive Summary

DPIF already possesses the two halves needed for pipeline data-flow intelligence:

- a **static half**: an AST-based Python/PySpark parser (`src/dpif/code/parser.py`, `code/models.py`) that detects reads, writes, transformations, joins, filters, aggregations, repartition/coalesce, cache/persist/checkpoint with file/line locations, plus a lightweight per-variable flow resolver (`code/flow.py`);
- a **runtime half**: stage/task-level execution evidence (`src/dpif/runtime/models.py`) with shuffle read/write, spill, GC, task skew, plus a static↔runtime correlation engine (`runtime/correlation.py`).

Both offline and online validation already feed the **same** checkpoint engine (CP-001..CP-024) and the **same** M5E implementation forensics analyzer. The shared-engine requirement is therefore already structurally satisfied — the difference is only in evidence acquisition (fixtures/contract vs live Databricks API).

What does **not** exist today is a **common data-flow model**: a graph of Source → Operation → Dataset → Shuffle boundary → Partition state → Runtime observation → Target that both modes populate and a single analysis engine traverses. Today the flow knowledge is scattered across flat operation lists, per-variable flow maps, heuristic line-order checks (M5E), rule-ID-based runtime correlation, and a single-source/single-target extractor (`discovery/synthesis.py`).

**Verdict per the core question:** *Can the framework already provide complete pipeline-level data-flow and optimization analysis for both offline and online validation?* — **No, not as an explicit end-to-end capability.** But it can reach that target with far less work than a rebuild: the analysis engine seam (shared `CodeAnalysis` + shared context dict + shared checkpoint/analyzer stack) already exists, and the evidence provenance machinery needed by the target design already exists in three overlapping forms.

The main gaps are: (1) no unified flow graph model, (2) no per-operation volume representation (UNKNOWN is correctly preserved today, but there is no node-level slot for runtime volume when it *is* available), (3) no operation↔stage correlation (only rule-ID correlation), (4) incomplete code-completeness detection (created-but-unused DataFrames, disconnected branches, bare `pass` are not detected), (5) no cache-opportunity (reused-without-cache) finding, (6) limited partition analysis (no `clusterBy`, no partition-count state, no `spark.sql.shuffle.partitions` capture), and (7) three separate provenance enums that should be consolidated in the common model.

---

## 2. Current Architecture

### 2.1 Offline path (`src/dpif/cli.py`, `_run_offline_validation`)

```
dpif validate --contract X.yaml --offline
  → load_contract_file()                     (contract YAML → PipelineContract)
  → _load_metadata_profile()                 (fixture JSON profile, CollectionMethod.FIXTURE)
  → _load_code_text()                        (code_path from contract/CLI → source text)
  → analyze_source(code, filename)           (AST → CodeAnalysis)
  → build_all_checkpoints(...)               (CP-001..CP-024 skeletons)
  → CheckpointEngine.run_all_checkpoints()   (YAML rules: SOURCE-*/DATA-*/CODE-PYSPARK-*/CODE-SQL-*/
                                              CONFIG-*/RUNTIME-PERF-*/SCALABILITY-*)
  → optional RuntimeRun from --runtime-run fixture; optional historical runs file
  → runtime.correlation.correlate_static_and_runtime()   (only when runtime fixture provided)
  → M5E DeveloperImplementationAnalyzer(analysis, rule_context)
  → M5F RerunIdempotencyAnalyzer → M5G ThreeLayerAlignmentAnalyzer →
    M5H EvidenceSufficiencyAnalyzer → Readiness → M5I DecisionRiskSynthesisAnalyzer
  → persist_offline_validation_report()      (reports/offline/<pipeline>/<ts>/validation.json|.md)
```

Evidence the offline path can consume today: contract YAML (source, target, volumes, SLA, reliability, scalability), fixture metadata profile (file sizes, partition sizes, schema), pipeline source code, optional runtime run fixture, optional historical runs fixture. It cannot consume live workspace/cluster/table metadata.

### 2.2 Online path (`src/dpif/orchestration/online.py`, `providers/base.py`)

```
dpif validate-online --job-id N | --pipeline-id X
  → OnlineValidationOrchestrator → DatabricksEvidenceProvider.acquire_pipeline_evidence()
      categories: WORKSPACE, JOB, PIPELINE (DLT), CLUSTER, PERMISSIONS, CODE,
                  TABLE_PROFILE, RUNTIME, HISTORICAL_RUNS
      (Normalizes payloads; AcquisitionError with structured error codes; credential sanitization;
       EvidenceProvenance per item: LIVE_API | FIXTURE, is_mock flag)
  → code retrieval: job task → workspace export / DBFS read / DLT pipeline libraries
  → analyze_source(code)                          (same AST parser as offline)
  → discovery.synthesis.synthesize_discovered_contract()   (single Source + single Target from code)
  → discovery.synthesis.synthesize_discovered_data_profile()  (runtime input bytes / table metadata)
  → build_all_checkpoints() + CheckpointEngine    (SAME engine as offline)
  → M5E/M5F/M5G/M5H/Readiness/M5I                 (SAME analyzers as offline)
  → OnlineValidationResult → persist_online_validation_report()
```

### 2.3 Shared core (already common)

| Component | Shared? | Location |
|---|---|---|
| AST code analysis (`CodeAnalysis`) | YES | `src/dpif/code/parser.py`, `code/models.py` |
| SQL analysis (`SQLAnalysis`, sqlglot) | YES | `src/dpif/sql/parser.py`, `sql/models.py` |
| Flow helpers (`build_flow`, `downstream_uses`, `flow_root`) | YES | `src/dpif/code/flow.py` |
| Checkpoint engine + CP-001..CP-024 | YES | `src/dpif/checkpoints/` |
| YAML rule engine (SOURCE/DATA/CODE/CONFIG/RUNTIME/SCALABILITY rules) | YES | `src/dpif/rules/engine.py` + `rules/**` |
| M5E implementation forensics | YES | `src/dpif/analyzers/implementation.py` |
| M5F/M5G/M5H/M5I analyzers | YES | `src/dpif/analyzers/` |
| Runtime models & analyzers | YES | `src/dpif/runtime/` |
| Scalability engine | YES | `src/dpif/scalability/` |
| Evidence acquisition | NO (offline: fixtures/contract; online: `connectors/`, `providers/`, `discovery/`) | by design |

---

## 3. Existing Capabilities (verified)

### 3.1 Static code intelligence

`CodeAnalysis` (`src/dpif/code/models.py`) holds per-file: functions, imports, `dataframe_variables`, `assignments` (`VarAssignment` target=source edges), `operations` (list of `Operation`), `driver_loops`, secrets/paths, and an optional `sql_analysis`.

`Operation` carries: `operation_type`, `line`, `column`, `dataframe`, `code` snippet, `arguments`, `context` — i.e. **source location per operation already exists** (requirement §3 satisfied at the model level).

Detected operation types (`OperationType`): READ, WRITE, SELECT, FILTER, JOIN, GROUP_BY, AGGREGATE, DISTINCT, DROP_DUPLICATES, ORDER_BY, WINDOW, REPARTITION, COALESCE, CACHE, PERSIST, COLLECT, TO_PANDAS, SHOW, TAKE, FIRST, COUNT, LIMIT, UDF, PANDAS_UDF, BROADCAST, EXPLODE, UNION, LOOP, CHECKPOINT, OTHER.

`code/flow.py` provides:
- `build_flow()` — per-dataframe ordered op-type chains (flat, **not a graph**);
- `downstream_uses(analysis, df, after_line)` — reuse counting (used by cache analysis);
- `flow_root(analysis, variable, before_line)` — resolves a variable to its assignment source through `assignments`, with cycle guards (used to group repeated repartitions across renames).

This is the closest existing thing to a lineage engine — but it is variable-name based, line-ordered, single-file, and has no dataset/edge/shuffle semantics.

### 3.2 SQL intelligence

`src/dpif/sql/parser.py` (sqlglot) produces `SQLAnalysis`: table references (catalog/schema/alias/line), joins (type, left/right tables, condition, **join columns**, line), filters (expression, columns, functions-on-columns), aggregations, window specs (partitionBy/orderBy), CTEs, order-by. SQL-side join-key and filter-column extraction **already exists** — the PySpark side lacks the equivalents.

### 3.3 Runtime intelligence (`src/dpif/runtime/`)

`RuntimeRun` → `RuntimeStage` → `RuntimeTask` → `RuntimeTaskMetrics` (input/output/shuffle read/shuffle write/memory spill/disk spill bytes, GC, CPU). Stage/task counts, failures, cluster utilization. Strict normalization (`runtime/normalization.py`) with UNKNOWN preservation (e.g. `analyze_shuffle_input_ratio` returns level UNKNOWN when input bytes are absent; `analyze_cluster_utilization` returns UNKNOWN when no utilization evidence).

12 deterministic analyzers (RUNTIME-PERF-001..012): stage duration, shuffle volume, shuffle/input ratio, task duration imbalance, data skew, memory spill, disk spill, GC ratio, task failures/retries, executor failures, low utilization, long-tail tasks.

### 3.4 Static↔runtime correlation (`runtime/correlation.py`)

`correlate_static_and_runtime()` maps ~10 static rule IDs (CODE-PYSPARK-001/002/006/007/008/012, CODE-SQL-002/003/005/007) to runtime rule IDs (RUNTIME-PERF-001..005, 008, 010) and emits `CorrelationResult` with statuses `RUNTIME_SUPPORTS_STATIC_RISK`, `STATIC_RISK_CONFIRMED`, `STATIC_RISK_NOT_OBSERVED_IN_SUPPLIED_RUN`, `RUNTIME_EVIDENCE_UNAVAILABLE`. This is **rule-ID-based, not operation-location-based**: it cannot correlate "join at transform.py:47" to "stage 12 shuffle read = 1.3 TB". The required correlation model (§11) must be stage/operation aware.

### 3.5 Scalability intelligence (`src/dpif/scalability/`)

14 YAML rules (SCALABILITY-001..014) → deterministic analyzers (expected/peak volume capacity, data growth, file-count growth, partition scalability, driver scalability, **shuffle scalability**, join scalability, aggregation scalability, cluster capacity, autoscaling boundary, SLA scalability, runtime regression, reliability degradation). Models include `ScalabilityScenario` (BASELINE/EXPECTED/PEAK/GROWTH_1/GROWTH_2), `ScalabilityProjection` (method + assumptions + confidence), `ScalabilityObservation` (per-run volume/duration/shuffle/spill), `ScalabilityTrend` (scaling behavior from ≥2 observations). Its own `EvidenceProvenance` enum explicitly separates CONTRACT / STATIC / DATABRICKS_API / DATABRICKS_METADATA / RUNTIME / EVENT_LOG / FIXTURE / PROJECTED / UNKNOWN.

### 3.6 Evidence & sufficiency

- `EvidenceRecord` (core): observed/expected/evidence list/recommendation/confidence + `AnalysisMethod` (metadata/sample/full/unavailable).
- `CollectionMethod`: metadata/sample/full_scan/runtime/fixture/unknown.
- M5E `EvidenceProvenanceKind`: STATIC_CODE / RUNTIME / HISTORICAL_RUN / FIXTURE / CONTRACT / JOB_CONFIG / METADATA.
- Scalability `EvidenceProvenance` (see 3.5).
- Provider-level `EvidenceProvenance` (per acquired item: LIVE_API/FIXTURE + source_system + acquired_at + is_mock).
- M5H `EvidenceSufficiencyAnalyzer`: 16 domains, per-domain `DomainEvidenceCoverage` (expected/available/unavailable evidence, provenance, freshness, quality, completeness score, `ConfidenceLevel` HIGH/MEDIUM/LOW/INSUFFICIENT, `decision_sufficient`), key pipeline decisions, critical missing evidence.

Requirement §18 (evidence classes) is therefore **already modeled**, but scattered across 4+ enums (see §18 below).

---

## 4. Offline Capability Matrix

Legend: IMPLEMENTED / PARTIAL / MISSING / UNKNOWN.

| Capability | Offline status | Evidence source | Notes |
|---|---|---|---|
| Source discovery | IMPLEMENTED | contract YAML (SOURCE-* rules, CP-001) | type/path/format/ingestion/volumes/jdbc/streaming/partitioning |
| Target discovery | PARTIAL | contract YAML `Target` | model exists; no offline validation rules on target |
| Code discovery | IMPLEMENTED | `code_path` in contract or CLI | local file read; multi-file NOT supported (single file per run) |
| Python/PySpark analysis | IMPLEMENTED | AST (`code/parser.py`) + CODE-PYSPARK-001..019 | single-file scope |
| SQL analysis | IMPLEMENTED | sqlglot (`sql/parser.py`) + CODE-SQL-001..010 | embedded SQL strings & .sql files via CodeAnalysis.sql_analysis |
| Transformation flow | PARTIAL | `build_flow` per-variable chains | flat chains, not a graph; no cross-file, no edge model |
| Data volume | PARTIAL | fixture profile + contract | pipeline-level total only; UNKNOWN preserved (CP-007 UNKNOWN without profile); no per-node volume |
| Partition analysis | PARTIAL | REPARTITION/COALESCE ops + DATA/SCALABILITY rules | no partitionBy(write) semantics analysis, no cluster_by, no shuffle-partitions config |
| Shuffle analysis | PARTIAL | M5E static heuristics + RUNTIME-PERF (fixture) | no shuffle-boundary graph; runtime only if fixture supplied |
| Join strategy | IMPLEMENTED | M5E JOIN_STRATEGY + CODE rules | broadcast/cross/filter-order; PySpark join keys missing |
| Cache/persist | IMPLEMENTED | M5E CACHE_LIFECYCLE | reuse-count based, contextual; no cache-opportunity finding |
| Repartition/coalesce | IMPLEMENTED | M5E PARTITIONING_QUALITY | repeated repartition, coalesce(1) placement |
| Runtime metrics | PARTIAL | `--runtime-run` fixture | fully modeled but optional |
| Historical runs | PARTIAL | `--historical-runs` fixture | consumed by CP-010/SCALABILITY-014/Readiness |
| Optimization findings | PARTIAL | M5E dimensions + code rules | no cross-operation (multi-node) optimization reasoning |
| Code completeness | PARTIAL | M5E IMPLEMENTATION_COMPLETENESS | TODO/FIXME/NotImplementedError only (see §14) |
| Source→target lineage | PARTIAL | contract + code first READ/WRITE | not a lineage; single source/target only |
| Evidence provenance | IMPLEMENTED | M5E provenance + CollectionMethod + M5H | scattered enums |
| Confidence | IMPLEMENTED | per-finding confidence + M5H ConfidenceLevel | — |
| Recommendations | IMPLEMENTED | every Finding/EvidenceRecord | — |
| Final decision | IMPLEMENTED | CP-024 + Readiness + M5I | INSUFFICIENT_EVIDENCE preserved |

## 5. Online Capability Matrix

| Capability | Online status | Evidence source | Notes |
|---|---|---|---|
| Source discovery | IMPLEMENTED | code AST/regex + table profile (`discovery/synthesis.py`) | first `spark.read/table/sql` wins; multi-source not modeled |
| Target discovery | IMPLEMENTED | `.saveAsTable/.insertInto/.save` + table profile | single target |
| Code discovery | IMPLEMENTED | job tasks → workspace export / DBFS / DLT libraries (`providers/base.py::_acquire_code`) | combined task code; multi-notebook flows flattened in one string |
| Python/PySpark analysis | IMPLEMENTED | same AST parser | identical to offline |
| SQL analysis | IMPLEMENTED | same sqlglot parser | identical to offline |
| Transformation flow | PARTIAL | same as offline | same gap |
| Data volume | PARTIAL | runtime input bytes / table profile (`synthesize_discovered_data_profile`) | falls back to `total_tasks` as record_count — a weak proxy; UNKNOWN otherwise |
| Partition analysis | PARTIAL | same static analysis + table profile partitioning info | same gap |
| Shuffle analysis | PARTIAL | RUNTIME-PERF on live RuntimeRun | same integration gap |
| Join strategy | IMPLEMENTED | M5E (volume from runtime/table metadata) | IMP-JOIN-002 path uses RUNTIME/HISTORICAL provenance |
| Cache/persist | IMPLEMENTED | M5E | same as offline |
| Repartition/coalesce | IMPLEMENTED | M5E | same as offline |
| Runtime metrics | IMPLEMENTED | Jobs API run output → RuntimeRun (stages/tasks/metrics) | UNKNOWN preserved for missing metrics |
| Historical runs | IMPLEMENTED | Jobs API recent runs (limit 10) | [] vs None distinction preserved |
| Optimization findings | PARTIAL | same analyzers | same gap |
| Code completeness | PARTIAL | M5E | same gap |
| Source→target lineage | PARTIAL | synthesized contract | same gap |
| Evidence provenance | IMPLEMENTED | per-item EvidenceProvenance (LIVE_API) + M5E/M5H | — |
| Confidence | IMPLEMENTED | M5H + synthesis | — |
| Recommendations | IMPLEMENTED | same as offline | — |
| Final decision | IMPLEMENTED | CP-024 + Readiness + M5I | — |

## 6. Common Capability Matrix (parity view)

| Capability | Offline | Online | Common Engine today? | Evidence source | Current Status |
|---|---|---|---|---|---|
| Source discovery | IMPLEMENTED | IMPLEMENTED (synthesized) | contract model shared | contract vs code+table profile | IMPLEMENTED |
| Target discovery | PARTIAL | PARTIAL | contract model shared | contract vs code | PARTIAL |
| Code discovery | IMPLEMENTED | IMPLEMENTED | same parser, different retrieval | local file vs workspace export | IMPLEMENTED |
| Python/PySpark analysis | IMPLEMENTED | IMPLEMENTED | YES (`code/parser.py`) | STATIC_CODE | IMPLEMENTED |
| SQL analysis | IMPLEMENTED | IMPLEMENTED | YES (`sql/parser.py`) | STATIC_CODE | IMPLEMENTED |
| Transformation flow | PARTIAL | PARTIAL | YES (flow.py) but flat | STATIC_CODE | PARTIAL |
| Data volume | PARTIAL | PARTIAL | YES (profile/context) | FIXTURE/METADATA vs RUNTIME | PARTIAL |
| Partition analysis | PARTIAL | PARTIAL | YES (M5E + rules) | STATIC + optional RUNTIME | PARTIAL |
| Shuffle analysis | PARTIAL | PARTIAL | YES (M5E static + RUNTIME-PERF) | STATIC vs RUNTIME | PARTIAL |
| Join strategy | IMPLEMENTED | IMPLEMENTED | YES (M5E) | STATIC (+RUNTIME volumes online) | IMPLEMENTED |
| Cache/persist | IMPLEMENTED | IMPLEMENTED | YES (M5E) | STATIC_CODE | IMPLEMENTED |
| Repartition/coalesce | IMPLEMENTED | IMPLEMENTED | YES (M5E) | STATIC_CODE | IMPLEMENTED |
| Runtime metrics | PARTIAL | IMPLEMENTED | YES (RuntimeRun model) | fixture vs LIVE_API | PARTIAL overall (offline optional) |
| Historical runs | PARTIAL | IMPLEMENTED | YES (context list) | fixture vs LIVE_API | PARTIAL overall |
| Optimization findings | PARTIAL | PARTIAL | YES (M5E + rules) | STATIC | PARTIAL |
| Code completeness | PARTIAL | PARTIAL | YES (M5E) | STATIC_CODE | PARTIAL |
| Source→target lineage | PARTIAL | PARTIAL | contract model only | STATIC/DERIVED | PARTIAL |
| Evidence provenance | IMPLEMENTED | IMPLEMENTED | enums duplicated, semantics shared | all | IMPLEMENTED (needs consolidation) |
| Confidence | IMPLEMENTED | IMPLEMENTED | YES (M5H) | all | IMPLEMENTED |
| Recommendations | IMPLEMENTED | IMPLEMENTED | YES | all | IMPLEMENTED |
| Final decision | IMPLEMENTED | IMPLEMENTED | YES (CP-024/Readiness/M5I) | all | IMPLEMENTED |

**Key parity finding:** there are **no offline-only or online-only analyzers**. Every analysis capability above is instantiated identically in `cli.py::_run_offline_validation` and `orchestration/online.py::validate`. Divergence exists only in *evidence acquisition* and in how much evidence is typically available (online usually has RuntimeRun; offline only with a fixture). The "common engine" requirement is already structurally true; what is missing is a common **data-flow model** that both paths populate.

---

## 7. M5E Coverage (`src/dpif/analyzers/implementation.py`, `models/implementation.py`, `tests/unit/test_developer_implementation_forensics.py`)

M5E consumes: `CodeAnalysis` (operations + assignments + raw source for AST re-parse) and a context dict (`pipeline_name`, `data_size_gb`/`table_size_gb`/`expected_volume_gb`/`peak_volume_gb`, `evidence_source`, `collection_method`). It works identically offline and online (same instantiation in both orchestrators; verified in `cli.py` and `orchestration/online.py`). Provenance is selected from context strings (`_get_provenance`), defaulting to STATIC_CODE. 20 test scenarios (A–T) cover all 9 dimensions.

| # | Dimension | Currently detects | Evidence consumed | Offline | Online | Does NOT detect |
|---|---|---|---|---|---|---|
| 1 | `transformation_quality` | Python UDF (IMP-TRANS-001, volume-conditioned), Pandas UDF (002), `count()` existence checks (003) | CodeAnalysis + volume context | YES | YES | UDF complexity, nested UDF in loops (partially via driver_loops elsewhere), UDF-in-window interplay |
| 2 | `shuffle_optimization` | Pre-join filter (001 PASS), post-join filter (002 WARN), repartition immediately before join (003, ≤3 lines) | CodeAnalysis | YES | YES | Join-key-aware pushdown, filter-column analysis, shuffle-boundary graph, repeated shuffle across flow (except repartition), aggregate-induced patterns |
| 3 | `partitioning_quality` | Repeated repartition per flow-root (001), coalesce(1) before aggregation (002 FAIL), coalesce(1) before write (003, volume-conditioned) | CodeAnalysis + volume | YES | YES | repartition(N) sizing vs volume, partitionBy(write) layout, cluster_by, partition-state before/after joins, `spark.sql.shuffle.partitions` |
| 4 | `join_strategy` | Broadcast with UNKNOWN volume (001), broadcast supported by RUNTIME/HISTORICAL provenance (002), dangerous broadcast ≥10 GB (003), cross join (004) | CodeAnalysis + volume + provenance | YES | YES (002 meaningful online) | Join keys (PySpark), join ordering (multi-join), broadcast *opportunity* (small undetected side), partitioning around joins |
| 5 | `cache_lifecycle` | Optimal cache+reuse+unpersist (001 PASS), single-use cache (002), reuse without unpersist (003) | CodeAnalysis + flow.downstream_uses | YES | YES | **Cache opportunity** (reused ≥2 without cache → potential recomputation) — explicitly missing; storage levels; cross-function reuse; cache lifetime vs action |
| 6 | `checkpoint_lifecycle` | checkpoint/localCheckpoint with/without downstream use (001/002) | CodeAnalysis | YES | YES | checkpoint dir config, frequency, interplay with cache |
| 7 | `resource_lifecycle` | `with` context manager (001 PASS), close in finally (002), close outside finally (003), acquire without close (004) | raw source AST | YES | YES | connections opened in loops, leaked session-scoped resources across functions |
| 8 | `exception_safety` | Swallowed/bare except (001 FAIL), broad catch (002), specific handling (003 PASS) | raw source AST | YES | YES | per-pipeline-stage exception masking, retry-swallow interplay |
| 9 | `implementation_completeness` | `raise NotImplementedError` (001), TODO/FIXME/XXX comments (002) | raw source AST | YES | YES | bare `pass` stubs, **created-but-unused DataFrames**, **transformations disconnected from target**, missing return paths, unreachable branches (see §14) |

**Design principle check (§20):** M5E is *not* a naive rule list — it is context- and evidence-conditioned (volume gates, provenance gates, flow-root grouping, reuse counting). It is the right seed for the future flow-aware engine, but it currently reasons per-operation/per-pattern, not over a flow graph.

## 8. Runtime Coverage

- Models: complete for stages/tasks/metrics incl. shuffle read/write, spill, GC (`runtime/models.py`); normalization tolerant and UNKNOWN-preserving (`runtime/normalization.py`).
- Analyzers: 12 deterministic detectors; missing metrics produce explicit UNKNOWN findings (e.g. shuffle/input ratio without input bytes).
- Historical: consumed via context list by CP-010/SCALABILITY-* and Readiness; `ScalabilityObservation` models per-run volume/duration/shuffle/spill with provenance.
- Correlation: rule-ID-mapping only (`runtime/correlation.py`); `CorrelationResult` is a good *conclusion* record but has no operation↔stage linkage. **The runtime evidence model already supports the data required for correlation** (stage ids, names, task counts, shuffle/spill per stage); what is missing is the mapping layer from flow-graph operations to stages.

## 9. Scalability Coverage

14 rules + deterministic scenario/projection/trend engine with strict provenance. Covers volume growth (expected/peak/growth scenarios), file-count growth, partition scalability, driver scalability, shuffle scalability (volume-scaled), join/aggregation scalability, cluster capacity, autoscaling boundary, SLA scalability, runtime regression, reliability degradation. This already covers the "future workload" half of optimization intelligence and should be reused, not duplicated, by the future engine.

## 10. Source → Target Lineage Coverage

- **No lineage model exists.** Repository-wide, "lineage" appears only in documentation text (`analyzers/sufficiency.py` comment context; architecture doc mentions governance lineage as an analyzer category).
- `discovery/synthesis.py` extracts exactly one `Source` (first READ op, then regex fallbacks) and one `Target` (first WRITE op, then `.saveAsTable/.insertInto/.format().save` regex) — provenance DERIVED, single-pipeline shape.
- `code/flow.py::flow_root` is a per-variable ancestor walk — the primitive a lineage builder would need, but it is name-based and not exposed as a graph.
- `SQLAnalysis` table references and join structures provide SQL-side lineage inputs.
- Contract `Source`/`Target` models exist with path/format/partitioning/schema.

**Missing:** multi-input pipelines (fan-in), multi-output (fan-out), intermediate datasets, dataset↔operation bipartite graph, cross-file/cross-task flows (Databricks multi-task jobs are concatenated into one code string online; structure is lost).

## 11. Partition Analysis Coverage

| Item | Static | Runtime | Status |
|---|---|---|---|
| `repartition()` detection | YES (op + literal args) | n/a | IMPLEMENTED |
| `coalesce()` detection | YES (incl. coalesce(1) literal) | n/a | IMPLEMENTED |
| `partitionBy()` | PARTIAL — write-chain `partitionBy` attr recorded; Window.partitionBy recorded as window context | table partition metadata via profile | PARTIAL |
| `cluster_by` | NO (no `clusterBy` detection anywhere) | table clustering via table profile only | MISSING |
| `spark.sql.shuffle.partitions` | NOT captured (config not parsed) | task_count per stage observable | MISSING (static) |
| Partition count state | NO model | `stage.task_count` exists | PARTIAL |
| Partitioning change tracking (before/after joins/aggs) | NO — only repeated-repartition and coalesce placement | stage sequence exists but unmapped | MISSING |
| Partitioning mismatch | partial via M5G alignment `_analyze_partitioning` (contract vs code partitionBy) | skew detectors (RUNTIME-PERF-004/005) | PARTIAL |

**Static inference is correctly never presented as runtime fact** — M5E findings are STATIC_CODE provenance and worded as "potential"; runtime counts are separate. This separation must be carried into the new model (see §20 proposed model: `PartitionState.evidence` field).

## 12. Shuffle Analysis Coverage

| Item | Status | Where |
|---|---|---|
| Join-induced shuffle (potential) | IMPLEMENTED (static, per-op) | M5E SHUFFLE_TRIGGER_OPS / IMP-SHUFFLE-002/003 |
| Aggregation-induced shuffle (potential) | PARTIAL — ops classified as shuffle-triggering but no dedicated findings | `SHUFFLE_OPS` in `code/pyspark.py`, M5E |
| repartition-induced shuffle | IMPLEMENTED | IMP-PART-001/IMP-SHUFFLE-003 |
| Unnecessary/repeated shuffle | PARTIAL — repeated repartition only | IMP-PART-001 |
| Partition mismatch (shuffle) | MISSING (static) / PARTIAL (runtime skew) | — / RUNTIME-PERF-004/005 |
| High shuffle risk | IMPLEMENTED (volume-conditioned) | CODE-PYSPARK-008/SQL-003 + M5E |
| Actual shuffle read/write | IMPLEMENTED (runtime) | RuntimeStage.shuffle_read/write_bytes |
| Spill | IMPLEMENTED (runtime) | RUNTIME-PERF-006/007 |
| Shuffle duration | PARTIAL — stage duration yes, no per-shuffle attribution | RUNTIME-PERF-001 |
| Stage↔operation correlation | MISSING | §3.4 |

"Never invent shuffle bytes from static code" is honored: static findings never carry byte counts; runtime analyzers require evidence.

## 13. Cache/Persist Coverage

Detected: `cache`/`persist`/`unpersist` ops (parser maps unpersist→PERSIST with code marker), downstream use counting via `flow.downstream_uses` (strictly-after-line semantics prevents self-count), matching unpersist search, three contextual findings (IMP-CACHE-001/002/003). The M5E design already avoids the forbidden "no cache → FAIL" pattern: findings are WARN/LOW and conditioned on reuse counts.

Missing for the target experience:
1. **Cache opportunity finding** — reused ≥2 times without cache → potential recomputation (the §6 example). Not implemented today.
2. Storage level capture (`persist(StorageLevel.MEMORY_AND_DISK)` args are not parsed into the Operation model).
3. Cache lifecycle across functions/branches (downstream_uses is name+line based in one file).
4. Cost/benefit reasoning (cache vs recomputation cost) — intentionally absent; belongs in the future flow-aware engine with volume evidence.

## 14. Code Completeness Coverage

Implemented (M5E IMPLEMENTATION_COMPLETENESS): TODO/FIXME/XXX comments, `raise NotImplementedError`.

**Not implemented** (verified by inspection of `_analyze_completeness` — it only walks `Raise` nodes and comment lines):
- bare `pass` stubs in function bodies;
- created-but-unused DataFrames (assignable via assignments + downstream_uses — primitives exist);
- transformations not reaching the target (no target-node linkage to test reachability);
- missing return paths / unreachable branches (no control-flow analysis beyond nesting counters);
- disconnected pipeline branches (requires the flow graph).
Conclusion: M5E does **not** already cover §9; the primitives (assignments, downstream_uses, WRITE ops) exist to build these checks on top of the future flow graph.

## 15. Optimization Coverage

| Optimization | Status |
|---|---|
| Filter pushdown opportunities | PARTIAL — post-join filter detection (IMP-SHUFFLE-002); no column-level pushdown for PySpark; SQL side has filter columns |
| Projection before expensive ops | MISSING |
| Unnecessary transformations | PARTIAL (single-use cache WARN; unused-DataFrame check missing) |
| Repeated transformations | PARTIAL (repeated repartition only) |
| Avoidable repartition/coalesce | IMPLEMENTED (IMP-PART-001/002/003) |
| Expensive operations (UDF, cross join, collect, window) | IMPLEMENTED per-op, volume-conditioned |
| Transformations after large joins | PARTIAL (filter-after-join only) |
| Unnecessary data movement | PARTIAL (shuffle heuristics; no flow-level dedup of shuffles) |
| Broadcast opportunity detection | MISSING |
| Cluster/utilization-driven optimization | IMPLEMENTED (RUNTIME-PERF-011, SCALABILITY-010) |

## 16. Evidence / Provenance Coverage

Five parallel provenance/evidence vocabularies exist (§3.6). Semantics overlap heavily: STATIC_CODE≈STATIC, RUNTIME=RUNTIME, FIXTURE=FIXTURE, CONTRACT=CONTRACT, DATABRICKS_METADATA≈METADATA; the scalability enum adds PROJECTED and EVENT_LOG (valuable, absent elsewhere); M5H adds freshness and confidence tiers. All non-implemented/missing evidence is represented (UNKNOWN statuses, `evidence_unavailable`, `confidence=0`), satisfying "never convert missing evidence into PASS/FAIL" — the checkpoint engine's dependency rule also blocks dependents on UNKNOWN (`checkpoints/engine.py::_check_dependencies`).

## 17. Missing Capabilities (consolidated)

1. **Common data-flow graph model** (nodes/edges/datasets/shuffle boundaries/partition states/runtime observations) — the central gap.
2. **Operation↔stage correlation** for runtime confirmation of static findings at the operation level.
3. **Per-node volume representation** (records/bytes in/out) fed by runtime when available, UNKNOWN otherwise.
4. **Cache-opportunity finding** (reuse without cache → potential recomputation).
5. **PySpark join-key extraction & filter-column analysis** (parity with SQL side).
6. **Broadcast-opportunity detection** (undetected small side; needs volume/table stats).
7. **`clusterBy` / write `partitionBy` / `spark.sql.shuffle.partitions` static capture.**
8. **Partition-state transition tracking** across the flow (before/after joins/aggs).
9. **Extended completeness checks**: unused DataFrames, disconnected branches, target reachability, bare-pass stubs.
10. **Multi-file / multi-task flow reconstruction** (Databricks multi-task jobs; cross-notebook imports).
11. **Multi-source / multi-target lineage** (fan-in/out; single-source/target synthesis today).
12. **Provenance enum consolidation** (one shared vocabulary incl. PROJECTED, EVENT_LOG, PROFILE/DERIVED, UNAVAILABLE).
13. **Cost/SLA impact dimension** for optimization findings (today explicitly UNKNOWN — correct; keep as UNKNOWN until evidence exists).

## 18. Duplicate / Overlapping Capabilities

| Overlap | Instances | Risk | Resolution direction |
|---|---|---|---|
| Partitioning analysis | M5E `partitioning_quality`, M5G `_analyze_partitioning`, SCALABILITY-005 | Divergent verdicts on same code | Future flow engine should consume one partition model; keep M5G's contract-vs-code intent check |
| Shuffle analysis | M5E `shuffle_optimization` (static), RUNTIME-PERF-002/003 (runtime), SCALABILITY-007 (scaled) | Same phenomenon, three vocabularies | One shuffle-boundary model with evidence classes |
| Join analysis | M5E `join_strategy`, CODE-PYSPARK-002/008, CODE-SQL-002/003, SCALABILITY-008 | Cross join/broadcast detected by both regex rules and AST M5E | Consolidate detection in flow engine; rules reference graph facts |
| Cross join detection | CODE-PYSPARK-002 (regex) and IMP-JOIN-004 (AST) | Duplicate findings for one code smell | Deduplicate at synthesis |
| Provenance enums | `EvidenceProvenanceKind` (M5E), `EvidenceProvenance` (scalability), provider `EvidenceProvenance`, `CollectionMethod`, `AnalysisMethod` | Mapping drift | Single shared enum set (§20) |
| Finding models | `Finding` (core) vs `ForensicFinding` (M5E) | Two shapes of the same thing | Common finding base in new model layer |
| Two rule systems | YAML regex/evaluator rules vs Python AST analyzers | Both scan the same code | Keep YAML rules for config/metadata; route code facts through the flow engine |

## 19. Recommended Minimal Architecture

```
                    OFFLINE                                   ONLINE
   contract YAML, fixture profile,                     Databricks APIs (job/cluster/
   code file, runtime fixture,                         pipeline/code/table/runtime/history),
   historical fixture                                  optional contract/code overrides
              |                                                 |
              └───────────────┬─────────────────────────────────┘
                              ▼
        (existing) evidence acquisition + normalization      ← unchanged
                              ▼
              COMMON EVIDENCE MODEL  (existing models, consolidated provenance)
                              ▼
        ┌───────────────────────────────────────────────────┐
        │ NEW: COMMON DATA-FLOW MODEL (PipelineFlowGraph)   │
        │  built from CodeAnalysis (+SQLAnalysis) static    │
        │  facts + runtime observations where available     │
        └───────────────────────────────────────────────────┘
                              ▼
        COMMON ANALYSIS ENGINE (flow-aware)
        ├── static analysis      (flow-based completeness, cache opportunity,
        │                         shuffle boundaries, partition transitions)
        ├── runtime analysis     (existing RUNTIME-PERF, mapped to flow nodes)
        └── historical analysis  (existing scalability trends/observations)
                              ▼
        OPTIMIZATION INTELLIGENCE (evidence-classed, contextual)
                              ▼
        FINDINGS → RECOMMENDATIONS (existing Finding/ForensicFinding shapes)
                              ▼
        EVIDENCE SUFFICIENCY (existing M5H) → DECISION (existing CP-024/M5I)
```

Deliberately minimal: **no second checkpoint engine, no second decision path, no runtime API additions.** The new layer sits between existing evidence and existing analyzers; M5E rules become graph queries instead of flat-list queries.

## 20. Proposed Common Data-Flow Model (design only — not implemented)

```python
class FlowNodeKind(StrEnum):
    SOURCE = "SOURCE"            # read entry (table/path/format/batch|streaming)
    OPERATION = "OPERATION"      # one detected transformation/action
    TARGET = "TARGET"            # write/sink

class FlowEdge(BaseModel):
    """Directed data movement. Bipartite-ish: DatasetNode OR direct op→op."""
    from_node: str               # node id
    to_node: str
    dataset: str | None          # DataFrame variable / SQL alias (UNKNOWN if none)
    via_assignment: bool         # edge derived from VarAssignment

class SourceLocation(BaseModel):
    file: str | None             # notebook/file path (None when unavailable)
    line: int | None
    column: int | None
    function: str | None

class Evidence(BaseModel):
    """Single evidence class set for the whole model (consolidates §3.6 enums)."""
    kind: EvidenceKind           # STATIC | RUNTIME | HISTORICAL_RUN | PROFILE |
                                 # CONTRACT | DERIVED | FIXTURE | PROJECTED |
                                 # EVENT_LOG | UNAVAILABLE
    confidence: float            # 0.0..1.0
    reference: str | None        # e.g. rule_id, stage_id, API endpoint

class VolumeObservation(BaseModel):
    records_in: int | None = None      # None => UNKNOWN, never invented
    records_out: int | None = None
    bytes_in: int | None = None
    bytes_out: int | None = None
    evidence: Evidence

class PartitionState(BaseModel):
    partition_count: int | None = None
    partition_columns: list[str] = Field(default_factory=list)
    is_shuffle_boundary: bool = False
    origin: str | None = None    # "repartition(200)", "join", "aggregation", "stage task_count"
    evidence: Evidence           # STATIC vs RUNTIME is explicit here

class ShuffleBoundary(BaseModel):
    cause: str                   # JOIN | AGGREGATION | REPARTITION | WINDOW | ORDER_BY | ...
    input_dataset: str | None
    partition_state: PartitionState | None
    static_risk: str | None      # potential | repeated | unnecessary | mismatch
    runtime: RuntimeObservation | None

class RuntimeObservation(BaseModel):
    stage_ids: list[int] = Field(default_factory=list)
    shuffle_read_bytes: int | None = None
    shuffle_write_bytes: int | None = None
    spill_bytes: int | None = None
    duration_seconds: float | None = None
    evidence: Evidence           # kind=RUNTIME; all quantities None-safe

class FlowNode(BaseModel):
    node_id: str
    kind: FlowNodeKind
    operation: OperationType | None      # None for SOURCE/TARGET
    location: SourceLocation | None      # from Operation.line/column (+file)
    name: str | None                     # function/variable/notebook
    volume: VolumeObservation | None
    partition_state: PartitionState | None
    runtime: RuntimeObservation | None
    evidence: Evidence

class PipelineFlowGraph(BaseModel):
    pipeline_name: str
    nodes: list[FlowNode]
    edges: list[FlowEdge]
    sources: list[str]                   # node ids (multi-source capable)
    targets: list[str]                   # node ids (multi-target capable)
    def shuffles(self) -> list[ShuffleBoundary]: ...
    def ancestors_of(self, node_id: str) -> list[str]: ...
    def reaches_target(self, node_id: str) -> bool | None: ...   # None = UNKNOWN
```

Answers to the mandated questions:
- **Pipeline node** = one source, one detected operation, or one target; carries static location + optional volume/partition/runtime + evidence.
- **Edge** = data dependency derived from variable assignments / call chains / SQL table refs; labeled with the dataset name.
- **Operation** = existing `Operation` (OperationType + location), promoted to a node.
- **Dataset** = an edge label (DataFrame variable or SQL alias); a node only when materialized (checkpoint/cache table) or a source/target.
- **Partition state** = `PartitionState` with explicit static-vs-runtime `Evidence.origin`.
- **Shuffle boundary** = `ShuffleBoundary` attached to the operation node that causes it.
- **Runtime observation** = `RuntimeObservation` referencing stage ids; attached only when a correlation mapping exists, otherwise absent.
- **Source-code location** = `SourceLocation` (file/line/column/function) — already collected per operation.
- **Provenance** = one consolidated `Evidence`/`EvidenceKind` enum (replaces the 5 current vocabularies at this layer; existing enums keep working at their layers).
- **Confidence** = per-node/per-edge/per-finding float + M5H ConfidenceLevel for conclusions.

UNKNOWN discipline: every optional field is None when unevidenced; no builder may synthesize volume, shuffle bytes, or partition counts from static code.

## 21. Offline → Common Model Flow

1. Existing offline acquisition unchanged (contract, fixture profile, code text, optional runtime fixture/historical file).
2. `analyze_source()` → `CodeAnalysis` (existing).
3. **New builder (offline):** `PipelineFlowGraphBuilder(code_analysis, sql_analysis, contract_source, contract_target)` → flow graph with SOURCE/TARGET nodes from contract (evidence CONTRACT) or code READ/WRITE ops (STATIC), edges from assignments/ops, shuffle boundaries from SHUFFLE_TRIGGER_OPS, partition states from repartition/coalesce ops (STATIC). Runtime fixture (if provided) contributes `RuntimeObservation`s and RUNTIME volume at the node level where mappable; otherwise stays None.
4. Common analysis engine consumes the graph; findings flow into the existing checkpoint/M5E/M5H/M5I stack unchanged.

## 22. Online → Common Model Flow

1. Existing online acquisition unchanged (provider categories incl. CODE, RUNTIME, TABLE_PROFILE, HISTORICAL_RUNS).
2. Same `analyze_source()` on retrieved code (existing).
3. **Same builder** as offline — SOURCE/TARGET nodes prefer synthesized contract + table profile (PROFILE/METADATA evidence), runtime observations map stages where mappable (RUNTIME evidence), else remain None.
4. Same common engine → same findings → existing M5E/M5H/M5I/CP-024 decision.

Because both modes feed the **same builder and same graph**, there is exactly one analysis engine — satisfying the §23 constraint without touching orchestration code beyond introducing the builder call.

## 23. Proposed Analysis Flow

```
Evidence (STATIC | RUNTIME | HISTORICAL_RUN | PROFILE | CONTRACT | DERIVED | FIXTURE)
        ▼
PipelineFlowGraph (nodes/edges; UNKNOWN preserved everywhere)
        ▼
Context resolution (per-node: input datasets, reuse counts, volume when available)
        ▼
Static analysis over graph: completeness (reachability, unused datasets, stubs),
cache opportunity (reuse≥2 & no cache), shuffle boundaries (cause + static risk),
partition transitions (before/after join/agg), join context (keys, filters, order)
        ▼
Runtime correlation (map RuntimeObservation onto nodes/stages; keep
RUNTIME_EVIDENCE_UNAVAILABLE when mapping is not possible — never guess)
        ▼
Historical/scalability layer (existing trends/observations on flow-critical nodes)
        ▼
Findings (evidence-classed, contextual, confidence-carrying)
        ▼
Recommendations (what/where/why/evidence/confidence/what-to-change/what-is-missing)
        ▼
Evidence sufficiency (M5H) → Decision (CP-024/M5I)
```

## 24. Risks

1. **Regression risk in M5E parity:** if M5E rules are re-expressed as graph queries, existing 20-scenario tests must keep passing; run them as the parity gate.
2. **False-positive cache/reachability findings:** name-based flow breaks with reassignment, branching, and UDF-internal reuse; `reaches_target` must return UNKNOWN rather than assert disconnection when the graph is ambiguous.
3. **Stage↔operation correlation is genuinely hard:** Spark stage names rarely contain code references; without Spark UI plan info, mapping may stay UNAVAILABLE — the design must tolerate that (correlation optional per node).
4. **Multi-task job flattening:** online code retrieval concatenates tasks into one string; line numbers collide across tasks. Graph builder needs per-task file attribution or must degrade to PARTIAL evidence.
5. **Provenance consolidation churn:** touching existing enums breaks persisted report schemas; consolidation should happen in the new model layer first.
6. **Scope creep into rule engine:** YAML regex rules and AST analyzers overlap; migrating detection into the graph must be phased or findings will duplicate.
7. **Volume misuse:** even with correct models, downstream rules could misuse runtime totals as per-node volumes; volume must be attached only where evidence maps.

## 25. Implementation Plan — HIGH LEVEL ONLY

- **Phase A — Model & provenance consolidation:** add `PipelineFlowGraph` + consolidated `Evidence` (design §20). No analyzer changes.
- **Phase B — Builder:** implement `PipelineFlowGraphBuilder` from `CodeAnalysis`/`SQLAnalysis` (+ contract source/target). Unit-test parity with `flow.py` behavior.
- **Phase C — Completeness over graph:** reachability-to-target, unused-dataset, stub detection; extend M5E `implementation_completeness` without changing existing rule IDs.
- **Phase D — Cache opportunity & shuffle boundaries:** graph-based reuse analysis (adds the missing cache-opportunity finding), explicit shuffle-boundary nodes with static risk classes.
- **Phase E — Runtime correlation:** map `RuntimeRun` stages to graph nodes where evidence allows; integrate `CorrelationResult` per node; extend M5H runtime domain coverage.
- **Phase F — Partition/join depth:** join-key extraction (PySpark parity with SQL), partition-state transitions, `clusterBy`/shuffle-partitions capture, broadcast-opportunity detection.
- **Phase G — Reporting:** surface the flow graph (rendered pipeline data-flow view) in offline/online reports; keep output additive.
- **Phase H — Deduplication:** migrate overlapping regex rules to graph facts per §18, one category at a time with snapshot tests.

Each phase is independently shippable; none requires a second analysis engine.

---

*End of audit. No framework behavior was modified in producing this document.*
