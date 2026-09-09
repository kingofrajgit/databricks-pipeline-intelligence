# ADR-009: Scalability Intelligence (Phase 8)

## Status: Accepted (Phase 8)

## Context
Phases 1–7 established DPIF foundation, offline checkpoint validation, source & data intelligence, Python/PySpark AST intelligence, SQL query intelligence, Databricks cluster/workspace environment intelligence, and runtime performance intelligence. However, assessing whether a pipeline can safely scale to larger data volumes or operational spikes requires proactive scalability intelligence. A pipeline running cleanly on 100 GB today may fail catastrophically at 1 TB or 5 TB due to driver memory exhaustion from unconstrained collection, explosive cartesian joins, unpartitioned large table scans, metastore listing bottlenecks from millions of tiny files, memory spill to disk, or super-linear run durations.

Assessing scalability readiness requires:
1. Deterministic scenario modeling (baseline, expected, peak, 3x growth, 5x growth) under explicit assumptions without speculative runtime guarantees.
2. Historical trend analysis across empirical execution runs (linear, sub-linear, super-linear, exponential, unknown).
3. Dedicated checkpoint validation via `CP-010 Scalability Validation`.
4. Strict evidence provenance distinguishing `RUNTIME`, `STATIC`, `CONTRACT`, `DATABRICKS_METADATA`, `PROJECTED`, and `UNKNOWN`.

## Decision

### 1. Strongly-Typed Scalability Domain Models
Implement robust Pydantic v2 domain models in `src/dpif/scalability/models.py`:
- `EvidenceProvenance`: Explicit provenance enumeration (`CONTRACT`, `STATIC`, `DATABRICKS_API`, `DATABRICKS_METADATA`, `RUNTIME`, `EVENT_LOG`, `FIXTURE`, `PROJECTED`, `UNKNOWN`).
- `ScenarioType`: Workload scenario categories (`BASELINE`, `EXPECTED`, `PEAK`, `GROWTH_1`, `GROWTH_2`, `CUSTOM`).
- `ProjectionMethod`: Mathematical projection models (`LINEAR`, `QUADRATIC`, `LOG_LINEAR`, `AMDAHL`, `EMPIRICAL_REGRESSION`, `CONSERVATIVE_BOUND`).
- `TrendDirection`: Empirical scaling trend directions (`LINEAR`, `SUB_LINEAR`, `SUPER_LINEAR`, `EXPONENTIAL`, `DEGRADING`, `INSUFFICIENT_DATA`, `UNKNOWN`).
- `RiskLevel`: Scalability risk categories (`SAFE`, `LOW_RISK`, `MODERATE_RISK`, `HIGH_RISK`, `CRITICAL_RISK`, `UNKNOWN`).
- `ScalabilityScenario`: Concrete workload volume evaluation scenario.
- `ScalabilityProjection`: Deterministic metric projection with explicit assumptions, baseline values, and bounded confidence.
- `ScalabilityObservation`: Empirical observation point from actual run execution telemetry with safe attribute aliases (`volume_gb`, `input_volume_gb`, `duration_minutes`, `duration_seconds`).
- `ScalabilityTrend`: Historical scaling trajectory computed across 2+ observation points.
- `ScalabilityAssessment`: Holistic pipeline scalability evaluation summarizing risk, scenarios, projections, trends, and findings.

### 2. Deterministic Scenario Generator & Linear Projection Engine
Implement the scenario generator and projection engine in `src/dpif/scalability/engine.py`:
- `extract_baseline_volume`: Resolves baseline volume hierarchically from contract, data profile, or runtime telemetry, recording exact source provenance.
- `generate_workload_scenarios`: Deterministically generates standard scenarios:
  - `BASELINE`: Current observed volume (1.0x).
  - `EXPECTED`: Contractual expected daily volume.
  - `PEAK`: Contractual peak daily volume.
  - `GROWTH_1`: 3.0x expansion factor.
  - `GROWTH_2`: 5.0x expansion factor.
- `project_metric_linear`: Deterministically projects metrics (`duration_minutes`, `shuffle_volume_gb`, `file_count`, `cost_usd`) from baseline to target volume under explicit linear assumptions with distance-decayed confidence (`0.90 / sqrt(scaling_factor)`).

### 3. Historical Trend Analyzer
Implement empirical trend analysis across multi-run execution history in `src/dpif/scalability/engine.py`:
- Minimum requirement: 2+ observation runs. Fewer than 2 runs strictly yields `TrendDirection.INSUFFICIENT_DATA` with confidence `0.0`.
- Linear scaling: $\Delta \text{duration} / \Delta \text{volume} \in [0.7, 1.3]$.
- Sub-linear scaling: Scaling elasticity $< 0.7$ (healthy efficiency gains at scale).
- Super-linear scaling: Scaling elasticity $> 1.3$ (degrading efficiency, potential skew or shuffle bottlenecks).
- Exponential / Escalating failure rate: Escalating task or stage failure rates across runs flags severe operational risk.

### 4. 14 Detector Functions & Bridge Evaluators
Implement pure analyzer detectors in `src/dpif/scalability/analyzers.py` and bridge evaluators in `src/dpif/scalability/evaluators.py`:
- `SCALABILITY-001`: Baseline Volume Undefined (Contract missing volume declaration).
- `SCALABILITY-002`: Peak Volume Capacity Risk (Peak multiplier $> 2.0$x without validated cluster capacity).
- `SCALABILITY-003`: Data Growth Risk (Annual growth $> 20\%$ or projected volume $> 1,000$ GB).
- `SCALABILITY-004`: Small File Proliferation Risk (Projected file count $> 5,000$ with avg size $< 32$ MB).
- `SCALABILITY-005`: Partition Scalability Risk (Unpartitioned $> 100$ GB, overpartitioned $> 10,000$, or partition skew $> 3.0$x).
- `SCALABILITY-006`: Driver Scalability Risk (Driver materialization `collect`/`toPandas` at volume $> 50$ GB).
- `SCALABILITY-007`: Shuffle Scalability Risk (Projected shuffle $> 200$ GB based on runtime telemetry).
- `SCALABILITY-008`: Join Scalability Risk (Cartesian cross-join detected at volume $> 10$ GB).
- `SCALABILITY-009`: Aggregation Scalability Risk (High-cardinality `SELECT DISTINCT` / `dropDuplicates` at volume $> 100$ GB).
- `SCALABILITY-010`: Cluster Capacity Risk (Workers unable to process target volume within memory headroom).
- `SCALABILITY-011`: Autoscaling Headroom Risk (Fixed-size cluster facing $> 2.0$x peak or growth bursts).
- `SCALABILITY-012`: SLA Scalability Risk (Projected duration exceeds contract SLA threshold).
- `SCALABILITY-013`: Historical Superlinear Degradation (Multi-run trend reveals superlinear scaling).
- `SCALABILITY-014`: Historical Failure Rate Escalation (Task failure rate escalates with workload size).

### 5. Checkpoint Engine Integration
- **CP-010 (Scalability Validation)**:
  - Category: `scalability`.
  - Weight in readiness scoring: `1.0` (integrated into `DEFAULT_WEIGHTS`).
  - Evaluates all 14 `SCALABILITY-` rules using contract, data profile, AST code analysis, cluster configuration, runtime run, and historical runs.
  - Strict `UNKNOWN` semantics: If required evidence is missing, emits `CheckpointStatus.UNKNOWN` with confidence `0.0`.

### 6. CLI Integration
- Added `--historical-runs` option to `dpif validate` accepting JSON/YAML files containing lists of historical run telemetry.
- Checkpoint table updated with `Scalability` line for `CP-010`.
- Dedicated `SCALABILITY INTELLIGENCE` section displaying overall assessment, historical scaling trends, and standard projected scenarios.

## Consequences
- 74 new automated tests added (10 model unit tests, 54 rule unit tests, 6 cross-phase unit tests, 4 integration tests).
- All 539 tests across the repository pass 100% green with zero regressions.
- Static, runtime, and environmental intelligence seamlessly synthesized to evaluate future workload growth without speculative runtime guarantees.
- Quality gates fully verified: `ruff` clean (0 errors) and `mypy` clean (0 errors in 56 source files).
