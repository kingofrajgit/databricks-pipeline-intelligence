# ADR-008: Runtime Performance Intelligence (Phase 7)

## Status: Accepted (Phase 7)

## Context
Phases 1–6 established DPIF foundation, offline checkpoint validation, data source intelligence, Python/PySpark AST intelligence, SQL query intelligence, and Databricks workspace/cluster environment intelligence. However, static code analysis and environment configuration alone cannot verify whether a pipeline performs efficiently at execution time. A pipeline with optimal static syntax and cluster sizing may suffer in production from severe runtime bottlenecks such as partition skew, shuffle explosions, excessive garbage collection, disk/memory spilling, straggler tasks, or SLA breaches.

Assessing production readiness requires inspecting Spark execution telemetry (jobs, stages, tasks, shuffle metrics, memory metrics, GC ratios) both from Databricks API execution runs and offline air-gapped run fixtures, without fabricating execution metrics, guessing execution timings, or compromising pipeline security.

## Decision

### 1. Strongly-Typed Runtime Domain Models
Implement robust Pydantic v2 domain models in `src/dpif/runtime/models.py`:
- `RuntimeTaskMetrics`: Task-level execution metrics (`executor_run_time_ms`, `jvm_gc_time_ms`, `input_bytes`, `output_bytes`, `shuffle_read_bytes`, `shuffle_write_bytes`, `memory_spilled_bytes`, `disk_spilled_bytes`, `cpu_time_ms`).
- `RuntimeTask`: Individual task execution record (`task_id`, `stage_id`, `index`, `attempt`, `launch_time`, `finish_time`, `duration_ms`, `status`, `host`, `metrics`).
- `RuntimeStage`: Stage-level execution record (`stage_id`, `attempt_id`, `name`, `status`, `submission_time`, `completion_time`, `duration_ms`, `tasks`, `input_bytes`, `output_bytes`, `shuffle_read_bytes`, `shuffle_write_bytes`, `memory_spill_bytes`, `disk_spill_bytes`, `failed_tasks_count`).
- `RuntimeRun`: Top-level pipeline run execution record (`run_id`, `job_id`, `start_time`, `end_time`, `duration_ms`, `status`, `cluster_id`, `stages`, `cluster_utilization`, `raw_payload`).
- `CorrelationResult`: Cross-domain correlation record linking static risks (`CODE-PYSPARK-*`, `CODE-SQL-*`) to runtime evidence.

Derived properties provide type-safe, pre-computed access:
- `stage.duration_seconds`, `stage.gc_ratio`, `stage.total_spill_bytes`, `stage.task_durations`, `stage.max_task_duration_seconds`.
- `run.total_duration_seconds`, `run.total_duration_minutes`, `run.total_shuffle_bytes`, `run.total_input_bytes`, `run.total_spill_bytes`, `run.shuffle_to_input_ratio`.

### 2. Metric Normalization & Safe Distribution Layer
Implement safe metric conversion and distribution statistics in `src/dpif/runtime/normalization.py`:
- Safe unit conversions: `bytes_to_mb`, `bytes_to_gb`, `ms_to_seconds`.
- Robust statistical functions: `calculate_distribution` computing count, min, max, mean, median (P50), P75, P90, P95, P99, and IQR with guards against zero-division and empty arrays.
- Schema normalization: `normalize_runtime_payload` accepting raw Databricks API responses, Spark REST API payloads, and test fixtures in camelCase (`stageId`, `diskBytesSpilled`, `jvmGcTime`) or snake_case (`stage_id`, `disk_spill_bytes`, `jvm_gc_time_ms`).

### 3. Pure Detector Functions & Bridge Evaluators
Implement 12 pure detector functions in `src/dpif/runtime/analyzers.py` and bridge evaluators in `src/dpif/runtime/evaluators.py`:
- `RUNTIME-PERF-001`: Excessive Stage Duration (> 300s WARN, > 900s HIGH).
- `RUNTIME-PERF-002`: High Shuffle Volume (> 50 GB WARN, > 200 GB HIGH).
- `RUNTIME-PERF-003`: Extreme Shuffle-to-Input Ratio (> 3.0x WARN, > 10.0x HIGH).
- `RUNTIME-PERF-004`: Task Duration Imbalance / Stragglers (Max-to-median duration ratio > 4.0x WARN, > 8.0x HIGH).
- `RUNTIME-PERF-005`: Partition Skew (Max-to-median bytes ratio > 5.0x WARN, > 10.0x HIGH).
- `RUNTIME-PERF-006`: Memory Spill to Disk (> 5 GB WARN, > 20 GB HIGH).
- `RUNTIME-PERF-007`: Disk Spill Volume (> 10 GB WARN, > 50 GB HIGH).
- `RUNTIME-PERF-008`: High JVM Garbage Collection Overhead (> 10% WARN, > 25% HIGH).
- `RUNTIME-PERF-009`: High Task Failure Rate (> 2% WARN, > 5% HIGH).
- `RUNTIME-PERF-010`: High Stage Failure Rate (> 0% WARN, > 10% HIGH).
- `RUNTIME-PERF-011`: Low Cluster Utilization / Over-provisioning (CPU < 30% WARN).
- `RUNTIME-PERF-012`: Long-Tail Task Risk (P95-to-median task ratio > 3.0x WARN).

Bridge evaluators construct immutable `Finding` objects attached to structured `EvidenceRecord` elements with full provenance.

### 4. 12 Externalized YAML Rules (`rules/runtime/`)
Rules are externalized in `rules/runtime/RUNTIME-PERF-001.yaml` through `RUNTIME-PERF-012.yaml`, parameterizing thresholds (such as warning/failure byte volumes, ratios, and percentages) to allow workspace and contract overrides without code changes.

### 5. Static ↔ Runtime Correlation Engine
Implement the cross-domain correlation engine in `src/dpif/runtime/correlation.py`:
- Bridges static code analysis findings (`CODE-PYSPARK-001`, `CODE-PYSPARK-005`, `CODE-PYSPARK-006`, `CODE-PYSPARK-007`, `CODE-SQL-001`, `CODE-SQL-003`, etc.) with actual runtime performance findings.
- Reconciles 4 formal correlation states:
  1. `STATIC_RISK_CONFIRMED`: Static risk detected AND corresponding runtime performance failure observed.
  2. `RUNTIME_SUPPORTS_STATIC_RISK`: Static risk detected AND runtime metrics exhibit warning/degradation symptoms.
  3. `STATIC_RISK_NOT_OBSERVED_IN_SUPPLIED_RUN`: Static risk detected BUT runtime metrics for the supplied execution run are healthy (e.g. data size in this run was insufficient to trigger skew).
  4. `RUNTIME_EVIDENCE_UNAVAILABLE`: Static risk detected BUT no runtime telemetry was provided or available.
- Correlation never mutates original findings; all correlation results are emitted as distinct `CorrelationResult` structures.

### 6. Checkpoint Engine Integration & Strict UNKNOWN Semantics
- **CP-008 (Performance Validation)**: Category `performance`, weight `1.0` in readiness scoring. Evaluates runtime performance rules against the provided `RuntimeRun`.
- **CP-023 (SLA Validation)**: Category `sla`. Evaluates contractual runtime limits (`contract.sla.max_runtime_minutes`). If actual execution exceeds SLA, emits `HIGH` severity finding and `FAIL` checkpoint status.
- **Strict UNKNOWN Semantics**:
  - Missing execution run data produces `UNKNOWN` checkpoint status with confidence `0.0`.
  - Missing specific metrics (e.g. missing input bytes for shuffle ratio, missing CPU metrics for cluster utilization, missing executor runtime for GC) strictly produces `UNKNOWN` status and never fabricates 0 values or passes.

### 7. Dual Connector Runtime Loading & CLI
- `LiveDatabricksConnector.get_run()`: Fetches job run execution details via Databricks Jobs API 2.1 (`/api/2.1/jobs/runs/get`).
- `OfflineDatabricksConnector.get_runtime_run()`: Loads offline JSON run fixtures from `tests/fixtures/runtime/`.
- CLI `dpif validate --runtime-run <path_or_id>`: Enables passing execution runs to pipeline validation, displaying performance findings, SLA compliance, and cross-domain static/runtime correlation matrices.

## Consequences
- 78 new automated tests (6 model unit tests, 60 rule unit tests, 6 correlation unit tests, 6 integration tests).
- 465 total automated tests pass green across the entire repository with zero regressions.
- All Phase 1–6 behaviors and existing fixtures remain completely backward compatible.
- Quality gates verified: `ruff` clean (0 errors) and `mypy` clean (0 errors across 51 source files).
