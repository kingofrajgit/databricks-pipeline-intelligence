# ADR-007: Databricks Environment Intelligence (Phase 6)

## Status: Accepted (Phase 6)

## Context
Phases 1–5 established DPIF foundation, offline checkpoint validation, data source intelligence, Python/PySpark AST intelligence, and SQL query intelligence. However, enterprise Databricks pipelines operate within a configured cloud environment: clusters, runtime engines, workflows/jobs, tasks, schedules, and policies. A pipeline may be clean in code and data contract, but fail in production if scheduled on an outdated Databricks Runtime (DBR), configured with non-positive or inverted autoscaling ranges, lacking task failure retries, running unbounded concurrent executions, or failing to assign corporate cluster policies.

Assessing production readiness requires inspecting Databricks environment configuration metadata both in live workspace environments and in offline air-gapped CI/CD environments without fabricating runtime execution metrics or compromising credentials.

## Decision

### 1. Unified Databricks Domain Models
Implement robust Pydantic v2 domain models in `src/dpif/discovery/models.py`:
- `DatabricksWorkspace`: Workspace URL, status, cloud provider, deployed runtimes.
- `DatabricksJob`: Job ID, name, creator, schedule, concurrency, retries, timeout, tasks, job clusters.
- `DatabricksTask`: Task key, description, task type, script path, dependencies (`TaskDependency`), cluster ID, retries, timeouts.
- `DatabricksCluster`: Cluster ID, name, spark_version, node types, worker count, autoscaling configuration, Photon engine status, policy ID, autotermination, spark_conf, and cluster source.
- `ClusterPolicy`: Policy ID, name, definition rules (`fixed`, `range`, `regex`, `allowlist`).
- `DatabricksPipeline`: Delta Live Tables (DLT) configuration metadata.
- `PermissionConfiguration`: Object ACLs and access control entries.

Derived properties provide clean, typed access without repetitive dict parsing:
- `cluster.is_autoscaling`, `cluster.is_photon`, `cluster.effective_workers`, `cluster.dbr_major_version`.
- `job.is_paused`, `job.task_keys`, `job.task_dependencies`.

### 2. Dual Connector Architecture: Live & Offline
- **LiveDatabricksConnector** (`src/dpif/connectors/live.py`):
  - Integrates with Databricks REST API 2.0 / 2.1 (`/api/2.1/jobs/get`, `/api/2.0/clusters/get`, `/api/2.0/policies/clusters/get`, `/api/2.0/permissions/*`, `/api/2.0/clusters/spark-versions`).
  - Strict token masking: tokens are sanitized from all log lines and exception traces.
  - Robust HTTP handling: 404 returns `None`, 401/403 raises `DatabricksApiError("Authentication failed")`, 429 raises rate-limit error, network timeouts raise timed-out errors.
  - Zero speculative runtime metrics fabrication: live connector fetches only configuration and metadata.
- **OfflineDatabricksConnector** (`src/dpif/connectors/offline.py`):
  - Loads workspace fixtures (`.json`, `.yaml`) from `tests/fixtures/databricks/`.
  - Tags all loaded payloads with `evidence_source: FIXTURE` and `_connector: offline-fixture`.
  - Never fabricates fake job execution runs.

### 3. Three-Way Evaluation: Expected vs Implemented vs Actual
Evaluators reconcile three distinct tiers of truth:
1. **Expected**: What the pipeline contract declares (e.g. required DBR 15.4, 2 retries, daily schedule at 02:00, Photon enabled).
2. **Implemented**: What the source code and job definition configure (e.g. script path, task dependencies).
3. **Actual**: What the Databricks cluster and job runtime configuration provide in the workspace.

Discrepancies between any of these tiers produce structured findings with clear evidence.

### 4. 12 Externalized Configuration Rules (`rules/config/`)
- **Job Rules** (`CONFIG-JOB-001` through `CONFIG-JOB-006`):
  - `CONFIG-JOB-001`: Missing Retry Configuration (ensures production tasks specify resilience).
  - `CONFIG-JOB-002`: Missing Task Timeout (prevents hanging executions).
  - `CONFIG-JOB-003`: Excessive Retries (> 5 retries warning on resource drains).
  - `CONFIG-JOB-004`: Unbounded Concurrency (`max_concurrent_runs > 1` on scheduled pipelines).
  - `CONFIG-JOB-005`: Schedule / Cron Mismatch (checks pause status and execution hour).
  - `CONFIG-JOB-006`: Job Source Mismatch (ensures job points to intended contract script).
- **Cluster Rules** (`CONFIG-CLUSTER-001` through `CONFIG-CLUSTER-006`):
  - `CONFIG-CLUSTER-001`: Outdated / Unapproved Databricks Runtime (DBR < 14.3 LTS).
  - `CONFIG-CLUSTER-002`: Missing Autoscaling on Variable Workloads.
  - `CONFIG-CLUSTER-003`: Invalid / Dangerous Autoscaling Range (`min > max`, `max <= 0`, ratio > 10x).
  - `CONFIG-CLUSTER-004`: Missing Photon on Heavy Workloads / Contract Requirement.
  - `CONFIG-CLUSTER-005`: Cluster Policy Violation (enforces fixed, range, and pattern policy rules).
  - `CONFIG-CLUSTER-006`: Missing Auto-Termination on All-Purpose Clusters.

### 5. Checkpoint Engine Routing & UNKNOWN Semantics
- Checkpoint `CP-009` (Cluster Validation, category `cluster`) runs `CONFIG-CLUSTER-*` rules.
- Checkpoint `CP-011` (Job Validation, category `job`) runs `CONFIG-JOB-*` rules.
- Checkpoint dependencies: If cluster/job configuration metadata is missing, `CP-009` and `CP-011` evaluate to `UNKNOWN` with confidence 0.0.
- Dependent checkpoints (`CP-014` Retry, `CP-015` Restartability, `CP-016` Idempotency) properly evaluate based on `CP-011` job presence.
- Strict `UNKNOWN` semantics: Absence of workspace credentials or offline fixtures never silently resolves to `PASS` or speculative `FAIL`.

## Consequences
- 84 new automated tests (7 model unit tests, 13 connector unit tests, 60 rule unit tests, 4 integration tests).
- 387 total automated tests pass green across the entire repository with zero regressions.
- All Phase 1–5 behaviors and existing fixtures remain completely backward compatible.
- Quality gates verified: `ruff` clean (0 errors) and `mypy` clean (0 errors across 46 source files).
