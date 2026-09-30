# DPIF Validation Report (Offline)

## Validation Summary

- **Contract ID**: `concurrency_retry_failure_pipeline-contract`
- **Pipeline Name**: `concurrency_retry_failure_pipeline`
- **Validation Timestamp**: `2026-09-30T13-06-06`
- **Environment**: `production`
- **Quality Score**: **60.0/100** (NOT_PRODUCTION_READY)
- **Confidence**: `MEDIUM`
- **Decision Sufficiency**: `FALSE`
- **Final Decision**: **`NOT_PRODUCTION_READY`**

## Evidence Summary

| Evidence Type | Status | Provenance | Details |
|:---|:---:|:---:|:---|
| Contract | `STATIC` | `CONTRACT_FILE` | `concurrency_retry_failure_pipeline-contract` |
| Source Definition | `DECLARED` | `CONTRACT` | `abfss://landing@acct.dfs.core.windows.net/events_stream/` |
| Target Definition | `DECLARED` | `CONTRACT` | `abfss://curated@acct.dfs.core.windows.net/events_unprotected/` |

## Checkpoint Results (CP-001..CP-024)

| Checkpoint | Name | Status | Severity | Score | Findings |
|:---|:---|:---:|:---:|:---:|:---:|
| `CP-001` | Source Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-003` | Schema Drift Validation | `UNKNOWN` | `INFO` | 0.0 | 0 |
| `CP-004` | Code Validation | `WARN` | `MEDIUM` | 0.0 | 1 |
| `CP-007` | Data Volume Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-008` | Performance Validation | `UNKNOWN` | `INFO` | 0.0 | 0 |
| `CP-009` | Cluster Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-010` | Scalability Validation | `PASS` | `INFO` | 1.0 | 4 |
| `CP-011` | Job Validation | `WARN` | `MEDIUM` | 1.0 | 1 |
| `CP-012` | Incremental Processing Validation | `WARN` | `MEDIUM` | 0.6 | 0 |
| `CP-013` | Error Handling Validation | `WARN` | `MEDIUM` | 0.6 | 0 |
| `CP-014` | Retry Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-015` | Restartability Validation | `WARN` | `MEDIUM` | 0.6 | 0 |
| `CP-016` | Idempotency Validation | `WARN` | `MEDIUM` | 0.6 | 0 |
| `CP-019` | Cost Validation | `UNKNOWN` | `INFO` | 0.0 | 0 |
| `CP-020` | Security Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-021` | Governance Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-022` | Data Quality Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-023` | SLA Validation | `UNKNOWN` | `INFO` | 0.0 | 0 |
| `CP-024` | Production Readiness | **`FAIL`** | `HIGH` | 0.6 | 1 |

## Developer Implementation Forensics (M5E)

- **Overall Status**: `PASS`
- **Evaluated Dimensions**: 9
- **Findings Count**: 0

## Rerun / Idempotency Forensics (M5F)

- **Overall Status**: `WARN`
- **Idempotency Status**: `WARN`
- **Duplicate Risk**: `HIGH`
- **Data Loss Risk**: `INFO`
- **Total Rerun Findings**: 7

## Three-Layer Alignment Forensics (M5G)

- **Overall Status**: `FAIL`
- **Drift Severity**: `BLOCKING`
- **Blocking Drift**: `YES`
- **Total Drifts**: 1

## Evidence Sufficiency (M5H)

- **Coverage Score**: 67.7%
- **Confidence Level**: `MEDIUM`
- **Decision Sufficiency**: `FALSE`
- **Critical Missing Evidence**:
  - Observed runtime duration telemetry
  - Databricks Unit (DBU) consumption rates and pricing tier metadata
  - Cluster specification containing node types, worker count or autoscale range
  - Observed runtime execution duration and task telemetry
  - At least 2 historical execution runs for trend and regression analysis

## Production Decision (CP-FINAL & M5I)

- **Final Decision**: **`NOT_PRODUCTION_READY`**
- **Quality Score**: 60.0/100
- **Confidence**: `MEDIUM`

### Blockers (4)

1. `[CRITICAL]` **Blocking Configuration Drift: target_storage_format** (Source: `ALIGN-TARGET_STORAGE_FORMAT`)
   - *Description*: Expected='Format: delta' vs Implemented='Formats: parquet' vs Actual='UNKNOWN (table metadata uninspected)'
   - *Resolution*: Align target_storage_format between pipeline contract, implementation, and Databricks runtime.
2. `[CRITICAL]` **Configuration Drift: target_storage_format** (Source: `ALIGN-TGT-001`)
   - *Description*: Configuration divergence across layers on target_storage_format: Expected='Format: delta', Implemented='Formats: parquet', Actual='UNKNOWN (table metadata uninspected)'.
   - *Resolution*: Remediate Configuration Drift: target_storage_format: Runtime behavior and dependencies will diverge from declared pipeline contract.
3. `[HIGH]` **High Network Shuffle & Spill Risk** (Source: `XDOM-003`)
   - *Description*: Observed or projected shuffle volume exceeds safe network thresholds, causing executor memory spill to disk and stage stragglers.
   - *Resolution*: Remediate High Network Shuffle & Spill Risk: Enable Adaptive Query Execution (AQE), co-partition datasets on join keys, and avoid wide transformations before filtering.
4. `[HIGH]` **Quality score (60.0/100) is below the minimum threshold (80.0/100).** (Source: `CP-024-DECISION`)
   - *Description*: Quality score (60.0/100) is below the minimum threshold (80.0/100).
   - *Resolution*: Enable Adaptive Query Execution (AQE), co-partition datasets on join keys, and avoid wide transformations before filtering.

### Top Operational & Architectural Risks (5)

1. `[CRITICAL]` **Configuration Drift: target_storage_format** (`CONFIGURATION_DRIFT`)
   - *Consequence*: Runtime behavior and dependencies will diverge from declared pipeline contract.
2. `[HIGH]` **High Network Shuffle & Spill Risk** (`OPERATIONAL`)
   - *Consequence*: Enable Adaptive Query Execution (AQE), co-partition datasets on join keys, and avoid wide transformations before filtering.
3. `[MEDIUM]` **Potential Large Shuffle** (`IMPLEMENTATION`)
   - *Consequence*: Operational risk associated with Potential Large Shuffle.
4. `[MEDIUM]` **Excessive Concurrent Runs** (`OPERATIONAL`)
   - *Consequence*: Operational risk associated with Excessive Concurrent Runs.
5. `[MEDIUM]` **Insufficient Evidence: COST_VIABILITY** (`EVIDENCE`)
   - *Consequence*: Pipeline behavior under production conditions cannot be certified safe.

### Required Actions

- `[PP0]` **Resolve Blocker: Blocking Configuration Drift: target_storage_format**: Expected='Format: delta' vs Implemented='Formats: parquet' vs Actual='UNKNOWN (table metadata uninspected)'
- `[PP0]` **Resolve Blocker: Configuration Drift: target_storage_format**: Configuration divergence across layers on target_storage_format: Expected='Format: delta', Implemented='Formats: parquet', Actual='UNKNOWN (table metadata uninspected)'.
- `[PP0]` **Resolve Blocker: High Network Shuffle & Spill Risk**: Observed or projected shuffle volume exceeds safe network thresholds, causing executor memory spill to disk and stage stragglers.
- `[PP0]` **Resolve Blocker: Quality score (60.0/100) is below the minimum threshold (80.0/100).**: Quality score (60.0/100) is below the minimum threshold (80.0/100).
- `[PP2]` **Address Warning: Potential Large Shuffle**: Flag shuffle-class operations (groupBy, distinct, repartition, orderBy, large joins, windows) over sizable inputs as potential shuffle risk. Static analysis cannot prove shuffle volume, so findings are WARN only.

- `[PP2]` **Address Warning: Excessive Concurrent Runs**: Detects unsafe concurrency on scheduled production pipelines.
- `[PP2]` **Address Warning: Insufficient Evidence: SLA_COMPLIANCE**: SLA compliance cannot be determined without runtime duration telemetry.
- `[PP2]` **Address Warning: Insufficient Evidence: COST_VIABILITY**: Cost projection unavailable in offline static analysis without DBU consumption telemetry.
- `[PP3]` **Provide Telemetry Evidence**: Supply Databricks runtime execution telemetry (run duration, task metrics, shuffle bytes) to enable performance, SLA, and cost decision sufficiency.
- `[PP3]` **Provide Telemetry Evidence**: Provide at least 2 historical execution runs under varying data volumes to enable empirical runtime regression and scalability trend forensics.
- `[PP3]` **Provide Telemetry Evidence**: Provide DBU pricing tier metadata and cluster duration metrics to enable cost viability analysis.
