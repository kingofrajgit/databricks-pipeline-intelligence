# DPIF Validation Report (Offline)

## Validation Summary

- **Contract ID**: `small_batch-contract`
- **Pipeline Name**: `small_batch`
- **Validation Timestamp**: `2026-09-17T09-38-43`
- **Environment**: `development`
- **Quality Score**: **67.3/100** (NOT_PRODUCTION_READY)
- **Confidence**: `LOW`
- **Decision Sufficiency**: `FALSE`
- **Final Decision**: **`NOT_PRODUCTION_READY`**

## Evidence Summary

| Evidence Type | Status | Provenance | Details |
|:---|:---:|:---:|:---|
| Contract | `STATIC` | `CONTRACT_FILE` | `small_batch-contract` |
| Source Definition | `DECLARED` | `CONTRACT` | `abfss://landing@acct.dfs.core.windows.net/small_batch/` |
| Target Definition | `DECLARED` | `CONTRACT` | `abfss://curated@acct.dfs.core.windows.net/small_batch/` |

## Checkpoint Results (CP-001..CP-024)

| Checkpoint | Name | Status | Severity | Score | Findings |
|:---|:---|:---:|:---:|:---:|:---:|
| `CP-001` | Source Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-003` | Schema Drift Validation | `UNKNOWN` | `INFO` | 0.0 | 0 |
| `CP-004` | Code Validation | `PASS` | `INFO` | 0.0 | 0 |
| `CP-007` | Data Volume Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-008` | Performance Validation | `UNKNOWN` | `INFO` | 0.0 | 0 |
| `CP-009` | Cluster Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-010` | Scalability Validation | `WARN` | `MEDIUM` | 1.0 | 5 |
| `CP-011` | Job Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-012` | Incremental Processing Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-013` | Error Handling Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-014` | Retry Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-015` | Restartability Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-016` | Idempotency Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-019` | Cost Validation | `UNKNOWN` | `INFO` | 0.0 | 0 |
| `CP-020` | Security Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-021` | Governance Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-022` | Data Quality Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-023` | SLA Validation | `UNKNOWN` | `INFO` | 0.0 | 0 |
| `CP-024` | Production Readiness | **`FAIL`** | `HIGH` | 0.7 | 1 |

## Developer Implementation Forensics (M5E)

- **Overall Status**: `WARN`
- **Evaluated Dimensions**: 9
- **Findings Count**: 1

## Rerun / Idempotency Forensics (M5F)

- **Overall Status**: `UNKNOWN`
- **Idempotency Status**: `UNKNOWN`
- **Duplicate Risk**: `INFO`
- **Data Loss Risk**: `INFO`
- **Total Rerun Findings**: 2

## Three-Layer Alignment Forensics (M5G)

- **Overall Status**: `FAIL`
- **Drift Severity**: `BLOCKING`
- **Blocking Drift**: `YES`
- **Total Drifts**: 1

## Evidence Sufficiency (M5H)

- **Coverage Score**: 64.1%
- **Confidence Level**: `LOW`
- **Decision Sufficiency**: `FALSE`
- **Critical Missing Evidence**:
  - Observed runtime duration telemetry
  - Explicit contract key uniqueness declaration or source deduplication key
  - Databricks Unit (DBU) consumption rates and pricing tier metadata
  - Cluster specification containing node types, worker count or autoscale range
  - Observed runtime execution duration and task telemetry
  - At least 2 historical execution runs for trend and regression analysis
  - Explicit contract uniqueness declaration or source deduplication key evidence

## Production Decision (CP-FINAL & M5I)

- **Final Decision**: **`NOT_PRODUCTION_READY`**
- **Quality Score**: 67.3/100
- **Confidence**: `LOW`

### Blockers (4)

1. `[CRITICAL]` **Blocking Configuration Drift: target_storage_format** (Source: `ALIGN-TARGET_STORAGE_FORMAT`)
   - *Description*: Expected='Format: delta' vs Implemented='Formats: parquet, delta' vs Actual='UNKNOWN (table metadata uninspected)'
   - *Resolution*: Align target_storage_format between pipeline contract, implementation, and Databricks runtime.
2. `[CRITICAL]` **Configuration Drift: target_storage_format** (Source: `ALIGN-TGT-001`)
   - *Description*: Configuration divergence across layers on target_storage_format: Expected='Format: delta', Implemented='Formats: parquet, delta', Actual='UNKNOWN (table metadata uninspected)'.
   - *Resolution*: Remediate Configuration Drift: target_storage_format: Runtime behavior and dependencies will diverge from declared pipeline contract.
3. `[HIGH]` **High Network Shuffle & Spill Risk** (Source: `XDOM-003`)
   - *Description*: Observed or projected shuffle volume exceeds safe network thresholds, causing executor memory spill to disk and stage stragglers.
   - *Resolution*: Remediate High Network Shuffle & Spill Risk: Enable Adaptive Query Execution (AQE), co-partition datasets on join keys, and avoid wide transformations before filtering.
4. `[HIGH]` **Quality score (67.3/100) is below the minimum threshold (80.0/100).** (Source: `CP-024-DECISION`)
   - *Description*: Quality score (67.3/100) is below the minimum threshold (80.0/100).
   - *Resolution*: Enable Adaptive Query Execution (AQE), co-partition datasets on join keys, and avoid wide transformations before filtering.

### Top Operational & Architectural Risks (5)

1. `[CRITICAL]` **Configuration Drift: target_storage_format** (`CONFIGURATION_DRIFT`)
   - *Consequence*: Runtime behavior and dependencies will diverge from declared pipeline contract.
2. `[HIGH]` **High Network Shuffle & Spill Risk** (`OPERATIONAL`)
   - *Consequence*: Enable Adaptive Query Execution (AQE), co-partition datasets on join keys, and avoid wide transformations before filtering.
3. `[MEDIUM]` **Peak Volume Capacity Risk: WARN** (`SCALABILITY`)
   - *Consequence*: Workload surges may cause heavy executor memory spill, severe performance degradation, or cluster out-of-memory failures.
4. `[MEDIUM]` **Insufficient Evidence: COST_VIABILITY** (`EVIDENCE`)
   - *Consequence*: Pipeline behavior under production conditions cannot be certified safe.
5. `[MEDIUM]` **Insufficient Evidence: IDEMPOTENCY_SAFETY** (`EVIDENCE`)
   - *Consequence*: Pipeline behavior under production conditions cannot be certified safe.

### Required Actions

- `[PP0]` **Resolve Blocker: Blocking Configuration Drift: target_storage_format**: Expected='Format: delta' vs Implemented='Formats: parquet, delta' vs Actual='UNKNOWN (table metadata uninspected)'
- `[PP0]` **Resolve Blocker: Configuration Drift: target_storage_format**: Configuration divergence across layers on target_storage_format: Expected='Format: delta', Implemented='Formats: parquet, delta', Actual='UNKNOWN (table metadata uninspected)'.
- `[PP0]` **Resolve Blocker: High Network Shuffle & Spill Risk**: Observed or projected shuffle volume exceeds safe network thresholds, causing executor memory spill to disk and stage stragglers.
- `[PP0]` **Resolve Blocker: Quality score (67.3/100) is below the minimum threshold (80.0/100).**: Quality score (67.3/100) is below the minimum threshold (80.0/100).
- `[PP2]` **Address Warning: Peak Volume Capacity Risk: WARN**: Evaluates contractual peak workload volume against observed runtime capacity and cluster headroom.
- `[PP2]` **Address Warning: Peak Workload Capacity Headroom Risk**: Contractual peak ingestion burst exceeds baseline workload capacity on a fixed or constrained cluster configuration without empirical verification.
- `[PP2]` **Address Warning: Insufficient Evidence: SLA_COMPLIANCE**: SLA compliance cannot be determined without runtime duration telemetry.
- `[PP2]` **Address Warning: Insufficient Evidence: IDEMPOTENCY_SAFETY**: MERGE key stability/uniqueness unproven; idempotency cannot be certified safe.
- `[PP2]` **Address Warning: Insufficient Evidence: COST_VIABILITY**: Cost projection unavailable in offline static analysis without DBU consumption telemetry.
- `[PP3]` **Provide Telemetry Evidence**: Supply Databricks runtime execution telemetry (run duration, task metrics, shuffle bytes) to enable performance, SLA, and cost decision sufficiency.
- `[PP3]` **Provide Telemetry Evidence**: Provide at least 2 historical execution runs under varying data volumes to enable empirical runtime regression and scalability trend forensics.
- `[PP3]` **Provide Telemetry Evidence**: Provide DBU pricing tier metadata and cluster duration metrics to enable cost viability analysis.
