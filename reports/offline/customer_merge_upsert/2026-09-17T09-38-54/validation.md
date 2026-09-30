# DPIF Validation Report (Offline)

## Validation Summary

- **Contract ID**: `customer_merge_upsert-contract`
- **Pipeline Name**: `customer_merge_upsert`
- **Validation Timestamp**: `2026-09-17T09-38-54`
- **Environment**: `production`
- **Quality Score**: **63.3/100** (NOT_PRODUCTION_READY)
- **Confidence**: `MEDIUM`
- **Decision Sufficiency**: `FALSE`
- **Final Decision**: **`NOT_PRODUCTION_READY`**

## Evidence Summary

| Evidence Type | Status | Provenance | Details |
|:---|:---:|:---:|:---|
| Contract | `STATIC` | `CONTRACT_FILE` | `customer_merge_upsert-contract` |
| Source Definition | `DECLARED` | `CONTRACT` | `abfss://landing@acct.dfs.core.windows.net/customers/` |
| Target Definition | `DECLARED` | `CONTRACT` | `abfss://curated@acct.dfs.core.windows.net/customers/` |

## Checkpoint Results (CP-001..CP-024)

| Checkpoint | Name | Status | Severity | Score | Findings |
|:---|:---|:---:|:---:|:---:|:---:|
| `CP-001` | Source Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-003` | Schema Drift Validation | `UNKNOWN` | `INFO` | 0.0 | 0 |
| `CP-004` | Code Validation | `WARN` | `MEDIUM` | 0.0 | 3 |
| `CP-007` | Data Volume Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-008` | Performance Validation | `UNKNOWN` | `INFO` | 0.0 | 0 |
| `CP-009` | Cluster Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-010` | Scalability Validation | `WARN` | `MEDIUM` | 1.0 | 7 |
| `CP-011` | Job Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-012` | Incremental Processing Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-013` | Error Handling Validation | `WARN` | `MEDIUM` | 0.6 | 0 |
| `CP-014` | Retry Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-015` | Restartability Validation | `WARN` | `MEDIUM` | 0.6 | 0 |
| `CP-016` | Idempotency Validation | `PASS` | `INFO` | 1.0 | 0 |
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

- **Overall Status**: `PASS`
- **Idempotency Status**: `PASS`
- **Duplicate Risk**: `INFO`
- **Data Loss Risk**: `INFO`
- **Total Rerun Findings**: 7

## Three-Layer Alignment Forensics (M5G)

- **Overall Status**: `UNKNOWN`
- **Drift Severity**: `NONE`
- **Blocking Drift**: `NO`
- **Total Drifts**: 0

## Evidence Sufficiency (M5H)

- **Coverage Score**: 69.8%
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
- **Quality Score**: 63.3/100
- **Confidence**: `MEDIUM`

### Blockers (2)

1. `[HIGH]` **High Network Shuffle & Spill Risk** (Source: `XDOM-003`)
   - *Description*: Observed or projected shuffle volume exceeds safe network thresholds, causing executor memory spill to disk and stage stragglers.
   - *Resolution*: Remediate High Network Shuffle & Spill Risk: Enable Adaptive Query Execution (AQE), co-partition datasets on join keys, and avoid wide transformations before filtering.
2. `[HIGH]` **Quality score (63.3/100) is below the minimum threshold (80.0/100).** (Source: `CP-024-DECISION`)
   - *Description*: Quality score (63.3/100) is below the minimum threshold (80.0/100).
   - *Resolution*: Enable Adaptive Query Execution (AQE), co-partition datasets on join keys, and avoid wide transformations before filtering.

### Top Operational & Architectural Risks (5)

1. `[HIGH]` **High Network Shuffle & Spill Risk** (`OPERATIONAL`)
   - *Consequence*: Enable Adaptive Query Execution (AQE), co-partition datasets on join keys, and avoid wide transformations before filtering.
2. `[MEDIUM]` **Potential Large Shuffle** (`IMPLEMENTATION`)
   - *Consequence*: Operational risk associated with Potential Large Shuffle.
3. `[MEDIUM]` **Potential Expensive Deduplication** (`IMPLEMENTATION`)
   - *Consequence*: Operational risk associated with Potential Expensive Deduplication.
4. `[MEDIUM]` **Expensive DISTINCT** (`IMPLEMENTATION`)
   - *Consequence*: Operational risk associated with Expensive DISTINCT.
5. `[MEDIUM]` **Peak Volume Capacity Risk: WARN** (`SCALABILITY`)
   - *Consequence*: Workload surges may cause heavy executor memory spill, severe performance degradation, or cluster out-of-memory failures.

### Required Actions

- `[PP0]` **Resolve Blocker: High Network Shuffle & Spill Risk**: Observed or projected shuffle volume exceeds safe network thresholds, causing executor memory spill to disk and stage stragglers.
- `[PP0]` **Resolve Blocker: Quality score (63.3/100) is below the minimum threshold (80.0/100).**: Quality score (63.3/100) is below the minimum threshold (80.0/100).
- `[PP2]` **Address Warning: Potential Large Shuffle**: Flag shuffle-class operations (groupBy, distinct, repartition, orderBy, large joins, windows) over sizable inputs as potential shuffle risk. Static analysis cannot prove shuffle volume, so findings are WARN only.

- `[PP2]` **Address Warning: Potential Expensive Deduplication**: Flag distinct()/dropDuplicates() over large inputs as potential cost/performance risk, using data evidence where available.

- `[PP2]` **Address Warning: Expensive DISTINCT**: Detect DISTINCT / COUNT(DISTINCT ...) on large inputs where the deduplication shuffle cost may be significant.

- `[PP2]` **Address Warning: Peak Volume Capacity Risk: WARN**: Evaluates contractual peak workload volume against observed runtime capacity and cluster headroom.
- `[PP2]` **Address Warning: Aggregation Scalability Risk: WARN**: Assesses distinct and grouping aggregations against high-cardinality data volumes.
- `[PP2]` **Address Warning: Autoscaling Boundary Risk: WARN**: Assesses autoscaling upper boundaries against peak workload expansion multipliers.
- `[PP2]` **Address Warning: Peak Workload Capacity Headroom Risk**: Contractual peak ingestion burst exceeds baseline workload capacity on a fixed or constrained cluster configuration without empirical verification.
- `[PP2]` **Address Warning: Insufficient Evidence: SLA_COMPLIANCE**: SLA compliance cannot be determined without runtime duration telemetry.
- `[PP2]` **Address Warning: Insufficient Evidence: COST_VIABILITY**: Cost projection unavailable in offline static analysis without DBU consumption telemetry.
- `[PP3]` **Provide Telemetry Evidence**: Supply Databricks runtime execution telemetry (run duration, task metrics, shuffle bytes) to enable performance, SLA, and cost decision sufficiency.
- `[PP3]` **Provide Telemetry Evidence**: Provide at least 2 historical execution runs under varying data volumes to enable empirical runtime regression and scalability trend forensics.
- `[PP3]` **Provide Telemetry Evidence**: Provide DBU pricing tier metadata and cluster duration metrics to enable cost viability analysis.
