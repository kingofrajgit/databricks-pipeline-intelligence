# DPIF Validation Report (Offline)

## Validation Summary

- **Contract ID**: `bad_production-contract`
- **Pipeline Name**: `bad_production`
- **Validation Timestamp**: `2026-09-17T10-38-29`
- **Environment**: `production`
- **Quality Score**: **17.7/100** (NOT_PRODUCTION_READY)
- **Confidence**: `LOW`
- **Decision Sufficiency**: `FALSE`
- **Final Decision**: **`NOT_PRODUCTION_READY`**

## Evidence Summary

| Evidence Type | Status | Provenance | Details |
|:---|:---:|:---:|:---|
| Contract | `STATIC` | `CONTRACT_FILE` | `bad_production-contract` |
| Source Definition | `DECLARED` | `CONTRACT` | `abfss://landing@acct.dfs.core.windows.net/bad_production/` |
| Target Definition | `DECLARED` | `CONTRACT` | `abfss://curated@acct.dfs.core.windows.net/bad_production/` |

## Checkpoint Results (CP-001..CP-024)

| Checkpoint | Name | Status | Severity | Score | Findings |
|:---|:---|:---:|:---:|:---:|:---:|
| `CP-001` | Source Validation | **`FAIL`** | `HIGH` | 1.0 | 1 |
| `CP-003` | Schema Drift Validation | `UNKNOWN` | `INFO` | 0.0 | 0 |
| `CP-004` | Code Validation | **`FAIL`** | `CRITICAL` | 0.0 | 14 |
| `CP-007` | Data Volume Validation | `UNKNOWN` | `INFO` | 1.0 | 0 |
| `CP-008` | Performance Validation | `UNKNOWN` | `INFO` | 0.0 | 0 |
| `CP-009` | Cluster Validation | `UNKNOWN` | `INFO` | 0.0 | 0 |
| `CP-010` | Scalability Validation | **`FAIL`** | `HIGH` | 1.0 | 9 |
| `CP-011` | Job Validation | `WARN` | `MEDIUM` | 0.6 | 2 |
| `CP-012` | Incremental Processing Validation | **`FAIL`** | `HIGH` | 0.2 | 0 |
| `CP-013` | Error Handling Validation | `WARN` | `MEDIUM` | 0.6 | 0 |
| `CP-014` | Retry Validation | **`FAIL`** | `HIGH` | 0.3 | 0 |
| `CP-015` | Restartability Validation | `WARN` | `MEDIUM` | 0.6 | 0 |
| `CP-016` | Idempotency Validation | `WARN` | `MEDIUM` | 0.6 | 0 |
| `CP-019` | Cost Validation | `UNKNOWN` | `INFO` | 0.0 | 0 |
| `CP-020` | Security Validation | **`FAIL`** | `CRITICAL` | 0.0 | 0 |
| `CP-021` | Governance Validation | `WARN` | `MEDIUM` | 0.6 | 0 |
| `CP-022` | Data Quality Validation | `PASS` | `INFO` | 1.0 | 0 |
| `CP-023` | SLA Validation | `UNKNOWN` | `INFO` | 0.0 | 0 |
| `CP-024` | Production Readiness | **`FAIL`** | `CRITICAL` | 0.2 | 1 |

## Developer Implementation Forensics (M5E)

- **Overall Status**: `FAIL`
- **Evaluated Dimensions**: 9
- **Findings Count**: 4

## Rerun / Idempotency Forensics (M5F)

- **Overall Status**: `UNKNOWN`
- **Idempotency Status**: `UNKNOWN`
- **Duplicate Risk**: `INFO`
- **Data Loss Risk**: `INFO`
- **Total Rerun Findings**: 0

## Three-Layer Alignment Forensics (M5G)

- **Overall Status**: `FAIL`
- **Drift Severity**: `BLOCKING`
- **Blocking Drift**: `YES`
- **Total Drifts**: 1

## Evidence Sufficiency (M5H)

- **Coverage Score**: 58.9%
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
- **Quality Score**: 17.7/100
- **Confidence**: `LOW`

### Blockers (13)

1. `[CRITICAL]` **Driver Collection Detection** (Source: `CODE-PYSPARK-001`)
   - *Description*: Detect driver-side collection of distributed data.

   - *Resolution*: Avoid collecting large distributed datasets to the driver.

2. `[CRITICAL]` **Large Driver Collection Risk** (Source: `CODE-PYSPARK-006`)
   - *Description*: Detect collect() calls evaluated against data evidence. An unrestricted collect() over a large input risks driver memory pressure/OOM, while the same call after limit() or on a small input is a qualified warning.

   - *Resolution*: Potential large driver-side collection: materializing 2048 GB on the driver risks memory pressure/OOM; keep data distributed (write/aggregate) or bound the result.
3. `[HIGH]` **Cartesian Cross Join Risk** (Source: `CODE-PYSPARK-019`)
   - *Description*: Detect crossJoin() or join(how="cross"). Cartesian products produce multiplicative row growth (O(N*M)), leading to massive shuffles, performance degradation, and executor OOM.

   - *Resolution*: Dangerous cross join over ~2048 GB: Cartesian product causes multiplicative row growth (O(N*M)) and catastrophic shuffle/OOM.
4. `[CRITICAL]` **toPandas Large Data Risk** (Source: `CODE-PYSPARK-007`)
   - *Description*: Detect toPandas() calls evaluated against data evidence. A bounded toPandas() (e.g. after limit()) is a qualified warning; an unbounded call over a large input is high risk. toPandas() is not banned.

   - *Resolution*: Potential large toPandas() over ~2048 GB: prefer distributed writes/aggregates or Arrow-based bounded transfer.
5. `[CRITICAL]` **Blocking Configuration Drift: target_storage_format** (Source: `ALIGN-TARGET_STORAGE_FORMAT`)
   - *Description*: Expected='Format: delta' vs Implemented='Formats: parquet' vs Actual='UNKNOWN (table metadata uninspected)'
   - *Resolution*: Align target_storage_format between pipeline contract, implementation, and Databricks runtime.
6. `[CRITICAL]` **Driver Collection Detection** (Source: `CODE-PYSPARK-001`)
   - *Description*: Detect driver-side collection of distributed data.

   - *Resolution*: Remediate Driver Collection Detection: Operational risk associated with Driver Collection Detection.
7. `[CRITICAL]` **Large Driver Collection Risk** (Source: `CODE-PYSPARK-006`)
   - *Description*: Detect collect() calls evaluated against data evidence. An unrestricted collect() over a large input risks driver memory pressure/OOM, while the same call after limit() or on a small input is a qualified warning.

   - *Resolution*: Remediate Large Driver Collection Risk: Operational risk associated with Large Driver Collection Risk.
8. `[CRITICAL]` **toPandas Large Data Risk** (Source: `CODE-PYSPARK-007`)
   - *Description*: Detect toPandas() calls evaluated against data evidence. A bounded toPandas() (e.g. after limit()) is a qualified warning; an unbounded call over a large input is high risk. toPandas() is not banned.

   - *Resolution*: Remediate toPandas Large Data Risk: Operational risk associated with toPandas Large Data Risk.
9. `[HIGH]` **Cartesian Cross Join Risk** (Source: `CODE-PYSPARK-019`)
   - *Description*: Detect crossJoin() or join(how="cross"). Cartesian products produce multiplicative row growth (O(N*M)), leading to massive shuffles, performance degradation, and executor OOM.

   - *Resolution*: Remediate Cartesian Cross Join Risk: Operational risk associated with Cartesian Cross Join Risk.
10. `[CRITICAL]` **Configuration Drift: target_storage_format** (Source: `ALIGN-TGT-001`)
   - *Description*: Configuration divergence across layers on target_storage_format: Expected='Format: delta', Implemented='Formats: parquet', Actual='UNKNOWN (table metadata uninspected)'.
   - *Resolution*: Remediate Configuration Drift: target_storage_format: Runtime behavior and dependencies will diverge from declared pipeline contract.
11. `[CRITICAL]` **Driver Memory Exhaustion Risk at Scale** (Source: `XDOM-001`)
   - *Description*: Driver-side materialization (`collect`/`toPandas`) detected in pipeline code combined with a substantial data workload (3072.0 GB). Materializing distributed datasets to the Spark driver causes out-of-memory driver crashes.
   - *Resolution*: Remediate Driver Memory Exhaustion Risk at Scale: Replace `collect()` and `toPandas()` with distributed DataFrame actions or write transformations directly to Delta lake.
12. `[CRITICAL]` **Cartesian Product Scaling Risk** (Source: `XDOM-002`)
   - *Description*: Cartesian cross join detected on a non-trivial workload (3072.0 GB). Unconstrained cross joins generate O(N*M) record explosions at scale, saturating executor disk and network.
   - *Resolution*: Remediate Cartesian Product Scaling Risk: Eliminate Cartesian joins: supply explicit join conditions with ON clauses or apply strict pre-filtering.
13. `[HIGH]` **High Network Shuffle & Spill Risk** (Source: `XDOM-003`)
   - *Description*: Observed or projected shuffle volume exceeds safe network thresholds, causing executor memory spill to disk and stage stragglers.
   - *Resolution*: Remediate High Network Shuffle & Spill Risk: Enable Adaptive Query Execution (AQE), co-partition datasets on join keys, and avoid wide transformations before filtering.

### Top Operational & Architectural Risks (5)

1. `[CRITICAL]` **Configuration Drift: target_storage_format** (`CONFIGURATION_DRIFT`)
   - *Consequence*: Runtime behavior and dependencies will diverge from declared pipeline contract.
2. `[CRITICAL]` **Driver Collection Detection** (`IMPLEMENTATION`)
   - *Consequence*: Operational risk associated with Driver Collection Detection.
3. `[CRITICAL]` **Large Driver Collection Risk** (`IMPLEMENTATION`)
   - *Consequence*: Operational risk associated with Large Driver Collection Risk.
4. `[CRITICAL]` **toPandas Large Data Risk** (`IMPLEMENTATION`)
   - *Consequence*: Operational risk associated with toPandas Large Data Risk.
5. `[CRITICAL]` **Driver Memory Exhaustion Risk at Scale** (`OPERATIONAL`)
   - *Consequence*: Replace `collect()` and `toPandas()` with distributed DataFrame actions or write transformations directly to Delta lake.

### Required Actions

- `[PP0]` **Resolve Blocker: Driver Collection Detection**: Detect driver-side collection of distributed data.

- `[PP0]` **Resolve Blocker: Large Driver Collection Risk**: Detect collect() calls evaluated against data evidence. An unrestricted collect() over a large input risks driver memory pressure/OOM, while the same call after limit() or on a small input is a qualified warning.

- `[PP0]` **Resolve Blocker: Cartesian Cross Join Risk**: Detect crossJoin() or join(how="cross"). Cartesian products produce multiplicative row growth (O(N*M)), leading to massive shuffles, performance degradation, and executor OOM.

- `[PP0]` **Resolve Blocker: toPandas Large Data Risk**: Detect toPandas() calls evaluated against data evidence. A bounded toPandas() (e.g. after limit()) is a qualified warning; an unbounded call over a large input is high risk. toPandas() is not banned.

- `[PP0]` **Resolve Blocker: Blocking Configuration Drift: target_storage_format**: Expected='Format: delta' vs Implemented='Formats: parquet' vs Actual='UNKNOWN (table metadata uninspected)'
- `[PP0]` **Resolve Blocker: Configuration Drift: target_storage_format**: Configuration divergence across layers on target_storage_format: Expected='Format: delta', Implemented='Formats: parquet', Actual='UNKNOWN (table metadata uninspected)'.
- `[PP0]` **Resolve Blocker: Driver Memory Exhaustion Risk at Scale**: Driver-side materialization (`collect`/`toPandas`) detected in pipeline code combined with a substantial data workload (3072.0 GB). Materializing distributed datasets to the Spark driver causes out-of-memory driver crashes.
- `[PP0]` **Resolve Blocker: Cartesian Product Scaling Risk**: Cartesian cross join detected on a non-trivial workload (3072.0 GB). Unconstrained cross joins generate O(N*M) record explosions at scale, saturating executor disk and network.
- `[PP0]` **Resolve Blocker: High Network Shuffle & Spill Risk**: Observed or projected shuffle volume exceeds safe network thresholds, causing executor memory spill to disk and stage stragglers.
- `[PP1]` **Mitigate High Risk: Missing Incremental Strategy**: Flag large-volume sources with no incremental ingestion evidence (no incremental/CDC/streaming mode and no watermark, CDC, checkpoint, or partitioning evidence).

- `[PP1]` **Mitigate High Risk: Pandas UDF Misuse Detection**: Detect use of Pandas UDF where native Spark UDF would be more appropriate.

- `[PP1]` **Mitigate High Risk: Show() in Production Path Detection**: Detect show() or take() calls in production pipeline code paths.

- `[PP1]` **Mitigate High Risk: Hard-coded Path Detection**: Detect hard-coded file paths in pipeline code.

- `[PP1]` **Mitigate High Risk: Potential Large Shuffle**: Flag shuffle-class operations (groupBy, distinct, repartition, orderBy, large joins, windows) over sizable inputs as potential shuffle risk. Static analysis cannot prove shuffle volume, so findings are WARN only.

- `[PP1]` **Mitigate High Risk: User Defined Function (UDF) Overhead Risk**: Detect Python and Pandas UDF usages. Standard Python UDFs force row-by-row serialization across the Python-JVM boundary, incurring severe performance penalties on large datasets. Prefer native Spark SQL/DataFrame expressions or vectorized Pandas UDFs.

- `[PP1]` **Mitigate High Risk: Data Growth Risk: FAIL**: Calculates deterministic compound data growth projections over the forecast horizon.
- `[PP1]` **Mitigate High Risk: Driver Scalability Risk: FAIL**: Evaluates driver-side materialization (collect, toPandas) in the context of scaled data volume.
- `[PP1]` **Mitigate High Risk: Join Scalability Risk: FAIL**: Evaluates join architecture to prevent Cartesian row explosion and unbounded shuffle joins at scale.
- `[PP1]` **Mitigate High Risk: Missing Retry Configuration**: Detects production jobs or tasks without retry configuration where resilience is required.
- `[PP2]` **Address Warning: CrossJoin Detection**: Detect crossJoin operations which can produce cartesian products.

- `[PP2]` **Address Warning: Potential Excessive Repartition**: Flag repeated repartition() calls on one DataFrame or unnecessarily large literal partition counts. A single reasonable repartition is not flagged.

- `[PP2]` **Address Warning: Unjustified Cache**: Flag cache()/persist() where static analysis shows no demonstrated reuse of the cached DataFrame. Reused frames are not flagged, and no runtime benefit is ever claimed without runtime evidence.

- `[PP2]` **Address Warning: Repeated Spark Action**: Flag multiple actions (count/collect/take/show/write) on one DataFrame, which may require additional computation. count() alone is never bad.

- `[PP2]` **Address Warning: Large Dataset Global Sort**: Flag global orderBy()/sort() over large inputs as a potential performance warning — never a definite failure without runtime evidence.

- `[PP2]` **Address Warning: Missing Task Timeout**: Detects production tasks without an explicit timeout configured.
- `[PP2]` **Address Warning: Peak Workload Capacity Headroom Risk**: Contractual peak ingestion burst exceeds baseline workload capacity on a fixed or constrained cluster configuration without empirical verification.
- `[PP2]` **Address Warning: Insufficient Evidence: SLA_COMPLIANCE**: SLA compliance cannot be determined without runtime duration telemetry.
- `[PP2]` **Address Warning: Insufficient Evidence: IDEMPOTENCY_SAFETY**: MERGE key stability/uniqueness unproven; idempotency cannot be certified safe.
- `[PP2]` **Address Warning: Insufficient Evidence: COST_VIABILITY**: Cost projection unavailable in offline static analysis without DBU consumption telemetry.
- `[PP3]` **Provide Telemetry Evidence**: Supply Databricks runtime execution telemetry (run duration, task metrics, shuffle bytes) to enable performance, SLA, and cost decision sufficiency.
- `[PP3]` **Provide Telemetry Evidence**: Provide at least 2 historical execution runs under varying data volumes to enable empirical runtime regression and scalability trend forensics.
- `[PP3]` **Provide Telemetry Evidence**: Provide DBU pricing tier metadata and cluster duration metrics to enable cost viability analysis.
