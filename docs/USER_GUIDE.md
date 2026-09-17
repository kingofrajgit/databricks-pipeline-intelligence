# Databricks Pipeline Intelligence Framework (DPIF) — User Guide

A comprehensive, production-grade guide to validating Databricks data pipelines with DPIF across offline, online, and enterprise fleet workflows.

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [What DPIF Validates](#2-what-dpif-validates)
3. [Architecture Overview](#3-architecture-overview)
4. [Installation](#4-installation)
5. [Configuration](#5-configuration)
6. [Offline Validation](#6-offline-validation)
7. [Online Validation](#7-online-validation)
8. [Online Validation — Real Example](#8-online-validation--real-example)
9. [Understanding the Validation Output](#9-understanding-the-validation-output)
10. [Checkpoints Reference (CP-001..CP-024)](#10-checkpoints-reference-cp-001cp-024)
11. [Developer Implementation Forensics (M5E)](#11-developer-implementation-forensics-m5e)
12. [Rerun / Idempotency Forensics (M5F)](#12-rerun--idempotency-forensics-m5f)
13. [Three-Layer Alignment Forensics (M5G)](#13-three-layer-alignment-forensics-m5g)
14. [Evidence Sufficiency (M5H)](#14-evidence-sufficiency-m5h)
15. [Production Decision (CP-FINAL & M5I)](#15-production-decision-cp-final--m5i)
16. [Generated Reports](#16-generated-reports)
17. [Custom Output Directory](#17-custom-output-directory)
18. [JSON Mode](#18-json-mode)
19. [Fleet Validation (M5K)](#19-fleet-validation-m5k)
20. [CI/CD Usage](#20-cicd-usage)
21. [Troubleshooting](#21-troubleshooting)
22. [Security](#22-security)
23. [Recommended First Run](#23-recommended-first-run)
24. [Command Reference](#24-command-reference)
25. [Frequently Asked Questions (FAQ)](#25-frequently-asked-questions-faq)

---

## 1. Introduction

### What DPIF Is
The **Databricks Pipeline Intelligence Framework (DPIF)** is an automated quality, reliability, and production-readiness inspection engine for Databricks data pipelines. It provides deep architectural, forensic, and behavioral intelligence across data contracts, PySpark/Python/SQL source code, Databricks Jobs, clusters, runtime telemetry, and cross-pipeline interactions.

### What Problem It Solves
Data pipelines in enterprise lakehouses often pass unit tests and run without immediate errors, yet fail catastrophically in production due to:
- Unbounded driver memory collections (`.collect()`, `.toPandas()`) under volume bursts.
- Missing retry policies causing transient network dropouts to fail multi-hour ETL jobs.
- Non-idempotent append write modes resulting in silent data duplication during automatic retries.
- Unkeyed wide transformations triggering massive all-to-all shuffles and disk spills.
- Configuration drift between what architects declared, what developers coded, and what actually runs on clusters.
- Lack of operational evidence to certify SLA compliance, cost viability, or scalability headroom.

DPIF shifts pipeline readiness validation from subjective manual peer reviews to an automated, deterministic, and evidence-backed engineering discipline.

### Validation Execution vs. Production-Readiness Decision
DPIF strictly separates the **execution of validation** from the **production-readiness decision**:
- **Validation Execution**: Systematically runs 24 standardized checkpoints (`CP-001` through `CP-024`) and 5 forensic analyzer engines against all available evidence, emitting structured findings.
- **Production-Readiness Decision**: Evaluates whether the pipeline can be safely promoted to production based on evidence coverage (`M5H`), blocking risks (`P0`), architectural anti-patterns, and enterprise release policies (`CP-FINAL` / `M5I`). A high numeric score alone never grants production approval if critical evidence is missing or a single blocking failure exists.

### Offline vs. Online Validation
- **Offline Validation (`dpif validate --offline`)**: Runs against local YAML contracts, static code files, and mock metadata fixtures. It requires **no Databricks credentials** or internet connection, making it ideal for local developer linting and pull-request CI checks.
- **Online Validation (`dpif validate-online`)**: Connects directly to a live Databricks workspace via secure REST APIs using a workload identifier (`--job-id` or `--pipeline-id`). It automatically queries the workspace, discovers job topology, exports notebook/script code, extracts source/target tables, fetches historical runs, and executes the exact same validation intelligence stack without requiring manual contract or code uploads.

---

## 2. What DPIF Validates

DPIF evaluates data pipelines across 16 core domains:

| Domain | What DPIF Validates |
|:---|:---|
| **Source & Storage** | Storage format suitability (Parquet, Delta, CSV, JSON), source connection configurations, path specifications, schema drift tolerance, and partitioning strategy. |
| **Python / PySpark Code** | Abstract Syntax Tree (AST) analysis of PySpark transformations, driver collection risks (`collect`, `toPandas`, `take`), unpartitioned window functions, broadcast join sizing, Pandas UDF misuse, caching lifecycle, and hardcoded credentials. |
| **SQL Transformations** | AST parsing of SQL expressions via `sqlglot`, cartesian joins, unkeyed joins, non-deterministic functions, table references, and syntax correctness. |
| **Databricks Configuration** | Job settings, task dependency DAGs, task timeouts, retry count/interval configurations, concurrency limits, trigger schedules, and cluster associations. |
| **Runtime & Performance** | Task execution durations, garbage collection overhead, stage stragglers, executor memory spill to disk, shuffle read/write volume, and driver/executor skew. |
| **Scalability & Growth** | Behavior under peak volume bursts (e.g. 2x–5x baseline), file count proliferation, metastore catalog bottlenecks, and SLA headroom. |
| **Implementation Forensics (M5E)** | 9 observable developer implementation dimensions including shuffle minimization, partitioning quality, join strategy selection, cache eviction, and exception safety. |
| **Rerun & Idempotency (M5F)** | 7 rerun scenarios including same-input reruns, incremental batches, partial failures, job retries, concurrent executions, and late-arriving restatements. |
| **Three-Layer Alignment (M5G)** | Architectural drift across Layer 1 (Contract EXPECTED), Layer 2 (Code/Config IMPLEMENTED), and Layer 3 (Workspace/Runtime ACTUAL). |
| **Evidence Sufficiency (M5H)** | Evidence completeness across all 16 domains, deterministic confidence rating, and decision sufficiency gating. |
| **Production Decision (M5I / CP-FINAL)** | Synthesis of quality score, blocking P0 risks, operational risk catalog, deterministic causal risk chains, and prioritized remediation actions. |

> [!NOTE]
> **Implemented Capabilities vs. Unavailable Evidence**: DPIF never hallucinates findings. If runtime Spark event logs or DBU consumption metrics are unavailable, the corresponding checkpoints remain `UNKNOWN`. DPIF validates strictly against observed and declared facts.

---

## 3. Architecture Overview

### General DPIF Execution Flow

```text
       +-------------------------------------------------------------+
       |                          User / CI                          |
       +-------------------------------------------------------------+
                                      |
                                      v
       +-------------------------------------------------------------+
       |                          DPIF CLI                           |
       |  (dpif validate, dpif validate-online, validate-online-fleet)|
       +-------------------------------------------------------------+
                                      |
                                      v
       +-------------------------------------------------------------+
       |                    Evidence Acquisition                     |
       |  - Offline: YAML contracts, code files, metadata fixtures   |
       |  - Online: LiveDatabricksConnector + EvidenceProvider        |
       +-------------------------------------------------------------+
                                      |
                                      v
       +-------------------------------------------------------------+
       |                 DPIF Intelligence Engines                   |
       |  1. AST & SQL Parsers                                       |
       |  2. Checkpoint Engine (CP-001..CP-024)                      |
       |  3. Developer Implementation Forensics (M5E)                |
       |  4. Rerun / Idempotency Forensics (M5F)                     |
       |  5. Three-Layer Alignment Forensics (M5G)                   |
       +-------------------------------------------------------------+
                                      |
                                      v
       +-------------------------------------------------------------+
       |               Evidence Sufficiency Engine (M5H)             |
       |  - Evaluates completeness, quality, and freshness           |
       |  - Produces confidence tier and decision_sufficiency flag   |
       +-------------------------------------------------------------+
                                      |
                                      v
       +-------------------------------------------------------------+
       |         Production Decision & Risk Synthesis (M5I)          |
       |  - Evaluates P0 blockers and causal risk chains             |
       |  - Synthesizes Final Decision (PRODUCTION_READY, etc.)      |
       +-------------------------------------------------------------+
                                      |
                     +----------------+----------------+
                     |                                 |
                     v                                 v
          +---------------------+           +---------------------+
          |   validation.json   |           |    validation.md    |
          |  (Machine-readable) |           |  (Human-readable)   |
          +---------------------+           +---------------------+
```

### Online Discovery Flow

In online mode, the user does not need to extract, upload, or reconstruct pipeline contracts or source files. DPIF queries the workspace directly:

```text
    +-------------------------------------------------------------+
    |                    Databricks Workspace                     |
    +-------------------------------------------------------------+
          |
          |--> Jobs API -------------> Job metadata, tasks, timeouts, retries
          |--> Clusters API ---------> Compute runtime, node types, autoscale
          |--> Runs API -------------> Historical runs, run duration, state
          |--> Workspace Export API -> Discovers & exports Notebook / Script code
          |
          v
    +-------------------------------------------------------------+
    |                     DPIF Synthesizer                        |
    |  - Synthesizes PipelineContract (sources, targets, cluster) |
    |  - Derives DataProfile from dataset metadata                |
    |  - Extracts SQL & PySpark logic from exported source        |
    |  - Tracks Evidence Provenance (LIVE_API, DERIVED, etc.)     |
    +-------------------------------------------------------------+
          |
          v
    +-------------------------------------------------------------+
    |               Unified DPIF Validation Pipeline              |
    |                 (CP-001..CP-024 + M5E..M5I)                 |
    +-------------------------------------------------------------+
```

---

## 4. Installation

### System Requirements
- **Python**: Version 3.11 or higher
- **OS**: Linux, macOS, or Windows (PowerShell / Command Prompt)
- **Network**: Internet access required only for online validation against Databricks REST APIs.

### Setup Instructions

1. **Clone the repository**:
   ```bash
   git clone https://github.com/kingofrajgit/databricks-pipeline-intelligence.git
   cd databricks-pipeline-intelligence
   ```

2. **Create and activate a virtual environment**:
   - **Linux / macOS**:
     ```bash
     python3 -m venv .venv
     source .venv/bin/activate
     ```
   - **Windows (PowerShell)**:
     ```powershell
     python -m venv .venv
     .venv\Scripts\Activate.ps1
     ```
   - **Windows (Command Prompt)**:
     ```cmd
     python -m venv .venv
     .venv\Scripts\activate.bat
     ```

3. **Install DPIF in editable mode**:
   ```bash
   pip install -e .
   ```
   *To include testing and linting tools (`pytest`, `ruff`, `mypy`):*
   ```bash
   pip install -e ".[dev]"
   ```

4. **Verify installation**:
   ```bash
   dpif --version
   dpif --help
   ```

---

## 5. Configuration

DPIF reads workspace connection settings from CLI arguments, environment variables, or a local `.env` file.

### Supported Configuration Keys

| Variable | Description | Example |
|:---|:---|:---|
| `DATABRICKS_HOST` | Full URL of the Databricks workspace | `https://<workspace-id>.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Personal Access Token (PAT) or OAuth token | `<your-token>` |
| `DATABRICKS_JOB_ID` | Optional default Job ID for online commands | `159900604537188` |
| `DATABRICKS_CLUSTER_ID` | Optional default Cluster ID | `0917-112233-abcdef12` |
| `DATABRICKS_RUN_ID` | Optional default Run ID | `987654321` |

### Setting Up Local `.env`

Copy the provided `.env.example` template:
```bash
cp .env.example .env
```

Edit `.env` using your preferred editor:
```env
DATABRICKS_HOST=https://<workspace-id>.cloud.databricks.com
DATABRICKS_TOKEN=<your-token>
```

> [!CAUTION]
> **Credential Security**: Never commit `.env` or paste personal access tokens into version control. `.env`, `.env.*`, and `.env.local` are excluded in `.gitignore`. DPIF sanitizes and masks all credentials in reports, logs, and CLI output.

### Configuration Precedence
When resolving credentials, DPIF applies strict precedence:
1. **Explicit CLI flags** (`--workspace`, `--host`, `--token`)
2. **Process environment variables** (`export DATABRICKS_HOST=...`)
3. **Local `.env` file** in the working directory
4. **Defaults / None**

---

## 6. Offline Validation

Offline validation evaluates pipelines locally without requiring Databricks credentials, live network connections, or active clusters.

### Running Offline Validation

Validate a pipeline contract using the `--offline` flag:
```bash
dpif validate --contract examples/customer_daily.yaml --offline
```

### What a Pipeline Contract Declares
A pipeline contract is a YAML specification declaring expectations:
- **`pipeline`**: Pipeline identifier, name, environment, owner.
- **`source`**: Ingestion type (`adls`, `s3`, `jdbc`), format (`parquet`, `delta`, `csv`), path, expected daily volume, peak volume.
- **`processing`**: Processing type (`batch`, `incremental`, `streaming`), language (`pyspark`, `sql`).
- **`target`**: Target type (`delta`), table name, catalog, schema, storage path.
- **`sla`**: Maximum allowed execution duration.
- **`reliability`**: Expected retry count, idempotency guarantee.
- **`cluster`**: Worker node count, node type, Databricks runtime version.
- **`job`**: Timeout seconds, retry policies.
- **`code_path`**: Path to the PySpark or SQL source file.

### Supplying Optional Offline Evidence Files
You can enrich offline validation with supplementary evidence flags:
```bash
dpif validate \
  --contract examples/customer_daily.yaml \
  --offline \
  --code-path tests/fixtures/code/good_pipeline.py \
  --metadata-profile tests/fixtures/metadata/customer_daily_profile.json \
  --runtime-run tests/fixtures/runtime/runtime_run.json \
  --historical-runs tests/fixtures/historical/runs_history.json
```

### Batch Offline Validation (CSV Manifest)
To validate hundreds of pipelines in a single batch:
```bash
dpif validate --input manifests/pipelines.csv --offline --output-dir batch_reports/
```
The CSV manifest specifies `pipeline_id`, `developer`, `contract_path`, `code_path`, and optional metadata/runtime paths for each pipeline.

---

## 7. Online Validation

Online validation connects to your Databricks workspace and inspects actual deployed jobs or Delta Live Tables (DLT) pipelines.

### Normal Online Workflow

Validate an existing Databricks Job:
```bash
dpif validate-online --job-id 159900604537188
```

Validate a Databricks Pipeline (DLT):
```bash
dpif validate-online --pipeline-id 2f4a03c2-e50f-418a-b84a-79d4fb2f0d8f
```

### What You Provide vs. What DPIF Discovers

```text
  User Provides:
    [--job-id <ID>]  +  [DATABRICKS_HOST]  +  [DATABRICKS_TOKEN]
                            |
                            v
  DPIF Automatically Discovers from Databricks:
    * Workspace metadata and URL
    * Job definition, topology, schedule, timeouts, and task DAG
    * Linked task notebooks and scripts via Workspace Export API
    * PySpark, Python, and SQL source code
    * Inferred source and target tables (via AST parser)
    * Cluster specifications (node type, worker count, Spark version, policies)
    * Historical execution runs and duration metrics
    * Synthesized PipelineContract and DataProfile
```

> [!IMPORTANT]
> **Evidence Availability Caveats**: Online validation extracts what Databricks exposes via its REST APIs and workspace permissions. If a job has never been run, runtime metrics and historical telemetry will be unavailable (`UNKNOWN`), which DPIF reports explicitly without fabricating synthetic metrics.

---

## 8. Online Validation — Real Example

Here is the exact output from validating an actual Databricks Job (`159900604537188`):

### Command Executed
```powershell
dpif validate-online --job-id 159900604537188
```

### Console Output
```text
============================================================
DPIF ONLINE VALIDATION
============================================================
    Resource Type:        JOB
    Resource ID:          159900604537188
    Pipeline Name:        testing
    Environment:          production
    Workspace:            https://<workspace-id>.cloud.databricks.com
    Mode:                 ONLINE (live Databricks workspace)

EVIDENCE SUMMARY
------------------------------------------------------------
    Workspace       : LIVE (LIVE_API)
    Job             : LIVE (LIVE_API)
    Cluster         : UNAVAILABLE (UNAVAILABLE)
    Runtime         : UNAVAILABLE (UNAVAILABLE)
    Historical      : LIVE (LIVE_API)
    Code            : LIVE (LIVE_API)
    Data Profile    : UNAVAILABLE (UNAVAILABLE)

CHECKPOINTS (CP-001..CP-024)
------------------------------------------------------------
    CP-001 Source Validation               : FAIL
    CP-003 Schema Drift Validation         : WARN
    CP-004 Code Validation                 : PASS
    CP-007 Data Volume Validation          : UNKNOWN
    CP-008 Performance Validation          : UNKNOWN
    CP-009 Cluster Validation              : UNKNOWN
    CP-010 Scalability Validation          : PASS
    CP-011 Job Validation                  : WARN
    CP-012 Incremental Processing          : PASS
    CP-013 Error Handling Validation       : WARN
    CP-014 Retry Validation                : FAIL
    CP-015 Restartability Validation       : WARN
    CP-016 Idempotency Validation          : PASS
    CP-019 Cost Validation                 : UNKNOWN
    CP-020 Security Validation             : PASS
    CP-021 Governance Validation           : PASS
    CP-022 Data Quality Validation         : UNKNOWN
    CP-023 SLA Validation                  : UNKNOWN
    CP-024 Production Readiness            : FAIL

------------------------------------------------------------
M5H EVIDENCE COVERAGE
------------------------------------------------------------
Coverage:            45%
Confidence:          LOW
Decision Sufficiency: FALSE

============================================================
DECISION & RISK SYNTHESIS (M5I)
============================================================
    FINAL DECISION:        NOT_PRODUCTION_READY
    CONFIDENCE:            LOW
    DECISION SUFFICIENCY:  FALSE
    QUALITY SCORE:         43.0/100 (CRITICAL)

    BLOCKERS (2):
      1. [HIGH] Reliability validation (CP-014..CP-016) failed blocking reliability gate.
         Resolution: Configure retry count (>= 1) on production tasks.
      2. [HIGH] High Network Shuffle & Spill Risk
         Resolution: Enable Adaptive Query Execution (AQE), co-partition datasets on join keys.

============================================================
VALIDATION REPORTS PERSISTED
============================================================
JSON report:
  reports/online/159900604537188/2026-09-17T09-41-31/validation.json

Markdown report:
  reports/online/159900604537188/2026-09-17T09-41-31/validation.md
```

---

## 9. Understanding the Validation Output

DPIF categorizes every checkpoint and rule evaluation into one of five statuses:

| Status | Meaning | Production Implication |
|:---:|:---|:---|
| **`PASS`** | The checkpoint or rule evaluated all necessary evidence and identified no violations. | Contributes 100 points to the category score. |
| **`WARN`** | A non-blocking risk, deviation from best practices, or potential bottleneck was identified. | Contributes 60 points to the category score. Does not block deployment unless designated as a P0 risk. |
| **`FAIL`** | A direct violation of architectural standards, reliability rules, or code safety was detected. | Contributes 0 points. If marked `blocking: true` (e.g. `P0`), forces the entire pipeline to `NOT_PRODUCTION_READY`. |
| **`UNKNOWN`** | **Required evidence was missing, incomplete, or inaccessible.** | Contributes 0 points. Indicates insufficient evidence to certify correctness. |
| **`NOT_APPLICABLE`** | The dimension does not apply to this workload (e.g. streaming rules on a batch pipeline). | Excluded from the weighted average score. |

### Critical Principle: `UNKNOWN != FAIL`
- `FAIL` means DPIF inspected evidence and proved the pipeline violates a requirement.
- `UNKNOWN` means DPIF **lacks sufficient evidence** to make a factual claim.
- DPIF strictly adheres to engineering honesty: it will never assume that missing pricing data means zero cost, or that missing Spark logs mean zero memory spill.
- When an upstream checkpoint is `FAIL` or `UNKNOWN`, downstream checkpoints that depend on it automatically become `UNKNOWN` to avoid evaluating false assumptions.

---

## 10. Checkpoints Reference (CP-001..CP-024)

| Checkpoint | Domain | Purpose | Possible Statuses |
|:---|:---|:---|:---:|
| **`CP-001`** | Source | Validates source format, storage layer, connection protocol, and access configuration. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |
| **`CP-003`** | Data | Detects schema drift risk between contract expectations and actual data profiles. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |
| **`CP-004`** | Code | AST analysis of PySpark/SQL transformations, collections, joins, and anti-patterns. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |
| **`CP-007`** | Data | Inspects dataset volume, record counts, file size distribution, and small-file proliferation. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |
| **`CP-008`** | Performance | Evaluates runtime execution duration, task skew, memory spill, and Spark GC overhead. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |
| **`CP-009`** | Cluster | Assesses cluster sizing, worker node types, autoscale bounds, Spark configuration, and runtime versions. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |
| **`CP-010`** | Scalability | Simulates peak workload volume growth (2x–5x), metastore pressure, and cluster headroom. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |
| **`CP-011`** | Job | Validates Databricks Job configuration, task DAG dependencies, timeouts, and execution schedules. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |
| **`CP-012`** | Pipeline | Verifies incremental processing strategy, CDC watermarks, and partition pruning. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |
| **`CP-013`** | Reliability | Checks structured error handling, try/except patterns, logging, and graceful task degradation. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |
| **`CP-014`** | Reliability | Evaluates task and job retry policies to protect against transient infrastructure failures. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |
| **`CP-015`** | Reliability | Validates restartability, checkpoint locations, and safe resumption from failure points. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |
| **`CP-016`** | Reliability | Analyzes idempotency: ensures duplicate runs do not create duplicate target records. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |
| **`CP-019`** | Cost | Evaluates DBU consumption rates, VM pricing tiers, compute idle timeouts, and cluster efficiency. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |
| **`CP-020`** | Security | Scans code and configuration for hardcoded secrets, plain-text credentials, and insecure endpoints. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |
| **`CP-021`** | Governance | Validates Unity Catalog namespaces, data lineage, pipeline ownership, and tagging compliance. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |
| **`CP-022`** | Data Quality | Inspects null rates, column constraints, uniqueness keys, and profile drift. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |
| **`CP-023`** | SLA | Compares actual and projected runtimes against contractual SLA deadlines. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |
| **`CP-024`** | Readiness | Aggregates all upstream checkpoint statuses into the core production readiness gate. | `PASS`, `WARN`, `FAIL`, `UNKNOWN` |

---

## 11. Developer Implementation Forensics (M5E)

The **M5E Engine** inspects the concrete implementation details in PySpark, Python, and SQL code across 9 dimensions:

1. **`transformation_quality`**: Evaluates expression efficiency, filtering order, and projection pruning.
2. **`shuffle_optimization`**: Flags unnecessary wide transformations and missing Adaptive Query Execution (AQE).
3. **`partitioning_quality`**: Analyzes `repartition()` vs. `coalesce()` choices and partition key granularity.
4. **`join_strategy`**: Identifies unkeyed cartesian cross-joins, broadcast threshold violations, and join ordering.
5. **`cache_lifecycle`**: Validates whether `.cache()` / `.persist()` calls have matching `.unpersist()` cleanups.
6. **`checkpoint_lifecycle`**: Ensures streaming checkpoints and lineage-breaking checkpoints are properly configured.
7. **`resource_lifecycle`**: Verifies database connections, file handles, and Spark sessions are closed cleanly.
8. **`exception_safety`**: Flags bare `except:` clauses, suppressed errors, or missing logging context.
9. **`implementation_completeness`**: Checks for unhandled TODOs, placeholder logic, or mock stub code in production pipelines.

---

## 12. Rerun / Idempotency Forensics (M5F)

The **M5F Engine** evaluates pipeline behavior across 7 operational rerun scenarios:

- **`SAME_INPUT`**: Running the pipeline twice on the exact same input data.
- **`INCREMENTAL_INPUT`**: Processing consecutive non-overlapping incremental micro-batches.
- **`OVERLAPPING_INPUT`**: Processing input data that contains previously processed records.
- **`PARTIAL_FAILURE`**: Pipeline crashes midway through a multi-stage transformation.
- **`JOB_RETRY`**: Automated Databricks Job retry re-executes a failed batch without human intervention.
- **`CONCURRENT_EXECUTION`**: Two instances of the pipeline execute simultaneously against shared storage.
- **`LATE_ARRIVING_DATA`**: Ingestion of records with historical event timestamps requiring partition backfills.

### What M5F Checks
- **Target Write Mode**: `APPEND` write modes without explicit target transaction boundaries or deduplication keys present severe duplicate-data risks.
- **Merge Key Stability**: Ensures `MERGE INTO` operations match on unique, immutable primary/natural keys.
- **State Recovery**: Verifies that re-running does not corrupt target Delta tables or leave uncommitted staging files.

---

## 13. Three-Layer Alignment Forensics (M5G)

The **M5G Engine** compares the pipeline across three distinct layers to detect architectural and configuration drift:

```text
  Layer 1: EXPECTED     (Contract YAML / Architecture requirements)
         vs
  Layer 2: IMPLEMENTED  (Code AST, SQL queries, Job/Cluster settings)
         vs
  Layer 3: ACTUAL       (Databricks live cluster, Runtime metrics, Historical runs)
```

### 9 Drift Dimensions
1. **`compute_runtime`**: Declared runtime version vs. actual cluster Spark version.
2. **`cluster_sizing_scaling`**: Declared node count vs. active cluster autoscale bounds.
3. **`job_workflow_cadence`**: Declared schedule frequency vs. active cron trigger.
4. **`processing_strategy`**: Batch vs. Incremental vs. Streaming alignment.
5. **`target_storage_format`**: Contracted Delta format vs. code write statements.
6. **`partitioning_layout`**: Expected partition columns vs. code `.partitionBy()` calls.
7. **`sla_execution_limits`**: Contractual SLA minutes vs. observed runtime duration.
8. **`reliability_retry_policy`**: Contractual retry count vs. Job task retry settings.
9. **`workload_volume_bounds`**: Contracted data volume vs. actual ingested byte size.

### Divergence Classifications
- `EXPECTED_VS_IMPLEMENTED`: Developer wrote code that diverges from the contract.
- `IMPLEMENTED_VS_ACTUAL`: Deployed Databricks compute diverges from what was written.
- `EXPECTED_VS_ACTUAL`: Live operational metrics diverge from contracted requirements.
- `THREE_WAY_DIVERGENCE`: Inconsistency detected across all three layers.
- **Drift Severity**: Classified as `NONE`, `WARN`, or `BLOCKING`.

---

## 14. Evidence Sufficiency (M5H)

The **M5H Engine** determines whether the framework has collected enough quality evidence to certify a production decision.

### Sufficiency Dimensions
- **Evidence Coverage (0–100%)**: Proportion of required evidence categories present across all 16 domains.
- **Confidence Level**: Deterministic evaluation of evidence strength:
  - **`HIGH`**: Comprehensive evidence present across contract, code AST, job config, and live runtime telemetry.
  - **`MEDIUM`**: Contract, code AST, and configuration available; historical telemetry partial.
  - **`LOW`**: Static code or configuration available, but zero runtime telemetry.
  - **`INSUFFICIENT`**: Critical inputs missing (e.g. no code or missing job metadata).
- **Evidence Quality**: Classified as `STRONG`, `MODERATE`, `WEAK`, or `INSUFFICIENT`.
- **Evidence Freshness**: Classified as `CURRENT`, `HISTORICAL`, `STALE`, `STATIC`, or `UNKNOWN`.
- **Decision Sufficiency (`TRUE` / `FALSE`)**: Boolean gate. If `FALSE`, production certification is blocked regardless of the quality score.

---

## 15. Production Decision (CP-FINAL & M5I)

The **CP-FINAL / M5I Synthesis Engine** synthesizes all findings into an engineering release decision.

### Final Decision Statuses

| Decision | Meaning | Action Required |
|:---|:---|:---|
| **`PRODUCTION_READY`** | All critical checkpoints passed, zero blocking P0 risks, and evidence sufficiency is certified. | Approved for automated deployment to production. |
| **`NOT_PRODUCTION_READY`** | One or more blocking P0 failures exist, or score falls below critical thresholds. | Deployment blocked. Must resolve identified blockers. |
| **`CONDITIONAL`** | Quality score is acceptable, but minor warnings or operational concerns require sign-off. | Manual peer or architect sign-off required before promotion. |
| **`INSUFFICIENT_EVIDENCE`** | Pipeline may have passing rules, but critical telemetry or operational evidence is missing. | Supply required telemetry before production certification. |

### Overall Quality Score Bands
- **`EXCELLENT`** (≥ 90.0)
- **`GOOD`** (80.0 – 89.9)
- **`NEEDS_IMPROVEMENT`** (70.0 – 79.9)
- **`HIGH_RISK`** (50.0 – 69.9)
- **`CRITICAL`** (< 50.0)

### Production Blockers (P0) & Causal Risk Chains
A pipeline with a 95/100 score will still be flagged **`NOT_PRODUCTION_READY`** if a single P0 blocker is present. M5I links related risks into **Causal Risk Chains**, such as:
> `Append Write Mode without Transaction Boundary`  
> ➔ `Automated Job Retry Enabled`  
> ➔ `Absence of Target Key Uniqueness Constraint`  
> ➔ `Silent Data Duplication on Transient Failure`

---

## 16. Generated Reports

Every validation run automatically persists its complete results to disk.

### Directory Structure

Reports are saved in timestamped run folders using filesystem-safe ISO timestamps (`YYYY-MM-DDTHH-MM-SS`) to prevent accidental overwrites:

```text
reports/
├── online/
│   └── <job-or-pipeline-id>/
│       └── 2026-09-17T09-41-31/
│           ├── validation.json
│           └── validation.md
└── offline/
    └── <contract-name>/
        └── 2026-09-17T11-30-22/
            ├── validation.json
            └── validation.md
```

### Report Files

#### `validation.json`
Complete, machine-readable full-fidelity result containing:
- Discovered job, pipeline, and workspace metadata.
- AST metrics and table lineage.
- Evidence diagnostics and provenance mappings (`LIVE_API`, `DERIVED`, `UNAVAILABLE`).
- Complete checkpoint results (`CP-001` through `CP-024`) with all evaluated rules and findings.
- M5E, M5F, M5G, M5H, and M5I forensic models.
- Deep credential masking applied to all fields.

#### `validation.md`
A human-readable markdown report containing 8 core sections:
1. **Validation Summary**: Target ID, name, environment, score, confidence, and final decision.
2. **Evidence Summary Table**: Status, provenance, identifier, and diagnostics for each category.
3. **Checkpoint Results Table**: Status (`PASS`, `WARN`, `FAIL`, `UNKNOWN`), severity, score, and finding counts for `CP-001`..`CP-024`.
4. **Developer Implementation Forensics (M5E)**: Evaluated dimensions, status, and findings.
5. **Rerun / Idempotency Forensics (M5F)**: Scenario evaluations, duplicate risk, and data loss risk.
6. **Three-Layer Alignment Forensics (M5G)**: Drift severity, divergence types, and layer mappings.
7. **Evidence Sufficiency (M5H)**: Coverage percentage, confidence tier, and list of missing evidence.
8. **Production Decision (CP-FINAL & M5I)**: Blockers list, top risks catalog, causal risk chains, and prioritized remediation actions (`P0`, `P1`, `P2`, `P3`).

---

## 17. Custom Output Directory

Use `--output-dir` (or `-o`) to store validation reports in a custom location:

```bash
dpif validate-online --job-id 159900604537188 --output-dir /tmp/dpif_ci_reports
```

Or for offline validation:
```bash
dpif validate --contract examples/customer_daily.yaml --offline --output-dir ./artifacts/
```

DPIF creates the target directory structure if it does not exist and outputs the exact file paths upon completion.

---

## 18. JSON Mode

Use the `--json` flag to emit machine-readable JSON directly to standard output for scripting and CLI piping:

```bash
dpif validate-online --job-id 159900604537188 --json | jq .final_decision
```

> [!TIP]
> Using `--json` **does not disable file persistence**. DPIF continues to write both `validation.json` and `validation.md` to disk, while suppressing console formatting so stdout remains clean JSON suitable for piping to `jq`.

---

## 19. Fleet Validation (M5K)

Fleet validation evaluates entire portfolios of Databricks jobs and pipelines against enterprise policy rules.

### Running Fleet Validation

```bash
dpif validate-online-fleet --manifest manifests/enterprise_fleet.yaml --environment production
```

### Fleet Manifest (`manifests/enterprise_fleet.yaml`)
```yaml
name: core-enterprise-pipelines
environment: production
workspace_host: https://dbc-56a6310c-9183.cloud.databricks.com
pipelines:
  - id: customer_360
    job_id: 159900604537188
  - id: telemetry_pipeline
    pipeline_id: 2f4a03c2-e50f-418a-b84a-79d4fb2f0d8f
```

### Fleet Features
- **Bounded Concurrency (`-w, --max-workers`)**: Validates pipelines in parallel using a thread pool (default: 4 workers).
- **Cross-Pipeline Collision Detection**: Scans across all pipelines in the manifest to detect shared target table write collisions (`CONFIRMED`, `POTENTIAL`, `UNKNOWN`).
- **Policy Tier Gating**: Evaluates results against tier-specific thresholds (`development`, `staging`, `production`) for minimum quality score, required confidence level, and P0 blocker tolerance.
- **Export Capabilities**:
  - `--export-markdown <PATH>`: Generates a PR summary table.
  - `--export-junit <PATH>`: Generates JUnit XML test results for CI/CD test dashboards.
  - `--export-sarif <PATH>`: Exports OASIS SARIF v2.1.0 security/code vulnerability reports.

---

## 20. CI/CD Usage

DPIF integrates directly into CI/CD pipelines (GitHub Actions, GitLab CI, Azure DevOps).

### CLI Exit Codes

#### For `validate` and `validate-online`:
- **`0`**: **Validation completed successfully.** The engine ran and produced a verdict. Note: An exit code of 0 does **not** mean the pipeline is production ready; a failing pipeline produces findings and exits 0 because the *validation execution* succeeded.
- **`1` or `2`**: **System or configuration failure.** Syntax error, invalid CLI flags, unauthenticated API calls, or missing local files.

#### For `validate-online-fleet`:
- **`0`**: **Fleet satisfies configured policy.** All pipelines met the minimum environment criteria.
- **`1`**: **Fleet validation completed but policy gate failed.** One or more pipelines contained blockers or fell below required thresholds.
- **`2`**: **System or configuration error.** Malformed manifest or unauthenticated workspace.

### Example GitHub Actions Workflow
```yaml
name: Databricks Pipeline Quality Gate

on:
  pull_request:
    branches: [ main ]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install DPIF
        run: pip install .

      - name: Run Fleet Policy Gate
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          dpif validate-online-fleet \
            --manifest manifests/production_fleet.yaml \
            --environment staging \
            --export-junit reports/junit.xml \
            --export-markdown reports/summary.md

      - name: Publish Test Results
        uses: EnricoMi/publish-unit-test-result-action@v2
        if: always()
        with:
          files: reports/junit.xml
```

---

## 21. Troubleshooting

### 1. Authentication Failure (`401 Unauthorized`)
- **Symptom**: `Validation crashed: Databricks API error (401): Invalid access token`
- **Resolution**: Verify that `DATABRICKS_TOKEN` is active and has not expired. Test with `curl -H "Authorization: Bearer $DATABRICKS_TOKEN" $DATABRICKS_HOST/api/2.0/clusters/list`.

### 2. Resource Not Found (`404 Not Found`)
- **Symptom**: `Job 12345: AcquisitionError (404)`
- **Resolution**: Confirm the Job ID exists in the specified workspace URL. In multi-workspace enterprises, ensure `DATABRICKS_HOST` matches the target workspace.

### 3. Missing Permissions
- **Symptom**: Job details load, but notebook export fails or historical runs return empty.
- **Resolution**: The PAT must have `CAN_VIEW` or `CAN_MANAGE` permissions on the target Job and read access to the workspace notebook path.

### 4. Checkpoints Returning `UNKNOWN`
- **Symptom**: `CP-008 Performance Validation: UNKNOWN`, `CP-023 SLA Validation: UNKNOWN`
- **Explanation**: This is expected behavior when a job has never been run or when active runtime execution telemetry is not supplied. DPIF does not invent runtime numbers. To resolve, ensure the job has at least one completed execution run.

### 5. Serverless / Pipeline Compute
- **Symptom**: `CP-009 Cluster Validation: UNKNOWN`
- **Explanation**: When using serverless compute or pipeline-managed compute where classic cluster configurations do not exist, cluster sizing evidence will be marked `UNAVAILABLE`.

---

## 22. Security

DPIF adheres to enterprise data security standards:

- **Read-Only Inspection**: Online validation interacts exclusively with read-only Databricks REST API endpoints (GET `/api/2.0/jobs/get`, GET `/api/2.0/runs/list`, GET `/api/2.0/workspace/export`). DPIF **never creates, modifies, triggers, or deletes** any jobs, clusters, tables, or pipelines.
- **Zero Credential Persistence**: Tokens, authorization headers, passwords, and API secrets are dynamically redacted via `mask_sensitive_credentials` before being written to console output, logs, `validation.json`, or `validation.md`.
- **Git Protection**: Default `.gitignore` rules exclude `.env`, `.env.*`, `reports/`, and local caches from being tracked.

---

## 23. Recommended First Run

Follow these 10 steps to validate your first pipeline:

1. **Clone and setup**:
   ```bash
   git clone https://github.com/kingofrajgit/databricks-pipeline-intelligence.git
   cd databricks-pipeline-intelligence
   python -m venv .venv
   source .venv/bin/activate  # Or .venv\Scripts\Activate.ps1 on Windows
   pip install -e .
   ```

2. **Configure your workspace**:
   Create `.env`:
   ```env
   DATABRICKS_HOST=https://<workspace-id>.cloud.databricks.com
   DATABRICKS_TOKEN=<your-token>
   ```

3. **Run online validation**:
   ```bash
   dpif validate-online --job-id <YOUR_DATABRICKS_JOB_ID>
   ```

4. **Review terminal output**: Check the high-level `QUALITY SCORE`, `CONFIDENCE`, and `BLOCKERS`.
5. **Open Markdown report**: Review `reports/online/<job-id>/<timestamp>/validation.md`.
6. **Inspect Evidence Summary**: Identify which evidence categories were `LIVE` vs `UNAVAILABLE`.
7. **Inspect Checkpoints**: Check `CP-001` through `CP-024` for `FAIL` or `WARN` statuses.
8. **Review M5E & M5F Forensics**: Check implementation quality and rerun safety.
9. **Review M5H Evidence Sufficiency**: Verify whether decision sufficiency is `TRUE` or `FALSE`.
10. **Review CP-FINAL Decision**: Address the prioritized `P0` and `P1` action items.

---

## 24. Command Reference

| Command | Purpose | Required Flags | Key Optional Flags | Primary Output |
|:---|:---|:---|:---|:---|
| **`dpif validate`** | Validate a pipeline contract (offline) or batch manifest. | `-c, --contract <PATH>` OR `-i, --input <CSV>` | `--offline`, `--code-path`, `--output-dir`, `--json` | Console summary + `reports/offline/...` |
| **`dpif validate-online`** | Validate a live Databricks job or pipeline end-to-end. | `--job-id <INT>` OR `--pipeline-id <STR>` | `--workspace`, `--token`, `--output-dir`, `--json` | Console summary + `reports/online/...` |
| **`dpif validate-online-fleet`** | Validate a portfolio of pipelines against enterprise policy. | `-m, --manifest <YAML>` | `-e, --environment`, `--export-junit`, `--export-markdown`, `--export-sarif`, `-w, --max-workers` | Console summary + exported JUnit/Markdown/SARIF |

---

## 25. Frequently Asked Questions (FAQ)

### Do I need to upload Python or PySpark code for online validation?
**No.** DPIF automatically queries the Databricks Workspace Export API using the notebook or script path declared in the Job task definition and analyzes the source code automatically.

### Do I need to provide SQL files?
**No.** If SQL queries are embedded in PySpark `spark.sql(...)` statements or stored in task notebooks, DPIF extracts and parses them with `sqlglot` automatically.

### Do I need to provide source and target table lists?
**No.** DPIF inspects the code's Abstract Syntax Tree (AST) to identify input tables (`spark.read.table(...)`) and output targets (`df.write.saveAsTable(...)`), automatically populating the pipeline contract.

### Does DPIF modify my Databricks environment?
**No.** All connector operations are strictly read-only HTTP GET requests. DPIF never triggers job runs, creates clusters, or modifies table contents.

### What does `UNKNOWN` mean?
`UNKNOWN` means necessary evidence was missing or insufficient to establish a factual verdict. It is not a failure, but an indicator of missing telemetry or documentation.

### Why can a pipeline get `NOT_PRODUCTION_READY` despite a passing score?
If a pipeline has a single blocking P0 risk (such as missing retry configurations or non-idempotent writes under automatic retries), the release gate is blocked regardless of numeric score.

### Where are validation reports saved?
By default, reports are saved to `reports/online/<id>/<timestamp>/` (or `reports/offline/...`). You can customize this with the `--output-dir <PATH>` option.

### Can I use DPIF without a Databricks account?
**Yes.** DPIF has full offline validation support (`dpif validate --contract <path> --offline`), which runs entirely on your local machine using static files and fixtures.
