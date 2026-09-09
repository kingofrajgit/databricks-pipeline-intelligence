# Enterprise Multi-Pipeline Validation Gap Analysis

## Executive Summary

This document performs a formal architectural audit of the Databricks Pipeline Intelligence Framework (DPIF) to support enterprise multi-pipeline validation via CSV manifest files while preserving single-pipeline offline and future online execution paths.

---

## 1. CURRENT IMPLEMENTATION

### Single Pipeline Entry Point
Currently, DPIF accepts pipeline validation requests primarily through the CLI (`dpif validate --offline --contract <contract.yaml>`) or direct invocation of internal helper functions.
The current entry function `_run_offline_validation` in `src/dpif/cli.py`:
1. Loads a single pipeline contract YAML (`load_contract_file(contract_path)`).
2. Loads a metadata profile JSON (`_load_metadata_profile(...)`).
3. Loads code text (`_load_code_text(...)`) and parses AST/SQL (`analyze_source(...)`).
4. Builds rule context and optionally loads runtime & historical execution run JSON fixtures.
5. Invokes `CheckpointEngine().run_all_checkpoints(checkpoints, context)`.
6. Evaluates CP-001 through CP-024 via `score_checkpoints` and `evaluate_production_readiness(...)`.
7. Outputs console summary text or JSON for a single pipeline run.

### Data Models & Architecture
- **Contract Models**: Defined in `src/dpif/contract/loader.py` and `src/dpif/models.py` (`PipelineContract`, `SourceConfig`, `ScalabilityConfig`, etc.).
- **Checkpoint Engine**: `CheckpointEngine` in `src/dpif/checkpoints/engine.py` evaluates checkpoints `CP-001` to `CP-024` with zero state leakage when instantiated per run.
- **Readiness Engine**: `evaluate_production_readiness(...)` produces a `ProductionReadinessAssessment` (`CP-024`), assessing overall readiness (`PRODUCTION_READY`, `PRODUCTION_READY_WITH_WARNINGS`, `NOT_PRODUCTION_READY`, `INSUFFICIENT_EVIDENCE`).
- **Connectors**: `OfflineDatabricksConnector` and `LiveDatabricksConnector` under `src/dpif/connectors/`.

---

## 2. EXPECTED IMPLEMENTATION

The multi-pipeline validation system allows enterprise teams with $N$ developers to submit pipeline contracts and evidence files registered in a CSV manifest:

```csv
pipeline_id,developer,contract_path,code_path,metadata_profile,runtime_run,historical_runs
P001,DeveloperA,contracts/customer.yaml,code/customer.py,metadata/customer.json,runtime/customer.json,history/customer.json
P002,DeveloperB,contracts/orders.yaml,code/orders.py,metadata/orders.json,runtime/orders.json,history/orders.json
```

### High-Level Architecture Flow
```
                     CSV Manifest File
                            │
                            ▼
              Manifest Parser & Validator
             (Strict validation, security check)
                            │
                            ▼
               Batch Validation Orchestrator
             (Isolated Per-Pipeline Processing)
         ┌──────────────────┼──────────────────┐
         ▼                  ▼                  ▼
    Pipeline P001      Pipeline P002      Pipeline P003
   (Isolated Context) (Isolated Context) (Isolated Context)
         │                  │                  │
         ▼                  ▼                  ▼
   DPIF Core Engine   DPIF Core Engine   DPIF Core Engine
   (CP-001..CP-024)   (CP-001..CP-024)   (CP-001..CP-024)
         │                  │                  │
         └──────────────────┼──────────────────┘
                            │
                            ▼
              Batch Result Aggregator
             (Consolidated JSON & CSV Report)
                            │
                            ▼
                       CLI Summary
```

---

## 3. GAPS

1. **No Manifest / Batch Input Handling**: The CLI currently only accepts `--contract` for a single pipeline file. There is no `--input <csv_path>` manifest option.
2. **Missing Manifest Schema & Validation Engine**: No loader exists to parse CSV manifests, validate required columns, detect duplicate IDs, check path existence, or handle malformed/empty CSVs safely.
3. **No Batch Result & Pipeline Isolation Models**: No structured data model represents a batch run containing multiple pipeline validation results along with processing statuses (`VALIDATION_COMPLETE`, `INVALID_SUBMISSION`, `MISSING_INPUT`, `PROCESSING_ERROR`).
4. **No Failure Isolation Mechanism**: Currently, file loading errors or unhandled exceptions in `_run_offline_validation` crash the process. In a batch of 100 pipelines, one broken pipeline must NOT halt the remaining 99.
5. **No Batch Report Generator**: Currently reporting formats output for a single pipeline (`assessment.to_dict()` or stdout terminal text). Consolidated multi-pipeline `batch_report.json` and optional `batch_report.csv` reports are required.

---

## 4. REUSE PLAN

We will strictly reuse existing core engines without rewriting domain rules:
- **`src/dpif/checkpoints/engine.py`**: Reuse `CheckpointEngine` per pipeline.
- **`src/dpif/checkpoints/definitions.py`**: Reuse `build_all_checkpoints(...)`.
- **`src/dpif/contract/loader.py`**: Reuse `load_contract_file(...)` and `contract_cluster_job(...)`.
- **`src/dpif/readiness/engine.py`**: Reuse `evaluate_production_readiness(...)`.
- **`src/dpif/scoring/engine.py`**: Reuse `score_checkpoints(...)` and `readiness_label(...)`.
- **`src/dpif/code/parser.py`**: Reuse `analyze_source(...)`.
- **`src/dpif/runtime/normalization.py`**: Reuse `normalize_runtime_payload(...)`.

---

## 5. IMPLEMENTATION PLAN

1. **Data Models (`src/dpif/orchestration/models.py`)**:
   - Define `PipelineSubmission`, `PipelineProcessingStatus`, `PipelineValidationResult`, and `BatchValidationResult`.
2. **CSV Loader & Validator (`src/dpif/orchestration/manifest.py`)**:
   - Parse CSV manifests, validate mandatory columns (`pipeline_id`, `developer`, `contract_path`, `code_path`), guard against path traversal, check file existence, and enforce unique pipeline IDs.
3. **Batch Orchestrator (`src/dpif/orchestration/batch.py`)**:
   - Run pipeline validations with per-pipeline isolated contexts (`run_pipeline_validation`).
   - Wrap per-pipeline processing in exception handlers to guarantee failure isolation.
4. **Consolidated Reporting (`src/dpif/reporting/batch.py`)**:
   - Generate `batch_report.json` and optional `batch_report.csv`.
5. **CLI Integration (`src/dpif/cli.py`)**:
   - Add `--input` / `-i` option to `dpif validate`, supporting batch validation while preserving single-pipeline `--contract` usage.
6. **Tests & Verification**:
   - Add unit and integration tests covering single/multiple pipelines, malformed CSVs, path traversal checks, failure isolation, state isolation, and 100-pipeline batch execution performance.
