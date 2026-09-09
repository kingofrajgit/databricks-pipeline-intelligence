# Multi-Pipeline Validation Implementation Plan

## Overview
Implement enterprise offline batch validation for Databricks Pipeline Intelligence Framework (DPIF).

## Phases & Roadmap

### M0: Repository Audit & Gap Analysis
- Perform full repository audit.
- Create `docs/architecture/multi-pipeline-validation-gap-analysis.md`.
- Verify existing tests, Ruff, and mypy pass.
- Acceptance Criteria: Gap analysis document created and verified against source code; quality checks pass.

### M1: Batch Manifest Models & CSV Loader/Validator
- Create `src/dpif/orchestration/models.py` (`PipelineSubmission`, `PipelineProcessingStatus`, `PipelineValidationResult`, `BatchValidationResult`).
- Create `src/dpif/orchestration/manifest.py` for loading and validating CSV manifests (`pipeline_id,developer,contract_path,code_path,metadata_profile,runtime_run,historical_runs`).
- Enforce path traversal safety and file access bounds.
- Acceptance Criteria: Strict validation of headers, missing fields, duplicate IDs, invalid/unsafe paths, malformed CSVs; unit tests added and passing.

### M2: Isolated Multi-Pipeline Orchestration
- Create `src/dpif/orchestration/batch.py` to run isolated validations per pipeline.
- Implement per-pipeline failure isolation (exceptions in P001 do not stop P002).
- Ensure zero shared mutable state between pipelines.
- Acceptance Criteria: Pipeline A and Pipeline B have completely isolated results, scores, findings, and readiness statuses. Failure in P001 records `PROCESSING_ERROR` or `MISSING_INPUT` while batch continues.

### M3: Consolidated Batch Results Aggregation
- Implement batch result synthesis in `src/dpif/orchestration/models.py` / `batch.py`.
- Summarize total submissions, processing status breakdown, CP-024 readiness breakdown, and severity counts.
- Acceptance Criteria: Consolidated result object correctly aggregates findings, scores, and readiness without leaking state.

### M4: Batch Reporting & CLI Integration
- Implement `generate_batch_json_report` (`batch_report.json`) and `generate_batch_csv_report` (`batch_report.csv`) in `src/dpif/reporting/batch.py`.
- Integrate `--input` / `-i` flag into `dpif validate --offline --input pipelines.csv` while maintaining backward compatibility for `dpif validate --offline --contract pipeline.yaml`.
- Update documentation (`docs/requirements/multi-pipeline-validation.md` and `docs/runbooks/offline-developer-validation.md`).
- Add comprehensive test suite including 100-pipeline batch performance test and mandatory isolation test.
- Acceptance Criteria: Full test suite, Ruff, and mypy pass; CLI outputs concise batch summaries and JSON/CSV reports.
