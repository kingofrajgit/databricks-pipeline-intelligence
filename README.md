# Databricks Pipeline Intelligence Framework (DPIF)

## Overview

DPIF is a production-oriented framework that inspects Databricks data pipelines and determines:
- Whether the pipeline is correctly designed
- Whether the source is appropriate and correctly configured
- Whether the input data characteristics are understood
- Whether the Python/PySpark/SQL code is correctly written
- Whether the code is appropriately optimized for the actual data characteristics
- Whether the cluster configuration is appropriate
- Whether the Databricks Job configuration is appropriate
- Whether the pipeline is reliable and restartable
- Whether the pipeline can handle expected future data growth
- Whether the pipeline is likely to meet its SLA
- Whether the pipeline has cost/performance risks
- Whether security and governance requirements are satisfied
- What is missing, what should be changed, and whether the pipeline is production ready

## Philosophy

The framework does NOT behave like a simplistic checklist. Recommendations are always based on context, combining:
- Pipeline Requirements
- Databricks Configuration
- Source Information
- Data Profile
- Code Analysis
- Runtime Metrics
- Historical Performance
- Rules
- Scalability Analysis
- Evidence

The three-layer validation model compares EXPECTED vs IMPLEMENTED vs ACTUAL.

**Phase 2 status:** the framework is a genuinely executable, testable OFFLINE
validation engine. No live Databricks credentials are required.

## How DPIF works

1. **Pipeline Contract** (`--contract pipeline.yaml`) declares what the pipeline
   is *supposed* to do: source, volumes, processing strategy, target, SLA,
   schedule, reliability, cluster/job expectations. See
   `tests/fixtures/contracts/` for five realistic examples.
2. **Checkpoints** (CP-001 … CP-024) each evaluate one dimension — source, data,
   code, cluster, job, incremental processing, retries, security, … — and return
   `PASS`, `WARN`, `FAIL`, `NOT_APPLICABLE`, or `UNKNOWN`.
3. **Rules** (`rules/`, YAML-defined) feed the code checkpoint with
   context-aware findings: a `collect()` over 500 GB is a blocking FAIL, the
   same call over a 50-row lookup is a qualified warning. Findings carry
   evidence, recommendation, confidence, and blocking flags — never invented
   metrics.
4. **Scoring** aggregates checkpoint outcomes (PASS=100, WARN=60, FAIL=0,
   UNKNOWN=0). Any blocking FAIL forces `NOT_PRODUCTION_READY` regardless of
   the numeric score.
5. **UNKNOWN is strict**: unavailable information (no pricing data, no runtime
   metrics, missing config) is reported as UNKNOWN with a reason — it is never
   converted into PASS.

## Quick Start

```bash
# Install (Python 3.11+)
pip install -e ".[dev]"

# Show help
dpif --help
dpif validate --help

# Offline validation (no Databricks credentials needed)
dpif validate --contract examples/customer_daily.yaml --offline

# Expected exit codes:
#   0        validation completed (even when findings contain FAIL)
#   non-zero execution/system error (bad args, missing file, crash)
# "Pipeline failed" is a result, not a crash.
```

Example output (good pipeline):

```text
DPIF Validation

    Pipeline:    customer_daily
    Environment: production
    Mode:        OFFLINE (fixture metadata, not runtime measurements)

CHECKPOINTS
    Source        : PASS
    Data          : PASS
    Code          : PASS
    ...
    Cost          : UNKNOWN
    SLA           : UNKNOWN

    Overall Score: 77/100
    Status: NOT_PRODUCTION_READY
```

Cost/SLA stay UNKNOWN offline because DPIF never fabricates pricing or
runtimes.

## Pipeline Contract

A contract is a YAML file with `pipeline`, `source`, `processing`, `target`,
`sla`, `schedule`, `reliability`, `scalability`, plus optional `cluster`,
`job`, and `code_path` sections. The loader (`src/dpif/contract/loader.py`)
validates it into a `PipelineContract` Pydantic model. Fixtures:

- `small_batch_pipeline.yaml` — 10 GB incremental dev workload
- `customer_daily_pipeline.yaml` — 500 GB/day, 3 TB peak (reference)
- `large_volume_pipeline.yaml` — 1 TB/day incremental
- `incremental_pipeline.yaml` — CDC workload
- `bad_production_pipeline.yaml` — full reload at 2 TB, no retries (fails well)

## Checkpoints

| ID | Name | Needs | Missing info → |
|---|---|---|---|
| CP-001 | Source | contract source | UNKNOWN |
| CP-004 | Code | code text + rules | UNKNOWN (no code) |
| CP-007 | Data | data profile | UNKNOWN |
| CP-009 | Cluster | cluster config | UNKNOWN |
| CP-011 | Job | job config | UNKNOWN |
| CP-012 | Incremental | contract strategy + volume | UNKNOWN |
| CP-013 | Error Handling | code | UNKNOWN |
| CP-014 | Retry | retry config | UNKNOWN |
| CP-015 | Restartability | checkpoint info | UNKNOWN |
| CP-016 | Idempotency | contract reliability | UNKNOWN |
| CP-019 | Cost | pricing/DBU data | UNKNOWN (always offline) |
| CP-020 | Security | code | UNKNOWN (no code) |
| CP-021 | Governance | contract owner/meta | WARN/UNKNOWN |
| CP-022 | Data Quality | data profile | UNKNOWN |
| CP-023 | SLA | runtime metrics | UNKNOWN (always offline) |
| CP-024 | Readiness | all of the above | UNKNOWN on failed deps |

**Dependency rule:** a FAIL or UNKNOWN dependency forces the dependent
checkpoint to UNKNOWN (documented in `docs/adr/adr-002-*.md`, tested in
`tests/unit/test_checkpoint_engine.py`).

## Rules

Rules live in `rules/` as versioned YAML (`rule_id`, `name`, `category`,
`severity`, `condition`, `evidence_required`, `recommendation`, `blocking`,
`score`, `version`, plus `context_aware` / `requires_context`). Current set:

- `CODE-PYSPARK-001` Driver Collection Detection (CRITICAL, blocking)
- `CODE-PYSPARK-002` Pandas UDF Misuse (HIGH)
- `CODE-PYSPARK-003` show()/take() in Production (HIGH)
- `CODE-PYSPARK-004` CrossJoin Detection (MEDIUM, needs `data_size_gb`)
- `CODE-PYSPARK-005` Hard-coded Path Detection (HIGH)

## Findings

Every finding contains `rule_id`, `category`, `severity`, `status`, `title`,
`description`, `evidence` (locations + observed/expected), `recommendation`,
`confidence`, and `blocking`. Secrets are masked in reports and never logged.

## Offline mode & connectors

`DatabricksConnector` (`src/dpif/connectors/base.py`) is the single seam for
Databricks access. The framework supports `OfflineDatabricksConnector` and `LiveDatabricksConnector`.

### Online Evidence Acquisition Architecture (M5A)

```text
Databricks Workspace
        ↓
LiveDatabricksConnector (REST API 2.0 / 2.1)
        ↓
DatabricksEvidenceProvider (Evidence Boundary)
        ↓
Normalized Pipeline Evidence & Provenance
        ↓
Existing Validation Engine (CP-001..CP-024)
        ↓
Production Readiness & Reports
```

Key Principles:
- **Evidence Acquisition != Validation**: Online mode acquires evidence via `DatabricksEvidenceProvider` without embedding validation rule logic.
- **Evidence Provenance**: Every payload carries full provenance (`LIVE_API` vs `FIXTURE`, `acquired_at`, workspace reference).
- **Strict UNKNOWN Semantics**: If Databricks API cannot provide a required piece of evidence (or returns 404), the category remains `UNKNOWN` / insufficient evidence. It is never converted to PASS or fabricated.
- **Credential Security**: Credentials (`DATABRICKS_TOKEN`, Bearer tokens) are masked at the API seam and never recorded in logs, errors, findings, or reports.
- **Connector Mode Seam**: `connector_mode` supports both `offline` fixture mode and `live` (`live-api`) online mode seamlessly downstream.

Synthetic metadata fixtures (`tests/fixtures/metadata/`, JSON, metadata only —
never multi-terabyte files): 10 GB, 100 GB, 500 GB, 1 TB, 3 TB, plus
small-file (1.8 M files @ ~284 KB), partition-skewed, and huge-growth
scenarios.

## Scoring

Category means mapped from PASS=100 / WARN=60 / FAIL=0 / UNKNOWN=0
(NOT_APPLICABLE excluded), then weighted mean → 0–100 with status bands
EXCELLENT ≥90, GOOD ≥80, NEEDS_IMPROVEMENT ≥70, HIGH_RISK ≥50, else CRITICAL.
Any blocking FAIL forces CRITICAL + `NOT_PRODUCTION_READY`.

## Test strategy

```bash
pytest          # 71 tests: models, config, rules (5×5), checkpoints, scoring, e2e
python -m ruff check src tests
python -m mypy src/dpif
```

- `tests/unit/` — models, config, rule engine (25 rule tests), checkpoints,
  scoring
- `tests/integration/test_offline_validation.py` — good vs bad pipeline e2e,
  small-file WARN, incremental, CLI exit codes, secret masking
- `tests/sample_pipelines/` — ADLS→Parquet→PySpark→Delta→Job scenario descriptors

## Pydantic v2 model fix (Phase 2)

Phase 1 models used Pydantic `Field()` without inheriting `BaseModel`, so
`Source(...)` raised `TypeError: Source() takes no arguments`. Phase 2 makes
every model a proper `BaseModel` subclass, so normal keyword construction
works: `Source(source_id="source-001", name="customer-source", type="adls",
format="parquet")`. Details in `docs/adr/adr-003-pydantic-models.md`.

## Repository Structure

```text
src/dpif/
├── cli.py            ← CLI (validate --contract ... --offline)
├── config.py         ← Settings (DPIF_* env vars, thresholds)
├── error_handling.py ← DPIFError hierarchy, logging
├── models.py         ← Pydantic models (contract, checkpoints, findings…)
├── contract/loader.py← YAML contract → PipelineContract
├── connectors/       ← DatabricksConnector / OfflineDatabricksConnector
├── checkpoints/      ← engine + definitions (CP-001…CP-024)
├── rules/engine.py   ← context-aware YAML rule engine
├── scoring/engine.py ← category + overall scoring
rules/code/           ← 5 versioned YAML rules
tests/
├── unit/             ← models, config, rules, checkpoints, scoring
├── integration/      ← offline e2e (good vs bad pipeline)
├── fixtures/         ← contracts, metadata, code samples
└── sample_pipelines/ ← scenario descriptors
```

## Documentation

- `docs/architecture/` - Architecture decision records and diagrams
- `docs/requirements/` - Product requirements and user stories
- `docs/adr/` - Architectural Decision Records
- `docs/development/` - Development guidelines and onboarding

## Development phases

Development is incremental across 15+ phases. Phase 2 (offline validation
engine) is complete; do not start Phase 3 without review. See
`docs/development/development_roadmap.md`.

## License

[Add license]
