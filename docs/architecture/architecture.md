# DPIF Architecture Document

## 1. System Overview

The Databricks Pipeline Intelligence Framework (DPIF) is a static and dynamic analysis framework that evaluates Databricks data pipelines across multiple dimensions to determine production readiness.

The framework uses a **three-layer validation model**:

```
Layer 1 — DESIGN VALIDATION: What was the pipeline supposed to do?
Layer 2 — IMPLEMENTATION VALIDATION: What did the developer actually build?
Layer 3 — RUNTIME VALIDATION: What actually happened?

The final system compares: EXPECTED vs IMPLEMENTED vs ACTUAL
```

## 2. Core Components

### 2.1 Pipeline Contract (YAML + Pydantic models)
- Defines the expected pipeline behavior
- Contains source, target, schedule, SLA, volume, growth, reliability, and security specs
- Represented using Pydantic models for validation and serialization

### 2.2 Source Intelligence
- Identifies source types (ADLS, S3, GCS, LOCAL FILE, CSV, JSON, PARQUET, AVRO, ORC, JDBC, REST API, KAFKA, EVENT HUB, STREAMING, DELTA)
- Collects data profiles (file sizes, record counts, schema, partitioning)
- Distinguishes metadata-based vs sample-based vs full-data analysis

### 2.3 Rule Engine
- YAML-defined rules for static analysis
- Each rule contains: rule_id, name, category, description, severity, condition, evidence, recommendation, blocking, score, version
- Rules are independently testable and externally defined

### 2.4 Checkpoint Engine
- First-class checkpoint model with: checkpoint_id, name, category, status, severity, score, evidence, findings, recommendations, timestamp, assumptions
- Status: PASS, WARN, FAIL, NOT_APPLICABLE, UNKNOWN
- Checkpoints can depend on previous checkpoints
- If a dependency fails, dependent checks default to UNKNOWN (not PASS)

### 2.5 Analyzers (Plug-in Architecture)
Each analyzer follows a consistent interface:

| Analyzer | Inspects |
|---|---|
| Source | Source type, configuration, data profile |
| Data | File sizes, partitioning, skew, small files |
| Python | AST-based static analysis |
| PySpark | PySpark code patterns |
| SQL | SQL static analysis |
| Cluster | Databricks cluster configuration |
| Job | Databricks Job configuration |
| Pipeline | Pipeline dependencies, strategy |
| Performance | Runtime metrics, spill, skew |
| Scalability | Volume growth, runtime projection |
| Security | Credentials, permissions, Unity Catalog |
| Cost | DBUs, runtime, cost per run |
| Reliability | Retry, idempotency, restartability |
| Governance | Ownership, tags, lineage, permissions |

### 2.6 Scoring Engine
- Produces category-level and overall scores
- Configurable weights per organization profile
- Category scores: Source, Data, Code, Pipeline, Cluster, Job, Performance, Scalability, Reliability, Security, Governance, Cost
- Critical blocking findings override overall score

### 2.7 Evidence Model
- Every finding has structured evidence
- Includes: rule_id, status, severity, observed, expected, evidence list, recommendation, confidence
- Records which analysis method was used (metadata-based, sample-based, full-data)

### 2.8 Three-Layer Validation Model

```
                              +-------------------+
                              |  EXPECTED         |
                              |  (Contract, SLA)  |
                              +---------+---------+
                                        |
                              +---------v---------+
                              | IMPLEMENTED       |
                              |  (Code, Config)   |
                              +---------+---------+
                                        |
                              +---------v---------+
                              |     ACTUAL        |
                              |  (Runtime, Metrics)|
                              +-------------------+
```

## 3. Data Flow

```
Pipeline Contract
        +
Databricks Configuration
        +
Source Information
        +
Data Profile
        +
Code Analysis
        +
Runtime Metrics
        +
Historical Performance
        +
Rules
        +
Scalability Analysis
        +
Evidence
        ↓
Production Readiness Assessment
```

## 4. Key Design Decisions

### 4.1 Evidence-Driven
Every finding must have structured evidence. "Looks correct" is not sufficient — there must be proven evidence supporting each conclusion.

### 4.2 Unknown is Important
The framework distinguishes PASS, FAIL, WARNING, and UNKNOWN. UNKNOWN means "we do not have sufficient evidence." UNKNOWN is never converted to PASS.

### 4.3 Context-Aware Rules
Rules are not universally true/false. Each rule's finding includes:
- What was detected?
- Why is it a problem?
- What evidence supports the finding?
- What is the severity?
- What should be changed?
- What is the expected impact?
- How confident are we?
- What assumptions were made?

### 4.4 Incremental Development
The framework is built across 15+ phases. Phase 1 must pass completely before Phase 2 begins.

### 4.5 Offline-First
A dry-run mode works without a live Databricks environment, using fixture metadata. This allows development without Databricks credentials.

### 4.6 Modular Checkpoints
Checkpoints are modular and can depend on previous checkpoints. No giant file contains all checkpoint logic.

### 4.7 Configurable Everything
- Thresholds are configurable
- Rules are versioned
- Scoring weights are configurable per organization profile
- Analysis methods are recorded

### 4.8 No Fabricated Runtime
If the framework cannot execute the pipeline or retrieve runtime metrics, it raises NotImplementedError or reports "Runtime prediction unavailable" with the reason "Insufficient historical execution data."

## 5. Technology Stack

- **Backend**: Python 3.11+, FastAPI, Pydantic, SQLAlchemy, Alembic, PostgreSQL
- **Data/Spark**: PySpark, PyArrow, Delta Lake metadata
- **Static Analysis**: Python AST, Ruff, mypy, pytest, SQL parser (via library)
- **Databricks**: Official Databricks Python SDK, supported Databricks APIs
- **Abstraction**: DatabricksConnector (single abstraction, not scattered REST calls)
- **Frontend**: React, TypeScript (planned)
- **DevOps**: Docker, Docker Compose, Git, CI/CD

## 6. Repository Structure (confirmed)

See README.md for the full directory structure. Key areas:
- `src/dpif/` - Core Python package
- `rules/` - YAML rule definitions
- `tests/` - Unit and integration tests
- `docs/` - Documentation and ADRs
- `frontend/` - React + TypeScript (planned)

## 7. API Design

REST endpoints include:
- `POST /api/v1/validation` - Start a validation run
- `GET /api/v1/validation/{id}` - Get validation run status
- `GET /api/v1/validation/{id}/findings` - Get findings
- `GET /api/v1/validation/{id}/checkpoints` - Get checkpoints
- `GET /api/v1/validation/{id}/report` - Get report
- `POST /api/v1/pipelines` - Create pipeline
- `GET /api/v1/rules` - List rules

## 8. Database

PostgreSQL with SQLAlchemy + Alembic models for:
- Project, Pipeline, PipelineContract, Source, Target
- ValidationRun, Checkpoint, Rule, Finding, Evidence
- RuntimeRun, RuntimeMetric, Recommendation, Score

## 9. CLI Commands (planned)

```
dpif --help
dpif validate --contract pipeline.yaml --offline
dpif validate --job-id 123 --environment prod
dpif scan code --path ./src
dpif scan table --table catalog.schema.table
dpif report --run-id <id>
```

## 10. Test Strategy

- Every rule requires: positive test, negative test, edge case
- Realistic fixtures: small_pipeline, medium_pipeline, large_pipeline, bad_pipeline, optimized_pipeline, skewed_pipeline, small_file_pipeline, full_reload_pipeline, incremental_pipeline, streaming_pipeline
- Synthetic datasets: 10 GB, 100 GB, 500 GB, 1 TB, 3 TB (metadata fixtures)
- pytest must pass completely before moving to next phase

## 11. Phase 3 Addendum: Source and Data Intelligence

- Source carries format, ingestion mode, volumes (None-when-missing), partitioning, compression, schema, and JDBC/streaming metadata; src/dpif/sources/ holds capability tables and contextual format guidance (no live I/O).
- DataProfile carries collection_method (METADATA/SAMPLE/FULL_SCAN/RUNTIME/FIXTURE/UNKNOWN) plus compression, partition sizes/counts, and schema columns. Fixture loaders stamp FIXTURE so offline metadata can never masquerade as runtime scans.
- Pure-function analyzers live in src/dpif/analyzers/data/; YAML rules reference them via evaluator: and the checkpoint engine routes accordingly with Settings-driven thresholds.
- CP-001/CP-007 are evaluator-driven skeletons that preserve UNKNOWN-on-missing; EvidenceCoverage is reported beside (never inside) the score.
- Full rationale: docs/adr/adr-004-source-data-intelligence.md.