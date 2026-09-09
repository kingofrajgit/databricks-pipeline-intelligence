# Product Requirements Document (PRD)

## Product Name
**Databricks Pipeline Intelligence Framework (DPIF)**

## 1. Target Audience
- Data Engineers building Databricks pipelines
- Data Engineering Leads assessing pipeline quality
- MLOps/Engineering Managers assessing production readiness
- Security and Governance teams

## 2. User Personas

### 2.1 Data Engineer
- Builds and maintains Databricks pipelines
- Wants to know: "Is my pipeline production ready? What needs to change?"
- Needs actionable recommendations with evidence
- Values context-aware analysis over universal rules

### 2.2 Engineering Lead
- Assesses multiple pipelines for team health
- Wants overall scores and category breakdowns
- Needs to prioritize remediation efforts
- Cares about cost, performance, and risk

### 2.3 Security/Governance Analyst
- Checks for compliance with security and governance policies
- Looks for hard-coded credentials, overly broad permissions
- Validates Unity Catalog governance coverage

## 3. Core Features

### 3.1 Pipeline Contract Validation
- Validate pipeline YAML/JSON against expected schema
- Check source/target compatibility
- Validate schedule, SLA, and volume expectations

### 3.2 Source Intelligence
- Identify source type from configuration
- Collect data profile (file sizes, record counts, schema, partitioning)
- Detect small-file conditions with configurable thresholds
- Distinguish metadata-based vs sample-based vs full-data analysis

### 3.3 Code Analysis
- Python AST-based static analysis
- PySpark pattern detection
- SQL static analysis
- Detect: collect(), toPandas(), driver loops, Python UDF, pandas UDF misuse, crossJoin, unnecessary repartition/coalesce, excessive cache/persist, repeated actions, count() for existence, large distinct/orderBy, unbounded windows, SELECT *, hard-coded credentials, hard-coded paths, missing error handling

### 3.4 Cluster Analysis
- Databricks Runtime version
- Worker type, driver type
- Worker count, min/max workers, autoscaling
- Photon configuration, spot configuration
- Spark configuration, libraries, access mode, cluster policy

### 3.5 Job Analysis
- Schedule, timeout, retry, max concurrent runs
- Parameters, task dependencies
- Notifications, libraries, cluster configuration
- Failure handling, repair behavior, backfill
- Idempotency, retry safety, duplicate execution safety

### 3.6 Pipeline Analysis
- Dependencies, source, transformations, target
- Checkpointing, incremental strategy
- Schema evolution, data quality, error handling
- Recovery, orchestration
- Processing type: FULL_LOAD, INCREMENTAL, CDC, BATCH, STREAMING, MICRO-BATCH

### 3.7 Incremental Processing Analysis
- Detect full reload vs incremental
- Historical data vs daily new data comparison
- Recommend: watermark, partition pruning, CDC, change tracking, incremental merge
- Do not automatically prescribe one solution

### 3.8 Performance Engine
- Analyze: input bytes, output bytes, shuffle read/write, spill memory/disk, stage duration, task duration, task count, failed tasks, retry count, executor utilization, driver utilization
- Detect: data skew, large shuffle, spill, too many tiny tasks, long-tail tasks, failed task patterns, unnecessary stages, poor parallelism

### 3.9 Scalability Engine
- Evaluate: current volume, expected volume, peak volume, future volume
- Produce: volume, estimated runtime, cluster requirements, risk, confidence
- Methods: historical run based, benchmark based, heuristic estimation
- Every prediction indicates: method, confidence, assumptions
- If insufficient historical data: Status: UNKNOWN / LOW CONFIDENCE
- Do NOT invent precise runtime predictions

### 3.10 SLA Engine
- Compare: expected SLA vs actual runtime vs projected runtime
- Produce: PASS/WARNING/FAIL per level (current/expected/peak)

### 3.11 Cost Engine
- Analyze: DBUs, runtime, cluster size, worker utilization, cost/run, cost/day, cost/month, cost per GB
- Detect: over-provisioning, under-utilization, expensive retries, runtime growth, cost growth
- Never fabricate pricing

### 3.12 Security Engine
- Detect: hard-coded credentials, tokens, passwords, secrets, unsafe storage paths, overly broad permissions, incorrect identity usage, missing Unity Catalog governance
- Never print discovered secrets in reports; mask sensitive values

### 3.13 Data Quality Engine
- Support checks: nulls, duplicates, schema mismatch, schema drift, unexpected columns, missing columns, invalid values, record counts, referential consistency
- Rules are configurable

### 3.14 Governance Checks
- Owner, description, tags, catalog, schema, table naming, environment
- Documentation, lineage availability, permissions

### 3.15 Scoring Engine
- Category-level and overall scores
- Configurable weights per organization profile
- Categories: Source, Data, Code, Pipeline, Cluster, Job, Performance, Scalability, Reliability, Security, Governance, Cost
- Overall status: EXCELLENT (90-100), GOOD (80-89), NEEDS_IMPROVEMENT (70-79), HIGH_RISK (50-69), CRITICAL (0-49)
- CRITICAL blocking finding overrides overall score

### 3.16 Evidence Model
- Every finding has structured evidence
- Includes: rule_id, status, severity, observed, expected, evidence list, recommendation, confidence
- Records analysis method used

### 3.17 AI Advisor (Secondary Layer)
- Input: Pipeline Contract + Findings + Evidence + Runtime Metrics + Historical Runs
- Output: summary, root_causes, prioritized_recommendations, optimization_options, scalability_risks, implementation_guidance
- Does NOT invent metrics
- If information unavailable: "Insufficient evidence."

### 3.18 Report Generation
Contains:
- Executive Summary
- Pipeline Overview
- Pipeline Architecture
- Checkpoint Summary
- Critical Findings
- Source Analysis
- Data Analysis
- Code Analysis
- Cluster Analysis
- Job Analysis
- Performance Analysis
- Scalability Analysis
- Cost Analysis
- Security Analysis
- Reliability Analysis
- Recommendations
- Assumptions
- Evidence
- Production Readiness

### 3.19 CLI
Commands:
- `dpif --help`
- `dpif validate --contract pipeline.yaml --offline`
- `dpif validate --job-id 123 --environment prod`
- `dpif scan code --path ./src`
- `dpif scan table --table catalog.schema.table`
- `dpif report --run-id <id>`

### 3.20 REST API
- `POST /api/v1/validation`
- `GET /api/v1/validation/{id}`
- `GET /api/v1/validation/{id}/findings`
- `GET /api/v1/validation/{id}/checkpoints`
- `GET /api/v1/validation/{id}/report`
- `POST /api/v1/pipelines`
- `GET /api/v1/pipelines/{id}`
- `GET /api/v1/rules`
- `GET /api/v1/rules/{id}`

## 4. Non-Functional Requirements

### 4.1 Offline-First
Dry-run mode works without Databricks credentials, using fixture metadata.

### 4.2 Incremental Development
Built across 15+ phases. Phase 1 must pass completely before Phase 2.

### 4.3 Evidence-Driven
Every finding must have structured evidence. "Looks correct" is not sufficient.

### 4.4 Unknown is Important
Framework distinguishes PASS, FAIL, WARNING, UNKNOWN. UNKNOWN never converts to PASS.

### 4.4 Context-Aware
Rules are context-aware. Not all collect() are bad; not all broadcast() are good. Evaluation considers: Code + Data Size + Data Distribution + Cluster Configuration + Execution Metrics + Workload Type.

### 4.5 Configurable Everything
- Thresholds are configurable
- Rules are versioned
- Scoring weights are configurable per organization profile
- Analysis methods are recorded

### 4.6 No Fabricated Runtime
If the framework cannot execute the pipeline, it reports "Runtime prediction unavailable" with reason "Insufficient historical execution data."

### 4.7 Modular Design
New analyzers, rules, and checkpoints are pluggable without rewriting the core.

### 4.8 Security
Never log credentials/secrets. Mask sensitive values in reports.

## 5. Acceptance Criteria (Phase 1)

### Core
- Project structure exists
- Python project is installable
- Configuration system exists
- Logging exists
- Error handling exists

### Models
Pydantic models exist for:
- PipelineContract
- Source
- Target
- Checkpoint
- Rule
- Finding
- Evidence
- ValidationRun

### Rule Engine
At least 5 example rules work.

### Checkpoint Engine
At least these checkpoint types exist:
- Source
- Data
- Code
- Cluster
- Job

### CLI
This works:
- `dpif --help`
- `dpif validate --contract examples/customer_daily.yaml --offline`

### Tests
`pytest` must pass.

### Documentation
README must explain:
- installation
- architecture
- CLI
- offline validation
- rule format
- checkpoint model
- development workflow

## Phase 3 Requirements (delivered)

- Source metadata: type, format, location, ingestion mode, expected/peak volumes, growth, partitioning, compression, schema, JDBC/streaming metadata; missing values are UNKNOWN, never zero.
- DataProfile collection method tracked (METADATA/SAMPLE/FULL_SCAN/RUNTIME/FIXTURE/UNKNOWN); every data finding cites its evidence source.
- Small-file, distribution, partition-imbalance, schema-drift, growth, format, JDBC, and streaming analysis with configurable thresholds and policies.
- Evidence coverage reported separately from the score.
- Precise terminology only ("potential risk", "runtime prediction unavailable"); no fabricated metrics or credentials.