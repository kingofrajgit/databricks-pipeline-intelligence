# Development Roadmap

## Phase 0: Architecture ✓
- [x] Directory structure created
- [x] pyproject.toml created
- [x] .gitignore created
- [x] README.md created
- [x] Architecture document created
- [x] Product requirements document created
- [x] Development roadmap created
- [x] ADR directory created

## Phase 1: Core Foundation
### Goals
- Installable Python package
- Configuration system (environment variables, config files)
- Logging system
- Error handling framework
- Pydantic models for core entities
- Rule engine with at least 5 example rules
- Checkpoint engine with: Source, Data, Code, Cluster, Job
- CLI: `dpif --help` and `dpif validate --contract ... --offline`
- Tests: `pytest` passes

### Sub-tasks
1. [ ] Create Pydantic models (PipelineContract, Source, Target, Checkpoint, Rule, Finding, Evidence, ValidationRun)
2. [ ] Create configuration system
3. [ ] Set up logging
4. [ ] Implement error handling
5. [ ] Create rule engine (YAML parsing + condition evaluation)
6. [ ] Create checkpoint engine
7. [ ] Implement CLI commands
8. [ ] Write unit tests for all components
9. [ ] Create fixture data
10. [ ] Run `pytest` - all must pass

### Definition of Done
- `pip install -e .` works
- `dpif --help` outputs usage
- `dpif validate --contract examples/customer_daily.yaml --offline` runs without errors
- All unit tests pass
- Documentation is updated

## Phase 2: Pipeline Contract
### Goals
- Parse pipeline YAML contract
- Validate contract against Pydantic models
- Support all contract fields: source, ingestion, transformation, target, schedule, SLA, expected volume, peak volume, growth, processing type, reliability, security, ownership, cost expectations, scalability expectations

### Sub-tasks
1. [ ] Create PipelineContract Pydantic model
2. [ ] Create contract parser
3. [ ] Implement contract validation
4. [ ] Create example pipeline YAML files
5. [ ] Write tests for contract validation

## Phase 3: Source/Data Intelligence — COMPLETE
### Goals
- Source type identification (ADLS, S3, GCS, LOCAL FILE, CSV, JSON, PARQUET, AVRO, ORC, JDBC, REST API, KAFKA, EVENT HUB, STREAMING, DELTA)
- Data profile collection (metadata-based, sample-based)
- Small-file analysis with configurable thresholds
- Distinguish analysis methods

### Delivered
1. [x] Expanded Source model (format, ingestion mode, volumes as None-when-missing, partitioning, compression, schema, JDBC/streaming metadata)
2. [x] Source abstraction (`src/dpif/sources/`): capabilities, required config, contextual format guidance
3. [x] DataProfile with `collection_method` (METADATA/SAMPLE/FULL_SCAN/RUNTIME/FIXTURE/UNKNOWN) + evidence source
4. [x] Analyzers: volume, small-file (+per-format factors), distribution, partitions, schema, growth, formats, JDBC, streaming, coverage
5. [x] Strengthened CP-001/CP-007 (evaluator-driven, UNKNOWN-preserving)
6. [x] 8 new rules (DATA-001…005, SOURCE-001…003), each with 5+ tests
7. [x] 18 fixtures incl. healthy_parquet, small_files, skewed_partitions, schema_drift, large_jdbc, incremental_jdbc, streaming_source, csv/json_large_volume, healthy_delta
8. [x] CLI DATA section + Evidence Coverage; 153 tests green, ruff + mypy clean
9. [x] ADRs 002 (dependencies), 003 (Pydantic fix), 004 (source/data intelligence)

## Phase 4: Python/PySpark Code Analyzer — COMPLETE
### Goals
- Python AST-based static analysis
- Detect: collect(), toPandas(), driver loops, Python UDF, etc.
- Context-aware rules (not all collect() are bad)
- PySpark pattern detection

### Delivered
1. [x] AST parser (`src/dpif/code/parser.py`) with line/column tracking, secrets/paths, flow roots
2. [x] PySpark operation detectors (`src/dpif/code/pyspark.py`): collect/toPandas, shuffle, repartition/coalesce, cache, actions, sort, windows, dedup, broadcast, UDFs, joins, complexity
3. [x] Code analysis models (`src/dpif/code/models.py`): Operation, FunctionInfo, CodeAnalysis, AnalysisContext
3. [x] Code evaluators (`src/dpif/code/evaluators.py`): 10 AST rules via evaluators
4. [x] 10 YAML rules (CODE-PYSPARK-006…015) with tunable params and cap_status
4. [x] CP-004 wired to AST analysis + CLI CODE section + AnalysisContext + EvidenceCoverage tiers
4. [x] 14 code fixtures (good/bad/optimized/large_collect/limited_to_pandas/broadcast_small/broadcast_unknown/expensive_join/repeated_cache/justified_cache/repartition_pipeline/coalesce_one/expensive_window/global_sort/repeated_actions/small_collect)
4. [x] 5 meaningful tests per rule (positive, negative, edge, false-positive, context) + cross-data tests = 227 tests total
5. [x] Cross-data tests prove context-awareness (collect at 10 MB vs 2 TB, broadcast 50 MB vs 20 GB, etc.)
6. [x] EvidenceCoverage tiers (static/context/runtime), CLI CODE section with flow + findings

## Phase 5: SQL Analyzer
### Goals
- Detect: SELECT *, Cartesian joins, missing join predicates, unnecessary DISTINCT, large ORDER BY, high-cardinality GROUP BY, etc.
- Context-aware findings

### Sub-tasks
1. [ ] SQL parser integration
2. [ ] Implement SQL rules (at least 5)
3. [ ] Write tests

## Phase 6: Databricks Job/Cluster/Pipeline Analyzer
### Goals
- Cluster: DT, worker type, count, autoscaling, Photon, spot, Spark config
- Job: schedule, retry, max concurrent runs, parameters, dependencies
- Pipeline: dependencies, transformations, target, checkpointing, incremental strategy

### Sub-tasks
1. [ ] Create Cluster analyzer
2. [ ] Create Job analyzer
3. [ ] Create Pipeline analyzer
4. [ ] Create checkpoints for each

## Phase 7: Rule + Checkpoint Engine
### Goals
- Rule engine fully externalized in YAML
- Checkpoint dependencies
- Status: PASS, WARN, FAIL, NOT_APPLICABLE, UNKNOWN
- If dependency fails, dependents default to UNKNOWN

### Sub-tasks
1. [ ] Externalize all rules to YAML
2. [ ] Implement checkpoint dependency graph
3. [ ] Implement status propagation

## Phase 8: Scoring Engine
### Goals
- Category-level and overall scores
- Configurable weights per organization profile
- Overall status with critical override

### Sub-tasks
1. [ ] Implement scoring calculation
2. [ ] Create organization profile system
3. [ ] Implement critical finding override

## Phase 9: Runtime/Performance Engine
### Goals
- Performance metric analysis (when available)
- Skew detection
- Shuffle analysis
- Spill detection

### Sub-tasks
1. [ ] Implement performance metric models
2. [ ] Create skew detection heuristic
3. [ ] Create spill detection heuristic

## Phase 10: Scalability Engine
### Goals
- Historical run based estimation
- Benchmark based estimation
- Heuristic estimation
- Every prediction indicates: method, confidence, assumptions

### Sub-tasks
1. [ ] Implement volume/runtime prediction models
2. [ ] Create confidence assessment
3. [ ] Write tests

## Phase 11: Reporting/API
### Goals
- Report generation (all sections)
- REST API endpoints
- CLI commands fully implemented

### Sub-tasks
1. [ ] Create report generator
2. [ ] Implement REST API
3. [ ] Complete CLI command set

## Phase 12: Dashboard
### Goals
- React + TypeScript dashboard
- Visual score breakdown
- Interactive findings

## Phase 13: AI Advisor
### Goals
- Secondary AI layer
- Input: Contract + Findings + Evidence + Runtime + Historical
- Output: summary, root_causes, prioritized_recommendations

## Phase 14: CI/CD
### Goals
- GitHub Actions CI pipeline
- Automated testing on PR
- Linting and type checking

## Phase 15: Production Hardening
### Goals
- Docker production configuration
- Secrets management
- Multi-tenant support
- RBAC

---

## Phase Progression Rule
**DO NOT MOVE TO THE NEXT PHASE UNTIL THE CURRENT PHASE PASSES ALL ACCEPTANCE CRITERIA.**

Each phase follows: PLAN → IMPLEMENT → TEST → INTEGRATION TEST → DOCUMENT → REVIEW → ACCEPTANCE CRITERIA → STOP

## Testing Strategy

### Test Pyramid
- **Unit tests**: 70% - Individual components (models, rules, checkpoints, analyzers)
- **Integration tests**: 20% - Component interactions (contract validation, checkpoint engine, scoring)
- **End-to-end tests**: 10% - CLI commands, report generation, API endpoints

### Test Requirements

Every rule requires:
- positive test: rule detects the expected pattern
- negative test: rule does NOT flag the pattern when it's not present
- edge case: rule handles boundary conditions correctly

### Realistic Fixtures
- small_pipeline: Small, simple pipeline, likely PASS
- medium_pipeline: Medium complexity, mixed results
- large_pipeline: Large pipeline with potential issues
- bad_pipeline: Pipeline with many issues (FAIL/WARN expected)
- optimized_pipeline: Well-optimized pipeline (high scores)
- skewed_pipeline: Data skew present
- small_file_pipeline: Small-file condition
- full_reload_pipeline: Full reload detected
- incremental_pipeline: Incremental processing
- streaming_pipeline: Streaming/micro-batch pipeline

### Synthetic Datasets
- 10 GB, 100 GB, 500 GB, 1 TB, 3 TB
- Metadata fixtures (not actual multi-terabyte files)
- Scalable synthetic generation strategies

### Running Tests
```bash
# From repository root
pytest
# Or with verbose output
pytest -v
```

### Test Data Strategy
- Use pytest fixtures for reusable test data
- Create synthetic data using Python's faker/data generation libraries
- Store fixture metadata, not actual large files
- Use PySpark test utilities for Spark-related tests
- Mock Databricks API calls for integration tests