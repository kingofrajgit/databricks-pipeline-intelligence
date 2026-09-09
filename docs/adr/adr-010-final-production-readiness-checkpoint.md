# ADR-010: CP-FINAL — Final Production Readiness Checkpoint & Release Decision

## Status: Accepted (CP-FINAL)

## Context
Phases 1 through 8 established specialized intelligence analyzers across the complete Databricks pipeline lifecycle:
- Phase 1–2: Foundation, Offline Checkpoints, Scoring & Readiness Models
- Phase 3: Source and Data Profile Intelligence
- Phase 4: Code Intelligence (AST Parsing, PySpark Pattern Detection)
- Phase 5: SQL Intelligence (SQLGlot AST Parsing, Spark SQL Anti-Patterns)
- Phase 6: Databricks Environment Intelligence (Cluster, Job, Runtime DBR)
- Phase 7: Runtime Performance Intelligence (Spark Metrics, Event Logs, Stragglers, Memory Spill)
- Phase 8: Scalability Intelligence (Scenario Modeling, Projections, Trend Analysis)

However, individual analyzers producing isolated pass/fail/warn checkpoints are insufficient to make a dependable go/no-go production deployment decision. Organizations face catastrophic production failures when:
1. A high aggregate score (e.g. 92/100) masks a single release-blocking vulnerability (such as a hardcoded credential or driver OOM on collection).
2. A pipeline with missing telemetry is silently assumed healthy or converted to PASS.
3. Configuration parameters diverge across contractual design (Expected), cluster configuration (Implemented), and live Databricks runtime (Actual).
4. Multi-domain risks that span multiple intelligence boundaries (e.g. static driver collection on a small driver cluster processing large datasets) are not synthesized into actionable remediation actions.

Making a conclusive production release decision requires a dedicated consolidation and decision layer: **CP-FINAL / CP-024**.

## Decision

### 1. CP-FINAL Architecture as a Consolidation & Decision Layer
CP-FINAL is strictly designed as a consolidation, synthesis, and decision layer, NOT another raw code or telemetry parser. It aggregates evaluated checkpoints, findings, and evidence across all preceding intelligence domains (Source, Data, Code, SQL, Cluster, Job, Performance, Scalability, Security, Reliability, Governance, SLA).

CP-024 is the canonical implementation of CP-FINAL. In `CheckpointEngine.execute_checkpoint`, CP-024 intercepts execution to synthesize all evaluated checkpoints and findings rather than turning `UNKNOWN` when individual dependencies fail.

### 2. Four Mutually Exclusive Readiness Statuses
The final assessment produces exactly one of 4 deterministic statuses:
1. `PRODUCTION_READY`: All evaluable checkpoints pass, quality score $\ge 80.0$, evidence coverage $\ge 70.0\%$, and zero blocking/critical findings.
2. `PRODUCTION_READY_WITH_WARNINGS`: Quality score $\ge 80.0$ and evidence coverage $\ge 70.0\%$, with zero blocking findings, but non-blocking warnings or operational advisories exist.
3. `NOT_PRODUCTION_READY`: Triggered whenever any blocking finding, critical finding, failed checkpoint, or score $< 80.0$ exists.
4. `INSUFFICIENT_EVIDENCE`: Triggered whenever overall evidence coverage $< 70.0\%$, or when policy-mandated evidence (runtime telemetry, scalability runs, or live workspace verification) is missing.

### 3. Strict Three-Dimensional Decoupling
To eliminate masking and false confidence, three dimensions are kept strictly orthogonal:
- **Quality Score (0–100)**: Evaluates compliance of evaluable checkpoints. Quality score NEVER overrides a blocking finding or forces deployment approval.
- **Evidence Coverage (0–100%)**: Evaluates evidence sufficiency across all 14 pipeline domains. High score on minimal evidence yields `INSUFFICIENT_EVIDENCE`.
- **Production Readiness Decision**: Deterministic decision state resolved strictly through policy gates and priority hierarchy.

### 4. Deterministic Precedence Hierarchy
When multiple conflicting conditions exist, readiness is evaluated in deterministic order:
1. Release-blocking CRITICAL findings $\rightarrow$ `NOT_PRODUCTION_READY`
2. Other blocking findings (Severity HIGH, contractual gates) $\rightarrow$ `NOT_PRODUCTION_READY`
3. Configured policy gates (Security failure, SLA failure, Reliability failure) $\rightarrow$ `NOT_PRODUCTION_READY`
4. Configuration consistency drift (Blocking DBR/worker mismatch) $\rightarrow$ `NOT_PRODUCTION_READY`
5. Missing mandatory evidence (Policy-required runtime/scalability) $\rightarrow$ `INSUFFICIENT_EVIDENCE`
6. Evidence coverage below policy threshold ($< 70.0\%$) $\rightarrow$ `INSUFFICIENT_EVIDENCE`
7. Quality score below policy threshold ($< 80.0$) $\rightarrow$ `NOT_PRODUCTION_READY`
8. Non-blocking warnings or synthesized cross-domain risks $\rightarrow$ `PRODUCTION_READY_WITH_WARNINGS`
9. Clean approval $\rightarrow$ `PRODUCTION_READY`

### 5. Multi-Domain Evidence Coverage Aggregator
Implemented in `src/dpif/readiness/coverage.py`:
- Aggregates evaluated, unknown, and not-applicable checks across 14 functional domains.
- Maps evidence provenance explicitly (`CONTRACT`, `STATIC`, `DATABRICKS_API`, `DATABRICKS_METADATA`, `RUNTIME`, `EVENT_LOG`, `FIXTURE`, `PROJECTED`, `UNKNOWN`).
- Assigns explicit `EvidenceQualityTier` (`EVALUATED`, `UNKNOWN`, `NOT_APPLICABLE`).

### 6. Cross-Domain Risk Synthesizer & Configuration Consistency
Implemented in `src/dpif/readiness/synthesizer.py`:
- Synthesizes multi-domain vulnerabilities:
  - `XDOM-001`: Driver Memory Exhaustion at Scale (AST collection + dataset $> 50$ GB).
  - `XDOM-002`: Cartesian Product Scaling Risk (Cross join + dataset $> 10$ GB).
  - `XDOM-003`: Network Shuffle & Disk Spill Risk (Observed/projected shuffle $> 200$ GB + disk spill).
  - `XDOM-004`: Peak Workload Capacity Headroom Risk (Fixed cluster sizing + peak burst multiplier).
  - `XDOM-005`: Small File Proliferation & Metastore Pressure (Streaming ingestion + small avg file size).
- Compares Expected vs Implemented vs Actual configurations across DBR version, node types, worker counts, daily volume, and SLA runtime limits, flagging configuration drift.

### 7. Explainable Decision Rationale & Prioritized Actions
- Deterministic, human-readable explanations generated for every decision reason.
- Actions strictly prioritized from P0 down to P3:
  - `P0`: Release-blocking critical vulnerabilities (must resolve before deployment).
  - `P1`: High operational risks (remediation strongly advised before production traffic).
  - `P2`: Medium warnings and capacity advisories.
  - `P3`: Observability and evidence enrichment recommendations.

### 8. CLI Executive Reporting & Machine-Readable Output
- `dpif validate` displays the comprehensive `CP-FINAL — PRODUCTION READINESS DECISION` executive dashboard.
- `dpif validate --json` outputs complete machine-readable assessment JSON for automated CI/CD gating.

## Consequences
- **Positive**:
  - Deterministic release gating prevents critical pipeline failures in production Databricks workspaces.
  - Strict decoupling prevents high scores from obscuring critical vulnerabilities.
  - Full provenance auditing and secret redaction ensure enterprise compliance.
  - Zero regressions across existing test suites (571 passing tests).
- **Negative / Trade-offs**:
  - Offline runs without runtime telemetry strictly resolve to `INSUFFICIENT_EVIDENCE` or `PRODUCTION_READY_WITH_WARNINGS` rather than false 100% clean approvals.
