"""Rerun, Idempotency, and Duplicate-Data Forensics Analyzer (M5F).

Deterministic analyzer assessing pipeline behavior under:
1. SAME_INPUT
2. INCREMENTAL_INPUT
3. OVERLAPPING_INPUT
4. PARTIAL_FAILURE
5. JOB_RETRY
6. CONCURRENT_EXECUTION
7. LATE_ARRIVING_DATA

Reuses Python AST analysis, PySpark flow, SQL analysis, job evidence,
runtime evidence, historical run evidence, and contract declarations.
"""

from __future__ import annotations

import re
from typing import Any

from dpif.code.models import CodeAnalysis, OperationType
from dpif.models import CheckpointStatus, PipelineContract, Severity
from dpif.models.implementation import EvidenceProvenanceKind
from dpif.models.rerun import (
    DataLossRiskAnalysis,
    DuplicateDataRiskAnalysis,
    IdempotencyAssessment,
    IdempotencyDimension,
    IdempotencyDimensionAssessment,
    RerunAnalysisResult,
    RerunFinding,
    RerunScenarioKind,
    ScenarioAssessment,
)
from dpif.sql.models import SQLAnalysis

# Regex patterns for incremental boundary detection
INCREMENTAL_FILTER_RE = re.compile(
    r"(?i)(date|time|timestamp|created_at|updated_at|modified_at|event_time|"
    r"date_sub|date_add|current_date|current_timestamp|window|batch_id|epoch)"
)

# Regex patterns for watermarks, checkpoints, and offsets
WATERMARK_RE = re.compile(r"(?i)withWatermark")
CHECKPOINT_RE = re.compile(r"(?i)(checkpointLocation|\.checkpoint\(|\.localCheckpoint\()")
OFFSET_RE = re.compile(
    r"(?i)(startingVersion|startingTimestamp|maxFilesPerTrigger|readStream|offset)"
)
LOOKBACK_RE = re.compile(r"(?i)(date_sub|days?|hours?|lookback|interval)")

# Common identifier column patterns for MERGE keys
STABLE_KEY_RE = re.compile(
    r"(?i)(\b(id|uuid|pk|key|code|hash)\b|[a-z0-9_]+_id|[a-z0-9_]+_key|[a-z0-9_]+_pk)"
)


class RerunIdempotencyAnalyzer:
    """Forensic analyzer assessing rerun, idempotency, and duplicate risks (M5F)."""

    def __init__(
        self,
        code_analysis: CodeAnalysis | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        self.code = code_analysis
        self.context = context or {}
        self.raw_source = (
            getattr(code_analysis, "_raw_source", "") or str(self.context.get("code_snippet") or "")
        )
        self.contract: PipelineContract | None = self.context.get("pipeline_contract")
        self.job_config: dict[str, Any] | None = self.context.get("job_config")
        self.runtime_run: Any | None = self.context.get("runtime_run") or self.context.get(
            "runtime_data"
        )
        self.historical_runs: list[Any] | None = self.context.get("historical_runs")
        self.sql_analysis: SQLAnalysis | None = (
            getattr(code_analysis, "sql_analysis", None)
            if code_analysis
            else self.context.get("sql_analysis")
        )

        # Extracted pipeline characteristics
        self.write_modes: list[str] = []
        self.is_merge: bool = False
        self.is_append: bool = False
        self.is_overwrite: bool = False
        self.is_partitioned_write: bool = False
        self.has_deduplication: bool = False
        self.has_incremental_filter: bool = False
        self.has_checkpoint: bool = False
        self.has_offset: bool = False
        self.has_watermark: bool = False
        self.has_lookback: bool = False
        self.has_source_filter: bool = False
        self.merge_condition_detected: bool = False
        self.merge_key_detected: bool = False
        self.merge_key_stable: bool | None = None
        self.merge_key_unique: bool = False
        self.source_duplicate_protection: bool = False
        self.merge_condition: str = ""

        self._extract_code_features()

    def _extract_code_features(self) -> None:
        """Inspect code AST, raw source, and SQL analysis for write & idempotency features."""
        source = self.raw_source
        source_lower = source.lower()

        # 1. Deduplication detection
        if self.code:
            drop_ops = self.code.of_type(OperationType.DROP_DUPLICATES, OperationType.DISTINCT)
            if drop_ops:
                self.has_deduplication = True
                self.source_duplicate_protection = True

        if "dropduplicates" in source_lower or "drop_duplicates" in source_lower:
            self.has_deduplication = True
            self.source_duplicate_protection = True
        if ".distinct(" in source_lower:
            self.has_deduplication = True
            self.source_duplicate_protection = True

        if self.sql_analysis:
            for q in self.sql_analysis.queries:
                if q.distinct:
                    self.has_deduplication = True
                    self.source_duplicate_protection = True
                # Detect ROW_NUMBER() / RANK() deduplication filter
                for w in q.windows:
                    if w.function.upper() in ("ROW_NUMBER", "RANK", "DENSE_RANK"):
                        for f in q.filters:
                            if "1" in f.expression:
                                self.has_deduplication = True
                                self.source_duplicate_protection = True

        # 2. Incremental filtering, watermarks, checkpoints, offsets, and source filters
        if WATERMARK_RE.search(source):
            self.has_watermark = True
        if CHECKPOINT_RE.search(source):
            self.has_checkpoint = True
        if OFFSET_RE.search(source):
            self.has_offset = True
        if LOOKBACK_RE.search(source):
            self.has_lookback = True

        if self.code:
            for op in self.code.of_type(OperationType.FILTER):
                snippet = op.code
                self.has_source_filter = True
                if INCREMENTAL_FILTER_RE.search(snippet):
                    self.has_incremental_filter = True
            if self.code.of_type(OperationType.CHECKPOINT):
                self.has_checkpoint = True

        if ".filter(" in source_lower or ".where(" in source_lower or " where " in source_lower:
            self.has_source_filter = True

        if self.sql_analysis:
            for f in self.sql_analysis.filters:
                self.has_source_filter = True
                if INCREMENTAL_FILTER_RE.search(f.expression):
                    self.has_incremental_filter = True

        # Check contract declarations for incremental ingestion
        if self.contract and self.contract.source:
            ing_mode = str(getattr(self.contract.source, "ingestion_mode", "")).lower()
            proc_type = str(getattr(self.contract, "processing", "")).lower()
            if "incremental" in ing_mode or "incremental" in proc_type or "streaming" in ing_mode:
                self.has_incremental_filter = True

        # 3. Write semantics & Partitioning
        if "partitionby" in source_lower or "partition_by" in source_lower:
            self.is_partitioned_write = True
        if self.contract and self.contract.source and self.contract.source.partitioning:
            self.is_partitioned_write = True

        # Inspect PySpark writes in raw source & operations
        if "mode('append')" in source_lower or 'mode("append")' in source_lower:
            self.is_append = True
            self.write_modes.append("APPEND")
        if "mode('overwrite')" in source_lower or 'mode("overwrite")' in source_lower:
            self.is_overwrite = True
            self.write_modes.append("OVERWRITE")
        if "insertinto" in source_lower:
            if "overwrite=true" in source_lower:
                self.is_overwrite = True
                self.write_modes.append("INSERT OVERWRITE")
            else:
                self.is_append = True
                self.write_modes.append("INSERT INTO")

        # DeltaTable merge
        if "deltatable" in source_lower and ".merge(" in source_lower:
            self.is_merge = True
            self.write_modes.append("MERGE")
            self._evaluate_deltatable_merge(source)

        # SQL writes (INSERT, INSERT OVERWRITE, MERGE)
        if "insert overwrite" in source_lower:
            self.is_overwrite = True
            if "INSERT OVERWRITE" not in self.write_modes:
                self.write_modes.append("INSERT OVERWRITE")
        elif "insert into" in source_lower and not self.is_append:
            self.is_append = True
            if "INSERT INTO" not in self.write_modes:
                self.write_modes.append("INSERT INTO")
        if "merge into" in source_lower:
            self.is_merge = True
            if "MERGE" not in self.write_modes:
                self.write_modes.append("MERGE")
            self._evaluate_sql_merge(source)

        if self.code:
            for op in self.code.of_type(OperationType.WRITE):
                code_snippet = op.code.lower()
                if "append" in code_snippet:
                    self.is_append = True
                if "overwrite" in code_snippet:
                    self.is_overwrite = True

    def _check_contract_uniqueness(self, cond: str) -> bool:
        """Check if contract, data profile, or schema evidence establishes uniqueness for the merge key."""
        profile = self.context.get("data_profile")
        if profile and hasattr(profile, "schema_columns"):
            for col in profile.schema_columns:
                col_name = getattr(col, "name", "")
                if col_name and col_name in cond:
                    if getattr(col, "unique", False) or getattr(col, "primary_key", False):
                        return True

        if self.contract:
            tgt = getattr(self.contract, "target", None)
            if tgt:
                tgt_schema = getattr(tgt, "schema_text", "") or getattr(tgt, "schema", "")
                if isinstance(tgt_schema, str) and ("primary key" in tgt_schema.lower() or "unique" in tgt_schema.lower()):
                    return True
            rel = getattr(self.contract, "reliability", None)
            if rel and getattr(rel, "unique_keys", None):
                return True

        source_lower = self.raw_source.lower()
        if "primary key" in source_lower or "add constraint" in source_lower or "unique key" in source_lower:
            return True

        return False

    def _evaluate_merge_condition(self, cond: str) -> None:
        """Evaluate key detection, stability, uniqueness, and duplicate protection."""
        self.merge_condition_detected = True
        self.merge_condition = cond

        if STABLE_KEY_RE.search(cond):
            self.merge_key_detected = True
        else:
            self.merge_key_detected = False

        if "=" in cond and not any(op in cond for op in ("!=", "<>", "<=", ">=", "<", ">", " rand(", " uuid()")):
            self.merge_key_stable = True
        else:
            self.merge_key_stable = False if self.merge_key_detected else None

        if self.has_deduplication:
            self.source_duplicate_protection = True

        if self._check_contract_uniqueness(cond):
            self.merge_key_unique = True

    def _evaluate_deltatable_merge(self, source: str) -> None:
        """Inspect DeltaTable merge condition and key safety."""
        merge_match = re.search(r"\.merge\s*\(([^,]+),\s*([^)]+)\)", source)
        if merge_match:
            cond = merge_match.group(2).strip().strip("'\"")
            self._evaluate_merge_condition(cond)
        else:
            self.merge_condition_detected = True
            self.merge_key_stable = None
            self.merge_key_detected = False

    def _evaluate_sql_merge(self, source: str) -> None:
        """Inspect SQL MERGE statement condition and key safety."""
        on_match = re.search(
            r"(?i)merge\s+into\s+[^\s]+\s+(?:as\s+[^\s]+\s+)?using\s+[^\s]+\s+(?:as\s+[^\s]+\s+)?on\s+([^;\n]+)",
            source,
        )
        if on_match:
            cond = on_match.group(1).strip()
            self._evaluate_merge_condition(cond)
        else:
            self.merge_condition_detected = True
            self.merge_key_stable = None
            self.merge_key_detected = False

    def analyze(self) -> RerunAnalysisResult:
        """Run full deterministic M5F forensic analysis."""
        pipeline_name = str(
            self.context.get("pipeline_name")
            or (self.contract.pipeline_name if self.contract else "unknown_pipeline")
        )

        all_findings: list[RerunFinding] = []

        # 1. Analyze Scenarios
        scenario_assessments: dict[str, ScenarioAssessment] = {}

        s_same = self._analyze_same_input()
        scenario_assessments[RerunScenarioKind.SAME_INPUT.value] = s_same
        all_findings.extend(s_same.findings)

        s_inc = self._analyze_incremental_input()
        scenario_assessments[RerunScenarioKind.INCREMENTAL_INPUT.value] = s_inc
        all_findings.extend(s_inc.findings)

        s_overlap = self._analyze_overlapping_input()
        scenario_assessments[RerunScenarioKind.OVERLAPPING_INPUT.value] = s_overlap
        all_findings.extend(s_overlap.findings)

        s_partial = self._analyze_partial_failure()
        scenario_assessments[RerunScenarioKind.PARTIAL_FAILURE.value] = s_partial
        all_findings.extend(s_partial.findings)

        s_retry = self._analyze_job_retry()
        scenario_assessments[RerunScenarioKind.JOB_RETRY.value] = s_retry
        all_findings.extend(s_retry.findings)

        s_conc = self._analyze_concurrent_execution()
        scenario_assessments[RerunScenarioKind.CONCURRENT_EXECUTION.value] = s_conc
        all_findings.extend(s_conc.findings)

        s_late = self._analyze_late_arriving_data()
        scenario_assessments[RerunScenarioKind.LATE_ARRIVING_DATA.value] = s_late
        all_findings.extend(s_late.findings)

        # 2. Performance Consequences
        perf_findings = self._analyze_performance_consequences()
        all_findings.extend(perf_findings)

        # 3. Contract Correlation
        contract_findings = self._analyze_contract_correlation()
        all_findings.extend(contract_findings)

        # 4. Duplicate Data Risk Analysis
        dup_risk = self._build_duplicate_risk_analysis(all_findings)

        # 5. Data Loss Risk Analysis
        loss_risk = self._build_data_loss_risk_analysis(all_findings)

        # 6. Idempotency Dimensions & Overall Assessment
        idempotency = self._build_idempotency_assessment(all_findings, scenario_assessments)

        # Compute overall status across scenarios, risks, and idempotency
        overall_status = CheckpointStatus.PASS
        if any(f.status == CheckpointStatus.FAIL for f in all_findings):
            overall_status = CheckpointStatus.FAIL
        elif any(f.status == CheckpointStatus.WARN for f in all_findings) or idempotency.overall_status == CheckpointStatus.WARN:
            overall_status = CheckpointStatus.WARN
        elif idempotency.overall_status == CheckpointStatus.UNKNOWN:
            overall_status = CheckpointStatus.UNKNOWN

        return RerunAnalysisResult(
            pipeline_name=pipeline_name,
            overall_status=overall_status,
            scenarios=scenario_assessments,
            idempotency=idempotency,
            duplicate_risk=dup_risk,
            data_loss_risk=loss_risk,
            performance_risks=perf_findings,
            all_findings=all_findings,
        )

    # -------------------------------------------------------------------------
    # Scenario Analyzers
    # -------------------------------------------------------------------------

    def _analyze_same_input(self) -> ScenarioAssessment:
        findings: list[RerunFinding] = []
        evidence: list[str] = []

        if not self.write_modes and not self.raw_source.strip():
            # Missing evidence entirely
            return ScenarioAssessment(
                scenario=RerunScenarioKind.SAME_INPUT,
                status=CheckpointStatus.UNKNOWN,
                severity=Severity.INFO,
                risk_summary="No code or write semantics available to assess same-input rerun.",
                findings=[],
                evidence=["no code or write operations detected"],
                observed={"write_modes": []},
                expected={"idempotent_write_semantics": True},
            )

        if self.is_append and not self.has_deduplication and not self.is_merge:
            finding = RerunFinding(
                finding_id="RER-DUP-001-SAME",
                rule_id="RER-DUP-001",
                dimension=IdempotencyDimension.OUTPUT_WRITE_IDEMPOTENCY,
                scenario=RerunScenarioKind.SAME_INPUT,
                severity=Severity.HIGH,
                status=CheckpointStatus.WARN,
                title="Potential duplicate-data risk on same-input rerun",
                description="Repeated execution with identical input uses APPEND write semantics without deduplication, creating duplicate records.",
                evidence=[f"Write mode: {', '.join(self.write_modes)}", "No deduplication operations (dropDuplicates/distinct) found"],
                observed={"write_modes": self.write_modes, "has_deduplication": False},
                expected={"write_mode": "OVERWRITE | MERGE or deduplicated append"},
                recommendation="Implement upsert / MERGE or enforce deduplication before appending.",
                confidence=0.9,
                provenance=EvidenceProvenanceKind.STATIC_CODE,
            )
            findings.append(finding)
            evidence.extend(finding.evidence)
            return ScenarioAssessment(
                scenario=RerunScenarioKind.SAME_INPUT,
                status=CheckpointStatus.WARN,
                severity=Severity.HIGH,
                risk_summary="Potential duplicate-data risk: repeated execution appends duplicate rows.",
                findings=findings,
                evidence=evidence,
                observed={"write_modes": self.write_modes, "has_deduplication": False},
                expected={"idempotent_write": True},
            )

        if self.is_merge:
            # Strong evidence for PASS requires: key detected + stable equality + (key unique OR source deduplication)
            if (
                self.merge_key_detected
                and self.merge_key_stable
                and (self.merge_key_unique or self.source_duplicate_protection)
            ):
                finding = RerunFinding(
                    finding_id="RER-IDM-002-SAME",
                    rule_id="RER-IDM-002",
                    dimension=IdempotencyDimension.OUTPUT_WRITE_IDEMPOTENCY,
                    scenario=RerunScenarioKind.SAME_INPUT,
                    severity=Severity.INFO,
                    status=CheckpointStatus.PASS,
                    title="Idempotent MERGE write with verified key uniqueness/deduplication",
                    description=(
                        "Pipeline executes MERGE with stable unique key matching backed by verified key uniqueness "
                        "or explicit source deduplication, ensuring safe same-input rerun idempotency."
                    ),
                    evidence=[
                        f"MERGE condition: {self.merge_condition}",
                        "Key expression detected: True",
                        f"Source deduplication: {self.source_duplicate_protection}",
                        f"Contract key uniqueness: {self.merge_key_unique}",
                    ],
                    observed={
                        "merge_condition": self.merge_condition,
                        "key_detected": True,
                        "key_stable": True,
                        "source_duplicate_protection": self.source_duplicate_protection,
                        "key_unique": self.merge_key_unique,
                    },
                    expected={"idempotent_merge_evidence": True},
                    recommendation="",
                    confidence=0.95,
                    provenance=EvidenceProvenanceKind.STATIC_CODE,
                )
                findings.append(finding)
                return ScenarioAssessment(
                    scenario=RerunScenarioKind.SAME_INPUT,
                    status=CheckpointStatus.PASS,
                    severity=Severity.INFO,
                    risk_summary="Idempotent: MERGE condition with stable keys and deduplication/uniqueness prevents duplicate generation on rerun.",
                    findings=findings,
                    evidence=[f"MERGE condition: {self.merge_condition}"],
                    observed={
                        "merge_condition": self.merge_condition,
                        "key_stable": True,
                        "key_unique_or_deduped": True,
                    },
                    expected={"idempotent_write": True},
                )
            elif self.merge_key_detected:
                # Key expression detected (e.g. t.id = s.id), but NO uniqueness/deduplication evidence!
                # Must NOT automatically be PASS -> UNKNOWN
                finding = RerunFinding(
                    finding_id="RER-IDM-004-SAME",
                    rule_id="RER-IDM-004",
                    dimension=IdempotencyDimension.OUTPUT_WRITE_IDEMPOTENCY,
                    scenario=RerunScenarioKind.SAME_INPUT,
                    severity=Severity.MEDIUM,
                    status=CheckpointStatus.UNKNOWN,
                    title="MERGE write without verified key uniqueness or source deduplication",
                    description=(
                        f"MERGE condition contains key expression ('{self.merge_condition}'), but neither contract key "
                        "uniqueness nor source deduplication is established. If source data contains duplicate keys, "
                        "MERGE behavior is non-deterministic or will raise duplicate match runtime errors."
                    ),
                    evidence=[
                        f"MERGE condition: {self.merge_condition}",
                        "Key expression detected: True",
                        "Source deduplication: False",
                        "Contract key uniqueness: False",
                    ],
                    observed={
                        "merge_condition": self.merge_condition,
                        "key_detected": True,
                        "key_stable": self.merge_key_stable,
                        "source_duplicate_protection": False,
                        "key_unique": False,
                    },
                    expected={"merge_key_uniqueness_or_deduplication": True},
                    recommendation="Enforce dropDuplicates on the merge key in source dataset or establish unique key contract constraint.",
                    confidence=0.75,
                    provenance=EvidenceProvenanceKind.STATIC_CODE,
                )
                findings.append(finding)
                return ScenarioAssessment(
                    scenario=RerunScenarioKind.SAME_INPUT,
                    status=CheckpointStatus.UNKNOWN,
                    severity=Severity.MEDIUM,
                    risk_summary="MERGE key detected, but uniqueness/deduplication evidence is missing; idempotency cannot be guaranteed.",
                    findings=findings,
                    evidence=[f"MERGE condition: {self.merge_condition}"],
                    observed={"key_stable": self.merge_key_stable, "key_unique_or_deduped": False},
                    expected={"key_stable": True, "key_unique_or_deduped": True},
                )
            else:
                # Merge condition lacks stable identifier key
                finding = RerunFinding(
                    finding_id="RER-IDM-004-SAME",
                    rule_id="RER-IDM-004",
                    dimension=IdempotencyDimension.OUTPUT_WRITE_IDEMPOTENCY,
                    scenario=RerunScenarioKind.SAME_INPUT,
                    severity=Severity.MEDIUM,
                    status=CheckpointStatus.WARN,
                    title="MERGE write with unknown or non-identifier key condition",
                    description=f"Pipeline utilizes MERGE, but condition ('{self.merge_condition or 'unknown'}') lacks an identifier key; merge collision or duplicate risk.",
                    evidence=[f"Condition: {self.merge_condition or 'unknown'}"],
                    observed={"merge_condition": self.merge_condition, "key_detected": False},
                    expected={"merge_key_detected": True},
                    recommendation="Ensure MERGE joins on a unique primary key column.",
                    confidence=0.8,
                    provenance=EvidenceProvenanceKind.STATIC_CODE,
                )
                findings.append(finding)
                return ScenarioAssessment(
                    scenario=RerunScenarioKind.SAME_INPUT,
                    status=CheckpointStatus.WARN,
                    severity=Severity.MEDIUM,
                    risk_summary="MERGE key condition lacks stable identifier; collision or duplicate risk.",
                    findings=findings,
                    evidence=[f"MERGE condition: {self.merge_condition or 'unknown'}"],
                    observed={"key_detected": False},
                    expected={"key_detected": True},
                )

        if self.is_overwrite:
            finding = RerunFinding(
                finding_id="RER-IDM-005-SAME",
                rule_id="RER-IDM-005",
                dimension=IdempotencyDimension.OUTPUT_WRITE_IDEMPOTENCY,
                scenario=RerunScenarioKind.SAME_INPUT,
                severity=Severity.INFO,
                status=CheckpointStatus.PASS,
                title="Overwrite achieves same-input idempotency",
                description="Output write uses OVERWRITE semantics, yielding deterministic target state on same-input rerun.",
                evidence=[f"Write modes: {', '.join(self.write_modes)}"],
                observed={"write_modes": self.write_modes},
                expected={"idempotent_write": True},
                recommendation="",
                confidence=0.85,
                provenance=EvidenceProvenanceKind.STATIC_CODE,
            )
            findings.append(finding)
            return ScenarioAssessment(
                scenario=RerunScenarioKind.SAME_INPUT,
                status=CheckpointStatus.PASS,
                severity=Severity.INFO,
                risk_summary="Idempotent: OVERWRITE mode replaces target cleanly on same-input rerun.",
                findings=findings,
                evidence=[f"Write mode: {', '.join(self.write_modes)}"],
                observed={"write_modes": self.write_modes},
                expected={"idempotent_write": True},
            )

        return ScenarioAssessment(
            scenario=RerunScenarioKind.SAME_INPUT,
            status=CheckpointStatus.UNKNOWN,
            severity=Severity.INFO,
            risk_summary="Write semantics unclear; same-input rerun behavior cannot be confirmed.",
            findings=[],
            evidence=["no clear write mode identified"],
            observed={"write_modes": self.write_modes},
            expected={"idempotent_write": True},
        )

    def _analyze_incremental_input(self) -> ScenarioAssessment:
        findings: list[RerunFinding] = []

        has_boundary = self.has_incremental_filter or self.has_offset or self.has_watermark

        if has_boundary and self.has_checkpoint:
            ev_list = []
            if self.has_incremental_filter:
                ev_list.append("Incremental predicate / timestamp filter detected")
            if self.has_offset:
                ev_list.append("Offset / stream tracking detected")
            if self.has_watermark:
                ev_list.append("Watermark lateness boundary detected")
            ev_list.append("Checkpointing mechanism detected for recovery tracking")

            finding = RerunFinding(
                finding_id="RER-INC-001",
                rule_id="RER-INC-001",
                dimension=IdempotencyDimension.INPUT_IDEMPOTENCY,
                scenario=RerunScenarioKind.INCREMENTAL_INPUT,
                severity=Severity.INFO,
                status=CheckpointStatus.PASS,
                title="Incremental processing boundary verified with checkpoint tracking",
                description="Pipeline establishes verified input boundaries using predicates/offsets/watermarks and maintains resilient checkpoint progress tracking.",
                evidence=ev_list,
                observed={
                    "has_incremental_filter": self.has_incremental_filter,
                    "has_offset": self.has_offset,
                    "has_watermark": self.has_watermark,
                    "has_checkpoint": True,
                },
                expected={"incremental_boundary_protection": True},
                recommendation="",
                confidence=0.9,
                provenance=EvidenceProvenanceKind.STATIC_CODE,
            )
            findings.append(finding)
            return ScenarioAssessment(
                scenario=RerunScenarioKind.INCREMENTAL_INPUT,
                status=CheckpointStatus.PASS,
                severity=Severity.INFO,
                risk_summary="Stable incremental boundaries identified.",
                findings=findings,
                evidence=ev_list,
                observed={"incremental_boundary": True, "has_checkpoint": True},
                expected={"incremental_boundary": True},
            )

        if has_boundary and not self.has_checkpoint:
            ev_list = []
            if self.has_incremental_filter:
                ev_list.append("Incremental predicate / timestamp filter detected")
            if self.has_offset:
                ev_list.append("Offset / stream tracking detected")
            if self.has_watermark:
                ev_list.append("Watermark lateness boundary detected")

            finding = RerunFinding(
                finding_id="RER-INC-001",
                rule_id="RER-INC-001",
                dimension=IdempotencyDimension.INPUT_IDEMPOTENCY,
                scenario=RerunScenarioKind.INCREMENTAL_INPUT,
                severity=Severity.INFO,
                status=CheckpointStatus.PASS,
                title="Incremental processing boundaries detected",
                description="Pipeline establishes stable input boundaries using predicates, offsets, or watermarks.",
                evidence=ev_list,
                observed={
                    "has_incremental_filter": self.has_incremental_filter,
                    "has_offset": self.has_offset,
                    "has_watermark": self.has_watermark,
                    "has_checkpoint": False,
                },
                expected={"incremental_boundary_protection": True},
                recommendation="",
                confidence=0.85,
                provenance=EvidenceProvenanceKind.STATIC_CODE,
            )
            findings.append(finding)
            return ScenarioAssessment(
                scenario=RerunScenarioKind.INCREMENTAL_INPUT,
                status=CheckpointStatus.PASS,
                severity=Severity.INFO,
                risk_summary="Stable incremental boundaries identified.",
                findings=findings,
                evidence=ev_list,
                observed={"incremental_boundary": True, "has_checkpoint": False},
                expected={"incremental_boundary": True},
            )

        if self.has_checkpoint and not has_boundary:
            # Checkpoint alone!
            # Must NOT automatically establish idempotent input processing -> UNKNOWN
            finding = RerunFinding(
                finding_id="RER-INC-002",
                rule_id="RER-INC-002",
                dimension=IdempotencyDimension.INPUT_IDEMPOTENCY,
                scenario=RerunScenarioKind.INCREMENTAL_INPUT,
                severity=Severity.INFO,
                status=CheckpointStatus.UNKNOWN,
                title="Checkpoint detected without established incremental input boundary",
                description=(
                    "Checkpointing mechanism detected for state and recovery tracking, but an explicit incremental input "
                    "boundary (predicate, offset, or watermark) could not be established statically. "
                    "Rerun input boundary cannot be confirmed idempotent."
                ),
                evidence=["Checkpointing mechanism detected", "No incremental predicates, offsets, or watermarks found"],
                observed={"has_checkpoint": True, "has_incremental_boundary": False},
                expected={"incremental_boundary_established": True},
                recommendation="Establish explicit incremental filtering or offset bounding alongside checkpointing.",
                confidence=0.8,
                provenance=EvidenceProvenanceKind.STATIC_CODE,
            )
            findings.append(finding)
            return ScenarioAssessment(
                scenario=RerunScenarioKind.INCREMENTAL_INPUT,
                status=CheckpointStatus.UNKNOWN,
                severity=Severity.INFO,
                risk_summary="Checkpoint detected for recovery tracking, but actual incremental rerun boundary cannot be established.",
                findings=findings,
                evidence=finding.evidence,
                observed={"has_checkpoint": True, "has_incremental_boundary": False},
                expected={"incremental_boundary": True},
            )

        if not self.raw_source.strip() and not self.write_modes:
            return ScenarioAssessment(
                scenario=RerunScenarioKind.INCREMENTAL_INPUT,
                status=CheckpointStatus.UNKNOWN,
                severity=Severity.INFO,
                risk_summary="No incremental processing evidence available.",
                findings=[],
                evidence=["code unavailable"],
                observed={},
                expected={},
            )

        # Batch without incremental filtering
        return ScenarioAssessment(
            scenario=RerunScenarioKind.INCREMENTAL_INPUT,
            status=CheckpointStatus.WARN if self.is_append else CheckpointStatus.UNKNOWN,
            severity=Severity.MEDIUM if self.is_append else Severity.INFO,
            risk_summary="No incremental boundary or timestamp filter detected; may reprocess entire dataset.",
            findings=findings,
            evidence=["no incremental predicates, offsets, or checkpoints found"],
            observed={"has_incremental_filter": False},
            expected={"incremental_filtering": True},
        )

    def _analyze_overlapping_input(self) -> ScenarioAssessment:
        findings: list[RerunFinding] = []

        if not self.raw_source.strip():
            return ScenarioAssessment(
                scenario=RerunScenarioKind.OVERLAPPING_INPUT,
                status=CheckpointStatus.UNKNOWN,
                severity=Severity.INFO,
                risk_summary="Code unavailable to evaluate overlapping input handling.",
                findings=[],
                evidence=[],
                observed={},
                expected={},
            )

        if self.has_deduplication or (
            self.is_merge
            and self.merge_key_detected
            and self.merge_key_stable
            and (self.merge_key_unique or self.source_duplicate_protection)
        ):
            finding = RerunFinding(
                finding_id="RER-DUP-003",
                rule_id="RER-DUP-003",
                dimension=IdempotencyDimension.TRANSFORMATION_IDEMPOTENCY,
                scenario=RerunScenarioKind.OVERLAPPING_INPUT,
                severity=Severity.INFO,
                status=CheckpointStatus.PASS,
                title="Overlapping input safely handled via deduplication or merge",
                description="Pipeline protects against duplicate ingestion from overlapping input windows via explicit deduplication or verified idempotent merge.",
                evidence=["Deduplication (dropDuplicates/distinct) or verified unique MERGE present"],
                observed={
                    "has_deduplication": self.has_deduplication,
                    "is_merge": self.is_merge,
                    "key_unique_or_deduped": True,
                },
                expected={"overlap_protection": True},
                recommendation="",
                confidence=0.9,
                provenance=EvidenceProvenanceKind.STATIC_CODE,
            )
            findings.append(finding)
            return ScenarioAssessment(
                scenario=RerunScenarioKind.OVERLAPPING_INPUT,
                status=CheckpointStatus.PASS,
                severity=Severity.INFO,
                risk_summary="Protected: overlapping records are deduplicated or merged safely.",
                findings=findings,
                evidence=finding.evidence,
                observed={"protected": True},
                expected={"protected": True},
            )

        if self.is_merge and self.merge_key_detected and not (self.merge_key_unique or self.source_duplicate_protection):
            finding = RerunFinding(
                finding_id="RER-DUP-005",
                rule_id="RER-DUP-005",
                dimension=IdempotencyDimension.TRANSFORMATION_IDEMPOTENCY,
                scenario=RerunScenarioKind.OVERLAPPING_INPUT,
                severity=Severity.MEDIUM,
                status=CheckpointStatus.UNKNOWN,
                title="Overlapping input safety unverified: MERGE key uniqueness unproven",
                description="Pipeline uses MERGE for writes, but source deduplication or target key uniqueness is unverified; overlapping input records may cause merge collisions or duplicate inserts.",
                evidence=[f"MERGE condition: {self.merge_condition}", "Source deduplication unverified"],
                observed={"is_merge": True, "key_unique_or_deduped": False},
                expected={"key_unique_or_deduped": True},
                recommendation="Enforce dropDuplicates on merge key before merging overlapping input.",
                confidence=0.75,
                provenance=EvidenceProvenanceKind.STATIC_CODE,
            )
            findings.append(finding)
            return ScenarioAssessment(
                scenario=RerunScenarioKind.OVERLAPPING_INPUT,
                status=CheckpointStatus.UNKNOWN,
                severity=Severity.MEDIUM,
                risk_summary="Overlapping input: MERGE key uniqueness unproven; potential merge collision.",
                findings=findings,
                evidence=finding.evidence,
                observed={"key_unique_or_deduped": False},
                expected={"key_unique_or_deduped": True},
            )

        if self.is_append and not self.has_deduplication:
            finding = RerunFinding(
                finding_id="RER-DUP-002",
                rule_id="RER-DUP-002",
                dimension=IdempotencyDimension.TRANSFORMATION_IDEMPOTENCY,
                scenario=RerunScenarioKind.OVERLAPPING_INPUT,
                severity=Severity.HIGH,
                status=CheckpointStatus.WARN,
                title="Potential duplicate-data risk from overlapping input",
                description="Pipeline uses APPEND write without deduplication; overlapping input windows will cause duplicate records.",
                evidence=["Write mode: APPEND", "No deduplication or upsert protection"],
                observed={"write_mode": "APPEND", "has_deduplication": False},
                expected={"deduplication": True},
                recommendation="Add dropDuplicates on primary business keys or use MERGE with key matching.",
                confidence=0.85,
                provenance=EvidenceProvenanceKind.STATIC_CODE,
            )
            findings.append(finding)
            return ScenarioAssessment(
                scenario=RerunScenarioKind.OVERLAPPING_INPUT,
                status=CheckpointStatus.WARN,
                severity=Severity.HIGH,
                risk_summary="Potential duplicate-data risk: overlapping input records will be appended as duplicates.",
                findings=findings,
                evidence=finding.evidence,
                observed={"has_deduplication": False},
                expected={"has_deduplication": True},
            )

        return ScenarioAssessment(
            scenario=RerunScenarioKind.OVERLAPPING_INPUT,
            status=CheckpointStatus.UNKNOWN,
            severity=Severity.INFO,
            risk_summary="Overlapping input behavior cannot be determined with certainty.",
            findings=[],
            evidence=[],
            observed={},
            expected={},
        )

    def _analyze_partial_failure(self) -> ScenarioAssessment:
        findings: list[RerunFinding] = []

        if not self.raw_source.strip() and not self.write_modes:
            return ScenarioAssessment(
                scenario=RerunScenarioKind.PARTIAL_FAILURE,
                status=CheckpointStatus.UNKNOWN,
                severity=Severity.INFO,
                risk_summary="Insufficient evidence to evaluate partial failure recovery.",
                findings=[],
                evidence=["code unavailable"],
                observed={},
                expected={},
            )

        if (
            (self.is_merge and self.merge_key_detected and self.merge_key_stable and (self.merge_key_unique or self.source_duplicate_protection))
            or (self.is_overwrite and self.is_partitioned_write)
        ):
            finding = RerunFinding(
                finding_id="RER-RET-004",
                rule_id="RER-RET-004",
                dimension=IdempotencyDimension.PARTIAL_FAILURE_RECOVERY,
                scenario=RerunScenarioKind.PARTIAL_FAILURE,
                severity=Severity.INFO,
                status=CheckpointStatus.PASS,
                title="Transactional write supports safe partial failure recovery",
                description="Pipeline uses ACID transactional MERGE with verified keys or partitioned OVERWRITE, preventing uncommitted or duplicate state on partial failure recovery.",
                evidence=[f"Write semantics: {', '.join(self.write_modes)}"],
                observed={"write_modes": self.write_modes},
                expected={"transactional_recovery": True},
                recommendation="",
                confidence=0.85,
                provenance=EvidenceProvenanceKind.STATIC_CODE,
            )
            findings.append(finding)
            return ScenarioAssessment(
                scenario=RerunScenarioKind.PARTIAL_FAILURE,
                status=CheckpointStatus.PASS,
                severity=Severity.INFO,
                risk_summary="Transactional write semantics allow safe recovery after partial failure.",
                findings=findings,
                evidence=finding.evidence,
                observed={"transactional_write": True},
                expected={"transactional_recovery": True},
            )

        if self.is_append and not self.has_deduplication:
            finding = RerunFinding(
                finding_id="RER-DUP-004",
                rule_id="RER-DUP-004",
                dimension=IdempotencyDimension.PARTIAL_FAILURE_RECOVERY,
                scenario=RerunScenarioKind.PARTIAL_FAILURE,
                severity=Severity.HIGH,
                status=CheckpointStatus.WARN,
                title="Potential duplicate-data risk on partial failure and retry",
                description="If write succeeds but a later pipeline step fails, subsequent retry will append identical records again.",
                evidence=["Write mode: APPEND without deduplication or transactional boundaries"],
                observed={"write_mode": "APPEND", "has_deduplication": False},
                expected={"idempotent_retry_boundary": True},
                recommendation="Enforce atomic transactions, checkpointing, or upsert writes to ensure idempotent retry.",
                confidence=0.85,
                provenance=EvidenceProvenanceKind.STATIC_CODE,
            )
            findings.append(finding)
            return ScenarioAssessment(
                scenario=RerunScenarioKind.PARTIAL_FAILURE,
                status=CheckpointStatus.WARN,
                severity=Severity.HIGH,
                risk_summary="Potential duplicate-data risk: retry after partial failure appends duplicate records.",
                findings=findings,
                evidence=finding.evidence,
                observed={"safe_recovery": False},
                expected={"safe_recovery": True},
            )

        return ScenarioAssessment(
            scenario=RerunScenarioKind.PARTIAL_FAILURE,
            status=CheckpointStatus.UNKNOWN,
            severity=Severity.INFO,
            risk_summary="Partial failure recovery behavior cannot be established.",
            findings=[],
            evidence=[],
            observed={},
            expected={},
        )

    def _analyze_job_retry(self) -> ScenarioAssessment:
        findings: list[RerunFinding] = []

        # Extract max_retries
        retries = 0
        retry_found = False
        if self.job_config:
            settings = self.job_config.get("settings", {})
            tasks = settings.get("tasks", [])
            for t in tasks:
                if "max_retries" in t:
                    retries = max(retries, int(t.get("max_retries", 0)))
                    retry_found = True
            if "max_retries" in settings:
                retries = max(retries, int(settings.get("max_retries", 0)))
                retry_found = True
            if "max_retries" in self.job_config:
                retries = max(retries, int(self.job_config.get("max_retries", 0)))
                retry_found = True

        if self.contract and hasattr(self.contract, "reliability"):
            rc = getattr(self.contract.reliability, "retry_count", None)
            if rc is not None:
                retries = max(retries, int(rc))
                retry_found = True

        if not retry_found:
            return ScenarioAssessment(
                scenario=RerunScenarioKind.JOB_RETRY,
                status=CheckpointStatus.UNKNOWN,
                severity=Severity.INFO,
                risk_summary="Job retry configuration is unavailable.",
                findings=[],
                evidence=["retry configuration unavailable in job_config and contract"],
                observed={"max_retries": None},
                expected={"retry_configuration_known": True},
            )

        if retries > 0:
            if self.is_append and not self.has_deduplication and not self.is_merge:
                finding = RerunFinding(
                    finding_id="RER-RET-001",
                    rule_id="RER-RET-001",
                    dimension=IdempotencyDimension.RETRY_IDEMPOTENCY,
                    scenario=RerunScenarioKind.JOB_RETRY,
                    severity=Severity.HIGH,
                    status=CheckpointStatus.WARN,
                    title="Potential duplicate-data risk under job retry",
                    description=f"Job retry is enabled ({retries} retries configured) with APPEND write mode and no deduplication protection.",
                    evidence=[f"max_retries: {retries}", "write_mode: APPEND", "has_deduplication: False"],
                    observed={"max_retries": retries, "write_mode": "APPEND", "has_deduplication": False},
                    expected={"idempotent_retry": True},
                    recommendation="Ensure pipeline is idempotent before enabling automatic retries.",
                    confidence=0.9,
                    provenance=EvidenceProvenanceKind.STATIC_CODE,
                )
                findings.append(finding)
                return ScenarioAssessment(
                    scenario=RerunScenarioKind.JOB_RETRY,
                    status=CheckpointStatus.WARN,
                    severity=Severity.HIGH,
                    risk_summary="Job retry enabled without idempotency; duplicate data risk on automatic retry.",
                    findings=findings,
                    evidence=finding.evidence,
                    observed={"max_retries": retries, "retry_safe": False},
                    expected={"retry_safe": True},
                )

            if (
                self.is_merge
                and self.merge_key_detected
                and self.merge_key_stable
                and (self.merge_key_unique or self.source_duplicate_protection)
            ):
                finding = RerunFinding(
                    finding_id="RER-RET-002",
                    rule_id="RER-RET-002",
                    dimension=IdempotencyDimension.RETRY_IDEMPOTENCY,
                    scenario=RerunScenarioKind.JOB_RETRY,
                    severity=Severity.INFO,
                    status=CheckpointStatus.PASS,
                    title="Job retry is safe with idempotent MERGE write",
                    description=f"Job retries ({retries}) backed by idempotent MERGE with verified key uniqueness or source deduplication.",
                    evidence=[
                        f"max_retries: {retries}",
                        f"MERGE condition: {self.merge_condition}",
                        "Verified key uniqueness or source deduplication: True",
                    ],
                    observed={"max_retries": retries, "is_merge": True, "key_unique_or_deduped": True},
                    expected={"retry_safe": True},
                    recommendation="",
                    confidence=0.9,
                    provenance=EvidenceProvenanceKind.STATIC_CODE,
                )
                findings.append(finding)
                return ScenarioAssessment(
                    scenario=RerunScenarioKind.JOB_RETRY,
                    status=CheckpointStatus.PASS,
                    severity=Severity.INFO,
                    risk_summary="Retry-safe: MERGE with verified keys allows safe automatic job retries.",
                    findings=findings,
                    evidence=finding.evidence,
                    observed={"max_retries": retries, "retry_safe": True},
                    expected={"retry_safe": True},
                )

            if self.is_merge:
                finding = RerunFinding(
                    finding_id="RER-RET-003",
                    rule_id="RER-RET-003",
                    dimension=IdempotencyDimension.RETRY_IDEMPOTENCY,
                    scenario=RerunScenarioKind.JOB_RETRY,
                    severity=Severity.MEDIUM,
                    status=CheckpointStatus.WARN,
                    title="Job retry with unverified MERGE key safety",
                    description=f"Job retries ({retries}) enabled with MERGE, but key uniqueness or source deduplication cannot be guaranteed statically.",
                    evidence=[f"max_retries: {retries}", f"MERGE condition: {self.merge_condition or 'unknown'}"],
                    observed={"max_retries": retries, "is_merge": True, "key_unique_or_deduped": False},
                    expected={"merge_key_unique_or_deduped": True},
                    recommendation="Verify unique constraints on MERGE target keys or deduplicate source before merge.",
                    confidence=0.7,
                    provenance=EvidenceProvenanceKind.STATIC_CODE,
                )
                findings.append(finding)
                return ScenarioAssessment(
                    scenario=RerunScenarioKind.JOB_RETRY,
                    status=CheckpointStatus.WARN,
                    severity=Severity.MEDIUM,
                    risk_summary="Job retries enabled but MERGE key safety is unverified.",
                    findings=findings,
                    evidence=finding.evidence,
                    observed={"max_retries": retries, "retry_safe": False},
                    expected={"retry_safe": True},
                )

        return ScenarioAssessment(
            scenario=RerunScenarioKind.JOB_RETRY,
            status=CheckpointStatus.PASS,
            severity=Severity.INFO,
            risk_summary="Job retries are disabled (max_retries=0); no retry-triggered duplication risk.",
            findings=[],
            evidence=[f"max_retries: {retries}"],
            observed={"max_retries": 0},
            expected={"max_retries_safe": True},
        )

    def _analyze_concurrent_execution(self) -> ScenarioAssessment:
        findings: list[RerunFinding] = []

        max_concurrent: int | None = None
        if self.job_config:
            settings = self.job_config.get("settings", {})
            if "max_concurrent_runs" in settings:
                max_concurrent = int(settings.get("max_concurrent_runs", 1))
            elif "max_concurrent_runs" in self.job_config:
                max_concurrent = int(self.job_config.get("max_concurrent_runs", 1))

        if max_concurrent is None:
            return ScenarioAssessment(
                scenario=RerunScenarioKind.CONCURRENT_EXECUTION,
                status=CheckpointStatus.UNKNOWN,
                severity=Severity.INFO,
                risk_summary="max_concurrent_runs is not specified in job configuration.",
                findings=[],
                evidence=["max_concurrent_runs configuration unavailable"],
                observed={"max_concurrent_runs": None},
                expected={"concurrency_configured": True},
            )

        if max_concurrent > 1:
            if (self.is_append or self.is_overwrite) and not self.is_partitioned_write:
                finding = RerunFinding(
                    finding_id="RER-CON-001",
                    rule_id="RER-CON-001",
                    dimension=IdempotencyDimension.CONCURRENT_EXECUTION_SAFETY,
                    scenario=RerunScenarioKind.CONCURRENT_EXECUTION,
                    severity=Severity.HIGH,
                    status=CheckpointStatus.WARN,
                    title="Concurrent execution risk on shared unpartitioned target",
                    description=f"Job allows concurrent runs (max_concurrent_runs={max_concurrent}) on an unpartitioned target, creating race conditions, overwrite collisions, or duplicate rows.",
                    evidence=[f"max_concurrent_runs: {max_concurrent}", "shared unpartitioned write target"],
                    observed={"max_concurrent_runs": max_concurrent, "partitioned": False},
                    expected={"concurrency_isolation": True},
                    recommendation="Set max_concurrent_runs=1 or ensure writes are isolated by partition / idempotency keys.",
                    confidence=0.85,
                    provenance=EvidenceProvenanceKind.STATIC_CODE,
                )
                findings.append(finding)
                return ScenarioAssessment(
                    scenario=RerunScenarioKind.CONCURRENT_EXECUTION,
                    status=CheckpointStatus.WARN,
                    severity=Severity.HIGH,
                    risk_summary=f"Concurrent runs ({max_concurrent}) permitted on unpartitioned target.",
                    findings=findings,
                    evidence=finding.evidence,
                    observed={"max_concurrent_runs": max_concurrent, "concurrency_safe": False},
                    expected={"concurrency_safe": True},
                )

        if max_concurrent == 1:
            finding = RerunFinding(
                finding_id="RER-CON-002",
                rule_id="RER-CON-002",
                dimension=IdempotencyDimension.CONCURRENT_EXECUTION_SAFETY,
                scenario=RerunScenarioKind.CONCURRENT_EXECUTION,
                severity=Severity.INFO,
                status=CheckpointStatus.PASS,
                title="Concurrency locked to 1 run",
                description="max_concurrent_runs is locked to 1, preventing concurrent race conditions and overlapping execution.",
                evidence=["max_concurrent_runs: 1"],
                observed={"max_concurrent_runs": 1},
                expected={"max_concurrent_runs": 1},
                recommendation="",
                confidence=0.95,
                provenance=EvidenceProvenanceKind.STATIC_CODE,
            )
            findings.append(finding)
            return ScenarioAssessment(
                scenario=RerunScenarioKind.CONCURRENT_EXECUTION,
                status=CheckpointStatus.PASS,
                severity=Severity.INFO,
                risk_summary="Concurrency locked to 1; concurrent collision prevented.",
                findings=findings,
                evidence=finding.evidence,
                observed={"max_concurrent_runs": 1},
                expected={"max_concurrent_runs": 1},
            )

        return ScenarioAssessment(
            scenario=RerunScenarioKind.CONCURRENT_EXECUTION,
            status=CheckpointStatus.PASS,
            severity=Severity.INFO,
            risk_summary="Concurrent execution safe under current configuration.",
            findings=[],
            evidence=[f"max_concurrent_runs: {max_concurrent}"],
            observed={"max_concurrent_runs": max_concurrent},
            expected={"concurrency_safe": True},
        )

    def _analyze_late_arriving_data(self) -> ScenarioAssessment:
        findings: list[RerunFinding] = []

        if self.has_watermark:
            reasons = ["Watermark handling (.withWatermark) configured for event-time lateness tracking"]
            finding = RerunFinding(
                finding_id="RER-LATE-001",
                rule_id="RER-LATE-001",
                dimension=IdempotencyDimension.LATE_DATA_HANDLING,
                scenario=RerunScenarioKind.LATE_ARRIVING_DATA,
                severity=Severity.INFO,
                status=CheckpointStatus.PASS,
                title="Event-time lateness threshold detected via watermark",
                description=(
                    "Pipeline defines watermark threshold (.withWatermark) for streaming event-time lateness handling. "
                    "Events arriving within the watermark window are processed, but unconstrained late data beyond the threshold "
                    "is dropped by streaming semantics."
                ),
                evidence=reasons,
                observed={"has_watermark": True, "blanket_correctness": False},
                expected={"late_data_handling": True},
                recommendation="",
                confidence=0.85,
                provenance=EvidenceProvenanceKind.STATIC_CODE,
            )
            findings.append(finding)
            return ScenarioAssessment(
                scenario=RerunScenarioKind.LATE_ARRIVING_DATA,
                status=CheckpointStatus.PASS,
                severity=Severity.INFO,
                risk_summary="Streaming event-time lateness handled up to configured watermark threshold.",
                findings=findings,
                evidence=reasons,
                observed={"has_watermark": True, "blanket_correctness": False},
                expected={"handled": True},
            )

        if self.has_lookback:
            if self.has_deduplication or (self.is_merge and self.merge_key_detected):
                reasons = ["Lookback window / date range buffer detected", "Deduplication or upsert write in place"]
                finding = RerunFinding(
                    finding_id="RER-LATE-002",
                    rule_id="RER-LATE-002",
                    dimension=IdempotencyDimension.LATE_DATA_HANDLING,
                    scenario=RerunScenarioKind.LATE_ARRIVING_DATA,
                    severity=Severity.INFO,
                    status=CheckpointStatus.PASS,
                    title="Late-data capture window protected by deduplication or upsert",
                    description=(
                        "Pipeline applies lookback window to capture late-arriving records, backed by deduplication "
                        "or upsert write semantics to prevent duplicate insertion."
                    ),
                    evidence=reasons,
                    observed={"has_lookback": True, "dedup_or_upsert": True},
                    expected={"late_data_handling": True},
                    recommendation="",
                    confidence=0.85,
                    provenance=EvidenceProvenanceKind.STATIC_CODE,
                )
                findings.append(finding)
                return ScenarioAssessment(
                    scenario=RerunScenarioKind.LATE_ARRIVING_DATA,
                    status=CheckpointStatus.PASS,
                    severity=Severity.INFO,
                    risk_summary="Protected: lookback window captures late data with deduplication/upsert protection.",
                    findings=findings,
                    evidence=reasons,
                    observed={"handled": True},
                    expected={"handled": True},
                )
            elif self.is_append and not self.has_deduplication:
                reasons = ["Lookback window detected with APPEND write mode", "No deduplication protection"]
                finding = RerunFinding(
                    finding_id="RER-LATE-004",
                    rule_id="RER-LATE-004",
                    dimension=IdempotencyDimension.LATE_DATA_HANDLING,
                    scenario=RerunScenarioKind.LATE_ARRIVING_DATA,
                    severity=Severity.MEDIUM,
                    status=CheckpointStatus.WARN,
                    title="Potential duplicate-data risk: lookback window with append write",
                    description=(
                        "Pipeline applies lookback window to capture late-arriving records but uses APPEND write mode "
                        "without deduplication; records in the lookback window will be duplicated on rerun."
                    ),
                    evidence=reasons,
                    observed={"has_lookback": True, "write_mode": "APPEND", "has_deduplication": False},
                    expected={"deduplication_on_lookback": True},
                    recommendation="Add dropDuplicates on primary keys or use MERGE when querying lookback windows.",
                    confidence=0.85,
                    provenance=EvidenceProvenanceKind.STATIC_CODE,
                )
                findings.append(finding)
                return ScenarioAssessment(
                    scenario=RerunScenarioKind.LATE_ARRIVING_DATA,
                    status=CheckpointStatus.WARN,
                    severity=Severity.MEDIUM,
                    risk_summary="Potential duplicate-data risk: lookback window with append write duplicates overlapping records.",
                    findings=findings,
                    evidence=reasons,
                    observed={"handled": False},
                    expected={"handled": True},
                )

        if self.is_merge:
            # MERGE alone without watermark or lookback
            # Must NOT be blanket late-data PASS -> UNKNOWN
            reasons = ["MERGE provides upsert capability", "No watermark or lookback capture window detected"]
            finding = RerunFinding(
                finding_id="RER-LATE-003",
                rule_id="RER-LATE-003",
                dimension=IdempotencyDimension.LATE_DATA_HANDLING,
                scenario=RerunScenarioKind.LATE_ARRIVING_DATA,
                severity=Severity.INFO,
                status=CheckpointStatus.UNKNOWN,
                title="MERGE upsert capability detected; late-data boundary unverified",
                description=(
                    "Pipeline uses MERGE which provides upsert capability for out-of-order records, but event-time "
                    "lateness boundary, arrival window, and out-of-order resolution are not established statically."
                ),
                evidence=reasons,
                observed={"is_merge": True, "has_watermark": False, "has_lookback": False},
                expected={"late_data_boundary_verified": True},
                recommendation="Define an explicit watermark or lookback window alongside MERGE.",
                confidence=0.8,
                provenance=EvidenceProvenanceKind.STATIC_CODE,
            )
            findings.append(finding)
            return ScenarioAssessment(
                scenario=RerunScenarioKind.LATE_ARRIVING_DATA,
                status=CheckpointStatus.UNKNOWN,
                severity=Severity.INFO,
                risk_summary="MERGE upsert capability detected, but late-data boundary and ordering remain unverified.",
                findings=findings,
                evidence=reasons,
                observed={"handled": False, "upsert_capable": True},
                expected={"handled": True},
            )

        # In accordance with specification: Missing evidence must remain UNKNOWN
        return ScenarioAssessment(
            scenario=RerunScenarioKind.LATE_ARRIVING_DATA,
            status=CheckpointStatus.UNKNOWN,
            severity=Severity.INFO,
            risk_summary="No evidence of watermark, lookback, or upsert handling for late-arriving records.",
            findings=[],
            evidence=["no watermark, lookback window, or upsert handling identified"],
            observed={"has_watermark": False, "has_lookback": False},
            expected={"late_data_handling": True},
        )

    # -------------------------------------------------------------------------
    # Performance & Contract Forensics
    # -------------------------------------------------------------------------

    def _analyze_performance_consequences(self) -> list[RerunFinding]:
        findings: list[RerunFinding] = []

        # If full table read without incremental filters is used with append on rerun
        if not self.has_incremental_filter and self.is_append:
            findings.append(
                RerunFinding(
                    finding_id="RER-PERF-001",
                    rule_id="RER-PERF-001",
                    dimension=IdempotencyDimension.INPUT_IDEMPOTENCY,
                    scenario=RerunScenarioKind.SAME_INPUT,
                    severity=Severity.MEDIUM,
                    status=CheckpointStatus.WARN,
                    title="Performance risk: repeated full-table processing on rerun",
                    description="Pipeline scans and processes entire dataset without incremental boundaries; each rerun repeats full recomputation and ingestion.",
                    evidence=["No timestamp filter or incremental predicate found", "Append write mode without boundary isolation"],
                    observed={"incremental_filter": False, "write_mode": "APPEND"},
                    expected={"incremental_processing": True},
                    recommendation="Partition and filter source data incrementally to avoid costly full-table reprocessing.",
                    confidence=0.8,
                    provenance=EvidenceProvenanceKind.STATIC_CODE,
                )
            )

        # Full table overwrite on high volume
        if self.is_overwrite and not self.is_partitioned_write:
            findings.append(
                RerunFinding(
                    finding_id="RER-PERF-002",
                    rule_id="RER-PERF-002",
                    dimension=IdempotencyDimension.OUTPUT_WRITE_IDEMPOTENCY,
                    scenario=RerunScenarioKind.SAME_INPUT,
                    severity=Severity.LOW,
                    status=CheckpointStatus.PASS,
                    title="Performance consequence: repeated full table replacement",
                    description="Pipeline overwrites entire destination table on rerun instead of updating modified partitions or records.",
                    evidence=["OVERWRITE mode without partition-scoped replacement"],
                    observed={"write_mode": "OVERWRITE", "partitioned": False},
                    expected={"partitioned_overwrite": True},
                    recommendation="Consider partition-level overwrites or MERGE to reduce I/O on large tables.",
                    confidence=0.75,
                    provenance=EvidenceProvenanceKind.STATIC_CODE,
                )
            )

        return findings

    def _analyze_contract_correlation(self) -> list[RerunFinding]:
        findings: list[RerunFinding] = []
        if not self.contract:
            return findings

        # Check expected idempotency vs implementation
        contract_idempotent = False
        if hasattr(self.contract, "reliability") and getattr(self.contract.reliability, "idempotent", None) is True:
            contract_idempotent = True

        if contract_idempotent and self.is_append and not self.has_deduplication and not self.is_merge:
            findings.append(
                RerunFinding(
                    finding_id="RER-IDM-003",
                    rule_id="RER-IDM-003",
                    dimension=IdempotencyDimension.OUTPUT_WRITE_IDEMPOTENCY,
                    scenario=RerunScenarioKind.SAME_INPUT,
                    severity=Severity.HIGH,
                    status=CheckpointStatus.FAIL,
                    title="Contract Idempotency Mismatch",
                    description="Pipeline contract declares idempotency (reliability.idempotent: true), but implementation uses APPEND write mode without deduplication or stable boundaries.",
                    evidence=["contract.reliability.idempotent: true", f"write_modes: {', '.join(self.write_modes)}", "has_deduplication: False"],
                    observed={"contract_declared": True, "implemented_idempotent": False, "write_modes": self.write_modes},
                    expected={"contract_declared": True, "implemented_idempotent": True},
                    recommendation="Align implementation with contract: implement MERGE/upsert or deduplication before appending.",
                    confidence=0.95,
                    blocking=True,
                    provenance=EvidenceProvenanceKind.CONTRACT,
                )
            )

        return findings

    # -------------------------------------------------------------------------
    # Risk Aggregations
    # -------------------------------------------------------------------------

    def _build_duplicate_risk_analysis(self, findings: list[RerunFinding]) -> DuplicateDataRiskAnalysis:
        dup_sources: list[str] = []
        max_sev = Severity.INFO
        status = CheckpointStatus.PASS

        dup_findings = [f for f in findings if "DUP" in f.rule_id or f.rule_id in ("RER-RET-001", "RER-IDM-003")]
        for f in dup_findings:
            if f.status in (CheckpointStatus.WARN, CheckpointStatus.FAIL):
                status = CheckpointStatus.FAIL if f.status == CheckpointStatus.FAIL else CheckpointStatus.WARN
                if f.severity == Severity.HIGH:
                    max_sev = Severity.HIGH
                elif f.severity == Severity.MEDIUM and max_sev != Severity.HIGH:
                    max_sev = Severity.MEDIUM
                dup_sources.append(f.title)

        if not dup_sources:
            if not self.raw_source.strip():
                return DuplicateDataRiskAnalysis(
                    status=CheckpointStatus.UNKNOWN,
                    risk_level=Severity.INFO,
                    summary="Duplicate-data risk cannot be evaluated without code or write evidence.",
                    potential_duplicate_sources=[],
                    findings=[],
                )
            return DuplicateDataRiskAnalysis(
                status=CheckpointStatus.PASS,
                risk_level=Severity.INFO,
                summary="No duplicate-data risk identified; write protections or deduplication in place.",
                potential_duplicate_sources=[],
                findings=dup_findings,
            )

        return DuplicateDataRiskAnalysis(
            status=status,
            risk_level=max_sev,
            summary=f"Potential duplicate-data risk identified across {len(dup_sources)} source(s).",
            potential_duplicate_sources=dup_sources,
            findings=dup_findings,
        )

    def _build_data_loss_risk_analysis(self, findings: list[RerunFinding]) -> DataLossRiskAnalysis:
        loss_sources: list[str] = []
        loss_findings: list[RerunFinding] = []

        if self.is_overwrite and not self.is_partitioned_write:
            # Check source completeness: filtered/incremental source vs complete source
            is_partial_source = self.has_incremental_filter or self.has_offset or self.has_source_filter
            if is_partial_source:
                finding = RerunFinding(
                    finding_id="RER-LOSS-001",
                    rule_id="RER-LOSS-001",
                    dimension=IdempotencyDimension.OUTPUT_WRITE_IDEMPOTENCY,
                    scenario=RerunScenarioKind.SAME_INPUT,
                    severity=Severity.MEDIUM,
                    status=CheckpointStatus.WARN,
                    title="Potential data-loss risk: destructive replacement of unpartitioned target with partial source",
                    description=(
                        "Write uses OVERWRITE on an unpartitioned target with filtered or incremental source data. "
                        "This presents a potential destructive replacement risk to existing historical data."
                    ),
                    evidence=[
                        "OVERWRITE mode without partition filters",
                        "Source read contains filters or incremental boundaries",
                    ],
                    observed={"write_mode": "OVERWRITE", "partitioned": False, "partial_source": True},
                    expected={"partition_isolated_overwrite": True},
                    recommendation="Scope overwrite to specific partitions or use dynamic partition replacement / MERGE.",
                    confidence=0.8,
                    provenance=EvidenceProvenanceKind.STATIC_CODE,
                )
                loss_findings.append(finding)
                loss_sources.append("unpartitioned_partial_source_destructive_replacement")
            else:
                # Safe complete-source overwrite of a derived/rebuildable table
                finding = RerunFinding(
                    finding_id="RER-LOSS-002",
                    rule_id="RER-LOSS-002",
                    dimension=IdempotencyDimension.OUTPUT_WRITE_IDEMPOTENCY,
                    scenario=RerunScenarioKind.SAME_INPUT,
                    severity=Severity.INFO,
                    status=CheckpointStatus.PASS,
                    title="Derived table complete rebuild via full overwrite",
                    description=(
                        "Destination table is completely regenerated from unfiltered source data via full table OVERWRITE. "
                        "Safe rebuildable derived state without destructive partial replacement."
                    ),
                    evidence=[
                        "Complete unfiltered source read",
                        "OVERWRITE mode replaces rebuildable target table",
                    ],
                    observed={"write_mode": "OVERWRITE", "partitioned": False, "complete_source": True},
                    expected={"rebuildable_target": True},
                    recommendation="",
                    confidence=0.85,
                    provenance=EvidenceProvenanceKind.STATIC_CODE,
                )
                loss_findings.append(finding)

        if not loss_sources:
            if not self.raw_source.strip():
                return DataLossRiskAnalysis(
                    status=CheckpointStatus.UNKNOWN,
                    risk_level=Severity.INFO,
                    summary="Data loss risk cannot be evaluated without code or write evidence.",
                    potential_data_loss_sources=[],
                    findings=[],
                )
            return DataLossRiskAnalysis(
                status=CheckpointStatus.PASS,
                risk_level=Severity.INFO,
                summary="No destructive replacement or data-loss risk identified.",
                potential_data_loss_sources=[],
                findings=loss_findings,
            )

        return DataLossRiskAnalysis(
            status=CheckpointStatus.WARN,
            risk_level=Severity.MEDIUM,
            summary="Potential data-loss risk: unpartitioned overwrite with filtered source presents potential destructive replacement risk.",
            potential_data_loss_sources=loss_sources,
            findings=loss_findings,
        )

    def _build_idempotency_assessment(
        self,
        findings: list[RerunFinding],
        scenarios: dict[str, ScenarioAssessment],
    ) -> IdempotencyAssessment:
        dimensions: dict[str, IdempotencyDimensionAssessment] = {}

        for dim in IdempotencyDimension:
            dim_findings = [f for f in findings if f.dimension == dim]
            status = CheckpointStatus.PASS
            max_sev = Severity.INFO

            if any(f.status == CheckpointStatus.FAIL for f in dim_findings):
                status = CheckpointStatus.FAIL
                max_sev = Severity.HIGH
            elif any(f.status == CheckpointStatus.WARN for f in dim_findings):
                status = CheckpointStatus.WARN
                max_sev = Severity.MEDIUM
            elif any(f.status == CheckpointStatus.UNKNOWN for f in dim_findings):
                status = CheckpointStatus.UNKNOWN
                max_sev = Severity.INFO
            elif not dim_findings:
                # Correlate with relevant scenario assessment if available
                if dim == IdempotencyDimension.OUTPUT_WRITE_IDEMPOTENCY:
                    same_s = scenarios.get(RerunScenarioKind.SAME_INPUT.value)
                    status = same_s.status if same_s else CheckpointStatus.UNKNOWN
                elif dim == IdempotencyDimension.INPUT_IDEMPOTENCY:
                    inc_s = scenarios.get(RerunScenarioKind.INCREMENTAL_INPUT.value)
                    status = inc_s.status if inc_s else CheckpointStatus.UNKNOWN
                elif dim == IdempotencyDimension.TRANSFORMATION_IDEMPOTENCY:
                    status = CheckpointStatus.PASS if self.has_deduplication else CheckpointStatus.UNKNOWN
                elif dim == IdempotencyDimension.RETRY_IDEMPOTENCY:
                    ret_s = scenarios.get(RerunScenarioKind.JOB_RETRY.value)
                    status = ret_s.status if ret_s else CheckpointStatus.UNKNOWN
                elif dim == IdempotencyDimension.CONCURRENT_EXECUTION_SAFETY:
                    conc_s = scenarios.get(RerunScenarioKind.CONCURRENT_EXECUTION.value)
                    status = conc_s.status if conc_s else CheckpointStatus.UNKNOWN
                elif dim == IdempotencyDimension.PARTIAL_FAILURE_RECOVERY:
                    part_s = scenarios.get(RerunScenarioKind.PARTIAL_FAILURE.value)
                    status = part_s.status if part_s else CheckpointStatus.UNKNOWN
                elif dim == IdempotencyDimension.LATE_DATA_HANDLING:
                    late_s = scenarios.get(RerunScenarioKind.LATE_ARRIVING_DATA.value)
                    status = late_s.status if late_s else CheckpointStatus.UNKNOWN
                else:
                    status = CheckpointStatus.UNKNOWN

            dimensions[dim.value] = IdempotencyDimensionAssessment(
                dimension=dim,
                status=status,
                max_severity=max_sev,
                summary=f"Assessment for {dim.value}: {status.value}",
                findings=dim_findings,
            )

        # 5. PIPELINE-LEVEL IDEMPOTENCY
        # Separate OUTPUT_WRITE_IDEMPOTENCY from OVERALL PIPELINE RERUN SAFETY.
        # Do not allow OUTPUT_WRITE_IDEMPOTENCY = PASS to automatically imply OVERALL IDEMPOTENCY = PASS
        # if retry, partial failure, concurrency, input boundary, or other critical dimensions remain unsafe or materially unknown.
        # A pipeline-level PASS requires sufficient evidence across the relevant execution path.
        # UNKNOWN must remain UNKNOWN.
        overall = CheckpointStatus.UNKNOWN
        is_idempotent: bool | None = None

        write_dim = dimensions.get(IdempotencyDimension.OUTPUT_WRITE_IDEMPOTENCY.value)
        conc_dim = dimensions.get(IdempotencyDimension.CONCURRENT_EXECUTION_SAFETY.value)
        retry_dim = dimensions.get(IdempotencyDimension.RETRY_IDEMPOTENCY.value)
        part_dim = dimensions.get(IdempotencyDimension.PARTIAL_FAILURE_RECOVERY.value)

        # 1. Any FAIL in any dimension -> FAIL
        if any(d.status == CheckpointStatus.FAIL for d in dimensions.values()):
            overall = CheckpointStatus.FAIL
            is_idempotent = False
        # 2. Any WARN in any dimension -> WARN (not PASS!)
        elif any(d.status == CheckpointStatus.WARN for d in dimensions.values()):
            overall = CheckpointStatus.WARN
            is_idempotent = False
        # 3. If output write is not PASS -> UNKNOWN
        elif not write_dim or write_dim.status != CheckpointStatus.PASS:
            overall = CheckpointStatus.UNKNOWN
            is_idempotent = None
        # 4. Output write is PASS: check critical rerun safety dimensions
        # If partial failure, concurrency, or retry are UNKNOWN, preserve uncertainty!
        elif (
            (part_dim and part_dim.status == CheckpointStatus.UNKNOWN)
            or (conc_dim and conc_dim.status == CheckpointStatus.UNKNOWN)
            or (retry_dim and retry_dim.status == CheckpointStatus.UNKNOWN)
        ):
            overall = CheckpointStatus.UNKNOWN
            is_idempotent = None
        # 5. Output write is PASS and no critical dimension is FAIL, WARN, or UNKNOWN -> PASS
        elif write_dim.status == CheckpointStatus.PASS:
            overall = CheckpointStatus.PASS
            is_idempotent = True

        return IdempotencyAssessment(
            overall_status=overall,
            is_idempotent=is_idempotent,
            summary=f"Overall idempotency status: {overall.value}",
            dimensions=dimensions,
        )
