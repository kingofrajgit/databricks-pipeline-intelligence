"""Three-Layer Alignment & Configuration Drift Forensics Analyzer (M5G).

Deterministic analyzer assessing configuration consistency and behavioral alignment across:
- Layer 1: EXPECTED (Pipeline Contract)
- Layer 2: IMPLEMENTED (Code AST, SQL, M5E, M5F, Databricks Job/Cluster config)
- Layer 3: ACTUAL (Databricks Workspace Cluster, Runtime Run Telemetry, Historical Runs)

Preserves strict UNKNOWN semantics when evidence is unavailable and never fabricates runtime facts.
"""

from __future__ import annotations

import re
from typing import Any

from dpif.code.models import CodeAnalysis, OperationType
from dpif.models import CheckpointStatus, PipelineContract, Severity
from dpif.models.alignment import (
    AlignmentDimension,
    AlignmentFinding,
    DimensionAlignmentAssessment,
    DriftSeverity,
    LayerDivergenceKind,
    ThreeLayerAlignmentAssessment,
)
from dpif.models.implementation import EvidenceProvenanceKind
from dpif.models.rerun import RerunAnalysisResult


class ThreeLayerAlignmentAnalyzer:
    """Forensic analyzer reconciling EXPECTED vs IMPLEMENTED vs ACTUAL (M5G)."""

    def __init__(
        self,
        code_analysis: CodeAnalysis | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        self.code = code_analysis
        self.context = context or {}
        self.contract: PipelineContract | None = (
            self.context.get("pipeline_contract") or self.context.get("contract")
        )
        self.raw_source = (
            getattr(code_analysis, "_raw_source", "")
            or str(self.context.get("code_snippet") or "")
        )
        self.code_filename = str(self.context.get("code_filename") or "pipeline.py")

        # Implemented layer: Job and Cluster config
        self.job_config: dict[str, Any] | None = self.context.get("job_config")
        self.cluster_config: dict[str, Any] | None = self.context.get("cluster_config")

        # Actual layer: Workspace Cluster, Runtime Run, Historical Runs
        self.workspace_cluster: dict[str, Any] | None = (
            self.context.get("workspace_cluster") or self.context.get("actual_environment")
        )
        self.runtime_run: Any | None = (
            self.context.get("runtime_run") or self.context.get("runtime_data")
        )
        self.historical_runs: list[Any] = self.context.get("historical_runs") or []
        self.data_profile: Any | None = self.context.get("data_profile")

        # Preceding forensic assessments
        self.impl_assessment = self.context.get("implementation_forensics")
        self.rerun_assessment: RerunAnalysisResult | None = self.context.get("rerun_analysis")

        # Code observations cache
        self.code_write_modes: list[str] = []
        self.code_formats: list[str] = []
        self.code_target_tables: list[str] = []
        self.code_partition_cols: list[str] = []
        self.has_watermark: bool = False
        self.has_incremental_filter: bool = False
        self.has_checkpoint: bool = False
        self.is_streaming: bool = False
        self._extract_code_characteristics()

    def _extract_code_characteristics(self) -> None:
        """Extract write, format, target, and incremental markers from AST code analysis."""
        if not self.code and not self.raw_source:
            return

        # Check raw source markers
        src = self.raw_source
        if re.search(r"(?i)readStream", src):
            self.is_streaming = True
        if re.search(r"(?i)withWatermark", src):
            self.has_watermark = True
        if re.search(
            r"(?i)(date|time|timestamp|created_at|updated_at|modified_at|event_time|"
            r"date_sub|date_add|current_date|current_timestamp|window|batch_id|epoch)",
            src,
        ):
            self.has_incremental_filter = True
        if re.search(r"(?i)(checkpointLocation|\.checkpoint\(|\.localCheckpoint\()", src):
            self.has_checkpoint = True

        # Extract write modes and formats from regex
        mode_matches = re.findall(r"""(?i)\.mode\(\s*['"]([a-z_]+)['"]\s*\)""", src)
        for m in mode_matches:
            self.code_write_modes.append(m.lower())

        format_matches = re.findall(r"""(?i)\.format\(\s*['"]([a-z_]+)['"]\s*\)""", src)
        for fmt in format_matches:
            self.code_formats.append(fmt.lower())

        table_matches = re.findall(
            r"""(?i)\.(?:saveAsTable|insertInto|table)\(\s*['"]([a-zA-Z0-9_.]+)['"]\s*\)""",
            src,
        )
        for tbl in table_matches:
            self.code_target_tables.append(tbl)

        part_matches = re.findall(
            r"""(?i)\.partitionBy\(\s*([^\)]+)\)""",
            src,
        )
        for p in part_matches:
            cols = [c.strip(" '\"") for c in p.split(",") if c.strip(" '\"")]
            self.code_partition_cols.extend(cols)

        # Check operations from AST
        if self.code and self.code.operations:
            for op in self.code.operations:
                if op.operation_type == OperationType.WRITE:
                    mode = op.arguments.get("mode") or op.arguments.get("write_mode")
                    if mode and str(mode).lower() not in self.code_write_modes:
                        self.code_write_modes.append(str(mode).lower())
                    fmt = op.arguments.get("format")
                    if fmt and str(fmt).lower() not in self.code_formats:
                        self.code_formats.append(str(fmt).lower())
                    tbl = op.arguments.get("table")
                    if tbl and str(tbl) not in self.code_target_tables:
                        self.code_target_tables.append(str(tbl))

    def analyze(self) -> ThreeLayerAlignmentAssessment:
        """Run complete deterministic Three-Layer Alignment & Drift forensic analysis."""
        pipeline_name = (
            getattr(self.contract, "pipeline_name", None)
            or str(self.context.get("pipeline_name") or "unknown_pipeline")
        )

        dims: dict[str, DimensionAlignmentAssessment] = {}
        all_findings: list[AlignmentFinding] = []

        # Evaluate all 9 dimensions
        dim_evaluators = [
            (AlignmentDimension.COMPUTE_RUNTIME, self._analyze_compute_runtime),
            (AlignmentDimension.CLUSTER_SIZING_SCALING, self._analyze_cluster_sizing),
            (AlignmentDimension.JOB_WORKFLOW_CADENCE, self._analyze_job_workflow),
            (AlignmentDimension.PROCESSING_STRATEGY, self._analyze_processing_strategy),
            (AlignmentDimension.TARGET_STORAGE_FORMAT, self._analyze_target_storage),
            (AlignmentDimension.PARTITIONING_LAYOUT, self._analyze_partitioning),
            (AlignmentDimension.SLA_EXECUTION_LIMITS, self._analyze_sla_performance),
            (AlignmentDimension.RELIABILITY_RETRY_POLICY, self._analyze_reliability_policy),
            (AlignmentDimension.WORKLOAD_VOLUME_BOUNDS, self._analyze_workload_volume),
        ]

        total_drifts = 0
        has_blocking = False
        unknown_layers = 0

        for dim, evaluator in dim_evaluators:
            assessment = evaluator()
            dims[dim.value] = assessment
            all_findings.extend(assessment.findings)
            if assessment.drift_severity == DriftSeverity.BLOCKING:
                has_blocking = True
            if assessment.drift_severity in (DriftSeverity.WARN, DriftSeverity.BLOCKING):
                total_drifts += len(assessment.findings) or 1
            if assessment.status == CheckpointStatus.UNKNOWN:
                unknown_layers += 1

        # Determine overall assessment status and drift severity
        if has_blocking:
            overall_status = CheckpointStatus.FAIL
            overall_drift_severity = DriftSeverity.BLOCKING
        elif any(d.drift_severity == DriftSeverity.WARN for d in dims.values()):
            overall_status = CheckpointStatus.WARN
            overall_drift_severity = DriftSeverity.WARN
        elif all(d.status == CheckpointStatus.PASS for d in dims.values()):
            overall_status = CheckpointStatus.PASS
            overall_drift_severity = DriftSeverity.NONE
        else:
            overall_status = CheckpointStatus.UNKNOWN
            overall_drift_severity = DriftSeverity.NONE

        return ThreeLayerAlignmentAssessment(
            pipeline_name=pipeline_name,
            overall_status=overall_status,
            drift_severity=overall_drift_severity,
            has_blocking_drift=has_blocking,
            total_drifts=total_drifts,
            unknown_layers_count=unknown_layers,
            dimensions=dims,
            findings=all_findings,
        )

    # -------------------------------------------------------------------------
    # Dimension 1: COMPUTE_RUNTIME
    # -------------------------------------------------------------------------
    def _analyze_compute_runtime(self) -> DimensionAlignmentAssessment:
        dim = AlignmentDimension.COMPUTE_RUNTIME
        findings: list[AlignmentFinding] = []

        exp_dbr = None
        exp_node = None
        if self.contract and hasattr(self.contract, "_cluster_raw"):
            raw_c = getattr(self.contract, "_cluster_raw", {})
            if isinstance(raw_c, dict):
                exp_dbr = raw_c.get("spark_version")
                exp_node = raw_c.get("node_type_id") or raw_c.get("node_type")

        impl_dbr = self.cluster_config.get("spark_version") if self.cluster_config else None
        impl_node = (
            (self.cluster_config.get("node_type_id") or self.cluster_config.get("node_type"))
            if self.cluster_config
            else None
        )

        act_dbr = (
            self.workspace_cluster.get("spark_version") if self.workspace_cluster else None
        )
        act_node = (
            self.workspace_cluster.get("node_type_id") if self.workspace_cluster else None
        )

        # Reconcile DBR / Spark Version
        if exp_dbr and impl_dbr and str(exp_dbr) != str(impl_dbr):
            findings.append(
                AlignmentFinding(
                    finding_id="ALIGN-COMP-001",
                    rule_id="ALIGN-COMP-001",
                    dimension=dim,
                    divergence_kind=LayerDivergenceKind.EXPECTED_VS_IMPLEMENTED,
                    title="Databricks Runtime (DBR) Version Mismatch",
                    description=(
                        f"Contract expects DBR '{exp_dbr}', but implemented cluster specifies '{impl_dbr}'."
                    ),
                    status=CheckpointStatus.WARN,
                    severity=Severity.HIGH,
                    drift_severity=DriftSeverity.WARN,
                    expected=exp_dbr,
                    implemented=impl_dbr,
                    actual=act_dbr,
                    evidence=[
                        f"Expected (Contract): {exp_dbr}",
                        f"Implemented (Cluster Config): {impl_dbr}",
                    ],
                    recommendation=f"Align cluster spark_version to contract version '{exp_dbr}'.",
                    confidence=0.95,
                    provenance=EvidenceProvenanceKind.CONTRACT,
                    location=self.code_filename,
                )
            )

        if act_dbr and exp_dbr and str(act_dbr) != str(exp_dbr):
            findings.append(
                AlignmentFinding(
                    finding_id="ALIGN-COMP-002",
                    rule_id="ALIGN-COMP-002",
                    dimension=dim,
                    divergence_kind=LayerDivergenceKind.EXPECTED_VS_ACTUAL,
                    title="Actual Workspace DBR Diverges from Contract",
                    description=(
                        f"Actual Databricks workspace cluster uses DBR '{act_dbr}', differing from contract '{exp_dbr}'."
                    ),
                    status=CheckpointStatus.FAIL,
                    severity=Severity.CRITICAL,
                    drift_severity=DriftSeverity.BLOCKING,
                    expected=exp_dbr,
                    implemented=impl_dbr,
                    actual=act_dbr,
                    evidence=[
                        f"Expected (Contract): {exp_dbr}",
                        f"Actual (Workspace): {act_dbr}",
                    ],
                    recommendation=f"Update workspace cluster runtime to match declared contract '{exp_dbr}'.",
                    confidence=0.98,
                    blocking=True,
                    provenance=EvidenceProvenanceKind.RUNTIME,
                    location=self.code_filename,
                )
            )

        # Reconcile Node Type
        if exp_node and impl_node and str(exp_node) != str(impl_node):
            findings.append(
                AlignmentFinding(
                    finding_id="ALIGN-COMP-003",
                    rule_id="ALIGN-COMP-003",
                    dimension=dim,
                    divergence_kind=LayerDivergenceKind.EXPECTED_VS_IMPLEMENTED,
                    title="Worker Node Type Divergence",
                    description=(
                        f"Contract declares node type '{exp_node}', but implemented cluster uses '{impl_node}'."
                    ),
                    status=CheckpointStatus.WARN,
                    severity=Severity.MEDIUM,
                    drift_severity=DriftSeverity.WARN,
                    expected=exp_node,
                    implemented=impl_node,
                    actual=act_node,
                    evidence=[
                        f"Expected (Contract): {exp_node}",
                        f"Implemented (Cluster Config): {impl_node}",
                    ],
                    recommendation="Align worker node type between contract and cluster configuration.",
                    confidence=0.90,
                    provenance=EvidenceProvenanceKind.CONTRACT,
                    location=self.code_filename,
                )
            )

        worst_drift = DriftSeverity.NONE
        status = CheckpointStatus.PASS
        if any(f.drift_severity == DriftSeverity.BLOCKING for f in findings):
            worst_drift = DriftSeverity.BLOCKING
            status = CheckpointStatus.FAIL
        elif any(f.drift_severity == DriftSeverity.WARN for f in findings):
            worst_drift = DriftSeverity.WARN
            status = CheckpointStatus.WARN
        elif not exp_dbr and not impl_dbr and not act_dbr:
            status = CheckpointStatus.UNKNOWN

        return DimensionAlignmentAssessment(
            dimension=dim,
            status=status,
            drift_severity=worst_drift,
            findings_count=len(findings),
            findings=findings,
            summary=(
                f"{len(findings)} compute divergence(s) detected"
                if findings
                else "Compute and runtime configuration aligned"
            ),
            expected_summary=f"DBR: {exp_dbr or 'unspecified'}, Node: {exp_node or 'unspecified'}",
            implemented_summary=f"DBR: {impl_dbr or 'unspecified'}, Node: {impl_node or 'unspecified'}",
            actual_summary=f"DBR: {act_dbr or 'UNKNOWN (not observed)'}, Node: {act_node or 'UNKNOWN'}",
        )

    # -------------------------------------------------------------------------
    # Dimension 2: CLUSTER_SIZING_SCALING
    # -------------------------------------------------------------------------
    def _analyze_cluster_sizing(self) -> DimensionAlignmentAssessment:
        dim = AlignmentDimension.CLUSTER_SIZING_SCALING
        findings: list[AlignmentFinding] = []

        exp_workers = None
        if self.contract and hasattr(self.contract, "_cluster_raw"):
            raw_c = getattr(self.contract, "_cluster_raw", {})
            if isinstance(raw_c, dict):
                exp_workers = raw_c.get("num_workers")

        impl_workers = self.cluster_config.get("num_workers") if self.cluster_config else None
        impl_autoscale = self.cluster_config.get("autoscale") if self.cluster_config else None
        act_workers = (
            self.workspace_cluster.get("num_workers") if self.workspace_cluster else None
        )

        # Check fixed vs autoscale drift
        if exp_workers is not None and impl_autoscale and isinstance(impl_autoscale, dict):
            min_w = impl_autoscale.get("min_workers")
            max_w = impl_autoscale.get("max_workers")
            if min_w != exp_workers or max_w != exp_workers:
                findings.append(
                    AlignmentFinding(
                        finding_id="ALIGN-CLUS-001",
                        rule_id="ALIGN-CLUS-001",
                        dimension=dim,
                        divergence_kind=LayerDivergenceKind.EXPECTED_VS_IMPLEMENTED,
                        title="Cluster Worker Sizing & Topology Divergence",
                        description=(
                            f"Contract specifies fixed {exp_workers} worker(s), but cluster is configured with "
                            f"autoscale [min={min_w}, max={max_w}]."
                        ),
                        status=CheckpointStatus.WARN,
                        severity=Severity.MEDIUM,
                        drift_severity=DriftSeverity.WARN,
                        expected=f"fixed {exp_workers} workers",
                        implemented=f"autoscale min={min_w}, max={max_w}",
                        actual=act_workers,
                        evidence=[
                            f"Contract declared num_workers: {exp_workers}",
                            f"Implemented autoscale range: min={min_w}, max={max_w}",
                        ],
                        recommendation="Verify whether autoscaling is intended and update contract declaration.",
                        confidence=0.88,
                        provenance=EvidenceProvenanceKind.JOB_CONFIG,
                        location=self.code_filename,
                    )
                )

        # Check fixed worker count drift
        if (
            exp_workers is not None
            and impl_workers is not None
            and exp_workers != impl_workers
            and not impl_autoscale
        ):
            findings.append(
                AlignmentFinding(
                    finding_id="ALIGN-CLUS-002",
                    rule_id="ALIGN-CLUS-002",
                    dimension=dim,
                    divergence_kind=LayerDivergenceKind.EXPECTED_VS_IMPLEMENTED,
                    title="Worker Count Specification Mismatch",
                    description=(
                        f"Contract declares {exp_workers} worker(s), but cluster configuration has {impl_workers} worker(s)."
                    ),
                    status=CheckpointStatus.WARN,
                    severity=Severity.MEDIUM,
                    drift_severity=DriftSeverity.WARN,
                    expected=exp_workers,
                    implemented=impl_workers,
                    actual=act_workers,
                    evidence=[
                        f"Contract num_workers: {exp_workers}",
                        f"Implemented num_workers: {impl_workers}",
                    ],
                    recommendation="Align worker count configuration with contractual capacity requirements.",
                    confidence=0.92,
                    provenance=EvidenceProvenanceKind.CONTRACT,
                    location=self.code_filename,
                )
            )

        worst_drift = (
            DriftSeverity.WARN
            if any(f.drift_severity == DriftSeverity.WARN for f in findings)
            else DriftSeverity.NONE
        )
        status = CheckpointStatus.WARN if findings else CheckpointStatus.PASS
        if exp_workers is None and impl_workers is None and not impl_autoscale:
            status = CheckpointStatus.UNKNOWN

        return DimensionAlignmentAssessment(
            dimension=dim,
            status=status,
            drift_severity=worst_drift,
            findings_count=len(findings),
            findings=findings,
            summary=(
                f"{len(findings)} cluster sizing drift(s) detected"
                if findings
                else "Cluster sizing and topology aligned"
            ),
            expected_summary=f"Workers: {exp_workers if exp_workers is not None else 'unspecified'}",
            implemented_summary=(
                f"Autoscale {impl_autoscale}"
                if impl_autoscale
                else f"Workers: {impl_workers if impl_workers is not None else 'unspecified'}"
            ),
            actual_summary=f"Workers: {act_workers if act_workers is not None else 'UNKNOWN'}",
        )

    # -------------------------------------------------------------------------
    # Dimension 3: JOB_WORKFLOW_CADENCE
    # -------------------------------------------------------------------------
    def _analyze_job_workflow(self) -> DimensionAlignmentAssessment:
        dim = AlignmentDimension.JOB_WORKFLOW_CADENCE
        findings: list[AlignmentFinding] = []

        exp_sched = getattr(self.contract, "schedule", None) if self.contract else None
        exp_freq = getattr(exp_sched, "frequency", None) if exp_sched else None
        exp_time = getattr(exp_sched, "time", None) if exp_sched else None

        impl_sched = self.job_config.get("schedule") if self.job_config else None
        is_paused = False
        cron_expr = None
        if isinstance(impl_sched, dict):
            is_paused = (
                str(impl_sched.get("pause_status", "")).upper() == "PAUSED"
                or bool(impl_sched.get("paused"))
            )
            cron_expr = impl_sched.get("quartz_cron_expression") or impl_sched.get("cron")

        if exp_freq and is_paused:
            findings.append(
                AlignmentFinding(
                    finding_id="ALIGN-JOB-001",
                    rule_id="ALIGN-JOB-001",
                    dimension=dim,
                    divergence_kind=LayerDivergenceKind.EXPECTED_VS_IMPLEMENTED,
                    title="Production Schedule Paused in Job Configuration",
                    description=(
                        f"Contract defines an active {exp_freq} schedule, but Databricks Job is PAUSED."
                    ),
                    status=CheckpointStatus.WARN,
                    severity=Severity.HIGH,
                    drift_severity=DriftSeverity.WARN,
                    expected=f"Active {exp_freq} schedule at {exp_time or 'specified time'}",
                    implemented="PAUSED",
                    actual="UNKNOWN (not triggered)",
                    evidence=[
                        f"Contract schedule: {exp_freq} at {exp_time}",
                        "Databricks Job schedule pause_status: PAUSED",
                    ],
                    recommendation="Unpause Databricks Job schedule prior to production deployment.",
                    confidence=0.95,
                    provenance=EvidenceProvenanceKind.JOB_CONFIG,
                    location=self.code_filename,
                )
            )

        worst_drift = DriftSeverity.WARN if findings else DriftSeverity.NONE
        status = CheckpointStatus.WARN if findings else CheckpointStatus.PASS
        if not exp_sched and not impl_sched:
            status = CheckpointStatus.UNKNOWN

        return DimensionAlignmentAssessment(
            dimension=dim,
            status=status,
            drift_severity=worst_drift,
            findings_count=len(findings),
            findings=findings,
            summary=(
                f"{len(findings)} workflow cadence divergence(s)"
                if findings
                else "Job schedule and workflow aligned"
            ),
            expected_summary=f"Frequency: {exp_freq or 'none'}, Time: {exp_time or 'none'}",
            implemented_summary=(
                f"Cron: {cron_expr or 'none'}, Paused: {is_paused}" if impl_sched else "Job config missing"
            ),
            actual_summary="UNKNOWN (no live trigger telemetry)",
        )

    # -------------------------------------------------------------------------
    # Dimension 4: PROCESSING_STRATEGY
    # -------------------------------------------------------------------------
    def _analyze_processing_strategy(self) -> DimensionAlignmentAssessment:
        dim = AlignmentDimension.PROCESSING_STRATEGY
        findings: list[AlignmentFinding] = []

        proc_type = getattr(self.contract, "processing", None) if self.contract else None
        declared_type = getattr(proc_type, "type", None) if proc_type else None
        if not declared_type and isinstance(proc_type, str):
            declared_type = proc_type

        # Check contract incremental vs code full reload
        is_declared_incremental = (
            isinstance(declared_type, str) and declared_type.lower() == "incremental"
        )
        if is_declared_incremental:
            # Code must show some evidence of incremental boundary (filter, watermark, checkpoint)
            if not self.has_incremental_filter and not self.has_watermark and not self.has_checkpoint:
                # If code does unconstrained read
                findings.append(
                    AlignmentFinding(
                        finding_id="ALIGN-PROC-001",
                        rule_id="ALIGN-PROC-001",
                        dimension=dim,
                        divergence_kind=LayerDivergenceKind.EXPECTED_VS_IMPLEMENTED,
                        title="Declared Incremental Processing Lacks Code Implementation",
                        description=(
                            "Contract declares incremental processing, but code inspection reveals no "
                            "timestamp filtering, watermark, or checkpoint boundaries (full reload detected)."
                        ),
                        status=CheckpointStatus.FAIL,
                        severity=Severity.HIGH,
                        drift_severity=DriftSeverity.BLOCKING,
                        expected="Incremental data bounds (event time, watermark, checkpoint)",
                        implemented="Full dataset scan without filtering",
                        actual="UNKNOWN (unfiltered volume)",
                        evidence=[
                            "Contract declaration: processing.type = 'incremental'",
                            "Code analysis: No timestamp filtering, withWatermark(), or checkpoint detected",
                        ],
                        recommendation="Implement incremental read filtering (e.g. event time range or streaming watermark).",
                        confidence=0.92,
                        blocking=True,
                        provenance=EvidenceProvenanceKind.STATIC_CODE,
                        location=self.code_filename,
                    )
                )

        # Check contract streaming vs code static read
        is_declared_streaming = (
            isinstance(declared_type, str) and declared_type.lower() == "streaming"
        )
        if is_declared_streaming and not self.is_streaming:
            findings.append(
                AlignmentFinding(
                    finding_id="ALIGN-PROC-002",
                    rule_id="ALIGN-PROC-002",
                    dimension=dim,
                    divergence_kind=LayerDivergenceKind.EXPECTED_VS_IMPLEMENTED,
                    title="Declared Streaming Pipeline Uses Batch Read",
                    description=(
                        "Contract declares streaming processing, but code implements batch read (`spark.read`)."
                    ),
                    status=CheckpointStatus.FAIL,
                    severity=Severity.HIGH,
                    drift_severity=DriftSeverity.BLOCKING,
                    expected="Streaming source reader (`spark.readStream`)",
                    implemented="Batch source reader (`spark.read`)",
                    actual="UNKNOWN",
                    evidence=[
                        "Contract declaration: processing.type = 'streaming'",
                        "Code analysis: static spark.read call detected without readStream",
                    ],
                    recommendation="Replace `spark.read` with `spark.readStream` or update contract to batch.",
                    confidence=0.95,
                    blocking=True,
                    provenance=EvidenceProvenanceKind.STATIC_CODE,
                    location=self.code_filename,
                )
            )

        worst_drift = DriftSeverity.NONE
        status = CheckpointStatus.PASS
        if any(f.drift_severity == DriftSeverity.BLOCKING for f in findings):
            worst_drift = DriftSeverity.BLOCKING
            status = CheckpointStatus.FAIL
        elif any(f.drift_severity == DriftSeverity.WARN for f in findings):
            worst_drift = DriftSeverity.WARN
            status = CheckpointStatus.WARN
        elif not declared_type:
            status = CheckpointStatus.UNKNOWN

        return DimensionAlignmentAssessment(
            dimension=dim,
            status=status,
            drift_severity=worst_drift,
            findings_count=len(findings),
            findings=findings,
            summary=(
                f"{len(findings)} processing strategy contradiction(s)"
                if findings
                else "Processing strategy aligned between contract and code"
            ),
            expected_summary=f"Processing type: {declared_type or 'unspecified'}",
            implemented_summary=(
                f"Incremental markers: watermark={self.has_watermark}, filter={self.has_incremental_filter}"
            ),
            actual_summary="UNKNOWN (runtime input boundary not observed)",
        )

    # -------------------------------------------------------------------------
    # Dimension 5: TARGET_STORAGE_FORMAT
    # -------------------------------------------------------------------------
    def _analyze_target_storage(self) -> DimensionAlignmentAssessment:
        dim = AlignmentDimension.TARGET_STORAGE_FORMAT
        findings: list[AlignmentFinding] = []

        tgt = getattr(self.contract, "target", None) if self.contract else None
        exp_fmt = getattr(tgt, "format", None) if tgt else None
        if exp_fmt is not None and hasattr(exp_fmt, "value"):
            exp_fmt = exp_fmt.value
        if exp_fmt is not None:
            exp_fmt = str(exp_fmt)

        # Reconcile format
        if exp_fmt and self.code_formats:
            for code_fmt in self.code_formats:
                if str(exp_fmt).lower() != str(code_fmt).lower():
                    # e.g. Contract specifies delta, code writes parquet
                    is_severe = str(exp_fmt).lower() == "delta" and str(code_fmt).lower() != "delta"
                    findings.append(
                        AlignmentFinding(
                            finding_id="ALIGN-TGT-001",
                            rule_id="ALIGN-TGT-001",
                            dimension=dim,
                            divergence_kind=LayerDivergenceKind.EXPECTED_VS_IMPLEMENTED,
                            title="Target Output Format Divergence",
                            description=(
                                f"Contract specifies target format '{exp_fmt}', but code writes '{code_fmt}'."
                            ),
                            status=CheckpointStatus.FAIL if is_severe else CheckpointStatus.WARN,
                            severity=Severity.HIGH if is_severe else Severity.MEDIUM,
                            drift_severity=DriftSeverity.BLOCKING if is_severe else DriftSeverity.WARN,
                            expected=exp_fmt,
                            implemented=code_fmt,
                            actual="UNKNOWN (target table uninspected)",
                            evidence=[
                                f"Contract target format: {exp_fmt}",
                                f"Code write format: {code_fmt}",
                            ],
                            recommendation=f"Update code write format to match contract format '{exp_fmt}'.",
                            confidence=0.94,
                            blocking=is_severe,
                            provenance=EvidenceProvenanceKind.STATIC_CODE,
                            location=self.code_filename,
                        )
                    )

        worst_drift = DriftSeverity.NONE
        status = CheckpointStatus.PASS
        if any(f.drift_severity == DriftSeverity.BLOCKING for f in findings):
            worst_drift = DriftSeverity.BLOCKING
            status = CheckpointStatus.FAIL
        elif any(f.drift_severity == DriftSeverity.WARN for f in findings):
            worst_drift = DriftSeverity.WARN
            status = CheckpointStatus.WARN
        elif not exp_fmt and not self.code_formats:
            status = CheckpointStatus.UNKNOWN

        return DimensionAlignmentAssessment(
            dimension=dim,
            status=status,
            drift_severity=worst_drift,
            findings_count=len(findings),
            findings=findings,
            summary=(
                f"{len(findings)} target storage divergence(s)"
                if findings
                else "Target storage format and destination aligned"
            ),
            expected_summary=f"Format: {exp_fmt or 'unspecified'}",
            implemented_summary=f"Formats: {', '.join(self.code_formats) or 'unspecified'}",
            actual_summary="UNKNOWN (table metadata uninspected)",
        )

    # -------------------------------------------------------------------------
    # Dimension 6: PARTITIONING_LAYOUT
    # -------------------------------------------------------------------------
    def _analyze_partitioning(self) -> DimensionAlignmentAssessment:
        dim = AlignmentDimension.PARTITIONING_LAYOUT
        findings: list[AlignmentFinding] = []

        src = getattr(self.contract, "source", None) if self.contract else None
        exp_parts = list(getattr(src, "partitioning", []) or []) if src else []

        # If contract declares specific partitioning, check code partitionBy
        if exp_parts and self.code_partition_cols:
            missing_parts = [p for p in exp_parts if p not in self.code_partition_cols]
            extra_parts = [p for p in self.code_partition_cols if p not in exp_parts]
            if missing_parts or extra_parts:
                findings.append(
                    AlignmentFinding(
                        finding_id="ALIGN-PART-001",
                        rule_id="ALIGN-PART-001",
                        dimension=dim,
                        divergence_kind=LayerDivergenceKind.EXPECTED_VS_IMPLEMENTED,
                        title="Partition Column Divergence Between Contract and Code",
                        description=(
                            f"Contract specifies partitioning {exp_parts}, but code partitions by {self.code_partition_cols}."
                        ),
                        status=CheckpointStatus.WARN,
                        severity=Severity.MEDIUM,
                        drift_severity=DriftSeverity.WARN,
                        expected=exp_parts,
                        implemented=self.code_partition_cols,
                        actual="UNKNOWN (target partitions uninspected)",
                        evidence=[
                            f"Contract declared partitions: {exp_parts}",
                            f"Code partitionBy columns: {self.code_partition_cols}",
                        ],
                        recommendation="Align partitionBy columns with contractual data layout specification.",
                        confidence=0.90,
                        provenance=EvidenceProvenanceKind.STATIC_CODE,
                        location=self.code_filename,
                    )
                )

        worst_drift = DriftSeverity.WARN if findings else DriftSeverity.NONE
        status = CheckpointStatus.WARN if findings else CheckpointStatus.PASS
        if not exp_parts and not self.code_partition_cols:
            status = CheckpointStatus.UNKNOWN

        return DimensionAlignmentAssessment(
            dimension=dim,
            status=status,
            drift_severity=worst_drift,
            findings_count=len(findings),
            findings=findings,
            summary=(
                f"{len(findings)} partitioning drift(s) detected"
                if findings
                else "Partitioning layout aligned"
            ),
            expected_summary=f"Partitions: {exp_parts or 'none'}",
            implemented_summary=f"Partitions: {self.code_partition_cols or 'none'}",
            actual_summary="UNKNOWN (data profile partitions not fully inspected)",
        )

    # -------------------------------------------------------------------------
    # Dimension 7: SLA_EXECUTION_LIMITS
    # -------------------------------------------------------------------------
    def _analyze_sla_performance(self) -> DimensionAlignmentAssessment:
        dim = AlignmentDimension.SLA_EXECUTION_LIMITS
        findings: list[AlignmentFinding] = []

        sla_obj = getattr(self.contract, "sla", None) if self.contract else None
        exp_sla_m = getattr(sla_obj, "max_runtime_minutes", None) if sla_obj else None

        impl_timeout_s = (
            self.job_config.get("timeout_seconds") if self.job_config else None
        )
        impl_timeout_m = round(impl_timeout_s / 60.0, 1) if impl_timeout_s else None

        act_dur_m = None
        if self.runtime_run:
            dur_s = getattr(self.runtime_run, "duration_seconds", None)
            if dur_s is None and hasattr(self.runtime_run, "execution_duration_ms"):
                dur_ms = self.runtime_run.execution_duration_ms
                if dur_ms is not None:
                    dur_s = dur_ms / 1000.0
            if dur_s is not None:
                act_dur_m = round(dur_s / 60.0, 1)

        # Check actual duration vs SLA
        if exp_sla_m and act_dur_m and act_dur_m > exp_sla_m:
            findings.append(
                AlignmentFinding(
                    finding_id="ALIGN-SLA-001",
                    rule_id="ALIGN-SLA-001",
                    dimension=dim,
                    divergence_kind=LayerDivergenceKind.EXPECTED_VS_ACTUAL,
                    title="Actual Execution Duration Exceeds Contractual SLA",
                    description=(
                        f"Actual runtime duration ({act_dur_m}m) exceeds contractual SLA threshold ({exp_sla_m}m)."
                    ),
                    status=CheckpointStatus.FAIL,
                    severity=Severity.HIGH,
                    drift_severity=DriftSeverity.BLOCKING,
                    expected=f"max {exp_sla_m} minutes",
                    implemented=f"{impl_timeout_m}m timeout" if impl_timeout_m else "no timeout",
                    actual=f"{act_dur_m} minutes",
                    evidence=[
                        f"Contract SLA: max_runtime_minutes = {exp_sla_m}",
                        f"Observed runtime duration: {act_dur_m} minutes ({act_dur_m - exp_sla_m:.1f}m breach)",
                    ],
                    recommendation="Optimize bottlenecks or allocate additional compute to satisfy SLA threshold.",
                    confidence=0.99,
                    blocking=True,
                    provenance=EvidenceProvenanceKind.RUNTIME,
                    location=self.code_filename,
                )
            )

        # Check job timeout vs SLA threshold
        if exp_sla_m and impl_timeout_m and impl_timeout_m < exp_sla_m * 0.5:
            findings.append(
                AlignmentFinding(
                    finding_id="ALIGN-SLA-002",
                    rule_id="ALIGN-SLA-002",
                    dimension=dim,
                    divergence_kind=LayerDivergenceKind.EXPECTED_VS_IMPLEMENTED,
                    title="Job Timeout Configured Below Contract SLA",
                    description=(
                        f"Databricks Job timeout ({impl_timeout_m}m) is configured significantly below contract SLA ({exp_sla_m}m)."
                    ),
                    status=CheckpointStatus.WARN,
                    severity=Severity.MEDIUM,
                    drift_severity=DriftSeverity.WARN,
                    expected=f"{exp_sla_m} minutes SLA",
                    implemented=f"{impl_timeout_m} minutes timeout",
                    actual=act_dur_m,
                    evidence=[
                        f"Contract SLA max_runtime_minutes: {exp_sla_m}",
                        f"Job timeout_seconds: {impl_timeout_s} ({impl_timeout_m}m)",
                    ],
                    recommendation="Increase job timeout to prevent premature task termination before SLA limit.",
                    confidence=0.90,
                    provenance=EvidenceProvenanceKind.JOB_CONFIG,
                    location=self.code_filename,
                )
            )

        worst_drift = DriftSeverity.NONE
        status = CheckpointStatus.PASS
        if any(f.drift_severity == DriftSeverity.BLOCKING for f in findings):
            worst_drift = DriftSeverity.BLOCKING
            status = CheckpointStatus.FAIL
        elif any(f.drift_severity == DriftSeverity.WARN for f in findings):
            worst_drift = DriftSeverity.WARN
            status = CheckpointStatus.WARN
        elif not exp_sla_m and not impl_timeout_m and not act_dur_m:
            status = CheckpointStatus.UNKNOWN

        return DimensionAlignmentAssessment(
            dimension=dim,
            status=status,
            drift_severity=worst_drift,
            findings_count=len(findings),
            findings=findings,
            summary=(
                f"{len(findings)} SLA / runtime violation(s)"
                if findings
                else "SLA limits and runtime duration aligned"
            ),
            expected_summary=f"SLA: {exp_sla_m}m" if exp_sla_m else "SLA unspecified",
            implemented_summary=f"Timeout: {impl_timeout_m}m" if impl_timeout_m else "Timeout unspecified",
            actual_summary=f"Duration: {act_dur_m}m" if act_dur_m is not None else "UNKNOWN (offline fixture)",
        )

    # -------------------------------------------------------------------------
    # Dimension 8: RELIABILITY_RETRY_POLICY
    # -------------------------------------------------------------------------
    def _analyze_reliability_policy(self) -> DimensionAlignmentAssessment:
        dim = AlignmentDimension.RELIABILITY_RETRY_POLICY
        findings: list[AlignmentFinding] = []

        rel = getattr(self.contract, "reliability", None) if self.contract else None
        exp_retry = getattr(rel, "retry_count", None) if rel else None
        exp_idempotent = getattr(rel, "idempotent", None) if rel else None

        impl_retries = (
            self.job_config.get("max_retries") if self.job_config else None
        )

        # Contradiction: Contract promises idempotent: true, but M5F discovered duplicate risk or non-idempotence
        if exp_idempotent is True and self.rerun_assessment:
            dup_risk = self.rerun_assessment.duplicate_risk
            idm_status = self.rerun_assessment.overall_status
            if dup_risk.status in (CheckpointStatus.WARN, CheckpointStatus.FAIL) or idm_status == CheckpointStatus.FAIL:
                findings.append(
                    AlignmentFinding(
                        finding_id="ALIGN-REL-001",
                        rule_id="ALIGN-REL-001",
                        dimension=dim,
                        divergence_kind=LayerDivergenceKind.EXPECTED_VS_IMPLEMENTED,
                        title="Contract Promises Idempotency But Code Implementation Exposes Duplicate Risk",
                        description=(
                            "Contract declares `idempotent: true`, but behavioral forensics (M5F) detected "
                            f"duplicate-data risks ({dup_risk.summary})."
                        ),
                        status=CheckpointStatus.FAIL,
                        severity=Severity.HIGH,
                        drift_severity=DriftSeverity.BLOCKING,
                        expected="idempotent: true",
                        implemented=f"Duplicate risk: {dup_risk.status.value}",
                        actual="UNKNOWN",
                        evidence=[
                            "Contract declaration: reliability.idempotent = True",
                            f"Rerun Forensics result: {dup_risk.summary}",
                        ],
                        recommendation="Implement deduplication (e.g. dropDuplicates) or safe Delta MERGE keys.",
                        confidence=0.96,
                        blocking=True,
                        provenance=EvidenceProvenanceKind.STATIC_CODE,
                        location=self.code_filename,
                    )
                )

        # Retry count drift
        if exp_retry is not None and impl_retries is not None and exp_retry != impl_retries:
            findings.append(
                AlignmentFinding(
                    finding_id="ALIGN-REL-002",
                    rule_id="ALIGN-REL-002",
                    dimension=dim,
                    divergence_kind=LayerDivergenceKind.EXPECTED_VS_IMPLEMENTED,
                    title="Retry Count Divergence Between Contract and Job Configuration",
                    description=(
                        f"Contract specifies {exp_retry} retries, but Databricks Job configures max_retries = {impl_retries}."
                    ),
                    status=CheckpointStatus.WARN,
                    severity=Severity.MEDIUM,
                    drift_severity=DriftSeverity.WARN,
                    expected=exp_retry,
                    implemented=impl_retries,
                    actual="UNKNOWN",
                    evidence=[
                        f"Contract retry_count: {exp_retry}",
                        f"Databricks Job max_retries: {impl_retries}",
                    ],
                    recommendation="Align job max_retries with contractual reliability policy.",
                    confidence=0.92,
                    provenance=EvidenceProvenanceKind.JOB_CONFIG,
                    location=self.code_filename,
                )
            )

        worst_drift = DriftSeverity.NONE
        status = CheckpointStatus.PASS
        if any(f.drift_severity == DriftSeverity.BLOCKING for f in findings):
            worst_drift = DriftSeverity.BLOCKING
            status = CheckpointStatus.FAIL
        elif any(f.drift_severity == DriftSeverity.WARN for f in findings):
            worst_drift = DriftSeverity.WARN
            status = CheckpointStatus.WARN
        elif exp_retry is None and impl_retries is None and exp_idempotent is None:
            status = CheckpointStatus.UNKNOWN

        return DimensionAlignmentAssessment(
            dimension=dim,
            status=status,
            drift_severity=worst_drift,
            findings_count=len(findings),
            findings=findings,
            summary=(
                f"{len(findings)} reliability policy divergence(s)"
                if findings
                else "Reliability and retry policies aligned"
            ),
            expected_summary=f"Retries: {exp_retry}, Idempotent: {exp_idempotent}",
            implemented_summary=f"Job retries: {impl_retries}",
            actual_summary="UNKNOWN (runtime retry history not observed)",
        )

    # -------------------------------------------------------------------------
    # Dimension 9: WORKLOAD_VOLUME_BOUNDS
    # -------------------------------------------------------------------------
    def _analyze_workload_volume(self) -> DimensionAlignmentAssessment:
        dim = AlignmentDimension.WORKLOAD_VOLUME_BOUNDS
        findings: list[AlignmentFinding] = []

        src = getattr(self.contract, "source", None) if self.contract else None
        exp_vol = getattr(src, "expected_volume_gb", None) if src else None
        peak_vol = getattr(src, "peak_volume_gb", None) if src else None

        obs_vol = getattr(self.data_profile, "total_gb", None) if self.data_profile else None
        act_vol = None
        if self.runtime_run:
            act_vol = getattr(self.runtime_run, "total_input_gb", None)
            if act_vol is None and hasattr(self.runtime_run, "total_input_bytes"):
                in_bytes = self.runtime_run.total_input_bytes
                if in_bytes is not None:
                    act_vol = round(in_bytes / (1024.0**3), 2)

        # Check observed or actual volume vs contractual expectations
        check_vol = act_vol if act_vol is not None else obs_vol
        if peak_vol and check_vol and check_vol > peak_vol * 1.5:
            findings.append(
                AlignmentFinding(
                    finding_id="ALIGN-VOL-001",
                    rule_id="ALIGN-VOL-001",
                    dimension=dim,
                    divergence_kind=(
                        LayerDivergenceKind.EXPECTED_VS_ACTUAL
                        if act_vol is not None
                        else LayerDivergenceKind.EXPECTED_VS_IMPLEMENTED
                    ),
                    title="Workload Ingestion Volume Drifts Far Above Contract Peak",
                    description=(
                        f"Observed ingestion volume ({check_vol:.1f} GB) exceeds contractual peak volume "
                        f"({peak_vol:.1f} GB) by more than 1.5x."
                    ),
                    status=CheckpointStatus.WARN,
                    severity=Severity.HIGH,
                    drift_severity=DriftSeverity.WARN,
                    expected=f"peak {peak_vol} GB",
                    implemented=obs_vol,
                    actual=act_vol,
                    evidence=[
                        f"Contract peak daily volume: {peak_vol} GB",
                        f"Observed volume: {check_vol:.1f} GB",
                    ],
                    recommendation="Re-evaluate cluster sizing and update contractual capacity declaration.",
                    confidence=0.88,
                    provenance=(
                        EvidenceProvenanceKind.RUNTIME
                        if act_vol is not None
                        else EvidenceProvenanceKind.FIXTURE
                    ),
                    location=self.code_filename,
                )
            )

        worst_drift = DriftSeverity.WARN if findings else DriftSeverity.NONE
        status = CheckpointStatus.WARN if findings else CheckpointStatus.PASS
        if exp_vol is None and obs_vol is None and act_vol is None:
            status = CheckpointStatus.UNKNOWN

        return DimensionAlignmentAssessment(
            dimension=dim,
            status=status,
            drift_severity=worst_drift,
            findings_count=len(findings),
            findings=findings,
            summary=(
                f"{len(findings)} workload volume divergence(s)"
                if findings
                else "Workload volume aligned with contractual limits"
            ),
            expected_summary=f"Expected: {exp_vol} GB, Peak: {peak_vol} GB",
            implemented_summary=f"Profile: {obs_vol:.1f} GB" if obs_vol is not None else "Profile missing",
            actual_summary=f"Actual: {act_vol:.1f} GB" if act_vol is not None else "UNKNOWN (runtime telemetry unprovided)",
        )
