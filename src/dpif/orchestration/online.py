"""Online End-to-End Databricks Validation Orchestrator (Phase M5J).

Connects real/live Databricks evidence to the existing DPIF intelligence and decision pipeline:
    LiveDatabricksConnector -> DatabricksEvidenceProvider -> NormalizedPipelineEvidence ->
    Checkpoints (CP-001..CP-024) -> M5E Implementation Forensics ->
    M5F Rerun/Idempotency -> M5G Three-Layer Alignment ->
    M5H Evidence Sufficiency -> M5I Decision & Risk Synthesis.

Enforces zero metric fabrication, strict UNKNOWN semantics, and credential sanitization.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from dpif.analyzers.alignment import ThreeLayerAlignmentAnalyzer
from dpif.analyzers.implementation import DeveloperImplementationAnalyzer
from dpif.analyzers.rerun import RerunIdempotencyAnalyzer
from dpif.analyzers.sufficiency import EvidenceSufficiencyAnalyzer
from dpif.analyzers.synthesis import DecisionRiskSynthesisAnalyzer
from dpif.checkpoints.definitions import build_all_checkpoints
from dpif.checkpoints.engine import CheckpointEngine
from dpif.code.parser import analyze_source as analyze_code
from dpif.connectors.base import DatabricksConnector
from dpif.connectors.live import LiveDatabricksConnector
from dpif.contract.loader import load_contract_file
from dpif.discovery.synthesis import (
    synthesize_discovered_contract,
    synthesize_discovered_data_profile,
)
from dpif.flow import EvidenceState, FlowProvenance, PipelineFlowGraph, build_pipeline_flow_graph
from dpif.flow.correlation import normalize_query_history
from dpif.models import (
    Checkpoint,
    CheckpointStatus,
    DataProfile,
    PipelineContract,
)
from dpif.models.alignment import ThreeLayerAlignmentAssessment
from dpif.models.implementation import EvidenceProvenanceKind, ImplementationForensicsResult
from dpif.models.rerun import RerunAnalysisResult
from dpif.models.sufficiency import EvidenceSufficiencyAssessment
from dpif.models.synthesis import DecisionRiskSynthesisResult
from dpif.providers.base import (
    AcquisitionError,
    AcquisitionErrorCode,
    DatabricksEvidenceProvider,
    EvidenceCategory,
    NormalizedPipelineEvidence,
    fetch_query_history_item,
    mask_sensitive_credentials,
    sanitize_job_payload,
)
from dpif.readiness.engine import evaluate_production_readiness
from dpif.readiness.models import ProductionReadinessAssessment
from dpif.runtime.models import RuntimeRun
from dpif.runtime.normalization import normalize_runtime_payload
from dpif.scoring.engine import readiness_label, score_checkpoints

logger = logging.getLogger(__name__)


class EvidenceCategoryStatus(BaseModel):
    """Diagnostic status for a single evidence category."""

    model_config = ConfigDict(populate_by_name=True)

    category: str
    status: str  # LIVE, STATIC_CODE, PARTIAL, INSUFFICIENT, UNAVAILABLE, ERROR
    provenance: str
    resource_id: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "status": self.status,
            "provenance": self.provenance,
            "resource_id": self.resource_id,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "details": self.details,
        }


class OnlineValidationResult(BaseModel):
    """Complete results from an online end-to-end Databricks validation."""

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)

    execution_mode: str = "online"
    timestamp: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())
    workspace: str
    resource_type: str  # "job" or "pipeline"
    resource_id: str
    pipeline_name: str
    environment: str = "production"
    evidence_summary: dict[str, str] = Field(default_factory=dict)
    evidence_diagnostics: list[EvidenceCategoryStatus] = Field(default_factory=list)
    checkpoints: dict[str, Checkpoint] = Field(default_factory=dict)
    findings: list[Any] = Field(default_factory=list)
    quality_score: float = 0.0
    has_blocking: bool = False
    readiness_label: str = "INSUFFICIENT_EVIDENCE"
    confidence: str = "INSUFFICIENT"
    decision_sufficiency: bool = False
    final_decision: str = "INSUFFICIENT_EVIDENCE"
    implementation_forensics: ImplementationForensicsResult | None = None
    rerun_analysis: RerunAnalysisResult | None = None
    alignment_analysis: ThreeLayerAlignmentAssessment | None = None
    evidence_sufficiency: EvidenceSufficiencyAssessment | None = None
    production_readiness: ProductionReadinessAssessment | None = None
    decision_risk_synthesis: DecisionRiskSynthesisResult | None = None
    flow_graph: PipelineFlowGraph | None = None
    normalized_evidence: NormalizedPipelineEvidence | None = None
    errors: list[AcquisitionError] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to fully sanitized JSON-serializable dictionary."""
        d: dict[str, Any] = {
            "execution_mode": self.execution_mode,
            "timestamp": self.timestamp,
            "workspace": mask_sensitive_credentials(self.workspace),
            "resource": {
                "type": self.resource_type,
                "id": self.resource_id,
            },
            "pipeline_name": self.pipeline_name,
            "environment": self.environment,
            "evidence_provenance": {diag.category: diag.provenance for diag in self.evidence_diagnostics},
            "evidence_summary": self.evidence_summary,
            "evidence_diagnostics": [diag.to_dict() for diag in self.evidence_diagnostics],
            "quality_score": round(self.quality_score, 1),
            "confidence": self.confidence,
            "decision_sufficiency": self.decision_sufficiency,
            "final_decision": self.final_decision,
            "readiness_label": self.readiness_label,
            "has_blocking": self.has_blocking,
            "checkpoints": {
                k: {
                    "checkpoint_id": v.checkpoint_id,
                    "name": v.name,
                    "status": v.status.value,
                    "severity": v.severity.value,
                    "score": round(v.score, 2),
                    "confidence": round(v.evidence.confidence, 2) if v.evidence else 0.0,
                    "findings_count": len(v.findings),
                }
                for k, v in self.checkpoints.items()
            },
            "findings": [
                {
                    "rule_id": f.rule_id,
                    "title": f.title or f.name,
                    "severity": f.severity.value,
                    "status": f.status.value,
                    "recommendation": f.recommendation,
                }
                for f in self.findings
            ],
            "errors": [e.to_dict() for e in self.errors],
        }

        if self.implementation_forensics:
            d["implementation_forensics"] = self.implementation_forensics.to_dict()
        if self.rerun_analysis:
            d["rerun_analysis"] = self.rerun_analysis.to_dict()
        if self.alignment_analysis:
            d["alignment_analysis"] = self.alignment_analysis.to_dict()
        if self.evidence_sufficiency:
            d["evidence_sufficiency"] = self.evidence_sufficiency.to_dict()
        if self.production_readiness:
            d["production_readiness"] = self.production_readiness.to_dict()
        if self.decision_risk_synthesis:
            d["decision_risk_synthesis"] = self.decision_risk_synthesis.to_dict()
        if self.flow_graph is not None:
            d["pipeline_flow_graph"] = self.flow_graph.to_dict()

        return sanitize_job_payload(d)


class OnlineValidationOrchestrator:
    """Orchestrator coordinating live evidence acquisition and downstream DPIF analysis."""

    def __init__(
        self,
        workspace: str | None = None,
        token: str | None = None,
        connector: DatabricksConnector | None = None,
        timeout_seconds: float = 15.0,
    ) -> None:
        from dpif.config import resolve_databricks_credentials

        creds = resolve_databricks_credentials(host=workspace, token=token, load_env=True)
        self.workspace = creds["host"] or ""
        self.token = creds["token"] or ""

        if connector is not None:
            self.connector = connector
            if not self.workspace and hasattr(connector, "host"):
                self.workspace = getattr(connector, "host", "") or ""
            if not self.token and hasattr(connector, "_token"):
                self.token = getattr(connector, "_token", "") or ""
        else:
            self.connector = LiveDatabricksConnector(
                host=self.workspace,
                token=self.token,
                timeout_seconds=timeout_seconds,
            )

        self.provider = DatabricksEvidenceProvider(connector=self.connector)

    def validate(
        self,
        job_id: int | str | None = None,
        pipeline_id: str | None = None,
        run_id: int | str | None = None,
        contract_path: str | None = None,
        code_path: str | None = None,
        environment: str | None = None,
        include_historical_runs: bool = True,
    ) -> OnlineValidationResult:
        """Execute full online validation flow."""
        if job_id is None and pipeline_id is None:
            raise ValueError(
                "Either job_id or pipeline_id must be provided for online Databricks validation."
            )

        resource_type = "job" if job_id is not None else "pipeline"
        resource_id = str(job_id if job_id is not None else pipeline_id)
        effective_env = environment or "production"

        # 1. Acquire live evidence via provider
        pipeline_key = str(pipeline_id or f"job-{job_id}")
        evidence = self.provider.acquire_pipeline_evidence(
            pipeline_id=pipeline_key,
            job_id=job_id,
            cluster_id=None,  # Provider discovers from job task if available
            run_id=run_id,
            include_historical_runs=include_historical_runs,
        )

        # If run_id was not explicitly specified, inspect discovered historical runs
        # to acquire the latest run details automatically (without inventing associations)
        if run_id is None and job_id is not None:
            hist_item = evidence.items.get(EvidenceCategory.HISTORICAL_RUNS.value)
            if hist_item and hist_item.is_available and isinstance(hist_item.payload, list):
                if len(hist_item.payload) > 0 and isinstance(hist_item.payload[0], dict):
                    discovered_run_id = hist_item.payload[0].get("run_id")
                    if discovered_run_id:
                        try:
                            discovered_runtime = self.provider.acquire_pipeline_evidence(
                                pipeline_id=pipeline_key,
                                run_id=discovered_run_id,
                            ).items.get(EvidenceCategory.RUNTIME.value)
                            if discovered_runtime and discovered_runtime.is_available:
                                evidence.items[EvidenceCategory.RUNTIME.value] = discovered_runtime
                        except Exception as e:
                            logger.info("Could not fetch discovered latest run: %s", e)

        # 2. Extract and categorize evidence
        diagnostics: list[EvidenceCategoryStatus] = []
        summary: dict[str, str] = {}

        # Workspace
        ws_item = evidence.items.get(EvidenceCategory.WORKSPACE.value)
        if ws_item and ws_item.is_available:
            summary["workspace"] = "LIVE"
            diagnostics.append(
                EvidenceCategoryStatus(
                    category="workspace",
                    status="LIVE",
                    provenance=ws_item.provenance.source_type,
                    resource_id=self.workspace,
                )
            )
        else:
            summary["workspace"] = "ERROR" if (ws_item and ws_item.error) else "UNAVAILABLE"
            diagnostics.append(
                EvidenceCategoryStatus(
                    category="workspace",
                    status=summary["workspace"],
                    provenance="UNAVAILABLE",
                    resource_id=self.workspace,
                    error_code=ws_item.error.error_code.value if ws_item and ws_item.error else None,
                    error_message=ws_item.error.message if ws_item and ws_item.error else None,
                )
            )

        # Job
        job_item = evidence.items.get(EvidenceCategory.JOB.value)
        job_config: dict[str, Any] | None = None
        if job_item and job_item.is_available and isinstance(job_item.payload, dict):
            summary["job"] = "LIVE"
            job_config = job_item.payload
            diagnostics.append(
                EvidenceCategoryStatus(
                    category="job",
                    status="LIVE",
                    provenance=job_item.provenance.source_type,
                    resource_id=str(job_id),
                )
            )
        elif job_id is not None:
            status_lbl = "ERROR" if (job_item and job_item.error) else "UNAVAILABLE"
            summary["job"] = status_lbl
            diagnostics.append(
                EvidenceCategoryStatus(
                    category="job",
                    status=status_lbl,
                    provenance="UNAVAILABLE",
                    resource_id=str(job_id),
                    error_code=job_item.error.error_code.value if job_item and job_item.error else None,
                    error_message=job_item.error.message if job_item and job_item.error else None,
                )
            )

        # Pipeline (DLT)
        pipe_item = evidence.items.get(EvidenceCategory.PIPELINE.value)
        if pipe_item and pipe_item.is_available and isinstance(pipe_item.payload, dict):
            summary["pipeline"] = "LIVE"
            diagnostics.append(
                EvidenceCategoryStatus(
                    category="pipeline",
                    status="LIVE",
                    provenance=pipe_item.provenance.source_type,
                    resource_id=str(pipeline_id),
                )
            )
        elif pipeline_id is not None:
            status_lbl = "ERROR" if (pipe_item and pipe_item.error) else "UNAVAILABLE"
            summary["pipeline"] = status_lbl
            diagnostics.append(
                EvidenceCategoryStatus(
                    category="pipeline",
                    status=status_lbl,
                    provenance="UNAVAILABLE",
                    resource_id=str(pipeline_id),
                    error_code=pipe_item.error.error_code.value if pipe_item and pipe_item.error else None,
                    error_message=pipe_item.error.message if pipe_item and pipe_item.error else None,
                )
            )

        # Cluster
        cluster_item = evidence.items.get(EvidenceCategory.CLUSTER.value)
        cluster_config: dict[str, Any] | None = None
        if cluster_item and cluster_item.is_available and isinstance(cluster_item.payload, dict):
            summary["cluster"] = "LIVE"
            cluster_config = cluster_item.payload
            diagnostics.append(
                EvidenceCategoryStatus(
                    category="cluster",
                    status="LIVE",
                    provenance=cluster_item.provenance.source_type,
                    resource_id=cluster_item.provenance.resource_id,
                )
            )
        else:
            status_lbl = "ERROR" if (cluster_item and cluster_item.error) else "UNAVAILABLE"
            summary["cluster"] = status_lbl
            diagnostics.append(
                EvidenceCategoryStatus(
                    category="cluster",
                    status=status_lbl,
                    provenance="UNAVAILABLE",
                    resource_id=cluster_item.provenance.resource_id if cluster_item else None,
                    error_code=cluster_item.error.error_code.value if cluster_item and cluster_item.error else None,
                    error_message=cluster_item.error.message if cluster_item and cluster_item.error else None,
                )
            )

        # Runtime run
        runtime_item = evidence.items.get(EvidenceCategory.RUNTIME.value)
        runtime_obj: RuntimeRun | None = None
        if runtime_item and runtime_item.is_available and isinstance(runtime_item.payload, dict):
            summary["runtime"] = "LIVE"
            try:
                norm_rt = normalize_runtime_payload(runtime_item.payload)
                runtime_obj = norm_rt if isinstance(norm_rt, RuntimeRun) else RuntimeRun(**norm_rt)
            except Exception as e:
                logger.warning("Failed to normalize runtime payload: %s", e)
                runtime_obj = None

            diagnostics.append(
                EvidenceCategoryStatus(
                    category="runtime",
                    status="LIVE" if runtime_obj else "PARTIAL",
                    provenance=runtime_item.provenance.source_type,
                    resource_id=runtime_item.provenance.resource_id,
                )
            )
        else:
            status_lbl = "ERROR" if (runtime_item and runtime_item.error) else "UNAVAILABLE"
            summary["runtime"] = status_lbl
            diagnostics.append(
                EvidenceCategoryStatus(
                    category="runtime",
                    status=status_lbl,
                    provenance="UNAVAILABLE",
                    resource_id=str(run_id) if run_id else None,
                    error_code=runtime_item.error.error_code.value if runtime_item and runtime_item.error else None,
                    error_message=runtime_item.error.message if runtime_item and runtime_item.error else None,
                )
            )

        # Historical runs (Preserve [] vs None distinction!)
        hist_item = evidence.items.get(EvidenceCategory.HISTORICAL_RUNS.value)
        historical_objs: list[Any] | None = None
        if hist_item and hist_item.is_available and isinstance(hist_item.payload, list):
            # API succeeded: payload is [] (empty) or [...] (non-empty)
            historical_objs = hist_item.payload
            summary["historical"] = "LIVE"
            diagnostics.append(
                EvidenceCategoryStatus(
                    category="historical",
                    status="LIVE",
                    provenance=hist_item.provenance.source_type,
                    resource_id=str(job_id),
                    details={"runs_count": len(historical_objs)},
                )
            )
        else:
            # API failure or not acquired -> None
            historical_objs = None
            status_lbl = "ERROR" if (hist_item and hist_item.error) else "UNAVAILABLE"
            summary["historical"] = "INSUFFICIENT" if status_lbl == "UNAVAILABLE" else status_lbl
            diagnostics.append(
                EvidenceCategoryStatus(
                    category="historical",
                    status=summary["historical"],
                    provenance="UNAVAILABLE",
                    resource_id=str(job_id),
                    error_code=hist_item.error.error_code.value if hist_item and hist_item.error else None,
                    error_message=hist_item.error.message if hist_item and hist_item.error else None,
                )
            )

        # Code Evidence
        code_text = ""
        analysis = None
        code_filename = "pipeline.py"
        contract: PipelineContract | None = None
        # Phase 8: per-task validation coverage (ANALYZED / UNSUPPORTED /
        # UNRETRIEVABLE). Populated from the live CODE payload when present.
        task_topology: list[dict[str, Any]] = []
        coverage_summary: dict[str, Any] = {}
        uncovered_tasks: list[dict[str, Any]] = []
        # Phase 9 (P9-2): attribution-only task analyses (never consumed by
        # checkpoints/rules/M5*) and the analysis object handed to the flow
        # graph builder (tagged deep copy when attribution exists, else the
        # combined analysis itself).
        task_analyses: dict[str, Any] = {}
        flow_analysis = analysis

        if contract_path:
            try:
                contract = load_contract_file(contract_path)
                if environment:
                    contract.environment = environment
            except Exception as e:
                logger.warning("Could not load contract file %s: %s", contract_path, e)

        if code_path and os.path.exists(code_path):
            try:
                with open(code_path, encoding="utf-8") as cf:
                    code_text = cf.read()
                code_filename = os.path.basename(code_path)
                analysis = analyze_code(code_text, filename=code_filename)
                if analysis and analysis.parse_error:
                    summary["code"] = "PARTIAL"
                else:
                    summary["code"] = "LIVE"
                diagnostics.append(
                    EvidenceCategoryStatus(
                        category="code",
                        status=summary["code"],
                        provenance="STATIC_CODE",
                        resource_id=code_filename,
                    )
                )
            except Exception as e:
                summary["code"] = "ERROR"
                diagnostics.append(
                    EvidenceCategoryStatus(
                        category="code",
                        status="ERROR",
                        provenance="UNAVAILABLE",
                        resource_id=code_path,
                        error_message=str(e),
                    )
                )
        elif contract and getattr(contract, "code_snippet", None):
            code_text = str(getattr(contract, "code_snippet", "") or "")
            analysis = analyze_code(code_text, filename="contract_snippet.py")
            summary["code"] = "PARTIAL"
            diagnostics.append(
                EvidenceCategoryStatus(
                    category="code",
                    status="PARTIAL",
                    provenance="CONTRACT",
                    resource_id="contract_snippet",
                )
            )
        else:
            # Check acquired live code from Databricks tasks / workspace
            code_item = evidence.items.get(EvidenceCategory.CODE.value)
        # Phase 8: per-task validation coverage travels with the code
        # payload. Every enumerated job task must remain represented
        # (ANALYZED / UNSUPPORTED / UNRETRIEVABLE) — never silent.
            # Phase 9 (P9-2): attribution-only task analyses + a tagged deep copy
            # for the flow graph. The combined `analysis` stays the sole
            # authority for checkpoints/rules/M5E/M5G and all decision inputs.
            from dpif.code.parser import (
                analyze_tasks_for_attribution,
                parse_task_boundaries,
                tag_operations_with_tasks,
            )

            task_analyses = {}
            flow_analysis = analysis
            if code_item and isinstance(code_item.payload, dict):
                task_topology = code_item.payload.get("task_topology", []) or []
                coverage_summary = code_item.payload.get("coverage_summary", {}) or {}
            uncovered_tasks = [
                t for t in task_topology if t.get("coverage_state") != "ANALYZED"
            ]
            analyzed_tasks: list[dict[str, Any]] = [
                t for t in task_topology if t.get("coverage_state") == "ANALYZED"
            ]
            if code_item and code_item.is_available and isinstance(code_item.payload, dict):
                discovered_tasks = code_item.payload.get("tasks", [])
                primary_src = code_item.payload.get("primary_source_code", "")
                primary_fname = code_item.payload.get("primary_filename", "pipeline.py")
                combined = code_item.payload.get("combined_code", primary_src)

                if analyzed_tasks or combined or primary_src:
                    code_text = combined or primary_src
                    code_filename = primary_fname
                    analysis = analyze_code(code_text, filename=code_filename)
                    status_lbl = "PARTIAL" if (analysis and analysis.parse_error) else "LIVE"
                    # Phase 9 (P9-2): attribution metadata only. Per-task
                    # analyses never feed checkpoints/rules/M5*; only the
                    # tagged deep copy below reaches the flow-graph builder.
                    task_analyses = analyze_tasks_for_attribution(
                        [
                            t
                            for t in discovered_tasks
                            if t.get("coverage_state") == "ANALYZED"
                        ]
                    )
                    if analysis is not None and any(
                        v is not None for v in task_analyses.values()
                    ):
                        attributed = analysis.model_copy(deep=True)
                        tag_operations_with_tasks(
                            attributed, parse_task_boundaries(code_text)
                        )
                        flow_analysis = attributed
                else:
                    # Phase 8: tasks enumerated but none yielded code.
                    # Coverage is recorded in task_topology; code content
                    # itself is UNAVAILABLE (UNKNOWN downstream, never PASS).
                    status_lbl = "UNAVAILABLE"
                summary["code"] = status_lbl
                diagnostics.append(
                    EvidenceCategoryStatus(
                        category="code",
                        status=status_lbl,
                        provenance=code_item.provenance.source_type,
                        resource_id=code_item.provenance.resource_id,
                        details={
                            "task_count": len(discovered_tasks),
                            "tasks_analyzed": len(analyzed_tasks),
                            "tasks_unsupported": len(
                                [t for t in uncovered_tasks if t.get("coverage_state") == "UNSUPPORTED"]
                            ),
                            "tasks_unretrievable": len(
                                [t for t in uncovered_tasks if t.get("coverage_state") == "UNRETRIEVABLE"]
                            ),
                            "uncovered_tasks": [
                                f"{t.get('task_key')} [{t.get('coverage_state')}]"
                                for t in uncovered_tasks
                            ],
                        },
                    )
                )
            else:
                is_err = (
                    code_item is not None
                    and code_item.error is not None
                    and code_item.error.error_code not in (AcquisitionErrorCode.RESOURCE_NOT_FOUND,)
                )
                status_lbl = "ERROR" if is_err else "UNAVAILABLE"
                summary["code"] = status_lbl
                diagnostics.append(
                    EvidenceCategoryStatus(
                        category="code",
                        status=status_lbl,
                        provenance="UNAVAILABLE",
                        resource_id=None,
                        error_code=code_item.error.error_code.value if (code_item and code_item.error) else None,
                        error_message=code_item.error.message if (code_item and code_item.error) else "No task code discovered from Databricks workload",
                    )
                )

        # Unity Catalog / Table metadata
        table_item = evidence.items.get(EvidenceCategory.TABLE_PROFILE.value)
        table_profile_payload: dict[str, Any] | None = (
            table_item.payload
            if (table_item and table_item.is_available and isinstance(table_item.payload, dict))
            else None
        )

        # If table profile wasn't directly requested, attempt discovery from parsed code
        if not table_profile_payload and analysis:
            import re
            discovered_table_name = None
            if hasattr(analysis, "operations"):
                for op in analysis.operations:
                    if op.operation_type.value == "READ" and op.arguments.get("via") == "table":
                        cand = str(op.arguments.get("query", "")).strip(" '\"")
                        if cand:
                            discovered_table_name = cand
                            break
            if not discovered_table_name and code_text:
                m_tbl = re.search(r"""(?i)spark(?:\.read)?\.table\(\s*['"]([a-zA-Z0-9_.]+)['"]\s*\)""", code_text)
                if m_tbl:
                    discovered_table_name = m_tbl.group(1)

            if discovered_table_name:
                try:
                    table_profile_payload = self.connector.get_table_profile(discovered_table_name)
                    if table_profile_payload:
                        diagnostics.append(
                            EvidenceCategoryStatus(
                                category="table_profile",
                                status="LIVE",
                                provenance="LIVE_API",
                                resource_id=discovered_table_name,
                            )
                        )
                except Exception as e:
                    logger.info("Could not fetch profile for discovered table %s: %s", discovered_table_name, e)

        # Determine pipeline name
        if contract and contract.pipeline_name:
            pipeline_name = contract.pipeline_name
        elif job_config and isinstance(job_config.get("settings"), dict) and job_config["settings"].get("name"):
            pipeline_name = str(job_config["settings"]["name"])
        elif job_config and job_config.get("name"):
            pipeline_name = str(job_config["name"])
        elif pipe_item and pipe_item.is_available and isinstance(pipe_item.payload, dict) and pipe_item.payload.get("name"):
            pipeline_name = str(pipe_item.payload["name"])
        else:
            pipeline_name = f"{resource_type}-{resource_id}"

        # Contract Synthesis if not manually provided
        if not contract:
            contract = synthesize_discovered_contract(
                job_config=job_config,
                cluster_config=cluster_config,
                code_analysis=analysis,
                raw_code=code_text,
                table_profile=table_profile_payload,
                pipeline_name=pipeline_name,
                environment=effective_env,
                resource_id=resource_id,
            )
            if contract:
                diagnostics.append(
                    EvidenceCategoryStatus(
                        category="contract",
                        status="LIVE",
                        provenance="DERIVED",
                        resource_id=contract.contract_id,
                    )
                )
            else:
                diagnostics.append(
                    EvidenceCategoryStatus(
                        category="contract",
                        status="UNAVAILABLE",
                        provenance="UNAVAILABLE",
                        resource_id=None,
                    )
                )

        # DataProfile Synthesis from runtime metrics and table metadata
        data_profile: DataProfile | None = synthesize_discovered_data_profile(
            runtime_run=runtime_obj,
            table_profile=table_profile_payload,
        )
        if data_profile:
            summary["data_profile"] = "LIVE"
            diagnostics.append(
                EvidenceCategoryStatus(
                    category="data_profile",
                    status="LIVE",
                    provenance=data_profile.evidence_source,
                    details={"total_gb": data_profile.total_gb, "record_count": data_profile.record_count},
                )
            )
        else:
            summary["data_profile"] = "UNAVAILABLE"
            diagnostics.append(
                EvidenceCategoryStatus(
                    category="data_profile",
                    status="UNAVAILABLE",
                    provenance="UNAVAILABLE",
                )
            )

        # Phase 3: query-history evidence for SQL operation correlation.
        # Retrieved ONLY when static SQL fingerprints exist (narrow scope —
        # no platform-wide crawling). Unavailable/error stays UNKNOWN.
        query_entries = None
        sql_queries = (
            list(analysis.sql_analysis.queries)
            if (analysis is not None and analysis.sql_analysis is not None)
            else []
        )
        if any(getattr(q, "fingerprint", None) for q in sql_queries):
            qh_item = fetch_query_history_item(
                self.connector, limit=25, resource_id=pipeline_name
            )
            if qh_item.is_available and isinstance(qh_item.payload, list):
                summary["query_history"] = "LIVE"
                query_entries = normalize_query_history(qh_item.payload)
            else:
                summary["query_history"] = (
                    "ERROR" if qh_item.error else "UNAVAILABLE"
                )
            diagnostics.append(
                EvidenceCategoryStatus(
                    category="query_history",
                    status=summary["query_history"],
                    provenance=qh_item.provenance.source_type,
                    resource_id=pipeline_name,
                    error_code=qh_item.error.error_code.value if qh_item.error else None,
                    error_message=qh_item.error.message if qh_item.error else None,
                    details={"entries_count": len(query_entries or [])}
                    if qh_item.is_available
                    else {},
                )
            )
            history_prov = FlowProvenance(
                kind=EvidenceProvenanceKind.RUNTIME,
                state=EvidenceState.KNOWN if qh_item.is_available else EvidenceState.UNKNOWN,
                reference="live query history",
            )
        else:
            history_prov = None

        # Common pipeline flow graph (GAP-001) — same builder as offline.
        # Phase 2: same SOURCE/TARGET volume baselines from live evidence.
        # Phase 3: same SQL operation correlation from live query history.
        # Phase 9 (P9-2): the builder receives the tagged deep copy when
        # task attribution exists; otherwise the combined analysis itself.
        if flow_analysis is None:
            flow_analysis = analysis
        flow_graph = build_pipeline_flow_graph(
            code_analysis=flow_analysis,
            contract=contract,
            pipeline_name=pipeline_name,
            raw_code=code_text,
            data_profile=data_profile,
            runtime_run=runtime_obj,
            query_history=query_entries,
            history_provenance=history_prov,
        )

        # 3. Downstream DPIF Intelligence Execution
        rule_context: dict[str, Any] = {
            "mode": "live-api",
            "evidence_source": "live databricks",
            "connector_mode": "live",
            # Phase 4: M5E completeness needs the code text + flow graph.
            "code_snippet": code_text,
            "flow_graph": flow_graph,
            "data_size_gb": (
                data_profile.total_gb
                if data_profile
                else (
                    getattr(contract.source, "expected_volume_gb", 0.0)
                    if (contract and contract.source)
                    else 0.0
                )
            ),
            "source_type": (
                getattr(contract.source.type, "value", "unknown")
                if (contract and contract.source and contract.source.type)
                else "unknown"
            ),
            "workload_type": getattr(contract, "processing", "batch") if contract else "batch",
            "ingestion_mode": (
                getattr(contract.source.ingestion_mode, "value", "batch")
                if (contract and contract.source and contract.source.ingestion_mode)
                else "batch"
            ),
            "source_format": (
                getattr(contract.source.format, "value", "unknown")
                if (contract and contract.source and contract.source.format)
                else "unknown"
            ),
        }

        context: dict[str, Any] = {
            "pipeline_name": pipeline_name,
            "pipeline_contract": contract,
            "contract": contract,
            "source": contract.source if contract else None,
            "data_profile": data_profile,
            "cluster_config": cluster_config,
            "cluster": cluster_config,
            "job_config": job_config,
            "job": job_config,
            "assumptions": {"mode": "live-api", "evidence_source": "live databricks"},
            "connector_mode": "live",
            "rule_context": rule_context,
            "runtime_run": runtime_obj,
            "runtime_data": runtime_obj,
            "historical_runs": historical_objs,
            "code_snippet": code_text,
            "code_filename": code_filename,
            "code_analysis": analysis,
            "flow_graph": flow_graph,
            "actual_environment": cluster_config,
            "workspace_cluster": cluster_config,
            # Phase 8: job task topology + validation coverage for M5H/M5I.
            "task_topology": task_topology,
            "task_coverage_summary": coverage_summary,
            # Phase 9 (P9-2): attribution-only per-task analyses. No consumer
            # reads this key; it exists for attribution metadata only.
            "task_analyses": task_analyses,
        }

        # Checkpoints CP-001..CP-024
        checkpoints = build_all_checkpoints(
            contract=contract,
            data_profile=data_profile,
            code_text=code_text,
            cluster_config=cluster_config,
            job_config=job_config,
            runtime_data=runtime_obj,
            historical_runs=historical_objs,
        )
        engine = CheckpointEngine()
        results = engine.run_all_checkpoints(checkpoints, context)

        score, has_blocking = score_checkpoints(results)
        has_fail = any(cp.status == CheckpointStatus.FAIL for cp in results.values())
        has_unknown_or_warn = any(
            cp.status in (CheckpointStatus.UNKNOWN, CheckpointStatus.WARN)
            for cp in results.values()
        )
        readiness_lbl = readiness_label(score, has_blocking, has_fail, has_unknown_or_warn)

        # M5E Developer Implementation Forensics
        effective_analysis = analysis or analyze_code("", filename="unavailable.py")
        impl_analyzer = DeveloperImplementationAnalyzer(effective_analysis, context=rule_context)
        impl_assessment = impl_analyzer.analyze()
        context["implementation_forensics"] = impl_assessment

        # M5F Rerun / Idempotency Forensics
        rerun_ctx = dict(rule_context)
        rerun_ctx.update(context)
        rerun_analyzer = RerunIdempotencyAnalyzer(analysis, context=rerun_ctx)
        rerun_assessment = rerun_analyzer.analyze()
        context["rerun_analysis"] = rerun_assessment

        # M5G Three-Layer Alignment Forensics
        alignment_ctx = dict(rule_context)
        alignment_ctx.update(context)
        alignment_analyzer = ThreeLayerAlignmentAnalyzer(analysis, context=alignment_ctx)
        alignment_assessment = alignment_analyzer.analyze()
        context["alignment_analysis"] = alignment_assessment

        # M5H Evidence Coverage, Confidence & Decision Sufficiency
        sufficiency_ctx = dict(rule_context)
        sufficiency_ctx.update(context)
        sufficiency_ctx["checkpoints"] = results
        sufficiency_analyzer = EvidenceSufficiencyAnalyzer(analysis, context=sufficiency_ctx)
        sufficiency_assessment = sufficiency_analyzer.analyze()
        context["evidence_sufficiency"] = sufficiency_assessment

        # Production Readiness Assessment
        assessment = evaluate_production_readiness(
            checkpoints=results,
            contract=contract,
            profile=data_profile,
            cluster_config=cluster_config,
            job_config=job_config,
            runtime_data=runtime_obj,
            historical_runs=historical_objs,
            connector_mode="live",
        )

        # M5I Decision & Risk Synthesis
        synthesis_analyzer = DecisionRiskSynthesisAnalyzer(
            checkpoints=results,
            readiness=assessment,
            implementation_forensics=impl_assessment,
            rerun_analysis=rerun_assessment,
            alignment_analysis=alignment_assessment,
            evidence_sufficiency=sufficiency_assessment,
            contract=contract,
            profile=data_profile,
            context=context,
        )
        synthesis_assessment = synthesis_analyzer.analyze()
        context["decision_risk_synthesis"] = synthesis_assessment

        # Collect all findings
        all_findings = [f for cp in results.values() for f in cp.findings]

        return OnlineValidationResult(
            execution_mode="online",
            workspace=self.workspace,
            resource_type=resource_type,
            resource_id=resource_id,
            pipeline_name=pipeline_name,
            environment=effective_env,
            evidence_summary=summary,
            evidence_diagnostics=diagnostics,
            checkpoints=results,
            findings=all_findings,
            quality_score=score.overall,
            has_blocking=has_blocking,
            readiness_label=readiness_lbl,
            confidence=synthesis_assessment.confidence.value,
            decision_sufficiency=synthesis_assessment.decision_sufficiency,
            final_decision=synthesis_assessment.final_decision.value,
            implementation_forensics=impl_assessment,
            rerun_analysis=rerun_assessment,
            alignment_analysis=alignment_assessment,
            evidence_sufficiency=sufficiency_assessment,
            production_readiness=assessment,
            decision_risk_synthesis=synthesis_assessment,
            flow_graph=flow_graph,
            normalized_evidence=evidence,
            errors=evidence.errors,
        )


def run_online_validation(
    workspace: str | None = None,
    token: str | None = None,
    job_id: int | str | None = None,
    pipeline_id: str | None = None,
    run_id: int | str | None = None,
    contract_path: str | None = None,
    code_path: str | None = None,
    environment: str | None = None,
    include_historical_runs: bool = True,
    connector: DatabricksConnector | None = None,
    timeout_seconds: float = 15.0,
) -> OnlineValidationResult:
    """Convenience functional interface for online Databricks validation."""
    from dpif.config import resolve_databricks_credentials

    creds = resolve_databricks_credentials(
        host=workspace,
        token=token,
        job_id=job_id,
        run_id=run_id,
        pipeline_id=pipeline_id,
        load_env=True,
    )
    orchestrator = OnlineValidationOrchestrator(
        workspace=creds["host"],
        token=creds["token"],
        connector=connector,
        timeout_seconds=timeout_seconds,
    )
    return orchestrator.validate(
        job_id=creds["job_id"],
        pipeline_id=creds["pipeline_id"],
        run_id=creds["run_id"],
        contract_path=contract_path,
        code_path=code_path,
        environment=environment,
        include_historical_runs=include_historical_runs,
    )
