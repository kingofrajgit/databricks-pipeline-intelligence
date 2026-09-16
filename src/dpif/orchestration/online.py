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
from dpif.models import (
    Checkpoint,
    CheckpointStatus,
    PipelineContract,
)
from dpif.models.alignment import ThreeLayerAlignmentAssessment
from dpif.models.implementation import ImplementationForensicsResult
from dpif.models.rerun import RerunAnalysisResult
from dpif.models.sufficiency import EvidenceSufficiencyAssessment
from dpif.models.synthesis import DecisionRiskSynthesisResult
from dpif.providers.base import (
    AcquisitionError,
    DatabricksEvidenceProvider,
    EvidenceCategory,
    NormalizedPipelineEvidence,
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
    normalized_evidence: NormalizedPipelineEvidence | None = None
    errors: list[AcquisitionError] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to fully sanitized JSON-serializable dictionary."""
        d: dict[str, Any] = {
            "execution_mode": self.execution_mode,
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
            # Code is not available from live API alone; do NOT fabricate code
            summary["code"] = "UNAVAILABLE"
            diagnostics.append(
                EvidenceCategoryStatus(
                    category="code",
                    status="UNAVAILABLE",
                    provenance="UNAVAILABLE",
                    resource_id=None,
                    error_message="Live Databricks code inspection requires explicit --code-path",
                )
            )

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

        # 3. Downstream DPIF Intelligence Execution
        rule_context: dict[str, Any] = {
            "mode": "live-api",
            "evidence_source": "live databricks",
            "connector_mode": "live",
            "data_size_gb": getattr(contract.source, "expected_volume_gb", 0.0) if (contract and contract.source) else 0.0,
            "source_type": getattr(contract.source.type, "value", "unknown") if (contract and contract.source and contract.source.type) else "unknown",
            "workload_type": getattr(contract, "processing", "batch") if contract else "batch",
        }

        context: dict[str, Any] = {
            "pipeline_name": pipeline_name,
            "pipeline_contract": contract,
            "contract": contract,
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
            "actual_environment": cluster_config,
            "workspace_cluster": cluster_config,
        }

        # Checkpoints CP-001..CP-024
        checkpoints = build_all_checkpoints(
            contract=contract,
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
