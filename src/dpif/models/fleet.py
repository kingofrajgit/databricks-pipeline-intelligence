"""Enterprise Fleet Validation Data Models (Phase M5K).

Provides domain models for:
- Environment tiers and enterprise policy specifications
- Multi-pipeline fleet manifests and execution targets
- Cross-pipeline target collisions and resource contention
- Aggregated fleet validation metrics and summary reports
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from dpif.models import Severity
from dpif.models.sufficiency import ConfidenceLevel
from dpif.models.synthesis import FinalDecisionStatus


class EnvironmentTier(StrEnum):
    """Supported enterprise deployment environment tiers."""

    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class CollisionStatus(StrEnum):
    """Certainty level of a cross-pipeline data or resource collision."""

    CONFIRMED = "CONFIRMED"
    POTENTIAL = "POTENTIAL"
    UNKNOWN = "UNKNOWN"


class EnterpriseEnvironmentPolicy(BaseModel):
    """Deterministic policy rules governing pipeline promotion across environment tiers."""

    model_config = ConfigDict(populate_by_name=True)

    tier: EnvironmentTier = EnvironmentTier.PRODUCTION
    min_quality_score: float = 75.0
    min_confidence: ConfidenceLevel = ConfidenceLevel.HIGH
    require_decision_sufficiency: bool = True
    allow_conditional_go: bool = False
    block_on_p0_risks: bool = True
    block_on_configuration_drift: bool = True

    @classmethod
    def default_for_tier(cls, tier: EnvironmentTier | str) -> EnterpriseEnvironmentPolicy:
        """Construct standard default policy for the specified environment tier."""
        tier_val = EnvironmentTier(str(tier).lower()) if not isinstance(tier, EnvironmentTier) else tier
        if tier_val == EnvironmentTier.DEVELOPMENT:
            return cls(
                tier=EnvironmentTier.DEVELOPMENT,
                min_quality_score=50.0,
                min_confidence=ConfidenceLevel.LOW,
                require_decision_sufficiency=False,
                allow_conditional_go=True,
                block_on_p0_risks=False,
                block_on_configuration_drift=False,
            )
        elif tier_val == EnvironmentTier.STAGING:
            return cls(
                tier=EnvironmentTier.STAGING,
                min_quality_score=70.0,
                min_confidence=ConfidenceLevel.MEDIUM,
                require_decision_sufficiency=True,
                allow_conditional_go=True,
                block_on_p0_risks=True,
                block_on_configuration_drift=True,
            )
        else:  # PRODUCTION
            return cls(
                tier=EnvironmentTier.PRODUCTION,
                min_quality_score=75.0,
                min_confidence=ConfidenceLevel.HIGH,
                require_decision_sufficiency=True,
                allow_conditional_go=False,
                block_on_p0_risks=True,
                block_on_configuration_drift=True,
            )

    def evaluate_pipeline(
        self,
        result: Any | None,
        error: str | None = None,
    ) -> tuple[bool, list[str]]:
        """Evaluate whether an individual pipeline validation meets this environment policy.

        Returns:
            Tuple of (passed: bool, violations: list[str]).
        """
        violations: list[str] = []

        if result is None or error:
            err_msg = error or "Execution failed without producing validation results"
            violations.append(f"Pipeline execution failure: {err_msg}")
            return False, violations

        # 1. Quality score
        if result.quality_score < self.min_quality_score:
            violations.append(
                f"Quality score {result.quality_score:.1f} is below policy minimum {self.min_quality_score:.1f}"
            )

        # 2. Confidence level
        confidence_ranks = {
            ConfidenceLevel.INSUFFICIENT: 0,
            ConfidenceLevel.LOW: 1,
            ConfidenceLevel.MEDIUM: 2,
            ConfidenceLevel.HIGH: 3,
        }
        res_conf_str = str(getattr(result, "confidence", ConfidenceLevel.INSUFFICIENT.value)).upper()
        try:
            res_conf = ConfidenceLevel(res_conf_str)
        except ValueError:
            res_conf = ConfidenceLevel.INSUFFICIENT

        if confidence_ranks.get(res_conf, 0) < confidence_ranks.get(self.min_confidence, 2):
            violations.append(
                f"Confidence '{res_conf.value}' is below required policy level '{self.min_confidence.value}'"
            )

        # 3. Decision sufficiency requirement
        if self.require_decision_sufficiency and not getattr(result, "decision_sufficiency", False):
            violations.append("Decision sufficiency is False (required evidence domains missing)")

        # 4. P0 blockers
        if self.block_on_p0_risks:
            has_blocking = getattr(result, "has_blocking", False)
            blockers = []
            if hasattr(result, "decision_risk_synthesis") and result.decision_risk_synthesis:
                blockers = result.decision_risk_synthesis.production_blockers

            if has_blocking or blockers:
                count = len(blockers) if blockers else 1
                violations.append(f"Pipeline contains {count} blocking P0 production risk(s)")

        # 5. Configuration drift
        if self.block_on_configuration_drift:
            alignment = getattr(result, "alignment_analysis", None)
            if alignment and getattr(alignment, "has_blocking_drift", False):
                violations.append("Pipeline exhibits blocking configuration drift across contract/code/runtime")

        # 6. Final Decision compatibility
        final_dec = str(getattr(result, "final_decision", "UNKNOWN")).upper()
        if final_dec == FinalDecisionStatus.NOT_PRODUCTION_READY.value:
            violations.append(f"Synthesized release decision is {final_dec}")
        elif final_dec == FinalDecisionStatus.INSUFFICIENT_EVIDENCE.value:
            violations.append("Synthesized release decision is INSUFFICIENT_EVIDENCE")
        elif final_dec == FinalDecisionStatus.CONDITIONAL.value and not self.allow_conditional_go:
            violations.append(
                f"Synthesized decision is {final_dec}, which is not permitted under '{self.tier.value}' policy"
            )

        return len(violations) == 0, violations


class EnterprisePipelineTarget(BaseModel):
    """Specification for a single Databricks pipeline/job within an enterprise fleet."""

    model_config = ConfigDict(populate_by_name=True)

    id: str
    job_id: int | None = None
    pipeline_id: str | None = None
    contract_path: Path | None = None
    code_path: Path | None = None
    environment: str | None = None
    workspace: str | None = None


class FleetManifest(BaseModel):
    """Authoritative manifest describing an enterprise fleet validation run."""

    model_config = ConfigDict(populate_by_name=True)

    name: str
    environment: EnvironmentTier = EnvironmentTier.PRODUCTION
    workspace_host: str | None = None
    pipelines: list[EnterprisePipelineTarget] = Field(default_factory=list)


class CrossPipelineCollisionFinding(BaseModel):
    """Identified cross-pipeline conflict such as shared table write collisions."""

    model_config = ConfigDict(populate_by_name=True)

    status: CollisionStatus = CollisionStatus.CONFIRMED
    severity: Severity = Severity.CRITICAL
    target_resource: str
    conflicting_pipeline_ids: list[str]
    write_modes: dict[str, str] = Field(default_factory=dict)
    description: str
    recommendation: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "severity": self.severity.value,
            "target_resource": self.target_resource,
            "conflicting_pipeline_ids": self.conflicting_pipeline_ids,
            "write_modes": self.write_modes,
            "description": self.description,
            "recommendation": self.recommendation,
        }


class FleetSummaryMetrics(BaseModel):
    """Consolidated KPI metrics across the enterprise fleet."""

    model_config = ConfigDict(populate_by_name=True)

    total_pipelines: int = 0
    successful_validations: int = 0
    failed_validations: int = 0
    passed_policy: int = 0
    blocked_policy: int = 0
    fleet_quality_score: float = 0.0
    confidence_distribution: dict[str, int] = Field(default_factory=dict)
    decision_distribution: dict[str, int] = Field(default_factory=dict)
    total_blockers: int = 0
    total_p0_risks: int = 0
    total_p1_risks: int = 0
    total_p2_risks: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_pipelines": self.total_pipelines,
            "successful_validations": self.successful_validations,
            "failed_validations": self.failed_validations,
            "passed_policy": self.passed_policy,
            "blocked_policy": self.blocked_policy,
            "fleet_quality_score": round(self.fleet_quality_score, 1),
            "confidence_distribution": self.confidence_distribution,
            "decision_distribution": self.decision_distribution,
            "total_blockers": self.total_blockers,
            "total_p0_risks": self.total_p0_risks,
            "total_p1_risks": self.total_p1_risks,
            "total_p2_risks": self.total_p2_risks,
        }


class PipelineFleetExecution(BaseModel):
    """Detailed execution and policy audit for an individual pipeline in the fleet."""

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)

    pipeline_id: str
    target: EnterprisePipelineTarget
    success: bool
    policy_passed: bool
    policy_violations: list[str] = Field(default_factory=list)
    validation_result: Any | None = None
    error_message: str | None = None
    duration_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "pipeline_id": self.pipeline_id,
            "success": self.success,
            "policy_passed": self.policy_passed,
            "policy_violations": self.policy_violations,
            "duration_seconds": round(self.duration_seconds, 2),
            "error_message": self.error_message,
        }
        if self.validation_result and hasattr(self.validation_result, "to_dict"):
            d["validation_result"] = self.validation_result.to_dict()
        return d


class FleetValidationResult(BaseModel):
    """Top-level enterprise fleet validation assessment and gate decision."""

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)

    fleet_name: str
    environment: EnvironmentTier
    policy: EnterpriseEnvironmentPolicy
    summary: FleetSummaryMetrics
    pipeline_executions: dict[str, PipelineFleetExecution] = Field(default_factory=dict)
    collisions: list[CrossPipelineCollisionFinding] = Field(default_factory=list)
    policy_passed: bool = True
    duration_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "fleet_name": self.fleet_name,
            "environment": self.environment.value,
            "policy": {
                "tier": self.policy.tier.value,
                "min_quality_score": self.policy.min_quality_score,
                "min_confidence": self.policy.min_confidence.value,
                "require_decision_sufficiency": self.policy.require_decision_sufficiency,
                "allow_conditional_go": self.policy.allow_conditional_go,
                "block_on_p0_risks": self.policy.block_on_p0_risks,
                "block_on_configuration_drift": self.policy.block_on_configuration_drift,
            },
            "summary": self.summary.to_dict(),
            "policy_passed": self.policy_passed,
            "duration_seconds": round(self.duration_seconds, 2),
            "collisions": [c.to_dict() for c in self.collisions],
            "pipelines": {pid: exec_obj.to_dict() for pid, exec_obj in self.pipeline_executions.items()},
        }
