"""Strongly-typed domain models for DPIF CP-FINAL Production Readiness.

Preserves the strict separation between:
1. Quality Score (0-100)
2. Evidence Coverage (0-100%)
3. Production Readiness (Decision Status)
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from dpif.models import Finding


class ProductionReadinessStatus(StrEnum):
    """Supported CP-FINAL production readiness decision statuses."""

    PRODUCTION_READY = "PRODUCTION_READY"
    PRODUCTION_READY_WITH_WARNINGS = "PRODUCTION_READY_WITH_WARNINGS"
    NOT_PRODUCTION_READY = "NOT_PRODUCTION_READY"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


class EvidenceQualityTier(StrEnum):
    """Evidence classification tiers."""

    AVAILABLE = "AVAILABLE"
    EVALUATED = "EVALUATED"
    UNKNOWN = "UNKNOWN"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class ReadinessPolicy(BaseModel):
    """Configurable gates for determining production readiness."""

    model_config = ConfigDict(populate_by_name=True)

    policy_version: str = "production-readiness-v1"
    minimum_quality_score: float = Field(default=80.0, ge=0.0, le=100.0)
    minimum_evidence_coverage: float = Field(default=70.0, ge=0.0, le=100.0)

    # Specific evidence requirements
    require_runtime_evidence: bool = False
    require_scalability_evidence: bool = False
    require_live_databricks_evidence: bool = False

    # Finding blocking gates
    block_on_critical: bool = True
    block_on_high: bool = False
    block_on_sla_failure: bool = True
    block_on_security_failure: bool = True
    block_on_reliability_failure: bool = True
    block_on_blocking_findings: bool = True

    # Warning tolerance
    max_tolerated_warnings: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


class DomainCoverage(BaseModel):
    """Detailed evidence coverage for a single pipeline domain."""

    model_config = ConfigDict(populate_by_name=True)

    domain: str
    total_checks: int = 0
    evaluated_checks: int = 0
    unknown_checks: int = 0
    not_applicable_checks: int = 0
    coverage_percentage: float = 0.0
    provenance_breakdown: dict[str, int] = Field(default_factory=dict)
    quality_tier: EvidenceQualityTier = EvidenceQualityTier.AVAILABLE

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


class ComprehensiveEvidenceCoverage(BaseModel):
    """Aggregated evidence coverage with domain-level breakdown."""

    model_config = ConfigDict(populate_by_name=True)

    total_required: int = 0
    total_evaluated: int = 0
    total_unknown: int = 0
    total_not_applicable: int = 0
    coverage_percentage: float = 0.0
    domain_coverages: dict[str, DomainCoverage] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_required": self.total_required,
            "total_evaluated": self.total_evaluated,
            "total_unknown": self.total_unknown,
            "total_not_applicable": self.total_not_applicable,
            "coverage_percentage": self.coverage_percentage,
            "domain_coverages": {k: v.to_dict() for k, v in self.domain_coverages.items()},
        }


class ExpectedVsImplementedVsActual(BaseModel):
    """Configuration consistency evaluation comparing Expected, Implemented, and Actual."""

    model_config = ConfigDict(populate_by_name=True)

    parameter: str
    domain: str
    expected: Any = None
    implemented: Any = None
    actual: Any = None
    is_drift: bool = False
    drift_severity: str = "NONE"  # NONE, WARN, BLOCKING
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


class CrossDomainRisk(BaseModel):
    """Consolidated cross-domain vulnerability bridging multiple intelligence domains."""

    model_config = ConfigDict(populate_by_name=True)

    risk_id: str
    title: str
    severity: str  # CRITICAL, HIGH, MEDIUM, LOW
    contributing_domains: list[str]
    evidence_sources: list[str]
    description: str
    recommendation: str
    confidence: float = 0.85

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


class PrioritizedAction(BaseModel):
    """Prioritized actionable remediation recommendation."""

    model_config = ConfigDict(populate_by_name=True)

    priority: str  # P0 (blocking), P1 (high risk), P2 (warning), P3 (advisory)
    rule_id: str
    checkpoint_id: str
    title: str
    description: str
    recommendation: str

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


class ProductionReadinessAssessment(BaseModel):
    """Comprehensive production-readiness decision object.

    Contains full audit trail, decision rationale, coverage, and prioritized actions.
    """

    model_config = ConfigDict(populate_by_name=True)

    status: ProductionReadinessStatus
    quality_score: float
    evidence_coverage: float
    comprehensive_coverage: ComprehensiveEvidenceCoverage

    decision_reasons: list[str] = Field(default_factory=list)
    required_actions: list[PrioritizedAction] = Field(default_factory=list)

    blocking_findings: list[Finding] = Field(default_factory=list)
    critical_findings: list[Finding] = Field(default_factory=list)
    high_findings: list[Finding] = Field(default_factory=list)
    warning_findings: list[Finding] = Field(default_factory=list)

    warning_count: int = 0
    unknown_count: int = 0

    applicable_checkpoint_count: int = 0
    passed_checkpoint_count: int = 0
    failed_checkpoint_count: int = 0
    unknown_checkpoint_count: int = 0

    domain_assessments: dict[str, str] = Field(default_factory=dict)
    config_comparisons: list[ExpectedVsImplementedVsActual] = Field(default_factory=list)
    cross_domain_risks: list[CrossDomainRisk] = Field(default_factory=list)

    runtime_summary: dict[str, Any] = Field(default_factory=dict)
    scalability_summary: dict[str, Any] = Field(default_factory=dict)
    sla_summary: dict[str, Any] = Field(default_factory=dict)
    security_summary: dict[str, Any] = Field(default_factory=dict)
    reliability_summary: dict[str, Any] = Field(default_factory=dict)

    policy_version: str = "production-readiness-v1"
    framework_version: str = "1.0.0"
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    connector_mode: str = "offline"

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "quality_score": self.quality_score,
            "evidence_coverage": self.evidence_coverage,
            "comprehensive_coverage": self.comprehensive_coverage.to_dict(),
            "decision_reasons": self.decision_reasons,
            "required_actions": [a.to_dict() for a in self.required_actions],
            "blocking_findings": [f.to_dict() for f in self.blocking_findings],
            "critical_findings": [f.to_dict() for f in self.critical_findings],
            "high_findings": [f.to_dict() for f in self.high_findings],
            "warning_count": self.warning_count,
            "unknown_count": self.unknown_count,
            "applicable_checkpoint_count": self.applicable_checkpoint_count,
            "passed_checkpoint_count": self.passed_checkpoint_count,
            "failed_checkpoint_count": self.failed_checkpoint_count,
            "unknown_checkpoint_count": self.unknown_checkpoint_count,
            "domain_assessments": self.domain_assessments,
            "config_comparisons": [c.to_dict() for c in self.config_comparisons],
            "cross_domain_risks": [r.to_dict() for r in self.cross_domain_risks],
            "runtime_summary": self.runtime_summary,
            "scalability_summary": self.scalability_summary,
            "sla_summary": self.sla_summary,
            "security_summary": self.security_summary,
            "reliability_summary": self.reliability_summary,
            "policy_version": self.policy_version,
            "framework_version": self.framework_version,
            "generated_at": self.generated_at.isoformat(),
            "connector_mode": self.connector_mode,
        }
