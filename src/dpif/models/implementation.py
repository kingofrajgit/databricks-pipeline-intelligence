"""Implementation Forensics Data Models & Enums (M5E).

Provides models and enums for assessing developer implementation decisions across:
- Transformations & pre-shuffle optimization
- Partitioning & join strategy
- Cache & checkpoint lifecycle
- Resource & process lifecycle
- Exception safety & process completeness
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

from dpif.models import CheckpointStatus, Severity


class ImplementationDimension(StrEnum):
    """The 9 observable dimensions of developer implementation forensics."""

    TRANSFORMATION_QUALITY = "transformation_quality"
    SHUFFLE_OPTIMIZATION = "shuffle_optimization"
    PARTITIONING_QUALITY = "partitioning_quality"
    JOIN_STRATEGY = "join_strategy"
    CACHE_LIFECYCLE = "cache_lifecycle"
    CHECKPOINT_LIFECYCLE = "checkpoint_lifecycle"
    RESOURCE_LIFECYCLE = "resource_lifecycle"
    EXCEPTION_SAFETY = "exception_safety"
    IMPLEMENTATION_COMPLETENESS = "implementation_completeness"


class EvidenceProvenanceKind(StrEnum):
    """Origin/provenance of evidence backing implementation forensic conclusions."""

    STATIC_CODE = "STATIC_CODE"
    RUNTIME = "RUNTIME"
    HISTORICAL_RUN = "HISTORICAL_RUN"
    FIXTURE = "FIXTURE"
    CONTRACT = "CONTRACT"
    METADATA = "METADATA"


class ForensicFinding(BaseModel):
    """A single implementation forensic observation or finding."""

    finding_id: str
    rule_id: str
    dimension: ImplementationDimension
    title: str
    description: str
    status: CheckpointStatus
    severity: Severity
    observed: dict[str, Any] = Field(default_factory=dict)
    expected: dict[str, Any] = Field(default_factory=dict)
    evidence: list[str] = Field(default_factory=list)
    recommendation: str = ""
    confidence: float = 0.0
    provenance: EvidenceProvenanceKind = EvidenceProvenanceKind.STATIC_CODE
    location: str = ""
    blocking: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "finding_id": self.finding_id,
            "rule_id": self.rule_id,
            "dimension": self.dimension.value,
            "title": self.title,
            "description": self.description,
            "status": self.status.value,
            "severity": self.severity.value,
            "observed": self.observed,
            "expected": self.expected,
            "evidence": self.evidence,
            "recommendation": self.recommendation,
            "confidence": self.confidence,
            "provenance": self.provenance.value,
            "location": self.location,
            "blocking": self.blocking,
        }


class DimensionAssessment(BaseModel):
    """Forensic summary for a single implementation dimension."""

    dimension: ImplementationDimension
    status: CheckpointStatus
    findings_count: int = 0
    max_severity: Severity = Severity.INFO
    summary: str = ""
    findings: list[ForensicFinding] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dimension": self.dimension.value,
            "status": self.status.value,
            "findings_count": self.findings_count,
            "max_severity": self.max_severity.value,
            "summary": self.summary,
            "findings": [f.to_dict() for f in self.findings],
        }


class ImplementationForensicsResult(BaseModel):
    """Complete Developer Implementation Forensics assessment report (M5E)."""

    pipeline_name: str
    overall_status: CheckpointStatus = CheckpointStatus.PASS
    dimensions: dict[str, DimensionAssessment] = Field(default_factory=dict)
    all_findings: list[ForensicFinding] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "overall_status": self.overall_status.value,
            "dimensions": {k: v.to_dict() for k, v in self.dimensions.items()},
            "all_findings": [f.to_dict() for f in self.all_findings],
        }
