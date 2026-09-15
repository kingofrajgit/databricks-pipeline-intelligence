"""Rerun, Idempotency, and Duplicate-Data Forensics Models (M5F).

Provides models and enums for assessing pipeline behavior under:
- Execution repetition and same-input reruns
- Incremental and overlapping inputs
- Partial pipeline failure and job retry
- Concurrent execution and shared output targets
- Late-arriving data and backfill / restatement
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

from dpif.models import CheckpointStatus, Severity
from dpif.models.implementation import EvidenceProvenanceKind


class RerunScenarioKind(StrEnum):
    """The 7 core execution rerun scenarios assessed in M5F."""

    SAME_INPUT = "SAME_INPUT"
    INCREMENTAL_INPUT = "INCREMENTAL_INPUT"
    OVERLAPPING_INPUT = "OVERLAPPING_INPUT"
    PARTIAL_FAILURE = "PARTIAL_FAILURE"
    JOB_RETRY = "JOB_RETRY"
    CONCURRENT_EXECUTION = "CONCURRENT_EXECUTION"
    LATE_ARRIVING_DATA = "LATE_ARRIVING_DATA"


class IdempotencyDimension(StrEnum):
    """The 7 observable dimensions of pipeline idempotency."""

    INPUT_IDEMPOTENCY = "INPUT_IDEMPOTENCY"
    TRANSFORMATION_IDEMPOTENCY = "TRANSFORMATION_IDEMPOTENCY"
    OUTPUT_WRITE_IDEMPOTENCY = "OUTPUT_WRITE_IDEMPOTENCY"
    RETRY_IDEMPOTENCY = "RETRY_IDEMPOTENCY"
    CONCURRENT_EXECUTION_SAFETY = "CONCURRENT_EXECUTION_SAFETY"
    PARTIAL_FAILURE_RECOVERY = "PARTIAL_FAILURE_RECOVERY"
    LATE_DATA_HANDLING = "LATE_DATA_HANDLING"


class RerunFinding(BaseModel):
    """A structured finding emitted during rerun and idempotency forensics."""

    finding_id: str
    rule_id: str
    category: str = "rerun_idempotency"
    dimension: IdempotencyDimension | None = None
    scenario: RerunScenarioKind | None = None
    severity: Severity = Severity.INFO
    status: CheckpointStatus = CheckpointStatus.PASS
    title: str
    description: str
    evidence: list[str] = Field(default_factory=list)
    observed: dict[str, Any] = Field(default_factory=dict)
    expected: dict[str, Any] = Field(default_factory=dict)
    recommendation: str = ""
    confidence: float = 0.0
    blocking: bool = False
    provenance: EvidenceProvenanceKind = EvidenceProvenanceKind.STATIC_CODE
    location: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "finding_id": self.finding_id,
            "rule_id": self.rule_id,
            "category": self.category,
            "dimension": self.dimension.value if self.dimension else None,
            "scenario": self.scenario.value if self.scenario else None,
            "severity": self.severity.value,
            "status": self.status.value,
            "title": self.title,
            "description": self.description,
            "evidence": self.evidence,
            "observed": self.observed,
            "expected": self.expected,
            "recommendation": self.recommendation,
            "confidence": self.confidence,
            "blocking": self.blocking,
            "provenance": self.provenance.value,
            "location": self.location,
        }


class ScenarioAssessment(BaseModel):
    """Forensic assessment of an individual rerun scenario."""

    scenario: RerunScenarioKind
    status: CheckpointStatus = CheckpointStatus.UNKNOWN
    severity: Severity = Severity.INFO
    risk_summary: str = ""
    findings: list[RerunFinding] = Field(default_factory=list)
    evidence: list[str] = Field(default_factory=list)
    observed: dict[str, Any] = Field(default_factory=dict)
    expected: dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario.value,
            "status": self.status.value,
            "severity": self.severity.value,
            "risk_summary": self.risk_summary,
            "findings": [f.to_dict() for f in self.findings],
            "evidence": self.evidence,
            "observed": self.observed,
            "expected": self.expected,
        }


class IdempotencyDimensionAssessment(BaseModel):
    """Forensic summary for a single idempotency dimension."""

    dimension: IdempotencyDimension
    status: CheckpointStatus = CheckpointStatus.UNKNOWN
    max_severity: Severity = Severity.INFO
    summary: str = ""
    findings: list[RerunFinding] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dimension": self.dimension.value,
            "status": self.status.value,
            "max_severity": self.max_severity.value,
            "summary": self.summary,
            "findings": [f.to_dict() for f in self.findings],
        }


class IdempotencyAssessment(BaseModel):
    """Holistic idempotency assessment aggregating all dimensions."""

    overall_status: CheckpointStatus = CheckpointStatus.UNKNOWN
    is_idempotent: bool | None = None
    summary: str = ""
    dimensions: dict[str, IdempotencyDimensionAssessment] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "overall_status": self.overall_status.value,
            "is_idempotent": self.is_idempotent,
            "summary": self.summary,
            "dimensions": {k: v.to_dict() for k, v in self.dimensions.items()},
        }


class DuplicateDataRiskAnalysis(BaseModel):
    """Assessment of potential duplicate record generation upon rerun or retry."""

    status: CheckpointStatus = CheckpointStatus.PASS
    risk_level: Severity = Severity.INFO
    summary: str = ""
    potential_duplicate_sources: list[str] = Field(default_factory=list)
    findings: list[RerunFinding] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "risk_level": self.risk_level.value,
            "summary": self.summary,
            "potential_duplicate_sources": self.potential_duplicate_sources,
            "findings": [f.to_dict() for f in self.findings],
        }


class DataLossRiskAnalysis(BaseModel):
    """Assessment of potential data loss risks (e.g. destructive overwrite)."""

    status: CheckpointStatus = CheckpointStatus.PASS
    risk_level: Severity = Severity.INFO
    summary: str = ""
    potential_data_loss_sources: list[str] = Field(default_factory=list)
    findings: list[RerunFinding] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "risk_level": self.risk_level.value,
            "summary": self.summary,
            "potential_data_loss_sources": self.potential_data_loss_sources,
            "findings": [f.to_dict() for f in self.findings],
        }


class RerunAnalysisResult(BaseModel):
    """Complete Rerun / Idempotency / Duplicate-Data Forensics report (M5F)."""

    pipeline_name: str
    overall_status: CheckpointStatus = CheckpointStatus.UNKNOWN
    scenarios: dict[str, ScenarioAssessment] = Field(default_factory=dict)
    idempotency: IdempotencyAssessment = Field(default_factory=IdempotencyAssessment)
    duplicate_risk: DuplicateDataRiskAnalysis = Field(default_factory=DuplicateDataRiskAnalysis)
    data_loss_risk: DataLossRiskAnalysis = Field(default_factory=DataLossRiskAnalysis)
    performance_risks: list[RerunFinding] = Field(default_factory=list)
    all_findings: list[RerunFinding] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "overall_status": self.overall_status.value,
            "scenarios": {k: v.to_dict() for k, v in self.scenarios.items()},
            "idempotency": self.idempotency.to_dict(),
            "duplicate_risk": self.duplicate_risk.to_dict(),
            "data_loss_risk": self.data_loss_risk.to_dict(),
            "performance_risks": [f.to_dict() for f in self.performance_risks],
            "all_findings": [f.to_dict() for f in self.all_findings],
        }
