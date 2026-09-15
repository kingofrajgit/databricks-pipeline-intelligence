"""Three-Layer Alignment & Configuration Drift Forensics Models (M5G).

Provides models and enums for assessing alignment and drift across:
- Layer 1: EXPECTED (Pipeline Contract)
- Layer 2: IMPLEMENTED (Code AST, SQL, M5E, M5F, Databricks Job/Cluster config)
- Layer 3: ACTUAL (Databricks Workspace Cluster, Runtime Run Telemetry, Historical Runs)
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

from dpif.models import CheckpointStatus, Severity
from dpif.models.implementation import EvidenceProvenanceKind


class AlignmentDimension(StrEnum):
    """The 9 observable dimensions of three-layer alignment and drift forensics."""

    COMPUTE_RUNTIME = "compute_runtime"
    CLUSTER_SIZING_SCALING = "cluster_sizing_scaling"
    JOB_WORKFLOW_CADENCE = "job_workflow_cadence"
    PROCESSING_STRATEGY = "processing_strategy"
    TARGET_STORAGE_FORMAT = "target_storage_format"
    PARTITIONING_LAYOUT = "partitioning_layout"
    SLA_EXECUTION_LIMITS = "sla_execution_limits"
    RELIABILITY_RETRY_POLICY = "reliability_retry_policy"
    WORKLOAD_VOLUME_BOUNDS = "workload_volume_bounds"


class LayerDivergenceKind(StrEnum):
    """Types of divergence across Expected, Implemented, and Actual layers."""

    EXPECTED_VS_IMPLEMENTED = "EXPECTED_VS_IMPLEMENTED"
    IMPLEMENTED_VS_ACTUAL = "IMPLEMENTED_VS_ACTUAL"
    EXPECTED_VS_ACTUAL = "EXPECTED_VS_ACTUAL"
    THREE_WAY_DIVERGENCE = "THREE_WAY_DIVERGENCE"


class DriftSeverity(StrEnum):
    """Severity classification of detected configuration or behavioral drift."""

    NONE = "NONE"
    WARN = "WARN"
    BLOCKING = "BLOCKING"


class AlignmentFinding(BaseModel):
    """A single alignment or drift forensic finding."""

    finding_id: str
    rule_id: str
    category: str = "three_layer_alignment"
    dimension: AlignmentDimension
    divergence_kind: LayerDivergenceKind
    title: str
    description: str
    status: CheckpointStatus = CheckpointStatus.PASS
    severity: Severity = Severity.INFO
    drift_severity: DriftSeverity = DriftSeverity.NONE
    expected: Any = None
    implemented: Any = None
    actual: Any = None
    evidence: list[str] = Field(default_factory=list)
    recommendation: str = ""
    confidence: float = 0.0
    blocking: bool = False
    provenance: EvidenceProvenanceKind = EvidenceProvenanceKind.CONTRACT
    location: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "finding_id": self.finding_id,
            "rule_id": self.rule_id,
            "category": self.category,
            "dimension": self.dimension.value,
            "divergence_kind": self.divergence_kind.value,
            "title": self.title,
            "description": self.description,
            "status": self.status.value,
            "severity": self.severity.value,
            "drift_severity": self.drift_severity.value,
            "expected": self.expected,
            "implemented": self.implemented,
            "actual": self.actual,
            "evidence": self.evidence,
            "recommendation": self.recommendation,
            "confidence": self.confidence,
            "blocking": self.blocking,
            "provenance": self.provenance.value,
            "location": self.location,
        }


class DimensionAlignmentAssessment(BaseModel):
    """Forensic alignment summary for a single dimension."""

    dimension: AlignmentDimension
    status: CheckpointStatus = CheckpointStatus.UNKNOWN
    drift_severity: DriftSeverity = DriftSeverity.NONE
    findings_count: int = 0
    findings: list[AlignmentFinding] = Field(default_factory=list)
    summary: str = ""
    expected_summary: str = ""
    implemented_summary: str = ""
    actual_summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "dimension": self.dimension.value,
            "status": self.status.value,
            "drift_severity": self.drift_severity.value,
            "findings_count": self.findings_count,
            "findings": [f.to_dict() for f in self.findings],
            "summary": self.summary,
            "expected_summary": self.expected_summary,
            "implemented_summary": self.implemented_summary,
            "actual_summary": self.actual_summary,
        }


class ThreeLayerAlignmentAssessment(BaseModel):
    """Complete Three-Layer Alignment & Configuration Drift Forensics report (M5G)."""

    pipeline_name: str
    overall_status: CheckpointStatus = CheckpointStatus.UNKNOWN
    drift_severity: DriftSeverity = DriftSeverity.NONE
    has_blocking_drift: bool = False
    total_drifts: int = 0
    unknown_layers_count: int = 0
    dimensions: dict[str, DimensionAlignmentAssessment] = Field(default_factory=dict)
    findings: list[AlignmentFinding] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "overall_status": self.overall_status.value,
            "drift_severity": self.drift_severity.value,
            "has_blocking_drift": self.has_blocking_drift,
            "total_drifts": self.total_drifts,
            "unknown_layers_count": self.unknown_layers_count,
            "dimensions": {k: v.to_dict() for k, v in self.dimensions.items()},
            "findings": [f.to_dict() for f in self.findings],
        }
