"""Evidence coverage, confidence, and decision sufficiency models for DPIF (M5H).

Provides structured modeling for:
- Evidence coverage across all 16 pipeline domains
- Deterministic, explainable confidence evaluation
- Decision sufficiency determination with required evidence identification
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from dpif.models.implementation import EvidenceProvenanceKind


class ConfidenceLevel(StrEnum):
    """Deterministic, explainable confidence tier for evidence and conclusions."""

    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INSUFFICIENT = "INSUFFICIENT"


class EvidenceQuality(StrEnum):
    """Quality classification of available evidence."""

    STRONG = "STRONG"
    MODERATE = "MODERATE"
    WEAK = "WEAK"
    INSUFFICIENT = "INSUFFICIENT"


class EvidenceFreshness(StrEnum):
    """Temporal freshness classification of available evidence."""

    CURRENT = "CURRENT"
    HISTORICAL = "HISTORICAL"
    STALE = "STALE"
    STATIC = "STATIC"
    UNKNOWN = "UNKNOWN"


class DomainEvidenceCoverage(BaseModel):
    """Detailed evidence coverage and sufficiency assessment for a single pipeline domain."""

    model_config = ConfigDict(populate_by_name=True)

    domain: str
    evidence_expected: list[str] = Field(default_factory=list)
    evidence_available: list[str] = Field(default_factory=list)
    evidence_unavailable: list[str] = Field(default_factory=list)
    provenance: list[EvidenceProvenanceKind] = Field(default_factory=list)
    freshness: EvidenceFreshness = EvidenceFreshness.UNKNOWN
    quality: EvidenceQuality = EvidenceQuality.INSUFFICIENT
    completeness_score: float = Field(default=0.0, ge=0.0, le=1.0)
    confidence: ConfidenceLevel = ConfidenceLevel.INSUFFICIENT
    decision_sufficient: bool = False
    required_evidence_for_sufficiency: list[str] = Field(default_factory=list)
    summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "domain": self.domain,
            "evidence_expected": self.evidence_expected,
            "evidence_available": self.evidence_available,
            "evidence_unavailable": self.evidence_unavailable,
            "provenance": [
                p.value if hasattr(p, "value") else str(p) for p in self.provenance
            ],
            "freshness": self.freshness.value,
            "quality": self.quality.value,
            "completeness_score": round(self.completeness_score, 2),
            "confidence": self.confidence.value,
            "decision_sufficient": self.decision_sufficient,
            "required_evidence_for_sufficiency": self.required_evidence_for_sufficiency,
            "summary": self.summary,
        }


class DecisionSufficiencyRecord(BaseModel):
    """Evaluation of whether evidence is sufficient to make a specific key pipeline decision."""

    model_config = ConfigDict(populate_by_name=True)

    decision_name: str
    domain: str
    decision_status: str  # PASS, WARN, FAIL, UNKNOWN
    is_sufficient: bool = False
    confidence: ConfidenceLevel = ConfidenceLevel.INSUFFICIENT
    supporting_evidence: list[str] = Field(default_factory=list)
    missing_evidence: list[str] = Field(default_factory=list)
    required_evidence: list[str] = Field(default_factory=list)
    rationale: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision_name": self.decision_name,
            "domain": self.domain,
            "decision_status": self.decision_status,
            "is_sufficient": self.is_sufficient,
            "confidence": self.confidence.value,
            "supporting_evidence": self.supporting_evidence,
            "missing_evidence": self.missing_evidence,
            "required_evidence": self.required_evidence,
            "rationale": self.rationale,
        }


class EvidenceSufficiencyAssessment(BaseModel):
    """Pipeline-level evidence coverage and decision sufficiency assessment."""

    model_config = ConfigDict(populate_by_name=True)

    overall_confidence: ConfidenceLevel = ConfidenceLevel.INSUFFICIENT
    overall_decision_sufficiency: bool = False
    domains_evaluated: int = 0
    domains_sufficient: int = 0
    domains_insufficient: int = 0
    coverage_score: float = Field(default=0.0, ge=0.0, le=100.0)
    domain_coverages: dict[str, DomainEvidenceCoverage] = Field(default_factory=dict)
    decisions: list[DecisionSufficiencyRecord] = Field(default_factory=list)
    critical_missing_evidence: list[str] = Field(default_factory=list)
    actionable_recommendations: list[str] = Field(default_factory=list)
    # Phase 9 (P9-5): per-source/per-operation volume attribution coverage
    # as produced by flow attribution (node counts per evidence nature, never
    # invented volumes). Informational only: no scoring, confidence, or
    # sufficiency computation reads this field.
    volume_attribution: dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "overall_confidence": self.overall_confidence.value,
            "overall_decision_sufficiency": self.overall_decision_sufficiency,
            "domains_evaluated": self.domains_evaluated,
            "domains_sufficient": self.domains_sufficient,
            "domains_insufficient": self.domains_insufficient,
            "coverage_score": round(self.coverage_score, 1),
            "domain_coverages": {
                k: v.to_dict() for k, v in self.domain_coverages.items()
            },
            "decisions": [d.to_dict() for d in self.decisions],
            "critical_missing_evidence": self.critical_missing_evidence,
            "actionable_recommendations": self.actionable_recommendations,
            "volume_attribution": self.volume_attribution,
        }
