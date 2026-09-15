"""Domain models for Phase M5I: Decision & Risk Synthesis Engine.

Orchestrates and synthesizes existing intelligence from:
- 24 Checkpoints (CP-001..CP-024)
- M5E Developer Implementation Forensics
- M5F Rerun & Idempotency Forensics
- M5G Three-Layer Alignment
- M5H Evidence Coverage & Decision Sufficiency
- CP-FINAL Production Readiness Assessment

Provides strongly typed, JSON-serializable structures for:
- Consolidated risk categories and severities
- Synthesized risk records with full provenance
- Production blockers and resolution requirements
- Deterministic multi-step causal risk chains
- Synthesized, prioritized remediation actions
- Domain-level synthesis summaries
- Final explainable engineering decision
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from dpif.models import Severity
from dpif.models.sufficiency import ConfidenceLevel


class FinalDecisionStatus(StrEnum):
    """Supported CP-FINAL / M5I production release decision statuses."""

    PRODUCTION_READY = "PRODUCTION_READY"
    NOT_PRODUCTION_READY = "NOT_PRODUCTION_READY"
    CONDITIONAL = "CONDITIONAL"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


class RiskCategory(StrEnum):
    """Consolidated risk classification categories across all pipeline domains."""

    DATA_QUALITY = "DATA_QUALITY"
    DATA_LOSS = "DATA_LOSS"
    DUPLICATE_DATA = "DUPLICATE_DATA"
    PERFORMANCE = "PERFORMANCE"
    SCALABILITY = "SCALABILITY"
    RELIABILITY = "RELIABILITY"
    IDEMPOTENCY = "IDEMPOTENCY"
    CONCURRENCY = "CONCURRENCY"
    CONFIGURATION_DRIFT = "CONFIGURATION_DRIFT"
    SECURITY = "SECURITY"
    GOVERNANCE = "GOVERNANCE"
    COST = "COST"
    SLA = "SLA"
    IMPLEMENTATION = "IMPLEMENTATION"
    OPERATIONAL = "OPERATIONAL"
    EVIDENCE = "EVIDENCE"


class SynthesizedRisk(BaseModel):
    """Consolidated risk synthesized from one or more analyzer findings or checks."""

    model_config = ConfigDict(populate_by_name=True)

    risk_id: str
    category: RiskCategory
    severity: Severity  # CRITICAL, HIGH, MEDIUM, LOW, INFO
    title: str
    description: str
    source_findings: list[str] = Field(default_factory=list)
    affected_domains: list[str] = Field(default_factory=list)
    blocking: bool = False
    consequence: str = ""
    confidence: ConfidenceLevel = ConfidenceLevel.MEDIUM
    evidence_provenance: list[str] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "risk_id": self.risk_id,
            "category": self.category.value,
            "severity": self.severity.value,
            "title": self.title,
            "description": self.description,
            "source_findings": self.source_findings,
            "affected_domains": self.affected_domains,
            "blocking": self.blocking,
            "consequence": self.consequence,
            "confidence": self.confidence.value,
            "evidence_provenance": self.evidence_provenance,
        }


class ProductionBlocker(BaseModel):
    """Hard blocker that prevents production deployment approval."""

    model_config = ConfigDict(populate_by_name=True)

    blocker_id: str
    title: str
    description: str
    source: str
    category: RiskCategory
    severity: Severity = Severity.CRITICAL
    resolution_requirement: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "blocker_id": self.blocker_id,
            "title": self.title,
            "description": self.description,
            "source": self.source,
            "category": self.category.value,
            "severity": self.severity.value,
            "resolution_requirement": self.resolution_requirement,
        }


class RiskChain(BaseModel):
    """Deterministic, evidence-backed causal chain linking root causes to production impacts."""

    model_config = ConfigDict(populate_by_name=True)

    chain_id: str
    title: str
    steps: list[str] = Field(default_factory=list)
    root_cause: str
    ultimate_impact: str
    source_findings: list[str] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "chain_id": self.chain_id,
            "title": self.title,
            "steps": self.steps,
            "root_cause": self.root_cause,
            "ultimate_impact": self.ultimate_impact,
            "source_findings": self.source_findings,
        }


class SynthesizedRemediation(BaseModel):
    """Aggregated, actionable remediation prioritized for engineering intervention."""

    model_config = ConfigDict(populate_by_name=True)

    remediation_id: str
    priority: str  # P0, P1, P2, P3
    severity: Severity
    category: RiskCategory
    title: str
    description: str
    source_findings: list[str] = Field(default_factory=list)
    required_evidence: list[str] = Field(default_factory=list)
    expected_outcome: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "remediation_id": self.remediation_id,
            "priority": self.priority,
            "severity": self.severity.value,
            "category": self.category.value,
            "title": self.title,
            "description": self.description,
            "source_findings": self.source_findings,
            "required_evidence": self.required_evidence,
            "expected_outcome": self.expected_outcome,
        }


class DomainSynthesisSummary(BaseModel):
    """Consolidated summary for an individual pipeline domain."""

    model_config = ConfigDict(populate_by_name=True)

    domain: str
    status: str  # PASS, WARN, FAIL, UNKNOWN
    severity: Severity = Severity.INFO
    confidence: ConfidenceLevel = ConfidenceLevel.HIGH
    decision_sufficiency: bool = True
    key_risks: list[str] = Field(default_factory=list)
    blockers: list[str] = Field(default_factory=list)
    missing_evidence: list[str] = Field(default_factory=list)
    recommended_actions: list[str] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "domain": self.domain,
            "status": self.status,
            "severity": self.severity.value,
            "confidence": self.confidence.value,
            "decision_sufficiency": self.decision_sufficiency,
            "key_risks": self.key_risks,
            "blockers": self.blockers,
            "missing_evidence": self.missing_evidence,
            "recommended_actions": self.recommended_actions,
        }


class DecisionRiskSynthesisResult(BaseModel):
    """Top-level M5I synthesized engineering decision and risk profile."""

    model_config = ConfigDict(populate_by_name=True)

    final_decision: FinalDecisionStatus
    quality_score: float
    confidence: ConfidenceLevel
    decision_sufficiency: bool
    decision_explanation: list[str] = Field(default_factory=list)
    score_override_reason: str | None = None
    blockers: list[ProductionBlocker] = Field(default_factory=list)
    top_risks: list[SynthesizedRisk] = Field(default_factory=list)
    all_risks: list[SynthesizedRisk] = Field(default_factory=list)
    risk_chains: list[RiskChain] = Field(default_factory=list)
    missing_evidence: list[str] = Field(default_factory=list)
    remediations: list[SynthesizedRemediation] = Field(default_factory=list)
    domain_summaries: dict[str, DomainSynthesisSummary] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "final_decision": self.final_decision.value,
            "quality_score": round(self.quality_score, 1),
            "confidence": self.confidence.value,
            "decision_sufficiency": self.decision_sufficiency,
            "decision_explanation": self.decision_explanation,
            "score_override_reason": self.score_override_reason,
            "blockers": [b.to_dict() for b in self.blockers],
            "top_risks": [r.to_dict() for r in self.top_risks],
            "all_risks": [r.to_dict() for r in self.all_risks],
            "risk_chains": [c.to_dict() for c in self.risk_chains],
            "missing_evidence": self.missing_evidence,
            "remediations": [rem.to_dict() for rem in self.remediations],
            "domain_summaries": {k: v.to_dict() for k, v in self.domain_summaries.items()},
            "metadata": self.metadata,
        }
