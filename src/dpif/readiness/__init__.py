"""DPIF CP-FINAL Production Readiness & Final Pipeline Decision module."""

from __future__ import annotations

from dpif.readiness.coverage import compute_comprehensive_coverage
from dpif.readiness.engine import evaluate_production_readiness
from dpif.readiness.models import (
    ComprehensiveEvidenceCoverage,
    CrossDomainRisk,
    DomainCoverage,
    EvidenceQualityTier,
    ExpectedVsImplementedVsActual,
    PrioritizedAction,
    ProductionReadinessAssessment,
    ProductionReadinessStatus,
    ReadinessPolicy,
)
from dpif.readiness.synthesizer import (
    compare_expected_implemented_actual,
    synthesize_cross_domain_risks,
)

__all__ = [
    "ComprehensiveEvidenceCoverage",
    "CrossDomainRisk",
    "DomainCoverage",
    "EvidenceQualityTier",
    "ExpectedVsImplementedVsActual",
    "PrioritizedAction",
    "ProductionReadinessAssessment",
    "ProductionReadinessStatus",
    "ReadinessPolicy",
    "compare_expected_implemented_actual",
    "compute_comprehensive_coverage",
    "evaluate_production_readiness",
    "synthesize_cross_domain_risks",
]
