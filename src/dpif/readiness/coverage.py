"""Comprehensive evidence coverage aggregation across all DPIF domains.

Exposes domain-level coverage and evidence tiers without hiding domain gaps.
"""

from __future__ import annotations

from dpif.models import AnalysisMethod, Checkpoint, CheckpointStatus
from dpif.readiness.models import (
    ComprehensiveEvidenceCoverage,
    DomainCoverage,
    EvidenceQualityTier,
)

# Standard category to domain mapping
CATEGORY_DOMAIN_MAP: dict[str, str] = {
    "source": "Source",
    "data": "Data",
    "code": "Code",
    "sql": "SQL",
    "cluster": "Cluster",
    "job": "Job",
    "pipeline": "Pipeline",
    "performance": "Performance",
    "runtime": "Performance",
    "scalability": "Scalability",
    "security": "Security",
    "reliability": "Reliability",
    "governance": "Governance",
    "data-quality": "Data Quality",
    "sla": "SLA",
    "cost": "Cost",
}


def _extract_provenance(cp: Checkpoint) -> list[str]:
    """Extract provenance labels from checkpoint evidence and findings."""
    provs: list[str] = []
    if cp.evidence:
        if cp.evidence.evidence:
            for e in cp.evidence.evidence:
                if "fixture" in str(e).lower():
                    provs.append("FIXTURE")
                elif "contract" in str(e).lower():
                    provs.append("CONTRACT")
                elif "runtime" in str(e).lower() or "telemetry" in str(e).lower():
                    provs.append("RUNTIME")
        m = getattr(cp.evidence, "method", None)
        if m is not None and hasattr(m, "value") and m != AnalysisMethod.UNAVAILABLE:
            provs.append(str(m.value).upper())
    for f in cp.findings:
        if f.evidence:
            if f.evidence.evidence:
                for e in f.evidence.evidence:
                    if "fixture" in str(e).lower():
                        provs.append("FIXTURE")
                    elif "contract" in str(e).lower():
                        provs.append("CONTRACT")
                    elif "runtime" in str(e).lower() or "telemetry" in str(e).lower():
                        provs.append("RUNTIME")
            m = getattr(f.evidence, "method", None)
            if m is not None and hasattr(m, "value") and m != AnalysisMethod.UNAVAILABLE:
                provs.append(str(m.value).upper())
    return provs or ["STATIC"]


def compute_comprehensive_coverage(
    checkpoints: list[Checkpoint] | dict[str, Checkpoint],
) -> ComprehensiveEvidenceCoverage:
    """Compute overall and per-domain evidence coverage."""
    cps = list(checkpoints.values()) if isinstance(checkpoints, dict) else list(checkpoints)

    by_domain: dict[str, list[Checkpoint]] = {}
    for cp in cps:
        domain = CATEGORY_DOMAIN_MAP.get(cp.category, cp.category.title())
        by_domain.setdefault(domain, []).append(cp)

    domain_coverages: dict[str, DomainCoverage] = {}
    total_req = 0
    total_eval = 0
    total_unk = 0
    total_na = 0

    for domain, d_cps in sorted(by_domain.items()):
        scoped = [c for c in d_cps if c.status != CheckpointStatus.NOT_APPLICABLE]
        na_count = len([c for c in d_cps if c.status == CheckpointStatus.NOT_APPLICABLE])
        d_total = len(scoped)
        d_unk = len([c for c in scoped if c.status == CheckpointStatus.UNKNOWN])
        d_eval = d_total - d_unk
        d_pct = round(100.0 * d_eval / d_total, 1) if d_total > 0 else 100.0

        prov_counts: dict[str, int] = {}
        for c in scoped:
            for p in _extract_provenance(c):
                prov_counts[p] = prov_counts.get(p, 0) + 1

        tier = EvidenceQualityTier.EVALUATED if d_unk == 0 else EvidenceQualityTier.UNKNOWN
        if d_eval == 0 and na_count > 0:
            tier = EvidenceQualityTier.NOT_APPLICABLE

        domain_coverages[domain] = DomainCoverage(
            domain=domain,
            total_checks=d_total,
            evaluated_checks=d_eval,
            unknown_checks=d_unk,
            not_applicable_checks=na_count,
            coverage_percentage=d_pct,
            provenance_breakdown=prov_counts,
            quality_tier=tier,
        )

        total_req += d_total
        total_eval += d_eval
        total_unk += d_unk
        total_na += na_count

    overall_pct = round(100.0 * total_eval / total_req, 1) if total_req > 0 else 0.0

    return ComprehensiveEvidenceCoverage(
        total_required=total_req,
        total_evaluated=total_eval,
        total_unknown=total_unk,
        total_not_applicable=total_na,
        coverage_percentage=overall_pct,
        domain_coverages=domain_coverages,
    )
