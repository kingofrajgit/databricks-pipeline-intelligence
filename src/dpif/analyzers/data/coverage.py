"""Evidence coverage: how much was actually validated (not a score)."""

from __future__ import annotations

from dpif.models import AnalysisMethod, Checkpoint, CheckpointStatus, EvidenceCoverage


def _tier(cp: Checkpoint) -> str:
    """Best evidence tier behind an evaluated checkpoint."""
    # AnalysisMethod has no RUNTIME member in offline phases; compare by value
    # so a future runtime method maps here without code changes.
    runtime = any(str(f.evidence.method.value) == "runtime" for f in cp.findings)
    if runtime:
        return "runtime"
    for f in cp.findings:
        obs = f.evidence.observed or {}
        if obs.get("data_context_gb") is not None or "context" in (f.assumptions or {}):
            return "context"
    if cp.findings or cp.evidence.method != AnalysisMethod.UNAVAILABLE:
        return "static"
    return "static"


def compute_coverage(checkpoints: list[Checkpoint] | dict[str, Checkpoint]) -> EvidenceCoverage:
    """UNKNOWN is reported, never treated as FAIL here."""
    cps = list(checkpoints.values()) if isinstance(checkpoints, dict) else list(checkpoints)
    scoped = [c for c in cps if c.status != CheckpointStatus.NOT_APPLICABLE]
    total = len(scoped)
    unknown = len([c for c in scoped if c.status == CheckpointStatus.UNKNOWN])
    evaluated = total - unknown
    pct = round(100.0 * evaluated / total, 1) if total else 0.0
    tiers = {"static": 0, "context": 0, "runtime": 0}
    for cp in scoped:
        if cp.status == CheckpointStatus.UNKNOWN:
            continue
        tiers[_tier(cp)] += 1
    return EvidenceCoverage(
        total_checks=total,
        evaluated_checks=evaluated,
        unknown_checks=unknown,
        coverage_percentage=pct,
        static_checks=tiers["static"],
        context_checks=tiers["context"],
        runtime_checks=tiers["runtime"],
    )
