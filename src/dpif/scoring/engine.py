"""Scoring engine: category + overall scores with UNKNOWN and blocking rules.

- PASS=100, WARN=60, FAIL=0, UNKNOWN=0 (never silently PASS), NOT_APPLICABLE excluded.
- Overall = weighted mean of category means (default equal weights; caller may
  pass organization profile weights).
- Any blocking FAIL finding forces NOT_PRODUCTION_READY regardless of score.
"""

from __future__ import annotations

from dpif.models import Checkpoint, CheckpointStatus, Score

STATUS_POINTS = {
    CheckpointStatus.PASS: 100.0,
    CheckpointStatus.WARN: 60.0,
    CheckpointStatus.FAIL: 0.0,
    CheckpointStatus.UNKNOWN: 0.0,
    CheckpointStatus.NOT_APPLICABLE: None,
}

DEFAULT_WEIGHTS: dict[str, float] = {
    "source": 1.0,
    "data": 1.0,
    "code": 1.0,
    "cluster": 1.0,
    "job": 1.0,
    "pipeline": 1.0,
    "reliability": 1.0,
    "cost": 1.0,
    "security": 1.0,
    "governance": 1.0,
    "data-quality": 1.0,
    "sla": 1.0,
    "readiness": 1.0,
    "performance": 1.0,
    "scalability": 1.0,
}


def _points(cp: Checkpoint) -> float | None:
    return STATUS_POINTS[cp.status]


def score_checkpoints(
    checkpoints: list[Checkpoint] | dict[str, Checkpoint],
    weights: dict[str, float] | None = None,
) -> tuple[Score, bool]:
    """Return (Score, has_blocking_failure)."""
    cps = list(checkpoints.values()) if isinstance(checkpoints, dict) else list(checkpoints)
    weights = weights or DEFAULT_WEIGHTS
    by_cat: dict[str, list[float]] = {}
    for cp in cps:
        pts = _points(cp)
        if pts is None:
            continue
        by_cat.setdefault(cp.category, []).append(pts)
    categories = {cat: (sum(v) / len(v) if v else 0.0) for cat, v in by_cat.items()}
    num = sum(categories.get(cat, 0.0) * weights.get(cat, 1.0) for cat in categories)
    den = sum(weights.get(cat, 1.0) for cat in categories)
    overall = round(num / den, 1) if den else 0.0
    blocking = any(
        f.blocking and f.status == CheckpointStatus.FAIL for cp in cps for f in cp.findings
    )
    if overall >= 90:
        status = "EXCELLENT"
    elif overall >= 80:
        status = "GOOD"
    elif overall >= 70:
        status = "NEEDS_IMPROVEMENT"
    elif overall >= 50:
        status = "HIGH_RISK"
    else:
        status = "CRITICAL"
    if blocking:
        status = "CRITICAL"
    rounded = {k: round(v, 1) for k, v in categories.items()}
    return Score(overall=overall, categories=rounded, status=status), blocking


def readiness_label(
    score: Score, has_blocking: bool, has_fail: bool, has_unknown_or_warn: bool
) -> str:
    if has_blocking or has_fail:
        return "NOT_PRODUCTION_READY"
    if score.status in ("EXCELLENT",) and not has_unknown_or_warn:
        return "PRODUCTION_READY"
    return "NOT_PRODUCTION_READY"
