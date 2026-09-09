"""Scoring: PASS/WARN/FAIL/UNKNOWN handling, blocking override."""

from __future__ import annotations

from dpif.models import (
    Checkpoint,
    CheckpointStatus,
    EvidenceRecord,
    Finding,
    Score,
    Severity,
)
from dpif.scoring.engine import readiness_label, score_checkpoints


def _cp(cid, cat, status):
    return Checkpoint(checkpoint_id=cid, name=cid, category=cat, status=status)


def _blocking_fail_cp():
    ev = EvidenceRecord(
        rule_id="CODE-PYSPARK-001",
        status=CheckpointStatus.FAIL,
        severity=Severity.CRITICAL,
        confidence=1.0,
    )
    f = Finding(
        finding_id="f",
        rule_id="CODE-PYSPARK-001",
        name="n",
        category="code",
        status=CheckpointStatus.FAIL,
        severity=Severity.CRITICAL,
        pipeline_name="p",
        evidence=ev,
        blocking=True,
    )
    cp = _cp("CP-004", "code", CheckpointStatus.FAIL)
    cp.severity = Severity.CRITICAL
    cp.findings = [f]
    return cp


def _score(status, **kw):
    return Score(overall=95, categories={}, status=status, **kw)


def test_all_pass_scores_100_excellent():
    cps = [
        _cp("CP-001", "source", CheckpointStatus.PASS),
        _cp("CP-004", "code", CheckpointStatus.PASS),
    ]
    score, blocking = score_checkpoints(cps)
    assert score.overall == 100.0
    assert score.status == "EXCELLENT"
    assert blocking is False


def test_unknown_scores_zero_not_pass():
    cps = [
        _cp("CP-001", "source", CheckpointStatus.PASS),
        _cp("CP-019", "cost", CheckpointStatus.UNKNOWN),
    ]
    score, _ = score_checkpoints(cps)
    assert score.categories["cost"] == 0.0
    assert score.overall == 50.0


def test_fail_scores_zero():
    score, _ = score_checkpoints([_cp("CP-004", "code", CheckpointStatus.FAIL)])
    assert score.overall == 0.0


def test_warn_scores_sixty():
    score, _ = score_checkpoints([_cp("CP-012", "pipeline", CheckpointStatus.WARN)])
    assert score.overall == 60.0


def test_blocking_failure_forces_critical():
    cps = [_blocking_fail_cp(), _cp("CP-001", "source", CheckpointStatus.PASS)]
    score, blocking = score_checkpoints(cps)
    assert blocking is True
    assert score.status == "CRITICAL"


def test_readiness_labels():
    assert readiness_label(_score("EXCELLENT"), False, False, False) == "PRODUCTION_READY"
    assert readiness_label(_score("EXCELLENT"), True, False, False) == "NOT_PRODUCTION_READY"
    assert readiness_label(_score("EXCELLENT"), False, True, False) == "NOT_PRODUCTION_READY"
    assert (
        readiness_label(_score("NEEDS_IMPROVEMENT"), False, False, True) == "NOT_PRODUCTION_READY"
    )
