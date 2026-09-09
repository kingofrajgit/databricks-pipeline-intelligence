"""Checkpoint engine: dependency FAIL/UNKNOWN propagation, ordering, scoring."""

from __future__ import annotations

from dpif.checkpoints.engine import CheckpointEngine
from dpif.models import Checkpoint, CheckpointStatus, Severity


def _cp(cid, status=CheckpointStatus.PASS, depends_on=None, category="misc"):
    return Checkpoint(
        checkpoint_id=cid,
        name=cid,
        category=category,
        status=status,
        severity=Severity.INFO,
        depends_on=depends_on or [],
    )


def test_fail_dependency_yields_unknown():
    eng = CheckpointEngine()
    a = _cp("CP-A", CheckpointStatus.FAIL)
    b = _cp("CP-B", depends_on=["CP-A"])
    eng.register_checkpoint(a)
    out = eng.execute_checkpoint(b, {})
    assert out.status == CheckpointStatus.UNKNOWN
    assert "unknown-reason" in out.assumptions


def test_unknown_dependency_yields_unknown():
    eng = CheckpointEngine()
    a = _cp("CP-A", CheckpointStatus.UNKNOWN)
    b = _cp("CP-B", depends_on=["CP-A"])
    eng.register_checkpoint(a)
    assert eng.execute_checkpoint(b, {}).status == CheckpointStatus.UNKNOWN


def test_missing_dependency_yields_unknown():
    eng = CheckpointEngine()
    b = _cp("CP-B", depends_on=["CP-NOPE"])
    assert eng.execute_checkpoint(b, {}).status == CheckpointStatus.UNKNOWN


def test_pass_dependency_allows_execution():
    eng = CheckpointEngine()
    a = _cp("CP-A", CheckpointStatus.PASS)
    b = _cp("CP-B", depends_on=["CP-A"], category="code")
    eng.register_checkpoint(a)
    out = eng.execute_checkpoint(b, {"code_snippet": "x = 1"})
    assert out.status == CheckpointStatus.PASS


def test_run_all_respects_dependency_order():
    eng = CheckpointEngine()
    a = _cp("CP-A", CheckpointStatus.PASS)
    b = _cp("CP-B", depends_on=["CP-A"], category="code")
    out = eng.run_all_checkpoints([b, a], {"code_snippet": "x = 1"})
    assert set(out) == {"CP-A", "CP-B"}
    assert out["CP-B"].status == CheckpointStatus.PASS


def test_run_all_failed_dep_marks_dependent_unknown():
    eng = CheckpointEngine()
    a = _cp("CP-A", CheckpointStatus.FAIL)
    b = _cp("CP-B", depends_on=["CP-A"])
    out = eng.run_all_checkpoints([a, b], {})
    assert out["CP-B"].status == CheckpointStatus.UNKNOWN


def test_get_score_unknown_is_zero_not_pass():
    eng = CheckpointEngine()
    assert eng.get_score(_cp("X", CheckpointStatus.UNKNOWN)) == 0.0
    assert eng.get_score(_cp("X", CheckpointStatus.PASS)) == 1.0


def test_get_overall_status_blocking_overrides():
    from dpif.models import EvidenceRecord, Finding

    eng = CheckpointEngine()
    ev = EvidenceRecord(
        rule_id="R", status=CheckpointStatus.FAIL, severity=Severity.CRITICAL, confidence=1.0
    )
    f = Finding(
        finding_id="f",
        rule_id="R",
        name="n",
        category="code",
        status=CheckpointStatus.FAIL,
        severity=Severity.CRITICAL,
        pipeline_name="p",
        evidence=ev,
        blocking=True,
    )
    cp = _cp("CP-X", CheckpointStatus.FAIL)
    cp.severity = Severity.HIGH
    cp.findings = [f]
    assert eng.get_overall_status({"CP-X": cp}) == "CRITICAL"


def test_get_overall_status_unknown_present():
    eng = CheckpointEngine()
    cps = {"A": _cp("A", CheckpointStatus.PASS), "B": _cp("B", CheckpointStatus.UNKNOWN)}
    assert eng.get_overall_status(cps) == "UNKNOWN"
