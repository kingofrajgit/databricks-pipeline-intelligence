"""Phase 5 regression tests: missing evidence must not become PASS (G1 + G2).

G1 (checkpoint engine):
- all findings UNKNOWN -> checkpoint UNKNOWN (was PASS before the fix)
- PASS finding -> PASS preserved
- WARN/FAIL precedence unchanged
- empty findings preserve existing skeleton/upgrade conventions

G2 (CP-023 SLA matrix):
- A: measured duration + SLA -> existing behavior
- B: measured duration + no SLA -> existing PASS preserved
- C: unmeasured duration + SLA -> existing behavior preserved per scope
- D: unmeasured duration + no SLA -> UNKNOWN (was PASS before the fix)
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dpif.checkpoints.definitions import cp023_sla
from dpif.checkpoints.engine import CheckpointEngine
from dpif.models import (
    Checkpoint,
    CheckpointStatus,
    EvidenceRecord,
    Finding,
    Severity,
    SLARules,
)


def _finding(status: CheckpointStatus, severity: Severity = Severity.INFO) -> Finding:
    ev = EvidenceRecord(
        rule_id="R-TEST", status=status, severity=severity, confidence=0.5
    )
    return Finding(
        finding_id="f-test",
        rule_id="R-TEST",
        name="test rule",
        category="runtime",
        status=status,
        severity=severity,
        pipeline_name="p",
        evidence=ev,
    )


def _rule_with(finding: Finding | None):
    rule = MagicMock()
    rule.rule_id = "R-TEST"
    rule.category = "runtime"
    rule.name = "test rule"
    rule.evaluate.return_value = finding
    return rule


def _run_engine(*findings: Finding | None) -> Checkpoint:
    eng = CheckpointEngine()
    eng._get_applicable_rules = lambda category: [_rule_with(f) for f in findings]  # noqa: SLF001
    cp = Checkpoint(
        checkpoint_id="CP-T",
        name="CP-T",
        category="runtime",
        status=CheckpointStatus.UNKNOWN,
        severity=Severity.INFO,
    )
    return eng.execute_checkpoint(cp, {"code_snippet": "x = 1"})


# ==============================================================================
# G1: engine UNKNOWN honesty
# ==============================================================================


def test_all_unknown_findings_stay_unknown():
    out = _run_engine(
        _finding(CheckpointStatus.UNKNOWN), _finding(CheckpointStatus.UNKNOWN)
    )
    assert out.status == CheckpointStatus.UNKNOWN


def test_single_unknown_finding_stays_unknown():
    out = _run_engine(_finding(CheckpointStatus.UNKNOWN))
    assert out.status == CheckpointStatus.UNKNOWN


def test_pass_finding_preserves_pass():
    out = _run_engine(_finding(CheckpointStatus.PASS))
    assert out.status == CheckpointStatus.PASS


def test_pass_plus_unknown_preserves_pass():
    out = _run_engine(
        _finding(CheckpointStatus.UNKNOWN), _finding(CheckpointStatus.PASS)
    )
    assert out.status == CheckpointStatus.PASS


def test_warn_precedence_unchanged():
    out = _run_engine(
        _finding(CheckpointStatus.PASS),
        _finding(CheckpointStatus.WARN, Severity.MEDIUM),
    )
    assert out.status == CheckpointStatus.WARN


def test_fail_precedence_unchanged():
    out = _run_engine(
        _finding(CheckpointStatus.WARN, Severity.MEDIUM),
        _finding(CheckpointStatus.FAIL, Severity.HIGH),
    )
    assert out.status == CheckpointStatus.FAIL


def test_empty_findings_preserve_unknown_skeleton():
    eng = CheckpointEngine()
    eng._get_applicable_rules = lambda category: []  # noqa: SLF001
    cp = Checkpoint(
        checkpoint_id="CP-T",
        name="CP-T",
        category="runtime",
        status=CheckpointStatus.UNKNOWN,
        severity=Severity.INFO,
        assumptions={"unknown-reason": "no runtime available"},
    )
    out = eng.execute_checkpoint(cp, {})
    assert out.status == CheckpointStatus.UNKNOWN


# ==============================================================================
# G2: CP-023 matrix
# ==============================================================================


def _runtime(seconds: float):
    from dpif.runtime.models import RuntimeRun

    return RuntimeRun(run_id="r1", duration_seconds=seconds)


def _sla_contract():
    from dpif.models import PipelineContract

    return PipelineContract.model_construct(sla=SLARules(max_runtime_minutes=60))


def test_cp023_measured_duration_with_sla_passes():
    out = cp023_sla(contract=_sla_contract(), runtime_data=_runtime(600.0))
    assert out.status == CheckpointStatus.PASS


def test_cp023_measured_duration_without_sla_passes():
    """CASE B: measured duration + no SLA is a VALID DEFAULT (preserved)."""
    out = cp023_sla(contract=None, runtime_data=_runtime(600.0))
    assert out.status == CheckpointStatus.PASS
    assert "no contractual SLA defined" in out.evidence.evidence[0]


def test_cp023_unmeasured_duration_with_sla_preserved():
    """CASE C: preserved per approved scope (existing SLA path untouched)."""
    out = cp023_sla(contract=_sla_contract(), runtime_data=_runtime(0.0))
    assert out.status == CheckpointStatus.PASS


def test_cp023_unmeasured_duration_without_sla_is_unknown():
    """CASE D: the G2 fix — was PASS with fabricated 'Runtime observed'."""
    out = cp023_sla(contract=None, runtime_data=_runtime(0.0))
    assert out.status == CheckpointStatus.UNKNOWN
    assert out.score == 0.0
    assert out.evidence.observed["actual_duration_seconds"] == 0.0
    assert not any(
        "Runtime observed;" in e for e in out.evidence.evidence
    ), "must not claim runtime was observed"


def test_cp023_missing_runtime_is_unknown():
    out = cp023_sla(contract=None, runtime_data=None)
    assert out.status == CheckpointStatus.UNKNOWN


def test_cp023_sla_violation_still_fails():
    out = cp023_sla(contract=_sla_contract(), runtime_data=_runtime(7200.0))
    assert out.status == CheckpointStatus.FAIL
