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

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from dpif.checkpoints.definitions import (
    cp008_performance,
    cp009_cluster,
    cp010_scalability,
    cp011_job,
    cp012_incremental,
    cp013_error_handling,
    cp015_restartability,
    cp016_idempotency,
    cp021_governance,
    cp022_data_quality,
    cp023_sla,
)
from dpif.checkpoints.engine import CheckpointEngine
from dpif.models import (
    Checkpoint,
    CheckpointStatus,
    DataProfile,
    EvidenceRecord,
    Finding,
    PipelineContract,
    ReliabilityRules,
    Severity,
    SLARules,
    Source,
    SourceType,
    Target,
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


# ==============================================================================
# Phase 6 (B1): verdict-honesty completion — presence alone must not PASS.
# Absence/insufficient evidence -> UNKNOWN (never FAIL); genuine evidence
# keeps its existing PASS/FAIL/WARN behavior.
# ==============================================================================


def _bare_contract(**overrides):
    """Direct/bare contract fixture: only explicitly passed fields are evidence."""
    kwargs = {
        "contract_id": "bare",
        "pipeline_name": "bare",
        "source": Source(source_id="s", type=SourceType.ADLS),
        "target": Target(target_id="t"),
    }
    kwargs.update(overrides)
    return PipelineContract(**kwargs)


def test_cp008_empty_object_is_unknown():
    assert cp008_performance({}).status == CheckpointStatus.UNKNOWN


def test_cp008_unmeasured_object_is_unknown():
    assert cp008_performance(_runtime(0.0)).status == CheckpointStatus.UNKNOWN


def test_cp008_measured_runtime_stays_pass_skeleton():
    out = cp008_performance(_runtime(600.0))
    assert out.status == CheckpointStatus.PASS


def test_cp009_arbitrary_config_is_unknown():
    out = cp009_cluster({"foo": 1})
    assert out.status == CheckpointStatus.UNKNOWN
    assert "unknown-reason" in out.assumptions


def test_cp009_meaningful_config_stays_pass_skeleton():
    out = cp009_cluster({"spark_version": "15.4.x-scala2.12", "num_workers": 4})
    assert out.status == CheckpointStatus.PASS


def test_cp010_all_volumes_none_is_unknown():
    out = cp010_scalability(contract=_bare_contract(), data_profile=DataProfile())
    assert out.status == CheckpointStatus.UNKNOWN
    assert "unknown-reason" in out.assumptions


def test_cp010_contract_volume_stays_pass_skeleton():
    contract = _bare_contract(expected_daily_volume_gb=500.0, peak_daily_volume_gb=3000.0)
    out = cp010_scalability(contract=contract, data_profile=None)
    assert out.status == CheckpointStatus.PASS


def test_cp011_arbitrary_config_is_unknown():
    out = cp011_job({"foo": "bar"})
    assert out.status == CheckpointStatus.UNKNOWN
    assert "unknown-reason" in out.assumptions


def test_cp011_reliability_config_stays_pass_skeleton():
    out = cp011_job({"max_retries": 2, "timeout_seconds": 2700})
    assert out.status == CheckpointStatus.PASS


def test_cp013_comment_only_try_is_unknown():
    out = cp013_error_handling("# please add try: handling except later")
    assert out.status == CheckpointStatus.UNKNOWN
    assert "unknown-reason" in out.assumptions


def test_cp013_parsed_handler_stays_pass():
    out = cp013_error_handling("try:\n    run()\nexcept Exception:\n    raise")
    assert out.status == CheckpointStatus.PASS


def test_cp013_no_handling_mention_stays_warn():
    out = cp013_error_handling("x = compute(df)\nwrite(x)")
    assert out.status == CheckpointStatus.WARN


def test_cp015_prose_only_checkpoint_is_unknown():
    out = cp015_restartability(None, "# TODO: add checkpoint here")
    assert out.status == CheckpointStatus.UNKNOWN
    assert "unknown-reason" in out.assumptions


def test_cp015_checkpoint_location_stays_pass():
    out = cp015_restartability(None, 'x.option("checkpointLocation", "abfss://c")')
    assert out.status == CheckpointStatus.PASS


def test_cp015_no_mention_stays_warn():
    out = cp015_restartability(None, "x = 1")
    assert out.status == CheckpointStatus.WARN


def test_cp016_bare_contract_is_unknown():
    """No explicit idempotency evidence (model default is not evidence)."""
    out = cp016_idempotency(_bare_contract())
    assert out.status == CheckpointStatus.UNKNOWN
    assert "unknown-reason" in out.assumptions


def test_cp016_explicit_idempotent_stays_pass():
    contract = _bare_contract(reliability=ReliabilityRules(idempotent=True))
    assert cp016_idempotency(contract).status == CheckpointStatus.PASS


def test_cp016_explicit_non_idempotent_stays_warn():
    contract = _bare_contract(reliability=ReliabilityRules(idempotent=False))
    assert cp016_idempotency(contract).status == CheckpointStatus.WARN


def test_cp021_owner_only_is_unknown():
    contract = _bare_contract(owner="alice")
    out = cp021_governance(contract)
    assert out.status == CheckpointStatus.UNKNOWN
    assert "unknown-reason" in out.assumptions


def test_cp021_owner_plus_catalog_stays_pass():
    contract = _bare_contract(owner="alice", target=Target(target_id="t", catalog="prod"))
    assert cp021_governance(contract).status == CheckpointStatus.PASS


def test_cp021_missing_owner_stays_warn():
    assert cp021_governance(_bare_contract()).status == CheckpointStatus.WARN


def test_cp022_volume_only_profile_is_unknown():
    profile = DataProfile(total_bytes=1000, file_count=5)
    assert profile.has_sufficient_metadata
    out = cp022_data_quality(profile)
    assert out.status == CheckpointStatus.UNKNOWN
    assert "unknown-reason" in out.assumptions


def test_cp022_shaped_profile_stays_pass():
    profile = DataProfile(total_bytes=1000, file_count=5, record_count=10, column_count=2)
    assert cp022_data_quality(profile).status == CheckpointStatus.PASS


def test_cp023_measured_no_sla_stays_pass_phase6():
    """Locks Phase 5 CASE B: measured duration + no SLA is a valid PASS."""
    out = cp023_sla(contract=None, runtime_data=_runtime(600.0))
    assert out.status == CheckpointStatus.PASS


def test_cp023_unmeasured_no_sla_stays_unknown_phase6():
    """Locks Phase 5 CASE D: unmeasured duration + no SLA is UNKNOWN."""
    out = cp023_sla(contract=None, runtime_data=_runtime(0.0))
    assert out.status == CheckpointStatus.UNKNOWN


def test_engine_rules_load_failure_no_clean_pass():
    """load_rules() raising must not manufacture a PASS skeleton."""
    eng = CheckpointEngine()
    skeleton = cp008_performance(_runtime(600.0))
    assert skeleton.status == CheckpointStatus.PASS
    with patch("dpif.rules.engine.load_rules", side_effect=RuntimeError("rules down")):
        out = eng.execute_checkpoint(skeleton, {})
    assert out.status == CheckpointStatus.UNKNOWN
    assert "unknown-reason" in out.assumptions


def test_engine_rules_load_failure_preserves_builder_fail():
    """Rules failure downgrades unconfirmed PASS only; genuine FAIL stands."""
    eng = CheckpointEngine()
    contract = _bare_contract(processing="full_load", expected_daily_volume_gb=500.0)
    skeleton = cp012_incremental(contract)
    assert skeleton.status == CheckpointStatus.FAIL
    with patch("dpif.rules.engine.load_rules", side_effect=RuntimeError("rules down")):
        out = eng.execute_checkpoint(skeleton, {"pipeline_contract": contract})
    assert out.status == CheckpointStatus.FAIL


def test_engine_rules_failure_downgrade_only_where_rules_cover():
    """PASS in a rule-covered category (cluster) needs the scan; without it -> UNKNOWN."""
    from dpif.checkpoints.engine import _RULE_COVERED_CATEGORIES

    assert "cluster" in _RULE_COVERED_CATEGORIES
    eng = CheckpointEngine()
    skeleton = cp009_cluster({"spark_version": "15.4.x-scala2.12", "num_workers": 4})
    assert skeleton.status == CheckpointStatus.PASS
    with patch("dpif.rules.engine.load_rules", side_effect=RuntimeError("rules down")):
        out = eng.execute_checkpoint(skeleton, {})
    assert out.status == CheckpointStatus.UNKNOWN
    assert "unknown-reason" in out.assumptions


def test_engine_rules_failure_keeps_cluster_fail():
    """FAIL established by the builder stands even where rules would also run."""
    eng = CheckpointEngine()
    skeleton = cp009_cluster({"num_workers": 0, "spark_version": "15.4.x-scala2.12"})
    assert skeleton.status == CheckpointStatus.FAIL
    with patch("dpif.rules.engine.load_rules", side_effect=RuntimeError("rules down")):
        out = eng.execute_checkpoint(skeleton, {})
    assert out.status == CheckpointStatus.FAIL


def test_engine_rules_failure_keeps_unknown_unknown():
    """UNKNOWN stays UNKNOWN when rules are unavailable."""
    eng = CheckpointEngine()
    skeleton = cp008_performance(None)
    assert skeleton.status == CheckpointStatus.UNKNOWN
    with patch("dpif.rules.engine.load_rules", side_effect=RuntimeError("rules down")):
        out = eng.execute_checkpoint(skeleton, {})
    assert out.status == CheckpointStatus.UNKNOWN


def test_engine_rules_failure_no_clean_code_pass():
    """The code-upgrade path must not manufacture PASS when the scan never ran."""
    eng = CheckpointEngine()
    skeleton = Checkpoint(
        checkpoint_id="CP-004",
        name="Code Validation",
        category="code",
        status=CheckpointStatus.UNKNOWN,
        severity=Severity.INFO,
    )
    assert "unknown-reason" not in skeleton.assumptions
    with patch("dpif.rules.engine.load_rules", side_effect=RuntimeError("rules down")):
        out = eng.execute_checkpoint(
            skeleton, {"code_snippet": "x = compute(df)\nwrite(x)"}
        )
    assert out.status == CheckpointStatus.UNKNOWN
    assert "unknown-reason" in out.assumptions


def test_engine_rules_failure_keeps_independent_pass():
    """PASS whose evidence never involves rules (governance) is retained."""
    from dpif.checkpoints.engine import _RULE_COVERED_CATEGORIES

    assert "governance" not in _RULE_COVERED_CATEGORIES
    eng = CheckpointEngine()
    contract = _bare_contract(owner="alice", target=Target(target_id="t", catalog="prod"))
    skeleton = cp021_governance(contract)
    assert skeleton.status == CheckpointStatus.PASS
    with patch("dpif.rules.engine.load_rules", side_effect=RuntimeError("rules down")):
        out = eng.execute_checkpoint(skeleton, {"pipeline_contract": contract})
    assert out.status == CheckpointStatus.PASS


def test_rule_covered_categories_match_loaded_rules():
    """Locks _RULE_COVERED_CATEGORIES against drift when rules change."""
    from dpif.checkpoints.engine import _RULE_COVERED_CATEGORIES
    from dpif.rules.engine import load_rules

    covered = {r.category for r in load_rules()}
    assert covered == set(_RULE_COVERED_CATEGORIES)


def _historical_runs_seen(monkeypatch, tmp_path: Path, payload: str | None) -> object:
    """Run offline validation while capturing the historical_runs kwarg."""
    import dpif.cli as cli_mod

    seen: dict = {}
    real = cli_mod.build_all_checkpoints

    def _spy(*args, **kwargs):
        seen.update(kwargs)
        return real(*args, **kwargs)

    monkeypatch.setattr(cli_mod, "build_all_checkpoints", _spy)
    args = [
        "validate",
        "--contract",
        "tests/fixtures/contracts/small_batch_pipeline.yaml",
        "--offline",
        "--output-dir",
        str(tmp_path),
    ]
    if payload is not None:
        hist = tmp_path / "historical.json"
        hist.write_text(payload, encoding="utf-8")
        args += ["--historical-runs", str(hist)]
    result = CliRunner().invoke(cli_mod.cli, args)
    assert result.exit_code == 0, result.output
    return seen.get("historical_runs")


def test_offline_missing_historical_file_is_none(monkeypatch, tmp_path):
    assert _historical_runs_seen(monkeypatch, tmp_path, None) is None


def test_offline_empty_historical_file_is_empty_list(monkeypatch, tmp_path):
    assert _historical_runs_seen(monkeypatch, tmp_path, "[]") == []


def test_offline_populated_historical_file_is_list(monkeypatch, tmp_path):
    entries = [
        {"run_id": "r1", "volume_gb": 10.0, "duration_minutes": 5.0},
        {"run_id": "r2", "volume_gb": 11.0, "duration_minutes": 5.5},
    ]
    assert _historical_runs_seen(monkeypatch, tmp_path, json.dumps(entries)) == entries
