"""P10-3/D3 focused tests: invalid scalability evidence stays UNKNOWN.

Malformed/incomplete historical observations (wrong-typed run_id, missing
volume_gb/duration_minutes) must never raise out of trend analysis and must
never become FAIL/HIGH workload findings. Valid siblings survive malformed
neighbors. Genuine failures and genuine UNKNOWNs are preserved, and
unexpected non-validation errors still propagate.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dpif.scalability.engine import analyze_historical_trends
from dpif.scalability.models import ScalabilityObservation, ScalingBehavior

LIVE_SHAPED = {"run_id": 998, "start_time": 1726000000000, "end_time": 1726000180000}
LIVE_SHAPED_2 = {"run_id": 999, "start_time": 1726000000000, "end_time": 1726000180000}


def _trend_names(trends):
    return [t.metric_name for t in trends]


# 1. Valid historical runtime observation evaluates normally.
def test_valid_observations_evaluated():
    trends = analyze_historical_trends(
        [
            {"run_id": "r1", "volume_gb": 100.0, "duration_minutes": 10.0},
            {"run_id": "r2", "volume_gb": 200.0, "duration_minutes": 22.0},
        ]
    )
    assert _trend_names(trends) == ["runtime_scaling", "failure_rate_scaling"]
    assert trends[0].scaling_behavior != ScalingBehavior.INSUFFICIENT_DATA


# 2. Integer run_id violates the str|None model contract → element unavailable.
def test_integer_run_id_is_invalid_element():
    with pytest.raises(ValidationError):
        ScalabilityObservation(run_id=998, volume_gb=100.0, duration_minutes=10.0)
    trends = analyze_historical_trends([LIVE_SHAPED, LIVE_SHAPED_2])
    assert len(trends) == 1
    assert trends[0].scaling_behavior == ScalingBehavior.INSUFFICIENT_DATA


# 3/4. Missing volume_gb / duration_minutes → unavailable, not exception.
def test_missing_volume_is_unknown_not_exception():
    trends = analyze_historical_trends(
        [
            {"run_id": "r1", "duration_minutes": 10.0},
            {"run_id": "r2", "duration_minutes": 12.0},
        ]
    )
    assert trends[0].scaling_behavior == ScalingBehavior.INSUFFICIENT_DATA


def test_missing_duration_is_unknown_not_exception():
    trends = analyze_historical_trends(
        [
            {"run_id": "r1", "volume_gb": 100.0},
            {"run_id": "r2", "volume_gb": 120.0},
        ]
    )
    assert trends[0].scaling_behavior == ScalingBehavior.INSUFFICIENT_DATA


# 5. Valid volume and duration evidence evaluates (incl. model aliases).
def test_alias_fields_evaluate():
    trends = analyze_historical_trends(
        [
            {"run_id": "r1", "input_volume_gb": 100.0, "duration_seconds": 600.0},
            {"run_id": "r2", "input_volume_gb": 200.0, "duration_seconds": 1500.0},
        ]
    )
    assert trends[0].scaling_behavior != ScalingBehavior.INSUFFICIENT_DATA
    assert trends[0].observations_count == 2


# 6. Malformed runtime evidence (garbage values) never raises.
def test_malformed_values_never_raise():
    trends = analyze_historical_trends(
        [
            {"run_id": "r1", "volume_gb": "lots", "duration_minutes": "soon"},
            {"run_id": "r2", "volume_gb": None, "duration_minutes": None},
        ]
    )
    assert trends[0].scaling_behavior == ScalingBehavior.INSUFFICIENT_DATA


# 7. Pydantic ValidationError at the model boundary (documents the contract).
def test_model_rejects_live_shaped_dict():
    with pytest.raises(ValidationError):
        ScalabilityObservation(**LIVE_SHAPED)


# Mixed: valid + invalid + valid preserves the valid pair.
def test_mixed_valid_invalid_valid_preserves_valid():
    trends = analyze_historical_trends(
        [
            {"run_id": "r1", "volume_gb": 100.0, "duration_minutes": 10.0},
            dict(LIVE_SHAPED),
            {"run_id": "r3", "volume_gb": 200.0, "duration_minutes": 22.0},
        ]
    )
    assert trends[0].observations_count == 2
    assert trends[0].scaling_behavior != ScalingBehavior.INSUFFICIENT_DATA


# 8. Genuine scalability failure remains FAIL (super-linear trend).
def test_genuine_superlinear_still_detected():
    trends = analyze_historical_trends(
        [
            {"run_id": "r1", "volume_gb": 100.0, "duration_minutes": 10.0},
            {"run_id": "r2", "volume_gb": 110.0, "duration_minutes": 40.0},
        ]
    )
    assert trends[0].scaling_behavior == ScalingBehavior.SUPER_LINEAR


# 9. Genuine scalability UNKNOWN remains UNKNOWN (single observation).
def test_single_observation_unknown_preserved():
    trends = analyze_historical_trends(
        [{"run_id": "r1", "volume_gb": 100.0, "duration_minutes": 10.0}]
    )
    assert trends[0].scaling_behavior == ScalingBehavior.INSUFFICIENT_DATA


# 10. Unexpected non-ValidationError exceptions still propagate (visible).
# A non-iterable observations argument is a programming error, not evidence,
# and must raise instead of degrading silently.
def test_unexpected_errors_not_swallowed():
    with pytest.raises(TypeError):
        analyze_historical_trends(None)


# 11/12. Offline path (fixture-shaped dicts) evaluates; online live shapes stay UNKNOWN.
def test_offline_fixture_shapes_evaluate():
    trends = analyze_historical_trends(
        [
            {"run_id": "run-101", "input_volume_gb": 100.0, "duration_minutes": 10.0},
            {"run_id": "run-102", "input_volume_gb": 150.0, "duration_minutes": 14.0},
        ]
    )
    assert trends[0].observations_count == 2


def test_online_live_shapes_stay_unknown():
    trends = analyze_historical_trends([LIVE_SHAPED, LIVE_SHAPED_2])
    assert all(t.confidence == 0.0 for t in trends)


# 13. Provenance: model instances pass through untouched.
def test_model_instances_preserved():
    obs = ScalabilityObservation(run_id="r1", volume_gb=100.0, duration_minutes=10.0)
    trends = analyze_historical_trends(
        [obs, {"run_id": "r2", "volume_gb": 200.0, "duration_minutes": 20.0}]
    )
    assert trends[0].observations_count == 2


# 14. Rule-level: 013/014 analyzers yield UNKNOWN (not exception) on malformed input.
def test_rule_analyzers_unknown_on_malformed():
    from dpif.scalability.analyzers import (
        analyze_reliability_degradation,
        analyze_runtime_regression,
    )

    for analyzer in (analyze_runtime_regression, analyze_reliability_degradation):
        out = analyzer(
            {"historical_runs": [dict(LIVE_SHAPED), dict(LIVE_SHAPED_2)]}, {}
        )
        assert out, "analyzer must return findings, not raise"
        assert all(f.get("level") == "UNKNOWN" for f in out)
