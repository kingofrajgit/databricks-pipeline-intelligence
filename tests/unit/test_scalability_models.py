"""Unit tests for Phase 8 Scalability Intelligence models and engine math."""

from __future__ import annotations

import pytest

from dpif.models import PipelineContract, Source, SourceType, Target
from dpif.scalability.engine import (
    analyze_historical_trends,
    extract_baseline_volume,
    generate_scenarios,
    project_metric_linear,
)
from dpif.scalability.models import (
    EvidenceProvenance,
    ProjectionMethod,
    ScalabilityAssessment,
    ScalabilityObservation,
    ScalabilityScenario,
    ScalingBehavior,
    ScenarioType,
)


def _make_contract(
    expected_gb: float = 100.0,
    peak_gb: float = 250.0,
) -> PipelineContract:
    return PipelineContract(
        contract_id="contract-01",
        pipeline_name="test_scalability_pipeline",
        source=Source(
            source_id="src-01",
            type=SourceType.DELTA,
            path="abfss://landing@acc.dfs.core.windows.net/orders",
        ),
        target=Target(target_id="tgt-01", type="delta", path="/mnt/delta/orders"),
        expected_daily_volume_gb=expected_gb,
        peak_daily_volume_gb=peak_gb,
    )


class TestScalabilityModels:
    def test_scalability_observation_validation(self):
        obs = ScalabilityObservation(
            run_id="run-1",
            volume_gb=100.0,
            duration_minutes=15.0,
            task_count=100,
            failed_tasks=2,
            shuffle_bytes=1024 * 1024 * 1024,
            spill_bytes=0,
        )
        assert obs.run_id == "run-1"
        assert obs.volume_gb == 100.0
        assert obs.duration_minutes == 15.0
        assert obs.failure_rate == 0.02
        assert obs.shuffle_gb == pytest.approx(1.0, abs=0.01)
        assert obs.spill_gb == 0.0

    def test_scalability_scenario_creation(self):
        sc = ScalabilityScenario(
            scenario_id="SCENARIO-G1",
            name="1-Year Growth",
            scenario_type=ScenarioType.GROWTH_1,
            input_volume_gb=250.0,
            growth_factor=2.5,
            source="Annual 25% compounded growth",
        )
        assert sc.scenario_type == ScenarioType.GROWTH_1
        assert sc.input_volume_gb == 250.0
        assert sc.input_volume_tb == pytest.approx(250.0 / 1024.0, abs=0.001)
        assert sc.growth_factor == 2.5
        d = sc.to_dict()
        assert d["scenario_type"] == ScenarioType.GROWTH_1.value
        assert d["input_volume_gb"] == 250.0

    def test_scalability_assessment_container(self):
        base = ScalabilityScenario(
            scenario_id="SC-BASE",
            name="Base",
            scenario_type=ScenarioType.BASELINE,
            input_volume_gb=100.0,
        )
        exp = ScalabilityScenario(
            scenario_id="SC-EXP",
            name="Expected",
            scenario_type=ScenarioType.EXPECTED,
            input_volume_gb=150.0,
        )
        assessment = ScalabilityAssessment(
            baseline_scenario=base,
            expected_scenario=exp,
            growth_scenarios=[],
            findings=[],
        )
        assert len(assessment.all_scenarios) == 2
        d = assessment.to_dict()
        assert d["baseline_scenario"]["input_volume_gb"] == 100.0
        assert d["expected_scenario"]["input_volume_gb"] == 150.0
        assert d["findings_count"] == 0


class TestScalabilityEngine:
    def test_extract_baseline_volume(self):
        contract = _make_contract(expected_gb=100.0)
        vol, prov = extract_baseline_volume(contract, None, None)
        assert vol == 100.0
        assert prov == EvidenceProvenance.CONTRACT

    def test_generate_scenarios_standard(self):
        contract = _make_contract(expected_gb=100.0, peak_gb=250.0)
        base, exp, peak, growth = generate_scenarios(
            contract=contract,
            growth_rate_override=20.0,
        )
        assert base is not None
        assert base.input_volume_gb == 100.0
        assert exp is not None
        assert exp.input_volume_gb == 100.0
        assert peak is not None
        assert peak.input_volume_gb == 250.0
        assert len(growth) == 2
        assert growth[0].scenario_type == ScenarioType.GROWTH_1
        assert round(growth[0].input_volume_gb, 1) == 120.0
        assert growth[1].scenario_type == ScenarioType.GROWTH_2
        assert round(growth[1].input_volume_gb, 1) == 144.0

    def test_generate_scenarios_missing_growth_rate(self):
        contract = _make_contract(expected_gb=100.0, peak_gb=150.0)
        base, exp, peak, growth = generate_scenarios(
            contract=contract,
            growth_rate_override=None,
        )
        assert base is not None
        assert peak is not None
        # Without growth rate, growth scenarios should not be fabricated
        assert len(growth) == 0

    def test_project_metric_linear_confidence_degradation(self):
        # 1x scaling -> full base confidence
        p1 = project_metric_linear(
            baseline_volume_gb=100.0,
            target_volume_gb=100.0,
            baseline_value=20.0,
            projected_metric="duration_minutes",
            base_confidence=0.90,
        )
        assert p1.projected_value == 20.0
        assert p1.confidence == 0.90
        assert p1.projection_method == ProjectionMethod.LINEAR

        # 10x scaling -> confidence should degrade by (0.05 * (10 - 1) / 3) = 0.15
        p10 = project_metric_linear(
            baseline_volume_gb=100.0,
            target_volume_gb=1000.0,
            baseline_value=20.0,
            projected_metric="duration_minutes",
            base_confidence=0.90,
        )
        assert p10.projected_value == 200.0
        assert p10.confidence == pytest.approx(0.75, abs=0.01)

    def test_analyze_historical_trends_insufficient_data(self):
        single_obs = [
            ScalabilityObservation(
                run_id="run-1",
                volume_gb=100.0,
                duration_minutes=10.0,
            )
        ]
        trends = analyze_historical_trends(single_obs)
        assert len(trends) == 1
        assert trends[0].scaling_behavior == ScalingBehavior.INSUFFICIENT_DATA
        assert trends[0].confidence == 0.0

    def test_analyze_historical_trends_linear(self):
        runs = [
            ScalabilityObservation(
                run_id="r1",
                volume_gb=100.0,
                duration_minutes=10.0,
                shuffle_bytes=20 * (1024**3),
            ),
            ScalabilityObservation(
                run_id="r2",
                volume_gb=200.0,
                duration_minutes=20.0,
                shuffle_bytes=40 * (1024**3),
            ),
            ScalabilityObservation(
                run_id="r3",
                volume_gb=300.0,
                duration_minutes=30.0,
                shuffle_bytes=60 * (1024**3),
            ),
        ]
        trends = analyze_historical_trends(runs)
        runtime_trend = next(t for t in trends if t.metric_name == "runtime_scaling")
        assert runtime_trend.scaling_behavior == ScalingBehavior.LINEAR
        assert runtime_trend.confidence >= 0.85

    def test_analyze_historical_trends_super_linear(self):
        runs = [
            ScalabilityObservation(
                run_id="r1",
                volume_gb=100.0,
                duration_minutes=10.0,
                spill_bytes=0,
                task_count=100,
                failed_tasks=0,
            ),
            ScalabilityObservation(
                run_id="r2",
                volume_gb=200.0,
                duration_minutes=35.0,
                spill_bytes=15 * (1024**3),
                task_count=200,
                failed_tasks=10,
            ),
            ScalabilityObservation(
                run_id="r3",
                volume_gb=300.0,
                duration_minutes=90.0,
                spill_bytes=80 * (1024**3),
                task_count=300,
                failed_tasks=35,
            ),
        ]
        trends = analyze_historical_trends(runs)
        runtime_trend = next(t for t in trends if t.metric_name == "runtime_scaling")
        assert runtime_trend.scaling_behavior == ScalingBehavior.SUPER_LINEAR
        failure_trend = next(
            (t for t in trends if t.metric_name == "failure_rate_scaling"), None
        )
        assert failure_trend is not None
        assert failure_trend.scaling_behavior == ScalingBehavior.SUPER_LINEAR
