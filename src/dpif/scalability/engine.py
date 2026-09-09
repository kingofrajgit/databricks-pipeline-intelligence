"""Scenario generation, deterministic projection, and empirical trend analysis."""

from __future__ import annotations

from typing import Any

from dpif.models import CollectionMethod, DataProfile, PipelineContract
from dpif.scalability.models import (
    EvidenceProvenance,
    ProjectionMethod,
    ScalabilityObservation,
    ScalabilityProjection,
    ScalabilityScenario,
    ScalabilityTrend,
    ScalingBehavior,
    ScenarioType,
)


def extract_baseline_volume(
    contract: PipelineContract | None,
    data_profile: DataProfile | None,
    runtime_run: Any | None,
) -> tuple[float | None, EvidenceProvenance]:
    """Extract the baseline volume in GB with strict provenance tracking."""
    if runtime_run is not None:
        in_bytes = getattr(runtime_run, "total_input_bytes", 0)
        if in_bytes and in_bytes > 0:
            return in_bytes / (1024.0**3), EvidenceProvenance.RUNTIME

    if data_profile is not None and data_profile.total_gb > 0:
        if data_profile.collection_method == CollectionMethod.FIXTURE:
            prov = EvidenceProvenance.FIXTURE
        else:
            prov = EvidenceProvenance.DATABRICKS_METADATA
        return data_profile.total_gb, prov

    if contract is not None and contract.expected_daily_volume_gb > 0:
        return contract.expected_daily_volume_gb, EvidenceProvenance.CONTRACT

    if contract is not None and contract.source and contract.source.expected_volume_gb:
        return contract.source.expected_volume_gb, EvidenceProvenance.CONTRACT

    return None, EvidenceProvenance.UNKNOWN


def generate_scenarios(
    contract: PipelineContract | None = None,
    data_profile: DataProfile | None = None,
    runtime_run: Any | None = None,
    growth_rate_override: float | None = None,
) -> tuple[
    ScalabilityScenario | None,
    ScalabilityScenario | None,
    ScalabilityScenario | None,
    list[ScalabilityScenario],
]:
    """Deterministically generate BASELINE, EXPECTED, PEAK, and GROWTH scenarios.

    Returns:
        (baseline, expected, peak, growth_scenarios)
    """
    baseline_vol, baseline_prov = extract_baseline_volume(contract, data_profile, runtime_run)
    sla_min = getattr(contract.sla, "max_runtime_minutes", None) if contract else None

    # 1. BASELINE SCENARIO
    baseline: ScalabilityScenario | None = None
    if baseline_vol is not None and baseline_vol > 0:
        file_cnt = data_profile.file_count if data_profile else None
        runtime_min = None
        if runtime_run is not None:
            runtime_min = getattr(runtime_run, "total_duration_minutes", None)
        baseline = ScalabilityScenario(
            scenario_id="SCENARIO-BASELINE",
            name="Current / Baseline Workload",
            scenario_type=ScenarioType.BASELINE,
            input_volume_gb=baseline_vol,
            file_count=file_cnt,
            expected_runtime_minutes=runtime_min,
            sla_minutes=sla_min,
            growth_factor=1.0,
            source="Observed telemetry / profile",
            evidence_source=baseline_prov,
        )

    # 2. EXPECTED SCENARIO
    expected: ScalabilityScenario | None = None
    exp_vol = None
    if contract is not None and contract.expected_daily_volume_gb > 0:
        exp_vol = contract.expected_daily_volume_gb
    elif contract is not None and contract.source and contract.source.expected_volume_gb:
        exp_vol = contract.source.expected_volume_gb

    if exp_vol is not None and exp_vol > 0:
        factor = (exp_vol / baseline_vol) if (baseline_vol and baseline_vol > 0) else 1.0
        expected = ScalabilityScenario(
            scenario_id="SCENARIO-EXPECTED",
            name="Contractual Expected Workload",
            scenario_type=ScenarioType.EXPECTED,
            input_volume_gb=exp_vol,
            sla_minutes=sla_min,
            growth_factor=round(factor, 2),
            source="Pipeline Contract",
            evidence_source=EvidenceProvenance.CONTRACT,
        )

    # 3. PEAK SCENARIO
    peak: ScalabilityScenario | None = None
    peak_vol = None
    if contract is not None and contract.peak_daily_volume_gb > 0:
        peak_vol = contract.peak_daily_volume_gb
    elif contract is not None and contract.source and contract.source.peak_volume_gb:
        peak_vol = contract.source.peak_volume_gb

    if peak_vol is not None and peak_vol > 0:
        ref_vol = baseline_vol or exp_vol or 1.0
        factor = peak_vol / ref_vol
        peak = ScalabilityScenario(
            scenario_id="SCENARIO-PEAK",
            name="Contractual Peak Workload",
            scenario_type=ScenarioType.PEAK,
            input_volume_gb=peak_vol,
            sla_minutes=sla_min,
            growth_factor=round(factor, 2),
            source="Pipeline Contract",
            evidence_source=EvidenceProvenance.CONTRACT,
        )

    # 4. GROWTH SCENARIOS
    growth_scenarios: list[ScalabilityScenario] = []
    growth_rate = growth_rate_override
    if growth_rate is None and contract is not None:
        growth_rate = contract.growth_rate_percent or getattr(
            contract.source, "growth_rate_percent", 0.0
        )
        if (not growth_rate or growth_rate <= 0) and contract.scalability:
            day_rate = contract.scalability.expected_growth_percent_per_day
            if day_rate and day_rate > 0:
                growth_rate = day_rate * 30.0  # approximate monthly compound

    if growth_rate and growth_rate > 0 and baseline_vol:
        multiplier_1 = 1.0 + (growth_rate / 100.0)
        g1_vol = baseline_vol * multiplier_1
        growth_scenarios.append(
            ScalabilityScenario(
                scenario_id="SCENARIO-GROWTH-1",
                name=f"Growth Horizon 1 (+{growth_rate:.1f}%)",
                scenario_type=ScenarioType.GROWTH_1,
                input_volume_gb=round(g1_vol, 2),
                sla_minutes=sla_min,
                growth_factor=round(multiplier_1, 2),
                source="Contract Growth Rate Projection",
                evidence_source=EvidenceProvenance.PROJECTED,
            )
        )

        multiplier_2 = multiplier_1 * multiplier_1
        g2_vol = baseline_vol * multiplier_2
        growth_scenarios.append(
            ScalabilityScenario(
                scenario_id="SCENARIO-GROWTH-2",
                name=f"Growth Horizon 2 (+{(multiplier_2 - 1.0) * 100.0:.1f}%)",
                scenario_type=ScenarioType.GROWTH_2,
                input_volume_gb=round(g2_vol, 2),
                sla_minutes=sla_min,
                growth_factor=round(multiplier_2, 2),
                source="Contract Growth Rate Compounded",
                evidence_source=EvidenceProvenance.PROJECTED,
            )
        )

    return baseline, expected, peak, growth_scenarios


def project_metric_linear(
    baseline_volume_gb: float,
    target_volume_gb: float,
    baseline_value: float,
    projected_metric: str,
    base_confidence: float = 0.6,
) -> ScalabilityProjection:
    """Deterministic linear projection under explicit documented assumptions."""
    if baseline_volume_gb <= 0:
        return ScalabilityProjection(
            baseline_volume_gb=0.0,
            target_volume_gb=target_volume_gb,
            scaling_factor=1.0,
            projection_method=ProjectionMethod.UNAVAILABLE,
            projected_metric=projected_metric,
            baseline_value=baseline_value,
            projected_value=baseline_value,
            confidence=0.0,
            assumptions=[
                "Baseline volume is zero or unavailable; cannot compute linear projection."
            ],
            evidence_source=EvidenceProvenance.UNKNOWN,
        )

    scaling_factor = target_volume_gb / baseline_volume_gb
    projected_value = baseline_value * scaling_factor

    # Confidence decays gracefully as projection distance grows
    confidence = base_confidence
    if scaling_factor > 10.0:
        confidence = max(0.2, base_confidence - 0.3)
    elif scaling_factor > 4.0:
        confidence = max(0.3, base_confidence - 0.15)

    assumptions = [
        (
            f"Assumes deterministic linear scaling of "
            f"{projected_metric} with workload volume ({scaling_factor:.2f}x)."
        ),
        "Assumes cluster worker capacity and resource allocation remain fixed.",
        "Assumes uniform key distribution and no new skew or spilling at higher scale.",
    ]

    return ScalabilityProjection(
        baseline_volume_gb=round(baseline_volume_gb, 2),
        target_volume_gb=round(target_volume_gb, 2),
        scaling_factor=round(scaling_factor, 2),
        projection_method=ProjectionMethod.LINEAR,
        projected_metric=projected_metric,
        baseline_value=round(baseline_value, 2),
        projected_value=round(projected_value, 2),
        confidence=round(confidence, 2),
        assumptions=assumptions,
        evidence_source=EvidenceProvenance.PROJECTED,
    )


def analyze_historical_trends(
    observations: list[ScalabilityObservation] | list[dict[str, Any]],
) -> list[ScalabilityTrend]:
    """Empirically evaluate scaling behavior across 2 or more execution runs."""
    obs_list: list[ScalabilityObservation] = []
    for o in observations:
        if isinstance(o, ScalabilityObservation):
            obs_list.append(o)
        elif isinstance(o, dict):
            obs_list.append(ScalabilityObservation(**o))

    obs_list = sorted(obs_list, key=lambda x: x.volume_gb)

    if len(obs_list) < 2:
        return [
            ScalabilityTrend(
                metric_name="runtime_scaling",
                observations_count=len(obs_list),
                scaling_behavior=ScalingBehavior.INSUFFICIENT_DATA,
                confidence=0.0,
                details={
                    "reason": (
                        "Empirical trend analysis requires at least 2 distinct "
                        "historical execution runs."
                    )
                },
            )
        ]

    first, last = obs_list[0], obs_list[-1]
    if first.volume_gb <= 0 or first.duration_minutes <= 0:
        vol_ratio = 1.0
        dur_ratio = 1.0
        scale_index = 1.0
    else:
        vol_ratio = last.volume_gb / first.volume_gb
        dur_ratio = last.duration_minutes / first.duration_minutes
        scale_index = dur_ratio / vol_ratio if vol_ratio > 0 else 1.0

    if scale_index > 1.25:
        behavior = ScalingBehavior.SUPER_LINEAR
    elif scale_index >= 0.8:
        behavior = ScalingBehavior.LINEAR
    else:
        behavior = ScalingBehavior.SUB_LINEAR

    confidence = 0.85 if len(obs_list) >= 3 else 0.70

    runtime_trend = ScalabilityTrend(
        metric_name="runtime_scaling",
        observations_count=len(obs_list),
        scaling_behavior=behavior,
        scaling_factor=round(scale_index, 2),
        confidence=confidence,
        details={
            "baseline_run": {"volume_gb": first.volume_gb, "duration_min": first.duration_minutes},
            "scaled_run": {"volume_gb": last.volume_gb, "duration_min": last.duration_minutes},
            "volume_multiplier": round(vol_ratio, 2),
            "duration_multiplier": round(dur_ratio, 2),
            "scaling_index": round(scale_index, 2),
            "empirical_runs": [o.to_dict() for o in obs_list],
        },
    )

    # Failure rate scaling trend
    fail_rate_first = first.failure_rate
    fail_rate_last = last.failure_rate
    fail_trend = ScalabilityTrend(
        metric_name="failure_rate_scaling",
        observations_count=len(obs_list),
        scaling_behavior=(
            ScalingBehavior.SUPER_LINEAR
            if fail_rate_last > fail_rate_first + 0.02
            else ScalingBehavior.LINEAR
        ),
        scaling_factor=round(fail_rate_last - fail_rate_first, 4),
        confidence=confidence,
        details={
            "baseline_failure_rate": round(fail_rate_first, 4),
            "scaled_failure_rate": round(fail_rate_last, 4),
            "increase": round(fail_rate_last - fail_rate_first, 4),
        },
    )

    return [runtime_trend, fail_trend]
