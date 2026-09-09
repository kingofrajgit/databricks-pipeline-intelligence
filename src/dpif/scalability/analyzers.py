"""Pure detector functions for Phase 8 Scalability Rules (SCALABILITY-001..014).

Strict principles:
- Deterministic and pure (no side-effects, no network, no credentials).
- Clear distinction: OBSERVED vs PROJECTED vs EXPECTED vs UNKNOWN.
- Projections always expose method, inputs, assumptions, and confidence.
- Missing data returns UNKNOWN with confidence 0.0—never fake PASS or FAIL.
"""

from __future__ import annotations

from typing import Any

from dpif.models import DataProfile, PipelineContract
from dpif.scalability.engine import (
    analyze_historical_trends,
    extract_baseline_volume,
    project_metric_linear,
)
from dpif.scalability.models import EvidenceProvenance, ScalingBehavior


def _get_contract(data: dict[str, Any], context: dict[str, Any] | None) -> PipelineContract | None:
    c = (
        data.get("pipeline_contract")
        or data.get("contract")
        or (context.get("pipeline_contract") if context else None)
        or (context.get("contract") if context else None)
    )
    return c if isinstance(c, PipelineContract) else None


def _get_profile(data: dict[str, Any], context: dict[str, Any] | None) -> DataProfile | None:
    p = (
        data.get("data_profile")
        or data.get("profile")
        or (context.get("data_profile") if context else None)
        or (context.get("profile") if context else None)
    )
    return p if isinstance(p, DataProfile) else None


def _get_runtime(data: dict[str, Any], context: dict[str, Any] | None) -> Any | None:
    return (
        data.get("runtime_run")
        or data.get("runtime")
        or (context.get("runtime_run") if context else None)
        or (context.get("runtime") if context else None)
    )


def _get_observations(data: dict[str, Any], context: dict[str, Any] | None) -> list[Any]:
    obs = (
        data.get("historical_runs")
        or data.get("observations")
        or (context.get("historical_runs") if context else None)
        or (context.get("observations") if context else None)
    )
    if isinstance(obs, list):
        return obs
    return []


# ====================================================================
# SCALABILITY-001: Expected Volume Capacity Risk
# ====================================================================
def analyze_expected_volume_capacity(
    data: dict[str, Any],
    context: dict[str, Any] | None = None,
    warn_ratio: float = 2.0,
) -> list[dict[str, Any]]:
    """Compare expected workload against available evidence."""
    contract = _get_contract(data, context)
    profile = _get_profile(data, context)
    runtime = _get_runtime(data, context)

    exp_vol = None
    if contract is not None and contract.expected_daily_volume_gb > 0:
        exp_vol = contract.expected_daily_volume_gb
    elif contract is not None and contract.source and contract.source.expected_volume_gb:
        exp_vol = contract.source.expected_volume_gb

    if exp_vol is None or exp_vol <= 0:
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {"expected_volume_gb": None},
                "expected": {"expected_volume_configured": True},
                "evidence": ["No expected daily volume declared in pipeline contract"],
                "recommendation": "Declare expected daily volume in contract to evaluate capacity.",
                "confidence": 0.0,
                "provenance": EvidenceProvenance.UNKNOWN,
            }
        ]

    baseline_vol, prov = extract_baseline_volume(None, profile, runtime)
    if baseline_vol is None or baseline_vol <= 0:
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {"expected_volume_gb": exp_vol, "evidence_volume_gb": None},
                "expected": {"evidence_volume_available": True},
                "evidence": [
                    "No runtime run telemetry or data profile available for capacity comparison"
                ],
                "recommendation": (
                    "Provide execution telemetry or a data profile to verify workload capacity."
                ),
                "confidence": 0.0,
                "provenance": EvidenceProvenance.UNKNOWN,
            }
        ]

    ratio = exp_vol / baseline_vol
    if ratio > warn_ratio:
        return [
            {
                "triggered": True,
                "level": "WARN",
                "observed": {
                    "expected_volume_gb": exp_vol,
                    "evidence_volume_gb": round(baseline_vol, 2),
                    "capacity_gap_ratio": round(ratio, 2),
                },
                "expected": {"max_capacity_gap_ratio": warn_ratio},
                "evidence": [
                    (
                        f"Expected daily volume ({exp_vol:.1f} GB) is {ratio:.1f}x larger than "
                        f"verified evidence volume ({baseline_vol:.1f} GB; source: {prov.value})"
                    )
                ],
                "recommendation": (
                    "Pipeline capacity has not been directly demonstrated at expected scale. "
                    "Conduct a dry-run or benchmark test using expected workload volume."
                ),
                "confidence": 0.75,
                "provenance": EvidenceProvenance.CONTRACT,
            }
        ]

    return []


# ====================================================================
# SCALABILITY-002: Peak Volume Capacity Risk
# ====================================================================
def analyze_peak_volume_capacity(
    data: dict[str, Any],
    context: dict[str, Any] | None = None,
    warn_ratio: float = 2.0,
    fail_ratio: float = 10.0,
) -> list[dict[str, Any]]:
    """Compare peak workload against observed workload and available evidence."""
    contract = _get_contract(data, context)
    profile = _get_profile(data, context)
    runtime = _get_runtime(data, context)

    peak_vol = None
    if contract is not None and contract.peak_daily_volume_gb > 0:
        peak_vol = contract.peak_daily_volume_gb
    elif contract is not None and contract.source and contract.source.peak_volume_gb:
        peak_vol = contract.source.peak_volume_gb

    if peak_vol is None or peak_vol <= 0:
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {"peak_volume_gb": None},
                "expected": {"peak_volume_configured": True},
                "evidence": ["No peak daily volume declared in pipeline contract"],
                "recommendation": "Declare peak volume in contract to assess peak capacity risk.",
                "confidence": 0.0,
                "provenance": EvidenceProvenance.UNKNOWN,
            }
        ]

    baseline_vol, prov = extract_baseline_volume(contract, profile, runtime)
    if baseline_vol is None or baseline_vol <= 0:
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {"peak_volume_gb": peak_vol, "observed_volume_gb": None},
                "expected": {"observed_volume_available": True},
                "evidence": [
                    "No observed volume evidence available to compare against peak workload"
                ],
                "recommendation": (
                    "Provide execution telemetry or profile to validate peak scaling headroom."
                ),
                "confidence": 0.0,
                "provenance": EvidenceProvenance.UNKNOWN,
            }
        ]

    ratio = peak_vol / baseline_vol
    if ratio > warn_ratio:
        level = "HIGH" if ratio >= fail_ratio else "WARN"
        return [
            {
                "triggered": True,
                "level": level,
                "observed": {
                    "peak_volume_gb": peak_vol,
                    "observed_volume_gb": round(baseline_vol, 2),
                    "peak_multiplier": round(ratio, 2),
                },
                "expected": {"max_unvalidated_peak_ratio": warn_ratio},
                "evidence": [
                    (
                        f"Peak workload ({peak_vol:.1f} GB) represents a {ratio:.1f}x multiplier "
                        f"over observed evidence ({baseline_vol:.1f} GB; source: {prov.value})"
                    )
                ],
                "recommendation": (
                    "Peak scenario not directly observed. Available evidence indicates potential "
                    "capacity headroom constraint during peak ingestion bursts."
                ),
                "confidence": 0.70,
                "provenance": EvidenceProvenance.CONTRACT,
            }
        ]

    return []


# ====================================================================
# SCALABILITY-003: Data Growth Risk
# ====================================================================
def analyze_data_growth(
    data: dict[str, Any],
    context: dict[str, Any] | None = None,
    warn_growth_rate: float = 20.0,
    fail_growth_rate: float = 50.0,
    warn_projected_gb: float = 1000.0,
    fail_projected_gb: float = 2500.0,
) -> list[dict[str, Any]]:
    """Analyze data growth rate and deterministic future volume projection."""
    contract = _get_contract(data, context)
    profile = _get_profile(data, context)
    runtime = _get_runtime(data, context)

    growth_rate = None
    if contract is not None:
        growth_rate = contract.growth_rate_percent or getattr(
            contract.source, "growth_rate_percent", None
        )
        if (growth_rate is None or growth_rate <= 0) and contract.scalability:
            day_rate = contract.scalability.expected_growth_percent_per_day
            if day_rate and day_rate > 0:
                growth_rate = day_rate * 30.0

    if growth_rate is None or growth_rate <= 0:
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {"growth_rate_percent": None},
                "expected": {"growth_rate_configured": True},
                "evidence": [
                    "Data growth rate is unavailable; cannot project future workload expansion"
                ],
                "recommendation": (
                    "Specify expected growth rate in pipeline contract to evaluate growth risk."
                ),
                "confidence": 0.0,
                "provenance": EvidenceProvenance.UNKNOWN,
            }
        ]

    baseline_vol, _ = extract_baseline_volume(contract, profile, runtime)
    if baseline_vol is None or baseline_vol <= 0:
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {"growth_rate_percent": growth_rate, "baseline_volume_gb": None},
                "expected": {"baseline_volume_available": True},
                "evidence": ["Baseline volume unavailable to calculate compound growth projection"],
                "recommendation": "Provide baseline workload volume to compute growth projections.",
                "confidence": 0.0,
                "provenance": EvidenceProvenance.UNKNOWN,
            }
        ]

    multiplier = (1.0 + (growth_rate / 100.0)) ** 2
    projected_gb = baseline_vol * multiplier

    if growth_rate >= warn_growth_rate or projected_gb >= warn_projected_gb:
        level = (
            "HIGH"
            if (growth_rate >= fail_growth_rate or projected_gb >= fail_projected_gb)
            else "WARN"
        )
        proj = project_metric_linear(
            baseline_volume_gb=baseline_vol,
            target_volume_gb=projected_gb,
            baseline_value=baseline_vol,
            projected_metric="annual_volume_gb",
            base_confidence=0.75,
        )
        return [
            {
                "triggered": True,
                "level": level,
                "observed": {
                    "growth_rate_percent": growth_rate,
                    "baseline_volume_gb": round(baseline_vol, 2),
                    "projected_horizon_volume_gb": round(projected_gb, 2),
                    "projection": proj.to_dict(),
                },
                "expected": {
                    "max_annual_growth_rate": warn_growth_rate,
                    "max_projected_volume_gb": warn_projected_gb,
                },
                "evidence": [
                    (
                        f"Configured growth rate ({growth_rate:.1f}%) projects data volume "
                        f"to expand from {baseline_vol:.1f} GB to {projected_gb:.1f} GB "
                        "over the forecast horizon"
                    )
                ],
                "recommendation": (
                    "Under configured growth assumptions, workload will exceed current "
                    "thresholds. Plan partition maintenance and adjust cluster sizing."
                ),
                "confidence": 0.80,
                "provenance": EvidenceProvenance.PROJECTED,
            }
        ]

    return []


# ====================================================================
# SCALABILITY-004: File Count Growth Risk
# ====================================================================
def analyze_file_count_growth(
    data: dict[str, Any],
    context: dict[str, Any] | None = None,
    warn_file_count: int = 50000,
    fail_file_count: int = 200000,
    small_file_kb: float = 10240.0,
) -> list[dict[str, Any]]:
    """Analyze file count expansion and interaction with small-file risks."""
    profile = _get_profile(data, context)
    contract = _get_contract(data, context)
    runtime = _get_runtime(data, context)

    if profile is None or profile.file_count <= 0:
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {"file_count": None},
                "expected": {"file_profile_available": True},
                "evidence": ["Data profile file count metadata is unavailable"],
                "recommendation": "Provide data profile metadata to evaluate file count scaling.",
                "confidence": 0.0,
                "provenance": EvidenceProvenance.UNKNOWN,
            }
        ]

    baseline_vol, _ = extract_baseline_volume(contract, profile, runtime)
    target_vol = None
    if contract is not None:
        target_vol = contract.peak_daily_volume_gb or contract.expected_daily_volume_gb

    if not target_vol or not baseline_vol or baseline_vol <= 0:
        scaling_factor = 2.0
        target_vol = (baseline_vol or 100.0) * scaling_factor
    else:
        scaling_factor = target_vol / baseline_vol

    projected_files = int(profile.file_count * scaling_factor)
    avg_size_kb = profile.average_file_size_kb

    if avg_size_kb < small_file_kb and projected_files >= warn_file_count:
        level = "HIGH" if projected_files >= fail_file_count else "WARN"
        return [
            {
                "triggered": True,
                "level": level,
                "observed": {
                    "current_file_count": profile.file_count,
                    "projected_file_count": projected_files,
                    "avg_file_size_mb": round(avg_size_kb / 1024.0, 2),
                    "scaling_factor": round(scaling_factor, 2),
                },
                "expected": {"max_projected_file_count": warn_file_count},
                "evidence": [
                    (
                        f"At projected volume ({target_vol:.1f} GB, "
                        f"{scaling_factor:.1f}x scaling), file count is projected "
                        f"to reach {projected_files:,} files while average file size "
                        f"is small ({avg_size_kb / 1024.0:.1f} MB)"
                    )
                ],
                "recommendation": (
                    "Enable Delta file compaction (`OPTIMIZE`) or Auto Compaction to prevent "
                    "metastore listing bottlenecks and driver memory exhaustion at scale."
                ),
                "confidence": 0.85,
                "provenance": EvidenceProvenance.PROJECTED,
            }
        ]

    return []


# ====================================================================
# SCALABILITY-005: Partition Scalability Risk
# ====================================================================
def analyze_partition_scalability(
    data: dict[str, Any],
    context: dict[str, Any] | None = None,
    max_partition_size_gb: float = 10.0,
    max_partition_count: int = 10000,
) -> list[dict[str, Any]]:
    """Analyze partition count and size scalability under workload expansion."""
    profile = _get_profile(data, context)
    contract = _get_contract(data, context)

    if profile is None or not profile.has_sufficient_metadata:
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {"partition_count": None},
                "expected": {"partition_profile_available": True},
                "evidence": [
                    "No partition profile metadata available for partition scalability analysis"
                ],
                "recommendation": "Provide partition metadata in data profile.",
                "confidence": 0.0,
                "provenance": EvidenceProvenance.UNKNOWN,
            }
        ]

    vol = contract.expected_daily_volume_gb if contract else profile.total_gb
    p_count = profile.partition_count

    if p_count <= 1 and vol >= 100.0:
        return [
            {
                "triggered": True,
                "level": "WARN",
                "observed": {"partition_count": p_count, "volume_gb": vol},
                "expected": {"min_partitions_for_large_data": 10},
                "evidence": [
                    (
                        f"Workload volume is {vol:.1f} GB but partition count is {p_count} "
                        "(unpartitioned)"
                    )
                ],
                "recommendation": (
                    "Introduce date or category partitioning to distribute file reads "
                    "across workers."
                ),
                "confidence": 0.85,
                "provenance": EvidenceProvenance.CONTRACT,
            }
        ]

    if p_count > max_partition_count:
        avg_part_gb = vol / p_count if p_count > 0 else 0.0
        if avg_part_gb < 0.05:
            return [
                {
                    "triggered": True,
                    "level": "WARN",
                    "observed": {
                        "partition_count": p_count,
                        "avg_partition_size_mb": round(avg_part_gb * 1024.0, 1),
                    },
                    "expected": {"max_partition_count": max_partition_count},
                    "evidence": [
                        f"Dataset has {p_count:,} partitions with tiny average size "
                        f"({avg_part_gb * 1024.0:.1f} MB), causing partition management overhead"
                    ],
                    "recommendation": (
                        "Reduce partitioning granularity to prevent excessive catalog "
                        "directory metadata."
                    ),
                    "confidence": 0.85,
                    "provenance": EvidenceProvenance.STATIC,
                }
            ]

    if profile.partition_sizes_gb:
        sizes = list(profile.partition_sizes_gb.values())
        if len(sizes) >= 2:
            max_s = max(sizes)
            avg_s = sum(sizes) / len(sizes)
            if avg_s > 0 and (max_s / avg_s) >= 3.0:
                return [
                    {
                        "triggered": True,
                        "level": "WARN",
                        "observed": {
                            "partition_count": p_count,
                            "max_partition_size_gb": round(max_s, 2),
                            "avg_partition_size_gb": round(avg_s, 2),
                            "skew_ratio": round(max_s / avg_s, 2),
                        },
                        "expected": {"max_partition_skew_ratio": 3.0},
                        "evidence": [
                            f"Partition data skew detected: largest partition is {max_s:.1f} GB, "
                            f"{(max_s / avg_s):.1f}x larger than average ({avg_s:.1f} GB)"
                        ],
                        "recommendation": (
                            "Re-examine partition column cardinality or add a secondary "
                            "partition key to avoid severe stragglers at scale."
                        ),
                        "confidence": 0.85,
                        "provenance": EvidenceProvenance.DATABRICKS_METADATA,
                    }
                ]

    return []


# ====================================================================
# SCALABILITY-006: Driver Scalability Risk
# ====================================================================
def analyze_driver_scalability(
    data: dict[str, Any],
    context: dict[str, Any] | None = None,
    critical_volume_gb: float = 50.0,
) -> list[dict[str, Any]]:
    """Consume Phase 4 findings: driver materialization risk scales with data volume."""
    code_text = (
        data.get("code_snippet")
        or data.get("code_text")
        or (context.get("code_snippet") if context else None)
        or (context.get("code_text") if context else None)
    )
    code_analysis = data.get("code_analysis") or (
        context.get("code_analysis") if context else None
    )
    if code_analysis is None and code_text:
        try:
            from dpif.code.parser import analyze_source

            code_analysis = analyze_source(code_text, filename="pipeline.py")
        except Exception:
            code_analysis = None

    contract = _get_contract(data, context)
    profile = _get_profile(data, context)
    runtime = _get_runtime(data, context)

    if code_analysis is None:
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {"code_analysis_available": False},
                "expected": {"code_analysis_available": True},
                "evidence": [
                    "No parsed code analysis available to assess driver scalability risk"
                ],
                "recommendation": "Provide pipeline code for AST inspection.",
                "confidence": 0.0,
                "provenance": EvidenceProvenance.UNKNOWN,
            }
        ]

    baseline_vol, _ = extract_baseline_volume(contract, profile, runtime)
    effective_vol = baseline_vol or (contract.expected_daily_volume_gb if contract else 0.0)

    ops = code_analysis.operation_counts() if hasattr(code_analysis, "operation_counts") else {}
    collect_count = (
        ops.get("COLLECT", 0)
        + ops.get("collect", 0)
        + ops.get("TO_PANDAS", 0)
        + ops.get("toPandas", 0)
        + ops.get("to_pandas", 0)
    )
    if collect_count == 0 and code_text:
        if "collect(" in code_text or "toPandas(" in code_text or "to_pandas(" in code_text:
            collect_count = 1

    if collect_count > 0:
        if effective_vol >= critical_volume_gb:
            return [
                {
                    "triggered": True,
                    "level": "HIGH",
                    "observed": {
                        "driver_materializations": collect_count,
                        "workload_volume_gb": round(effective_vol, 2),
                    },
                    "expected": {"driver_materialization_at_scale": False},
                    "evidence": [
                        (
                            f"Driver-side materialization detected ({collect_count} "
                            f"collect/toPandas call(s)) combined with a substantial "
                            f"data workload ({effective_vol:.1f} GB)"
                        )
                    ],
                    "recommendation": (
                        "Replace `collect()` and `toPandas()` with distributed DataFrame "
                        "transformations or write directly to storage to prevent driver OOM."
                    ),
                    "confidence": 0.90,
                    "provenance": EvidenceProvenance.STATIC,
                }
            ]
        else:
            return [
                {
                    "triggered": True,
                    "level": "WARN",
                    "observed": {
                        "driver_materializations": collect_count,
                        "workload_volume_gb": round(effective_vol, 2),
                    },
                    "expected": {"driver_materialization_at_scale": False},
                    "evidence": [
                        (
                            f"Driver materialization detected ({collect_count} call(s)). "
                            f"Tolerable at current volume ({effective_vol:.1f} GB), "
                            "but risky at scale"
                        )
                    ],
                    "recommendation": (
                        "Refactor driver collection before workload volume increases."
                    ),
                    "confidence": 0.80,
                    "provenance": EvidenceProvenance.STATIC,
                }
            ]

    return []


# ====================================================================
# SCALABILITY-007: Shuffle Scalability Risk
# ====================================================================
def analyze_shuffle_scalability(
    data: dict[str, Any],
    context: dict[str, Any] | None = None,
    warn_projected_shuffle_gb: float = 200.0,
) -> list[dict[str, Any]]:
    """Consume Phase 4/7 shuffle evidence and project shuffle growth at scale."""
    runtime = _get_runtime(data, context)
    contract = _get_contract(data, context)
    profile = _get_profile(data, context)

    if runtime is None:
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {"runtime_shuffle_telemetry": None},
                "expected": {"runtime_telemetry_available": True},
                "evidence": [
                    "No runtime execution telemetry available to project shuffle scalability"
                ],
                "recommendation": (
                    "Supply runtime execution telemetry to project shuffle volume under scale."
                ),
                "confidence": 0.0,
                "provenance": EvidenceProvenance.UNKNOWN,
            }
        ]

    observed_shuffle_bytes = getattr(runtime, "total_shuffle_bytes", 0)
    observed_shuffle_gb = observed_shuffle_bytes / (1024.0**3)

    baseline_vol, _ = extract_baseline_volume(contract, profile, runtime)
    target_vol = (
        contract.peak_daily_volume_gb
        if (contract and contract.peak_daily_volume_gb)
        else ((baseline_vol or 100.0) * 2.0)
    )

    if not baseline_vol or baseline_vol <= 0:
        scaling_factor = 2.0
    else:
        scaling_factor = target_vol / baseline_vol

    projected_shuffle_gb = observed_shuffle_gb * scaling_factor

    if projected_shuffle_gb >= warn_projected_shuffle_gb:
        proj = project_metric_linear(
            baseline_volume_gb=baseline_vol or 100.0,
            target_volume_gb=target_vol,
            baseline_value=observed_shuffle_gb,
            projected_metric="shuffle_volume_gb",
            base_confidence=0.70,
        )
        return [
            {
                "triggered": True,
                "level": "WARN",
                "observed": {
                    "observed_shuffle_gb": round(observed_shuffle_gb, 2),
                    "projected_shuffle_gb": round(projected_shuffle_gb, 2),
                    "scaling_factor": round(scaling_factor, 2),
                    "projection": proj.to_dict(),
                },
                "expected": {"max_projected_shuffle_gb": warn_projected_shuffle_gb},
                "evidence": [
                    (
                        f"Observed shuffle volume ({observed_shuffle_gb:.1f} GB) projects to "
                        f"{projected_shuffle_gb:.1f} GB at target scale ({target_vol:.1f} GB; "
                        f"{scaling_factor:.1f}x) under linear projection assumption"
                    )
                ],
                "recommendation": (
                    "Projected shuffle volume may cause executor disk spill and "
                    "network bottlenecks. Enable AQE, optimize join keys, and "
                    "replace repartition with coalesce."
                ),
                "confidence": 0.75,
                "provenance": EvidenceProvenance.PROJECTED,
            }
        ]

    return []


# ====================================================================
# SCALABILITY-008: Join Scalability Risk
# ====================================================================
def analyze_join_scalability(
    data: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Analyze join complexity and cross-join risks under scaling."""
    code_text = (
        data.get("code_snippet")
        or data.get("code_text")
        or (context.get("code_snippet") if context else None)
        or (context.get("code_text") if context else None)
    )
    code_analysis = data.get("code_analysis") or (
        context.get("code_analysis") if context else None
    )
    if code_analysis is None and code_text:
        try:
            from dpif.code.parser import analyze_source

            code_analysis = analyze_source(code_text, filename="pipeline.py")
        except Exception:
            code_analysis = None

    sql_analysis = data.get("sql_analysis") or (context.get("sql_analysis") if context else None)
    if sql_analysis is not None and hasattr(sql_analysis, "analysis"):
        sql_analysis = sql_analysis.analysis
    contract = _get_contract(data, context)
    profile = _get_profile(data, context)
    runtime = _get_runtime(data, context)

    baseline_vol, _ = extract_baseline_volume(contract, profile, runtime)
    vol = baseline_vol or (contract.expected_daily_volume_gb if contract else 10.0)

    has_cross_join = False
    cross_details = []

    if sql_analysis is not None:
        for q in getattr(sql_analysis, "queries", []):
            for j in getattr(q, "joins", []):
                if getattr(j, "join_type", "").lower() in ("cross", "comma"):
                    has_cross_join = True
                    cross_details.append(f"SQL cross join on {getattr(j, 'right_table', '')}")

    if code_analysis is not None:
        for op in getattr(code_analysis, "operations", []):
            code_str = getattr(op, "code", "") or ""
            if "crossJoin" in code_str or getattr(op, "name", "") == "crossJoin":
                has_cross_join = True
                cross_details.append("PySpark crossJoin() call")

    if not has_cross_join and code_text and "crossJoin" in code_text:
        has_cross_join = True
        cross_details.append("PySpark crossJoin() call")

    if has_cross_join:
        level = "HIGH" if vol >= 10.0 else "WARN"
        return [
            {
                "triggered": True,
                "level": level,
                "observed": {
                    "cross_join_detected": True,
                    "workload_volume_gb": round(vol, 2),
                    "details": cross_details,
                },
                "expected": {"cross_join_at_scale": False},
                "evidence": [
                    (
                        f"Cartesian cross join detected: {'; '.join(cross_details)}. "
                        f"At {vol:.1f} GB workload, Cartesian product produces severe scaling risk"
                    )
                ],
                "recommendation": (
                    "Eliminate Cartesian cross join: provide an explicit join key, apply strict "
                    "pre-filtering, or use broadcast joins for tiny dimension tables."
                ),
                "confidence": 0.90,
                "provenance": EvidenceProvenance.STATIC,
            }
        ]

    return []


# ====================================================================
# SCALABILITY-009: Aggregation Scalability Risk
# ====================================================================
def analyze_aggregation_scalability(
    data: dict[str, Any],
    context: dict[str, Any] | None = None,
    high_volume_threshold_gb: float = 100.0,
) -> list[dict[str, Any]]:
    """Analyze high-cardinality aggregations and distinct operations under scale."""
    sql_analysis = data.get("sql_analysis") or (context.get("sql_analysis") if context else None)
    code_text = (
        data.get("code_snippet")
        or data.get("code_text")
        or (context.get("code_snippet") if context else None)
        or (context.get("code_text") if context else None)
    )
    code_analysis = data.get("code_analysis") or (
        context.get("code_analysis") if context else None
    )
    if code_analysis is None and code_text:
        try:
            from dpif.code.parser import analyze_source

            code_analysis = analyze_source(code_text, filename="pipeline.py")
        except Exception:
            code_analysis = None

    contract = _get_contract(data, context)
    profile = _get_profile(data, context)
    runtime = _get_runtime(data, context)

    baseline_vol, _ = extract_baseline_volume(contract, profile, runtime)
    vol = baseline_vol or (contract.expected_daily_volume_gb if contract else 0.0)

    has_distinct_agg = False
    if sql_analysis is not None:
        for q in getattr(sql_analysis, "queries", []):
            if getattr(q, "has_distinct", False):
                has_distinct_agg = True
                break

    if not has_distinct_agg and code_analysis is not None:
        ops = (
            code_analysis.operation_counts()
            if hasattr(code_analysis, "operation_counts")
            else {}
        )
        if (
            ops.get("DISTINCT", 0) > 0
            or ops.get("distinct", 0) > 0
            or ops.get("DROP_DUPLICATES", 0) > 0
            or ops.get("drop_duplicates", 0) > 0
        ):
            has_distinct_agg = True
        else:
            for op in getattr(code_analysis, "operations", []):
                if "distinct" in (getattr(op, "code", "") or ""):
                    has_distinct_agg = True
                    break
    if not has_distinct_agg and code_text:
        if "distinct(" in code_text or "distinct()" in code_text or "dropDuplicates(" in code_text:
            has_distinct_agg = True

    if has_distinct_agg and vol >= high_volume_threshold_gb:
        return [
            {
                "triggered": True,
                "level": "WARN",
                "observed": {
                    "high_cardinality_distinct": True,
                    "workload_volume_gb": round(vol, 2),
                },
                "expected": {"scalable_aggregation_at_volume": True},
                "evidence": [
                    (
                        "`SELECT DISTINCT` or `COUNT(DISTINCT)` detected with "
                        f"workload volume {vol:.1f} GB"
                    )
                ],
                "recommendation": (
                    "High-cardinality distinct aggregations require full network shuffle. "
                    "Consider approximate algorithms (`approx_count_distinct`) or pre-aggregation."
                ),
                "confidence": 0.80,
                "provenance": EvidenceProvenance.STATIC,
            }
        ]

    return []


# ====================================================================
# SCALABILITY-010: Cluster Capacity Risk
# ====================================================================
def analyze_cluster_capacity(
    data: dict[str, Any],
    context: dict[str, Any] | None = None,
    min_workers_for_large_scale: int = 4,
    large_scale_volume_gb: float = 500.0,
) -> list[dict[str, Any]]:
    """Evaluate cluster compute capacity against projected peak/growth volume."""
    cluster = (
        data.get("cluster")
        or data.get("cluster_config")
        or (context.get("cluster") if context else None)
    )
    contract = _get_contract(data, context)
    profile = _get_profile(data, context)
    runtime = _get_runtime(data, context)

    if cluster is None:
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {"cluster_config_available": False},
                "expected": {"cluster_config_available": True},
                "evidence": [
                    "Cluster configuration metadata is unavailable to evaluate compute capacity"
                ],
                "recommendation": (
                    "Provide cluster configuration to evaluate sizing against workload scale."
                ),
                "confidence": 0.0,
                "provenance": EvidenceProvenance.UNKNOWN,
            }
        ]

    workers = getattr(cluster, "effective_workers", None)
    if workers is None and isinstance(cluster, dict):
        workers = cluster.get("num_workers") or cluster.get("autoscale", {}).get("max_workers", 1)

    peak_vol = contract.peak_daily_volume_gb if contract else 0.0
    baseline_vol, _ = extract_baseline_volume(contract, profile, runtime)
    effective_peak = peak_vol or ((baseline_vol or 100.0) * 2.0)

    if (
        workers is not None
        and workers < min_workers_for_large_scale
        and effective_peak >= large_scale_volume_gb
    ):
        return [
            {
                "triggered": True,
                "level": "WARN",
                "observed": {
                    "configured_workers": workers,
                    "projected_peak_volume_gb": round(effective_peak, 2),
                },
                "expected": {"min_workers_for_peak": min_workers_for_large_scale},
                "evidence": [
                    (
                        f"Cluster capacity ({workers} worker(s)) is constrained for the projected "
                        f"peak workload of {effective_peak:.1f} GB"
                    )
                ],
                "recommendation": (
                    "Cluster may experience prolonged execution or task starvation at peak volume. "
                    "Increase worker count or enable autoscaling."
                ),
                "confidence": 0.80,
                "provenance": EvidenceProvenance.DATABRICKS_METADATA,
            }
        ]

    return []


# ====================================================================
# SCALABILITY-011: Autoscaling Boundary Risk
# ====================================================================
def analyze_autoscaling_boundary(
    data: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Analyze autoscaling boundary constraints vs scaling multiplier."""
    cluster = (
        data.get("cluster")
        or data.get("cluster_config")
        or (context.get("cluster") if context else None)
    )
    contract = _get_contract(data, context)
    profile = _get_profile(data, context)
    runtime = _get_runtime(data, context)

    if cluster is None:
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {"cluster_config_available": False},
                "expected": {"cluster_config_available": True},
                "evidence": [
                    "Cluster configuration is unavailable to evaluate autoscaling boundaries"
                ],
                "recommendation": "Provide cluster configuration to assess autoscaling bounds.",
                "confidence": 0.0,
                "provenance": EvidenceProvenance.UNKNOWN,
            }
        ]

    is_auto = getattr(cluster, "is_autoscaling", False)
    max_w = None
    min_w = None
    if isinstance(cluster, dict):
        autoscale = cluster.get("autoscale")
        is_auto = bool(autoscale)
        if autoscale:
            min_w = autoscale.get("min_workers")
            max_w = autoscale.get("max_workers")
    else:
        autoscale_obj = getattr(cluster, "autoscale", None)
        if autoscale_obj:
            min_w = getattr(autoscale_obj, "min_workers", None)
            max_w = getattr(autoscale_obj, "max_workers", None)

    baseline_vol, _ = extract_baseline_volume(contract, profile, runtime)
    peak_vol = contract.peak_daily_volume_gb if contract else None

    if peak_vol and baseline_vol and baseline_vol > 0:
        factor = peak_vol / baseline_vol
        if factor >= 3.0:
            if not is_auto:
                return [
                    {
                        "triggered": True,
                        "level": "WARN",
                        "observed": {
                            "is_autoscaling": False,
                            "workload_expansion_factor": round(factor, 2),
                        },
                        "expected": {"autoscaling_enabled_for_variable_load": True},
                        "evidence": [
                            (
                                f"Peak workload ({peak_vol:.1f} GB) is a {factor:.1f}x surge over "
                                "baseline but cluster has fixed sizing without autoscaling"
                            )
                        ],
                        "recommendation": (
                            "Enable cluster autoscaling to dynamically absorb peak bursts without "
                            "paying for idle compute during baseline execution."
                        ),
                        "confidence": 0.85,
                        "provenance": EvidenceProvenance.DATABRICKS_METADATA,
                    }
                ]
            elif max_w and min_w and (max_w / min_w < factor / 2.0):
                return [
                    {
                        "triggered": True,
                        "level": "WARN",
                        "observed": {
                            "min_workers": min_w,
                            "max_workers": max_w,
                            "autoscaling_ratio": round(max_w / min_w, 2),
                            "workload_expansion_factor": round(factor, 2),
                        },
                        "expected": {"sufficient_autoscaling_headroom": True},
                        "evidence": [
                            (
                                f"Autoscaling upper bound ({max_w} workers) allows only "
                                f"{max_w / min_w:.1f}x expansion, constraining a "
                                f"{factor:.1f}x surge"
                            )
                        ],
                        "recommendation": (
                            "Increase `max_workers` on cluster policy to provide adequate headroom."
                        ),
                        "confidence": 0.80,
                        "provenance": EvidenceProvenance.DATABRICKS_METADATA,
                    }
                ]

    return []


# ====================================================================
# SCALABILITY-012: SLA Scalability Risk
# ====================================================================
def analyze_sla_scalability(
    data: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Evaluate contractual SLA against projected runtime at peak volume."""
    contract = _get_contract(data, context)
    runtime = _get_runtime(data, context)
    profile = _get_profile(data, context)

    sla_min = getattr(contract.sla, "max_runtime_minutes", None) if contract else None
    if sla_min is None or sla_min <= 0:
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {"sla_max_runtime_minutes": None},
                "expected": {"sla_configured": True},
                "evidence": ["Contractual SLA runtime limit is unconfigured"],
                "recommendation": "Configure `sla.max_runtime_minutes` in contract.",
                "confidence": 0.0,
                "provenance": EvidenceProvenance.UNKNOWN,
            }
        ]

    if runtime is None:
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {"runtime_duration_minutes": None},
                "expected": {"runtime_telemetry_available": True},
                "evidence": [
                    "No runtime execution telemetry available to project SLA compliance at scale"
                ],
                "recommendation": "Supply runtime execution telemetry.",
                "confidence": 0.0,
                "provenance": EvidenceProvenance.UNKNOWN,
            }
        ]

    dur_sec = getattr(runtime, "duration_seconds", 0.0) or getattr(
        runtime, "total_duration_seconds", 0.0
    )
    baseline_dur_min = getattr(runtime, "total_duration_minutes", 0.0) or (dur_sec / 60.0)
    baseline_vol, _ = extract_baseline_volume(contract, profile, runtime)
    peak_vol = contract.peak_daily_volume_gb if contract else 0.0

    if not peak_vol or not baseline_vol or baseline_vol <= 0:
        scaling_factor = 2.0
        peak_vol = (baseline_vol or 100.0) * scaling_factor
    else:
        scaling_factor = peak_vol / baseline_vol

    projected_dur_min = baseline_dur_min * scaling_factor

    if projected_dur_min > sla_min:
        proj = project_metric_linear(
            baseline_volume_gb=baseline_vol or 100.0,
            target_volume_gb=peak_vol,
            baseline_value=baseline_dur_min,
            projected_metric="runtime_minutes",
            base_confidence=0.75,
        )
        return [
            {
                "triggered": True,
                "level": "WARN",
                "observed": {
                    "baseline_runtime_minutes": round(baseline_dur_min, 1),
                    "projected_peak_runtime_minutes": round(projected_dur_min, 1),
                    "sla_limit_minutes": sla_min,
                    "projection": proj.to_dict(),
                },
                "expected": {"max_projected_runtime_minutes": sla_min},
                "evidence": [
                    (
                        f"Projected execution duration at peak volume ({projected_dur_min:.1f}m) "
                        f"exceeds contractual SLA limit ({sla_min}m) under linear "
                        "projection assumption"
                    )
                ],
                "recommendation": (
                    "Pipeline will violate contractual SLA during peak workload periods. "
                    "Optimize bottlenecks or negotiate revised SLA window."
                ),
                "confidence": 0.80,
                "provenance": EvidenceProvenance.PROJECTED,
            }
        ]

    return []


# ====================================================================
# SCALABILITY-013: Runtime Regression at Scale
# ====================================================================
def analyze_runtime_regression(
    data: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Detect super-linear runtime growth across multiple empirical execution runs."""
    observations = _get_observations(data, context)
    trends = analyze_historical_trends(observations)

    runtime_trend = next((t for t in trends if t.metric_name == "runtime_scaling"), None)
    if runtime_trend is None or runtime_trend.scaling_behavior == ScalingBehavior.INSUFFICIENT_DATA:
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {"historical_runs_count": len(observations)},
                "expected": {"min_historical_runs": 2},
                "evidence": [
                    "Empirical trend analysis requires at least 2 distinct historical "
                    "execution runs"
                ],
                "recommendation": (
                    "Provide 2 or more historical run records to detect empirical "
                    "runtime regression."
                ),
                "confidence": 0.0,
                "provenance": EvidenceProvenance.UNKNOWN,
            }
        ]

    if runtime_trend.scaling_behavior == ScalingBehavior.SUPER_LINEAR:
        scale_idx = runtime_trend.scaling_factor or 1.0
        v1 = runtime_trend.details.get("baseline_run", {}).get("volume_gb", 0)
        v2 = runtime_trend.details.get("scaled_run", {}).get("volume_gb", 0)
        return [
            {
                "triggered": True,
                "level": "WARN",
                "observed": {
                    "scaling_behavior": runtime_trend.scaling_behavior.value,
                    "scaling_index": scale_idx,
                    "details": runtime_trend.details,
                },
                "expected": {"max_scaling_index": 1.25},
                "evidence": [
                    (
                        f"Empirical telemetry indicates super-linear runtime growth "
                        f"({scale_idx:.2f}x scaling index between {v1:.1f} GB and {v2:.1f} GB runs)"
                    )
                ],
                "recommendation": (
                    "Observed runtime scaling is worse than linear. Investigate causes: "
                    "shuffle amplification, partition skew, executor memory spill, or GC pauses."
                ),
                "confidence": runtime_trend.confidence,
                "provenance": EvidenceProvenance.RUNTIME,
            }
        ]

    return []


# ====================================================================
# SCALABILITY-014: Reliability Degradation at Scale
# ====================================================================
def analyze_reliability_degradation(
    data: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Detect escalating failure rate or spill across multiple historical runs."""
    observations = _get_observations(data, context)
    trends = analyze_historical_trends(observations)

    fail_trend = next((t for t in trends if t.metric_name == "failure_rate_scaling"), None)
    if fail_trend is None or fail_trend.scaling_behavior == ScalingBehavior.INSUFFICIENT_DATA:
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {"historical_runs_count": len(observations)},
                "expected": {"min_historical_runs": 2},
                "evidence": [
                    "Reliability degradation analysis requires at least 2 historical runs"
                ],
                "recommendation": (
                    "Provide 2 or more historical run records to assess failure rate correlation."
                ),
                "confidence": 0.0,
                "provenance": EvidenceProvenance.UNKNOWN,
            }
        ]

    if fail_trend.scaling_behavior == ScalingBehavior.SUPER_LINEAR:
        details = fail_trend.details
        f1 = details.get("baseline_failure_rate", 0.0) * 100.0
        f2 = details.get("scaled_failure_rate", 0.0) * 100.0
        return [
            {
                "triggered": True,
                "level": "WARN",
                "observed": {
                    "baseline_failure_rate_pct": round(f1, 2),
                    "scaled_failure_rate_pct": round(f2, 2),
                    "details": details,
                },
                "expected": {"stable_failure_rate_under_scale": True},
                "evidence": [
                    f"Task failure rate increased from {f1:.1f}% to {f2:.1f}% at larger volume"
                ],
                "recommendation": (
                    "Pipeline exhibits reliability degradation under scale. Check memory pressure, "
                    "shuffle fetch failures, or node spot evictions during larger runs."
                ),
                "confidence": fail_trend.confidence,
                "provenance": EvidenceProvenance.RUNTIME,
            }
        ]

    return []
