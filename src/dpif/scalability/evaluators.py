"""Bridge Evaluators for Scalability Intelligence Rules (SCALABILITY-001..014).

Bridges YAML rule declarations to pure analyzer detectors in `dpif.scalability.analyzers`,
constructing typed `Finding` and `EvidenceRecord` instances with explicit provenance.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from dpif.models import (
    AnalysisMethod,
    CheckpointStatus,
    EvidenceRecord,
    Finding,
    Rule,
    Severity,
)
from dpif.scalability import analyzers

_LEVEL_MAP: dict[str, tuple[CheckpointStatus, Severity, int]] = {
    "CRITICAL": (CheckpointStatus.FAIL, Severity.CRITICAL, 4),
    "HIGH": (CheckpointStatus.FAIL, Severity.HIGH, 3),
    "FAIL": (CheckpointStatus.FAIL, Severity.HIGH, 3),
    "MEDIUM": (CheckpointStatus.WARN, Severity.MEDIUM, 2),
    "WARN": (CheckpointStatus.WARN, Severity.MEDIUM, 2),
    "LOW": (CheckpointStatus.WARN, Severity.LOW, 1),
    "INFO": (CheckpointStatus.PASS, Severity.INFO, 0),
    "UNKNOWN": (CheckpointStatus.UNKNOWN, Severity.INFO, 0),
}


def _aggregate(
    rule: Rule,
    results: list[dict[str, Any]],
    data: dict[str, Any],
    entity_name: str,
) -> Finding | None:
    hits = [r for r in results if r.get("triggered")]
    if not hits:
        return None

    worst = max(
        hits,
        key=lambda r: _LEVEL_MAP.get(r.get("level", "WARN"), (None, None, 1))[2],
    )
    level = worst.get("level", "WARN")
    status, severity, _ = _LEVEL_MAP.get(level, (CheckpointStatus.WARN, Severity.MEDIUM, 2))

    if rule.severity == Severity.CRITICAL and status == CheckpointStatus.FAIL:
        severity = Severity.CRITICAL
    if rule.cap_status == "WARN" and status == CheckpointStatus.FAIL:
        status = CheckpointStatus.WARN

    evidence_lines: list[str] = []
    for r in hits:
        if r.get("evidence"):
            evidence_lines.extend(r["evidence"])

    observed = worst.get("observed", {})
    expected = worst.get("expected", {})
    recommendation = worst.get("recommendation", rule.recommendation)
    confidence = float(worst.get("confidence", 0.8))
    if status == CheckpointStatus.UNKNOWN:
        confidence = 0.0

    prov = worst.get("provenance")
    if prov is not None and hasattr(prov, "value"):
        prov_str = str(prov.value)
    elif prov is not None:
        prov_str = str(prov)
    else:
        prov_str = "PROJECTED"
    pipeline_name = str(data.get("pipeline_name", "unknown"))

    evidence = EvidenceRecord(
        rule_id=rule.rule_id,
        status=status,
        severity=severity,
        observed=observed,
        expected=expected,
        evidence=evidence_lines,
        recommendation=recommendation,
        confidence=confidence,
        method=AnalysisMethod.SAMPLE if "projection" in observed else AnalysisMethod.METADATA,
    )

    finding = Finding(
        finding_id=f"{rule.rule_id}-{entity_name}-{status.value.lower()}",
        rule_id=rule.rule_id,
        name=rule.name,
        title=f"{rule.name}: {status.value}",
        description=rule.description,
        category=rule.category,
        status=status,
        severity=severity,
        pipeline_name=pipeline_name,
        timestamp=datetime.now(UTC),
        evidence=evidence,
        recommendation=recommendation,
        confidence=confidence,
        blocking=rule.blocking and status == CheckpointStatus.FAIL,
        assumptions={
            "evidence_provenance": prov_str,
            **dict(data.get("assumptions", {}) or {}),
        },
    )
    return finding


def eval_expected_volume_capacity(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    ratio = float(rule.params.get("warn_ratio", 2.0))
    res = analyzers.analyze_expected_volume_capacity(data, context, warn_ratio=ratio)
    return _aggregate(rule, res, data, "expected-volume")


def eval_peak_volume_capacity(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    ratio = float(rule.params.get("warn_ratio", 2.0))
    res = analyzers.analyze_peak_volume_capacity(data, context, warn_ratio=ratio)
    return _aggregate(rule, res, data, "peak-volume")


def eval_data_growth(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    warn_rate = float(rule.params.get("warn_growth_rate", 20.0))
    warn_gb = float(rule.params.get("warn_projected_gb", 1000.0))
    res = analyzers.analyze_data_growth(
        data, context, warn_growth_rate=warn_rate, warn_projected_gb=warn_gb
    )
    return _aggregate(rule, res, data, "data-growth")


def eval_file_count_growth(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    warn_cnt = int(rule.params.get("warn_file_count", 50000))
    small_kb = float(rule.params.get("small_file_kb", 10240.0))
    res = analyzers.analyze_file_count_growth(
        data, context, warn_file_count=warn_cnt, small_file_kb=small_kb
    )
    return _aggregate(rule, res, data, "file-growth")


def eval_partition_scalability(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    max_sz = float(rule.params.get("max_partition_size_gb", 10.0))
    max_cnt = int(rule.params.get("max_partition_count", 10000))
    res = analyzers.analyze_partition_scalability(
        data, context, max_partition_size_gb=max_sz, max_partition_count=max_cnt
    )
    return _aggregate(rule, res, data, "partition-scalability")


def eval_driver_scalability(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    crit_gb = float(rule.params.get("critical_volume_gb", 50.0))
    res = analyzers.analyze_driver_scalability(data, context, critical_volume_gb=crit_gb)
    return _aggregate(rule, res, data, "driver-scalability")


def eval_shuffle_scalability(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    warn_shuffle = float(rule.params.get("warn_projected_shuffle_gb", 200.0))
    res = analyzers.analyze_shuffle_scalability(
        data, context, warn_projected_shuffle_gb=warn_shuffle
    )
    return _aggregate(rule, res, data, "shuffle-scalability")


def eval_join_scalability(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    res = analyzers.analyze_join_scalability(data, context)
    return _aggregate(rule, res, data, "join-scalability")


def eval_aggregation_scalability(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    hi_vol = float(rule.params.get("high_volume_threshold_gb", 100.0))
    res = analyzers.analyze_aggregation_scalability(data, context, high_volume_threshold_gb=hi_vol)
    return _aggregate(rule, res, data, "agg-scalability")


def eval_cluster_capacity(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    min_w = int(rule.params.get("min_workers_for_large_scale", 4))
    lg_vol = float(rule.params.get("large_scale_volume_gb", 500.0))
    res = analyzers.analyze_cluster_capacity(
        data, context, min_workers_for_large_scale=min_w, large_scale_volume_gb=lg_vol
    )
    return _aggregate(rule, res, data, "cluster-capacity")


def eval_autoscaling_boundary(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    res = analyzers.analyze_autoscaling_boundary(data, context)
    return _aggregate(rule, res, data, "autoscaling-boundary")


def eval_sla_scalability(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    res = analyzers.analyze_sla_scalability(data, context)
    return _aggregate(rule, res, data, "sla-scalability")


def eval_runtime_regression(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    res = analyzers.analyze_runtime_regression(data, context)
    return _aggregate(rule, res, data, "runtime-regression")


def eval_reliability_degradation(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    res = analyzers.analyze_reliability_degradation(data, context)
    return _aggregate(rule, res, data, "reliability-degradation")


_EVALUATOR_REGISTRY: dict[
    str,
    Callable[[Rule, dict[str, Any], dict[str, Any] | None], Finding | None],
] = {
    "eval_expected_volume_capacity": eval_expected_volume_capacity,
    "eval_peak_volume_capacity": eval_peak_volume_capacity,
    "eval_data_growth": eval_data_growth,
    "eval_file_count_growth": eval_file_count_growth,
    "eval_partition_scalability": eval_partition_scalability,
    "eval_driver_scalability": eval_driver_scalability,
    "eval_shuffle_scalability": eval_shuffle_scalability,
    "eval_join_scalability": eval_join_scalability,
    "eval_aggregation_scalability": eval_aggregation_scalability,
    "eval_cluster_capacity": eval_cluster_capacity,
    "eval_autoscaling_boundary": eval_autoscaling_boundary,
    "eval_sla_scalability": eval_sla_scalability,
    "eval_runtime_regression": eval_runtime_regression,
    "eval_reliability_degradation": eval_reliability_degradation,
}


def resolve(
    name: str,
) -> Callable[[Rule, dict[str, Any], dict[str, Any] | None], Finding | None] | None:
    """Resolve an evaluator by function name."""
    return _EVALUATOR_REGISTRY.get(name)
