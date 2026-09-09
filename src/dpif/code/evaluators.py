"""Contextual evaluators for CODE-PYSPARK-006…015 (Phase 4).

Each evaluator consumes a ``CodeAnalysis`` (pre-built by the checkpoint
engine, or parsed on demand from ``data['text']``) plus the merged rule
context (volumes, source info) and returns one aggregated Finding or None.
Thresholds come from the YAML rule ``params:`` — never hard-coded here.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from dpif.code import pyspark as detectors
from dpif.code.models import CodeAnalysis
from dpif.code.parser import analyze_source
from dpif.models import CheckpointStatus, EvidenceRecord, Finding, Rule, Severity

_LEVEL_STATUS = {
    "CRITICAL": CheckpointStatus.FAIL,
    "HIGH": CheckpointStatus.FAIL,
    "MEDIUM": CheckpointStatus.WARN,
    "WARN": CheckpointStatus.WARN,
    "LOW": CheckpointStatus.WARN,
}
_LEVEL_SEVERITY = {
    "CRITICAL": Severity.CRITICAL,
    "HIGH": Severity.HIGH,
    "MEDIUM": Severity.MEDIUM,
    "WARN": Severity.MEDIUM,
    "LOW": Severity.LOW,
}
_SEVERITY_RANK = {
    Severity.INFO: 0,
    Severity.LOW: 1,
    Severity.MEDIUM: 2,
    Severity.HIGH: 3,
    Severity.CRITICAL: 4,
}


def get_analysis(data: dict[str, Any]) -> CodeAnalysis:
    """Reuse the engine-built analysis or parse the snippet on demand."""
    existing = data.get("code_analysis")
    if isinstance(existing, CodeAnalysis):
        return existing
    return analyze_source(
        str(data.get("text", "")),
        filename=str(data.get("source_file", "<code>")),
    )


def _params(rule: Rule, defaults: dict[str, Any]) -> dict[str, Any]:
    merged = dict(defaults)
    merged.update(rule.params or {})
    return merged


def _aggregate(
    rule: Rule,
    results: list[dict[str, Any]],
    data: dict[str, Any],
    filename: str,
) -> Finding | None:
    """Aggregate per-operation detector results into one contextual Finding."""
    hits = [r for r in results if r.get("triggered")]
    if not hits:
        return None
    severities = [_LEVEL_SEVERITY.get(str(r.get("level", "WARN")), Severity.MEDIUM) for r in hits]
    worst = max(severities, key=lambda s: _SEVERITY_RANK[s])
    status = (
        CheckpointStatus.FAIL
        if worst in (Severity.CRITICAL, Severity.HIGH)
        else CheckpointStatus.WARN
    )
    if rule.cap_status.upper() == "WARN" and status == CheckpointStatus.FAIL:
        status = CheckpointStatus.WARN
    top = next(r for r, s in zip(hits, severities, strict=True) if s == worst)
    evidence: list[str] = []
    for r in hits:
        op = r.get("op")
        if op is not None and hasattr(op, "line"):
            location = f"{filename}:{op.line}"
            code = str(getattr(op, "code", "") or "")[:120]
            evidence.append(f"{location} `{code}`" if code else location)
    volume = None
    for key in ("data_size_gb", "table_size_gb", "expected_volume_gb"):
        value = (data.get("context", {}) or {}).get(key)
        if isinstance(value, (int, float)):
            volume = float(value)
            break
    if volume is not None:
        evidence.append(f"Expected input: {volume:.0f} GB/day")
    else:
        evidence.append("Input size: UNKNOWN")
    observed: dict[str, Any] = {
        "operations": [r["observed"] for r in hits],
        "data_context_gb": volume,
    }
    assumptions: dict[str, Any] = {"static-analysis": "no runtime metrics claimed"}
    for r in hits:
        assumptions.update(r.get("assumptions", {}))
    assumptions.update(data.get("assumptions", {}) or {})
    recommendation = str(top.get("recommendation") or rule.recommendation)
    now = datetime.now(UTC)
    return Finding(
        finding_id=f"{rule.rule_id}-{rule.category}",
        rule_id=rule.rule_id,
        name=rule.name,
        title=rule.name,
        description=rule.description,
        category=rule.category,
        status=status,
        severity=worst,
        pipeline_name=str(data.get("pipeline_name", "unknown")),
        timestamp=now,
        evidence=EvidenceRecord(
            rule_id=rule.rule_id,
            status=status,
            severity=worst,
            observed=observed,
            expected={"policy": "see rule params"},
            evidence=evidence,
            recommendation=recommendation,
            confidence=float(top.get("confidence", 0.7)),
        ),
        recommendation=recommendation,
        confidence=float(top.get("confidence", 0.7)),
        blocking=rule.blocking,
        assumptions=assumptions,
    )


def _ctx_dict(data: dict[str, Any], context: dict[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    nested = data.get("context")
    if isinstance(nested, dict):
        merged.update(nested)
    if context:
        merged.update(context)
    return merged


def eval_large_collect(rule: Rule, data: dict, context: dict | None) -> Finding | None:
    p = _params(rule, {"small_gb": 1.0, "critical_gb": 500.0})
    results = detectors.analyze_driver_collect(
        get_analysis(data),
        _ctx_dict(data, context),
        small_gb=float(p["small_gb"]),
        critical_gb=float(p["critical_gb"]),
    )
    return _aggregate(rule, results, data, str(data.get("source_file", "<code>")))


def eval_topandas(rule: Rule, data: dict, context: dict | None) -> Finding | None:
    p = _params(rule, {"small_gb": 1.0, "critical_gb": 500.0})
    results = detectors.analyze_topandas(
        get_analysis(data),
        _ctx_dict(data, context),
        small_gb=float(p["small_gb"]),
        critical_gb=float(p["critical_gb"]),
    )
    return _aggregate(rule, results, data, str(data.get("source_file", "<code>")))


def eval_shuffle(rule: Rule, data: dict, context: dict | None) -> Finding | None:
    p = _params(rule, {"shuffle_gb": 100.0, "high_gb": 1000.0})
    results = detectors.analyze_shuffle_risk(
        get_analysis(data),
        _ctx_dict(data, context),
        shuffle_gb=float(p["shuffle_gb"]),
        high_gb=float(p["high_gb"]),
    )
    return _aggregate(rule, results, data, str(data.get("source_file", "<code>")))


def eval_repartition(rule: Rule, data: dict, context: dict | None) -> Finding | None:
    p = _params(rule, {"max_partitions": 2000})
    results = detectors.analyze_repartition(
        get_analysis(data),
        _ctx_dict(data, context),
        max_partitions=int(p["max_partitions"]),
    )
    return _aggregate(rule, results, data, str(data.get("source_file", "<code>")))


def eval_coalesce(rule: Rule, data: dict, context: dict | None) -> Finding | None:
    p = _params(rule, {"large_gb": 100.0})
    results = detectors.analyze_coalesce(
        get_analysis(data), _ctx_dict(data, context), large_gb=float(p["large_gb"])
    )
    return _aggregate(rule, results, data, str(data.get("source_file", "<code>")))


def eval_cache(rule: Rule, data: dict, context: dict | None) -> Finding | None:
    results = detectors.analyze_cache(get_analysis(data), _ctx_dict(data, context))
    return _aggregate(rule, results, data, str(data.get("source_file", "<code>")))


def eval_actions(rule: Rule, data: dict, context: dict | None) -> Finding | None:
    results = detectors.analyze_repeated_actions(get_analysis(data), _ctx_dict(data, context))
    return _aggregate(rule, results, data, str(data.get("source_file", "<code>")))


def eval_sort(rule: Rule, data: dict, context: dict | None) -> Finding | None:
    p = _params(rule, {"sort_gb": 100.0})
    results = detectors.analyze_global_sort(
        get_analysis(data), _ctx_dict(data, context), sort_gb=float(p["sort_gb"])
    )
    return _aggregate(rule, results, data, str(data.get("source_file", "<code>")))


def eval_window(rule: Rule, data: dict, context: dict | None) -> Finding | None:
    results = detectors.analyze_windows(get_analysis(data), _ctx_dict(data, context))
    return _aggregate(rule, results, data, str(data.get("source_file", "<code>")))


def eval_dedup(rule: Rule, data: dict, context: dict | None) -> Finding | None:
    p = _params(rule, {"dedup_gb": 100.0})
    results = detectors.analyze_dedup(
        get_analysis(data), _ctx_dict(data, context), dedup_gb=float(p["dedup_gb"])
    )
    return _aggregate(rule, results, data, str(data.get("source_file", "<code>")))


def eval_broadcast(rule: Rule, data: dict, context: dict | None) -> Finding | None:
    p = _params(rule, {"broadcast_max_gb": 10.0})
    results = detectors.analyze_broadcast(
        get_analysis(data),
        _ctx_dict(data, context),
        broadcast_max_gb=float(p["broadcast_max_gb"]),
    )
    return _aggregate(rule, results, data, str(data.get("source_file", "<code>")))


def eval_udf(rule: Rule, data: dict, context: dict | None) -> Finding | None:
    results = detectors.analyze_udfs(get_analysis(data), _ctx_dict(data, context))
    return _aggregate(rule, results, data, str(data.get("source_file", "<code>")))


def eval_count_for_existence(rule: Rule, data: dict, context: dict | None) -> Finding | None:
    results = detectors.analyze_count_for_existence(get_analysis(data), _ctx_dict(data, context))
    return _aggregate(rule, results, data, str(data.get("source_file", "<code>")))


def eval_cross_join(rule: Rule, data: dict, context: dict | None) -> Finding | None:
    p = _params(rule, {"cross_max_gb": 10.0})
    results = detectors.analyze_cross_join(
        get_analysis(data),
        _ctx_dict(data, context),
        cross_max_gb=float(p["cross_max_gb"]),
    )
    return _aggregate(rule, results, data, str(data.get("source_file", "<code>")))


EVALUATORS = {
    "large_collect": eval_large_collect,
    "topandas_risk": eval_topandas,
    "shuffle_risk": eval_shuffle,
    "repartition_risk": eval_repartition,
    "coalesce_risk": eval_coalesce,
    "unjustified_cache": eval_cache,
    "repeated_actions": eval_actions,
    "global_sort": eval_sort,
    "window_risk": eval_window,
    "dedup_risk": eval_dedup,
    "broadcast_risk": eval_broadcast,
    "udf_risk": eval_udf,
    "count_for_existence": eval_count_for_existence,
    "cross_join_risk": eval_cross_join,
}


def resolve(name: str) -> Any:
    return EVALUATORS.get(name or "")
