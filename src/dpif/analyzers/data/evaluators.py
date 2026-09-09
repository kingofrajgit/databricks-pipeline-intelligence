"""Evaluator registry: YAML ``evaluator:`` names -> analyzer functions.

Each evaluator has signature ``(rule, data, context) -> Finding | None``
where ``data`` carries ``observed``/``text``/``pipeline_name``/``assumptions``
and ``context`` carries thresholds, expected schemas, and evidence-source
labels. Nothing is ever invented: absent inputs yield ``None`` (no finding)
and the checkpoint keeps its UNKNOWN skeleton.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from dpif.analyzers.data import distribution, formats, jdbc, partitions, schema
from dpif.analyzers.data import small_files as sf
from dpif.analyzers.data import volume as volume_mod
from dpif.models import CheckpointStatus, EvidenceRecord, Finding, Rule, Severity

Evaluator = Callable[[Rule, dict[str, Any], dict[str, Any]], "Finding | None"]


def _thresholds(context: dict[str, Any]) -> dict[str, Any]:
    return dict(context.get("thresholds", {}) or {})


def _provenance(context: dict[str, Any], observed: dict[str, Any]) -> tuple[str, str]:
    method = str(observed.get("collection_method") or context.get("collection_method") or "unknown")
    source = str(context.get("evidence_source") or observed.get("evidence_source") or "")
    if not source:
        source = "fixture metadata" if method == "fixture" else "contract/profile metadata"
    return method, source


def make_finding(
    rule: Rule,
    status: CheckpointStatus,
    observed: dict[str, Any],
    expected: dict[str, Any],
    evidence: list[str],
    recommendation: str,
    confidence: float,
    assumptions: dict[str, Any],
    pipeline_name: str,
) -> Finding:
    now = datetime.now(UTC)
    ev = EvidenceRecord(
        rule_id=rule.rule_id,
        status=status,
        severity=rule.severity,
        observed=observed,
        expected=expected,
        evidence=evidence,
        recommendation=recommendation,
        confidence=confidence,
    )
    return Finding(
        finding_id=f"{rule.rule_id}-{rule.category}",
        rule_id=rule.rule_id,
        name=rule.name,
        title=rule.name,
        description=rule.description,
        category=rule.category,
        status=status,
        severity=rule.severity,
        pipeline_name=pipeline_name,
        timestamp=now,
        evidence=ev,
        recommendation=recommendation,
        confidence=confidence,
        blocking=rule.blocking,
        assumptions=assumptions,
    )


def _finding_from_result(
    rule: Rule,
    result: dict[str, Any],
    data: dict[str, Any],
    context: dict[str, Any],
    fail_status: CheckpointStatus = CheckpointStatus.FAIL,
) -> Finding | None:
    if not result.get("triggered"):
        return None
    observed = dict(result.get("observed", {}))
    method, source = _provenance(context, observed)
    observed["collection_method"] = method
    observed["evidence_source"] = source
    assumptions = dict(result.get("assumptions", {}))
    assumptions.setdefault("evidence_source", source)
    assumptions.setdefault("collection_method", method)
    assumptions.update(data.get("assumptions", {}) or {})
    status = fail_status
    if rule.severity in (Severity.LOW, Severity.INFO) and fail_status == CheckpointStatus.FAIL:
        status = CheckpointStatus.WARN
    return make_finding(
        rule,
        status,
        observed,
        dict(result.get("expected", {})),
        [f"evidence source: {source}", f"collection method: {method}"],
        str(result.get("recommendation", rule.recommendation)),
        float(result.get("confidence", 0.8)),
        assumptions,
        str(data.get("pipeline_name", "unknown")),
    )


def _profile_observed(data: dict[str, Any]) -> dict[str, Any]:
    obs = dict(data.get("observed", {}) or {})
    # Engine packs DataProfile.to_dict() here; keep the full distribution.
    return obs


def eval_small_files(rule: Rule, data: dict, context: dict) -> Finding | None:
    obs = _profile_observed(data)
    th = _thresholds(context)
    result = sf.analyze_small_files(
        obs,
        format_name=str(obs.get("format") or context.get("source_format") or "unknown"),
        base_threshold_kb=float(th.get("small_file_threshold_kb", 1000.0)),
        min_files=int(th.get("small_file_min_files", 1000)),
        factors=th.get("format_factors"),
    )
    if result.get("triggered"):
        dist = distribution.describe_distribution(obs)
        result["observed"] = {**result["observed"], **{k: v for k, v in dist.items()}}
    return _finding_from_result(
        rule,
        result,
        data,
        context,
        CheckpointStatus.WARN if rule.severity != Severity.CRITICAL else CheckpointStatus.FAIL,
    )


def eval_excessive_count(rule: Rule, data: dict, context: dict) -> Finding | None:
    obs = _profile_observed(data)
    th = _thresholds(context)
    result = sf.analyze_file_count(obs, int(th.get("excessive_file_count", 1000000)))
    return _finding_from_result(rule, result, data, context, CheckpointStatus.WARN)


def eval_partition_imbalance(rule: Rule, data: dict, context: dict) -> Finding | None:
    obs = _profile_observed(data)
    th = _thresholds(context)
    result = partitions.analyze_partitions(
        obs.get("partition_sizes_gb"),
        obs.get("partition_record_counts"),
        float(obs.get("total_gb", 0.0) or 0.0),
        float(th.get("partition_imbalance_ratio", 5.0)),
        float(th.get("partition_imbalance_min_gb", 10.0)),
    )
    return _finding_from_result(rule, result, data, context, CheckpointStatus.WARN)


def eval_schema_drift(rule: Rule, data: dict, context: dict) -> Finding | None:
    expected = context.get("expected_schema")
    observed_cols = _profile_observed(data).get("schema_columns", [])
    result = schema.compare_schemas(
        expected, {"columns": observed_cols}, context.get("schema_policy")
    )
    if result["status"] in ("UNKNOWN", "PASS"):
        return None
    status = CheckpointStatus[result["status"]]
    enriched = {
        "triggered": True,
        "observed": {
            "missing": result["missing"],
            "unexpected": result["unexpected"],
            "type_changes": result["type_changes"],
            "nullable_changes": result["nullable_changes"],
        },
        "expected": {"drift": "none"},
        "recommendation": rule.recommendation
        or (
            "Align the observed schema with the expected contract schema; "
            "gate deployments on schema compatibility."
        ),
        "confidence": 0.85,
        "assumptions": result["assumptions"],
    }
    return _finding_from_result(rule, enriched, data, context, status)


def eval_missing_volume(rule: Rule, data: dict, context: dict) -> Finding | None:
    result = volume_mod.analyze_volume_presence(
        context.get("expected_volume_gb"), context.get("peak_volume_gb")
    )
    return _finding_from_result(rule, result, data, context, CheckpointStatus.WARN)


def eval_source_format(rule: Rule, data: dict, context: dict) -> Finding | None:
    obs = dict(data.get("observed", {}) or {})
    fmt = str(obs.get("format") or context.get("source_format") or "unknown")
    vol = context.get("expected_volume_gb", obs.get("expected_volume_gb"))
    try:
        vol_f = float(vol) if vol is not None else None
    except (TypeError, ValueError):
        vol_f = None
    stype = str(obs.get("source_type") or context.get("source_type") or "")
    result = formats.analyze_format(fmt, vol_f, stype)
    level = result.get("level", "WARN")
    status = CheckpointStatus.FAIL if level == "FAIL" else CheckpointStatus.WARN
    return _finding_from_result(rule, result, data, context, status)


def eval_incremental_strategy(rule: Rule, data: dict, context: dict) -> Finding | None:
    mode = str(
        context.get("ingestion_mode") or data.get("observed", {}).get("ingestion_mode") or "unknown"
    ).lower()
    try:
        vol = float(context.get("expected_volume_gb") or 0.0)
    except (TypeError, ValueError):
        vol = 0.0
    th = _thresholds(context)
    large = vol >= float(th.get("large_volume_gb", 500.0))
    incremental = mode in ("incremental", "cdc", "streaming", "micro_batch")
    has_evidence = bool(
        (context.get("jdbc") or {}).get("incremental_column")
        or (context.get("streaming") or {}).get("checkpoint_location")
        or (context.get("partitioning") or [])
    )
    if incremental or has_evidence or not large:
        return None
    result = {
        "triggered": True,
        "observed": {"ingestion_mode": mode, "expected_volume_gb": vol},
        "expected": {"incremental_strategy": "documented for large volumes"},
        "recommendation": rule.recommendation
        or (
            "Define an incremental strategy (watermark, CDC, key-based merge) "
            "for this large-volume source."
        ),
        "confidence": 0.8,
        "assumptions": {"large-no-incremental": f"{vol} GB without incremental evidence"},
    }
    return _finding_from_result(rule, result, data, context, CheckpointStatus.FAIL)


def eval_jdbc_parallel(rule: Rule, data: dict, context: dict) -> Finding | None:
    obs = data.get("observed", {}) if isinstance(data.get("observed"), dict) else {}
    jdbc_meta = context.get("jdbc") or obs.get("jdbc")
    source_type = str(context.get("source_type") or obs.get("source_type") or "").lower()
    if jdbc_meta is None and source_type != "jdbc":
        return None  # not a JDBC source: rule does not apply
    th = _thresholds(context)
    try:
        vol = float(context.get("expected_volume_gb") or 0.0)
    except (TypeError, ValueError):
        vol = 0.0
    result = jdbc.analyze_jdbc(jdbc_meta, vol, float(th.get("jdbc_parallel_min_gb", 100.0)))
    return _finding_from_result(rule, result, data, context)


def eval_growth_info(rule: Rule, data: dict, context: dict) -> Finding | None:
    """Growth is informational: projections surface in the CLI, not findings."""
    return None


EVALUATORS: dict[str, Evaluator] = {
    "small_files": eval_small_files,
    "excessive_count": eval_excessive_count,
    "partition_imbalance": eval_partition_imbalance,
    "schema_drift": eval_schema_drift,
    "missing_volume": eval_missing_volume,
    "source_format": eval_source_format,
    "incremental_strategy": eval_incremental_strategy,
    "jdbc_parallel": eval_jdbc_parallel,
    "growth_info": eval_growth_info,
}


def _register_code_evaluators() -> None:
    """Merge Phase 4 AST evaluators (lazy import avoids a hard dependency)."""
    try:
        from dpif.code import evaluators as code_evaluators
    except Exception:
        return
    for name, fn in code_evaluators.EVALUATORS.items():
        EVALUATORS.setdefault(name, fn)


_register_code_evaluators()


def resolve(name: str) -> Evaluator | None:
    return EVALUATORS.get(name or "")
