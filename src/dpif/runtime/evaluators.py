"""Bridge Evaluators for Runtime Performance Rules (Phase 7).

Maps YAML rule declarations to pure analyzer detectors in `dpif.runtime.analyzers`,
constructing typed `Finding` and `EvidenceRecord` instances.
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
from dpif.runtime import analyzers

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


def _get_contract(data: dict[str, Any], context: dict[str, Any] | None) -> Any:
    if context and "pipeline_contract" in context:
        return context["pipeline_contract"]
    return data.get("pipeline_contract") or data.get("contract")


def _get_runtime_data(data: dict[str, Any], context: dict[str, Any] | None) -> Any:
    if "runtime_run" in data and data["runtime_run"] is not None:
        return data["runtime_run"]
    if "runtime" in data and data["runtime"] is not None:
        return data["runtime"]
    if context and "runtime_run" in context and context["runtime_run"] is not None:
        return context["runtime_run"]
    if context and "runtime" in context and context["runtime"] is not None:
        return context["runtime"]
    return None


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

    ev_source = str(data.get("evidence_source", "DATABRICKS_API"))
    pipeline_name = str(data.get("pipeline_name", "unknown"))

    return Finding(
        finding_id=f"{rule.rule_id}-{entity_name}",
        rule_id=rule.rule_id,
        name=rule.name,
        title=rule.name,
        description=rule.description,
        category=rule.category or "performance",
        pipeline_name=pipeline_name,
        status=status,
        severity=severity,
        recommendation=recommendation,
        confidence=confidence,
        blocking=rule.blocking and status == CheckpointStatus.FAIL,
        assumptions={"entity": entity_name, "evidence_source": ev_source},
        evidence=EvidenceRecord(
            rule_id=rule.rule_id,
            status=status,
            severity=severity,
            observed=observed,
            expected=expected,
            evidence=evidence_lines,
            recommendation=recommendation,
            confidence=confidence,
            method=AnalysisMethod.METADATA,
        ),
        timestamp=datetime.now(UTC),
    )


# ====================================================================
# Evaluator Functions
# ====================================================================


def eval_stage_duration(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    run = _get_runtime_data(data, context)
    if not run:
        return None
    contract = _get_contract(data, context)
    warn_sec = float(rule.params.get("warn_seconds", 600.0))
    fail_sec = float(rule.params.get("fail_seconds", 1800.0))
    res = analyzers.analyze_stage_duration(
        run, contract, warn_seconds=warn_sec, fail_seconds=fail_sec
    )
    return _aggregate(rule, res, data, "stage-duration")


def eval_shuffle_volume(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    run = _get_runtime_data(data, context)
    if not run:
        return None
    contract = _get_contract(data, context)
    warn_gb = float(rule.params.get("warn_gb", 100.0))
    fail_gb = float(rule.params.get("fail_gb", 500.0))
    res = analyzers.analyze_shuffle_volume(run, contract, warn_gb=warn_gb, fail_gb=fail_gb)
    return _aggregate(rule, res, data, "shuffle-volume")


def eval_shuffle_input_ratio(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    run = _get_runtime_data(data, context)
    if not run:
        return None
    contract = _get_contract(data, context)
    warn_r = float(rule.params.get("warn_ratio", 2.0))
    fail_r = float(rule.params.get("fail_ratio", 5.0))
    res = analyzers.analyze_shuffle_input_ratio(run, contract, warn_ratio=warn_r, fail_ratio=fail_r)
    return _aggregate(rule, res, data, "shuffle-input-ratio")


def eval_task_duration_imbalance(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    run = _get_runtime_data(data, context)
    if not run:
        return None
    contract = _get_contract(data, context)
    warn_r = float(rule.params.get("warn_ratio", 3.0))
    fail_r = float(rule.params.get("fail_ratio", 6.0))
    res = analyzers.analyze_task_duration_imbalance(
        run, contract, warn_ratio=warn_r, fail_ratio=fail_r
    )
    return _aggregate(rule, res, data, "task-imbalance")


def eval_data_skew(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    run = _get_runtime_data(data, context)
    if not run:
        return None
    contract = _get_contract(data, context)
    warn_r = float(rule.params.get("warn_skew_ratio", 3.0))
    fail_r = float(rule.params.get("fail_skew_ratio", 6.0))
    res = analyzers.analyze_data_skew(run, contract, warn_skew_ratio=warn_r, fail_skew_ratio=fail_r)
    return _aggregate(rule, res, data, "data-skew")


def eval_memory_spill(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    run = _get_runtime_data(data, context)
    if not run:
        return None
    contract = _get_contract(data, context)
    warn_gb = float(rule.params.get("warn_spill_gb", 1.0))
    fail_gb = float(rule.params.get("fail_spill_gb", 10.0))
    res = analyzers.analyze_memory_spill(
        run, contract, warn_spill_gb=warn_gb, fail_spill_gb=fail_gb
    )
    return _aggregate(rule, res, data, "memory-spill")


def eval_disk_spill(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    run = _get_runtime_data(data, context)
    if not run:
        return None
    contract = _get_contract(data, context)
    warn_gb = float(rule.params.get("warn_spill_gb", 0.5))
    fail_gb = float(rule.params.get("fail_spill_gb", 5.0))
    res = analyzers.analyze_disk_spill(run, contract, warn_spill_gb=warn_gb, fail_spill_gb=fail_gb)
    return _aggregate(rule, res, data, "disk-spill")


def eval_gc_time(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    run = _get_runtime_data(data, context)
    if not run:
        return None
    contract = _get_contract(data, context)
    warn_r = float(rule.params.get("warn_gc_ratio", 0.10))
    fail_r = float(rule.params.get("fail_gc_ratio", 0.20))
    res = analyzers.analyze_gc_time(run, contract, warn_gc_ratio=warn_r, fail_gc_ratio=fail_r)
    return _aggregate(rule, res, data, "gc-overhead")


def eval_task_failures(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    run = _get_runtime_data(data, context)
    if not run:
        return None
    contract = _get_contract(data, context)
    max_f = int(rule.params.get("max_failed_tasks", 0))
    res = analyzers.analyze_task_failures_and_retries(run, contract, max_failed_tasks=max_f)
    return _aggregate(rule, res, data, "task-failures")


def eval_executor_failures(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    run = _get_runtime_data(data, context)
    if not run:
        return None
    contract = _get_contract(data, context)
    res = analyzers.analyze_executor_failures(run, contract)
    return _aggregate(rule, res, data, "executor-failures")


def eval_cluster_utilization(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    run = _get_runtime_data(data, context)
    if not run:
        return None
    contract = _get_contract(data, context)
    min_cpu = float(rule.params.get("min_cpu_utilization", 0.30))
    res = analyzers.analyze_cluster_utilization(run, contract, min_cpu_utilization=min_cpu)
    return _aggregate(rule, res, data, "cluster-utilization")


def eval_long_tail_tasks(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    run = _get_runtime_data(data, context)
    if not run:
        return None
    contract = _get_contract(data, context)
    ratio = float(rule.params.get("tail_ratio_threshold", 4.0))
    res = analyzers.analyze_long_tail_tasks(run, contract, tail_ratio_threshold=ratio)
    return _aggregate(rule, res, data, "long-tail-tasks")


_EVALUATOR_REGISTRY: dict[
    str,
    Callable[[Rule, dict[str, Any], dict[str, Any] | None], Finding | None],
] = {
    "eval_stage_duration": eval_stage_duration,
    "eval_shuffle_volume": eval_shuffle_volume,
    "eval_shuffle_input_ratio": eval_shuffle_input_ratio,
    "eval_task_duration_imbalance": eval_task_duration_imbalance,
    "eval_data_skew": eval_data_skew,
    "eval_memory_spill": eval_memory_spill,
    "eval_disk_spill": eval_disk_spill,
    "eval_gc_time": eval_gc_time,
    "eval_task_failures": eval_task_failures,
    "eval_executor_failures": eval_executor_failures,
    "eval_cluster_utilization": eval_cluster_utilization,
    "eval_long_tail_tasks": eval_long_tail_tasks,
}


def resolve(
    name: str,
) -> Callable[[Rule, dict[str, Any], dict[str, Any] | None], Finding | None] | None:
    """Resolve evaluator name to callable."""
    return _EVALUATOR_REGISTRY.get(name)
