"""Databricks Environment Evaluators (Phase 6).

Context-aware bridge functions connecting rules to pure analyzers.
Produces shared Finding models with explicit Expected vs Implemented vs Actual evidence.
"""

from __future__ import annotations

from typing import Any

from dpif.discovery import analyzers
from dpif.models import (
    AnalysisMethod,
    CheckpointStatus,
    EvidenceRecord,
    Finding,
    Rule,
    Severity,
)

_LEVEL_MAP: dict[str, tuple[CheckpointStatus, Severity, int]] = {
    "CRITICAL": (CheckpointStatus.FAIL, Severity.CRITICAL, 4),
    "HIGH": (CheckpointStatus.FAIL, Severity.HIGH, 3),
    "FAIL": (CheckpointStatus.FAIL, Severity.HIGH, 3),
    "MEDIUM": (CheckpointStatus.WARN, Severity.MEDIUM, 2),
    "WARN": (CheckpointStatus.WARN, Severity.MEDIUM, 2),
    "LOW": (CheckpointStatus.WARN, Severity.LOW, 1),
    "INFO": (CheckpointStatus.PASS, Severity.INFO, 0),
}


def _aggregate(
    rule: Rule,
    results: list[dict[str, Any]],
    data: dict[str, Any],
    entity_name: str,
) -> Finding | None:
    """Aggregate detector results into a single Finding."""
    hits = [r for r in results if r.get("triggered")]
    if not hits:
        return None

    worst = max(hits, key=lambda r: _LEVEL_MAP.get(r.get("level", "WARN"), (None, None, 1))[2])
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

    ev_source = str(data.get("evidence_source", "DATABRICKS_API"))
    assumptions = {"entity": entity_name, "evidence_source": ev_source}
    pipeline_name = str(data.get("pipeline_name", "unknown"))
    category = rule.category or "cluster"

    return Finding(
        finding_id=f"{rule.rule_id}-{entity_name}",
        rule_id=rule.rule_id,
        name=rule.name,
        title=rule.name,
        description=rule.description,
        category=category,
        pipeline_name=pipeline_name,
        status=status,
        severity=severity,
        recommendation=recommendation,
        confidence=confidence,
        blocking=rule.blocking and status == CheckpointStatus.FAIL,
        assumptions=assumptions,
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
    )


def _get_contract(data: dict[str, Any], context: dict[str, Any] | None) -> Any:
    if context and "pipeline_contract" in context:
        return context["pipeline_contract"]
    return data.get("pipeline_contract") or data.get("contract")


# ====================================================================
# Job Rule Evaluators
# ====================================================================


def eval_job_retries(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    job = (
        data.get("job")
        or data.get("job_config")
        or (context.get("job_config") if context else None)
    )
    if not job:
        return None
    contract = _get_contract(data, context)
    min_retries = int(rule.params.get("min_retries", 1))
    results = analyzers.analyze_job_retries(job, contract, min_retries=min_retries)
    return _aggregate(rule, results, data, "job-retries")


def eval_task_timeout(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    job = (
        data.get("job")
        or data.get("job_config")
        or (context.get("job_config") if context else None)
    )
    if not job:
        return None
    contract = _get_contract(data, context)
    min_timeout = int(rule.params.get("min_timeout_seconds", 60))
    results = analyzers.analyze_task_timeouts(job, contract, min_timeout_seconds=min_timeout)
    return _aggregate(rule, results, data, "task-timeout")


def eval_excessive_retries(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    job = (
        data.get("job")
        or data.get("job_config")
        or (context.get("job_config") if context else None)
    )
    if not job:
        return None
    contract = _get_contract(data, context)
    max_retries = int(rule.params.get("max_allowed_retries", 5))
    results = analyzers.analyze_excessive_retries(job, contract, max_allowed_retries=max_retries)
    return _aggregate(rule, results, data, "excessive-retries")


def eval_concurrency(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    job = (
        data.get("job")
        or data.get("job_config")
        or (context.get("job_config") if context else None)
    )
    if not job:
        return None
    contract = _get_contract(data, context)
    max_concurrency = int(rule.params.get("max_allowed_concurrency", 1))
    results = analyzers.analyze_concurrency(job, contract, max_allowed_concurrency=max_concurrency)
    return _aggregate(rule, results, data, "job-concurrency")


def eval_schedule_mismatch(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    job = (
        data.get("job")
        or data.get("job_config")
        or (context.get("job_config") if context else None)
    )
    contract = _get_contract(data, context)
    if not job or not contract:
        return None
    results = analyzers.analyze_schedule_mismatch(job, contract)
    return _aggregate(rule, results, data, "schedule-mismatch")


def eval_source_mismatch(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    job = (
        data.get("job")
        or data.get("job_config")
        or (context.get("job_config") if context else None)
    )
    contract = _get_contract(data, context)
    if not job or not contract:
        return None
    results = analyzers.analyze_source_mismatch(job, contract)
    return _aggregate(rule, results, data, "source-mismatch")


# ====================================================================
# Cluster Rule Evaluators
# ====================================================================


def eval_cluster_runtime(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    cluster = (
        data.get("cluster")
        or data.get("cluster_config")
        or (context.get("cluster_config") if context else None)
    )
    if not cluster:
        return None
    contract = _get_contract(data, context)
    min_ver = str(rule.params.get("min_version", "14.3"))
    approved = rule.params.get("approved_versions")
    results = analyzers.analyze_cluster_runtime(
        cluster, contract, approved_versions=approved, min_version=min_ver
    )
    return _aggregate(rule, results, data, "cluster-runtime")


def eval_autoscaling(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    cluster = (
        data.get("cluster")
        or data.get("cluster_config")
        or (context.get("cluster_config") if context else None)
    )
    if not cluster:
        return None
    contract = _get_contract(data, context)
    require_auto = bool(rule.params.get("require_autoscaling", False))
    results = analyzers.analyze_autoscaling(cluster, contract, require_autoscaling=require_auto)
    return _aggregate(rule, results, data, "cluster-autoscaling")


def eval_autoscaling_range(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    cluster = (
        data.get("cluster")
        or data.get("cluster_config")
        or (context.get("cluster_config") if context else None)
    )
    if not cluster:
        return None
    contract = _get_contract(data, context)
    max_ratio = float(rule.params.get("max_ratio", 10.0))
    max_workers = int(rule.params.get("max_workers_limit", 128))
    results = analyzers.analyze_autoscaling_range(
        cluster, contract, max_ratio=max_ratio, max_workers_limit=max_workers
    )
    return _aggregate(rule, results, data, "autoscaling-range")


def eval_photon(rule: Rule, data: dict[str, Any], context: dict[str, Any] | None) -> Finding | None:
    cluster = (
        data.get("cluster")
        or data.get("cluster_config")
        or (context.get("cluster_config") if context else None)
    )
    if not cluster:
        return None
    contract = _get_contract(data, context)
    results = analyzers.analyze_photon(cluster, contract)
    return _aggregate(rule, results, data, "cluster-photon")


def eval_cluster_policy(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    cluster = (
        data.get("cluster")
        or data.get("cluster_config")
        or (context.get("cluster_config") if context else None)
    )
    policy = data.get("cluster_policy") or (context.get("cluster_policy") if context else None)
    if not cluster or not policy:
        return None
    results = analyzers.analyze_cluster_policy(cluster, policy)
    return _aggregate(rule, results, data, "cluster-policy")


def eval_autotermination(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    cluster = (
        data.get("cluster")
        or data.get("cluster_config")
        or (context.get("cluster_config") if context else None)
    )
    if not cluster:
        return None
    contract = _get_contract(data, context)
    max_auto = int(rule.params.get("max_autotermination_minutes", 60))
    results = analyzers.analyze_autotermination(
        cluster, contract, max_autotermination_minutes=max_auto
    )
    return _aggregate(rule, results, data, "autotermination")


EVALUATORS = {
    "eval_job_retries": eval_job_retries,
    "job_retries": eval_job_retries,
    "eval_task_timeout": eval_task_timeout,
    "task_timeout": eval_task_timeout,
    "eval_excessive_retries": eval_excessive_retries,
    "excessive_retries": eval_excessive_retries,
    "eval_concurrency": eval_concurrency,
    "concurrency": eval_concurrency,
    "eval_schedule_mismatch": eval_schedule_mismatch,
    "schedule_mismatch": eval_schedule_mismatch,
    "eval_source_mismatch": eval_source_mismatch,
    "source_mismatch": eval_source_mismatch,
    "eval_cluster_runtime": eval_cluster_runtime,
    "cluster_runtime": eval_cluster_runtime,
    "eval_autoscaling": eval_autoscaling,
    "autoscaling": eval_autoscaling,
    "eval_autoscaling_range": eval_autoscaling_range,
    "autoscaling_range": eval_autoscaling_range,
    "eval_photon": eval_photon,
    "photon": eval_photon,
    "eval_cluster_policy": eval_cluster_policy,
    "cluster_policy": eval_cluster_policy,
    "eval_autotermination": eval_autotermination,
    "autotermination": eval_autotermination,
}


def resolve(name: str) -> Any:
    """Resolve an evaluator name to a callable."""
    return EVALUATORS.get(name) or globals().get(f"eval_{name}") or globals().get(name)
