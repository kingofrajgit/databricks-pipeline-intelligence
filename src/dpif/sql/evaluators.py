"""SQL Rule Evaluators (Phase 5).

Each evaluator bridges a YAML rule to an analyzer function.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from dpif.models import CheckpointStatus, EvidenceRecord, Finding, Rule, Severity
from dpif.sql.analyzers import ANALYZERS
from dpif.sql.models import SQLAnalysis

_LEVEL_MAP = {
    "INFO": (CheckpointStatus.PASS, Severity.INFO, 0),
    "LOW": (CheckpointStatus.WARN, Severity.LOW, 1),
    "MEDIUM": (CheckpointStatus.WARN, Severity.MEDIUM, 2),
    "WARN": (CheckpointStatus.WARN, Severity.MEDIUM, 2),
    "HIGH": (CheckpointStatus.FAIL, Severity.HIGH, 3),
    "FAIL": (CheckpointStatus.FAIL, Severity.HIGH, 3),
    "CRITICAL": (CheckpointStatus.FAIL, Severity.CRITICAL, 4),
}


def _ctx_dict(data: dict[str, Any], context: dict[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    nested = data.get("context")
    if isinstance(nested, dict):
        merged.update(nested)
    if context:
        merged.update(context)
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

    worst = max(hits, key=lambda r: _LEVEL_MAP.get(r.get("level", "WARN"), (None, None, 1))[2])
    level = worst.get("level", "WARN")
    status, severity, _ = _LEVEL_MAP.get(level, (CheckpointStatus.WARN, Severity.MEDIUM, 2))

    if rule.severity == Severity.CRITICAL and status == CheckpointStatus.FAIL:
        severity = Severity.CRITICAL

    if rule.cap_status == "WARN" and status == CheckpointStatus.FAIL:
        status = CheckpointStatus.WARN

    evidence: list[str] = []
    for r in hits:
        if r.get("evidence"):
            if isinstance(r["evidence"], list):
                evidence.extend(str(e) for e in r["evidence"] if e)
            else:
                evidence.append(str(r["evidence"]))
        elif r.get("line"):
            evidence.append(f"{filename}:{r['line']}")

    vol = None
    merged_ctx = _ctx_dict(data, None)
    for key in ("data_size_gb", "table_size_gb", "expected_volume_gb"):
        val = merged_ctx.get(key)
        if isinstance(val, (int, float)):
            vol = float(val)
            break

    if vol is not None:
        evidence.append(f"Expected input: {vol:.0f} GB")
    elif any("context-unavailable" in r.get("assumptions", {}) for r in hits):
        evidence.append("Input size: UNKNOWN")

    observed: dict[str, Any] = {
        "operations": [r["observed"] for r in hits if "observed" in r],
        "data_context_gb": vol,
    }
    assumptions: dict[str, Any] = {"static-analysis": "no runtime metrics claimed"}
    for r in hits:
        assumptions.update(r.get("assumptions", {}))
    assumptions.update(data.get("assumptions", {}) or {})

    blocking = rule.blocking or any(r.get("blocking", False) for r in hits)
    recommendation = str(worst.get("recommendation") or rule.recommendation)
    confidence = float(worst.get("confidence", 0.75))

    return Finding(
        finding_id=f"{rule.rule_id}-{rule.category}",
        rule_id=rule.rule_id,
        name=rule.name,
        title=rule.name,
        description=rule.description,
        category=rule.category,
        status=status,
        severity=severity,
        pipeline_name=str(data.get("pipeline_name", "unknown")),
        timestamp=datetime.now(UTC),
        evidence=EvidenceRecord(
            rule_id=rule.rule_id,
            status=status,
            severity=severity,
            observed=observed,
            expected={"policy": "see rule params"},
            evidence=evidence,
            recommendation=recommendation,
            confidence=confidence,
        ),
        recommendation=recommendation,
        confidence=confidence,
        blocking=blocking,
        assumptions=assumptions,
    )


def get_analysis(data: dict[str, Any]) -> SQLAnalysis | None:
    """Get or create SQLAnalysis from data."""
    if "sql_analysis" in data:
        sa = data["sql_analysis"]
        if isinstance(sa, SQLAnalysis):
            return sa
        if isinstance(sa, dict):
            return SQLAnalysis.model_validate(sa)

    code_analysis = data.get("code_analysis")
    if code_analysis is not None:
        sa = getattr(code_analysis, "sql_analysis", None)
        if isinstance(sa, SQLAnalysis):
            return sa
        if getattr(code_analysis, "language", "") == "python":
            return None

    filename = str(data.get("source_file", "<sql>"))
    if filename.endswith((".py", ".ipynb")):
        return None

    # Parse on demand for .sql or pure SQL text
    from dpif.sql.parser import parse_sql

    text = str(data.get("text", ""))
    if not text.strip():
        return None
    result = parse_sql(text, filename)
    return result.analysis


def eval_select_star(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    analysis = get_analysis(data)
    if analysis is None:
        return None
    ctx = _ctx_dict(data, context)
    results = ANALYZERS["select_star"](analysis, ctx)
    return _aggregate(rule, results, data, str(data.get("source_file", "<sql>")))


def eval_cross_join(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    analysis = get_analysis(data)
    if analysis is None:
        return None
    ctx = _ctx_dict(data, context)
    results = ANALYZERS["cross_join"](analysis, ctx)
    return _aggregate(rule, results, data, str(data.get("source_file", "<sql>")))


def eval_large_join(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    analysis = get_analysis(data)
    if analysis is None:
        return None
    p = {"large_join_gb": 100.0, "high_gb": 1000.0}
    p.update(rule.params or {})
    ctx = _ctx_dict(data, context)
    results = ANALYZERS["large_join"](
        analysis,
        ctx,
        large_join_gb=float(p.get("large_join_gb", 100.0)),
        high_gb=float(p.get("high_gb", 1000.0)),
    )
    return _aggregate(rule, results, data, str(data.get("source_file", "<sql>")))


def eval_large_agg(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    analysis = get_analysis(data)
    if analysis is None:
        return None
    p = {"large_gb": 100.0, "high_gb": 1000.0}
    p.update(rule.params or {})
    ctx = _ctx_dict(data, context)
    results = ANALYZERS["large_agg"](
        analysis,
        ctx,
        large_gb=float(p.get("large_gb", 100.0)),
        high_gb=float(p.get("high_gb", 1000.0)),
    )
    return _aggregate(rule, results, data, str(data.get("source_file", "<sql>")))


def eval_global_sort(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    analysis = get_analysis(data)
    if analysis is None:
        return None
    p = {"sort_gb": 100.0}
    p.update(rule.params or {})
    ctx = _ctx_dict(data, context)
    results = ANALYZERS["global_sort"](
        analysis,
        ctx,
        sort_gb=float(p.get("sort_gb", 100.0)),
    )
    return _aggregate(rule, results, data, str(data.get("source_file", "<sql>")))


def eval_dedup(rule: Rule, data: dict[str, Any], context: dict[str, Any] | None) -> Finding | None:
    code_analysis = data.get("code_analysis")
    if code_analysis is not None and getattr(code_analysis, "language", "") == "python":
        from dpif.code import evaluators as code_evaluators

        py_finding = code_evaluators.eval_dedup(rule, data, context)
        if py_finding is not None:
            return py_finding

    analysis = get_analysis(data)
    if analysis is None:
        return None
    p = {"dedup_gb": 100.0}
    p.update(rule.params or {})
    ctx = _ctx_dict(data, context)
    results = ANALYZERS["dedup_risk"](
        analysis,
        ctx,
        dedup_gb=float(p.get("dedup_gb", 100.0)),
    )
    return _aggregate(rule, results, data, str(data.get("source_file", "<sql>")))


def eval_window(rule: Rule, data: dict[str, Any], context: dict[str, Any] | None) -> Finding | None:
    analysis = get_analysis(data)
    if analysis is None:
        return None
    ctx = _ctx_dict(data, context)
    results = ANALYZERS["window_risk"](analysis, ctx)
    return _aggregate(rule, results, data, str(data.get("source_file", "<sql>")))


def eval_function_on_column(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    analysis = get_analysis(data)
    if analysis is None:
        return None
    ctx = _ctx_dict(data, context)
    results = ANALYZERS["function_on_column"](analysis, ctx)
    return _aggregate(rule, results, data, str(data.get("source_file", "<sql>")))


def eval_complexity(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    analysis = get_analysis(data)
    if analysis is None:
        return None
    p = {"max_joins": 7, "max_ctes": 10, "max_nesting": 3}
    p.update(rule.params or {})
    ctx = _ctx_dict(data, context)
    results = ANALYZERS["complexity"](
        analysis,
        ctx,
        max_joins=int(p.get("max_joins", 7)),
        max_ctes=int(p.get("max_ctes", 10)),
        max_nesting=int(p.get("max_nesting", 3)),
    )
    return _aggregate(rule, results, data, str(data.get("source_file", "<sql>")))


def eval_parse_error(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    analysis = get_analysis(data)
    if analysis is None:
        return None
    ctx = _ctx_dict(data, context)
    results = ANALYZERS["parse_errors"](analysis, ctx)
    return _aggregate(rule, results, data, str(data.get("source_file", "<sql>")))


def eval_schema_drift(
    rule: Rule, data: dict[str, Any], context: dict[str, Any] | None
) -> Finding | None:
    from dpif.sql.analyzers import analyze_schema_drift

    ctx = _ctx_dict(data, context)
    expected = ctx.get("expected_schema") or data.get("expected", {})
    observed = ctx.get("observed_schema") or data.get("observed", {})
    result = analyze_schema_drift(expected, observed)
    results = [result] if result.get("status") in ("WARN", "FAIL") else []
    return _aggregate(rule, results, data, str(data.get("source_file", "<sql>")))


EVALUATORS = {
    "select_star": eval_select_star,
    "cross_join": eval_cross_join,
    "large_join": eval_large_join,
    "large_agg": eval_large_agg,
    "large_aggregation": eval_large_agg,
    "global_sort": eval_global_sort,
    "dedup_risk": eval_dedup,
    "distinct": eval_dedup,
    "window_risk": eval_window,
    "window": eval_window,
    "function_on_column": eval_function_on_column,
    "function_on_filter": eval_function_on_column,
    "complexity": eval_complexity,
    "parse_errors": eval_parse_error,
    "parse_error": eval_parse_error,
    "schema_drift": eval_schema_drift,
}


def resolve(name: str) -> Any:
    """Resolve an evaluator name to a callable."""
    return EVALUATORS.get(name) or globals().get(f"eval_{name}")
