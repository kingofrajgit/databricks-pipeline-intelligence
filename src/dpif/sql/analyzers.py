"""SQL Analyzers (Phase 5).

Pure functions over SQLAnalysis (or dicts) that return finding dicts.
Each analyzer is a pure function: no I/O, no state, deterministic.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dpif.sql.models import (
    SQLAnalysis,
)

# ----------------------------------------------------------------
# Helper utilities
# ----------------------------------------------------------------


def _to_analysis(analysis: SQLAnalysis | dict[str, Any]) -> SQLAnalysis:
    """Coerce input to SQLAnalysis model."""
    if isinstance(analysis, SQLAnalysis):
        return analysis
    if isinstance(analysis, dict):
        return SQLAnalysis.model_validate(analysis)
    return SQLAnalysis()


def _volume_gb(context: dict[str, Any] | None) -> float | None:
    """Extract data volume from context."""
    if not context:
        return None
    for key in ("data_size_gb", "expected_volume_gb", "peak_volume_gb", "table_size_gb"):
        if key in context and context[key] is not None:
            try:
                return float(context[key])
            except (TypeError, ValueError):
                continue
    return None


# ----------------------------------------------------------------
# Rule: CODE-SQL-001 SELECT * Risk
# ----------------------------------------------------------------


def analyze_select_star(
    analysis: SQLAnalysis | dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Detect SELECT * and table.* patterns."""
    model = _to_analysis(analysis)
    findings: list[dict[str, Any]] = []

    vol = _volume_gb(context)
    assumptions = {"context-unavailable": "table sizes unknown"} if vol is None else {}

    for q in model.queries:
        if q.projection and q.projection.has_wildcard:
            p = q.projection
            wildcard_tables = list(p.wildcard_tables)
            proj_str = ", ".join(p.expressions) if p.expressions else "*"
            evidence = [
                f"Wildcard projection detected: {proj_str}",
            ]
            if p.line:
                evidence.append(f"Line {p.line}")
            if wildcard_tables:
                evidence.append(f"Tables: {', '.join(wildcard_tables)}")

            findings.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "observed": {
                        "wildcard_tables": wildcard_tables,
                        "projection": p.expressions,
                        "line": p.line,
                    },
                    "expected": {"wildcard_usage": "avoid in production"},
                    "evidence": evidence,
                    "recommendation": (
                        "Avoid SELECT * in production; explicitly list required columns "
                        "to reduce data movement and enable schema evolution."
                    ),
                    "confidence": 0.8 if wildcard_tables else 0.75,
                    "assumptions": assumptions,
                    "line": p.line,
                }
            )

    return findings


# ----------------------------------------------------------------
# Rule: CODE-SQL-002 Cross Join Risk
# ----------------------------------------------------------------


def analyze_cross_join(
    analysis: SQLAnalysis | dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Detect CROSS JOIN operations."""
    model = _to_analysis(analysis)
    findings: list[dict[str, Any]] = []

    for j in model.joins:
        if j.join_type == "CROSS":
            left = j.left_table.name
            right = j.right_table.name
            evidence = [f"CROSS JOIN between `{left}` and `{right}`"]
            if j.line:
                evidence.append(f"Line {j.line}")

            findings.append(
                {
                    "triggered": True,
                    "level": "HIGH",
                    "blocking": True,
                    "observed": {
                        "join_type": "CROSS",
                        "left_table": left,
                        "right_table": right,
                        "condition": j.condition,
                        "line": j.line,
                    },
                    "expected": {"join_type": "explicit inner/left/right with ON condition"},
                    "evidence": evidence,
                    "recommendation": (
                        "CROSS JOIN produces Cartesian product; replace with explicit "
                        "join condition using INNER/LEFT/RIGHT JOIN with ON clause."
                    ),
                    "confidence": 0.95,
                    "assumptions": {},
                    "line": j.line,
                }
            )

    return findings


# ----------------------------------------------------------------
# Rule: CODE-SQL-003 Potential Large Join
# ----------------------------------------------------------------


def analyze_large_join(
    analysis: SQLAnalysis | dict[str, Any],
    context: dict[str, Any] | None = None,
    large_join_gb: float = 100.0,
    high_gb: float = 1000.0,
) -> list[dict[str, Any]]:
    """Flag joins on large datasets where shuffle may be significant."""
    model = _to_analysis(analysis)
    if not model.joins:
        return []

    vol = _volume_gb(context)
    if vol is not None and vol < large_join_gb:
        return []

    assumptions: dict[str, Any]
    if vol is not None:
        level = "HIGH" if vol >= high_gb else "MEDIUM"
        confidence = 0.85 if vol >= high_gb else 0.75
        assumptions = {"volume_estimate_gb": vol}
        evidence = [f"Join on ~{vol:.0f} GB input exceeds threshold ({large_join_gb:.0f} GB)"]
    else:
        level = "WARN"
        confidence = 0.5
        assumptions = {
            "context-unavailable": (
                "Input volume unknown; large join risk cannot be verified statically"
            )
        }
        evidence = ["Join detected between tables, but input volume is unknown"]

    join_details = [
        {
            "left": j.left_table.name,
            "right": j.right_table.name,
            "type": str(j.join_type),
            "line": j.line,
        }
        for j in model.joins
    ]

    return [
        {
            "triggered": True,
            "level": level,
            "observed": {
                "join_count": len(model.joins),
                "joins": join_details,
                "volume_gb": vol,
                "threshold_gb": large_join_gb,
            },
            "expected": {"join_optimized": True},
            "evidence": evidence,
            "recommendation": (
                "Potential large shuffle from join; consider broadcast join for small "
                "table, salting for skew, or partitioning strategy."
            ),
            "confidence": confidence,
            "assumptions": assumptions,
            "line": model.joins[0].line if model.joins else 0,
        }
    ]


# ----------------------------------------------------------------
# Rule: CODE-SQL-004 Potential Large Aggregation
# ----------------------------------------------------------------


def analyze_large_aggregation(
    analysis: SQLAnalysis | dict[str, Any],
    context: dict[str, Any] | None = None,
    large_gb: float = 100.0,
    high_gb: float = 1000.0,
) -> list[dict[str, Any]]:
    """Flag GROUP BY / aggregations on large inputs."""
    model = _to_analysis(analysis)
    has_agg = bool(model.aggregations) or any(bool(q.group_by) for q in model.queries)
    if not has_agg:
        return []

    vol = _volume_gb(context)
    if vol is not None and vol < large_gb:
        return []

    assumptions: dict[str, Any]
    if vol is not None:
        level = "HIGH" if vol >= high_gb else "MEDIUM"
        confidence = 0.85 if vol >= high_gb else 0.75
        assumptions = {"volume_estimate_gb": vol}
        evidence = [f"Aggregation on ~{vol:.0f} GB input exceeds threshold ({large_gb:.0f} GB)"]
    else:
        level = "WARN"
        confidence = 0.5
        assumptions = {
            "context-unavailable": "Input volume unknown; aggregation risk cannot be verified"
        }
        evidence = ["Aggregation detected, but input volume is unknown"]

    all_group_by: list[str] = []
    for q in model.queries:
        all_group_by.extend(q.group_by)

    return [
        {
            "triggered": True,
            "level": level,
            "observed": {
                "aggregations": [a.function for a in model.aggregations],
                "group_by": all_group_by,
                "input_gb": vol,
                "threshold_gb": large_gb,
            },
            "expected": {"aggregation_optimized": True},
            "evidence": evidence,
            "recommendation": (
                f"Potential large aggregation shuffle"
                f"{' on ~' + str(int(vol)) + ' GB' if vol else ''}; "
                "consider pre-aggregation, salting, or approximate algorithms."
            ),
            "confidence": confidence,
            "assumptions": assumptions,
            "line": model.aggregations[0].line if model.aggregations else 0,
        }
    ]


# ----------------------------------------------------------------
# Rule: CODE-SQL-005 Global ORDER BY Risk
# ----------------------------------------------------------------


def analyze_global_sort(
    analysis: SQLAnalysis | dict[str, Any],
    context: dict[str, Any] | None = None,
    sort_gb: float = 100.0,
) -> list[dict[str, Any]]:
    """Flag global ORDER BY on large inputs."""
    model = _to_analysis(analysis)
    has_sort = any(bool(q.order_by) for q in model.queries)
    if not has_sort:
        return []

    vol = _volume_gb(context)
    if vol is not None and vol < sort_gb:
        return []

    assumptions: dict[str, Any]
    if vol is not None:
        level = "HIGH" if vol >= 1000.0 else "MEDIUM"
        confidence = 0.8 if vol >= 1000.0 else 0.75
        assumptions = {"volume_estimate_gb": vol}
        evidence = [f"Global ORDER BY on ~{vol:.0f} GB input exceeds threshold ({sort_gb:.0f} GB)"]
    else:
        level = "WARN"
        confidence = 0.5
        assumptions = {"context-unavailable": "Input volume unknown; sort risk cannot be verified"}
        evidence = ["Global ORDER BY detected, but input volume is unknown"]

    all_order_by: list[dict[str, Any]] = []
    first_line = 0
    for q in model.queries:
        for ob in q.order_by:
            if not first_line and ob.line:
                first_line = ob.line
            all_order_by.append({"column": ob.column, "direction": ob.direction, "line": ob.line})

    return [
        {
            "triggered": True,
            "level": level,
            "observed": {
                "order_by": all_order_by,
                "input_gb": vol,
                "threshold_gb": sort_gb,
            },
            "expected": {"global_sort_avoided": True},
            "evidence": evidence,
            "recommendation": (
                f"Global sort{' on ~' + str(int(vol)) + ' GB' if vol else ''} forces wide shuffle; "
                "consider partitioned ordering or avoiding global sort if not strictly necessary."
            ),
            "confidence": confidence,
            "assumptions": assumptions,
            "line": first_line,
        }
    ]


# ----------------------------------------------------------------
# Rule: CODE-SQL-006 Expensive DISTINCT
# ----------------------------------------------------------------


def analyze_distinct(
    analysis: SQLAnalysis | dict[str, Any],
    context: dict[str, Any] | None = None,
    dedup_gb: float = 100.0,
) -> list[dict[str, Any]]:
    """Flag DISTINCT / COUNT(DISTINCT ...) on large inputs."""
    model = _to_analysis(analysis)
    has_distinct = any(q.distinct for q in model.queries) or any(
        agg.distinct or agg.function == "COUNT_DISTINCT" for agg in model.aggregations
    )
    if not has_distinct:
        return []

    vol = _volume_gb(context)
    if vol is not None and vol < dedup_gb:
        return []

    assumptions: dict[str, Any]
    if vol is not None:
        level = "HIGH" if vol >= 1000.0 else "MEDIUM"
        confidence = 0.8 if vol >= 1000.0 else 0.7
        assumptions = {"volume_estimate_gb": vol}
        evidence = [
            f"DISTINCT deduplication on ~{vol:.0f} GB input exceeds threshold ({dedup_gb:.0f} GB)"
        ]
    else:
        level = "WARN"
        confidence = 0.5
        assumptions = {
            "context-unavailable": "Input volume unknown; distinct cost cannot be verified"
        }
        evidence = ["DISTINCT operation detected, but input volume is unknown"]

    return [
        {
            "triggered": True,
            "level": level,
            "observed": {
                "distinct": True,
                "input_gb": vol,
                "threshold_gb": dedup_gb,
            },
            "expected": {"dedup_optimized": True},
            "evidence": evidence,
            "recommendation": (
                f"Potential expensive deduplication"
                f"{' on ~' + str(int(vol)) + ' GB' if vol else ''}; "
                "consider approximate algorithms (approx_count_distinct) or keyed deduplication."
            ),
            "confidence": confidence,
            "assumptions": assumptions,
            "line": 0,
        }
    ]


# ----------------------------------------------------------------
# Rule: CODE-SQL-007 Window Operation Risk
# ----------------------------------------------------------------


def analyze_window(
    analysis: SQLAnalysis | dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Flag unbounded window functions."""
    model = _to_analysis(analysis)
    findings: list[dict[str, Any]] = []

    for w in model.windows:
        if not w.bounded:
            evidence = [f"Window function `{w.function}` without explicit frame bounds"]
            if w.line:
                evidence.append(f"Line {w.line}")
            findings.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "observed": {
                        "function": w.function,
                        "partition_by": w.partition_by,
                        "order_by": w.order_by,
                        "bounded": False,
                        "line": w.line,
                    },
                    "expected": {"frame_bounded": True},
                    "evidence": evidence,
                    "recommendation": (
                        "Potentially expensive window operation: add explicit frame bound "
                        "(ROWS/RANGE BETWEEN) where semantics allow."
                    ),
                    "confidence": 0.75,
                    "assumptions": {},
                    "line": w.line,
                }
            )

    return findings


# ----------------------------------------------------------------
# Rule: CODE-SQL-008 Function on Filter Column
# ----------------------------------------------------------------


def analyze_function_on_column(
    analysis: SQLAnalysis | dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Detect functions applied to columns in WHERE/HAVING."""
    model = _to_analysis(analysis)
    findings: list[dict[str, Any]] = []

    for f in model.filters:
        if f.functions_on_columns:
            fn_names = ", ".join(f.functions_on_columns)
            evidence = [
                f"Function `{fn_names}` applied to column in filter: `{f.expression[:100]}`",
            ]
            if f.line:
                evidence.append(f"Line {f.line}")
            findings.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "observed": {
                        "function": f.functions_on_columns,
                        "column": f.columns,
                        "filter": f.expression[:200],
                        "line": f.line,
                    },
                    "expected": {"filter_on_raw_columns": True},
                    "evidence": evidence,
                    "recommendation": (
                        "Functions on filter columns may prevent partition pruning and index "
                        "usage; consider rewriting predicates to avoid functions on columns."
                    ),
                    "confidence": 0.75,
                    "assumptions": {"partition_pruning": "not verified statically"},
                    "line": f.line,
                }
            )

    return findings


# ----------------------------------------------------------------
# Rule: CODE-SQL-009 Excessive Query Complexity
# ----------------------------------------------------------------


def analyze_complexity(
    analysis: SQLAnalysis | dict[str, Any],
    context: dict[str, Any] | None = None,
    max_joins: int = 7,
    max_ctes: int = 10,
    max_nesting: int = 3,
) -> list[dict[str, Any]]:
    """Flag excessively complex queries."""
    model = _to_analysis(analysis)
    findings: list[dict[str, Any]] = []

    for q in model.queries:
        comp = q.complexity
        joins = comp.get("join_count", len(q.joins))
        ctes = comp.get("cte_count", len(q.ctes))
        nesting = comp.get("max_nesting", 0)

        triggered = joins > max_joins or ctes > max_ctes or nesting > max_nesting
        if triggered:
            evidence = []
            if joins > max_joins:
                evidence.append(f"Join count {joins} exceeds threshold ({max_joins})")
            if ctes > max_ctes:
                evidence.append(f"CTE count {ctes} exceeds threshold ({max_ctes})")
            if nesting > max_nesting:
                evidence.append(
                    f"Subquery nesting depth {nesting} exceeds threshold ({max_nesting})"
                )
            if q.line:
                evidence.append(f"Line {q.line}")

            findings.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "observed": {
                        "join_count": joins,
                        "cte_count": ctes,
                        "nesting_depth": nesting,
                        "line": q.line,
                    },
                    "expected": {
                        "join_count": f"<= {max_joins}",
                        "cte_count": f"<= {max_ctes}",
                        "nesting_depth": f"<= {max_nesting}",
                    },
                    "evidence": evidence,
                    "recommendation": (
                        "Consider breaking query into stages, using temporary views, or "
                        "simplifying logic. Complex queries are harder to optimize and maintain."
                    ),
                    "confidence": 0.75,
                    "assumptions": {},
                    "line": q.line,
                }
            )

    return findings


# ----------------------------------------------------------------
# Rule: CODE-SQL-010 SQL Parse Error
# ----------------------------------------------------------------


def analyze_parse_errors(
    analysis: SQLAnalysis | dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Report SQL parse errors as blocking findings."""
    model = _to_analysis(analysis)
    findings: list[dict[str, Any]] = []

    for err in model.parse_errors:
        evidence = [f"SQL syntax error: {err.message}"]
        if err.line:
            evidence.append(f"Line {err.line}")
        if err.column:
            evidence.append(f"Column {err.column}")

        findings.append(
            {
                "triggered": True,
                "level": "CRITICAL",
                "blocking": True,
                "observed": {
                    "file": err.file,
                    "line": err.line,
                    "column": err.column,
                    "error_message": err.message,
                },
                "expected": {"valid_sql": True},
                "evidence": evidence,
                "recommendation": f"Fix SQL syntax error at line {err.line}: {err.message}",
                "confidence": 1.0,
                "assumptions": {},
                "line": err.line,
            }
        )

    return findings


# ----------------------------------------------------------------
# Rule: DATA-003 Schema Drift (preserved for CP-003)
# ----------------------------------------------------------------


def _norm(cols: Any) -> dict[str, dict[str, Any]]:
    """Normalise a column list (dicts or SchemaColumn-likes) to name->attrs."""
    out: dict[str, dict[str, Any]] = {}
    for c in cols or []:
        if isinstance(c, dict):
            name = str(c.get("name", ""))
            out[name] = {
                "data_type": str(c.get("data_type", "unknown")),
                "nullable": bool(c.get("nullable", True)),
            }
        else:
            out[str(getattr(c, "name", ""))] = {
                "data_type": str(getattr(c, "data_type", "unknown")),
                "nullable": bool(getattr(c, "nullable", True)),
            }
    return out


def analyze_schema_drift(
    expected: Any,
    observed: Any,
    policy: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Compare expected vs observed schemas (DATA-003).

    Returns dict with missing/unexpected/type_changes/nullable_changes plus
    an aggregate ``status``. Either side absent -> UNKNOWN (never PASS).
    """
    policy = policy or {
        "missing": "FAIL",
        "type_change": "FAIL",
        "unexpected": "WARN",
        "nullable_change": "WARN",
    }
    exp = _norm(
        expected.get("columns")
        if isinstance(expected, dict)
        else getattr(expected, "columns", None)
    )
    obs = _norm(
        observed.get("columns")
        if isinstance(observed, dict)
        else getattr(observed, "columns", None)
    )
    if not exp or not obs:
        return {
            "status": "UNKNOWN",
            "missing": [],
            "unexpected": [],
            "type_changes": [],
            "nullable_changes": [],
            "assumptions": {"insufficient": "expected and observed schemas both required"},
        }
    missing = sorted(set(exp) - set(obs))
    unexpected = sorted(set(obs) - set(exp))
    type_changes = sorted(
        n for n in set(exp) & set(obs) if exp[n]["data_type"].lower() != obs[n]["data_type"].lower()
    )
    nullable_changes = sorted(
        n for n in set(exp) & set(obs) if exp[n]["nullable"] != obs[n]["nullable"]
    )
    worst = "PASS"
    checks = [
        ("missing", missing),
        ("type_change", type_changes),
        ("unexpected", unexpected),
        ("nullable_change", nullable_changes),
    ]
    for kind, items in checks:
        if items:
            level = policy.get(kind, "WARN")
            if ["PASS", "WARN", "FAIL"].index(level) > ["PASS", "WARN", "FAIL"].index(worst):
                worst = level
    return {
        "status": worst,
        "missing": missing,
        "unexpected": unexpected,
        "type_changes": type_changes,
        "nullable_changes": nullable_changes,
        "assumptions": {},
    }


# ----------------------------------------------------------------
# Registry
# ----------------------------------------------------------------

ANALYZERS: dict[str, Callable[..., Any]] = {
    "select_star": analyze_select_star,
    "cross_join": analyze_cross_join,
    "large_join": analyze_large_join,
    "large_agg": analyze_large_aggregation,
    "large_aggregation": analyze_large_aggregation,
    "global_sort": analyze_global_sort,
    "dedup_risk": analyze_distinct,
    "distinct": analyze_distinct,
    "window_risk": analyze_window,
    "window": analyze_window,
    "function_on_column": analyze_function_on_column,
    "function_on_filter": analyze_function_on_column,
    "complexity": analyze_complexity,
    "parse_errors": analyze_parse_errors,
    "parse_error": analyze_parse_errors,
    "schema_drift": analyze_schema_drift,
}
