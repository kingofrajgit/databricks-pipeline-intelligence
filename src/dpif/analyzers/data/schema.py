"""Schema comparison: expected vs observed (DATA-003).

Policy maps each drift class to a status; defaults are strict on missing
columns and type changes, lenient on unexpected/nullable changes. Unknown
is returned when either side is absent.
"""

from __future__ import annotations

from typing import Any

DEFAULT_POLICY = {
    "missing": "FAIL",
    "type_change": "FAIL",
    "unexpected": "WARN",
    "nullable_change": "WARN",
}

_SEVERITY_ORDER = ("PASS", "WARN", "FAIL")


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


def compare_schemas(
    expected: Any,
    observed: Any,
    policy: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Compare expected vs observed columns.

    Returns dict with missing/unexpected/type_changes/nullable_changes plus
    an aggregate ``status``. Either side absent -> UNKNOWN (never PASS).
    """
    policy = policy or DEFAULT_POLICY
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
            if _SEVERITY_ORDER.index(level) > _SEVERITY_ORDER.index(worst):
                worst = level
    return {
        "status": worst,
        "missing": missing,
        "unexpected": unexpected,
        "type_changes": type_changes,
        "nullable_changes": nullable_changes,
        "assumptions": {},
    }
