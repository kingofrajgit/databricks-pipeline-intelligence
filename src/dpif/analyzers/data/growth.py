"""Growth projection. Projects only with an explicit rate; otherwise UNKNOWN.

Contract ``growth_rate_percent`` is interpreted as an ANNUAL rate prorated
over the horizon — compounding a daily percent for a year produces absurd
figures no planner declared. An explicitly per-day rate can be passed with
``period="daily"`` (see ``ScalabilityRules.expected_growth_percent_per_day``).
"""

from __future__ import annotations

from typing import Any


def project_growth(
    current_gb: float | None,
    growth_rate_percent: float | None,
    horizon_days: int = 365,
    period: str = "annual",
) -> dict[str, Any]:
    """Prorated projection. Missing inputs -> UNKNOWN, never invented."""
    if current_gb is None or current_gb <= 0:
        return {
            "status": "UNKNOWN",
            "projected_gb": None,
            "assumptions": {"insufficient": "current volume unknown"},
        }
    if growth_rate_percent is None:
        return {
            "status": "UNKNOWN",
            "projected_gb": None,
            "observed": {"current_gb": current_gb},
            "assumptions": {"no-growth-rate": "growth rate absent; not invented"},
        }
    rate = float(growth_rate_percent) / 100.0
    horizon = int(horizon_days)
    if period == "daily":
        factor = (1.0 + rate) ** horizon
    else:
        factor = (1.0 + rate) ** (horizon / 365.0)
    projected = float(current_gb) * factor
    return {
        "status": "OK",
        "projected_gb": round(projected, 1),
        "observed": {
            "current_gb": current_gb,
            "growth_rate_percent": growth_rate_percent,
            "horizon_days": horizon,
            "rate_period": period,
        },
        "assumptions": {"model": f"compound {period} rate prorated over horizon"},
    }
