"""Volume presence checks (DATA-004). Never invents a volume."""

from __future__ import annotations

from typing import Any


def analyze_volume_presence(
    expected_gb: float | None,
    peak_gb: float | None,
) -> dict[str, Any]:
    """Return a result dict; ``triggered`` when expected volume is missing."""
    if expected_gb is not None and expected_gb > 0:
        return {
            "triggered": False,
            "observed": {"expected_daily_volume_gb": expected_gb, "peak_daily_volume_gb": peak_gb},
            "assumptions": {},
        }
    return {
        "triggered": True,
        "observed": {"expected_daily_volume_gb": expected_gb, "peak_daily_volume_gb": peak_gb},
        "expected": {"expected_daily_volume_gb": "> 0"},
        "recommendation": (
            "Declare expected_daily_volume_gb (and peak) in the contract; "
            "sizing, cluster, and SLA analysis require it."
        ),
        "confidence": 0.9,
        "assumptions": {"missing-volume": "expected volume unconfigured"},
    }
