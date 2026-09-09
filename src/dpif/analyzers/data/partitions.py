"""Partition imbalance analysis (DATA-002).

Uses only partition metadata (sizes/counts). Wording is deliberately
"potential partition imbalance" — Spark runtime skew requires runtime
metrics, which offline analysis must not claim.
"""

from __future__ import annotations

from typing import Any


def analyze_partitions(
    sizes_gb: dict[str, float] | None,
    record_counts: dict[str, int] | None = None,
    total_gb: float = 0.0,
    imbalance_ratio: float = 5.0,
    min_gb: float = 10.0,
) -> dict[str, Any]:
    """Detect an obviously imbalanced partition layout."""
    sizes = {k: float(v) for k, v in (sizes_gb or {}).items()}
    observed: dict[str, Any] = {
        "partition_count": len(sizes),
        "total_gb": total_gb,
    }
    if len(sizes) < 2:
        return {
            "triggered": False,
            "observed": observed,
            "assumptions": {"insufficient": "fewer than 2 partitions with sizes"},
        }
    biggest = max(sizes, key=lambda k: sizes[k])
    smallest = min(sizes, key=lambda k: sizes[k])
    big, small = sizes[biggest], sizes[smallest]
    ratio = (big / small) if small > 0 else float("inf")
    share = (big / sum(sizes.values())) if sum(sizes.values()) > 0 else 0.0
    observed.update(
        {
            "largest_partition": biggest,
            "largest_gb": big,
            "smallest_partition": smallest,
            "smallest_gb": small,
            "max_min_ratio": round(ratio, 2) if ratio != float("inf") else "inf",
            "largest_share": round(share, 3),
        }
    )
    if big >= min_gb and (ratio >= imbalance_ratio or share >= 0.6):
        return {
            "triggered": True,
            "observed": observed,
            "expected": {"max_min_ratio": f"< {imbalance_ratio}", "largest_share": "< 0.6"},
            "recommendation": (
                "Potential partition imbalance detected: review the partition key "
                "and consider salting or re-bucketing the hot partition; confirm "
                "with runtime task metrics before tuning joins."
            ),
            "confidence": 0.8,
            "assumptions": {"metadata-only": "no Spark runtime skew claimed"},
        }
    return {"triggered": False, "observed": observed, "assumptions": {}}
