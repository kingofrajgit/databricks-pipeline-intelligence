"""File-size distribution description (avg/median/P95/P99/count).

A healthy-looking average can hide millions of tiny files behind a few huge
ones, so verdicts always consider the full distribution.
"""

from __future__ import annotations

from typing import Any


def describe_distribution(profile: dict[str, Any]) -> dict[str, Any]:
    """Summarise the distribution; flag median-far-below-average shapes."""
    avg = float(profile.get("average_file_size_kb", 0.0) or 0.0)
    median = float(profile.get("median_file_size_kb", 0.0) or 0.0)
    p95 = float(profile.get("p95_file_size_kb", 0.0) or 0.0)
    p99 = float(profile.get("p99_file_size_kb", 0.0) or 0.0)
    count = int(profile.get("file_count", 0) or 0)
    total_gb = float(profile.get("total_gb", 0.0) or 0.0)
    median_ratio = (median / avg) if avg > 0 else 0.0
    # Median far below average => right-skewed sizes: most files much smaller
    # than the mean suggests.
    skewed_shape = avg > 0 and median > 0 and median_ratio < 0.25 and count >= 1000
    return {
        "average_file_size_kb": avg,
        "median_file_size_kb": median,
        "p95_file_size_kb": p95,
        "p99_file_size_kb": p99,
        "file_count": count,
        "total_gb": total_gb,
        "median_to_average_ratio": round(median_ratio, 3),
        "right_skewed_shape": skewed_shape,
    }
