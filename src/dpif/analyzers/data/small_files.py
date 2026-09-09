"""Small-file and excessive-count analysis (DATA-001, DATA-005).

Thresholds are configurable; per-format multipliers keep one universal
threshold from misjudging row-based formats. Volume alone never decides:
a small average with few files is fine, a small average with millions of
files is a potential Spark performance risk (metadata overhead, task churn).
"""

from __future__ import annotations

from typing import Any

DEFAULT_FORMAT_FACTORS = {
    "parquet": 1.0,
    "delta": 1.0,
    "orc": 1.0,
    "avro": 0.8,
    "csv": 0.25,
    "json": 0.25,
    "unknown": 0.5,
}


def effective_threshold(
    base_threshold_kb: float, format_name: str, factors: dict[str, float] | None = None
) -> float:
    factors = factors or DEFAULT_FORMAT_FACTORS
    return base_threshold_kb * factors.get((format_name or "unknown").lower(), 0.5)


def analyze_small_files(
    profile: dict[str, Any],
    format_name: str = "unknown",
    base_threshold_kb: float = 1000.0,
    min_files: int = 1000,
    factors: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Flag a small-file distribution; average alone never decides."""
    avg = float(profile.get("average_file_size_kb", 0.0) or 0.0)
    median = float(profile.get("median_file_size_kb", 0.0) or 0.0)
    p95 = float(profile.get("p95_file_size_kb", 0.0) or 0.0)
    count = int(profile.get("file_count", 0) or 0)
    threshold = effective_threshold(base_threshold_kb, format_name, factors)
    observed = {
        "average_file_size_kb": avg,
        "median_file_size_kb": median,
        "p95_file_size_kb": p95,
        "file_count": count,
        "total_gb": profile.get("total_gb", 0.0),
        "format": format_name,
    }
    expected = {"average_file_size_kb": f">= {threshold:.0f} (format-adjusted)"}
    if count <= 0 or avg <= 0:
        return {
            "triggered": False,
            "observed": observed,
            "assumptions": {"insufficient": "no file statistics available"},
        }
    # Small average is only a problem at scale: many files amplify it.
    if avg < threshold and count >= min_files:
        severity_hint = "high" if median < threshold and p95 < threshold else "medium"
        return {
            "triggered": True,
            "observed": {**observed, "configured_threshold_kb": threshold},
            "expected": expected,
            "recommendation": (
                "Potential Spark performance risk from small files: coalesce output, "
                "raise target file size (e.g. Delta OPTIMIZE / Auto Optimize), or "
                "repartition before writing; validate with a benchmark."
            ),
            "confidence": 0.9 if severity_hint == "high" else 0.75,
            "assumptions": {"distribution": f"avg+median+p95 below threshold ({severity_hint})"},
        }
    return {"triggered": False, "observed": observed, "assumptions": {}}


def analyze_file_count(profile: dict[str, Any], excessive_count: int = 1000000) -> dict[str, Any]:
    """Flag an excessive absolute file count (DATA-005)."""
    count = int(profile.get("file_count", 0) or 0)
    observed = {"file_count": count, "total_gb": profile.get("total_gb", 0.0)}
    if count >= excessive_count:
        return {
            "triggered": True,
            "observed": {**observed, "configured_limit": excessive_count},
            "expected": {"file_count": f"< {excessive_count}"},
            "recommendation": (
                "File count implies driver/metadata pressure; consolidate files "
                "and confirm partition strategy."
            ),
            "confidence": 0.85,
            "assumptions": {"count-threshold": f">= {excessive_count} files"},
        }
    return {"triggered": False, "observed": observed, "assumptions": {}}
