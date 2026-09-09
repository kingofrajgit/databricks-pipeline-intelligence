"""JDBC parallel-extraction analysis (SOURCE-003)."""

from __future__ import annotations

from typing import Any


def analyze_jdbc(
    jdbc: dict[str, Any] | None,
    volume_gb: float | None,
    min_gb: float = 100.0,
) -> dict[str, Any]:
    """Flag large JDBC extractions without documented parallelization."""
    jdbc = jdbc or {}
    observed = {
        "partition_column": jdbc.get("partition_column"),
        "num_partitions": jdbc.get("num_partitions"),
        "incremental_column": jdbc.get("incremental_column"),
        "volume_gb": volume_gb,
    }
    if volume_gb is None or volume_gb < min_gb:
        return {
            "triggered": False,
            "observed": observed,
            "assumptions": {"small": f"volume below {min_gb} GB parallelization bar"},
        }
    parallel = bool(jdbc.get("partition_column")) and (jdbc.get("num_partitions") or 0) > 1
    if parallel:
        return {
            "triggered": False,
            "observed": observed,
            "assumptions": {"parallel": "partition_column + num_partitions documented"},
        }
    return {
        "triggered": True,
        "observed": observed,
        "expected": {"partition_column": "set", "num_partitions": "> 1"},
        "recommendation": (
            "Large JDBC extraction lacks documented parallelization: define "
            "partition_column/num_partitions (or an incremental column strategy) "
            "to avoid single-threaded full extracts."
        ),
        "confidence": 0.8,
        "assumptions": {"large-jdbc": f">= {min_gb} GB without sharding evidence"},
    }
