"""Contextual source-format analysis (feeds SOURCE-001)."""

from __future__ import annotations

from typing import Any

from dpif.sources.base import format_guidance

KNOWN_FORMATS = ("csv", "json", "parquet", "avro", "orc", "delta")


def analyze_format(
    format_name: str | None,
    volume_gb: float | None,
    source_type: str | None = None,
) -> dict[str, Any]:
    """Decide whether the format finding triggers and at what level."""
    fmt = (format_name or "unknown").lower()
    observed = {"format": fmt, "volume_gb": volume_gb}
    if fmt not in KNOWN_FORMATS:
        if (source_type or "").lower() in ("jdbc", "rest_api"):
            # Row-protocol extracts have no file format; the reader contract
            # is the query/API definition, assessed elsewhere (SOURCE-003).
            return {
                "triggered": False,
                "observed": {**observed, "source_type": source_type},
                "assumptions": {"not-applicable": "file format N/A for row-protocol source"},
            }
        return {
            "triggered": True,
            "level": "FAIL",
            "observed": observed,
            "expected": {"format": "one of csv/json/parquet/avro/orc/delta"},
            "recommendation": (
                "Unsupported/unknown source format: confirm the reader contract "
                "and parsing strategy before production."
            ),
            "confidence": 0.85,
            "assumptions": {},
        }
    if fmt in ("csv", "json") and volume_gb is not None and volume_gb >= 100:
        return {
            "triggered": True,
            "level": "WARN",
            "observed": observed,
            "expected": {"format": "columnar for large analytical workloads"},
            "recommendation": format_guidance(fmt, volume_gb),  # type: ignore[arg-type]
            "confidence": 0.75,
            "assumptions": {"workload": "analytical assumption; confirm access pattern"},
        }
    return {
        "triggered": False,
        "observed": {**observed, "guidance": format_guidance(fmt, volume_gb)},  # type: ignore[arg-type]
        "assumptions": {},
    }
