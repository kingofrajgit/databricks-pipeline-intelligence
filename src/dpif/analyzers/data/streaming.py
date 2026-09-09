"""Streaming configuration analysis. Runtime state is always UNKNOWN offline."""

from __future__ import annotations

from typing import Any


def analyze_streaming(meta: dict[str, Any] | None) -> dict[str, Any]:
    """Check streaming config presence; lag/throughput stay UNKNOWN."""
    meta = meta or {}
    missing = [k for k in ("checkpoint_location", "trigger", "watermark") if not meta.get(k)]
    observed = {
        "partition_count": meta.get("partition_count"),
        "checkpoint_location": bool(meta.get("checkpoint_location")),
        "trigger": meta.get("trigger"),
        "watermark": meta.get("watermark"),
        "starting_position": meta.get("starting_position"),
    }
    unknowns = ["consumer_lag", "throughput", "runtime_state"]
    if missing:
        return {
            "triggered": True,
            "observed": observed,
            "expected": {"checkpoint_location": "set", "trigger": "set", "watermark": "set"},
            "recommendation": (
                f"Streaming source missing: {', '.join(missing)}. Define "
                "checkpointing, trigger, and watermark before production."
            ),
            "confidence": 0.75,
            "unknowns": unknowns,
            "assumptions": {"runtime": "lag/throughput UNKNOWN without runtime info"},
        }
    return {
        "triggered": False,
        "observed": observed,
        "unknowns": unknowns,
        "assumptions": {"runtime": "lag/throughput UNKNOWN without runtime info"},
    }
