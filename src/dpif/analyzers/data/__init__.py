"""Data-intelligence analyzers (pure functions over metadata dicts)."""

from dpif.analyzers.data import (
    coverage,
    distribution,
    formats,
    growth,
    jdbc,
    partitions,
    schema,
    small_files,
    streaming,
    volume,
)

__all__ = [
    "coverage",
    "distribution",
    "formats",
    "growth",
    "jdbc",
    "partitions",
    "schema",
    "small_files",
    "streaming",
    "volume",
]
