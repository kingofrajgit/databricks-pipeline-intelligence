"""Source-type capabilities without live connectors.

No I/O happens here: this module only describes what each source type needs
and what it can do, so validators can judge configurations from metadata.
"""

from __future__ import annotations

from dpif.models import SourceFormat, SourceType

SUPPORTED_SOURCE_TYPES = (
    SourceType.ADLS,
    SourceType.S3,
    SourceType.GCS,
    SourceType.JDBC,
    SourceType.REST_API,
    SourceType.KAFKA,
    SourceType.EVENT_HUB,
    SourceType.FILE,
    SourceType.LOCAL_FILE,
    SourceType.DELTA,
    SourceType.OTHER,
)

SUPPORTED_FORMATS = (
    SourceFormat.CSV,
    SourceFormat.JSON,
    SourceFormat.PARQUET,
    SourceFormat.AVRO,
    SourceFormat.ORC,
    SourceFormat.DELTA,
    SourceFormat.UNKNOWN,
)

# Source types that can natively expose incremental change evidence.
_INCREMENTAL_CAPABLE = frozenset(
    {
        SourceType.DELTA,
        SourceType.JDBC,
        SourceType.KAFKA,
        SourceType.EVENT_HUB,
        SourceType.ADLS,
        SourceType.S3,
        SourceType.GCS,
    }
)

# Minimal config keys we look for per type (informational, not fatal).
_REQUIRED_CONFIG: dict[SourceType, tuple[str, ...]] = {
    SourceType.ADLS: ("path",),
    SourceType.S3: ("path",),
    SourceType.GCS: ("path",),
    SourceType.JDBC: ("url",),
    SourceType.REST_API: ("url",),
    SourceType.KAFKA: ("bootstrap_servers", "topic"),
    SourceType.EVENT_HUB: ("connection_string", "event_hub"),
    SourceType.DELTA: ("path",),
    SourceType.FILE: ("path",),
    SourceType.LOCAL_FILE: ("path",),
}


def supports_incremental(source_type: SourceType) -> bool:
    """Whether the type can in principle expose incremental evidence."""
    return source_type in _INCREMENTAL_CAPABLE


def required_config_keys(source_type: SourceType) -> tuple[str, ...]:
    """Config keys expected for the type (empty tuple = nothing mandated)."""
    return _REQUIRED_CONFIG.get(source_type, ())


def format_guidance(source_format: SourceFormat, volume_gb: float | None) -> str:
    """Contextual format guidance — never a universal good/bad verdict."""
    big = volume_gb is not None and volume_gb >= 100
    if source_format in (SourceFormat.CSV, SourceFormat.JSON):
        if big:
            return (
                "Consider conversion to a columnar format (Parquet/Delta) for "
                "large analytical workloads where appropriate; row-based text "
                "formats inflate scan and parse cost at this volume."
            )
        return (
            "Row-based text format is acceptable at this volume; revisit if "
            "analytical scan workloads grow."
        )
    if source_format == SourceFormat.PARQUET:
        return "Evaluate compression (snappy/zstd) and file sizing for Parquet."
    if source_format == SourceFormat.DELTA:
        return "Evaluate Delta liquid clustering, Z-ordering, and OPTIMIZE scheduling."
    if source_format in (SourceFormat.AVRO, SourceFormat.ORC):
        return "Columnar-friendly format; confirm reader support across consumers."
    return "Format unknown; confirm the reader contract before production."
