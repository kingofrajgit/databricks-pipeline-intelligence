"""Source-type abstraction: capabilities, required config, format guidance."""

from dpif.sources.base import (
    SUPPORTED_FORMATS,
    SUPPORTED_SOURCE_TYPES,
    format_guidance,
    required_config_keys,
    supports_incremental,
)

__all__ = [
    "SUPPORTED_FORMATS",
    "SUPPORTED_SOURCE_TYPES",
    "format_guidance",
    "required_config_keys",
    "supports_incremental",
]
