from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int | None) -> int | None:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


class Settings(BaseModel):
    """Application settings loaded from environment and config files."""

    model_config = ConfigDict(extra="ignore")

    # Core settings
    environment: str = "development"
    debug: bool = False

    # Databricks connection (never hard-coded; env or secret store only)
    databricks_host: str | None = None
    databricks_token: str | None = None
    databricks_job_id: int | None = None

    # Database
    database_url: str | None = None

    # Paths
    contract_path: str | None = None
    fixtures_dir: str | None = None
    rules_dir: str | None = None

    # Scoring
    default_profile: str = "production"

    # Feature flags
    enable_ai_advisor: bool = False
    enable_cost_analysis: bool = True
    enable_scalability_analysis: bool = True

    # Configurable thresholds
    small_file_threshold_kb: float = 1000.0
    min_average_file_size_kb: float = 100.0
    max_average_file_size_kb: float = 10000.0
    # Phase 3 data-intelligence thresholds
    excessive_file_count: int = 1000000
    small_file_min_files: int = 1000
    partition_imbalance_ratio: float = 5.0
    partition_imbalance_min_gb: float = 10.0
    large_volume_gb: float = 500.0
    jdbc_parallel_min_gb: float = 100.0
    # Per-format multipliers applied to small_file_threshold_kb
    format_size_factors_json: str = (
        '{"parquet": 1.0, "delta": 1.0, "orc": 1.0, "avro": 0.8, '
        '"csv": 0.25, "json": 0.25, "unknown": 0.5}'
    )


# Global settings instance
_settings: Settings | None = None


def _settings_from_env() -> Settings:
    """Build Settings from DPIF_* environment variables with type coercion."""
    return Settings(
        environment=os.environ.get("DPIF_ENVIRONMENT", "development"),
        debug=_env_bool("DPIF_DEBUG", False),
        databricks_host=os.environ.get("DPIF_DATABRICKS_HOST"),
        databricks_token=os.environ.get("DPIF_DATABRICKS_TOKEN"),
        databricks_job_id=_env_int("DPIF_DATABRICKS_JOB_ID", None),
        database_url=os.environ.get("DPIF_DATABASE_URL"),
        contract_path=os.environ.get("DPIF_CONTRACT_PATH"),
        fixtures_dir=os.environ.get("DPIF_FIXTURES_DIR"),
        rules_dir=os.environ.get("DPIF_RULES_DIR"),
        default_profile=os.environ.get("DPIF_DEFAULT_PROFILE", "production"),
        enable_ai_advisor=_env_bool("DPIF_ENABLE_AI_ADVISOR", False),
        enable_cost_analysis=_env_bool("DPIF_ENABLE_COST_ANALYSIS", True),
        enable_scalability_analysis=_env_bool("DPIF_ENABLE_SCALABILITY_ANALYSIS", True),
        small_file_threshold_kb=_env_float("DPIF_SMALL_FILE_THRESHOLD_KB", 1000.0),
        min_average_file_size_kb=_env_float("DPIF_MIN_AVG_FILE_SIZE_KB", 100.0),
        max_average_file_size_kb=_env_float("DPIF_MAX_AVG_FILE_SIZE_KB", 10000.0),
        excessive_file_count=_env_int("DPIF_EXCESSIVE_FILE_COUNT", 1000000) or 1000000,
        small_file_min_files=_env_int("DPIF_SMALL_FILE_MIN_FILES", 1000) or 1000,
        partition_imbalance_ratio=_env_float("DPIF_PARTITION_IMBALANCE_RATIO", 5.0),
        partition_imbalance_min_gb=_env_float("DPIF_PARTITION_IMBALANCE_MIN_GB", 10.0),
        large_volume_gb=_env_float("DPIF_LARGE_VOLUME_GB", 500.0),
        jdbc_parallel_min_gb=_env_float("DPIF_JDBC_PARALLEL_MIN_GB", 100.0),
        format_size_factors_json=os.environ.get(
            "DPIF_FORMAT_SIZE_FACTORS",
            '{"parquet": 1.0, "delta": 1.0, "orc": 1.0, "avro": 0.8, '
            '"csv": 0.25, "json": 0.25, "unknown": 0.5}',
        ),
    )


def format_size_factors(settings: Settings | None = None) -> dict[str, float]:
    """Parse per-format small-file threshold multipliers."""
    import json

    raw = (settings or get_settings()).format_size_factors_json
    try:
        parsed = json.loads(raw)
        return {str(k).lower(): float(v) for k, v in parsed.items()}
    except (ValueError, AttributeError):
        return {"unknown": 0.5}


def get_settings() -> Settings:
    """Get the global settings instance, loading from environment if needed."""
    global _settings
    if _settings is None:
        _settings = _settings_from_env()
    return _settings


def reload_settings() -> Settings:
    """Reload settings from environment."""
    global _settings
    _settings = _settings_from_env()
    return _settings


def load_config(config_path: str) -> dict[str, Any]:
    """Load a YAML config file."""
    import yaml

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path) as f:
        return yaml.safe_load(f) or {}


def save_config(config_path: str, config: dict[str, Any]) -> None:
    """Save a YAML config file."""
    import yaml

    path = Path(config_path)
    with open(path, "w") as f:
        yaml.dump(config, f, default_flow_style=False)


def _project_root() -> Path:
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return here.parent.parent.parent


def get_rules_dir() -> Path:
    """Get the rules directory path (project ``rules/``, not the package dir)."""
    settings = get_settings()
    if settings.rules_dir:
        return Path(settings.rules_dir)
    return _project_root() / "rules"


def get_fixtures_dir() -> Path:
    """Get the fixtures directory path."""
    settings = get_settings()
    if settings.fixtures_dir:
        return Path(settings.fixtures_dir)
    return Path(__file__).parent.parent / "tests" / "fixtures"


def get_contract_path() -> Path | None:
    """Get the contract path from settings or environment."""
    settings = get_settings()
    if settings.contract_path:
        return Path(settings.contract_path)
    return None
