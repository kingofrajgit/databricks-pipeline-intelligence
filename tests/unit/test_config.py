"""Configuration system tests."""

from __future__ import annotations

from dpif.config import Settings, get_settings, reload_settings


def test_default_settings():
    s = Settings()
    assert s.environment == "development"
    assert s.debug is False
    assert s.default_profile == "production"
    assert s.enable_cost_analysis is True
    assert s.small_file_threshold_kb == 1000.0


def test_env_override(monkeypatch):
    monkeypatch.setenv("DPIF_ENVIRONMENT", "production")
    monkeypatch.setenv("DPIF_DEBUG", "true")
    monkeypatch.setenv("DPIF_SMALL_FILE_THRESHOLD_KB", "2048")
    s = reload_settings()
    try:
        assert s.environment == "production"
        assert s.debug is True
        assert s.small_file_threshold_kb == 2048.0
    finally:
        reload_settings()


def test_get_settings_singleton():
    assert get_settings() is get_settings()


def test_thresholds_configurable():
    s = Settings(small_file_threshold_kb=512.0, min_average_file_size_kb=10.0)
    assert s.small_file_threshold_kb == 512.0
    assert s.min_average_file_size_kb == 10.0


def test_no_secrets_in_defaults():
    s = Settings()
    assert s.databricks_token is None
    dumped = s.model_dump_json()
    assert "token" not in dumped or "null" in dumped
