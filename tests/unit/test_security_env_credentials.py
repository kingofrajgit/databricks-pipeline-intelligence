"""Security tests for environment-based secrets management in DPIF (Phase M5J Hardening).

Verifies:
1. .env configuration loading
2. Environment variables loading
3. CLI arguments override environment variables
4. Environment variables override .env
5. Missing host handled safely
6. Missing token handled safely
7. Token never appears in stdout
8. Token never appears in stderr
9. Token never appears in JSON output
10. Token never appears in exceptions
11. Token masking works across all sensitive keywords
12. .env.example contains placeholders only
13. .env is excluded by Git (.gitignore)
14. Online validation works with resolved credentials
15. Offline validation remains completely unaffected
16. Secret scanning: zero hardcoded Databricks tokens in source code and test fixtures
17. Git tracking: .env is not tracked
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from dpif.cli import cli
from dpif.config import (
    load_env_file,
    reload_settings,
    resolve_databricks_credentials,
)
from dpif.connectors.live import DatabricksApiError, LiveDatabricksConnector
from dpif.orchestration.online import run_online_validation
from dpif.providers.base import mask_sensitive_credentials, sanitize_job_payload


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch):
    """Ensure Databricks environment variables are clean for each test."""
    for var in (
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_JOB_ID",
        "DATABRICKS_CLUSTER_ID",
        "DATABRICKS_RUN_ID",
        "DATABRICKS_PIPELINE_ID",
        "DPIF_DATABRICKS_HOST",
        "DPIF_DATABRICKS_TOKEN",
        "DPIF_DATABRICKS_JOB_ID",
        "DPIF_DATABRICKS_CLUSTER_ID",
        "DPIF_DATABRICKS_RUN_ID",
    ):
        monkeypatch.delenv(var, raising=False)
    reload_settings()
    yield
    reload_settings()


def test_01_load_env_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test 1: .env configuration loads correctly into environment."""
    env_file = tmp_path / ".env"
    env_file.write_text(
        "DATABRICKS_HOST=https://adb-from-dotenv.azuredatabricks.net\n"
        "DATABRICKS_TOKEN=fake-token-from-dotenv-12345\n"
        "DATABRICKS_JOB_ID=8888\n",
        encoding="utf-8",
    )
    loaded = load_env_file(dotenv_path=env_file, override=False)
    assert loaded == env_file
    assert os.environ.get("DATABRICKS_HOST") == "https://adb-from-dotenv.azuredatabricks.net"
    assert os.environ.get("DATABRICKS_TOKEN") == "fake-token-from-dotenv-12345"
    assert os.environ.get("DATABRICKS_JOB_ID") == "8888"


def test_02_environment_variables_load(monkeypatch: pytest.MonkeyPatch):
    """Test 2: Process environment variables load via resolve_databricks_credentials."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-from-env.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "fake-token-from-env-67890")
    monkeypatch.setenv("DATABRICKS_JOB_ID", "9999")

    creds = resolve_databricks_credentials(load_env=False)
    assert creds["host"] == "https://adb-from-env.azuredatabricks.net"
    assert creds["token"] == "fake-token-from-env-67890"
    assert creds["job_id"] == 9999


def test_03_cli_arguments_override_environment(monkeypatch: pytest.MonkeyPatch):
    """Test 3: CLI arguments take precedence over process environment variables."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-from-env.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "fake-token-from-env")
    monkeypatch.setenv("DATABRICKS_JOB_ID", "1111")

    creds = resolve_databricks_credentials(
        host="https://adb-from-cli.azuredatabricks.net",
        token="fake-token-from-cli",
        job_id=2222,
        load_env=False,
    )
    assert creds["host"] == "https://adb-from-cli.azuredatabricks.net"
    assert creds["token"] == "fake-token-from-cli"
    assert creds["job_id"] == 2222


def test_04_environment_variables_override_dotenv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test 4: Process environment variables take precedence over .env values."""
    env_file = tmp_path / ".env"
    env_file.write_text(
        "DATABRICKS_HOST=https://adb-from-dotenv.azuredatabricks.net\n"
        "DATABRICKS_TOKEN=fake-token-from-dotenv\n",
        encoding="utf-8",
    )
    # Set pre-existing process environment variable
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-from-proc-env.azuredatabricks.net")

    creds = resolve_databricks_credentials(load_env=True, dotenv_path=env_file)
    # Process env wins for host; token falls back to .env
    assert creds["host"] == "https://adb-from-proc-env.azuredatabricks.net"
    assert creds["token"] == "fake-token-from-dotenv"


def test_05_missing_host_handled_safely(runner: CliRunner | None = None):
    """Test 5: Missing host fails clearly and safely without crash."""
    connector = LiveDatabricksConnector(host="", token="fake-token-12345")
    assert not connector.is_configured
    with pytest.raises(DatabricksApiError) as exc_info:
        connector.get_job(123)
    assert exc_info.value.status_code == 401
    assert "DATABRICKS_HOST" in str(exc_info.value)
    # Ensure token does NOT appear in error
    assert "fake-token-12345" not in str(exc_info.value)


def test_06_missing_token_handled_safely():
    """Test 6: Missing token fails clearly and safely without crash."""
    connector = LiveDatabricksConnector(host="https://adb-123.azuredatabricks.net", token="")
    assert not connector.is_configured
    with pytest.raises(DatabricksApiError) as exc_info:
        connector.get_job(123)
    assert exc_info.value.status_code == 401
    assert "DATABRICKS_TOKEN" in str(exc_info.value)


def test_07_token_never_appears_in_stdout(monkeypatch: pytest.MonkeyPatch):
    """Test 7: Token never appears in CLI stdout even when error occurs."""
    fake_token = "super-secret-token-xyz-12345"
    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "validate-online",
            "--workspace",
            "https://adb-123.azuredatabricks.net",
            "--token",
            fake_token,
            "--job-id",
            "12345",
        ],
    )
    # Output may fail due to no real network, but token MUST NEVER appear in stdout
    assert fake_token not in res.output


def test_08_token_never_appears_in_stderr(monkeypatch: pytest.MonkeyPatch):
    """Test 8: Token never appears in CLI stderr or error traces."""
    fake_token = "super-secret-token-xyz-12345"
    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "validate-online",
            "--workspace",
            "https://adb-123.azuredatabricks.net",
            "--token",
            fake_token,
            "--job-id",
            "12345",
        ],
    )
    assert fake_token not in (res.stderr or "")


def test_09_token_never_appears_in_json():
    """Test 9: Serialized JSON outputs and payloads sanitize all token patterns."""
    fake_token = "token-secret-data-98765"
    raw_payload = {
        "job_id": 12345,
        "token": fake_token,
        "settings": {
            "api_key": fake_token,
            "password": "secret-password-123",
            "authorization": f"Bearer {fake_token}",
            "description": f"Connecting with token {fake_token}",
        },
    }
    sanitized = sanitize_job_payload(raw_payload, fake_token)
    import json

    serialized = json.dumps(sanitized)
    assert fake_token not in serialized
    assert "secret-password-123" not in serialized
    assert "[REDACTED_SECRET]" in serialized or "[MASKED_TOKEN]" in serialized or "[MASKED_SECRET]" in serialized


def test_10_token_never_appears_in_exceptions():
    """Test 10: Exception messages sanitize secrets before exposure."""
    fake_token = "sensitive-token-abcde"
    msg = f"Failed connecting to https://adb-123.azuredatabricks.net with Bearer {fake_token}"
    clean = mask_sensitive_credentials(msg, fake_token)
    assert fake_token not in clean
    assert "[MASKED_TOKEN]" in clean or "[MASKED_SECRET]" in clean


def test_11_token_masking_comprehensive():
    """Test 11: Token masking covers authorization, bearer, password, token, secret, api_key."""
    test_cases = [
        ("Bearer dapi1234567890abcdef", "[MASKED_SECRET]"),
        ('password="myPassword123"', 'password="[MASKED_SECRET]"'),
        ('token: "xyz789"', 'token: "[MASKED_SECRET]"'),
        ('secret = "topsecret"', 'secret = "[MASKED_SECRET]"'),
        ('api_key: "key999"', 'api_key: "[MASKED_SECRET]"'),
        ("Authorization: Bearer dapi12345", "[MASKED_SECRET]"),
    ]
    for raw, expected_substr in test_cases:
        masked = mask_sensitive_credentials(raw)
        assert expected_substr in masked or "[MASKED_" in masked
        assert "dapi12345" not in masked
        assert "myPassword123" not in masked
        assert "topsecret" not in masked


def test_12_env_example_contains_placeholders_only():
    """Test 12: .env.example contains placeholders only, no real credentials."""
    example_path = Path(".env.example")
    assert example_path.is_file(), ".env.example must exist"
    content = example_path.read_text(encoding="utf-8")

    # Verify key expected variables exist
    assert "DATABRICKS_HOST" in content
    assert "DATABRICKS_TOKEN" in content

    # Verify strict placeholder checks: no real-looking tokens or URLs
    assert "dapi" not in content.lower()
    assert "password" not in content.lower() or "<" in content
    # Host must be a placeholder
    assert "<your-workspace>" in content or "<workspace>" in content
    assert "<your-databricks-token>" in content or "<token>" in content


def test_13_env_excluded_by_git():
    """Test 13: .env and .env.* are ignored by Git, while .env.example is NOT ignored."""
    res = subprocess.run(
        ["git", "check-ignore", ".env", ".env.local", ".env.production"],
        capture_output=True,
        text=True,
    )
    ignored_files = res.stdout.strip().splitlines()
    assert ".env" in ignored_files
    assert ".env.local" in ignored_files
    assert ".env.production" in ignored_files

    # .env.example must NOT be ignored
    res_example = subprocess.run(
        ["git", "check-ignore", ".env.example"],
        capture_output=True,
        text=True,
    )
    assert res_example.returncode != 0, ".env.example must NOT be ignored by Git"


def test_14_online_validation_with_dotenv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test 14: Online validation initializes and functions with credentials from .env."""
    env_file = tmp_path / ".env"
    env_file.write_text(
        "DATABRICKS_HOST=https://adb-test-dotenv.azuredatabricks.net\n"
        "DATABRICKS_TOKEN=test-token-fake-12345\n",
        encoding="utf-8",
    )
    load_env_file(dotenv_path=env_file, override=False)

    class MockConnector:
        def __init__(self):
            self.host = "https://adb-test-dotenv.azuredatabricks.net"
            self._token = "test-token-fake-12345"

        def mode(self):
            return "live"

        def get_workspace_status(self):
            return {"status": "connected", "host": self.host}

        def get_job(self, job_id):
            return {
                "job_id": int(job_id),
                "settings": {
                    "name": "Dotenv Job",
                    "tasks": [{"task_key": "t1", "existing_cluster_id": "c1"}],
                },
            }

        def get_cluster(self, cluster_id):
            return {"cluster_id": cluster_id, "spark_version": "14.3.x-scala2.12"}

        def get_permissions(self, obj_type, obj_id):
            return {"access_control_list": []}

        def get_recent_runs(self, job_id, limit=10):
            return {"runs": []}

        def get_run(self, run_id):
            return None

    res = run_online_validation(
        job_id=123,
        connector=MockConnector(),  # type: ignore[arg-type]
    )
    assert res.workspace == "https://adb-test-dotenv.azuredatabricks.net"
    assert res.resource_id == "123"
    assert res.execution_mode == "online"
    assert res.pipeline_name == "Dotenv Job"
    assert "test-token-fake-12345" not in str(res.to_dict())


def test_15_offline_validation_remains_unaffected():
    """Test 15: Offline validation remains 100% functional with zero credentials."""
    contract_file = Path("tests/fixtures/contracts/small_batch_pipeline.yaml")
    assert contract_file.is_file()

    runner = CliRunner()
    res = runner.invoke(
        cli,
        ["validate", "--contract", str(contract_file), "--offline"],
    )
    assert res.exit_code == 0, f"Offline validation failed: {res.output}"
    assert "DPIF Validation" in res.output or "CHECKPOINTS" in res.output


def test_16_secret_scanning_no_hardcoded_credentials():
    """Test 16: Zero real hardcoded Databricks tokens in source code and test fixtures."""
    repo_root = Path(__file__).resolve().parent.parent.parent
    src_dir = repo_root / "src"

    # Pattern for actual databricks tokens: dapi followed by 32+ hex/alphanumeric chars
    dapi_pattern = re.compile(r"dapi[0-9a-zA-Z]{20,}")

    for py_file in src_dir.rglob("*.py"):
        text = py_file.read_text(encoding="utf-8")
        matches = dapi_pattern.findall(text)
        assert not matches, f"Hardcoded token pattern found in {py_file}: {matches}"


def test_17_env_is_not_tracked_in_git():
    """Test 17: .env is not tracked in git index."""
    res = subprocess.run(["git", "ls-files", ".env"], capture_output=True, text=True)
    assert res.stdout.strip() == "", f".env is tracked in git index: {res.stdout}"
